//! LAN zero-config discovery via mDNS (feature `mdns`).
//!
//! Nodes advertise a service instance named after their [`NodeId`] carrying
//! their mesh port; a browser thread watches for peers and records their
//! addresses. Discovered addresses feed the existing beacon/gossip exchange
//! — mDNS only bootstraps *where* peers are, while the MOSA protocol (spec
//! §4) supplies everything else. That keeps TXT payloads empty and lets
//! hardware/capability data keep flowing through the versioned protocol.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use mdns_sd::{ServiceDaemon, ServiceEvent, ServiceInfo};
use tpt_mosaic_core::NodeId;

/// mDNS service type every tpt-mosaic node registers under.
pub const MDNS_SERVICE: &str = "_mosaic._udp.local.";

/// Instance-name prefix; the rest is the 32-hex node identity.
const INSTANCE_PREFIX: &str = "mosaic-";

/// How long the browser waits for an event before re-checking the stop flag.
const BROWSE_POLL: Duration = Duration::from_millis(250);

/// Build the mDNS instance name for a node: `mosaic-<32 hex chars>`.
pub fn instance_name(node_id: NodeId) -> String {
    let mut hex = String::with_capacity(32);
    for b in node_id.as_bytes() {
        use std::fmt::Write;
        let _ = write!(hex, "{b:02x}");
    }
    format!("{INSTANCE_PREFIX}{hex}")
}

/// Parse a node identity out of an mDNS instance name; `None` for foreign
/// services or malformed identities.
pub fn node_id_from_instance(fullname: &str) -> Option<NodeId> {
    let rest = fullname.strip_prefix(INSTANCE_PREFIX)?;
    let body = rest.split('.').next()?; // instance names may carry port suffixes
    if body.len() != 32 {
        return None;
    }
    let mut bytes = [0u8; 16];
    for (i, chunk) in body.as_bytes().chunks(2).enumerate() {
        let hi = (chunk[0] as char).to_digit(16)?;
        let lo = (chunk[1] as char).to_digit(16)?;
        bytes[i] = (hi * 16 + lo) as u8;
    }
    Some(NodeId::from_bytes(bytes))
}

/// Live mDNS advertiser + browser for this node.
///
/// Dropping the handle stops the browser thread and unregisters the service
/// (best-effort; the TTL expires it regardless).
pub struct MdnsHandle {
    discovered: Arc<Mutex<HashMap<NodeId, SocketAddr>>>,
    stop: Arc<AtomicBool>,
    daemon: Option<ServiceDaemon>,
    full_name: String,
    browser: Option<JoinHandle<()>>,
}

impl MdnsHandle {
    /// Advertise `node_id` on `mesh_port` and start browsing for peers.
    ///
    /// Failure to start (multicast blocked, etc.) surfaces as `Err`; callers
    /// typically degrade to static seeds with a warning.
    pub fn spawn(node_id: NodeId, mesh_port: u16) -> Result<Self, String> {
        let daemon = ServiceDaemon::new().map_err(|e| e.to_string())?;
        let instance = instance_name(node_id);
        let full_name = format!("{instance}.{MDNS_SERVICE}");
        let host = format!("{instance}.local.");
        let info = ServiceInfo::new(
            MDNS_SERVICE,
            &instance,
            &host,
            "0.0.0.0",
            mesh_port,
            None::<HashMap<String, String>>,
        )
        .map_err(|e| e.to_string())?
        .enable_addr_auto();
        daemon.register(info).map_err(|e| e.to_string())?;

        let discovered: Arc<Mutex<HashMap<NodeId, SocketAddr>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let receiver = daemon.browse(MDNS_SERVICE).map_err(|e| e.to_string())?;

        let browser = {
            let discovered = Arc::clone(&discovered);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || loop {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                match receiver.recv_timeout(BROWSE_POLL) {
                    Ok(ServiceEvent::ServiceResolved(info)) => {
                        let Some(peer) = node_id_from_instance(info.get_fullname()) else {
                            continue;
                        };
                        let Some(addr) = info.get_addresses_v4().iter().next().copied() else {
                            continue;
                        };
                        let addr = SocketAddr::new((*addr).into(), info.get_port());
                        discovered
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .insert(peer, addr);
                    }
                    Ok(ServiceEvent::ServiceRemoved(_, fullname)) => {
                        if let Some(peer) = node_id_from_instance(&fullname) {
                            discovered
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .remove(&peer);
                        }
                    }
                    Ok(_) => {}
                    Err(_) => break, // channel closed (daemon stopped)
                }
            })
        };

        Ok(Self {
            discovered,
            stop,
            daemon: Some(daemon),
            full_name,
            browser: Some(browser),
        })
    }

    /// Addresses of peers currently seen on the LAN, for the beacon
    /// exchange to treat as seeds.
    pub fn targets(&self) -> Vec<SocketAddr> {
        self.discovered
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .copied()
            .collect()
    }

    /// Identities currently discovered (diagnostics).
    pub fn discovered_ids(&self) -> Vec<NodeId> {
        self.discovered
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .keys()
            .copied()
            .collect()
    }
}

impl Drop for MdnsHandle {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(daemon) = self.daemon.take() {
            let _ = daemon.unregister(&self.full_name);
            let _ = daemon.shutdown();
        }
        if let Some(browser) = self.browser.take() {
            let _ = browser.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instance_name_round_trips_the_identity() {
        let id = NodeId::from_bytes([
            0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54,
            0x32, 0x10,
        ]);
        let name = instance_name(id);
        assert!(name.starts_with(INSTANCE_PREFIX));
        assert_eq!(name.len(), INSTANCE_PREFIX.len() + 32);
        assert_eq!(node_id_from_instance(&name), Some(id));
        // Resolved names carry service suffixes; parsing tolerates them.
        assert_eq!(
            node_id_from_instance(&format!("{name}.{MDNS_SERVICE}")),
            Some(id)
        );
    }

    #[test]
    fn foreign_or_malformed_instances_parse_to_none() {
        assert!(node_id_from_instance("some-other-service._mosaic._udp.local.").is_none());
        assert!(node_id_from_instance("mosaic-not-hex").is_none());
        assert!(node_id_from_instance("mosaic-00112233").is_none()); // too short
    }

    /// Same-host mDNS exchange: two handles on this machine must discover
    /// each other. Multicast behaviour is environment-dependent (firewalls,
    /// interface state), so this is `#[ignore]`d for CI; run it locally with
    /// `cargo test -p tpt-mosaic-discovery --features mdns -- --ignored`.
    #[test]
    #[ignore = "requires a live multicast-capable network interface"]
    fn two_handles_discover_each_other_on_the_lan() {
        let a_id = NodeId::from_bytes([1; 16]);
        let b_id = NodeId::from_bytes([2; 16]);
        let a = MdnsHandle::spawn(a_id, 0).expect("mdns start");
        let b = MdnsHandle::spawn(b_id, 0).expect("mdns start");

        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        while std::time::Instant::now() < deadline {
            if a.discovered_ids().contains(&b_id) && b.discovered_ids().contains(&a_id) {
                assert!(!a.targets().is_empty());
                return;
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        panic!("handles did not discover each other within 15s");
    }
}
