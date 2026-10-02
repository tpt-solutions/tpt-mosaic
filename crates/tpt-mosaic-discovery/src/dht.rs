//! Mesh-backed peer directory implementing [`DhtClient`] (spec §6.3).
//!
//! This is the v0 directory: `announce` pushes an advertisement to every live
//! peer, `lookup` unions the local table with the tables of all reachable
//! peers (queried with [`DhtQuery`] frames, answered with [`PeerGossip`]).
//! There is no key-based routing yet — `key` is carried in the wire format
//! but matching is by capability filter. A Kademlia-style successor will add
//! rendezvous routing once wide-area transports exist.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tpt_mosaic_core::{CapabilityFlags, MosaicError, NodeId, NodeKind};
use tpt_mosaic_proto::codec::MAX_GOSSIP_PEERS;
use tpt_mosaic_proto::{DhtQuery, PeerAdvert, PeerGossip, WireMessage};

use crate::transport::TcpMesh;
use crate::{PeerRecord, PeerTable};

/// Build the reply for an inbound [`DhtQuery`]: every live entry in `table`
/// whose capabilities satisfy the query's filter and which is currently
/// available, capped at the query's limit and [`MAX_GOSSIP_PEERS`].
pub fn answer_query(table: &PeerTable, query: &DhtQuery) -> PeerGossip {
    let mut peers = Vec::new();
    for record in table.live_peers() {
        if peers.len() >= query.limit as usize || peers.len() >= MAX_GOSSIP_PEERS {
            break;
        }
        if !record.capabilities.contains(query.capability_filter) {
            continue;
        }
        if !record.hardware.is_available() {
            continue;
        }
        peers.push(PeerAdvert {
            node_id: record.node_id,
            addr: record.addr,
            hardware: record.hardware,
            capabilities: record.capabilities,
        });
    }
    PeerGossip { peers }
}

/// `DhtClient` over the TCP mesh, backed by a shared [`PeerTable`].
pub struct MeshDht {
    /// This node's own advertisement, included in lookups when it matches.
    self_advert: PeerAdvert,
    peers: Arc<Mutex<PeerTable>>,
    timeout: Duration,
}

impl MeshDht {
    /// Create a directory bound to `peers`; `self_advert` represents this
    /// node in announce/lookup results.
    pub fn new(self_advert: PeerAdvert, peers: Arc<Mutex<PeerTable>>, timeout: Duration) -> Self {
        Self {
            self_advert,
            peers,
            timeout,
        }
    }

    /// Push `advert` to every live peer with a mesh address.
    ///
    /// Returns the number of peers the frame was successfully written to.
    /// Best-effort by design: unreachable peers simply miss the update and
    /// catch up on the next exchange.
    pub fn advertise(&self, advert: PeerAdvert) -> usize {
        let targets: Vec<_> = {
            let peers = self.peers.lock().expect("peer table poisoned");
            peers
                .live_peers()
                .filter(|p| p.node_id != advert.node_id)
                .filter_map(|p| p.addr)
                .collect()
        };
        let frame = WireMessage::PeerGossip(PeerGossip {
            peers: vec![advert],
        });
        targets
            .iter()
            .filter(|addr| TcpMesh::send(**addr, &frame, self.timeout))
            .count()
    }

    /// Live peers with a mesh address (lookup query targets).
    fn targets(&self) -> Vec<std::net::SocketAddr> {
        let peers = self.peers.lock().expect("peer table poisoned");
        peers
            .live_peers()
            .filter(|p| p.node_id != self.self_advert.node_id)
            .filter_map(|p| p.addr)
            .collect()
    }

    /// Apply the lookup filter to an advert and convert it to a record.
    fn matches(query: &DhtQuery, advert: &PeerAdvert) -> Option<PeerRecord> {
        if !advert.capabilities.contains(query.capability_filter) {
            return None;
        }
        if !advert.hardware.is_available() {
            return None;
        }
        Some(PeerRecord {
            node_id: advert.node_id,
            hardware: advert.hardware,
            capabilities: advert.capabilities,
            addr: advert.addr,
            last_seen: std::time::Instant::now(),
        })
    }
}

impl crate::DhtClient for MeshDht {
    fn announce(&self, node_id: NodeId, capabilities: CapabilityFlags) -> Result<(), MosaicError> {
        // Prefer the full local record for `node_id` when we have one; the
        // trait signature only carries identity + capabilities, so unknown
        // nodes are announced without hardware details or address.
        let advert = {
            let peers = self.peers.lock().expect("peer table poisoned");
            match peers.get(&node_id) {
                Some(record) => PeerAdvert {
                    node_id,
                    addr: record.addr,
                    hardware: record.hardware,
                    capabilities: record.capabilities,
                },
                None if node_id == self.self_advert.node_id => self.self_advert,
                None => PeerAdvert {
                    node_id,
                    addr: None,
                    hardware: tpt_mosaic_core::HardwareProfile {
                        kind: NodeKind::EdgeTile,
                        gpu_vendor: tpt_mosaic_core::GpuVendor::None,
                        npu_present: false,
                        cpu_arch: tpt_mosaic_core::CpuArch::Other,
                        memory_mb: 0,
                        battery_level: 255,
                        thermal_state: tpt_mosaic_core::ThermalState::Nominal,
                    },
                    capabilities,
                },
            }
        };
        self.advertise(advert);
        Ok(())
    }

    fn lookup(
        &self,
        key: &[u8; 32],
        capability_filter: CapabilityFlags,
        limit: u16,
    ) -> Result<Vec<PeerRecord>, MosaicError> {
        let query = DhtQuery {
            key: *key,
            limit,
            capability_filter,
        };

        // Own table first, then every reachable peer's table.
        let mut results: Vec<PeerRecord> = Vec::new();
        {
            let peers = self.peers.lock().expect("peer table poisoned");
            for record in peers.live_peers() {
                let advert = PeerAdvert {
                    node_id: record.node_id,
                    addr: record.addr,
                    hardware: record.hardware,
                    capabilities: record.capabilities,
                };
                if let Some(record) = Self::matches(&query, &advert) {
                    results.push(record);
                }
            }
        }
        if capability_filter.is_empty() || self.self_advert.capabilities.contains(capability_filter)
        {
            if let Some(record) = Self::matches(&query, &self.self_advert) {
                results.push(record);
            }
        }

        for addr in self.targets() {
            if results.len() >= limit as usize {
                break;
            }
            if let Some(WireMessage::PeerGossip(gossip)) =
                TcpMesh::exchange(addr, &WireMessage::DhtQuery(query.clone()), self.timeout)
            {
                for advert in gossip.peers {
                    if let Some(record) = Self::matches(&query, &advert) {
                        results.push(record);
                    }
                }
            }
        }

        // Dedupe by identity (first seen wins), order deterministically.
        results.sort_by_key(|r| r.node_id);
        results.dedup_by(|a, b| a.node_id == b.node_id);
        results.truncate(limit as usize);
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::FrameHandler;
    use std::net::{SocketAddr, TcpListener};

    fn hardware(kind: NodeKind) -> tpt_mosaic_core::HardwareProfile {
        tpt_mosaic_core::HardwareProfile {
            kind,
            gpu_vendor: tpt_mosaic_core::GpuVendor::None,
            npu_present: false,
            cpu_arch: tpt_mosaic_core::CpuArch::X86_64,
            memory_mb: 8192,
            battery_level: 255,
            thermal_state: tpt_mosaic_core::ThermalState::Nominal,
        }
    }

    fn advert(id: u8, port: u16, caps: CapabilityFlags) -> PeerAdvert {
        PeerAdvert {
            node_id: NodeId::from_bytes([id; 16]),
            addr: Some(SocketAddr::from(([127, 0, 0, 1], port))),
            hardware: hardware(NodeKind::EdgeTile),
            capabilities: caps,
        }
    }

    fn record_from(advert: &PeerAdvert) -> PeerRecord {
        PeerRecord {
            node_id: advert.node_id,
            hardware: advert.hardware,
            capabilities: advert.capabilities,
            addr: advert.addr,
            last_seen: std::time::Instant::now(),
        }
    }

    /// Spawn a DhtQuery-answering mesh node over `table`. Gossiped adverts
    /// are upserted, mirroring what real participants do.
    fn serve(table: Arc<Mutex<PeerTable>>) -> (SocketAddr, Arc<std::sync::atomic::AtomicBool>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("addr");
        let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let handler: FrameHandler = Arc::new(move |msg| match msg {
            WireMessage::DhtQuery(query) => Some(WireMessage::PeerGossip(answer_query(
                &table.lock().expect("poisoned"),
                query,
            ))),
            WireMessage::PeerGossip(gossip) => {
                let mut table = table.lock().expect("poisoned");
                for advert in &gossip.peers {
                    table.upsert(record_from(advert));
                }
                None
            }
            _ => None,
        });
        TcpMesh::serve(listener, handler, running.clone());
        (addr, running)
    }

    #[test]
    fn lookup_unions_local_and_remote_tables() {
        // Node B advertises CUDA; node A knows B's address and queries it.
        let b_advert = advert(2, 1, CapabilityFlags::CUDA);
        let table_b = Arc::new(Mutex::new(PeerTable::new(Duration::from_secs(30))));
        table_b
            .lock()
            .expect("poisoned")
            .upsert(record_from(&advert(9, 9, CapabilityFlags::empty())));
        let (b_addr, b_running) = serve(Arc::clone(&table_b));

        let table_a = Arc::new(Mutex::new(PeerTable::new(Duration::from_secs(30))));
        table_a
            .lock()
            .expect("poisoned")
            .upsert(record_from(&PeerAdvert {
                addr: Some(b_addr),
                ..b_advert
            }));

        let dht = MeshDht::new(
            advert(1, 0, CapabilityFlags::CPU_VECTOR),
            Arc::clone(&table_a),
            Duration::from_secs(2),
        );
        let found = crate::DhtClient::lookup(&dht, &[0u8; 32], CapabilityFlags::CUDA, 10)
            .expect("lookup must not fail");
        assert_eq!(found.len(), 1, "exactly B matches the CUDA filter");
        assert_eq!(found[0].node_id, NodeId::from_bytes([2; 16]));
        b_running.store(false, std::sync::atomic::Ordering::Relaxed);
    }

    #[test]
    fn announce_pushes_the_advert_to_live_peers() {
        let table_b = Arc::new(Mutex::new(PeerTable::new(Duration::from_secs(30))));
        let (b_addr, b_running) = serve(Arc::clone(&table_b));

        // A knows only B; announcing A's CUDA capability must land in B's table.
        let table_a = Arc::new(Mutex::new(PeerTable::new(Duration::from_secs(30))));
        table_a
            .lock()
            .expect("poisoned")
            .upsert(record_from(&advert(9, 9, CapabilityFlags::empty())));
        // Point B's record at its real address for A's announce targets.
        let mut b_record = record_from(&advert(2, 0, CapabilityFlags::CUDA));
        b_record.addr = Some(b_addr);
        table_a.lock().expect("poisoned").upsert(b_record);

        let dht = MeshDht::new(
            advert(1, 0, CapabilityFlags::CUDA),
            Arc::clone(&table_a),
            Duration::from_secs(2),
        );
        crate::DhtClient::announce(&dht, NodeId::from_bytes([1; 16]), CapabilityFlags::CUDA)
            .expect("announce is best-effort and never fails");

        assert!(
            wait_for(
                || table_b
                    .lock()
                    .expect("poisoned")
                    .get(&NodeId::from_bytes([1; 16]))
                    .is_some(),
                50
            ),
            "B must learn about A via the announce"
        );
        b_running.store(false, std::sync::atomic::Ordering::Relaxed);
    }

    #[test]
    fn lookup_respects_limit_and_capability_filter() {
        let table = Arc::new(Mutex::new(PeerTable::new(Duration::from_secs(30))));
        for id in 1..=4u8 {
            let caps = if id % 2 == 0 {
                CapabilityFlags::NPU
            } else {
                CapabilityFlags::CPU_VECTOR
            };
            table
                .lock()
                .expect("poisoned")
                .upsert(record_from(&advert(id, id as u16, caps)));
        }
        let dht = MeshDht::new(
            advert(9, 0, CapabilityFlags::empty()),
            Arc::clone(&table),
            Duration::from_secs(2),
        );

        let npu = crate::DhtClient::lookup(&dht, &[0; 32], CapabilityFlags::NPU, 10).unwrap();
        assert_eq!(npu.len(), 2, "only the two NPU peers match");
        assert!(npu
            .iter()
            .all(|r| r.capabilities.contains(CapabilityFlags::NPU)));

        let limited =
            crate::DhtClient::lookup(&dht, &[0; 32], CapabilityFlags::empty(), 2).unwrap();
        assert_eq!(limited.len(), 2, "limit caps the result set");
    }

    fn wait_for(mut check: impl FnMut() -> bool, attempts: usize) -> bool {
        for _ in 0..attempts {
            if check() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        check()
    }
}
