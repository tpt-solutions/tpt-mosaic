//! Peer discovery and network topology management.
//!
//! Handles BLE/UWB beacon broadcasting and scanning, Wi-Fi mesh peer detection,
//! capability advertisement, heartbeat protocol, and DHT integration.

#![deny(missing_docs)]

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use tpt_mosaic_core::{CapabilityFlags, HardwareProfile, NodeId};

pub use dht::{answer_query, MeshDht};
pub use peer_table::PeerTable;
pub use traits::{BeaconBroadcaster, BeaconScanner, DhtClient};
pub use transport::{FrameHandler, TcpMesh};

mod dht;
mod peer_table;
mod traits;
mod transport;

/// A snapshot of a discovered peer's state.
#[derive(Debug, Clone)]
pub struct PeerRecord {
    /// Unique identifier of the peer.
    pub node_id: NodeId,
    /// Last-known hardware profile.
    pub hardware: HardwareProfile,
    /// Compute capability flags.
    pub capabilities: CapabilityFlags,
    /// The peer's mesh listen address, when advertised. Task dispatch and
    /// beacon exchange target this.
    pub addr: Option<SocketAddr>,
    /// When this record was last updated.
    pub last_seen: Instant,
}

impl PeerRecord {
    /// Returns `true` if this record is fresher than `max_age`.
    pub fn is_fresh(&self, max_age: Duration) -> bool {
        self.last_seen.elapsed() < max_age
    }
}
