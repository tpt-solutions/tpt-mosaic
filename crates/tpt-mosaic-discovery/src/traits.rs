//! Discovery layer trait definitions.

use crate::PeerRecord;
use tpt_mosaic_core::{CapabilityFlags, MosaicError, NodeId};
use tpt_mosaic_proto::HeartbeatBeacon;

/// Broadcasts heartbeat beacons over BLE, UWB, or Wi-Fi.
pub trait BeaconBroadcaster: Send + Sync {
    /// Encode and broadcast a heartbeat beacon to local peers.
    fn broadcast(&self, beacon: &HeartbeatBeacon) -> Result<(), MosaicError>;
}

/// Scans for incoming heartbeat beacons and converts them to [`PeerRecord`]s.
pub trait BeaconScanner: Send + Sync {
    /// Block (or yield) until a beacon arrives, then decode it into a `PeerRecord`.
    fn next_beacon(&self) -> Result<PeerRecord, MosaicError>;
}

/// Global distributed hash table interface for cross-region peer lookup.
pub trait DhtClient: Send + Sync {
    /// Look up peers that satisfy `capability_filter`, returning up to `limit` results.
    fn lookup(
        &self,
        key: &[u8; 32],
        capability_filter: CapabilityFlags,
        limit: u16,
    ) -> Result<Vec<PeerRecord>, MosaicError>;

    /// Announce this node's existence and capabilities to the DHT.
    fn announce(&self, node_id: NodeId, capabilities: CapabilityFlags) -> Result<(), MosaicError>;
}
