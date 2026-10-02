//! In-memory peer table with automatic staleness eviction.

use std::collections::HashMap;
use std::time::Duration;

use crate::PeerRecord;
use tpt_mosaic_core::NodeId;

/// Thread-safe in-memory registry of live peers.
///
/// Records are evicted when they exceed `max_age` without a refresh.
#[derive(Debug, Default)]
pub struct PeerTable {
    records: HashMap<NodeId, PeerRecord>,
    max_age: Duration,
}

impl PeerTable {
    /// Create a new table with the given staleness threshold.
    pub fn new(max_age: Duration) -> Self {
        Self {
            records: HashMap::new(),
            max_age,
        }
    }

    /// Insert or refresh a peer record.
    pub fn upsert(&mut self, record: PeerRecord) {
        self.records.insert(record.node_id, record);
    }

    /// Evict all records older than `max_age` and return how many were removed.
    pub fn evict_stale(&mut self) -> usize {
        let max_age = self.max_age;
        let before = self.records.len();
        self.records.retain(|_, r| r.is_fresh(max_age));
        before - self.records.len()
    }

    /// Return an iterator over all live (non-stale) peer records.
    pub fn live_peers(&self) -> impl Iterator<Item = &PeerRecord> {
        let max_age = self.max_age;
        self.records.values().filter(move |r| r.is_fresh(max_age))
    }

    /// Look up a specific peer by `NodeId`.
    pub fn get(&self, id: &NodeId) -> Option<&PeerRecord> {
        self.records.get(id)
    }

    /// Number of records in the table (including potentially stale ones).
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Returns `true` if the table has no records.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Instant;
    use tpt_mosaic_core::{
        CapabilityFlags, CpuArch, GpuVendor, HardwareProfile, NodeId, NodeKind, ThermalState,
    };

    fn make_record(id: [u8; 16]) -> PeerRecord {
        PeerRecord {
            node_id: NodeId::from_bytes(id),
            hardware: HardwareProfile {
                kind: NodeKind::EdgeTile,
                gpu_vendor: GpuVendor::None,
                npu_present: false,
                cpu_arch: CpuArch::Aarch64,
                memory_mb: 4096,
                battery_level: 80,
                thermal_state: ThermalState::Nominal,
            },
            capabilities: CapabilityFlags::CPU_VECTOR,
            addr: None,
            last_seen: Instant::now(),
        }
    }

    #[test]
    fn upsert_and_live() {
        let mut table = PeerTable::new(Duration::from_secs(30));
        table.upsert(make_record([1u8; 16]));
        table.upsert(make_record([2u8; 16]));
        assert_eq!(table.live_peers().count(), 2);
    }

    #[test]
    fn evict_stale() {
        let mut table = PeerTable::new(Duration::from_millis(10));
        table.upsert(make_record([1u8; 16]));
        thread::sleep(Duration::from_millis(20));
        let evicted = table.evict_stale();
        assert_eq!(evicted, 1);
        assert!(table.is_empty());
    }
}
