//! Quorum assembly, heterogeneous hardware matching, straggler detection, and task routing.

#![deny(missing_docs)]

use tpt_mosaic_core::{CapabilityFlags, MosaicError, NodeId, QuorumConfig};
use tpt_mosaic_discovery::PeerRecord;

pub use straggler::{DispatchTracker, StragglerPolicy};

mod straggler;

/// Policy governing how quorums are assembled from the peer table.
pub trait SchedulerPolicy: Send + Sync {
    /// Select exactly `config.n` peers from `candidates` that satisfy the policy.
    ///
    /// Implementations must ensure hardware diversity (GPU vendors, CPU architectures)
    /// and the correct edge/anchor mix.
    fn assemble(
        &self,
        candidates: &[PeerRecord],
        config: &QuorumConfig,
        required_capabilities: CapabilityFlags,
    ) -> Result<Vec<NodeId>, MosaicError>;
}

/// Default heterogeneous quorum assembler.
///
/// Selects nodes to maximise GPU vendor and CPU architecture diversity, then
/// fills remaining slots with any available compatible node.
#[derive(Debug, Default)]
pub struct HeterogeneousAssembler;

impl SchedulerPolicy for HeterogeneousAssembler {
    fn assemble(
        &self,
        candidates: &[PeerRecord],
        config: &QuorumConfig,
        required_capabilities: CapabilityFlags,
    ) -> Result<Vec<NodeId>, MosaicError> {
        if !config.is_valid() {
            return Err(MosaicError::InvalidQuorumConfig);
        }

        // Filter to nodes that are available and have the required capabilities.
        let mut eligible: Vec<&PeerRecord> = candidates
            .iter()
            .filter(|p| p.hardware.is_available() && p.capabilities.contains(required_capabilities))
            .collect();

        if eligible.len() < config.n as usize {
            return Err(MosaicError::InsufficientCapability);
        }

        // Sort to maximise diversity: anchor nodes first, then by GPU vendor (as u8).
        eligible.sort_unstable_by_key(|p| {
            (
                !p.hardware
                    .kind
                    .eq(&tpt_mosaic_core::NodeKind::AnchorBallast),
                p.hardware.gpu_vendor as u8,
                p.hardware.cpu_arch as u8,
            )
        });

        // Greedily pick N nodes, preferring diverse hardware.
        let mut selected: Vec<NodeId> = Vec::with_capacity(config.n as usize);
        let mut seen_gpu_vendors = std::collections::HashSet::new();

        for peer in &eligible {
            if selected.len() >= config.n as usize {
                break;
            }
            if seen_gpu_vendors.insert(peer.hardware.gpu_vendor as u8) || selected.is_empty() {
                selected.push(peer.node_id);
            }
        }

        // Fill remaining slots if diversity pool was exhausted.
        for peer in &eligible {
            if selected.len() >= config.n as usize {
                break;
            }
            if !selected.contains(&peer.node_id) {
                selected.push(peer.node_id);
            }
        }

        Ok(selected)
    }
}

/// Quorum assembler with an explicit edge/anchor balance (spec §3.2: the mix
/// is intentional, not incidental).
///
/// Selection picks [`Self::min_anchors`]..[`Self::max_anchors`] anchor nodes
/// (diversity-first within each class), fills the rest with edge tiles, and
/// tops up from the other pool when one class runs short — meeting the quorum
/// takes precedence over honoring the cap.
#[derive(Debug, Clone)]
pub struct BalancedAssembler {
    /// Fewest anchor nodes to include when that many are available.
    pub min_anchors: usize,
    /// Most anchor nodes to include while the quorum can still be met.
    pub max_anchors: usize,
}

impl Default for BalancedAssembler {
    fn default() -> Self {
        Self {
            min_anchors: 1,
            // Uncapped: anchors remain preferred (as in
            // `HeterogeneousAssembler`) unless a cap is configured explicitly.
            max_anchors: usize::MAX,
        }
    }
}

/// Greedy diversity-first fill up to `take` total selections from an
/// anchor-ordered pool: prefer unseen GPU vendors, then accept duplicates.
fn pick_diverse(pool: &[&PeerRecord], take: usize, selected: &mut Vec<NodeId>) {
    let mut seen_gpu_vendors = std::collections::HashSet::new();
    for peer in pool {
        if selected.len() >= take {
            break;
        }
        if (selected.is_empty() || seen_gpu_vendors.insert(peer.hardware.gpu_vendor as u8))
            && !selected.contains(&peer.node_id)
        {
            selected.push(peer.node_id);
        }
    }
    for peer in pool {
        if selected.len() >= take {
            break;
        }
        if !selected.contains(&peer.node_id) {
            selected.push(peer.node_id);
        }
    }
}

impl SchedulerPolicy for BalancedAssembler {
    fn assemble(
        &self,
        candidates: &[PeerRecord],
        config: &QuorumConfig,
        required_capabilities: CapabilityFlags,
    ) -> Result<Vec<NodeId>, MosaicError> {
        if !config.is_valid() {
            return Err(MosaicError::InvalidQuorumConfig);
        }
        let n = config.n as usize;

        let mut eligible: Vec<&PeerRecord> = candidates
            .iter()
            .filter(|p| p.hardware.is_available() && p.capabilities.contains(required_capabilities))
            .collect();
        // Anchors first, then GPU/CPU diversity ordering (matches
        // `HeterogeneousAssembler`), so both pools inherit the preference.
        eligible.sort_unstable_by_key(|p| {
            (
                !matches!(p.hardware.kind, tpt_mosaic_core::NodeKind::AnchorBallast),
                p.hardware.gpu_vendor as u8,
                p.hardware.cpu_arch as u8,
            )
        });
        let (anchors, edges): (Vec<&PeerRecord>, Vec<&PeerRecord>) = eligible
            .iter()
            .partition(|p| matches!(p.hardware.kind, tpt_mosaic_core::NodeKind::AnchorBallast));

        let avail_anchors = anchors.len();
        let anchor_take = avail_anchors
            .min(self.max_anchors)
            .max(self.min_anchors.min(avail_anchors))
            .min(n);

        let mut out = Vec::with_capacity(n);
        pick_diverse(&anchors, anchor_take, &mut out);
        pick_diverse(&edges, n, &mut out);

        // Shortfall in one pool: top up from the other — meeting the quorum
        // wins over honoring the cap.
        if out.len() < n {
            pick_diverse(&eligible, n, &mut out);
        }
        if out.len() < n {
            return Err(MosaicError::InsufficientCapability);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod balanced_tests {
    use super::*;
    use std::net::SocketAddr;
    use std::time::Instant;
    use tpt_mosaic_core::{
        CapabilityFlags, CpuArch, GpuVendor, HardwareProfile, NodeKind, ThermalState, TierLevel,
    };

    fn record(id: u8, kind: NodeKind, gpu: GpuVendor) -> PeerRecord {
        PeerRecord {
            node_id: NodeId::from_bytes([id; 16]),
            hardware: HardwareProfile {
                kind,
                gpu_vendor: gpu,
                npu_present: false,
                cpu_arch: CpuArch::X86_64,
                memory_mb: 8192,
                battery_level: 255,
                thermal_state: ThermalState::Nominal,
            },
            capabilities: CapabilityFlags::empty(),
            addr: Some(SocketAddr::from(([127, 0, 0, 1], id as u16))),
            last_seen: Instant::now(),
        }
    }

    fn count_anchors(selected: &[NodeId], candidates: &[PeerRecord]) -> usize {
        selected
            .iter()
            .filter(|id| {
                candidates.iter().any(|c| {
                    c.node_id == **id && matches!(c.hardware.kind, NodeKind::AnchorBallast)
                })
            })
            .count()
    }

    #[test]
    fn min_anchors_is_honored_when_available() {
        let candidates = [
            record(1, NodeKind::EdgeTile, GpuVendor::Nvidia),
            record(2, NodeKind::EdgeTile, GpuVendor::Amd),
            record(3, NodeKind::AnchorBallast, GpuVendor::None),
            record(4, NodeKind::AnchorBallast, GpuVendor::None),
        ];
        let policy = BalancedAssembler {
            min_anchors: 1,
            max_anchors: 1,
        };
        let selected = policy
            .assemble(
                &candidates,
                &QuorumConfig::new(2, 3, TierLevel::Standard),
                CapabilityFlags::empty(),
            )
            .expect("quorum must assemble");
        assert_eq!(selected.len(), 3);
        assert_eq!(count_anchors(&selected, &candidates), 1);
    }

    #[test]
    fn quorum_wins_over_the_anchor_cap() {
        // 1 edge only, 3 anchors, n = 3: a hard cap of 1 would strand the
        // round, so the shortfall is topped up from the anchor pool.
        let candidates = [
            record(1, NodeKind::EdgeTile, GpuVendor::Nvidia),
            record(2, NodeKind::AnchorBallast, GpuVendor::None),
            record(3, NodeKind::AnchorBallast, GpuVendor::None),
            record(4, NodeKind::AnchorBallast, GpuVendor::None),
        ];
        let policy = BalancedAssembler {
            min_anchors: 0,
            max_anchors: 1,
        };
        let selected = policy
            .assemble(
                &candidates,
                &QuorumConfig::new(2, 3, TierLevel::Standard),
                CapabilityFlags::empty(),
            )
            .expect("shortfall must be topped up from the anchor pool");
        assert_eq!(selected.len(), 3);
    }

    #[test]
    fn anchors_are_prioritized_within_the_cap() {
        let candidates = [
            record(1, NodeKind::EdgeTile, GpuVendor::Nvidia),
            record(2, NodeKind::AnchorBallast, GpuVendor::None),
            record(3, NodeKind::AnchorBallast, GpuVendor::None),
            record(4, NodeKind::EdgeTile, GpuVendor::Amd),
        ];
        let policy = BalancedAssembler {
            min_anchors: 2,
            max_anchors: 2,
        };
        let selected = policy
            .assemble(
                &candidates,
                &QuorumConfig::new(3, 4, TierLevel::Standard),
                CapabilityFlags::empty(),
            )
            .expect("quorum must assemble");
        assert_eq!(selected.len(), 4);
        assert_eq!(count_anchors(&selected, &candidates), 2);
    }

    #[test]
    fn insufficient_when_both_pools_run_dry() {
        let candidates = [record(1, NodeKind::EdgeTile, GpuVendor::Nvidia)];
        let policy = BalancedAssembler::default();
        let err = policy
            .assemble(
                &candidates,
                &QuorumConfig::new(2, 3, TierLevel::Standard),
                CapabilityFlags::empty(),
            )
            .expect_err("3 nodes cannot come from 1 candidate");
        assert!(matches!(err, MosaicError::InsufficientCapability));
    }

    #[test]
    fn invalid_config_rejected() {
        let candidates = [record(1, NodeKind::EdgeTile, GpuVendor::None)];
        let policy = BalancedAssembler::default();
        assert!(policy
            .assemble(
                &candidates,
                &QuorumConfig::new(6, 3, TierLevel::Standard),
                CapabilityFlags::empty(),
            )
            .is_err());
    }

    #[test]
    fn default_uncapped_keeps_anchor_preference() {
        let candidates = [
            record(1, NodeKind::AnchorBallast, GpuVendor::None),
            record(2, NodeKind::EdgeTile, GpuVendor::Nvidia),
            record(3, NodeKind::EdgeTile, GpuVendor::Amd),
        ];
        let policy = BalancedAssembler::default();
        let selected = policy
            .assemble(
                &candidates,
                &QuorumConfig::new(2, 2, TierLevel::Standard),
                CapabilityFlags::empty(),
            )
            .expect("quorum must assemble");
        assert!(
            selected.contains(&NodeId::from_bytes([1; 16])),
            "anchor sorts first by default"
        );
    }
}
