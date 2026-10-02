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
