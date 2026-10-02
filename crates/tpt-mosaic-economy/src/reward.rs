//! Micro-reward calculation and slashing records.
//!
//! Rewards scale with the quorum tier (§7 of the design spec: a 7-of-10
//! heterogeneous quorum costs more than a 1-of-1 edge inference) and with the
//! hardware capability flags the contributing node advertised.

use tpt_mosaic_core::{CapabilityFlags, NodeId, QuorumConfig};

/// Base micro-reward per executed shard, Best Effort tier.
const BASE_PER_SHARD_BEST_EFFORT: u64 = 1;
/// Base micro-reward per executed shard, Standard tier.
const BASE_PER_SHARD_STANDARD: u64 = 4;
/// Base micro-reward per executed shard, Mission Critical tier.
const BASE_PER_SHARD_MISSION_CRITICAL: u64 = 10;
/// Bonus percentage points added to the payout per advertised capability flag.
const CAPABILITY_BONUS_PCT: u64 = 5;

/// Calculate the micro-reward (in chain-native micro-units) for one quorum
/// contribution that executed `shards_executed` micro-task shards.
///
/// Deterministic and side-effect free; the caller submits the result through a
/// [`crate::Settlement`] adapter.
pub fn calculate_reward(
    quorum: &QuorumConfig,
    capabilities: CapabilityFlags,
    shards_executed: u32,
) -> u64 {
    let base_per_shard = match quorum.tier {
        tpt_mosaic_core::TierLevel::BestEffort => BASE_PER_SHARD_BEST_EFFORT,
        tpt_mosaic_core::TierLevel::Standard => BASE_PER_SHARD_STANDARD,
        tpt_mosaic_core::TierLevel::MissionCritical => BASE_PER_SHARD_MISSION_CRITICAL,
    };
    let bonus_pct = 100 + capabilities.iter().count() as u64 * CAPABILITY_BONUS_PCT;
    base_per_shard * u64::from(shards_executed) * bonus_pct / 100
}

/// Why a node was slashed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlashReason {
    /// Node submitted hashes that consistently diverged from the quorum.
    ByzantineFault,
    /// Node repeatedly timed out without responding.
    RepeatedTimeout,
    /// Node attempted to escape its sandbox capability grants.
    SandboxViolation,
}

/// Record of a slashing event, produced by the slashing logic and handed to a
/// [`crate::Settlement`] adapter for on-chain reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlashingRecord {
    /// Node that was slashed.
    pub node_id: NodeId,
    /// Why the slash was issued.
    pub reason: SlashReason,
    /// Amount (chain-native micro-units) deducted.
    pub amount: u64,
    /// Unix timestamp (milliseconds) of the slashing event.
    pub recorded_at_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tpt_mosaic_core::{QuorumConfig, TierLevel};

    #[test]
    fn zero_shards_pay_nothing() {
        let reward = calculate_reward(&QuorumConfig::STANDARD_3_OF_5, CapabilityFlags::CUDA, 0);
        assert_eq!(reward, 0);
    }

    #[test]
    fn higher_tiers_pay_more() {
        let caps = CapabilityFlags::empty();
        let best = calculate_reward(&QuorumConfig::BEST_EFFORT_1_OF_1, caps, 10);
        let std = calculate_reward(&QuorumConfig::STANDARD_3_OF_5, caps, 10);
        let critical = calculate_reward(&QuorumConfig::MISSION_CRITICAL_7_OF_10, caps, 10);
        assert!(best < std);
        assert!(std < critical);
    }

    #[test]
    fn capability_flags_increase_payout() {
        let quorum = QuorumConfig::STANDARD_3_OF_5;
        let bare = calculate_reward(&quorum, CapabilityFlags::empty(), 1);
        let with_caps = calculate_reward(&quorum, CapabilityFlags::CUDA | CapabilityFlags::NPU, 1);
        // Two flags → +10% over the base payout.
        assert_eq!(with_caps, bare * 110 / 100);
    }

    #[test]
    fn reward_is_deterministic() {
        let quorum = QuorumConfig::MISSION_CRITICAL_7_OF_10;
        let caps = CapabilityFlags::VULKAN | CapabilityFlags::CPU_VECTOR;
        assert_eq!(
            calculate_reward(&quorum, caps, 7),
            calculate_reward(&quorum, caps, 7)
        );
    }
}
