//! Quorum configuration and tier-level definitions.

/// Compute confidence tier — the user's position on the cost / latency / confidence triangle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum TierLevel {
    /// 1-of-1 or 2-of-3. Low cost, ultra-low latency. Casual inference and non-critical tasks.
    BestEffort = 0,
    /// 3-of-5. Tolerates 2 faulty nodes. Financial analysis, medical screening, autonomous navigation.
    Standard = 1,
    /// 7-of-10+. Tolerates 3+ compromised nodes. High-value contracts, surgical robotics.
    MissionCritical = 2,
}

/// K-of-N quorum parameters for a single task.
///
/// The scheduler must assemble exactly `n` nodes and the quorum is met when
/// `k` of them return matching output hashes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuorumConfig {
    /// Number of agreeing hashes required to confirm the result.
    pub k: u8,
    /// Total number of nodes in the quorum.
    pub n: u8,
    /// Tier this config belongs to (informational; drives scheduler selection logic).
    pub tier: TierLevel,
}

impl QuorumConfig {
    /// Convenience constructor.
    #[inline]
    pub const fn new(k: u8, n: u8, tier: TierLevel) -> Self {
        Self { k, n, tier }
    }

    /// Returns `true` if `k <= n`, both are non-zero, and `k` is a strict
    /// majority of `n` (`2k > n`) so two different hashes can never both
    /// reach the threshold.
    #[inline]
    pub const fn is_valid(&self) -> bool {
        self.k > 0 && self.n > 0 && self.k <= self.n && (self.k as u16) * 2 > self.n as u16
    }

    /// Preset: Best Effort 1-of-1.
    pub const BEST_EFFORT_1_OF_1: Self = Self::new(1, 1, TierLevel::BestEffort);

    /// Preset: Best Effort 2-of-3.
    pub const BEST_EFFORT_2_OF_3: Self = Self::new(2, 3, TierLevel::BestEffort);

    /// Preset: Standard 3-of-5.
    pub const STANDARD_3_OF_5: Self = Self::new(3, 5, TierLevel::Standard);

    /// Preset: Mission Critical 7-of-10.
    pub const MISSION_CRITICAL_7_OF_10: Self = Self::new(7, 10, TierLevel::MissionCritical);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_configs() {
        assert!(QuorumConfig::BEST_EFFORT_1_OF_1.is_valid());
        assert!(QuorumConfig::STANDARD_3_OF_5.is_valid());
        assert!(QuorumConfig::MISSION_CRITICAL_7_OF_10.is_valid());
    }

    #[test]
    fn invalid_configs() {
        assert!(!QuorumConfig::new(0, 5, TierLevel::Standard).is_valid());
        assert!(!QuorumConfig::new(6, 5, TierLevel::Standard).is_valid());
        assert!(!QuorumConfig::new(1, 0, TierLevel::BestEffort).is_valid());
    }

    #[test]
    fn non_majority_threshold_is_invalid() {
        // k <= n/2 would let two different hashes both reach quorum.
        assert!(!QuorumConfig::new(2, 4, TierLevel::Standard).is_valid());
        assert!(!QuorumConfig::new(1, 3, TierLevel::BestEffort).is_valid());
        assert!(QuorumConfig::new(3, 4, TierLevel::Standard).is_valid());
    }

    #[test]
    fn tier_ordering() {
        assert!(TierLevel::BestEffort < TierLevel::Standard);
        assert!(TierLevel::Standard < TierLevel::MissionCritical);
    }
}
