//! Timeout policy and straggler detection for dispatched quorum tasks
//! (spec §3.3: nodes that miss their deadline are replaced so the quorum can
//! still be met; spec §6.4: replacement-node dispatch).

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tpt_mosaic_core::{NodeId, QuorumConfig};

/// Policy governing dispatch deadlines and replacement budgeting.
#[derive(Debug, Clone)]
pub struct StragglerPolicy {
    /// How long a node may hold a dispatch before it counts as a straggler.
    pub dispatch_timeout: Duration,
}

impl Default for StragglerPolicy {
    fn default() -> Self {
        Self {
            dispatch_timeout: Duration::from_secs(30),
        }
    }
}

impl StragglerPolicy {
    /// Deadline for a dispatch made at `now`.
    pub fn deadline_from(&self, now: Instant) -> Instant {
        now + self.dispatch_timeout
    }

    /// How many replacement nodes to dispatch after `stragglers` were
    /// detected among `outstanding` tracked dispatches.
    ///
    /// Replacements are 1:1 but total dispatches never exceed the quorum's
    /// configured `n` — a task is never widened beyond its original quorum.
    pub fn replacements_needed(
        &self,
        config: &QuorumConfig,
        outstanding: usize,
        stragglers: usize,
    ) -> usize {
        let healthy = outstanding.saturating_sub(stragglers);
        stragglers.min((config.n as usize).saturating_sub(healthy))
    }
}

/// Tracks outstanding node dispatches for a single task and detects which
/// have become stragglers.
#[derive(Debug, Default)]
pub struct DispatchTracker {
    deadlines: HashMap<NodeId, Instant>,
}

impl DispatchTracker {
    /// Create an empty tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a dispatch to `node_id` that must complete by `deadline`.
    pub fn track(&mut self, node_id: NodeId, deadline: Instant) {
        self.deadlines.insert(node_id, deadline);
    }

    /// Record a completed (or cancelled) dispatch; returns `true` if the node
    /// was tracked.
    pub fn complete(&mut self, node_id: NodeId) -> bool {
        self.deadlines.remove(&node_id).is_some()
    }

    /// Number of dispatches still outstanding.
    pub fn outstanding(&self) -> usize {
        self.deadlines.len()
    }

    /// IDs of tracked nodes whose deadline has passed, ordered by node ID for
    /// deterministic handling.
    pub fn stragglers(&self, now: Instant) -> Vec<NodeId> {
        let mut late: Vec<NodeId> = self
            .deadlines
            .iter()
            .filter(|(_, deadline)| **deadline <= now)
            .map(|(node_id, _)| *node_id)
            .collect();
        late.sort_unstable();
        late
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;
    use tpt_mosaic_core::{QuorumConfig, TierLevel};

    fn node(b: u8) -> NodeId {
        NodeId::from_bytes([b; 16])
    }

    fn policy(timeout_ms: u64) -> StragglerPolicy {
        StragglerPolicy {
            dispatch_timeout: Duration::from_millis(timeout_ms),
        }
    }

    #[test]
    fn complete_removes_tracking() {
        let mut tracker = DispatchTracker::new();
        let now = Instant::now();
        tracker.track(node(1), now);
        tracker.track(node(2), now);
        assert_eq!(tracker.outstanding(), 2);
        assert!(tracker.complete(node(1)));
        assert!(!tracker.complete(node(1))); // already removed
        assert_eq!(tracker.outstanding(), 1);
    }

    #[test]
    fn detects_only_expired_dispatches() {
        let pol = policy(10);
        let mut tracker = DispatchTracker::new();
        let now = Instant::now();
        tracker.track(node(1), pol.deadline_from(now)); // expires quickly
        tracker.track(node(2), now + Duration::from_secs(60)); // still fresh

        sleep(Duration::from_millis(25));
        let late = tracker.stragglers(Instant::now());
        assert_eq!(late, vec![node(1)]);
    }

    #[test]
    fn stragglers_are_ordered_and_deterministic() {
        let mut tracker = DispatchTracker::new();
        let past = Instant::now() - Duration::from_secs(1);
        tracker.track(node(9), past);
        tracker.track(node(2), past);
        tracker.track(node(5), past);
        assert_eq!(
            tracker.stragglers(Instant::now()),
            vec![node(2), node(5), node(9)]
        );
    }

    #[test]
    fn replacements_are_capped_by_quorum_size() {
        let pol = StragglerPolicy::default();
        let config = QuorumConfig::new(3, 5, TierLevel::Standard);

        // 1 straggler among 4 outstanding, n = 5 → one replacement.
        assert_eq!(pol.replacements_needed(&config, 4, 1), 1);
        // 3 stragglers among 5 outstanding → 3 replacements, total stays ≤ 5.
        assert_eq!(pol.replacements_needed(&config, 5, 3), 3);
        // Widening is capped: 1 healthy + 4 stragglers, but only 4 free slots.
        assert_eq!(pol.replacements_needed(&config, 5, 4), 4);
    }

    #[test]
    fn fully_straggled_quorum_is_replaced_one_for_one() {
        let pol = StragglerPolicy::default();
        let solo = QuorumConfig::BEST_EFFORT_1_OF_1;
        // The lone node straggled: one replacement, total dispatches stay at n.
        assert_eq!(pol.replacements_needed(&solo, 1, 1), 1);
    }

    #[test]
    fn saturates_gracefully_on_degenerate_input() {
        let pol = StragglerPolicy::default();
        let config = QuorumConfig::MISSION_CRITICAL_7_OF_10;
        // More stragglers reported than outstanding must not panic.
        assert_eq!(pol.replacements_needed(&config, 2, 5), 5);
    }
}
