//! Reputation scoring: tracks node reliability over time.
//!
//! Successful quorum contributions nudge a node's score up; timeouts and
//! diverging (Byzantine) results pull it down. Nodes whose score falls below
//! [`SLASH_THRESHOLD`] should be excluded from quorums and slashed.

use std::collections::HashMap;

use tpt_mosaic_core::NodeId;
use tpt_mosaic_quorum::QuorumResult;

/// Starting score for nodes with no history yet.
pub const DEFAULT_SCORE: f32 = 0.5;
/// Score added per successful quorum contribution.
pub const SUCCESS_DELTA: f32 = 0.01;
/// Score subtracted per failed, timed-out, or divergent contribution.
pub const FAILURE_DELTA: f32 = 0.05;
/// Below this score a node should be slashed and excluded from quorums.
pub const SLASH_THRESHOLD: f32 = 0.2;

/// In-memory reputation store.
///
/// Scores are clamped to `[0.0, 1.0]`. Swap for a persistent backing store
/// before production use.
#[derive(Debug, Clone)]
pub struct ReputationStore {
    scores: HashMap<NodeId, f32>,
}

impl Default for ReputationStore {
    fn default() -> Self {
        Self::new()
    }
}

impl ReputationStore {
    /// Create an empty store.
    pub fn new() -> Self {
        Self {
            scores: HashMap::new(),
        }
    }

    /// Record a successful quorum contribution; returns the updated score.
    pub fn record_success(&mut self, node_id: NodeId) -> f32 {
        self.bump(node_id, SUCCESS_DELTA)
    }

    /// Record a failed contribution (timeout, divergence, sandbox violation);
    /// returns the updated score.
    pub fn record_failure(&mut self, node_id: NodeId) -> f32 {
        self.bump(node_id, -FAILURE_DELTA)
    }

    /// Record a contribution based on its [`QuorumResult`]; returns the
    /// updated score.
    pub fn record_outcome(&mut self, node_id: NodeId, outcome: &QuorumResult) -> f32 {
        match outcome {
            QuorumResult::Met { .. } => self.record_success(node_id),
            QuorumResult::Timeout | QuorumResult::Diverged => self.record_failure(node_id),
        }
    }

    /// Current reputation of `node_id` ([`DEFAULT_SCORE`] if never seen).
    pub fn score(&self, node_id: NodeId) -> f32 {
        self.scores.get(&node_id).copied().unwrap_or(DEFAULT_SCORE)
    }

    /// Returns `true` if the node's reputation has fallen below
    /// [`SLASH_THRESHOLD`].
    pub fn should_slash(&self, node_id: NodeId) -> bool {
        self.score(node_id) < SLASH_THRESHOLD
    }

    /// Number of tracked nodes.
    pub fn len(&self) -> usize {
        self.scores.len()
    }

    /// Returns `true` if no nodes are tracked.
    pub fn is_empty(&self) -> bool {
        self.scores.is_empty()
    }

    fn bump(&mut self, node_id: NodeId, delta: f32) -> f32 {
        let next = (self.score(node_id) + delta).clamp(0.0, 1.0);
        self.scores.insert(node_id, next);
        next
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tpt_mosaic_quorum::QuorumResult;

    fn node(b: u8) -> NodeId {
        NodeId::from_bytes([b; 16])
    }

    fn close(a: f32, b: f32) -> bool {
        (a - b).abs() < 1e-6
    }

    #[test]
    fn unseen_nodes_use_default_score() {
        let store = ReputationStore::new();
        assert!(close(store.score(node(1)), DEFAULT_SCORE));
        assert!(!store.should_slash(node(1)));
    }

    #[test]
    fn success_and_failure_move_score() {
        let mut store = ReputationStore::new();
        let up = store.record_success(node(1));
        assert!(close(up, DEFAULT_SCORE + SUCCESS_DELTA));
        let down = store.record_failure(node(1));
        assert!(close(down, DEFAULT_SCORE + SUCCESS_DELTA - FAILURE_DELTA));
    }

    #[test]
    fn score_clamped_to_unit_interval() {
        let mut store = ReputationStore::new();
        for _ in 0..100 {
            store.record_success(node(1));
        }
        assert!(close(store.score(node(1)), 1.0));
        for _ in 0..200 {
            store.record_failure(node(2));
        }
        assert!(close(store.score(node(2)), 0.0));
        assert!(store.should_slash(node(2)));
    }

    #[test]
    fn record_outcome_maps_quorum_results() {
        let mut store = ReputationStore::new();
        let met = QuorumResult::Met {
            agreed_hash: [0u8; 32],
            confirmations: 3,
        };
        let after_met = store.record_outcome(node(1), &met);
        assert!(close(after_met, DEFAULT_SCORE + SUCCESS_DELTA));

        let after_timeout = store.record_outcome(node(1), &QuorumResult::Timeout);
        assert!(close(
            after_timeout,
            DEFAULT_SCORE + SUCCESS_DELTA - FAILURE_DELTA
        ));

        let after_diverged = store.record_outcome(node(1), &QuorumResult::Diverged);
        assert!(close(
            after_diverged,
            DEFAULT_SCORE + SUCCESS_DELTA - 2.0 * FAILURE_DELTA
        ));
    }
}
