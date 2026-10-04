//! Reputation scoring: tracks node reliability over time.
//!
//! Successful quorum contributions nudge a node's score up; timeouts and
//! diverging (Byzantine) results pull it down. Nodes whose score falls below
//! [`SLASH_THRESHOLD`] should be excluded from quorums and slashed.

use std::collections::HashMap;

use tpt_mosaic_core::{MosaicError, NodeId};
use tpt_mosaic_quorum::QuorumResult;

use crate::chains::write_atomically;
use crate::lock_ignoring_poison;

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
/// Scores are clamped to `[0.0, 1.0]`. The internal map is behind a
/// poison-tolerant mutex, so `score` can be read through a shared reference
/// while another thread records outcomes.
#[derive(Debug)]
pub struct ReputationStore {
    scores: std::sync::Mutex<HashMap<NodeId, f32>>,
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
            scores: std::sync::Mutex::new(HashMap::new()),
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
        lock_ignoring_poison(&self.scores)
            .get(&node_id)
            .copied()
            .unwrap_or(DEFAULT_SCORE)
    }

    /// Returns `true` if the node's reputation has fallen below
    /// [`SLASH_THRESHOLD`].
    pub fn should_slash(&self, node_id: NodeId) -> bool {
        self.score(node_id) < SLASH_THRESHOLD
    }

    /// Number of tracked nodes.
    pub fn len(&self) -> usize {
        lock_ignoring_poison(&self.scores).len()
    }

    /// Returns `true` if no nodes are tracked.
    pub fn is_empty(&self) -> bool {
        lock_ignoring_poison(&self.scores).is_empty()
    }

    /// Serialize every score: `u32` LE count, then per entry 16 id bytes +
    /// `f32` LE bit pattern.
    pub fn to_bytes(&self) -> Vec<u8> {
        let scores = lock_ignoring_poison(&self.scores);
        let mut out = Vec::with_capacity(4 + scores.len() * 20);
        out.extend_from_slice(&(scores.len() as u32).to_le_bytes());
        for (id, score) in scores.iter() {
            out.extend_from_slice(id.as_bytes());
            out.extend_from_slice(&score.to_le_bytes());
        }
        out
    }

    /// Restore a store written by [`ReputationStore::to_bytes`].
    ///
    /// Scores are clamped back into `[0.0, 1.0]` on load, so a corrupt file
    /// cannot inject out-of-range reputations.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, MosaicError> {
        if bytes.len() < 4 {
            return Err(MosaicError::SerializationError);
        }
        let count = u32::from_le_bytes(bytes[0..4].try_into().expect("4 bytes")) as usize;
        if bytes.len() != 4 + count * 20 {
            return Err(MosaicError::SerializationError);
        }
        let mut scores = HashMap::with_capacity(count);
        for entry in bytes[4..].chunks_exact(20) {
            let id = NodeId::from_bytes(entry[0..16].try_into().expect("16 bytes"));
            let score = f32::from_le_bytes(entry[16..20].try_into().expect("4 bytes"));
            scores.insert(id, score.clamp(0.0, 1.0));
        }
        Ok(Self {
            scores: std::sync::Mutex::new(scores),
        })
    }

    /// Persist atomically (temp file + rename) to `path`.
    pub fn save(&self, path: &std::path::Path) -> std::io::Result<()> {
        write_atomically(path, &self.to_bytes())
    }

    /// Load a store previously written by [`ReputationStore::save`]. A
    /// missing file yields an empty store; a corrupt one is an error.
    pub fn load(path: &std::path::Path) -> std::io::Result<Self> {
        match std::fs::read(path) {
            Ok(bytes) => Self::from_bytes(&bytes)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::new()),
            Err(e) => Err(e),
        }
    }

    fn bump(&mut self, node_id: NodeId, delta: f32) -> f32 {
        let next = (self.score(node_id) + delta).clamp(0.0, 1.0);
        lock_ignoring_poison(&self.scores).insert(node_id, next);
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
