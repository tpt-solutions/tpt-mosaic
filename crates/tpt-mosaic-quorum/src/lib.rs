//! K-of-N consensus validation, hash collection, early termination, and Byzantine fault detection.

#![deny(missing_docs)]

use std::collections::HashMap;
use tpt_mosaic_core::{MosaicError, NodeId, QuorumConfig, TaskId};
use tpt_mosaic_proto::{CancellationReason, CancellationSignal};

/// Outcome of a quorum evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuorumResult {
    /// K matching hashes received — quorum is satisfied.
    Met {
        /// The agreed-upon hash value.
        agreed_hash: [u8; 32],
        /// Number of nodes that contributed the winning hash.
        confirmations: u8,
    },
    /// Deadline expired without meeting the threshold.
    Timeout,
    /// Sufficient hashes received but none agreed — Byzantine condition.
    Diverged,
}

/// Current state of a quorum round.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuorumState {
    /// Waiting for nodes to be dispatched.
    Pending,
    /// Collecting hashes from dispatched nodes.
    Collecting,
    /// Terminal: quorum reached, timed out, or diverged.
    Finished(QuorumResult),
}

/// Collects and evaluates result hashes from quorum participants.
#[derive(Debug)]
pub struct HashCollector {
    task_id: TaskId,
    config: QuorumConfig,
    /// Map from hash value to the set of nodes that submitted it.
    votes: HashMap<[u8; 32], Vec<NodeId>>,
    /// Nodes flagged for consistently diverging hashes.
    suspected_byzantine: Vec<NodeId>,
    state: QuorumState,
}

impl HashCollector {
    /// Create a new collector for `task_id` governed by `config`.
    pub fn new(task_id: TaskId, config: QuorumConfig) -> Result<Self, MosaicError> {
        if !config.is_valid() {
            return Err(MosaicError::InvalidQuorumConfig);
        }
        Ok(Self {
            task_id,
            config,
            votes: HashMap::new(),
            suspected_byzantine: Vec::new(),
            state: QuorumState::Pending,
        })
    }

    /// Record a hash submission from `node_id`. Returns the updated [`QuorumState`].
    pub fn submit(&mut self, node_id: NodeId, hash: [u8; 32]) -> &QuorumState {
        if matches!(self.state, QuorumState::Finished(_)) {
            return &self.state;
        }
        self.state = QuorumState::Collecting;
        self.votes.entry(hash).or_default().push(node_id);
        self.evaluate();
        &self.state
    }

    /// Mark the round as timed out (no more submissions will be accepted).
    pub fn timeout(&mut self) {
        if !matches!(self.state, QuorumState::Finished(_)) {
            self.state = QuorumState::Finished(QuorumResult::Timeout);
        }
    }

    /// Task this collector is aggregating hashes for.
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    /// Current state of this collector.
    pub fn state(&self) -> &QuorumState {
        &self.state
    }

    /// Nodes flagged as potentially Byzantine (consistently diverging hashes).
    pub fn suspected_byzantine(&self) -> &[NodeId] {
        &self.suspected_byzantine
    }

    /// Cancellation signal to broadcast for this round, if one is warranted.
    ///
    /// `QuorumMet` once the threshold is reached (early termination, spec
    /// §3.3), `Timeout` after [`HashCollector::timeout`]. A diverged round
    /// produces no signal — every node has already voted, so there is nothing
    /// left to cancel.
    pub fn cancellation_signal(&self) -> Option<CancellationSignal> {
        match &self.state {
            QuorumState::Finished(QuorumResult::Met { .. }) => Some(CancellationSignal {
                task_id: self.task_id,
                reason: CancellationReason::QuorumMet,
            }),
            QuorumState::Finished(QuorumResult::Timeout) => Some(CancellationSignal {
                task_id: self.task_id,
                reason: CancellationReason::Timeout,
            }),
            _ => None,
        }
    }

    fn evaluate(&mut self) {
        let k = self.config.k as usize;

        // Check if any hash has reached the K threshold.
        for (hash, voters) in &self.votes {
            if voters.len() >= k {
                self.state = QuorumState::Finished(QuorumResult::Met {
                    agreed_hash: *hash,
                    confirmations: voters.len() as u8,
                });
                return;
            }
        }

        // Check if it is now impossible to reach quorum (all votes cast but no winner).
        let total_votes: usize = self.votes.values().map(|v| v.len()).sum();
        if total_votes >= self.config.n as usize {
            // Flag nodes whose hash is in the minority as suspected Byzantine.
            if let Some((winning_hash, _)) = self.votes.iter().max_by_key(|(_, v)| v.len()) {
                for (hash, voters) in &self.votes {
                    if hash != winning_hash {
                        self.suspected_byzantine.extend_from_slice(voters);
                    }
                }
            }
            self.state = QuorumState::Finished(QuorumResult::Diverged);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use tpt_mosaic_core::{QuorumConfig, TaskId, TierLevel};
    use tpt_mosaic_proto::{CancellationReason, CancellationSignal};

    fn make_collector() -> HashCollector {
        HashCollector::new(TaskId::NIL, QuorumConfig::STANDARD_3_OF_5).unwrap()
    }

    fn node(b: u8) -> NodeId {
        NodeId::from_bytes([b; 16])
    }

    #[test]
    fn quorum_met_at_k() {
        let mut c = make_collector();
        let hash = [1u8; 32];
        c.submit(node(1), hash);
        c.submit(node(2), hash);
        let state = c.submit(node(3), hash);
        assert!(matches!(
            state,
            QuorumState::Finished(QuorumResult::Met {
                confirmations: 3,
                ..
            })
        ));
    }

    #[test]
    fn quorum_diverged() {
        let mut c = make_collector();
        for i in 0..5u8 {
            c.submit(node(i), [i; 32]); // every node sends a unique hash
        }
        assert!(matches!(
            c.state(),
            QuorumState::Finished(QuorumResult::Diverged)
        ));
    }

    #[test]
    fn timeout_marks_finished() {
        let mut c = make_collector();
        c.submit(node(1), [1u8; 32]);
        c.timeout();
        assert!(matches!(
            c.state(),
            QuorumState::Finished(QuorumResult::Timeout)
        ));
    }

    #[test]
    fn invalid_config_rejected() {
        let result = HashCollector::new(TaskId::NIL, QuorumConfig::new(6, 5, TierLevel::Standard));
        assert!(result.is_err());
    }

    #[test]
    fn cancellation_signal_follows_round_outcome() {
        // Collecting: nothing to cancel yet.
        let mut c = make_collector();
        assert!(c.cancellation_signal().is_none());
        c.submit(node(1), [1u8; 32]);
        assert!(c.cancellation_signal().is_none());

        // Quorum met: early-termination broadcast.
        c.submit(node(2), [1u8; 32]);
        c.submit(node(3), [1u8; 32]);
        assert_eq!(
            c.cancellation_signal(),
            Some(CancellationSignal {
                task_id: TaskId::NIL,
                reason: CancellationReason::QuorumMet,
            })
        );

        // Timeout with quorum unmet: timeout broadcast.
        let mut c = make_collector();
        c.submit(node(1), [1u8; 32]);
        c.timeout();
        assert_eq!(
            c.cancellation_signal(),
            Some(CancellationSignal {
                task_id: TaskId::NIL,
                reason: CancellationReason::Timeout,
            })
        );

        // Diverged: all nodes already voted, no signal.
        let mut c = make_collector();
        for i in 0..5u8 {
            c.submit(node(i), [i; 32]);
        }
        assert!(c.cancellation_signal().is_none());
    }

    proptest! {
        /// For arbitrary vote patterns: a `Met` result always carries at least
        /// `k` confirmations, and a finished round is immutable under further
        /// submissions.
        #[test]
        fn met_implies_k_and_finished_is_immutable(
            votes in prop::collection::vec((0u8..16, any::<u8>()), 0..16),
            k in 1u8..4,
            extra in 0u8..6,
        ) {
            let config = QuorumConfig::new(k, k.saturating_add(extra), TierLevel::BestEffort);
            let mut c = HashCollector::new(TaskId::NIL, config).unwrap();
            for (node_id, hash) in &votes {
                let state = c.submit(node(*node_id), [*hash; 32]);
                if let QuorumState::Finished(QuorumResult::Met { confirmations, .. }) = state {
                    prop_assert!(*confirmations >= k);
                }
            }
            let snapshot = c.state().clone();
            if matches!(snapshot, QuorumState::Finished(_)) {
                // Further submissions never change a finished round.
                c.submit(node(255), [255u8; 32]);
                prop_assert_eq!(&snapshot, c.state());
            }
        }
    }
}
