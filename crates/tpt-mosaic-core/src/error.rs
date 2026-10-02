//! Workspace-wide error type.

use crate::{NodeId, TaskId};

/// All error conditions that can arise within the tpt-mosaic system.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum MosaicError {
    /// The required K-of-N hash agreement was not reached before the deadline.
    QuorumNotMet {
        /// The task that failed to reach quorum.
        task_id: TaskId,
        /// How many matching hashes were actually collected.
        got: u8,
        /// How many were required.
        required: u8,
    },
    /// A specific node could not be contacted or did not respond.
    NodeUnavailable(NodeId),
    /// A task exceeded its execution deadline.
    TaskTimeout(TaskId),
    /// A serialization or deserialization operation failed.
    SerializationError,
    /// Hardware-specific compilation of a workload failed.
    CompilationFailed,
    /// A task attempted to violate its sandbox capability grants.
    SandboxViolation,
    /// On-chain settlement transaction was rejected or timed out.
    SettlementFailed,
    /// A node submitted a hash that consistently diverges from the majority.
    ByzantineFault(NodeId),
    /// The provided `QuorumConfig` is structurally invalid (e.g., k > n).
    InvalidQuorumConfig,
    /// The selected node lacks the hardware capability required by the task.
    InsufficientCapability,
    /// Wire-format magic bytes or version field did not match expectations.
    WireProtocolMismatch,
}

impl core::fmt::Display for MosaicError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::QuorumNotMet {
                task_id: _,
                got,
                required,
            } => {
                write!(f, "quorum not met: got {got} of {required} required hashes")
            }
            Self::NodeUnavailable(id) => {
                write!(f, "node {:?} unavailable", id.as_bytes())
            }
            Self::TaskTimeout(id) => {
                write!(f, "task {:?} timed out", id.as_bytes())
            }
            Self::SerializationError => f.write_str("serialization error"),
            Self::CompilationFailed => f.write_str("hardware compilation failed"),
            Self::SandboxViolation => f.write_str("sandbox capability violation"),
            Self::SettlementFailed => f.write_str("on-chain settlement failed"),
            Self::ByzantineFault(id) => {
                write!(f, "byzantine fault detected from node {:?}", id.as_bytes())
            }
            Self::InvalidQuorumConfig => f.write_str("invalid quorum configuration"),
            Self::InsufficientCapability => f.write_str("node lacks required capability"),
            Self::WireProtocolMismatch => f.write_str("wire protocol magic/version mismatch"),
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for MosaicError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NodeId, TaskId};

    #[test]
    fn display_quorum_not_met() {
        let e = MosaicError::QuorumNotMet {
            task_id: TaskId::NIL,
            got: 2,
            required: 3,
        };
        let s = e.to_string();
        assert!(s.contains("2 of 3"), "got: {s}");
    }

    #[test]
    fn display_node_unavailable() {
        let e = MosaicError::NodeUnavailable(NodeId::NIL);
        assert!(e.to_string().contains("unavailable"));
    }
}
