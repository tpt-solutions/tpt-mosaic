//! Core traits implemented by nodes, executors, and quorum participants.

use crate::{CapabilityFlags, HardwareProfile, MosaicError, NodeId, TaskId};

/// Describes a node's identity and hardware capabilities.
pub trait NodeCapability {
    /// Returns this node's unique identifier.
    fn node_id(&self) -> NodeId;

    /// Returns the node's hardware description as advertised in heartbeat beacons.
    fn hardware_profile(&self) -> &HardwareProfile;

    /// Returns the compute capability flags for this node.
    fn capability_flags(&self) -> CapabilityFlags;

    /// Returns `true` if this node is a datacenter anchor; `false` for an edge tile.
    fn is_anchor(&self) -> bool {
        matches!(self.hardware_profile().kind, crate::NodeKind::AnchorBallast)
    }
}

/// Executes a raw compute payload and returns the output bytes.
///
/// Implementations live in `tpt-mosaic-sandbox` (via `tpt-archon`) and
/// `tpt-mosaic-compiler` (dispatch layer).
pub trait TaskExecutor {
    /// Execution-specific error type.
    type Error;

    /// Execute `payload` for `task_id` and return the raw output bytes.
    fn execute<'a>(
        &self,
        task_id: TaskId,
        payload: &[u8],
        output: &'a mut [u8],
    ) -> Result<&'a [u8], Self::Error>;
}

/// A node's interface for participating in quorum consensus.
pub trait QuorumParticipant {
    /// Submit a cryptographic hash of the execution output for `task_id`.
    fn submit_hash(&self, task_id: TaskId, hash: &[u8; 32]) -> Result<(), MosaicError>;

    /// Receive and act on a quorum cancellation signal (quorum already met).
    fn receive_cancellation(&self, task_id: TaskId);
}
