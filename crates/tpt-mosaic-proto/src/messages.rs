//! Hand-written message types. Replace with flatc-generated code once build.rs is wired.
//!
//! All payloads are little-endian; wire version history is documented on
//! `tpt_mosaic_core::WIRE_VERSION`.

use core::net::SocketAddr;

use tpt_mosaic_core::{CapabilityFlags, HardwareProfile, NodeId, QuorumConfig, TaskId};

/// Sent by the scheduler to dispatch a task to a selected node.
#[derive(Debug, Clone, PartialEq)]
pub struct TaskAssignment {
    /// Identifier of the task being assigned.
    pub task_id: TaskId,
    /// Quorum configuration this execution participates in.
    pub quorum_config: QuorumConfig,
    /// Raw payload shard (model weights or inference input).
    pub payload: Bytes,
    /// Unix timestamp (milliseconds) after which this assignment expires.
    pub deadline_ms: u64,
    /// Mesh address the worker must send its [`ResultHash`] to.
    pub coordinator: SocketAddr,
}

/// Submitted by a node after completing execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResultHash {
    /// Task this hash belongs to.
    pub task_id: TaskId,
    /// Node that produced this hash.
    pub node_id: NodeId,
    /// BLAKE3 or SHA-256 digest of the raw output bytes.
    pub hash: [u8; 32],
    /// Unix timestamp (milliseconds) of result production.
    pub produced_at_ms: u64,
}

/// Periodic liveness and capability advertisement broadcast by every node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeartbeatBeacon {
    /// Advertising node.
    pub node_id: NodeId,
    /// Current hardware profile (thermal, battery, memory).
    pub hardware: HardwareProfile,
    /// Compute capability flags.
    pub capabilities: CapabilityFlags,
    /// Unix timestamp (milliseconds) of this beacon.
    pub timestamp_ms: u64,
    /// The advertiser's mesh listen address, when it accepts mesh
    /// connections. Peers learn where to send assignments from this.
    pub addr: Option<SocketAddr>,
}

/// Broadcast by the quorum coordinator once K matching hashes are received.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CancellationSignal {
    /// Task whose quorum has been satisfied.
    pub task_id: TaskId,
    /// Reason code for the cancellation.
    pub reason: CancellationReason,
}

/// Why a task was cancelled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CancellationReason {
    /// Quorum threshold was met — remaining nodes should stop.
    QuorumMet = 0,
    /// Task was aborted by the submitter.
    ClientAbort = 1,
    /// Global deadline exceeded before quorum was reached.
    Timeout = 2,
}

/// Sent to DHT nodes to discover peers matching a capability filter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DhtQuery {
    /// Lookup key (typically derived from geographic region or capability hash).
    pub key: [u8; 32],
    /// Maximum number of peer results to return.
    pub limit: u16,
    /// Only return peers that satisfy these capability flags.
    pub capability_filter: CapabilityFlags,
}

/// One peer's advertisement inside a [`PeerGossip`] payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeerAdvert {
    /// The advertised peer's identity.
    pub node_id: NodeId,
    /// The peer's mesh listen address, when it accepts mesh connections.
    pub addr: Option<SocketAddr>,
    /// Last-known hardware profile.
    pub hardware: HardwareProfile,
    /// Compute capability flags.
    pub capabilities: CapabilityFlags,
}

/// A snapshot of a node's peer table, exchanged as the reply to a
/// [`HeartbeatBeacon`] so a single static seed propagates the whole view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerGossip {
    /// Advertised peers (bounded by `codec::MAX_GOSSIP_PEERS`).
    pub peers: alloc::vec::Vec<PeerAdvert>,
}

/// Payload bytes. `alloc::vec::Vec<u8>` in both build modes; the crate is
/// `no_std` + `alloc` when the `std` feature is off.
pub type Bytes = alloc::vec::Vec<u8>;
