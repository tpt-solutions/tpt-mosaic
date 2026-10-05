//! Shared foundation types, traits, error enums, and constants for the tpt-mosaic workspace.
//!
//! All types are `no_std`-compatible by default. Enable the `std` feature for
//! `std::error::Error` implementations on [`MosaicError`].

#![cfg_attr(not(feature = "std"), no_std)]
#![deny(missing_docs)]

// ── Re-exports ────────────────────────────────────────────────────────────────

pub use error::MosaicError;
pub use hardware::{CapabilityFlags, CpuArch, GpuVendor, HardwareProfile, NodeKind, ThermalState};
pub use ids::{NodeId, TaskId};
pub use quorum::{QuorumConfig, TierLevel};
pub use traits::{NodeCapability, QuorumParticipant, TaskExecutor};

/// Convenience glob import for the most common tpt-mosaic-core types.
pub mod prelude {
    pub use crate::{
        CapabilityFlags, CpuArch, GpuVendor, HardwareProfile, MosaicError, NodeCapability, NodeId,
        NodeKind, QuorumConfig, QuorumParticipant, TaskExecutor, TaskId, ThermalState, TierLevel,
        WIRE_MAGIC, WIRE_VERSION,
    };
}

mod error;
mod hardware;
mod ids;
mod quorum;
mod traits;

// ── Wire-format constants ─────────────────────────────────────────────────────

/// Wire protocol version. Increment on any breaking schema change.
///
/// v3: `HeartbeatBeacon`, `TaskAssignment`, and `ResultHash` carry Ed25519
/// authentication fields (`pubkey` + `signature`; the beacon also carries a
/// replay `nonce`). v2 had no authentication fields.
/// v2: `HeartbeatBeacon` carries the sender's mesh address and
/// `TaskAssignment` carries the coordinator's mesh address (family-tagged).
/// v1 had no address fields.
pub const WIRE_VERSION: u16 = 3;

/// Magic bytes at the start of every tpt-mosaic frame: ASCII `"MOSA"`.
pub const WIRE_MAGIC: u32 = 0x4D4F_5341;
