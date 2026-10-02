//! Wire protocol and serialization for tpt-mosaic inter-node communication.
//!
//! # Schemas
//! FlatBuffers `.fbs` schemas will live in `schemas/` with a `build.rs`
//! generating Rust bindings into `$OUT_DIR`. Until `flatc` code-gen is wired
//! up, this crate exposes hand-written message types and a hand-rolled frame
//! codec in [`codec`].
//!
//! # Frame format
//! Every frame on the wire is: 4-byte magic `"MOSA"`
//! ([`tpt_mosaic_core::WIRE_MAGIC`] as big-endian bytes), `u16` LE wire
//! version, `u8` message tag ([`codec::MessageTag`]), `u32` LE payload
//! length, then the payload. All integers inside payloads are little-endian.
//!
//! # Messages
//! - [`TaskAssignment`] — dispatched from scheduler to a selected node
//! - [`ResultHash`] — submitted by a node after execution
//! - [`HeartbeatBeacon`] — periodic liveness and capability advertisement
//! - [`CancellationSignal`] — broadcast by coordinator once quorum is met
//! - [`DhtQuery`] — peer lookup queries to the distributed hash table

#![cfg_attr(not(feature = "std"), no_std)]
#![deny(missing_docs)]

extern crate alloc;

pub use codec::{decode, encode, WireMessage};
#[cfg(feature = "std")]
pub use codec::{read_frame, write_frame};
pub use messages::{
    CancellationReason, CancellationSignal, DhtQuery, HeartbeatBeacon, PeerAdvert, PeerGossip,
    ResultHash, TaskAssignment,
};

pub mod codec;
mod messages;
