//! Library surface of the tpt-mosaic node daemon.
//!
//! The daemon binary is a thin wrapper around these modules; the library
//! surface exists so examples, black-box tooling, and embedders can assemble
//! and drive a [`daemon::NodeDaemon`] directly.

pub mod config;
pub mod control;
pub mod daemon;
pub mod id;

#[cfg(test)]
mod mesh_integration;
