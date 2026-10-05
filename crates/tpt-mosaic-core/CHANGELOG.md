# Changelog — tpt-mosaic-core

All notable changes to `tpt-mosaic-core` are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions
follow [Semantic Versioning](https://semver.org/). Workspace-wide notes live
in the root [CHANGELOG.md](../../CHANGELOG.md).

## [Unreleased]

### Changed

- **Breaking:** `WIRE_VERSION` bumped to 3 - `HeartbeatBeacon`,
  `TaskAssignment`, and `ResultHash` carry Ed25519 authentication fields
  (`pubkey` + `signature`; the beacon also carries a replay `nonce`). v2
  peers cannot interoperate with v3.

### Planned

- Additional `CapabilityFlags` bits as new accelerator classes appear
  (upstream TPTIR device types).
- Optional `Hash`-backed identity helpers for content-addressed task ids.

## [0.1.0] — 2026-01-15

### Added

- Opaque `NodeId` / `TaskId` 16-byte identities with explicit-encoding
  conversions.
- `HardwareProfile` (node kind, GPU vendor, CPU architecture, NPU presence,
  memory, battery, thermal state) with derived `is_available()`.
- `CapabilityFlags` bitflags: `CUDA`, `METAL`, `VULKAN`, `NPU`, `DSP`,
  `FPGA`, `CPU_VECTOR`.
- `QuorumConfig` (K-of-N + `TierLevel`) with the `BEST_EFFORT_1_OF_1`,
  `BEST_EFFORT_2_OF_3`, `STANDARD_3_OF_5` and `MISSION_CRITICAL_7_OF_10`
  presets and `is_valid()`.
- `TierLevel`: `BestEffort`, `Standard`, `MissionCritical`.
- `#[non_exhaustive]` `MosaicError` taxonomy shared by every crate.
- Extension traits `NodeCapability`, `TaskExecutor`, `QuorumParticipant`.
- Wire constants `WIRE_MAGIC` (`"MOSA"`) and `WIRE_VERSION` (2).
- `prelude` module glob-importing the common types.
- `#![deny(missing_docs)]` and full `no_std` compatibility.

### Notes

- `WIRE_VERSION` is 2: v2 added mesh-address fields to `HeartbeatBeacon` and
  `TaskAssignment`. v1 had no address fields.