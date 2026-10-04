# tpt-mosaic-core

> Shared foundation types, traits, error enums, and constants for the [tpt-mosaic](../../README.md) workspace.

[![CI](https://github.com/tpt-solutions/tpt-mosaic/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/tpt-solutions/tpt-mosaic/actions/workflows/ci.yml)

*By TPT Solutions · Licensed under [MIT](../../LICENSE-MIT) OR [Apache-2.0](../../LICENSE-APACHE)*

---

## Overview

`tpt-mosaic-core` is the bottom of the dependency graph: every other crate in
the workspace depends on it and it depends on nothing but `bitflags`. It holds
the vocabulary that the rest of the fabric speaks — node and task identities,
hardware descriptions, quorum configurations, the shared error taxonomy, the
extension traits a node or a backend implements, and the wire-format constants.

It is `no_std`-compatible by default (`no_std` + `alloc` where needed), so it
can be linked into microkernel builds, RTOS targets, and embedded edge devices.

## Features

| Feature | Default | Effect |
|---|---|---|
| `std` | off | Implements `std::error::Error` / `core::fmt::Display` conventions for `MosaicError` |

Without `std`, the crate is `#![no_std]`.

## Installation

```toml
[dependencies]
tpt-mosaic-core = { path = "crates/tpt-mosaic-core" }
```

Publishing to crates.io is currently blocked on the upstream `tpt-*` crates;
see the root [CHANGELOG](../../CHANGELOG.md).

## What it provides

### Identities — `ids`

`NodeId` and `TaskId` are opaque `#[repr(transparent)]` 16-byte newtypes with
explicit-encoding methods, so an identity is never accidentally constructed
from a different 16-byte value.

```rust
use tpt_mosaic_core::NodeId;

let id = NodeId::from_bytes([7u8; 16]);
assert_eq!(id.as_bytes(), &[7u8; 16]);
```

### Hardware description — `hardware`

`HardwareProfile` is what a node advertises in every heartbeat: node kind
(`EdgeTile` / `AnchorBallast`), `GpuVendor`, `CpuArch`, `npu_present`,
`memory_mb`, `battery_level`, and `ThermalState`. `CapabilityFlags` is a
`bitflags` set (`CUDA`, `METAL`, `VULKAN`, `NPU`, `DSP`, `FPGA`, `CPU_VECTOR`).

Availability is derived, not stored:

```rust
use tpt_mosaic_core::{GpuVendor, HardwareProfile, NodeKind, ThermalState, CpuArch};

let profile = HardwareProfile {
    kind: NodeKind::AnchorBallast,
    gpu_vendor: GpuVendor::Nvidia,
    npu_present: true,
    cpu_arch: CpuArch::X86_64,
    memory_mb: 65_536,
    battery_level: 255,          // 255 = mains-powered / no battery
    thermal_state: ThermalState::Nominal,
};
assert!(profile.is_available());

let hot = HardwareProfile { thermal_state: ThermalState::Critical, ..profile };
assert!(!hot.is_available());
```

A node is unavailable when it is thermally critical or below 10% battery
(unless battery-powered reporting is disabled with the `255` sentinel).

### Quorum configuration — `quorum`

`QuorumConfig` is K-of-N plus a `TierLevel`, with named presets:

| Constant | Config | Use case |
|---|---|---|
| `BEST_EFFORT_1_OF_1` | 1-of-1 | Casual inference, summarization |
| `BEST_EFFORT_2_OF_3` | 2-of-3 | Cheap redundancy |
| `STANDARD_3_OF_5` | 3-of-5 | Financial analysis, medical screening |
| `MISSION_CRITICAL_7_OF_10` | 7-of-10 | Surgical robotics, high-value contracts |

```rust
use tpt_mosaic_core::{QuorumConfig, TierLevel};

assert!(QuorumConfig::STANDARD_3_OF_5.is_valid());
assert!(QuorumConfig::new(0, 5, TierLevel::Standard).is_valid() == false);
```

### Errors — `error`

`MosaicError` is `#[non_exhaustive]` so new variants can be added without a
breaking change. It covers quorum failures, wire-protocol mismatches,
serialization problems, hardware unavailability, timeout, Byzantine faults,
sandbox violations, and configuration errors.

### Traits — `traits`

| Trait | Implement it when |
|---|---|
| `NodeCapability` | Describing what a node can do (hardware profile + flags + thermal state) |
| `TaskExecutor` | Executing a dispatched shard and returning raw output bytes |
| `QuorumParticipant` | Submitting / validating result hashes for a quorum round |

### Wire constants

`WIRE_MAGIC` (ASCII `MOSA`) and `WIRE_VERSION` (currently `2`) define the
frame header shared by every tpt-mosaic transport. `tpt-mosaic-proto` owns the
encoding; this crate owns the constants so that non-Rust implementations can
interop without depending on the codec.

### `prelude`

```rust
use tpt_mosaic_core::prelude::*;

let id = NodeId::from_bytes([0; 16]);
let cfg = QuorumConfig::STANDARD_3_OF_5;
```

## Feature graph

```
tpt-mosaic-core (std off)   no_std, bitflags only
        ▲
        ├── tpt-mosaic-proto      (+ std)
        ├── tpt-mosaic-verify     (+ std, blake3, sha2)
        └── every other crate     (+ std)
```

## Testing

```bash
cargo test -p tpt-mosaic-core
cargo test -p tpt-mosaic-core --features std
cargo hack check -p tpt-mosaic-core --feature-powerset   # no_std matrix
```

## Links

- Workspace overview: [root README](../../README.md)
- Release notes: [CHANGELOG.md](CHANGELOG.md)
- Next crate: [`tpt-mosaic-proto`](../tpt-mosaic-proto/README.md)
- API docs: <https://docs.rs/tpt-mosaic-core>

## License

Licensed under either of Apache License, Version 2.0 or the MIT license at your
option. Copyright © 2026 TPT Solutions.