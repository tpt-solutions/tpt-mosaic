# tpt-mosaic-sandbox

> `tpt-archon` microkernel interface, capability grants, and RTOS preemption hooks for [tpt-mosaic](../../README.md).

[![CI](https://github.com/tpt-solutions/tpt-mosaic/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/tpt-solutions/tpt-mosaic/actions/workflows/ci.yml)

*By TPT Solutions · Licensed under [MIT](../../LICENSE-MIT) OR [Apache-2.0](../../LICENSE-APACHE)*

---

## Overview

Dispatching a stranger's model shard onto a phone in a parked car is only safe
if the device can actually say no. This crate is the boundary: it converts a
policy decision into a `CapabilityGrant` (memory ceiling, CPU/GPU time budget,
network permission), enforces thermal limits, and — with the `archon` feature —
executes the workload inside a `tpt-archon` capability-confined memory slice
where every page access is capability-checked by the kernel.

## Features

| Feature | Default | Effect |
|---|---|---|
| `archon` | off | Real capability-confined execution through `tpt-archon-kernel` / `-bridge` / `-core` |

Without `archon`, `execute` is a pass-through stub with the real signature, so
the workspace builds and tests offline.

## Installation

```toml
[dependencies]
tpt-mosaic-sandbox = { path = "crates/tpt-mosaic-sandbox", features = ["archon"] }
```

## Capability model

`CapabilityGrant` bounds a single execution:

| Field | Meaning |
|---|---|
| `task_id` | Task the grant belongs to |
| `max_memory_bytes` | Hard allocation ceiling; becomes the archon page-pool size |
| `max_cpu_ms` | CPU budget before preemption |
| `max_gpu_ms` | GPU budget; `0` means no GPU access at all |
| `network_access` | Must be `false` for untrusted workloads |

`CapabilityGrant::new(task_id, max_memory_bytes, max_cpu_ms)` starts with
`max_gpu_ms = 0` and `network_access = false` — deny by default.

## Thermal policy

`ThermalPolicy` converts temperature into action:

| Field | Meaning |
|---|---|
| `pause_threshold_celsius` | Above this, new dispatches pause |
| `kill_threshold_celsius` | Above this, running work is preempted |

This is the host's right to kill any task instantly, and the reason an edge tile
under a burning sun drops out of a quorum instead of throttling to a
straggler.

## Usage

```rust
use tpt_mosaic_core::TaskId;
use tpt_mosaic_sandbox::{CapabilityGrant, Sandbox, ThermalPolicy};

let sandbox = Sandbox::new(ThermalPolicy {
    pause_threshold_celsius: 80,
    kill_threshold_celsius: 90,
});

let grant = CapabilityGrant::new(TaskId::from_bytes([5u8; 16]), 64 * 1024 * 1024, 500);
assert_eq!(grant.max_gpu_ms, 0);          // no GPU access by default
assert!(!grant.network_access);           // deny by default

let output = sandbox.execute(&grant, workload_bytes)?;
```

### What `archon` actually does

With the feature enabled, `execute` builds a real kernel memory slice:

- the grant's `max_memory_bytes` becomes the size of a `BufferPool` drawn from
  an `InMemoryBlockDevice`;
- each page is handed to the workload as a minted `Capability`
  (`map_read` / `map_write` rights) from a `SharedIssuer`;
- the work runs as a cooperative task on the archon kernel scheduler;
- **revoking the capability denies all further access** — this is asserted by
  the test suite, not just documented.

Any access beyond the granted pool fails rather than over-allocating, so a
malicious shard cannot read the submitter's memory or the rest of the device.

## Testing

```bash
cargo test -p tpt-mosaic-sandbox
cargo test -p tpt-mosaic-sandbox --features archon
```

The `archon` tests cover grant construction, thermal decisions, and capability
revocation actually denying access.

## Notes and limitations

- `execute` returns the raw output bytes; hashing and quorum evaluation happen
  upstream in [`tpt-mosaic-verify`](../tpt-mosaic-verify/README.md) and
  [`tpt-mosaic-quorum`](../tpt-mosaic-quorum/README.md).
- RTOS preemption hooks are modelled through `ThermalPolicy` and the CPU/GPU
  budgets; board-specific RTOS integration is not yet wired up.

## Links

- Source: [`src/lib.rs`](src/lib.rs)
- Upstream: [`tpt-archon`](https://github.com/tpt-solutions/tpt-archon)
- Release notes: [CHANGELOG.md](CHANGELOG.md)
- API docs: <https://docs.rs/tpt-mosaic-sandbox>

## License

Licensed under either of Apache License, Version 2.0 or the MIT license at your
option. Copyright © 2026 TPT Solutions.