# tpt-mosaic-compiler

> Hardware-aware compilation dispatch for [tpt-mosaic](../../README.md): routes each workload to `tpt-gpu` (CUDA/Metal/Vulkan) or `tpt-crucible` (NPU/DSP/CPU), with a persistent JIT cache.

[![CI](https://github.com/tpt-solutions/tpt-mosaic/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/tpt-solutions/tpt-mosaic/actions/workflows/ci.yml)

*By TPT Solutions · Licensed under [MIT](../../LICENSE-MIT) OR [Apache-2.0](../../LICENSE-APACHE)*

---

## Overview

A car NPU, a datacenter GPU and a phone DSP cannot run the same binary. This
crate inspects the target `HardwareProfile`, picks a compilation backend,
produces compiled bytes with a universal fallback when the preferred backend
fails, and caches the result on disk keyed by a (workload, hardware)
fingerprint so the same shard is never compiled twice on the same device.

## Features

| Feature | Default | Backend | Upstream crate |
|---|---|---|---|
| `gpu` | off | `CompilationBackend::Gpu` — TPTIR *text* compiled through `Device::load_module` | [`tpt-gpu-runtime`](https://github.com/tpt-solutions/tpt-gpu) |
| `crucible` | off | `CompilationBackend::Universal` — model bytes (SafeTensors, GGUF) lowered to serialized TPT-IR | [`tpt-crucible-catalyst`](https://github.com/tpt-solutions/tpt-crucible) |

With **no** features the same APIs run against in-crate stubs (the GPU backend
reports failure and falls through to universal, which passes bytes through),
so the workspace builds and tests with no network access.

## Installation

```toml
[dependencies]
tpt-mosaic-compiler = { path = "crates/tpt-mosaic-compiler", features = ["gpu", "crucible"] }
```

## Usage

### Backend selection

```rust
use tpt_mosaic_core::{CpuArch, GpuVendor, HardwareProfile, NodeKind, ThermalState};
use tpt_mosaic_compiler::{CompilationBackend, select_backend};

let profile = HardwareProfile {
    kind: NodeKind::AnchorBallast,
    gpu_vendor: GpuVendor::Nvidia,
    npu_present: false,
    cpu_arch: CpuArch::X86_64,
    memory_mb: 65_536,
    battery_level: 255,
    thermal_state: ThermalState::Nominal,
};

assert_eq!(select_backend(&profile), CompilationBackend::Gpu);
```

A node with `GpuVendor::None` or `GpuVendor::Other` is routed to
`CompilationBackend::Universal`.

### Compile with fallback

```rust
use tpt_mosaic_compiler::compile;

let binary = compile(workload_bytes, &profile)?;
```

`compile` dispatches to the selected backend and, if that fails, retries through
`CompilationBackend::Universal`. A failure of both paths returns
`MosaicError::CompilationFailed`.

### JIT cache

```rust
use tpt_mosaic_compiler::JitCache;

let cache = JitCache::new("var/jit");          // created lazily on first store
let fp = tpt_mosaic_compiler::fingerprint(workload_bytes, &profile);

let binary = cache.compile_cached(workload_bytes, &profile)?;  // hit or compile
let _ = cache.entry_path(fp);                  // var/jit/<hex>.fbin
```

Cache entries are keyed by a BLAKE3 fingerprint over the workload bytes, the
hardware-profile fields, and the compiled-in backend feature set, so a cached
artifact can never be replayed onto incompatible silicon or a different
backend build. Entries are stored integrity-framed as `BLAKE3(artifact) ||
artifact`: corrupt or oversize entries are discarded and recompiled, never
served. The fingerprint depends only on the inputs, so entries never go stale.

### GPU probe (`gpu` feature)

```rust
#[cfg(feature = "gpu")]
{
    let probe = tpt_mosaic_compiler::probe_gpu();
    println!("{} ({} MB)", probe.name, probe.memory_mb);
}
```

By default this opens the simulated device; real CUDA hardware is an upstream
opt-in.

## Backend matrix

| Workload | Node hardware | Backend | Result |
|---|---|---|---|
| TPTIR text | NVIDIA / AMD / Intel / Apple GPU | `gpu` | compiled module via `tpt-gpu` |
| TPTIR text | NPU / DSP / CPU only | `crucible` (stub fallback) | bytes passed through |
| SafeTensors / GGUF | any | `crucible` | serialized TPT-IR graph |
| anything | any, backend failed | universal fallback | degraded but non-fatal |

## Testing

```bash
cargo test -p tpt-mosaic-compiler
cargo test -p tpt-mosaic-compiler --features gpu
cargo test -p tpt-mosaic-compiler --features crucible
cargo test -p tpt-mosaic-compiler --all-features
```

## Links

- Sources: [`src/lib.rs`](src/lib.rs) (selection, dispatch, fallback),
  [`src/cache.rs`](src/cache.rs)
- Called by [`tpt-mosaic-node`](../tpt-mosaic-node/README.md) during task
  execution.
- Release notes: [CHANGELOG.md](CHANGELOG.md)
- API docs: <https://docs.rs/tpt-mosaic-compiler>

## License

Licensed under either of Apache License, Version 2.0 or the MIT license at your
option. Copyright © 2026 TPT Solutions.