# Changelog — tpt-mosaic-compiler

All notable changes to `tpt-mosaic-compiler` are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions
follow [Semantic Versioning](https://semver.org/). Workspace-wide notes live
in the root [CHANGELOG.md](../../CHANGELOG.md).

## [Unreleased]

### Changed

- **Breaking (on-disk format):** cache entries are now
  `BLAKE3(artifact) || artifact`; raw legacy entries are ignored and
  recompiled, corrupt/oversize entries are discarded, and the fingerprint
  includes the compiled-in backend feature set. Temp writes use
  process-unique names; artifacts above 64 MiB are not cached.

### Planned

- Cache eviction / size quotas so long-lived edge devices cannot fill storage.
- Backend selection weighted by measured throughput rather than hardware
  class alone.

## [0.1.0] — 2026-01-15

### Added

- `CompilationBackend` (`Gpu`, `Universal`) and `select_backend` dispatching on
  the node's GPU vendor.
- `compile` with a universal fallback path: if the preferred backend fails the
  workload is retried through `CompilationBackend::Universal`, and only a
  failure of both returns `MosaicError::CompilationFailed`.
- `fingerprint` — a stable BLAKE3 (workload, hardware profile) digest used as
  the artifact cache key.
- `JitCache` persisting compiled artifacts to `<fingerprint-hex>.fbin`, with
  `compile_cached` and `entry_path`.
- `gpu` feature: real compilation of TPTIR text through `tpt-gpu-runtime`'s
  `Device::load_module` (simulated device unless upstream `cuda` is enabled),
  plus `probe_gpu` reporting device name and memory.
- `crucible` feature: lowering of SafeTensors / GGUF model bytes to serialized
  TPT-IR via `tpt-crucible-catalyst`.
- Stub backends with identical signatures so the workspace builds and tests
  without network access or upstream crates.