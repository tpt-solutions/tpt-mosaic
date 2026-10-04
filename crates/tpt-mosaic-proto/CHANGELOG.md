# Changelog — tpt-mosaic-proto

All notable changes to `tpt-mosaic-proto` are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions
follow [Semantic Versioning](https://semver.org/). Workspace-wide notes live
in the root [CHANGELOG.md](../../CHANGELOG.md).

## [Unreleased]

### Changed

- `read_frame` no longer pre-allocates the announced payload size; the
  buffer grows with the bytes that actually arrive, so a lying header
  cannot force a 16 MiB allocation. Short frames now fail with
  `UnexpectedEof`.

### Planned

- FlatBuffers `.fbs` schemas under `schemas/` with a `build.rs` running
  `flatc` to generate bindings, replacing the hand-rolled codec
  (`build-dependencies` is already stubbed).
- Batch frames that carry several messages in one write for high-latency links.

## [0.1.0] — 2026-01-15

### Added

- Initial MOSA v2 wire protocol: `TaskAssignment`, `ResultHash`,
  `HeartbeatBeacon`, `CancellationSignal`, `DhtQuery` and `PeerGossip`,
  wrapped in the `WireMessage` enum.
- `CancellationReason` codes: `QuorumMet`, `ClientAbort`, `Timeout`.
- `PeerAdvert` — one peer's entry inside a `PeerGossip` snapshot (identity,
  mesh address, hardware profile, capability flags).
- Hand-rolled frame codec (`encode` / `decode`) over an 11-byte header of
  magic, version, tag and payload length.
- Strict decoder: rejects bad magic, wrong version, unknown tags,
  truncation, trailing bytes, invalid enum discriminants and over-cap payloads
  without allocating.
- `std`-gated `read_frame` / `write_frame` stream helpers.
- `no_std` + `alloc` build mode via the `std` feature flag.
- `proptest` suites: round-trip fidelity for every message type and
  never-panic behaviour on arbitrary input.

### Notes

- Wire version is `2`: `HeartbeatBeacon` carries the advertiser's mesh
  address and `TaskAssignment` carries a family-tagged coordinator address, so
  peers learn where to dispatch work and where to return hashes. v1 had no
  address fields.
- `PeerGossip` replies to heartbeats are what let a single configured seed
  propagate the whole peer view across a hub-and-spoke deployment.