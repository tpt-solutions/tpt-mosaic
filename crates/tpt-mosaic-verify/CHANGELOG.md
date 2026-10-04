# Changelog — tpt-mosaic-verify

All notable changes to `tpt-mosaic-verify` are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions
follow [Semantic Versioning](https://semver.org/). Workspace-wide notes live
in the root [CHANGELOG.md](../../CHANGELOG.md).

## [Unreleased]

### Planned

- A real ZK-ML prover behind the existing `ZkProver` trait (ZK-SNARK / ZK-STARK),
  replacing `StubProver`.
- Per-round hash-algorithm negotiation so a quorum can be verified across
  mixed-algorithm fleets.

## [0.1.0] — 2026-01-15

### Added

- `HashAlgorithm` (`Blake3` as default, `Sha256` as compatibility fallback) and
  `hash_output`, returning a 32-byte digest for both backends.
- `ZkProof` and the `ZkProver` trait (`prove` / `verify`) reserving the
  zero-knowledge proof interface.
- `StubProver` — accepts everything, commits to the BLAKE3 hash of the output,
  and emits empty proof bytes. Interface placeholder, not a security guarantee.
- `eve` feature: the `eve_hooks` module, lowering inference-output claims to
  provenance/confidence-scored facts and reporting symbolic contradictions via
  `tpt-eve-symbolic`'s `ConsistencyChecker`.
- `std` feature gating `std` support in `tpt-mosaic-core`, `blake3` and `sha2`.
- `no_std` + `alloc` default build mode.
- `proptest` suites asserting hash determinism and BLAKE3/SHA-256 backend
  agreement.