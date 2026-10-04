# tpt-mosaic-verify

> Cryptographic output hashing (BLAKE3 / SHA-256), ZK-ML proof hooks, and `tpt-eve` symbolic consistency checks for [tpt-mosaic](../../README.md).

[![CI](https://github.com/tpt-solutions/tpt-mosaic/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/tpt-solutions/tpt-mosaic/actions/workflows/ci.yml)

*By TPT Solutions · Licensed under [MIT](../../LICENSE-MIT) OR [Apache-2.0](../../LICENSE-APACHE)*

---

## Overview

Nodes never send raw output across the network — they send a digest. This crate
produces those digests deterministically, and reserves the interfaces for two
stronger proofs of correctness: zero-knowledge proofs of correct execution, and
symbolic consistency checking of the claims an inference result makes.

## Features

| Feature | Default | Effect |
|---|---|---|
| `std` | off | Enables `std` support in `tpt-mosaic-core`, `blake3` and `sha2` |
| `eve` | off | Symbolic consistency checking through [`tpt-eve`](https://github.com/tpt-solutions/tpt-eve); implies `std` |

Without features the crate is `no_std` + `alloc`.

## Installation

```toml
[dependencies]
tpt-mosaic-verify = { path = "crates/tpt-mosaic-verify", features = ["std", "eve"] }
```

## Usage

### Hashing output

```rust
use tpt_mosaic_verify::{HashAlgorithm, hash_output};

let digest = hash_output(b"inference result", HashAlgorithm::Blake3);
assert_eq!(digest.len(), 32);

// Hashing is a pure function of (bytes, algorithm).
assert_eq!(
    digest,
    hash_output(b"inference result", HashAlgorithm::Blake3)
);

// SHA-256 is available for compatibility with external systems.
let compat = hash_output(b"inference result", HashAlgorithm::Sha256);
assert_eq!(compat.len(), 32);
```

`HashAlgorithm::Blake3` is the default. Both backends return exactly 32 bytes,
which is what `ResultHash` on the wire carries — so a quorum can mix BLAKE3 and
SHA-256 nodes only by agreeing on the algorithm per round; in practice the
fabric uses BLAKE3 everywhere.

### ZK-ML proofs

```rust
use tpt_mosaic_core::TaskId;
use tpt_mosaic_verify::{StubProver, ZkProver};

let prover = StubProver;
let proof = prover.prove(TaskId::from_bytes([4u8; 16]), b"input", b"output").unwrap();
assert!(prover.verify(&proof).unwrap());
assert_eq!(proof.proof_bytes, Vec::<u8>::new());   // stub: no proof circuit yet
```

`ZkProver` is the extension point — implement `prove` / `verify` against a real
ZK-SNARK or ZK-STARK backend and every consumer picks it up unchanged.
`ZkProof` commits to the output hash via `committed_hash`.

> `StubProver` accepts everything. It is an interface reservation, **not** a
> security guarantee; do not use it where a proof must actually mean something.

### tpt-eve consistency hooks (`eve` feature)

```rust
use tpt_mosaic_verify::eve_hooks::Claim;

let claim = Claim::new("output-17", "classified_as", "benign", 0.9);
assert_eq!(claim.confidence, 0.9);
```

With `eve` enabled, each claim is lowered to a `tpt_eve_core` `Fact` carrying
its provenance and a confidence score in `0.0`–`1.0`, then checked against
other claims with `ConsistencyChecker`. Conflicting assertions about the same
`(subject, predicate)` pair surface as a `Contradiction`. This catches a class
of failure hashing cannot: a result that is internally consistent as bytes but
contradicts another fact the fabric already knows. The check is purely
in-memory — no disk access, no persistence.

## Testing

```bash
cargo test -p tpt-mosaic-verify
cargo test -p tpt-mosaic-verify --features std
cargo test -p tpt-mosaic-verify --all-features
```

`proptest` suites cover hash determinism and agreement between the BLAKE3 and
SHA-256 code paths.

## Links

- Sources: [`src/lib.rs`](src/lib.rs) (hashing, ZK hooks),
  [`src/eve_hooks.rs`](src/eve_hooks.rs)
- Consumers: [`tpt-mosaic-quorum`](../tpt-mosaic-quorum/README.md)
- Release notes: [CHANGELOG.md](CHANGELOG.md)
- API docs: <https://docs.rs/tpt-mosaic-verify>

## License

Licensed under either of Apache License, Version 2.0 or the MIT license at your
option. Copyright © 2026 TPT Solutions.