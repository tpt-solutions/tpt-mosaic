# tpt-mosaic-quorum

> K-of-N consensus validation, hash collection, early termination, and Byzantine fault detection for [tpt-mosaic](../../README.md).

[![CI](https://github.com/tpt-solutions/tpt-mosaic/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/tpt-solutions/tpt-mosaic/actions/workflows/ci.yml)

*By TPT Solutions · Licensed under [MIT](../../LICENSE-MIT) OR [Apache-2.0](../../LICENSE-APACHE)*

---

## Overview

This crate is the consensus heart of the fabric. Nodes hash their raw output
bytes and submit the digest — never the output itself. `HashCollector` groups
those digests, decides when K-of-N agreement has been reached, flags nodes
that consistently diverge, and builds the cancellation signal that stops the
rest of the quorum from wasting cycles.

Because only digests travel the network, a result is verified without ever
moving megabytes of model weights off the device.

## Installation

```toml
[dependencies]
tpt-mosaic-quorum = { path = "crates/tpt-mosaic-quorum" }
```

## The state machine

```text
                  submit(...) xN
                        │
      ┌─────────────────▼─────────────────┐
      │                                   │
   Pending ──────► Collecting ──────► Finished(Met | Timeout | Diverged)
      │                                   ▲
      └───────────────────────────────────┘
                  timeout()
```

| `QuorumResult` | Meaning |
|---|---|
| `Met { agreed_hash, confirmations }` | K matching digests received; quorum satisfied |
| `Timeout` | Deadline expired without meeting the threshold |
| `Diverged` | Enough hashes received but none agreed — the Byzantine condition |

A finished round is terminal: further `submit` calls are ignored and return the
existing state.

## Usage

```rust
use tpt_mosaic_core::{NodeId, QuorumConfig, TaskId};
use tpt_mosaic_quorum::{HashCollector, QuorumResult, QuorumState};

let config = QuorumConfig::STANDARD_3_OF_5;      // k = 3, n = 5
let mut collector = HashCollector::new(TaskId::from_bytes([1u8; 16]), config)?;

let good = [0xAAu8; 32];
let evil = [0xBBu8; 32];

collector.submit(NodeId::from_bytes([1u8; 16]), good);
collector.submit(NodeId::from_bytes([2u8; 16]), good);
collector.submit(NodeId::from_bytes([3u8; 16]), good);   // third match — quorum met

match collector.state() {
    QuorumState::Finished(QuorumResult::Met { agreed_hash, confirmations }) => {
        assert_eq!(agreed_hash, good);
        assert_eq!(*confirmations, 3);
    }
    other => panic!("unexpected state: {other:?}"),
}

// Early termination: tell the stragglers to stop.
let signal = collector.cancellation_signal().unwrap();
assert_eq!(signal.reason, tpt_mosaic_proto::CancellationReason::QuorumMet);
```

### Divergence and suspicion

```rust
# use tpt_mosaic_core::{NodeId, QuorumConfig, TaskId};
# use tpt_mosaic_quorum::{HashCollector, QuorumResult, QuorumState};
# let mut collector = HashCollector::new(TaskId::from_bytes([2u8; 16]), QuorumConfig::STANDARD_3_OF_5)?;
for i in 1u8..=5 {
    // Every node returns a different digest — nobody agrees with anybody.
    collector.submit(NodeId::from_bytes([i; 16]), [i; 32]);
}

assert!(matches!(
    collector.state(),
    QuorumState::Finished(QuorumResult::Diverged)
));
assert!(!collector.suspected_byzantine().is_empty());
```

### Timeout

```rust
# use tpt_mosaic_core::{NodeId, QuorumConfig, TaskId};
# use tpt_mosaic_quorum::{HashCollector, QuorumResult, QuorumState};
# let mut collector = HashCollector::new(TaskId::from_bytes([3u8; 16]), QuorumConfig::STANDARD_3_OF_5)?;
collector.submit(NodeId::from_bytes([1u8; 16]), [1u8; 32]);
collector.timeout();

assert!(matches!(
    collector.state(),
    QuorumState::Finished(QuorumResult::Timeout)
));
# Ok::<(), tpt_mosaic_core::MosaicError>(())
```

## Invariants

- **Met wins early.** The moment any hash reaches K voters the round finishes;
  later submissions cannot change the outcome.
- **Diverged only when impossible.** A round is declared diverged once all N
  votes are in and no hash reached K — not before.
- **Suspects are tracked.** Nodes whose hashes consistently diverge are
  collected in `suspected_byzantine()` and reported to the economy layer for
  slashing.
- **No cancellation for divergence.** Every node has already voted, so there
  is nothing left to stop; `cancellation_signal()` returns `None` while the
  round is still running.

These are asserted by both unit tests and `proptest` suites over arbitrary
vote orderings.

## Testing

```bash
cargo test -p tpt-mosaic-quorum
```

## Links

- Source: [`src/lib.rs`](src/lib.rs)
- Digests come from [`tpt-mosaic-verify`](../tpt-mosaic-verify/README.md);
  cancellation frames are defined in
  [`tpt-mosaic-proto`](../tpt-mosaic-proto/README.md).
- Release notes: [CHANGELOG.md](CHANGELOG.md)
- API docs: <https://docs.rs/tpt-mosaic-quorum>

## License

Licensed under either of Apache License, Version 2.0 or the MIT license at your
option. Copyright © 2026 TPT Solutions.