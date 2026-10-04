# tpt-mosaic-scheduler

> Quorum assembly, heterogeneous hardware matching, straggler detection, and task routing for [tpt-mosaic](../../README.md).

[![CI](https://github.com/tpt-solutions/tpt-mosaic/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/tpt-solutions/tpt-mosaic/actions/workflows/ci.yml)

*By TPT Solutions · Licensed under [MIT](../../LICENSE-MIT) OR [Apache-2.0](../../LICENSE-APACHE)*

---

## Overview

tpt-mosaic gets its resilience from two ideas: never run the same kind of
silicon twice if you can help it, and never let one slow node hold up the
quorum. This crate implements both.

- **`HeterogeneousAssembler`** (default) maximises hardware diversity — GPU
  vendor and CPU architecture spread across the quorum — and prefers anchor
  nodes for the first slots.
- **`BalancedAssembler`** trades some diversity for a guaranteed mix of
  edge and anchor nodes, with explicit `min_anchors` / `max_anchors` bounds.
- **`DispatchTracker` + `StragglerPolicy`** detect nodes that miss their
  deadline and budget replacements at 1:1.

## Installation

```toml
[dependencies]
tpt-mosaic-scheduler = { path = "crates/tpt-mosaic-scheduler" }
```

## The `SchedulerPolicy` trait

```rust
use tpt_mosaic_core::{CapabilityFlags, MosaicError, NodeId, QuorumConfig};
use tpt_mosaic_discovery::PeerRecord;
use tpt_mosaic_scheduler::SchedulerPolicy;

pub trait MyPolicy: Send + Sync {
    fn assemble(
        &self,
        candidates: &[PeerRecord],
        config: &QuorumConfig,
        required_capabilities: CapabilityFlags,
    ) -> Result<Vec<NodeId>, MosaicError>;
}
```

Implement it to plug a custom selection strategy (energy-aware, geographic,
cost-optimising) into the node daemon. Implementations must return exactly
`config.n` nodes that satisfy the policy.

## Built-in assemblers

### `HeterogeneousAssembler`

```rust
use tpt_mosaic_core::{CapabilityFlags, QuorumConfig};
use tpt_mosaic_scheduler::{HeterogeneousAssembler, SchedulerPolicy};

let config = QuorumConfig::STANDARD_3_OF_5;
let selected = HeterogeneousAssembler.assemble(&candidates, &config, CapabilityFlags::CUDA)?;
```

Selection proceeds in three stages:

1. **Filter** — keep only peers where `hardware.is_available()` (not
   thermally critical, battery above 10%) and `capabilities` contain every
   required flag.
2. **Diversify** — fill slots preferring, in order, unseen GPU vendors, then
   unseen CPU architectures.
3. **Fill** — top up with any remaining eligible peer.

If fewer than `n` peers are eligible the call fails rather than assembling an
under-strength quorum. An invalid `QuorumConfig` (`k == 0`, `k > n`) returns
`MosaicError::InvalidQuorumConfig`.

### `BalancedAssembler`

```rust
use tpt_mosaic_scheduler::BalancedAssembler;

let assembler = BalancedAssembler {
    min_anchors: 1,
    max_anchors: 3,
};
```

Honours the anchor bounds when the fleet can satisfy them; when it cannot, the
`max_anchors` cap yields first so the quorum can still be assembled with edge
tiles.

## Straggler handling

```rust
use std::time::{Duration, Instant};
use tpt_mosaic_scheduler::{DispatchTracker, StragglerPolicy};

let policy = StragglerPolicy {
    dispatch_timeout: Duration::from_millis(250),
    max_replacements: 2,   // replacement budget per dispatch
};

let now = Instant::now();
let mut tracker = DispatchTracker::new();
tracker.track(node_a, policy.deadline_from(now));   // arm a deadline
let late = tracker.stragglers(now);                // nodes past their deadline
assert_eq!(tracker.outstanding(), 1);
tracker.complete(node_a);                          // worker answered
# let _ = late;
```

Replacement is capped 1:1 — the scheduler never inflates N beyond the quorum
configuration, so a slow fleet cannot be silently padded with extra nodes.

## Testing

```bash
cargo test -p tpt-mosaic-scheduler
```

## Links

- Sources: [`src/lib.rs`](src/lib.rs) (assemblers),
  [`src/straggler.rs`](src/straggler.rs)
- Inputs come from [`tpt-mosaic-discovery`](../tpt-mosaic-discovery/README.md)
  (`PeerRecord`) and [`tpt-mosaic-core`](../tpt-mosaic-core/README.md)
  (`QuorumConfig`, `CapabilityFlags`).
- Release notes: [CHANGELOG.md](CHANGELOG.md)
- API docs: <https://docs.rs/tpt-mosaic-scheduler>

## License

Licensed under either of Apache License, Version 2.0 or the MIT license at your
option. Copyright © 2026 TPT Solutions.