# tpt-mosaic-economy

> Incentive layer for [tpt-mosaic](../../README.md): micro-rewards, reputation scoring, slashing, and on-chain settlement across Solana, Base, and NEAR.

[![CI](https://github.com/tpt-solutions/tpt-mosaic/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/tpt-solutions/tpt-mosaic/actions/workflows/ci.yml)

*By TPT Solutions · Licensed under [MIT](../../LICENSE-MIT) OR [Apache-2.0](../../LICENSE-APACHE)*

---

## Overview

Volunteering someone's idle GPU is worthless unless something is at stake. This
crate prices a contribution, tracks who has been reliable, records penalties for
misbehaviour, and settles the result on chain.

The pricing rule is simple and deterministic: **cost scales with confidence**.
A 7-of-10 mission-critical quorum is worth far more than a 1-of-1 edge
inference, and a node that advertised an accelerator is worth more than one
that did not.

## Features

| Feature | Default | Effect |
|---|---|---|
| `solana` | off | Reserved for the concrete Solana RPC adapter |
| `base` | off | Reserved for the Base (Ethereum L2) RPC adapter |
| `near` | off | Reserved for the NEAR RPC adapter |
| `all-chains` | off | Enables all three |

The adapters currently settle against a local `InMemoryLedger`, which keeps the
full node loop testable with no network access. These features are the switch
that will swap the ledger calls for real SDK transactions once the SDK
dependencies are re-enabled.

## Installation

```toml
[dependencies]
tpt-mosaic-economy = { path = "crates/tpt-mosaic-economy" }
```

## Reward model

```rust
use tpt_mosaic_core::{CapabilityFlags, QuorumConfig};
use tpt_mosaic_economy::calculate_reward;

// Mission Critical tier, node advertising two capabilities, 10 shards.
let payout = calculate_reward(
    &QuorumConfig::MISSION_CRITICAL_7_OF_10,
    CapabilityFlags::CUDA | CapabilityFlags::NPU,
    10,
);
```

Base payout per executed shard:

| Tier | Base |
|---|---|
| `BestEffort` | 1 |
| `Standard` | 4 |
| `MissionCritical` | 10 |

Each advertised capability flag adds **5 percentage points** to the payout.
The function is pure and side-effect free — it computes an amount, and the
caller submits it through a `Settlement` adapter. Zero shards pays zero.

## Reputation

```rust
use tpt_mosaic_core::NodeId;
use tpt_mosaic_economy::{DEFAULT_SCORE, ReputationStore, SLASH_THRESHOLD};
use tpt_mosaic_quorum::QuorumResult;

let mut store = ReputationStore::new();
let node = NodeId::from_bytes([8u8; 16]);

assert_eq!(store.score(node), DEFAULT_SCORE);   // 0.5 for an untracked node

store.record_success(node);

// Or drive it straight from the consensus outcome:
store.record_outcome(node, &QuorumResult::Met {
    agreed_hash: [0xAA; 32],
    confirmations: 3,
});

assert_eq!(store.len(), 1);
assert!(!store.should_slash(node));
```

| Constant | Value | Effect |
|---|---|---|
| `DEFAULT_SCORE` | `0.5` | Starting score for a node with no history |
| `SUCCESS_DELTA` | `+0.01` | Per successful quorum contribution |
| `FAILURE_DELTA` | `-0.05` | Per timeout, divergence or sandbox violation |
| `SLASH_THRESHOLD` | `0.2` | Below this, `should_slash` returns `true` |

Failures hurt five times as much as successes help, so a node cannot buy trust
by succeeding occasionally. Scores are clamped to `[0.0, 1.0]`. The store is
in-memory — swap in a persistent backing store before production use.

## Slashing

`SlashingRecord` documents a penalty and the reason behind it:

| `SlashReason` | Cause |
|---|---|
| `ByzantineFault` | Hashes consistently diverged from the quorum |
| `RepeatedTimeout` | Repeatedly failed to respond |
| `SandboxViolation` | Attempted to escape its capability grants |

A record carries the node, the reason, the amount deducted and a millisecond
timestamp, and is handed to a `Settlement` adapter for on-chain reporting.

## Settlement

The chain-agnostic interface is two methods:

```rust
pub trait Settlement: Send + Sync {
    fn submit_reward(&self, node_id: NodeId, amount: u64, task_context: &str)
        -> Result<(), MosaicError>;
    fn balance(&self, node_id: NodeId) -> Result<u64, MosaicError>;
}
```

`Chain` selects the adapter at runtime — `Solana` (high-throughput
micro-payments), `Base` (Ethereum L2, ERC-20 reward token), or `Near` (WASM
contracts). Implementations: `SolanaSettlement::new(rpc_url)`,
`BaseSettlement::new(rpc_url)`, `NearSettlement::new(rpc_url)`.

> The current adapters settle locally against an in-memory ledger and emit a
> `tracing::debug!` line. Real RPC broadcast is pending upstream SDK features —
> do not treat these balances as real money yet.

## Testing

```bash
cargo test -p tpt-mosaic-economy
cargo test -p tpt-mosaic-economy --all-features
```

## Links

- Sources: [`src/lib.rs`](src/lib.rs) (traits, `Chain`),
  [`src/reward.rs`](src/reward.rs), [`src/reputation.rs`](src/reputation.rs),
  [`src/chains.rs`](src/chains.rs)
- Outcomes come from [`tpt-mosaic-quorum`](../tpt-mosaic-quorum/README.md).
- Release notes: [CHANGELOG.md](CHANGELOG.md)
- API docs: <https://docs.rs/tpt-mosaic-economy>

## License

Licensed under either of Apache License, Version 2.0 or the MIT license at your
option. Copyright © 2026 TPT Solutions.