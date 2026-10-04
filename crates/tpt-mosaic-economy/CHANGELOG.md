# Changelog — tpt-mosaic-economy

All notable changes to `tpt-mosaic-economy` are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions
follow [Semantic Versioning](https://semver.org/). Workspace-wide notes live
in the root [CHANGELOG.md](../../CHANGELOG.md).

## [Unreleased]

### Planned

- On-chain settlement adapters with live RPC once the Solana / Ethers /
  NEAR SDK dependencies are re-enabled; the `solana`, `base` and `near`
  features already gate that swap.
- Slashing amounts derived from the severity of the quorum failure rather than
  a fixed penalty.

## [0.1.0] — 2026-01-15

### Added

- `calculate_reward`: deterministic, side-effect-free micro-reward calculation
  scaling with quorum tier (base 1 / 4 / 10 per executed shard for Best Effort
  / Standard / Mission Critical) plus 5 percentage points per advertised
  capability flag.
- `SlashingRecord` and `SlashReason` (`ByzantineFault`, `RepeatedTimeout`,
  `SandboxViolation`) capturing the node, reason, amount and timestamp.
- `ReputationStore` with `record_success`, `record_failure` and
  `record_outcome` (driven directly by a `QuorumResult`), with clamped scores.
- Chain-agnostic `Settlement` trait (`submit_reward`, `balance`) and the `Chain`
  enum (`Solana`, `Base`, `Near`).
- `SolanaSettlement`, `BaseSettlement` and `NearSettlement` adapters backed by
  a shared `InMemoryLedger`, plus `LedgerBacked` plumbing and `tracing::debug!`
  settlement logs — keeping the node loop testable with no network access.
- `solana`, `base`, `near` and `all-chains` Cargo features reserved for the
  concrete RPC implementations.