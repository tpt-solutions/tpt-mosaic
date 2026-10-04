# Changelog — tpt-mosaic-scheduler

All notable changes to `tpt-mosaic-scheduler` are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions
follow [Semantic Versioning](https://semver.org/). Workspace-wide notes live
in the root [CHANGELOG.md](../../CHANGELOG.md).

## [Unreleased]

### Changed

- **Breaking:** `SchedulerPolicy::assemble` takes a `reputation` view
  (`&dyn Fn(NodeId) -> f32`); built-in assemblers prefer higher-reputation
  peers when hardware diversity ties.

### Planned

- Energy- and thermal-cost-aware assembler policies (battery drain and
  thermal headroom as tie-breakers after diversity).
- Geographic / data-residency constraints as an additional assembly filter.

## [0.1.0] — 2026-01-15

### Added

- `SchedulerPolicy` trait so custom selection strategies can be plugged in.
- `HeterogeneousAssembler`: diversity-first, anchor-preferring selection —
  filters on availability and required capabilities, then maximises GPU vendor
  and CPU architecture spread before topping up.
- `BalancedAssembler` with explicit `min_anchors` / `max_anchors` bounds; the
  cap yields before the floor so a quorum can always be assembled.
- `StragglerPolicy` (`dispatch_timeout`, `max_replacements`) with
  `deadline_from`.
- `DispatchTracker` with `track`, `stragglers`, `clear` for timeout-based
  straggler detection and 1:1 replacement budgeting.
- Error handling for invalid quorum configurations and insufficient eligible
  peers (`MosaicError::InvalidQuorumConfig`).