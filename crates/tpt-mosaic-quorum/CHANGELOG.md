# Changelog — tpt-mosaic-quorum

All notable changes to `tpt-mosaic-quorum` are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions
follow [Semantic Versioning](https://semver.org/). Workspace-wide notes live
in the root [CHANGELOG.md](../../CHANGELOG.md).

## [Unreleased]

### Planned

- Weighted voting so heterogeneous hardware classes carry proportional trust
  rather than one vote each.
- Slashing report emission straight from `suspected_byzantine()` instead of
  requiring the caller to translate it.

## [0.1.0] — 2026-01-15

### Added

- `HashCollector` K-of-N state machine over submitted 32-byte digests.
- `QuorumState` (`Pending`, `Collecting`, `Finished`) and `QuorumResult`
  (`Met { agreed_hash, confirmations }`, `Timeout`, `Diverged`).
- Early termination: the round finishes the instant any hash reaches K voters,
  and later submissions are ignored.
- Byzantine suspicion tracking via `suspected_byzantine()` for nodes whose
  hashes consistently diverge.
- `Diverged` declared only once all N votes are in with no winner, so a
  premature divergence cannot mask a still-reachable quorum.
- `cancellation_signal()` producing a `QuorumMet` signal on success and a
  `Timeout` signal after `timeout()`; `None` for diverged or in-flight rounds.
- `proptest` suites asserting the invariants above over arbitrary vote
  orderings.