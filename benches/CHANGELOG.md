# Changelog — tpt-mosaic-benches

All notable changes to `tpt-mosaic-benches` are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions
follow [Semantic Versioning](https://semver.org/). Workspace-wide notes live
in the root [CHANGELOG.md](../CHANGELOG.md).

## [Unreleased]

### Planned

- Benchmark regression thresholds in CI, comparing each result against a
  stored baseline.

## [0.1.0] — 2026-01-15

### Added

- Criterion harness (500 ms warm-up, 2 s measurement window per benchmark).
- `codec` group: `encode` / `decode` for a realistic `HeartbeatBeacon` and for
  a shard-sized 64 KiB `TaskAssignment`, with byte throughput configured.
- `task` group: `split_task/256k_into_64k_shards`, sharding 256 KiB into 64 KiB
  micro-tasks.
- `verify` group: `blake3/64k` and `sha256/64k` output-hashing throughput.
- `quorum` group: `hash_collector/5_of_5`, a full divergent 5-of-5 round.
- `scheduler` group: `balanced_assemble/500_candidates_7_of_10`, assembling a
  7-of-10 quorum from a 500-peer table mixing anchor/edge nodes, four GPU
  vendors and two CPU architectures.
- CI release build type-checks the benchmarks.