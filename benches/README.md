# tpt-mosaic-benches

> Criterion benchmarks for the tpt-mosaic hot paths: wire codec, task sharding, output hashing, quorum evaluation, and quorum assembly.

*By TPT Solutions · Licensed under [MIT](../LICENSE-MIT) OR [Apache-2.0](../LICENSE-APACHE)*

---

## Overview

Performance is a correctness concern for this fabric: a quorum has a deadline,
and a slow encode or a slow shard split turns into a straggler. This crate
measures the five paths that run on every single task, so a regression shows
up as a number rather than as a timeout in the field.

This is a private workspace member (`publish = false`) — it is not a library and
is not published to crates.io.

## Running

```bash
# Run every benchmark
cargo bench -p tpt-mosaic-benches

# One group only
cargo bench -p tpt-mosaic-benches -- codec
cargo bench -p tpt-mosaic-benches -- quorum

# Quick pass while iterating (fewer samples)
cargo bench -p tpt-mosaic-benches -- --quick

# Filter to a single benchmark
cargo bench -p tpt-mosaic-benches -- "blake3/64k"
```

HTML reports are written to `target/criterion/`; each benchmark gets a page with
a historical trend, so a comparison against an earlier revision is one command
away:

```bash
git checkout <old-rev> && cargo bench -p tpt-mosaic-benches
git checkout - && cargo bench -p tpt-mosaic-benches
cargo bench -p tpt-mosaic-benches -- --save-baseline main
cargo bench -p tpt-mosaic-benches -- --baseline main
```

Criterion is configured with a 500 ms warm-up and a 2 s measurement window per
benchmark.

## Benchmark groups

| Group | Benchmark | What it measures |
|---|---|---|
| `codec` | `encode/beacon` | Serialising a realistic `HeartbeatBeacon` |
| `codec` | `decode/beacon` | Parsing that beacon back |
| `codec` | `encode/assignment_64k` | Serialising a shard-sized 64 KiB `TaskAssignment` (throughput in bytes) |
| `codec` | `decode/assignment_64k` | Parsing it back — the dominant data-plane cost |
| `task` | `split_task/256k_into_64k_shards` | Sharding 256 KiB into 64 KiB micro-tasks |
| `verify` | `blake3/64k` | BLAKE3 digest throughput |
| `verify` | `sha256/64k` | SHA-256 digest throughput (compatibility path) |
| `quorum` | `hash_collector/5_of_5` | A full divergent 5-of-5 round through `HashCollector` |
| `scheduler` | `balanced_assemble/500_candidates_7_of_10` | Assembling 7-of-10 from a 500-peer table |

The candidate fixture for the scheduler benchmark deliberately mixes anchor and
edge nodes across four GPU vendors and two CPU architectures, so assembly
measures the real diversity-search path rather than a trivial early match.

## Tuning guidance

| Lever | Knob | Effect |
|---|---|---|
| Shard size | `[node]` / `split_task` `target_shard_bytes` | Smaller shards = more parallelism, more per-shard overhead |
| JIT cache | `[compiler] cache_dir` | Eliminates repeat compiles for identical (workload, hardware) pairs |
| Hash algorithm | `HashAlgorithm` | BLAKE3 is the default and the faster of the two |
| Quorum size | `QuorumConfig` preset | Bigger N costs more assembly, hashing and network time |

## CI

The release build type-checks the benchmarks (so a broken bench fails CI
without paying full benchmark time). Enforced regression thresholds are planned
— see the root [CHANGELOG](../CHANGELOG.md).

## Links

- Source: [`benches/mosaic.rs`](benches/mosaic.rs)
- Manifest: [`Cargo.toml`](Cargo.toml)
- Release notes: [CHANGELOG.md](CHANGELOG.md)
- Workspace overview: [root README](../README.md)

## License

Licensed under either of Apache License, Version 2.0 or the MIT license at your
option. Copyright © 2026 TPT Solutions.