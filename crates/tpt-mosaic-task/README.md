# tpt-mosaic-task

> Workload definition, micro-task sharding, checkpoint/restore, priority queues, and data-routing policy for [tpt-mosaic](../../README.md).

[![CI](https://github.com/tpt-solutions/tpt-mosaic/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/tpt-solutions/tpt-mosaic/actions/workflows/ci.yml)

*By TPT Solutions · Licensed under [MIT](../../LICENSE-MIT) OR [Apache-2.0](../../LICENSE-APACHE)*

---

## Overview

A mosaic job is far too large for any single idle device. This crate turns one
job into many small, stateless, retryable shards sized to run in 50–100 ms,
tracks how far each shard sequence has progressed, checkpoints that progress to
disk, queues shards by priority with host-side preemption, and enforces the
rule that raw model weights never travel over cellular.

## Installation

```toml
[dependencies]
tpt-mosaic-task = { path = "crates/tpt-mosaic-task" }
```

## Core types

| Type | Purpose |
|---|---|
| `MicroTask` | One stateless shard: parent `task_id`, `shard_index`, `total_shards`, `priority`, `payload` |
| `TaskPriority` | `BestEffort`, `Standard`, `Critical` |
| `TaskLifecycle` | `Queued`, `Dispatched`, `Executing`, `Complete`, `Failed` |
| `Checkpoint` | Durable progress marker (task id + last completed shard + state blob) |
| `TaskProgress` | In-memory resume cursor over a shard sequence |
| `TaskQueue` | Priority queue with FIFO within a priority, plus `preempt_below` |
| `routing` | Cellular data-routing policy (§4 of the spec) |

## Usage

### Sharding

```rust
use tpt_mosaic_core::TaskId;
use tpt_mosaic_task::{TaskPriority, split_task};

let payload = vec![0u8; 200_000];
let shards = split_task(
    TaskId::from_bytes([3u8; 16]),
    &payload,
    TaskPriority::Standard,
    64 * 1024,   // target shard size budget
)?;

assert_eq!(shards.len(), 4);
assert_eq!(shards[0].total_shards, 4);
assert_eq!(shards[0].shard_index, 0);
assert_eq!(shards[3].shard_index, 3);
# Ok::<(), tpt_mosaic_core::MosaicError>(())
```

`target_shard_bytes` is a rough size budget, not a guarantee — tune it against
the criterion benchmarks in [`tpt-mosaic-benches`](../../benches/README.md).
An empty payload is rejected (`MosaicError::SerializationError`); a
`target_shard_bytes` of `0` is clamped to `1`.

Because shards are indexed and self-describing, a lost or timed-out shard can
simply be re-dispatched without coordinating with the worker that had it.

### Queueing and preemption

```rust
use tpt_mosaic_task::TaskQueue;

let mut queue = TaskQueue::new();
queue.push(shard_best_effort);
queue.push(shard_standard);
queue.push(shard_critical);

// Highest priority first, FIFO within a priority.
let next = queue.pop().unwrap();

// A Critical dispatch evicts everything below it and hands the evicted
// shards back for checkpointing or dropping.
let evicted = queue.preempt_below(TaskPriority::Critical);
```

`preempt_below` is the host-side preemption hook: a safety-critical dispatch
reclaims capacity from background work without blocking.

### Checkpoint and resume

```rust
use tpt_mosaic_task::{Checkpoint, TaskProgress, restore_checkpoint, save_checkpoint};

let mut progress = TaskProgress::new(task_id, 4);
progress.advance();   // shard 0 done
progress.advance();   // shard 1 done

// `last_completed_shard` doubles as the completed-shard count (shards are
// zero-indexed), so it is also the index of the next shard to run.
let checkpoint = Checkpoint {
    task_id,
    last_completed_shard: 1,
    state_blob: vec![0xAB; 64],
};
save_checkpoint(std::path::Path::new("task.ckpt"), &checkpoint)?;

// Later — possibly after a process restart.
let restored = restore_checkpoint(std::path::Path::new("task.ckpt"))?;
let resumed = TaskProgress::from_checkpoint(&restored, 4)?;
assert_eq!(resumed.task_id(), task_id);
# Ok::<(), std::io::Error>(())
```

On-disk layout: `task_id` (16 bytes) + last completed shard (`u32` LE) + blob
length (`u32` LE) + blob. `TaskProgress::from_checkpoint` fails when a
checkpoint claims more completed shards than the task actually has, so a
corrupted or mismatched checkpoint is rejected rather than silently accepted.

### Cellular routing policy

```rust
use tpt_mosaic_task::routing::{PayloadKind, TransportClass, routing_allowed};

// Wi-Fi carries everything, including model shards.
assert!(routing_allowed(TransportClass::WifiMesh, PayloadKind::ModelShard));

// Cellular carries only control, assignments and proofs — never weights.
assert!(routing_allowed(TransportClass::Cellular, PayloadKind::Control));
assert!(routing_allowed(TransportClass::Cellular, PayloadKind::ResultProof));
assert!(!routing_allowed(TransportClass::Cellular, PayloadKind::ModelShard));
assert!(!routing_allowed(TransportClass::Cellular, PayloadKind::InferenceInput));
```

This is a hard guardrail, not advice: the daemon consults it before routing, so
a misconfigured fallback cannot leak large payloads over metered links.

## Testing

```bash
cargo test -p tpt-mosaic-task
```

## Links

- Sources: [`src/lib.rs`](src/lib.rs) (sharding, checkpoints, queue),
  [`src/routing.rs`](src/routing.rs)
- Release notes: [CHANGELOG.md](CHANGELOG.md)
- API docs: <https://docs.rs/tpt-mosaic-task>

## License

Licensed under either of Apache License, Version 2.0 or the MIT license at your
option. Copyright © 2026 TPT Solutions.