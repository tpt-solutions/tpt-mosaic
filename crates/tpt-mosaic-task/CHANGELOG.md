# Changelog — tpt-mosaic-task

All notable changes to `tpt-mosaic-task` are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions
follow [Semantic Versioning](https://semver.org/). Workspace-wide notes live
in the root [CHANGELOG.md](../../CHANGELOG.md).

## [Unreleased]

### Planned

- Checkpoint format versioning so older state blobs can be migrated on load.
- Deadline propagation from the scheduler into `MicroTask` metadata.

## [0.1.0] — 2026-01-15

### Added

- `MicroTask` shard type with `TaskPriority` (`BestEffort`, `Standard`,
  `Critical`) and `TaskLifecycle` (`Queued`, `Dispatched`, `Executing`,
  `Complete`, `Failed`).
- `split_task` micro-task sharding with a configurable `target_shard_bytes`
  budget (64 KiB default in the daemon); empty payloads are rejected and a zero
  budget is clamped to one byte.
- `Checkpoint` with disk persistence via `save_checkpoint` / `restore_checkpoint`
  (16-byte task id + `u32` LE last shard + `u32` LE blob length + blob).
- `TaskProgress` resume cursor with `advance`, `snapshot` and
  `from_checkpoint`, rejecting checkpoints that claim more completed shards
  than the task has.
- `TaskQueue`: binary-heap priority queue, FIFO within a priority, plus
  `preempt_below` host-side preemption returning evicted shards for
  checkpointing.
- `routing_allowed` — the §4 cellular data-routing policy. Wi-Fi carries every
  payload kind; cellular permits only `Control`, `TaskAssignment` and
  `ResultProof`, so model shards and raw weights never leave over metered
  links.