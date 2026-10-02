//! Workload definition, micro-task sharding, checkpoint/restore, priority
//! queues, and data-routing policy.

#![deny(missing_docs)]

pub mod routing;

use tpt_mosaic_core::{MosaicError, TaskId};

/// Priority of a task in the execution queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum TaskPriority {
    /// Background / best-effort work.
    BestEffort = 0,
    /// Standard throughput work.
    Standard = 1,
    /// Time-sensitive safety-critical work.
    Critical = 2,
}

/// Lifecycle state of a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskLifecycle {
    /// Accepted but not yet dispatched to any node.
    Queued,
    /// Dispatched to one or more nodes; awaiting result hashes.
    Dispatched,
    /// Actively executing on a node.
    Executing,
    /// Quorum met; final result returned to submitter.
    Complete,
    /// Irrecoverably failed (timeout, Byzantine fault, etc.).
    Failed,
}

/// A single stateless micro-task shard (target: 50–100 ms execution time).
#[derive(Debug, Clone)]
pub struct MicroTask {
    /// Parent task this shard belongs to.
    pub task_id: TaskId,
    /// Zero-based index within the parent task's shard sequence.
    pub shard_index: u32,
    /// Total number of shards the parent task was split into.
    pub total_shards: u32,
    /// Priority inherited from the parent task.
    pub priority: TaskPriority,
    /// Raw payload bytes for this shard.
    pub payload: Vec<u8>,
}

/// Splits a large payload into 50–100 ms micro-task shards.
///
/// `target_shard_bytes` is a rough size budget per shard; tune based on hardware benchmarks.
pub fn split_task(
    task_id: TaskId,
    payload: &[u8],
    priority: TaskPriority,
    target_shard_bytes: usize,
) -> Result<Vec<MicroTask>, MosaicError> {
    if payload.is_empty() {
        return Err(MosaicError::SerializationError);
    }
    let target = target_shard_bytes.max(1);
    let total_shards = payload.len().div_ceil(target);

    let shards = payload
        .chunks(target)
        .enumerate()
        .map(|(i, chunk)| MicroTask {
            task_id,
            shard_index: i as u32,
            total_shards: total_shards as u32,
            priority,
            payload: chunk.to_vec(),
        })
        .collect();

    Ok(shards)
}

/// Serialized checkpoint of an in-progress task (for interrupt/restore).
///
/// `last_completed_shard` doubles as the count of completed shards (shards
/// are zero-indexed), so it is also the index of the next shard to run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Checkpoint {
    /// Task being checkpointed.
    pub task_id: TaskId,
    /// Index of the last successfully completed shard.
    pub last_completed_shard: u32,
    /// Opaque state blob from the executor.
    pub state_blob: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tpt_mosaic_core::TaskId;

    #[test]
    fn split_produces_correct_shard_count() {
        let payload = vec![0u8; 1000];
        let shards = split_task(TaskId::NIL, &payload, TaskPriority::Standard, 300).unwrap();
        assert_eq!(shards.len(), 4); // ceil(1000/300) = 4
    }

    #[test]
    fn split_reassembly_round_trip() {
        let payload: Vec<u8> = (0..255).collect();
        let shards = split_task(TaskId::NIL, &payload, TaskPriority::Critical, 64).unwrap();
        let reassembled: Vec<u8> = shards
            .iter()
            .flat_map(|s| s.payload.iter().copied())
            .collect();
        assert_eq!(reassembled, payload);
    }

    #[test]
    fn split_empty_payload_errors() {
        let result = split_task(TaskId::NIL, &[], TaskPriority::BestEffort, 64);
        assert!(result.is_err());
    }

    #[test]
    fn priority_ordering() {
        assert!(TaskPriority::Critical > TaskPriority::Standard);
        assert!(TaskPriority::Standard > TaskPriority::BestEffort);
    }
}

/// FIFO-within-priority execution queue (spec §6.5: task priority queues).
///
/// Pops return the highest-priority queued [`MicroTask`]; ties resolve in
/// insertion order. [`TaskQueue::preempt_below`] implements the host-side
/// half of the safety-first preemption model (spec §5.2): when higher-class
/// work arrives, lower-class queued tasks are evicted so the host can reclaim
/// resources. Evicted tasks are returned for checkpointing or dropping.
#[derive(Debug, Default)]
pub struct TaskQueue {
    heap: std::collections::BinaryHeap<QueueEntry>,
    seq: u64,
}

#[derive(Debug)]
struct QueueEntry {
    seq: u64,
    task: MicroTask,
}

impl PartialEq for QueueEntry {
    fn eq(&self, other: &Self) -> bool {
        self.task.priority == other.task.priority && self.seq == other.seq
    }
}

impl Eq for QueueEntry {}

impl PartialOrd for QueueEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QueueEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Max-heap: higher priority first; within a priority, lower sequence
        // number (earlier insert) first.
        self.task
            .priority
            .cmp(&other.task.priority)
            .then(other.seq.cmp(&self.seq))
    }
}

impl TaskQueue {
    /// Create an empty queue.
    pub fn new() -> Self {
        Self::default()
    }

    /// Queue a task.
    pub fn push(&mut self, task: MicroTask) {
        let seq = self.seq;
        self.seq = self.seq.wrapping_add(1);
        self.heap.push(QueueEntry { seq, task });
    }

    /// Pop the highest-priority queued task (FIFO within a priority).
    pub fn pop(&mut self) -> Option<MicroTask> {
        self.heap.pop().map(|entry| entry.task)
    }

    /// Evict every queued task whose priority is strictly below `priority`,
    /// returning them in queue order. This is the preemption hook: a
    /// `Critical` dispatch evicts `Standard` and `BestEffort` work.
    pub fn preempt_below(&mut self, priority: TaskPriority) -> Vec<MicroTask> {
        let (kept, evicted): (Vec<_>, Vec<_>) = self
            .heap
            .drain()
            .partition(|entry| entry.task.priority >= priority);
        self.heap = kept.into_iter().collect();
        let mut evicted: Vec<(u64, MicroTask)> =
            evicted.into_iter().map(|e| (e.seq, e.task)).collect();
        evicted.sort_by_key(|(seq, _)| *seq);
        evicted.into_iter().map(|(_, task)| task).collect()
    }

    /// Number of queued tasks.
    pub fn len(&self) -> usize {
        self.heap.len()
    }

    /// Returns `true` when nothing is queued.
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }
}

/// Persist `checkpoint` to `path` (atomic: temp file + rename).
///
/// Layout: task id (16 bytes) + last completed shard (`u32` LE) + blob length
/// (`u32` LE) + blob.
pub fn save_checkpoint(path: &std::path::Path, checkpoint: &Checkpoint) -> std::io::Result<()> {
    let mut bytes = Vec::with_capacity(24 + checkpoint.state_blob.len());
    bytes.extend_from_slice(checkpoint.task_id.as_bytes());
    bytes.extend_from_slice(&checkpoint.last_completed_shard.to_le_bytes());
    bytes.extend_from_slice(&(checkpoint.state_blob.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&checkpoint.state_blob);

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, &bytes)?;
    let _ = std::fs::remove_file(path);
    std::fs::rename(&tmp, path)
}

/// Restore a checkpoint previously written by [`save_checkpoint`].
pub fn restore_checkpoint(path: &std::path::Path) -> std::io::Result<Checkpoint> {
    let bytes = std::fs::read(path)?;
    if bytes.len() < 24 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "checkpoint too short",
        ));
    }
    let task_id = TaskId::from_bytes(bytes[0..16].try_into().expect("16 bytes"));
    let last_completed_shard = u32::from_le_bytes(bytes[16..20].try_into().expect("4 bytes"));
    let blob_len = u32::from_le_bytes(bytes[20..24].try_into().expect("4 bytes")) as usize;
    if bytes.len() != 24 + blob_len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "checkpoint blob length mismatch",
        ));
    }
    Ok(Checkpoint {
        task_id,
        last_completed_shard,
        state_blob: bytes[24..].to_vec(),
    })
}

/// Shard-completion cursor for an in-progress task: advances shard by shard,
/// snapshots itself into a [`Checkpoint`], and rebuilds from one on restore.
#[derive(Debug, Clone)]
pub struct TaskProgress {
    task_id: TaskId,
    total_shards: u32,
    /// Number of shards completed so far (also the index of the next shard).
    completed: u32,
}

impl TaskProgress {
    /// Start tracking a task of `total_shards` shards.
    pub fn new(task_id: TaskId, total_shards: u32) -> Self {
        Self {
            task_id,
            total_shards,
            completed: 0,
        }
    }

    /// Rebuild progress from a checkpoint. Fails when the checkpoint claims
    /// more completed shards than the task has.
    pub fn from_checkpoint(
        checkpoint: &Checkpoint,
        total_shards: u32,
    ) -> Result<Self, MosaicError> {
        if checkpoint.last_completed_shard > total_shards {
            return Err(MosaicError::SerializationError);
        }
        Ok(Self {
            task_id: checkpoint.task_id,
            total_shards,
            completed: checkpoint.last_completed_shard,
        })
    }

    /// The task being tracked.
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    /// Mark the next shard completed; returns `false` when already complete.
    pub fn advance(&mut self) -> bool {
        if self.completed >= self.total_shards {
            return false;
        }
        self.completed += 1;
        true
    }

    /// Returns `true` when every shard has completed.
    pub fn is_complete(&self) -> bool {
        self.completed >= self.total_shards
    }

    /// Number of shards already completed (resume offset).
    pub fn completed(&self) -> u32 {
        self.completed
    }

    /// Snapshot the cursor into a checkpoint with an opaque state blob.
    pub fn checkpoint(&self, state_blob: Vec<u8>) -> Checkpoint {
        Checkpoint {
            task_id: self.task_id,
            last_completed_shard: self.completed,
            state_blob,
        }
    }

    /// The slice of the full payload still to execute: everything after the
    /// completed shards, using the same `shard_target` split size.
    pub fn remaining_payload<'a>(&self, full_payload: &'a [u8], shard_target: usize) -> &'a [u8] {
        let skip = self.completed as usize * shard_target.max(1);
        full_payload.get(skip..).unwrap_or(&[])
    }
}

#[cfg(test)]
mod queue_tests {
    use super::*;
    use tpt_mosaic_core::TaskId;

    fn shard(priority: TaskPriority, index: u32) -> MicroTask {
        MicroTask {
            task_id: TaskId::NIL,
            shard_index: index,
            total_shards: 1,
            priority,
            payload: vec![index as u8],
        }
    }

    #[test]
    fn pops_in_priority_order_then_fifo() {
        let mut queue = TaskQueue::new();
        queue.push(shard(TaskPriority::BestEffort, 0));
        queue.push(shard(TaskPriority::Standard, 1));
        queue.push(shard(TaskPriority::Standard, 2));
        queue.push(shard(TaskPriority::Critical, 3));

        let popped: Vec<u32> = std::iter::from_fn(|| queue.pop())
            .map(|t| t.shard_index)
            .collect();
        assert_eq!(popped, vec![3, 1, 2, 0]);
        assert!(queue.is_empty());
    }

    #[test]
    fn preemption_evicts_only_lower_priorities() {
        let mut queue = TaskQueue::new();
        queue.push(shard(TaskPriority::BestEffort, 0));
        queue.push(shard(TaskPriority::Standard, 1));
        queue.push(shard(TaskPriority::BestEffort, 2));
        queue.push(shard(TaskPriority::Critical, 3));

        let evicted = queue.preempt_below(TaskPriority::Standard);
        assert_eq!(
            evicted.iter().map(|t| t.shard_index).collect::<Vec<_>>(),
            vec![0, 2]
        );
        assert_eq!(queue.len(), 2);

        // Standard and Critical survive a Standard-level preemption.
        let survivors: Vec<u32> = std::iter::from_fn(|| queue.pop())
            .map(|t| t.shard_index)
            .collect();
        assert_eq!(survivors, vec![3, 1]);
    }

    #[test]
    fn preemption_of_higher_priority_evicts_nothing() {
        let mut queue = TaskQueue::new();
        queue.push(shard(TaskPriority::Standard, 1));
        assert!(queue.preempt_below(TaskPriority::BestEffort).is_empty());
        assert_eq!(queue.len(), 1);
    }
}

#[cfg(test)]
mod checkpoint_tests {
    use super::*;
    use tpt_mosaic_core::TaskId;

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("mosaic-task-{}-{name}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        dir
    }

    #[test]
    fn checkpoint_round_trips_through_disk() {
        let dir = temp_dir("ckpt");
        let path = dir.join("cp.bin");
        let checkpoint = Checkpoint {
            task_id: TaskId::from_bytes([9; 16]),
            last_completed_shard: 7,
            state_blob: vec![0xAB; 300],
        };
        save_checkpoint(&path, &checkpoint).expect("save");
        let restored = restore_checkpoint(&path).expect("restore");
        assert_eq!(restored, checkpoint);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn truncated_checkpoint_is_rejected() {
        let dir = temp_dir("trunc");
        let path = dir.join("cp.bin");
        std::fs::write(&path, [0u8; 10]).unwrap();
        assert!(restore_checkpoint(&path).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn progress_resumes_from_a_checkpoint() {
        let task_id = TaskId::from_bytes([4; 16]);
        let payload = vec![0u8; 1000];
        const SHARD_TARGET: usize = 300; // 4 shards

        let mut progress = TaskProgress::new(task_id, 4);
        for _ in 0..2 {
            assert!(progress.advance());
        }
        let checkpoint = progress.checkpoint(vec![1, 2, 3]);
        assert_eq!(checkpoint.last_completed_shard, 2);

        // Interrupt: persist, then restore into a fresh cursor.
        let dir = temp_dir("resume");
        let path = dir.join("cp.bin");
        save_checkpoint(&path, &checkpoint).unwrap();
        let restored = restore_checkpoint(&path).unwrap();
        let mut resumed = TaskProgress::from_checkpoint(&restored, 4).expect("valid checkpoint");
        assert_eq!(resumed.task_id(), task_id);
        assert_eq!(resumed.remaining_payload(&payload, SHARD_TARGET).len(), 400);

        while resumed.advance() {}
        assert!(resumed.is_complete());
        assert!(!resumed.advance());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn checkpoint_beyond_total_shards_is_rejected() {
        let checkpoint = Checkpoint {
            task_id: TaskId::NIL,
            last_completed_shard: 9,
            state_blob: vec![],
        };
        assert!(TaskProgress::from_checkpoint(&checkpoint, 4).is_err());
    }

    #[test]
    fn remaining_payload_is_empty_when_complete() {
        // A checkpoint claiming exactly all shards is a finished cursor.
        let progress = TaskProgress::from_checkpoint(
            &Checkpoint {
                task_id: TaskId::NIL,
                last_completed_shard: 4,
                state_blob: vec![],
            },
            4,
        )
        .expect("completed == total is a valid finished checkpoint");
        assert!(progress.is_complete());
        assert!(progress.remaining_payload(&[0u8; 1000], 300).is_empty());
    }
}
