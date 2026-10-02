//! Workload definition, micro-task sharding, checkpoint/restore, and lifecycle management.

#![deny(missing_docs)]

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
#[derive(Debug, Clone)]
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
