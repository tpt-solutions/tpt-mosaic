//! Implement the [`TaskExecutor`] extension point with your own execution
//! semantics.
//!
//! ```text
//! cargo run -p tpt-mosaic-node --example custom_executor
//! ```
//!
//! The daemon ships a sandboxed executor (compile → `tpt-archon` confined
//! run). Devices with specialised silicon can implement [`TaskExecutor`]
//! instead: the trait receives the shard payload and an output scratch
//! buffer, and returns the produced slice, which the quorum layer hashes.

use tpt_mosaic_core::{TaskExecutor, TaskId};
use tpt_mosaic_verify::{hash_output, HashAlgorithm};

/// A toy executor: "renders" text payloads by upper-casing them. A real
/// device would run the shard on its GPU/NPU/DSP here.
struct UppercaseExecutor;

impl TaskExecutor for UppercaseExecutor {
    type Error = std::convert::Infallible;

    fn execute<'a>(
        &self,
        _task_id: TaskId,
        payload: &[u8],
        output: &'a mut [u8],
    ) -> Result<&'a [u8], Self::Error> {
        for (out, byte) in output.iter_mut().zip(payload.iter()) {
            *out = byte.to_ascii_uppercase();
        }
        let produced = payload.len().min(output.len());
        Ok(&output[..produced])
    }
}

fn main() {
    let task_id = TaskId::from_bytes([7; 16]);
    let executor = UppercaseExecutor;

    let mut scratch = vec![0u8; 1024];
    let produced = executor
        .execute(task_id, b"ship the shards, verify the quorum", &mut scratch)
        .expect("infallible executor");

    println!(
        "task     {}",
        tpt_mosaic_node::id::to_hex(task_id.as_bytes())
    );
    println!("produced {}", String::from_utf8_lossy(produced));
    println!(
        "output hash {}  <- this digest is what a quorum member votes",
        tpt_mosaic_node::id::to_hex(&hash_output(produced, HashAlgorithm::Blake3))
    );
}
