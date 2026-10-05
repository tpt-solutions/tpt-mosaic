//! Run a 1-of-1 task on a single node and print the receipt.
//!
//! ```text
//! cargo run -p tpt-mosaic-node --example hello_quorum
//! ```
//!
//! This is the smallest complete tpt-mosaic lifecycle: shard → compile →
//! sandbox → hash → quorum → settlement, all in-process, no networking.

use tpt_mosaic_core::QuorumConfig;
use tpt_mosaic_node::config::NodeConfig;
use tpt_mosaic_node::daemon::NodeDaemon;
use tpt_mosaic_verify::{hash_output, HashAlgorithm};

fn main() {
    // Default configuration: no mesh, no control API, ephemeral identity.
    let daemon = NodeDaemon::new(NodeConfig::from_toml_str("").expect("defaults are valid"));

    let payload = b"hello, mosaic!";
    let receipt = daemon
        .run_local_task(payload, QuorumConfig::BEST_EFFORT_1_OF_1)
        .expect("a lone node always meets a 1-of-1 quorum");

    println!("task          {}", hex(receipt.task_id.as_bytes()));
    println!("agreed hash   {}", hex(&receipt.agreed_hash));
    println!("confirmations {}", receipt.confirmations);
    println!("shards        {}", receipt.shards);
    println!("reward        {} micro-units", receipt.reward);
    println!(
        "payload hash  {} (BLAKE3 of the input, matched by the quorum)",
        hex(&hash_output(payload, HashAlgorithm::Blake3))
    );
    assert_eq!(
        receipt.agreed_hash,
        hash_output(payload, HashAlgorithm::Blake3)
    );
}

/// Lowercase hex for printing ids and hashes.
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}
