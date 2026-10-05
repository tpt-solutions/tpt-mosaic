//! Bring up a two-node mesh on loopback and meet a 2-of-2 quorum over real
//! TCP (signed MOSA v3 frames).
//!
//! ```text
//! cargo run -p tpt-mosaic-node --example two_node_mesh
//! ```
//!
//! The hub listens on an ephemeral port; the worker seeds itself to the hub
//! and learns the rest through gossip. Both nodes sign their beacons,
//! assignments, and result hashes with their Ed25519 identities.

use std::sync::Arc;
use std::time::Duration;

use tpt_mosaic_core::{QuorumConfig, TierLevel};
use tpt_mosaic_node::config::NodeConfig;
use tpt_mosaic_node::daemon::{MeshGuard, NodeDaemon};

fn main() {
    // Hub: mesh on an ephemeral loopback port, no seeds.
    let hub = spawn("[mesh]\nlisten_port = 0\n");
    let hub_addr = hub.0.mesh_addr().expect("hub mesh addr");

    // Worker: mesh on an ephemeral port, seeded to the hub.
    let worker = spawn(&format!(
        "[mesh]\nlisten_port = 0\nseeds = [\"{hub_addr}\"]\n"
    ));

    // Gossip: the hub registers itself, the worker's beacon exchange teaches
    // the hub about the worker, and the hub's gossip reply completes the view.
    hub.0.exchange_beacons();
    let mut attempts = 0;
    while {
        worker.0.exchange_beacons();
        hub.0.peer_ids().len() < 2
    } {
        attempts += 1;
        assert!(attempts < 100, "peers never discovered each other");
        std::thread::sleep(Duration::from_millis(20));
    }
    println!(
        "peer discovery complete: hub sees {} nodes",
        hub.0.peer_ids().len()
    );

    // A 2-of-2 quorum: both nodes execute the same payload and must agree.
    let payload = b"a payload for both nodes";
    let receipt = hub
        .0
        .run_network_task(payload, QuorumConfig::new(2, 2, TierLevel::BestEffort))
        .expect("both nodes are live; the quorum must be met");

    println!(
        "task          {}",
        tpt_mosaic_node::id::to_hex(receipt.task_id.as_bytes())
    );
    println!(
        "agreed hash   {}",
        tpt_mosaic_node::id::to_hex(&receipt.agreed_hash)
    );
    println!("confirmations {} (hub + worker)", receipt.confirmations);
    println!(
        "reward        {} micro-units (paid to each contributor)",
        receipt.reward
    );
    println!(
        "worker paid   {} micro-units on the hub's ledger",
        hub.0.balance_of(worker.0.node_id())
    );
}

/// Spawn a daemon with the given config TOML and start its mesh server.
fn spawn(toml: &str) -> (Arc<NodeDaemon>, MeshGuard) {
    let daemon = Arc::new(NodeDaemon::new(
        NodeConfig::from_toml_str(toml).expect("example config is valid"),
    ));
    let guard = daemon
        .start_mesh()
        .expect("the mesh listener must bind for this example");
    (daemon, guard)
}
