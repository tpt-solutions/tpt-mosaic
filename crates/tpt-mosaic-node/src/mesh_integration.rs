//! Multi-node mesh integration tests: real daemons over real loopback TCP.
//!
//! Each node is a fully wired [`NodeDaemon`] — mesh listener, frame handler,
//! sandboxed execution, quorum collector — talking MOSA v2 frames through
//! `TcpMesh`. No sockets are mocked.

use std::sync::Arc;
use std::time::Duration;

use crate::config::NodeConfig;
use crate::daemon::{MeshGuard, NodeDaemon};
use tpt_mosaic_core::{QuorumConfig, TierLevel};
use tpt_mosaic_verify::hash_output;
use tpt_mosaic_verify::HashAlgorithm;

/// Spawn a daemon with mesh enabled on an ephemeral loopback port,
/// optionally setting its node `kind` (e.g. `"anchor"`) and seeding it to
/// `hub`. The returned [`MeshGuard`] must be kept alive for the daemon to
/// keep serving mesh connections.
fn spawn_daemon(
    hub: Option<&std::net::SocketAddr>,
    kind: Option<&str>,
) -> (Arc<NodeDaemon>, MeshGuard) {
    let mut toml = String::new();
    if let Some(kind) = kind {
        toml.push_str(&format!("[node]\nkind = \"{kind}\"\n\n"));
    }
    toml.push_str("[mesh]\nlisten_port = 0\n");
    if let Some(hub) = hub {
        toml.push_str(&format!("seeds = [\"{hub}\"]\n"));
    }
    let daemon = Arc::new(NodeDaemon::new(
        NodeConfig::from_toml_str(&toml).expect("valid mesh config"),
    ));
    let guard = daemon
        .start_mesh()
        .expect("mesh listener must be bound for the test");
    (daemon, guard)
}

/// Poll `check` until it passes or `attempts` ticks elapse (listener startup
/// and beacon exchanges are asynchronous across threads).
fn wait_until(mut check: impl FnMut() -> bool, attempts: usize) -> bool {
    for _ in 0..attempts {
        if check() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    check()
}

#[test]
fn two_daemons_meet_a_two_of_two_quorum() {
    let (hub, _hub_guard) = spawn_daemon(None, None);
    let hub_addr = hub.mesh_addr().expect("hub mesh addr");
    let (worker, _worker_guard) = spawn_daemon(Some(&hub_addr), None);

    // The hub registers itself, then the worker's beacon exchange teaches the
    // hub about the worker; the hub's reply teaches the worker about the hub.
    hub.exchange_beacons();
    assert!(
        wait_until(
            || {
                worker.exchange_beacons();
                hub.peer_ids().len() == 2
            },
            50
        ),
        "hub must know self + worker"
    );

    let receipt = hub
        .run_network_task(
            b"shared mesh payload",
            QuorumConfig::new(2, 2, TierLevel::BestEffort),
        )
        .expect("2-of-2 quorum must be met over the mesh");
    assert_eq!(receipt.confirmations, 2);
    assert_eq!(
        receipt.agreed_hash,
        hash_output(b"shared mesh payload", HashAlgorithm::Blake3)
    );
}

#[test]
fn five_daemons_meet_three_of_five_with_early_termination() {
    let (hub, _hub_guard) = spawn_daemon(None, None);
    let hub_addr = hub.mesh_addr().expect("hub mesh addr");
    let (workers, guards): (Vec<_>, Vec<_>) =
        (0..4).map(|_| spawn_daemon(Some(&hub_addr), None)).unzip();
    let _ = guards;

    hub.exchange_beacons();
    assert!(
        wait_until(
            || {
                for worker in &workers {
                    worker.exchange_beacons();
                }
                hub.peer_ids().len() == 5
            },
            50
        ),
        "hub must know all five members"
    );

    let receipt = hub
        .run_network_task(b"quorum payload", QuorumConfig::STANDARD_3_OF_5)
        .expect("3-of-5 quorum must be met with five live members");
    assert!(receipt.confirmations >= 3);
    // Every contributor computes the identical deterministic hash.
    assert_eq!(
        receipt.agreed_hash,
        hash_output(b"quorum payload", HashAlgorithm::Blake3)
    );
}

#[test]
fn insufficient_peers_rejects_the_quorum_before_dispatch() {
    let (hub, _hub_guard) = spawn_daemon(None, None);
    let hub_addr = hub.mesh_addr().expect("hub mesh addr");
    let (_worker, _worker_guard) = spawn_daemon(Some(&hub_addr), None);

    // One worker only: 2 candidates for a 3-of-5 round. No assignment frames
    // are ever sent — the assembler rejects the round up front.
    let err = hub
        .run_network_task(b"payload", QuorumConfig::STANDARD_3_OF_5)
        .expect_err("2 candidates cannot satisfy 3-of-5");
    assert!(matches!(
        err,
        tpt_mosaic_core::MosaicError::InsufficientCapability
    ));
}

#[test]
fn dead_member_is_replaced_from_the_spare_pool() {
    let (hub, _hub_guard) = spawn_daemon(None, None);
    let hub_addr = hub.mesh_addr().expect("hub mesh addr");
    // Anchor decoy: the heterogeneous assembler sorts anchors first, so the
    // dead member is deterministically among the five selected.
    let (dead, _dead_guard) = spawn_daemon(Some(&hub_addr), Some("anchor"));
    let (lives, life_guards): (Vec<_>, Vec<_>) =
        (0..4).map(|_| spawn_daemon(Some(&hub_addr), None)).unzip();
    let _ = life_guards;

    hub.exchange_beacons();
    assert!(wait_until(
        || {
            dead.exchange_beacons();
            for life in &lives {
                life.exchange_beacons();
            }
            hub.peer_ids().len() == 6
        },
        50
    ));

    // Kill the anchor: its socket closes but its (fresh) peer record stays.
    let dead_addr = dead.mesh_addr().expect("dead mesh addr");
    drop(_dead_guard);
    drop(dead);
    assert!(port_is_closed(dead_addr), "dead socket must be closed");

    // 5-of-5 with 6 candidates: the dead anchor is selected, the leftover
    // live member is the spare. Wave 1 gathers 4 confirmations; the
    // replacement wave fills the anchor's slot.
    let receipt = hub
        .run_network_task(
            b"replacement payload",
            QuorumConfig::new(5, 5, TierLevel::BestEffort),
        )
        .expect("quorum must be met after replacing the dead member");
    assert_eq!(receipt.confirmations, 5);
    assert_eq!(
        receipt.agreed_hash,
        hash_output(b"replacement payload", HashAlgorithm::Blake3)
    );
}

/// Best-effort check that nothing is listening on `addr` any more.
fn port_is_closed(addr: std::net::SocketAddr) -> bool {
    std::net::TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_err()
}

#[test]
fn dead_member_without_spares_fails_the_round() {
    let (hub, _hub_guard) = spawn_daemon(None, None);
    let hub_addr = hub.mesh_addr().expect("hub mesh addr");
    let (dead, _dead_guard) = spawn_daemon(Some(&hub_addr), Some("anchor"));

    hub.exchange_beacons();
    assert!(wait_until(
        || {
            dead.exchange_beacons();
            hub.peer_ids().len() == 2
        },
        50
    ));

    drop(_dead_guard);
    drop(dead);

    // Two candidates (hub + the dead anchor), 2-of-2 so both are selected.
    // The anchor cannot be replaced: no spare pool exists.
    let err = hub
        .run_network_task(
            b"lonely payload",
            QuorumConfig::new(2, 2, TierLevel::BestEffort),
        )
        .expect_err("a dead member with no spares must fail the round");
    assert!(matches!(
        err,
        tpt_mosaic_core::MosaicError::QuorumNotMet {
            got: 1,
            required: 2,
            ..
        }
    ));
}

#[test]
fn workers_are_paid_and_reputed_by_the_coordinator() {
    let (hub, _hub_guard) = spawn_daemon(None, None);
    let hub_addr = hub.mesh_addr().expect("hub mesh addr");
    let (worker, _worker_guard) = spawn_daemon(Some(&hub_addr), None);

    hub.exchange_beacons();
    assert!(wait_until(
        || {
            worker.exchange_beacons();
            hub.peer_ids().len() == 2
        },
        50
    ));

    let receipt = hub
        .run_network_task(
            b"payment payload",
            QuorumConfig::new(2, 2, TierLevel::BestEffort),
        )
        .expect("2-of-2 quorum must be met");
    assert_eq!(receipt.confirmations, 2);

    // Both contributors are credited on the coordinator's ledger with the
    // same per-node reward, and both gain reputation with it.
    assert_eq!(hub.balance_of(worker.node_id()), receipt.reward);
    assert_eq!(hub.balance_of(hub.node_id()), receipt.reward);
    assert!(
        hub.reputation_of(worker.node_id()) > tpt_mosaic_economy::reputation::DEFAULT_SCORE,
        "the remote worker's reputation must rise"
    );
    assert!(
        hub.reputation_of(hub.node_id()) > tpt_mosaic_economy::reputation::DEFAULT_SCORE,
        "the coordinator's own reputation must rise"
    );
}

#[test]
fn ledger_and_reputation_persist_across_restarts() {
    let dir = std::env::temp_dir().join(format!("mosaic-econ-{}", std::process::id()));
    let state = dir.join("ledger.bin");
    let rep = dir.join("reputation.bin");
    let config = NodeConfig::from_toml_str(&format!(
        "[node]\nid = \"00112233445566778899aabbccddeeff\"\n\n[mesh]\nlisten_port = 0\n\n[economy]\nstate_file = {:?}\nreputation_file = {:?}",
        state, rep
    ))
    .expect("valid economy config");

    let worker_id;
    let paid;
    {
        let (hub, _hub_guard) = {
            let daemon = Arc::new(NodeDaemon::new(config.clone()));
            let guard = daemon.start_mesh().expect("mesh bound");
            (daemon, guard)
        };
        let hub_addr = hub.mesh_addr().expect("hub mesh addr");
        let (worker, _worker_guard) = spawn_daemon(Some(&hub_addr), None);
        hub.exchange_beacons();
        assert!(wait_until(
            || {
                worker.exchange_beacons();
                hub.peer_ids().len() == 2
            },
            50
        ));
        let receipt = hub
            .run_network_task(
                b"persisted payload",
                QuorumConfig::new(2, 2, TierLevel::BestEffort),
            )
            .expect("quorum must be met");
        worker_id = worker.node_id();
        paid = receipt.reward;
        assert_eq!(hub.balance_of(worker_id), paid);
    }
    // Everything is dropped: sockets closed, in-memory state gone.

    let restarted = Arc::new(NodeDaemon::new(config));
    assert_eq!(
        restarted.balance_of(worker_id),
        paid,
        "ledger must be restored from the state file"
    );
    assert!(
        restarted.reputation_of(worker_id) > tpt_mosaic_economy::reputation::DEFAULT_SCORE,
        "reputation must be restored from the state file"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn gossip_propagates_peers_beyond_static_seeds() {
    // Star topology: both workers seed ONLY the hub; they never list each
    // other. The hub's gossip replies must teach them about one another.
    let (hub, _hub_guard) = spawn_daemon(None, None);
    let hub_addr = hub.mesh_addr().expect("hub mesh addr");
    let (w1, _g1) = spawn_daemon(Some(&hub_addr), None);
    let (w2, _g2) = spawn_daemon(Some(&hub_addr), None);

    hub.exchange_beacons();
    assert!(
        wait_until(
            || {
                w1.exchange_beacons();
                w2.exchange_beacons();
                w1.peer_ids().contains(&w2.node_id()) && w2.peer_ids().contains(&w1.node_id())
            },
            50
        ),
        "workers must learn each other purely through hub gossip"
    );
    assert_eq!(w1.peer_ids().len(), 3, "w1 knows self + hub + w2");

    // And that enables peer-to-peer quorums with no hub involvement.
    let receipt = w1
        .run_network_task(
            b"gossip payload",
            QuorumConfig::new(2, 2, TierLevel::BestEffort),
        )
        .expect("worker-coordinated quorum must be met");
    assert_eq!(receipt.confirmations, 2);
    assert_eq!(
        receipt.agreed_hash,
        hash_output(b"gossip payload", HashAlgorithm::Blake3)
    );
}
