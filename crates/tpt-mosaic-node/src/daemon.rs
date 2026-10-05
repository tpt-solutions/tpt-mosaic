//! Node daemon: wires discovery, compilation, sandboxed execution, quorum
//! verification, and settlement into a single runtime (spec §8 task lifecycle).
//!
//! Local tasks run shard → compile → sandbox → hash → quorum → settlement.
//! With `[mesh]` configured, the daemon also serves TCP mesh connections,
//! trades beacon exchanges with seeds/peers, and coordinates multi-node
//! quorums: assemble → dispatch `TaskAssignment` → collect `ResultHash` →
//! broadcast cancellation (spec §3.3, §8).

use std::collections::HashMap;
use std::error::Error;
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::watch;
use tokio::time::interval;

use tpt_mosaic_compiler as compiler;
use tpt_mosaic_core::{CapabilityFlags, MosaicError, NodeId, NodeKind, QuorumConfig, TaskId};
use tpt_mosaic_discovery::{
    mdns::MdnsHandle, BeaconBroadcaster, FrameHandler, PeerRecord, PeerTable, TcpMesh,
};
use tpt_mosaic_economy::chains::{BaseSettlement, NearSettlement, SolanaSettlement};
use tpt_mosaic_economy::{calculate_reward, Chain, ReputationStore, Settlement};
use tpt_mosaic_proto::codec::{encode, MAX_GOSSIP_PEERS};
use tpt_mosaic_proto::{
    DhtQuery, HeartbeatBeacon, PeerAdvert, PeerGossip, TaskAssignment, WireMessage,
};
use tpt_mosaic_quorum::{HashCollector, QuorumResult, QuorumState};
use tpt_mosaic_sandbox::{CapabilityGrant, Sandbox, ThermalPolicy};
use tpt_mosaic_scheduler::{BalancedAssembler, DispatchTracker, SchedulerPolicy, StragglerPolicy};
use tpt_mosaic_task::{
    restore_checkpoint, save_checkpoint, split_task, TaskPriority, TaskProgress,
};
use tpt_mosaic_verify::{hash_output, HashAlgorithm};

use crate::config::NodeConfig;
use crate::id;

/// Target shard size for micro-task splitting: 64 KiB ≈ a 50–100 ms chunk on
/// current edge silicon (spec §6.5).
const SHARD_TARGET_BYTES: usize = 64 * 1024;

/// Per-task execution grant handed to the sandbox.
const GRANT_MAX_MEMORY_BYTES: u64 = 256 * 1024 * 1024;
const GRANT_MAX_CPU_MS: u32 = 5_000;

/// Mesh RPC budget: connect/read/write timeout for one frame exchange with a
/// peer. Loopback connects fail in milliseconds; 30 s covers slow handlers.
const MESH_RPC_TIMEOUT: Duration = Duration::from_secs(30);

/// Per-wave budget for one task-assignment dispatch; a member that misses it
/// is a straggler and gets replaced from the spare pool (spec §3.3).
const MESH_DISPATCH_TIMEOUT: Duration = Duration::from_secs(10);

/// Per-peer budget for one beacon exchange. Heartbeats dispatch every peer
/// in parallel, so a full round stays near this bound even when several
/// peers are dead.
const BEACON_EXCHANGE_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a task cancellation notice is honoured: late or reordered
/// assignments for a cancelled task are refused until this expires.
const CANCELLATION_TTL: Duration = Duration::from_secs(300);

/// Upper bound on remembered cancellations, keeping the map bounded no
/// matter how many distinct tasks are cancelled.
const MAX_CANCELLED_TASKS: usize = 1024;

/// How far into the future a frame timestamp may sit before it is rejected
/// (tolerated clock skew between peers).
const CLOCK_SKEW_TOLERANCE_MS: u64 = 60_000;

/// How old a `ResultHash`'s `produced_at_ms` may be: two dispatch waves plus
/// the mesh RPC budget.
const RESULT_MAX_AGE_MS: u64 = 50_000;

/// Recent-nonce memory per peer (and peer-map cap) for beacon replay
/// rejection.
const BEACON_NONCES_PER_PEER: usize = 64;
const MAX_NONCE_PEERS: usize = 512;

/// Cumulative daemon counters.
#[derive(Debug, Default)]
pub struct Stats {
    heartbeats_sent: AtomicU64,
    tasks_completed: AtomicU64,
    tasks_failed: AtomicU64,
}

impl Stats {
    /// Heartbeats broadcast since startup.
    pub fn heartbeats_sent(&self) -> u64 {
        self.heartbeats_sent.load(Ordering::Relaxed)
    }

    /// Tasks whose quorum was met.
    pub fn tasks_completed(&self) -> u64 {
        self.tasks_completed.load(Ordering::Relaxed)
    }

    /// Tasks that failed (timeout, divergence, execution error).
    pub fn tasks_failed(&self) -> u64 {
        self.tasks_failed.load(Ordering::Relaxed)
    }
}

/// Point-in-time snapshot of [`Stats`] counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Heartbeats broadcast since startup.
    pub heartbeats_sent: u64,
    /// Tasks whose quorum was met.
    pub tasks_completed: u64,
    /// Tasks that failed.
    pub tasks_failed: u64,
}

/// Receipt returned when a task's quorum is met.
#[derive(Debug, Clone)]
pub struct TaskReceipt {
    /// Identifier of the completed task.
    pub task_id: TaskId,
    /// Hash agreed upon by the quorum.
    pub agreed_hash: [u8; 32],
    /// Nodes that contributed the winning hash (including this one).
    pub confirmations: u8,
    /// Micro-reward credited for the contribution.
    pub reward: u64,
    /// Micro-task shards the payload was split into.
    pub shards: u32,
}

/// The node daemon.
pub struct NodeDaemon {
    config: NodeConfig,
    self_id: NodeId,
    peers: Arc<Mutex<PeerTable>>,
    /// Mesh listener bound at construction (when `[mesh]` is configured), so
    /// the mesh address is known before `run` starts serving.
    mesh_listener: Mutex<Option<TcpListener>>,
    /// The bound listener's address, captured at construction so it stays
    /// available after `start_mesh` takes the listener.
    mesh_addr: Option<SocketAddr>,
    /// Compiled-artifact cache, when `[compiler] cache_dir` is configured.
    jit: Option<compiler::JitCache>,
    /// Checkpoint directory, when `[task] checkpoint_dir` is configured.
    checkpoints: Option<std::path::PathBuf>,
    /// LAN mDNS advertiser/browser, when `[mesh] mdns` is enabled.
    mdns: Option<MdnsHandle>,
    broadcaster: TracingBroadcaster,
    sandbox: Sandbox,
    reputation: Arc<Mutex<ReputationStore>>,
    settlement: Arc<dyn Settlement>,
    stats: Arc<Stats>,
    /// Tasks this node has been told to cancel, with the time of the notice.
    cancelled: Mutex<HashMap<TaskId, Instant>>,
    /// When set, peer reputation scores are loaded from and saved to this file.
    reputation_file: Option<std::path::PathBuf>,
    /// Ed25519 identity backing `self_id`. `None` only when `node.id` is
    /// explicitly configured: frames are then sent unsigned and inbound
    /// authentication is skipped (legacy compatibility mode).
    identity: Option<id::Identity>,
    /// Monotonic beacon-nonce counter, randomly seeded at startup.
    beacon_nonce: AtomicU64,
    /// Recent beacon nonces per node, for replay rejection.
    seen_nonces: Mutex<HashMap<NodeId, Vec<u64>>>,
}

/// Keeps the mesh accept-loop alive; stops it on drop.
pub struct MeshGuard {
    running: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Drop for MeshGuard {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl NodeDaemon {
    /// Assemble the daemon from parsed configuration.
    ///
    /// Generates a random node identity when the config omits one, and binds
    /// the mesh listener (when `[mesh]` is configured) so
    /// [`NodeDaemon::mesh_addr`] is known before [`NodeDaemon::run`] starts.
    pub fn new(config: NodeConfig) -> Self {
        // Identity: an Ed25519 keypair whose pubkey hash is the node id,
        // persisted as a seed when `[node] state_file` is configured. An
        // explicitly configured `node.id` overrides the derived id and
        // downgrades to unsigned frames (the key then cannot bind to the id).
        let (self_id, identity) = match config.identity.id {
            Some(explicit) => {
                tracing::warn!(
                    "node.id is explicitly configured; mesh frames are sent unsigned \
                     and inbound signature verification is disabled"
                );
                (explicit, None)
            }
            None => {
                let identity = match &config.identity.state_file {
                    Some(path) => id::Identity::load_or_create(path).unwrap_or_else(|e| {
                        tracing::warn!(
                            path = %path.display(),
                            error = %e,
                            "identity state file unusable; using an ephemeral keypair"
                        );
                        id::Identity::generate()
                    }),
                    None => id::Identity::generate(),
                };
                (identity.node_id(), Some(identity))
            }
        };
        let peer_max_age = config.discovery.peer_max_age;
        let jit = config.jit_cache.as_deref().map(compiler::JitCache::new);
        let checkpoints = config.checkpoint_dir.clone();
        let mesh_listener = match config.mesh.listen {
            Some(addr) => match TcpListener::bind(addr) {
                Ok(listener) => {
                    let bound = listener.local_addr().ok();
                    (Some(listener), bound)
                }
                Err(e) => {
                    tracing::warn!(addr = %addr, error = %e, "mesh bind failed; mesh disabled");
                    (None, None)
                }
            },
            None => (None, None),
        };
        let (mesh_listener, mesh_addr) = mesh_listener;
        let mdns = match (mesh_addr, config.mesh.mdns) {
            (Some(addr), true) => match MdnsHandle::spawn(self_id, addr.port()) {
                Ok(handle) => {
                    tracing::info!("mDNS discovery active");
                    Some(handle)
                }
                Err(e) => {
                    tracing::warn!(error = %e, "mDNS start failed; using static seeds only");
                    None
                }
            },
            _ => None,
        };
        let settlement = make_settlement(
            config.chain,
            config.rpc_url.as_deref(),
            config.economy_state_file.as_deref(),
        );
        let reputation_path = config.reputation_file.clone();
        let reputation = match &reputation_path {
            Some(path) => match ReputationStore::load(path) {
                Ok(store) => {
                    tracing::info!(path = %path.display(), nodes = store.len(), "reputation restored");
                    store
                }
                Err(e) => {
                    tracing::warn!(path = %path.display(), error = %e, "reputation file unreadable; starting empty");
                    ReputationStore::new()
                }
            },
            None => ReputationStore::new(),
        };
        Self {
            config,
            self_id,
            peers: Arc::new(Mutex::new(PeerTable::new(peer_max_age))),
            mesh_listener: Mutex::new(mesh_listener),
            mesh_addr,
            jit,
            checkpoints,
            mdns,
            broadcaster: TracingBroadcaster,
            sandbox: Sandbox::new(ThermalPolicy::default()),
            reputation: Arc::new(Mutex::new(reputation)),
            settlement,
            stats: Arc::new(Stats::default()),
            cancelled: Mutex::new(HashMap::new()),
            reputation_file: reputation_path,
            identity,
            beacon_nonce: AtomicU64::new({
                let mut seed = [0u8; 8];
                getrandom::fill(&mut seed).expect("OS entropy source is unavailable");
                u64::from_le_bytes(seed)
            }),
            seen_nonces: Mutex::new(HashMap::new()),
        }
    }

    /// This node's mesh listen address, when mesh networking is enabled.
    ///
    /// Captured at bind time, so it stays valid after [`NodeDaemon::start_mesh`]
    /// takes ownership of the listener.
    pub fn mesh_addr(&self) -> Option<SocketAddr> {
        self.mesh_addr
    }

    /// Start serving mesh connections until the returned guard is dropped.
    pub fn start_mesh(self: &Arc<Self>) -> Option<MeshGuard> {
        let listener = lock_ok(&self.mesh_listener).take()?;
        let running = Arc::new(AtomicBool::new(true));
        let handler: FrameHandler = {
            let daemon = Arc::clone(self);
            Arc::new(move |msg| daemon.handle_mesh_frame(msg))
        };
        let handle = TcpMesh::serve(listener, handler, Arc::clone(&running));
        Some(MeshGuard {
            running,
            handle: Some(handle),
        })
    }

    /// This node's identity.
    pub fn node_id(&self) -> NodeId {
        self.self_id
    }

    /// This node's kind (edge tile or anchor).
    pub fn node_kind(&self) -> NodeKind {
        self.config.identity.kind
    }

    /// IDs of live peers currently in the table (including this node).
    pub fn peer_ids(&self) -> Vec<NodeId> {
        self.peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .live_peers()
            .map(|p| p.node_id)
            .collect()
    }

    /// Balance this node's settlement ledger currently credits to `node_id`.
    // Used by the mesh integration tests; will back the Phase-15 status API.
    #[allow(dead_code)]
    pub fn balance_of(&self, node_id: NodeId) -> u64 {
        self.settlement.balance(node_id).unwrap_or(0)
    }

    /// Reputation score this node currently assigns to `node_id`.
    // Used by the mesh integration tests; will back the Phase-15 status API.
    #[allow(dead_code)]
    pub fn reputation_of(&self, node_id: NodeId) -> f32 {
        lock_ok(&self.reputation).score(node_id)
    }

    /// Snapshot of the daemon counters.
    pub fn stats(&self) -> StatsSnapshot {
        StatsSnapshot {
            heartbeats_sent: self.stats.heartbeats_sent(),
            tasks_completed: self.stats.tasks_completed(),
            tasks_failed: self.stats.tasks_failed(),
        }
    }

    /// Run the local-only task lifecycle (spec §8, single node): shard →
    /// compile → sandboxed execute → hash, then submit into the quorum
    /// collector.
    ///
    /// Only this node participates, so quorums above 1-of-1 fail with
    /// [`MosaicError::QuorumNotMet`]. For multi-node quorums use
    /// [`NodeDaemon::run_network_task`].
    pub fn run_local_task(
        &self,
        payload: &[u8],
        quorum: QuorumConfig,
    ) -> Result<TaskReceipt, MosaicError> {
        let task_id = id::generate_task_id();
        tracing::info!(task = %id::to_hex(task_id.as_bytes()), shards_target = SHARD_TARGET_BYTES, "task accepted");

        let (hash, shards) = self.execute_locally(task_id, payload)?;

        // Submit to the quorum collector.
        let mut collector = HashCollector::new(task_id, quorum)?;
        collector.submit(self.self_id, hash);
        self.finish_task(task_id, quorum, &collector, 1, shards)
    }

    /// Execute a task on this node: shard the payload, compile every shard for
    /// the local hardware, run it in the sandbox, and BLAKE3 the combined
    /// output. Returns the digest and the number of shards executed. Shared
    /// by the local and mesh-coordinated paths.
    fn execute_locally(
        &self,
        task_id: TaskId,
        payload: &[u8],
    ) -> Result<([u8; 32], u32), MosaicError> {
        let task_hex = id::to_hex(task_id.as_bytes());
        let shards = split_task(task_id, payload, TaskPriority::Standard, SHARD_TARGET_BYTES)?;
        let grant = CapabilityGrant::new(task_id, GRANT_MAX_MEMORY_BYTES, GRANT_MAX_CPU_MS);

        // Checkpoints are keyed by the task id (unique per submission), with
        // the (payload, hardware) fingerprint as a nonce binding the file to
        // this exact workload — two tasks can never share or clobber each
        // other's resume state. The blob carries the output produced so far,
        // so a resumed run hashes the full output and votes the same digest
        // as an uninterrupted run.
        let cp_path = self.checkpoints.as_ref().map(|dir| {
            dir.join(format!(
                "{}-{}.cp",
                id::to_hex(task_id.as_bytes()),
                id::to_hex(&compiler::fingerprint(payload, &self.config.hardware))
            ))
        });
        let fresh = || TaskProgress::new(task_id, shards.len() as u32);
        let (mut progress, mut output) = match cp_path.as_deref().map(restore_checkpoint) {
            Some(Ok(cp)) if cp.task_id == task_id => {
                match TaskProgress::from_checkpoint(&cp, shards.len() as u32) {
                    Ok(progress) => {
                        if progress.completed() > 0 {
                            tracing::info!(
                                task = %task_hex,
                                resumed_at = progress.completed(),
                                "resuming interrupted task from checkpoint"
                            );
                        }
                        (progress, cp.state_blob)
                    }
                    Err(_) => (fresh(), Vec::new()),
                }
            }
            _ => (fresh(), Vec::new()),
        };

        for shard in &shards[progress.completed() as usize..] {
            let compiled = match &self.jit {
                Some(cache) => cache.compile_cached(&shard.payload, &self.config.hardware)?,
                None => compiler::compile(&shard.payload, &self.config.hardware)?,
            };
            let produced = self.sandbox.execute(&grant, &compiled)?;
            output.extend_from_slice(&produced);
            progress.advance();
            if let Some(path) = &cp_path {
                if let Err(e) = save_checkpoint(path, &progress.checkpoint(output.clone())) {
                    tracing::warn!(task = %task_hex, error = %e, "checkpoint write failed");
                }
            }
        }
        if let Some(path) = &cp_path {
            match std::fs::remove_file(path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    tracing::warn!(task = %task_hex, error = %e, "checkpoint cleanup failed")
                }
            }
        }
        Ok((
            hash_output(&output, HashAlgorithm::Blake3),
            shards.len() as u32,
        ))
    }

    /// Settle a finished quorum round: reward + reputation on success,
    /// failure accounting otherwise.
    fn finish_task(
        &self,
        task_id: TaskId,
        quorum: QuorumConfig,
        collector: &HashCollector,
        got: u32,
        shards: u32,
    ) -> Result<TaskReceipt, MosaicError> {
        let task_hex = id::to_hex(task_id.as_bytes());
        match collector.state() {
            QuorumState::Finished(QuorumResult::Met {
                agreed_hash,
                confirmations,
            }) => {
                let reward = calculate_reward(&quorum, self.config.capabilities, shards);
                // Pay every contributor that voted for the winning hash —
                // the coordinator is just one member of the quorum.
                let voters: Vec<NodeId> = collector.voters_for(agreed_hash).to_vec();
                for voter in &voters {
                    self.settlement.submit_reward(*voter, reward, &task_hex)?;
                }
                {
                    let mut reputation = lock_ok(&self.reputation);
                    for voter in &voters {
                        reputation.record_success(*voter);
                    }
                    for suspect in collector.suspected_byzantine() {
                        reputation.record_failure(*suspect);
                    }
                }
                self.persist_reputation();
                self.stats.tasks_completed.fetch_add(1, Ordering::Relaxed);
                tracing::info!(
                    task = %task_hex,
                    confirmations,
                    reward,
                    paid = voters.len(),
                    "quorum met"
                );
                Ok(TaskReceipt {
                    task_id,
                    agreed_hash: *agreed_hash,
                    confirmations: *confirmations,
                    reward,
                    shards,
                })
            }
            _ => {
                lock_ok(&self.reputation).record_failure(self.self_id);
                self.persist_reputation();
                self.stats.tasks_failed.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(task = %task_hex, required = quorum.k, got, "quorum not met");
                Err(MosaicError::QuorumNotMet {
                    task_id,
                    got: got.min(u8::MAX as u32) as u8,
                    required: quorum.k,
                })
            }
        }
    }

    /// Best-effort persistence of the reputation store when
    /// `[economy] reputation_file` is configured.
    fn persist_reputation(&self) {
        if let Some(path) = &self.reputation_file {
            if let Err(e) = lock_ok(&self.reputation).save(path) {
                tracing::warn!(path = %path.display(), error = %e, "reputation persistence failed");
            }
        }
    }

    /// Coordinate a multi-node quorum over the mesh (spec §8, full flow):
    /// assemble a heterogeneous quorum from the live peer table, dispatch
    /// `TaskAssignment` frames, collect `ResultHash` replies into the quorum
    /// collector, and broadcast cancellation once the threshold is met.
    ///
    /// Requires mesh networking and enough live peers to satisfy the quorum's
    /// `n`; otherwise the scheduler's assembler rejects the round.
    pub fn run_network_task(
        &self,
        payload: &[u8],
        quorum: QuorumConfig,
    ) -> Result<TaskReceipt, MosaicError> {
        let coordinator = self
            .mesh_addr()
            .ok_or(MosaicError::NodeUnavailable(self.self_id))?;
        let task_id = id::generate_task_id();
        let task_hex = id::to_hex(task_id.as_bytes());
        tracing::info!(task = %task_hex, required = quorum.k, of = quorum.n, "mesh task accepted");

        // Assemble from every live table entry — including this node, which
        // participates as one of the n contributors.
        let candidates: Vec<PeerRecord> = self
            .peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .live_peers()
            .cloned()
            .collect();
        // Peers slashed below the reputation threshold are excluded from
        // assembly (the coordinator always keeps its own slot: it must stay
        // able to contribute its vote).
        let candidates: Vec<PeerRecord> = {
            let reputation = lock_ok(&self.reputation);
            candidates
                .into_iter()
                .filter(|p| p.node_id == self.self_id || !reputation.should_slash(p.node_id))
                .collect()
        };
        let selected = BalancedAssembler::default().assemble(
            &candidates,
            &quorum,
            CapabilityFlags::empty(),
            &|id| lock_ok(&self.reputation).score(id),
        )?;

        // Only assembled candidates (quorum members and spares) may vote.
        let mut collector =
            HashCollector::new(task_id, quorum)?.with_members(candidates.iter().map(|p| p.node_id));
        let mut got: u32 = 0;
        let mut replied: Vec<NodeId> = Vec::new();
        // Every node splits the same payload deterministically, so the shard
        // count (and thus the work-based reward) is computable locally.
        let shards = payload.len().div_ceil(SHARD_TARGET_BYTES) as u32;

        // Own contribution first: it is local and cannot time out.
        if selected.contains(&self.self_id) {
            let (hash, _) = self.execute_locally(task_id, payload)?;
            collector.submit(self.self_id, hash);
            got += 1;
        }

        // Dispatch targets: quorum members first, then spare candidates for
        // straggler replacement (spec §3.3/§6.4: a missing member is replaced
        // 1:1 while total contributors stay within the quorum's n).
        let peers = lock_ok(&self.peers);
        let mut pending: Vec<(NodeId, SocketAddr)> = selected
            .iter()
            .filter(|node| **node != self.self_id)
            .filter_map(|node| {
                peers
                    .get(node)
                    .and_then(|p| p.addr)
                    .map(|addr| (*node, addr))
            })
            .collect();
        let mut spares: Vec<(NodeId, SocketAddr)> = candidates
            .iter()
            .map(|p| p.node_id)
            .filter(|node| !selected.contains(node))
            .filter_map(|node| {
                if node == self.self_id {
                    None
                } else {
                    peers
                        .get(&node)
                        .and_then(|p| p.addr)
                        .map(|addr| (node, addr))
                }
            })
            .collect();
        drop(peers);
        // The coordinator itself is also a valid spare when it was not
        // selected (its hash is computed locally, no RPC needed).
        let mut self_is_spare = !selected.contains(&self.self_id);

        let policy = StragglerPolicy {
            dispatch_timeout: MESH_DISPATCH_TIMEOUT,
        };
        let mut tracker = DispatchTracker::new();
        let mut dispatched: Vec<(NodeId, SocketAddr)> = Vec::new();
        // Sign the assignment once; every wave dispatches a clone of the
        // signed frame.
        let mut assignment_frame = WireMessage::TaskAssignment(TaskAssignment {
            task_id,
            quorum_config: quorum,
            payload: payload.to_vec(),
            deadline_ms: now_ms() + MESH_DISPATCH_TIMEOUT.as_millis() as u64,
            coordinator,
            pubkey: [0; 32],
            signature: [0; 64],
        });
        self.auth_wire(&mut assignment_frame);

        // Up to three waves: dispatch, collect, replace the stragglers.
        for _wave in 0..3 {
            if pending.is_empty() {
                break;
            }
            for (node, _) in &pending {
                tracker.track(*node, policy.deadline_from(Instant::now()));
            }
            dispatched.append(&mut pending.clone());

            let handles: Vec<_> = pending
                .iter()
                .map(|(_, addr)| {
                    let frame = assignment_frame.clone();
                    let addr = *addr;
                    std::thread::spawn(move || {
                        TcpMesh::exchange(addr, &frame, MESH_DISPATCH_TIMEOUT).and_then(|reply| {
                            match reply {
                                WireMessage::ResultHash(rh) => Some(rh),
                                _ => None,
                            }
                        })
                    })
                })
                .collect();

            let mut missing: Vec<NodeId> = Vec::new();
            for ((node, _), handle) in pending.drain(..).zip(handles) {
                match handle.join() {
                    // A reply only counts if it answers this task, comes from
                    // the node that was dialed, carries a valid signature
                    // binding it to that node, and is fresh; anything else is
                    // spoofed, stale, or replayed and counts as missing.
                    Ok(Some(rh)) if rh.task_id == task_id && rh.node_id == node => {
                        if let Err(reason) = self.verify_wire(&WireMessage::ResultHash(rh)) {
                            tracing::warn!(
                                peer = %id::to_hex(node.as_bytes()),
                                reason,
                                "rejected unauthenticated result hash"
                            );
                            missing.push(node);
                        } else if !result_is_fresh(rh.produced_at_ms) {
                            tracing::warn!(
                                peer = %id::to_hex(node.as_bytes()),
                                "rejected result hash with a stale timestamp"
                            );
                            missing.push(node);
                        } else {
                            collector.submit(node, rh.hash);
                            replied.push(node);
                            got += 1;
                            tracker.complete(node);
                        }
                    }
                    _ => missing.push(node),
                }
            }

            if matches!(
                collector.state(),
                QuorumState::Finished(QuorumResult::Met { .. })
            ) {
                break;
            }

            // Replacement budget: the policy keeps totals within n; never
            // exceed the k confirmations still missing.
            let replacements = policy
                .replacements_needed(&quorum, tracker.outstanding(), missing.len())
                .min(quorum.n.saturating_sub(got as u8) as usize);
            if replacements == 0 {
                break;
            }

            for _ in 0..replacements {
                if self_is_spare {
                    // The coordinator was a spare: contribute locally.
                    self_is_spare = false;
                    let (hash, _) = self.execute_locally(task_id, payload)?;
                    collector.submit(self.self_id, hash);
                    replied.push(self.self_id);
                    got += 1;
                } else if let Some(spare) = spares.pop() {
                    // Resolved stragglers free their slot for the replacement.
                    for node in &missing {
                        tracker.complete(*node);
                    }
                    missing.clear();
                    tracker.track(spare.0, policy.deadline_from(Instant::now()));
                    pending.push(spare);
                } else {
                    break;
                }
            }
        }

        let outcome = self.finish_task(task_id, quorum, &collector, got, shards);

        // Early termination (spec §3.3) and timeout teardown: every
        // dispatched worker that had not contributed is told to stop —
        // `QuorumMet` when the threshold was reached, `Timeout` when the
        // round failed without a diverged vote (a diverged round has every
        // member voted already, so there is nothing left to cancel).
        // Fire-and-forget.
        if outcome.is_ok() {
            collector.timeout(); // no-op on an already-finished round
        }
        if let Some(cancellation) = collector.cancellation_signal() {
            let frame = WireMessage::CancellationSignal(cancellation);
            for (node, addr) in &dispatched {
                if !replied.contains(node) {
                    TcpMesh::send(*addr, &frame, MESH_RPC_TIMEOUT);
                }
            }
        }

        outcome
    }

    /// Mesh frame handler: process one inbound frame, optionally replying.
    fn handle_mesh_frame(&self, msg: &WireMessage) -> Option<WireMessage> {
        match msg {
            WireMessage::HeartbeatBeacon(beacon) => {
                if let Err(reason) = self.verify_wire(msg) {
                    tracing::warn!(
                        peer = %id::to_hex(beacon.node_id.as_bytes()),
                        reason,
                        "rejected unauthenticated beacon"
                    );
                    return None;
                }
                if !self.beacon_is_fresh(beacon) {
                    tracing::debug!(
                        peer = %id::to_hex(beacon.node_id.as_bytes()),
                        "rejected stale or replayed beacon"
                    );
                    return None;
                }
                self.upsert_advert(PeerAdvert {
                    node_id: beacon.node_id,
                    addr: beacon.addr,
                    hardware: beacon.hardware,
                    capabilities: beacon.capabilities,
                });
                tracing::debug!(peer = %id::to_hex(beacon.node_id.as_bytes()), "mesh beacon received");
                // The reply is a full peer-table snapshot (gossip): one static
                // seed therefore propagates the whole view (spec §4).
                Some(WireMessage::PeerGossip(self.gossip_snapshot()))
            }
            WireMessage::TaskAssignment(assignment) => {
                // Execute inline on the connection thread; the stub workload
                // path is fast, and the coordinator's read timeout bounds us.
                let task_hex = id::to_hex(assignment.task_id.as_bytes());
                if let Err(reason) = self.verify_wire(msg) {
                    tracing::warn!(task = %task_hex, reason, "rejected unauthenticated assignment");
                    return None;
                }
                if self.is_cancelled(assignment.task_id) {
                    tracing::info!(task = %task_hex, "mesh assignment refused: task cancelled");
                    return None;
                }
                if now_ms() > assignment.deadline_ms {
                    tracing::warn!(task = %task_hex, "mesh assignment refused: deadline passed");
                    return None;
                }
                tracing::info!(task = %task_hex, "mesh assignment received");
                match self.execute_locally(assignment.task_id, &assignment.payload) {
                    Ok((hash, _shards)) => {
                        let mut reply = WireMessage::ResultHash(tpt_mosaic_proto::ResultHash {
                            task_id: assignment.task_id,
                            node_id: self.self_id,
                            hash,
                            produced_at_ms: now_ms(),
                            pubkey: [0; 32],
                            signature: [0; 64],
                        });
                        self.auth_wire(&mut reply);
                        Some(reply)
                    }
                    Err(e) => {
                        tracing::warn!(task = %task_hex, error = %e, "mesh assignment failed");
                        None
                    }
                }
            }
            WireMessage::CancellationSignal(signal) => {
                tracing::info!(
                    task = %id::to_hex(signal.task_id.as_bytes()),
                    reason = ?signal.reason,
                    "mesh cancellation received"
                );
                self.record_cancellation(signal.task_id);
                None
            }
            WireMessage::DhtQuery(query) => {
                // Mesh directory lookup (spec §6.3): reply with the matching
                // slice of our peer table, ourselves included when we match.
                tracing::debug!(
                    limit = query.limit,
                    filter = ?query.capability_filter,
                    "mesh dht query received"
                );
                Some(WireMessage::PeerGossip(
                    self.gossip_snapshot_filtered(query),
                ))
            }
            WireMessage::ResultHash(_) | WireMessage::PeerGossip(_) => None,
        }
    }

    /// Record a cancellation notice for `task_id`, honouring it for
    /// [`CANCELLATION_TTL`] so a late or reordered assignment is still
    /// refused. The map is pruned and capped on every touch.
    fn record_cancellation(&self, task_id: TaskId) {
        let mut cancelled = lock_ok(&self.cancelled);
        prune_cancelled(&mut cancelled);
        if cancelled.len() >= MAX_CANCELLED_TASKS {
            cancelled.clear();
        }
        cancelled.insert(task_id, Instant::now());
    }

    /// Returns `true` while a cancellation notice for `task_id` is current.
    fn is_cancelled(&self, task_id: TaskId) -> bool {
        let mut cancelled = lock_ok(&self.cancelled);
        prune_cancelled(&mut cancelled);
        cancelled.contains_key(&task_id)
    }

    /// Fill the authentication fields of an outgoing frame and sign it. In
    /// unsigned mode (explicit `node.id`), the fields stay zeroed.
    fn auth_wire(&self, msg: &mut WireMessage) {
        let Some(identity) = &self.identity else {
            return;
        };
        let pubkey = identity.pubkey();
        match msg {
            WireMessage::HeartbeatBeacon(beacon) => beacon.pubkey = pubkey,
            WireMessage::TaskAssignment(assignment) => assignment.pubkey = pubkey,
            WireMessage::ResultHash(result) => result.pubkey = pubkey,
            _ => return,
        }
        zero_signature(msg);
        let signature = identity.sign(&encode(msg));
        if let Some(field) = signature_field_mut(msg) {
            *field = signature.to_bytes();
        }
    }

    /// Authenticate an inbound frame: the pubkey must hash to the claimed
    /// node id (assignments carry no node id, so there only the signature's
    /// proof-of-possession is checked) and the Ed25519 signature must verify
    /// over the frame with the signature field zeroed. Unsigned mode accepts
    /// every frame; signed mode rejects zeroed auth fields (an all-zero
    /// pubkey never hashes to a real node id).
    fn verify_wire(&self, msg: &WireMessage) -> Result<(), &'static str> {
        let Some(_) = &self.identity else {
            return Ok(());
        };
        let (claimed_id, pubkey, signature) = match msg {
            WireMessage::HeartbeatBeacon(beacon) => {
                (Some(beacon.node_id), beacon.pubkey, beacon.signature)
            }
            WireMessage::ResultHash(result) => {
                (Some(result.node_id), result.pubkey, result.signature)
            }
            WireMessage::TaskAssignment(assignment) => {
                (None, assignment.pubkey, assignment.signature)
            }
            _ => return Ok(()),
        };
        if let Some(node_id) = claimed_id {
            if id::node_id_for_pubkey(&pubkey) != node_id {
                return Err("pubkey does not hash to the claimed node id");
            }
        }
        let mut unsigned = msg.clone();
        zero_signature(&mut unsigned);
        if !id::Identity::verify(&pubkey, &encode(&unsigned), &signature) {
            return Err("signature does not verify");
        }
        Ok(())
    }

    /// Beacon freshness: the timestamp must sit inside the acceptance window
    /// (four heartbeat intervals, floored at 30 s, plus tolerated skew) and
    /// the nonce must be one this peer has not used recently.
    fn beacon_is_fresh(&self, beacon: &HeartbeatBeacon) -> bool {
        let now = now_ms();
        let max_age = (self.config.discovery.heartbeat_interval.as_millis() as u64 * 4).max(30_000);
        if beacon.timestamp_ms + max_age < now
            || beacon.timestamp_ms > now + CLOCK_SKEW_TOLERANCE_MS
        {
            return false;
        }
        let mut seen = lock_ok(&self.seen_nonces);
        if seen.len() >= MAX_NONCE_PEERS && !seen.contains_key(&beacon.node_id) {
            seen.clear();
        }
        let nonces = seen.entry(beacon.node_id).or_default();
        if nonces.contains(&beacon.nonce) {
            return false;
        }
        if nonces.len() >= BEACON_NONCES_PER_PEER {
            nonces.remove(0);
        }
        nonces.push(beacon.nonce);
        true
    }

    /// Insert or refresh a peer record from an advertisement.
    ///
    /// Gossiped data is sanitised before it enters the table: our own id is
    /// never accepted (a peer's view of us must not overwrite our record),
    /// and addresses that cannot identify a peer (unspecified, broadcast,
    /// multicast) are dropped.
    fn upsert_advert(&self, advert: PeerAdvert) {
        if advert.node_id == self.self_id {
            tracing::debug!("ignoring gossiped advert for ourselves");
            return;
        }
        if let Some(addr) = advert.addr {
            if !plausible_addr(addr) {
                tracing::debug!(peer = %id::to_hex(advert.node_id.as_bytes()), %addr, "ignoring advert with implausible address");
                return;
            }
        }
        self.peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .upsert(PeerRecord {
                node_id: advert.node_id,
                hardware: advert.hardware,
                capabilities: advert.capabilities,
                addr: advert.addr,
                last_seen: Instant::now(),
            });
    }

    /// Build a gossip snapshot: ourselves plus the freshest live peers,
    /// capped at [`MAX_GOSSIP_PEERS`] entries.
    fn gossip_snapshot(&self) -> PeerGossip {
        self.gossip_snapshot_filtered(&DhtQuery {
            key: [0; 32],
            limit: MAX_GOSSIP_PEERS as u16,
            capability_filter: CapabilityFlags::empty(),
        })
    }

    /// Build a gossip snapshot restricted to `query`'s capability filter and
    /// limit — the reply side of a mesh [`DhtQuery`].
    fn gossip_snapshot_filtered(&self, query: &DhtQuery) -> PeerGossip {
        let mut peers: Vec<PeerAdvert> = Vec::new();
        let consider = |peers: &mut Vec<PeerAdvert>, advert: PeerAdvert| {
            if peers.len() < query.limit as usize
                && peers.len() < MAX_GOSSIP_PEERS
                && advert.capabilities.contains(query.capability_filter)
                && advert.hardware.is_available()
            {
                peers.push(advert);
            }
        };

        let beacon = self.current_beacon();
        consider(
            &mut peers,
            PeerAdvert {
                node_id: beacon.node_id,
                addr: beacon.addr,
                hardware: beacon.hardware,
                capabilities: beacon.capabilities,
            },
        );
        let table = lock_ok(&self.peers);
        for peer in table.live_peers() {
            if peer.node_id == self.self_id {
                continue;
            }
            consider(
                &mut peers,
                PeerAdvert {
                    node_id: peer.node_id,
                    addr: peer.addr,
                    hardware: peer.hardware,
                    capabilities: peer.capabilities,
                },
            );
        }
        PeerGossip { peers }
    }

    /// Beacon exchange: refresh our own record, then trade beacons with every
    /// configured seed and every live peer that has a mesh address. The
    /// responder's gossip reply (its own record plus its known peers) is
    /// upserted into the table, so one static seed propagates the full view.
    /// Targets are contacted in parallel, each bounded by the 5 s
    /// `BEACON_EXCHANGE_TIMEOUT`, so dead peers cannot stretch the round.
    pub fn exchange_beacons(&self) {
        let beacon = self.current_beacon();
        self.broadcaster.broadcast(&beacon).ok();
        self.stats.heartbeats_sent.fetch_add(1, Ordering::Relaxed);
        self.refresh_self(&beacon);

        let peers = lock_ok(&self.peers);
        let mut targets: Vec<SocketAddr> = self.config.mesh.seeds.clone();
        if let Some(mdns) = &self.mdns {
            for addr in mdns.targets() {
                if !targets.contains(&addr) {
                    targets.push(addr);
                }
            }
        }
        for peer in peers.live_peers() {
            if peer.node_id != self.self_id {
                if let Some(addr) = peer.addr {
                    if !targets.contains(&addr) {
                        targets.push(addr);
                    }
                }
            }
        }
        drop(peers);

        let handles: Vec<_> = targets
            .into_iter()
            .map(|addr| {
                let frame = WireMessage::HeartbeatBeacon(beacon);
                std::thread::spawn(move || {
                    (
                        addr,
                        TcpMesh::exchange(addr, &frame, BEACON_EXCHANGE_TIMEOUT),
                    )
                })
            })
            .collect();
        for handle in handles {
            let Ok((_, reply)) = handle.join() else {
                continue;
            };
            match reply {
                Some(WireMessage::PeerGossip(gossip)) => {
                    for advert in gossip.peers {
                        self.upsert_advert(advert);
                    }
                }
                // Defensive: a peer running an older reply contract.
                Some(WireMessage::HeartbeatBeacon(reply)) => {
                    self.upsert_advert(PeerAdvert {
                        node_id: reply.node_id,
                        addr: reply.addr,
                        hardware: reply.hardware,
                        capabilities: reply.capabilities,
                    });
                }
                _ => {}
            }
        }
    }

    /// Build the heartbeat beacon for this node as of now: fresh timestamp,
    /// next replay nonce, and (in signed mode) our pubkey and signature.
    fn current_beacon(&self) -> HeartbeatBeacon {
        let nonce = self.beacon_nonce.fetch_add(1, Ordering::Relaxed);
        let mut frame = WireMessage::HeartbeatBeacon(HeartbeatBeacon {
            node_id: self.self_id,
            hardware: self.config.hardware,
            capabilities: self.config.capabilities,
            timestamp_ms: now_ms(),
            addr: self.mesh_addr(),
            nonce,
            pubkey: [0; 32],
            signature: [0; 64],
        });
        self.auth_wire(&mut frame);
        match frame {
            WireMessage::HeartbeatBeacon(beacon) => beacon,
            _ => unreachable!("constructed as a beacon above"),
        }
    }

    /// Refresh this node's own record in the peer table.
    fn refresh_self(&self, beacon: &HeartbeatBeacon) {
        self.peers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .upsert(PeerRecord {
                node_id: beacon.node_id,
                hardware: beacon.hardware,
                capabilities: beacon.capabilities,
                addr: beacon.addr,
                last_seen: Instant::now(),
            });
    }

    /// Run the daemon until Ctrl-C (or SIGTERM): heartbeat loop, peer
    /// maintenance, mesh serving (when configured), and the local TCP
    /// control API.
    pub async fn run(self: Arc<Self>) -> Result<(), Box<dyn Error + Send + Sync>> {
        tracing::info!(
            node = %id::to_hex(self.self_id.as_bytes()),
            kind = ?self.config.identity.kind,
            chain = ?self.config.chain,
            heartbeat_ms = self.config.discovery.heartbeat_interval.as_millis() as u64,
            "tpt-mosaic node starting"
        );

        // Keep the mesh guard alive for the lifetime of the daemon.
        let _mesh_guard = match self.start_mesh() {
            Some(guard) => {
                tracing::info!(addr = %self.mesh_addr().expect("guard implies listener"), "mesh listening");
                Some(guard)
            }
            None => {
                tracing::info!("mesh disabled");
                None
            }
        };

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut tasks = Vec::new();

        tasks.push(tokio::spawn(heartbeat_loop(
            self.clone(),
            shutdown_rx.clone(),
        )));
        tasks.push(tokio::spawn(eviction_loop(
            self.clone(),
            shutdown_rx.clone(),
        )));

        if let Some(control) = self.config.control.clone() {
            let listener = tokio::net::TcpListener::bind(control.listen)
                .await
                .map_err(|e| format!("cannot bind control API on {}: {e}", control.listen))?;
            tracing::info!(addr = %control.listen, "control API listening");
            tasks.push(tokio::spawn(crate::control::serve(
                self.clone(),
                listener,
                shutdown_rx,
            )));
        } else {
            tracing::info!("control API disabled");
        }

        tokio::signal::ctrl_c()
            .await
            .map_err(|e| format!("failed to listen for shutdown signal: {e}"))?;
        tracing::info!("shutdown signal received");

        let _ = shutdown_tx.send(true);
        for handle in tasks {
            let _ = handle.await;
        }
        tracing::info!(
            tasks_completed = self.stats.tasks_completed(),
            tasks_failed = self.stats.tasks_failed(),
            "tpt-mosaic node stopped"
        );
        Ok(())
    }
}

/// Periodically broadcast heartbeats, refresh our own peer record, and trade
/// beacons with seeds and known peers over the mesh.
async fn heartbeat_loop(daemon: Arc<NodeDaemon>, mut shutdown: watch::Receiver<bool>) {
    let mut ticker = interval(daemon.config.discovery.heartbeat_interval);
    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            _ = ticker.tick() => {
                // Blocking network I/O on the async tick: bounded by the mesh
                // RPC timeout and offloaded so the runtime is not stalled.
                let daemon = daemon.clone();
                let _ = tokio::task::spawn_blocking(move || daemon.exchange_beacons()).await;
            }
        }
    }
}

/// Evict peers that have gone silent past the staleness window.
async fn eviction_loop(daemon: Arc<NodeDaemon>, mut shutdown: watch::Receiver<bool>) {
    let mut ticker = interval(
        daemon
            .config
            .discovery
            .peer_max_age
            .max(std::time::Duration::from_secs(1)),
    );
    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            _ = ticker.tick() => {
                let evicted = lock_ok(&daemon.peers).evict_stale();
                if evicted > 0 {
                    tracing::debug!(evicted, "stale peers evicted");
                }
            }
        }
    }
}

/// Select the settlement adapter for the configured chain, optionally
/// persisting balances to `state_file` across restarts.
fn make_settlement(
    chain: Chain,
    rpc_url: Option<&str>,
    state_file: Option<&std::path::Path>,
) -> Arc<dyn Settlement> {
    let rpc_url = rpc_url.unwrap_or("stub://local");
    match chain {
        Chain::Solana => Arc::new(SolanaSettlement::new_with_state(rpc_url, state_file)),
        Chain::Base => Arc::new(BaseSettlement::new_with_state(rpc_url, state_file)),
        Chain::Near => Arc::new(NearSettlement::new_with_state(rpc_url, state_file)),
    }
}

/// Lock a shared map, tolerating poisoning: a panic in one task must not
/// wedge every later task behind a poisoned mutex.
fn lock_ok<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Unix timestamp in milliseconds.
pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Drop expired cancellation notices.
fn prune_cancelled(cancelled: &mut HashMap<TaskId, Instant>) {
    let now = Instant::now();
    cancelled.retain(|_, seen| now.duration_since(*seen) < CANCELLATION_TTL);
}

/// Mutable access to a frame's `signature` field, for signing and zeroing.
fn signature_field_mut(msg: &mut WireMessage) -> Option<&mut [u8; 64]> {
    match msg {
        WireMessage::HeartbeatBeacon(beacon) => Some(&mut beacon.signature),
        WireMessage::TaskAssignment(assignment) => Some(&mut assignment.signature),
        WireMessage::ResultHash(result) => Some(&mut result.signature),
        _ => None,
    }
}

/// Zero a frame's `signature` field (the canonical pre-signing encoding).
fn zero_signature(msg: &mut WireMessage) {
    if let Some(field) = signature_field_mut(msg) {
        *field = [0; 64];
    }
}

/// `true` when a result's production timestamp sits inside the acceptance
/// window (bounded age, bounded future skew).
fn result_is_fresh(produced_at_ms: u64) -> bool {
    let now = now_ms();
    produced_at_ms + RESULT_MAX_AGE_MS >= now && produced_at_ms <= now + CLOCK_SKEW_TOLERANCE_MS
}

/// `true` for addresses that could plausibly identify a reachable peer:
/// rejects unspecified (0.0.0.0/::), broadcast, and multicast targets.
/// Loopback stays valid — tests and single-host meshes use it.
fn plausible_addr(addr: SocketAddr) -> bool {
    let ip = addr.ip();
    !(ip.is_unspecified()
        || ip.is_multicast()
        || matches!(ip, std::net::IpAddr::V4(v4) if v4.is_broadcast()))
}

/// Logs heartbeats instead of transmitting them; replaced by the BLE/Wi-Fi
/// transports in Phase 3 of the roadmap.
#[derive(Debug, Default)]
struct TracingBroadcaster;

impl BeaconBroadcaster for TracingBroadcaster {
    fn broadcast(&self, beacon: &HeartbeatBeacon) -> Result<(), MosaicError> {
        tracing::debug!(node = %id::to_hex(beacon.node_id.as_bytes()), ts = beacon.timestamp_ms, "heartbeat broadcast");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::Signer;
    use tpt_mosaic_core::QuorumConfig;

    fn test_daemon() -> NodeDaemon {
        NodeDaemon::new(
            crate::config::NodeConfig::from_toml_str("").expect("defaults are always valid"),
        )
    }

    #[test]
    fn best_effort_task_meets_quorum_and_returns_blake3_of_payload() {
        let daemon = test_daemon();
        let payload = b"mosaic integration payload";
        let receipt = daemon
            .run_local_task(payload, QuorumConfig::BEST_EFFORT_1_OF_1)
            .expect("1-of-1 must succeed locally");

        assert_eq!(receipt.confirmations, 1);
        assert_eq!(
            receipt.agreed_hash,
            hash_output(payload, HashAlgorithm::Blake3)
        );
        assert!(receipt.reward > 0);
        assert_eq!(daemon.stats().tasks_completed, 1);
        assert_eq!(daemon.stats().tasks_failed, 0);
    }

    #[test]
    fn higher_tier_fails_without_networked_peers() {
        let daemon = test_daemon();
        let err = daemon
            .run_local_task(b"payload", QuorumConfig::STANDARD_3_OF_5)
            .expect_err("3-of-5 cannot be met by a lone node");
        assert!(matches!(
            err,
            MosaicError::QuorumNotMet {
                got: 1,
                required: 3,
                ..
            }
        ));
        assert_eq!(daemon.stats().tasks_failed, 1);
    }

    #[test]
    fn empty_payload_is_rejected() {
        let daemon = test_daemon();
        assert!(matches!(
            daemon.run_local_task(b"", QuorumConfig::BEST_EFFORT_1_OF_1),
            Err(MosaicError::SerializationError)
        ));
    }

    #[test]
    fn reward_is_credited_to_the_ledger() {
        let daemon = test_daemon();
        let receipt = daemon
            .run_local_task(b"pay", QuorumConfig::BEST_EFFORT_1_OF_1)
            .unwrap();
        assert_eq!(
            daemon.settlement.balance(daemon.node_id()).unwrap(),
            receipt.reward
        );
    }

    #[test]
    fn interrupted_tasks_resume_from_checkpoints() {
        let dir = std::env::temp_dir().join(format!("mosaic-cp-{}", std::process::id()));
        let config = crate::config::NodeConfig::from_toml_str(&format!(
            "[task]
checkpoint_dir = {:?}",
            dir
        ))
        .unwrap();
        let daemon = NodeDaemon::new(config);

        // Two shards: 100 KiB against a 64 KiB shard target.
        let payload = vec![7u8; 100 * 1024];
        let task_id = id::generate_task_id();
        let shards = split_task(
            task_id,
            &payload,
            TaskPriority::Standard,
            SHARD_TARGET_BYTES,
        )
        .unwrap();
        assert_eq!(shards.len(), 2);

        // Simulate an interruption after the first shard: reproduce the
        // daemon's per-shard execution so the checkpoint carries the true
        // output prefix.
        let grant = CapabilityGrant::new(task_id, GRANT_MAX_MEMORY_BYTES, GRANT_MAX_CPU_MS);
        let compiled = compiler::compile(&shards[0].payload, &daemon.config.hardware).unwrap();
        let prefix = daemon.sandbox.execute(&grant, &compiled).unwrap();
        let cp_path = dir.join(format!(
            "{}-{}.cp",
            id::to_hex(task_id.as_bytes()),
            id::to_hex(&compiler::fingerprint(&payload, &daemon.config.hardware))
        ));
        std::fs::create_dir_all(&dir).unwrap();
        tpt_mosaic_task::save_checkpoint(
            &cp_path,
            &tpt_mosaic_task::Checkpoint {
                task_id,
                last_completed_shard: 1,
                state_blob: prefix,
            },
        )
        .unwrap();

        // The resumed run must hash the full output — identical to an
        // uninterrupted run — not just the output of the remaining shards.
        let (hash, executed) = daemon.execute_locally(task_id, &payload).unwrap();
        assert_eq!(executed, 2);
        assert_eq!(hash, hash_output(&payload, HashAlgorithm::Blake3));
        assert!(!cp_path.exists(), "finished tasks clear their checkpoint");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn checkpoints_are_isolated_per_task() {
        let dir = std::env::temp_dir().join(format!("mosaic-cp-iso-{}", std::process::id()));
        let config = crate::config::NodeConfig::from_toml_str(&format!(
            "[task]
checkpoint_dir = {:?}",
            dir
        ))
        .unwrap();
        let daemon = NodeDaemon::new(config);
        let payload = vec![3u8; 64 * 1024];

        // A stale checkpoint left by a different task at this task's path
        // must be ignored, not resumed.
        let foreign = id::generate_task_id();
        let cp_path = dir.join(format!(
            "{}-{}.cp",
            id::to_hex(foreign.as_bytes()),
            id::to_hex(&compiler::fingerprint(&payload, &daemon.config.hardware))
        ));
        std::fs::create_dir_all(&dir).unwrap();
        tpt_mosaic_task::save_checkpoint(
            &cp_path,
            &tpt_mosaic_task::Checkpoint {
                task_id: foreign,
                last_completed_shard: 7,
                state_blob: vec![0xEE; 8],
            },
        )
        .unwrap();

        let task_id = id::generate_task_id();
        let (hash, executed) = daemon.execute_locally(task_id, &payload).unwrap();
        assert_eq!(executed, 1);
        assert_eq!(hash, hash_output(&payload, HashAlgorithm::Blake3));
        // The foreign checkpoint is untouched by this task.
        assert!(cp_path.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Build a properly signed assignment (worker-side checks are
    /// proof-of-possession, so any keypair works).
    fn signed_assignment(task_id: TaskId, deadline_ms: u64) -> WireMessage {
        let mut frame = WireMessage::TaskAssignment(TaskAssignment {
            task_id,
            quorum_config: QuorumConfig::BEST_EFFORT_1_OF_1,
            payload: b"payload".to_vec(),
            deadline_ms,
            coordinator: SocketAddr::from(([127, 0, 0, 1], 1)),
            pubkey: [0; 32],
            signature: [0; 64],
        });
        let signing = ed25519_dalek::SigningKey::from_bytes(&[9u8; 32]);
        if let WireMessage::TaskAssignment(assignment) = &mut frame {
            assignment.pubkey = signing.verifying_key().to_bytes();
        }
        let signature = signing.sign(&encode(&frame));
        if let Some(field) = signature_field_mut(&mut frame) {
            *field = signature.to_bytes();
        }
        frame
    }

    #[test]
    fn expired_and_cancelled_assignments_are_refused() {
        let daemon = test_daemon();
        let task_id = id::generate_task_id();
        let expired = signed_assignment(task_id, 0); // long past
        assert!(
            daemon.handle_mesh_frame(&expired).is_none(),
            "an assignment past its deadline must not execute"
        );

        // After a cancellation notice the task stays refused even with a
        // live deadline (late/reordered dispatch).
        daemon.record_cancellation(task_id);
        let live = signed_assignment(task_id, now_ms() + 10_000);
        assert!(daemon.handle_mesh_frame(&live).is_none());

        // A control: the same task unsigned (fresh id) executes and replies.
        let fresh = signed_assignment(id::generate_task_id(), now_ms() + 10_000);
        assert!(
            daemon.handle_mesh_frame(&fresh).is_some(),
            "a well-signed live assignment must execute"
        );
    }

    #[test]
    fn unauthenticated_and_replayed_frames_are_rejected() {
        let daemon = test_daemon();

        // A beacon whose pubkey does not hash to its claimed node id is
        // rejected even with a valid signature from that key.
        let signing = ed25519_dalek::SigningKey::from_bytes(&[5u8; 32]);
        let mut forged = daemon.current_beacon();
        forged.node_id = NodeId::from_bytes([3; 16]); // not hash(pubkey)
        forged.pubkey = signing.verifying_key().to_bytes();
        forged.signature = [0; 64];
        let signature = signing.sign(&encode(&WireMessage::HeartbeatBeacon(forged)));
        forged.signature = signature.to_bytes();
        assert!(daemon
            .verify_wire(&WireMessage::HeartbeatBeacon(forged))
            .is_err());

        // A badly signed beacon (right key binding, wrong signature) is
        // rejected.
        let mut bad_sig = daemon.current_beacon();
        bad_sig.signature = [7; 64];
        assert!(daemon
            .verify_wire(&WireMessage::HeartbeatBeacon(bad_sig))
            .is_err());

        // Replay: the first beacon from a peer is fresh; an identical repeat
        // (same nonce) must be refused.
        let peer = ed25519_dalek::SigningKey::from_bytes(&[6u8; 32]);
        let peer_id = id::node_id_for_pubkey(&peer.verifying_key().to_bytes());
        let mk_beacon = |nonce: u64| {
            let mut beacon = HeartbeatBeacon {
                node_id: peer_id,
                hardware: daemon.config.hardware,
                capabilities: daemon.config.capabilities,
                timestamp_ms: now_ms(),
                addr: Some(SocketAddr::from(([127, 0, 0, 1], 7000))),
                nonce,
                pubkey: peer.verifying_key().to_bytes(),
                signature: [0; 64],
            };
            let signature = peer.sign(&encode(&WireMessage::HeartbeatBeacon(beacon)));
            beacon.signature = signature.to_bytes();
            beacon
        };
        assert!(daemon.beacon_is_fresh(&mk_beacon(1)));
        assert!(
            !daemon.beacon_is_fresh(&mk_beacon(1)),
            "a repeated nonce must be refused"
        );

        // A stale timestamp is refused (window floor is 30 s).
        let mut stale = mk_beacon(2);
        stale.timestamp_ms = now_ms().saturating_sub(400_000);
        assert!(!daemon.beacon_is_fresh(&stale));
    }

    #[test]
    fn gossiped_adverts_are_validated() {
        let daemon = test_daemon();

        // Addresses that cannot identify a peer are dropped.
        for bogus_ip in [
            [0, 0, 0, 0],         // unspecified
            [255, 255, 255, 255], // broadcast
            [224, 0, 0, 1],       // multicast
        ] {
            let id = NodeId::from_bytes([bogus_ip[0]; 16]);
            daemon.upsert_advert(PeerAdvert {
                node_id: id,
                addr: Some(SocketAddr::from((bogus_ip, 7000))),
                hardware: daemon.config.hardware,
                capabilities: CapabilityFlags::empty(),
            });
            assert!(
                daemon.peers.lock().unwrap().get(&id).is_none(),
                "addr {bogus_ip:?} must be rejected"
            );
        }

        // A plausible advert is accepted.
        let good = NodeId::from_bytes([10; 16]);
        daemon.upsert_advert(PeerAdvert {
            node_id: good,
            addr: Some(SocketAddr::from(([127, 0, 0, 1], 7001))),
            hardware: daemon.config.hardware,
            capabilities: CapabilityFlags::empty(),
        });
        assert!(daemon.peers.lock().unwrap().get(&good).is_some());

        // A gossiped advert carrying our own id never enters the table as a
        // foreign record or overwrites our self-record.
        let self_advert_peer = NodeId::from_bytes([11; 16]);
        daemon.upsert_advert(PeerAdvert {
            node_id: daemon.node_id(),
            addr: Some(SocketAddr::from(([127, 0, 0, 1], 7002))),
            hardware: daemon.config.hardware,
            capabilities: CapabilityFlags::empty(),
        });
        assert!(daemon
            .peers
            .lock()
            .unwrap()
            .get(&self_advert_peer)
            .is_none());
    }

    #[test]
    fn dht_queries_are_answered_with_filtered_gossip() {
        let daemon = test_daemon();
        // A CUDA-capable peer plus an unavailable one (critical thermal).
        let cuda_peer = PeerRecord {
            node_id: NodeId::from_bytes([7; 16]),
            hardware: tpt_mosaic_core::HardwareProfile {
                kind: NodeKind::AnchorBallast,
                ..daemon.config.hardware
            },
            capabilities: CapabilityFlags::CUDA,
            addr: None,
            last_seen: Instant::now(),
        };
        let hot_peer = PeerRecord {
            node_id: NodeId::from_bytes([8; 16]),
            hardware: tpt_mosaic_core::HardwareProfile {
                thermal_state: tpt_mosaic_core::ThermalState::Critical,
                ..cuda_peer.hardware
            },
            capabilities: CapabilityFlags::CUDA,
            addr: None,
            last_seen: Instant::now(),
        };
        {
            let mut peers = lock_ok(&daemon.peers);
            peers.upsert(cuda_peer);
            peers.upsert(hot_peer);
        }

        // CUDA query: the matching live peer comes back; the hot peer and
        // the daemon itself (CPU_VECTOR only) are filtered out.
        let reply = daemon.handle_mesh_frame(&WireMessage::DhtQuery(DhtQuery {
            key: [0; 32],
            limit: 10,
            capability_filter: CapabilityFlags::CUDA,
        }));
        let WireMessage::PeerGossip(gossip) = reply.expect("query must be answered") else {
            panic!("reply must be gossip");
        };
        assert_eq!(gossip.peers.len(), 1);
        assert_eq!(gossip.peers[0].node_id, NodeId::from_bytes([7; 16]));

        // Unfiltered query: self plus the one available peer.
        let reply = daemon.handle_mesh_frame(&WireMessage::DhtQuery(DhtQuery {
            key: [0; 32],
            limit: 10,
            capability_filter: CapabilityFlags::empty(),
        }));
        let WireMessage::PeerGossip(gossip) = reply.expect("query must be answered") else {
            panic!("reply must be gossip");
        };
        assert_eq!(
            gossip.peers.len(),
            2,
            "self + the live peer, hot one excluded"
        );
    }
}
