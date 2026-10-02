//! Node daemon: wires discovery, compilation, sandboxed execution, quorum
//! verification, and settlement into a single runtime (spec §8 task lifecycle).
//!
//! Local tasks run shard → compile → sandbox → hash → quorum → settlement.
//! With `[mesh]` configured, the daemon also serves TCP mesh connections,
//! trades beacon exchanges with seeds/peers, and coordinates multi-node
//! quorums: assemble → dispatch `TaskAssignment` → collect `ResultHash` →
//! broadcast cancellation (spec §3.3, §8).

use std::error::Error;
use std::net::{SocketAddr, TcpListener};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::watch;
use tokio::time::interval;

use tpt_mosaic_compiler as compiler;
use tpt_mosaic_core::{CapabilityFlags, MosaicError, NodeId, NodeKind, QuorumConfig, TaskId};
use tpt_mosaic_discovery::{BeaconBroadcaster, FrameHandler, PeerRecord, PeerTable, TcpMesh};
use tpt_mosaic_economy::chains::{BaseSettlement, NearSettlement, SolanaSettlement};
use tpt_mosaic_economy::{calculate_reward, Chain, ReputationStore, Settlement};
use tpt_mosaic_proto::codec::MAX_GOSSIP_PEERS;
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
    broadcaster: TracingBroadcaster,
    sandbox: Sandbox,
    reputation: Arc<Mutex<ReputationStore>>,
    settlement: Arc<dyn Settlement>,
    stats: Arc<Stats>,
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
        let self_id = match config.identity.id {
            Some(id) => id,
            None => match &config.identity.state_file {
                Some(path) => id::load_or_create(path).unwrap_or_else(|e| {
                    tracing::warn!(
                        path = %path.display(),
                        error = %e,
                        "identity state file unusable; using a random id"
                    );
                    id::generate_node_id()
                }),
                None => id::generate_node_id(),
            },
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
        let settlement = make_settlement(config.chain, config.rpc_url.as_deref());
        Self {
            config,
            self_id,
            peers: Arc::new(Mutex::new(PeerTable::new(peer_max_age))),
            mesh_listener: Mutex::new(mesh_listener),
            mesh_addr,
            jit,
            checkpoints,
            broadcaster: TracingBroadcaster,
            sandbox: Sandbox::new(ThermalPolicy::default()),
            reputation: Arc::new(Mutex::new(ReputationStore::new())),
            settlement,
            stats: Arc::new(Stats::default()),
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
        let listener = self
            .mesh_listener
            .lock()
            .expect("mesh listener poisoned")
            .take()?;
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
            .expect("peer table poisoned")
            .live_peers()
            .map(|p| p.node_id)
            .collect()
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
        let shards = split_task(task_id, payload, TaskPriority::Standard, SHARD_TARGET_BYTES)?;
        let grant = CapabilityGrant::new(task_id, GRANT_MAX_MEMORY_BYTES, GRANT_MAX_CPU_MS);

        // Resume cursor: keyed by the (payload, hardware) fingerprint, so a
        // resubmitted identical task continues where an interrupted one
        // stopped. The checkpoint is removed once every shard has completed.
        let cp_path = self.checkpoints.as_ref().map(|dir| {
            dir.join(format!(
                "{}.cp",
                id::to_hex(&compiler::fingerprint(payload, &self.config.hardware))
            ))
        });
        let mut progress = match &cp_path {
            Some(path) => restore_checkpoint(path)
                .ok()
                .and_then(|cp| TaskProgress::from_checkpoint(&cp, shards.len() as u32).ok())
                .unwrap_or_else(|| TaskProgress::new(task_id, shards.len() as u32)),
            None => TaskProgress::new(task_id, shards.len() as u32),
        };
        if progress.completed() > 0 {
            tracing::info!(
                task = %id::to_hex(task_id.as_bytes()),
                resumed_at = progress.completed(),
                "resuming interrupted task from checkpoint"
            );
        }

        let mut output = Vec::with_capacity(payload.len());
        for shard in &shards[progress.completed() as usize..] {
            let compiled = match &self.jit {
                Some(cache) => cache.compile_cached(&shard.payload, &self.config.hardware)?,
                None => compiler::compile(&shard.payload, &self.config.hardware)?,
            };
            let produced = self.sandbox.execute(&grant, &compiled)?;
            output.extend_from_slice(&produced);
            progress.advance();
            if let Some(path) = &cp_path {
                let _ = save_checkpoint(path, &progress.checkpoint(Vec::new()));
            }
        }
        if let Some(path) = &cp_path {
            let _ = std::fs::remove_file(path);
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
                self.settlement
                    .submit_reward(self.self_id, reward, &task_hex)?;
                self.reputation
                    .lock()
                    .expect("reputation poisoned")
                    .record_success(self.self_id);
                self.stats.tasks_completed.fetch_add(1, Ordering::Relaxed);
                tracing::info!(task = %task_hex, confirmations, reward, "quorum met");
                Ok(TaskReceipt {
                    task_id,
                    agreed_hash: *agreed_hash,
                    confirmations: *confirmations,
                    reward,
                    shards,
                })
            }
            _ => {
                self.reputation
                    .lock()
                    .expect("reputation poisoned")
                    .record_failure(self.self_id);
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
            .expect("peer table poisoned")
            .live_peers()
            .cloned()
            .collect();
        let selected = BalancedAssembler::default().assemble(
            &candidates,
            &quorum,
            CapabilityFlags::empty(),
        )?;

        let mut collector = HashCollector::new(task_id, quorum)?;
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
        let peers = self.peers.lock().expect("peer table poisoned");
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
        let assignment = TaskAssignment {
            task_id,
            quorum_config: quorum,
            payload: payload.to_vec(),
            deadline_ms: now_ms() + MESH_DISPATCH_TIMEOUT.as_millis() as u64,
            coordinator,
        };

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
                    let frame = WireMessage::TaskAssignment(assignment.clone());
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
                    Ok(Some(rh)) => {
                        collector.submit(rh.node_id, rh.hash);
                        replied.push(rh.node_id);
                        got += 1;
                        tracker.complete(node);
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

        // Early termination (spec §3.3): once the quorum is met, tell any
        // dispatched worker that had not contributed to stop. Fire-and-forget.
        if outcome.is_ok() {
            let cancellation =
                WireMessage::CancellationSignal(tpt_mosaic_proto::CancellationSignal {
                    task_id,
                    reason: tpt_mosaic_proto::CancellationReason::QuorumMet,
                });
            for (node, addr) in &dispatched {
                if !replied.contains(node) {
                    TcpMesh::send(*addr, &cancellation, MESH_RPC_TIMEOUT);
                }
            }
        }

        outcome
    }

    /// Mesh frame handler: process one inbound frame, optionally replying.
    fn handle_mesh_frame(&self, msg: &WireMessage) -> Option<WireMessage> {
        match msg {
            WireMessage::HeartbeatBeacon(beacon) => {
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
                tracing::info!(task = %task_hex, "mesh assignment received");
                match self.execute_locally(assignment.task_id, &assignment.payload) {
                    Ok((hash, _shards)) => {
                        Some(WireMessage::ResultHash(tpt_mosaic_proto::ResultHash {
                            task_id: assignment.task_id,
                            node_id: self.self_id,
                            hash,
                            produced_at_ms: now_ms(),
                        }))
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

    /// Insert or refresh a peer record from an advertisement.
    fn upsert_advert(&self, advert: PeerAdvert) {
        self.peers
            .lock()
            .expect("peer table poisoned")
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
        let table = self.peers.lock().expect("peer table poisoned");
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
    pub fn exchange_beacons(&self) {
        let beacon = self.current_beacon();
        self.broadcaster.broadcast(&beacon).ok();
        self.stats.heartbeats_sent.fetch_add(1, Ordering::Relaxed);
        self.refresh_self(&beacon);

        let peers = self.peers.lock().expect("peer table poisoned");
        let mut targets: Vec<SocketAddr> = self.config.mesh.seeds.clone();
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

        for addr in targets {
            match TcpMesh::exchange(
                addr,
                &WireMessage::HeartbeatBeacon(beacon),
                MESH_RPC_TIMEOUT,
            ) {
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

    /// Build the heartbeat beacon for this node as of now.
    fn current_beacon(&self) -> HeartbeatBeacon {
        HeartbeatBeacon {
            node_id: self.self_id,
            hardware: self.config.hardware,
            capabilities: self.config.capabilities,
            timestamp_ms: now_ms(),
            addr: self.mesh_addr(),
        }
    }

    /// Refresh this node's own record in the peer table.
    fn refresh_self(&self, beacon: &HeartbeatBeacon) {
        self.peers
            .lock()
            .expect("peer table poisoned")
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
                let evicted = daemon.peers.lock().expect("peer table poisoned").evict_stale();
                if evicted > 0 {
                    tracing::debug!(evicted, "stale peers evicted");
                }
            }
        }
    }
}

/// Select the settlement adapter for the configured chain.
fn make_settlement(chain: Chain, rpc_url: Option<&str>) -> Arc<dyn Settlement> {
    let rpc_url = rpc_url.unwrap_or("stub://local");
    match chain {
        Chain::Solana => Arc::new(SolanaSettlement::new(rpc_url)),
        Chain::Base => Arc::new(BaseSettlement::new(rpc_url)),
        Chain::Near => Arc::new(NearSettlement::new(rpc_url)),
    }
}

/// Unix timestamp in milliseconds.
pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
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
        let fingerprint = compiler::fingerprint(&payload, &daemon.config.hardware);
        let cp_path = dir.join(format!("{}.cp", id::to_hex(&fingerprint)));

        // Simulate an interruption after the first shard completed.
        std::fs::create_dir_all(&dir).unwrap();
        let interrupted = tpt_mosaic_task::Checkpoint {
            task_id: TaskId::NIL,
            last_completed_shard: 1,
            state_blob: vec![],
        };
        tpt_mosaic_task::save_checkpoint(&cp_path, &interrupted).unwrap();

        let receipt = daemon
            .run_local_task(&payload, QuorumConfig::BEST_EFFORT_1_OF_1)
            .expect("resumed task must complete");
        // Only the remaining shard executed: the hash covers the resume slice.
        assert_eq!(
            receipt.agreed_hash,
            hash_output(&payload[64 * 1024..], HashAlgorithm::Blake3)
        );
        assert!(!cp_path.exists(), "finished tasks clear their checkpoint");
        let _ = std::fs::remove_dir_all(&dir);
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
            let mut peers = daemon.peers.lock().expect("peer table poisoned");
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
