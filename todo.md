# tpt-mosaic — Development Roadmap

Phased checklist mirroring [spec.txt](spec.txt). Checked items are implemented
in-tree; unchecked items are pending. Stubbed integration points are marked.

## Phase 0 — Workspace Scaffolding

- [x] Cargo workspace with 11 crates, shared profiles, pinned toolchain (rustfmt + clippy)
- [x] CI: fmt, clippy `-D warnings`, tests on Linux/macOS/Windows, release build, docs `-D warnings`
- [x] MIT OR Apache-2.0 dual licensing, README, design spec
- [x] CONTRIBUTING.md, CHANGELOG.md, CI status badge, tracked `Cargo.lock`

## Phase 1 — `tpt-mosaic-core`

- [x] `NodeId` / `TaskId` opaque 16-byte identifiers
- [x] `QuorumConfig` + `TierLevel` presets (1-of-1, 2-of-3, 3-of-5, 7-of-10)
- [x] `HardwareProfile`, `CapabilityFlags`, `ThermalState`, `NodeKind`
- [x] `MosaicError` taxonomy
- [x] `NodeCapability` / `TaskExecutor` / `QuorumParticipant` traits

## Phase 2 — `tpt-mosaic-proto`

- [x] Message type definitions (`TaskAssignment`, `ResultHash`, `HeartbeatBeacon`, `CancellationSignal`, `DhtQuery`)
- [x] Frame format encoder + decoder (magic `MOSA` + wire version + tag + length, strict validation)
- [x] Round-trip tests + `decode`-never-panics property test
- [ ] `.fbs` schemas + `flatc` codegen via `build.rs` (replace the hand-rolled `codec.rs`)
- [ ] True zero-copy accessors (current codec copies payloads)

## Phase 3 — `tpt-mosaic-discovery`

- [x] `PeerTable` with staleness eviction
- [x] `BeaconBroadcaster` / `BeaconScanner` / `DhtClient` traits
- [x] Heartbeat + eviction loops running in the node daemon
- [x] TCP mesh transport (`TcpMesh`): beacon exchange with static seeds, loopback/LAN (v0 data plane)
- [x] Gossip discovery: beacon replies carry the responder's peer-table snapshot, so a single seed propagates the full view (star topologies work)
- [ ] BLE transport (control plane beacons)
- [ ] UWB ranging integration
- [x] Wi-Fi mDNS peer detection (`mdns` feature via mdns-sd; zero-config bootstrap feeding the beacon/gossip path)
- [x] Mesh directory DHT (`MeshDht` implements `DhtClient` over `TcpMesh`): announce floods adverts, lookup unions all reachable peer tables, capability-filtered
- [ ] Key-routed DHT (Kademlia-style rendezvous) for wide-area indexing

## Phase 4 — `tpt-mosaic-scheduler`

- [x] `HeterogeneousAssembler`: diversity-first greedy quorum selection
- [x] Capability filtering + availability gating (thermal / battery)
- [x] Straggler detection (`DispatchTracker`) and 1:1 replacement budgeting
- [x] Timeout policy engine (`StragglerPolicy`)
- [x] Networked dispatch wired: assembler selects live peers, mesh RPC bounded by timeouts
- [x] `DispatchTracker` + `StragglerPolicy` wired into mesh dispatch: failed/missing members are replaced 1:1 from the spare pool (tested with a dead anchor member in a 5-of-5 round)
- [x] Edge/anchor ratio balancing policy (`BalancedAssembler`, min/max anchors)

## Phase 5 — `tpt-mosaic-task`

- [x] Micro-task splitting (shards) and reassembly
- [x] `Checkpoint` type + disk persistence + `TaskProgress` resume cursor
- [x] Priority queue with preemption (`TaskQueue::preempt_below`, spec §5.2)
- [ ] Checkpoint/restore wiring into the daemon executor (library API done)
- [ ] Data-routing guard: raw weights never routed over cellular

## Phase 6 — `tpt-mosaic-compiler`

- [x] Backend selection (`tpt-gpu` vs `tpt-crucible`) + workload fingerprinting + fallback chain
- [x] `tpt-gpu-runtime` integration (`gpu` feature): TPTIR text compiled via `Device::load_module`
- [x] `tpt-crucible-catalyst` integration (`crucible` feature): SafeTensors/GGUF lowered to TPT-IR
- [ ] Real CUDA/Metal/Vulkan hardware paths (upstream `cuda` feature, opt-in)
- [x] On-disk JIT binary cache keyed by fingerprint (`JitCache`, `[compiler] cache_dir`)

## Phase 7 — `tpt-mosaic-sandbox`

- [x] `CapabilityGrant` construction + `ThermalPolicy`
- [x] `tpt-archon-kernel` integration (`archon` feature): grant-sized page pool, minted capabilities, scheduler-run confined execution
- [ ] RTOS preemption hook registration (archon scheduler is cooperative; CPU/GPU-ms quotas are structural only)
- [ ] Zero-copy I/O channels (upstream `mmap` feature is available)

## Phase 8 — `tpt-mosaic-quorum`

- [x] `HashCollector` K-of-N state machine (met / diverged / timeout)
- [x] Byzantine suspicion flagging on divergence
- [x] Early termination semantics (quorum short-circuits remaining submissions)
- [x] `HashCollector::cancellation_signal` (QuorumMet / Timeout) for the broadcast path
- [x] Property-based fuzzing of the state machine (proptest)
- [x] Cancellation broadcast fan-out over the mesh (non-contributors told to stop on quorum-met)

## Phase 9 — `tpt-mosaic-verify`

- [x] BLAKE3 / SHA-256 output hashing
- [x] Hashing property tests (determinism + backend agreement)
- [x] `ZkProver` trait + `StubProver`
- [x] `tpt-eve-symbolic` hooks (`eve` feature): claims lowered to facts, `ConsistencyChecker` contradiction reporting
- [ ] Real ZK-ML backend integration
- [ ] Proof-of-learning verification for training workloads

## Phase 10 — `tpt-mosaic-economy`

- [x] Tier- and capability-weighted reward calculation
- [x] `SlashingRecord` + slash reasons
- [x] `ReputationStore` with quorum-outcome recording
- [x] `Settlement` trait + ledger-backed Solana/Base/NEAR adapters (stub RPC)
- [ ] Solana adapter with real RPC (`solana` feature)
- [ ] Base adapter with real RPC (`base` feature)
- [ ] NEAR adapter with real RPC (`near` feature)
- [ ] Datacenter spot-pricing integration

## Phase 11 — `tpt-mosaic-node`

- [x] `node.toml` config loading with defaults + validation (`node.toml.example`)
- [x] Identity generation, subsystem wiring, heartbeat loop, graceful shutdown
- [x] Local task lifecycle: shard → compile → sandbox → hash → quorum → settlement
- [x] `real-backends` feature: daemon builds against the real tpt-gpu/crucible/archon/eve backends
- [x] Local TCP control API (`STATUS` / `PEERS` / `SUBMIT` / `HELP`)
- [ ] Networked task dispatch (scheduler → real transports)
- [x] Persistent node identity across restarts (`[node] state_file`)
- [ ] systemd / launchd packaging

## Phase 12 — Hardening & Release

- [x] Dependency audit (removed unused `tokio`/`tracing`/`flatbuffers`/`proto` deps; re-add per roadmap)
- [ ] Fuzzing (`cargo-fuzz`) for proto decoding and the quorum state machine
- [x] Benchmark suite (criterion, `benches/`): codec, sharding, hashing, quorum evaluation, assembly
- [ ] Benchmark regression tracking (critical thresholds in CI)
- [ ] `no_std` feature-combination matrix (`cargo-hack`) in CI
- [x] v0.1.0 CHANGELOG
- [ ] crates.io publish — blocked on the upstream tpt-* crates publishing first (Cargo rejects git dependencies in published packages; confirmed via `cargo package --dry-run`)

## Phase 13 — Correctness & security fixes (from platform review)

- [x] `HashCollector`: one vote per node, optional member allow-list, early divergence, deterministic tie-break
- [x] `QuorumConfig::is_valid` requires strict majority (`2k > n`)
- [x] Daemon rejects `ResultHash` replies with wrong `task_id` / `node_id`; only assembled candidates may vote
- [x] Sandbox violation flag shared via `Rc<Cell<bool>>` (was copied, never observed)
- [x] Run full test suite / clippy for the changes above
- [x] Checkpoint resume hashes the full output (state blob carries the output-so-far); files keyed by task id + fingerprint; write failures logged
- [x] Mesh server hardening: connection cap (64), 60 s per-connection wall-clock budget (enforced across reads), incremental body allocation (no 16 MiB pre-alloc), transient `accept` errors survived
- [x] Honour `deadline_ms` + `CancellationSignal` on workers (cancelled tasks remembered 5 min); `HashCollector::timeout()` applied in `run_network_task` (`Timeout` broadcast to non-repliers)
- [x] Control API: 16 MiB line cap; `SUBMIT` via `spawn_blocking`
- [x] Heartbeat: parallel beacon exchange with 5 s per-peer budget, peer table capped at 512 (stalest evicted), gossiped addrs validated (self/self-id, unspecified/broadcast/multicast rejected)
- [x] Pay workers (every winning voter credited, not only the coordinator); reputation wired into the assemblers (tie-break) + `finish_task` (winners up, Byzantine suspects down); ledger + reputation persist via `[economy] state_file` / `reputation_file`
- [x] Poisoned-mutex `expect`s replaced with poison-tolerant locking (node + discovery); saturating ledger arithmetic; CSPRNG ids (`getrandom`); `0600` identity file on Unix
- [x] Compiler cache: BLAKE3 integrity digest per entry (corrupt/oversize entries discarded), backend feature set in the fingerprint, process-unique tmp names, 64 MiB size cap
- [x] Node identity: Ed25519 keypairs, `NodeId = BLAKE3(pubkey)[..16]`, signed beacons/assignments/results (wire v3), beacon nonce + timestamp replay rejection, persisted seed file (`0600`); explicit `node.id` keeps a legacy unsigned mode

Phase 13 complete.

## Phase 14 — Adoption & usability

- [ ] `examples/`: `hello_quorum`, `two_node_mesh`, `submit_job`, `custom_executor`, `byzantine_demo`
- [ ] `Dockerfile` + `docker-compose.yml` 5-node mesh; sample configs (`hub.toml`, `edge.toml`, `anchor.toml`); `just`/`xtask` shortcuts
- [ ] README Quick Start rewrite (prereqs/MSRV, `cargo install`, two-node demo, expected output, GIF)
- [ ] `clap` CLI: `--version`, `--check-config`, `init`, `status`, `peers`, `submit`, `result`, `cancel`; `MOSAIC_*` env overrides; config search path; config errors with field/line
- [ ] Service packaging (systemd, launchd, Windows) + `cargo-dist` release workflow
- [ ] Repo hygiene: `SECURITY.md`, issue/PR templates, `CODE_OF_CONDUCT.md`, `dependabot.yml`, `deny.toml` + audit job, MSRV job, coverage job, `.kilo/` in `.gitignore`
- [ ] crates.io readiness: READMEs for compiler/economy/node/quorum/sandbox/verify/benches, commit untracked READMEs/CHANGELOGs, `version` on path deps, `docs.rs` metadata, `exclude` list, doctests
- [ ] Black-box tests (`assert_cmd`) for the real node binary; `cargo-fuzz` for proto decoder

## Phase 15 — Observability & APIs

- [ ] `/metrics` (Prometheus), `/healthz`, JSON logs, OpenTelemetry spans
- [ ] `mosaic top` TUI or web dashboard
- [ ] Hardware/thermal/battery auto-detection
- [ ] Config hot-reload + JSON schema for `node.toml`
- [ ] Job lifecycle API (HTTP/gRPC with auth) + `tpt-mosaic-client` SDK crate
- [ ] release-plz, nightly compose soak test, mdBook docs site

## Phase 16 — Innovation backlog

- [ ] Reputation-weighted adaptive quorum + canary tasks
- [ ] Optimistic execution with random re-audit and fraud-proof slashing
- [ ] NAT traversal (STUN/hole punching/relay) + Kademlia DHT
- [ ] Deterministic WASM workload runtime with fuel metering
- [ ] Job templates (`mosaic submit --template ...`)
- [ ] Real Solana/Base/NEAR settlement + "no-chain" signed-receipt mode
- [ ] Deterministic network simulator with fault injection
- [ ] Energy/carbon-aware scheduling
