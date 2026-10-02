# tpt-mosaic — Development Roadmap

Phased checklist mirroring [spec.txt](spec.txt). Checked items are implemented
in-tree; unchecked items are pending. Stubbed integration points are marked.

## Phase 0 — Workspace Scaffolding

- [x] Cargo workspace with 11 crates, shared profiles, pinned toolchain (rustfmt + clippy)
- [x] CI: fmt, clippy `-D warnings`, tests on Linux/macOS/Windows, release build, docs `-D warnings`
- [x] MIT OR Apache-2.0 dual licensing, README, design spec

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
- [ ] Wi-Fi mDNS peer detection
- [ ] DHT integration for global node indexing

## Phase 4 — `tpt-mosaic-scheduler`

- [x] `HeterogeneousAssembler`: diversity-first greedy quorum selection
- [x] Capability filtering + availability gating (thermal / battery)
- [x] Straggler detection (`DispatchTracker`) and 1:1 replacement budgeting
- [x] Timeout policy engine (`StragglerPolicy`)
- [x] Networked dispatch wired: assembler selects live peers, mesh RPC bounded by timeouts
- [x] `DispatchTracker` + `StragglerPolicy` wired into mesh dispatch: failed/missing members are replaced 1:1 from the spare pool (tested with a dead anchor member in a 5-of-5 round)
- [ ] Edge/anchor ratio balancing policy

## Phase 5 — `tpt-mosaic-task`

- [x] Micro-task splitting (shards) and reassembly
- [x] `Checkpoint` type
- [ ] Checkpoint/restore wiring into the executor
- [ ] Priority queue with preemption
- [ ] Data-routing guard: raw weights never routed over cellular

## Phase 6 — `tpt-mosaic-compiler`

- [x] Backend selection (`tpt-gpu` vs `tpt-crucible`) + workload fingerprinting + fallback chain
- [x] `tpt-gpu-runtime` integration (`gpu` feature): TPTIR text compiled via `Device::load_module`
- [x] `tpt-crucible-catalyst` integration (`crucible` feature): SafeTensors/GGUF lowered to TPT-IR
- [ ] Real CUDA/Metal/Vulkan hardware paths (upstream `cuda` feature, opt-in)
- [ ] On-disk JIT binary cache keyed by fingerprint

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
- [ ] Persistent node identity across restarts
- [ ] systemd / launchd packaging

## Phase 12 — Hardening & Release

- [x] Dependency audit (removed unused `tokio`/`tracing`/`flatbuffers`/`proto` deps; re-add per roadmap)
- [ ] Fuzzing (`cargo-fuzz`) for proto decoding and the quorum state machine
- [ ] Benchmark suite (criterion) for sharding, hashing, quorum evaluation
- [ ] `no_std` feature-combination matrix (`cargo-hack`) in CI
- [ ] v0.1.0 release + crates.io publish
