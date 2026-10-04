# Changelog

All notable changes to tpt-mosaic are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Wi-Fi mDNS zero-config LAN discovery (`mdns` feature, `[mesh] mdns = true`):
  nodes advertise under `_mosaic._udp.local.` and feed discovered addresses
  into the beacon/gossip exchange.

- Worker settlement: the coordinator now pays **every** contributor that
  voted for the winning hash (not just itself) and records reputation for
  all winners and Byzantine suspects.

- Optional economy persistence — `[economy] state_file` (settlement ledger
  balances) and `[economy] reputation_file` (peer reputation scores),
  loaded at startup and rewritten atomically after updates. Balances
  saturate on overflow instead of wrapping.

- `ReputationStore` and `InMemoryLedger` gained `save`/`load` (fixed binary
  format, atomic temp-file writes, corrupt files rejected).

- Scheduler assemblers take a reputation view and prefer higher-reputation
  peers within an equally diverse group; the daemon excludes slashed peers
  from assembly. Node/Task IDs are drawn from the OS CSPRNG (`getrandom`),
  and the identity state file is created `0600` on Unix.

### Changed

- **Breaking (on-disk formats):** checkpoint files are keyed by
  `<task-id>-<fingerprint>` instead of the fingerprint alone, and their
  state blob now carries the output produced so far — a resumed run hashes
  the full output and votes the same digest as an uninterrupted run.
  Compiled-artifact cache entries are now `BLAKE3(artifact) || artifact`;
  raw legacy entries are ignored and recompiled, and fingerprints include
  the compiled-in backend feature set. Old caches/checkpoints are safely
  discarded.

- Mesh server hardening: concurrent-connection cap (64), a hard 60 s
  wall-clock budget per inbound connection (enforced across every socket
  read, so slow-dribble peers cannot extend it), body buffers that grow
  with the bytes actually received (no 16 MiB pre-allocation from a lying
  frame header), and transient `accept` errors no longer stop the accept
  loop.

- Worker honouring: assignments past `deadline_ms` or carrying a cancelled
  task id are refused (cancellation notices are remembered for 5 minutes);
  failed mesh rounds broadcast a `Timeout` cancellation to non-repliers
  via `HashCollector::timeout()`.

- Heartbeat: beacon exchanges run in parallel with a 5 s per-peer budget
  (previously sequential × 30 s); the peer table is capped at 512 entries
  (stalest evicted); gossiped adverts for ourselves and adverts with
  unspecified/broadcast/multicast addresses are ignored.

- Control API: commands are capped at 16 MiB (buffering was previously
  unbounded) and `SUBMIT` runs on the blocking thread pool so it cannot
  stall the async runtime.

- Poisoned mutexes no longer wedge the daemon: shared maps use
  poison-tolerant locking throughout `tpt-mosaic-node` and
  `tpt-mosaic-discovery`.

### Planned

- Networked transports beyond the TCP mesh: BLE / UWB control-plane beacons.
- Key-routed (Kademlia-style) DHT for wide-area indexing.
- Real ZK-ML proof backend behind the existing `ZkProver` trait.
- On-chain settlement adapters with live RPC (Solana / Base / NEAR).
- Benchmark regression thresholds in CI.

## [0.1.0] — initial public state

The first complete vertical slice of the ambient compute fabric: every
subsystem from the design spec is implemented in-tree and exercised by the
test suite, with real external backends where upstream crates allow it.

### Added

- **`tpt-mosaic-core`** — shared foundation: opaque `NodeId`/`TaskId`,
  `QuorumConfig` with tier presets (1-of-1 / 2-of-3 / 3-of-5 / 7-of-10),
  `HardwareProfile` + `CapabilityFlags` + `ThermalState`, the `MosaicError`
  taxonomy, and the `NodeCapability` / `TaskExecutor` / `QuorumParticipant`
  traits. `no_std`-compatible.

- **`tpt-mosaic-proto`** — MOSA v2 wire protocol: length-prefixed binary
  frames (magic + version + tag + payload) for `TaskAssignment`,
  `ResultHash`, `HeartbeatBeacon` (carries mesh address), `CancellationSignal`,
  `DhtQuery`, and `PeerGossip` (peer-table snapshots). Strict decoder —
  bad magic/version, unknown tags, truncation, trailing bytes, invalid enum
  discriminants, and over-cap payloads are all rejected; stream framing
  helpers for TCP. Property-tested to never panic on arbitrary input.

- **`tpt-mosaic-discovery`** — `PeerTable` with staleness eviction; `TcpMesh`
  connection-per-message transport (serve / exchange / send); gossip-based
  peer discovery (beacon replies carry peer-table snapshots, so a single
  seed propagates the whole view); `MeshDht`, a capability-filtered peer
  directory implementing `DhtClient` over the mesh; beacon traits for the
  future BLE/UWB transports.

- **`tpt-mosaic-scheduler`** — `HeterogeneousAssembler` (diversity-first
  anchor-preferring selection) and `BalancedAssembler` (min/max anchor
  policy); capability + availability gating; `DispatchTracker` and
  `StragglerPolicy` for timeout-based straggler detection and 1:1
  replacement budgeting.

- **`tpt-mosaic-task`** — micro-task sharding (64 KiB target) with
  reassembly; disk-persisted `Checkpoint`s and the `TaskProgress` resume
  cursor; `TaskQueue` with FIFO-within-priority ordering and `preempt_below`
  host-side preemption; the §4 cellular data-routing policy
  (`routing_allowed` — model shards and raw weights never on cellular).

- **`tpt-mosaic-compiler`** — backend selection, workload fingerprinting,
  and the universal-fallback chain. Feature-gated real backends:
  `gpu` compiles TPTIR text through `tpt-gpu-runtime` (simulated device;
  upstream `cuda` opt-in); `crucible` lowers SafeTensors/GGUF model bytes to
  serialized TPT-IR via `tpt-crucible-catalyst`. `JitCache` persists
  compiled artifacts keyed by (workload, hardware) fingerprint.

- **`tpt-mosaic-sandbox`** — `CapabilityGrant` + `ThermalPolicy`; with the
  `archon` feature, execution runs inside a `tpt-archon` capability-confined
  memory slice (grant-sized page pool, minted capabilities per page,
  scheduler-run work; revocation denies access — proven by test).

- **`tpt-mosaic-quorum`** — `HashCollector` K-of-N state machine
  (met / diverged / timeout) with Byzantine suspicion flagging, early
  termination, and cancellation-signal construction; property-tested
  invariants.

- **`tpt-mosaic-verify`** — BLAKE3/SHA-256 output hashing (property-tested
  determinism + backend agreement); `ZkProver` trait + stub; `eve` feature
  lowers output claims to provenance/confidence-scored facts and reports
  symbolic contradictions via `tpt-eve-symbolic`.

- **`tpt-mosaic-economy`** — tier- and capability-weighted reward
  calculation; `SlashingRecord`s; `ReputationStore` recording quorum
  outcomes; chain-agnostic `Settlement` trait with ledger-backed Solana /
  Base / NEAR adapters (real RPC pending upstream SDK features).

- **`tpt-mosaic-node`** — the daemon: `node.toml` configuration
  (`node.toml.example`), persistent node identity (`state_file`), heartbeat
  + eviction loops, TCP mesh serving with gossip replies, the local task
  lifecycle (shard → compile → sandbox → hash → quorum → settlement),
  mesh-coordinated multi-node quorums with straggler replacement and
  cancellation fan-out, an in-process integrated checkpoint/resume path,
  and the loopback control API (`STATUS` / `PEERS` /
  `SUBMIT [best|standard|critical]` / `HELP`). The `real-backends` feature
  builds the daemon against all four external backends.

- **CI** — fmt, clippy `-D warnings`, tests on Linux/macOS/Windows, a
  `no_std` feature matrix (`cargo hack`), an external-backends job
  (`--all-features`), release build with benchmark type-check, and
  warning-free docs.

- **Benchmarks** — criterion suite covering the wire codec, task sharding,
  output hashing, quorum evaluation, and quorum assembly.

### Notes

- Mesh networking v0 uses static seed lists; gossip spreads the rest.
  Multi-node quorums (including 3-of-5 with straggler replacement and
  peer-to-peer dispatch after gossip) are covered by integration tests over
  real loopback TCP.
- Upstream fixes contributed back during integration:
  `tpt-solutions/tpt-crucible#fix/crucible-common-compile` (master didn't
  compile on stable rustc; shape-inference repairs) and
  `tpt-solutions/tpt-archon#fix/template-manifest-noise` (template manifest
  poisoned git-dependency resolution). Mosaic pins track those branches
  until merged.
- crates.io publishing is blocked on the upstream `tpt-*` crates publishing
  first: Cargo rejects git dependencies in published packages.
