# Changelog

All notable changes to tpt-mosaic are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Wi-Fi mDNS zero-config LAN discovery (`mdns` feature, `[mesh] mdns = true`):
  nodes advertise under `_mosaic._udp.local.` and feed discovered addresses
  into the beacon/gossip exchange.

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
