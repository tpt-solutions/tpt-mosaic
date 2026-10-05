# tpt-mosaic-node

> The tpt-mosaic daemon — the single entry point that runs on every device and wires all subsystems together.

[![CI](https://github.com/tpt-solutions/tpt-mosaic/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/tpt-solutions/tpt-mosaic/actions/workflows/ci.yml)

*By TPT Solutions · Licensed under [MIT](../../LICENSE-MIT) OR [Apache-2.0](../../LICENSE-APACHE)*

---

## Overview

`tpt-mosaic-node` is the only binary in the workspace. The same binary runs on a
phone in a parked car and on an idle datacenter GPU cluster — the difference is
configuration, not code. It owns the node identity, the heartbeat and eviction
loops, the TCP mesh, and the full task lifecycle:

```text
submit → shard → compile → sandbox → hash → quorum → settle
```

With a `[mesh]` section configured, it also coordinates multi-node quorums
over TCP: assemble a heterogeneous peer set, dispatch `TaskAssignment` frames,
collect `ResultHash` replies, replace stragglers, and cancel the remainder once
K hashes agree.

## Features

| Feature | Default | Effect |
|---|---|---|
| `real-backends` | off | Build against the real `tpt-gpu` / `tpt-crucible` / `tpt-archon` / `tpt-eve` backends instead of the in-crate stubs |

## Installation

```bash
cargo install --path crates/tpt-mosaic-node
# or, against the real upstream backends:
cargo install --path crates/tpt-mosaic-node --features real-backends
```

## Running

```bash
cargo run -p tpt-mosaic-node -- --config node.toml
tpt-mosaic-node --config node.toml     # installed binary
tpt-mosaic-node --help
```

Copy [`node.toml.example`](../../node.toml.example) to `node.toml`; every field
and section is optional, and the example documents the defaults applied when one
is omitted.

### Minimal single-node setup

```toml
[node]
kind = "edge"

[hardware]
npu_present = true
cpu_arch = "aarch64"

[capabilities]
flags = ["npu", "cpu_vector"]
```

Omitting the `[mesh]` section entirely runs the node standalone: it accepts
tasks over the control API and executes them locally as a 1-of-1 quorum.

## Configuration

| Section | Purpose |
|---|---|
| `[node]` | Identity and role (`kind` = `edge` / `anchor`). `state_file` persists the Ed25519 seed; the node id is `BLAKE3(pubkey)[..16]`. An explicit `id` disables signing/verification (legacy mode) |
| `[compiler]` | On-disk JIT cache directory (`cache_dir`; empty disables caching) |
| `[hardware]` | Advertised silicon: `gpu_vendor`, `npu_present`, `cpu_arch`, `memory_mb`, `battery_level`, `thermal_state` |
| `[capabilities]` | Capability flags advertised in beacons: `cuda`, `metal`, `vulkan`, `npu`, `dsp`, `fpga`, `cpu_vector` |
| `[discovery]` | `heartbeat_interval_ms` and `peer_max_age_ms` (eviction window) |
| `[mesh]` | TCP mesh `listen_addr` / `listen_port`, `mdns`, and `seeds` |
| `[economy]` | Settlement `chain` (`solana` / `base` / `near`), optional `rpc_url`, and persistence: `state_file` (ledger balances) / `reputation_file` (peer scores), both rewritten atomically |
| `[task]` | `checkpoint_dir` for resuming interrupted executions (empty disables) |
| `[control]` | Loopback control API `listen_addr` / `listen_port` (`0` disables) |

The presence of `[mesh]` enables the mesh. Set `listen_port = 0` for an
ephemeral port — peers learn the real port from the node's beacons. Bind
`0.0.0.0` when `mdns = true` so LAN peers can reach your address.
## Control API

A line-based protocol on loopback TCP (default `127.0.0.1:7331`). Responses are
prefixed `OK` or `ERR`.

```text
STATUS   → node identity, kind, peer count, counters
PEERS    → live peer IDs as hex
SUBMIT [best|standard|critical] <hex-payload>
         → best runs locally (1-of-1); standard/critical assemble a
           heterogeneous quorum over the mesh (3-of-5 / 7-of-10), dispatch
           the payload to every member, and collect matching BLAKE3 hashes
HELP     → command summary
```

## Multi-node mesh

```text
        ┌──────────────────────────────────────────┐
        │ coordinator: assemble 3-of-5 (diverse HW) │
        └───┬──────────────┬──────────────┬─────────┘
   TaskAssignment   TaskAssignment   TaskAssignment
            │              │              │
        ┌───▼──┐       ┌───▼──┐       ┌───▼──┐
        │edge A│       │edge B│       │anchor│
        └───┬──┘       └───┬──┘       └───┬──┘
            └─────── ResultHash ──────────┘
                     K match → CancellationSignal to stragglers
```

- **Discovery** — on every heartbeat a node trades beacons with its configured
  `seeds`, any mDNS-discovered LAN peers, and all known peers. The reply is a
  **gossip snapshot** of the responder's peer table, so listing one
  well-connected seed propagates the whole view. A pure mDNS deployment needs
  no seeds at all.
- **Dispatch** — the coordinator assembles a heterogeneous quorum from its live
  peer table, sends `TaskAssignment`, and each worker executes in its sandboxed
  backend and replies with a `ResultHash`.
- **Early termination** — once K matching hashes arrive, remaining workers get a
  `CancellationSignal` (spec §3.3).

A hub-and-spoke deployment only needs every node to list the hub as its seed;
gossip spreads the rest.

## Observability

Logging is `tracing` + `tracing-subscriber` with `env-filter` and JSON support:

```bash
RUST_LOG=info tpt-mosaic-node --config node.toml
RUST_LOG=tpt_mosaic_node=debug,tpt_mosaic_discovery=trace tpt-mosaic-node --config node.toml
```

`STATUS` exposes live counters (heartbeats sent, peers known) without attaching
a debugger.

## Testing

```bash
cargo test -p tpt-mosaic-node
cargo test -p tpt-mosaic-node --features real-backends
```

Multi-node integration tests spin up real nodes over loopback TCP and exercise
gossip convergence, dispatch, quorum evaluation, straggler replacement and
cancellation fan-out — see [`src/mesh_integration.rs`](src/mesh_integration.rs).

## Source layout

| File | Responsibility |
|---|---|
| `src/main.rs` | CLI parsing, runtime setup, exit codes |
| `src/config.rs` | `node.toml` parsing and defaults (`ConfigError`) |
| `src/id.rs` | Persistent node identity generation and storage |
| `src/daemon.rs` | Heartbeat / eviction loops, mesh serving, task lifecycle, `Stats` |
| `src/control.rs` | Loopback control API |
| `src/mesh_integration.rs` | Multi-node tests over real TCP |

## Links

- Example config: [`node.toml.example`](../../node.toml.example)
- Design spec: [`spec.txt`](../../spec.txt)
- Workspace overview: [root README](../../README.md)
- Release notes: [CHANGELOG.md](CHANGELOG.md)
- API docs: <https://docs.rs/tpt-mosaic-node>

## License

Licensed under either of Apache License, Version 2.0 or the MIT license at your
option. Copyright © 2026 TPT Solutions.