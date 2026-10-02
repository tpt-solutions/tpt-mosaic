# tpt-mosaic

> **Decentralized ambient compute fabric** — unifying idle edge devices and datacenter infrastructure into a single, self-verifying peer-to-peer supercomputer.

[![CI](https://github.com/tpt-solutions/tpt-mosaic/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/tpt-solutions/tpt-mosaic/actions/workflows/ci.yml)

*By TPT Solutions · Licensed under [MIT](LICENSE-MIT) OR [Apache-2.0](LICENSE-APACHE)*

---

## Overview

tpt-mosaic transforms fragmented idle silicon — cars, robots, phones, IoT devices, and spot-capacity datacenters — into a mathematically secure, globally distributed compute network. It solves Byzantine Fault Tolerance through **dynamic quorum consensus** and **heterogeneous hardware redundancy**.

```
Edge Nodes (Tiles)              Anchor Nodes (Ballast)
┌─────────┐ ┌─────────┐        ┌──────────────────────┐
│  Car    │ │  Robot  │        │   Datacenter (idle)   │
│  NPU    │ │  GPU    │   ◄──► │   GPU cluster (spot)  │
└─────────┘ └─────────┘        └──────────────────────┘
         ▲          ▲                    ▲
         └──────────┴────── QUORUM ──────┘
                  3-of-5 · SHA-256/BLAKE3 hashes
```

## Architecture

| Crate | Role |
|---|---|
| `tpt-mosaic-core` | Shared types, traits, error enums (`no_std`) |
| `tpt-mosaic-proto` | FlatBuffers wire protocol, zero-copy serialization |
| `tpt-mosaic-discovery` | BLE/UWB/Wi-Fi peer discovery, DHT, heartbeat |
| `tpt-mosaic-scheduler` | Quorum assembly, heterogeneous matching, straggler detection |
| `tpt-mosaic-task` | Micro-task splitting (50–100 ms chunks), checkpoint/restore |
| `tpt-mosaic-compiler` | Routes to `tpt-gpu` (CUDA/Metal/Vulkan) or `tpt-crucible` (NPU/DSP/CPU) |
| `tpt-mosaic-sandbox` | `tpt-archon` microkernel interface, RTOS preemption hooks |
| `tpt-mosaic-quorum` | K-of-N consensus, early termination, Byzantine fault detection |
| `tpt-mosaic-verify` | BLAKE3/SHA-256 hashing, ZK-ML proof stubs, `tpt-eve` hooks |
| `tpt-mosaic-economy` | Micro-rewards, reputation, slashing — Solana / Base / NEAR |
| `tpt-mosaic-node` | Main daemon binary: wires all subsystems, runs on every device |

## Quorum Tiers

| Tier | Config | Use Case |
|---|---|---|
| Best Effort | 1-of-1 / 2-of-3 | Casual inference, text summarization |
| Standard | 3-of-5 | Financial analysis, medical screening, autonomous lane changes |
| Mission Critical | 7-of-10+ | High-value smart contracts, surgical robotics |

## Networking Stack

- **BLE / UWB** — control plane: peer discovery, heartbeat, capability advertisement
- **Wi-Fi** — data plane: model shards, task payloads, result uploads
- **4G / 5G** — fallback: task assignments and cryptographic proofs only; raw weights never routed over cellular

## Quick Start

```bash
# Build the full workspace
cargo build --workspace

# Run all tests
cargo test --workspace

# Run the node daemon (requires node.toml — see node.toml.example)
cargo run -p tpt-mosaic-node -- --config node.toml
```

## Control API

The daemon exposes a line-based control API on loopback TCP (default
`127.0.0.1:7331`, configurable under `[control]` in `node.toml`):

```text
STATUS   → node identity, kind, peer count, counters
PEERS    → live peer IDs as hex
SUBMIT [best|standard|critical] <hex-payload>
         → best runs locally (1-of-1); standard/critical assemble a
           heterogeneous quorum over the mesh (3-of-5 / 7-of-10), dispatch
           the payload to every member, and collect matching BLAKE3 hashes
HELP     → command summary
```

Responses are prefixed `OK` / `ERR`.

## Mesh Networking

With a `[mesh]` section in `node.toml`, nodes find each other and run
multi-node quorums over loopback/LAN TCP (MOSA v2 frames):

- **Discovery**: on every heartbeat, a node trades beacons with its configured
  `seeds`, any mDNS-discovered LAN peers (`mdns = true` under `[mesh]`), and
  all known peers. Each beacon carries the sender's identity, hardware
  profile, capabilities, and mesh address; the reply is a **gossip snapshot**
  of the responder's peer table — so listing a single well-connected seed
  propagates the whole view, and a pure mDNS deployment needs no seeds at
  all (bind the mesh to `0.0.0.0` so peers can reach your LAN address).
- **Dispatch**: the coordinator assembles a heterogeneous quorum from its live
  peer table (`tpt-mosaic-scheduler`), sends `TaskAssignment` frames, and each
  worker executes in its sandboxed backend and replies with a `ResultHash`.
- **Early termination**: once K matching hashes arrive, remaining workers get
  a `CancellationSignal` (spec §3.3).

A hub-and-spoke deployment only needs every node to list the hub as its
seed; gossip spreads the rest. See the multi-node integration tests in
`crates/tpt-mosaic-node/src/mesh_integration.rs`.

## Development

```bash
cargo fmt --check
cargo clippy --workspace -- -D warnings
cargo doc --workspace --no-deps
```

See [todo.md](todo.md) for the full phased development checklist.

## External Dependencies

tpt-mosaic integrates with other TPT Solutions crates. All four upstream
repositories are multi-crate workspaces; mosaic depends on the sub-crates
below, pinned to verified revisions and gated behind cargo features (off by
default — enable with `--all-features` or the node's `real-backends` feature):

| Mosaic crate | Feature | Upstream crate | Integration |
|---|---|---|---|
| `tpt-mosaic-compiler` | `gpu` | [`tpt-gpu-runtime`](https://github.com/tpt-solutions/tpt-gpu) | TPTIR text compiled through the tpt-gpu device (`Device::load_module`) |
| `tpt-mosaic-compiler` | `crucible` | [`tpt-crucible-catalyst`](https://github.com/tpt-solutions/tpt-crucible) | Model bytes (SafeTensors/GGUF) lowered to serialized TPT-IR |
| `tpt-mosaic-sandbox` | `archon` | [`tpt-archon-kernel`](https://github.com/tpt-solutions/tpt-archon) | Capability-confined execution: grant-sized page pool, minted capabilities, kernel-scheduler tasks |
| `tpt-mosaic-verify` | `eve` | [`tpt-eve-symbolic`](https://github.com/tpt-solutions/tpt-eve) | Output claims checked for contradictions by tpt-eve's `ConsistencyChecker` |

Without the features, the same APIs run in-crate stubs so the workspace
builds and tests with no network access.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT License ([LICENSE-MIT](LICENSE-MIT))

at your option.

Copyright © 2026 TPT Solutions
