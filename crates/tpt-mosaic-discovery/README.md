# tpt-mosaic-discovery

> Peer discovery and network topology management for [tpt-mosaic](../../README.md): gossip peer table, TCP mesh transport, mDNS LAN discovery, and a capability-filtered peer directory.

[![CI](https://github.com/tpt-solutions/tpt-mosaic/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/tpt-solutions/tpt-mosaic/actions/workflows/ci.yml)

*By TPT Solutions · Licensed under [MIT](../../LICENSE-MIT) OR [Apache-2.0](../../LICENSE-APACHE)*

---

## Overview

A mosaic node is only useful if it can find peers it can trust with work. This
crate owns that problem in four pieces:

- **`PeerTable`** — the local view of the mesh, with automatic staleness
  eviction.
- **`TcpMesh`** — the stateless v0 transport: one connection per message,
  serving inbound beacons and sending outbound ones.
- **Gossip** — every beacon reply carries a bounded snapshot of the responder's
  peer table, so configuring a single well-connected seed propagates the whole
  view through the mesh.
- **`MeshDht`** — a capability-filtered peer directory implementing
  `DhtClient`, plus `BeaconBroadcaster` / `BeaconScanner` traits reserved for
  the BLE/UWB control plane.

## Features

| Feature | Default | Effect |
|---|---|---|
| `mdns` | off | Zero-config LAN discovery via `mdns-sd` (no system daemon needed) |

## Installation

```toml
[dependencies]
tpt-mosaic-discovery = { path = "crates/tpt-mosaic-discovery", features = ["mdns"] }
```

## Usage

### The peer table

```rust
use std::time::{Duration, Instant};
use tpt_mosaic_core::{CapabilityFlags, CpuArch, GpuVendor, HardwareProfile, NodeId, NodeKind, ThermalState};
use tpt_mosaic_discovery::{PeerRecord, PeerTable};

let mut table = PeerTable::new(Duration::from_secs(30));

table.upsert(PeerRecord {
    node_id: NodeId::from_bytes([2u8; 16]),
    hardware: HardwareProfile {
        kind: NodeKind::AnchorBallast,
        gpu_vendor: GpuVendor::Nvidia,
        npu_present: true,
        cpu_arch: CpuArch::X86_64,
        memory_mb: 65_536,
        battery_level: 255,
        thermal_state: ThermalState::Nominal,
    },
    capabilities: CapabilityFlags::CUDA,
    addr: Some("127.0.0.1:7745".parse().unwrap()),
    last_seen: Instant::now(),
});

assert_eq!(table.live_peers().count(), 1);
// Records older than max_age are dropped on demand:
assert_eq!(table.evict_stale(), 0);
```

`PeerRecord::is_fresh(max_age)` is the liveness predicate used by the table,
the scheduler's candidate filter, and the node's eviction loop.

### TCP mesh

`TcpMesh` is deliberately stateless: `serve` accepts connections on a listener
until an `AtomicBool` is cleared; `exchange` writes one frame and reads one
frame back; `send` writes one frame and closes. The `FrameHandler` trait is
where application logic plugs in — the node's implementation answers a
`HeartbeatBeacon` with a `PeerGossip` snapshot.

### mDNS LAN discovery

```toml
# node.toml
[mesh]
mdns = true
```

With `mdns` enabled the node registers under `_mosaic._udp.local.` and starts
browsing. Discovered addresses are merged into the same beacon/gossip exchange
as seed-configured peers, so a pure mDNS deployment needs **no seeds at all** —
bind the mesh to `0.0.0.0` so LAN peers can reach your address. Dropping the
returned `MdnsHandle` stops the browser thread and unregisters the service
(the TTL expires it regardless).

### Peer directory (DHT)

```rust
use tpt_mosaic_discovery::{answer_query, PeerTable};
use tpt_mosaic_core::{CapabilityFlags, NodeId};
use tpt_mosaic_proto::DhtQuery;

let table = PeerTable::new(std::time::Duration::from_secs(30));
let query = DhtQuery {
    key: [9u8; 32],
    limit: 16,
    capability_filter: CapabilityFlags::CUDA,
};
let gossip = answer_query(&table, &query);
let _ = gossip.peers.len();
```

`answer_query` returns live peers that satisfy the capability filter and are
currently available, capped by `limit` and `MAX_GOSSIP_PEERS`. `MeshDht::advertise`
is best-effort by design: unreachable peers simply miss the update and catch up
on the next exchange.

## Discovery topology

```text
        seed (one well-connected node)
          │ heartbeat + PeerGossip reply
          ▼
    ┌──────────┐        ┌──────────┐
    │  peer A  │◄──────►│  peer B  │     gossip converges the whole table
    └──────────┘        └──────────┘
          ▲
          │ mDNS (_mosaic._udp.local.) — no seed required
          ▼
      LAN peers
```

## Testing

```bash
cargo test -p tpt-mosaic-discovery
cargo test -p tpt-mosaic-discovery --features mdns
```

Multi-node behaviour over real loopback TCP is exercised by the integration
tests in [`tpt-mosaic-node`](../tpt-mosaic-node/README.md).

## Notes and limitations

- The current transport is TCP. BLE/UWB are represented by the
  `BeaconBroadcaster` / `BeaconScanner` traits but are not implemented yet.
- Mesh discovery uses static seed lists plus gossip; there is no
  Kademlia-style key routing yet (planned in the root changelog).

## Links

- Sources: [`dht.rs`](src/dht.rs), [`peer_table.rs`](src/peer_table.rs),
  [`transport.rs`](src/transport.rs), [`mdns.rs`](src/mdns.rs),
  [`traits.rs`](src/traits.rs)
- Release notes: [CHANGELOG.md](CHANGELOG.md)
- API docs: <https://docs.rs/tpt-mosaic-discovery>

## License

Licensed under either of Apache License, Version 2.0 or the MIT license at your
option. Copyright © 2026 TPT Solutions.