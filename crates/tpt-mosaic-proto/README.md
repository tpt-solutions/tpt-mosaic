# tpt-mosaic-proto

> MOSA v2 wire protocol and zero-copy serialization for [tpt-mosaic](../../README.md) inter-node communication.

[![CI](https://github.com/tpt-solutions/tpt-mosaic/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/tpt-solutions/tpt-mosaic/actions/workflows/ci.yml)

*By TPT Solutions · Licensed under [MIT](../../LICENSE-MIT) OR [Apache-2.0](../../LICENSE-APACHE)*

---

## Overview

`tpt-mosaic-proto` is the only crate that knows how tpt-mosaic bytes look on
the wire. It defines the six message types the mesh exchanges and a strict,
allocation-bounded frame codec. Anything that wants to talk to a mosaic node —
the scheduler, the TCP mesh, a future BLE/UWB transport, or a third-party
implementation in another language — starts here.

The crate is `no_std` + `alloc` by default; the `std` feature adds the
`Read`/`Write` stream framing helpers used by the TCP transport.

## Features

| Feature | Default | Effect |
|---|---|---|
| `std` | off | `read_frame` / `write_frame` helpers over `std::io::Read` / `Write` |

## Installation

```toml
[dependencies]
tpt-mosaic-proto = { path = "crates/tpt-mosaic-proto", features = ["std"] }
```

## Frame format

Every frame is a fixed 11-byte header followed by a payload. All multi-byte
integers inside payloads are little-endian.

```text
┌────────────┬──────────────┬────────┬──────────────┬───────────────┐
│ magic (4)  │ version (2)  │ tag(1) │ length (4)   │ payload (len) │
│ "MOSA"     │ LE u16 = 2   │ u8     │ LE u32       │               │
└────────────┴──────────────┴────────┴──────────────┴───────────────┘
```

| Constant | Value |
|---|---|
| `WIRE_MAGIC` | `0x4D4F5341` (ASCII `MOSA`), big-endian on the wire |
| `WIRE_VERSION` | `2` |
| `HEADER_LEN` | `11` |
| `MAX_PAYLOAD_LEN` | hard allocation cap (16 MiB) |
| `MAX_GOSSIP_PEERS` | peers carried in one `PeerGossip` |

## Messages

| Tag | Type | Direction | Purpose |
|---|---|---|---|
| 1 | `TaskAssignment` | scheduler → node | Dispatch a shard, carries deadline + coordinator address |
| 2 | `ResultHash` | node → coordinator | BLAKE3/SHA-256 digest of the raw output |
| 3 | `HeartbeatBeacon` | broadcast | Liveness, hardware profile, capabilities, mesh address |
| 4 | `CancellationSignal` | coordinator → workers | Quorum met / client abort / timeout |
| 5 | `DhtQuery` | lookup → directory | Capability-filtered peer query |
| 6 | `PeerGossip` | beacon reply | Bounded peer-table snapshot |

Every message is wrapped in the `WireMessage` enum for encoding.

## Usage

### Encode and decode a frame

```rust
use tpt_mosaic_core::{CapabilityFlags, HardwareProfile, NodeId, TaskId};
use tpt_mosaic_proto::{HeartbeatBeacon, WireMessage, decode, encode};

let beacon = WireMessage::HeartbeatBeacon(HeartbeatBeacon {
    node_id: NodeId::from_bytes([1u8; 16]),
    hardware: HardwareProfile {
        kind: tpt_mosaic_core::NodeKind::EdgeTile,
        gpu_vendor: tpt_mosaic_core::GpuVendor::None,
        npu_present: true,
        cpu_arch: tpt_mosaic_core::CpuArch::Aarch64,
        memory_mb: 8_192,
        battery_level: 77,
        thermal_state: tpt_mosaic_core::ThermalState::Nominal,
    },
    capabilities: CapabilityFlags::NPU | CapabilityFlags::CPU_VECTOR,
    timestamp_ms: 1_700_000_000_000,
    addr: Some("127.0.0.1:7745".parse().unwrap()),
});

let frame = encode(&beacon);
assert_eq!(decode(&frame).unwrap(), beacon);
```

### Stream framing (`std`)

```rust
use tpt_mosaic_proto::{WireMessage, read_frame, write_frame};

// write
let mut stream: Vec<u8> = Vec::new();
write_frame(&mut stream, &WireMessage::HeartbeatBeacon(beacon.clone())).unwrap();

// read exactly one frame back, leaving any following bytes in the reader
let mut cursor = std::io::Cursor::new(stream);
let decoded: WireMessage = read_frame(&mut cursor).unwrap();
assert_eq!(decoded, beacon);
```

`read_frame` uses `read_exact` for the header and body, so it is safe on a
TCP stream carrying several frames back to back.

## Strictness guarantees

`decode` rejects — never panics on — any of:

- frames shorter than `HEADER_LEN`
- bad magic
- a wire version this build does not speak
- unknown message tags
- truncated payloads and trailing bytes after the declared length
- payload lengths above `MAX_PAYLOAD_LEN` (rejected before allocating)
- invalid enum discriminants (e.g. a `NodeKind` that is not 0 or 1)
- semantically invalid content (empty shards, inconsistent shard counts)

These paths are covered by `proptest` round-trip and fuzz-style property tests
that feed arbitrary byte strings to the decoder.

## Schema roadmap

The message types are hand-written in `src/messages.rs` and the codec in
`src/codec.rs`. FlatBuffers `.fbs` schemas under `schemas/` plus a `build.rs`
invoking `flatc` will replace them once the code-gen path is wired up; the
`build-dependencies` block is already stubbed. The public API is intended to
stay stable across that migration.

## Testing

```bash
cargo test -p tpt-mosaic-proto
cargo test -p tpt-mosaic-proto --features std
cargo test -p tpt-mosaic-proto --no-default-features   # no_std + alloc
```

## Links

- Messages source: [`src/messages.rs`](src/messages.rs)
- Codec source: [`src/codec.rs`](src/codec.rs)
- Constants: [`tpt-mosaic-core`](../tpt-mosaic-core/README.md)
- Release notes: [CHANGELOG.md](CHANGELOG.md)
- API docs: <https://docs.rs/tpt-mosaic-proto>

## License

Licensed under either of Apache License, Version 2.0 or the MIT license at your
option. Copyright © 2026 TPT Solutions.