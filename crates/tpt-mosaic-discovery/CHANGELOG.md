# Changelog — tpt-mosaic-discovery

All notable changes to `tpt-mosaic-discovery` are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions
follow [Semantic Versioning](https://semver.org/). Workspace-wide notes live
in the root [CHANGELOG.md](../../CHANGELOG.md).

## [Unreleased]

### Added

- Wi-Fi mDNS zero-config LAN discovery behind the `mdns` feature: nodes
  advertise under `_mosaic._udp.local.` and the discovered addresses feed into
  the existing beacon/gossip exchange. A pure mDNS deployment therefore needs
  no seed list.

### Planned

- Real BLE / UWB control-plane beacon transports implementing the existing
  `BeaconBroadcaster` / `BeaconScanner` traits.
- Key-routed (Kademlia-style) DHT for wide-area indexing, replacing the
  capability-filtered flat directory.

## [0.1.0] — 2026-01-15

### Added

- `PeerTable` with `upsert` / `live_peers` / `evict_stale` / `len` and
  staleness-based eviction.
- `PeerRecord` snapshots (identity, hardware profile, capability flags, mesh
  address, last-seen instant) with `is_fresh`.
- `TcpMesh` stateless transport: `serve` (listener loop until a stop flag),
  `exchange` (one request, one response) and `send` (fire and forget), driven
  through the `FrameHandler` trait.
- Gossip-based peer discovery: a `HeartbeatBeacon` reply carries a bounded
  `PeerGossip` snapshot, so listing one well-connected seed propagates the
  whole view.
- `MeshDht` implementing `DhtClient` over the mesh, plus the free function
  `answer_query` for local capability-filtered lookups.
- `BeaconBroadcaster`, `BeaconScanner` and `DhtClient` traits reserved for the
  future BLE/UWB transports and remote directories.