# Changelog — tpt-mosaic-node

All notable changes to `tpt-mosaic-node` are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions
follow [Semantic Versioning](https://semver.org/). Workspace-wide notes live
in the root [CHANGELOG.md](../../CHANGELOG.md).

## [Unreleased]

### Added

- `[mesh] mdns = true` — zero-config LAN discovery; nodes advertise under
  `_mosaic._udp.local.` and the discovered addresses feed into the existing
  beacon/gossip exchange, so a deployment can run with no seed list at all.
- The `mdns` feature is now enabled unconditionally for this crate (previously
  it had to be requested explicitly).

## [0.1.0] — 2026-01-15

### Added

- The `tpt-mosaic-node` daemon binary, the single entry point that runs on every
  device (edge tile or datacenter anchor).
- `node.toml` configuration with documented defaults in `node.toml.example`;
  sections `[node]`, `[compiler]`, `[hardware]`, `[capabilities]`,
  `[discovery]`, `[mesh]`, `[economy]` and `[control]`, all optional.
- Persistent node identity: a random identity is generated at startup when
  `[node] id` is empty, and stored in `[node] state_file` so restarts keep the
  same identity.
- Heartbeat broadcast and stale-peer eviction loops driven by
  `[discovery] heartbeat_interval_ms` / `peer_max_age_ms`.
- TCP mesh serving with gossip replies, so one configured seed propagates the
  whole peer table.
- The local task lifecycle: shard → compile → sandbox → hash → quorum →
  settlement.
- Mesh-coordinated multi-node quorums with heterogeneous assembly, straggler
  replacement and cancellation fan-out.
- In-process integrated checkpoint/resume path.
- Loopback control API (`STATUS` / `PEERS` / `SUBMIT [best|standard|critical]`
  / `HELP`) with `OK` / `ERR` response prefixes.
- `tracing` + `tracing-subscriber` logging with `env-filter` and JSON support.
- `real-backends` feature building the daemon against all four external
  backends (`tpt-gpu`, `tpt-crucible`, `tpt-archon`, `tpt-eve`).
- Multi-node integration tests over real loopback TCP covering gossip
  convergence, dispatch, quorum evaluation, straggler replacement and
  cancellation.