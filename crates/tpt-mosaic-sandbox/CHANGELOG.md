# Changelog — tpt-mosaic-sandbox

All notable changes to `tpt-mosaic-sandbox` are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions
follow [Semantic Versioning](https://semver.org/). Workspace-wide notes live
in the root [CHANGELOG.md](../../CHANGELOG.md).

## [Unreleased]

### Planned

- Board-specific RTOS preemption hooks (kill-from-elsewhere wiring) on top of
  the existing CPU/GPU budgets and thermal policy.
- Per-grant filesystem and IPC capability classes.

## [0.1.0] — 2026-01-15

### Added

- `CapabilityGrant` bounding memory, CPU time, GPU time and network access for
  a single execution; `CapabilityGrant::new` denies GPU and network by default
  (`max_gpu_ms = 0`, `network_access = false`).
- `ThermalPolicy` with pause and kill temperature thresholds — the host's right
  to preempt any task instantly.
- `Sandbox::new`, `Sandbox::thermal_policy` and `Sandbox::execute`.
- `archon` feature: genuine capability-confined execution via
  `tpt-archon-kernel`, `tpt-archon-bridge` and `tpt-archon-core` — the grant's
  memory ceiling becomes a page pool on an `InMemoryBlockDevice`, each page is
  accessed only through a minted `Capability` (`map_read` / `map_write`), and
  work runs as a cooperative task on the kernel scheduler. Revoking a
  capability denies all further access (asserted by test).
- Pass-through stub backend with the real call signature so the workspace
  builds and tests without upstream crates.