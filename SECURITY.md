# Security Policy

## Supported versions

tpt-mosaic is pre-1.0 and only the latest `master` receives security fixes.
Please run the most recent commit or release when reporting issues.

## Reporting a vulnerability

**Do not open a public issue for a security vulnerability.**

Report privately via [GitHub security advisories](
https://github.com/tpt-solutions/tpt-mosaic/security/advisories/new)
(“Report a vulnerability”), or email `security@tpt-solutions.dev` with:

- a description of the issue and its impact,
- the commit or version affected,
- steps or a proof-of-concept to reproduce.

You will receive an acknowledgement within 72 hours. We will coordinate a fix
and disclosure timeline with you and credit you in the release notes unless
you prefer to remain anonymous.

## Scope notes

tpt-mosaic executes work contributed by peers on the mesh. The current
release includes the following relevant protections; treat their limits as
in scope for security review:

- **Sandboxing** — task execution runs in a capability-confined sandbox
  (`tpt-mosaic-sandbox`, `tpt-archon`); the default stub workload path is not
  a general-purpose sandbox for arbitrary code.
- **Transport authentication** — mesh frames (beacons, assignments, result
  hashes) are Ed25519-signed (wire v3) with `NodeId = BLAKE3(pubkey)[..16]`,
  nonce and timestamp replay protection, connection caps, and per-connection
  wall-clock budgets on every served socket.
- **Result validation** — K-of-N hash agreement with Byzantine suspicion
  flagging; there is no ZK-ML proof verification yet (the `ZkProver` hook is
  a stub).

## Known gaps (pre-1.0)

- Reputation, slashing, and ledger records are local to a node (or its
  configured state file); on-chain settlement adapters are stubs.
- Gossip peer-table entries (`PeerGossip`) are second-hand announcements and
  are not themselves signed.
- No transport encryption yet — loopback/LAN data plane is plaintext TCP.
