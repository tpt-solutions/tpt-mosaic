//! Local TCP control API for the host OS.
//!
//! Line-based request/response over a loopback TCP socket:
//!
//! ```text
//! STATUS   → node identity, kind, peer count, counters
//! PEERS    → live peer IDs as hex
//! SUBMIT [best|standard|critical] <hex-payload>
//!          → runs the full local task lifecycle, returns the agreed hash
//! HELP     → command summary
//! ```
//!
//! Responses are prefixed `OK` / `ERR` for trivial machine parsing.

use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::watch;

use tpt_mosaic_core::QuorumConfig;

use crate::daemon::NodeDaemon;
use crate::id;

/// Upper bound on one control command in bytes. A hex payload for SUBMIT is
/// double its binary size, so this still admits every payload the mesh could
/// carry (16 MiB binary) while a client cannot force unbounded buffering.
const MAX_COMMAND_BYTES: usize = 16 * 1024 * 1024;

/// Accept connections until shutdown is signalled.
pub(crate) async fn serve(
    daemon: Arc<NodeDaemon>,
    listener: TcpListener,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            _ = shutdown.changed() => break,
            accepted = listener.accept() => match accepted {
                Ok((stream, peer)) => {
                    tracing::debug!(%peer, "control connection opened");
                    tokio::spawn(handle_connection(daemon.clone(), stream));
                }
                Err(e) => {
                    tracing::warn!(error = %e, "control accept failed");
                    break;
                }
            }
        }
    }
}

async fn handle_connection(daemon: Arc<NodeDaemon>, stream: tokio::net::TcpStream) {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    loop {
        let line = match next_line_capped(&mut reader, MAX_COMMAND_BYTES).await {
            Ok(Some(line)) => line,
            Ok(None) => break,
            Err(ControlReadError::Oversize) => {
                let _ = writer.write_all(b"ERR command too long\n").await;
                break;
            }
            Err(ControlReadError::Io(e)) => {
                tracing::debug!(error = %e, "control connection read failed");
                break;
            }
        };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        // SUBMIT runs a whole task round (local execution or a mesh quorum)
        // and can block for seconds: offload it so the async runtime's
        // worker threads stay free.
        let response = if trimmed.starts_with("SUBMIT") {
            let daemon = daemon.clone();
            let command = trimmed.to_owned();
            tokio::task::spawn_blocking(move || handle_command(&daemon, &command))
                .await
                .unwrap_or_else(|e| format!("ERR submit worker failed: {e}"))
        } else {
            handle_command(&daemon, trimmed)
        };
        if writer
            .write_all(format!("{response}\n").as_bytes())
            .await
            .is_err()
        {
            break;
        }
    }
}

enum ControlReadError {
    /// The peer sent more than the command cap without a newline.
    Oversize,
    Io(std::io::Error),
}

/// Read one newline-terminated line, giving up once `max` bytes have been
/// buffered (unlike [`AsyncBufReadExt::read_line`], which grows forever).
async fn next_line_capped(
    reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
    max: usize,
) -> Result<Option<String>, ControlReadError> {
    let mut line = Vec::new();
    loop {
        let available = reader.fill_buf().await.map_err(ControlReadError::Io)?;
        if available.is_empty() {
            // EOF: a final unterminated fragment is not a command.
            return Ok(None);
        }
        match available.iter().position(|&b| b == b'\n') {
            Some(i) => {
                line.extend_from_slice(&available[..i]);
                reader.consume(i + 1);
                return Ok(Some(String::from_utf8_lossy(&line).into_owned()));
            }
            None => {
                let n = available.len();
                line.extend_from_slice(available);
                reader.consume(n);
                if line.len() > max {
                    return Err(ControlReadError::Oversize);
                }
            }
        }
    }
}

/// Execute one control command. Synchronous and side-effect-bounded, so it is
/// directly unit-testable without a socket.
fn handle_command(daemon: &NodeDaemon, line: &str) -> String {
    let mut parts = line.split_whitespace();
    match parts.next() {
        Some("HELP") => {
            "OK commands: STATUS | PEERS | SUBMIT [best|standard|critical] <hex-payload> | HELP"
                .to_owned()
        }
        Some("STATUS") => {
            let stats = daemon.stats();
            format!(
                "OK node={} kind={:?} peers={} heartbeats={} tasks_completed={} tasks_failed={}",
                id::to_hex(daemon.node_id().as_bytes()),
                daemon.node_kind(),
                daemon.peer_ids().len(),
                stats.heartbeats_sent,
                stats.tasks_completed,
                stats.tasks_failed,
            )
        }
        Some("PEERS") => {
            let ids = daemon.peer_ids();
            if ids.is_empty() {
                "OK peers=(none)".to_owned()
            } else {
                let hexes: Vec<String> = ids.iter().map(|n| id::to_hex(n.as_bytes())).collect();
                format!("OK peers={}", hexes.join(","))
            }
        }
        Some("SUBMIT") => handle_submit(daemon, parts.collect()),
        Some(other) => format!("ERR unknown command '{other}' (try HELP)"),
        None => "ERR empty command".to_owned(),
    }
}

fn handle_submit(daemon: &NodeDaemon, args: Vec<&str>) -> String {
    if args.is_empty() {
        return "ERR usage: SUBMIT [best|standard|critical] <hex-payload>".to_owned();
    }

    // `best` executes locally at 1-of-1; `standard`/`critical` coordinate a
    // multi-node quorum over the mesh (3-of-5 / 7-of-10).
    let (quorum, mesh, payload_hex) = match args[0] {
        "best" => (QuorumConfig::BEST_EFFORT_1_OF_1, false, &args[1..]),
        "standard" => (QuorumConfig::STANDARD_3_OF_5, true, &args[1..]),
        "critical" => (QuorumConfig::MISSION_CRITICAL_7_OF_10, true, &args[1..]),
        _ => (QuorumConfig::BEST_EFFORT_1_OF_1, false, &args[..]),
    };
    let payload_hex = payload_hex.join("");
    if payload_hex.is_empty() {
        return "ERR usage: SUBMIT [best|standard|critical] <hex-payload>".to_owned();
    }
    let payload = match id::parse_hex(&payload_hex) {
        Some(bytes) if !bytes.is_empty() => bytes,
        _ => return "ERR payload must be non-empty hex".to_owned(),
    };

    let result = if mesh {
        daemon.run_network_task(&payload, quorum)
    } else {
        daemon.run_local_task(&payload, quorum)
    };
    match result {
        Ok(receipt) => format!(
            "OK task={} hash={} confirmations={} reward={} shards={}",
            id::to_hex(receipt.task_id.as_bytes()),
            id::to_hex(&receipt.agreed_hash),
            receipt.confirmations,
            receipt.reward,
            receipt.shards,
        ),
        Err(e) => format!("ERR {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::NodeConfig;
    use crate::daemon::NodeDaemon;

    fn daemon() -> NodeDaemon {
        NodeDaemon::new(NodeConfig::from_toml_str("").unwrap())
    }

    #[test]
    fn help_and_unknown_commands() {
        let d = daemon();
        assert!(handle_command(&d, "HELP").starts_with("OK "));
        assert!(handle_command(&d, "FROB").starts_with("ERR "));
        assert!(handle_command(&d, "").starts_with("ERR "));
    }

    #[test]
    fn status_reports_counters() {
        let d = daemon();
        let status = handle_command(&d, "STATUS");
        assert!(status.starts_with("OK node="), "got: {status}");
        assert!(status.contains("peers=0"));
    }

    #[test]
    fn submit_best_effort_returns_hash() {
        let d = daemon();
        let response = handle_command(&d, "SUBMIT deadbeef");
        assert!(response.starts_with("OK task="), "got: {response}");
        assert!(response.contains("confirmations=1"));
    }

    #[test]
    fn submit_standard_tier_requires_mesh() {
        // The default test daemon has no mesh listener, so a coordinated
        // quorum cannot even be attempted.
        let d = daemon();
        let response = handle_command(&d, "SUBMIT standard deadbeef");
        assert!(response.starts_with("ERR node "), "got: {response}");
    }

    #[test]
    fn submit_rejects_bad_payload() {
        let d = daemon();
        assert!(handle_command(&d, "SUBMIT").starts_with("ERR usage:"));
        assert!(handle_command(&d, "SUBMIT zz").starts_with("ERR payload"));
    }

    #[test]
    fn peers_starts_empty() {
        let d = daemon();
        assert_eq!(handle_command(&d, "PEERS"), "OK peers=(none)");
    }
}
