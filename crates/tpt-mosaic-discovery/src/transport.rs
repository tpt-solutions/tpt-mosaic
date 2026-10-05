//! TCP mesh transport: request/response exchange of MOSA frames.
//!
//! Connection-per-message with no session state: a node serves inbound
//! connections through a [`FrameHandler`], and reaches peers with
//! [`TcpMesh::exchange`] (send + await one reply) or [`TcpMesh::send`]
//! (fire-and-forget). Loopback-friendly and deterministic, which makes it
//! suitable for CI integration tests; BLE/UWB/Wi-Fi transports will implement
//! the same handler contract later.

use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tpt_mosaic_proto::{read_frame, write_frame, WireMessage};

/// Handles one inbound frame, optionally producing a single reply frame.
///
/// Handlers run on a dedicated per-connection thread and may block (e.g. a
/// `TaskAssignment` handler that executes the task inline before replying
/// with its [`ResultHash`][tpt_mosaic_proto::ResultHash]).
pub type FrameHandler = Arc<dyn Fn(&WireMessage) -> Option<WireMessage> + Send + Sync>;

/// Wall-clock budget for one inbound connection (request read + handler run +
/// reply write). Enforced across every socket operation, so a peer dribbling
/// bytes just under the per-read timeout still hits a hard deadline.
const CONNECTION_BUDGET: Duration = Duration::from_secs(60);

/// Upper bound on concurrently served mesh connections; excess connections
/// are accepted and immediately closed instead of growing the thread count.
const MAX_CONNECTIONS: usize = 64;

/// Poll interval for the non-blocking accept loop while shutting down.
const ACCEPT_POLL: Duration = Duration::from_millis(20);

/// Stateless TCP mesh transport.
pub struct TcpMesh;

impl TcpMesh {
    /// Serve inbound connections on `listener` until `running` is cleared.
    ///
    /// Each accepted connection is handled on its own thread, capped at 64
    /// concurrent handlers; a malformed, slow, or empty exchange is ignored,
    /// and transient `accept` errors never stop the loop.
    pub fn serve(
        listener: TcpListener,
        handler: FrameHandler,
        running: Arc<AtomicBool>,
    ) -> std::thread::JoinHandle<()> {
        let _ = listener.set_nonblocking(true);
        let active = Arc::new(AtomicUsize::new(0));
        std::thread::spawn(move || {
            while running.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, peer)) => {
                        if active.load(Ordering::Relaxed) >= MAX_CONNECTIONS {
                            tracing::debug!(
                                peer = %peer,
                                "mesh connection cap reached; closing inbound connection"
                            );
                            continue; // dropping the stream closes the connection
                        }
                        let handler = handler.clone();
                        let active = active.clone();
                        active.fetch_add(1, Ordering::Relaxed);
                        std::thread::spawn(move || {
                            handle_connection(stream, handler);
                            active.fetch_sub(1, Ordering::Relaxed);
                        });
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(ACCEPT_POLL);
                    }
                    // Transient failures (fd exhaustion, aborted connects)
                    // must not kill the accept loop; back off and retry.
                    Err(e) => {
                        tracing::warn!(error = %e, "mesh accept failed; continuing");
                        std::thread::sleep(ACCEPT_POLL);
                    }
                }
            }
        })
    }

    /// Send `frame` to `addr` and wait up to `timeout` for a single reply.
    ///
    /// Returns `None` when the peer is unreachable, times out, or closes
    /// without replying — call sites treat that as "peer did not answer".
    pub fn exchange(
        addr: SocketAddr,
        frame: &WireMessage,
        timeout: Duration,
    ) -> Option<WireMessage> {
        let mut stream = Self::connect(addr, timeout)?;
        write_frame(&mut stream, frame).ok()?;
        read_frame(&mut stream).ok()
    }

    /// Fire-and-forget send; `true` when the frame was written.
    pub fn send(addr: SocketAddr, frame: &WireMessage, timeout: Duration) -> bool {
        match Self::connect(addr, timeout) {
            Some(mut stream) => write_frame(&mut stream, frame).is_ok(),
            None => false,
        }
    }

    fn connect(addr: SocketAddr, timeout: Duration) -> Option<TcpStream> {
        let stream = TcpStream::connect_timeout(&addr, timeout).ok()?;
        stream.set_read_timeout(Some(timeout)).ok()?;
        stream.set_write_timeout(Some(timeout)).ok()?;
        Some(stream)
    }
}

/// [`std::io::Read`] adapter enforcing an absolute deadline across every
/// socket read: each syscall gets whatever budget remains, so a peer cannot
/// extend the exchange indefinitely by dribbling bytes under the timeout.
struct DeadlineReader<'a> {
    stream: &'a mut TcpStream,
    deadline: Instant,
}

impl std::io::Read for DeadlineReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let remaining = self.deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "mesh connection budget exhausted",
            ));
        }
        self.stream.set_read_timeout(Some(remaining))?;
        self.stream.read(buf)
    }
}

fn handle_connection(mut stream: TcpStream, handler: FrameHandler) {
    let deadline = Instant::now() + CONNECTION_BUDGET;
    let request = {
        let mut reader = DeadlineReader {
            stream: &mut stream,
            deadline,
        };
        match read_frame(&mut reader) {
            Ok(msg) => msg,
            Err(_) => return,
        }
    };
    // The handler may legitimately run long (inline task execution); the
    // reply write gets whatever connection budget is left.
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return;
    }
    let _ = stream.set_write_timeout(Some(remaining));
    if let Some(reply) = handler(&request) {
        let _ = write_frame(&mut stream, &reply);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use tpt_mosaic_proto::{HeartbeatBeacon, ResultHash};

    fn beacon(port: u16) -> WireMessage {
        WireMessage::HeartbeatBeacon(HeartbeatBeacon {
            node_id: tpt_mosaic_core::NodeId::from_bytes([1; 16]),
            hardware: sample_hardware(),
            capabilities: tpt_mosaic_core::CapabilityFlags::CPU_VECTOR,
            timestamp_ms: 0,
            addr: Some(SocketAddr::from(([127, 0, 0, 1], port))),
            nonce: 0,
            pubkey: [0; 32],
            signature: [0; 64],
        })
    }

    fn sample_hardware() -> tpt_mosaic_core::HardwareProfile {
        tpt_mosaic_core::HardwareProfile {
            kind: tpt_mosaic_core::NodeKind::EdgeTile,
            gpu_vendor: tpt_mosaic_core::GpuVendor::None,
            npu_present: false,
            cpu_arch: tpt_mosaic_core::CpuArch::X86_64,
            memory_mb: 8192,
            battery_level: 255,
            thermal_state: tpt_mosaic_core::ThermalState::Nominal,
        }
    }

    fn start_echo_server() -> (SocketAddr, Arc<AtomicBool>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        let running = Arc::new(AtomicBool::new(true));
        let handler: FrameHandler = Arc::new(|msg| match msg {
            // Echo beacons back unchanged (the beacon-exchange contract).
            WireMessage::HeartbeatBeacon(_) => Some(msg.clone()),
            _ => None,
        });
        TcpMesh::serve(listener, handler, running.clone());
        (addr, running)
    }

    #[test]
    fn exchange_round_trips_a_frame() {
        let (addr, running) = start_echo_server();
        let reply = TcpMesh::exchange(addr, &beacon(addr.port()), Duration::from_secs(2));
        assert_eq!(reply, Some(beacon(addr.port())));
        running.store(false, Ordering::Relaxed);
    }

    #[test]
    fn exchange_times_out_on_dead_peer() {
        // Port 1 on loopback is never listening.
        let reply = TcpMesh::exchange(
            SocketAddr::from(([127, 0, 0, 1], 1)),
            &beacon(1),
            Duration::from_millis(250),
        );
        assert_eq!(reply, None);
    }

    #[test]
    fn server_survives_garbage_connections() {
        let (addr, running) = start_echo_server();

        // Raw non-MOSA bytes, then an abrupt close: neither may kill the
        // accept loop or poison later exchanges.
        for _ in 0..3 {
            let mut junk = std::net::TcpStream::connect(addr).expect("connect");
            std::io::Write::write_all(&mut junk, &[0xDE, 0xAD, 0xBE, 0xEF]).unwrap();
            drop(junk);
        }
        // A connection that never sends anything is dropped when the
        // connection budget expires; close it right away instead.
        let idle = std::net::TcpStream::connect(addr).expect("connect idle");
        drop(idle);

        let reply = TcpMesh::exchange(addr, &beacon(addr.port()), Duration::from_secs(2));
        assert_eq!(reply, Some(beacon(addr.port())));
        running.store(false, Ordering::Relaxed);
    }

    #[test]
    fn handler_without_reply_yields_none() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
        let addr = listener.local_addr().expect("local addr");
        let running = Arc::new(AtomicBool::new(true));
        let handler: FrameHandler = Arc::new(|_| None);
        TcpMesh::serve(listener, handler, running.clone());

        let result_hash = WireMessage::ResultHash(ResultHash {
            task_id: tpt_mosaic_core::TaskId::NIL,
            node_id: tpt_mosaic_core::NodeId::NIL,
            hash: [0; 32],
            produced_at_ms: 0,
            pubkey: [0; 32],
            signature: [0; 64],
        });
        let reply = TcpMesh::exchange(addr, &result_hash, Duration::from_secs(2));
        assert_eq!(reply, None);
        running.store(false, Ordering::Relaxed);
    }
}
