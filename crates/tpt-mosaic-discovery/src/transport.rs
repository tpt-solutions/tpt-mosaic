//! TCP mesh transport: request/response exchange of MOSA frames.
//!
//! Connection-per-message with no session state: a node serves inbound
//! connections through a [`FrameHandler`], and reaches peers with
//! [`TcpMesh::exchange`] (send + await one reply) or [`TcpMesh::send`]
//! (fire-and-forget). Loopback-friendly and deterministic, which makes it
//! suitable for CI integration tests; BLE/UWB/Wi-Fi transports will implement
//! the same handler contract later.

use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tpt_mosaic_proto::{read_frame, write_frame, WireMessage};

/// Handles one inbound frame, optionally producing a single reply frame.
///
/// Handlers run on a dedicated per-connection thread and may block (e.g. a
/// `TaskAssignment` handler that executes the task inline before replying
/// with its [`ResultHash`][tpt_mosaic_proto::ResultHash]).
pub type FrameHandler = Arc<dyn Fn(&WireMessage) -> Option<WireMessage> + Send + Sync>;

/// Read timeout applied to inbound connections, generous enough for inline
/// task execution on the handler thread.
const INBOUND_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// Poll interval for the non-blocking accept loop while shutting down.
const ACCEPT_POLL: Duration = Duration::from_millis(20);

/// Stateless TCP mesh transport.
pub struct TcpMesh;

impl TcpMesh {
    /// Serve inbound connections on `listener` until `running` is cleared.
    ///
    /// Each accepted connection is handled on its own thread; a malformed or
    /// empty exchange is ignored.
    pub fn serve(
        listener: TcpListener,
        handler: FrameHandler,
        running: Arc<AtomicBool>,
    ) -> std::thread::JoinHandle<()> {
        let _ = listener.set_nonblocking(true);
        std::thread::spawn(move || {
            while running.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _peer)) => {
                        let handler = handler.clone();
                        std::thread::spawn(move || handle_connection(stream, handler));
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(ACCEPT_POLL);
                    }
                    Err(_) => break,
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

fn handle_connection(mut stream: TcpStream, handler: FrameHandler) {
    let _ = stream.set_read_timeout(Some(INBOUND_READ_TIMEOUT));
    let request = match read_frame(&mut stream) {
        Ok(msg) => msg,
        Err(_) => return,
    };
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
        });
        let reply = TcpMesh::exchange(addr, &result_hash, Duration::from_secs(2));
        assert_eq!(reply, None);
        running.store(false, Ordering::Relaxed);
    }
}
