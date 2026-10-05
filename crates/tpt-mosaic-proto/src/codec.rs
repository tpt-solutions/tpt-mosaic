//! Hand-rolled binary frame codec for tpt-mosaic messages.
//!
//! Frame layout (all integers little-endian unless noted):
//!
//! ```text
//! offset  size  field
//! 0       4     magic "MOSA" (`WIRE_MAGIC` as big-endian bytes)
//! 4       2     wire version (`WIRE_VERSION`, LE)
//! 6       1     message tag (`MessageTag`)
//! 7       4     payload length in bytes (LE)
//! 11      ..    payload
//! ```
//!
//! [`decode`] expects exactly one complete frame with no trailing bytes.

use alloc::vec::Vec;
use core::net::{IpAddr, SocketAddr};
#[cfg(feature = "std")]
use std::io::Read;

use tpt_mosaic_core::{
    CapabilityFlags, CpuArch, GpuVendor, HardwareProfile, MosaicError, NodeId, NodeKind,
    QuorumConfig, TaskId, ThermalState, TierLevel, WIRE_MAGIC, WIRE_VERSION,
};

use crate::messages::{
    CancellationReason, DhtQuery, HeartbeatBeacon, PeerAdvert, PeerGossip, ResultHash,
    TaskAssignment,
};
use crate::CancellationSignal;

/// Upper bound on decoded payload size, so a corrupt length header cannot
/// trigger an absurd allocation on stream reads.
pub const MAX_PAYLOAD_LEN: usize = 16 * 1024 * 1024;

/// Upper bound on the peer count in one [`PeerGossip`] payload, bounding both
/// frame size and parse time.
pub const MAX_GOSSIP_PEERS: usize = 64;

/// Size of the fixed frame header: magic + version + tag + payload length.
pub const HEADER_LEN: usize = 11;

/// Discriminant of the message carried in a frame payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageTag {
    /// Scheduler → node task dispatch.
    TaskAssignment = 1,
    /// Node → coordinator result hash.
    ResultHash = 2,
    /// Periodic liveness and capability advertisement.
    HeartbeatBeacon = 3,
    /// Quorum-met / abort / timeout cancellation.
    CancellationSignal = 4,
    /// DHT peer lookup.
    DhtQuery = 5,
    /// Peer-table snapshot exchanged as a beacon reply.
    PeerGossip = 6,
}

/// Any message type that can travel in a tpt-mosaic frame.
#[derive(Debug, Clone, PartialEq)]
pub enum WireMessage {
    /// Dispatch a task shard to a node.
    TaskAssignment(TaskAssignment),
    /// Submit an execution result hash.
    ResultHash(ResultHash),
    /// Advertise liveness and capabilities.
    HeartbeatBeacon(HeartbeatBeacon),
    /// Tell nodes to stop working on a task.
    CancellationSignal(CancellationSignal),
    /// Query the DHT for peers.
    DhtQuery(DhtQuery),
    /// Advertise a snapshot of the peer table.
    PeerGossip(PeerGossip),
}

/// Encode `msg` into a complete frame.
pub fn encode(msg: &WireMessage) -> Vec<u8> {
    let (tag, payload) = match msg {
        WireMessage::TaskAssignment(m) => (MessageTag::TaskAssignment, encode_task_assignment(m)),
        WireMessage::ResultHash(m) => (MessageTag::ResultHash, encode_result_hash(m)),
        WireMessage::HeartbeatBeacon(m) => (MessageTag::HeartbeatBeacon, encode_heartbeat(m)),
        WireMessage::CancellationSignal(m) => {
            (MessageTag::CancellationSignal, encode_cancellation(m))
        }
        WireMessage::DhtQuery(m) => (MessageTag::DhtQuery, encode_dht_query(m)),
        WireMessage::PeerGossip(m) => (MessageTag::PeerGossip, encode_gossip(m)),
    };

    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.extend_from_slice(&WIRE_MAGIC.to_be_bytes());
    out.extend_from_slice(&WIRE_VERSION.to_le_bytes());
    out.push(tag as u8);
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&payload);
    out
}

/// Decode exactly one frame. Rejects bad magic, wrong wire version, unknown
/// tags, truncated or trailing bytes, and semantically invalid payloads.
pub fn decode(frame: &[u8]) -> Result<WireMessage, MosaicError> {
    if frame.len() < HEADER_LEN {
        return Err(MosaicError::WireProtocolMismatch);
    }
    if frame[0..4] != WIRE_MAGIC.to_be_bytes() {
        return Err(MosaicError::WireProtocolMismatch);
    }
    let version = u16::from_le_bytes([frame[4], frame[5]]);
    if version != WIRE_VERSION {
        return Err(MosaicError::WireProtocolMismatch);
    }
    let tag = frame[6];
    let payload_len = u32::from_le_bytes([frame[7], frame[8], frame[9], frame[10]]) as usize;
    let payload = frame
        .get(HEADER_LEN..)
        .ok_or(MosaicError::WireProtocolMismatch)?;
    if payload.len() != payload_len {
        return Err(MosaicError::WireProtocolMismatch);
    }

    match tag {
        1 => parse_task_assignment(payload).map(WireMessage::TaskAssignment),
        2 => parse_result_hash(payload).map(WireMessage::ResultHash),
        3 => parse_heartbeat(payload).map(WireMessage::HeartbeatBeacon),
        4 => parse_cancellation(payload).map(WireMessage::CancellationSignal),
        5 => parse_dht_query(payload).map(WireMessage::DhtQuery),
        6 => parse_gossip(payload).map(WireMessage::PeerGossip),
        _ => Err(MosaicError::WireProtocolMismatch),
    }
}

/// Read exactly one frame from a byte stream (feature `std`).
///
/// Corrupt headers map to `InvalidData`; payloads larger than
/// [`MAX_PAYLOAD_LEN`] are rejected without allocation. The body buffer grows
/// with the bytes that actually arrive instead of pre-allocating the announced
/// size, so a lying header cannot force a 16 MiB allocation.
#[cfg(feature = "std")]
pub fn read_frame<R: std::io::Read>(reader: &mut R) -> std::io::Result<WireMessage> {
    let mut header = [0u8; HEADER_LEN];
    reader.read_exact(&mut header)?;
    let payload_len = u32::from_le_bytes([header[7], header[8], header[9], header[10]]) as usize;
    if payload_len > MAX_PAYLOAD_LEN {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "frame payload exceeds MAX_PAYLOAD_LEN",
        ));
    }
    let mut frame = alloc::vec![0u8; HEADER_LEN];
    frame.copy_from_slice(&header);
    let read = reader.take(payload_len as u64).read_to_end(&mut frame)?;
    if read != payload_len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "frame payload shorter than announced",
        ));
    }
    decode(&frame).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

/// Write one complete frame to a byte stream (feature `std`).
#[cfg(feature = "std")]
pub fn write_frame<W: std::io::Write>(writer: &mut W, msg: &WireMessage) -> std::io::Result<()> {
    writer.write_all(&encode(msg))
}

// ── Socket address codec (v2) ─────────────────────────────────────────────────

/// Append `addr` as: family byte (`4`/`6`) + octets + `u16` LE port.
fn encode_socket(out: &mut Vec<u8>, addr: &SocketAddr) {
    match addr {
        SocketAddr::V4(v4) => {
            out.push(4);
            out.extend_from_slice(&v4.ip().octets());
        }
        SocketAddr::V6(v6) => {
            out.push(6);
            out.extend_from_slice(&v6.ip().octets());
        }
    }
    out.extend_from_slice(&addr.port().to_le_bytes());
}

/// Parse a socket address at `pos`; returns the address and the end offset.
fn parse_socket(p: &[u8], pos: usize) -> Result<(SocketAddr, usize), MosaicError> {
    let family = *p.get(pos).ok_or(MosaicError::SerializationError)?;
    let end = match family {
        4 => pos + 1 + 4 + 2,
        6 => pos + 1 + 16 + 2,
        _ => return Err(MosaicError::SerializationError),
    };
    if p.len() < end {
        return Err(MosaicError::SerializationError);
    }
    let ip = match family {
        4 => IpAddr::from(
            <[u8; 4]>::try_from(&p[pos + 1..pos + 1 + 4]).expect("bounds checked above"),
        ),
        _ => IpAddr::from(
            <[u8; 16]>::try_from(&p[pos + 1..pos + 1 + 16]).expect("bounds checked above"),
        ),
    };
    let port = u16::from_le_bytes([p[end - 2], p[end - 1]]);
    Ok((SocketAddr::new(ip, port), end))
}

// ── Payload encoders ──────────────────────────────────────────────────────────

fn encode_task_assignment(m: &TaskAssignment) -> Vec<u8> {
    let mut out = Vec::with_capacity(31 + m.payload.len() + 23);
    out.extend_from_slice(m.task_id.as_bytes());
    out.push(m.quorum_config.k);
    out.push(m.quorum_config.n);
    out.push(m.quorum_config.tier as u8);
    out.extend_from_slice(&m.deadline_ms.to_le_bytes());
    out.extend_from_slice(&(m.payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&m.payload);
    encode_socket(&mut out, &m.coordinator);
    out.extend_from_slice(&m.pubkey);
    out.extend_from_slice(&m.signature);
    out
}

fn encode_result_hash(m: &ResultHash) -> Vec<u8> {
    let mut out = Vec::with_capacity(72 + 96);
    out.extend_from_slice(m.task_id.as_bytes());
    out.extend_from_slice(m.node_id.as_bytes());
    out.extend_from_slice(&m.hash);
    out.extend_from_slice(&m.produced_at_ms.to_le_bytes());
    out.extend_from_slice(&m.pubkey);
    out.extend_from_slice(&m.signature);
    out
}

fn encode_heartbeat(m: &HeartbeatBeacon) -> Vec<u8> {
    let mut out = Vec::with_capacity(38 + 23 + 104);
    out.extend_from_slice(m.node_id.as_bytes());
    encode_hardware_block(&mut out, &m.hardware, m.capabilities);
    out.extend_from_slice(&m.timestamp_ms.to_le_bytes());
    match m.addr {
        None => out.push(0),
        Some(ref addr) => {
            encode_socket(&mut out, addr);
        }
    }
    out.extend_from_slice(&m.nonce.to_le_bytes());
    out.extend_from_slice(&m.pubkey);
    out.extend_from_slice(&m.signature);
    out
}

/// Append the 14-byte hardware block: kind, gpu, npu, cpu, memory (u32 LE),
/// battery, thermal, capability flags (u32 LE).
fn encode_hardware_block(
    out: &mut Vec<u8>,
    hardware: &HardwareProfile,
    capabilities: CapabilityFlags,
) {
    out.push(hardware.kind as u8);
    out.push(hardware.gpu_vendor as u8);
    out.push(u8::from(hardware.npu_present));
    out.push(hardware.cpu_arch as u8);
    out.extend_from_slice(&hardware.memory_mb.to_le_bytes());
    out.push(hardware.battery_level);
    out.push(hardware.thermal_state as u8);
    out.extend_from_slice(&capabilities.bits().to_le_bytes());
}

/// Append a peer-table snapshot: `u32` LE count, then per peer: id (16),
/// hardware block (14), address (family-tagged, absent = `0`).
fn encode_gossip(m: &PeerGossip) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + m.peers.len() * 43);
    out.extend_from_slice(&(m.peers.len() as u32).to_le_bytes());
    for advert in &m.peers {
        out.extend_from_slice(advert.node_id.as_bytes());
        encode_hardware_block(&mut out, &advert.hardware, advert.capabilities);
        match advert.addr {
            None => out.push(0),
            Some(ref addr) => {
                encode_socket(&mut out, addr);
            }
        }
    }
    out
}

fn encode_cancellation(m: &CancellationSignal) -> Vec<u8> {
    let mut out = Vec::with_capacity(17);
    out.extend_from_slice(m.task_id.as_bytes());
    out.push(m.reason as u8);
    out
}

fn encode_dht_query(m: &DhtQuery) -> Vec<u8> {
    let mut out = Vec::with_capacity(38);
    out.extend_from_slice(&m.key);
    out.extend_from_slice(&m.limit.to_le_bytes());
    out.extend_from_slice(&m.capability_filter.bits().to_le_bytes());
    out
}

// ── Payload parsers ───────────────────────────────────────────────────────────

fn parse_task_assignment(p: &[u8]) -> Result<TaskAssignment, MosaicError> {
    // 31-byte fixed prefix + smallest coordinator (IPv4) + auth tail (96).
    const MIN_LEN: usize = 31 + 7 + 96;
    if p.len() < MIN_LEN {
        return Err(MosaicError::SerializationError);
    }
    let tier = parse_tier(p[18])?;
    let quorum_config = QuorumConfig::new(p[16], p[17], tier);
    if !quorum_config.is_valid() {
        return Err(MosaicError::SerializationError);
    }
    let payload_len = u32::from_le_bytes(p[27..31].try_into().expect("4 bytes")) as usize;
    let payload_end = 31usize
        .checked_add(payload_len)
        .ok_or(MosaicError::SerializationError)?;
    let payload = p
        .get(31..payload_end)
        .ok_or(MosaicError::SerializationError)?;
    let (coordinator, end) = parse_socket(p, payload_end)?;
    if p.len() != end + 96 {
        return Err(MosaicError::SerializationError);
    }
    Ok(TaskAssignment {
        task_id: TaskId::from_bytes(p[0..16].try_into().expect("16 bytes")),
        quorum_config,
        deadline_ms: u64::from_le_bytes(p[19..27].try_into().expect("8 bytes")),
        payload: payload.to_vec(),
        coordinator,
        pubkey: p[end..end + 32].try_into().expect("32 bytes"),
        signature: p[end + 32..end + 96].try_into().expect("64 bytes"),
    })
}

fn parse_result_hash(p: &[u8]) -> Result<ResultHash, MosaicError> {
    // 72-byte fixed prefix + pubkey (32) + signature (64).
    if p.len() != 72 + 96 {
        return Err(MosaicError::SerializationError);
    }
    Ok(ResultHash {
        task_id: TaskId::from_bytes(p[0..16].try_into().expect("16 bytes")),
        node_id: NodeId::from_bytes(p[16..32].try_into().expect("16 bytes")),
        hash: p[32..64].try_into().expect("32 bytes"),
        produced_at_ms: u64::from_le_bytes(p[64..72].try_into().expect("8 bytes")),
        pubkey: p[72..104].try_into().expect("32 bytes"),
        signature: p[104..168].try_into().expect("64 bytes"),
    })
}

fn parse_heartbeat(p: &[u8]) -> Result<HeartbeatBeacon, MosaicError> {
    // 38-byte fixed prefix + smallest addr tag (1 = absent) + auth tail
    // (nonce 8 + pubkey 32 + signature 64 = 104).
    const TAIL_LEN: usize = 104;
    if p.len() < 38 + 1 + TAIL_LEN {
        return Err(MosaicError::SerializationError);
    }
    let (hardware, capabilities) = parse_hardware_block(p, 16)?;
    let addr = match p.get(38) {
        Some(0) | None => {
            if p.len() != 39 + TAIL_LEN {
                return Err(MosaicError::SerializationError);
            }
            None
        }
        Some(_) => {
            let (addr, end) = parse_socket(p, 38)?;
            if p.len() != end + TAIL_LEN {
                return Err(MosaicError::SerializationError);
            }
            Some(addr)
        }
    };
    let tail = p.len() - TAIL_LEN;
    Ok(HeartbeatBeacon {
        node_id: NodeId::from_bytes(p[0..16].try_into().expect("16 bytes")),
        hardware,
        capabilities,
        timestamp_ms: u64::from_le_bytes(p[30..38].try_into().expect("8 bytes")),
        addr,
        nonce: u64::from_le_bytes(p[tail..tail + 8].try_into().expect("8 bytes")),
        pubkey: p[tail + 8..tail + 40].try_into().expect("32 bytes"),
        signature: p[tail + 40..tail + 104].try_into().expect("64 bytes"),
    })
}

/// Parse the 14-byte hardware block at `pos`.
fn parse_hardware_block(
    p: &[u8],
    pos: usize,
) -> Result<(HardwareProfile, CapabilityFlags), MosaicError> {
    if p.len() < pos + 14 {
        return Err(MosaicError::SerializationError);
    }
    let kind = match p[pos] {
        0 => NodeKind::EdgeTile,
        1 => NodeKind::AnchorBallast,
        _ => return Err(MosaicError::SerializationError),
    };
    let gpu_vendor = match p[pos + 1] {
        0 => GpuVendor::None,
        1 => GpuVendor::Nvidia,
        2 => GpuVendor::Amd,
        3 => GpuVendor::Apple,
        4 => GpuVendor::Intel,
        255 => GpuVendor::Other,
        _ => return Err(MosaicError::SerializationError),
    };
    let npu_present = match p[pos + 2] {
        0 => false,
        1 => true,
        _ => return Err(MosaicError::SerializationError),
    };
    let cpu_arch = match p[pos + 3] {
        0 => CpuArch::X86_64,
        1 => CpuArch::Aarch64,
        2 => CpuArch::RiscV64,
        255 => CpuArch::Other,
        _ => return Err(MosaicError::SerializationError),
    };
    let thermal_state = match p[pos + 9] {
        0 => ThermalState::Nominal,
        1 => ThermalState::Warm,
        2 => ThermalState::Hot,
        3 => ThermalState::Critical,
        _ => return Err(MosaicError::SerializationError),
    };
    let hardware = HardwareProfile {
        kind,
        gpu_vendor,
        npu_present,
        cpu_arch,
        memory_mb: u32::from_le_bytes(p[pos + 4..pos + 8].try_into().expect("4 bytes")),
        battery_level: p[pos + 8],
        thermal_state,
    };
    let capabilities = CapabilityFlags::from_bits_truncate(u32::from_le_bytes(
        p[pos + 10..pos + 14].try_into().expect("4 bytes"),
    ));
    Ok((hardware, capabilities))
}

/// Parse a peer-table snapshot; entries are variable-length (address family),
/// so parsing walks offsets and demands an exact end.
fn parse_gossip(p: &[u8]) -> Result<PeerGossip, MosaicError> {
    if p.len() < 4 {
        return Err(MosaicError::SerializationError);
    }
    let count = u32::from_le_bytes(p[0..4].try_into().expect("4 bytes")) as usize;
    if count > MAX_GOSSIP_PEERS {
        return Err(MosaicError::SerializationError);
    }
    let mut peers = alloc::vec::Vec::with_capacity(count);
    let mut pos = 4usize;
    for _ in 0..count {
        if p.len() < pos + 16 + 14 + 1 {
            return Err(MosaicError::SerializationError);
        }
        let node_id = NodeId::from_bytes(p[pos..pos + 16].try_into().expect("16 bytes"));
        pos += 16;
        let (hardware, capabilities) = parse_hardware_block(p, pos)?;
        pos += 14;
        let addr = match p.get(pos) {
            Some(0) => {
                pos += 1;
                None
            }
            Some(_) => {
                let (addr, end) = parse_socket(p, pos)?;
                pos = end;
                Some(addr)
            }
            None => return Err(MosaicError::SerializationError),
        };
        peers.push(PeerAdvert {
            node_id,
            addr,
            hardware,
            capabilities,
        });
    }
    if p.len() != pos {
        return Err(MosaicError::SerializationError);
    }
    Ok(PeerGossip { peers })
}

fn parse_cancellation(p: &[u8]) -> Result<CancellationSignal, MosaicError> {
    if p.len() != 17 {
        return Err(MosaicError::SerializationError);
    }
    let reason = match p[16] {
        0 => CancellationReason::QuorumMet,
        1 => CancellationReason::ClientAbort,
        2 => CancellationReason::Timeout,
        _ => return Err(MosaicError::SerializationError),
    };
    Ok(CancellationSignal {
        task_id: TaskId::from_bytes(p[0..16].try_into().expect("16 bytes")),
        reason,
    })
}

fn parse_dht_query(p: &[u8]) -> Result<DhtQuery, MosaicError> {
    if p.len() != 38 {
        return Err(MosaicError::SerializationError);
    }
    Ok(DhtQuery {
        key: p[0..32].try_into().expect("32 bytes"),
        limit: u16::from_le_bytes(p[32..34].try_into().expect("2 bytes")),
        capability_filter: CapabilityFlags::from_bits_truncate(u32::from_le_bytes(
            p[34..38].try_into().expect("4 bytes"),
        )),
    })
}

fn parse_tier(b: u8) -> Result<TierLevel, MosaicError> {
    match b {
        0 => Ok(TierLevel::BestEffort),
        1 => Ok(TierLevel::Standard),
        2 => Ok(TierLevel::MissionCritical),
        _ => Err(MosaicError::SerializationError),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::messages::Bytes;
    use alloc::vec;
    use proptest::prelude::*;

    fn sample_beacon() -> HeartbeatBeacon {
        HeartbeatBeacon {
            node_id: NodeId::from_bytes([7; 16]),
            hardware: HardwareProfile {
                kind: NodeKind::AnchorBallast,
                gpu_vendor: GpuVendor::Nvidia,
                npu_present: true,
                cpu_arch: CpuArch::Aarch64,
                memory_mb: 65536,
                battery_level: 255,
                thermal_state: ThermalState::Warm,
            },
            capabilities: CapabilityFlags::CUDA
                | CapabilityFlags::NPU
                | CapabilityFlags::CPU_VECTOR,
            timestamp_ms: 1_700_000_000_123,
            addr: Some(SocketAddr::from(([127, 0, 0, 1], 7745))),
            nonce: 0x0102_0304_0506_0708,
            pubkey: [0x44; 32],
            signature: [0x66; 64],
        }
    }

    fn cancellation(task: u8, reason: CancellationReason) -> WireMessage {
        WireMessage::CancellationSignal(CancellationSignal {
            task_id: TaskId::from_bytes([task; 16]),
            reason,
        })
    }

    #[test]
    fn frame_header_layout() {
        let frame = encode(&cancellation(9, CancellationReason::QuorumMet));
        assert_eq!(&frame[0..4], b"MOSA");
        assert_eq!(&frame[4..6], &WIRE_VERSION.to_le_bytes());
        assert_eq!(frame[6], MessageTag::CancellationSignal as u8);
        assert_eq!(&frame[7..11], &17u32.to_le_bytes()); // 16 id bytes + 1 reason byte
        assert_eq!(frame.len(), HEADER_LEN + 17);
    }

    #[test]
    fn round_trip_all_messages() {
        let payload: Bytes = (0..300).map(|_| 0xABu8).collect();
        let messages = vec![
            WireMessage::TaskAssignment(TaskAssignment {
                task_id: TaskId::from_bytes([1; 16]),
                quorum_config: QuorumConfig::STANDARD_3_OF_5,
                payload,
                deadline_ms: 1_700_000_100_000,
                coordinator: SocketAddr::from(([192, 168, 1, 7], 7331)),
                pubkey: [0x11; 32],
                signature: [0x22; 64],
            }),
            WireMessage::ResultHash(ResultHash {
                task_id: TaskId::from_bytes([2; 16]),
                node_id: NodeId::from_bytes([3; 16]),
                hash: [0x55; 32],
                produced_at_ms: 42,
                pubkey: [0x33; 32],
                signature: [0x44; 64],
            }),
            WireMessage::HeartbeatBeacon(sample_beacon()),
            cancellation(4, CancellationReason::Timeout),
            WireMessage::DhtQuery(DhtQuery {
                key: [0xEE; 32],
                limit: 20,
                capability_filter: CapabilityFlags::VULKAN,
            }),
        ];
        for msg in messages {
            assert_eq!(decode(&encode(&msg)).unwrap(), msg);
        }
    }

    #[test]
    fn address_variants_round_trip() {
        // Beacon without a mesh address (listener disabled).
        let mut beacon = sample_beacon();
        beacon.addr = None;
        assert_eq!(
            decode(&encode(&WireMessage::HeartbeatBeacon(beacon))).unwrap(),
            WireMessage::HeartbeatBeacon(beacon)
        );

        // IPv6 coordinator on an assignment.
        let assignment = WireMessage::TaskAssignment(TaskAssignment {
            task_id: TaskId::from_bytes([5; 16]),
            quorum_config: QuorumConfig::BEST_EFFORT_2_OF_3,
            payload: Bytes::new(),
            deadline_ms: 0,
            coordinator: "[::1]:7331".parse().expect("valid v6 socket addr"),
            pubkey: [0; 32],
            signature: [0; 64],
        });
        assert_eq!(decode(&encode(&assignment)).unwrap(), assignment);
    }

    #[test]
    fn gossip_round_trip_and_bounds() {
        let hardware = sample_beacon().hardware;
        let gossip = WireMessage::PeerGossip(PeerGossip {
            peers: vec![
                PeerAdvert {
                    node_id: NodeId::from_bytes([1; 16]),
                    addr: Some(SocketAddr::from(([10, 0, 0, 7], 7801))),
                    hardware,
                    capabilities: CapabilityFlags::CUDA,
                },
                PeerAdvert {
                    node_id: NodeId::from_bytes([2; 16]),
                    addr: None,
                    hardware,
                    capabilities: CapabilityFlags::empty(),
                },
            ],
        });
        assert_eq!(decode(&encode(&gossip)).unwrap(), gossip);

        let empty = WireMessage::PeerGossip(PeerGossip { peers: vec![] });
        assert_eq!(decode(&encode(&empty)).unwrap(), empty);

        // A declared count above the cap is rejected without allocating.
        let mut frame = encode(&empty);
        frame[HEADER_LEN..HEADER_LEN + 4]
            .copy_from_slice(&(MAX_GOSSIP_PEERS as u32 + 1).to_le_bytes());
        assert_eq!(decode(&frame), Err(MosaicError::SerializationError));

        // A truncated entry is rejected (exact-end check).
        let mut frame = encode(&gossip);
        let plen = u32::from_le_bytes(frame[7..11].try_into().expect("4 bytes")) - 1;
        frame[7..11].copy_from_slice(&plen.to_le_bytes());
        frame.truncate(frame.len() - 1);
        assert_eq!(decode(&frame), Err(MosaicError::SerializationError));
    }

    #[cfg(feature = "std")]
    #[test]
    fn stream_frame_round_trip_and_size_cap() {
        let msg = sample_assignment();
        let mut buf = Vec::new();
        write_frame(&mut buf, &msg).expect("write must succeed");
        let decoded = read_frame(&mut std::io::Cursor::new(&buf)).expect("read must succeed");
        assert_eq!(decoded, msg);

        // Corrupt the payload length beyond the cap -> rejected without alloc.
        let mut evil = encode(&msg);
        evil[7..11].copy_from_slice(&u32::MAX.to_le_bytes());
        let err = read_frame(&mut std::io::Cursor::new(&evil)).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);

        // Truncated stream -> UnexpectedEof from read_exact.
        let err = read_frame(&mut std::io::Cursor::new(&buf[..buf.len() - 1])).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);

        // A header announcing more payload than the stream delivers is
        // rejected without pre-allocating the announced size.
        let mut short = buf.clone();
        let plen = u32::from_le_bytes(short[7..11].try_into().expect("4 bytes"));
        short[7..11].copy_from_slice(&(plen + 1).to_le_bytes());
        let err = read_frame(&mut std::io::Cursor::new(&short)).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    fn sample_assignment() -> WireMessage {
        WireMessage::TaskAssignment(TaskAssignment {
            task_id: TaskId::from_bytes([1; 16]),
            quorum_config: QuorumConfig::STANDARD_3_OF_5,
            payload: Bytes::new(),
            deadline_ms: 0,
            coordinator: SocketAddr::from(([127, 0, 0, 1], 7331)),
            pubkey: [0; 32],
            signature: [0; 64],
        })
    }

    #[test]
    fn empty_payload_round_trips() {
        let assignment = WireMessage::TaskAssignment(TaskAssignment {
            task_id: TaskId::from_bytes([1; 16]),
            quorum_config: QuorumConfig::BEST_EFFORT_1_OF_1,
            payload: Bytes::new(),
            deadline_ms: 0,
            coordinator: SocketAddr::from(([127, 0, 0, 1], 7331)),
            pubkey: [0; 32],
            signature: [0; 64],
        });
        assert_eq!(decode(&encode(&assignment)).unwrap(), assignment);
    }

    #[test]
    fn rejects_bad_magic_and_version() {
        let mut frame = encode(&cancellation(0, CancellationReason::QuorumMet));
        frame[0] = b'X';
        assert_eq!(decode(&frame), Err(MosaicError::WireProtocolMismatch));

        let mut frame = encode(&cancellation(0, CancellationReason::QuorumMet));
        frame[4] = 0xFF; // corrupts the wire version
        assert_eq!(decode(&frame), Err(MosaicError::WireProtocolMismatch));
    }

    #[test]
    fn rejects_unknown_tag_truncated_and_trailing() {
        let frame = encode(&WireMessage::DhtQuery(DhtQuery {
            key: [0; 32],
            limit: 1,
            capability_filter: CapabilityFlags::empty(),
        }));

        let mut unknown = frame.clone();
        unknown[6] = 200;
        assert_eq!(decode(&unknown), Err(MosaicError::WireProtocolMismatch));

        let truncated = &frame[..frame.len() - 1];
        assert_eq!(decode(truncated), Err(MosaicError::WireProtocolMismatch));

        let mut trailing = frame.clone();
        trailing.push(0);
        assert_eq!(decode(&trailing), Err(MosaicError::WireProtocolMismatch));

        assert_eq!(decode(&[]), Err(MosaicError::WireProtocolMismatch));
    }

    #[test]
    fn rejects_semantically_invalid_payloads() {
        // k > n fails QuorumConfig::is_valid.
        let assignment = sample_assignment();
        let mut frame = encode(&assignment);
        frame[HEADER_LEN + 16] = 6; // k = 6, n = 5
        assert_eq!(decode(&frame), Err(MosaicError::SerializationError));

        // Unknown TierLevel discriminant.
        let mut frame = encode(&assignment);
        frame[HEADER_LEN + 18] = 9;
        assert_eq!(decode(&frame), Err(MosaicError::SerializationError));

        // Unknown socket-address family on the coordinator (family byte sits
        // 7 bytes from the end of the address block, followed by the 96-byte
        // auth tail).
        let mut frame = encode(&assignment);
        let family_at = frame.len() - 7 - 96;
        frame[family_at] = 9;
        assert_eq!(decode(&frame), Err(MosaicError::SerializationError));

        // Unknown NodeKind discriminant.
        let mut frame = encode(&WireMessage::HeartbeatBeacon(sample_beacon()));
        frame[HEADER_LEN + 16] = 7;
        assert_eq!(decode(&frame), Err(MosaicError::SerializationError));

        // Truncated beacon address (presence byte claims an address): fix up
        // the declared payload length so the frame-level check passes and the
        // payload parser's own bounds check is what rejects it.
        let mut frame = encode(&WireMessage::HeartbeatBeacon(sample_beacon()));
        let plen = u32::from_le_bytes(frame[7..11].try_into().expect("4 bytes")) - 1;
        frame[7..11].copy_from_slice(&plen.to_le_bytes());
        frame.truncate(frame.len() - 1);
        assert_eq!(decode(&frame), Err(MosaicError::SerializationError));

        // Unknown CancellationReason discriminant.
        let mut frame = encode(&cancellation(0, CancellationReason::QuorumMet));
        frame[HEADER_LEN + 16] = 42;
        assert_eq!(decode(&frame), Err(MosaicError::SerializationError));
    }

    proptest! {
        /// `decode` must never panic on arbitrary input — worst case is an error.
        #[test]
        fn decode_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..256)) {
            let _ = decode(&bytes);
        }
    }
}
