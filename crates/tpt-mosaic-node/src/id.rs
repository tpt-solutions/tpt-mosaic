//! Node/task identity generation and hex encoding helpers.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::time::{SystemTime, UNIX_EPOCH};

use tpt_mosaic_core::{NodeId, TaskId};

/// Generate 16 bytes of low-quality randomness by hashing the wall clock under
/// two independently-seeded `RandomState` keys. Sufficient for node/task IDs
/// in the current stub networking layer; replace with OS entropy
/// (`getrandom`) when real peer identity lands.
pub fn generate_bytes() -> [u8; 16] {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);

    let mut h1 = RandomState::new().build_hasher();
    h1.write_u128(nanos);
    let mut h2 = RandomState::new().build_hasher();
    h2.write_u128(nanos.swap_bytes());

    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&h1.finish().to_le_bytes());
    out[8..].copy_from_slice(&h2.finish().to_le_bytes());
    out
}

/// Generate a fresh random [`NodeId`].
pub fn generate_node_id() -> NodeId {
    NodeId::from_bytes(generate_bytes())
}

/// Generate a fresh random [`TaskId`].
pub fn generate_task_id() -> TaskId {
    TaskId::from_bytes(generate_bytes())
}

/// Lowercase hex-encode `bytes`.
pub fn to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Decode an arbitrary-length hex string (optional `0x` prefix) into bytes.
pub fn parse_hex(s: &str) -> Option<Vec<u8>> {
    let body = s.strip_prefix("0x").unwrap_or(s);
    if body.len() % 2 != 0 {
        return None;
    }
    (0..body.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&body[i..i + 2], 16).ok())
        .collect()
}

/// Decode exactly 16 bytes of hex into a node/task ID seed.
pub fn parse_hex16(s: &str) -> Option<[u8; 16]> {
    let bytes = parse_hex(s)?;
    if bytes.len() != 16 {
        return None;
    }
    let mut out = [0u8; 16];
    out.copy_from_slice(&bytes);
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_round_trip() {
        let bytes: [u8; 16] = core::array::from_fn(|i| i as u8 * 17);
        let hex = to_hex(&bytes);
        assert_eq!(hex.len(), 32);
        assert_eq!(parse_hex16(&hex).as_ref(), Some(&bytes));
        assert_eq!(parse_hex16(&format!("0x{hex}")).as_ref(), Some(&bytes));
    }

    #[test]
    fn hex_parse_rejects_garbage() {
        assert!(parse_hex("zz").is_none());
        assert!(parse_hex("abc").is_none()); // odd length
        assert!(parse_hex16("0011").is_none()); // wrong length
    }

    #[test]
    fn generated_ids_are_distinct() {
        let a = generate_bytes();
        let b = generate_bytes();
        assert_ne!(a, b);
        assert_ne!(a, [0u8; 16]);
    }
}
