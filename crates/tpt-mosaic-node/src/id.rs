//! Node/task identity generation, Ed25519 key management, and hex helpers.

use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use tpt_mosaic_core::{NodeId, TaskId};

/// Generate 16 bytes from the operating system's CSPRNG. Node and task IDs
/// are security-relevant (they gate quorum membership and checkpoint paths),
/// so they never fall back to time- or address-derived pseudo-entropy.
///
/// Panics only if the OS entropy source is broken — effectively never on
/// supported platforms.
pub fn generate_bytes() -> [u8; 16] {
    let mut out = [0u8; 16];
    getrandom::fill(&mut out).expect("OS entropy source is unavailable");
    out
}

/// Generate a fresh random [`TaskId`].
pub fn generate_task_id() -> TaskId {
    TaskId::from_bytes(generate_bytes())
}

/// A node's cryptographic identity: an Ed25519 signing key whose public-key
/// hash *is* the [`NodeId`] (`NodeId = BLAKE3(pubkey)[..16]`), so a claimed
/// identity can always be checked against the key that signed for it.
#[derive(Debug, Clone)]
pub struct Identity {
    signing_key: SigningKey,
    node_id: NodeId,
}

impl Identity {
    /// Derive an identity from an existing signing key.
    pub fn from_signing_key(signing_key: SigningKey) -> Self {
        let node_id = node_id_for_pubkey(&signing_key.verifying_key().to_bytes());
        Self {
            signing_key,
            node_id,
        }
    }

    /// Generate a fresh identity from OS entropy.
    pub fn generate() -> Self {
        let mut seed = [0u8; 32];
        getrandom::fill(&mut seed).expect("OS entropy source is unavailable");
        Self::from_signing_key(SigningKey::from_bytes(&seed))
    }

    /// Load a persisted identity from `path` (64 hex characters: the Ed25519
    /// seed), generating and storing a fresh one when the file is missing or
    /// unreadable. Best-effort: if the write fails, the generated identity is
    /// still returned — it just will not survive restarts.
    ///
    /// A file holding a legacy 16-byte identity (pre-Ed25519 format) is
    /// treated as absent: a fresh keypair is generated and overwrites it.
    pub fn load_or_create(path: &std::path::Path) -> std::io::Result<Self> {
        if let Ok(text) = std::fs::read_to_string(path) {
            if let Some(seed) = parse_hex32(text.trim()) {
                return Ok(Self::from_signing_key(SigningKey::from_bytes(&seed)));
            }
        }
        let identity = Self::generate();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        write_private(path, &to_hex(&identity.signing_key.to_bytes()))?;
        Ok(identity)
    }

    /// This identity's node id: `BLAKE3(pubkey)[..16]`.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// The Ed25519 verifying (public) key bytes.
    pub fn pubkey(&self) -> [u8; 32] {
        self.signing_key.verifying_key().to_bytes()
    }

    /// Sign `msg` (arbitrary wire bytes).
    pub fn sign(&self, msg: &[u8]) -> Signature {
        self.signing_key.sign(msg)
    }

    /// Verify `msg` against `pubkey` (strict: rejects malleable signatures).
    pub fn verify(pubkey: &[u8; 32], msg: &[u8], signature: &[u8; 64]) -> bool {
        let Ok(key) = VerifyingKey::from_bytes(pubkey) else {
            return false;
        };
        let Ok(signature) = Signature::from_slice(signature) else {
            return false;
        };
        key.verify_strict(msg, &signature).is_ok()
    }
}

/// [`NodeId`] bound to an Ed25519 public key: the first 16 bytes of
/// `BLAKE3(pubkey)`.
pub fn node_id_for_pubkey(pubkey: &[u8; 32]) -> NodeId {
    let hash = blake3::hash(pubkey);
    let mut out = [0u8; 16];
    out.copy_from_slice(&hash.as_bytes()[..16]);
    NodeId::from_bytes(out)
}

/// Write `contents` to `path` owner-only: created `0600` on Unix, default
/// ACLs elsewhere (the file holds the persistent node identity).
fn write_private(path: &std::path::Path, contents: &str) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)?
            .write_all(contents.as_bytes())?;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        std::fs::write(path, contents)
    }
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

/// Decode exactly 32 bytes of hex (an Ed25519 seed or public key).
pub fn parse_hex32(s: &str) -> Option<[u8; 32]> {
    let bytes = parse_hex(s)?;
    if bytes.len() != 32 {
        return None;
    }
    let mut out = [0u8; 32];
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
    fn identity_persists_across_loads() {
        let dir = std::env::temp_dir().join(format!("mosaic-id-{}-persist", std::process::id()));
        let path = dir.join("node.id");
        let first = Identity::load_or_create(&path).expect("create identity");
        assert!(path.is_file(), "identity file must be written");
        let second = Identity::load_or_create(&path).expect("reload identity");
        assert_eq!(
            first.node_id(),
            second.node_id(),
            "identity must be stable across restarts"
        );
        assert_eq!(first.pubkey(), second.pubkey());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn corrupt_identity_file_regenerates() {
        let dir = std::env::temp_dir().join(format!("mosaic-id-{}-corrupt", std::process::id()));
        let path = dir.join("node.id");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(&path, "not-hex").unwrap();
        let identity = Identity::load_or_create(&path).expect("regenerate identity");
        assert_ne!(identity.node_id(), tpt_mosaic_core::NodeId::NIL);
        // The file now holds the fresh identity in hex.
        assert_eq!(
            Identity::load_or_create(&path).unwrap().node_id(),
            identity.node_id()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn legacy_identity_file_is_replaced_by_a_keypair() {
        let dir = std::env::temp_dir().join(format!("mosaic-id-{}-legacy", std::process::id()));
        let path = dir.join("node.id");
        std::fs::create_dir_all(&dir).unwrap();
        // A pre-Ed25519 16-byte identity file.
        std::fs::write(&path, to_hex(&[7u8; 16])).unwrap();
        let identity = Identity::load_or_create(&path).expect("regenerate over legacy file");
        // The file now holds the 64-byte expanded key.
        let stored = std::fs::read_to_string(&path).unwrap();
        assert_eq!(stored.trim().len(), 64);
        assert_eq!(
            Identity::load_or_create(&path).unwrap().node_id(),
            identity.node_id()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn node_id_is_derived_from_the_public_key() {
        let identity = Identity::generate();
        assert_eq!(
            identity.node_id(),
            node_id_for_pubkey(&identity.pubkey()),
            "NodeId must equal BLAKE3(pubkey)[..16]"
        );
        // Different keys → different ids.
        assert_ne!(Identity::generate().node_id(), identity.node_id());
    }

    #[test]
    fn signatures_verify_only_against_the_signing_key() {
        let identity = Identity::generate();
        let other = Identity::generate();
        let msg = b"authenticated frame bytes";
        let sig = identity.sign(msg).to_bytes();

        assert!(Identity::verify(&identity.pubkey(), msg, &sig));
        assert!(!Identity::verify(&other.pubkey(), msg, &sig));
        // Any tampering with the message breaks the signature.
        assert!(!Identity::verify(&identity.pubkey(), b"tampered", &sig));
        // Garbage signatures and garbage keys are rejected, not panics.
        assert!(!Identity::verify(&identity.pubkey(), msg, &[0u8; 64]));
        assert!(!Identity::verify(&[0u8; 32], msg, &sig));
    }

    #[test]
    fn generated_ids_are_distinct() {
        let a = generate_bytes();
        let b = generate_bytes();
        assert_ne!(a, b);
        assert_ne!(a, [0u8; 16]);
    }
}
