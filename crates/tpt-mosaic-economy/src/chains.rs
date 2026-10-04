//! Settlement adapters for Solana, Base (Ethereum L2), and NEAR.
//!
//! The current adapters implement the [`Settlement`] trait against a local
//! [`InMemoryLedger`], which keeps the full node loop testable without network
//! access. The `solana` / `base` / `near` Cargo features are reserved for
//! swapping the ledger calls for real RPC transactions once the corresponding
//! SDK dependencies are re-enabled in `Cargo.toml`.
//!
//! The ledger can persist itself: adapters built with
//! [`SolanaSettlement::new_with_state`] (and siblings) load balances from the
//! file at construction and rewrite it after every credit, so earnings
//! survive a restart.

use std::collections::HashMap;
use std::sync::Mutex;

use tpt_mosaic_core::{MosaicError, NodeId};

use crate::{lock_ignoring_poison, Settlement};

/// In-memory reward ledger backing the stub adapters (and tests).
#[derive(Debug, Default)]
pub struct InMemoryLedger {
    balances: Mutex<HashMap<NodeId, u64>>,
}

impl InMemoryLedger {
    /// Create an empty ledger.
    pub fn new() -> Self {
        Self::default()
    }

    /// Credit `amount` micro-units to `node_id`.
    ///
    /// Saturating: an overflowing balance clamps instead of wrapping.
    pub fn credit(&self, node_id: NodeId, amount: u64) {
        let mut balances = lock_ignoring_poison(&self.balances);
        let entry = balances.entry(node_id).or_insert(0);
        *entry = entry.saturating_add(amount);
    }

    /// Current balance of `node_id` in micro-units.
    pub fn balance(&self, node_id: NodeId) -> u64 {
        lock_ignoring_poison(&self.balances)
            .get(&node_id)
            .copied()
            .unwrap_or(0)
    }

    /// Serialize every balance: `u32` LE count, then per entry 16 id bytes +
    /// `u64` LE amount.
    pub fn to_bytes(&self) -> Vec<u8> {
        let balances = lock_ignoring_poison(&self.balances);
        let mut out = Vec::with_capacity(4 + balances.len() * 24);
        out.extend_from_slice(&(balances.len() as u32).to_le_bytes());
        for (id, amount) in balances.iter() {
            out.extend_from_slice(id.as_bytes());
            out.extend_from_slice(&amount.to_le_bytes());
        }
        out
    }

    /// Restore a ledger written by [`InMemoryLedger::to_bytes`].
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, MosaicError> {
        if bytes.len() < 4 {
            return Err(MosaicError::SerializationError);
        }
        let count = u32::from_le_bytes(bytes[0..4].try_into().expect("4 bytes")) as usize;
        if bytes.len() != 4 + count * 24 {
            return Err(MosaicError::SerializationError);
        }
        let mut balances = HashMap::with_capacity(count);
        for entry in bytes[4..].chunks_exact(24) {
            let id = NodeId::from_bytes(entry[0..16].try_into().expect("16 bytes"));
            let amount = u64::from_le_bytes(entry[16..24].try_into().expect("8 bytes"));
            balances.insert(id, amount);
        }
        Ok(Self {
            balances: Mutex::new(balances),
        })
    }

    /// Persist atomically (temp file + rename) to `path`.
    pub fn save(&self, path: &std::path::Path) -> std::io::Result<()> {
        write_atomically(path, &self.to_bytes())
    }

    /// Load a ledger previously written by [`InMemoryLedger::save`]. A
    /// missing file yields an empty ledger; a corrupt one is an error.
    pub fn load(path: &std::path::Path) -> std::io::Result<Self> {
        match std::fs::read(path) {
            Ok(bytes) => Self::from_bytes(&bytes)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::new()),
            Err(e) => Err(e),
        }
    }
}

/// Write `bytes` to `path` via a temp file + rename, so a crash mid-write
/// never leaves a truncated state file behind.
pub(crate) fn write_atomically(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, bytes)?;
    let _ = std::fs::remove_file(path);
    std::fs::rename(&tmp, path)
}

/// Shared plumbing for the ledger-backed chain adapters.
#[derive(Debug)]
struct LedgerBacked {
    chain: &'static str,
    rpc_url: String,
    ledger: InMemoryLedger,
    /// When set, balances are loaded from (and rewritten to) this file.
    state_path: Option<std::path::PathBuf>,
}

impl LedgerBacked {
    fn new(
        chain: &'static str,
        rpc_url: impl Into<String>,
        state_path: Option<&std::path::Path>,
    ) -> Self {
        let ledger = state_path
            .and_then(|path| match InMemoryLedger::load(path) {
                Ok(ledger) => Some(ledger),
                Err(e) => {
                    tracing::warn!(
                        path = %path.display(),
                        error = %e,
                        "ledger state file unreadable; starting empty"
                    );
                    None
                }
            })
            .unwrap_or_default();
        Self {
            chain,
            rpc_url: rpc_url.into(),
            ledger,
            state_path: state_path.map(std::path::Path::to_path_buf),
        }
    }

    fn submit_reward(
        &self,
        node_id: NodeId,
        amount: u64,
        task_context: &str,
    ) -> Result<(), MosaicError> {
        // TODO: broadcast a real transaction via `rpc_url` when the chain SDK
        // features are enabled. For now the adapter settles locally.
        tracing::debug!(
            chain = self.chain,
            rpc_url = %self.rpc_url,
            amount,
            task_context,
            "settlement credited to local ledger"
        );
        self.ledger.credit(node_id, amount);
        if let Some(path) = &self.state_path {
            if let Err(e) = self.ledger.save(path) {
                tracing::warn!(path = %path.display(), error = %e, "ledger persistence failed");
            }
        }
        Ok(())
    }

    fn balance(&self, node_id: NodeId) -> Result<u64, MosaicError> {
        Ok(self.ledger.balance(node_id))
    }
}

/// Solana settlement adapter (stub-backed; see module docs).
#[derive(Debug)]
pub struct SolanaSettlement {
    inner: LedgerBacked,
}

impl SolanaSettlement {
    /// Create an adapter targeting `rpc_url` (unused until the SDK is wired).
    pub fn new(rpc_url: impl Into<String>) -> Self {
        Self::new_with_state(rpc_url, None)
    }

    /// Create an adapter that persists balances to `state_file`.
    pub fn new_with_state(
        rpc_url: impl Into<String>,
        state_file: Option<&std::path::Path>,
    ) -> Self {
        Self {
            inner: LedgerBacked::new("solana", rpc_url, state_file),
        }
    }
}

impl Settlement for SolanaSettlement {
    fn submit_reward(
        &self,
        node_id: NodeId,
        amount: u64,
        task_context: &str,
    ) -> Result<(), MosaicError> {
        self.inner.submit_reward(node_id, amount, task_context)
    }

    fn balance(&self, node_id: NodeId) -> Result<u64, MosaicError> {
        self.inner.balance(node_id)
    }
}

/// Base (Ethereum L2) settlement adapter (stub-backed; see module docs).
#[derive(Debug)]
pub struct BaseSettlement {
    inner: LedgerBacked,
}

impl BaseSettlement {
    /// Create an adapter targeting `rpc_url` (unused until the SDK is wired).
    pub fn new(rpc_url: impl Into<String>) -> Self {
        Self::new_with_state(rpc_url, None)
    }

    /// Create an adapter that persists balances to `state_file`.
    pub fn new_with_state(
        rpc_url: impl Into<String>,
        state_file: Option<&std::path::Path>,
    ) -> Self {
        Self {
            inner: LedgerBacked::new("base", rpc_url, state_file),
        }
    }
}

impl Settlement for BaseSettlement {
    fn submit_reward(
        &self,
        node_id: NodeId,
        amount: u64,
        task_context: &str,
    ) -> Result<(), MosaicError> {
        self.inner.submit_reward(node_id, amount, task_context)
    }

    fn balance(&self, node_id: NodeId) -> Result<u64, MosaicError> {
        self.inner.balance(node_id)
    }
}

/// NEAR protocol settlement adapter (stub-backed; see module docs).
#[derive(Debug)]
pub struct NearSettlement {
    inner: LedgerBacked,
}

impl NearSettlement {
    /// Create an adapter targeting `rpc_url` (unused until the SDK is wired).
    pub fn new(rpc_url: impl Into<String>) -> Self {
        Self::new_with_state(rpc_url, None)
    }

    /// Create an adapter that persists balances to `state_file`.
    pub fn new_with_state(
        rpc_url: impl Into<String>,
        state_file: Option<&std::path::Path>,
    ) -> Self {
        Self {
            inner: LedgerBacked::new("near", rpc_url, state_file),
        }
    }
}

impl Settlement for NearSettlement {
    fn submit_reward(
        &self,
        node_id: NodeId,
        amount: u64,
        task_context: &str,
    ) -> Result<(), MosaicError> {
        self.inner.submit_reward(node_id, amount, task_context)
    }

    fn balance(&self, node_id: NodeId) -> Result<u64, MosaicError> {
        self.inner.balance(node_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Settlement;

    fn node(b: u8) -> NodeId {
        NodeId::from_bytes([b; 16])
    }

    #[test]
    fn ledger_credit_accumulates() {
        let ledger = InMemoryLedger::new();
        ledger.credit(node(1), 10);
        ledger.credit(node(1), 15);
        assert_eq!(ledger.balance(node(1)), 25);
        assert_eq!(ledger.balance(node(2)), 0);
    }

    #[test]
    fn adapters_settle_through_the_trait() {
        let adapters: Vec<Box<dyn Settlement>> = vec![
            Box::new(SolanaSettlement::new("stub://solana")),
            Box::new(BaseSettlement::new("stub://base")),
            Box::new(NearSettlement::new("stub://near")),
        ];
        for (i, adapter) in adapters.iter().enumerate() {
            adapter
                .submit_reward(node(9), 100 + i as u64, "test")
                .unwrap();
            assert_eq!(adapter.balance(node(9)).unwrap(), 100 + i as u64);
        }
    }

    #[test]
    fn ledger_credit_saturates() {
        let ledger = InMemoryLedger::new();
        ledger.credit(node(1), u64::MAX);
        ledger.credit(node(1), 10);
        assert_eq!(ledger.balance(node(1)), u64::MAX, "overflow clamps");
    }

    #[test]
    fn ledger_round_trips_through_disk() {
        let dir = std::env::temp_dir().join(format!("mosaic-ledger-{}", std::process::id()));
        let path = dir.join("ledger.bin");
        let ledger = InMemoryLedger::new();
        ledger.credit(node(1), 42);
        ledger.credit(node(2), 7);
        ledger.save(&path).expect("save");

        let restored = InMemoryLedger::load(&path).expect("load");
        assert_eq!(restored.balance(node(1)), 42);
        assert_eq!(restored.balance(node(2)), 7);
        assert_eq!(restored.balance(node(3)), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn ledger_load_missing_file_starts_empty() {
        let path = std::env::temp_dir()
            .join(format!("mosaic-ledger-missing-{}", std::process::id()))
            .join("ledger.bin");
        let ledger = InMemoryLedger::load(&path).expect("missing file is an empty ledger");
        assert_eq!(ledger.balance(node(1)), 0);
    }

    #[test]
    fn ledger_rejects_corrupt_state() {
        assert!(InMemoryLedger::from_bytes(&[1, 2, 3]).is_err());
        // Announced count does not match the actual bytes.
        let evil = InMemoryLedger::new();
        evil.credit(node(1), 5);
        let mut bytes = evil.to_bytes();
        bytes[0..4].copy_from_slice(&2u32.to_le_bytes());
        assert!(InMemoryLedger::from_bytes(&bytes).is_err());
    }

    #[test]
    fn adapter_persists_balances_across_instances() {
        let dir =
            std::env::temp_dir().join(format!("mosaic-ledger-adapter-{}", std::process::id()));
        let state = dir.join("solana.bin");
        {
            let adapter = SolanaSettlement::new_with_state("stub://solana", Some(&state));
            adapter.submit_reward(node(4), 33, "task-a").unwrap();
        }
        let reopened = SolanaSettlement::new_with_state("stub://solana", Some(&state));
        assert_eq!(reopened.balance(node(4)).unwrap(), 33);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
