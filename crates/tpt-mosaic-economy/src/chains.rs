//! Settlement adapters for Solana, Base (Ethereum L2), and NEAR.
//!
//! The current adapters implement the [`Settlement`] trait against a local
//! [`InMemoryLedger`], which keeps the full node loop testable without network
//! access. The `solana` / `base` / `near` Cargo features are reserved for
//! swapping the ledger calls for real RPC transactions once the corresponding
//! SDK dependencies are re-enabled in `Cargo.toml`.

use std::collections::HashMap;
use std::sync::Mutex;

use tpt_mosaic_core::{MosaicError, NodeId};

use crate::Settlement;

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
    pub fn credit(&self, node_id: NodeId, amount: u64) {
        let mut balances = self.balances.lock().expect("ledger poisoned");
        *balances.entry(node_id).or_insert(0) += amount;
    }

    /// Current balance of `node_id` in micro-units.
    pub fn balance(&self, node_id: NodeId) -> u64 {
        self.balances
            .lock()
            .expect("ledger poisoned")
            .get(&node_id)
            .copied()
            .unwrap_or(0)
    }
}

/// Shared plumbing for the ledger-backed chain adapters.
#[derive(Debug)]
struct LedgerBacked {
    chain: &'static str,
    rpc_url: String,
    ledger: InMemoryLedger,
}

impl LedgerBacked {
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
        Self {
            inner: LedgerBacked {
                chain: "solana",
                rpc_url: rpc_url.into(),
                ledger: InMemoryLedger::new(),
            },
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
        Self {
            inner: LedgerBacked {
                chain: "base",
                rpc_url: rpc_url.into(),
                ledger: InMemoryLedger::new(),
            },
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
        Self {
            inner: LedgerBacked {
                chain: "near",
                rpc_url: rpc_url.into(),
                ledger: InMemoryLedger::new(),
            },
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
}
