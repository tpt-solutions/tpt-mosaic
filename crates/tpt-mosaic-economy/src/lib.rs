//! Incentive layer: micro-rewards, reputation scoring, slashing, and on-chain settlement.
//!
//! Provides a chain-agnostic [`Settlement`] trait with concrete adapters for
//! Solana, Base (Ethereum L2), and NEAR.

#![deny(missing_docs)]

pub mod chains;
pub mod reputation;
pub mod reward;

pub use reputation::ReputationStore;
pub use reward::{calculate_reward, SlashingRecord};

use tpt_mosaic_core::{MosaicError, NodeId};

/// Lock `mutex`, tolerating a poisoned lock: a panic in another thread must
/// not permanently wedge settlement or reputation bookkeeping.
pub(crate) fn lock_ignoring_poison<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Chain-agnostic interface for submitting micro-reward transactions.
pub trait Settlement: Send + Sync {
    /// Submit a reward of `amount` (in chain-native micro-units) to `node_id`.
    fn submit_reward(
        &self,
        node_id: NodeId,
        amount: u64,
        task_context: &str,
    ) -> Result<(), MosaicError>;

    /// Query the current on-chain balance for `node_id`.
    fn balance(&self, node_id: NodeId) -> Result<u64, MosaicError>;
}

/// Selects which on-chain settlement adapter to use at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Chain {
    /// Solana (high-throughput micro-payments).
    Solana,
    /// Base — Ethereum L2 (ERC-20 reward token).
    Base,
    /// NEAR Protocol (WASM contracts).
    Near,
}
