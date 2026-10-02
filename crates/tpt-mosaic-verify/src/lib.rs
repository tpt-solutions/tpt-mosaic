//! Cryptographic output hashing (BLAKE3/SHA-256), ZK-ML proof stubs, and tpt-eve hooks.
//!
//! The `eve` feature enables the `eve_hooks` module: inference-output claims
//! are lowered to provenance/confidence-scored facts and checked for
//! contradictions with tpt-eve's symbolic `ConsistencyChecker`.

#![cfg_attr(not(feature = "std"), no_std)]
#![deny(missing_docs)]

extern crate alloc;

use alloc::vec;
use alloc::vec::Vec;
use sha2::{Digest, Sha256};
use tpt_mosaic_core::{MosaicError, TaskId};

#[cfg(feature = "eve")]
pub mod eve_hooks;

/// Hash algorithm selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HashAlgorithm {
    /// BLAKE3 — primary algorithm. Fast, parallel-friendly.
    #[default]
    Blake3,
    /// SHA-256 — compatibility fallback.
    Sha256,
}

/// Compute a 32-byte digest of `output` using the specified algorithm.
pub fn hash_output(output: &[u8], algorithm: HashAlgorithm) -> [u8; 32] {
    match algorithm {
        HashAlgorithm::Blake3 => *blake3::hash(output).as_bytes(),
        HashAlgorithm::Sha256 => {
            let mut h = Sha256::new();
            h.update(output);
            h.finalize().into()
        }
    }
}

/// A zero-knowledge proof of correct ML inference (stub).
///
/// Full ZK-ML implementation is deferred; this type reserves the interface.
#[derive(Debug, Clone)]
pub struct ZkProof {
    /// Opaque proof bytes (ZK-SNARK or ZK-STARK when implemented).
    pub proof_bytes: Vec<u8>,
    /// The committed output hash this proof attests to.
    pub committed_hash: [u8; 32],
}

/// Trait for generating ZK-ML proofs of correct execution.
pub trait ZkProver {
    /// Generate a proof that `output` was correctly produced from `input`.
    fn prove(&self, task_id: TaskId, input: &[u8], output: &[u8]) -> Result<ZkProof, MosaicError>;

    /// Verify a proof without access to the raw output.
    fn verify(&self, proof: &ZkProof) -> Result<bool, MosaicError>;
}

/// Stub prover — accepts everything. Replace with real ZK-ML circuit when ready.
#[derive(Debug, Default)]
pub struct StubProver;

impl ZkProver for StubProver {
    fn prove(
        &self,
        _task_id: TaskId,
        _input: &[u8],
        output: &[u8],
    ) -> Result<ZkProof, MosaicError> {
        Ok(ZkProof {
            committed_hash: hash_output(output, HashAlgorithm::Blake3),
            proof_bytes: vec![],
        })
    }

    fn verify(&self, _proof: &ZkProof) -> Result<bool, MosaicError> {
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn blake3_is_deterministic() {
        let data = b"tpt-mosaic test";
        assert_eq!(
            hash_output(data, HashAlgorithm::Blake3),
            hash_output(data, HashAlgorithm::Blake3)
        );
    }

    #[test]
    fn sha256_is_deterministic() {
        let data = b"tpt-mosaic test";
        assert_eq!(
            hash_output(data, HashAlgorithm::Sha256),
            hash_output(data, HashAlgorithm::Sha256)
        );
    }

    #[test]
    fn different_data_different_hash() {
        assert_ne!(
            hash_output(b"aaa", HashAlgorithm::Blake3),
            hash_output(b"bbb", HashAlgorithm::Blake3)
        );
    }

    #[test]
    fn stub_prover_round_trip() {
        let prover = StubProver;
        let proof = prover.prove(TaskId::NIL, b"input", b"output").unwrap();
        assert!(prover.verify(&proof).unwrap());
    }

    proptest! {
        /// Hashing arbitrary data is deterministic and agrees with the
        /// underlying `blake3` / `sha2` implementations.
        #[test]
        fn hashing_is_deterministic_and_matches_backends(
            data in prop::collection::vec(any::<u8>(), 0..1024),
        ) {
            prop_assert_eq!(
                hash_output(&data, HashAlgorithm::Blake3),
                hash_output(&data, HashAlgorithm::Blake3)
            );
            prop_assert_eq!(
                hash_output(&data, HashAlgorithm::Sha256),
                hash_output(&data, HashAlgorithm::Sha256)
            );
            prop_assert_eq!(
                hash_output(&data, HashAlgorithm::Blake3),
                *blake3::hash(&data).as_bytes()
            );
            let mut h = Sha256::new();
            h.update(&data);
            let expected: [u8; 32] = h.finalize().into();
            prop_assert_eq!(hash_output(&data, HashAlgorithm::Sha256), expected);
        }
    }
}
