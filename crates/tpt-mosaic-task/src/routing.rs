//! Data-routing policy (spec §4): cellular is a fallback control plane.
//!
//! BLE/UWB/Wi-Fi form the primary planes; 4G/5G may carry control-plane
//! traffic, task assignments, and cryptographic proofs — but raw model
//! weights are never routed over cellular. Transport implementations call
//! [`routing_allowed`] before sending; the policy is pure so it is trivially
//! testable.

/// Physical transport tier a message would travel on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportClass {
    /// Local mesh / infrastructure Wi-Fi — the primary data plane.
    WifiMesh,
    /// 4G/5G fallback — control plane, assignments, and proofs only.
    Cellular,
}

/// Classification of what a message contains.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadKind {
    /// Discovery, heartbeat, and control-plane metadata.
    Control,
    /// A scheduler → worker task assignment (code + small inputs).
    TaskAssignment,
    /// A cryptographic proof or result hash.
    ResultProof,
    /// A model shard downloaded for execution.
    ModelShard,
    /// Raw model weights.
    RawWeights,
}

/// Returns `true` when `kind` may travel on `class`.
///
/// Wi-Fi permits everything. Cellular permits [`PayloadKind::Control`],
/// [`PayloadKind::TaskAssignment`], and [`PayloadKind::ResultProof`] only.
pub fn routing_allowed(class: TransportClass, kind: PayloadKind) -> bool {
    match class {
        TransportClass::WifiMesh => true,
        TransportClass::Cellular => matches!(
            kind,
            PayloadKind::Control | PayloadKind::TaskAssignment | PayloadKind::ResultProof
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wifi_permits_everything() {
        for kind in [
            PayloadKind::Control,
            PayloadKind::TaskAssignment,
            PayloadKind::ResultProof,
            PayloadKind::ModelShard,
            PayloadKind::RawWeights,
        ] {
            assert!(routing_allowed(TransportClass::WifiMesh, kind));
        }
    }

    #[test]
    fn cellular_is_control_and_proofs_only() {
        assert!(routing_allowed(
            TransportClass::Cellular,
            PayloadKind::Control
        ));
        assert!(routing_allowed(
            TransportClass::Cellular,
            PayloadKind::TaskAssignment
        ));
        assert!(routing_allowed(
            TransportClass::Cellular,
            PayloadKind::ResultProof
        ));
    }

    #[test]
    fn cellular_never_carries_model_data() {
        assert!(!routing_allowed(
            TransportClass::Cellular,
            PayloadKind::ModelShard
        ));
        assert!(!routing_allowed(
            TransportClass::Cellular,
            PayloadKind::RawWeights
        ));
    }
}
