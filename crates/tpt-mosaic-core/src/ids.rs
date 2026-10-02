//! Opaque identifier types for nodes and tasks.

/// A unique identifier for a network node (edge tile or anchor).
///
/// Backed by 16 raw bytes; typically populated from a UUID v4 at startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct NodeId([u8; 16]);

impl NodeId {
    /// Construct a `NodeId` from raw bytes.
    #[inline]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Return the raw byte representation.
    #[inline]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// A sentinel nil ID (all zeros). Used as a placeholder before assignment.
    pub const NIL: Self = Self([0u8; 16]);
}

/// A unique identifier for a submitted compute task.
///
/// Backed by 16 raw bytes; typically populated from a UUID v4 at job submission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct TaskId([u8; 16]);

impl TaskId {
    /// Construct a `TaskId` from raw bytes.
    #[inline]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Return the raw byte representation.
    #[inline]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// A sentinel nil ID (all zeros).
    pub const NIL: Self = Self([0u8; 16]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_id_round_trip() {
        let bytes = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let id = NodeId::from_bytes(bytes);
        assert_eq!(id.as_bytes(), &bytes);
    }

    #[test]
    fn task_id_round_trip() {
        let bytes = [16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1];
        let id = TaskId::from_bytes(bytes);
        assert_eq!(id.as_bytes(), &bytes);
    }

    #[test]
    fn nil_ids_are_distinct_types() {
        let _n = NodeId::NIL;
        let _t = TaskId::NIL;
        // Different types — this just confirms they compile independently.
    }
}
