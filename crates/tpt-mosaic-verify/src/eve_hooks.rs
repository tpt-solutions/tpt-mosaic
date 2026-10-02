//! Symbolic consistency hooks backed by tpt-eve (feature `eve`).
//!
//! Lowers claims about task outputs into provenance/confidence-scored facts
//! and checks them for contradictions with tpt-eve's `ConsistencyChecker`.
//! Purely in-memory: no redb persistence, no disk access.

use tpt_eve_core::{Fact, FactSource, Value};
use tpt_eve_symbolic::ConsistencyChecker;

/// A claim about a task output, to be checked for internal consistency.
#[derive(Debug, Clone, PartialEq)]
pub struct Claim {
    /// What the claim is about (e.g. `"output-17"`).
    pub subject: String,
    /// The asserted relation (e.g. `"is"`, `"classified_as"`).
    pub predicate: String,
    /// The asserted value (e.g. `"benign"`).
    pub object: String,
    /// Confidence of the claim, `0.0`–`1.0`.
    pub confidence: f32,
}

impl Claim {
    /// Construct a claim.
    pub fn new(
        subject: impl Into<String>,
        predicate: impl Into<String>,
        object: impl Into<String>,
        confidence: f32,
    ) -> Self {
        Self {
            subject: subject.into(),
            predicate: predicate.into(),
            object: object.into(),
            confidence,
        }
    }
}

/// A contradiction found among the claims: one `(subject, predicate)` pair
/// asserted with conflicting objects.
#[derive(Debug, Clone, PartialEq)]
pub struct Contradiction {
    /// Subject the claims disagree about.
    pub subject: String,
    /// Predicate the claims disagree through.
    pub predicate: String,
    /// The mutually conflicting objects.
    pub conflicting_objects: Vec<String>,
}

/// Check `claims` for symbolic contradictions. Synchronous and side-effect
/// free; an empty result means the claim set is consistent.
///
/// tpt-eve's checker treats exclusive predicates (`"is"`, `"was"`) with two
/// or more distinct objects as contradictory; other predicates are additive.
pub fn check_claims(claims: &[Claim]) -> Vec<Contradiction> {
    let facts: Vec<Fact> = claims
        .iter()
        .map(|c| Fact {
            subject: Value::Text(c.subject.clone()),
            predicate: Value::Text(c.predicate.clone()),
            object: Value::Text(c.object.clone()),
            source: FactSource::Extracted {
                confidence: c.confidence,
            },
            confidence: c.confidence,
        })
        .collect();

    ConsistencyChecker::new()
        .check(&facts)
        .into_iter()
        .map(|c| Contradiction {
            subject: c.subject.as_str().to_owned(),
            predicate: c.predicate.as_str().to_owned(),
            conflicting_objects: c
                .conflicting_objects
                .iter()
                .map(|v| v.as_str().to_owned())
                .collect(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exclusive_predicates_conflict() {
        let claims = vec![
            Claim::new("output-17", "is", "benign", 0.9),
            Claim::new("output-17", "is", "malicious", 0.8),
        ];
        let contradictions = check_claims(&claims);
        assert_eq!(contradictions.len(), 1);
        assert_eq!(contradictions[0].subject, "output-17");
        assert_eq!(contradictions[0].predicate, "is");
        assert_eq!(contradictions[0].conflicting_objects.len(), 2);
    }

    #[test]
    fn consistent_claims_pass() {
        let claims = vec![
            Claim::new("output-17", "is", "benign", 0.9),
            Claim::new("output-18", "is", "benign", 0.7),
            Claim::new("output-17", "classified_as", "spam", 0.6),
            Claim::new("output-17", "classified_as", "urgent", 0.5),
        ];
        assert!(check_claims(&claims).is_empty());
    }

    #[test]
    fn single_claim_never_contradicts() {
        let claims = vec![Claim::new("output-1", "is", "safe", 0.99)];
        assert!(check_claims(&claims).is_empty());
    }
}
