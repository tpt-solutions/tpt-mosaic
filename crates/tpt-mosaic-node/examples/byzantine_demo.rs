//! Watch the quorum layer catch a Byzantine node.
//!
//! ```text
//! cargo run -p tpt-mosaic-node --example byzantine_demo
//! ```
//!
//! Two nodes execute the same task but one is dishonest (or faulty) and
//! votes a different digest. A 2-of-2 collector must detect the divergence,
//! finish the round as `Diverged`, and flag the dissenting voter as
//! suspected Byzantine — which the scheduler then excludes from future
//! quorums (see the economy crate's reputation store).

use tpt_mosaic_core::{NodeId, QuorumConfig, TaskId, TierLevel};
use tpt_mosaic_quorum::HashCollector;
use tpt_mosaic_verify::{hash_output, HashAlgorithm};

fn main() {
    let honest = NodeId::from_bytes([1; 16]);
    let byzantine = NodeId::from_bytes([2; 16]);

    // 2-of-2: both votes are needed, so a single dissenter diverges the round.
    let quorum = QuorumConfig::new(2, 2, TierLevel::BestEffort);
    let mut collector =
        HashCollector::new(TaskId::from_bytes([9; 16]), quorum).expect("valid quorum config");

    let payload = b"the same input bytes for everyone";
    let honest_hash = hash_output(payload, HashAlgorithm::Blake3);
    let forged_hash = hash_output(b"tampered output", HashAlgorithm::Blake3);

    // The honest node votes first, the Byzantine node dissents.
    collector.submit(honest, honest_hash);
    collector.submit(byzantine, forged_hash);

    println!("round state    {:?}", collector.state());
    println!(
        "suspected      {}",
        collector
            .suspected_byzantine()
            .iter()
            .map(|n| tpt_mosaic_node::id::to_hex(n.as_bytes()))
            .collect::<Vec<_>>()
            .join(", ")
    );

    assert!(matches!(
        collector.state(),
        tpt_mosaic_quorum::QuorumState::Finished(tpt_mosaic_quorum::QuorumResult::Diverged)
    ));
    assert_eq!(collector.suspected_byzantine(), &[byzantine]);
    println!("the diverged round produces no cancellation broadcast: every member already voted");
}
