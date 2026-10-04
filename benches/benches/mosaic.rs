//! Criterion benchmarks for tpt-mosaic hot paths: wire codec, task sharding,
//! output hashing, quorum evaluation, and quorum assembly.

use std::hint::black_box;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use tpt_mosaic_core::{
    CapabilityFlags, CpuArch, GpuVendor, HardwareProfile, NodeId, NodeKind, QuorumConfig, TaskId,
    ThermalState,
};

/// A realistic heartbeat beacon.
fn sample_beacon() -> tpt_mosaic_proto::WireMessage {
    tpt_mosaic_proto::WireMessage::HeartbeatBeacon(tpt_mosaic_proto::HeartbeatBeacon {
        node_id: NodeId::from_bytes([7; 16]),
        hardware: HardwareProfile {
            kind: NodeKind::AnchorBallast,
            gpu_vendor: GpuVendor::Nvidia,
            npu_present: true,
            cpu_arch: CpuArch::Aarch64,
            memory_mb: 65536,
            battery_level: 255,
            thermal_state: ThermalState::Warm,
        },
        capabilities: CapabilityFlags::CUDA | CapabilityFlags::CPU_VECTOR,
        timestamp_ms: 1_700_000_000_000,
        addr: Some(std::net::SocketAddr::from(([127, 0, 0, 1], 7745))),
    })
}

fn bench_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("codec");
    let beacon = sample_beacon();

    group.bench_function("encode/beacon", |b| {
        b.iter(|| tpt_mosaic_proto::encode(black_box(&beacon)))
    });
    let beacon_frame = tpt_mosaic_proto::encode(&beacon);
    group.bench_function("decode/beacon", |b| {
        b.iter(|| tpt_mosaic_proto::decode(black_box(&beacon_frame)).expect("valid frame"))
    });

    // A 64 KiB task assignment — the shard-sized dispatch case.
    let assignment =
        tpt_mosaic_proto::WireMessage::TaskAssignment(tpt_mosaic_proto::TaskAssignment {
            task_id: TaskId::from_bytes([1; 16]),
            quorum_config: QuorumConfig::STANDARD_3_OF_5,
            payload: vec![0xAB; 64 * 1024],
            deadline_ms: 0,
            coordinator: std::net::SocketAddr::from(([127, 0, 0, 1], 7331)),
        });
    let assignment_frame = tpt_mosaic_proto::encode(&assignment);
    group.throughput(criterion::Throughput::Bytes(assignment_frame.len() as u64));
    group.bench_function("encode/assignment_64k", |b| {
        b.iter(|| tpt_mosaic_proto::encode(black_box(&assignment)))
    });
    group.bench_function("decode/assignment_64k", |b| {
        b.iter(|| tpt_mosaic_proto::decode(black_box(&assignment_frame)).expect("valid frame"))
    });
    group.finish();
}

fn bench_sharding(c: &mut Criterion) {
    let mut group = c.benchmark_group("task");
    let payload = vec![0u8; 256 * 1024];
    group.throughput(criterion::Throughput::Bytes(payload.len() as u64));
    group.bench_function("split_task/256k_into_64k_shards", |b| {
        b.iter(|| {
            tpt_mosaic_task::split_task(
                black_box(TaskId::NIL),
                black_box(&payload),
                tpt_mosaic_task::TaskPriority::Standard,
                64 * 1024,
            )
            .expect("valid split")
        })
    });
    group.finish();
}

fn bench_hashing(c: &mut Criterion) {
    let mut group = c.benchmark_group("verify");
    let payload = vec![0u8; 64 * 1024];
    group.throughput(criterion::Throughput::Bytes(payload.len() as u64));
    group.bench_function("blake3/64k", |b| {
        b.iter(|| {
            tpt_mosaic_verify::hash_output(
                black_box(&payload),
                tpt_mosaic_verify::HashAlgorithm::Blake3,
            )
        })
    });
    group.bench_function("sha256/64k", |b| {
        b.iter(|| {
            tpt_mosaic_verify::hash_output(
                black_box(&payload),
                tpt_mosaic_verify::HashAlgorithm::Sha256,
            )
        })
    });
    group.finish();
}

fn bench_quorum(c: &mut Criterion) {
    let mut group = c.benchmark_group("quorum");
    group.bench_function("hash_collector/5_of_5", |b| {
        b.iter(|| {
            let mut collector = tpt_mosaic_quorum::HashCollector::new(
                TaskId::NIL,
                QuorumConfig::new(5, 5, tpt_mosaic_core::TierLevel::MissionCritical),
            )
            .expect("valid config");
            for i in 0..5u8 {
                collector.submit(NodeId::from_bytes([i; 16]), [i; 32]);
            }
            black_box(collector.state().clone())
        })
    });
    group.finish();
}

fn bench_assembly(c: &mut Criterion) {
    let mut group = c.benchmark_group("scheduler");
    let candidates: Vec<tpt_mosaic_discovery::PeerRecord> = (0..500u32)
        .map(|i| tpt_mosaic_discovery::PeerRecord {
            node_id: NodeId::from_bytes([(i % 256) as u8; 16]),
            hardware: HardwareProfile {
                kind: if i % 4 == 0 {
                    NodeKind::AnchorBallast
                } else {
                    NodeKind::EdgeTile
                },
                gpu_vendor: match i % 4 {
                    0 => GpuVendor::Nvidia,
                    1 => GpuVendor::Amd,
                    2 => GpuVendor::Apple,
                    _ => GpuVendor::None,
                },
                npu_present: i % 5 == 0,
                cpu_arch: if i % 2 == 0 {
                    CpuArch::X86_64
                } else {
                    CpuArch::Aarch64
                },
                memory_mb: 8192,
                battery_level: 255,
                thermal_state: ThermalState::Nominal,
            },
            capabilities: CapabilityFlags::empty(),
            addr: None,
            last_seen: std::time::Instant::now(),
        })
        .collect();
    let policy = tpt_mosaic_scheduler::BalancedAssembler::default();
    group.bench_function("balanced_assemble/500_candidates_7_of_10", |b| {
        b.iter(|| {
            tpt_mosaic_scheduler::SchedulerPolicy::assemble(
                black_box(&policy),
                black_box(&candidates),
                &QuorumConfig::MISSION_CRITICAL_7_OF_10,
                CapabilityFlags::empty(),
                &|_| 0.5,
            )
            .expect("assembles from 500 candidates")
        })
    });
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2));
    targets = bench_codec, bench_sharding, bench_hashing, bench_quorum, bench_assembly
}
criterion_main!(benches);
