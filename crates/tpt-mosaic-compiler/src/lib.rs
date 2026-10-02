//! Hardware-aware compilation dispatch: routes workloads to tpt-gpu or tpt-crucible.
//!
//! Inspects the target node's [`HardwareProfile`] and selects the appropriate
//! compilation backend. Caches compiled binaries keyed by workload fingerprint.
//!
//! # Backends
//! Without cargo features the backends are stubs (GPU always fails into the
//! universal fallback; universal passes bytes through). With features enabled
//! the real external pipelines run:
//!
//! - `gpu` — `tpt-gpu-runtime`: TPTIR *text* workloads are compiled through
//!   `Device::load_module` (simulated device unless upstream `cuda` is on).
//! - `crucible` — `tpt-crucible-catalyst`: recognized model containers
//!   (SafeTensors, GGUF) are lowered to serialized TPT-IR
//!   (`tpt_crucible_common::Graph::to_binary`).

#![deny(missing_docs)]

use tpt_mosaic_core::{GpuVendor, HardwareProfile, MosaicError};

/// Identifies which compilation backend should handle a workload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompilationBackend {
    /// `tpt-gpu`: CUDA (NVIDIA), Metal (Apple), or Vulkan (AMD/Intel).
    Gpu,
    /// `tpt-crucible`: NPU, DSP, FPGA, or CPU vector instructions.
    Universal,
}

/// Inspect a hardware profile and select the preferred compilation backend.
pub fn select_backend(profile: &HardwareProfile) -> CompilationBackend {
    match profile.gpu_vendor {
        GpuVendor::None | GpuVendor::Other => CompilationBackend::Universal,
        _ => CompilationBackend::Gpu,
    }
}

/// Compute a stable fingerprint for a (workload, hardware profile) pair.
///
/// Used as the cache key for compiled binary artifacts.
pub fn fingerprint(workload: &[u8], profile: &HardwareProfile) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(workload);
    hasher.update(&[profile.gpu_vendor as u8]);
    hasher.update(&[profile.cpu_arch as u8]);
    hasher.update(&[profile.npu_present as u8]);
    *hasher.finalize().as_bytes()
}

/// Dispatch a workload for compilation against `profile`.
///
/// Returns the compiled binary bytes. Falls back to [`CompilationBackend::Universal`]
/// if the primary backend fails.
pub fn compile(workload: &[u8], profile: &HardwareProfile) -> Result<Vec<u8>, MosaicError> {
    let backend = select_backend(profile);
    dispatch(workload, profile, backend).or_else(|_| {
        // Fallback: try the universal backend.
        dispatch(workload, profile, CompilationBackend::Universal)
    })
}

fn dispatch(
    workload: &[u8],
    _profile: &HardwareProfile,
    backend: CompilationBackend,
) -> Result<Vec<u8>, MosaicError> {
    match backend {
        CompilationBackend::Gpu => gpu_dispatch(workload),
        CompilationBackend::Universal => universal_dispatch(workload),
    }
}

/// Gpu backend: real TPTIR compilation with the `gpu` feature, otherwise a
/// stub that fails into the universal fallback.
#[cfg(feature = "gpu")]
fn gpu_dispatch(workload: &[u8]) -> Result<Vec<u8>, MosaicError> {
    if !looks_like_tptir(workload) {
        return Err(MosaicError::CompilationFailed);
    }
    let source = core::str::from_utf8(workload).expect("checked by looks_like_tptir");
    let device = tpt_gpu_runtime::Device::open().map_err(|_| MosaicError::CompilationFailed)?;
    let kernel = device
        .load_module(source)
        .map_err(|_| MosaicError::CompilationFailed)?;
    let module = kernel.module().ok_or(MosaicError::CompilationFailed)?;
    Ok(module.compiled.clone().into_bytes())
}

/// Entry gate for the GPU path: tpt-gpu's native compiler is lenient and will
/// "compile" arbitrary UTF-8 into empty modules, so require the workload to
/// carry the TPTIR module + op markers before dispatching. Non-TPTIR payloads
/// fail into the universal fallback chosen by `compile`.
#[cfg(feature = "gpu")]
fn looks_like_tptir(workload: &[u8]) -> bool {
    let Ok(text) = core::str::from_utf8(workload) else {
        return false;
    };
    text.contains("module") && text.contains("tptir.")
}

/// Gpu backend stub (feature `gpu` off).
#[cfg(not(feature = "gpu"))]
fn gpu_dispatch(_workload: &[u8]) -> Result<Vec<u8>, MosaicError> {
    Err(MosaicError::CompilationFailed)
}

/// Universal backend: real model lowering with the `crucible` feature,
/// otherwise a pass-through stub.
#[cfg(feature = "crucible")]
fn universal_dispatch(workload: &[u8]) -> Result<Vec<u8>, MosaicError> {
    match lower_model(workload) {
        // Recognized model container: serialize the lowered TPT-IR graph.
        Some(graph) => {
            graph
                .validate()
                .map_err(|_| MosaicError::CompilationFailed)?;
            graph
                .to_binary()
                .map_err(|_| MosaicError::CompilationFailed)
        }
        // Not a recognized model: pass the payload through unchanged (not
        // every task payload is a model).
        None => Ok(workload.to_vec()),
    }
}

/// Sniff the model-container magic bytes and lower to a TPT-IR `Graph`.
/// `None` for unrecognized containers.
#[cfg(feature = "crucible")]
fn lower_model(workload: &[u8]) -> Option<tpt_crucible_common::Graph> {
    const SOURCE: &str = "tpt-mosaic-workload";

    if workload.starts_with(b"GGUF") {
        return tpt_crucible_catalyst::gguf::build_graph(workload, SOURCE).ok();
    }

    // SafeTensors: 8-byte LE header length followed by a JSON header.
    if workload.len() > 8 && workload[8] == b'{' {
        let header_len = u64::from_le_bytes(workload[0..8].try_into().ok()?);
        if (header_len as usize) + 8 <= workload.len() {
            return tpt_crucible_catalyst::safetensors::parse_bytes(workload, SOURCE).ok();
        }
    }

    None
}

/// Universal backend stub (feature `crucible` off).
#[cfg(not(feature = "crucible"))]
fn universal_dispatch(workload: &[u8]) -> Result<Vec<u8>, MosaicError> {
    Ok(workload.to_vec())
}

/// Description of the tpt-gpu device this build would dispatch GPU work to.
#[cfg(feature = "gpu")]
#[derive(Debug, Clone)]
pub struct GpuProbe {
    /// Device name as reported by the backend.
    pub name: String,
    /// Backend driver family (`cuda`, `simulated`, ...).
    pub backend: String,
    /// Total device memory in bytes.
    pub total_memory_bytes: u64,
    /// `false` when running on the in-process simulated device.
    pub real_hardware: bool,
}

/// Probe the tpt-gpu runtime for an available device (feature `gpu`).
///
/// Without upstream `cuda` support compiled in this always reports the
/// in-process simulated device.
#[cfg(feature = "gpu")]
pub fn probe_gpu() -> GpuProbe {
    match tpt_gpu_runtime::Device::open() {
        Ok(device) => {
            let props = device.properties();
            GpuProbe {
                name: props.name.clone(),
                backend: props.backend.name().to_string(),
                total_memory_bytes: props.total_memory,
                real_hardware: device.is_real(),
            }
        }
        Err(_) => GpuProbe {
            name: "none".to_owned(),
            backend: "none".to_owned(),
            total_memory_bytes: 0,
            real_hardware: false,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tpt_mosaic_core::{CpuArch, GpuVendor, HardwareProfile, NodeKind, ThermalState};

    fn profile(gpu: GpuVendor) -> HardwareProfile {
        HardwareProfile {
            kind: NodeKind::EdgeTile,
            gpu_vendor: gpu,
            npu_present: false,
            cpu_arch: CpuArch::Aarch64,
            memory_mb: 8192,
            battery_level: 255,
            thermal_state: ThermalState::Nominal,
        }
    }

    #[test]
    fn nvidia_selects_gpu_backend() {
        assert_eq!(
            select_backend(&profile(GpuVendor::Nvidia)),
            CompilationBackend::Gpu
        );
    }

    #[test]
    fn no_gpu_selects_universal() {
        assert_eq!(
            select_backend(&profile(GpuVendor::None)),
            CompilationBackend::Universal
        );
    }

    #[test]
    fn fingerprint_is_deterministic() {
        let p = profile(GpuVendor::Amd);
        let w = b"workload bytes";
        assert_eq!(fingerprint(w, &p), fingerprint(w, &p));
    }

    #[test]
    fn fingerprint_differs_by_hardware() {
        let w = b"workload bytes";
        assert_ne!(
            fingerprint(w, &profile(GpuVendor::Nvidia)),
            fingerprint(w, &profile(GpuVendor::Amd))
        );
    }

    #[cfg(all(test, feature = "gpu"))]
    mod gpu {
        use super::*;

        const REDUCE_MAX_TPTIR: &str = r#"
module {
  func.func @reduce_max(%in: memref<*xf32>, %out: memref<*xf32>) attributes {tptir.kernel} {
    ^entry:
      %v = tptir.load(%in)
      %m = tptir.max(%v)
      tptir.store(%m, %out)
      tptir.return
  }
}
"#;

        #[test]
        fn tptir_module_compiles_through_the_gpu_path() {
            let artifact = compile(REDUCE_MAX_TPTIR.as_bytes(), &profile(GpuVendor::Nvidia))
                .expect("TPTIR text must compile on the simulated device");
            assert!(!artifact.is_empty());
        }

        #[test]
        fn non_tptir_workload_falls_back_to_universal() {
            let workload: &[u8] = &[0x00, 0x01, 0x02, 0x03];
            let artifact = compile(workload, &profile(GpuVendor::Nvidia))
                .expect("gpu failure must fall back to the universal backend");
            assert_eq!(artifact, workload);
        }

        #[test]
        fn probe_reports_a_device() {
            let probe = probe_gpu();
            assert!(!probe.name.is_empty());
            assert!(!probe.backend.is_empty());
        }
    }

    #[cfg(all(test, feature = "crucible"))]
    mod crucible {
        use super::*;
        use std::collections::BTreeMap;

        fn safetensors_fixture() -> Vec<u8> {
            let mut tensors = BTreeMap::new();
            tensors.insert(
                "weight".to_owned(),
                tpt_crucible_common::Tensor::from_f32(vec![2, 2], &[1.0, 2.0, 3.0, 4.0]),
            );
            tpt_crucible_catalyst::safetensors::encode(&tensors).expect("fixture must encode")
        }

        #[test]
        fn safetensors_model_lowers_to_tptir_binary() {
            let model = safetensors_fixture();
            let artifact = compile(&model, &profile(GpuVendor::None))
                .expect("safetensors must lower through the universal backend");
            assert_eq!(&artifact[..6], tpt_crucible_common::BINARY_MAGIC);

            let graph = tpt_crucible_common::Graph::from_binary(&artifact)
                .expect("artifact must decode as TPT-IR");
            assert!(graph
                .nodes
                .iter()
                .any(|n| matches!(n.op, tpt_crucible_common::Op::Constant { .. })));
        }

        #[test]
        fn unrecognized_bytes_pass_through_unchanged() {
            let workload: &[u8] = b"plain inference input, not a model";
            let artifact = compile(workload, &profile(GpuVendor::None)).expect("pass-through");
            assert_eq!(artifact, workload);
        }

        #[test]
        fn truncated_safetensors_passes_through() {
            let model = safetensors_fixture();
            let truncated = &model[..12.min(model.len())];
            let artifact = compile(truncated, &profile(GpuVendor::None)).expect("pass-through");
            assert_eq!(artifact, truncated);
        }
    }
}
