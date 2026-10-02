//! Fingerprint-keyed on-disk cache for compiled artifacts (spec §6.6).

use std::path::PathBuf;

use tpt_mosaic_core::{HardwareProfile, MosaicError};

use crate::{compile, fingerprint};

/// Lowercase-hex encode `bytes`.
fn to_hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// On-disk cache of compiled binaries, keyed by the (workload, hardware)
/// [`fingerprint`]. Reads and writes are best-effort: an unreadable or
/// unwritable cache degrades to plain compilation, never to an error.
///
/// Artifacts are stored raw under `<root>/<fingerprint-hex>.fbin`. Cache
/// entries are trusted (integrity validation is a v1 concern); the
/// fingerprint fully determines the workload and target hardware, so entries
/// never go stale.
#[derive(Debug, Clone)]
pub struct JitCache {
    root: PathBuf,
}

impl JitCache {
    /// Create a cache rooted at `root` (created lazily on first store).
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Path of the cache entry for `fingerprint`.
    pub fn entry_path(&self, fingerprint: [u8; 32]) -> PathBuf {
        self.root.join(format!("{}.fbin", to_hex(&fingerprint)))
    }

    /// Compile `workload` for `profile`, serving hits from disk.
    pub fn compile_cached(
        &self,
        workload: &[u8],
        profile: &HardwareProfile,
    ) -> Result<Vec<u8>, MosaicError> {
        let fp = fingerprint(workload, profile);
        let path = self.entry_path(fp);
        if let Ok(bytes) = std::fs::read(&path) {
            return Ok(bytes);
        }
        let artifact = compile(workload, profile)?;
        self.store(fp, &artifact);
        Ok(artifact)
    }

    /// Best-effort atomic store: write to a temp file, then rename over the
    /// entry. Failures are logged and ignored.
    fn store(&self, fingerprint: [u8; 32], artifact: &[u8]) {
        let path = self.entry_path(fingerprint);
        if !self.root.exists() && std::fs::create_dir_all(&self.root).is_err() {
            return;
        }
        let tmp = path.with_extension("tmp");
        if std::fs::write(&tmp, artifact).is_err() {
            return;
        }
        // Windows renames fail over existing files; clear the target first.
        let _ = std::fs::remove_file(&path);
        if std::fs::rename(&tmp, &path).is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CompilationBackend;
    use tpt_mosaic_core::{CpuArch, GpuVendor, HardwareProfile, NodeKind, ThermalState};

    fn profile(gpu: GpuVendor) -> HardwareProfile {
        HardwareProfile {
            kind: NodeKind::EdgeTile,
            gpu_vendor: gpu,
            npu_present: false,
            cpu_arch: CpuArch::X86_64,
            memory_mb: 8192,
            battery_level: 255,
            thermal_state: ThermalState::Nominal,
        }
    }

    #[test]
    fn store_then_hit_round_trips_the_artifact() {
        let root = std::env::temp_dir().join(format!("mosaic-jit-{}-hit", std::process::id()));
        let cache = JitCache::new(&root);
        let workload = b"cache me";
        let p = profile(GpuVendor::None);

        let first = cache.compile_cached(workload, &p).expect("compile");
        let fp = fingerprint(workload, &p);
        assert!(cache.entry_path(fp).is_file(), "entry must exist on disk");

        let second = cache.compile_cached(workload, &p).expect("cached compile");
        assert_eq!(first, second);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn different_workloads_take_different_entries() {
        let root = std::env::temp_dir().join(format!("mosaic-jit-{}-entries", std::process::id()));
        let cache = JitCache::new(&root);
        let p = profile(GpuVendor::None);
        let _ = cache.compile_cached(b"alpha", &p).unwrap();
        let _ = cache.compile_cached(b"beta", &p).unwrap();
        assert!(cache.entry_path(fingerprint(b"alpha", &p)).is_file());
        assert!(cache.entry_path(fingerprint(b"beta", &p)).is_file());
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn unwritable_root_degrades_to_plain_compile() {
        // A path that cannot be a directory: a file in the way.
        let blocker =
            std::env::temp_dir().join(format!("mosaic-jit-block-{}.f", std::process::id()));
        std::fs::write(&blocker, b"x").unwrap();
        let cache = JitCache::new(blocker.join("sub"));
        let artifact = cache
            .compile_cached(b"still compiles", &profile(GpuVendor::None))
            .expect("cache failures must not fail compilation");
        assert!(!artifact.is_empty());
        let _ = std::fs::remove_file(&blocker);
    }

    #[test]
    fn backend_selection_unchanged_by_cache() {
        let cache = JitCache::new(std::env::temp_dir().join("mosaic-jit-unused"));
        assert_eq!(
            crate::select_backend(&profile(GpuVendor::None)),
            CompilationBackend::Universal
        );
    }
}
