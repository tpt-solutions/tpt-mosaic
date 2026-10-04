//! Fingerprint-keyed on-disk cache for compiled artifacts (spec §6.6).

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

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

/// Upper bound on one cached artifact. Larger entries are neither written
/// nor served, so a corrupt or hostile cache file cannot balloon memory.
const MAX_ARTIFACT_BYTES: u64 = 64 * 1024 * 1024;

/// BLAKE3 digest prefixed to every stored artifact.
const DIGEST_LEN: usize = 32;

/// Process-unique suffix for temp files, so two threads compiling the same
/// fingerprint never collide on one temp path.
fn unique_suffix() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    format!(
        "{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// On-disk cache of compiled binaries, keyed by the (workload, hardware)
/// [`fingerprint`] (which includes the compiled-in backend feature set).
/// Reads and writes are best-effort: an unreadable, unwritable, corrupt, or
/// oversized cache degrades to plain compilation, never to an error.
///
/// Entries live under `<root>/<fingerprint-hex>.fbin` as
/// `BLAKE3(artifact) || artifact`; on a digest mismatch the entry is deleted
/// and the workload recompiles.
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

    /// Compile `workload` for `profile`, serving verified hits from disk.
    pub fn compile_cached(
        &self,
        workload: &[u8],
        profile: &HardwareProfile,
    ) -> Result<Vec<u8>, MosaicError> {
        let fp = fingerprint(workload, profile);
        let path = self.entry_path(fp);
        if let Some(artifact) = Self::load_verified(&path) {
            return Ok(artifact);
        }
        let artifact = compile(workload, profile)?;
        self.store(fp, &artifact);
        Ok(artifact)
    }

    /// Load `path` and verify its integrity digest; `None` on any miss,
    /// corruption, or oversize entry (corrupt/oversize entries are removed).
    fn load_verified(path: &std::path::Path) -> Option<Vec<u8>> {
        let len = std::fs::metadata(path).ok()?.len();
        if len > MAX_ARTIFACT_BYTES {
            tracing::warn!(path = %path.display(), len, "oversize cache entry removed");
            let _ = std::fs::remove_file(path);
            return None;
        }
        if len <= DIGEST_LEN as u64 {
            return None;
        }
        let bytes = std::fs::read(path).ok()?;
        let (digest, artifact) = bytes.split_at(DIGEST_LEN);
        if digest != blake3::hash(artifact).as_bytes() {
            tracing::warn!(path = %path.display(), "cache entry failed integrity check; recompiling");
            let _ = std::fs::remove_file(path);
            return None;
        }
        Some(artifact.to_vec())
    }

    /// Best-effort atomic store: write `BLAKE3(artifact) || artifact` to a
    /// unique temp file, then rename over the entry. Failures are logged and
    /// ignored.
    fn store(&self, fingerprint: [u8; 32], artifact: &[u8]) {
        if artifact.len() as u64 > MAX_ARTIFACT_BYTES {
            tracing::debug!(len = artifact.len(), "artifact too large to cache");
            return;
        }
        let path = self.entry_path(fingerprint);
        if !self.root.exists() && std::fs::create_dir_all(&self.root).is_err() {
            return;
        }
        let mut blob = Vec::with_capacity(DIGEST_LEN + artifact.len());
        blob.extend_from_slice(blake3::hash(artifact).as_bytes());
        blob.extend_from_slice(artifact);

        let tmp = path.with_extension(format!("tmp-{}", unique_suffix()));
        if let Err(e) = std::fs::write(&tmp, &blob) {
            tracing::debug!(path = %tmp.display(), error = %e, "cache write failed");
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
        let _cache = JitCache::new(std::env::temp_dir().join("mosaic-jit-unused"));
        assert_eq!(
            crate::select_backend(&profile(GpuVendor::None)),
            CompilationBackend::Universal
        );
    }

    #[test]
    fn corrupted_entry_is_recompiled_not_served() {
        let root = std::env::temp_dir().join(format!("mosaic-jit-{}-corrupt", std::process::id()));
        let cache = JitCache::new(&root);
        let workload = b"integrity matters";
        let p = profile(GpuVendor::None);

        let fresh = cache.compile_cached(workload, &p).expect("compile");
        let path = cache.entry_path(fingerprint(workload, &p));

        // Flip one byte of the artifact body: the digest no longer matches.
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] = bytes[last].wrapping_add(1);
        std::fs::write(&path, bytes).unwrap();

        assert_eq!(
            cache.compile_cached(workload, &p).expect("recompile"),
            fresh,
            "a corrupt entry must not be served"
        );
        // The recompile re-stored a clean entry at the same path.
        let bytes = std::fs::read(&path).unwrap();
        let (digest, body) = bytes.split_at(32);
        assert_eq!(digest, blake3::hash(body).as_bytes());
        assert_eq!(body, fresh);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn raw_or_truncated_entries_are_ignored() {
        let root = std::env::temp_dir().join(format!("mosaic-jit-{}-raw", std::process::id()));
        let cache = JitCache::new(&root);
        let workload = b"no digest here";
        let p = profile(GpuVendor::None);

        // A legacy raw entry (pre-integrity format) is not served.
        let path = cache.entry_path(fingerprint(workload, &p));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(&path, b"raw artifact bytes").unwrap();
        let compiled = cache.compile_cached(workload, &p).expect("compile");
        assert!(!compiled.is_empty());

        // A digest-only entry (no body) is not served either.
        std::fs::write(&path, [0u8; 32]).unwrap();
        assert_eq!(
            cache.compile_cached(workload, &p).expect("compile"),
            compiled
        );
        let _ = std::fs::remove_dir_all(&root);
    }
}
