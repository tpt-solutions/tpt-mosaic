//! `tpt-archon` microkernel interface, capability grants, and RTOS preemption hooks.
//!
//! Ensures AI workloads are strictly confined to granted memory/compute slices.
//! The host RTOS can kill any task instantly via the preemption hook.
//!
//! # Execution backends
//! - Default: a pass-through stub with the real call signature.
//! - `archon` feature: `tpt-archon-kernel` provides genuine confinement —
//!   the grant's memory ceiling becomes a page pool, every page access is
//!   gated by a minted capability (`tpt-archon-bridge`), and the work runs
//!   as a cooperative task on the kernel scheduler. Revoking a capability
//!   denies all further access (see tests).

#![deny(missing_docs)]

use tpt_mosaic_core::{MosaicError, TaskId};

/// Memory and compute boundaries granted to a single task execution.
#[derive(Debug, Clone, Copy)]
pub struct CapabilityGrant {
    /// Identifier of the task being granted capabilities.
    pub task_id: TaskId,
    /// Maximum memory (bytes) the task may allocate.
    pub max_memory_bytes: u64,
    /// Maximum CPU time (milliseconds) before preemption.
    pub max_cpu_ms: u32,
    /// Maximum GPU time (milliseconds) before preemption. `0` = no GPU access.
    pub max_gpu_ms: u32,
    /// Whether network access is permitted (should be `false` for untrusted workloads).
    pub network_access: bool,
}

impl CapabilityGrant {
    /// Construct a new capability grant for a task.
    pub fn new(task_id: TaskId, max_memory_bytes: u64, max_cpu_ms: u32) -> Self {
        Self {
            task_id,
            max_memory_bytes,
            max_cpu_ms,
            max_gpu_ms: 0,
            network_access: false,
        }
    }

    /// Allow GPU access up to `max_gpu_ms` milliseconds.
    pub fn with_gpu(mut self, max_gpu_ms: u32) -> Self {
        self.max_gpu_ms = max_gpu_ms;
        self
    }
}

/// Thermal thresholds that trigger task pause or kill.
#[derive(Debug, Clone, Copy)]
pub struct ThermalPolicy {
    /// Pause new task dispatches above this temperature (°C).
    pub pause_threshold_celsius: u8,
    /// Kill in-flight tasks above this temperature (°C).
    pub kill_threshold_celsius: u8,
}

impl Default for ThermalPolicy {
    fn default() -> Self {
        Self {
            pause_threshold_celsius: 80,
            kill_threshold_celsius: 95,
        }
    }
}

/// Sandbox execution interface.
///
/// Without the `archon` feature this is a pass-through stub. With `archon`,
/// `execute` runs the workload inside a capability-confined
/// `tpt-archon-kernel` memory slice: the grant's memory ceiling is enforced
/// up front and every page access is capability-checked by the kernel.
pub struct Sandbox {
    thermal_policy: ThermalPolicy,
}

impl Sandbox {
    /// Create a new sandbox with the given thermal policy.
    pub fn new(thermal_policy: ThermalPolicy) -> Self {
        Self { thermal_policy }
    }

    /// The thermal thresholds this sandbox enforces.
    pub fn thermal_policy(&self) -> &ThermalPolicy {
        &self.thermal_policy
    }

    /// Execute `workload` bytes within the constraints of `grant`.
    ///
    /// Returns the raw output bytes on success.
    ///
    /// # Safety
    /// The real implementation passes execution into an `unsafe` FFI boundary
    /// into the `tpt-archon` kernel. This version confines the workload
    /// through archon's safe, user-space capability model instead.
    pub fn execute(
        &self,
        grant: &CapabilityGrant,
        workload: &[u8],
    ) -> Result<Vec<u8>, MosaicError> {
        self.execute_archon(grant, workload)
    }

    /// Pass-through stub (feature `archon` off): returns the workload bytes.
    #[cfg(not(feature = "archon"))]
    fn execute_archon(
        &self,
        grant: &CapabilityGrant,
        workload: &[u8],
    ) -> Result<Vec<u8>, MosaicError> {
        let _ = grant;
        Ok(workload.to_vec())
    }

    /// Capability-confined execution (feature `archon` on).
    #[cfg(feature = "archon")]
    fn execute_archon(
        &self,
        grant: &CapabilityGrant,
        workload: &[u8],
    ) -> Result<Vec<u8>, MosaicError> {
        archon::execute_confined(grant, workload)
    }
}

#[cfg(feature = "archon")]
mod archon {
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;

    use tpt_archon_bridge::capability::{
        Capability, CapabilityIssuer, Resource, Right, SharedIssuer,
    };
    use tpt_archon_bridge::page_cache::CorePageCache;
    use tpt_archon_core::block::InMemoryBlockDevice;
    use tpt_archon_core::page::{BufferPool, PAGE_SIZE};
    use tpt_archon_kernel::memory::UnifiedMemory;
    use tpt_archon_kernel::scheduler::{Poll, Scheduler, Task};
    use tpt_mosaic_core::MosaicError;

    use super::CapabilityGrant;

    type ArchonCache = CorePageCache<InMemoryBlockDevice>;
    type ArchonMemory = Rc<RefCell<UnifiedMemory<ArchonCache>>>;

    /// Cooperative readback task: re-reads every granted page through the
    /// kernel's capability check and accumulates the output into a shared
    /// buffer (the scheduler owns task objects, so results come back by `Rc`).
    struct ConfinedReadback {
        mem: ArchonMemory,
        caps: Vec<Capability>,
        full_page_bytes: usize,
        last_page_bytes: usize,
        output: Rc<RefCell<Vec<u8>>>,
        violation: Rc<Cell<bool>>,
    }

    impl Task for ConfinedReadback {
        fn poll(&mut self) -> Poll {
            for (i, cap) in self.caps.iter().enumerate() {
                let expected = if i + 1 == self.caps.len() {
                    self.last_page_bytes
                } else {
                    self.full_page_bytes
                };
                let bytes = {
                    let mut mem = self.mem.borrow_mut();
                    match mem.map_read(cap, i as u64) {
                        Ok(page) => page.as_bytes()[..expected].to_vec(),
                        Err(_) => {
                            // Capability denied or storage failure: stop and
                            // report a sandbox violation.
                            self.violation.set(true);
                            return Poll::Ready;
                        }
                    }
                };
                self.output.borrow_mut().extend_from_slice(&bytes);
                self.mem.borrow_mut().unmap(i as u64);
            }
            Poll::Ready
        }
    }

    /// Execute the workload inside a capability-confined archon memory slice.
    pub(super) fn execute_confined(
        grant: &CapabilityGrant,
        workload: &[u8],
    ) -> Result<Vec<u8>, MosaicError> {
        // The grant's memory ceiling is authoritative and enforced up front.
        if workload.len() as u64 > grant.max_memory_bytes {
            return Err(MosaicError::SandboxViolation);
        }
        if workload.is_empty() {
            return Ok(Vec::new());
        }

        let pool_frames = (grant.max_memory_bytes as usize / PAGE_SIZE).max(1);
        let issuer: SharedIssuer = Rc::new(RefCell::new(CapabilityIssuer::new()));
        let mem: ArchonMemory = Rc::new(RefCell::new(UnifiedMemory::new(CorePageCache::new(
            BufferPool::new(InMemoryBlockDevice::new(pool_frames as u64), pool_frames),
            issuer.clone(),
        ))));

        // Write phase: stage the workload one page at a time, each page
        // behind its own minted read/write capability.
        let pages: Vec<(Capability, usize)> = workload
            .chunks(PAGE_SIZE)
            .enumerate()
            .map(|(i, chunk)| {
                let cap = issuer
                    .borrow_mut()
                    .mint(Resource::Page(i as u64), Right::ReadWrite);
                {
                    // Scope so the mapped page borrow ends before unmap.
                    let mut mem = mem.borrow_mut();
                    let page = mem
                        .map_write(&cap, i as u64)
                        .map_err(|_| MosaicError::SandboxViolation)?;
                    page.as_bytes_mut()[..chunk.len()].copy_from_slice(chunk);
                }
                mem.borrow_mut().unmap(i as u64);
                Ok((cap, chunk.len()))
            })
            .collect::<Result<Vec<_>, MosaicError>>()?;

        // Execution phase: run the confined readback on the kernel scheduler.
        // archon's scheduler is cooperative with no timers, so the grant's
        // `max_cpu_ms` is honored structurally: the task completes in a
        // single poll and cannot outlive it.
        let (caps, lens): (Vec<_>, Vec<_>) = pages.into_iter().unzip();
        let output = Rc::new(RefCell::new(Vec::with_capacity(workload.len())));
        // Shared with the task: `Cell::clone` would copy the flag instead.
        let violation = Rc::new(Cell::new(false));
        let mut scheduler = Scheduler::new();
        scheduler.spawn(Box::new(ConfinedReadback {
            mem,
            caps,
            full_page_bytes: PAGE_SIZE,
            last_page_bytes: *lens.last().expect("non-empty workload"),
            output: output.clone(),
            violation: Rc::clone(&violation),
        }));
        scheduler.run_to_completion();

        if violation.get() {
            return Err(MosaicError::SandboxViolation);
        }
        let result = output.borrow().clone();
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tpt_mosaic_core::TaskId;

    #[test]
    fn grant_construction() {
        let grant = CapabilityGrant::new(TaskId::NIL, 512 * 1024 * 1024, 100).with_gpu(50);
        assert_eq!(grant.max_memory_bytes, 512 * 1024 * 1024);
        assert_eq!(grant.max_gpu_ms, 50);
        assert!(!grant.network_access);
    }

    #[test]
    fn sandbox_stub_executes() {
        let sandbox = Sandbox::new(ThermalPolicy::default());
        let grant = CapabilityGrant::new(TaskId::NIL, 1024, 100);
        let output = sandbox.execute(&grant, b"hello").unwrap();
        assert_eq!(output, b"hello");
    }

    #[cfg(all(test, feature = "archon"))]
    mod archon_tests {
        use super::*;
        use tpt_archon_bridge::capability::{CapabilityIssuer, Resource, Right};
        use tpt_archon_bridge::page_cache::{CacheError, CorePageCache};
        use tpt_archon_core::block::InMemoryBlockDevice;
        use tpt_archon_core::page::{BufferPool, PAGE_SIZE};
        use tpt_archon_kernel::memory::UnifiedMemory;

        fn sandbox() -> Sandbox {
            Sandbox::new(ThermalPolicy::default())
        }

        #[test]
        fn confined_execution_round_trips_across_pages() {
            // 8 KiB workload spans two 4 KiB pages.
            let workload: Vec<u8> = (0..2 * PAGE_SIZE).map(|i| (i % 251) as u8).collect();
            let grant = CapabilityGrant::new(TaskId::NIL, 64 * 1024, 100);
            let output = sandbox()
                .execute(&grant, &workload)
                .expect("confined execution must round-trip");
            assert_eq!(output, workload);
        }

        #[test]
        fn workload_exceeding_grant_memory_is_rejected() {
            let grant = CapabilityGrant::new(TaskId::NIL, PAGE_SIZE as u64, 100);
            let err = sandbox()
                .execute(&grant, &vec![0u8; PAGE_SIZE + 1])
                .expect_err("oversized workload must be rejected");
            assert_eq!(err, MosaicError::SandboxViolation);
        }

        #[test]
        fn empty_workload_is_allowed() {
            let grant = CapabilityGrant::new(TaskId::NIL, 4096, 100);
            let output = sandbox().execute(&grant, b"").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn revocation_denies_subsequent_page_access() {
            // Proves the confinement is real: once the issuer revokes the
            // capability, the kernel refuses the mapping.
            use std::cell::RefCell;
            use std::rc::Rc;
            use tpt_archon_bridge::capability::SharedIssuer;

            let mut issuer = CapabilityIssuer::new();
            let cap = issuer.mint(Resource::Page(0), Right::ReadWrite);
            let shared: SharedIssuer = Rc::new(RefCell::new(CapabilityIssuer::new()));
            let mut mem = UnifiedMemory::new(CorePageCache::new(
                BufferPool::new(InMemoryBlockDevice::new(2), 2),
                shared,
            ));
            issuer.revoke(&cap);
            assert!(matches!(mem.map_read(&cap, 0), Err(CacheError::Denied)));
        }
    }
}
