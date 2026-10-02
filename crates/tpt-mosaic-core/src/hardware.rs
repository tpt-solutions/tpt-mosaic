//! Hardware profile, capability flags, and device classification types.

use bitflags::bitflags;

/// GPU silicon vendor present on this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum GpuVendor {
    /// No dedicated GPU.
    None = 0,
    /// NVIDIA (CUDA-capable).
    Nvidia = 1,
    /// AMD (ROCm / Vulkan).
    Amd = 2,
    /// Apple (Metal).
    Apple = 3,
    /// Intel (integrated / Arc).
    Intel = 4,
    /// Other / unknown vendor.
    Other = 255,
}

/// CPU instruction-set architecture of the host device.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum CpuArch {
    /// x86-64 (AMD64).
    X86_64 = 0,
    /// 64-bit ARM (Apple Silicon, Qualcomm Oryon, server ARM).
    Aarch64 = 1,
    /// RISC-V 64-bit.
    RiscV64 = 2,
    /// Other / unknown architecture.
    Other = 255,
}

/// Thermal status of the device at the time of capability advertisement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum ThermalState {
    /// Operating within normal parameters.
    Nominal = 0,
    /// Elevated temperature; performance may be slightly reduced.
    Warm = 1,
    /// Throttling active; scheduler should prefer other nodes.
    Hot = 2,
    /// Critically hot; node should not accept new tasks.
    Critical = 3,
}

bitflags! {
    /// Compute capability flags advertised by a node in its heartbeat beacon.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct CapabilityFlags: u32 {
        /// NVIDIA CUDA execution path.
        const CUDA       = 1 << 0;
        /// Apple Metal execution path.
        const METAL      = 1 << 1;
        /// Vulkan compute execution path.
        const VULKAN     = 1 << 2;
        /// Neural Processing Unit available.
        const NPU        = 1 << 3;
        /// Digital Signal Processor available.
        const DSP        = 1 << 4;
        /// Field-Programmable Gate Array available.
        const FPGA       = 1 << 5;
        /// CPU vector instructions (AVX-512, NEON, SVE).
        const CPU_VECTOR = 1 << 6;
    }
}

/// Distinguishes edge tiles from datacenter anchor nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum NodeKind {
    /// Edge device: car, robot, phone, IoT. Volatile, battery-powered.
    EdgeTile = 0,
    /// Datacenter node: stable uptime, high bandwidth, idle/spot capacity.
    AnchorBallast = 1,
}

/// Complete hardware description of a node, advertised in heartbeat beacons.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HardwareProfile {
    /// Kind of node (edge vs. anchor).
    pub kind: NodeKind,
    /// GPU vendor (or `None` if CPU-only).
    pub gpu_vendor: GpuVendor,
    /// Whether a dedicated NPU is present.
    pub npu_present: bool,
    /// Host CPU architecture.
    pub cpu_arch: CpuArch,
    /// Available RAM in megabytes.
    pub memory_mb: u32,
    /// Battery charge 0–100. Use `255` for mains-powered (no battery).
    pub battery_level: u8,
    /// Current thermal state.
    pub thermal_state: ThermalState,
}

impl HardwareProfile {
    /// Returns `true` if the node is safe to accept new tasks right now.
    #[inline]
    pub fn is_available(&self) -> bool {
        self.thermal_state < ThermalState::Critical
            && (self.battery_level == 255 || self.battery_level > 10)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_profile(thermal: ThermalState, battery: u8) -> HardwareProfile {
        HardwareProfile {
            kind: NodeKind::EdgeTile,
            gpu_vendor: GpuVendor::Nvidia,
            npu_present: false,
            cpu_arch: CpuArch::X86_64,
            memory_mb: 8192,
            battery_level: battery,
            thermal_state: thermal,
        }
    }

    #[test]
    fn available_when_nominal() {
        assert!(make_profile(ThermalState::Nominal, 80).is_available());
    }

    #[test]
    fn unavailable_when_critical_thermal() {
        assert!(!make_profile(ThermalState::Critical, 80).is_available());
    }

    #[test]
    fn unavailable_when_low_battery() {
        assert!(!make_profile(ThermalState::Nominal, 5).is_available());
    }

    #[test]
    fn mains_powered_always_has_battery() {
        assert!(make_profile(ThermalState::Nominal, 255).is_available());
    }

    #[test]
    fn capability_flags_composition() {
        let flags = CapabilityFlags::CUDA | CapabilityFlags::VULKAN | CapabilityFlags::NPU;
        assert!(flags.contains(CapabilityFlags::CUDA));
        assert!(flags.contains(CapabilityFlags::NPU));
        assert!(!flags.contains(CapabilityFlags::FPGA));
    }
}
