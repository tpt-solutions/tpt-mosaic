//! Daemon configuration (`node.toml`) loading and validation.
//!
//! Every field is optional and falls back to the defaults documented in
//! `node.toml.example`. Unknown sections are ignored so older configs keep
//! loading on newer daemons.

use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

use tpt_mosaic_core::{
    CapabilityFlags, CpuArch, GpuVendor, HardwareProfile, NodeId, NodeKind, ThermalState,
};
use tpt_mosaic_economy::Chain;

/// Default heartbeat period when `[discovery]` is omitted.
const DEFAULT_HEARTBEAT_MS: u64 = 5_000;
/// Default peer staleness window when `[discovery]` is omitted.
const DEFAULT_PEER_MAX_AGE_MS: u64 = 30_000;
/// Default control API port when `[control]` is omitted.
const DEFAULT_CONTROL_PORT: u64 = 7331;

/// Identity and role of this node.
#[derive(Debug, Clone)]
pub struct IdentityConfig {
    /// Node identity; `None` generates a random ID at startup (persisted to
    /// `state_file` when configured).
    pub id: Option<NodeId>,
    /// Edge tile or datacenter anchor.
    pub kind: NodeKind,
    /// Optional path for the persisted identity. When set and `id` is
    /// empty, the identity is loaded from (or generated into) this file so
    /// it survives restarts.
    pub state_file: Option<std::path::PathBuf>,
}

/// Discovery and heartbeat tuning.
#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    /// Period between heartbeat broadcasts.
    pub heartbeat_interval: Duration,
    /// Peers silent longer than this are evicted from the peer table.
    pub peer_max_age: Duration,
}

/// Local control API socket.
#[derive(Debug, Clone)]
pub struct ControlConfig {
    /// Address the TCP control API binds.
    pub listen: SocketAddr,
}

/// Mesh networking (spec §4 data plane over loopback/LAN TCP).
#[derive(Debug, Clone)]
pub struct MeshConfig {
    /// Address this node's mesh listener binds; `None` disables the mesh.
    pub listen: Option<SocketAddr>,
    /// Bootstrap peer addresses contacted on every heartbeat. A static
    /// seed list is the v0 discovery model; gossip comes later.
    pub seeds: Vec<SocketAddr>,
}

/// Fully-parsed daemon configuration.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// Node identity and role.
    pub identity: IdentityConfig,
    /// Hardware description advertised in heartbeat beacons.
    pub hardware: HardwareProfile,
    /// Compute capability flags advertised in heartbeat beacons.
    pub capabilities: CapabilityFlags,
    /// Discovery tuning.
    pub discovery: DiscoveryConfig,
    /// Settlement chain for earned micro-rewards.
    pub chain: Chain,
    /// Optional RPC endpoint handed to the settlement adapter.
    pub rpc_url: Option<String>,
    /// Local control API, if enabled (port `0` disables it).
    pub control: Option<ControlConfig>,
    /// Mesh networking, if enabled.
    pub mesh: MeshConfig,
    /// Optional on-disk cache directory for compiled artifacts.
    pub jit_cache: Option<std::path::PathBuf>,
    /// Optional directory for in-flight task checkpoints, enabling resume of
    /// interrupted executions.
    pub checkpoint_dir: Option<std::path::PathBuf>,
}

/// Configuration parse failure with a human-readable description.
#[derive(Debug)]
pub struct ConfigError {
    message: String,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ConfigError {}

impl NodeConfig {
    /// Load and parse a `node.toml` file.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|e| ConfigError {
            message: format!("cannot read {}: {e}", path.display()),
        })?;
        Self::from_toml_str(&text)
    }

    /// Parse configuration from TOML text. Missing fields fall back to defaults.
    pub fn from_toml_str(text: &str) -> Result<Self, ConfigError> {
        let root: toml::Value = text.parse().map_err(|e| ConfigError {
            message: format!("invalid TOML: {e}"),
        })?;

        // ── [node] ────────────────────────────────────────────────────────
        let node = root.get("node");
        let id = match opt_str(node, "node", "id")? {
            None | Some("") => None,
            Some(hex) => Some(NodeId::from_bytes(crate::id::parse_hex16(hex).ok_or_else(
                || ConfigError {
                    message: "node.id must be 32 hex characters (16 bytes)".into(),
                },
            )?)),
        };
        let kind = match opt_str(node, "node", "kind")? {
            None | Some("edge") => NodeKind::EdgeTile,
            Some("anchor") => NodeKind::AnchorBallast,
            Some(other) => return Err(bad_value("node.kind", other, &["edge", "anchor"])),
        };
        let state_file = opt_str(node, "node", "state_file")?
            .filter(|s| !s.is_empty())
            .map(std::path::PathBuf::from);

        // ── [hardware] ────────────────────────────────────────────────────
        let hw = root.get("hardware");
        let gpu_vendor = match opt_str(hw, "hardware", "gpu_vendor")? {
            None | Some("none") => GpuVendor::None,
            Some("nvidia") => GpuVendor::Nvidia,
            Some("amd") => GpuVendor::Amd,
            Some("apple") => GpuVendor::Apple,
            Some("intel") => GpuVendor::Intel,
            Some("other") => GpuVendor::Other,
            Some(other) => {
                return Err(bad_value(
                    "hardware.gpu_vendor",
                    other,
                    &["none", "nvidia", "amd", "apple", "intel", "other"],
                ))
            }
        };
        let npu_present = opt_bool(hw, "hardware", "npu_present")?.unwrap_or(false);
        let cpu_arch = match opt_str(hw, "hardware", "cpu_arch")? {
            None | Some("x86_64") => CpuArch::X86_64,
            Some("aarch64") => CpuArch::Aarch64,
            Some("riscv64") => CpuArch::RiscV64,
            Some("other") => CpuArch::Other,
            Some(other) => {
                return Err(bad_value(
                    "hardware.cpu_arch",
                    other,
                    &["x86_64", "aarch64", "riscv64", "other"],
                ))
            }
        };
        let memory_mb = opt_u64(hw, "hardware", "memory_mb")?.unwrap_or(8_192);
        let memory_mb = u32::try_from(memory_mb).map_err(|_| ConfigError {
            message: "hardware.memory_mb must fit in 32 bits".into(),
        })?;
        let battery_level = opt_u64(hw, "hardware", "battery_level")?.unwrap_or(255);
        let battery_level = u8::try_from(battery_level).map_err(|_| ConfigError {
            message: "hardware.battery_level must be 0-255 (255 = mains power)".into(),
        })?;
        let thermal_state = match opt_str(hw, "hardware", "thermal_state")? {
            None | Some("nominal") => ThermalState::Nominal,
            Some("warm") => ThermalState::Warm,
            Some("hot") => ThermalState::Hot,
            Some("critical") => ThermalState::Critical,
            Some(other) => {
                return Err(bad_value(
                    "hardware.thermal_state",
                    other,
                    &["nominal", "warm", "hot", "critical"],
                ))
            }
        };

        // ── [capabilities] ────────────────────────────────────────────────
        let flags_value = root.get("capabilities").and_then(|c| c.get("flags"));
        let capabilities = match flags_value {
            None => CapabilityFlags::CPU_VECTOR,
            Some(v) => {
                let arr = v.as_array().ok_or_else(|| ConfigError {
                    message: "capabilities.flags must be an array of strings".into(),
                })?;
                let mut flags = CapabilityFlags::empty();
                for item in arr {
                    let s = item.as_str().ok_or_else(|| ConfigError {
                        message: "capabilities.flags entries must be strings".into(),
                    })?;
                    let flag = match s {
                        "cuda" => CapabilityFlags::CUDA,
                        "metal" => CapabilityFlags::METAL,
                        "vulkan" => CapabilityFlags::VULKAN,
                        "npu" => CapabilityFlags::NPU,
                        "dsp" => CapabilityFlags::DSP,
                        "fpga" => CapabilityFlags::FPGA,
                        "cpu_vector" => CapabilityFlags::CPU_VECTOR,
                        other => {
                            return Err(bad_value(
                                "capabilities.flags",
                                other,
                                &[
                                    "cuda",
                                    "metal",
                                    "vulkan",
                                    "npu",
                                    "dsp",
                                    "fpga",
                                    "cpu_vector",
                                ],
                            ))
                        }
                    };
                    flags |= flag;
                }
                flags
            }
        };

        // ── [discovery] ───────────────────────────────────────────────────
        let disc = root.get("discovery");
        let heartbeat_ms =
            opt_u64(disc, "discovery", "heartbeat_interval_ms")?.unwrap_or(DEFAULT_HEARTBEAT_MS);
        let peer_max_age_ms =
            opt_u64(disc, "discovery", "peer_max_age_ms")?.unwrap_or(DEFAULT_PEER_MAX_AGE_MS);
        if heartbeat_ms == 0 || peer_max_age_ms == 0 {
            return Err(ConfigError {
                message: "discovery intervals must be greater than zero".into(),
            });
        }

        // ── [economy] ─────────────────────────────────────────────────────
        let econ = root.get("economy");
        let chain = match opt_str(econ, "economy", "chain")? {
            None | Some("solana") => Chain::Solana,
            Some("base") => Chain::Base,
            Some("near") => Chain::Near,
            Some(other) => {
                return Err(bad_value(
                    "economy.chain",
                    other,
                    &["solana", "base", "near"],
                ))
            }
        };
        let rpc_url = opt_str(econ, "economy", "rpc_url")?.map(str::to_owned);

        // ── [control] ─────────────────────────────────────────────────────
        let ctrl = root.get("control");
        let listen_addr = opt_str(ctrl, "control", "listen_addr")?.unwrap_or("127.0.0.1");
        let listen_ip = IpAddr::from_str(listen_addr).map_err(|_| ConfigError {
            message: format!("control.listen_addr is not a valid IP address: {listen_addr}"),
        })?;
        let listen_port = opt_u64(ctrl, "control", "listen_port")?.unwrap_or(DEFAULT_CONTROL_PORT);
        let listen_port = u16::try_from(listen_port).map_err(|_| ConfigError {
            message: "control.listen_port must be 0-65535".into(),
        })?;
        let control = (listen_port != 0).then_some(ControlConfig {
            listen: SocketAddr::new(listen_ip, listen_port),
        });

        // ── [compiler] ────────────────────────────────────────────────────
        let jit_cache = opt_str(root.get("compiler"), "compiler", "cache_dir")?
            .filter(|s| !s.is_empty())
            .map(std::path::PathBuf::from);

        // ── [task] ────────────────────────────────────────────────────────
        let checkpoint_dir = opt_str(root.get("task"), "task", "checkpoint_dir")?
            .filter(|s| !s.is_empty())
            .map(std::path::PathBuf::from);

        // ── [mesh] ────────────────────────────────────────────────────────
        // The section's presence enables the mesh. `listen_port` defaults to
        // 0 = bind an ephemeral port (the OS picks; peers learn the real port
        // from this node's beacons).
        let mesh_sec = root.get("mesh");
        let mesh_enabled = mesh_sec.is_some();
        let mesh_port = opt_u64(mesh_sec, "mesh", "listen_port")?.unwrap_or(0);
        let mesh_port = u16::try_from(mesh_port).map_err(|_| ConfigError {
            message: "mesh.listen_port must be 0-65535".into(),
        })?;
        let mesh_listen = if mesh_enabled {
            let addr = opt_str(mesh_sec, "mesh", "listen_addr")?.unwrap_or("127.0.0.1");
            let ip = IpAddr::from_str(addr).map_err(|_| ConfigError {
                message: format!("mesh.listen_addr is not a valid IP address: {addr}"),
            })?;
            Some(SocketAddr::new(ip, mesh_port))
        } else {
            None
        };
        let mut seeds = Vec::new();
        if let Some(arr) = mesh_sec
            .and_then(|m| m.get("seeds"))
            .and_then(|s| s.as_array())
        {
            for item in arr {
                let s = item.as_str().ok_or_else(|| ConfigError {
                    message: "mesh.seeds entries must be 'ip:port' strings".into(),
                })?;
                seeds.push(SocketAddr::from_str(s).map_err(|_| ConfigError {
                    message: format!("mesh.seeds entry is not a valid 'ip:port' address: {s}"),
                })?);
            }
        } else if mesh_sec.and_then(|m| m.get("seeds")).is_some() {
            return Err(ConfigError {
                message: "mesh.seeds must be an array of 'ip:port' strings".into(),
            });
        }

        Ok(Self {
            identity: IdentityConfig {
                id,
                kind,
                state_file,
            },
            hardware: HardwareProfile {
                kind,
                gpu_vendor,
                npu_present,
                cpu_arch,
                memory_mb,
                battery_level,
                thermal_state,
            },
            capabilities,
            discovery: DiscoveryConfig {
                heartbeat_interval: Duration::from_millis(heartbeat_ms),
                peer_max_age: Duration::from_millis(peer_max_age_ms),
            },
            chain,
            rpc_url,
            control,
            mesh: MeshConfig {
                listen: mesh_listen,
                seeds,
            },
            jit_cache,
            checkpoint_dir,
        })
    }
}

fn opt_str<'a>(
    section: Option<&'a toml::Value>,
    section_name: &str,
    key: &str,
) -> Result<Option<&'a str>, ConfigError> {
    match section.and_then(|s| s.get(key)) {
        None => Ok(None),
        Some(v) => v
            .as_str()
            .map(Some)
            .ok_or_else(|| type_err(section_name, key, "a string")),
    }
}

fn opt_bool(
    section: Option<&toml::Value>,
    section_name: &str,
    key: &str,
) -> Result<Option<bool>, ConfigError> {
    match section.and_then(|s| s.get(key)) {
        None => Ok(None),
        Some(v) => v
            .as_bool()
            .map(Some)
            .ok_or_else(|| type_err(section_name, key, "a boolean")),
    }
}

fn opt_u64(
    section: Option<&toml::Value>,
    section_name: &str,
    key: &str,
) -> Result<Option<u64>, ConfigError> {
    match section.and_then(|s| s.get(key)) {
        None => Ok(None),
        Some(v) => v
            .as_integer()
            .and_then(|i| u64::try_from(i).ok())
            .map(Some)
            .ok_or_else(|| type_err(section_name, key, "a non-negative integer")),
    }
}

fn type_err(section: &str, key: &str, expected: &str) -> ConfigError {
    ConfigError {
        message: format!("{section}.{key} must be {expected}"),
    }
}

fn bad_value(section_key: &str, got: &str, allowed: &[&str]) -> ConfigError {
    ConfigError {
        message: format!("{section_key} is '{got}'; expected one of {allowed:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id;

    #[test]
    fn empty_config_yields_defaults() {
        let cfg = NodeConfig::from_toml_str("").unwrap();
        assert_eq!(cfg.identity.kind, NodeKind::EdgeTile);
        assert!(cfg.identity.id.is_none());
        assert_eq!(cfg.hardware.memory_mb, 8_192);
        assert_eq!(cfg.hardware.battery_level, 255);
        assert_eq!(cfg.capabilities, CapabilityFlags::CPU_VECTOR);
        assert_eq!(
            cfg.discovery.heartbeat_interval,
            Duration::from_millis(5_000)
        );
        assert_eq!(cfg.chain, Chain::Solana);
        assert_eq!(
            cfg.control.unwrap().listen,
            SocketAddr::from_str("127.0.0.1:7331").unwrap()
        );
        assert!(cfg.identity.state_file.is_none());
        assert!(cfg.jit_cache.is_none());
        assert!(cfg.checkpoint_dir.is_none());
    }

    #[test]
    fn parses_full_config() {
        let text = r#"
            [node]
            id = "000102030405060708090a0b0c0d0e0f"
            kind = "anchor"

            [hardware]
            gpu_vendor = "nvidia"
            npu_present = true
            cpu_arch = "aarch64"
            memory_mb = 65536
            battery_level = 255
            thermal_state = "warm"

            [capabilities]
            flags = ["cuda", "cpu_vector"]

            [discovery]
            heartbeat_interval_ms = 1000
            peer_max_age_ms = 10000

            [economy]
            chain = "near"
            rpc_url = "https://rpc.example.test"

            [control]
            listen_addr = "0.0.0.0"
            listen_port = 9000
        "#;
        let cfg = NodeConfig::from_toml_str(text).unwrap();
        assert_eq!(
            cfg.identity.id,
            Some(NodeId::from_bytes([
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
            ]))
        );
        assert_eq!(cfg.identity.kind, NodeKind::AnchorBallast);
        assert_eq!(cfg.hardware.gpu_vendor, GpuVendor::Nvidia);
        assert!(cfg.hardware.npu_present);
        assert_eq!(cfg.hardware.cpu_arch, CpuArch::Aarch64);
        assert_eq!(cfg.hardware.thermal_state, ThermalState::Warm);
        assert_eq!(
            cfg.capabilities,
            CapabilityFlags::CUDA | CapabilityFlags::CPU_VECTOR
        );
        assert_eq!(
            cfg.discovery.heartbeat_interval,
            Duration::from_millis(1_000)
        );
        assert_eq!(cfg.chain, Chain::Near);
        assert_eq!(cfg.rpc_url.as_deref(), Some("https://rpc.example.test"));
        assert_eq!(cfg.control.unwrap().listen.to_string(), "0.0.0.0:9000");
    }

    #[test]
    fn port_zero_disables_control_api() {
        let cfg = NodeConfig::from_toml_str("[control]\nlisten_port = 0").unwrap();
        assert!(cfg.control.is_none());
    }

    #[test]
    fn state_file_and_jit_cache_parse() {
        let cfg = NodeConfig::from_toml_str(
            "[node]\nstate_file = \"mosaic-node.id\"\n\n[compiler]\ncache_dir = \"target/jit\"",
        )
        .unwrap();
        assert_eq!(
            cfg.identity.state_file.as_deref(),
            Some(std::path::Path::new("mosaic-node.id"))
        );
        assert_eq!(
            cfg.jit_cache.as_deref(),
            Some(std::path::Path::new("target/jit"))
        );

        // Empty strings mean disabled.
        let off =
            NodeConfig::from_toml_str("[node]\nstate_file = \"\"\n\n[compiler]\ncache_dir = \"\"")
                .unwrap();
        assert!(off.identity.state_file.is_none());
        assert!(off.jit_cache.is_none());
    }

    #[test]
    fn checkpoint_dir_parses() {
        let cfg = NodeConfig::from_toml_str("[task]\ncheckpoint_dir = \"state/cp\"").unwrap();
        assert_eq!(
            cfg.checkpoint_dir.as_deref(),
            Some(std::path::Path::new("state/cp"))
        );
        let off = NodeConfig::from_toml_str("").unwrap();
        assert!(off.checkpoint_dir.is_none());
    }

    #[test]
    fn mesh_config_parses_and_defaults_to_disabled() {
        let cfg = NodeConfig::from_toml_str(
            "[mesh]\nlisten_port = 7745\nseeds = [\"127.0.0.1:7801\", \"[::1]:7802\"]",
        )
        .unwrap();
        assert_eq!(
            cfg.mesh.listen,
            Some(SocketAddr::from_str("127.0.0.1:7745").unwrap())
        );
        assert_eq!(cfg.mesh.seeds.len(), 2);
        assert_eq!(cfg.mesh.seeds[1].to_string(), "[::1]:7802");

        let off = NodeConfig::from_toml_str("").unwrap();
        assert!(off.mesh.listen.is_none());
        assert!(off.mesh.seeds.is_empty());
    }

    #[test]
    fn mesh_config_rejects_bad_values() {
        assert!(NodeConfig::from_toml_str("[mesh]\nseeds = \"127.0.0.1:1\"").is_err());
        assert!(NodeConfig::from_toml_str("[mesh]\nseeds = [\"nohost\"]").is_err());
        assert!(NodeConfig::from_toml_str("[mesh]\nlisten_port = 99999").is_err());
    }

    #[test]
    fn rejects_unknown_enum_values() {
        assert!(NodeConfig::from_toml_str("[node]\nkind = \"satellite\"").is_err());
        assert!(NodeConfig::from_toml_str("[economy]\nchain = \"ton\"").is_err());
        assert!(NodeConfig::from_toml_str("[capabilities]\nflags = [\"quantum\"]").is_err());
        assert!(NodeConfig::from_toml_str("[hardware]\nbattery_level = 900").is_err());
        assert!(NodeConfig::from_toml_str("node.id = \"nothex\"").is_err());
    }

    #[test]
    fn generated_default_id_round_trips() {
        // The config accepts any hex string the generator can produce.
        let hex = id::to_hex(&id::generate_bytes());
        let cfg = NodeConfig::from_toml_str(&format!("[node]\nid = \"{hex}\"")).unwrap();
        assert!(cfg.identity.id.is_some());
    }
}
