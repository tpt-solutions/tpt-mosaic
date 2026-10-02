//! tpt-mosaic node daemon — the single entry point that runs on every device
//! (edge tile or datacenter anchor) and wires all subsystems together.
//!
//! Usage: `tpt-mosaic-node [--config <path>]` with a `node.toml`; see
//! `node.toml.example` for a documented template.

mod config;
mod control;
mod daemon;
mod id;
#[cfg(test)]
mod mesh_integration;

use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use config::NodeConfig;
use daemon::NodeDaemon;

const USAGE: &str = "\
tpt-mosaic-node — decentralized ambient compute node

USAGE:
    tpt-mosaic-node [--config <path>]

OPTIONS:
    -c, --config <path>    Path to the configuration file (default: node.toml)
    -h, --help             Print this help and exit
";

fn main() -> ExitCode {
    let mut config_path = PathBuf::from("node.toml");
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                print!("{USAGE}");
                return ExitCode::SUCCESS;
            }
            "-c" | "--config" => match args.next() {
                Some(path) => config_path = PathBuf::from(path),
                None => {
                    eprintln!("error: {arg} requires a path argument");
                    eprint!("{USAGE}");
                    return ExitCode::from(2);
                }
            },
            other => {
                eprintln!("error: unknown argument '{other}'");
                eprint!("{USAGE}");
                return ExitCode::from(2);
            }
        }
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let node_config = match NodeConfig::load(&config_path) {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("error loading {}: {e}", config_path.display());
            eprintln!("hint: copy node.toml.example to node.toml and edit it");
            return ExitCode::from(1);
        }
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| {
            eprintln!("error: failed to start async runtime: {e}");
            ExitCode::from(1)
        });

    match rt {
        Ok(runtime) => {
            let daemon = Arc::new(NodeDaemon::new(node_config));
            match runtime.block_on(daemon.run()) {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    eprintln!("fatal: {e}");
                    ExitCode::from(1)
                }
            }
        }
        Err(code) => code,
    }
}
