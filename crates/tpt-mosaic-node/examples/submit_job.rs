//! Submit a job to a *running* node through the loopback control API — the
//! way an operator or an external scheduler would.
//!
//! ```text
//! cargo run -p tpt-mosaic-node --example submit_job
//! ```
//!
//! The example starts a full daemon (control API on 127.0.0.1:17331), then
//! connects as a plain TCP client and drives the line protocol:
//! `STATUS`, `SUBMIT`, `PEERS`.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use tpt_mosaic_node::config::NodeConfig;
use tpt_mosaic_node::daemon::NodeDaemon;

/// Dev port chosen away from the default (7331) so the example can run next
/// to a real node.
const CONTROL_PORT: u16 = 17331;

fn main() -> ExitCode {
    let config = NodeConfig::from_toml_str(&format!(
        "[control]\nlisten_addr = \"127.0.0.1\"\nlisten_port = {CONTROL_PORT}\n"
    ))
    .expect("example config is valid");

    // Run the daemon (async runtime + Ctrl-C loop) on a background thread;
    // this example only lives long enough to submit one job.
    let daemon = Arc::new(NodeDaemon::new(config));
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        rt.block_on(daemon.run())
            .expect("daemon runs until shutdown");
    });

    // Wait for the control API to accept connections.
    let mut stream = None;
    for _ in 0..100 {
        if let Ok(s) = TcpStream::connect(("127.0.0.1", CONTROL_PORT)) {
            stream = Some(s);
            break;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let mut stream = stream.expect("control API must come up");

    let status = ask(&mut stream, "STATUS");
    println!("> STATUS\n{status}");
    let reply = ask(&mut stream, "SUBMIT best 68656c6c6f2c206d6f73616963"); // "hello, mosaic"
    println!("> SUBMIT best <hex payload>\n{reply}");
    let peers = ask(&mut stream, "PEERS");
    println!("> PEERS\n{peers}");

    ExitCode::SUCCESS
}

/// Send one command line and read the single `OK`/`ERR` response line.
fn ask(stream: &mut TcpStream, command: &str) -> String {
    stream
        .write_all(format!("{command}\n").as_bytes())
        .expect("control connection is alive");
    let mut line = String::new();
    BufReader::new(stream)
        .read_line(&mut line)
        .expect("control API must answer");
    line.trim_end().to_owned()
}
