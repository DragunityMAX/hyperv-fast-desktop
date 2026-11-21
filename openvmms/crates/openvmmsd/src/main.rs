use clap::Parser;
use openvmms_core::models::default_data_dir;
use openvmmsd::DaemonOptions;
use openvmmsd::run_daemon;
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "openvmmsd", about = "OpenVMM Virtual Machine Management Service daemon")]
struct Args {
    /// Directory for VM configs, logs, and host configuration
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// HTTP API listen address (e.g. 127.0.0.1:47620)
    #[arg(long)]
    listen: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let raw_data_dir = args.data_dir.unwrap_or_else(default_data_dir);
    let data_dir = openvmms_core::api_client::resolve_client_path(&raw_data_dir);
    let _ = std::fs::create_dir_all(&data_dir);
    // Do not retain a caller's working directory across daemon lifetime.
    let _ = std::env::set_current_dir(&data_dir);
    run_daemon(DaemonOptions {
        data_dir,
        listen_addr: args.listen,
    })
    .await
}
