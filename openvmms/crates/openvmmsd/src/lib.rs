pub mod server;
pub mod supervisor;

use anyhow::Context;
use openvmms_core::models::DEFAULT_DAEMON_LISTEN;
use openvmms_core::models::default_data_dir;
use openvmms_core::models::parse_loopback_listen_addr;
use std::path::PathBuf;
use std::sync::Arc;
use supervisor::Supervisor;
use tokio::net::TcpListener;
use tracing::info;

#[derive(Debug, Clone)]
pub struct DaemonOptions {
    pub data_dir: PathBuf,
    pub listen_addr: Option<String>,
}

impl Default for DaemonOptions {
    fn default() -> Self {
        Self {
            data_dir: default_data_dir(),
            listen_addr: None,
        }
    }
}

pub async fn run_daemon(opts: DaemonOptions) -> anyhow::Result<()> {
    let supervisor = Supervisor::new(&opts.data_dir, opts.listen_addr.as_deref())?;
    let listen_addr = supervisor.listen_addr();
    let addr = parse_loopback_listen_addr(&listen_addr)?;

    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding openvmmsd HTTP server on {addr}"))?;
    // Adopt running VMs only after listen validation and TCP bind have succeeded so a failed
    // startup never alters or cleans up running VM runtime files.
    supervisor.adopt_running_vms();
    let bound_addr = supervisor.serve_on_listener(listener)?;
    info!(address = %bound_addr, data_dir = %opts.data_dir.display(), "openvmmsd listening");

    tokio::select! {
        _ = wait_for_os_shutdown_signal() => {
            info!("received shutdown signal");
        }
        _ = supervisor.wait_for_shutdown_signal() => {
            info!("received API shutdown request");
        }
    }
    supervisor.mark_shutting_down();
    if supervisor.should_stop_vms_on_shutdown() {
        supervisor.shutdown_all_vms().await;
    } else {
        supervisor.detach_all_vms().await;
    }
    supervisor.join_all_listeners().await;
    supervisor.clear_runtime_listen_addr();

    Ok(())
}

#[cfg(unix)]
async fn wait_for_os_shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sigterm = signal(SignalKind::terminate()).ok();
    let mut sighup = signal(SignalKind::hangup()).ok();
    let mut sigquit = signal(SignalKind::quit()).ok();

    let wait_term = async {
        if let Some(s) = sigterm.as_mut() {
            let _ = s.recv().await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    let wait_hup = async {
        if let Some(s) = sighup.as_mut() {
            let _ = s.recv().await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    let wait_quit = async {
        if let Some(s) = sigquit.as_mut() {
            let _ = s.recv().await;
        } else {
            std::future::pending::<()>().await;
        }
    };

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = wait_term => {}
        _ = wait_hup => {}
        _ = wait_quit => {}
    }
}

#[cfg(windows)]
async fn wait_for_os_shutdown_signal() {
    use tokio::signal::windows;

    let mut ctrl_break = windows::ctrl_break().ok();
    let mut ctrl_close = windows::ctrl_close().ok();
    let mut ctrl_shutdown = windows::ctrl_shutdown().ok();

    let wait_break = async {
        if let Some(s) = ctrl_break.as_mut() {
            let _ = s.recv().await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    let wait_close = async {
        if let Some(s) = ctrl_close.as_mut() {
            let _ = s.recv().await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    let wait_shutdown = async {
        if let Some(s) = ctrl_shutdown.as_mut() {
            let _ = s.recv().await;
        } else {
            std::future::pending::<()>().await;
        }
    };

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = wait_break => {}
        _ = wait_close => {}
        _ = wait_shutdown => {}
    }
}

#[cfg(not(any(unix, windows)))]
async fn wait_for_os_shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

pub struct TestDaemonHandle {
    pub base_url: String,
    pub token: String,
    pub supervisor: Arc<Supervisor>,
    task: tokio::task::JoinHandle<()>,
}

impl TestDaemonHandle {
    pub async fn spawn(data_dir: PathBuf) -> anyhow::Result<Self> {
        let supervisor = Supervisor::new(&data_dir, Some("127.0.0.1:0"))?;
        let token = supervisor.auth_token().to_string();
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        supervisor.adopt_running_vms();
        let bound_addr = supervisor.serve_on_listener(listener)?;

        let shutdown_sup = supervisor.clone();
        let task = tokio::spawn(async move {
            shutdown_sup.wait_for_shutdown_signal().await;
            shutdown_sup.mark_shutting_down();
            if shutdown_sup.should_stop_vms_on_shutdown() {
                shutdown_sup.shutdown_all_vms().await;
            } else {
                shutdown_sup.detach_all_vms().await;
            }
            shutdown_sup.join_all_listeners().await;
            shutdown_sup.clear_runtime_listen_addr();
        });

        Ok(Self {
            base_url: format!("http://{bound_addr}"),
            token,
            supervisor,
            task,
        })
    }

    pub async fn shutdown(self) {
        self.supervisor.request_shutdown_with_options(true);
        let _ = self.task.await;
    }

    pub async fn shutdown_preserving_vms(self) {
        self.supervisor.request_shutdown_with_options(false);
        let _ = self.task.await;
    }
}

pub fn default_listen_addr() -> &'static str {
    DEFAULT_DAEMON_LISTEN
}
