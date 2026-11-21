use anyhow::Context;
use anyhow::bail;
use chrono::Utc;
use openvmms_core::disk;
use openvmms_core::models::AttachDiskPayload;
use openvmms_core::models::CreateDiskPayload;
use openvmms_core::models::CreateVmPayload;
use openvmms_core::models::DaemonEvent;
use openvmms_core::models::DiskAttachment;
use openvmms_core::models::DiskFormat;
use openvmms_core::models::DiskInfo;
use openvmms_core::models::DiskKind;
use openvmms_core::models::FastDeskTransport;
use openvmms_core::models::HostConfig;
use openvmms_core::models::HostInfo;
use openvmms_core::models::IpProto;
use openvmms_core::models::LiveVmProperties;
use openvmms_core::models::MountIsoPayload;
use openvmms_core::models::NicAttachment;
use openvmms_core::models::NicBackendConfig;
use openvmms_core::models::PortForward;
use openvmms_core::models::ResolvedHostPaths;
use openvmms_core::models::SecureBootTemplateConfig;
use openvmms_core::models::ShutdownVmPayload;
use openvmms_core::models::UpdateVmPayload;
use openvmms_core::models::VirtualSwitchInfo;
use openvmms_core::models::VmConfig;
use openvmms_core::models::VmRuntimeState;
use openvmms_core::models::VmStatus;
use openvmms_core::models::VmSummary;
use openvmms_core::models::resolve_host_paths;
use openvmms_core::models::parse_loopback_listen_addr;
use openvmms_core::store::RegistryStore;
use openvmms_core::ttrpc::TtrpcClient;
use openvmms_core::ttrpc::WaitVmOutcome;
use openvmms_core::vmservice;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::net::SocketAddr;
use std::path::Path;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::RwLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::watch;
use tracing::info;
use tracing::warn;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum ApiErrorKind {
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    Internal(String),
}

pub(crate) fn err_not_found(msg: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(ApiErrorKind::NotFound(msg.into()))
}

pub(crate) fn err_conflict(msg: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(ApiErrorKind::Conflict(msg.into()))
}

pub(crate) fn err_bad_request(msg: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(ApiErrorKind::BadRequest(msg.into()))
}

#[allow(dead_code)]
pub(crate) fn err_internal(msg: impl Into<String>) -> anyhow::Error {
    anyhow::Error::new(ApiErrorKind::Internal(msg.into()))
}

struct ManagedVm {
    config: VmConfig,
    runtime: VmRuntimeState,
    child: Option<Child>,
    adopted_proc: Option<Arc<openvmms_core::process::AdoptedProcess>>,
    monitor_cancel: Option<watch::Sender<bool>>,
    monitor_handle: Option<tokio::task::JoinHandle<()>>,
    generation: u64,
    start_cancel: Option<watch::Sender<bool>>,
    stop_wait: Option<watch::Receiver<bool>>,
    prepare_lock: Arc<tokio::sync::Mutex<()>>,
    power_op_lock: Arc<tokio::sync::Mutex<()>>,
    detach_on_drop: bool,
}

impl Drop for ManagedVm {
    fn drop(&mut self) {
        if let Some(tx) = self.monitor_cancel.take() {
            let _ = tx.send(true);
        }
        if let Some(handle) = self.monitor_handle.take() {
            handle.abort();
        }
        if let Some(mut child) = self.child.take()
            && !self.detach_on_drop
        {
            let _ = child.kill();
            let _ = child.wait();
            cleanup_runtime_files(
                self.runtime.rpc_socket_path.take(),
                self.runtime.shm_path.take(),
            );
        }
    }
}

struct ActiveListener {
    addr: SocketAddr,
    stop_tx: watch::Sender<bool>,
    handle: tokio::task::JoinHandle<()>,
}

impl ManagedVm {
    fn summary(&self) -> VmSummary {
        VmSummary {
            config: self.config.clone(),
            runtime: self.runtime.clone(),
        }
    }

    fn active_rpc_socket(&self, action: &str) -> anyhow::Result<Option<PathBuf>> {
        match self.runtime.status {
            VmStatus::Starting | VmStatus::Stopping => Err(err_conflict(format!(
                "cannot {action} while VM '{}' is {}",
                self.config.name, self.runtime.status
            ))),
            VmStatus::Running | VmStatus::Paused => Ok(Some(
                self.runtime
                    .rpc_socket_path
                    .clone()
                    .ok_or_else(|| err_internal("VM is active but has no RPC socket"))?,
            )),
            VmStatus::Off | VmStatus::Error => Ok(None),
        }
    }
}

pub struct Supervisor {
    instance_id: Uuid,
    store: RegistryStore,
    _daemon_lock: fs::File,
    auth_token: String,
    initial_listen_addr: String,
    host_config: RwLock<HostConfig>,
    host_config_lock: tokio::sync::Mutex<()>,
    active_listener: std::sync::Mutex<Option<ActiveListener>>,
    draining_listeners: std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>,
    vms: RwLock<HashMap<Uuid, ManagedVm>>,
    events_tx: broadcast::Sender<DaemonEvent>,
    event_seq: AtomicU64,
    shutdown_watch: watch::Sender<bool>,
    shutting_down: AtomicBool,
    runtime_dir_changing: AtomicBool,
    stop_vms_on_shutdown: AtomicBool,
}

impl Supervisor {
    pub fn new(data_dir: &Path, listen_override: Option<&str>) -> anyhow::Result<Arc<Self>> {
        let store = RegistryStore::new(data_dir)?;
        let daemon_lock = store.try_acquire_daemon_lock()?;
        let host_config = store.load_or_init_host_config()?;
        let raw_listen_addr = listen_override
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| host_config.listen_addr.clone());
        let initial_listen_addr = parse_loopback_listen_addr(&raw_listen_addr)?.to_string();
        let auth_token = store.load_or_create_token()?;
        let loaded_vms = store.load_all_vms()?;

        let mut vm_map = HashMap::new();
        for cfg in loaded_vms {
            vm_map.insert(
                cfg.id,
                ManagedVm {
                    config: cfg,
                    runtime: VmRuntimeState::default(),
                    child: None,
                    adopted_proc: None,
                    monitor_cancel: None,
                    monitor_handle: None,
                    generation: 0,
                    start_cancel: None,
                    stop_wait: None,
                    prepare_lock: Arc::new(tokio::sync::Mutex::new(())),
                    power_op_lock: Arc::new(tokio::sync::Mutex::new(())),
                    detach_on_drop: false,
                },
            );
        }

        let (events_tx, _) = broadcast::channel(256);
        let (shutdown_watch, _) = watch::channel(false);
        let supervisor = Arc::new(Self {
            instance_id: Uuid::new_v4(),
            store,
            _daemon_lock: daemon_lock,
            auth_token,
            initial_listen_addr,
            host_config: RwLock::new(host_config),
            host_config_lock: tokio::sync::Mutex::new(()),
            active_listener: std::sync::Mutex::new(None),
            draining_listeners: std::sync::Mutex::new(Vec::new()),
            vms: RwLock::new(vm_map),
            events_tx,
            event_seq: AtomicU64::new(1),
            shutdown_watch,
            shutting_down: AtomicBool::new(false),
            runtime_dir_changing: AtomicBool::new(false),
            stop_vms_on_shutdown: AtomicBool::new(false),
        });

        Ok(supervisor)
    }

    fn next_revision(&self) -> u64 {
        self.event_seq.fetch_add(1, Ordering::SeqCst)
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::Acquire) || *self.shutdown_watch.borrow()
    }

    /// Probes existing VM RPC sockets under `runtime_dir` and re-adopts running VMs.
    /// Must only be called after the daemon has validated config and bound its HTTP listener.
    pub fn adopt_running_vms(self: &Arc<Self>) {
        let runtime_dir = self.host_config().runtime_dir;
        let expected_bin = self.resolved_paths().openvmm_bin;
        let mut adopted_vms = Vec::new();
        {
            let mut guard = self.vms.write().unwrap();
            for vm in guard.values_mut() {
                if vm.generation != 0 || vm.runtime.status != VmStatus::Off {
                    continue;
                }
                let id = vm.config.id;
                let sock_path = runtime_dir.join(format!("{id}.sock"));
                if !sock_path.exists() {
                    continue;
                }
                let pid_path = sock_path.with_extension("pid");
                let shm_candidate = runtime_dir.join(format!("vram-{id}.shm"));
                let ttrpc_client = TtrpcClient::new(&sock_path);
                match ttrpc_client.properties_vm_blocking(Duration::from_secs(2)) {
                    Ok(props)
                        if props.state == vmservice::VmState::Running as i32
                            || props.state == vmservice::VmState::Paused as i32 =>
                    {
                        let verified = openvmms_core::process::open_verified_pid(
                            &pid_path,
                            expected_bin.as_deref(),
                        );
                        vm.runtime.status = if props.state == vmservice::VmState::Paused as i32 {
                            VmStatus::Paused
                        } else {
                            VmStatus::Running
                        };
                        vm.runtime.pid = verified.as_ref().map(|p| p.pid());
                        vm.adopted_proc = verified;
                        vm.runtime.rpc_socket_path = Some(sock_path.clone());
                        vm.runtime.fastdesk_endpoint = props
                            .fastdesk_listen_addr
                            .as_deref()
                            .map(normalize_fastdesk_endpoint);
                        vm.runtime.shm_path = props.fastdesk_shm_path.or_else(|| {
                            shm_candidate
                                .exists()
                                .then(|| shm_candidate.to_string_lossy().into_owned())
                        });
                        vm.runtime.revision = self.next_revision();
                        vm.generation = 1;
                        adopted_vms.push((id, sock_path));
                        info!(vm_id = %id, name = %vm.config.name, status = %vm.runtime.status, "re-adopted running OpenVMM VM");
                    }
                    Ok(_) => {
                        let _ = ttrpc_client.teardown_and_quit_blocking(Duration::from_secs(2));
                        cleanup_runtime_files(
                            Some(sock_path),
                            shm_candidate
                                .exists()
                                .then(|| shm_candidate.to_string_lossy().into_owned()),
                        );
                    }
                    Err(_) => {
                        if !ttrpc_client.is_socket_accepting() {
                            cleanup_runtime_files(
                                Some(sock_path),
                                shm_candidate
                                    .exists()
                                    .then(|| shm_candidate.to_string_lossy().into_owned()),
                            );
                        } else {
                            let verified = openvmms_core::process::open_verified_pid(
                                &pid_path,
                                expected_bin.as_deref(),
                            );
                            vm.runtime.status = VmStatus::Running;
                            vm.runtime.pid = verified.as_ref().map(|p| p.pid());
                            vm.adopted_proc = verified;
                            vm.runtime.rpc_socket_path = Some(sock_path.clone());
                            vm.runtime.shm_path = shm_candidate
                                .exists()
                                .then(|| shm_candidate.to_string_lossy().into_owned());
                            vm.runtime.revision = self.next_revision();
                            vm.generation = 1;
                            adopted_vms.push((id, sock_path));
                            warn!(
                                vm_id = %id,
                                name = %vm.config.name,
                                "re-adopted running OpenVMM VM (properties probe failed; monitor will refresh)"
                            );
                        }
                    }
                }
            }
        }

        if !adopted_vms.is_empty()
            && let Ok(handle) = tokio::runtime::Handle::try_current()
        {
            for (id, sock_path) in adopted_vms {
                let (cancel_tx, cancel_rx) = watch::channel(false);
                let sup = Arc::clone(self);
                let monitor = handle.spawn(async move {
                    sup.monitor_vm(id, 1, sock_path, cancel_rx).await;
                });
                if let Some(vm) = self.vms.write().unwrap().get_mut(&id)
                    && vm.generation == 1
                {
                    vm.monitor_cancel = Some(cancel_tx);
                    vm.monitor_handle = Some(monitor);
                } else {
                    let _ = cancel_tx.send(true);
                    monitor.abort();
                }
            }
        }
    }

    pub fn auth_token(&self) -> &str {
        &self.auth_token
    }

    pub fn listen_addr(&self) -> String {
        if let Some(active) = self.active_listener.lock().unwrap().as_ref() {
            return active.addr.to_string();
        }
        self.initial_listen_addr.clone()
    }

    pub fn host_config(&self) -> HostConfig {
        self.host_config.read().unwrap().clone()
    }

    pub fn resolved_paths(&self) -> ResolvedHostPaths {
        let cfg = self.host_config.read().unwrap().clone();
        resolve_host_paths(&cfg)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<DaemonEvent> {
        self.events_tx.subscribe()
    }

    pub fn subscribe_shutdown(&self) -> watch::Receiver<bool> {
        self.shutdown_watch.subscribe()
    }

    pub fn mark_shutting_down(&self) {
        {
            let guard = self.vms.write().unwrap();
            self.shutting_down.store(true, Ordering::Release);
            for vm in guard.values() {
                if vm.runtime.status == VmStatus::Starting
                    && let Some(tx) = vm.start_cancel.as_ref()
                {
                    let _ = tx.send(true);
                }
            }
        }
        let _ = self.events_tx.send(DaemonEvent::DaemonStopping);
        let _ = self.shutdown_watch.send(true);
    }

    pub fn request_shutdown(&self) {
        self.request_shutdown_with_options(false);
    }

    pub fn request_shutdown_with_options(&self, stop_vms: bool) {
        if stop_vms {
            self.stop_vms_on_shutdown.store(true, Ordering::SeqCst);
        }
        self.mark_shutting_down();
    }

    pub fn should_stop_vms_on_shutdown(&self) -> bool {
        self.stop_vms_on_shutdown.load(Ordering::SeqCst)
    }

    pub fn clear_runtime_listen_addr(&self) {
        self.store.clear_runtime_listen_addr();
    }

    pub async fn wait_for_shutdown_signal(&self) {
        let mut rx = self.shutdown_watch.subscribe();
        while !*rx.borrow_and_update() {
            if rx.changed().await.is_err() {
                break;
            }
        }
    }

    fn emit(&self, event: DaemonEvent) {
        let _ = self.events_tx.send(event);
    }

    pub fn host_info(&self) -> HostInfo {
        let cfg = self.host_config();
        let resolved_paths = resolve_host_paths(&cfg);
        let hostname = std::env::var("HOSTNAME")
            .or_else(|_| std::env::var("COMPUTERNAME"))
            .unwrap_or_else(|_| "localhost".to_string());
        HostInfo {
            hostname,
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            cpu_count: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1),
            daemon_version: env!("CARGO_PKG_VERSION").to_string(),
            data_dir: cfg.data_dir,
            runtime_dir: cfg.runtime_dir,
            listen_addr: self.listen_addr(),
            resolved_paths,
            daemon_instance_id: Some(self.instance_id),
        }
    }

    /// Spawns an HTTP API server on `listener` and shuts down any previously active listener.
    pub fn serve_on_listener(
        self: &Arc<Self>,
        listener: tokio::net::TcpListener,
    ) -> anyhow::Result<SocketAddr> {
        let bound_addr = listener.local_addr()?;
        let app = crate::server::build_router(Arc::clone(self), self.auth_token.clone(), bound_addr);
        let mut shutdown_rx = self.subscribe_shutdown();
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let handle = tokio::spawn(async move {
            let result = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let already_stopping = *shutdown_rx.borrow() || *stop_rx.borrow();
                    if already_stopping {
                        return;
                    }
                    tokio::select! {
                        _ = shutdown_rx.changed() => {}
                        _ = stop_rx.changed() => {}
                    }
                })
                .await;
            if let Err(err) = result {
                warn!(address = %bound_addr, error = %err, "openvmmsd HTTP listener exited with error");
            }
        });
        let previous = self.active_listener.lock().unwrap().replace(ActiveListener {
            addr: bound_addr,
            stop_tx,
            handle,
        });
        self.store
            .write_runtime_listen_addr(&bound_addr.to_string())?;
        if let Some(mut old) = previous {
            let _ = old.stop_tx.send(true);
            let drain = tokio::spawn(async move {
                if tokio::time::timeout(Duration::from_millis(200), &mut old.handle)
                    .await
                    .is_err()
                {
                    old.handle.abort();
                    let _ = old.handle.await;
                }
            });
            self.draining_listeners.lock().unwrap().push(drain);
        }
        Ok(bound_addr)
    }

    /// Waits for all active HTTP listener tasks started by this supervisor to exit.
    pub async fn join_all_listeners(&self) {
        let active = self.active_listener.lock().unwrap().take();
        if let Some(listener) = active {
            let _ = listener.stop_tx.send(true);
            let _ = listener.handle.await;
        }
        let drains = {
            let mut guard = self.draining_listeners.lock().unwrap();
            std::mem::take(&mut *guard)
        };
        for handle in drains {
            let _ = handle.await;
        }
    }

    pub async fn update_host_config(
        self: &Arc<Self>,
        mut new_cfg: HostConfig,
    ) -> anyhow::Result<HostConfig> {
        let _lock = self.host_config_lock.lock().await;
        let current_cfg = self.host_config();
        if new_cfg.data_dir.as_os_str().is_empty() {
            new_cfg.data_dir = self.store.data_dir().to_path_buf();
        }
        if new_cfg.runtime_dir.as_os_str().is_empty() {
            new_cfg.runtime_dir = openvmms_core::models::default_runtime_dir(&new_cfg.data_dir);
        }
        if new_cfg.data_dir != current_cfg.data_dir {
            return Err(err_bad_request(
                "cannot change data_dir over the API; restart openvmmsd with --data-dir instead",
            ));
        }
        openvmms_core::models::validate_runtime_dir(&new_cfg.runtime_dir)
            .map_err(|e| err_bad_request(format!("{e:#}")))?;
        struct ResetRuntimeDirChanging<'a>(&'a AtomicBool);
        impl Drop for ResetRuntimeDirChanging<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::Release);
            }
        }

        let changing_runtime_dir = new_cfg.runtime_dir != current_cfg.runtime_dir;
        let _runtime_dir_guard = if changing_runtime_dir {
            let guard = self.vms.write().unwrap();
            let has_active_vms = guard
                .values()
                .any(|vm| !matches!(vm.runtime.status, VmStatus::Off | VmStatus::Error));
            if has_active_vms {
                return Err(err_conflict(
                    "cannot change runtime_dir while VMs are active; stop all running VMs first",
                ));
            }
            self.runtime_dir_changing.store(true, Ordering::Release);
            Some(ResetRuntimeDirChanging(&self.runtime_dir_changing))
        } else {
            None
        };

        let target_addr = parse_loopback_listen_addr(&new_cfg.listen_addr)
            .map_err(|e| err_bad_request(format!("{e:#}")))?;

        let configured_addr = target_addr.to_string();
        let listen_changed = configured_addr != current_cfg.listen_addr || target_addr.port() == 0;
        let active_addr = self
            .active_listener
            .lock()
            .unwrap()
            .as_ref()
            .map(|l| l.addr);
        if listen_changed && active_addr.is_some() && active_addr != Some(target_addr) {
            let listener = tokio::net::TcpListener::bind(target_addr)
                .await
                .with_context(|| format!("binding openvmmsd HTTP server on {target_addr}"))?;
            let bound_addr = self.serve_on_listener(listener)?;
            new_cfg.listen_addr = bound_addr.to_string();
        } else {
            new_cfg.listen_addr = configured_addr;
        }

        if changing_runtime_dir {
            let _vms_guard = self.vms.write().unwrap();
            self.store.save_host_config(&new_cfg)?;
            *self.host_config.write().unwrap() = new_cfg.clone();
        } else {
            self.store.save_host_config(&new_cfg)?;
            *self.host_config.write().unwrap() = new_cfg.clone();
        }
        drop(_runtime_dir_guard);
        self.emit(DaemonEvent::HostConfigUpdated);
        Ok(new_cfg)
    }

    pub fn list_switches(&self) -> Vec<VirtualSwitchInfo> {
        let mut out = vec![VirtualSwitchInfo {
            id: "consomme".to_string(),
            name: "Default NAT (Consomme)".to_string(),
            kind: "nat".to_string(),
            description: "User-mode NAT with live TCP/UDP host port forwarding".to_string(),
        }];

        #[cfg(windows)]
        {
            out.extend(discover_windows_switches());
        }
        #[cfg(target_os = "linux")]
        {
            out.extend(discover_linux_tap_interfaces());
        }
        out
    }

    fn resolve_vm_id(&self, id_or_name: &str) -> anyhow::Result<Uuid> {
        let guard = self.vms.read().unwrap();
        if let Ok(uuid) = Uuid::parse_str(id_or_name)
            && guard.contains_key(&uuid)
        {
            return Ok(uuid);
        }
        let target = id_or_name.trim().to_ascii_lowercase();
        let mut matches = guard
            .values()
            .filter(|m| m.config.name.to_ascii_lowercase() == target)
            .map(|m| m.config.id);
        if let Some(first) = matches.next() {
            if matches.next().is_some() {
                return Err(err_conflict(format!(
                    "multiple VMs share name '{id_or_name}'; specify UUID"
                )));
            }
            return Ok(first);
        }
        Err(err_not_found(format!("VM not found: {id_or_name}")))
    }

    pub fn list_vms(&self) -> Vec<VmSummary> {
        let guard = self.vms.read().unwrap();
        let mut items: Vec<VmSummary> = guard.values().map(ManagedVm::summary).collect();
        items.sort_by(|a, b| {
            a.config
                .name
                .to_lowercase()
                .cmp(&b.config.name.to_lowercase())
                .then(a.config.id.cmp(&b.config.id))
        });
        items
    }

    pub fn get_vm(&self, id_or_name: &str) -> anyhow::Result<VmSummary> {
        let id = self.resolve_vm_id(id_or_name)?;
        let guard = self.vms.read().unwrap();
        Ok(guard.get(&id).context("VM not found")?.summary())
    }

    pub async fn get_vm_with_live_properties(&self, id_or_name: &str) -> anyhow::Result<VmSummary> {
        let id = self.resolve_vm_id(id_or_name)?;
        let (status, sock_path, rpc_gen) = {
            let guard = self.vms.read().unwrap();
            let vm = guard.get(&id).context("VM not found")?;
            (
                vm.runtime.status,
                vm.runtime.rpc_socket_path.clone(),
                vm.generation,
            )
        };

        if matches!(status, VmStatus::Running | VmStatus::Paused)
            && let Some(sock) = sock_path
            && let Ok(props) = TtrpcClient::new(sock)
                .properties_vm(vmservice::PropertiesVmRequest {
                    types: vec![
                        vmservice::properties_vm_request::PropertiesType::Memory as i32,
                        vmservice::properties_vm_request::PropertiesType::Processor as i32,
                    ],
                })
                .await
        {
            let state_str = match vmservice::VmState::from_i32(props.state) {
                Some(vmservice::VmState::Uninitialized) => "Uninitialized",
                Some(vmservice::VmState::Paused) => "Paused",
                Some(vmservice::VmState::Running) => "Running",
                Some(vmservice::VmState::Halted) => "Halted",
                None => "Unknown",
            }
            .to_string();
            let live = LiveVmProperties {
                state: state_str,
                halt_reason: props.halt_reason,
                working_set_bytes: props.memory_stats.map(|m| m.working_set_bytes),
                total_runtime_ns: props.processor_stats.map(|p| p.total_runtime_ns),
            };
            let mut guard = self.vms.write().unwrap();
            if let Some(vm) = guard.get_mut(&id)
                && vm.generation == rpc_gen
                && matches!(vm.runtime.status, VmStatus::Running | VmStatus::Paused)
            {
                vm.runtime.live_properties = Some(live);
                vm.runtime.revision = self.next_revision();
                return Ok(vm.summary());
            }
        }

        self.get_vm(&id.to_string())
    }

    pub fn create_vm(&self, payload: CreateVmPayload) -> anyhow::Result<VmSummary> {
        self.create_vm_cancellable(payload, &AtomicBool::new(false))
    }

    pub fn create_vm_cancellable(
        &self,
        payload: CreateVmPayload,
        cancelled: &AtomicBool,
    ) -> anyhow::Result<VmSummary> {
        let trimmed_name = payload.name.trim().to_string();
        openvmms_core::models::validate_vm_name(&trimmed_name)
            .map_err(|e| err_bad_request(format!("{e:#}")))?;

        {
            let guard = self.vms.read().unwrap();
            if guard
                .values()
                .any(|m| m.config.name.eq_ignore_ascii_case(&trimmed_name))
            {
                return Err(err_conflict(format!(
                    "a VM named '{trimmed_name}' already exists"
                )));
            }
        }

        let probe_id = Uuid::new_v4();
        let vm_dir = self.store.vm_dir(probe_id);
        let mut cfg = VmConfig::from_preset(trimmed_name.clone(), payload.preset, &vm_dir);
        cfg.id = probe_id;
        cfg.vmgs_path = Some(vm_dir.join("state.vmgs"));
        cfg.description = payload.description;

        if let Some(vcpus) = payload.vcpus {
            cfg.vcpus = vcpus;
        }
        if let Some(memory_mb) = payload.memory_mb {
            cfg.memory_mb = memory_mb;
        }
        if let Some(nested) = payload.nested_virt {
            cfg.nested_virt = nested;
        }
        if let Some(fw) = payload.uefi_firmware.filter(|p| !p.as_os_str().is_empty()) {
            cfg.uefi_firmware = Some(fw);
        }
        if let Some(sb) = payload.secure_boot_enabled {
            cfg.secure_boot_enabled = sb;
        }
        if let Some(tpl) = payload.secure_boot_template {
            cfg.secure_boot_template = tpl;
        }
        if let Some(tpm) = payload.tpm_enabled {
            cfg.tpm_enabled = tpm;
        }
        if let Some(fd) = payload.fastdesk {
            cfg.fastdesk = fd;
        }
        if let Some(nics) = payload.nics {
            cfg.nics = nics;
        }

        if let Some(iso_path) = &payload.iso_path
            && !iso_path.is_file()
        {
            return Err(err_bad_request(format!(
                "ISO image not found: {}",
                iso_path.display()
            )));
        }

        let paths = self.resolved_paths();

        for mut disk_att in payload.disks {
            if disk_att.kind == DiskKind::Dvd
                || disk_att.format == DiskFormat::Iso
                || DiskFormat::from_path(&disk_att.path) == DiskFormat::Iso
            {
                disk_att.kind = DiskKind::Dvd;
                disk_att.read_only = true;
                disk_att.format = DiskFormat::Iso;
            }
            cfg.disks.push(disk_att);
        }

        let pending_disk_req = if let Some(mut disk_req) = payload.create_disk {
            if disk_req.path.as_os_str().is_empty() {
                let ext = match disk_req.format {
                    DiskFormat::Vhdx => "vhdx",
                    _ => "img",
                };
                let stem = sanitize_disk_stem(&cfg.name);
                let short_id = &cfg.id.to_string()[..8];
                disk_req.path = self
                    .store
                    .disks_dir()
                    .join(format!("{stem}-{short_id}-disk0.{ext}"));
            } else {
                disk::sync_disk_path_extension(&mut disk_req.path, disk_req.format);
            }
            let lun = cfg.next_free_lun();
            cfg.disks.push(DiskAttachment {
                lun,
                kind: DiskKind::HardDisk,
                path: disk_req.path.clone(),
                read_only: false,
                format: disk_req.format,
            });
            Some(disk_req)
        } else {
            None
        };

        if let Some(iso_path) = payload.iso_path {
            let lun = cfg.next_free_lun();
            cfg.disks.push(DiskAttachment {
                lun,
                kind: DiskKind::Dvd,
                path: iso_path,
                read_only: true,
                format: DiskFormat::Iso,
            });
        }

        for nic in &mut cfg.nics {
            if let NicBackendConfig::Consomme { port_forwards, .. } = &mut nic.backend {
                for pf in port_forwards {
                    pf.normalize();
                }
            }
        }

        cfg.validate().map_err(|e| err_bad_request(format!("{e:#}")))?;
        for d in &cfg.disks {
            ensure_disk_format_safe(&d.path, d.format, false)?;
        }
        {
            let guard = self.vms.read().unwrap();
            if guard
                .values()
                .any(|m| m.config.name.eq_ignore_ascii_case(&trimmed_name))
            {
                return Err(err_conflict(format!(
                    "a VM named '{trimmed_name}' already exists"
                )));
            }
            ensure_disks_not_conflicting(&guard, None, &cfg.disks)?;
        }

        if let Some(ref disk_req) = pending_disk_req {
            disk::validate_disk_path_for_format(&disk_req.path, disk_req.format)
                .map_err(|e| err_bad_request(format!("{e:#}")))?;
        }

        let created_disk_path = if let Some(disk_req) = pending_disk_req {
            let info = disk::create_disk_cancellable(
                &disk_req,
                paths.openvmm_img_bin.as_deref(),
                cancelled,
            )
            .map_err(|e| err_bad_request(format!("{e:#}")))?;
            Some(info.path)
        } else {
            None
        };

        let persist_res: anyhow::Result<VmSummary> = (|| {
            if cancelled.load(Ordering::Acquire) {
                return Err(err_bad_request("VM creation cancelled"));
            }
            let mut guard = self.vms.write().unwrap();
            if guard
                .values()
                .any(|m| m.config.name.eq_ignore_ascii_case(&trimmed_name))
            {
                return Err(err_conflict(format!(
                    "a VM named '{trimmed_name}' already exists"
                )));
            }
            ensure_disks_not_conflicting(&guard, None, &cfg.disks)?;
            self.store.save_vm(&cfg)?;

            let managed = ManagedVm {
                config: cfg,
                runtime: VmRuntimeState {
                    revision: self.next_revision(),
                    ..Default::default()
                },
                child: None,
                adopted_proc: None,
                monitor_cancel: None,
                monitor_handle: None,
                generation: 0,
                start_cancel: None,
                stop_wait: None,
                prepare_lock: Arc::new(tokio::sync::Mutex::new(())),
                power_op_lock: Arc::new(tokio::sync::Mutex::new(())),
                detach_on_drop: false,
            };
            let summary = managed.summary();
            guard.insert(summary.config.id, managed);
            Ok(summary)
        })();

        let summary = match persist_res {
            Ok(summary) => summary,
            Err(err) => {
                if let Some(path) = created_disk_path {
                    let _ = fs::remove_file(path);
                }
                let _ = self.store.delete_vm_dir(probe_id);
                return Err(err);
            }
        };

        self.emit(DaemonEvent::VmUpdated {
            vm: summary.clone(),
        });
        Ok(summary)
    }

    pub fn update_vm(&self, id_or_name: &str, payload: UpdateVmPayload) -> anyhow::Result<VmSummary> {
        let id = self.resolve_vm_id(id_or_name)?;
        let mut guard = self.vms.write().unwrap();

        let hardware_change = payload.vcpus.is_some()
            || payload.memory_mb.is_some()
            || payload.nested_virt.is_some()
            || payload.uefi_firmware.is_some()
            || payload.secure_boot_enabled.is_some()
            || payload.secure_boot_template.is_some()
            || payload.tpm_enabled.is_some()
            || payload.fastdesk.is_some()
            || payload.disks.is_some()
            || payload.nics.is_some();

        if let Some(new_name) = &payload.name {
            let trimmed = new_name.trim();
            openvmms_core::models::validate_vm_name(trimmed)
                .map_err(|e| err_bad_request(format!("{e:#}")))?;
            if guard
                .values()
                .any(|m| m.config.id != id && m.config.name.eq_ignore_ascii_case(trimmed))
            {
                return Err(err_conflict(format!(
                    "another VM is already named '{trimmed}'"
                )));
            }
        }

        let disks_updated = payload.disks.is_some();
        let vm = guard
            .get(&id)
            .ok_or_else(|| err_not_found("VM not found"))?;
        if hardware_change && !matches!(vm.runtime.status, VmStatus::Off | VmStatus::Error) {
            return Err(err_conflict(format!(
                "cannot edit hardware configuration while VM '{}' is {}",
                vm.config.name, vm.runtime.status
            )));
        }

        let mut updated = vm.config.clone();
        if let Some(name) = payload.name {
            updated.name = name.trim().to_string();
        }
        if let Some(desc) = payload.description {
            updated.description = desc;
        }
        if let Some(vcpus) = payload.vcpus {
            updated.vcpus = vcpus;
        }
        if let Some(mem) = payload.memory_mb {
            updated.memory_mb = mem;
        }
        if let Some(nested) = payload.nested_virt {
            updated.nested_virt = nested;
        }
        if let Some(fw) = payload.uefi_firmware {
            updated.uefi_firmware = fw.filter(|p| !p.as_os_str().is_empty());
        }
        if let Some(sb) = payload.secure_boot_enabled {
            updated.secure_boot_enabled = sb;
        }
        if let Some(tpl) = payload.secure_boot_template {
            updated.secure_boot_template = tpl;
        }
        if let Some(tpm) = payload.tpm_enabled {
            updated.tpm_enabled = tpm;
        }
        if let Some(fd) = payload.fastdesk {
            updated.fastdesk = fd;
        }
        if let Some(disks) = payload.disks {
            updated.disks = disks;
        }
        if let Some(nics) = payload.nics {
            updated.nics = nics;
        }
        for nic in &mut updated.nics {
            if let NicBackendConfig::Consomme { port_forwards, .. } = &mut nic.backend {
                for pf in port_forwards {
                    pf.normalize();
                }
            }
        }
        updated.updated_at = Utc::now();

        updated
            .validate()
            .map_err(|e| err_bad_request(format!("{e:#}")))?;
        if disks_updated {
            for d in &updated.disks {
                ensure_disk_format_safe(&d.path, d.format, true)?;
            }
            ensure_disks_not_conflicting(&guard, Some(id), &updated.disks)?;
        }
        self.store.save_vm(&updated)?;
        let vm = guard
            .get_mut(&id)
            .ok_or_else(|| err_not_found("VM not found"))?;
        vm.config = updated;
        vm.runtime.revision = self.next_revision();
        let summary = vm.summary();
        drop(guard);

        self.emit(DaemonEvent::VmUpdated {
            vm: summary.clone(),
        });
        Ok(summary)
    }

    pub fn delete_vm(&self, id_or_name: &str, delete_disks: bool) -> anyhow::Result<()> {
        let id = self.resolve_vm_id(id_or_name)?;
        let (disks_to_delete, revision) = {
            let mut guard = self.vms.write().unwrap();
            let vm = guard
                .get(&id)
                .ok_or_else(|| err_not_found("VM not found"))?;
            if !matches!(vm.runtime.status, VmStatus::Off | VmStatus::Error) {
                return Err(err_conflict(format!(
                    "cannot delete VM '{}' while status is {}; stop it first",
                    vm.config.name, vm.runtime.status
                )));
            }
            let disks = if delete_disks {
                let referenced_by_others: HashSet<PathBuf> = guard
                    .values()
                    .filter(|other| other.config.id != id)
                    .flat_map(|other| other.config.disks.iter())
                    .map(|d| openvmms_core::models::normalize_path_key(&d.path))
                    .collect();
                let mut seen = HashSet::new();
                let mut candidates = Vec::new();
                for d in &vm.config.disks {
                    if d.kind == DiskKind::HardDisk && !d.read_only {
                        let key = openvmms_core::models::normalize_path_key(&d.path);
                        if referenced_by_others.contains(&key) {
                            warn!(
                                vm_id = %id,
                                path = %d.path.display(),
                                "refusing to delete disk referenced by another VM"
                            );
                        } else if seen.insert(key) {
                            candidates.push(d.path.clone());
                        }
                    }
                }
                candidates
            } else {
                Vec::new()
            };

            // Remove persisted VM metadata first so a store failure does not
            // orphan an active in-memory VM or delete its disk files.
            self.store.delete_vm_dir(id)?;
            guard.remove(&id);
            (disks, self.next_revision())
        };

        for path in disks_to_delete {
            if path.is_file()
                && let Err(err) = fs::remove_file(&path)
            {
                warn!(path = %path.display(), error = %err, "failed to delete VM disk");
            }
        }
        self.emit(DaemonEvent::VmDeleted { id, revision });
        Ok(())
    }

    fn is_start_generation_active(&self, id: Uuid, start_gen: u64) -> bool {
        self.vms
            .read()
            .unwrap()
            .get(&id)
            .is_some_and(|vm| vm.generation == start_gen && vm.runtime.status == VmStatus::Starting)
    }

    pub async fn start_vm(self: &Arc<Self>, id_or_name: &str) -> anyhow::Result<VmSummary> {
        let id = self.resolve_vm_id(id_or_name)?;
        let (cfg, host_cfg, paths, start_gen, mut cancel_rx, prepare_lock) = {
            let mut guard = self.vms.write().unwrap();
            if self.is_shutting_down() {
                return Err(err_conflict("cannot start VM while openvmmsd is shutting down"));
            }
            if self.runtime_dir_changing.load(Ordering::Acquire) {
                return Err(err_conflict(
                    "cannot start VM while host runtime_dir is being updated",
                ));
            }
            let vm = guard
                .get_mut(&id)
                .ok_or_else(|| err_not_found("VM not found"))?;
            if !matches!(vm.runtime.status, VmStatus::Off | VmStatus::Error) {
                return Err(err_conflict(format!(
                    "VM '{}' is already {}",
                    vm.config.name, vm.runtime.status
                )));
            }
            vm.generation += 1;
            let start_gen = vm.generation;
            let (cancel_tx, cancel_rx) = watch::channel(false);
            vm.start_cancel = Some(cancel_tx);
            vm.detach_on_drop = false;
            vm.runtime.status = VmStatus::Starting;
            vm.runtime.last_error = None;
            vm.runtime.live_properties = None;
            vm.runtime.revision = self.next_revision();
            (
                vm.config.clone(),
                self.host_config(),
                self.resolved_paths(),
                start_gen,
                cancel_rx,
                Arc::clone(&vm.prepare_lock),
            )
        };

        self.emit_vm_snapshot(id);

        struct StartCleanupGuard {
            sup: Arc<Supervisor>,
            id: Uuid,
            start_gen: u64,
            armed: bool,
        }

        impl Drop for StartCleanupGuard {
            fn drop(&mut self) {
                if self.armed {
                    self.sup.cleanup_failed_start(
                        self.id,
                        self.start_gen,
                        "VM startup task aborted",
                    );
                }
            }
        }

        // Run startup in a detached task so an HTTP client disconnect (dropping the request future)
        // cannot orphan the VM in Starting state without cleanup.
        let sup = Arc::clone(self);
        let handle = tokio::spawn(async move {
            let mut guard = StartCleanupGuard {
                sup: Arc::clone(&sup),
                id,
                start_gen,
                armed: true,
            };
            let result = sup
                .start_vm_inner(
                    id,
                    start_gen,
                    &mut cancel_rx,
                    prepare_lock,
                    cfg,
                    host_cfg,
                    paths,
                )
                .await;
            match result {
                Ok(summary) => {
                    guard.armed = false;
                    Ok(summary)
                }
                Err(err) => {
                    guard.armed = false;
                    sup.cleanup_failed_start(id, start_gen, &format!("{err:#}"));
                    Err(err)
                }
            }
        });

        match handle.await {
            Ok(res) => res,
            Err(join_err) => {
                self.cleanup_failed_start(
                    id,
                    start_gen,
                    &format!("VM startup task failed: {join_err}"),
                );
                Err(anyhow::anyhow!("VM startup task failed: {join_err}"))
            }
        }
    }

    async fn start_vm_inner(
        self: &Arc<Self>,
        id: Uuid,
        start_gen: u64,
        cancel_rx: &mut watch::Receiver<bool>,
        prepare_lock: Arc<tokio::sync::Mutex<()>>,
        cfg: VmConfig,
        host_cfg: HostConfig,
        paths: ResolvedHostPaths,
    ) -> anyhow::Result<VmSummary> {
        let openvmm_bin = paths.openvmm_bin.ok_or_else(|| {
            err_bad_request(
                "openvmm binary not found; configure openvmm_bin in Host Settings or place openvmm next to openvmmsd",
            )
        })?;
        let uefi_path = cfg
            .uefi_firmware
            .clone()
            .or(paths.uefi_firmware_path)
            .ok_or_else(|| {
                err_bad_request(
                    "UEFI firmware (MSVM.fd) not found; configure uefi_firmware_path in Host Settings",
                )
            })?;
        if !uefi_path.is_file() {
            return Err(err_bad_request(format!(
                "UEFI firmware not found at {}",
                uefi_path.display()
            )));
        }

        for d in &cfg.disks {
            if !d.path.is_file() {
                return Err(err_bad_request(format!(
                    "attached disk/ISO not found: {}",
                    d.path.display()
                )));
            }
            ensure_disk_format_safe(&d.path, d.format, d.format != DiskFormat::Raw)?;
        }
        ensure_disks_not_conflicting(&self.vms.read().unwrap(), Some(id), &cfg.disks)?;
        openvmms_core::models::validate_runtime_dir(&host_cfg.runtime_dir)
            .map_err(|e| err_bad_request(format!("{e:#}")))?;

        if *cancel_rx.borrow()
            || self.is_shutting_down()
            || !self.is_start_generation_active(id, start_gen)
        {
            bail!("VM startup cancelled by stop request");
        }

        let runtime_dir = &host_cfg.runtime_dir;
        fs::create_dir_all(runtime_dir)
            .with_context(|| format!("creating runtime dir {}", runtime_dir.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = fs::set_permissions(runtime_dir, fs::Permissions::from_mode(0o700));
        }

        let prepare_guard = prepare_lock.lock().await;
        if *cancel_rx.borrow()
            || self.is_shutting_down()
            || !self.is_start_generation_active(id, start_gen)
        {
            bail!("VM startup cancelled by stop request");
        }

        let sock_path = runtime_dir.join(format!("{id}.sock"));
        let pid_path = sock_path.with_extension("pid");
        if sock_path.exists() {
            let sock_check = sock_path.clone();
            let pid_check = pid_path.clone();
            let expected_bin = openvmm_bin.clone();
            let reclaim_cancel = Arc::new(AtomicBool::new(false));
            let reclaim_cancel_worker = Arc::clone(&reclaim_cancel);
            let mut reclaim_task = tokio::task::spawn_blocking(move || {
                reclaim_or_verify_inactive_socket(
                    &sock_check,
                    &pid_check,
                    Some(&expected_bin),
                    &reclaim_cancel_worker,
                )
            });
            let mut shutdown_rx = self.subscribe_shutdown();
            let still_alive = tokio::select! {
                res = &mut reclaim_task => {
                    res.context("checking existing OpenVMM RPC socket")?
                }
                _ = cancel_rx.changed() => {
                    reclaim_cancel.store(true, Ordering::Release);
                    let _ = reclaim_task.await;
                    bail!("VM startup cancelled by stop request");
                }
                _ = wait_until_true(&mut shutdown_rx) => {
                    reclaim_cancel.store(true, Ordering::Release);
                    let _ = reclaim_task.await;
                    bail!("VM startup cancelled by daemon shutdown");
                }
            };
            if still_alive {
                bail!(
                    "refusing to start VM '{}': existing openvmm instance on {} is still active",
                    cfg.name,
                    sock_path.display()
                );
            }
        }

        let log_path = self.store.vm_log_path(id);
        let stderr_log_path = log_path.with_extension("stderr.log");
        let (shm_path, stderr_file) = {
            let mut guard = self.vms.write().unwrap();
            let vm = guard.get_mut(&id).context("VM vanished")?;
            if *cancel_rx.borrow()
                || self.is_shutting_down()
                || vm.generation != start_gen
                || vm.runtime.status != VmStatus::Starting
            {
                bail!("VM startup cancelled by stop request");
            }
            let _ = fs::remove_file(&sock_path);
            let _ = fs::remove_file(&pid_path);

            let shm_path = if cfg.fastdesk.enabled && cfg.fastdesk.use_shm {
                let path = runtime_dir.join(format!("vram-{id}.shm"));
                preallocate_shm_file(&path, cfg.fastdesk.vram_mb)?;
                Some(path)
            } else {
                None
            };

            if let Some(parent) = log_path.parent() {
                fs::create_dir_all(parent)?;
            }
            let stderr_file = fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&stderr_log_path)
                .with_context(|| format!("opening {}", stderr_log_path.display()))?;

            vm.runtime.rpc_socket_path = Some(sock_path.clone());
            vm.runtime.shm_path = shm_path.as_ref().map(|p| p.to_string_lossy().into_owned());
            (shm_path, stderr_file)
        };

        let rpc_arg = format!("path={},transport=ttrpc", sock_path.display());
        let mut cmd = Command::new(&openvmm_bin);
        cmd.arg("--rpc")
            .arg(rpc_arg)
            .arg("--log-file")
            .arg(&log_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(stderr_file));

        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            cmd.process_group(0);
        }

        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            const CREATE_NO_WINDOW: u32 = 0x0800_0000;
            const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
            cmd.creation_flags(CREATE_NO_WINDOW | CREATE_NEW_PROCESS_GROUP);
        }

        let mut child = cmd
            .spawn()
            .with_context(|| format!("spawning {}", openvmm_bin.display()))?;
        let pid = child.id();

        {
            let mut guard = self.vms.write().unwrap();
            let vm = guard.get_mut(&id).context("VM vanished")?;
            if *cancel_rx.borrow()
                || self.is_shutting_down()
                || vm.generation != start_gen
                || vm.runtime.status != VmStatus::Starting
            {
                let _ = child.kill();
                let _ = child.wait();
                bail!("VM startup cancelled by stop request");
            }
            vm.child = Some(child);
            vm.adopted_proc = None;
            vm.runtime.pid = Some(pid);
            vm.runtime.rpc_socket_path = Some(sock_path.clone());
            vm.runtime.shm_path = shm_path.as_ref().map(|p| p.to_string_lossy().into_owned());
            if let Err(err) = openvmms_core::process::write_pid_file(&pid_path, pid) {
                warn!(vm_id = %id, error = %err, "failed to write VM pid file");
            }
        }
        drop(prepare_guard);

        let client = TtrpcClient::new(&sock_path);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            if *cancel_rx.borrow() || !self.is_start_generation_active(id, start_gen) {
                bail!("VM startup cancelled by stop request");
            }
            {
                let mut guard = self.vms.write().unwrap();
                if let Some(vm) = guard.get_mut(&id)
                    && vm.generation == start_gen
                    && let Some(child) = vm.child.as_mut()
                    && let Some(status) = child.try_wait()?
                {
                    let err_tail = read_log_tail(&stderr_log_path, 20)
                        .or_else(|_| read_log_tail(&log_path, 20))
                        .unwrap_or_default();
                    bail!(
                        "openvmm exited prematurely with {status}{}",
                        if err_tail.trim().is_empty() {
                            String::new()
                        } else {
                            format!(":\n{}", err_tail.trim())
                        }
                    );
                }
            }
            if sock_path.exists() && run_cancellable_rpc(cancel_rx, client.ping()).await.is_ok() {
                break;
            }
            if *cancel_rx.borrow() {
                bail!("VM startup cancelled by stop request");
            }
            if tokio::time::Instant::now() >= deadline {
                bail!(
                    "timed out waiting for OpenVMM RPC socket at {}",
                    sock_path.display()
                );
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(50)) => {}
                _ = cancel_rx.changed() => {
                    bail!("VM startup cancelled by stop request");
                }
            }
        }

        let create_req = build_create_vm_request(&cfg, &uefi_path, shm_path.as_deref())?;
        if let Err(err) = run_cancellable_rpc(cancel_rx, client.create_vm(create_req)).await {
            let msg = format!("{err:#}");
            if cfg.tpm_enabled
                && (msg.contains("TpmDeviceHandle")
                    || msg.contains("no resolver")
                    || msg.to_ascii_lowercase().contains("tpm"))
            {
                return Err(err).context(
                    "OpenVMM CreateVM failed (vTPM 2.0 requires an openvmm binary built with --features tpm, or disable vTPM in VM settings)",
                );
            }
            return Err(err).context("OpenVMM CreateVM failed");
        }
        run_cancellable_rpc(cancel_rx, client.resume_vm())
            .await
            .context("OpenVMM ResumeVM failed")?;

        let props = run_cancellable_rpc(
            cancel_rx,
            client.properties_vm(vmservice::PropertiesVmRequest { types: vec![] }),
        )
        .await
        .context("querying VM properties")?;

        let fastdesk_endpoint = props
            .fastdesk_listen_addr
            .as_deref()
            .map(normalize_fastdesk_endpoint);

        let summary = {
            let mut guard = self.vms.write().unwrap();
            let vm = guard.get_mut(&id).context("VM vanished")?;
            if vm.generation != start_gen || vm.runtime.status != VmStatus::Starting {
                bail!("VM startup cancelled by stop request");
            }
            vm.start_cancel = None;
            vm.runtime.status = VmStatus::Running;
            vm.runtime.started_at = Some(Utc::now());
            vm.runtime.fastdesk_endpoint = fastdesk_endpoint;
            if let Some(shm) = props.fastdesk_shm_path {
                vm.runtime.shm_path = Some(shm);
            }
            vm.runtime.revision = self.next_revision();

            let (mon_cancel_tx, mon_cancel_rx) = watch::channel(false);
            let sup = Arc::clone(self);
            let sock_clone = sock_path.clone();
            let monitor = tokio::spawn(async move {
                sup.monitor_vm(id, start_gen, sock_clone, mon_cancel_rx).await;
            });
            vm.monitor_cancel = Some(mon_cancel_tx);
            vm.monitor_handle = Some(monitor);
            vm.summary()
        };

        self.emit(DaemonEvent::VmUpdated {
            vm: summary.clone(),
        });
        info!(vm_id = %id, name = %summary.config.name, pid, "VM started");
        Ok(summary)
    }

    fn cleanup_failed_start(&self, id: Uuid, start_gen: u64, err_msg: &str) {
        let mut guard = self.vms.write().unwrap();
        if let Some(vm) = guard.get_mut(&id) {
            if vm.generation != start_gen {
                return;
            }
            vm.start_cancel = None;
            if let Some(tx) = vm.monitor_cancel.take() {
                let _ = tx.send(true);
            }
            if let Some(handle) = vm.monitor_handle.take() {
                handle.abort();
            }
            if let Some(mut child) = vm.child.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
            cleanup_runtime_files(vm.runtime.rpc_socket_path.take(), vm.runtime.shm_path.take());
            vm.adopted_proc = None;
            vm.runtime.pid = None;
            vm.runtime.started_at = None;
            vm.runtime.fastdesk_endpoint = None;
            vm.runtime.status = VmStatus::Error;
            vm.runtime.last_error = Some(err_msg.to_string());
            vm.runtime.revision = self.next_revision();
            let summary = vm.summary();
            drop(guard);
            self.emit(DaemonEvent::VmUpdated { vm: summary });
        }
    }

    fn is_monitor_generation_active(&self, id: Uuid, run_gen: u64) -> bool {
        if *self.shutdown_watch.borrow() && !self.should_stop_vms_on_shutdown() {
            return false;
        }
        self.vms.read().unwrap().get(&id).is_some_and(|vm| {
            vm.generation == run_gen
                && !matches!(
                    vm.runtime.status,
                    VmStatus::Stopping | VmStatus::Off | VmStatus::Error
                )
        })
    }

    async fn is_vm_process_alive(&self, id: Uuid, run_gen: u64) -> bool {
        let adopted_proc = {
            let mut guard = self.vms.write().unwrap();
            let Some(vm) = guard.get_mut(&id) else {
                return false;
            };
            if vm.generation != run_gen {
                return false;
            }
            if let Some(child) = vm.child.as_mut() {
                return matches!(child.try_wait(), Ok(None));
            }
            vm.adopted_proc.clone()
        };
        if let Some(proc_handle) = adopted_proc {
            return tokio::task::spawn_blocking(move || proc_handle.is_alive())
                .await
                .unwrap_or(false);
        }
        false
    }

    fn sync_status_from_properties(
        &self,
        id: Uuid,
        run_gen: u64,
        props: &vmservice::PropertiesVmResponse,
    ) {
        let next_status = if props.state == vmservice::VmState::Paused as i32 {
            VmStatus::Paused
        } else if props.state == vmservice::VmState::Running as i32 {
            VmStatus::Running
        } else {
            return;
        };
        let next_endpoint = props
            .fastdesk_listen_addr
            .as_deref()
            .map(normalize_fastdesk_endpoint);

        let updated_summary = {
            let mut guard = self.vms.write().unwrap();
            let Some(vm) = guard.get_mut(&id) else {
                return;
            };
            if vm.generation != run_gen
                || matches!(vm.runtime.status, VmStatus::Stopping | VmStatus::Off | VmStatus::Error)
            {
                return;
            }
            let mut changed = false;
            if vm.runtime.status != next_status {
                vm.runtime.status = next_status;
                changed = true;
            }
            if vm.runtime.fastdesk_endpoint.is_none() && next_endpoint.is_some() {
                vm.runtime.fastdesk_endpoint = next_endpoint;
                changed = true;
            }
            if vm.runtime.shm_path.is_none()
                && let Some(shm) = &props.fastdesk_shm_path
            {
                vm.runtime.shm_path = Some(shm.clone());
                changed = true;
            }
            if changed {
                vm.runtime.revision = self.next_revision();
            }
            changed.then(|| vm.summary())
        };
        if let Some(summary) = updated_summary {
            self.emit(DaemonEvent::VmUpdated { vm: summary });
        }
    }

    async fn monitor_vm(
        self: Arc<Self>,
        id: Uuid,
        run_gen: u64,
        sock_path: PathBuf,
        mut cancel_rx: watch::Receiver<bool>,
    ) {
        let client = TtrpcClient::new(&sock_path);
        let (wait_res, mut props) = loop {
            if *cancel_rx.borrow() || !self.is_monitor_generation_active(id, run_gen) {
                return;
            }
            let needs_endpoint = self
                .vms
                .read()
                .unwrap()
                .get(&id)
                .is_some_and(|vm| vm.generation == run_gen && vm.runtime.fastdesk_endpoint.is_none());
            if needs_endpoint
                && let Ok(Ok(props)) = tokio::time::timeout(
                    Duration::from_secs(1),
                    client.properties_vm(vmservice::PropertiesVmRequest { types: vec![] }),
                )
                .await
            {
                if props.state == vmservice::VmState::Halted as i32 {
                    break (Ok(()), Some(props));
                }
                self.sync_status_from_properties(id, run_gen, &props);
            }

            let res = client.wait_vm_cancellable(cancel_rx.clone()).await;
            if *cancel_rx.borrow() || !self.is_monitor_generation_active(id, run_gen) {
                return;
            }
            match res {
                Ok(WaitVmOutcome::Cancelled) => return,
                Ok(WaitVmOutcome::Exited) => {
                    let props = tokio::time::timeout(
                        Duration::from_secs(2),
                        client.properties_vm(vmservice::PropertiesVmRequest { types: vec![] }),
                    )
                    .await
                    .ok()
                    .and_then(Result::ok);
                    break (Ok(()), props);
                }
                Err(err) => {
                    // Distinguish transient RPC/EOF glitches on a live VM from actual VM exit.
                    let mut confirmed_alive = false;
                    let mut last_props = None;
                    for attempt in 0..3 {
                        if *cancel_rx.borrow() || !self.is_monitor_generation_active(id, run_gen) {
                            return;
                        }
                        if let Ok(Ok(p)) = tokio::time::timeout(
                            Duration::from_secs(2),
                            client.properties_vm(vmservice::PropertiesVmRequest { types: vec![] }),
                        )
                        .await
                        {
                            let is_live_state = p.state == vmservice::VmState::Running as i32
                                || p.state == vmservice::VmState::Paused as i32;
                            if is_live_state {
                                self.sync_status_from_properties(id, run_gen, &p);
                                confirmed_alive = true;
                                break;
                            }
                            last_props = Some(p);
                            break;
                        }
                        let proc_alive = self.is_vm_process_alive(id, run_gen).await;
                        let sock_client = client.clone();
                        let sock_alive = tokio::task::spawn_blocking(move || {
                            sock_client.is_socket_accepting()
                        })
                        .await
                        .unwrap_or(false);
                        if !proc_alive && !sock_alive {
                            break;
                        }
                        if attempt == 2 && proc_alive && sock_alive {
                            confirmed_alive = true;
                            break;
                        }
                        if sleep_or_cancel(Duration::from_millis(250), &mut cancel_rx).await {
                            return;
                        }
                    }
                    if confirmed_alive {
                        warn!(
                            vm_id = %id,
                            error = %err,
                            "WaitVM returned error while VM is still alive; re-attaching monitor"
                        );
                        if sleep_or_cancel(Duration::from_millis(250), &mut cancel_rx).await {
                            return;
                        }
                        continue;
                    }
                    break (Err(err), last_props);
                }
            }
        };

        let stop_done_tx = {
            let mut guard = self.vms.write().unwrap();
            if *self.shutdown_watch.borrow() && !self.should_stop_vms_on_shutdown() {
                return;
            }
            let Some(vm) = guard.get_mut(&id) else {
                return;
            };
            if vm.generation != run_gen || vm.runtime.status == VmStatus::Stopping {
                return;
            }
            let (stop_done_tx, stop_done_rx) = watch::channel(false);
            vm.runtime.status = VmStatus::Stopping;
            vm.monitor_cancel = None;
            vm.monitor_handle = None;
            vm.stop_wait = Some(stop_done_rx);
            stop_done_tx
        };

        if props.is_none() {
            props = tokio::time::timeout(
                Duration::from_secs(2),
                client.properties_vm(vmservice::PropertiesVmRequest { types: vec![] }),
            )
            .await
            .ok()
            .and_then(Result::ok);
        }

        let _ = tokio::time::timeout(Duration::from_secs(3), client.teardown_vm()).await;
        let _ = tokio::time::timeout(Duration::from_secs(2), client.quit()).await;

        let mut child_opt = {
            let mut guard = self.vms.write().unwrap();
            guard
                .get_mut(&id)
                .filter(|vm| vm.generation == run_gen)
                .and_then(|vm| vm.child.take())
        };

        let exit_status = if let Some(mut child) = child_opt.take() {
            tokio::task::spawn_blocking(move || {
                let start = std::time::Instant::now();
                loop {
                    match child.try_wait() {
                        Ok(Some(st)) => return Some(st),
                        Ok(None) if start.elapsed() < Duration::from_secs(3) => {
                            std::thread::sleep(Duration::from_millis(50));
                        }
                        _ => {
                            let _ = child.kill();
                            return child.wait().ok();
                        }
                    }
                }
            })
            .await
            .ok()
            .flatten()
        } else {
            None
        };

        let summary = {
            let mut guard = self.vms.write().unwrap();
            let Some(vm) = guard.get_mut(&id) else {
                let _ = stop_done_tx.send(true);
                return;
            };
            if vm.generation != run_gen {
                let _ = stop_done_tx.send(true);
                return;
            }
            cleanup_runtime_files(vm.runtime.rpc_socket_path.take(), vm.runtime.shm_path.take());
            vm.adopted_proc = None;
            vm.runtime.pid = None;
            vm.runtime.started_at = None;
            vm.runtime.fastdesk_endpoint = None;
            vm.runtime.live_properties = None;
            vm.monitor_cancel = None;
            vm.monitor_handle = None;
            vm.stop_wait = None;

            let halt_reason = props.as_ref().and_then(|p| p.halt_reason.clone());
            let is_halted = props
                .as_ref()
                .is_some_and(|p| p.state == vmservice::VmState::Halted as i32);
            match wait_res {
                Ok(()) => {
                    vm.runtime.status = VmStatus::Off;
                    vm.runtime.last_error = halt_reason.filter(|r| r != "PowerOff");
                }
                Err(err) => {
                    if exit_status.is_some_and(|st| st.success()) || is_halted {
                        vm.runtime.status = VmStatus::Off;
                        vm.runtime.last_error = None;
                    } else {
                        vm.runtime.status = VmStatus::Error;
                        vm.runtime.last_error = Some(format!("VM exited: {err:#}"));
                    }
                }
            }
            vm.runtime.revision = self.next_revision();
            vm.summary()
        };

        let _ = stop_done_tx.send(true);
        self.emit(DaemonEvent::VmUpdated { vm: summary });
    }

    pub async fn stop_vm(self: &Arc<Self>, id_or_name: &str) -> anyhow::Result<VmSummary> {
        let id = self.resolve_vm_id(id_or_name)?;
        let (wait_rx, stop_task_args) = {
            let mut guard = self.vms.write().unwrap();
            let vm = guard
                .get_mut(&id)
                .ok_or_else(|| err_not_found("VM not found"))?;
            if matches!(vm.runtime.status, VmStatus::Off | VmStatus::Error) {
                vm.runtime.status = VmStatus::Off;
                vm.runtime.last_error = None;
                return Ok(vm.summary());
            }
            if vm.runtime.status == VmStatus::Stopping {
                if let Some(rx) = vm.stop_wait.clone() {
                    (Some(rx), None)
                } else {
                    return Err(err_conflict(format!(
                        "cannot stop VM '{}' while it is already Stopping",
                        vm.config.name
                    )));
                }
            } else {
                vm.generation += 1;
                let stop_gen = vm.generation;
                if let Some(cancel_tx) = vm.start_cancel.take() {
                    let _ = cancel_tx.send(true);
                }
                let (stop_done_tx, stop_done_rx) = watch::channel(false);
                vm.stop_wait = Some(stop_done_rx.clone());
                vm.runtime.status = VmStatus::Stopping;
                vm.runtime.revision = self.next_revision();
                let adopted_proc = if vm.child.is_none() {
                    vm.adopted_proc.take()
                } else {
                    vm.adopted_proc = None;
                    None
                };
                (
                    Some(stop_done_rx),
                    Some((
                        stop_gen,
                        vm.runtime.rpc_socket_path.take(),
                        vm.runtime.shm_path.take(),
                        vm.child.take(),
                        adopted_proc,
                        vm.monitor_cancel.take(),
                        vm.monitor_handle.take(),
                        stop_done_tx,
                    )),
                )
            }
        };

        if let Some((
            stop_gen,
            sock_opt,
            shm_opt,
            child_opt,
            adopted_proc,
            mon_cancel_opt,
            mon_handle_opt,
            stop_done_tx,
        )) = stop_task_args
        {
            if let Some(tx) = mon_cancel_opt {
                let _ = tx.send(true);
            }
            self.emit_vm_snapshot(id);
            let sup = Arc::clone(self);
            tokio::spawn(async move {
                if let Some(mut mon_handle) = mon_handle_opt
                    && tokio::time::timeout(Duration::from_secs(2), &mut mon_handle)
                        .await
                        .is_err()
                {
                    mon_handle.abort();
                    let _ = mon_handle.await;
                }
                if let Some(sock) = &sock_opt {
                    let client = TtrpcClient::new(sock);
                    let _ = tokio::time::timeout(Duration::from_secs(4), client.teardown_vm()).await;
                    let _ = tokio::time::timeout(Duration::from_secs(2), client.quit()).await;
                }

                if let Some(mut child) = child_opt {
                    let _ = tokio::task::spawn_blocking(move || {
                        let start = std::time::Instant::now();
                        loop {
                            match child.try_wait() {
                                Ok(Some(_)) => break,
                                Ok(None) if start.elapsed() < Duration::from_secs(3) => {
                                    std::thread::sleep(Duration::from_millis(50));
                                }
                                _ => {
                                    let _ = child.kill();
                                    let _ = child.wait();
                                    break;
                                }
                            }
                        }
                    })
                    .await;
                } else if let Some(proc_handle) = adopted_proc {
                    let sock_wait = sock_opt.clone();
                    let _ = tokio::task::spawn_blocking(move || {
                        wait_or_terminate_adopted_proc(sock_wait.as_deref(), &proc_handle);
                    })
                    .await;
                }

                cleanup_runtime_files(sock_opt, shm_opt);
                let summary = {
                    let mut guard = sup.vms.write().unwrap();
                    if let Some(vm) = guard.get_mut(&id) {
                        if vm.generation == stop_gen {
                            vm.runtime.status = VmStatus::Off;
                            vm.adopted_proc = None;
                            vm.runtime.pid = None;
                            vm.runtime.started_at = None;
                            vm.runtime.fastdesk_endpoint = None;
                            vm.runtime.live_properties = None;
                            vm.runtime.last_error = None;
                            vm.stop_wait = None;
                            vm.runtime.revision = sup.next_revision();
                            Some(vm.summary())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                };
                let _ = stop_done_tx.send(true);
                if let Some(summary) = summary {
                    sup.emit(DaemonEvent::VmUpdated { vm: summary });
                }
            });
        }

        if let Some(mut rx) = wait_rx {
            while !*rx.borrow_and_update() {
                if rx.changed().await.is_err() {
                    break;
                }
            }
        }

        let mut emit_normalized = None;
        let summary = {
            let mut guard = self.vms.write().unwrap();
            let vm = guard.get_mut(&id).context("VM not found")?;
            if matches!(vm.runtime.status, VmStatus::Off | VmStatus::Error) {
                let changed = vm.runtime.status == VmStatus::Error || vm.runtime.last_error.is_some();
                vm.runtime.status = VmStatus::Off;
                vm.runtime.last_error = None;
                if changed {
                    vm.runtime.revision = self.next_revision();
                }
                let s = vm.summary();
                if changed {
                    emit_normalized = Some(s.clone());
                }
                Ok(s)
            } else {
                Ok(vm.summary())
            }
        };
        if let Some(s) = emit_normalized {
            self.emit(DaemonEvent::VmUpdated { vm: s });
        }
        summary
    }

    pub async fn shutdown_vm(
        &self,
        id_or_name: &str,
        req: ShutdownVmPayload,
    ) -> anyhow::Result<VmSummary> {
        let id = self.resolve_vm_id(id_or_name)?;
        let sock = self.require_running_socket(id)?;
        let client = TtrpcClient::new(sock);

        let mechanism = if req.acpi {
            vmservice::shutdown_vm_request::Mechanism::Acpi as i32
        } else {
            vmservice::shutdown_vm_request::Mechanism::HypervIc as i32
        };

        let first = client
            .shutdown_vm(vmservice::ShutdownVmRequest {
                mechanism,
                force: req.force,
            })
            .await;

        if let Err(err) = first {
            if !req.acpi {
                // Fall back to ACPI power button when guest shutdown IC is not ready.
                client
                    .shutdown_vm(vmservice::ShutdownVmRequest {
                        mechanism: vmservice::shutdown_vm_request::Mechanism::Acpi as i32,
                        force: false,
                    })
                    .await
                    .with_context(|| {
                        format!("Hyper-V Shutdown IC failed ({err:#}) and ACPI fallback failed")
                    })?;
            } else {
                return Err(err);
            }
        }

        self.get_vm(&id.to_string())
    }

    fn power_op_lock(&self, id: Uuid) -> anyhow::Result<Arc<tokio::sync::Mutex<()>>> {
        let guard = self.vms.read().unwrap();
        let vm = guard
            .get(&id)
            .ok_or_else(|| err_not_found("VM not found"))?;
        Ok(Arc::clone(&vm.power_op_lock))
    }

    pub async fn pause_vm(self: &Arc<Self>, id_or_name: &str) -> anyhow::Result<VmSummary> {
        let sup = Arc::clone(self);
        let id_or_name = id_or_name.to_string();
        tokio::spawn(async move {
            let id = sup.resolve_vm_id(&id_or_name)?;
            let power_lock = sup.power_op_lock(id)?;
            let _power_guard = power_lock.lock().await;
            let (sock, rpc_gen) = sup.require_socket_with_generation(id, &[VmStatus::Running])?;
            TtrpcClient::new(sock).pause_vm().await?;
            sup.commit_power_transition(id, rpc_gen, "pause", VmStatus::Paused)
        })
        .await
        .map_err(|e| anyhow::anyhow!("VM pause task failed: {e}"))?
    }

    pub async fn resume_vm(self: &Arc<Self>, id_or_name: &str) -> anyhow::Result<VmSummary> {
        let sup = Arc::clone(self);
        let id_or_name = id_or_name.to_string();
        tokio::spawn(async move {
            let id = sup.resolve_vm_id(&id_or_name)?;
            let power_lock = sup.power_op_lock(id)?;
            let _power_guard = power_lock.lock().await;
            let (sock, rpc_gen) =
                sup.require_socket_with_generation(id, &[VmStatus::Running, VmStatus::Paused])?;
            TtrpcClient::new(sock).resume_vm().await?;
            sup.commit_power_transition(id, rpc_gen, "resume", VmStatus::Running)
        })
        .await
        .map_err(|e| anyhow::anyhow!("VM resume task failed: {e}"))?
    }

    /// Applies the result of a pause/resume RPC only if the VM lifecycle did not move on
    /// while the RPC was in flight. A stop or restart bumps the generation and sets
    /// `Stopping`/`Starting`; overwriting that state would let a second stop or a premature
    /// restart race with the in-progress teardown.
    pub(crate) fn commit_power_transition(
        &self,
        id: Uuid,
        rpc_gen: u64,
        action: &str,
        new_status: VmStatus,
    ) -> anyhow::Result<VmSummary> {
        let summary = {
            let mut guard = self.vms.write().unwrap();
            let vm = guard
                .get_mut(&id)
                .ok_or_else(|| err_not_found("VM not found"))?;
            if vm.generation != rpc_gen
                || !matches!(vm.runtime.status, VmStatus::Running | VmStatus::Paused)
            {
                return Err(err_conflict(format!(
                    "cannot complete {action} because VM '{}' changed state to {}",
                    vm.config.name, vm.runtime.status
                )));
            }
            vm.runtime.status = new_status;
            vm.runtime.revision = self.next_revision();
            vm.summary()
        };
        self.emit(DaemonEvent::VmUpdated {
            vm: summary.clone(),
        });
        Ok(summary)
    }

    pub async fn reset_vm(self: &Arc<Self>, id_or_name: &str) -> anyhow::Result<VmSummary> {
        let sup = Arc::clone(self);
        let id_or_name = id_or_name.to_string();
        tokio::spawn(async move {
            let id = sup.resolve_vm_id(&id_or_name)?;
            let power_lock = sup.power_op_lock(id)?;
            let _power_guard = power_lock.lock().await;
            let sock = sup.require_active_socket(id)?;
            TtrpcClient::new(sock).reset_vm().await?;
            sup.get_vm(&id.to_string())
        })
        .await
        .map_err(|e| anyhow::anyhow!("VM reset task failed: {e}"))?
    }

    pub async fn attach_disk(
        self: &Arc<Self>,
        id_or_name: &str,
        req: AttachDiskPayload,
    ) -> anyhow::Result<VmSummary> {
        let sup = Arc::clone(self);
        let id_or_name = id_or_name.to_string();
        tokio::spawn(async move { sup.attach_disk_inner(&id_or_name, req).await })
            .await
            .map_err(|e| anyhow::anyhow!("attach_disk task failed: {e}"))?
    }

    async fn attach_disk_inner(
        &self,
        id_or_name: &str,
        req: AttachDiskPayload,
    ) -> anyhow::Result<VmSummary> {
        if !req.path.is_file() {
            return Err(err_bad_request(format!(
                "disk file not found: {}",
                req.path.display()
            )));
        }
        let id = self.resolve_vm_id(id_or_name)?;
        let explicit_format = req.format.is_some();
        let format = req
            .format
            .unwrap_or_else(|| DiskFormat::from_path(&req.path));
        ensure_disk_format_safe(&req.path, format, explicit_format)?;
        let kind = if format == DiskFormat::Iso {
            DiskKind::Dvd
        } else {
            DiskKind::HardDisk
        };
        if let Some(lun) = req.lun
            && lun > 255
        {
            return Err(err_bad_request(format!(
                "SCSI LUN must be 0..=255 (got {lun})"
            )));
        }

        let pending = {
            let mut guard = self.vms.write().unwrap();
            let vm = guard
                .get(&id)
                .ok_or_else(|| err_not_found("VM not found"))?;
            let active_sock = vm.active_rpc_socket("attach disk")?;
            let lun = req.lun.unwrap_or_else(|| vm.config.next_free_lun());
            if lun > 255 {
                return Err(err_bad_request(format!(
                    "SCSI LUN must be 0..=255 (got {lun})"
                )));
            }
            if vm.config.disks.iter().any(|d| d.lun == lun) {
                return Err(err_conflict(format!("SCSI LUN {lun} is already occupied")));
            }
            let attachment = DiskAttachment {
                lun,
                kind,
                path: req.path,
                read_only: req.read_only || kind == DiskKind::Dvd,
                format,
            };
            let mut candidate_disks = vm.config.disks.clone();
            candidate_disks.push(attachment.clone());
            ensure_disks_not_conflicting(&guard, Some(id), &candidate_disks)?;

            match active_sock {
                Some(sock) => Ok((attachment, sock, vm.generation)),
                None => {
                    let vm = guard
                        .get_mut(&id)
                        .ok_or_else(|| err_not_found("VM not found"))?;
                    let mut updated = vm.config.clone();
                    updated.disks.push(attachment);
                    updated.disks.sort_by_key(|d| d.lun);
                    updated.updated_at = Utc::now();
                    self.store.save_vm(&updated)?;
                    vm.config = updated;
                    vm.runtime.revision = self.next_revision();
                    Err(vm.summary())
                }
            }
        };

        let summary = match pending {
            Err(summary) => summary,
            Ok((attachment, sock, rpc_gen)) => {
                TtrpcClient::new(sock)
                    .modify_resource(vmservice::ModifyResourceRequest {
                        r#type: vmservice::ModifyType::Add as i32,
                        resource: Some(vmservice::modify_resource_request::Resource::ScsiDisk(
                            to_proto_scsi_disk(&attachment),
                        )),
                    })
                    .await
                    .context("hot-adding SCSI disk to running VM")?;

                let mut guard = self.vms.write().unwrap();
                let vm = guard
                    .get(&id)
                    .ok_or_else(|| err_not_found("VM not found"))?;
                if vm.generation != rpc_gen
                    || !matches!(vm.runtime.status, VmStatus::Running | VmStatus::Paused)
                {
                    return Err(err_conflict(format!(
                        "cannot complete disk attach because VM '{}' changed state to {}",
                        vm.config.name, vm.runtime.status
                    )));
                }
                if vm.config.disks.iter().any(|d| d.lun == attachment.lun) {
                    return Err(err_conflict(format!(
                        "SCSI LUN {} is already occupied",
                        attachment.lun
                    )));
                }
                let mut candidate_disks = vm.config.disks.clone();
                candidate_disks.push(attachment.clone());
                ensure_disks_not_conflicting(&guard, Some(id), &candidate_disks)?;
                let mut updated = vm.config.clone();
                updated.disks.push(attachment);
                updated.disks.sort_by_key(|d| d.lun);
                updated.updated_at = Utc::now();
                self.store.save_vm(&updated)?;
                let vm = guard
                    .get_mut(&id)
                    .ok_or_else(|| err_not_found("VM not found"))?;
                vm.config = updated;
                vm.runtime.revision = self.next_revision();
                vm.summary()
            }
        };

        self.emit(DaemonEvent::VmUpdated {
            vm: summary.clone(),
        });
        Ok(summary)
    }

    pub async fn detach_disk(
        self: &Arc<Self>,
        id_or_name: &str,
        lun: u32,
    ) -> anyhow::Result<VmSummary> {
        let sup = Arc::clone(self);
        let id_or_name = id_or_name.to_string();
        tokio::spawn(async move { sup.detach_disk_inner(&id_or_name, lun).await })
            .await
            .map_err(|e| anyhow::anyhow!("detach_disk task failed: {e}"))?
    }

    async fn detach_disk_inner(&self, id_or_name: &str, lun: u32) -> anyhow::Result<VmSummary> {
        let id = self.resolve_vm_id(id_or_name)?;
        let pending = {
            let mut guard = self.vms.write().unwrap();
            let vm = guard
                .get_mut(&id)
                .ok_or_else(|| err_not_found("VM not found"))?;
            let active_sock = vm.active_rpc_socket("detach disk")?;
            let disk = vm
                .config
                .disks
                .iter()
                .find(|d| d.lun == lun)
                .cloned()
                .ok_or_else(|| err_not_found(format!("no disk attached at SCSI LUN {lun}")))?;
            match active_sock {
                Some(sock) => Ok((disk, sock, vm.generation)),
                None => {
                    let mut updated = vm.config.clone();
                    updated.disks.retain(|d| d.lun != lun);
                    updated.updated_at = Utc::now();
                    self.store.save_vm(&updated)?;
                    vm.config = updated;
                    vm.runtime.revision = self.next_revision();
                    Err(vm.summary())
                }
            }
        };

        let summary = match pending {
            Err(summary) => summary,
            Ok((existing, sock, rpc_gen)) => {
                TtrpcClient::new(sock)
                    .modify_resource(vmservice::ModifyResourceRequest {
                        r#type: vmservice::ModifyType::Remove as i32,
                        resource: Some(vmservice::modify_resource_request::Resource::ScsiDisk(
                            vmservice::ScsiDisk {
                                controller: 0,
                                lun: existing.lun,
                                ..Default::default()
                            },
                        )),
                    })
                    .await
                    .context("hot-removing SCSI disk from running VM")?;

                let mut guard = self.vms.write().unwrap();
                let vm = guard
                    .get_mut(&id)
                    .ok_or_else(|| err_not_found("VM not found"))?;
                if vm.generation != rpc_gen
                    || !matches!(vm.runtime.status, VmStatus::Running | VmStatus::Paused)
                {
                    return Err(err_conflict(format!(
                        "cannot complete disk detach because VM '{}' changed state to {}",
                        vm.config.name, vm.runtime.status
                    )));
                }
                let mut updated = vm.config.clone();
                updated.disks.retain(|d| d.lun != lun);
                updated.updated_at = Utc::now();
                self.store.save_vm(&updated)?;
                vm.config = updated;
                vm.runtime.revision = self.next_revision();
                vm.summary()
            }
        };

        self.emit(DaemonEvent::VmUpdated {
            vm: summary.clone(),
        });
        Ok(summary)
    }

    pub async fn mount_iso(
        self: &Arc<Self>,
        id_or_name: &str,
        req: MountIsoPayload,
    ) -> anyhow::Result<VmSummary> {
        let sup = Arc::clone(self);
        let id_or_name = id_or_name.to_string();
        tokio::spawn(async move { sup.mount_iso_inner(&id_or_name, req).await })
            .await
            .map_err(|e| anyhow::anyhow!("mount_iso task failed: {e}"))?
    }

    async fn mount_iso_inner(
        &self,
        id_or_name: &str,
        req: MountIsoPayload,
    ) -> anyhow::Result<VmSummary> {
        if !req.path.is_file() {
            return Err(err_bad_request(format!(
                "ISO image not found: {}",
                req.path.display()
            )));
        }
        if let Err(err) = fs::File::open(&req.path) {
            return Err(err_bad_request(format!(
                "cannot open ISO image '{}': {err}",
                req.path.display()
            )));
        }
        ensure_disk_format_safe(&req.path, DiskFormat::Iso, true)?;
        if let Some(lun) = req.lun
            && lun > 255
        {
            return Err(err_bad_request(format!(
                "SCSI LUN must be 0..=255 (got {lun})"
            )));
        }

        let id = self.resolve_vm_id(id_or_name)?;
        let active_plan = {
            let mut guard = self.vms.write().unwrap();
            let vm = guard
                .get(&id)
                .ok_or_else(|| err_not_found("VM not found"))?;
            let active_sock = vm.active_rpc_socket("mount ISO")?;
            let (target_lun, previous_dvd) = match req.lun {
                Some(l) => match vm.config.disks.iter().find(|d| d.lun == l) {
                    Some(d) if d.kind != DiskKind::Dvd => {
                        return Err(err_conflict(format!(
                            "SCSI LUN {l} is occupied by a non-DVD disk ({}); specify a different LUN or omit --lun",
                            d.path.display()
                        )));
                    }
                    Some(d) => (d.lun, Some(d.clone())),
                    None => (l, None),
                },
                None => match vm.config.disks.iter().find(|d| d.kind == DiskKind::Dvd) {
                    Some(d) => (d.lun, Some(d.clone())),
                    None => (vm.config.next_free_lun(), None),
                },
            };
            if target_lun > 255 {
                return Err(err_bad_request(format!(
                    "SCSI LUN must be 0..=255 (got {target_lun})"
                )));
            }

            let new_dvd = DiskAttachment {
                lun: target_lun,
                kind: DiskKind::Dvd,
                path: req.path.clone(),
                read_only: true,
                format: DiskFormat::Iso,
            };
            let mut candidate_disks: Vec<DiskAttachment> = vm
                .config
                .disks
                .iter()
                .filter(|d| d.lun != target_lun)
                .cloned()
                .collect();
            candidate_disks.push(new_dvd);
            candidate_disks.sort_by_key(|d| d.lun);
            ensure_disks_not_conflicting(&guard, Some(id), &candidate_disks)?;

            if active_sock.is_none() {
                let mut updated = vm.config.clone();
                updated.disks = candidate_disks;
                updated.updated_at = Utc::now();
                self.store.save_vm(&updated)?;
                let vm = guard
                    .get_mut(&id)
                    .ok_or_else(|| err_not_found("VM not found"))?;
                vm.config = updated;
                vm.runtime.revision = self.next_revision();
                Err(vm.summary())
            } else {
                Ok((target_lun, previous_dvd))
            }
        };

        let (target_lun, previous_dvd) = match active_plan {
            Err(summary) => {
                self.emit(DaemonEvent::VmUpdated {
                    vm: summary.clone(),
                });
                return Ok(summary);
            }
            Ok(plan) => plan,
        };

        if previous_dvd.is_some() {
            let _ = self.detach_disk_inner(id_or_name, target_lun).await?;
        }

        match self
            .attach_disk_inner(
                id_or_name,
                AttachDiskPayload {
                    path: req.path,
                    lun: Some(target_lun),
                    read_only: true,
                    format: Some(DiskFormat::Iso),
                },
            )
            .await
        {
            Ok(summary) => Ok(summary),
            Err(attach_err) => {
                if let Some(prev) = previous_dvd
                    && let Err(restore_err) = self
                        .attach_disk_inner(
                            id_or_name,
                            AttachDiskPayload {
                                path: prev.path,
                                lun: Some(prev.lun),
                                read_only: prev.read_only,
                                format: Some(prev.format),
                            },
                        )
                        .await
                {
                    warn!(
                        vm_id = %id,
                        lun = target_lun,
                        error = %restore_err,
                        "failed to restore previous ISO after mount failure"
                    );
                }
                Err(attach_err)
            }
        }
    }

    pub async fn eject_iso(
        self: &Arc<Self>,
        id_or_name: &str,
        lun: Option<u32>,
    ) -> anyhow::Result<VmSummary> {
        let sup = Arc::clone(self);
        let id_or_name = id_or_name.to_string();
        tokio::spawn(async move {
            let id = sup.resolve_vm_id(&id_or_name)?;
            let target_lun = {
                let guard = sup.vms.read().unwrap();
                let vm = guard
                    .get(&id)
                    .ok_or_else(|| err_not_found("VM not found"))?;
                let _ = vm.active_rpc_socket("eject ISO")?;
                match lun {
                    Some(l) => vm
                        .config
                        .disks
                        .iter()
                        .find(|d| d.lun == l && d.kind == DiskKind::Dvd)
                        .map(|d| d.lun)
                        .ok_or_else(|| err_not_found(format!("no ISO/DVD attached at LUN {l}")))?,
                    None => vm
                        .config
                        .disks
                        .iter()
                        .find(|d| d.kind == DiskKind::Dvd)
                        .map(|d| d.lun)
                        .ok_or_else(|| err_not_found("no ISO/DVD drive attached to VM"))?,
                }
            };
            sup.detach_disk_inner(&id_or_name, target_lun).await
        })
        .await
        .map_err(|e| anyhow::anyhow!("eject_iso task failed: {e}"))?
    }

    pub async fn add_port_forward(
        self: &Arc<Self>,
        id_or_name: &str,
        nic_id: Uuid,
        port: PortForward,
    ) -> anyhow::Result<VmSummary> {
        self.modify_port_forward(id_or_name, nic_id, port, true)
            .await
    }

    pub async fn remove_port_forward(
        self: &Arc<Self>,
        id_or_name: &str,
        nic_id: Uuid,
        port: PortForward,
    ) -> anyhow::Result<VmSummary> {
        self.modify_port_forward(id_or_name, nic_id, port, false)
            .await
    }

    async fn modify_port_forward(
        self: &Arc<Self>,
        id_or_name: &str,
        nic_id: Uuid,
        port: PortForward,
        add: bool,
    ) -> anyhow::Result<VmSummary> {
        let sup = Arc::clone(self);
        let id_or_name = id_or_name.to_string();
        tokio::spawn(async move {
            sup.modify_port_forward_inner(&id_or_name, nic_id, port, add)
                .await
        })
        .await
        .map_err(|e| anyhow::anyhow!("modify_port_forward task failed: {e}"))?
    }

    async fn modify_port_forward_inner(
        &self,
        id_or_name: &str,
        nic_id: Uuid,
        mut port: PortForward,
        add: bool,
    ) -> anyhow::Result<VmSummary> {
        port.normalize();
        if add {
            port.validate()
                .map_err(|e| err_bad_request(format!("{e:#}")))?;
        }
        let id = self.resolve_vm_id(id_or_name)?;
        let active_sock = {
            let mut guard = self.vms.write().unwrap();
            let vm = guard
                .get_mut(&id)
                .ok_or_else(|| err_not_found("VM not found"))?;
            let sock_opt = vm.active_rpc_socket("modify port forwarding")?;
            {
                let nic = vm
                    .config
                    .nics
                    .iter()
                    .find(|n| n.id == nic_id)
                    .ok_or_else(|| err_not_found(format!("NIC {nic_id} not found")))?;
                let NicBackendConfig::Consomme { port_forwards, .. } = &nic.backend else {
                    return Err(err_bad_request(
                        "port forwarding is only supported on Consomme NAT NICs",
                    ));
                };
                if add && port_forwards.contains(&port) {
                    return Err(err_conflict(format!("port forward {port} already exists")));
                }
                if !add && !port_forwards.contains(&port) {
                    return Err(err_not_found(format!("port forward {port} not found")));
                }
            }
            match sock_opt {
                Some(sock) => Ok((sock, vm.generation)),
                None => {
                    let mut updated = vm.config.clone();
                    let nic = updated
                        .nics
                        .iter_mut()
                        .find(|n| n.id == nic_id)
                        .ok_or_else(|| err_not_found("NIC vanished"))?;
                    if let NicBackendConfig::Consomme { port_forwards, .. } = &mut nic.backend {
                        if add {
                            port_forwards.push(port.clone());
                        } else {
                            port_forwards.retain(|p| p != &port);
                        }
                    }
                    updated.updated_at = Utc::now();
                    self.store.save_vm(&updated)?;
                    vm.config = updated;
                    vm.runtime.revision = self.next_revision();
                    Err(vm.summary())
                }
            }
        };

        let summary = match active_sock {
            Err(summary) => summary,
            Ok((sock, rpc_gen)) => {
                let modify_type = if add {
                    vmservice::ModifyType::Update as i32
                } else {
                    vmservice::ModifyType::Remove as i32
                };
                TtrpcClient::new(sock)
                    .modify_resource(vmservice::ModifyResourceRequest {
                        r#type: modify_type,
                        resource: Some(vmservice::modify_resource_request::Resource::NicConfig(
                            vmservice::NicConfig {
                                nic_id: nic_id.to_string(),
                                backend: Some(vmservice::nic_config::Backend::Consomme(
                                    vmservice::ConsommeBackend {
                                        ports: vec![to_proto_port_config(&port)],
                                        ..Default::default()
                                    },
                                )),
                                ..Default::default()
                            },
                        )),
                    })
                    .await
                    .context("updating Consomme port forward on running VM")?;

                let mut guard = self.vms.write().unwrap();
                let vm = guard
                    .get_mut(&id)
                    .ok_or_else(|| err_not_found("VM not found"))?;
                if vm.generation != rpc_gen
                    || !matches!(vm.runtime.status, VmStatus::Running | VmStatus::Paused)
                {
                    return Err(err_conflict(format!(
                        "cannot complete port forward update because VM '{}' changed state to {}",
                        vm.config.name, vm.runtime.status
                    )));
                }
                let mut updated = vm.config.clone();
                let nic = updated
                    .nics
                    .iter_mut()
                    .find(|n| n.id == nic_id)
                    .ok_or_else(|| err_not_found("NIC vanished"))?;
                if let NicBackendConfig::Consomme { port_forwards, .. } = &mut nic.backend {
                    if add {
                        if !port_forwards.contains(&port) {
                            port_forwards.push(port);
                        }
                    } else {
                        port_forwards.retain(|p| p != &port);
                    }
                }
                updated.updated_at = Utc::now();
                self.store.save_vm(&updated)?;
                vm.config = updated;
                vm.runtime.revision = self.next_revision();
                vm.summary()
            }
        };

        self.emit(DaemonEvent::VmUpdated {
            vm: summary.clone(),
        });
        Ok(summary)
    }

    #[cfg(test)]
    pub fn set_vm_status_for_test(&self, id: Uuid, status: VmStatus) {
        if let Some(vm) = self.vms.write().unwrap().get_mut(&id) {
            vm.runtime.status = status;
        }
    }

    #[cfg(test)]
    pub fn generation_for_test(&self, id: Uuid) -> u64 {
        self.vms.read().unwrap()[&id].generation
    }

    /// Simulates a lifecycle transition (stop/start) that bumps the generation.
    #[cfg(test)]
    pub fn advance_lifecycle_for_test(&self, id: Uuid, status: VmStatus) {
        if let Some(vm) = self.vms.write().unwrap().get_mut(&id) {
            vm.generation += 1;
            vm.runtime.status = status;
        }
    }

    #[cfg(test)]
    pub fn set_runtime_dir_changing_for_test(&self, changing: bool) {
        self.runtime_dir_changing.store(changing, Ordering::Release);
    }

    pub fn create_disk(&self, req: CreateDiskPayload) -> anyhow::Result<DiskInfo> {
        self.create_disk_cancellable(req, &AtomicBool::new(false))
    }

    pub fn create_disk_cancellable(
        &self,
        req: CreateDiskPayload,
        cancelled: &AtomicBool,
    ) -> anyhow::Result<DiskInfo> {
        let paths = self.resolved_paths();
        disk::create_disk_cancellable(&req, paths.openvmm_img_bin.as_deref(), cancelled)
            .map_err(|e| err_bad_request(format!("{e:#}")))
    }

    pub fn inspect_disk(&self, path: &Path) -> anyhow::Result<DiskInfo> {
        let paths = self.resolved_paths();
        disk::inspect_disk(path, paths.openvmm_img_bin.as_deref())
            .map_err(|e| err_bad_request(format!("{e:#}")))
    }

    pub fn vm_logs(&self, id_or_name: &str, tail: usize) -> anyhow::Result<String> {
        let id = self.resolve_vm_id(id_or_name)?;
        let log_path = self.store.vm_log_path(id);
        let stderr_path = log_path.with_extension("stderr.log");
        let mut out = String::new();
        if log_path.is_file() {
            out = read_log_tail(&log_path, tail.max(1))?;
        }
        if stderr_path.is_file() {
            let err_tail = read_log_tail(&stderr_path, 40)?;
            if !err_tail.trim().is_empty() {
                if !out.is_empty() {
                    out.push_str("\n--- stderr ---\n");
                }
                out.push_str(&err_tail);
            }
        }
        Ok(out)
    }

    pub async fn detach_all_vms(self: &Arc<Self>) {
        self.mark_shutting_down();
        let (starting_ids, stopping_waits, monitor_handles) = {
            let mut guard = self.vms.write().unwrap();
            let mut starting_ids = Vec::new();
            let mut stopping_waits = Vec::new();
            let mut monitor_handles = Vec::new();
            for vm in guard.values_mut() {
                vm.detach_on_drop = true;
                match vm.runtime.status {
                    VmStatus::Starting => {
                        starting_ids.push(vm.config.id);
                    }
                    VmStatus::Stopping => {
                        if let Some(rx) = vm.stop_wait.clone() {
                            stopping_waits.push(rx);
                        }
                    }
                    VmStatus::Running | VmStatus::Paused => {
                        if let Some(tx) = vm.monitor_cancel.take() {
                            let _ = tx.send(true);
                        }
                        if let Some(handle) = vm.monitor_handle.take() {
                            monitor_handles.push(handle);
                        }
                        // Drop Child without killing/waiting and preserve runtime files so the next daemon re-adopts it.
                        let _ = vm.child.take();
                        vm.runtime.rpc_socket_path = None;
                        vm.runtime.shm_path = None;
                    }
                    VmStatus::Off | VmStatus::Error => {}
                }
            }
            (starting_ids, stopping_waits, monitor_handles)
        };

        for mut handle in monitor_handles {
            if tokio::time::timeout(Duration::from_secs(3), &mut handle)
                .await
                .is_err()
            {
                handle.abort();
                let _ = handle.await;
            }
        }
        for id in starting_ids {
            if let Err(err) = self.stop_vm(&id.to_string()).await {
                warn!(vm_id = %id, error = %err, "failed to cancel starting VM during daemon detach");
            }
        }
        for mut rx in stopping_waits {
            let _ = tokio::time::timeout(Duration::from_secs(5), async {
                while !*rx.borrow_and_update() {
                    if rx.changed().await.is_err() {
                        break;
                    }
                }
            })
            .await;
        }
        let mut guard = self.vms.write().unwrap();
        for vm in guard.values_mut() {
            vm.detach_on_drop = true;
            if matches!(vm.runtime.status, VmStatus::Running | VmStatus::Paused) {
                let _ = vm.child.take();
                vm.adopted_proc = None;
                vm.runtime.rpc_socket_path = None;
                vm.runtime.shm_path = None;
            }
        }
    }

    pub async fn shutdown_all_vms(self: &Arc<Self>) {
        let ids: Vec<Uuid> = {
            let guard = self.vms.read().unwrap();
            guard
                .values()
                .filter(|vm| !matches!(vm.runtime.status, VmStatus::Off | VmStatus::Error))
                .map(|vm| vm.config.id)
                .collect()
        };
        for id in ids {
            if let Err(err) = self.stop_vm(&id.to_string()).await {
                warn!(vm_id = %id, error = %err, "failed to stop VM during daemon shutdown");
            }
        }
    }

    fn require_running_socket(&self, id: Uuid) -> anyhow::Result<PathBuf> {
        self.require_socket_with_generation(id, &[VmStatus::Running])
            .map(|(sock, _)| sock)
    }

    fn require_socket_with_generation(
        &self,
        id: Uuid,
        allowed: &[VmStatus],
    ) -> anyhow::Result<(PathBuf, u64)> {
        let guard = self.vms.read().unwrap();
        let vm = guard
            .get(&id)
            .ok_or_else(|| err_not_found("VM not found"))?;
        if !allowed.contains(&vm.runtime.status) {
            let names: Vec<String> = allowed.iter().map(ToString::to_string).collect();
            return Err(err_conflict(format!(
                "VM '{}' is {} (must be {})",
                vm.config.name,
                vm.runtime.status,
                names.join(" or ")
            )));
        }
        let sock = vm
            .runtime
            .rpc_socket_path
            .clone()
            .ok_or_else(|| err_internal("VM has no active RPC socket"))?;
        Ok((sock, vm.generation))
    }

    fn require_active_socket(&self, id: Uuid) -> anyhow::Result<PathBuf> {
        self.require_socket_with_generation(id, &[VmStatus::Running, VmStatus::Paused])
            .map(|(sock, _)| sock)
    }

    fn emit_vm_snapshot(&self, id: Uuid) {
        let guard = self.vms.read().unwrap();
        if let Some(vm) = guard.get(&id) {
            self.emit(DaemonEvent::VmUpdated { vm: vm.summary() });
        }
    }
}

fn ensure_disk_format_safe(
    path: &Path,
    format: DiskFormat,
    explicit_format: bool,
) -> anyhow::Result<()> {
    if path.as_os_str().is_empty() || path.file_name().is_none() {
        return Err(err_bad_request("disk path must specify a file name"));
    }
    if format == DiskFormat::Raw {
        let lower = path
            .extension()
            .and_then(|e| e.to_str())
            .map(|e| e.to_ascii_lowercase());
        if matches!(
            lower.as_deref(),
            Some("vhdx" | "avhdx" | "vhd" | "avhd" | "vmdk" | "vmgs" | "iso")
        ) {
            return Err(err_bad_request(format!(
                "disk path '{}' is incompatible with raw format: extension is reserved for another disk format (specify the matching format such as vhdx, vhd, or iso)",
                path.display()
            )));
        }
        if !explicit_format && disk::file_has_vhdx_signature(path) {
            return Err(err_bad_request(format!(
                "disk image '{}' starts with a VHDX signature ('vhdxfile') but its format was inferred as raw; rename it to .vhdx or specify --format vhdx explicitly",
                path.display()
            )));
        }
    }
    Ok(())
}

fn ensure_disks_not_conflicting(
    vms: &HashMap<Uuid, ManagedVm>,
    self_id: Option<Uuid>,
    disks: &[DiskAttachment],
) -> anyhow::Result<()> {
    let mut seen_paths: HashMap<PathBuf, u32> = HashMap::new();
    for d in disks {
        let key = openvmms_core::models::normalize_path_key(&d.path);
        if let Some(prev_lun) = seen_paths.insert(key.clone(), d.lun) {
            return Err(err_conflict(format!(
                "disk image '{}' is attached multiple times to the same VM (LUNs {prev_lun} and {})",
                d.path.display(),
                d.lun
            )));
        }
        for other in vms.values() {
            if Some(other.config.id) == self_id {
                continue;
            }
            for other_disk in &other.config.disks {
                if openvmms_core::models::normalize_path_key(&other_disk.path) == key
                    && (!d.read_only || !other_disk.read_only)
                {
                    return Err(err_conflict(format!(
                        "disk image '{}' is already attached to VM '{}' (LUN {}); detach it first or attach both read-only",
                        d.path.display(),
                        other.config.name,
                        other_disk.lun
                    )));
                }
            }
        }
    }
    Ok(())
}

fn sanitize_disk_stem(name: &str) -> String {
    let cleaned: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_') {
                c
            } else {
                '-'
            }
        })
        .collect();
    let trimmed = cleaned.trim_matches('-');
    if trimmed.is_empty() {
        "vm".to_string()
    } else {
        trimmed.to_string()
    }
}

fn preallocate_shm_file(path: &Path, vram_mb: u32) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut opts = fs::OpenOptions::new();
    opts.read(true).write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let file = opts
        .open(path)
        .with_context(|| format!("creating FastDesk SHM file {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = file.set_permissions(fs::Permissions::from_mode(0o600));
    }
    let size = (vram_mb.clamp(8, 256) as u64) * 1024 * 1024;
    file.set_len(size)
        .with_context(|| format!("sizing FastDesk SHM file {}", path.display()))?;
    Ok(())
}

fn wait_or_terminate_adopted_proc(
    sock_path: Option<&Path>,
    proc_handle: &openvmms_core::process::AdoptedProcess,
) {
    let client = sock_path.map(TtrpcClient::new);
    let start = std::time::Instant::now();
    loop {
        let sock_alive = client.as_ref().is_some_and(TtrpcClient::is_socket_accepting);
        let proc_alive = proc_handle.is_alive();
        if !sock_alive && !proc_alive {
            return;
        }
        if start.elapsed() >= Duration::from_secs(3) {
            proc_handle.terminate();
            let kill_deadline = std::time::Instant::now() + Duration::from_secs(1);
            while std::time::Instant::now() < kill_deadline {
                let still_sock = client.as_ref().is_some_and(TtrpcClient::is_socket_accepting);
                if !still_sock && !proc_handle.is_alive() {
                    return;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn reclaim_or_verify_inactive_socket(
    sock_path: &Path,
    pid_path: &Path,
    expected_bin: Option<&Path>,
    cancelled: &AtomicBool,
) -> bool {
    if cancelled.load(Ordering::Acquire) {
        return false;
    }
    let client = TtrpcClient::new(sock_path);
    let verified_proc = openvmms_core::process::open_verified_pid(pid_path, expected_bin);
    let responded = client
        .properties_vm_blocking_cancellable(Duration::from_secs(1), cancelled)
        .is_ok();
    if cancelled.load(Ordering::Acquire) {
        return false;
    }
    if responded || client.is_socket_accepting_cancellable(Duration::from_secs(1), cancelled) {
        if cancelled.load(Ordering::Acquire) {
            return false;
        }
        let _ = client.teardown_and_quit_blocking_cancellable(Duration::from_secs(2), cancelled);
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while std::time::Instant::now() < deadline {
            if cancelled.load(Ordering::Acquire) {
                return false;
            }
            if !client.is_socket_accepting_cancellable(Duration::from_millis(200), cancelled) {
                return false;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        if let Some(proc_handle) = verified_proc {
            if cancelled.load(Ordering::Acquire) {
                return false;
            }
            proc_handle.terminate();
            let kill_deadline = std::time::Instant::now() + Duration::from_secs(1);
            while std::time::Instant::now() < kill_deadline {
                if cancelled.load(Ordering::Acquire) {
                    return false;
                }
                if !client.is_socket_accepting_cancellable(Duration::from_millis(200), cancelled) {
                    return false;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
        return !cancelled.load(Ordering::Acquire)
            && client.is_socket_accepting_cancellable(Duration::from_millis(200), cancelled);
    }
    false
}

fn cleanup_runtime_files(sock_path: Option<PathBuf>, shm_path: Option<String>) {
    if let Some(sock) = sock_path {
        let _ = fs::remove_file(sock.with_extension("pid"));
        let _ = fs::remove_file(sock);
    }
    if let Some(shm) = shm_path {
        let _ = fs::remove_file(shm);
    }
}

fn normalize_fastdesk_endpoint(raw: &str) -> String {
    let normalized = if raw.starts_with("tcp://")
        || raw.starts_with("hvsock://")
        || raw.starts_with("vsock://")
    {
        raw.to_string()
    } else {
        format!("tcp://{raw}")
    };
    if let Some(rest) = normalized.strip_prefix("tcp://0.0.0.0:") {
        return format!("tcp://127.0.0.1:{rest}");
    }
    if let Some(rest) = normalized.strip_prefix("tcp://[::]:") {
        return format!("tcp://[::1]:{rest}");
    }
    normalized
}

pub(crate) fn build_create_vm_request(
    cfg: &VmConfig,
    uefi_path: &Path,
    shm_path: Option<&Path>,
) -> anyhow::Result<vmservice::CreateVmRequest> {
    let sb_template = match cfg.secure_boot_template {
        SecureBootTemplateConfig::None => {
            vmservice::uefi::initial_variables::SecureBootTemplate::None as i32
        }
        SecureBootTemplateConfig::MicrosoftWindows => {
            vmservice::uefi::initial_variables::SecureBootTemplate::MicrosoftWindows as i32
        }
        SecureBootTemplateConfig::MicrosoftUefiCa => {
            vmservice::uefi::initial_variables::SecureBootTemplate::MicrosoftUefiCertificateAuthority
                as i32
        }
    };

    let vmgs = cfg.vmgs_path.as_ref().map(|path| {
        let create_size_bytes = (!path.exists()).then_some(4 * 1024 * 1024);
        vmservice::VmgsConfig {
            backend: Some(vmservice::DiskBackend {
                kind: Some(vmservice::disk_backend::Kind::File(vmservice::FileDisk {
                    path: path.to_string_lossy().into_owned(),
                    direct: false,
                    create_size_bytes,
                })),
            }),
            provision_condition: vmservice::vmgs_config::ProvisionCondition::OnEmpty as i32,
        }
    });

    let scsi_disks = cfg.disks.iter().map(to_proto_scsi_disk).collect();
    let nic_config = cfg.nics.iter().map(to_proto_nic_config).collect();

    let fastdesk_config = if cfg.fastdesk.enabled {
        Some(vmservice::FastDeskConfig {
            vram_mb: cfg.fastdesk.vram_mb,
            listen_addr: cfg.fastdesk.listen_addr.clone(),
            vsock_port: match cfg.fastdesk.transport {
                FastDeskTransport::HvSock => Some(cfg.fastdesk.vsock_port.unwrap_or(0)),
                FastDeskTransport::Tcp => None,
            },
            shm_path: shm_path.map(|p| p.to_string_lossy().into_owned()),
            max_clients: 16,
        })
    } else {
        None
    };

    Ok(vmservice::CreateVmRequest {
        config: Some(vmservice::VmConfig {
            memory_config: Some(vmservice::MemoryConfig {
                memory_mb: cfg.memory_mb,
                ..Default::default()
            }),
            processor_config: Some(vmservice::ProcessorConfig {
                processor_count: cfg.vcpus,
                ..Default::default()
            }),
            boot_config: Some(vmservice::vm_config::BootConfig::Uefi(vmservice::Uefi {
                firmware_path: uefi_path.to_string_lossy().into_owned(),
                initial_variables: Some(vmservice::uefi::InitialVariables {
                    secure_boot_template: sb_template,
                    custom_uefi_json_path: None,
                }),
                secure_boot_enabled: cfg.secure_boot_enabled,
            })),
            devices_config: Some(vmservice::DevicesConfig {
                scsi_disks,
                nic_config,
                enable_scsi_controller: true,
                ..Default::default()
            }),
            nested_virt: cfg.nested_virt,
            vmgs,
            fastdesk_config,
            tpm_enabled: cfg.tpm_enabled,
            ..Default::default()
        }),
        log_id: cfg.id.to_string(),
    })
}

fn to_proto_scsi_disk(d: &DiskAttachment) -> vmservice::ScsiDisk {
    let disk_type = if d.kind == DiskKind::Dvd {
        vmservice::DiskType::ScsiDiskTypePhysical as i32
    } else {
        match d.format {
            DiskFormat::Vhdx => vmservice::DiskType::ScsiDiskTypeVhdx as i32,
            DiskFormat::Vhd => vmservice::DiskType::ScsiDiskTypeVhd1 as i32,
            DiskFormat::Raw | DiskFormat::Iso => vmservice::DiskType::ScsiDiskTypePhysical as i32,
        }
    };
    vmservice::ScsiDisk {
        controller: 0,
        lun: d.lun,
        host_path: d.path.to_string_lossy().into_owned(),
        r#type: disk_type,
        read_only: d.read_only || d.kind == DiskKind::Dvd,
        is_dvd: d.kind == DiskKind::Dvd,
    }
}

fn to_proto_port_config(pf: &PortForward) -> vmservice::PortConfig {
    vmservice::PortConfig {
        host_port: pf.host_port as u32,
        guest_port: pf.guest_port as u32,
        protocol: match pf.protocol {
            IpProto::Tcp => vmservice::IpProtocol::Tcp as i32,
            IpProto::Udp => vmservice::IpProtocol::Udp as i32,
        },
        host_address: pf.host_addr.clone(),
    }
}

fn to_proto_nic_config(nic: &NicAttachment) -> vmservice::NicConfig {
    let backend = match &nic.backend {
        NicBackendConfig::Consomme {
            cidr,
            port_forwards,
            internal_dns,
            allow_host_local_access,
        } => vmservice::nic_config::Backend::Consomme(vmservice::ConsommeBackend {
            cidr: cidr.clone().unwrap_or_default(),
            ports: port_forwards.iter().map(to_proto_port_config).collect(),
            internal_dns: Some(*internal_dns),
            allow_host_local_access: *allow_host_local_access,
            ..Default::default()
        }),
        NicBackendConfig::VmSwitch { switch_id, .. } => {
            vmservice::nic_config::Backend::Dio(vmservice::DioBackend {
                switch_id: switch_id.clone(),
                port_id: String::new(),
            })
        }
        NicBackendConfig::Tap { name } => {
            vmservice::nic_config::Backend::Tap(vmservice::TapBackend {
                source: Some(vmservice::tap_backend::Source::Name(name.clone())),
            })
        }
    };
    vmservice::NicConfig {
        nic_id: nic.id.to_string(),
        mac_address: nic.mac_address.clone(),
        nic_name: nic.name.clone(),
        frontend: vmservice::NicFrontend::Vmbus as i32,
        backend: Some(backend),
        ..Default::default()
    }
}

fn read_log_tail(path: &Path, tail: usize) -> anyhow::Result<String> {
    const CHUNK_SIZE: u64 = 64 * 1024;
    const MAX_TAIL_BYTES: usize = 4 * 1024 * 1024;

    let tail = tail.clamp(1, 10_000);
    let mut file = fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let file_len = file.metadata()?.len();
    if file_len == 0 {
        return Ok(String::new());
    }

    let mut pos = file_len;
    let mut chunks: Vec<Vec<u8>> = Vec::new();
    let mut total_bytes = 0usize;
    let mut newline_count = 0usize;

    while pos > 0 && total_bytes < MAX_TAIL_BYTES {
        let to_read = pos.min(CHUNK_SIZE).min((MAX_TAIL_BYTES - total_bytes) as u64);
        pos -= to_read;
        file.seek(SeekFrom::Start(pos))?;
        let mut buf = vec![0u8; to_read as usize];
        file.read_exact(&mut buf)?;
        newline_count += buf.iter().filter(|&&b| b == b'\n').count();
        total_bytes += buf.len();
        chunks.push(buf);
        if newline_count > tail {
            break;
        }
    }

    let mut combined = Vec::with_capacity(total_bytes);
    for chunk in chunks.into_iter().rev() {
        combined.extend_from_slice(&chunk);
    }

    let text = String::from_utf8_lossy(&combined);
    let lines: Vec<&str> = text.lines().collect();
    let start = lines.len().saturating_sub(tail);
    Ok(lines[start..].join("\n"))
}

#[cfg(windows)]
fn windows_hyperv_module_available() -> bool {
    let mut roots: Vec<PathBuf> = std::env::var_os("PSModulePath")
        .map(|val| std::env::split_paths(&val).collect())
        .unwrap_or_default();
    if let Some(sys_root) = std::env::var_os("SystemRoot") {
        roots.push(
            PathBuf::from(sys_root)
                .join("System32")
                .join("WindowsPowerShell")
                .join("v1.0")
                .join("Modules"),
        );
    }
    roots.into_iter().any(|dir| dir.join("Hyper-V").is_dir())
}

#[cfg(windows)]
fn discover_windows_switches() -> Vec<VirtualSwitchInfo> {
    use std::io::Read;
    use std::os::windows::process::CommandExt;
    use std::sync::OnceLock;
    use std::time::Instant;

    const CACHE_TTL: Duration = Duration::from_secs(10);
    const QUERY_TIMEOUT: Duration = Duration::from_secs(3);
    const CREATE_NO_WINDOW: u32 = 0x0800_0000;

    static CACHE: OnceLock<std::sync::Mutex<Option<(Instant, Vec<VirtualSwitchInfo>)>>> =
        OnceLock::new();
    let cache = CACHE.get_or_init(|| std::sync::Mutex::new(None));
    let mut guard = cache.lock().unwrap();
    if let Some((captured_at, cached)) = guard.as_ref()
        && captured_at.elapsed() < CACHE_TTL
    {
        return cached.clone();
    }

    let mut out = vec![VirtualSwitchInfo {
        id: "c08cb7b8-9b3c-408e-8e30-5e16a3aeb444".to_string(),
        name: "Default Switch".to_string(),
        kind: "vmswitch".to_string(),
        description: "Windows Hyper-V Default Switch (HCN)".to_string(),
    }];

    if windows_hyperv_module_available()
        && let Ok(mut child) = Command::new("powershell.exe")
            .args([
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "Get-VMSwitch | Select-Object @{n='Id';e={$_.Id.ToString()}},Name,SwitchType | ConvertTo-Json -Compress",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .creation_flags(CREATE_NO_WINDOW)
            .spawn()
    {
        let reader_handle = child.stdout.take().map(|mut stdout| {
            std::thread::spawn(move || {
                let mut buf = Vec::new();
                let _ = stdout.read_to_end(&mut buf);
                buf
            })
        });
        let deadline = Instant::now() + QUERY_TIMEOUT;
        let mut status = None;
        loop {
            match child.try_wait() {
                Ok(Some(st)) => {
                    status = Some(st);
                    break;
                }
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(25));
                }
                _ => {
                    let _ = child.kill();
                    let _ = child.wait();
                    break;
                }
            }
        }
        if status.is_some_and(|st| st.success())
            && let Some(stdout_bytes) = reader_handle.and_then(|h| h.join().ok())
            && let Ok(val) = serde_json::from_slice::<serde_json::Value>(&stdout_bytes)
        {
            let arr = match val {
                serde_json::Value::Array(items) => items,
                obj @ serde_json::Value::Object(_) => vec![obj],
                _ => Vec::new(),
            };
            for item in arr {
                if let (Some(id), Some(name)) = (item["Id"].as_str(), item["Name"].as_str()) {
                    let id_lower = id.to_ascii_lowercase();
                    if !out.iter().any(|s| s.id.eq_ignore_ascii_case(&id_lower)) {
                        out.push(VirtualSwitchInfo {
                            id: id_lower,
                            name: name.to_string(),
                            kind: "vmswitch".to_string(),
                            description: format!(
                                "Hyper-V Virtual Switch ({})",
                                item["SwitchType"].as_str().unwrap_or("Unknown")
                            ),
                        });
                    }
                }
            }
        }
    }

    *guard = Some((Instant::now(), out.clone()));
    out
}

#[cfg(target_os = "linux")]
fn discover_linux_tap_interfaces() -> Vec<VirtualSwitchInfo> {
    discover_linux_tap_interfaces_in(Path::new("/sys/class/net"))
}

#[cfg(any(target_os = "linux", test))]
fn discover_linux_tap_interfaces_in(sys_class_net: &Path) -> Vec<VirtualSwitchInfo> {
    let mut out = Vec::new();
    if let Ok(entries) = fs::read_dir(sys_class_net) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if let Some(info) = parse_linux_tap_in_dir(&entry.path(), &name) {
                out.push(info);
            }
        }
    }
    out
}

#[cfg(any(target_os = "linux", test))]
fn parse_linux_tap_in_dir(iface_path: &Path, name: &str) -> Option<VirtualSwitchInfo> {
    const IFF_TAP: u32 = 0x0002;
    let tun_flags_raw = fs::read_to_string(iface_path.join("tun_flags")).ok()?;
    let trimmed = tun_flags_raw
        .trim()
        .trim_start_matches("0x")
        .trim_start_matches("0X");
    let flags = u32::from_str_radix(trimmed, 16).ok()?;
    if (flags & IFF_TAP) == 0 {
        return None;
    }
    let master_bridge = fs::read_link(iface_path.join("master"))
        .ok()
        .and_then(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()));
    let description = match master_bridge {
        Some(bridge) => format!("Linux persistent TAP interface (attached to bridge '{bridge}')"),
        None => "Linux persistent TAP interface".to_string(),
    };
    Some(VirtualSwitchInfo {
        id: name.to_string(),
        name: name.to_string(),
        kind: "tap".to_string(),
        description,
    })
}

async fn wait_until_true(rx: &mut watch::Receiver<bool>) {
    while !*rx.borrow_and_update() {
        if rx.changed().await.is_err() {
            break;
        }
    }
}

async fn sleep_or_cancel(duration: Duration, cancel_rx: &mut watch::Receiver<bool>) -> bool {
    if *cancel_rx.borrow_and_update() {
        return true;
    }
    tokio::select! {
        _ = tokio::time::sleep(duration) => false,
        res = cancel_rx.changed() => res.is_err() || *cancel_rx.borrow_and_update(),
    }
}

async fn run_cancellable_rpc<F, T>(
    cancel_rx: &mut watch::Receiver<bool>,
    fut: F,
) -> anyhow::Result<T>
where
    F: std::future::Future<Output = anyhow::Result<T>>,
{
    if *cancel_rx.borrow() {
        bail!("VM startup cancelled by stop request");
    }
    tokio::select! {
        res = fut => res,
        _ = cancel_rx.changed() => {
            bail!("VM startup cancelled by stop request");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn linux_switch_discovery_filters_bridges_and_tun_devices() {
        let dir = tempfile::tempdir().unwrap();
        let net = dir.path();

        // Linux bridge -> must NOT appear
        fs::create_dir_all(net.join("br0").join("bridge")).unwrap();

        // Layer-3 TUN device (IFF_TUN = 0x0001, no IFF_TAP) -> must NOT appear
        fs::create_dir_all(net.join("tun0")).unwrap();
        fs::write(net.join("tun0").join("tun_flags"), "0x1001\n").unwrap();

        // Persistent TAP attached to bridge br0 (IFF_TAP = 0x0002) -> MUST appear
        fs::create_dir_all(net.join("tap0")).unwrap();
        fs::write(net.join("tap0").join("tun_flags"), "0x1002\n").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(net.join("br0"), net.join("tap0").join("master")).unwrap();

        let switches = discover_linux_tap_interfaces_in(net);
        assert_eq!(switches.len(), 1);
        assert_eq!(switches[0].id, "tap0");
        assert_eq!(switches[0].kind, "tap");
        #[cfg(unix)]
        assert!(switches[0].description.contains("br0"));
    }

    #[test]
    fn read_log_tail_reads_exact_last_lines_from_large_file() {
        use std::io::Write;

        let dir = tempfile::tempdir().unwrap();
        let log_path = dir.path().join("large.log");
        {
            let mut f = fs::File::create(&log_path).unwrap();
            let filler = "x".repeat(120);
            for i in 0..25_000 {
                writeln!(f, "line-{i:05}:{filler}").unwrap();
            }
        }
        assert!(fs::metadata(&log_path).unwrap().len() > 3 * 1024 * 1024);

        let tail = read_log_tail(&log_path, 5).unwrap();
        let lines: Vec<&str> = tail.lines().collect();
        assert_eq!(lines.len(), 5);
        assert!(lines[0].starts_with("line-24995:"));
        assert!(lines[4].starts_with("line-24999:"));
    }

    #[tokio::test]
    async fn start_vm_is_rejected_while_runtime_dir_is_changing() {
        let dir = tempfile::tempdir().unwrap();
        let sup = Supervisor::new(dir.path(), Some("127.0.0.1:0")).unwrap();
        let created = sup
            .create_vm(CreateVmPayload {
                name: "RuntimeDirRace-VM".into(),
                preset: openvmms_core::models::VmPreset::Custom,
                vcpus: Some(2),
                memory_mb: Some(1024),
                ..Default::default()
            })
            .unwrap();

        sup.set_runtime_dir_changing_for_test(true);
        let err = sup.start_vm(&created.config.id.to_string()).await.unwrap_err();
        let kind = err
            .chain()
            .find_map(|c| c.downcast_ref::<ApiErrorKind>());
        assert!(matches!(kind, Some(ApiErrorKind::Conflict(_))), "{err:#}");
        assert_eq!(
            sup.get_vm(&created.config.id.to_string())
                .unwrap()
                .runtime
                .status,
            VmStatus::Off
        );

        sup.set_runtime_dir_changing_for_test(false);
    }

    #[test]
    fn build_create_vm_request_translates_all_compute_boot_disk_nic_and_fastdesk_fields() {
        let vm_dir = tempfile::tempdir().unwrap();
        let disk_vhdx = vm_dir.path().join("os.vhdx");
        let iso_path = vm_dir.path().join("install.iso");
        let uefi_path = vm_dir.path().join("MSVM.fd");
        let shm_path = vm_dir.path().join("vram.shm");
        let vmgs_path = vm_dir.path().join("state.vmgs");

        let mut cfg = VmConfig::from_preset(
            "TranslateTest",
            openvmms_core::models::VmPreset::Windows11,
            vm_dir.path(),
        );
        cfg.vcpus = 6;
        cfg.memory_mb = 6144;
        cfg.nested_virt = true;
        cfg.secure_boot_enabled = true;
        cfg.secure_boot_template = SecureBootTemplateConfig::MicrosoftUefiCa;
        cfg.tpm_enabled = true;
        cfg.vmgs_path = Some(vmgs_path);
        cfg.disks = vec![
            DiskAttachment {
                lun: 0,
                kind: DiskKind::HardDisk,
                path: disk_vhdx.clone(),
                read_only: false,
                format: DiskFormat::Vhdx,
            },
            DiskAttachment {
                lun: 1,
                kind: DiskKind::Dvd,
                path: iso_path.clone(),
                read_only: true,
                format: DiskFormat::Iso,
            },
        ];
        cfg.nics = vec![
            NicAttachment {
                id: Uuid::new_v4(),
                name: "nat0".into(),
                mac_address: "52:54:00:11:22:33".into(),
                backend: NicBackendConfig::Consomme {
                    cidr: Some("10.0.2.0/24".into()),
                    port_forwards: vec![PortForward {
                        host_addr: "127.0.0.1".into(),
                        host_port: 2222,
                        guest_port: 22,
                        protocol: IpProto::Tcp,
                    }],
                    internal_dns: true,
                    allow_host_local_access: true,
                },
            },
            NicAttachment {
                id: Uuid::new_v4(),
                name: "tap0".into(),
                mac_address: "52:54:00:44:55:66".into(),
                backend: NicBackendConfig::Tap {
                    name: "tap-test".into(),
                },
            },
        ];
        cfg.fastdesk.enabled = true;
        cfg.fastdesk.vram_mb = 64;
        cfg.fastdesk.listen_addr = "127.0.0.1:5905".into();
        cfg.fastdesk.transport = FastDeskTransport::HvSock;
        cfg.fastdesk.vsock_port = Some(6005);

        let req = build_create_vm_request(&cfg, &uefi_path, Some(&shm_path)).unwrap();
        assert_eq!(req.log_id, cfg.id.to_string());
        let vm_cfg = req.config.unwrap();
        assert_eq!(vm_cfg.memory_config.unwrap().memory_mb, 6144);
        assert_eq!(vm_cfg.processor_config.unwrap().processor_count, 6);
        assert!(vm_cfg.nested_virt);
        assert!(vm_cfg.tpm_enabled);

        match vm_cfg.boot_config.unwrap() {
            vmservice::vm_config::BootConfig::Uefi(u) => {
                assert_eq!(u.firmware_path, uefi_path.to_string_lossy());
                assert!(u.secure_boot_enabled);
                assert_eq!(
                    u.initial_variables.unwrap().secure_boot_template,
                    vmservice::uefi::initial_variables::SecureBootTemplate::MicrosoftUefiCertificateAuthority as i32
                );
            }
            other => panic!("unexpected boot config: {other:?}"),
        }

        let devices = vm_cfg.devices_config.unwrap();
        assert!(devices.enable_scsi_controller);
        assert_eq!(devices.scsi_disks.len(), 2);
        assert_eq!(devices.scsi_disks[0].lun, 0);
        assert_eq!(
            devices.scsi_disks[0].r#type,
            vmservice::DiskType::ScsiDiskTypeVhdx as i32
        );
        assert!(!devices.scsi_disks[0].read_only);
        assert!(!devices.scsi_disks[0].is_dvd);
        assert_eq!(devices.scsi_disks[1].lun, 1);
        assert_eq!(
            devices.scsi_disks[1].r#type,
            vmservice::DiskType::ScsiDiskTypePhysical as i32
        );
        assert!(devices.scsi_disks[1].read_only);
        assert!(devices.scsi_disks[1].is_dvd);

        assert_eq!(devices.nic_config.len(), 2);
        assert_eq!(devices.nic_config[0].mac_address, "52:54:00:11:22:33");
        match devices.nic_config[0].backend.as_ref().unwrap() {
            vmservice::nic_config::Backend::Consomme(c) => {
                assert_eq!(c.cidr, "10.0.2.0/24");
                assert_eq!(c.internal_dns, Some(true));
                assert!(c.allow_host_local_access);
                assert_eq!(c.ports.len(), 1);
                assert_eq!(c.ports[0].host_port, 2222);
                assert_eq!(c.ports[0].guest_port, 22);
            }
            other => panic!("expected Consomme backend, got {other:?}"),
        }
        match devices.nic_config[1].backend.as_ref().unwrap() {
            vmservice::nic_config::Backend::Tap(t) => {
                assert!(matches!(
                    t.source,
                    Some(vmservice::tap_backend::Source::Name(ref n)) if n == "tap-test"
                ));
            }
            other => panic!("expected Tap backend, got {other:?}"),
        }

        let fd = vm_cfg.fastdesk_config.unwrap();
        assert_eq!(fd.vram_mb, 64);
        assert_eq!(fd.listen_addr, "127.0.0.1:5905");
        assert_eq!(fd.vsock_port, Some(6005));
        assert_eq!(fd.shm_path.as_deref(), Some(shm_path.to_string_lossy().as_ref()));
    }
}
