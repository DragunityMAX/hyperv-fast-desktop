use anyhow::Context;
use openvmms_core::api_client::ApiClient;
use openvmms_core::api_client::ensure_daemon_running;
use openvmms_core::api_client::launch_fastdesk_viewer;
use openvmms_core::api_client::resolve_local_viewer_binary;
use openvmms_core::models::AttachDiskPayload;
use openvmms_core::models::CreateDiskPayload;
use openvmms_core::models::CreateVmPayload;
use openvmms_core::models::DaemonEvent;
use openvmms_core::models::DiskInfo;
use openvmms_core::models::HostConfig;
use openvmms_core::models::HostInfo;
use openvmms_core::models::MountIsoPayload;
use openvmms_core::models::PortForward;
use openvmms_core::models::ShutdownVmPayload;
use openvmms_core::models::UpdateVmPayload;
use openvmms_core::models::VirtualSwitchInfo;
use openvmms_core::models::VmSummary;
use std::collections::HashSet;
use std::io::BufRead;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use uuid::Uuid;

type SharedEndpoint = Arc<RwLock<Option<(String, String)>>>;

#[derive(Debug, Clone)]
pub enum GuiRequest {
    ConnectOrStartDaemon,
    RefreshAll,
    FetchVmDetails { id: Uuid },
    FetchVmLogs { id: Uuid },
    CreateVm { wizard_id: Uuid, payload: CreateVmPayload },
    UpdateVm { id: Uuid, payload: UpdateVmPayload },
    DeleteVm { id: Uuid, delete_disks: bool },
    StartVm { id: Uuid },
    StopVm { id: Uuid },
    ShutdownVm { id: Uuid, acpi: bool, force: bool },
    PauseVm { id: Uuid },
    ResumeVm { id: Uuid },
    ResetVm { id: Uuid },
    ConnectViewer { id: Uuid, no_shm: bool },
    AttachDisk { id: Uuid, payload: AttachDiskPayload },
    DetachDisk { id: Uuid, lun: u32 },
    MountIso { id: Uuid, payload: MountIsoPayload },
    EjectIso { id: Uuid, lun: Option<u32> },
    AddPortForward { id: Uuid, nic_id: Uuid, port: PortForward },
    RemovePortForward { id: Uuid, nic_id: Uuid, port: PortForward },
    CreateDisk(CreateDiskPayload),
    InspectDisk(PathBuf),
    SaveHostConfig(HostConfig),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageOpKind {
    AttachDisk,
    MountIso,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DialogKind {
    NewVmWizard(Uuid),
    VmSettings(Uuid),
    HostSettings,
    DeleteVm(Uuid),
}

#[derive(Debug)]
pub enum GuiUpdate {
    DaemonConnected {
        base_url: String,
        host_info: HostInfo,
        host_config: HostConfig,
        switches: Vec<VirtualSwitchInfo>,
        vms: Vec<VmSummary>,
        snapshot_seq: u64,
    },
    DaemonDisconnected(String),
    VmUpdated {
        vm: VmSummary,
        select: bool,
        is_details_fetch: bool,
    },
    VmDeleted {
        id: Uuid,
        revision: u64,
    },
    VmLogs { id: Uuid, content: String },
    DiskInspected(DiskInfo),
    DiskCreated(DiskInfo),
    HostConfigUpdated {
        base_url: String,
        host_info: HostInfo,
        host_config: HostConfig,
    },
    DialogFinished {
        kind: DialogKind,
        error: Option<String>,
    },
    StorageOpFinished {
        vm_id: Uuid,
        op: StorageOpKind,
    },
    StatusMessage(String),
    ErrorMessage(String),
}

pub struct BackendHandle {
    rt: Option<tokio::runtime::Runtime>,
    state: Arc<BackendState>,
    upd_rx: mpsc::Receiver<GuiUpdate>,
}

impl Drop for BackendHandle {
    fn drop(&mut self) {
        self.state.shutdown.store(true, Ordering::Release);
        if let Some(rt) = self.rt.take() {
            rt.shutdown_background();
        }
    }
}

struct BackendState {
    data_dir: Option<PathBuf>,
    daemon_url: Option<String>,
    client: RwLock<Option<ApiClient>>,
    shared_endpoint: SharedEndpoint,
    sse_active: AtomicBool,
    shutdown: AtomicBool,
    connect_in_flight: AtomicBool,
    snapshot_seq: std::sync::atomic::AtomicU64,
    host_cfg_lock: Mutex<()>,
    logs_in_flight: Mutex<HashSet<Uuid>>,
    details_in_flight: Mutex<HashSet<Uuid>>,
    attach_disk_in_flight: Mutex<HashSet<Uuid>>,
    mount_iso_in_flight: Mutex<HashSet<Uuid>>,
    upd_tx: mpsc::Sender<GuiUpdate>,
    repaint_ctx: egui::Context,
}

fn is_idempotent_read(req: &GuiRequest) -> bool {
    matches!(
        req,
        GuiRequest::RefreshAll
            | GuiRequest::FetchVmDetails { .. }
            | GuiRequest::FetchVmLogs { .. }
            | GuiRequest::InspectDisk(_)
    )
}

struct InFlightUuidGuard<'a> {
    set: &'a Mutex<HashSet<Uuid>>,
    id: Uuid,
    notify: Option<(&'a BackendState, StorageOpKind)>,
}

impl Drop for InFlightUuidGuard<'_> {
    fn drop(&mut self) {
        self.set.lock().unwrap().remove(&self.id);
        if let Some((backend, op)) = self.notify {
            backend.emit(GuiUpdate::StorageOpFinished {
                vm_id: self.id,
                op,
            });
        }
    }
}

struct ResetBoolGuard<'a>(&'a AtomicBool);

impl Drop for ResetBoolGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

fn request_dialog_kind(req: &GuiRequest) -> Option<DialogKind> {
    match req {
        GuiRequest::CreateVm { wizard_id, .. } => Some(DialogKind::NewVmWizard(*wizard_id)),
        GuiRequest::UpdateVm { id, .. } => Some(DialogKind::VmSettings(*id)),
        GuiRequest::SaveHostConfig(_) => Some(DialogKind::HostSettings),
        GuiRequest::DeleteVm { id, .. } => Some(DialogKind::DeleteVm(*id)),
        _ => None,
    }
}

impl BackendHandle {
    pub fn spawn(
        data_dir: Option<PathBuf>,
        daemon_url: Option<String>,
        repaint_ctx: egui::Context,
    ) -> Self {
        let (upd_tx, upd_rx) = mpsc::channel::<GuiUpdate>();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(32)
            .thread_name("openvmms-gui-worker")
            .enable_all()
            .build()
            .expect("failed to build GUI backend runtime");

        let state = Arc::new(BackendState {
            data_dir,
            daemon_url,
            client: RwLock::new(None),
            shared_endpoint: Arc::new(RwLock::new(None)),
            sse_active: AtomicBool::new(false),
            shutdown: AtomicBool::new(false),
            connect_in_flight: AtomicBool::new(false),
            snapshot_seq: std::sync::atomic::AtomicU64::new(0),
            host_cfg_lock: Mutex::new(()),
            logs_in_flight: Mutex::new(HashSet::new()),
            details_in_flight: Mutex::new(HashSet::new()),
            attach_disk_in_flight: Mutex::new(HashSet::new()),
            mount_iso_in_flight: Mutex::new(HashSet::new()),
            upd_tx,
            repaint_ctx,
        });

        Self {
            rt: Some(rt),
            state,
            upd_rx,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(repaint_ctx: egui::Context) -> Self {
        let (upd_tx, upd_rx) = mpsc::channel::<GuiUpdate>();
        let state = Arc::new(BackendState {
            data_dir: None,
            daemon_url: None,
            client: RwLock::new(None),
            shared_endpoint: Arc::new(RwLock::new(None)),
            sse_active: AtomicBool::new(false),
            shutdown: AtomicBool::new(false),
            connect_in_flight: AtomicBool::new(false),
            snapshot_seq: std::sync::atomic::AtomicU64::new(0),
            host_cfg_lock: Mutex::new(()),
            logs_in_flight: Mutex::new(HashSet::new()),
            details_in_flight: Mutex::new(HashSet::new()),
            attach_disk_in_flight: Mutex::new(HashSet::new()),
            mount_iso_in_flight: Mutex::new(HashSet::new()),
            upd_tx,
            repaint_ctx,
        });
        Self {
            rt: None,
            state,
            upd_rx,
        }
    }

    pub fn send(&self, req: GuiRequest) {
        if self.state.shutdown.load(Ordering::Acquire) {
            return;
        }
        if let Some(rt) = &self.rt {
            let state = Arc::clone(&self.state);
            rt.spawn_blocking(move || state.process_request(req));
        }
    }

    pub fn try_recv(&self) -> Option<GuiUpdate> {
        self.upd_rx.try_recv().ok()
    }
}

impl BackendState {
    fn emit(&self, msg: GuiUpdate) {
        let _ = self.upd_tx.send(msg);
        self.repaint_ctx.request_repaint();
    }

    fn next_snapshot_seq(&self) -> u64 {
        self.snapshot_seq.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn ensure_sse(self: &Arc<Self>) {
        if self
            .sse_active
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            spawn_sse_listener(Arc::clone(self));
        }
    }

    fn process_request(self: &Arc<Self>, req: GuiRequest) {
        if self.shutdown.load(Ordering::Acquire) {
            return;
        }

        let _dedup_guard = match &req {
            GuiRequest::FetchVmLogs { id } => {
                if !self.logs_in_flight.lock().unwrap().insert(*id) {
                    return;
                }
                Some(InFlightUuidGuard {
                    set: &self.logs_in_flight,
                    id: *id,
                    notify: None,
                })
            }
            GuiRequest::FetchVmDetails { id } => {
                if !self.details_in_flight.lock().unwrap().insert(*id) {
                    return;
                }
                Some(InFlightUuidGuard {
                    set: &self.details_in_flight,
                    id: *id,
                    notify: None,
                })
            }
            GuiRequest::AttachDisk { id, .. } => {
                if !self.attach_disk_in_flight.lock().unwrap().insert(*id) {
                    return;
                }
                Some(InFlightUuidGuard {
                    set: &self.attach_disk_in_flight,
                    id: *id,
                    notify: Some((self.as_ref(), StorageOpKind::AttachDisk)),
                })
            }
            GuiRequest::MountIso { id, .. } => {
                if !self.mount_iso_in_flight.lock().unwrap().insert(*id) {
                    return;
                }
                Some(InFlightUuidGuard {
                    set: &self.mount_iso_in_flight,
                    id: *id,
                    notify: Some((self.as_ref(), StorageOpKind::MountIso)),
                })
            }
            _ => None,
        };

        match req {
            GuiRequest::ConnectOrStartDaemon => {
                if self
                    .connect_in_flight
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    return;
                }
                let _connect_guard = ResetBoolGuard(&self.connect_in_flight);
                match ensure_daemon_running(self.data_dir.as_deref(), self.daemon_url.as_deref()) {
                    Ok(c) => {
                        set_shared_endpoint(&self.shared_endpoint, &c);
                        self.ensure_sse();
                        let snapshot_seq = self.next_snapshot_seq();
                        match fetch_full_snapshot(&c) {
                            Ok((host_info, host_config, switches, vms)) => {
                                let base_url = c.base_url().to_string();
                                *self.client.write().unwrap() = Some(c);
                                self.emit(GuiUpdate::DaemonConnected {
                                    base_url,
                                    host_info,
                                    host_config,
                                    switches,
                                    vms,
                                    snapshot_seq,
                                });
                            }
                            Err(err) => {
                                self.emit(GuiUpdate::DaemonDisconnected(format!("{err:#}")));
                            }
                        }
                    }
                    Err(err) => {
                        self.emit(GuiUpdate::DaemonDisconnected(format!("{err:#}")));
                    }
                }
            }
            other => {
                let dialog_kind = request_dialog_kind(&other);
                let mut current_client = self.client.read().unwrap().clone();
                if current_client.is_none()
                    && let Some(reloaded) = reload_client_from_runtime_addr(
                        self.data_dir.as_deref(),
                        self.daemon_url.as_deref(),
                        &self.shared_endpoint,
                    )
                {
                    set_shared_endpoint(&self.shared_endpoint, &reloaded);
                    self.ensure_sse();
                    if !matches!(other, GuiRequest::RefreshAll) {
                        let snapshot_seq = self.next_snapshot_seq();
                        if let Ok((host_info, host_config, switches, vms)) =
                            fetch_full_snapshot(&reloaded)
                        {
                            self.emit(GuiUpdate::DaemonConnected {
                                base_url: reloaded.base_url().to_string(),
                                host_info,
                                host_config,
                                switches,
                                vms,
                                snapshot_seq,
                            });
                        }
                    }
                    *self.client.write().unwrap() = Some(reloaded.clone());
                    current_client = Some(reloaded);
                }

                let Some(mut c) = current_client else {
                    let err_msg = "Not connected to openvmmsd".to_string();
                    self.emit(GuiUpdate::ErrorMessage(err_msg.clone()));
                    if let Some(kind) = dialog_kind {
                        self.emit(GuiUpdate::DialogFinished {
                            kind,
                            error: Some(err_msg),
                        });
                    }
                    return;
                };

                self.ensure_sse();
                if sync_client_before_request(
                    &mut c,
                    self.data_dir.as_deref(),
                    self.daemon_url.as_deref(),
                    &self.shared_endpoint,
                ) {
                    *self.client.write().unwrap() = Some(c.clone());
                }

                match self.execute_client_request(&mut c, other.clone()) {
                    Ok(()) => {
                        if let Some(kind) = dialog_kind {
                            self.emit(GuiUpdate::DialogFinished { kind, error: None });
                        }
                    }
                    Err(err) => {
                        let reloaded = reload_client_from_runtime_addr(
                            self.data_dir.as_deref(),
                            self.daemon_url.as_deref(),
                            &self.shared_endpoint,
                        );
                        if let Some(reloaded) = reloaded
                            && (reloaded.base_url() != c.base_url()
                                || reloaded.token() != c.token())
                        {
                            c = reloaded;
                            set_shared_endpoint(&self.shared_endpoint, &c);
                            *self.client.write().unwrap() = Some(c.clone());
                            self.ensure_sse();
                            if is_idempotent_read(&other)
                                || openvmms_core::api_client::is_pre_request_connect_error(&err)
                            {
                                match self.execute_client_request(&mut c, other) {
                                    Ok(()) => {
                                        if let Some(kind) = dialog_kind {
                                            self.emit(GuiUpdate::DialogFinished {
                                                kind,
                                                error: None,
                                            });
                                        }
                                    }
                                    Err(retry_err) => {
                                        let err_msg = format!("{retry_err:#}");
                                        self.emit(GuiUpdate::ErrorMessage(err_msg.clone()));
                                        if let Some(kind) = dialog_kind {
                                            self.emit(GuiUpdate::DialogFinished {
                                                kind,
                                                error: Some(err_msg),
                                            });
                                        }
                                    }
                                }
                            } else {
                                let snapshot_seq = self.next_snapshot_seq();
                                if let Ok((host_info, host_config, switches, vms)) =
                                    fetch_full_snapshot(&c)
                                {
                                    self.emit(GuiUpdate::DaemonConnected {
                                        base_url: c.base_url().to_string(),
                                        host_info,
                                        host_config,
                                        switches,
                                        vms,
                                        snapshot_seq,
                                    });
                                }
                                let err_msg = format!(
                                    "{err:#} (not retried after daemon endpoint changed)"
                                );
                                self.emit(GuiUpdate::ErrorMessage(err_msg.clone()));
                                if let Some(kind) = dialog_kind {
                                    self.emit(GuiUpdate::DialogFinished {
                                        kind,
                                        error: Some(err_msg),
                                    });
                                }
                            }
                        } else if c.health().is_err() {
                            let mut guard = self.client.write().unwrap();
                            if guard
                                .as_ref()
                                .is_some_and(|cur| cur.base_url() == c.base_url())
                            {
                                *guard = None;
                            }
                            drop(guard);
                            let err_msg = format!("Lost connection to openvmmsd: {err:#}");
                            self.emit(GuiUpdate::DaemonDisconnected(err_msg.clone()));
                            if let Some(kind) = dialog_kind {
                                self.emit(GuiUpdate::DialogFinished {
                                    kind,
                                    error: Some(err_msg),
                                });
                            }
                        } else {
                            let err_msg = format!("{err:#}");
                            self.emit(GuiUpdate::ErrorMessage(err_msg.clone()));
                            if let Some(kind) = dialog_kind {
                                self.emit(GuiUpdate::DialogFinished {
                                    kind,
                                    error: Some(err_msg),
                                });
                            }
                        }
                    }
                }
            }
        }
    }
}

fn set_shared_endpoint(shared_endpoint: &SharedEndpoint, client: &ApiClient) {
    *shared_endpoint.write().unwrap() =
        Some((client.base_url().to_string(), client.token().to_string()));
}

fn reload_client_from_runtime_addr(
    data_dir: Option<&Path>,
    daemon_url: Option<&str>,
    shared_endpoint: &SharedEndpoint,
) -> Option<ApiClient> {
    let current = shared_endpoint.read().unwrap().clone();
    if let Ok(Some(c)) = ApiClient::from_runtime_addr(data_dir) {
        let token_matches = current
            .as_ref()
            .is_none_or(|(_, cur_token)| c.token() == cur_token);
        if (daemon_url.is_none() || token_matches) && c.health().is_ok() {
            set_shared_endpoint(shared_endpoint, &c);
            return Some(c);
        }
    }
    if let Some((base_url, token)) = current
        && let Ok(c) = ApiClient::new(base_url, token)
        && c.health().is_ok()
    {
        return Some(c);
    }
    None
}

fn sync_client_before_request(
    client: &mut ApiClient,
    data_dir: Option<&Path>,
    daemon_url: Option<&str>,
    shared_endpoint: &SharedEndpoint,
) -> bool {
    if let Some((shared_url, shared_token)) = shared_endpoint.read().unwrap().clone()
        && (shared_url != client.base_url() || shared_token != client.token())
        && let Ok(updated) = ApiClient::new(shared_url, shared_token)
        && updated.health().is_ok()
    {
        *client = updated;
        return true;
    }

    if let Ok(Some(runtime_client)) = ApiClient::from_runtime_addr(data_dir) {
        let token_matches = runtime_client.token() == client.token();
        let changed = runtime_client.base_url() != client.base_url()
            || runtime_client.token() != client.token();
        if changed && (daemon_url.is_none() || token_matches) && runtime_client.health().is_ok() {
            set_shared_endpoint(shared_endpoint, &runtime_client);
            *client = runtime_client;
            return true;
        }
    }
    false
}

pub fn fetch_full_snapshot(
    client: &ApiClient,
) -> anyhow::Result<(HostInfo, HostConfig, Vec<VirtualSwitchInfo>, Vec<VmSummary>)> {
    let host_info = client.host_info()?;
    let host_config = client.host_config()?;
    let switches = client.list_switches()?;
    let vms = client.list_vms()?;
    Ok((host_info, host_config, switches, vms))
}

impl BackendState {
    fn execute_client_request(
        &self,
        client: &mut ApiClient,
        req: GuiRequest,
    ) -> anyhow::Result<()> {
        match req {
            GuiRequest::ConnectOrStartDaemon => Ok(()),
            GuiRequest::RefreshAll => {
                let snapshot_seq = self.next_snapshot_seq();
                let (host_info, host_config, switches, vms) = fetch_full_snapshot(client)?;
                self.emit(GuiUpdate::DaemonConnected {
                    base_url: client.base_url().to_string(),
                    host_info,
                    host_config,
                    switches,
                    vms,
                    snapshot_seq,
                });
                Ok(())
            }
            GuiRequest::FetchVmDetails { id } => {
                let vm = client.get_vm(&id.to_string())?;
                self.emit(GuiUpdate::VmUpdated {
                    vm,
                    select: false,
                    is_details_fetch: true,
                });
                Ok(())
            }
            GuiRequest::FetchVmLogs { id } => {
                let content = client.vm_logs(&id.to_string(), 250)?;
                self.emit(GuiUpdate::VmLogs { id, content });
                Ok(())
            }
            GuiRequest::CreateVm { payload, .. } => {
                let vm = client.create_vm(&payload)?;
                self.emit(GuiUpdate::StatusMessage(format!(
                    "Created VM '{}'",
                    vm.config.name
                )));
                self.emit(GuiUpdate::VmUpdated {
                    vm,
                    select: true,
                    is_details_fetch: false,
                });
                Ok(())
            }
            GuiRequest::UpdateVm { id, payload } => {
                let vm = client.update_vm(&id.to_string(), &payload)?;
                self.emit(GuiUpdate::StatusMessage(format!(
                    "Updated settings for '{}'",
                    vm.config.name
                )));
                self.emit(GuiUpdate::VmUpdated {
                    vm,
                    select: false,
                    is_details_fetch: false,
                });
                Ok(())
            }
            GuiRequest::DeleteVm { id, delete_disks } => {
                client.delete_vm(&id.to_string(), delete_disks)?;
                self.emit(GuiUpdate::StatusMessage("Deleted virtual machine".to_string()));
                self.emit(GuiUpdate::VmDeleted { id, revision: 0 });
                Ok(())
            }
            GuiRequest::StartVm { id } => {
                let vm = client.start_vm(&id.to_string())?;
                self.emit(GuiUpdate::StatusMessage(format!(
                    "Started '{}'",
                    vm.config.name
                )));
                self.emit(GuiUpdate::VmUpdated {
                    vm,
                    select: false,
                    is_details_fetch: false,
                });
                Ok(())
            }
            GuiRequest::StopVm { id } => {
                let vm = client.stop_vm(&id.to_string())?;
                self.emit(GuiUpdate::StatusMessage(format!(
                    "Turned off '{}'",
                    vm.config.name
                )));
                self.emit(GuiUpdate::VmUpdated {
                    vm,
                    select: false,
                    is_details_fetch: false,
                });
                Ok(())
            }
            GuiRequest::ShutdownVm { id, acpi, force } => {
                let vm =
                    client.shutdown_vm(&id.to_string(), &ShutdownVmPayload { force, acpi })?;
                self.emit(GuiUpdate::StatusMessage(format!(
                    "Sent shutdown signal to '{}'",
                    vm.config.name
                )));
                self.emit(GuiUpdate::VmUpdated {
                    vm,
                    select: false,
                    is_details_fetch: false,
                });
                Ok(())
            }
            GuiRequest::PauseVm { id } => {
                let vm = client.pause_vm(&id.to_string())?;
                self.emit(GuiUpdate::StatusMessage(format!(
                    "Paused '{}'",
                    vm.config.name
                )));
                self.emit(GuiUpdate::VmUpdated {
                    vm,
                    select: false,
                    is_details_fetch: false,
                });
                Ok(())
            }
            GuiRequest::ResumeVm { id } => {
                let vm = client.resume_vm(&id.to_string())?;
                self.emit(GuiUpdate::StatusMessage(format!(
                    "Resumed '{}'",
                    vm.config.name
                )));
                self.emit(GuiUpdate::VmUpdated {
                    vm,
                    select: false,
                    is_details_fetch: false,
                });
                Ok(())
            }
            GuiRequest::ResetVm { id } => {
                let vm = client.reset_vm(&id.to_string())?;
                self.emit(GuiUpdate::StatusMessage(format!(
                    "Reset '{}'",
                    vm.config.name
                )));
                self.emit(GuiUpdate::VmUpdated {
                    vm,
                    select: false,
                    is_details_fetch: false,
                });
                Ok(())
            }
            GuiRequest::ConnectViewer { id, no_shm } => {
                let vm = client.get_vm(&id.to_string())?;
                let endpoint = vm
                    .runtime
                    .fastdesk_endpoint
                    .as_deref()
                    .context("VM does not have an active FastDesk endpoint")?;
                let viewer_bin = resolve_local_viewer_binary(self.data_dir.as_deref())?;
                let mut child =
                    launch_fastdesk_viewer(&viewer_bin, endpoint, &vm.config.name, no_shm)?;
                let _ = thread::Builder::new()
                    .name("fastdesk-viewer-reaper".into())
                    .spawn(move || {
                        let _ = child.wait();
                    });
                self.emit(GuiUpdate::StatusMessage(format!(
                    "Launched FastDesk viewer for '{}' ({endpoint})",
                    vm.config.name
                )));
                Ok(())
            }
            GuiRequest::AttachDisk { id, payload } => {
                let vm = client.attach_disk(&id.to_string(), &payload)?;
                self.emit(GuiUpdate::StatusMessage("Attached disk".to_string()));
                self.emit(GuiUpdate::VmUpdated {
                    vm,
                    select: false,
                    is_details_fetch: false,
                });
                Ok(())
            }
            GuiRequest::DetachDisk { id, lun } => {
                let vm = client.detach_disk(&id.to_string(), lun)?;
                self.emit(GuiUpdate::StatusMessage(format!(
                    "Detached SCSI LUN {lun}"
                )));
                self.emit(GuiUpdate::VmUpdated {
                    vm,
                    select: false,
                    is_details_fetch: false,
                });
                Ok(())
            }
            GuiRequest::MountIso { id, payload } => {
                let vm = client.mount_iso(&id.to_string(), &payload)?;
                self.emit(GuiUpdate::StatusMessage("Mounted ISO image".to_string()));
                self.emit(GuiUpdate::VmUpdated {
                    vm,
                    select: false,
                    is_details_fetch: false,
                });
                Ok(())
            }
            GuiRequest::EjectIso { id, lun } => {
                let vm = client.eject_iso(&id.to_string(), lun)?;
                self.emit(GuiUpdate::StatusMessage("Ejected ISO image".to_string()));
                self.emit(GuiUpdate::VmUpdated {
                    vm,
                    select: false,
                    is_details_fetch: false,
                });
                Ok(())
            }
            GuiRequest::AddPortForward { id, nic_id, port } => {
                let vm = client.add_port_forward(&id.to_string(), nic_id, port)?;
                self.emit(GuiUpdate::StatusMessage("Added port forward".to_string()));
                self.emit(GuiUpdate::VmUpdated {
                    vm,
                    select: false,
                    is_details_fetch: false,
                });
                Ok(())
            }
            GuiRequest::RemovePortForward { id, nic_id, port } => {
                let vm = client.remove_port_forward(&id.to_string(), nic_id, port)?;
                self.emit(GuiUpdate::StatusMessage("Removed port forward".to_string()));
                self.emit(GuiUpdate::VmUpdated {
                    vm,
                    select: false,
                    is_details_fetch: false,
                });
                Ok(())
            }
            GuiRequest::CreateDisk(payload) => {
                let info = client.create_disk(&payload)?;
                self.emit(GuiUpdate::StatusMessage(format!(
                    "Created disk {}",
                    info.path.display()
                )));
                self.emit(GuiUpdate::DiskCreated(info));
                Ok(())
            }
            GuiRequest::InspectDisk(path) => {
                let info = client.inspect_disk(&path)?;
                self.emit(GuiUpdate::DiskInspected(info));
                Ok(())
            }
            GuiRequest::SaveHostConfig(cfg) => {
                let _guard = self.host_cfg_lock.lock().unwrap();
                let updated = client.update_host_config(&cfg)?;
                // Follow only `daemon.addr` when the daemon rebinds; never connect to `updated.listen_addr`.
                if let Some(next_client) = reload_client_from_runtime_addr(
                    self.data_dir.as_deref(),
                    self.daemon_url.as_deref(),
                    &self.shared_endpoint,
                ) && next_client.base_url() != client.base_url()
                {
                    *client = next_client;
                    *self.client.write().unwrap() = Some(client.clone());
                }
                let host_info = client.host_info()?;
                let base_url = client.base_url().to_string();
                self.emit(GuiUpdate::StatusMessage("Saved host settings".to_string()));
                self.emit(GuiUpdate::HostConfigUpdated {
                    base_url,
                    host_info,
                    host_config: updated,
                });
                Ok(())
            }
        }
    }
}

fn resolve_updated_client(
    current_base_url: &str,
    current_token: &str,
    data_dir: Option<&Path>,
    daemon_url: Option<&str>,
    shared_endpoint: &SharedEndpoint,
) -> Option<ApiClient> {
    if let Some(c) = reload_client_from_runtime_addr(data_dir, daemon_url, shared_endpoint) {
        return Some(c);
    }
    ApiClient::new(current_base_url, current_token).ok()
}

fn maybe_switch_endpoint_on_disconnect(
    current_base_url: &str,
    data_dir: Option<&Path>,
    daemon_url: Option<&str>,
    shared_endpoint: &SharedEndpoint,
    snapshot_seq: &std::sync::atomic::AtomicU64,
    upd_tx: &mpsc::Sender<GuiUpdate>,
    repaint_ctx: &egui::Context,
) -> bool {
    if let Some(c) = reload_client_from_runtime_addr(data_dir, daemon_url, shared_endpoint)
        && c.base_url() != current_base_url
    {
        let seq = snapshot_seq.fetch_add(1, Ordering::Relaxed) + 1;
        if let Ok((host_info, host_config, switches, vms)) = fetch_full_snapshot(&c) {
            let _ = upd_tx.send(GuiUpdate::DaemonConnected {
                base_url: c.base_url().to_string(),
                host_info,
                host_config,
                switches,
                vms,
                snapshot_seq: seq,
            });
            repaint_ctx.request_repaint();
        }
        return true;
    }
    false
}

fn spawn_sse_listener(state: Arc<BackendState>) {
    thread::spawn(move || {
        let http = match reqwest::blocking::Client::builder()
            .no_proxy()
            .timeout(None)
            .build()
        {
            Ok(c) => c,
            Err(_) => {
                state.sse_active.store(false, Ordering::SeqCst);
                return;
            }
        };
        let mut was_connected = true;
        while !state.shutdown.load(Ordering::Acquire) {
            let endpoint = state.shared_endpoint.read().unwrap().clone().or_else(|| {
                reload_client_from_runtime_addr(
                    state.data_dir.as_deref(),
                    state.daemon_url.as_deref(),
                    &state.shared_endpoint,
                )
                .map(|c| (c.base_url().to_string(), c.token().to_string()))
            });
            let Some((mut base_url, mut token)) = endpoint else {
                thread::sleep(Duration::from_millis(500));
                continue;
            };
            // Verify health authentication challenge before transmitting token on the SSE request.
            if !ApiClient::new(&base_url, &token).is_ok_and(|c| c.health().is_ok()) {
                if let Some(reloaded) = reload_client_from_runtime_addr(
                    state.data_dir.as_deref(),
                    state.daemon_url.as_deref(),
                    &state.shared_endpoint,
                ) {
                    base_url = reloaded.base_url().to_string();
                    token = reloaded.token().to_string();
                } else {
                    if was_connected {
                        was_connected = false;
                        if state
                            .upd_tx
                            .send(GuiUpdate::DaemonDisconnected(
                                "Lost connection or failed daemon verification".to_string(),
                            ))
                            .is_err()
                        {
                            break;
                        }
                        state.repaint_ctx.request_repaint();
                    }
                    thread::sleep(Duration::from_millis(500));
                    continue;
                }
            }
            let url = format!("{base_url}/api/v1/events");
            let mut switched_endpoint = false;
            let mut saw_daemon_stopping = false;
            match http.get(&url).bearer_auth(&token).send() {
                Ok(resp) if resp.status().is_success() => {
                    if !was_connected {
                        was_connected = true;
                        let snapshot_seq = state.next_snapshot_seq();
                        if let Ok(c) = ApiClient::new(&base_url, &token)
                            && let Ok((host_info, host_config, switches, vms)) =
                                fetch_full_snapshot(&c)
                        {
                            *state.client.write().unwrap() = Some(c);
                            if state
                                .upd_tx
                                .send(GuiUpdate::DaemonConnected {
                                    base_url: base_url.clone(),
                                    host_info,
                                    host_config,
                                    switches,
                                    vms,
                                    snapshot_seq,
                                })
                                .is_err()
                            {
                                break;
                            }
                            state.repaint_ctx.request_repaint();
                        }
                    }
                    let reader = std::io::BufReader::new(resp);
                    for line in reader.lines().map_while(Result::ok) {
                        if state.shutdown.load(Ordering::Acquire) {
                            break;
                        }
                        if let Some(json_str) = line.strip_prefix("data: ")
                            && let Ok(event) = serde_json::from_str::<DaemonEvent>(json_str)
                        {
                            match event {
                                DaemonEvent::VmUpdated { vm } => {
                                    if state
                                        .upd_tx
                                        .send(GuiUpdate::VmUpdated {
                                            vm,
                                            select: false,
                                            is_details_fetch: false,
                                        })
                                        .is_err()
                                    {
                                        break;
                                    }
                                    state.repaint_ctx.request_repaint();
                                }
                                DaemonEvent::VmDeleted { id, revision } => {
                                    if state
                                        .upd_tx
                                        .send(GuiUpdate::VmDeleted { id, revision })
                                        .is_err()
                                    {
                                        break;
                                    }
                                    state.repaint_ctx.request_repaint();
                                }
                                DaemonEvent::HostConfigUpdated => {
                                    if let Some(c) = resolve_updated_client(
                                        &base_url,
                                        &token,
                                        state.data_dir.as_deref(),
                                        state.daemon_url.as_deref(),
                                        &state.shared_endpoint,
                                    ) {
                                        let snapshot_seq = state.next_snapshot_seq();
                                        if let Ok((host_info, host_config, switches, vms)) =
                                            fetch_full_snapshot(&c)
                                        {
                                            let new_base = c.base_url().to_string();
                                            *state.client.write().unwrap() = Some(c);
                                            if state
                                                .upd_tx
                                                .send(GuiUpdate::DaemonConnected {
                                                    base_url: new_base.clone(),
                                                    host_info,
                                                    host_config,
                                                    switches,
                                                    vms,
                                                    snapshot_seq,
                                                })
                                                .is_err()
                                            {
                                                break;
                                            }
                                            state.repaint_ctx.request_repaint();
                                            if new_base != base_url {
                                                switched_endpoint = true;
                                                break;
                                            }
                                        }
                                    }
                                }
                                DaemonEvent::DaemonStopping => {
                                    was_connected = false;
                                    saw_daemon_stopping = true;
                                    *state.client.write().unwrap() = None;
                                    if state
                                        .upd_tx
                                        .send(GuiUpdate::DaemonDisconnected(
                                            "Daemon stopped".to_string(),
                                        ))
                                        .is_err()
                                    {
                                        break;
                                    }
                                    state.repaint_ctx.request_repaint();
                                    break;
                                }
                            }
                        }
                    }
                    if !state.shutdown.load(Ordering::Acquire) && !switched_endpoint {
                        if saw_daemon_stopping {
                            thread::sleep(Duration::from_millis(500));
                            continue;
                        }
                        if maybe_switch_endpoint_on_disconnect(
                            &base_url,
                            state.data_dir.as_deref(),
                            state.daemon_url.as_deref(),
                            &state.shared_endpoint,
                            &state.snapshot_seq,
                            &state.upd_tx,
                            &state.repaint_ctx,
                        ) {
                            was_connected = true;
                            continue;
                        }
                        let healthy =
                            ApiClient::new(&base_url, &token).is_ok_and(|c| c.health().is_ok());
                        if !healthy && was_connected {
                            was_connected = false;
                            if state
                                .upd_tx
                                .send(GuiUpdate::DaemonDisconnected(
                                    "Lost connection to openvmmsd".to_string(),
                                ))
                                .is_err()
                            {
                                break;
                            }
                            state.repaint_ctx.request_repaint();
                        }
                        thread::sleep(Duration::from_millis(500));
                    }
                }
                _ => {
                    if maybe_switch_endpoint_on_disconnect(
                        &base_url,
                        state.data_dir.as_deref(),
                        state.daemon_url.as_deref(),
                        &state.shared_endpoint,
                        &state.snapshot_seq,
                        &state.upd_tx,
                        &state.repaint_ctx,
                    ) {
                        was_connected = true;
                        continue;
                    }
                    let healthy =
                        ApiClient::new(&base_url, &token).is_ok_and(|c| c.health().is_ok());
                    if !healthy && was_connected {
                        was_connected = false;
                        if state
                            .upd_tx
                            .send(GuiUpdate::DaemonDisconnected(
                                "Lost connection to openvmmsd".to_string(),
                            ))
                            .is_err()
                        {
                            break;
                        }
                        state.repaint_ctx.request_repaint();
                    }
                    thread::sleep(Duration::from_millis(500));
                }
            }
        }
        state.sse_active.store(false, Ordering::SeqCst);
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use openvmms_core::api_client::normalize_base_url;
    use openvmms_core::models::VmPreset;
    use openvmms_core::models::VmStatus;

    fn recv_until<F>(handle: &BackendHandle, timeout: Duration, mut pred: F) -> GuiUpdate
    where
        F: FnMut(&GuiUpdate) -> bool,
    {
        let deadline = std::time::Instant::now() + timeout;
        let mut seen = Vec::new();
        loop {
            while let Some(upd) = handle.try_recv() {
                if let GuiUpdate::ErrorMessage(err) = &upd {
                    panic!("unexpected GuiUpdate::ErrorMessage: {err}");
                }
                if pred(&upd) {
                    return upd;
                }
                seen.push(format!("{upd:?}"));
            }
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for expected GuiUpdate after {timeout:?} (seen: {seen:?})"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn backend_handle_follows_external_daemon_port_change_for_requests_and_events() {
        let dir = tempfile::tempdir().unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let daemon = rt
            .block_on(openvmmsd::TestDaemonHandle::spawn(dir.path().to_path_buf()))
            .unwrap();
        let old_base_url = daemon.base_url.clone();

        let backend = BackendHandle::spawn(
            Some(dir.path().to_path_buf()),
            None,
            egui::Context::default(),
        );
        backend.send(GuiRequest::ConnectOrStartDaemon);

        match recv_until(&backend, Duration::from_secs(5), |u| {
            matches!(u, GuiUpdate::DaemonConnected { .. })
        }) {
            GuiUpdate::DaemonConnected { base_url, .. } => {
                assert_eq!(base_url, old_base_url);
            }
            _ => unreachable!(),
        }

        // Another client updates listen_addr to a fresh ephemeral port, closing the old listener.
        let external_client = ApiClient::new(&old_base_url, &daemon.token).unwrap();
        let mut cfg = external_client.host_config().unwrap();
        cfg.listen_addr = "127.0.0.1:0".to_string();
        let updated_cfg = external_client.update_host_config(&cfg).unwrap();
        let new_base_url = normalize_base_url(&updated_cfg.listen_addr);
        assert_ne!(new_base_url, old_base_url);

        // Wait until the GUI's SSE listener emits the reconnected snapshot on the new URL.
        match recv_until(&backend, Duration::from_secs(5), |u| {
            matches!(
                u,
                GuiUpdate::DaemonConnected { base_url, .. } if base_url == &new_base_url
            )
        }) {
            GuiUpdate::DaemonConnected { base_url, .. } => {
                assert_eq!(base_url, new_base_url);
            }
            _ => unreachable!(),
        }

        // Ensure the old listener has completely stopped accepting connections before testing requests.
        let old_addr: std::net::SocketAddr = old_base_url
            .trim_start_matches("http://")
            .parse()
            .unwrap();
        let close_deadline = std::time::Instant::now() + Duration::from_secs(3);
        while std::net::TcpStream::connect(old_addr).is_ok() {
            assert!(
                std::time::Instant::now() < close_deadline,
                "old listener {old_addr} stayed open"
            );
            thread::sleep(Duration::from_millis(20));
        }

        // GUI RefreshAll request must succeed using the new endpoint.
        backend.send(GuiRequest::RefreshAll);
        match recv_until(&backend, Duration::from_secs(5), |u| {
            matches!(u, GuiUpdate::DaemonConnected { .. })
        }) {
            GuiUpdate::DaemonConnected { base_url, .. } => {
                assert_eq!(base_url, new_base_url);
            }
            _ => unreachable!(),
        }

        // An external client creating a VM on the new port must arrive over the reconnected SSE stream.
        let new_external = ApiClient::new(&new_base_url, &daemon.token).unwrap();
        let created = new_external
            .create_vm(&CreateVmPayload {
                name: "PortSync-SSE-VM".into(),
                description: String::new(),
                preset: VmPreset::Custom,
                vcpus: Some(2),
                memory_mb: Some(512),
                nested_virt: None,
                uefi_firmware: None,
                secure_boot_enabled: None,
                secure_boot_template: None,
                tpm_enabled: None,
                fastdesk: None,
                disks: vec![],
                create_disk: None,
                iso_path: None,
                nics: None,
            })
            .unwrap();

        match recv_until(&backend, Duration::from_secs(5), |u| {
            matches!(u, GuiUpdate::VmUpdated { vm, .. } if vm.config.id == created.config.id)
        }) {
            GuiUpdate::VmUpdated { vm, .. } => {
                assert_eq!(vm.config.name, "PortSync-SSE-VM");
            }
            _ => unreachable!(),
        }

        // GUI mutation request after port change must also succeed on the new endpoint.
        backend.send(GuiRequest::UpdateVm {
            id: created.config.id,
            payload: UpdateVmPayload {
                description: Some("updated via GUI after port change".into()),
                ..Default::default()
            },
        });
        match recv_until(&backend, Duration::from_secs(5), |u| {
            matches!(
                u,
                GuiUpdate::VmUpdated { vm, .. }
                    if vm.config.id == created.config.id
                        && vm.config.description == "updated via GUI after port change"
            )
        }) {
            GuiUpdate::VmUpdated { vm, .. } => {
                assert_eq!(vm.config.description, "updated via GUI after port change");
            }
            _ => unreachable!(),
        }

        rt.block_on(daemon.shutdown());
    }

    #[test]
    fn backend_handle_recovers_requests_and_sse_after_external_daemon_restart() {
        let dir = tempfile::tempdir().unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let daemon1 = rt
            .block_on(openvmmsd::TestDaemonHandle::spawn(dir.path().to_path_buf()))
            .unwrap();
        let url1 = daemon1.base_url.clone();

        let backend = BackendHandle::spawn(
            Some(dir.path().to_path_buf()),
            None,
            egui::Context::default(),
        );
        backend.send(GuiRequest::ConnectOrStartDaemon);

        match recv_until(&backend, Duration::from_secs(5), |u| {
            matches!(u, GuiUpdate::DaemonConnected { .. })
        }) {
            GuiUpdate::DaemonConnected { base_url, .. } => assert_eq!(base_url, url1),
            _ => unreachable!(),
        }

        // Daemon stops; GUI must report DaemonDisconnected and wait for restart.
        rt.block_on(daemon1.shutdown());
        match recv_until(&backend, Duration::from_secs(5), |u| {
            matches!(u, GuiUpdate::DaemonDisconnected(_))
        }) {
            GuiUpdate::DaemonDisconnected(_) => {}
            _ => unreachable!(),
        }

        // External daemon starts on the same data_dir with a fresh ephemeral port.
        let daemon2 = rt
            .block_on(openvmmsd::TestDaemonHandle::spawn(dir.path().to_path_buf()))
            .unwrap();
        let url2 = daemon2.base_url.clone();
        assert_ne!(url2, url1);

        match recv_until(&backend, Duration::from_secs(6), |u| {
            matches!(
                u,
                GuiUpdate::DaemonConnected { base_url, .. } if base_url == &url2
            )
        }) {
            GuiUpdate::DaemonConnected { base_url, .. } => assert_eq!(base_url, url2),
            _ => unreachable!(),
        }

        // Requests sent through BackendHandle after external restart must succeed (never "Not connected").
        let wizard_id = Uuid::new_v4();
        backend.send(GuiRequest::CreateVm {
            wizard_id,
            payload: CreateVmPayload {
                name: "Gui-After-Restart".into(),
                preset: VmPreset::Custom,
                vcpus: Some(2),
                memory_mb: Some(512),
                ..Default::default()
            },
        });
        match recv_until(&backend, Duration::from_secs(5), |u| {
            matches!(
                u,
                GuiUpdate::DialogFinished {
                    kind: DialogKind::NewVmWizard(wid),
                    error: None
                } if *wid == wizard_id
            )
        }) {
            GuiUpdate::DialogFinished { error, .. } => assert!(error.is_none()),
            _ => unreachable!(),
        }

        // External mutations on daemon2 must also stream over the revived SSE connection.
        let ext2 = ApiClient::new(&url2, &daemon2.token).unwrap();
        let ext_vm = ext2
            .create_vm(&CreateVmPayload {
                name: "External-After-Restart".into(),
                preset: VmPreset::Custom,
                vcpus: Some(2),
                memory_mb: Some(512),
                ..Default::default()
            })
            .unwrap();
        match recv_until(&backend, Duration::from_secs(5), |u| {
            matches!(u, GuiUpdate::VmUpdated { vm, .. } if vm.config.id == ext_vm.config.id)
        }) {
            GuiUpdate::VmUpdated { vm, .. } => assert_eq!(vm.config.name, "External-After-Restart"),
            _ => unreachable!(),
        }

        rt.block_on(daemon2.shutdown());
    }

    #[test]
    fn rejected_create_vm_emits_dialog_finished_with_error() {
        let dir = tempfile::tempdir().unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let daemon = rt
            .block_on(openvmmsd::TestDaemonHandle::spawn(dir.path().to_path_buf()))
            .unwrap();

        let backend = BackendHandle::spawn(
            Some(dir.path().to_path_buf()),
            None,
            egui::Context::default(),
        );
        backend.send(GuiRequest::ConnectOrStartDaemon);
        let _ = recv_until(&backend, Duration::from_secs(5), |u| {
            matches!(u, GuiUpdate::DaemonConnected { .. })
        });

        let wizard_id = Uuid::new_v4();
        backend.send(GuiRequest::CreateVm {
            wizard_id,
            payload: CreateVmPayload {
                name: "Invalid-Vcpu-VM".into(),
                preset: VmPreset::Custom,
                vcpus: Some(0),
                memory_mb: Some(512),
                ..Default::default()
            },
        });

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut saw_dialog_err = false;
        while std::time::Instant::now() < deadline {
            if let Some(upd) = backend.try_recv()
                && let GuiUpdate::DialogFinished {
                    kind: DialogKind::NewVmWizard(wid),
                    error: Some(err),
                } = upd
                && wid == wizard_id
            {
                assert!(err.contains("vcpus"), "{err}");
                saw_dialog_err = true;
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }
        assert!(saw_dialog_err, "expected DialogFinished with error");

        rt.block_on(daemon.shutdown());
    }

    #[test]
    fn save_host_config_on_ephemeral_port_stays_on_daemon_addr_and_never_contacts_configured_listen_addr() {
        let dir = tempfile::tempdir().unwrap();
        let squatter = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let squatter_addr = squatter.local_addr().unwrap();
        let squatter_hit = Arc::new(AtomicBool::new(false));
        let squatter_hit_srv = Arc::clone(&squatter_hit);
        let squatter_stop = Arc::new(AtomicBool::new(false));
        let squatter_stop_srv = Arc::clone(&squatter_stop);
        let _ = squatter.set_nonblocking(true);

        let squatter_thread = thread::spawn(move || {
            while !squatter_stop_srv.load(Ordering::Acquire) {
                match squatter.accept() {
                    Ok((_stream, _)) => {
                        squatter_hit_srv.store(true, Ordering::SeqCst);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(20));
                    }
                    Err(_) => break,
                }
            }
        });

        // Pre-seed config.toml with the occupied squatter address before starting the ephemeral daemon.
        let store = openvmms_core::store::RegistryStore::new(dir.path()).unwrap();
        let mut init_cfg = store.load_or_init_host_config().unwrap();
        init_cfg.listen_addr = squatter_addr.to_string();
        store.save_host_config(&init_cfg).unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        let daemon = rt
            .block_on(openvmmsd::TestDaemonHandle::spawn(dir.path().to_path_buf()))
            .unwrap();
        let daemon_url = daemon.base_url.clone();
        assert_ne!(daemon_url, format!("http://{squatter_addr}"));

        let backend = BackendHandle::spawn(
            Some(dir.path().to_path_buf()),
            None,
            egui::Context::default(),
        );
        backend.send(GuiRequest::ConnectOrStartDaemon);

        let host_cfg = match recv_until(&backend, Duration::from_secs(5), |u| {
            matches!(u, GuiUpdate::DaemonConnected { .. })
        }) {
            GuiUpdate::DaemonConnected {
                base_url,
                host_config,
                ..
            } => {
                assert_eq!(base_url, daemon_url);
                host_config
            }
            _ => unreachable!(),
        };

        // Save Host Settings without altering `listen_addr` (still the squatter address).
        let mut edited = host_cfg;
        edited.openvmm_bin = Some(dir.path().join("custom-openvmm"));
        backend.send(GuiRequest::SaveHostConfig(edited));

        let mut saw_updated = false;
        let mut saw_dialog_ok = false;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while std::time::Instant::now() < deadline && (!saw_updated || !saw_dialog_ok) {
            while let Some(upd) = backend.try_recv() {
                match upd {
                    GuiUpdate::ErrorMessage(e) => panic!("unexpected ErrorMessage: {e}"),
                    GuiUpdate::HostConfigUpdated { base_url, .. } => {
                        assert_eq!(base_url, daemon_url);
                        saw_updated = true;
                    }
                    GuiUpdate::DialogFinished {
                        kind: DialogKind::HostSettings,
                        error: None,
                    } => {
                        saw_dialog_ok = true;
                    }
                    _ => {}
                }
            }
            thread::sleep(Duration::from_millis(20));
        }
        assert!(saw_updated && saw_dialog_ok, "SaveHostConfig did not complete cleanly");
        assert!(
            !squatter_hit.load(Ordering::SeqCst),
            "GUI contacted configured listen_addr instead of daemon.addr"
        );

        squatter_stop.store(true, Ordering::Release);
        squatter_thread.join().unwrap();
        rt.block_on(daemon.shutdown());
    }

    #[test]
    fn stop_vm_does_not_queue_behind_slow_start_vm() {
        use std::io::{Read, Write};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let token = "concurrent-test-token";
        let vm_id = Uuid::new_v4();
        let vm_cfg = openvmms_core::models::VmConfig::from_preset(
            "Concurrent-UI-VM",
            VmPreset::Custom,
            Path::new("/tmp/vm"),
        );
        let mut vm_cfg = vm_cfg;
        vm_cfg.id = vm_id;

        let start_entered = Arc::new(AtomicBool::new(false));
        let release_start = Arc::new(AtomicBool::new(false));
        let stop_server = Arc::new(AtomicBool::new(false));
        let _ = listener.set_nonblocking(true);

        let srv_start_entered = Arc::clone(&start_entered);
        let srv_release_start = Arc::clone(&release_start);
        let srv_stop = Arc::clone(&stop_server);
        let vm_cfg_srv = vm_cfg.clone();

        let srv = thread::spawn(move || {
            let mut handlers = Vec::new();
            while !srv_stop.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let _ = stream.set_nonblocking(false);
                        let start_entered = Arc::clone(&srv_start_entered);
                        let release_start = Arc::clone(&srv_release_start);
                        let srv_stop_conn = Arc::clone(&srv_stop);
                        let vm_cfg_conn = vm_cfg_srv.clone();
                        handlers.push(thread::spawn(move || {
                            let mut buf = [0u8; 4096];
                            let n = stream.read(&mut buf).unwrap_or(0);
                            if n == 0 {
                                return;
                            }
                            let req = String::from_utf8_lossy(&buf[..n]);
                            let first_line = req.lines().next().unwrap_or("");
                            let target = first_line.split_whitespace().nth(1).unwrap_or("");
                            let method = first_line.split_whitespace().next().unwrap_or("");

                            let (status, body) = if target.starts_with("/api/v1/health?") {
                                let query = target.split_once('?').map(|(_, q)| q).unwrap_or("");
                                let challenge = query
                                    .split('&')
                                    .filter_map(|kv| kv.split_once('='))
                                    .find(|(k, _)| *k == "challenge")
                                    .map(|(_, v)| v)
                                    .unwrap_or("");
                                let proof = openvmms_core::models::compute_health_auth_proof(
                                    token, addr, challenge,
                                );
                                (
                                    "200 OK",
                                    format!(
                                        r#"{{"status":"ok","service":"openvmmsd","auth_proof":"{proof}"}}"#
                                    ),
                                )
                            } else if target == "/api/v1/host/info" {
                                let info = HostInfo {
                                    hostname: "test".into(),
                                    os: "linux".into(),
                                    arch: "x86_64".into(),
                                    cpu_count: 4,
                                    daemon_version: "0.1.0".into(),
                                    data_dir: PathBuf::from("/tmp"),
                                    runtime_dir: PathBuf::from("/tmp"),
                                    listen_addr: addr.to_string(),
                                    daemon_instance_id: None,
                                    resolved_paths: Default::default(),
                                };
                                ("200 OK", serde_json::to_string(&info).unwrap())
                            } else if target == "/api/v1/host/config" {
                                ("200 OK", serde_json::to_string(&HostConfig::default()).unwrap())
                            } else if target == "/api/v1/host/switches" {
                                ("200 OK", "[]".into())
                            } else if target == "/api/v1/vms" {
                                let summary = VmSummary {
                                    config: vm_cfg_conn.clone(),
                                    runtime: Default::default(),
                                };
                                ("200 OK", serde_json::to_string(&vec![summary]).unwrap())
                            } else if target == "/api/v1/events" {
                                let hdr = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\n\r\n";
                                let _ = stream.write_all(hdr.as_bytes());
                                let _ = stream.flush();
                                while !srv_stop_conn.load(Ordering::Acquire) {
                                    thread::sleep(Duration::from_millis(25));
                                }
                                return;
                            } else if method == "POST" && target.ends_with("/start") {
                                start_entered.store(true, Ordering::Release);
                                while !release_start.load(Ordering::Acquire)
                                    && !srv_stop_conn.load(Ordering::Acquire)
                                {
                                    thread::sleep(Duration::from_millis(20));
                                }
                                let mut rt = openvmms_core::models::VmRuntimeState::default();
                                rt.status = VmStatus::Off;
                                let summary = VmSummary {
                                    config: vm_cfg_conn.clone(),
                                    runtime: rt,
                                };
                                ("200 OK", serde_json::to_string(&summary).unwrap())
                            } else if method == "POST" && target.ends_with("/stop") {
                                let mut rt = openvmms_core::models::VmRuntimeState::default();
                                rt.status = VmStatus::Off;
                                let summary = VmSummary {
                                    config: vm_cfg_conn.clone(),
                                    runtime: rt,
                                };
                                ("200 OK", serde_json::to_string(&summary).unwrap())
                            } else {
                                ("404 Not Found", r#"{"error":"not found"}"#.into())
                            };
                            let resp = format!(
                                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                                body.len(),
                                body
                            );
                            let _ = stream.write_all(resp.as_bytes());
                            let _ = stream.flush();
                        }));
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(15));
                    }
                    Err(_) => break,
                }
            }
            for h in handlers {
                let _ = h.join();
            }
        });

        let dir = tempfile::tempdir().unwrap();
        let store = openvmms_core::store::RegistryStore::new(dir.path()).unwrap();
        store.write_runtime_listen_addr(&addr.to_string()).unwrap();
        let _lock = store.try_acquire_daemon_lock().unwrap();
        openvmms_core::store::atomic_write(&store.token_path(), token.as_bytes(), true).unwrap();

        let backend = BackendHandle::spawn(
            Some(dir.path().to_path_buf()),
            Some(format!("http://{addr}")),
            egui::Context::default(),
        );
        backend.send(GuiRequest::ConnectOrStartDaemon);
        let _ = recv_until(&backend, Duration::from_secs(5), |u| {
            matches!(u, GuiUpdate::DaemonConnected { .. })
        });

        backend.send(GuiRequest::StartVm { id: vm_id });
        let enter_deadline = std::time::Instant::now() + Duration::from_secs(3);
        while !start_entered.load(Ordering::Acquire) {
            assert!(
                std::time::Instant::now() < enter_deadline,
                "StartVm did not reach server handler"
            );
            thread::sleep(Duration::from_millis(15));
        }

        // While StartVm is still blocked in-flight, StopVm must complete concurrently!
        backend.send(GuiRequest::StopVm { id: vm_id });
        match recv_until(&backend, Duration::from_secs(2), |u| {
            matches!(u, GuiUpdate::StatusMessage(msg) if msg.contains("Turned off"))
        }) {
            GuiUpdate::StatusMessage(msg) => assert!(msg.contains("Turned off")),
            _ => unreachable!(),
        }

        release_start.store(true, Ordering::Release);
        stop_server.store(true, Ordering::Release);
        drop(backend);
        srv.join().unwrap();
    }

    #[test]
    fn non_idempotent_create_and_delete_are_not_retried_after_port_change() {
        use std::io::{Read, Write};
        use std::sync::atomic::AtomicUsize;

        #[allow(clippy::too_many_arguments)]
        fn spawn_mock_endpoint(
            listener: std::net::TcpListener,
            token: &'static str,
            store_dir: PathBuf,
            vm_summary: VmSummary,
            post_count: Arc<AtomicUsize>,
            delete_count: Arc<AtomicUsize>,
            switch_on_post: Option<std::net::SocketAddr>,
            switch_on_delete: Option<std::net::SocketAddr>,
            stop: Arc<AtomicBool>,
        ) -> thread::JoinHandle<()> {
            let addr = listener.local_addr().unwrap();
            let _ = listener.set_nonblocking(true);
            thread::spawn(move || {
                let mut handlers = Vec::new();
                while !stop.load(Ordering::Acquire) {
                    match listener.accept() {
                        Ok((mut stream, _)) => {
                            let _ = stream.set_nonblocking(false);
                            let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
                            let store_dir = store_dir.clone();
                            let vm_summary = vm_summary.clone();
                            let post_count = Arc::clone(&post_count);
                            let delete_count = Arc::clone(&delete_count);
                            let stop_conn = Arc::clone(&stop);
                            handlers.push(thread::spawn(move || {
                                let mut raw = Vec::with_capacity(1024);
                                let mut tmp = [0u8; 1024];
                                let mut header_end = None;
                                while header_end.is_none() {
                                    match stream.read(&mut tmp) {
                                        Ok(0) | Err(_) => return,
                                        Ok(n) => {
                                            raw.extend_from_slice(&tmp[..n]);
                                            header_end = raw
                                                .windows(4)
                                                .position(|w| w == b"\r\n\r\n")
                                                .map(|p| p + 4);
                                            if raw.len() > 64 * 1024 {
                                                break;
                                            }
                                        }
                                    }
                                }
                                let Some(hdr_end) = header_end else {
                                    return;
                                };
                                let headers_str = String::from_utf8_lossy(&raw[..hdr_end]).into_owned();
                                let content_len: usize = headers_str
                                    .lines()
                                    .find_map(|line| {
                                        let (k, v) = line.split_once(':')?;
                                        if k.eq_ignore_ascii_case("content-length") {
                                            v.trim().parse().ok()
                                        } else {
                                            None
                                        }
                                    })
                                    .unwrap_or(0);
                                while raw.len() < hdr_end + content_len {
                                    match stream.read(&mut tmp) {
                                        Ok(0) | Err(_) => break,
                                        Ok(n) => raw.extend_from_slice(&tmp[..n]),
                                    }
                                }
                                let first_line = headers_str.lines().next().unwrap_or("");
                                let method = first_line.split_whitespace().next().unwrap_or("");
                                let target = first_line.split_whitespace().nth(1).unwrap_or("");

                                if target == "/api/v1/events" {
                                    let hdrs = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: close\r\n\r\n";
                                    if stream.write_all(hdrs.as_bytes()).is_err() {
                                        return;
                                    }
                                    let _ = stream.flush();
                                    while !stop_conn.load(Ordering::Acquire) {
                                        if stream.write_all(b": keepalive\n\n").is_err() {
                                            break;
                                        }
                                        let _ = stream.flush();
                                        thread::sleep(Duration::from_millis(25));
                                    }
                                    return;
                                }

                                if method == "POST" && target == "/api/v1/vms" {
                                    post_count.fetch_add(1, Ordering::SeqCst);
                                    if let Some(next_addr) = switch_on_post {
                                        let store =
                                            openvmms_core::store::RegistryStore::new(&store_dir)
                                                .unwrap();
                                        store
                                            .write_runtime_listen_addr(&next_addr.to_string())
                                            .unwrap();
                                        return;
                                    }
                                } else if method == "DELETE" && target.starts_with("/api/v1/vms/") {
                                    delete_count.fetch_add(1, Ordering::SeqCst);
                                    if let Some(next_addr) = switch_on_delete {
                                        let store =
                                            openvmms_core::store::RegistryStore::new(&store_dir)
                                                .unwrap();
                                        store
                                            .write_runtime_listen_addr(&next_addr.to_string())
                                            .unwrap();
                                        return;
                                    }
                                }

                                let (status, body) = if target.starts_with("/api/v1/health?") {
                                    let query =
                                        target.split_once('?').map(|(_, q)| q).unwrap_or("");
                                    let challenge = query
                                        .split('&')
                                        .filter_map(|kv| kv.split_once('='))
                                        .find(|(k, _)| *k == "challenge")
                                        .map(|(_, v)| v)
                                        .unwrap_or("");
                                    let proof = openvmms_core::models::compute_health_auth_proof(
                                        token, addr, challenge,
                                    );
                                    (
                                        "200 OK",
                                        format!(
                                            r#"{{"status":"ok","service":"openvmmsd","auth_proof":"{proof}"}}"#
                                        ),
                                    )
                                } else if target == "/api/v1/host/info" {
                                    let info = HostInfo {
                                        hostname: "test".into(),
                                        os: "linux".into(),
                                        arch: "x86_64".into(),
                                        cpu_count: 4,
                                        daemon_version: "0.1.0".into(),
                                        data_dir: store_dir.clone(),
                                        runtime_dir: store_dir.clone(),
                                        listen_addr: addr.to_string(),
                                        daemon_instance_id: None,
                                        resolved_paths: Default::default(),
                                    };
                                    ("200 OK", serde_json::to_string(&info).unwrap())
                                } else if target == "/api/v1/host/config" {
                                    (
                                        "200 OK",
                                        serde_json::to_string(&HostConfig::default()).unwrap(),
                                    )
                                } else if target == "/api/v1/host/switches" {
                                    ("200 OK", "[]".into())
                                } else if method == "GET" && target == "/api/v1/vms" {
                                    (
                                        "200 OK",
                                        serde_json::to_string(&vec![vm_summary.clone()]).unwrap(),
                                    )
                                } else {
                                    ("404 Not Found", "{}".into())
                                };
                                let resp = format!(
                                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                                    body.len()
                                );
                                let _ = stream.write_all(resp.as_bytes());
                                let _ = stream.flush();
                            }));
                        }
                        Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(10));
                        }
                        Err(_) => break,
                    }
                }
                for h in handlers {
                    let _ = h.join();
                }
            })
        }

        let listener1 = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr1 = listener1.local_addr().unwrap();
        let listener2 = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr2 = listener2.local_addr().unwrap();
        let listener3 = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr3 = listener3.local_addr().unwrap();

        let token = "no-retry-token";
        let vm_id = Uuid::new_v4();
        let mut vm_cfg = openvmms_core::models::VmConfig::from_preset(
            "Existing-VM",
            VmPreset::Custom,
            Path::new("/tmp/vm"),
        );
        vm_cfg.id = vm_id;
        let vm_summary = VmSummary {
            config: vm_cfg,
            runtime: Default::default(),
        };

        let dir = tempfile::tempdir().unwrap();
        let store = openvmms_core::store::RegistryStore::new(dir.path()).unwrap();
        let _lock = store.try_acquire_daemon_lock().unwrap();
        openvmms_core::store::atomic_write(&store.token_path(), token.as_bytes(), true).unwrap();
        store.write_runtime_listen_addr(&addr1.to_string()).unwrap();

        let stop = Arc::new(AtomicBool::new(false));
        let p1_posts = Arc::new(AtomicUsize::new(0));
        let p1_deletes = Arc::new(AtomicUsize::new(0));
        let p2_posts = Arc::new(AtomicUsize::new(0));
        let p2_deletes = Arc::new(AtomicUsize::new(0));
        let p3_posts = Arc::new(AtomicUsize::new(0));
        let p3_deletes = Arc::new(AtomicUsize::new(0));

        let srv1 = spawn_mock_endpoint(
            listener1,
            token,
            dir.path().to_path_buf(),
            vm_summary.clone(),
            Arc::clone(&p1_posts),
            Arc::clone(&p1_deletes),
            Some(addr2),
            None,
            Arc::clone(&stop),
        );
        let srv2 = spawn_mock_endpoint(
            listener2,
            token,
            dir.path().to_path_buf(),
            vm_summary.clone(),
            Arc::clone(&p2_posts),
            Arc::clone(&p2_deletes),
            None,
            Some(addr3),
            Arc::clone(&stop),
        );
        let srv3 = spawn_mock_endpoint(
            listener3,
            token,
            dir.path().to_path_buf(),
            vm_summary,
            Arc::clone(&p3_posts),
            Arc::clone(&p3_deletes),
            None,
            None,
            Arc::clone(&stop),
        );

        let backend = BackendHandle::spawn(
            Some(dir.path().to_path_buf()),
            None,
            egui::Context::default(),
        );
        backend.send(GuiRequest::ConnectOrStartDaemon);
        let url1 = format!("http://{addr1}");
        let url2 = format!("http://{addr2}");
        let url3 = format!("http://{addr3}");

        match recv_until(&backend, Duration::from_secs(5), |u| {
            matches!(u, GuiUpdate::DaemonConnected { .. })
        }) {
            GuiUpdate::DaemonConnected { base_url, .. } => assert_eq!(base_url, url1),
            _ => unreachable!(),
        }

        // Send CreateVm while connected to port1; port1 switches daemon.addr to port2 and drops
        // the connection before replying. The GUI must refresh onto port2 without retrying POST.
        let wizard_id = Uuid::new_v4();
        backend.send(GuiRequest::CreateVm {
            wizard_id,
            payload: CreateVmPayload {
                name: "NoRetry-VM".into(),
                preset: VmPreset::Custom,
                vcpus: Some(2),
                memory_mb: Some(512),
                ..Default::default()
            },
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut saw_url2 = false;
        let mut saw_create_err = false;
        while std::time::Instant::now() < deadline && (!saw_url2 || !saw_create_err) {
            if let Some(upd) = backend.try_recv() {
                match upd {
                    GuiUpdate::DaemonConnected { base_url, .. } if base_url == url2 => {
                        saw_url2 = true;
                    }
                    GuiUpdate::DialogFinished {
                        kind: DialogKind::NewVmWizard(wid),
                        error: Some(_),
                    } if wid == wizard_id => {
                        saw_create_err = true;
                    }
                    _ => {}
                }
            } else {
                thread::sleep(Duration::from_millis(10));
            }
        }
        assert!(saw_url2 && saw_create_err, "CreateVm did not switch to url2 and report failure");
        assert_eq!(p1_posts.load(Ordering::SeqCst), 1);
        assert_eq!(p2_posts.load(Ordering::SeqCst), 0);

        // Next send DeleteVm on port2; port2 switches daemon.addr to port3 and drops the
        // connection before replying. The GUI must refresh onto port3 without retrying DELETE.
        backend.send(GuiRequest::DeleteVm {
            id: vm_id,
            delete_disks: false,
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut saw_url3 = false;
        let mut saw_delete_err = false;
        while std::time::Instant::now() < deadline && (!saw_url3 || !saw_delete_err) {
            if let Some(upd) = backend.try_recv() {
                match upd {
                    GuiUpdate::DaemonConnected { base_url, .. } if base_url == url3 => {
                        saw_url3 = true;
                    }
                    GuiUpdate::DialogFinished {
                        kind: DialogKind::DeleteVm(id),
                        error: Some(_),
                    } if id == vm_id => {
                        saw_delete_err = true;
                    }
                    _ => {}
                }
            } else {
                thread::sleep(Duration::from_millis(10));
            }
        }
        assert!(saw_url3 && saw_delete_err, "DeleteVm did not switch to url3 and report failure");
        assert_eq!(p2_deletes.load(Ordering::SeqCst), 1);
        assert_eq!(p3_deletes.load(Ordering::SeqCst), 0);

        drop(backend);
        stop.store(true, Ordering::Release);
        srv1.join().unwrap();
        srv2.join().unwrap();
        srv3.join().unwrap();
    }
}
