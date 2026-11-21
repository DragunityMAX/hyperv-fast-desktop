mod controller;
mod dialogs;

use clap::Parser;
use controller::BackendHandle;
use controller::DialogKind;
use controller::GuiRequest;
use controller::GuiUpdate;
use dialogs::ConfirmActionDialog;
use dialogs::DeleteVmDialog;
use dialogs::DiskManagerDialog;
use dialogs::FilePickerModal;
use dialogs::FilePickerTarget;
use dialogs::HostSettingsDialog;
use dialogs::NewVmWizard;
use dialogs::PendingConfirmAction;
use dialogs::VmSettingsDialog;
use dialogs::render_confirm_action_dialog;
use dialogs::render_delete_vm_dialog;
use dialogs::render_disk_manager_dialog;
use dialogs::render_file_picker;
use dialogs::render_host_settings_dialog;
use dialogs::render_new_vm_wizard;
use dialogs::render_vm_settings_dialog;
use egui::Color32;
use egui::RichText;
use openvmms_core::models::AttachDiskPayload;
use openvmms_core::models::DiskFormat;
use openvmms_core::models::DiskKind;
use openvmms_core::models::HostConfig;
use openvmms_core::models::HostInfo;
use openvmms_core::models::IpProto;
use openvmms_core::models::MountIsoPayload;
use openvmms_core::models::NicBackendConfig;
use openvmms_core::models::PortForward;
use openvmms_core::models::VirtualSwitchInfo;
use openvmms_core::models::VmPreset;
use openvmms_core::models::VmStatus;
use openvmms_core::models::VmSummary;
use openvmms_core::models::default_data_dir;
use openvmms_core::models::format_memory_mb;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(name = "openvmms-gui", about = "Hyper-V-style desktop manager for OpenVMM and FastDesk")]
struct Args {
    /// Directory for VM configs, logs, and host configuration
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Override daemon HTTP endpoint (e.g. http://127.0.0.1:47620)
    #[arg(long)]
    daemon_url: Option<String>,

    /// Run headless egui UI and controller verification against an ephemeral daemon
    #[arg(long)]
    self_test: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NavFilter {
    All,
    Running,
    Paused,
    Off,
    Switches,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DetailTab {
    Summary,
    Storage,
    Networking,
    ConsoleLogs,
}

struct OpenVmmsApp {
    data_dir: PathBuf,
    backend: BackendHandle,
    connected: bool,
    host_config_loaded: bool,
    connection_status: String,
    host_info: Option<HostInfo>,
    host_config: HostConfig,
    switches: Vec<VirtualSwitchInfo>,
    vms: Vec<VmSummary>,
    selected_vm: Option<Uuid>,
    daemon_instance_id: Option<Uuid>,
    last_snapshot_seq: u64,
    vm_revisions: std::collections::HashMap<Uuid, u64>,
    deleted_vms: std::collections::HashMap<Uuid, u64>,
    nav_filter: NavFilter,
    detail_tab: DetailTab,
    logs_cache: Option<(Uuid, String)>,
    last_log_fetch: Instant,
    status_line: String,
    error_banner: Option<String>,

    // Inline inputs in bottom inspector tabs
    attach_disk_input: String,
    mount_iso_input: String,
    attach_disk_in_flight: std::collections::HashSet<Uuid>,
    mount_iso_in_flight: std::collections::HashSet<Uuid>,
    pf_proto: IpProto,
    pf_host_addr: String,
    pf_host_port: u16,
    pf_guest_port: u16,

    // Modals & wizards
    new_vm_wizard: Option<NewVmWizard>,
    vm_settings_dlg: Option<VmSettingsDialog>,
    disk_mgr_dlg: Option<DiskManagerDialog>,
    host_settings_dlg: Option<HostSettingsDialog>,
    delete_vm_dlg: Option<DeleteVmDialog>,
    confirm_action_dlg: Option<ConfirmActionDialog>,
    file_picker: Option<FilePickerModal>,
}

impl OpenVmmsApp {
    fn new(cc: &eframe::CreationContext<'_>, data_dir: PathBuf, daemon_url: Option<String>) -> Self {
        Self::new_headless(&cc.egui_ctx, data_dir, daemon_url)
    }

    fn new_headless(ctx: &egui::Context, data_dir: PathBuf, daemon_url: Option<String>) -> Self {
        ctx.set_visuals(egui::Visuals::dark());
        let backend = BackendHandle::spawn(Some(data_dir.clone()), daemon_url, ctx.clone());
        backend.send(GuiRequest::ConnectOrStartDaemon);
        Self::new_with_backend(data_dir, backend)
    }

    #[cfg(test)]
    fn new_for_test(ctx: &egui::Context, data_dir: PathBuf) -> Self {
        Self::new_with_backend(data_dir, BackendHandle::new_for_test(ctx.clone()))
    }

    fn new_with_backend(data_dir: PathBuf, backend: BackendHandle) -> Self {
        Self {
            data_dir,
            backend,
            connected: false,
            host_config_loaded: false,
            connection_status: "Connecting to openvmmsd…".to_string(),
            host_info: None,
            host_config: HostConfig::default(),
            switches: Vec::new(),
            vms: Vec::new(),
            selected_vm: None,
            daemon_instance_id: None,
            last_snapshot_seq: 0,
            vm_revisions: std::collections::HashMap::new(),
            deleted_vms: std::collections::HashMap::new(),
            nav_filter: NavFilter::All,
            detail_tab: DetailTab::Summary,
            logs_cache: None,
            last_log_fetch: Instant::now(),
            status_line: "Starting OpenVMMS…".to_string(),
            error_banner: None,
            attach_disk_input: String::new(),
            mount_iso_input: String::new(),
            attach_disk_in_flight: std::collections::HashSet::new(),
            mount_iso_in_flight: std::collections::HashSet::new(),
            pf_proto: IpProto::Tcp,
            pf_host_addr: "127.0.0.1".to_string(),
            pf_host_port: 3389,
            pf_guest_port: 3389,
            new_vm_wizard: None,
            vm_settings_dlg: None,
            disk_mgr_dlg: None,
            host_settings_dlg: None,
            delete_vm_dlg: None,
            confirm_action_dlg: None,
            file_picker: None,
        }
    }

    fn process_updates(&mut self) {
        while let Some(upd) = self.backend.try_recv() {
            self.apply_update(upd);
        }
    }

    fn apply_update(&mut self, upd: GuiUpdate) {
        match upd {
            GuiUpdate::DaemonConnected {
                base_url,
                host_info,
                host_config,
                switches,
                vms,
                snapshot_seq,
            } => {
                if snapshot_seq > 0 && snapshot_seq < self.last_snapshot_seq {
                    return;
                }
                if snapshot_seq > self.last_snapshot_seq {
                    self.last_snapshot_seq = snapshot_seq;
                }
                let same_daemon = self.daemon_instance_id.is_some()
                    && self.daemon_instance_id == host_info.daemon_instance_id;
                self.daemon_instance_id = host_info.daemon_instance_id;

                self.connected = true;
                self.host_config_loaded = true;
                self.connection_status = format!("Connected ({base_url})");
                self.host_info = Some(host_info);
                self.host_config = host_config;
                self.switches = switches;

                if same_daemon {
                    let existing_by_id: std::collections::HashMap<Uuid, VmSummary> =
                        self.vms.drain(..).map(|v| (v.config.id, v)).collect();
                    let mut merged = Vec::with_capacity(vms.len());
                    for vm in vms {
                        let id = vm.config.id;
                        if self.deleted_vms.contains_key(&id) {
                            continue;
                        }
                        let snap_rev = vm.runtime.revision;
                        let known_rev = self.vm_revisions.get(&id).copied().unwrap_or(0);
                        if snap_rev > 0
                            && snap_rev < known_rev
                            && let Some(existing) = existing_by_id.get(&id)
                        {
                            merged.push(existing.clone());
                            continue;
                        }
                        if snap_rev > 0 {
                            self.vm_revisions.insert(id, snap_rev.max(known_rev));
                        }
                        merged.push(vm);
                    }
                    merged.sort_by(|a, b| {
                        a.config
                            .name
                            .to_lowercase()
                            .cmp(&b.config.name.to_lowercase())
                    });
                    self.vms = merged;
                } else {
                    self.vm_revisions.clear();
                    self.deleted_vms.clear();
                    for vm in &vms {
                        if vm.runtime.revision > 0 {
                            self.vm_revisions.insert(vm.config.id, vm.runtime.revision);
                        }
                    }
                    self.vms = vms;
                }

                self.error_banner = None;
                self.attach_disk_in_flight.clear();
                self.mount_iso_in_flight.clear();
                self.status_line = format!("{} virtual machine(s) loaded", self.vms.len());
                if self
                    .selected_vm
                    .is_none_or(|sel| !self.vms.iter().any(|v| v.config.id == sel))
                {
                    self.selected_vm = self.vms.first().map(|v| v.config.id);
                }
            }
            GuiUpdate::DaemonDisconnected(reason) => {
                self.connected = false;
                self.attach_disk_in_flight.clear();
                self.mount_iso_in_flight.clear();
                self.connection_status = "Disconnected".to_string();
                self.error_banner = Some(reason);
            }
            GuiUpdate::VmUpdated {
                vm,
                select,
                is_details_fetch,
            } => {
                let id = vm.config.id;
                if self.deleted_vms.contains_key(&id) {
                    return;
                }
                let rev = vm.runtime.revision;
                if rev > 0 {
                    let prev_rev = self.vm_revisions.get(&id).copied().unwrap_or(0);
                    if rev < prev_rev {
                        if select && self.vms.iter().any(|v| v.config.id == id) {
                            self.selected_vm = Some(id);
                        }
                        return;
                    }
                }
                if let Some(slot) = self.vms.iter_mut().find(|v| v.config.id == id) {
                    if rev > 0 {
                        self.vm_revisions.insert(id, rev);
                    }
                    if let Some(dlg) = self.vm_settings_dlg.as_mut()
                        && dlg.vm_id == id
                    {
                        dlg.base_fastdesk = vm.config.fastdesk.clone();
                        dlg.nics = vm.config.nics.clone();
                        dlg.is_offline =
                            matches!(vm.runtime.status, VmStatus::Off | VmStatus::Error);
                    }
                    *slot = vm;
                    if select {
                        self.selected_vm = Some(id);
                    }
                } else if !is_details_fetch {
                    if rev > 0 {
                        self.vm_revisions.insert(id, rev);
                    }
                    self.vms.push(vm);
                    self.vms.sort_by(|a, b| {
                        a.config
                            .name
                            .to_lowercase()
                            .cmp(&b.config.name.to_lowercase())
                    });
                    if select || self.selected_vm.is_none() {
                        self.selected_vm = Some(id);
                    }
                }
            }
            GuiUpdate::VmDeleted { id, revision } => {
                let del_entry = self.deleted_vms.entry(id).or_insert(0);
                *del_entry = (*del_entry).max(revision);
                if revision > 0 {
                    let rev_entry = self.vm_revisions.entry(id).or_insert(0);
                    *rev_entry = (*rev_entry).max(revision);
                }
                self.vms.retain(|v| v.config.id != id);
                if self.selected_vm == Some(id) {
                    self.selected_vm = self.vms.first().map(|v| v.config.id);
                }
            }
            _ => self.apply_misc_update(upd),
        }
    }

    fn apply_misc_update(&mut self, upd: GuiUpdate) {
        match upd {
            GuiUpdate::VmLogs { id, content } => {
                    self.logs_cache = Some((id, content));
                }
                GuiUpdate::DiskCreated(info) | GuiUpdate::DiskInspected(info) => {
                    if let Some(dlg) = self.disk_mgr_dlg.as_mut() {
                        dlg.last_info = Some(info);
                    }
                }
                GuiUpdate::HostConfigUpdated {
                    base_url,
                    host_info,
                    host_config,
                } => {
                    self.connected = true;
                    self.host_config_loaded = true;
                    self.connection_status = format!("Connected ({base_url})");
                    self.host_info = Some(host_info);
                    self.host_config = host_config;
                }
                GuiUpdate::DialogFinished { kind, error } => match (kind, error) {
                    (DialogKind::NewVmWizard(wid), None) => {
                        if self
                            .new_vm_wizard
                            .as_ref()
                            .is_some_and(|w| w.wizard_id == wid)
                        {
                            self.new_vm_wizard = None;
                        }
                    }
                    (DialogKind::NewVmWizard(wid), Some(err)) => {
                        if let Some(w) = self.new_vm_wizard.as_mut()
                            && w.wizard_id == wid
                        {
                            w.submitting = false;
                            w.error = Some(err);
                        }
                    }
                    (DialogKind::VmSettings(id), None) => {
                        if self.vm_settings_dlg.as_ref().is_some_and(|d| d.vm_id == id) {
                            self.vm_settings_dlg = None;
                        }
                    }
                    (DialogKind::VmSettings(id), Some(err)) => {
                        if let Some(d) = self.vm_settings_dlg.as_mut()
                            && d.vm_id == id
                        {
                            d.submitting = false;
                            d.error = Some(err);
                        }
                    }
                    (DialogKind::HostSettings, None) => self.host_settings_dlg = None,
                    (DialogKind::HostSettings, Some(err)) => {
                        if let Some(d) = self.host_settings_dlg.as_mut() {
                            d.submitting = false;
                            d.error = Some(err);
                        }
                    }
                    (DialogKind::DeleteVm(id), None) => {
                        if self.delete_vm_dlg.as_ref().is_some_and(|d| d.vm_id == id) {
                            self.delete_vm_dlg = None;
                        }
                    }
                    (DialogKind::DeleteVm(id), Some(err)) => {
                        if let Some(d) = self.delete_vm_dlg.as_mut()
                            && d.vm_id == id
                        {
                            d.submitting = false;
                            d.error = Some(err);
                        }
                    }
                },
                GuiUpdate::StorageOpFinished { vm_id, op } => match op {
                    controller::StorageOpKind::AttachDisk => {
                        self.attach_disk_in_flight.remove(&vm_id);
                    }
                    controller::StorageOpKind::MountIso => {
                        self.mount_iso_in_flight.remove(&vm_id);
                    }
                },
                GuiUpdate::StatusMessage(msg) => {
                    self.status_line = msg;
                    self.error_banner = None;
                }
                GuiUpdate::ErrorMessage(err) => {
                    self.error_banner = Some(err);
                }
                _ => {}
        }
    }

    fn selected_vm_summary(&self) -> Option<&VmSummary> {
        let id = self.selected_vm?;
        self.vms.iter().find(|v| v.config.id == id)
    }

    fn filtered_vms(&self) -> Vec<&VmSummary> {
        self.vms
            .iter()
            .filter(|v| match self.nav_filter {
                NavFilter::All | NavFilter::Switches => true,
                NavFilter::Running => v.runtime.status == VmStatus::Running,
                NavFilter::Paused => v.runtime.status == VmStatus::Paused,
                NavFilter::Off => matches!(v.runtime.status, VmStatus::Off | VmStatus::Error),
            })
            .collect()
    }

    fn apply_picked_file(&mut self, target: FilePickerTarget, path: PathBuf) {
        let path_str = path.to_string_lossy().into_owned();
        match target {
            FilePickerTarget::WizardExistingDisk => {
                if let Some(w) = self.new_vm_wizard.as_mut() {
                    w.existing_disk_path = path_str;
                }
            }
            FilePickerTarget::WizardNewDiskPath => {
                if let Some(w) = self.new_vm_wizard.as_mut() {
                    w.new_disk_path = path_str;
                }
            }
            FilePickerTarget::WizardIso => {
                if let Some(w) = self.new_vm_wizard.as_mut() {
                    w.iso_path = path_str;
                }
            }
            FilePickerTarget::AttachDiskPath => self.attach_disk_input = path_str,
            FilePickerTarget::MountIsoPath => self.mount_iso_input = path_str,
            FilePickerTarget::DiskMgrCreatePath => {
                if let Some(d) = self.disk_mgr_dlg.as_mut() {
                    d.create_path = path_str;
                }
            }
            FilePickerTarget::DiskMgrInspectPath => {
                if let Some(d) = self.disk_mgr_dlg.as_mut() {
                    d.inspect_path = path_str;
                }
            }
            FilePickerTarget::SettingsFirmwarePath => {
                if let Some(d) = self.vm_settings_dlg.as_mut() {
                    d.uefi_firmware = path_str;
                }
            }
            FilePickerTarget::HostOpenVmmBin => {
                if let Some(d) = self.host_settings_dlg.as_mut() {
                    d.openvmm_bin = path_str;
                }
            }
            FilePickerTarget::HostViewerBin => {
                if let Some(d) = self.host_settings_dlg.as_mut() {
                    d.fastdesk_viewer_bin = path_str;
                }
            }
            FilePickerTarget::HostImgBin => {
                if let Some(d) = self.host_settings_dlg.as_mut() {
                    d.openvmm_img_bin = path_str;
                }
            }
            FilePickerTarget::HostUefiFirmware => {
                if let Some(d) = self.host_settings_dlg.as_mut() {
                    d.uefi_firmware_path = path_str;
                }
            }
        }
    }
}

impl eframe::App for OpenVmmsApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.render_ui(ctx);
    }
}

impl OpenVmmsApp {
    fn render_ui(&mut self, ctx: &egui::Context) {
        self.process_updates();

        if self.detail_tab == DetailTab::ConsoleLogs
            && let Some(sel) = self.selected_vm
        {
            const LOG_REFRESH_INTERVAL: Duration = Duration::from_secs(2);
            let elapsed = self.last_log_fetch.elapsed();
            if elapsed >= LOG_REFRESH_INTERVAL {
                self.last_log_fetch = Instant::now();
                self.backend.send(GuiRequest::FetchVmLogs { id: sel });
                ctx.request_repaint_after(LOG_REFRESH_INTERVAL);
            } else {
                ctx.request_repaint_after(LOG_REFRESH_INTERVAL - elapsed);
            }
        }

        // Top toolbar
        egui::TopBottomPanel::top("top_toolbar").show(ctx, |ui| {
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                ui.label(RichText::new("OpenVMM Manager").strong().size(16.0));
                ui.separator();

                let pill_color = if self.connected {
                    Color32::from_rgb(65, 195, 110)
                } else {
                    Color32::from_rgb(225, 95, 85)
                };
                ui.colored_label(pill_color, format!("● {}", self.connection_status));
                if !self.connected && ui.button("Reconnect / Start Daemon").clicked() {
                    self.backend.send(GuiRequest::ConnectOrStartDaemon);
                }

                ui.separator();
                if ui.button("➕ New VM…").clicked() {
                    self.new_vm_wizard = Some(NewVmWizard::new(&self.data_dir));
                }
                if ui.button("💾 Virtual Disk Manager…").clicked() {
                    self.disk_mgr_dlg = Some(DiskManagerDialog::new(&self.data_dir));
                }
                let can_edit_host = self.connected && self.host_config_loaded;
                if ui
                    .add_enabled(can_edit_host, egui::Button::new("⚙ Host Settings…"))
                    .on_disabled_hover_text("Connect to openvmmsd before editing host settings")
                    .clicked()
                {
                    self.host_settings_dlg =
                        Some(HostSettingsDialog::from_config(&self.host_config));
                }
                if ui.button("↻ Refresh").clicked() {
                    self.backend.send(GuiRequest::RefreshAll);
                }
            });
            ui.add_space(4.0);
        });

        // Bottom status bar
        egui::TopBottomPanel::bottom("bottom_status").show(ctx, |ui| {
            ui.horizontal(|ui| {
                if let Some(err) = &self.error_banner {
                    ui.colored_label(Color32::LIGHT_RED, format!("Error: {err}"));
                    if ui.small_button("Dismiss").clicked() {
                        self.error_banner = None;
                    }
                } else {
                    ui.label(&self.status_line);
                }
            });
        });

        // Left navigation tree
        egui::SidePanel::left("left_nav")
            .default_width(210.0)
            .resizable(true)
            .show(ctx, |ui| {
                ui.add_space(6.0);
                let host_name = self
                    .host_info
                    .as_ref()
                    .map(|h| h.hostname.as_str())
                    .unwrap_or("localhost");
                ui.label(RichText::new(format!("🖥 {host_name}")).strong());
                ui.separator();

                let total = self.vms.len();
                let running = self
                    .vms
                    .iter()
                    .filter(|v| v.runtime.status == VmStatus::Running)
                    .count();
                let paused = self
                    .vms
                    .iter()
                    .filter(|v| v.runtime.status == VmStatus::Paused)
                    .count();
                let off = self
                    .vms
                    .iter()
                    .filter(|v| matches!(v.runtime.status, VmStatus::Off | VmStatus::Error))
                    .count();

                ui.selectable_value(
                    &mut self.nav_filter,
                    NavFilter::All,
                    format!("All Virtual Machines ({total})"),
                );
                ui.selectable_value(
                    &mut self.nav_filter,
                    NavFilter::Running,
                    format!("Running ({running})"),
                );
                ui.selectable_value(
                    &mut self.nav_filter,
                    NavFilter::Paused,
                    format!("Paused ({paused})"),
                );
                ui.selectable_value(
                    &mut self.nav_filter,
                    NavFilter::Off,
                    format!("Off / Error ({off})"),
                );

                ui.separator();
                ui.selectable_value(
                    &mut self.nav_filter,
                    NavFilter::Switches,
                    format!("Virtual Switches ({})", self.switches.len()),
                );
            });

        // Right Hyper-V style Actions pane
        egui::SidePanel::right("right_actions")
            .default_width(220.0)
            .resizable(true)
            .show(ctx, |ui| {
                ui.add_space(6.0);
                ui.label(RichText::new("Actions").strong().size(14.0));
                ui.separator();

                ui.label(RichText::new("Host").strong());
                if ui.button("New Virtual Machine…").clicked() {
                    self.new_vm_wizard = Some(NewVmWizard::new(&self.data_dir));
                }
                if ui.button("Virtual Hard Disk Manager…").clicked() {
                    self.disk_mgr_dlg = Some(DiskManagerDialog::new(&self.data_dir));
                }
                let can_edit_host = self.connected && self.host_config_loaded;
                if ui
                    .add_enabled(can_edit_host, egui::Button::new("Host Settings…"))
                    .on_disabled_hover_text("Connect to openvmmsd before editing host settings")
                    .clicked()
                {
                    self.host_settings_dlg =
                        Some(HostSettingsDialog::from_config(&self.host_config));
                }

                ui.add_space(10.0);
                ui.separator();

                if let Some(vm) = self.selected_vm_summary().cloned() {
                    let id = vm.config.id;
                    let status = vm.runtime.status;
                    let can_start = matches!(status, VmStatus::Off | VmStatus::Error);
                    let is_running = status == VmStatus::Running;
                    let is_paused = status == VmStatus::Paused;
                    let is_active = is_running || is_paused;
                    let can_turn_off = is_active || status == VmStatus::Starting;
                    let has_fastdesk = vm.runtime.fastdesk_endpoint.is_some();

                    ui.label(RichText::new(&vm.config.name).strong());
                    ui.add_space(4.0);

                    ui.add_enabled_ui(has_fastdesk, |ui| {
                        if ui
                            .button(RichText::new("🖥 Connect (FastDesk)…").strong())
                            .clicked()
                        {
                            self.backend.send(GuiRequest::ConnectViewer { id, no_shm: false });
                        }
                    });
                    if ui.add_enabled(can_start, egui::Button::new("▶ Start")).clicked() {
                        self.backend.send(GuiRequest::StartVm { id });
                    }
                    if ui
                        .add_enabled(is_running, egui::Button::new("⏻ Shut Down"))
                        .clicked()
                    {
                        self.backend.send(GuiRequest::ShutdownVm {
                            id,
                            acpi: false,
                            force: false,
                        });
                    }
                    if ui
                        .add_enabled(can_turn_off, egui::Button::new("⏹ Turn Off"))
                        .clicked()
                    {
                        self.confirm_action_dlg = Some(ConfirmActionDialog {
                            action: PendingConfirmAction::TurnOff {
                                id,
                                vm_name: vm.config.name.clone(),
                            },
                        });
                    }
                    if is_paused {
                        if ui.button("▶ Resume").clicked() {
                            self.backend.send(GuiRequest::ResumeVm { id });
                        }
                    } else if ui
                        .add_enabled(is_running, egui::Button::new("⏸ Pause"))
                        .clicked()
                    {
                        self.backend.send(GuiRequest::PauseVm { id });
                    }
                    if ui
                        .add_enabled(is_active, egui::Button::new("↻ Reset"))
                        .clicked()
                    {
                        self.confirm_action_dlg = Some(ConfirmActionDialog {
                            action: PendingConfirmAction::Reset {
                                id,
                                vm_name: vm.config.name.clone(),
                            },
                        });
                    }
                    ui.separator();
                    if ui.button("⚙ Settings…").clicked() {
                        self.vm_settings_dlg =
                            Some(VmSettingsDialog::from_config(&vm.config, status));
                    }
                    if ui
                        .add_enabled(
                            can_start,
                            egui::Button::new(RichText::new("🗑 Delete…").color(Color32::LIGHT_RED)),
                        )
                        .clicked()
                    {
                        self.delete_vm_dlg = Some(DeleteVmDialog {
                            vm_id: id,
                            vm_name: vm.config.name.clone(),
                            delete_disks: false,
                            submitting: false,
                            error: None,
                        });
                    }
                } else {
                    ui.label("Select a VM to see actions.");
                }
            });

        // Center Workspace
        egui::CentralPanel::default().show(ctx, |ui| {
            if self.nav_filter == NavFilter::Switches {
                ui.heading("Virtual Switches & Network Backends");
                ui.separator();
                egui::Grid::new("switches_table")
                    .striped(true)
                    .num_columns(4)
                    .spacing([18.0, 8.0])
                    .show(ui, |ui| {
                        ui.label(RichText::new("Name").strong());
                        ui.label(RichText::new("Kind").strong());
                        ui.label(RichText::new("ID").strong());
                        ui.label(RichText::new("Description").strong());
                        ui.end_row();
                        for sw in &self.switches {
                            ui.label(&sw.name);
                            ui.label(&sw.kind);
                            ui.monospace(&sw.id);
                            ui.label(&sw.description);
                            ui.end_row();
                        }
                    });
                return;
            }

            // Center-top: Virtual Machines Table
            ui.label(RichText::new("Virtual Machines").strong().size(14.0));
            ui.separator();

            let filtered: Vec<VmSummary> = self.filtered_vms().into_iter().cloned().collect();
            egui::ScrollArea::vertical()
                .id_salt("vm_table_scroll")
                .max_height(260.0)
                .show(ui, |ui| {
                    if filtered.is_empty() {
                        ui.add_space(16.0);
                        ui.label("No virtual machines match this filter. Click 'New VM…' to create one.");
                    } else {
                        egui::Grid::new("vm_list_grid")
                            .striped(true)
                            .num_columns(6)
                            .spacing([16.0, 6.0])
                            .show(ui, |ui| {
                                ui.label(RichText::new("Name").strong());
                                ui.label(RichText::new("State").strong());
                                ui.label(RichText::new("CPU").strong());
                                ui.label(RichText::new("Memory").strong());
                                ui.label(RichText::new("FastDesk").strong());
                                ui.label(RichText::new("Storage").strong());
                                ui.end_row();

                                for vm in &filtered {
                                    let selected = self.selected_vm == Some(vm.config.id);
                                    let row_resp =
                                        ui.selectable_label(selected, &vm.config.name);
                                    if row_resp.clicked() {
                                        self.selected_vm = Some(vm.config.id);
                                        self.backend
                                            .send(GuiRequest::FetchVmDetails { id: vm.config.id });
                                        if self.detail_tab == DetailTab::ConsoleLogs {
                                            self.backend
                                                .send(GuiRequest::FetchVmLogs { id: vm.config.id });
                                        }
                                    }
                                    if row_resp.double_clicked()
                                        && vm.runtime.fastdesk_endpoint.is_some()
                                    {
                                        self.backend.send(GuiRequest::ConnectViewer {
                                            id: vm.config.id,
                                            no_shm: false,
                                        });
                                    }

                                    let (state_color, state_text) = match vm.runtime.status {
                                        VmStatus::Running => (Color32::from_rgb(70, 200, 115), "Running"),
                                        VmStatus::Paused => (Color32::from_rgb(240, 190, 70), "Paused"),
                                        VmStatus::Starting | VmStatus::Stopping => {
                                            (Color32::from_rgb(100, 180, 245), "Transitioning")
                                        }
                                        VmStatus::Off => (Color32::GRAY, "Off"),
                                        VmStatus::Error => (Color32::LIGHT_RED, "Error"),
                                    };
                                    ui.colored_label(state_color, state_text);
                                    ui.label(vm.config.vcpus.to_string());
                                    ui.label(format_memory_mb(vm.config.memory_mb));
                                    ui.label(
                                        vm.runtime
                                            .fastdesk_endpoint
                                            .as_deref()
                                            .unwrap_or(if vm.config.fastdesk.enabled {
                                                "Ready"
                                            } else {
                                                "Off"
                                            }),
                                    );
                                    ui.label(format!("{} LUN(s)", vm.config.disks.len()));
                                    ui.end_row();
                                }
                            });
                    }
                });

            ui.add_space(10.0);
            ui.separator();

            // Center-bottom: Details Inspector Tabs
            if let Some(vm) = self.selected_vm_summary().cloned() {
                ui.horizontal(|ui| {
                    ui.selectable_value(&mut self.detail_tab, DetailTab::Summary, "Summary");
                    ui.selectable_value(
                        &mut self.detail_tab,
                        DetailTab::Storage,
                        format!("Storage & ISO ({})", vm.config.disks.len()),
                    );
                    ui.selectable_value(
                        &mut self.detail_tab,
                        DetailTab::Networking,
                        format!("Networking ({})", vm.config.nics.len()),
                    );
                    if ui
                        .selectable_value(
                            &mut self.detail_tab,
                            DetailTab::ConsoleLogs,
                            "Console & Logs",
                        )
                        .clicked()
                    {
                        self.last_log_fetch = Instant::now();
                        self.backend
                            .send(GuiRequest::FetchVmLogs { id: vm.config.id });
                    }
                });
                ui.separator();

                egui::ScrollArea::vertical()
                    .id_salt("details_scroll")
                    .show(ui, |ui| match self.detail_tab {
                        DetailTab::Summary => self.render_summary_tab(ui, &vm),
                        DetailTab::Storage => self.render_storage_tab(ui, &vm),
                        DetailTab::Networking => self.render_networking_tab(ui, &vm),
                        DetailTab::ConsoleLogs => self.render_logs_tab(ui, &vm),
                    });
            } else {
                ui.label("Select a virtual machine above to inspect its summary, storage, networking, and logs.");
            }
        });

        // Render modal dialogs
        render_new_vm_wizard(
            ctx,
            &mut self.new_vm_wizard,
            &self.switches,
            &self.backend,
            &mut self.file_picker,
        );
        render_vm_settings_dialog(
            ctx,
            &mut self.vm_settings_dlg,
            &self.switches,
            &self.backend,
            &mut self.file_picker,
        );
        render_disk_manager_dialog(
            ctx,
            &mut self.disk_mgr_dlg,
            &self.backend,
            &mut self.file_picker,
        );
        render_host_settings_dialog(
            ctx,
            &mut self.host_settings_dlg,
            self.host_info.as_ref(),
            &self.backend,
            &mut self.file_picker,
        );
        render_delete_vm_dialog(ctx, &mut self.delete_vm_dlg, &self.backend);
        render_confirm_action_dialog(ctx, &mut self.confirm_action_dlg, &self.backend);
        if let Some((target, path)) = render_file_picker(ctx, &mut self.file_picker) {
            self.apply_picked_file(target, path);
        }
    }
}

impl OpenVmmsApp {
    fn render_summary_tab(&self, ui: &mut egui::Ui, vm: &VmSummary) {
        egui::Grid::new("vm_summary_grid")
            .num_columns(2)
            .spacing([18.0, 6.0])
            .show(ui, |ui| {
                ui.label("UUID:");
                ui.monospace(vm.config.id.to_string());
                ui.end_row();
                ui.label("Preset:");
                ui.label(vm.config.preset.to_string());
                ui.end_row();
                ui.label("Compute:");
                ui.label(format!(
                    "{} vCPU, {} RAM (Nested Virt: {})",
                    vm.config.vcpus,
                    format_memory_mb(vm.config.memory_mb),
                    if vm.config.nested_virt { "On" } else { "Off" }
                ));
                ui.end_row();
                ui.label("UEFI & Security:");
                ui.label(format!(
                    "Secure Boot: {} | Template: {}",
                    if vm.config.secure_boot_enabled {
                        "On"
                    } else {
                        "Off"
                    },
                    vm.config.secure_boot_template
                ));
                ui.end_row();
                ui.label("FastDesk Display:");
                if let Some(ep) = &vm.runtime.fastdesk_endpoint {
                    ui.monospace(format!(
                        "{ep} ({} MiB VRAM, SHM: {})",
                        vm.config.fastdesk.vram_mb,
                        vm.runtime.shm_path.as_deref().unwrap_or("disabled")
                    ));
                } else {
                    ui.label(format!(
                        "{} ({} MiB VRAM, SHM: {})",
                        if vm.config.fastdesk.enabled {
                            "Configured (offline)"
                        } else {
                            "Disabled"
                        },
                        vm.config.fastdesk.vram_mb,
                        if vm.config.fastdesk.use_shm { "On" } else { "Off" }
                    ));
                }
                ui.end_row();
                if let Some(pid) = vm.runtime.pid {
                    ui.label("Worker PID:");
                    ui.monospace(pid.to_string());
                    ui.end_row();
                }
                if let Some(err) = &vm.runtime.last_error {
                    ui.label("Last Error:");
                    ui.colored_label(Color32::LIGHT_RED, err);
                    ui.end_row();
                }
            });
    }

    fn render_storage_tab(&mut self, ui: &mut egui::Ui, vm: &VmSummary) {
        let id = vm.config.id;
        egui::Grid::new("storage_tab_grid")
            .striped(true)
            .num_columns(5)
            .spacing([14.0, 6.0])
            .show(ui, |ui| {
                ui.label(RichText::new("LUN").strong());
                ui.label(RichText::new("Kind").strong());
                ui.label(RichText::new("Path").strong());
                ui.label(RichText::new("Access").strong());
                ui.label(RichText::new("Action").strong());
                ui.end_row();

                for d in &vm.config.disks {
                    ui.label(d.lun.to_string());
                    ui.label(if d.kind == DiskKind::Dvd {
                        "DVD/ISO"
                    } else {
                        "Hard Disk"
                    });
                    ui.monospace(d.path.display().to_string());
                    ui.label(if d.read_only { "Read-Only" } else { "Read/Write" });
                    if d.kind == DiskKind::Dvd {
                        if ui.small_button("Eject").clicked() {
                            self.backend.send(GuiRequest::EjectIso {
                                id,
                                lun: Some(d.lun),
                            });
                        }
                    } else if ui.small_button("Detach").clicked() {
                        self.confirm_action_dlg = Some(ConfirmActionDialog {
                            action: PendingConfirmAction::DetachDisk {
                                id,
                                vm_name: vm.config.name.clone(),
                                lun: d.lun,
                                path: d.path.clone(),
                                vm_active: !matches!(vm.runtime.status, VmStatus::Off | VmStatus::Error),
                            },
                        });
                    }
                    ui.end_row();
                }
            });

        ui.separator();
        ui.horizontal(|ui| {
            ui.label("Attach Disk:");
            ui.add(egui::TextEdit::singleline(&mut self.attach_disk_input).desired_width(300.0));
            if ui.button("Browse…").clicked() {
                self.file_picker = Some(FilePickerModal::open(
                    FilePickerTarget::AttachDiskPath,
                    &self.attach_disk_input,
                    false,
                    vec!["vhdx", "vhd", "img", "raw"],
                ));
            }
            let attach_busy = self.attach_disk_in_flight.contains(&id);
            let attach_label = if attach_busy { "Attaching…" } else { "Attach" };
            let can_attach = !attach_busy && !self.attach_disk_input.trim().is_empty();
            if ui
                .add_enabled(can_attach, egui::Button::new(attach_label))
                .clicked()
            {
                self.attach_disk_in_flight.insert(id);
                self.backend.send(GuiRequest::AttachDisk {
                    id,
                    payload: AttachDiskPayload {
                        path: PathBuf::from(self.attach_disk_input.trim()),
                        lun: None,
                        read_only: false,
                        format: None,
                    },
                });
            }
        });

        ui.horizontal(|ui| {
            ui.label("Mount ISO:  ");
            ui.add(egui::TextEdit::singleline(&mut self.mount_iso_input).desired_width(300.0));
            if ui.button("Browse…").clicked() {
                self.file_picker = Some(FilePickerModal::open(
                    FilePickerTarget::MountIsoPath,
                    &self.mount_iso_input,
                    false,
                    vec!["iso"],
                ));
            }
            let mount_busy = self.mount_iso_in_flight.contains(&id);
            let mount_label = if mount_busy {
                "Mounting…"
            } else {
                "Mount ISO"
            };
            let can_mount = !mount_busy && !self.mount_iso_input.trim().is_empty();
            if ui
                .add_enabled(can_mount, egui::Button::new(mount_label))
                .clicked()
            {
                self.mount_iso_in_flight.insert(id);
                self.backend.send(GuiRequest::MountIso {
                    id,
                    payload: MountIsoPayload {
                        path: PathBuf::from(self.mount_iso_input.trim()),
                        lun: None,
                    },
                });
            }
        });
    }

    fn render_networking_tab(&mut self, ui: &mut egui::Ui, vm: &VmSummary) {
        let id = vm.config.id;
        for nic in &vm.config.nics {
            ui.group(|ui| {
                ui.horizontal(|ui| {
                    ui.label(RichText::new(&nic.name).strong());
                    ui.monospace(format!("MAC: {}", nic.mac_address));
                    ui.monospace(format!("ID: {}", nic.id));
                });
                match &nic.backend {
                    NicBackendConfig::Consomme { port_forwards, .. } => {
                        ui.label("Backend: Consomme User-Mode NAT");
                        for pf in port_forwards {
                            ui.horizontal(|ui| {
                                ui.monospace(pf.to_string());
                                if ui.small_button("Remove").clicked() {
                                    self.backend.send(GuiRequest::RemovePortForward {
                                        id,
                                        nic_id: nic.id,
                                        port: pf.clone(),
                                    });
                                }
                            });
                        }
                        ui.horizontal(|ui| {
                            egui::ComboBox::from_id_salt(("pf_proto", nic.id))
                                .selected_text(self.pf_proto.to_string())
                                .show_ui(ui, |ui| {
                                    ui.selectable_value(&mut self.pf_proto, IpProto::Tcp, "tcp");
                                    ui.selectable_value(&mut self.pf_proto, IpProto::Udp, "udp");
                                });
                            ui.add(
                                egui::TextEdit::singleline(&mut self.pf_host_addr)
                                    .desired_width(110.0),
                            );
                            ui.label("Host Port:");
                            ui.add(egui::DragValue::new(&mut self.pf_host_port).range(1..=65535));
                            ui.label("-> Guest Port:");
                            ui.add(egui::DragValue::new(&mut self.pf_guest_port).range(1..=65535));
                            if ui.button("Add Port Forward").clicked() {
                                self.backend.send(GuiRequest::AddPortForward {
                                    id,
                                    nic_id: nic.id,
                                    port: PortForward {
                                        protocol: self.pf_proto,
                                        host_addr: self.pf_host_addr.clone(),
                                        host_port: self.pf_host_port,
                                        guest_port: self.pf_guest_port,
                                    },
                                });
                            }
                        });
                    }
                    NicBackendConfig::VmSwitch {
                        switch_id,
                        switch_name,
                    } => {
                        ui.label(format!(
                            "Backend: Windows Hyper-V VMSwitch ({} / {switch_id})",
                            switch_name.as_deref().unwrap_or("Switch")
                        ));
                    }
                    NicBackendConfig::Tap { name } => {
                        ui.label(format!("Backend: Linux TAP ({name})"));
                    }
                }
            });
        }
        if vm.config.nics.is_empty() {
            ui.label("No network adapters attached. Edit VM Settings while off to add one.");
        }
    }

    fn render_logs_tab(&mut self, ui: &mut egui::Ui, vm: &VmSummary) {
        ui.horizontal(|ui| {
            let can_connect = vm.runtime.fastdesk_endpoint.is_some();
            if ui
                .add_enabled(can_connect, egui::Button::new("🖥 Launch FastDesk Viewer"))
                .clicked()
            {
                self.backend.send(GuiRequest::ConnectViewer {
                    id: vm.config.id,
                    no_shm: false,
                });
            }
            if ui
                .add_enabled(
                    can_connect,
                    egui::Button::new("Launch Viewer (LZ4 TCP / --no-shm)"),
                )
                .clicked()
            {
                self.backend.send(GuiRequest::ConnectViewer {
                    id: vm.config.id,
                    no_shm: true,
                });
            }
            if ui.button("Refresh Logs").clicked() {
                self.backend
                    .send(GuiRequest::FetchVmLogs { id: vm.config.id });
            }
        });
        ui.separator();
        let text = match &self.logs_cache {
            Some((id, content)) if *id == vm.config.id && !content.trim().is_empty() => {
                content.as_str()
            }
            _ => "(No OpenVMM log output yet)",
        };
        ui.add(
            egui::TextEdit::multiline(&mut text.to_string())
                .font(egui::TextStyle::Monospace)
                .desired_width(f32::INFINITY)
                .desired_rows(12),
        );
    }
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    if args.self_test {
        return run_self_test();
    }

    let data_dir = args.data_dir.unwrap_or_else(default_data_dir);
    let daemon_url = args.daemon_url;
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1280.0, 820.0])
            .with_min_inner_size([960.0, 620.0]),
        ..Default::default()
    };

    eframe::run_native(
        "OpenVMM Manager (OpenVMMS)",
        options,
        Box::new(move |cc| Ok(Box::new(OpenVmmsApp::new(cc, data_dir, daemon_url)))),
    )
    .map_err(|err| anyhow::anyhow!("eframe error: {err}"))
}

fn step_headless_ui(ctx: &egui::Context, app: &mut OpenVmmsApp, frames: usize) -> egui::FullOutput {
    let raw_input = egui::RawInput {
        screen_rect: Some(egui::Rect::from_min_size(
            egui::Pos2::ZERO,
            egui::vec2(1280.0, 820.0),
        )),
        ..Default::default()
    };
    let mut last_output = None;
    for _ in 0..frames.max(1) {
        last_output = Some(ctx.run(raw_input.clone(), |c| app.render_ui(c)));
    }
    last_output.unwrap()
}

fn wait_for_app<F>(
    ctx: &egui::Context,
    app: &mut OpenVmmsApp,
    timeout: Duration,
    what: &str,
    mut pred: F,
) -> anyhow::Result<()>
where
    F: FnMut(&OpenVmmsApp) -> bool,
{
    let deadline = Instant::now() + timeout;
    loop {
        let _ = step_headless_ui(ctx, app, 1);
        if pred(app) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "timed out waiting for {what} (status='{}', error={:?})",
                app.status_line,
                app.error_banner
            );
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn run_self_test() -> anyhow::Result<()> {
    let tmp = tempfile::tempdir()?;
    let data_dir = tmp.path().to_path_buf();
    let rt = tokio::runtime::Runtime::new()?;
    let daemon = rt.block_on(openvmmsd::TestDaemonHandle::spawn(data_dir.clone()))?;

    let ctx = egui::Context::default();
    let mut app = OpenVmmsApp::new_headless(&ctx, data_dir.clone(), Some(daemon.base_url.clone()));

    wait_for_app(&ctx, &mut app, Duration::from_secs(15), "daemon connection", |a| {
        a.connected && a.host_config_loaded && a.host_info.is_some()
    })?;

    // Render DiskManagerDialog and verify Raw fixed/sparse creation + inspect through the backend.
    let disk_path = tmp.path().join("selftest.img");
    let mut disk_dlg = DiskManagerDialog::new(tmp.path());
    disk_dlg.create_format = DiskFormat::Raw;
    disk_dlg.create_fixed = true;
    disk_dlg.create_path = disk_path.to_string_lossy().into_owned();
    app.disk_mgr_dlg = Some(disk_dlg);
    let _ = step_headless_ui(&ctx, &mut app, 2);
    app.backend
        .send(GuiRequest::CreateDisk(openvmms_core::models::CreateDiskPayload {
            path: disk_path.clone(),
            size_bytes: 4 * 1024 * 1024,
            format: DiskFormat::Raw,
            fixed: true,
        }));
    wait_for_app(&ctx, &mut app, Duration::from_secs(15), "disk creation", |a| {
        a.disk_mgr_dlg
            .as_ref()
            .and_then(|d| d.last_info.as_ref())
            .is_some_and(|i| i.path == disk_path && i.virtual_size_bytes == 4 * 1024 * 1024)
    })?;
    if let Some(dlg) = app.disk_mgr_dlg.as_mut() {
        dlg.tab = 1;
        dlg.inspect_path = disk_path.to_string_lossy().into_owned();
    }
    let _ = step_headless_ui(&ctx, &mut app, 2);
    app.backend.send(GuiRequest::InspectDisk(disk_path.clone()));
    wait_for_app(&ctx, &mut app, Duration::from_secs(15), "disk inspect", |a| {
        a.disk_mgr_dlg
            .as_ref()
            .and_then(|d| d.last_info.as_ref())
            .is_some_and(|i| i.path == disk_path)
    })?;
    app.disk_mgr_dlg = None;

    // Render every step of NewVmWizard and submit VM creation.
    let mut wiz = NewVmWizard::new(tmp.path());
    assert!(wiz.tpm_enabled, "Win11 preset must default vTPM on");
    wiz.name = "SelfTest-VM".to_string();
    for step in 0..=4 {
        wiz.step = step;
        app.new_vm_wizard = Some(wiz);
        let _ = step_headless_ui(&ctx, &mut app, 1);
        wiz = app.new_vm_wizard.take().unwrap();
    }
    wiz.apply_preset(VmPreset::LinuxDesktop);
    wiz.name = "SelfTest-VM".to_string();
    wiz.disk_mode = dialogs::DiskMode::None;
    let create_payload = wiz.build_create_payload(&app.switches);
    let wizard_id = wiz.wizard_id;
    app.new_vm_wizard = Some(wiz);
    app.backend.send(GuiRequest::CreateVm {
        wizard_id,
        payload: create_payload,
    });
    wait_for_app(&ctx, &mut app, Duration::from_secs(15), "VM creation", |a| {
        a.new_vm_wizard.is_none() && a.vms.iter().any(|v| v.config.name == "SelfTest-VM")
    })?;

    let vm = app
        .vms
        .iter()
        .find(|v| v.config.name == "SelfTest-VM")
        .cloned()
        .expect("SelfTest-VM missing");
    let vm_id = vm.config.id;
    app.selected_vm = Some(vm_id);

    // Render every inspector tab and verify idle ConsoleLogs schedules a 2s repaint.
    for tab in [
        DetailTab::Summary,
        DetailTab::Storage,
        DetailTab::Networking,
        DetailTab::ConsoleLogs,
    ] {
        app.detail_tab = tab;
        let out = step_headless_ui(&ctx, &mut app, 2);
        if tab == DetailTab::ConsoleLogs {
            let repaint_delay = out
                .viewport_output
                .get(&egui::ViewportId::ROOT)
                .map(|v| v.repaint_delay)
                .unwrap_or(Duration::MAX);
            anyhow::ensure!(
                repaint_delay <= Duration::from_secs(2),
                "ConsoleLogs tab did not schedule idle auto-refresh repaint (delay={repaint_delay:?})"
            );
        }
    }

    // Exercise FilePickerModal navigation via typed dir_input and rendering.
    let sub_dir = tmp.path().join("picker-subdir");
    std::fs::create_dir_all(&sub_dir)?;
    let mut picker = FilePickerModal::open(
        FilePickerTarget::MountIsoPath,
        &tmp.path().to_string_lossy(),
        false,
        vec!["iso"],
    );
    picker.dir_input = sub_dir.to_string_lossy().into_owned();
    picker.navigate_to(sub_dir.clone());
    let picker_deadline = std::time::Instant::now() + Duration::from_secs(5);
    while picker.loading && std::time::Instant::now() < picker_deadline {
        let _ = picker.poll_load();
        if picker.loading {
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    anyhow::ensure!(picker.current_dir == sub_dir);
    app.file_picker = Some(picker);
    let _ = step_headless_ui(&ctx, &mut app, 2);
    app.file_picker = None;

    // Exercise VmSettingsDialog and UpdateVm.
    let mut settings_dlg = VmSettingsDialog::from_config(&vm.config, vm.runtime.status);
    settings_dlg.description = "Headless UI self-test".to_string();
    settings_dlg.vcpus = 2;
    let update_payload = settings_dlg.build_update_payload(&app.switches);
    app.vm_settings_dlg = Some(settings_dlg);
    let _ = step_headless_ui(&ctx, &mut app, 2);
    app.backend.send(GuiRequest::UpdateVm {
        id: vm_id,
        payload: update_payload,
    });
    wait_for_app(&ctx, &mut app, Duration::from_secs(15), "VM settings update", |a| {
        a.vm_settings_dlg.is_none()
            && a.vms
                .iter()
                .find(|v| v.config.id == vm_id)
                .is_some_and(|v| v.config.description == "Headless UI self-test")
    })?;

    // Exercise ISO mount and eject.
    let iso_path = tmp.path().join("selftest.iso");
    std::fs::write(&iso_path, vec![0u8; 4096])?;
    app.backend.send(GuiRequest::MountIso {
        id: vm_id,
        payload: MountIsoPayload {
            path: iso_path.clone(),
            lun: None,
        },
    });
    wait_for_app(&ctx, &mut app, Duration::from_secs(15), "ISO mount", |a| {
        a.vms
            .iter()
            .find(|v| v.config.id == vm_id)
            .is_some_and(|v| v.config.disks.iter().any(|d| d.kind == DiskKind::Dvd))
    })?;
    app.backend
        .send(GuiRequest::EjectIso { id: vm_id, lun: None });
    wait_for_app(&ctx, &mut app, Duration::from_secs(15), "ISO eject", |a| {
        a.vms
            .iter()
            .find(|v| v.config.id == vm_id)
            .is_some_and(|v| v.config.disks.iter().all(|d| d.kind != DiskKind::Dvd))
    })?;

    // Exercise PortForward add/remove.
    let nic_id = vm
        .config
        .nics
        .first()
        .map(|n| n.id)
        .ok_or_else(|| anyhow::anyhow!("expected default Consomme NIC"))?;
    let pf = PortForward {
        protocol: IpProto::Udp,
        host_addr: "127.0.0.1".to_string(),
        host_port: 15353,
        guest_port: 53,
    };
    app.backend.send(GuiRequest::AddPortForward {
        id: vm_id,
        nic_id,
        port: pf.clone(),
    });
    wait_for_app(&ctx, &mut app, Duration::from_secs(15), "port forward add", |a| {
        a.vms.iter().find(|v| v.config.id == vm_id).is_some_and(|v| {
            v.config.nics.iter().any(|n| {
                matches!(&n.backend, NicBackendConfig::Consomme { port_forwards, .. } if port_forwards.contains(&pf))
            })
        })
    })?;
    app.backend.send(GuiRequest::RemovePortForward {
        id: vm_id,
        nic_id,
        port: pf.clone(),
    });
    wait_for_app(&ctx, &mut app, Duration::from_secs(15), "port forward remove", |a| {
        a.vms.iter().find(|v| v.config.id == vm_id).is_some_and(|v| {
            v.config.nics.iter().all(|n| {
                !matches!(&n.backend, NicBackendConfig::Consomme { port_forwards, .. } if port_forwards.contains(&pf))
            })
        })
    })?;

    // Render HostSettingsDialog and verify saving config keeps client connected on daemon's actual URL.
    let host_dlg = HostSettingsDialog::from_config(&app.host_config);
    let host_cfg = host_dlg.base_config.clone();
    app.host_settings_dlg = Some(host_dlg);
    let _ = step_headless_ui(&ctx, &mut app, 2);
    app.backend.send(GuiRequest::SaveHostConfig(host_cfg));
    wait_for_app(&ctx, &mut app, Duration::from_secs(15), "host settings save", |a| {
        a.host_settings_dlg.is_none()
    })?;
    anyhow::ensure!(
        app.connection_status.contains(&daemon.base_url),
        "unexpected connection status '{}' (expected '{}')",
        app.connection_status,
        daemon.base_url
    );

    // Render ConfirmActionDialog and DeleteVmDialog, then delete the VM.
    app.confirm_action_dlg = Some(ConfirmActionDialog {
        action: PendingConfirmAction::Reset {
            id: vm_id,
            vm_name: "SelfTest-VM".to_string(),
        },
    });
    let _ = step_headless_ui(&ctx, &mut app, 1);
    app.confirm_action_dlg = None;

    app.delete_vm_dlg = Some(DeleteVmDialog {
        vm_id,
        vm_name: "SelfTest-VM".to_string(),
        delete_disks: false,
        submitting: false,
        error: None,
    });
    let _ = step_headless_ui(&ctx, &mut app, 2);
    app.backend.send(GuiRequest::DeleteVm {
        id: vm_id,
        delete_disks: false,
    });
    wait_for_app(&ctx, &mut app, Duration::from_secs(15), "VM delete", |a| {
        a.delete_vm_dlg.is_none() && !a.vms.iter().any(|v| v.config.id == vm_id)
    })?;

    drop(app);
    rt.block_on(daemon.shutdown());
    println!("SELF-TEST OK (headless egui UI + controller)");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use openvmms_core::models::{
        HostConfig, HostInfo, VmConfig, VmPreset, VmRuntimeState, VmStatus, VmSummary,
    };

    fn make_test_vm(id: Uuid, name: &str, status: VmStatus, revision: u64) -> VmSummary {
        let mut cfg =
            VmConfig::from_preset(name, VmPreset::Custom, std::path::Path::new("/tmp/vm"));
        cfg.id = id;
        VmSummary {
            config: cfg,
            runtime: VmRuntimeState {
                status,
                revision,
                ..Default::default()
            },
        }
    }

    fn make_host_info(daemon_instance_id: Option<Uuid>) -> HostInfo {
        HostInfo {
            hostname: "test".into(),
            os: "linux".into(),
            arch: "x86_64".into(),
            cpu_count: 4,
            daemon_version: "0.1.0".into(),
            data_dir: PathBuf::from("/tmp"),
            runtime_dir: PathBuf::from("/tmp"),
            listen_addr: "127.0.0.1:0".into(),
            daemon_instance_id,
            resolved_paths: Default::default(),
        }
    }

    #[test]
    fn external_vm_update_does_not_hijack_selection_while_gui_create_selects() {
        let dir = tempfile::tempdir().unwrap();
        let mut app = OpenVmmsApp::new_for_test(&egui::Context::default(), dir.path().to_path_buf());
        let daemon_id = Uuid::new_v4();
        let vm_a_id = Uuid::new_v4();
        let vm_b_id = Uuid::new_v4();
        let vm_c_id = Uuid::new_v4();

        app.apply_update(GuiUpdate::DaemonConnected {
            base_url: "http://127.0.0.1:1".into(),
            host_info: make_host_info(Some(daemon_id)),
            host_config: HostConfig::default(),
            switches: vec![],
            vms: vec![make_test_vm(vm_a_id, "Alpha", VmStatus::Off, 1)],
            snapshot_seq: 1,
        });
        assert_eq!(app.selected_vm, Some(vm_a_id));

        // External / SSE creation arrives with `select: false`: must not switch selection.
        app.apply_update(GuiUpdate::VmUpdated {
            vm: make_test_vm(vm_b_id, "Beta", VmStatus::Off, 2),
            select: false,
            is_details_fetch: false,
        });
        assert_eq!(app.vms.len(), 2);
        assert_eq!(app.selected_vm, Some(vm_a_id));

        // Even if SSE delivers `vm_c` at a newer revision first, the GUI CreateVm response
        // (`select: true`) must still select `vm_c` without regressing its state.
        app.apply_update(GuiUpdate::VmUpdated {
            vm: make_test_vm(vm_c_id, "Gamma", VmStatus::Running, 4),
            select: false,
            is_details_fetch: false,
        });
        assert_eq!(app.selected_vm, Some(vm_a_id));
        app.apply_update(GuiUpdate::VmUpdated {
            vm: make_test_vm(vm_c_id, "Gamma", VmStatus::Off, 3),
            select: true,
            is_details_fetch: false,
        });
        assert_eq!(app.selected_vm, Some(vm_c_id));
        let vm_c = app.vms.iter().find(|v| v.config.id == vm_c_id).unwrap();
        assert_eq!(vm_c.runtime.status, VmStatus::Running);
        assert_eq!(vm_c.runtime.revision, 4);
    }

    #[test]
    fn late_details_fetch_and_stale_updates_do_not_resurrect_or_regress_vm() {
        let dir = tempfile::tempdir().unwrap();
        let mut app = OpenVmmsApp::new_for_test(&egui::Context::default(), dir.path().to_path_buf());
        let daemon_id = Uuid::new_v4();
        let vm_a_id = Uuid::new_v4();
        let vm_b_id = Uuid::new_v4();
        let unknown_id = Uuid::new_v4();

        app.apply_update(GuiUpdate::DaemonConnected {
            base_url: "http://127.0.0.1:1".into(),
            host_info: make_host_info(Some(daemon_id)),
            host_config: HostConfig::default(),
            switches: vec![],
            vms: vec![
                make_test_vm(vm_a_id, "Alpha", VmStatus::Off, 1),
                make_test_vm(vm_b_id, "Beta", VmStatus::Off, 2),
            ],
            snapshot_seq: 2,
        });

        // Stale snapshot (lower snapshot_seq) must be dropped completely.
        app.apply_update(GuiUpdate::DaemonConnected {
            base_url: "http://127.0.0.1:1".into(),
            host_info: make_host_info(Some(daemon_id)),
            host_config: HostConfig::default(),
            switches: vec![],
            vms: vec![make_test_vm(unknown_id, "Ghost", VmStatus::Off, 1)],
            snapshot_seq: 1,
        });
        assert_eq!(app.vms.len(), 2);
        assert!(!app.vms.iter().any(|v| v.config.id == unknown_id));

        // Advance `vm_a` to rev 5, then deliver stale rev 3 update and stale rev 4 snapshot.
        app.apply_update(GuiUpdate::VmUpdated {
            vm: make_test_vm(vm_a_id, "Alpha", VmStatus::Running, 5),
            select: false,
            is_details_fetch: false,
        });
        app.apply_update(GuiUpdate::VmUpdated {
            vm: make_test_vm(vm_a_id, "Alpha", VmStatus::Off, 3),
            select: false,
            is_details_fetch: false,
        });
        assert_eq!(
            app.vms.iter().find(|v| v.config.id == vm_a_id).unwrap().runtime.status,
            VmStatus::Running
        );

        // Delete `vm_b`, then send late details fetch, late SSE update, and pre-delete snapshot.
        app.apply_update(GuiUpdate::VmDeleted {
            id: vm_b_id,
            revision: 6,
        });
        assert!(!app.vms.iter().any(|v| v.config.id == vm_b_id));

        app.apply_update(GuiUpdate::VmUpdated {
            vm: make_test_vm(vm_b_id, "Beta", VmStatus::Off, 2),
            select: false,
            is_details_fetch: true,
        });
        app.apply_update(GuiUpdate::VmUpdated {
            vm: make_test_vm(unknown_id, "Unknown", VmStatus::Off, 1),
            select: false,
            is_details_fetch: true,
        });
        app.apply_update(GuiUpdate::VmUpdated {
            vm: make_test_vm(vm_b_id, "Beta", VmStatus::Off, 2),
            select: false,
            is_details_fetch: false,
        });
        app.apply_update(GuiUpdate::DaemonConnected {
            base_url: "http://127.0.0.1:1".into(),
            host_info: make_host_info(Some(daemon_id)),
            host_config: HostConfig::default(),
            switches: vec![],
            vms: vec![
                make_test_vm(vm_a_id, "Alpha", VmStatus::Off, 4),
                make_test_vm(vm_b_id, "Beta", VmStatus::Off, 2),
            ],
            snapshot_seq: 3,
        });
        assert_eq!(app.vms.len(), 1);
        let vm_a = app.vms.iter().find(|v| v.config.id == vm_a_id).unwrap();
        assert_eq!(vm_a.config.id, vm_a_id);
        assert_eq!(vm_a.runtime.status, VmStatus::Running);
        assert_eq!(vm_a.runtime.revision, 5);
    }

    #[test]
    fn stale_new_vm_wizard_completion_does_not_close_newer_wizard() {
        let dir = tempfile::tempdir().unwrap();
        let mut app = OpenVmmsApp::new_for_test(&egui::Context::default(), dir.path().to_path_buf());

        let old_wiz = NewVmWizard::new(dir.path());
        let old_id = old_wiz.wizard_id;
        let new_wiz = NewVmWizard::new(dir.path());
        let new_id = new_wiz.wizard_id;
        assert_ne!(old_id, new_id);
        app.new_vm_wizard = Some(new_wiz);

        // Completion (success or error) from a cancelled/superseded wizard must not
        // close or mutate the currently open wizard.
        app.apply_update(GuiUpdate::DialogFinished {
            kind: DialogKind::NewVmWizard(old_id),
            error: Some("stale failure".into()),
        });
        assert!(app.new_vm_wizard.as_ref().is_some_and(|w| w.wizard_id == new_id && w.error.is_none()));

        app.apply_update(GuiUpdate::DialogFinished {
            kind: DialogKind::NewVmWizard(old_id),
            error: None,
        });
        assert!(app.new_vm_wizard.as_ref().is_some_and(|w| w.wizard_id == new_id));

        // Matching wizard_id completion closes it cleanly.
        app.apply_update(GuiUpdate::DialogFinished {
            kind: DialogKind::NewVmWizard(new_id),
            error: None,
        });
        assert!(app.new_vm_wizard.is_none());
    }

    #[test]
    fn storage_in_flight_flags_clear_on_storage_op_finished() {
        let dir = tempfile::tempdir().unwrap();
        let mut app = OpenVmmsApp::new_for_test(&egui::Context::default(), dir.path().to_path_buf());
        let vm_id = Uuid::new_v4();

        app.attach_disk_in_flight.insert(vm_id);
        app.mount_iso_in_flight.insert(vm_id);
        app.apply_update(GuiUpdate::StorageOpFinished {
            vm_id,
            op: controller::StorageOpKind::AttachDisk,
        });
        assert!(!app.attach_disk_in_flight.contains(&vm_id));
        assert!(app.mount_iso_in_flight.contains(&vm_id));

        app.apply_update(GuiUpdate::StorageOpFinished {
            vm_id,
            op: controller::StorageOpKind::MountIso,
        });
        assert!(!app.mount_iso_in_flight.contains(&vm_id));
    }
}
