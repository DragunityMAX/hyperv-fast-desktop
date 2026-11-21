use crate::controller::BackendHandle;
use crate::controller::GuiRequest;
use egui::Color32;
use egui::RichText;
use openvmms_core::models::CreateDiskPayload;
use openvmms_core::models::CreateVmPayload;
use openvmms_core::models::DiskAttachment;
use openvmms_core::models::DiskFormat;
use openvmms_core::models::DiskInfo;
use openvmms_core::models::DiskKind;
use openvmms_core::models::FastDeskSettings;
use openvmms_core::models::FastDeskTransport;
use openvmms_core::models::HostConfig;
use openvmms_core::models::HostInfo;
use openvmms_core::models::MAX_VM_MEMORY_MB;
use openvmms_core::models::NicAttachment;
use openvmms_core::models::NicBackendConfig;
use openvmms_core::models::SecureBootTemplateConfig;
use openvmms_core::models::UpdateVmPayload;
use openvmms_core::models::VirtualSwitchInfo;
use openvmms_core::models::VmConfig;
use openvmms_core::models::VmPreset;
use openvmms_core::models::VmStatus;
use openvmms_core::models::format_bytes;
use openvmms_core::models::generate_hyperv_mac;
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilePickerTarget {
    WizardExistingDisk,
    WizardNewDiskPath,
    WizardIso,
    AttachDiskPath,
    MountIsoPath,
    DiskMgrCreatePath,
    DiskMgrInspectPath,
    SettingsFirmwarePath,
    HostOpenVmmBin,
    HostViewerBin,
    HostImgBin,
    HostUefiFirmware,
}

struct FilePickerLoadResult {
    seq: u64,
    resolved_dir: Option<(PathBuf, String, Option<String>)>,
    entries: Vec<(String, PathBuf, bool)>,
    error: Option<String>,
}

fn read_dir_entries(
    dir: &std::path::Path,
    filter_exts: &[String],
) -> (Vec<(String, PathBuf, bool)>, Option<String>) {
    let read = match std::fs::read_dir(dir) {
        Ok(r) => r,
        Err(err) => {
            return (
                Vec::new(),
                Some(format!("Cannot read {}: {err}", dir.display())),
            );
        }
    };
    let mut dirs = Vec::new();
    let mut files = Vec::new();
    for entry in read.flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        let is_dir = path.is_dir();
        if is_dir {
            dirs.push((name, path, true));
        } else {
            if !filter_exts.is_empty() {
                let ext = path
                    .extension()
                    .and_then(|e| e.to_str())
                    .map(|e| e.to_ascii_lowercase())
                    .unwrap_or_default();
                if !filter_exts.contains(&ext) {
                    continue;
                }
            }
            files.push((name, path, false));
        }
    }
    dirs.sort_by(|a, b| a.0.to_lowercase().cmp(&b.0.to_lowercase()));
    files.sort_by(|a, b| a.0.to_lowercase().cmp(&b.0.to_lowercase()));
    let mut entries = Vec::with_capacity(dirs.len() + files.len());
    entries.extend(dirs);
    entries.extend(files);
    (entries, None)
}

pub struct FilePickerModal {
    pub target: FilePickerTarget,
    pub current_dir: PathBuf,
    pub dir_input: String,
    pub filename_input: String,
    pub save_mode: bool,
    pub filter_exts: Vec<String>,
    pub entries: Vec<(String, PathBuf, bool)>,
    pub error: Option<String>,
    pub loading: bool,
    dir_input_edited: bool,
    dir_input_at_load_start: String,
    load_seq: u64,
    load_rx: Option<std::sync::mpsc::Receiver<FilePickerLoadResult>>,
    repaint_ctx: Option<egui::Context>,
    ignore_file_clicks_until: Option<std::time::Instant>,
}

impl FilePickerModal {
    pub fn open(
        target: FilePickerTarget,
        initial_path: &str,
        save_mode: bool,
        filter_exts: Vec<&str>,
    ) -> Self {
        let trimmed = initial_path.trim().to_string();
        let initial_guess = if !trimmed.is_empty() {
            PathBuf::from(&trimmed)
        } else {
            PathBuf::from(".")
        };
        let mut modal = Self {
            target,
            current_dir: initial_guess,
            dir_input: trimmed.clone(),
            filename_input: String::new(),
            save_mode,
            filter_exts: filter_exts.into_iter().map(|s| s.to_ascii_lowercase()).collect(),
            entries: Vec::new(),
            error: None,
            loading: false,
            dir_input_edited: false,
            dir_input_at_load_start: trimmed.clone(),
            load_seq: 0,
            load_rx: None,
            repaint_ctx: None,
            ignore_file_clicks_until: None,
        };
        let filter_exts_owned = modal.filter_exts.clone();
        modal.start_load(trimmed.clone(), move |seq| {
            let initial = PathBuf::from(&trimmed);
            let (start_dir, fname) = if !trimmed.is_empty() && initial.is_dir() {
                (initial, String::new())
            } else if !trimmed.is_empty()
                && let Some(parent) = initial.parent()
                && parent.is_dir()
            {
                (
                    parent.to_path_buf(),
                    initial
                        .file_name()
                        .map(|n| n.to_string_lossy().into_owned())
                        .unwrap_or_default(),
                )
            } else {
                let home = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
                (home, String::new())
            };
            let dir_str = start_dir.to_string_lossy().into_owned();
            let (entries, error) = read_dir_entries(&start_dir, &filter_exts_owned);
            FilePickerLoadResult {
                seq,
                resolved_dir: Some((start_dir, dir_str, Some(fname))),
                entries,
                error,
            }
        });
        modal
    }

    fn start_load<F>(&mut self, dir_input_snapshot: String, work: F)
    where
        F: FnOnce(u64) -> FilePickerLoadResult + Send + 'static,
    {
        self.load_seq = self.load_seq.wrapping_add(1).max(1);
        let seq = self.load_seq;
        self.loading = true;
        self.error = None;
        self.dir_input_at_load_start = dir_input_snapshot;
        let (tx, rx) = std::sync::mpsc::channel();
        self.load_rx = Some(rx);
        let repaint_ctx = self.repaint_ctx.clone();
        std::thread::spawn(move || {
            let res = work(seq);
            let _ = tx.send(res);
            if let Some(ctx) = repaint_ctx {
                ctx.request_repaint();
            }
        });
    }

    pub fn poll_load(&mut self) -> bool {
        let Some(rx) = self.load_rx.as_ref() else {
            return false;
        };
        match rx.try_recv() {
            Ok(res) => {
                self.load_rx = None;
                self.loading = false;
                if res.seq != self.load_seq {
                    return false;
                }
                if let Some((dir, dir_str, fname_opt)) = res.resolved_dir {
                    self.current_dir = dir;
                    if !self.dir_input_edited && self.dir_input == self.dir_input_at_load_start {
                        self.dir_input = dir_str;
                    }
                    if let Some(fname) = fname_opt
                        && self.filename_input.is_empty()
                    {
                        self.filename_input = fname;
                    }
                    self.entries = res.entries;
                }
                self.error = res.error;
                true
            }
            Err(std::sync::mpsc::TryRecvError::Empty) => false,
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                self.load_rx = None;
                self.loading = false;
                true
            }
        }
    }

    pub fn navigate_to(&mut self, dir: PathBuf) {
        let dir_str = dir.to_string_lossy().into_owned();
        self.dir_input = dir_str.clone();
        self.dir_input_edited = false;
        self.ignore_file_clicks_until =
            Some(std::time::Instant::now() + std::time::Duration::from_millis(350));
        let filter_exts = self.filter_exts.clone();
        self.start_load(dir_str.clone(), move |seq| {
            if dir.is_dir() {
                let (entries, error) = read_dir_entries(&dir, &filter_exts);
                FilePickerLoadResult {
                    seq,
                    resolved_dir: Some((dir, dir_str, None)),
                    entries,
                    error,
                }
            } else {
                FilePickerLoadResult {
                    seq,
                    resolved_dir: None,
                    entries: Vec::new(),
                    error: Some(format!("Directory not found: {}", dir.display())),
                }
            }
        });
    }

    pub fn reload(&mut self) {
        let dir = self.current_dir.clone();
        let dir_str = dir.to_string_lossy().into_owned();
        let filter_exts = self.filter_exts.clone();
        let snapshot = self.dir_input.clone();
        self.start_load(snapshot, move |seq| {
            let (entries, error) = read_dir_entries(&dir, &filter_exts);
            FilePickerLoadResult {
                seq,
                resolved_dir: Some((dir, dir_str, None)),
                entries,
                error,
            }
        });
    }
}

pub enum DiskMode {
    CreateNew,
    UseExisting,
    None,
}

pub struct NewVmWizard {
    pub wizard_id: Uuid,
    pub step: usize,
    pub name: String,
    pub description: String,
    pub preset: VmPreset,
    pub vcpus: u32,
    pub memory_mb: u64,
    pub secure_boot_enabled: bool,
    pub secure_boot_template: SecureBootTemplateConfig,
    pub tpm_enabled: bool,
    pub nested_virt: bool,
    pub fastdesk_enabled: bool,
    pub vram_mb: u32,
    pub use_shm: bool,
    pub transport: FastDeskTransport,
    pub selected_switch_id: String,
    pub disk_mode: DiskMode,
    pub new_disk_path: String,
    /// Directory used to suggest a per-VM disk file name when browsing.
    pub default_disk_dir: PathBuf,
    pub new_disk_size_gb: u64,
    pub new_disk_format: DiskFormat,
    pub new_disk_fixed: bool,
    pub existing_disk_path: String,
    pub iso_path: String,
    pub submitting: bool,
    pub error: Option<String>,
}

impl NewVmWizard {
    pub fn new(default_dir: &std::path::Path) -> Self {
        let mut w = Self {
            wizard_id: Uuid::new_v4(),
            step: 0,
            name: "Windows-11-VM".to_string(),
            description: String::new(),
            preset: VmPreset::Windows11,
            vcpus: 4,
            memory_mb: 8192,
            secure_boot_enabled: true,
            secure_boot_template: SecureBootTemplateConfig::MicrosoftWindows,
            tpm_enabled: true,
            nested_virt: false,
            fastdesk_enabled: true,
            vram_mb: 32,
            use_shm: true,
            transport: FastDeskTransport::Tcp,
            selected_switch_id: "consomme".to_string(),
            disk_mode: DiskMode::CreateNew,
            // Empty means the daemon picks a unique `<name>-<id>-disk0.<ext>` path in its
            // disks directory, so wizards for different VMs never collide on one file.
            new_disk_path: String::new(),
            default_disk_dir: default_dir.join("disks"),
            new_disk_size_gb: 64,
            new_disk_format: DiskFormat::Vhdx,
            new_disk_fixed: false,
            existing_disk_path: String::new(),
            iso_path: String::new(),
            submitting: false,
            error: None,
        };
        w.apply_preset(VmPreset::Windows11);
        w
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.name.trim().is_empty() {
            return Err("VM name cannot be empty".to_string());
        }
        if self.disk_mode == DiskMode::UseExisting && self.existing_disk_path.trim().is_empty() {
            return Err("Please specify an existing virtual hard disk path".to_string());
        }
        Ok(())
    }

    fn sanitized_disk_stem(&self) -> String {
        let stem: String = self
            .name
            .trim()
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || matches!(c, '-' | '_') {
                    c
                } else {
                    '-'
                }
            })
            .collect();
        let trimmed = stem.trim_matches('-');
        if trimmed.is_empty() {
            "vm".to_string()
        } else {
            trimmed.to_string()
        }
    }

    /// Hint text shown for the optional disk path when no custom path is entered.
    pub fn auto_disk_hint(&self) -> String {
        let stem = self.sanitized_disk_stem();
        let ext = if self.new_disk_format == DiskFormat::Vhdx {
            "vhdx"
        } else {
            "img"
        };
        format!("auto: {stem}-<id>-disk0.{ext}")
    }

    /// Path pre-filled in the Browse dialog when no explicit disk path is set; derived
    /// from the current VM name and avoids colliding with existing files in `default_disk_dir`.
    pub fn suggested_disk_path(&self) -> String {
        let stem = self.sanitized_disk_stem();
        let ext = if self.new_disk_format == DiskFormat::Vhdx {
            "vhdx"
        } else {
            "img"
        };
        let mut candidate = self.default_disk_dir.join(format!("{stem}.{ext}"));
        let mut n = 2usize;
        while candidate.exists() {
            candidate = self.default_disk_dir.join(format!("{stem}-{n}.{ext}"));
            n += 1;
        }
        candidate.to_string_lossy().into_owned()
    }

    pub fn build_create_payload(&mut self, switches: &[VirtualSwitchInfo]) -> CreateVmPayload {
        let (disks, create_disk) = match self.disk_mode {
            DiskMode::CreateNew => {
                sync_disk_path_extension(&mut self.new_disk_path, self.new_disk_format);
                (
                    Vec::new(),
                    Some(CreateDiskPayload {
                        path: PathBuf::from(self.new_disk_path.trim()),
                        size_bytes: self.new_disk_size_gb.saturating_mul(1024 * 1024 * 1024),
                        format: self.new_disk_format,
                        fixed: self.new_disk_fixed,
                    }),
                )
            }
            DiskMode::UseExisting => {
                let p = PathBuf::from(self.existing_disk_path.trim());
                let format = DiskFormat::from_path(&p);
                let (kind, read_only) = if format == DiskFormat::Iso {
                    (DiskKind::Dvd, true)
                } else {
                    (DiskKind::HardDisk, false)
                };
                (
                    vec![DiskAttachment {
                        lun: 0,
                        kind,
                        format,
                        path: p,
                        read_only,
                    }],
                    None,
                )
            }
            DiskMode::None => (Vec::new(), None),
        };
        let nics = build_nics_from_switch_id(&self.selected_switch_id, switches, &[]);
        CreateVmPayload {
            name: self.name.trim().to_string(),
            description: self.description.clone(),
            preset: self.preset,
            vcpus: Some(self.vcpus),
            memory_mb: Some(self.memory_mb),
            nested_virt: Some(self.nested_virt),
            uefi_firmware: None,
            secure_boot_enabled: Some(self.secure_boot_enabled),
            secure_boot_template: Some(self.secure_boot_template),
            tpm_enabled: Some(self.tpm_enabled),
            fastdesk: Some(FastDeskSettings {
                enabled: self.fastdesk_enabled,
                vram_mb: self.vram_mb,
                use_shm: self.use_shm,
                transport: self.transport,
                listen_addr: "127.0.0.1:0".to_string(),
                vsock_port: None,
            }),
            disks,
            create_disk,
            iso_path: string_to_opt(&self.iso_path),
            nics: Some(nics),
        }
    }

    pub fn apply_preset(&mut self, preset: VmPreset) {
        self.preset = preset;
        match preset {
            VmPreset::Windows11 => {
                self.vcpus = 4;
                self.memory_mb = 8192;
                self.secure_boot_enabled = true;
                self.secure_boot_template = SecureBootTemplateConfig::MicrosoftWindows;
                self.tpm_enabled = true;
                self.vram_mb = 32;
            }
            VmPreset::LinuxDesktop => {
                self.vcpus = 4;
                self.memory_mb = 4096;
                self.secure_boot_enabled = false;
                self.secure_boot_template = SecureBootTemplateConfig::MicrosoftUefiCa;
                self.tpm_enabled = false;
                self.vram_mb = 32;
            }
            VmPreset::Custom => {
                self.vcpus = 2;
                self.memory_mb = 4096;
                self.secure_boot_enabled = false;
                self.secure_boot_template = SecureBootTemplateConfig::None;
                self.tpm_enabled = false;
            }
        }
    }
}

pub struct VmSettingsDialog {
    pub vm_id: Uuid,
    pub is_offline: bool,
    pub initial_config: VmConfig,
    pub name: String,
    pub description: String,
    pub vcpus: u32,
    pub memory_mb: u64,
    pub nested_virt: bool,
    pub secure_boot_enabled: bool,
    pub secure_boot_template: SecureBootTemplateConfig,
    pub tpm_enabled: bool,
    pub uefi_firmware: String,
    pub fastdesk_enabled: bool,
    pub vram_mb: u32,
    pub use_shm: bool,
    pub transport: FastDeskTransport,
    pub base_fastdesk: FastDeskSettings,
    pub initial_switch_id: String,
    pub selected_switch_id: String,
    pub nics: Vec<NicAttachment>,
    pub submitting: bool,
    pub error: Option<String>,
}

impl VmSettingsDialog {
    pub fn from_config(cfg: &VmConfig, status: VmStatus) -> Self {
        let selected_switch_id = match cfg.nics.first().map(|n| &n.backend) {
            None => "none".to_string(),
            Some(NicBackendConfig::Consomme { .. }) => "consomme".to_string(),
            Some(NicBackendConfig::VmSwitch { switch_id, .. }) => switch_id.clone(),
            Some(NicBackendConfig::Tap { name }) => name.clone(),
        };
        Self {
            vm_id: cfg.id,
            is_offline: matches!(status, VmStatus::Off | VmStatus::Error),
            initial_config: cfg.clone(),
            name: cfg.name.clone(),
            description: cfg.description.clone(),
            vcpus: cfg.vcpus,
            memory_mb: cfg.memory_mb,
            nested_virt: cfg.nested_virt,
            secure_boot_enabled: cfg.secure_boot_enabled,
            secure_boot_template: cfg.secure_boot_template,
            tpm_enabled: cfg.tpm_enabled,
            uefi_firmware: cfg
                .uefi_firmware
                .as_ref()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_default(),
            fastdesk_enabled: cfg.fastdesk.enabled,
            vram_mb: cfg.fastdesk.vram_mb,
            use_shm: cfg.fastdesk.use_shm,
            transport: cfg.fastdesk.transport,
            base_fastdesk: cfg.fastdesk.clone(),
            initial_switch_id: selected_switch_id.clone(),
            selected_switch_id,
            nics: cfg.nics.clone(),
            submitting: false,
            error: None,
        }
    }

    pub fn has_hardware_changes(&self) -> bool {
        self.vcpus != self.initial_config.vcpus
            || self.memory_mb != self.initial_config.memory_mb
            || self.nested_virt != self.initial_config.nested_virt
            || string_to_opt(&self.uefi_firmware) != self.initial_config.uefi_firmware
            || self.secure_boot_enabled != self.initial_config.secure_boot_enabled
            || self.secure_boot_template != self.initial_config.secure_boot_template
            || self.tpm_enabled != self.initial_config.tpm_enabled
            || self.fastdesk_enabled != self.initial_config.fastdesk.enabled
            || self.vram_mb != self.initial_config.fastdesk.vram_mb
            || self.use_shm != self.initial_config.fastdesk.use_shm
            || self.transport != self.initial_config.fastdesk.transport
            || self.selected_switch_id != self.initial_switch_id
    }

    pub fn build_update_payload(&self, switches: &[VirtualSwitchInfo]) -> UpdateVmPayload {
        let trimmed_name = self.name.trim();
        let name = (trimmed_name != self.initial_config.name).then(|| trimmed_name.to_string());
        let description =
            (self.description != self.initial_config.description).then(|| self.description.clone());

        // Only emit fields that were actually changed so concurrent CLI/API edits
        // to other fields are never overwritten by a stale dialog snapshot.
        let nics = (self.selected_switch_id != self.initial_switch_id).then(|| {
            build_nics_from_switch_id(&self.selected_switch_id, switches, &self.nics)
        });
        let fastdesk_changed = self.fastdesk_enabled != self.initial_config.fastdesk.enabled
            || self.vram_mb != self.initial_config.fastdesk.vram_mb
            || self.use_shm != self.initial_config.fastdesk.use_shm
            || self.transport != self.initial_config.fastdesk.transport;
        let fastdesk_diff = fastdesk_changed.then(|| {
            let mut fastdesk = self.base_fastdesk.clone();
            fastdesk.enabled = self.fastdesk_enabled;
            fastdesk.vram_mb = self.vram_mb;
            fastdesk.use_shm = self.use_shm;
            fastdesk.transport = self.transport;
            fastdesk
        });
        let cur_fw = string_to_opt(&self.uefi_firmware);
        let uefi_firmware = (cur_fw != self.initial_config.uefi_firmware).then_some(cur_fw);

        UpdateVmPayload {
            name,
            description,
            vcpus: (self.vcpus != self.initial_config.vcpus).then_some(self.vcpus),
            memory_mb: (self.memory_mb != self.initial_config.memory_mb)
                .then_some(self.memory_mb),
            nested_virt: (self.nested_virt != self.initial_config.nested_virt)
                .then_some(self.nested_virt),
            uefi_firmware,
            secure_boot_enabled: (self.secure_boot_enabled
                != self.initial_config.secure_boot_enabled)
                .then_some(self.secure_boot_enabled),
            secure_boot_template: (self.secure_boot_template
                != self.initial_config.secure_boot_template)
                .then_some(self.secure_boot_template),
            tpm_enabled: (self.tpm_enabled != self.initial_config.tpm_enabled)
                .then_some(self.tpm_enabled),
            fastdesk: fastdesk_diff,
            nics,
            ..Default::default()
        }
    }
}

pub struct DiskManagerDialog {
    pub tab: usize,
    pub create_path: String,
    pub create_size_gb: u64,
    pub create_format: DiskFormat,
    pub create_fixed: bool,
    pub inspect_path: String,
    pub last_info: Option<DiskInfo>,
}

impl DiskManagerDialog {
    pub fn new(default_dir: &std::path::Path) -> Self {
        Self {
            tab: 0,
            create_path: default_dir.join("new-disk.vhdx").to_string_lossy().into_owned(),
            create_size_gb: 64,
            create_format: DiskFormat::Vhdx,
            create_fixed: false,
            inspect_path: String::new(),
            last_info: None,
        }
    }
}

pub struct HostSettingsDialog {
    pub listen_addr: String,
    pub openvmm_bin: String,
    pub fastdesk_viewer_bin: String,
    pub openvmm_img_bin: String,
    pub uefi_firmware_path: String,
    pub base_config: HostConfig,
    pub submitting: bool,
    pub error: Option<String>,
}

impl HostSettingsDialog {
    pub fn from_config(cfg: &HostConfig) -> Self {
        Self {
            listen_addr: cfg.listen_addr.clone(),
            openvmm_bin: opt_to_string(cfg.openvmm_bin.as_deref()),
            fastdesk_viewer_bin: opt_to_string(cfg.fastdesk_viewer_bin.as_deref()),
            openvmm_img_bin: opt_to_string(cfg.openvmm_img_bin.as_deref()),
            uefi_firmware_path: opt_to_string(cfg.uefi_firmware_path.as_deref()),
            base_config: cfg.clone(),
            submitting: false,
            error: None,
        }
    }
}

pub struct DeleteVmDialog {
    pub vm_id: Uuid,
    pub vm_name: String,
    pub delete_disks: bool,
    pub submitting: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub enum PendingConfirmAction {
    TurnOff {
        id: Uuid,
        vm_name: String,
    },
    Reset {
        id: Uuid,
        vm_name: String,
    },
    DetachDisk {
        id: Uuid,
        vm_name: String,
        lun: u32,
        path: PathBuf,
        vm_active: bool,
    },
}

pub struct ConfirmActionDialog {
    pub action: PendingConfirmAction,
}

fn opt_to_string(p: Option<&std::path::Path>) -> String {
    p.map(|x| x.to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn string_to_opt(s: &str) -> Option<PathBuf> {
    let t = s.trim();
    if t.is_empty() {
        None
    } else {
        Some(PathBuf::from(t))
    }
}

pub fn sync_disk_path_extension(path_str: &mut String, format: DiskFormat) {
    let trimmed = path_str.trim();
    if trimmed.is_empty() {
        return;
    }
    let mut path = PathBuf::from(trimmed);
    let before = path.clone();
    openvmms_core::disk::sync_disk_path_extension(&mut path, format);
    if path != before {
        *path_str = path.to_string_lossy().into_owned();
    }
}

pub fn build_nics_from_switch_id(
    switch_id: &str,
    switches: &[VirtualSwitchInfo],
    existing_nics: &[NicAttachment],
) -> Vec<NicAttachment> {
    let trailing = existing_nics.iter().skip(1).cloned();
    if switch_id == "none" {
        return trailing.collect();
    }
    if switch_id == "consomme" {
        let primary = match existing_nics.first() {
            Some(existing) if matches!(existing.backend, NicBackendConfig::Consomme { .. }) => {
                existing.clone()
            }
            Some(existing) => {
                let mut nic = NicAttachment::default_consomme();
                nic.id = existing.id;
                nic.mac_address = existing.mac_address.clone();
                nic
            }
            None => NicAttachment::default_consomme(),
        };
        let mut out = vec![primary];
        out.extend(trailing);
        return out;
    }
    let sw = switches.iter().find(|s| s.id == switch_id);
    if sw.is_some_and(|s| s.kind == "bridge") {
        // Linux bridges are not TAP devices; keep existing NICs or fall back to Consomme rather than emitting an unusable TAP config.
        if !existing_nics.is_empty() {
            return existing_nics.to_vec();
        }
        return vec![NicAttachment::default_consomme()];
    }
    let mac = existing_nics
        .first()
        .map(|n| n.mac_address.clone())
        .unwrap_or_else(generate_hyperv_mac);
    let backend = match sw.map(|s| s.kind.as_str()) {
        Some("tap") => NicBackendConfig::Tap {
            name: switch_id.to_string(),
        },
        _ => NicBackendConfig::VmSwitch {
            switch_id: switch_id.to_string(),
            switch_name: sw.map(|s| s.name.clone()),
        },
    };
    let mut out = vec![NicAttachment {
        id: existing_nics
            .first()
            .map(|n| n.id)
            .unwrap_or_else(Uuid::new_v4),
        name: sw
            .map(|s| s.name.clone())
            .unwrap_or_else(|| format!("Switch ({switch_id})")),
        mac_address: mac,
        backend,
    }];
    out.extend(trailing);
    out
}

pub fn render_file_picker(
    ctx: &egui::Context,
    picker_opt: &mut Option<FilePickerModal>,
) -> Option<(FilePickerTarget, PathBuf)> {
    let mut chosen: Option<(FilePickerTarget, PathBuf)> = None;
    let mut close = false;
    if let Some(picker) = picker_opt.as_mut() {
        picker.repaint_ctx = Some(ctx.clone());
        picker.poll_load();
        if picker.loading {
            ctx.request_repaint_after(std::time::Duration::from_millis(50));
        }
        egui::Window::new(if picker.save_mode {
            "Choose Output File Path"
        } else {
            "Select File"
        })
        .collapsible(false)
        .resizable(true)
        .default_size([620.0, 440.0])
        .show(ctx, |ui| {
            ui.horizontal(|ui| {
                if ui.button("⬆ Up").clicked()
                    && let Some(parent) = picker.current_dir.parent().map(PathBuf::from)
                {
                    picker.navigate_to(parent);
                }
                let dir_resp =
                    ui.add(egui::TextEdit::singleline(&mut picker.dir_input).desired_width(400.0));
                if dir_resp.changed() {
                    picker.dir_input_edited = true;
                }
                let enter_pressed =
                    dir_resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
                let go_clicked = ui.button("Go").clicked();
                let edited_away = dir_resp.lost_focus()
                    && picker.dir_input_edited
                    && picker.dir_input.trim() != picker.current_dir.to_string_lossy();
                if go_clicked || enter_pressed || edited_away {
                    let p = PathBuf::from(picker.dir_input.trim());
                    picker.navigate_to(p);
                }
                if ui.button("↻").clicked() {
                    picker.reload();
                }
                if picker.loading {
                    ui.spinner();
                    ui.label("Loading directory…");
                }
            });

            if let Some(err) = &picker.error {
                ui.colored_label(Color32::LIGHT_RED, err);
            }

            ui.separator();
            egui::ScrollArea::vertical()
                .id_salt("file_picker_scroll")
                .max_height(290.0)
                .show(ui, |ui| {
                    let allow_file_click = picker
                        .ignore_file_clicks_until
                        .is_none_or(|until| std::time::Instant::now() >= until);
                    let entries = picker.entries.clone();
                    ui.push_id(picker.load_seq, |ui| {
                        for (name, path, is_dir) in entries {
                            let label = if is_dir {
                                format!("📁 {name}")
                            } else {
                                format!("📄 {name}")
                            };
                            let selected = !is_dir && picker.filename_input == name;
                            let resp = ui.selectable_label(selected, label);
                            if resp.clicked() {
                                if is_dir {
                                    picker.navigate_to(path.clone());
                                } else if allow_file_click {
                                    picker.filename_input = name;
                                }
                            }
                            if resp.double_clicked() && !is_dir && allow_file_click {
                                chosen = Some((picker.target, path));
                                close = true;
                            }
                        }
                    });
                });

            ui.separator();
            ui.horizontal(|ui| {
                ui.label("File:");
                ui.add(
                    egui::TextEdit::singleline(&mut picker.filename_input).desired_width(380.0),
                );
                if ui.button("OK").clicked() && !picker.filename_input.trim().is_empty() {
                    let raw = PathBuf::from(picker.filename_input.trim());
                    let resolved = if raw.is_absolute() {
                        raw
                    } else {
                        picker.current_dir.join(raw)
                    };
                    chosen = Some((picker.target, resolved));
                    close = true;
                }
                if ui.button("Cancel").clicked() {
                    close = true;
                }
            });
        });
    }
    if close {
        *picker_opt = None;
    }
    chosen
}

pub fn render_new_vm_wizard(
    ctx: &egui::Context,
    wizard_opt: &mut Option<NewVmWizard>,
    switches: &[VirtualSwitchInfo],
    backend: &BackendHandle,
    file_picker: &mut Option<FilePickerModal>,
) {
    let mut close = false;
    if let Some(w) = wizard_opt.as_mut() {
        egui::Window::new("New Virtual Machine Wizard")
            .collapsible(false)
            .resizable(true)
            .default_size([620.0, 480.0])
            .show(ctx, |ui| {
                let steps = [
                    "1. Name & Preset",
                    "2. Compute & Security",
                    "3. FastDesk Display",
                    "4. Networking",
                    "5. Storage & ISO",
                ];
                ui.horizontal_wrapped(|ui| {
                    for (idx, title) in steps.iter().enumerate() {
                        let selected = w.step == idx;
                        if ui.selectable_label(selected, *title).clicked() {
                            w.step = idx;
                        }
                    }
                });
                ui.separator();

                match w.step {
                    0 => {
                        ui.heading("Specify Name and Guest Preset");
                        ui.add_space(6.0);
                        egui::Grid::new("wiz_step0").num_columns(2).spacing([12.0, 8.0]).show(
                            ui,
                            |ui| {
                                ui.label("VM Name:");
                                ui.add(egui::TextEdit::singleline(&mut w.name).desired_width(340.0));
                                ui.end_row();

                                ui.label("Description:");
                                ui.add(
                                    egui::TextEdit::singleline(&mut w.description)
                                        .desired_width(340.0),
                                );
                                ui.end_row();

                                ui.label("Preset:");
                                ui.horizontal(|ui| {
                                    for p in [
                                        VmPreset::Windows11,
                                        VmPreset::LinuxDesktop,
                                        VmPreset::Custom,
                                    ] {
                                        if ui.selectable_label(w.preset == p, p.to_string()).clicked()
                                        {
                                            w.apply_preset(p);
                                        }
                                    }
                                });
                                ui.end_row();
                            },
                        );
                    }
                    1 => {
                        ui.heading("Assign Memory, Processors & Firmware Security");
                        ui.add_space(6.0);
                        egui::Grid::new("wiz_step1").num_columns(2).spacing([12.0, 8.0]).show(
                            ui,
                            |ui| {
                                ui.label("Virtual CPUs:");
                                ui.add(egui::Slider::new(&mut w.vcpus, 1..=256));
                                ui.end_row();

                                ui.label("Startup Memory (MiB):");
                                ui.horizontal(|ui| {
                                    let max_mem = w.memory_mb.clamp(262_144, MAX_VM_MEMORY_MB);
                                    ui.add(egui::DragValue::new(&mut w.memory_mb).range(128..=max_mem).speed(256));
                                    for (label, mb) in [("4G", 4096), ("8G", 8192), ("16G", 16384), ("32G", 32768)] {
                                        if ui.button(label).clicked() {
                                            w.memory_mb = mb;
                                        }
                                    }
                                });
                                ui.end_row();

                                ui.label("UEFI Secure Boot:");
                                ui.checkbox(&mut w.secure_boot_enabled, "Enable Secure Boot");
                                ui.end_row();

                                ui.label("Secure Boot Template:");
                                egui::ComboBox::from_id_salt("wiz_sb_tpl")
                                    .selected_text(w.secure_boot_template.to_string())
                                    .show_ui(ui, |ui| {
                                        for tpl in [
                                            SecureBootTemplateConfig::MicrosoftWindows,
                                            SecureBootTemplateConfig::MicrosoftUefiCa,
                                            SecureBootTemplateConfig::None,
                                        ] {
                                            ui.selectable_value(
                                                &mut w.secure_boot_template,
                                                tpl,
                                                tpl.to_string(),
                                            );
                                        }
                                    });
                                ui.end_row();

                                ui.label("Nested Virtualization:");
                                ui.checkbox(&mut w.nested_virt, "Expose hardware virtualization extensions");
                                ui.end_row();

                                ui.label("vTPM 2.0:");
                                ui.checkbox(&mut w.tpm_enabled, "Enable Trusted Platform Module 2.0")
                                    .on_hover_text("Requires openvmm built with --features tpm");
                                ui.end_row();
                            },
                        );
                    }
                    2 => {
                        ui.heading("FastDesk 2D Accelerated Display & Input");
                        ui.add_space(6.0);
                        ui.checkbox(&mut w.fastdesk_enabled, "Enable FastDesk Synthetic Video / Keyboard / Mouse");
                        ui.add_enabled_ui(w.fastdesk_enabled, |ui| {
                            egui::Grid::new("wiz_step2").num_columns(2).spacing([12.0, 8.0]).show(
                                ui,
                                |ui| {
                                    ui.label("Video Memory (VRAM MiB):");
                                    ui.horizontal(|ui| {
                                        ui.add(egui::Slider::new(&mut w.vram_mb, 8..=256));
                                        ui.label("(32 MiB supports 4K)");
                                    });
                                    ui.end_row();

                                    ui.label("Zero-Copy Shared VRAM:");
                                    ui.checkbox(&mut w.use_shm, "Use local SHM backing file for zero-copy viewer");
                                    ui.end_row();

                                    ui.label("Transport:");
                                    ui.horizontal(|ui| {
                                        ui.radio_value(&mut w.transport, FastDeskTransport::Tcp, "TCP Loopback");
                                        ui.radio_value(&mut w.transport, FastDeskTransport::HvSock, "AF_HYPERV / AF_VSOCK");
                                    });
                                    ui.end_row();
                                },
                            );
                        });
                    }
                    3 => {
                        ui.heading("Configure Virtual Networking");
                        ui.add_space(6.0);
                        ui.radio_value(&mut w.selected_switch_id, "none".to_string(), "Not Connected");
                        for sw in switches {
                            ui.radio_value(
                                &mut w.selected_switch_id,
                                sw.id.clone(),
                                format!("{} [{}] — {}", sw.name, sw.kind, sw.description),
                            );
                        }
                    }
                    _ => {
                        ui.heading("Connect Virtual Hard Disk & Installer ISO");
                        ui.add_space(6.0);
                        ui.horizontal(|ui| {
                            ui.selectable_value(
                                &mut w.disk_mode,
                                DiskMode::CreateNew,
                                "Create a new virtual hard disk",
                            );
                            ui.selectable_value(
                                &mut w.disk_mode,
                                DiskMode::UseExisting,
                                "Use an existing virtual hard disk",
                            );
                            ui.selectable_value(&mut w.disk_mode, DiskMode::None, "Attach later");
                        });
                        ui.add_space(6.0);
                        match w.disk_mode {
                            DiskMode::CreateNew => {
                                egui::Grid::new("wiz_disk_new").num_columns(2).spacing([10.0, 6.0]).show(
                                    ui,
                                    |ui| {
                                        ui.label("Disk Path (optional):");
                                        let auto_hint = w.auto_disk_hint();
                                        ui.horizontal(|ui| {
                                            ui.add(
                                                egui::TextEdit::singleline(&mut w.new_disk_path)
                                                    .hint_text(auto_hint)
                                                    .desired_width(300.0),
                                            );
                                            if ui.button("Browse…").clicked() {
                                                let initial = if w.new_disk_path.trim().is_empty() {
                                                    w.suggested_disk_path()
                                                } else {
                                                    w.new_disk_path.clone()
                                                };
                                                *file_picker = Some(FilePickerModal::open(
                                                    FilePickerTarget::WizardNewDiskPath,
                                                    &initial,
                                                    true,
                                                    vec!["vhdx", "img", "raw"],
                                                ));
                                            }
                                        });
                                        ui.end_row();

                                        ui.label("Size (GiB):");
                                        ui.add(egui::DragValue::new(&mut w.new_disk_size_gb).range(1..=4096));
                                        ui.end_row();

                                        ui.label("Format:");
                                        ui.horizontal(|ui| {
                                            let prev_format = w.new_disk_format;
                                            ui.radio_value(&mut w.new_disk_format, DiskFormat::Vhdx, "VHDX");
                                            ui.radio_value(&mut w.new_disk_format, DiskFormat::Raw, "Raw (.img)");
                                            if w.new_disk_format != prev_format {
                                                sync_disk_path_extension(&mut w.new_disk_path, w.new_disk_format);
                                            }
                                            ui.checkbox(&mut w.new_disk_fixed, "Fixed size");
                                        });
                                        ui.end_row();
                                    },
                                );
                            }
                            DiskMode::UseExisting => {
                                ui.horizontal(|ui| {
                                    ui.label("Existing Disk:");
                                    ui.add(egui::TextEdit::singleline(&mut w.existing_disk_path).desired_width(320.0));
                                    if ui.button("Browse…").clicked() {
                                        *file_picker = Some(FilePickerModal::open(
                                            FilePickerTarget::WizardExistingDisk,
                                            &w.existing_disk_path,
                                            false,
                                            vec!["vhdx", "vhd", "img", "raw"],
                                        ));
                                    }
                                });
                            }
                            DiskMode::None => {
                                ui.label("You can attach a disk later in Storage & ISO.");
                            }
                        }

                        ui.separator();
                        ui.horizontal(|ui| {
                            ui.label("Installer ISO (optional):");
                            ui.add(egui::TextEdit::singleline(&mut w.iso_path).desired_width(300.0));
                            if ui.button("Browse…").clicked() {
                                *file_picker = Some(FilePickerModal::open(
                                    FilePickerTarget::WizardIso,
                                    &w.iso_path,
                                    false,
                                    vec!["iso"],
                                ));
                            }
                        });
                    }
                }

                if let Some(err) = &w.error {
                    ui.add_space(6.0);
                    ui.colored_label(Color32::LIGHT_RED, err);
                }

                ui.add_space(12.0);
                ui.separator();
                ui.horizontal(|ui| {
                    if ui.add_enabled(!w.submitting, egui::Button::new("Cancel")).clicked() {
                        close = true;
                    }
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        if w.step == 4 {
                            let finish_label = if w.submitting {
                                "Creating…"
                            } else {
                                "Finish & Create VM"
                            };
                            if ui
                                .add_enabled(
                                    !w.submitting,
                                    egui::Button::new(RichText::new(finish_label).strong()),
                                )
                                .clicked()
                            {
                                if let Err(msg) = w.validate() {
                                    w.error = Some(msg);
                                    return;
                                }
                                w.error = None;
                                w.submitting = true;
                                let payload = w.build_create_payload(switches);
                                backend.send(GuiRequest::CreateVm {
                                    wizard_id: w.wizard_id,
                                    payload,
                                });
                            }
                        } else if ui.button("Next >").clicked() {
                            w.step += 1;
                        }
                        if w.step > 0 && ui.button("< Back").clicked() {
                            w.step -= 1;
                        }
                    });
                });
            });
    }
    if close {
        *wizard_opt = None;
    }
}

impl PartialEq for DiskMode {
    fn eq(&self, other: &Self) -> bool {
        core::mem::discriminant(self) == core::mem::discriminant(other)
    }
}

pub fn render_vm_settings_dialog(
    ctx: &egui::Context,
    dlg_opt: &mut Option<VmSettingsDialog>,
    switches: &[VirtualSwitchInfo],
    backend: &BackendHandle,
    file_picker: &mut Option<FilePickerModal>,
) {
    let mut close = false;
    if let Some(d) = dlg_opt.as_mut() {
        egui::Window::new(format!("Settings — {}", d.name))
            .id(egui::Id::new(("vm_settings_dialog", d.vm_id)))
            .collapsible(false)
            .resizable(true)
            .default_size([560.0, 440.0])
            .show(ctx, |ui| {
                if !d.is_offline {
                    ui.colored_label(
                        Color32::from_rgb(240, 190, 70),
                        "Hardware settings are read-only while VM is active. Turn off VM to modify CPU, RAM, or firmware.",
                    );
                    ui.separator();
                }

                egui::Grid::new("vm_settings_grid").num_columns(2).spacing([12.0, 8.0]).show(
                    ui,
                    |ui| {
                        ui.label("Name:");
                        ui.add(egui::TextEdit::singleline(&mut d.name).desired_width(300.0));
                        ui.end_row();

                        ui.label("Description:");
                        ui.add(egui::TextEdit::singleline(&mut d.description).desired_width(300.0));
                        ui.end_row();
                    },
                );

                ui.add_enabled_ui(d.is_offline, |ui| {
                    ui.separator();
                    egui::Grid::new("vm_settings_hw").num_columns(2).spacing([12.0, 8.0]).show(
                        ui,
                        |ui| {
                            ui.label("Virtual CPUs:");
                            ui.add(egui::Slider::new(&mut d.vcpus, 1..=256));
                            ui.end_row();

                            ui.label("Memory (MiB):");
                            let max_mem = d.memory_mb.clamp(262_144, MAX_VM_MEMORY_MB);
                            ui.add(egui::DragValue::new(&mut d.memory_mb).range(128..=max_mem).speed(256));
                            ui.end_row();

                            ui.label("Secure Boot:");
                            ui.checkbox(&mut d.secure_boot_enabled, "Enable UEFI Secure Boot");
                            ui.end_row();

                            ui.label("SB Template:");
                            egui::ComboBox::from_id_salt("dlg_sb_tpl")
                                .selected_text(d.secure_boot_template.to_string())
                                .show_ui(ui, |ui| {
                                    for tpl in [
                                        SecureBootTemplateConfig::MicrosoftWindows,
                                        SecureBootTemplateConfig::MicrosoftUefiCa,
                                        SecureBootTemplateConfig::None,
                                    ] {
                                        ui.selectable_value(
                                            &mut d.secure_boot_template,
                                            tpl,
                                            tpl.to_string(),
                                        );
                                    }
                                });
                            ui.end_row();

                            ui.label("Nested Virt:");
                            ui.checkbox(&mut d.nested_virt, "Expose hardware virtualization");
                            ui.end_row();

                            ui.label("vTPM 2.0:");
                            ui.checkbox(&mut d.tpm_enabled, "Enable Trusted Platform Module 2.0");
                            ui.end_row();

                            ui.label("UEFI Firmware:");
                            ui.horizontal(|ui| {
                                ui.add(egui::TextEdit::singleline(&mut d.uefi_firmware).desired_width(240.0));
                                if ui.button("Browse…").clicked() {
                                    *file_picker = Some(FilePickerModal::open(
                                        FilePickerTarget::SettingsFirmwarePath,
                                        &d.uefi_firmware,
                                        false,
                                        vec!["fd", "bin"],
                                    ));
                                }
                            });
                            ui.end_row();

                            ui.label("FastDesk Display:");
                            ui.checkbox(&mut d.fastdesk_enabled, "Enabled");
                            ui.end_row();

                            ui.label("VRAM (MiB):");
                            ui.add(egui::Slider::new(&mut d.vram_mb, 8..=256));
                            ui.end_row();

                            ui.label("Shared VRAM (SHM):");
                            ui.checkbox(&mut d.use_shm, "Zero-copy SHM");
                            ui.end_row();

                            ui.label("Network Adapter:");
                            egui::ComboBox::from_id_salt("dlg_net_sw")
                                .selected_text(&d.selected_switch_id)
                                .show_ui(ui, |ui| {
                                    ui.selectable_value(
                                        &mut d.selected_switch_id,
                                        "none".to_string(),
                                        "Not Connected",
                                    );
                                    for sw in switches {
                                        ui.selectable_value(
                                            &mut d.selected_switch_id,
                                            sw.id.clone(),
                                            format!("{} ({})", sw.name, sw.kind),
                                        );
                                    }
                                });
                            ui.end_row();
                        },
                    );
                });

                if let Some(err) = &d.error {
                    ui.add_space(6.0);
                    ui.colored_label(Color32::LIGHT_RED, err);
                }

                ui.separator();
                ui.horizontal(|ui| {
                    let save_label = if d.submitting { "Saving…" } else { "Save Changes" };
                    if ui.add_enabled(!d.submitting, egui::Button::new(save_label)).clicked() {
                        let trimmed = d.name.trim();
                        if let Err(err) = openvmms_core::models::validate_vm_name(trimmed) {
                            d.error = Some(err.to_string());
                            return;
                        }
                        if !d.is_offline && d.has_hardware_changes() {
                            d.error = Some(
                                "Cannot save hardware changes while the VM is active. Turn off the VM first, or revert hardware changes to save only name/description."
                                    .to_string(),
                            );
                            return;
                        }
                        let payload = d.build_update_payload(switches);
                        if payload.is_empty() {
                            close = true;
                        } else {
                            d.error = None;
                            d.submitting = true;
                            backend.send(GuiRequest::UpdateVm {
                                id: d.vm_id,
                                payload,
                            });
                        }
                    }
                    if ui.add_enabled(!d.submitting, egui::Button::new("Cancel")).clicked() {
                        close = true;
                    }
                });
            });
    }
    if close {
        *dlg_opt = None;
    }
}

pub fn render_disk_manager_dialog(
    ctx: &egui::Context,
    dlg_opt: &mut Option<DiskManagerDialog>,
    backend: &BackendHandle,
    file_picker: &mut Option<FilePickerModal>,
) {
    let mut close = false;
    if let Some(d) = dlg_opt.as_mut() {
        egui::Window::new("Virtual Hard Disk Manager")
            .collapsible(false)
            .resizable(true)
            .default_size([560.0, 360.0])
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.selectable_value(&mut d.tab, 0, "Create New Disk");
                    ui.selectable_value(&mut d.tab, 1, "Inspect Disk");
                });
                ui.separator();

                if d.tab == 0 {
                    egui::Grid::new("disk_mgr_create").num_columns(2).spacing([10.0, 8.0]).show(
                        ui,
                        |ui| {
                            ui.label("Output Path:");
                            ui.horizontal(|ui| {
                                ui.add(egui::TextEdit::singleline(&mut d.create_path).desired_width(320.0));
                                if ui.button("Browse…").clicked() {
                                    *file_picker = Some(FilePickerModal::open(
                                        FilePickerTarget::DiskMgrCreatePath,
                                        &d.create_path,
                                        true,
                                        vec!["vhdx", "img", "raw"],
                                    ));
                                }
                            });
                            ui.end_row();

                            ui.label("Format:");
                            ui.horizontal(|ui| {
                                let prev_format = d.create_format;
                                ui.radio_value(&mut d.create_format, DiskFormat::Vhdx, "VHDX");
                                ui.radio_value(&mut d.create_format, DiskFormat::Raw, "Raw (.img)");
                                if d.create_format != prev_format {
                                    sync_disk_path_extension(&mut d.create_path, d.create_format);
                                }
                                ui.checkbox(&mut d.create_fixed, "Fixed allocation");
                            });
                            ui.end_row();

                            ui.label("Size (GiB):");
                            ui.add(egui::DragValue::new(&mut d.create_size_gb).range(1..=4096));
                            ui.end_row();
                        },
                    );
                    ui.add_space(10.0);
                    if ui.button("Create Virtual Disk").clicked() {
                        sync_disk_path_extension(&mut d.create_path, d.create_format);
                        backend.send(GuiRequest::CreateDisk(CreateDiskPayload {
                            path: PathBuf::from(d.create_path.trim()),
                            size_bytes: d.create_size_gb.saturating_mul(1024 * 1024 * 1024),
                            format: d.create_format,
                            fixed: d.create_fixed,
                        }));
                    }
                } else {
                    ui.horizontal(|ui| {
                        ui.label("Disk Path:");
                        ui.add(egui::TextEdit::singleline(&mut d.inspect_path).desired_width(320.0));
                        if ui.button("Browse…").clicked() {
                            *file_picker = Some(FilePickerModal::open(
                                FilePickerTarget::DiskMgrInspectPath,
                                &d.inspect_path,
                                false,
                                vec!["vhdx", "vhd", "img", "raw", "iso"],
                            ));
                        }
                        if ui.button("Inspect").clicked() && !d.inspect_path.trim().is_empty() {
                            backend.send(GuiRequest::InspectDisk(PathBuf::from(
                                d.inspect_path.trim(),
                            )));
                        }
                    });
                }

                if let Some(info) = &d.last_info {
                    ui.separator();
                    ui.label(RichText::new("Disk Properties").strong());
                    egui::Grid::new("disk_info_grid").num_columns(2).spacing([12.0, 4.0]).show(
                        ui,
                        |ui| {
                            ui.label("Path:");
                            ui.label(info.path.display().to_string());
                            ui.end_row();
                            ui.label("Format / Type:");
                            ui.label(format!("{} ({})", info.format, info.disk_type));
                            ui.end_row();
                            ui.label("Virtual Size:");
                            ui.label(format_bytes(info.virtual_size_bytes));
                            ui.end_row();
                            ui.label("File Size on Disk:");
                            ui.label(format_bytes(info.file_size_bytes));
                            ui.end_row();
                        },
                    );
                }

                ui.separator();
                if ui.button("Close").clicked() {
                    close = true;
                }
            });
    }
    if close {
        *dlg_opt = None;
    }
}

pub fn render_host_settings_dialog(
    ctx: &egui::Context,
    dlg_opt: &mut Option<HostSettingsDialog>,
    host_info: Option<&HostInfo>,
    backend: &BackendHandle,
    file_picker: &mut Option<FilePickerModal>,
) {
    let mut close = false;
    if let Some(d) = dlg_opt.as_mut() {
        egui::Window::new("Host & Binary Settings")
            .collapsible(false)
            .resizable(true)
            .default_size([640.0, 380.0])
            .show(ctx, |ui| {
                if let Some(info) = host_info {
                    ui.label(format!(
                        "Host: {} ({} {})  |  Data Dir: {}",
                        info.hostname,
                        info.os,
                        info.arch,
                        info.data_dir.display()
                    ));
                    ui.separator();
                }

                egui::Grid::new("host_cfg_grid").num_columns(2).spacing([10.0, 8.0]).show(
                    ui,
                    |ui| {
                        ui.label("Daemon Listen Addr:");
                        ui.add(egui::TextEdit::singleline(&mut d.listen_addr).desired_width(240.0));
                        ui.end_row();

                        path_row(
                            ui,
                            "openvmm Binary:",
                            &mut d.openvmm_bin,
                            FilePickerTarget::HostOpenVmmBin,
                            file_picker,
                        );
                        path_row(
                            ui,
                            "fastdesk-viewer Binary:",
                            &mut d.fastdesk_viewer_bin,
                            FilePickerTarget::HostViewerBin,
                            file_picker,
                        );
                        path_row(
                            ui,
                            "openvmm-img Binary:",
                            &mut d.openvmm_img_bin,
                            FilePickerTarget::HostImgBin,
                            file_picker,
                        );
                        path_row(
                            ui,
                            "UEFI Firmware (MSVM.fd):",
                            &mut d.uefi_firmware_path,
                            FilePickerTarget::HostUefiFirmware,
                            file_picker,
                        );
                    },
                );

                if let Some(info) = host_info {
                    ui.separator();
                    ui.label(RichText::new("Active Discovered Paths").strong());
                    ui.label(format!(
                        "openvmm: {} | viewer: {} | img: {} | UEFI: {}",
                        opt_status(info.resolved_paths.openvmm_bin.as_deref()),
                        opt_status(info.resolved_paths.fastdesk_viewer_bin.as_deref()),
                        opt_status(info.resolved_paths.openvmm_img_bin.as_deref()),
                        opt_status(info.resolved_paths.uefi_firmware_path.as_deref()),
                    ));
                }

                if let Some(err) = &d.error {
                    ui.add_space(6.0);
                    ui.colored_label(Color32::LIGHT_RED, err);
                }

                ui.separator();
                ui.horizontal(|ui| {
                    let save_label = if d.submitting {
                        "Saving…"
                    } else {
                        "Save Host Settings"
                    };
                    if ui.add_enabled(!d.submitting, egui::Button::new(save_label)).clicked() {
                        let mut cfg = d.base_config.clone();
                        cfg.listen_addr = d.listen_addr.trim().to_string();
                        cfg.openvmm_bin = string_to_opt(&d.openvmm_bin);
                        cfg.fastdesk_viewer_bin = string_to_opt(&d.fastdesk_viewer_bin);
                        cfg.openvmm_img_bin = string_to_opt(&d.openvmm_img_bin);
                        cfg.uefi_firmware_path = string_to_opt(&d.uefi_firmware_path);
                        d.error = None;
                        d.submitting = true;
                        backend.send(GuiRequest::SaveHostConfig(cfg));
                    }
                    if ui.add_enabled(!d.submitting, egui::Button::new("Cancel")).clicked() {
                        close = true;
                    }
                });
            });
    }
    if close {
        *dlg_opt = None;
    }
}

fn path_row(
    ui: &mut egui::Ui,
    label: &str,
    value: &mut String,
    target: FilePickerTarget,
    file_picker: &mut Option<FilePickerModal>,
) {
    ui.label(label);
    ui.horizontal(|ui| {
        ui.add(egui::TextEdit::singleline(value).desired_width(360.0));
        if ui.button("Browse…").clicked() {
            *file_picker = Some(FilePickerModal::open(target, value, false, vec![]));
        }
    });
    ui.end_row();
}

fn opt_status(p: Option<&std::path::Path>) -> String {
    p.map(|x| x.file_name().unwrap_or_default().to_string_lossy().into_owned())
        .unwrap_or_else(|| "missing".to_string())
}

pub fn render_delete_vm_dialog(
    ctx: &egui::Context,
    dlg_opt: &mut Option<DeleteVmDialog>,
    backend: &BackendHandle,
) {
    let mut close = false;
    if let Some(d) = dlg_opt.as_mut() {
        egui::Window::new("Delete Virtual Machine")
            .id(egui::Id::new(("delete_vm_dialog", d.vm_id)))
            .collapsible(false)
            .resizable(false)
            .show(ctx, |ui| {
                ui.label(format!(
                    "Are you sure you want to delete virtual machine '{}'?",
                    d.vm_name
                ));
                ui.checkbox(
                    &mut d.delete_disks,
                    "Also delete writable virtual hard disk files attached to this VM",
                );
                if let Some(err) = &d.error {
                    ui.add_space(6.0);
                    ui.colored_label(Color32::LIGHT_RED, err);
                }
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    let del_label = if d.submitting { "Deleting…" } else { "Delete" };
                    if ui
                        .add_enabled(
                            !d.submitting,
                            egui::Button::new(RichText::new(del_label).color(Color32::LIGHT_RED)),
                        )
                        .clicked()
                    {
                        d.error = None;
                        d.submitting = true;
                        backend.send(GuiRequest::DeleteVm {
                            id: d.vm_id,
                            delete_disks: d.delete_disks,
                        });
                    }
                    if ui.add_enabled(!d.submitting, egui::Button::new("Cancel")).clicked() {
                        close = true;
                    }
                });
            });
    }
    if close {
        *dlg_opt = None;
    }
}

pub fn render_confirm_action_dialog(
    ctx: &egui::Context,
    dlg_opt: &mut Option<ConfirmActionDialog>,
    backend: &BackendHandle,
) {
    let mut close = false;
    if let Some(dlg) = dlg_opt.as_ref() {
        let (title, body, confirm_label, req) = match &dlg.action {
            PendingConfirmAction::TurnOff { id, vm_name } => (
                "Turn Off Virtual Machine",
                format!(
                    "Force-turn off '{vm_name}' immediately?\n\nThis cuts power without a guest shutdown and may cause unsaved data loss."
                ),
                "Turn Off",
                GuiRequest::StopVm { id: *id },
            ),
            PendingConfirmAction::Reset { id, vm_name } => (
                "Reset Virtual Machine",
                format!(
                    "Force-reset '{vm_name}' immediately?\n\nThis performs a hard reboot without shutting down the guest OS cleanly."
                ),
                "Reset",
                GuiRequest::ResetVm { id: *id },
            ),
            PendingConfirmAction::DetachDisk {
                id,
                vm_name,
                lun,
                path,
                vm_active,
            } => {
                let live_note = if *vm_active {
                    "\n\nThis will hot-unplug the disk while the VM is running."
                } else {
                    ""
                };
                (
                    "Detach Virtual Hard Disk",
                    format!(
                        "Detach SCSI LUN {lun} ({}) from '{vm_name}'?{live_note}",
                        path.display()
                    ),
                    "Detach",
                    GuiRequest::DetachDisk { id: *id, lun: *lun },
                )
            }
        };

        egui::Window::new(title)
            .id(egui::Id::new("confirm_destructive_action_dialog"))
            .collapsible(false)
            .resizable(false)
            .show(ctx, |ui| {
                ui.label(body);
                ui.add_space(10.0);
                ui.horizontal(|ui| {
                    if ui
                        .button(RichText::new(confirm_label).color(Color32::LIGHT_RED))
                        .clicked()
                    {
                        backend.send(req);
                        close = true;
                    }
                    if ui.button("Cancel").clicked() {
                        close = true;
                    }
                });
            });
    }
    if close {
        *dlg_opt = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openvmms_core::models::PortForward;
    use std::path::Path;

    #[test]
    fn new_vm_wizard_raw_format_syncs_default_extension() {
        let mut wizard = NewVmWizard::new(Path::new("/tmp/openvmms"));
        wizard.new_disk_path = "/tmp/openvmms/disks/custom.vhdx".to_string();
        wizard.new_disk_format = DiskFormat::Raw;
        let payload = wizard.build_create_payload(&[]);
        let disk = payload.create_disk.unwrap();
        assert_eq!(disk.format, DiskFormat::Raw);
        assert_eq!(disk.path.extension().and_then(|e| e.to_str()), Some("img"));
    }

    #[test]
    fn new_vm_wizards_do_not_share_a_default_disk_path() {
        let dir = tempfile::tempdir().unwrap();
        let mut first = NewVmWizard::new(dir.path());
        let mut second = NewVmWizard::new(dir.path());
        second.name = "Build Server".to_string();
        // Empty path lets the daemon allocate a unique `<name>-<id>-disk0` file per VM.
        let a = first.build_create_payload(&[]).create_disk.unwrap();
        let b = second.build_create_payload(&[]).create_disk.unwrap();
        assert!(a.path.as_os_str().is_empty());
        assert!(b.path.as_os_str().is_empty());
        // Hint and Browse suggestions follow the VM name and format, and avoid existing files.
        assert_eq!(first.auto_disk_hint(), "auto: Windows-11-VM-<id>-disk0.vhdx");
        assert!(first.suggested_disk_path().ends_with("Windows-11-VM.vhdx"));
        std::fs::create_dir_all(&first.default_disk_dir).unwrap();
        std::fs::write(first.default_disk_dir.join("Windows-11-VM.vhdx"), b"existing").unwrap();
        assert!(first.suggested_disk_path().ends_with("Windows-11-VM-2.vhdx"));

        second.new_disk_format = DiskFormat::Raw;
        assert_eq!(second.auto_disk_hint(), "auto: Build-Server-<id>-disk0.img");
        assert!(second.suggested_disk_path().ends_with("Build-Server.img"));
    }

    #[test]
    fn new_vm_wizards_create_distinct_disks_via_daemon() {
        let dir = tempfile::tempdir().unwrap();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let daemon = rt
            .block_on(openvmmsd::TestDaemonHandle::spawn(dir.path().to_path_buf()))
            .unwrap();
        let client =
            openvmms_core::api_client::ApiClient::new(&daemon.base_url, &daemon.token).unwrap();

        let mut wiz1 = NewVmWizard::new(dir.path());
        wiz1.new_disk_format = DiskFormat::Raw;
        wiz1.new_disk_size_gb = 1;
        let vm1 = client.create_vm(&wiz1.build_create_payload(&[])).unwrap();

        let mut wiz2 = NewVmWizard::new(dir.path());
        wiz2.name = "Ubuntu-Dev".to_string();
        wiz2.new_disk_format = DiskFormat::Raw;
        wiz2.new_disk_size_gb = 1;
        let vm2 = client.create_vm(&wiz2.build_create_payload(&[])).unwrap();

        assert_eq!(vm1.config.disks.len(), 1);
        assert_eq!(vm2.config.disks.len(), 1);
        assert_ne!(vm1.config.disks[0].path, vm2.config.disks[0].path);
        assert!(vm1.config.disks[0].path.is_file());
        assert!(vm2.config.disks[0].path.is_file());

        rt.block_on(daemon.shutdown());
    }

    #[test]
    fn vm_settings_dialog_preserves_extra_nics_and_custom_fastdesk() {
        let mut cfg = VmConfig::from_preset("DualNic", VmPreset::Custom, Path::new("/tmp/vm"));
        cfg.fastdesk.listen_addr = "127.0.0.1:5999".to_string();
        cfg.fastdesk.vsock_port = Some(6000);
        cfg.nics.push(NicAttachment {
            id: Uuid::new_v4(),
            name: "Secondary TAP".to_string(),
            mac_address: generate_hyperv_mac(),
            backend: NicBackendConfig::Tap {
                name: "tap0".to_string(),
            },
        });
        if let NicBackendConfig::Consomme { port_forwards, .. } = &mut cfg.nics[0].backend {
            port_forwards.push(PortForward {
                protocol: openvmms_core::models::IpProto::Tcp,
                host_addr: "127.0.0.1".to_string(),
                host_port: 2222,
                guest_port: 22,
            });
        }

        let mut dlg = VmSettingsDialog::from_config(&cfg, VmStatus::Off);
        // Unmodified dialog must produce an empty diff payload so concurrent CLI edits are preserved.
        assert!(dlg.build_update_payload(&[]).is_empty());

        // Editing only description must leave all hardware/NIC fields as None.
        dlg.description = "edited description".to_string();
        let desc_only = dlg.build_update_payload(&[]);
        assert_eq!(desc_only.description.as_deref(), Some("edited description"));
        assert!(desc_only.vcpus.is_none());
        assert!(desc_only.memory_mb.is_none());
        assert!(desc_only.fastdesk.is_none());
        assert!(desc_only.nics.is_none());

        // Editing VRAM only must preserve custom FastDesk listen_addr/vsock_port while leaving other fields None.
        dlg.vram_mb = 64;
        let vram_payload = dlg.build_update_payload(&[]);
        let fd = vram_payload.fastdesk.unwrap();
        assert_eq!(fd.vram_mb, 64);
        assert_eq!(fd.listen_addr, "127.0.0.1:5999");
        assert_eq!(fd.vsock_port, Some(6000));
        assert!(vram_payload.vcpus.is_none());
        assert!(vram_payload.nics.is_none());

        // Switching NIC backend still preserves extra NICs on the update payload.
        let switches = vec![VirtualSwitchInfo {
            id: "tap1".to_string(),
            name: "tap1".to_string(),
            kind: "tap".to_string(),
            description: "Secondary test TAP".to_string(),
        }];
        dlg.selected_switch_id = "tap1".to_string();
        let switched_payload = dlg.build_update_payload(&switches);
        let nics = switched_payload.nics.unwrap();
        assert_eq!(nics.len(), 2);
        assert!(matches!(nics[0].backend, NicBackendConfig::Tap { ref name } if name == "tap1"));
        assert!(matches!(nics[1].backend, NicBackendConfig::Tap { ref name } if name == "tap0"));

        // Selecting "Not Connected" removes only the primary NIC and preserves trailing NICs.
        dlg.selected_switch_id = "none".to_string();
        let none_payload = dlg.build_update_payload(&switches);
        let remaining = none_payload.nics.unwrap();
        assert_eq!(remaining.len(), 1);
        assert!(matches!(remaining[0].backend, NicBackendConfig::Tap { ref name } if name == "tap0"));
    }

    fn wait_file_picker_loaded(picker: &mut FilePickerModal) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while picker.loading && std::time::Instant::now() < deadline {
            let _ = picker.poll_load();
            if picker.loading {
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
        }
        assert!(!picker.loading, "file picker load timed out");
    }

    #[test]
    fn file_picker_dir_input_persists_while_typing_and_navigates_on_go() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("subdir");
        std::fs::create_dir_all(&sub).unwrap();
        std::fs::write(sub.join("disk.img"), b"IMG").unwrap();

        let mut picker_opt = Some(FilePickerModal::open(
            FilePickerTarget::WizardExistingDisk,
            &dir.path().to_string_lossy(),
            false,
            vec!["img"],
        ));
        let ctx = egui::Context::default();
        let _ = ctx.run(egui::RawInput::default(), |ctx| {
            let _ = render_file_picker(ctx, &mut picker_opt);
        });

        // Simulate user editing dir_input across frames before committing navigation.
        picker_opt.as_mut().unwrap().dir_input = sub.to_string_lossy().into_owned();
        let _ = ctx.run(egui::RawInput::default(), |ctx| {
            let _ = render_file_picker(ctx, &mut picker_opt);
        });
        wait_file_picker_loaded(picker_opt.as_mut().unwrap());
        assert_eq!(
            picker_opt.as_ref().unwrap().dir_input,
            sub.to_string_lossy().as_ref(),
            "dir_input must not be overwritten across frames while typing"
        );

        let target_dir = PathBuf::from(&picker_opt.as_ref().unwrap().dir_input);
        picker_opt.as_mut().unwrap().navigate_to(target_dir);
        wait_file_picker_loaded(picker_opt.as_mut().unwrap());
        assert_eq!(picker_opt.as_ref().unwrap().current_dir, sub);
        assert!(
            picker_opt
                .as_ref()
                .unwrap()
                .entries
                .iter()
                .any(|(name, _, is_dir)| name == "disk.img" && !*is_dir)
        );
    }

    #[test]
    fn file_picker_open_and_navigate_are_nonblocking_and_drop_stale_loads() {
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("fast-sub");
        std::fs::create_dir_all(&sub).unwrap();
        std::fs::write(sub.join("ubuntu.iso"), b"ISO").unwrap();

        let mut picker = FilePickerModal::open(
            FilePickerTarget::MountIsoPath,
            &dir.path().to_string_lossy(),
            false,
            vec!["iso"],
        );
        wait_file_picker_loaded(&mut picker);

        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
        let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
        let stale_path = dir.path().join("stale-dir");
        let start = std::time::Instant::now();
        picker.start_load(dir.path().to_string_lossy().into_owned(), move |seq| {
            let _ = started_tx.send(());
            let _ = release_rx.recv();
            let dir_str = stale_path.to_string_lossy().into_owned();
            let res = FilePickerLoadResult {
                seq,
                resolved_dir: Some((stale_path, dir_str, None)),
                entries: vec![("stale.iso".to_string(), PathBuf::from("stale.iso"), false)],
                error: Some("stale error".to_string()),
            };
            let _ = done_tx.send(());
            res
        });
        assert!(
            start.elapsed() < std::time::Duration::from_millis(200),
            "start_load must return immediately without blocking UI thread"
        );
        started_rx.recv_timeout(std::time::Duration::from_secs(2)).unwrap();
        assert!(picker.loading);

        // Supersede the blocked load with a fast navigation.
        picker.navigate_to(sub.clone());
        wait_file_picker_loaded(&mut picker);
        assert_eq!(picker.current_dir, sub);
        assert!(
            picker
                .entries
                .iter()
                .any(|(name, _, is_dir)| name == "ubuntu.iso" && !*is_dir)
        );
        assert!(picker.error.is_none());

        // Release the stale load and verify it is ignored when polled.
        let _ = release_tx.send(());
        done_rx.recv_timeout(std::time::Duration::from_secs(2)).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert!(!picker.poll_load());
        assert_eq!(picker.current_dir, sub);
        assert!(!picker.entries.iter().any(|(name, _, _)| name == "stale.iso"));
        assert!(picker.error.is_none());
    }

    #[test]
    fn bridge_switch_is_never_mapped_to_tap_backend() {
        let switches = vec![VirtualSwitchInfo {
            id: "br0".to_string(),
            name: "br0".to_string(),
            kind: "bridge".to_string(),
            description: "Linux bridge".to_string(),
        }];
        let nics = build_nics_from_switch_id("br0", &switches, &[]);
        assert_eq!(nics.len(), 1);
        assert!(matches!(nics[0].backend, NicBackendConfig::Consomme { .. }));
    }

    #[test]
    fn vm_settings_dialog_rendering_preserves_max_vcpus_and_vram() {
        let mut cfg = VmConfig::from_preset("HighSpec", VmPreset::Custom, Path::new("/tmp/highspec"));
        cfg.vcpus = 128;
        cfg.memory_mb = 524_288;
        cfg.fastdesk.vram_mb = 256;
        cfg.validate().unwrap();

        let ctx = egui::Context::default();
        let backend = BackendHandle::spawn(None, None, ctx.clone());
        let mut dlg_opt = Some(VmSettingsDialog::from_config(&cfg, VmStatus::Off));
        let mut file_picker = None;
        let _ = ctx.run(egui::RawInput::default(), |ctx| {
            render_vm_settings_dialog(ctx, &mut dlg_opt, &[], &backend, &mut file_picker);
        });

        let dlg = dlg_opt.as_mut().unwrap();
        assert_eq!(dlg.vcpus, 128);
        assert_eq!(dlg.memory_mb, 524_288);
        assert_eq!(dlg.vram_mb, 256);

        // Unchanged dialog produces an empty diff; changing vram_mb emits only fastdesk with 256 preserved.
        assert!(dlg.build_update_payload(&[]).is_empty());
        dlg.memory_mb = 262_144;
        let payload = dlg.build_update_payload(&[]);
        assert_eq!(payload.memory_mb, Some(262_144));
        assert!(payload.vcpus.is_none());
        assert!(payload.fastdesk.is_none());

        // Wizard controls must also preserve supported upper/lower limits across renders.
        let mut wiz = NewVmWizard::new(Path::new("/tmp/wiz"));
        wiz.step = 1;
        wiz.vcpus = 256;
        wiz.memory_mb = 128;
        wiz.vram_mb = 256;
        let mut wiz_opt = Some(wiz);
        let _ = ctx.run(egui::RawInput::default(), |ctx| {
            render_new_vm_wizard(ctx, &mut wiz_opt, &[], &backend, &mut file_picker);
        });
        wiz_opt.as_mut().unwrap().step = 2;
        let _ = ctx.run(egui::RawInput::default(), |ctx| {
            render_new_vm_wizard(ctx, &mut wiz_opt, &[], &backend, &mut file_picker);
        });
        let wiz = wiz_opt.as_ref().unwrap();
        assert_eq!(wiz.vcpus, 256);
        assert_eq!(wiz.memory_mb, 128);
        assert_eq!(wiz.vram_mb, 256);
    }

    #[test]
    fn new_vm_wizard_rejects_blank_existing_disk_path() {
        let mut wiz = NewVmWizard::new(Path::new("/tmp/wiz"));
        wiz.disk_mode = DiskMode::UseExisting;
        wiz.existing_disk_path = "   ".to_string();
        assert!(wiz.validate().is_err());

        wiz.existing_disk_path = "/tmp/existing.vhdx".to_string();
        assert!(wiz.validate().is_ok());

        wiz.existing_disk_path = "/tmp/installer.iso".to_string();
        let payload = wiz.build_create_payload(&[]);
        assert_eq!(payload.disks[0].kind, DiskKind::Dvd);
        assert!(payload.disks[0].read_only);
        assert_eq!(payload.disks[0].format, DiskFormat::Iso);
    }

    #[test]
    fn vm_settings_dialog_concurrent_vm_updated_does_not_revert_cli_edits() {
        let mut cfg = VmConfig::from_preset("Concurrent", VmPreset::Custom, Path::new("/tmp/vm"));
        cfg.vcpus = 4;
        cfg.fastdesk.listen_addr = "127.0.0.1:5900".to_string();

        let mut dlg = VmSettingsDialog::from_config(&cfg, VmStatus::Off);

        // Simulate a concurrent CLI edit arriving via SSE/VmUpdated (mirroring GuiUpdate::VmUpdated):
        // vcpus changes 4 -> 8, fastdesk.listen_addr changes 5900 -> 5955, and an extra TAP NIC is added.
        let mut cli_cfg = cfg.clone();
        cli_cfg.vcpus = 8;
        cli_cfg.fastdesk.listen_addr = "127.0.0.1:5955".to_string();
        cli_cfg.nics.push(NicAttachment {
            id: Uuid::new_v4(),
            name: "CLI TAP".to_string(),
            mac_address: generate_hyperv_mac(),
            backend: NicBackendConfig::Tap {
                name: "tap0".to_string(),
            },
        });
        dlg.base_fastdesk = cli_cfg.fastdesk.clone();
        dlg.nics = cli_cfg.nics.clone();
        dlg.is_offline = true;

        // Editing only description must NOT revert the CLI's vcpus edit or emit fastdesk/nics.
        dlg.description = "gui description".to_string();
        let desc_only = dlg.build_update_payload(&[]);
        assert_eq!(desc_only.description.as_deref(), Some("gui description"));
        assert!(desc_only.vcpus.is_none());
        assert!(desc_only.fastdesk.is_none());
        assert!(desc_only.nics.is_none());

        // Editing vram_mb must emit fastdesk with the new CLI listen_addr preserved while leaving vcpus/nics None.
        dlg.vram_mb = 64;
        let vram_payload = dlg.build_update_payload(&[]);
        let fd = vram_payload.fastdesk.expect("expected fastdesk diff");
        assert_eq!(fd.vram_mb, 64);
        assert_eq!(fd.listen_addr, "127.0.0.1:5955");
        assert!(vram_payload.vcpus.is_none());
        assert!(vram_payload.nics.is_none());
    }

    #[test]
    fn vm_settings_dialog_has_hardware_changes_and_build_update_payload_never_drops_edits() {
        let mut cfg = VmConfig::from_preset("ActiveHw", VmPreset::Custom, Path::new("/tmp/vm"));
        cfg.vcpus = 4;
        cfg.memory_mb = 4096;

        let mut dlg = VmSettingsDialog::from_config(&cfg, VmStatus::Running);
        assert!(!dlg.is_offline);
        assert!(!dlg.has_hardware_changes());

        dlg.description = "only meta".to_string();
        assert!(!dlg.has_hardware_changes());

        dlg.vcpus = 8;
        dlg.memory_mb = 8192;
        assert!(dlg.has_hardware_changes());

        let payload = dlg.build_update_payload(&[]);
        assert_eq!(payload.vcpus, Some(8));
        assert_eq!(payload.memory_mb, Some(8192));
        assert_eq!(payload.description.as_deref(), Some("only meta"));
    }

    #[test]
    fn build_nics_preserves_id_and_mac_when_switching_to_consomme() {
        let nic_id = Uuid::new_v4();
        let mac = "00:15:5d:ab:cd:ef".to_string();
        let existing = vec![NicAttachment {
            id: nic_id,
            name: "eth-tap".to_string(),
            mac_address: mac.clone(),
            backend: NicBackendConfig::Tap {
                name: "tap0".to_string(),
            },
        }];

        let nics = build_nics_from_switch_id("consomme", &[], &existing);
        assert_eq!(nics.len(), 1);
        assert_eq!(nics[0].id, nic_id);
        assert_eq!(nics[0].mac_address, mac);
        assert!(matches!(nics[0].backend, NicBackendConfig::Consomme { .. }));
    }

    #[test]
    fn file_picker_folder_navigation_blocks_immediate_file_click_through() {
        let dir = tempfile::tempdir().unwrap();
        let subdir = dir.path().join("a_subdir");
        std::fs::create_dir_all(&subdir).unwrap();
        let inner_file = subdir.join("target.vhdx");
        std::fs::write(&inner_file, b"VHDX").unwrap();

        let mut modal = FilePickerModal::open(
            FilePickerTarget::AttachDiskPath,
            &dir.path().to_string_lossy(),
            false,
            vec!["vhdx"],
        );
        let start_seq = modal.load_seq;
        modal.navigate_to(subdir.clone());
        assert!(modal.load_seq > start_seq);
        assert!(modal.ignore_file_clicks_until.is_some());

        // Populate loaded entries immediately while the click-through cooldown is still active.
        modal.entries = vec![("target.vhdx".into(), inner_file.clone(), false)];
        modal.loading = false;
        let mut picker_opt = Some(modal);

        let ctx = egui::Context::default();
        let _ = ctx.run(egui::RawInput::default(), |ctx| {
            let chosen = render_file_picker(ctx, &mut picker_opt);
            assert!(chosen.is_none());
        });

        // Directly run frames with rapid double-clicks across rows in the scroll area to
        // verify click-through onto the file row is suppressed during the cooldown window.
        for pos in [
            egui::pos2(ctx.screen_rect().center().x, 120.0),
            egui::pos2(ctx.screen_rect().center().x, 150.0),
            egui::pos2(ctx.screen_rect().center().x, 180.0),
            egui::pos2(ctx.screen_rect().center().x, 210.0),
        ] {
            let mut input = egui::RawInput::default();
            input.events.push(egui::Event::PointerMoved(pos));
            input.events.push(egui::Event::PointerButton {
                pos,
                button: egui::PointerButton::Primary,
                pressed: true,
                modifiers: egui::Modifiers::NONE,
            });
            input.events.push(egui::Event::PointerButton {
                pos,
                button: egui::PointerButton::Primary,
                pressed: false,
                modifiers: egui::Modifiers::NONE,
            });
            input.events.push(egui::Event::PointerButton {
                pos,
                button: egui::PointerButton::Primary,
                pressed: true,
                modifiers: egui::Modifiers::NONE,
            });
            input.events.push(egui::Event::PointerButton {
                pos,
                button: egui::PointerButton::Primary,
                pressed: false,
                modifiers: egui::Modifiers::NONE,
            });
            let mut chosen = None;
            let _ = ctx.run(input, |ctx| {
                chosen = render_file_picker(ctx, &mut picker_opt);
            });
            assert!(chosen.is_none(), "double-click must not click through during cooldown");
            assert!(picker_opt.is_some());
            assert!(picker_opt.as_ref().unwrap().filename_input.is_empty());
        }
    }
}
