use anyhow::Context;
use anyhow::bail;
use clap::Args;
use clap::Parser;
use clap::Subcommand;
use clap::ValueEnum;
use openvmms_core::api_client::ApiClient;
use openvmms_core::api_client::ensure_daemon_running;
use openvmms_core::api_client::find_openvmmsd_binary;
use openvmms_core::api_client::launch_fastdesk_viewer;
use openvmms_core::api_client::resolve_client_path;
use openvmms_core::api_client::resolve_local_viewer_binary;
use openvmms_core::models::AttachDiskPayload;
use openvmms_core::models::CreateDiskPayload;
use openvmms_core::models::CreateVmPayload;
use openvmms_core::models::DiskAttachment;
use openvmms_core::models::DiskFormat;
use openvmms_core::models::DiskInfo;
use openvmms_core::models::DiskKind;
use openvmms_core::models::FastDeskSettings;
use openvmms_core::models::FastDeskTransport;
use openvmms_core::models::MountIsoPayload;
use openvmms_core::models::NicAttachment;
use openvmms_core::models::NicBackendConfig;
use openvmms_core::models::ShutdownVmPayload;
use openvmms_core::models::UpdateVmPayload;
use openvmms_core::models::VmPreset;
use openvmms_core::models::VmSummary;
use openvmms_core::models::default_data_dir;
use openvmms_core::models::format_bytes;
use openvmms_core::models::format_memory_mb;
use openvmms_core::models::generate_hyperv_mac;
use openvmms_core::models::parse_byte_size;
use openvmms_core::models::parse_memory_mb;
use openvmms_core::models::parse_port_forward;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(
    name = "openvmms",
    about = "Hyper-V-style VM management CLI for OpenVMM and FastDesk"
)]
struct Cli {
    /// Directory for VM configs, logs, and host configuration
    #[arg(long, global = true)]
    data_dir: Option<PathBuf>,

    /// Override daemon HTTP endpoint (e.g. http://127.0.0.1:47620)
    #[arg(long, global = true)]
    daemon_url: Option<String>,

    /// Emit JSON output
    #[arg(long, global = true)]
    json: bool,

    /// Do not auto-start openvmmsd if it is not already running
    #[arg(long, global = true)]
    no_auto_start: bool,

    #[command(subcommand)]
    command: TopCommand,
}

#[derive(Subcommand, Debug)]
enum TopCommand {
    /// Manage the openvmmsd background service
    Daemon {
        #[command(subcommand)]
        action: DaemonCmd,
    },
    /// Inspect or configure host paths and daemon settings
    Host {
        #[command(subcommand)]
        action: HostCmd,
    },
    /// List virtual switches and network backends
    Switch {
        #[command(subcommand)]
        action: SwitchCmd,
    },
    /// Create or inspect virtual disk images
    Disk {
        #[command(subcommand)]
        action: DiskCmd,
    },
    /// Manage virtual machines
    Vm {
        #[command(subcommand)]
        action: VmCmd,
    },
}

#[derive(Subcommand, Debug)]
enum DaemonCmd {
    /// Check daemon health and host info
    Status,
    /// Start openvmmsd (detached by default, or --foreground)
    Start {
        #[arg(long)]
        foreground: bool,
    },
    /// Stop openvmmsd (leaves running VMs alive for re-adoption unless --stop-vms is passed)
    Stop {
        /// Also stop all running VMs managed by openvmmsd before exiting
        #[arg(long)]
        stop_vms: bool,
    },
}

#[derive(Subcommand, Debug)]
enum HostCmd {
    /// Show host info and discovered binaries/firmware
    Info,
    /// Show or update host binary/firmware configuration
    Config {
        #[arg(long)]
        openvmm_bin: Option<PathBuf>,
        #[arg(long)]
        viewer_bin: Option<PathBuf>,
        #[arg(long)]
        img_bin: Option<PathBuf>,
        #[arg(long)]
        uefi_firmware: Option<PathBuf>,
        #[arg(long)]
        listen_addr: Option<String>,
    },
}

#[derive(Subcommand, Debug)]
enum SwitchCmd {
    /// List available virtual switches / network backends
    List,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum DiskFormatArg {
    Vhdx,
    Raw,
}

impl From<DiskFormatArg> for DiskFormat {
    fn from(value: DiskFormatArg) -> Self {
        match value {
            DiskFormatArg::Vhdx => DiskFormat::Vhdx,
            DiskFormatArg::Raw => DiskFormat::Raw,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum PresetArg {
    Win11,
    Linux,
    Custom,
}

impl From<PresetArg> for VmPreset {
    fn from(value: PresetArg) -> Self {
        match value {
            PresetArg::Win11 => VmPreset::Windows11,
            PresetArg::Linux => VmPreset::LinuxDesktop,
            PresetArg::Custom => VmPreset::Custom,
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ToggleArg {
    On,
    Off,
}

impl From<ToggleArg> for bool {
    fn from(value: ToggleArg) -> Self {
        matches!(value, ToggleArg::On)
    }
}

#[derive(Subcommand, Debug)]
enum DiskCmd {
    /// Create a new virtual disk image (.vhdx or raw .img)
    Create {
        path: PathBuf,
        #[arg(long)]
        size: String,
        #[arg(long, value_enum)]
        format: Option<DiskFormatArg>,
        #[arg(long)]
        fixed: bool,
    },
    /// Inspect virtual disk image metadata
    Info { path: PathBuf },
}

#[derive(Subcommand, Debug)]
enum VmCmd {
    /// List all virtual machines
    List,
    /// Show detailed configuration and state of a VM
    Show { vm: String },
    /// Create a new virtual machine
    Create(CreateVmArgs),
    /// Modify an offline VM's configuration
    Edit(EditVmArgs),
    /// Delete a stopped virtual machine
    Delete {
        vm: String,
        #[arg(long)]
        delete_disks: bool,
        /// Skip interactive confirmation when --delete-disks is set
        #[arg(long, short = 'y')]
        yes: bool,
    },
    /// Start a virtual machine
    Start { vm: String },
    /// Force turn off a virtual machine
    Stop { vm: String },
    /// Request cooperative guest OS shutdown
    Shutdown {
        vm: String,
        #[arg(long)]
        acpi: bool,
        #[arg(long)]
        force: bool,
    },
    /// Pause a running virtual machine
    Pause { vm: String },
    /// Resume a paused virtual machine
    Resume { vm: String },
    /// Reset (reboot) a running virtual machine
    Reset { vm: String },
    /// Launch fastdesk-viewer connected to a running VM
    Connect {
        vm: String,
        #[arg(long)]
        no_shm: bool,
    },
    /// View OpenVMM logs for a VM
    Logs {
        vm: String,
        #[arg(long, default_value_t = 100)]
        tail: usize,
    },
    /// Attach or detach SCSI disks
    Disk {
        #[command(subcommand)]
        action: VmDiskCmd,
    },
    /// Mount or eject ISO optical media
    Iso {
        #[command(subcommand)]
        action: VmIsoCmd,
    },
    /// Add a Consomme NAT port forward (quote in shells because of '>', e.g. "tcp:127.0.0.1:3389->3389")
    PortAdd {
        vm: String,
        spec: String,
        #[arg(long)]
        nic: Option<Uuid>,
    },
    /// Remove a Consomme NAT port forward (quote in shells because of '>', e.g. "tcp:127.0.0.1:3389->3389")
    PortRemove {
        vm: String,
        spec: String,
        #[arg(long)]
        nic: Option<Uuid>,
    },
}

#[derive(Args, Debug)]
struct CreateVmArgs {
    name: String,
    #[arg(long, value_enum, default_value_t = PresetArg::Win11)]
    preset: PresetArg,
    #[arg(long, default_value = "")]
    description: String,
    #[arg(long)]
    cpus: Option<u32>,
    #[arg(long)]
    memory: Option<String>,
    /// Attach one or more existing hard disk images
    #[arg(long)]
    disk: Vec<PathBuf>,
    /// Create and attach a new OS disk of this size (e.g. 64G)
    #[arg(long)]
    create_disk: Option<String>,
    /// Optional explicit path for --create-disk (defaults to <vm_dir>/disk0.<ext>)
    #[arg(long, requires = "create_disk")]
    create_disk_path: Option<PathBuf>,
    #[arg(long, value_enum, requires = "create_disk")]
    disk_format: Option<DiskFormatArg>,
    #[arg(long, requires = "create_disk")]
    fixed_disk: bool,
    /// Optional installer ISO image
    #[arg(long)]
    iso: Option<PathBuf>,
    /// Network specification: consomme | vmswitch:<guid_or_default> | tap:<dev> | none
    #[arg(long)]
    net: Option<String>,
    #[arg(long)]
    vram_mb: Option<u32>,
    /// Optional UEFI firmware override (MSVM.fd)
    #[arg(long)]
    uefi_firmware: Option<PathBuf>,
    #[arg(long, value_enum)]
    secure_boot: Option<ToggleArg>,
    #[arg(long, value_enum)]
    tpm: Option<ToggleArg>,
    #[arg(long, value_enum)]
    nested_virt: Option<ToggleArg>,
    #[arg(long)]
    no_shm: bool,
    #[arg(long)]
    hvsock: bool,
}

#[derive(Args, Debug)]
struct EditVmArgs {
    vm: String,
    #[arg(long)]
    name: Option<String>,
    #[arg(long)]
    description: Option<String>,
    #[arg(long)]
    cpus: Option<u32>,
    #[arg(long)]
    memory: Option<String>,
    #[arg(long)]
    vram_mb: Option<u32>,
    /// Override the UEFI firmware image path (MSVM.fd)
    #[arg(long)]
    uefi_firmware: Option<PathBuf>,
    /// Clear the per-VM UEFI firmware override and revert to the host default
    #[arg(long, conflicts_with = "uefi_firmware")]
    clear_uefi_firmware: bool,
    #[arg(long, value_enum)]
    secure_boot: Option<ToggleArg>,
    #[arg(long, value_enum)]
    tpm: Option<ToggleArg>,
    #[arg(long, value_enum)]
    nested_virt: Option<ToggleArg>,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum AttachDiskFormatArg {
    Vhdx,
    Vhd,
    Raw,
    Iso,
}

impl From<AttachDiskFormatArg> for DiskFormat {
    fn from(value: AttachDiskFormatArg) -> Self {
        match value {
            AttachDiskFormatArg::Vhdx => DiskFormat::Vhdx,
            AttachDiskFormatArg::Vhd => DiskFormat::Vhd,
            AttachDiskFormatArg::Raw => DiskFormat::Raw,
            AttachDiskFormatArg::Iso => DiskFormat::Iso,
        }
    }
}

#[derive(Subcommand, Debug)]
enum VmDiskCmd {
    /// Attach an existing disk image to a VM (hot-plugs if running)
    Attach {
        vm: String,
        path: PathBuf,
        #[arg(long)]
        lun: Option<u32>,
        #[arg(long)]
        ro: bool,
        #[arg(long, value_enum)]
        format: Option<AttachDiskFormatArg>,
    },
    /// Detach a SCSI disk by LUN (hot-unplugs if running)
    Detach {
        vm: String,
        #[arg(long)]
        lun: u32,
    },
}

#[derive(Subcommand, Debug)]
enum VmIsoCmd {
    /// Mount an ISO image into the VM's optical drive (hot-mounts if running)
    Mount {
        vm: String,
        path: PathBuf,
        #[arg(long)]
        lun: Option<u32>,
    },
    /// Eject the mounted ISO image
    Eject {
        vm: String,
        #[arg(long)]
        lun: Option<u32>,
    },
}

fn main() {
    if let Err(err) = run() {
        eprintln!("Error: {err:#}");
        std::process::exit(1);
    }
}

fn get_client(cli: &Cli) -> anyhow::Result<ApiClient> {
    if cli.no_auto_start {
        ApiClient::from_data_dir(cli.data_dir.as_deref(), cli.daemon_url.as_deref())
    } else {
        ensure_daemon_running(cli.data_dir.as_deref(), cli.daemon_url.as_deref())
    }
}

fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        TopCommand::Daemon { action } => handle_daemon(&cli, action),
        TopCommand::Host { action } => handle_host(&cli, action),
        TopCommand::Switch { action } => handle_switch(&cli, action),
        TopCommand::Disk { action } => handle_disk(&cli, action),
        TopCommand::Vm { action } => handle_vm(&cli, action),
    }
}

fn handle_daemon(cli: &Cli, action: &DaemonCmd) -> anyhow::Result<()> {
    match action {
        DaemonCmd::Status => {
            let client = ApiClient::from_data_dir(cli.data_dir.as_deref(), cli.daemon_url.as_deref())?;
            client.health()?;
            let info = client.host_info()?;
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&info)?);
            } else {
                println!("openvmmsd is running at {}", client.base_url());
                println!("Data directory:    {}", info.data_dir.display());
                println!("Runtime directory: {}", info.runtime_dir.display());
            }
            Ok(())
        }
        DaemonCmd::Start { foreground } => {
            if *foreground {
                let bin = find_openvmmsd_binary()?;
                let fg_args = build_foreground_daemon_args(
                    cli.data_dir.as_deref(),
                    cli.daemon_url.as_deref(),
                )?;
                let mut cmd = Command::new(bin);
                cmd.args(fg_args);
                #[cfg(unix)]
                {
                    use std::os::unix::process::CommandExt;
                    let err = cmd.exec();
                    return Err(err).context("execing openvmmsd in foreground");
                }
                #[cfg(not(unix))]
                {
                    let status = cmd.status().context("running openvmmsd in foreground")?;
                    if !status.success() {
                        bail!("openvmmsd exited with {status}");
                    }
                    return Ok(());
                }
            }
            let client = ensure_daemon_running(cli.data_dir.as_deref(), cli.daemon_url.as_deref())?;
            if cli.json {
                let info = client.host_info()?;
                println!("{}", serde_json::to_string_pretty(&info)?);
            } else {
                println!("openvmmsd is running at {}", client.base_url());
            }
            Ok(())
        }
        DaemonCmd::Stop { stop_vms } => {
            let client = ApiClient::from_data_dir(cli.data_dir.as_deref(), cli.daemon_url.as_deref())?;
            client.shutdown_daemon_with_options(*stop_vms)?;
            if cli.json {
                println!(
                    "{}",
                    serde_json::to_string(&serde_json::json!({
                        "status": "stopping",
                        "stop_vms": stop_vms
                    }))?
                );
            } else if *stop_vms {
                println!("Shutdown signal sent to openvmmsd (stopping all VMs).");
            } else {
                println!("Shutdown signal sent to openvmmsd (leaving running VMs active).");
            }
            Ok(())
        }
    }
}

fn handle_host(cli: &Cli, action: &HostCmd) -> anyhow::Result<()> {
    let client = get_client(cli)?;
    match action {
        HostCmd::Info => {
            let info = client.host_info()?;
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&info)?);
            } else {
                println!("Host:             {} ({} {})", info.hostname, info.os, info.arch);
                println!("Logical CPUs:     {}", info.cpu_count);
                println!("Daemon Version:   {}", info.daemon_version);
                println!("Listen Address:   {}", info.listen_addr);
                println!("Data Directory:   {}", info.data_dir.display());
                println!("Runtime Dir:      {}", info.runtime_dir.display());
                println!(
                    "openvmm:          {}",
                    display_opt_path(info.resolved_paths.openvmm_bin.as_deref())
                );
                println!(
                    "fastdesk-viewer:  {}",
                    display_opt_path(info.resolved_paths.fastdesk_viewer_bin.as_deref())
                );
                println!(
                    "openvmm-img:      {}",
                    display_opt_path(info.resolved_paths.openvmm_img_bin.as_deref())
                );
                println!(
                    "UEFI Firmware:    {}",
                    display_opt_path(info.resolved_paths.uefi_firmware_path.as_deref())
                );
            }
            Ok(())
        }
        HostCmd::Config {
            openvmm_bin,
            viewer_bin,
            img_bin,
            uefi_firmware,
            listen_addr,
        } => {
            let mut cfg = client.host_config()?;
            let mut changed = false;
            if let Some(p) = openvmm_bin {
                cfg.openvmm_bin = Some(resolve_client_path(p));
                changed = true;
            }
            if let Some(p) = viewer_bin {
                cfg.fastdesk_viewer_bin = Some(resolve_client_path(p));
                changed = true;
            }
            if let Some(p) = img_bin {
                cfg.openvmm_img_bin = Some(resolve_client_path(p));
                changed = true;
            }
            if let Some(p) = uefi_firmware {
                cfg.uefi_firmware_path = Some(resolve_client_path(p));
                changed = true;
            }
            if let Some(addr) = listen_addr {
                cfg.listen_addr = addr.clone();
                changed = true;
            }
            let out = if changed {
                client.update_host_config(&cfg)?
            } else {
                cfg
            };
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&out)?);
            } else {
                println!("{}", toml::to_string_pretty(&out)?);
            }
            Ok(())
        }
    }
}

fn handle_switch(cli: &Cli, action: &SwitchCmd) -> anyhow::Result<()> {
    let client = get_client(cli)?;
    match action {
        SwitchCmd::List => {
            let switches = client.list_switches()?;
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&switches)?);
            } else {
                println!("{:<38} {:<24} {:<10} {}", "ID", "NAME", "KIND", "DESCRIPTION");
                for sw in switches {
                    println!(
                        "{:<38} {:<24} {:<10} {}",
                        sw.id, sw.name, sw.kind, sw.description
                    );
                }
            }
            Ok(())
        }
    }
}

fn handle_disk(cli: &Cli, action: &DiskCmd) -> anyhow::Result<()> {
    let client = get_client(cli)?;
    match action {
        DiskCmd::Create {
            path,
            size,
            format,
            fixed,
        } => {
            let resolved_path = resolve_client_path(path);
            let size_bytes = parse_byte_size(size)?;
            let fmt = format
                .map(DiskFormat::from)
                .unwrap_or_else(|| DiskFormat::from_path(&resolved_path));
            let info = client.create_disk(&CreateDiskPayload {
                path: resolved_path,
                size_bytes,
                format: fmt,
                fixed: *fixed,
            })?;
            print_disk_info(&info, cli.json)
        }
        DiskCmd::Info { path } => {
            let info = client.inspect_disk(&resolve_client_path(path))?;
            print_disk_info(&info, cli.json)
        }
    }
}

fn handle_vm(cli: &Cli, action: &VmCmd) -> anyhow::Result<()> {
    let client = get_client(cli)?;
    match action {
        VmCmd::List => {
            let vms = client.list_vms()?;
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&vms)?);
            } else if vms.is_empty() {
                println!("No virtual machines registered. Create one with `openvmms vm create <name>`.");
            } else {
                println!(
                    "{:<22} {:<10} {:>4} {:>9} {:<24} {}",
                    "NAME", "STATE", "VCPU", "MEMORY", "FASTDESK", "ID"
                );
                for vm in vms {
                    let mem = format_memory_mb(vm.config.memory_mb);
                    let fd = vm
                        .runtime
                        .fastdesk_endpoint
                        .as_deref()
                        .unwrap_or(if vm.config.fastdesk.enabled {
                            "ready (offline)"
                        } else {
                            "disabled"
                        });
                    println!(
                        "{:<22} {:<10} {:>4} {:>9} {:<24} {}",
                        truncate(&vm.config.name, 21),
                        vm.runtime.status.to_string(),
                        vm.config.vcpus,
                        mem,
                        truncate(fd, 23),
                        vm.config.id
                    );
                }
            }
            Ok(())
        }
        VmCmd::Show { vm } => {
            let summary = client.get_vm(vm)?;
            print_vm_summary(&summary, cli.json)
        }
        VmCmd::Create(args) => {
            let memory_mb = args
                .memory
                .as_deref()
                .map(parse_memory_mb)
                .transpose()?;

            let create_disk = if let Some(size_str) = &args.create_disk {
                let size_bytes = parse_byte_size(size_str)?;
                let resolved_disk_path = args
                    .create_disk_path
                    .as_deref()
                    .map(resolve_client_path)
                    .unwrap_or_default();
                let format = args
                    .disk_format
                    .map(DiskFormat::from)
                    .or_else(|| {
                        (!resolved_disk_path.as_os_str().is_empty())
                            .then(|| DiskFormat::from_path(&resolved_disk_path))
                    })
                    .unwrap_or(DiskFormat::Vhdx);
                Some(CreateDiskPayload {
                    path: resolved_disk_path,
                    size_bytes,
                    format,
                    fixed: args.fixed_disk,
                })
            } else {
                None
            };

            let mut disks = Vec::new();
            for (idx, p) in args.disk.iter().enumerate() {
                disks.push(disk_attachment_from_cli_path(idx as u32, p));
            }

            let fastdesk = if args.vram_mb.is_some() || args.no_shm || args.hvsock {
                Some(FastDeskSettings {
                    enabled: true,
                    vram_mb: args.vram_mb.unwrap_or(32),
                    use_shm: !args.no_shm,
                    transport: if args.hvsock {
                        FastDeskTransport::HvSock
                    } else {
                        FastDeskTransport::Tcp
                    },
                    listen_addr: "127.0.0.1:0".to_string(),
                    vsock_port: None,
                })
            } else {
                None
            };

            let nics = args.net.as_deref().map(parse_net_spec).transpose()?;

            let payload = CreateVmPayload {
                name: args.name.clone(),
                description: args.description.clone(),
                preset: args.preset.into(),
                vcpus: args.cpus,
                memory_mb,
                nested_virt: args.nested_virt.map(bool::from),
                uefi_firmware: args
                    .uefi_firmware
                    .as_deref()
                    .map(resolve_client_path)
                    .filter(|p| !p.as_os_str().is_empty()),
                secure_boot_enabled: args.secure_boot.map(bool::from),
                secure_boot_template: None,
                tpm_enabled: args.tpm.map(bool::from),
                fastdesk,
                disks,
                create_disk,
                iso_path: args.iso.as_deref().map(resolve_client_path),
                nics,
            };
            let summary = client.create_vm(&payload)?;
            print_vm_summary(&summary, cli.json)
        }
        VmCmd::Edit(args) => {
            let current = client.get_vm(&args.vm)?;
            let mut fastdesk = None;
            if let Some(vram_mb) = args.vram_mb {
                let mut fd = current.config.fastdesk.clone();
                fd.vram_mb = vram_mb;
                fastdesk = Some(fd);
            }
            let uefi_firmware = if args.clear_uefi_firmware {
                Some(None)
            } else if let Some(fw) = &args.uefi_firmware {
                let resolved = resolve_client_path(fw);
                if resolved.as_os_str().is_empty() {
                    Some(None)
                } else {
                    Some(Some(resolved))
                }
            } else {
                None
            };
            let payload = UpdateVmPayload {
                name: args.name.clone(),
                description: args.description.clone(),
                vcpus: args.cpus,
                memory_mb: args.memory.as_deref().map(parse_memory_mb).transpose()?,
                nested_virt: args.nested_virt.map(bool::from),
                uefi_firmware,
                secure_boot_enabled: args.secure_boot.map(bool::from),
                tpm_enabled: args.tpm.map(bool::from),
                fastdesk,
                ..Default::default()
            };
            let summary = client.update_vm(&args.vm, &payload)?;
            print_vm_summary(&summary, cli.json)
        }
        VmCmd::Delete {
            vm,
            delete_disks,
            yes,
        } => {
            if *delete_disks && !*yes {
                let summary = client.get_vm(vm)?;
                let writable_disks: Vec<PathBuf> = summary
                    .config
                    .disks
                    .iter()
                    .filter(|d| d.kind == DiskKind::HardDisk && !d.read_only)
                    .map(|d| d.path.clone())
                    .collect();
                if !writable_disks.is_empty() {
                    use std::io::IsTerminal;
                    if cli.json || !std::io::stdin().is_terminal() {
                        bail!(
                            "refusing to delete disk files without confirmation in non-interactive mode; pass --yes to confirm"
                        );
                    }
                    confirm_disk_deletion(
                        &summary.config.name,
                        &writable_disks,
                        &mut std::io::stdin().lock(),
                        &mut std::io::stderr(),
                    )?;
                }
            }
            client.delete_vm(vm, *delete_disks)?;
            if cli.json {
                println!("{}", serde_json::to_string(&serde_json::json!({ "deleted": vm }))?);
            } else {
                println!("Deleted VM '{vm}'.");
            }
            Ok(())
        }
        VmCmd::Start { vm } => print_vm_summary(&client.start_vm(vm)?, cli.json),
        VmCmd::Stop { vm } => print_vm_summary(&client.stop_vm(vm)?, cli.json),
        VmCmd::Shutdown { vm, acpi, force } => print_vm_summary(
            &client.shutdown_vm(
                vm,
                &ShutdownVmPayload {
                    force: *force,
                    acpi: *acpi,
                },
            )?,
            cli.json,
        ),
        VmCmd::Pause { vm } => print_vm_summary(&client.pause_vm(vm)?, cli.json),
        VmCmd::Resume { vm } => print_vm_summary(&client.resume_vm(vm)?, cli.json),
        VmCmd::Reset { vm } => print_vm_summary(&client.reset_vm(vm)?, cli.json),
        VmCmd::Connect { vm, no_shm } => {
            let summary = client.get_vm(vm)?;
            let endpoint = summary.runtime.fastdesk_endpoint.as_deref().context(
                "VM does not have an active FastDesk endpoint (is it Running with FastDesk enabled?)",
            )?;
            let viewer_bin = resolve_local_viewer_binary(cli.data_dir.as_deref())?;
            let mut child = launch_fastdesk_viewer(&viewer_bin, endpoint, &summary.config.name, *no_shm)?;
            if cli.json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "vm": summary.config.name,
                        "endpoint": endpoint,
                        "viewer_pid": child.id(),
                    }))?
                );
            } else {
                println!(
                    "Connected fastdesk-viewer to '{}' ({endpoint})",
                    summary.config.name
                );
            }
            let status = child.wait().context("waiting for fastdesk-viewer")?;
            if !status.success() {
                bail!("fastdesk-viewer exited with {status}");
            }
            Ok(())
        }
        VmCmd::Logs { vm, tail } => {
            let content = client.vm_logs(vm, *tail)?;
            if cli.json {
                println!("{}", serde_json::to_string_pretty(&serde_json::json!({ "content": content }))?);
            } else {
                println!("{content}");
            }
            Ok(())
        }
        VmCmd::Disk { action } => match action {
            VmDiskCmd::Attach {
                vm,
                path,
                lun,
                ro,
                format,
            } => {
                let summary = client.attach_disk(
                    vm,
                    &AttachDiskPayload {
                        path: resolve_client_path(path),
                        lun: *lun,
                        read_only: *ro,
                        format: format.map(Into::into),
                    },
                )?;
                print_vm_summary(&summary, cli.json)
            }
            VmDiskCmd::Detach { vm, lun } => {
                print_vm_summary(&client.detach_disk(vm, *lun)?, cli.json)
            }
        },
        VmCmd::Iso { action } => match action {
            VmIsoCmd::Mount { vm, path, lun } => {
                let summary = client.mount_iso(
                    vm,
                    &MountIsoPayload {
                        path: resolve_client_path(path),
                        lun: *lun,
                    },
                )?;
                print_vm_summary(&summary, cli.json)
            }
            VmIsoCmd::Eject { vm, lun } => {
                print_vm_summary(&client.eject_iso(vm, *lun)?, cli.json)
            }
        },
        VmCmd::PortAdd { vm, spec, nic } => {
            let pf = parse_port_forward(spec)?;
            let nic_id = resolve_consomme_nic_id(&client, vm, *nic)?;
            print_vm_summary(&client.add_port_forward(vm, nic_id, pf)?, cli.json)
        }
        VmCmd::PortRemove { vm, spec, nic } => {
            let pf = parse_port_forward(spec)?;
            let nic_id = resolve_consomme_nic_id(&client, vm, *nic)?;
            print_vm_summary(&client.remove_port_forward(vm, nic_id, pf)?, cli.json)
        }
    }
}

fn build_foreground_daemon_args(
    data_dir: Option<&Path>,
    daemon_url: Option<&str>,
) -> anyhow::Result<Vec<std::ffi::OsString>> {
    let dir = data_dir
        .map(resolve_client_path)
        .unwrap_or_else(default_data_dir);
    let mut args = vec![
        std::ffi::OsString::from("--data-dir"),
        dir.into_os_string(),
    ];
    if let Some(url) = daemon_url {
        let listen = openvmms_core::models::parse_loopback_listen_addr(url)?.to_string();
        args.push(std::ffi::OsString::from("--listen"));
        args.push(std::ffi::OsString::from(listen));
    }
    Ok(args)
}

fn disk_attachment_from_cli_path(lun: u32, path: &Path) -> DiskAttachment {
    let resolved = resolve_client_path(path);
    let format = DiskFormat::from_path(&resolved);
    let (kind, read_only) = if format == DiskFormat::Iso {
        (DiskKind::Dvd, true)
    } else {
        (DiskKind::HardDisk, false)
    };
    DiskAttachment {
        lun,
        kind,
        path: resolved,
        read_only,
        format,
    }
}

fn confirm_disk_deletion(
    vm_name: &str,
    disk_paths: &[PathBuf],
    input: &mut impl std::io::BufRead,
    output: &mut impl std::io::Write,
) -> anyhow::Result<()> {
    writeln!(
        output,
        "Deleting VM '{vm_name}' will permanently delete the following disk file(s):"
    )?;
    for path in disk_paths {
        writeln!(output, "  - {}", path.display())?;
    }
    write!(output, "Permanently delete these disk file(s)? [y/N]: ")?;
    output.flush()?;
    let mut answer = String::new();
    input.read_line(&mut answer)?;
    let trimmed = answer.trim();
    if trimmed.eq_ignore_ascii_case("y") || trimmed.eq_ignore_ascii_case("yes") {
        Ok(())
    } else {
        bail!("aborted: VM '{vm_name}' and its disk file(s) were not deleted");
    }
}

fn resolve_consomme_nic_id(
    client: &ApiClient,
    vm_name: &str,
    explicit_nic: Option<Uuid>,
) -> anyhow::Result<Uuid> {
    if let Some(id) = explicit_nic {
        return Ok(id);
    }
    let summary = client.get_vm(vm_name)?;
    summary
        .config
        .nics
        .iter()
        .find(|n| matches!(n.backend, NicBackendConfig::Consomme { .. }))
        .map(|n| n.id)
        .context("VM does not have a Consomme NAT network adapter")
}

pub fn parse_net_spec(spec: &str) -> anyhow::Result<Vec<NicAttachment>> {
    let trimmed = spec.trim();
    if trimmed.eq_ignore_ascii_case("none") {
        return Ok(Vec::new());
    }
    if trimmed.eq_ignore_ascii_case("consomme") || trimmed.eq_ignore_ascii_case("nat") {
        return Ok(vec![NicAttachment::default_consomme()]);
    }
    if let Some(switch_id) = trimmed.strip_prefix("vmswitch:") {
        let sid = if switch_id.is_empty() {
            "default".to_string()
        } else {
            switch_id.to_string()
        };
        return Ok(vec![NicAttachment {
            id: Uuid::new_v4(),
            name: format!("VMSwitch ({sid})"),
            mac_address: generate_hyperv_mac(),
            backend: NicBackendConfig::VmSwitch {
                switch_id: sid,
                switch_name: None,
            },
        }]);
    }
    if let Some(tap_name) = trimmed.strip_prefix("tap:") {
        if tap_name.trim().is_empty() {
            bail!("tap:<interface> requires an interface name");
        }
        return Ok(vec![NicAttachment {
            id: Uuid::new_v4(),
            name: format!("TAP ({tap_name})"),
            mac_address: generate_hyperv_mac(),
            backend: NicBackendConfig::Tap {
                name: tap_name.trim().to_string(),
            },
        }]);
    }
    bail!(
        "invalid --net spec '{spec}' (expected consomme, vmswitch:<id|default>, tap:<dev>, or none)"
    )
}

fn print_disk_info(info: &DiskInfo, json: bool) -> anyhow::Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(info)?);
    } else {
        println!("Path:         {}", info.path.display());
        println!("Format:       {} ({})", info.format, info.disk_type);
        println!(
            "Virtual Size: {} ({} bytes)",
            format_bytes(info.virtual_size_bytes),
            info.virtual_size_bytes
        );
        println!(
            "File Size:    {} ({} bytes)",
            format_bytes(info.file_size_bytes),
            info.file_size_bytes
        );
        if let Some(parent) = &info.parent_path {
            println!("Parent:       {parent}");
        }
    }
    Ok(())
}

fn print_vm_summary(vm: &VmSummary, json: bool) -> anyhow::Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(vm)?);
    } else {
        println!("Name:         {} ({})", vm.config.name, vm.config.id);
        println!("Preset:       {}", vm.config.preset);
        println!("State:        {}", vm.runtime.status);
        println!("Compute:      {} vCPU, {}", vm.config.vcpus, format_memory_mb(vm.config.memory_mb));
        println!(
            "Firmware:     UEFI (Secure Boot: {}, Template: {}, vTPM 2.0: {})",
            if vm.config.secure_boot_enabled {
                "On"
            } else {
                "Off"
            },
            vm.config.secure_boot_template,
            if vm.config.tpm_enabled { "On" } else { "Off" }
        );
        if let Some(fw) = &vm.config.uefi_firmware {
            println!("UEFI Path:    {}", fw.display());
        }
        if let Some(ep) = &vm.runtime.fastdesk_endpoint {
            println!("FastDesk:     {ep}");
        } else {
            println!(
                "FastDesk:     {} ({} MiB VRAM, SHM: {})",
                if vm.config.fastdesk.enabled {
                    "enabled (offline)"
                } else {
                    "disabled"
                },
                vm.config.fastdesk.vram_mb,
                if vm.config.fastdesk.use_shm { "yes" } else { "no" }
            );
        }
        if let Some(err) = &vm.runtime.last_error {
            println!("Last Error:   {err}");
        }
        println!("Storage:");
        if vm.config.disks.is_empty() {
            println!("  (none)");
        } else {
            for d in &vm.config.disks {
                println!(
                    "  LUN {}: [{:?}] {} ({})",
                    d.lun,
                    d.kind,
                    d.path.display(),
                    if d.read_only { "RO" } else { "RW" }
                );
            }
        }
        println!("Networking:");
        if vm.config.nics.is_empty() {
            println!("  (none)");
        } else {
            for n in &vm.config.nics {
                println!("  {} [{}] {:?}", n.name, n.mac_address, n.backend);
            }
        }
    }
    Ok(())
}

fn display_opt_path(p: Option<&Path>) -> String {
    p.map(|x| x.display().to_string())
        .unwrap_or_else(|| "(not found)".to_string())
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(max.saturating_sub(1)).collect();
        out.push('…');
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_parses_vm_create_and_net_specs() {
        let parsed = Cli::try_parse_from([
            "openvmms",
            "--json",
            "vm",
            "create",
            "Win11-Dev",
            "--preset",
            "win11",
            "--cpus",
            "8",
            "--memory",
            "16G",
            "--create-disk",
            "80G",
            "--net",
            "vmswitch:default",
        ])
        .unwrap();
        assert!(parsed.json);
        match parsed.command {
            TopCommand::Vm {
                action: VmCmd::Create(args),
            } => {
                assert_eq!(args.name, "Win11-Dev");
                assert_eq!(args.cpus, Some(8));
                assert_eq!(args.memory.as_deref(), Some("16G"));
                let nics = parse_net_spec(args.net.as_deref().unwrap()).unwrap();
                assert_eq!(nics.len(), 1);
                assert!(matches!(nics[0].backend, NicBackendConfig::VmSwitch { .. }));
            }
            _ => panic!("unexpected command"),
        }

        assert!(parse_net_spec("none").unwrap().is_empty());
        assert_eq!(parse_net_spec("consomme").unwrap().len(), 1);
        assert!(parse_net_spec("tap:tap0").is_ok());
        assert!(parse_net_spec("invalid").is_err());
    }

    #[test]
    fn cli_resolves_relative_paths_against_cwd() {
        let cwd = std::env::current_dir().unwrap();
        let resolved = resolve_client_path(Path::new("relative.img"));
        assert!(resolved.is_absolute());
        assert_eq!(resolved, cwd.join("relative.img"));
    }

    #[test]
    fn cli_parses_vm_edit_firmware_override_and_clear() {
        let parsed_set =
            Cli::try_parse_from(["openvmms", "vm", "edit", "VM1", "--uefi-firmware", "MSVM.fd"])
                .unwrap();
        match parsed_set.command {
            TopCommand::Vm {
                action: VmCmd::Edit(args),
            } => {
                assert_eq!(args.uefi_firmware.as_deref(), Some(Path::new("MSVM.fd")));
                assert!(!args.clear_uefi_firmware);
            }
            _ => panic!("unexpected command"),
        }

        let parsed_clear =
            Cli::try_parse_from(["openvmms", "vm", "edit", "VM1", "--clear-uefi-firmware"])
                .unwrap();
        match parsed_clear.command {
            TopCommand::Vm {
                action: VmCmd::Edit(args),
            } => {
                assert!(args.clear_uefi_firmware);
                assert!(args.uefi_firmware.is_none());
            }
            _ => panic!("unexpected command"),
        }
    }

    #[test]
    fn cli_requires_create_disk_for_disk_creation_options() {
        for flag_args in [
            vec!["openvmms", "vm", "create", "VM1", "--create-disk-path", "os.vhdx"],
            vec!["openvmms", "vm", "create", "VM1", "--disk-format", "raw"],
            vec!["openvmms", "vm", "create", "VM1", "--fixed-disk"],
        ] {
            assert!(
                Cli::try_parse_from(flag_args.clone()).is_err(),
                "expected {flag_args:?} to require --create-disk"
            );
        }

        assert!(
            Cli::try_parse_from([
                "openvmms",
                "vm",
                "create",
                "VM1",
                "--create-disk",
                "64G",
                "--create-disk-path",
                "os.img",
                "--disk-format",
                "raw",
                "--fixed-disk",
            ])
            .is_ok()
        );
    }

    #[test]
    fn cli_disk_helper_attaches_iso_as_readonly_dvd_and_vhd_as_vhd() {
        let iso = disk_attachment_from_cli_path(0, Path::new("installer.iso"));
        assert_eq!(iso.kind, DiskKind::Dvd);
        assert!(iso.read_only);
        assert_eq!(iso.format, DiskFormat::Iso);

        let vhd = disk_attachment_from_cli_path(1, Path::new("legacy.vhd"));
        assert_eq!(vhd.kind, DiskKind::HardDisk);
        assert!(!vhd.read_only);
        assert_eq!(vhd.format, DiskFormat::Vhd);
    }

    #[test]
    fn cli_parses_delete_yes_and_daemon_stop_flags_and_confirm_prompt() {
        let parsed_del = Cli::try_parse_from([
            "openvmms",
            "vm",
            "delete",
            "VM1",
            "--delete-disks",
            "-y",
        ])
        .unwrap();
        match parsed_del.command {
            TopCommand::Vm {
                action:
                    VmCmd::Delete {
                        vm,
                        delete_disks,
                        yes,
                    },
            } => {
                assert_eq!(vm, "VM1");
                assert!(delete_disks);
                assert!(yes);
            }
            _ => panic!("unexpected command"),
        }

        match Cli::try_parse_from(["openvmms", "daemon", "stop"])
            .unwrap()
            .command
        {
            TopCommand::Daemon {
                action: DaemonCmd::Stop { stop_vms },
            } => assert!(!stop_vms),
            _ => panic!("unexpected command"),
        }

        match Cli::try_parse_from(["openvmms", "daemon", "stop", "--stop-vms"])
            .unwrap()
            .command
        {
            TopCommand::Daemon {
                action: DaemonCmd::Stop { stop_vms },
            } => assert!(stop_vms),
            _ => panic!("unexpected command"),
        }

        let paths = vec![PathBuf::from("/tmp/os.vhdx")];
        let mut out = Vec::new();
        assert!(
            confirm_disk_deletion("VM1", &paths, &mut std::io::Cursor::new(b"y\n"), &mut out)
                .is_ok()
        );
        assert!(
            confirm_disk_deletion("VM1", &paths, &mut std::io::Cursor::new(b"YES\n"), &mut out)
                .is_ok()
        );
        assert!(
            confirm_disk_deletion("VM1", &paths, &mut std::io::Cursor::new(b"\n"), &mut out)
                .is_err()
        );
        assert!(
            confirm_disk_deletion("VM1", &paths, &mut std::io::Cursor::new(b"n\n"), &mut out)
                .is_err()
        );
    }

    #[test]
    fn foreground_daemon_args_honor_daemon_url_and_reject_https() {
        let dir = Path::new("/tmp/openvmms-fg-test");
        let expected_dir = resolve_client_path(dir).into_os_string();
        let without_url = build_foreground_daemon_args(Some(dir), None).unwrap();
        assert_eq!(
            without_url,
            vec![
                std::ffi::OsString::from("--data-dir"),
                expected_dir.clone(),
            ]
        );

        let with_url =
            build_foreground_daemon_args(Some(dir), Some("http://127.0.0.1:48123")).unwrap();
        assert_eq!(
            with_url,
            vec![
                std::ffi::OsString::from("--data-dir"),
                expected_dir,
                std::ffi::OsString::from("--listen"),
                std::ffi::OsString::from("127.0.0.1:48123"),
            ]
        );

        assert!(
            build_foreground_daemon_args(Some(dir), Some("https://127.0.0.1:48123")).is_err()
        );
    }

    #[test]
    fn disk_attach_parses_explicit_format_flag() {
        for (flag, expected) in [
            ("vhdx", DiskFormat::Vhdx),
            ("vhd", DiskFormat::Vhd),
            ("raw", DiskFormat::Raw),
            ("iso", DiskFormat::Iso),
        ] {
            let parsed = Cli::try_parse_from([
                "openvmms",
                "vm",
                "disk",
                "attach",
                "MyVm",
                "disk.avhdx",
                "--format",
                flag,
            ])
            .unwrap();
            match parsed.command {
                TopCommand::Vm {
                    action:
                        VmCmd::Disk {
                            action: VmDiskCmd::Attach { format, .. },
                        },
                } => assert_eq!(format.map(DiskFormat::from), Some(expected)),
                other => panic!("unexpected parsed command: {other:?}"),
            }
        }

        let omitted =
            Cli::try_parse_from(["openvmms", "vm", "disk", "attach", "MyVm", "disk.img"]).unwrap();
        match omitted.command {
            TopCommand::Vm {
                action:
                    VmCmd::Disk {
                        action: VmDiskCmd::Attach { format, .. },
                    },
            } => assert!(format.is_none()),
            other => panic!("unexpected parsed command: {other:?}"),
        }
    }
}
