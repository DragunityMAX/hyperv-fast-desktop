use openvmms_core::api_client::ApiClient;
use openvmms_core::models::{
    AttachDiskPayload, CreateVmPayload, DiskFormat, FastDeskSettings, HostConfig, IpProto,
    NicAttachment, NicBackendConfig, PortForward, VmPreset, VmStatus, VmSummary,
};
use openvmms_core::store::RegistryStore;
use openvmms_core::ttrpc::{TtrpcClient, TtrpcStatus, encode_frame, read_frame};
use openvmms_core::vmservice;
use openvmmsd::TestDaemonHandle;
use openvmmsd::supervisor::Supervisor;
use prost::Message;
use socket2::{Domain, SockAddr, Socket, Type};
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

const MESSAGE_TYPE_REQUEST: u8 = 1;
const MESSAGE_TYPE_RESPONSE: u8 = 2;

#[derive(Clone, PartialEq, prost::Message)]
struct WireRequest {
    #[prost(string, tag = "1")]
    service: String,
    #[prost(string, tag = "2")]
    method: String,
    #[prost(bytes = "vec", tag = "3")]
    payload: Vec<u8>,
    #[prost(uint64, tag = "4")]
    timeout_nano: u64,
    #[prost(message, repeated, tag = "5")]
    metadata: Vec<WireKeyValue>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct WireKeyValue {
    #[prost(string, tag = "1")]
    key: String,
    #[prost(string, tag = "2")]
    value: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct WireResponse {
    #[prost(oneof = "wire_response::Result", tags = "1, 2")]
    result: Option<wire_response::Result>,
}

mod wire_response {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Result {
        #[prost(message, tag = "1")]
        Status(super::TtrpcStatus),
        #[prost(bytes, tag = "2")]
        Payload(Vec<u8>),
    }
}

fn parse_rpc_socket_arg(args: &[String]) -> Option<PathBuf> {
    for (idx, arg) in args.iter().enumerate() {
        if arg == "--rpc"
            && let Some(val) = args.get(idx + 1)
        {
            for part in val.split(',') {
                if let Some(p) = part.strip_prefix("path=") {
                    return Some(PathBuf::from(p));
                }
            }
        }
    }
    None
}

fn load_stub_config(state_dir: &Path) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if let Ok(text) = fs::read_to_string(state_dir.join("fake_openvmm_mode.txt")) {
        for line in text.lines() {
            if let Some((k, v)) = line.split_once('=') {
                map.insert(k.trim().to_string(), v.trim().to_string());
            }
        }
    }
    map
}

fn write_stub_mode(state_dir: &Path, mode: &str, extra: &[(&str, &str)]) {
    let mut text = format!("mode={mode}\n");
    for (k, v) in extra {
        text.push_str(&format!("{k}={v}\n"));
    }
    fs::write(state_dir.join("fake_openvmm_mode.txt"), text).unwrap();
}

fn bump_counter(path: &Path) -> u64 {
    let cur = fs::read_to_string(path)
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(0);
    let next = cur + 1;
    let _ = fs::write(path, next.to_string());
    next
}

fn touch(path: &Path) {
    let _ = fs::write(path, b"1");
}

fn send_ttrpc_ok(stream: &mut Socket, stream_id: u32, payload: Vec<u8>) {
    let resp = WireResponse {
        result: Some(wire_response::Result::Payload(payload)),
    };
    if let Ok(frame) = encode_frame(stream_id, MESSAGE_TYPE_RESPONSE, &resp.encode_to_vec()) {
        let _ = stream.write_all(&frame);
        let _ = stream.flush();
    }
}

fn send_ttrpc_err(stream: &mut Socket, stream_id: u32, code: i32, msg: &str) {
    let resp = WireResponse {
        result: Some(wire_response::Result::Status(TtrpcStatus {
            code,
            message: msg.to_string(),
            details: Vec::new(),
        })),
    };
    if let Ok(frame) = encode_frame(stream_id, MESSAGE_TYPE_RESPONSE, &resp.encode_to_vec()) {
        let _ = stream.write_all(&frame);
        let _ = stream.flush();
    }
}

struct StubCtx {
    state_dir: PathBuf,
    mode: String,
    launch: u64,
    endpoint: Option<String>,
    stop_evt: Arc<AtomicBool>,
    paused: Arc<AtomicBool>,
    wait_calls: Arc<AtomicUsize>,
    wait_in_flight: Arc<AtomicBool>,
}

struct WaitInFlightGuard(Arc<AtomicBool>);

impl Drop for WaitInFlightGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

fn handle_rpc_conn(mut stream: Socket, ctx: Arc<StubCtx>) {
    loop {
        let Ok((stream_id, msg_type, req_bytes)) = read_frame(&mut stream) else {
            return;
        };
        if msg_type != MESSAGE_TYPE_REQUEST {
            return;
        }
        let Ok(req) = WireRequest::decode(req_bytes.as_slice()) else {
            return;
        };

        match req.method.as_str() {
            "CreateVM" => {
                let Ok(create_req) = vmservice::CreateVmRequest::decode(req.payload.as_slice())
                else {
                    send_ttrpc_err(&mut stream, stream_id, 3, "invalid CreateVmRequest bytes");
                    return;
                };
                let valid = !create_req.log_id.trim().is_empty()
                    && create_req.config.as_ref().is_some_and(|c| {
                        c.memory_config.as_ref().is_some_and(|m| m.memory_mb >= 128)
                            && c.processor_config
                                .as_ref()
                                .is_some_and(|p| p.processor_count >= 1)
                            && matches!(
                                c.boot_config,
                                Some(vmservice::vm_config::BootConfig::Uefi(_))
                            )
                            && c.devices_config
                                .as_ref()
                                .is_some_and(|d| d.enable_scsi_controller)
                    });
                if !valid {
                    send_ttrpc_err(&mut stream, stream_id, 3, "incomplete CreateVmRequest fields");
                    return;
                }
                if ctx.mode == "race_restart" && ctx.launch == 1 {
                    touch(&ctx.state_dir.join("first_create_entered"));
                    std::thread::sleep(Duration::from_millis(700));
                    send_ttrpc_err(&mut stream, stream_id, 13, "delayed failure from cancelled startup");
                } else if ctx.mode == "slow_create" {
                    touch(&ctx.state_dir.join("create_entered"));
                    std::thread::sleep(Duration::from_millis(400));
                    send_ttrpc_ok(&mut stream, stream_id, Vec::new());
                } else {
                    send_ttrpc_ok(&mut stream, stream_id, Vec::new());
                }
            }
            "WaitVM" => {
                let call_idx = ctx.wait_calls.fetch_add(1, Ordering::SeqCst) + 1;
                if ctx.mode == "wait_transient_err" && call_idx == 1 {
                    touch(&ctx.state_dir.join("wait_transient_err_sent"));
                    send_ttrpc_err(&mut stream, stream_id, 14, "transient WaitVM stream glitch");
                    return;
                }
                if ctx.wait_in_flight.swap(true, Ordering::AcqRel) {
                    touch(&ctx.state_dir.join("wait_already_in_flight"));
                    send_ttrpc_err(&mut stream, stream_id, 9, "wait VM already in flight");
                    return;
                }
                let _in_flight = WaitInFlightGuard(Arc::clone(&ctx.wait_in_flight));
                let _ = stream.set_read_timeout(Some(Duration::from_millis(50)));
                for _ in 0..1200 {
                    if ctx.stop_evt.load(Ordering::Acquire) {
                        break;
                    }
                    let mut buf = [0u8; 1];
                    match stream.read(&mut buf) {
                        Ok(0) => return,
                        Ok(_) => {}
                        Err(err)
                            if matches!(
                                err.kind(),
                                std::io::ErrorKind::WouldBlock
                                    | std::io::ErrorKind::TimedOut
                                    | std::io::ErrorKind::Interrupted
                            ) => {}
                        Err(_) => return,
                    }
                }
                send_ttrpc_ok(&mut stream, stream_id, Vec::new());
            }
            "PropertiesVM" => {
                if ctx.mode == "readopt_props_fail" && ctx.state_dir.join("fail_next_props").exists()
                {
                    let _ = fs::remove_file(ctx.state_dir.join("fail_next_props"));
                    touch(&ctx.state_dir.join("props_failed_once"));
                    send_ttrpc_err(&mut stream, stream_id, 14, "simulated PropertiesVM failure");
                    return;
                }
                let state = if ctx.stop_evt.load(Ordering::Acquire) {
                    vmservice::VmState::Halted as i32
                } else if ctx.paused.load(Ordering::Acquire) {
                    vmservice::VmState::Paused as i32
                } else {
                    vmservice::VmState::Running as i32
                };
                let payload = vmservice::PropertiesVmResponse {
                    state,
                    fastdesk_listen_addr: ctx.endpoint.clone(),
                    ..Default::default()
                }
                .encode_to_vec();
                send_ttrpc_ok(&mut stream, stream_id, payload);
            }
            "PauseVM" => {
                if ctx.mode == "pause_resume" && ctx.launch == 1 {
                    touch(&ctx.state_dir.join("pause_entered"));
                    std::thread::sleep(Duration::from_millis(350));
                } else if ctx.mode == "concurrent_power" {
                    touch(&ctx.state_dir.join("power_pause_entered"));
                    std::thread::sleep(Duration::from_millis(300));
                }
                ctx.paused.store(true, Ordering::Release);
                send_ttrpc_ok(&mut stream, stream_id, Vec::new());
            }
            "ResumeVM" => {
                if ctx.mode == "pause_resume" && ctx.launch == 2 {
                    touch(&ctx.state_dir.join("resume_entered"));
                    std::thread::sleep(Duration::from_millis(350));
                }
                ctx.paused.store(false, Ordering::Release);
                send_ttrpc_ok(&mut stream, stream_id, Vec::new());
            }
            "TeardownVM" => {
                touch(&ctx.state_dir.join("teardown_called"));
                if ctx.mode == "concurrent_stop" && ctx.launch == 1 {
                    touch(&ctx.state_dir.join("teardown_entered"));
                    std::thread::sleep(Duration::from_millis(600));
                    touch(&ctx.state_dir.join("teardown_finished"));
                } else if ctx.mode == "pause_resume" {
                    if ctx.launch == 1 {
                        touch(&ctx.state_dir.join("teardown_entered_1"));
                        std::thread::sleep(Duration::from_millis(750));
                    } else if ctx.launch == 2 {
                        touch(&ctx.state_dir.join("teardown_entered_2"));
                        std::thread::sleep(Duration::from_millis(750));
                    }
                }
                ctx.stop_evt.store(true, Ordering::Release);
                send_ttrpc_ok(&mut stream, stream_id, Vec::new());
            }
            "Quit" => {
                if ctx.mode == "concurrent_stop" && ctx.launch == 2 {
                    touch(&ctx.state_dir.join("launch2_quit"));
                } else if ctx.mode == "pause_resume" {
                    if ctx.launch == 2 {
                        touch(&ctx.state_dir.join("launch2_quit"));
                    } else if ctx.launch == 3 {
                        touch(&ctx.state_dir.join("launch3_quit"));
                    }
                }
                ctx.stop_evt.store(true, Ordering::Release);
                send_ttrpc_ok(&mut stream, stream_id, Vec::new());
            }
            "ModifyResource" => {
                let Ok(mod_req) =
                    vmservice::ModifyResourceRequest::decode(req.payload.as_slice())
                else {
                    send_ttrpc_err(&mut stream, stream_id, 3, "invalid ModifyResourceRequest bytes");
                    return;
                };
                let valid = matches!(
                    mod_req.r#type,
                    x if x == vmservice::ModifyType::Add as i32
                        || x == vmservice::ModifyType::Remove as i32
                        || x == vmservice::ModifyType::Update as i32
                )
                    && match &mod_req.resource {
                        Some(vmservice::modify_resource_request::Resource::ScsiDisk(disk)) => {
                            !disk.host_path.is_empty()
                                || mod_req.r#type == vmservice::ModifyType::Remove as i32
                        }
                        Some(vmservice::modify_resource_request::Resource::NicConfig(nic)) => {
                            !nic.nic_id.is_empty()
                        }
                        Some(_) => true,
                        None => false,
                    };
                if !valid {
                    send_ttrpc_err(
                        &mut stream,
                        stream_id,
                        3,
                        "incomplete ModifyResourceRequest payload",
                    );
                    return;
                }
                if ctx.mode == "slow_modify_resource" {
                    touch(&ctx.state_dir.join("modify_resource_entered"));
                    std::thread::sleep(Duration::from_millis(350));
                }
                send_ttrpc_ok(&mut stream, stream_id, Vec::new());
            }
            _ => send_ttrpc_ok(&mut stream, stream_id, Vec::new()),
        }
    }
}

fn run_fake_openvmm_server(args: &[String]) {
    let sock_path = parse_rpc_socket_arg(args).expect("missing --rpc path=...");
    let state_dir = sock_path
        .parent()
        .expect("socket path must have parent directory")
        .to_path_buf();
    let cfg = load_stub_config(&state_dir);
    let mode = cfg.get("mode").map(String::as_str).unwrap_or("normal");
    let launch = bump_counter(&state_dir.join("launch_count"));

    if mode == "drop_start" {
        std::thread::sleep(Duration::from_millis(120));
        std::process::exit(1);
    }

    let _ = fs::remove_file(&sock_path);
    let listener = Socket::new(Domain::UNIX, Type::STREAM, None).expect("create AF_UNIX socket");
    let addr = SockAddr::unix(&sock_path).expect("AF_UNIX sockaddr");
    listener.bind(&addr).expect("bind AF_UNIX socket");
    listener.listen(64).expect("listen AF_UNIX socket");
    let _ = listener.set_nonblocking(true);

    let stop_evt = Arc::new(AtomicBool::new(false));
    let ctx = Arc::new(StubCtx {
        state_dir: state_dir.clone(),
        mode: mode.to_string(),
        launch,
        endpoint: cfg.get("endpoint").cloned(),
        stop_evt: Arc::clone(&stop_evt),
        paused: Arc::new(AtomicBool::new(false)),
        wait_calls: Arc::new(AtomicUsize::new(0)),
        wait_in_flight: Arc::new(AtomicBool::new(false)),
    });

    let max_deadline = std::time::Instant::now() + Duration::from_secs(60);
    while !stop_evt.load(Ordering::Acquire) && std::time::Instant::now() < max_deadline {
        if state_dir.join("exit_now").exists() {
            stop_evt.store(true, Ordering::Release);
            break;
        }
        match listener.accept() {
            Ok((conn, _)) => {
                let _ = conn.set_nonblocking(false);
                let ctx_clone = Arc::clone(&ctx);
                std::thread::spawn(move || handle_rpc_conn(conn, ctx_clone));
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(15));
            }
            Err(_) => break,
        }
    }
    // Give WaitVM/Quit responders a moment to flush replies before exiting.
    std::thread::sleep(Duration::from_millis(40));
}

async fn configure_stub_host(
    client: &reqwest::Client,
    daemon: &TestDaemonHandle,
    dir: &Path,
    fw_path: &Path,
) -> PathBuf {
    let stub_bin = std::env::current_exe().unwrap();
    let runtime_dir = dir.join("run");
    fs::create_dir_all(&runtime_dir).unwrap();
    let mut cfg: HostConfig = client
        .get(format!("{}/api/v1/host/config", daemon.base_url))
        .bearer_auth(&daemon.token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    cfg.runtime_dir = runtime_dir.clone();
    cfg.openvmm_bin = Some(stub_bin);
    cfg.uefi_firmware_path = Some(fw_path.to_path_buf());
    assert_eq!(
        client
            .put(format!("{}/api/v1/host/config", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&cfg)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    runtime_dir
}

async fn create_stub_vm(client: &reqwest::Client, daemon: &TestDaemonHandle, name: &str) -> VmSummary {
    let resp = client
        .post(format!("{}/api/v1/vms", daemon.base_url))
        .bearer_auth(&daemon.token)
        .json(&CreateVmPayload {
            name: name.into(),
            description: String::new(),
            preset: VmPreset::Custom,
            vcpus: Some(2),
            memory_mb: Some(1024),
            fastdesk: Some(FastDeskSettings {
                enabled: false,
                ..Default::default()
            }),
            nics: Some(vec![]),
            ..Default::default()
        })
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    resp.json().await.unwrap()
}

fn test_http_client() -> reqwest::Client {
    reqwest::Client::builder().no_proxy().build().unwrap()
}

async fn wait_for_file(path: &Path) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !path.exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for sentinel {}",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn test_dropping_start_vm_future_does_not_leave_vm_stuck_in_starting() {
    let dir = tempfile::tempdir().unwrap();
    let store = RegistryStore::new(dir.path()).unwrap();
    let fake_uefi = dir.path().join("MSVM.fd");
    fs::write(&fake_uefi, b"FAKE_UEFI").unwrap();

    let runtime_dir = dir.path().join("run");
    fs::create_dir_all(&runtime_dir).unwrap();
    let mut host = store.load_or_init_host_config().unwrap();
    host.runtime_dir = runtime_dir.clone();
    host.openvmm_bin = Some(std::env::current_exe().unwrap());
    host.uefi_firmware_path = Some(fake_uefi);
    store.save_host_config(&host).unwrap();
    write_stub_mode(&runtime_dir, "drop_start", &[]);

    let sup = Supervisor::new(dir.path(), Some("127.0.0.1:0")).unwrap();
    let created = sup
        .create_vm(CreateVmPayload {
            name: "DropStartTest".to_string(),
            vcpus: Some(2),
            memory_mb: Some(1024),
            ..Default::default()
        })
        .unwrap();
    let vm_id = created.config.id;

    tokio::select! {
        res = sup.start_vm("DropStartTest") => {
            panic!("expected start_vm to outlive 20ms cancel window, got {res:?}");
        }
        _ = tokio::time::sleep(Duration::from_millis(20)) => {}
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
    loop {
        let status = sup.get_vm(&vm_id.to_string()).unwrap().runtime.status;
        if status == VmStatus::Error {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "VM stayed in {status:?} after start_vm future was dropped"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn test_stopping_vm_during_startup_then_restarting_preserves_replacement_vm() {
    let dir = tempfile::tempdir().unwrap();
    let daemon = TestDaemonHandle::spawn(dir.path().to_path_buf())
        .await
        .unwrap();
    let client = test_http_client();
    let fw_path = dir.path().join("MSVM.fd");
    fs::write(&fw_path, b"FAKE_UEFI").unwrap();
    let runtime_dir = configure_stub_host(&client, &daemon, dir.path(), &fw_path).await;
    write_stub_mode(&runtime_dir, "race_restart", &[]);

    create_stub_vm(&client, &daemon, "Race-Restart-VM").await;

    let base_url = daemon.base_url.clone();
    let token = daemon.token.clone();
    let start1 = tokio::spawn(async move {
        test_http_client()
            .post(format!("{base_url}/api/v1/vms/Race-Restart-VM/start"))
            .bearer_auth(token)
            .send()
            .await
    });

    wait_for_file(&runtime_dir.join("first_create_entered")).await;

    let stop_resp = client
        .post(format!("{}/api/v1/vms/Race-Restart-VM/stop", daemon.base_url))
        .bearer_auth(&daemon.token)
        .send()
        .await
        .unwrap();
    assert_eq!(stop_resp.status(), 200);

    let start2_resp = client
        .post(format!("{}/api/v1/vms/Race-Restart-VM/start", daemon.base_url))
        .bearer_auth(&daemon.token)
        .send()
        .await
        .unwrap();
    assert_eq!(start2_resp.status(), 200);
    let started2: VmSummary = start2_resp.json().await.unwrap();
    assert_eq!(started2.runtime.status, VmStatus::Running);
    let pid2 = started2.runtime.pid.expect("replacement VM must have PID");

    let _ = start1.await;
    tokio::time::sleep(Duration::from_millis(850)).await;

    let check: VmSummary = client
        .get(format!("{}/api/v1/vms/Race-Restart-VM", daemon.base_url))
        .bearer_auth(&daemon.token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(check.runtime.status, VmStatus::Running);
    assert_eq!(check.runtime.pid, Some(pid2));

    assert_eq!(
        client
            .post(format!("{}/api/v1/vms/Race-Restart-VM/stop", daemon.base_url))
            .bearer_auth(&daemon.token)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    daemon.shutdown().await;
}

async fn test_concurrent_stop_requests_wait_for_cleanup_and_preserve_restarted_vm() {
    let dir = tempfile::tempdir().unwrap();
    let daemon = TestDaemonHandle::spawn(dir.path().to_path_buf())
        .await
        .unwrap();
    let client = test_http_client();
    let fw_path = dir.path().join("MSVM.fd");
    fs::write(&fw_path, b"FAKE_UEFI").unwrap();
    let runtime_dir = configure_stub_host(&client, &daemon, dir.path(), &fw_path).await;
    write_stub_mode(&runtime_dir, "concurrent_stop", &[]);

    create_stub_vm(&client, &daemon, "ConcurrentStop-VM").await;
    assert_eq!(
        client
            .post(format!("{}/api/v1/vms/ConcurrentStop-VM/start", daemon.base_url))
            .bearer_auth(&daemon.token)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );

    let base_url = daemon.base_url.clone();
    let token = daemon.token.clone();
    let stop1 = tokio::spawn(async move {
        test_http_client()
            .post(format!("{base_url}/api/v1/vms/ConcurrentStop-VM/stop"))
            .bearer_auth(token)
            .send()
            .await
            .unwrap()
    });

    wait_for_file(&runtime_dir.join("teardown_entered")).await;

    assert_eq!(
        client
            .post(format!("{}/api/v1/vms/ConcurrentStop-VM/start", daemon.base_url))
            .bearer_auth(&daemon.token)
            .send()
            .await
            .unwrap()
            .status(),
        409
    );

    let stop2_resp = client
        .post(format!("{}/api/v1/vms/ConcurrentStop-VM/stop", daemon.base_url))
        .bearer_auth(&daemon.token)
        .send()
        .await
        .unwrap();
    assert_eq!(stop2_resp.status(), 200);
    let stop2_vm: VmSummary = stop2_resp.json().await.unwrap();
    assert_eq!(stop2_vm.runtime.status, VmStatus::Off);
    assert!(runtime_dir.join("teardown_finished").exists());

    let stop1_resp = stop1.await.unwrap();
    assert_eq!(stop1_resp.status(), 200);
    assert_eq!(stop1_resp.json::<VmSummary>().await.unwrap().runtime.status, VmStatus::Off);

    let start2_resp = client
        .post(format!("{}/api/v1/vms/ConcurrentStop-VM/start", daemon.base_url))
        .bearer_auth(&daemon.token)
        .send()
        .await
        .unwrap();
    assert_eq!(start2_resp.status(), 200);
    let started2: VmSummary = start2_resp.json().await.unwrap();
    let pid2 = started2.runtime.pid.expect("replacement VM must have PID");

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!runtime_dir.join("launch2_quit").exists());

    let check: VmSummary = client
        .get(format!("{}/api/v1/vms/ConcurrentStop-VM", daemon.base_url))
        .bearer_auth(&daemon.token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(check.runtime.status, VmStatus::Running);
    assert_eq!(check.runtime.pid, Some(pid2));

    assert_eq!(
        client
            .post(format!("{}/api/v1/vms/ConcurrentStop-VM/stop", daemon.base_url))
            .bearer_auth(&daemon.token)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    daemon.shutdown().await;
}

async fn test_delayed_pause_and_resume_rpcs_do_not_break_concurrent_stop_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let daemon = TestDaemonHandle::spawn(dir.path().to_path_buf())
        .await
        .unwrap();
    let client = test_http_client();
    let fw_path = dir.path().join("MSVM.fd");
    fs::write(&fw_path, b"FAKE_UEFI").unwrap();
    let runtime_dir = configure_stub_host(&client, &daemon, dir.path(), &fw_path).await;
    write_stub_mode(&runtime_dir, "pause_resume", &[]);

    create_stub_vm(&client, &daemon, "PauseStop-VM").await;

    assert_eq!(
        client
            .post(format!("{}/api/v1/vms/PauseStop-VM/start", daemon.base_url))
            .bearer_auth(&daemon.token)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );

    let base_url = daemon.base_url.clone();
    let token = daemon.token.clone();
    let pause_task = tokio::spawn(async move {
        test_http_client()
            .post(format!("{base_url}/api/v1/vms/PauseStop-VM/pause"))
            .bearer_auth(token)
            .send()
            .await
            .unwrap()
    });
    wait_for_file(&runtime_dir.join("pause_entered")).await;

    let base_url = daemon.base_url.clone();
    let token = daemon.token.clone();
    let stop1_task = tokio::spawn(async move {
        test_http_client()
            .post(format!("{base_url}/api/v1/vms/PauseStop-VM/stop"))
            .bearer_auth(token)
            .send()
            .await
            .unwrap()
    });
    wait_for_file(&runtime_dir.join("teardown_entered_1")).await;

    assert_eq!(pause_task.await.unwrap().status(), 409);
    assert_eq!(stop1_task.await.unwrap().status(), 200);

    assert_eq!(
        client
            .post(format!("{}/api/v1/vms/PauseStop-VM/start", daemon.base_url))
            .bearer_auth(&daemon.token)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        client
            .post(format!("{}/api/v1/vms/PauseStop-VM/pause", daemon.base_url))
            .bearer_auth(&daemon.token)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );

    let base_url = daemon.base_url.clone();
    let token = daemon.token.clone();
    let resume_task = tokio::spawn(async move {
        test_http_client()
            .post(format!("{base_url}/api/v1/vms/PauseStop-VM/resume"))
            .bearer_auth(token)
            .send()
            .await
            .unwrap()
    });
    wait_for_file(&runtime_dir.join("resume_entered")).await;

    let base_url = daemon.base_url.clone();
    let token = daemon.token.clone();
    let stop2_task = tokio::spawn(async move {
        test_http_client()
            .post(format!("{base_url}/api/v1/vms/PauseStop-VM/stop"))
            .bearer_auth(token)
            .send()
            .await
            .unwrap()
    });
    wait_for_file(&runtime_dir.join("teardown_entered_2")).await;

    assert_eq!(resume_task.await.unwrap().status(), 409);
    assert_eq!(stop2_task.await.unwrap().status(), 200);

    let start3_resp = client
        .post(format!("{}/api/v1/vms/PauseStop-VM/start", daemon.base_url))
        .bearer_auth(&daemon.token)
        .send()
        .await
        .unwrap();
    assert_eq!(start3_resp.status(), 200);
    let started3: VmSummary = start3_resp.json().await.unwrap();
    let pid3 = started3.runtime.pid.expect("replacement VM must have PID");

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!runtime_dir.join("launch3_quit").exists());
    let check: VmSummary = client
        .get(format!("{}/api/v1/vms/PauseStop-VM", daemon.base_url))
        .bearer_auth(&daemon.token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(check.runtime.status, VmStatus::Running);
    assert_eq!(check.runtime.pid, Some(pid3));

    assert_eq!(
        client
            .post(format!("{}/api/v1/vms/PauseStop-VM/stop", daemon.base_url))
            .bearer_auth(&daemon.token)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    daemon.shutdown().await;
}

async fn test_wait_vm_transient_error_does_not_teardown_healthy_vm() {
    let dir = tempfile::tempdir().unwrap();
    let daemon = TestDaemonHandle::spawn(dir.path().to_path_buf())
        .await
        .unwrap();
    let client = test_http_client();
    let fw_path = dir.path().join("MSVM.fd");
    fs::write(&fw_path, b"FAKE_UEFI").unwrap();
    let runtime_dir = configure_stub_host(&client, &daemon, dir.path(), &fw_path).await;
    write_stub_mode(&runtime_dir, "wait_transient_err", &[]);

    create_stub_vm(&client, &daemon, "TransientWait-VM").await;
    assert_eq!(
        client
            .post(format!("{}/api/v1/vms/TransientWait-VM/start", daemon.base_url))
            .bearer_auth(&daemon.token)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );

    wait_for_file(&runtime_dir.join("wait_transient_err_sent")).await;
    tokio::time::sleep(Duration::from_millis(600)).await;

    assert!(
        !runtime_dir.join("teardown_called").exists(),
        "monitor sent TeardownVM on transient WaitVM RPC error"
    );
    let check: VmSummary = client
        .get(format!("{}/api/v1/vms/TransientWait-VM", daemon.base_url))
        .bearer_auth(&daemon.token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(check.runtime.status, VmStatus::Running);

    assert_eq!(
        client
            .post(format!("{}/api/v1/vms/TransientWait-VM/stop", daemon.base_url))
            .bearer_auth(&daemon.token)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    daemon.shutdown().await;
}

async fn test_readopted_vm_whose_initial_properties_failed_gets_monitored_and_transitions_on_exit() {
    let dir = tempfile::tempdir().unwrap();
    let fw_path = dir.path().join("MSVM.fd");
    fs::write(&fw_path, b"FAKE_UEFI").unwrap();

    let daemon1 = TestDaemonHandle::spawn(dir.path().to_path_buf())
        .await
        .unwrap();
    let client = test_http_client();
    let runtime_dir = configure_stub_host(&client, &daemon1, dir.path(), &fw_path).await;
    write_stub_mode(
        &runtime_dir,
        "readopt_props_fail",
        &[("endpoint", "tcp://127.0.0.1:45901")],
    );

    let created = create_stub_vm(&client, &daemon1, "ReadoptMonitor-VM").await;
    let start_resp = client
        .post(format!("{}/api/v1/vms/ReadoptMonitor-VM/start", daemon1.base_url))
        .bearer_auth(&daemon1.token)
        .send()
        .await
        .unwrap();
    assert_eq!(start_resp.status(), 200);
    let sock_path = runtime_dir.join(format!("{}.sock", created.config.id));

    // Detach daemon1 without stopping the VM, and arm the stub to fail the next PropertiesVM once.
    daemon1.shutdown_preserving_vms().await;
    touch(&runtime_dir.join("fail_next_props"));
    assert!(TtrpcClient::new(&sock_path).is_socket_accepting());

    let daemon2 = TestDaemonHandle::spawn(dir.path().to_path_buf())
        .await
        .unwrap();
    wait_for_file(&runtime_dir.join("props_failed_once")).await;

    let readopted: VmSummary = client
        .get(format!("{}/api/v1/vms/ReadoptMonitor-VM", daemon2.base_url))
        .bearer_auth(&daemon2.token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(readopted.runtime.status, VmStatus::Running);

    // Trigger stub process exit; the monitor on daemon2 must transition the VM out of Running.
    touch(&runtime_dir.join("exit_now"));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let vm: VmSummary = client
            .get(format!("{}/api/v1/vms/ReadoptMonitor-VM", daemon2.base_url))
            .bearer_auth(&daemon2.token)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if matches!(vm.runtime.status, VmStatus::Off | VmStatus::Error) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "re-adopted VM stayed stuck in {:?}",
            vm.runtime.status
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    daemon2.shutdown().await;
}

async fn test_daemon_stop_without_stop_vms_preserves_running_vm_and_readopts_on_restart() {
    let dir = tempfile::tempdir().unwrap();
    let fw_path = dir.path().join("MSVM.fd");
    fs::write(&fw_path, b"FAKE_UEFI").unwrap();

    let daemon1 = TestDaemonHandle::spawn(dir.path().to_path_buf())
        .await
        .unwrap();
    let client = test_http_client();
    let runtime_dir = configure_stub_host(&client, &daemon1, dir.path(), &fw_path).await;
    write_stub_mode(
        &runtime_dir,
        "normal",
        &[("endpoint", "tcp://127.0.0.1:45902")],
    );

    let created = create_stub_vm(&client, &daemon1, "SurviveStop-VM").await;
    assert_eq!(
        client
            .post(format!("{}/api/v1/vms/SurviveStop-VM/start", daemon1.base_url))
            .bearer_auth(&daemon1.token)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    let sock_path = runtime_dir.join(format!("{}.sock", created.config.id));

    // Default `daemon stop` (without ?stop_vms=true) must detach and keep the VM running.
    let base_url1 = daemon1.base_url.clone();
    let token1 = daemon1.token.clone();
    tokio::task::spawn_blocking(move || ApiClient::new(&base_url1, &token1)?.shutdown_daemon())
        .await
        .unwrap()
        .unwrap();
    drop(daemon1);

    // Wait briefly for daemon1 to release its lock and finish detaching.
    let store = RegistryStore::new(dir.path()).unwrap();
    let lock_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while store.is_daemon_locked() {
        assert!(tokio::time::Instant::now() < lock_deadline, "daemon1 never released lock");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    assert!(
        !runtime_dir.join("teardown_called").exists(),
        "default daemon stop called TeardownVM instead of detaching"
    );
    assert!(
        TtrpcClient::new(&sock_path).is_socket_accepting(),
        "VM RPC socket died on default daemon stop"
    );

    // Restart daemon and confirm the running VM is re-adopted with its FastDesk endpoint.
    let daemon2 = TestDaemonHandle::spawn(dir.path().to_path_buf())
        .await
        .unwrap();
    let readopted: VmSummary = client
        .get(format!("{}/api/v1/vms/SurviveStop-VM", daemon2.base_url))
        .bearer_auth(&daemon2.token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(readopted.runtime.status, VmStatus::Running);
    assert_eq!(
        readopted.runtime.fastdesk_endpoint.as_deref(),
        Some("tcp://127.0.0.1:45902")
    );

    // Now test explicit ?stop_vms=true stops the running VM.
    let base_url2 = daemon2.base_url.clone();
    let token2 = daemon2.token.clone();
    tokio::task::spawn_blocking(move || {
        ApiClient::new(&base_url2, &token2)?.shutdown_daemon_with_options(true)
    })
    .await
    .unwrap()
    .unwrap();
    drop(daemon2);
    let lock_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while store.is_daemon_locked() {
        assert!(tokio::time::Instant::now() < lock_deadline, "daemon2 never released lock");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    assert!(runtime_dir.join("teardown_called").exists());
    assert!(!TtrpcClient::new(&sock_path).is_socket_accepting());
}

struct ChildKillGuard(Option<Child>);

impl Drop for ChildKillGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn spawn_real_daemon(data_dir: &Path, listen: &str, log_path: &Path) -> ChildKillGuard {
    let err_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_openvmmsd"))
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--listen")
        .arg(listen)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::from(err_file))
        .spawn()
        .expect("spawning real openvmmsd binary");
    ChildKillGuard(Some(child))
}

async fn wait_for_real_daemon_health(data_dir: &Path, log_path: &Path) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let dir = data_dir.to_path_buf();
        let ready = tokio::task::spawn_blocking(move || {
            Ok::<bool, anyhow::Error>(
                ApiClient::from_runtime_addr(Some(&dir))?
                    .is_some_and(|c| c.health().is_ok()),
            )
        })
        .await
        .unwrap()
        .unwrap_or(false);
        if ready {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for real openvmmsd health; log:\n{}",
            fs::read_to_string(log_path).unwrap_or_default()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn run_real_api<R: Send + 'static>(
    data_dir: &Path,
    f: impl FnOnce(&ApiClient) -> anyhow::Result<R> + Send + 'static,
) -> R {
    let dir = data_dir.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let client = ApiClient::from_runtime_addr(Some(&dir))?
            .ok_or_else(|| anyhow::anyhow!("daemon runtime address unavailable"))?;
        f(&client)
    })
    .await
    .unwrap()
    .unwrap()
}

async fn wait_for_child_exit_zero(
    guard: &mut ChildKillGuard,
    timeout: Duration,
    label: &str,
    log_path: &Path,
) {
    let child = guard.0.as_mut().expect("daemon child missing");
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match child.try_wait().unwrap() {
            Some(status) => {
                let _ = guard.0.take();
                assert!(
                    status.success(),
                    "{label} exited with non-zero status {status}; log:\n{}",
                    fs::read_to_string(log_path).unwrap_or_default()
                );
                return;
            }
            None => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "{label} did not exit within {timeout:?}; log:\n{}",
                    fs::read_to_string(log_path).unwrap_or_default()
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    }
}

fn configure_real_daemon_stub_host(
    client: &ApiClient,
    dir: &Path,
    fw_path: &Path,
) -> PathBuf {
    let stub_bin = std::env::current_exe().unwrap();
    let runtime_dir = dir.join("run");
    fs::create_dir_all(&runtime_dir).unwrap();
    let mut cfg = client.host_config().unwrap();
    cfg.runtime_dir = runtime_dir.clone();
    cfg.openvmm_bin = Some(stub_bin);
    cfg.uefi_firmware_path = Some(fw_path.to_path_buf());
    client.update_host_config(&cfg).unwrap();
    runtime_dir
}

async fn test_real_daemon_process_exits_on_default_stop_and_sigterm_while_vm_running_and_readopts_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let fw_path = dir.path().join("MSVM.fd");
    fs::write(&fw_path, b"FAKE_UEFI").unwrap();
    let log_path = dir.path().join("daemon.stderr.log");

    let mut d1 = spawn_real_daemon(dir.path(), "127.0.0.1:0", &log_path);
    wait_for_real_daemon_health(dir.path(), &log_path).await;
    let dir_clone = dir.path().to_path_buf();
    let fw_clone = fw_path.clone();
    let runtime_dir = run_real_api(dir.path(), move |client1| {
        Ok(configure_real_daemon_stub_host(client1, &dir_clone, &fw_clone))
    })
    .await;
    write_stub_mode(
        &runtime_dir,
        "normal",
        &[("endpoint", "tcp://127.0.0.1:45911")],
    );

    let created = run_real_api(dir.path(), |client1| {
        client1.create_vm(&CreateVmPayload {
            name: "RealDaemon-VM".into(),
            description: String::new(),
            preset: VmPreset::Custom,
            vcpus: Some(2),
            memory_mb: Some(1024),
            fastdesk: Some(FastDeskSettings {
                enabled: false,
                ..Default::default()
            }),
            nics: Some(vec![]),
            ..Default::default()
        })
    })
    .await;
    let created_id = created.config.id.to_string();
    let started = run_real_api(dir.path(), move |client1| client1.start_vm(&created_id)).await;
    assert_eq!(started.runtime.status, VmStatus::Running);
    let sock_path = runtime_dir.join(format!("{}.sock", created.config.id));

    // Default `daemon stop` (without `--stop-vms`) must exit promptly while VM continues running.
    run_real_api(dir.path(), |client1| client1.shutdown_daemon()).await;
    wait_for_child_exit_zero(&mut d1, Duration::from_secs(5), "daemon1 default stop", &log_path)
        .await;
    assert!(!RegistryStore::open(dir.path()).is_daemon_locked());
    assert!(!runtime_dir.join("teardown_called").exists());
    assert!(TtrpcClient::new(&sock_path).is_socket_accepting());

    // Restart real daemon and verify clean WaitVM re-adoption with no duplicate waiter.
    let mut d2 = spawn_real_daemon(dir.path(), "127.0.0.1:0", &log_path);
    wait_for_real_daemon_health(dir.path(), &log_path).await;
    tokio::time::sleep(Duration::from_millis(350)).await;
    let vm2 = run_real_api(dir.path(), |client2| client2.get_vm("RealDaemon-VM")).await;
    assert_eq!(vm2.runtime.status, VmStatus::Running);
    assert_eq!(
        vm2.runtime.fastdesk_endpoint.as_deref(),
        Some("tcp://127.0.0.1:45911")
    );
    assert!(
        !runtime_dir.join("wait_already_in_flight").exists(),
        "old daemon leaked WaitVM waiter across default daemon stop"
    );

    // Send SIGTERM directly to the running daemon process on Unix and ensure prompt clean detach.
    #[cfg(unix)]
    {
        let pid2 = d2.0.as_ref().unwrap().id();
        let kill_status = Command::new("kill")
            .args(["-TERM", &pid2.to_string()])
            .status()
            .unwrap();
        assert!(kill_status.success(), "kill -TERM failed");
        wait_for_child_exit_zero(&mut d2, Duration::from_secs(5), "daemon2 SIGTERM", &log_path)
            .await;
        assert!(!RegistryStore::open(dir.path()).is_daemon_locked());
        assert!(!runtime_dir.join("teardown_called").exists());
        assert!(TtrpcClient::new(&sock_path).is_socket_accepting());
    }
    #[cfg(not(unix))]
    {
        run_real_api(dir.path(), |client2| client2.shutdown_daemon()).await;
        wait_for_child_exit_zero(&mut d2, Duration::from_secs(5), "daemon2 stop", &log_path).await;
    }

    // Restart real daemon a third time and verify WaitVM re-adopts cleanly after SIGTERM too.
    let mut d3 = spawn_real_daemon(dir.path(), "127.0.0.1:0", &log_path);
    wait_for_real_daemon_health(dir.path(), &log_path).await;
    tokio::time::sleep(Duration::from_millis(350)).await;
    let vm3 = run_real_api(dir.path(), |client3| client3.get_vm("RealDaemon-VM")).await;
    assert_eq!(vm3.runtime.status, VmStatus::Running);
    assert!(
        !runtime_dir.join("wait_already_in_flight").exists(),
        "old daemon leaked WaitVM waiter across SIGTERM"
    );

    run_real_api(dir.path(), |client3| client3.shutdown_daemon_with_options(true)).await;
    wait_for_child_exit_zero(&mut d3, Duration::from_secs(5), "daemon3 stop-vms", &log_path).await;
    assert!(runtime_dir.join("teardown_called").exists());
    assert!(!TtrpcClient::new(&sock_path).is_socket_accepting());
}

async fn test_failed_daemon_start_preserves_running_vm_runtime_files() {
    let dir = tempfile::tempdir().unwrap();
    let fw_path = dir.path().join("MSVM.fd");
    fs::write(&fw_path, b"FAKE_UEFI").unwrap();
    let log_path = dir.path().join("daemon.stderr.log");

    let mut d1 = spawn_real_daemon(dir.path(), "127.0.0.1:0", &log_path);
    wait_for_real_daemon_health(dir.path(), &log_path).await;
    let dir_clone = dir.path().to_path_buf();
    let fw_clone = fw_path.clone();
    let runtime_dir = run_real_api(dir.path(), move |client1| {
        Ok(configure_real_daemon_stub_host(client1, &dir_clone, &fw_clone))
    })
    .await;
    write_stub_mode(&runtime_dir, "normal", &[("endpoint", "tcp://127.0.0.1:45912")]);

    let (created, started) = run_real_api(dir.path(), |client1| {
        let created = client1.create_vm(&CreateVmPayload {
            name: "PreserveFiles-VM".into(),
            description: String::new(),
            preset: VmPreset::Custom,
            vcpus: Some(2),
            memory_mb: Some(1024),
            fastdesk: Some(FastDeskSettings {
                enabled: true,
                use_shm: true,
                ..Default::default()
            }),
            nics: Some(vec![]),
            ..Default::default()
        })?;
        let started = client1.start_vm(&created.config.id.to_string())?;
        Ok((created, started))
    })
    .await;
    assert_eq!(started.runtime.status, VmStatus::Running);

    let id = created.config.id;
    let sock_path = runtime_dir.join(format!("{id}.sock"));
    let pid_path = runtime_dir.join(format!("{id}.pid"));
    let shm_path = started
        .runtime
        .shm_path
        .map(PathBuf::from)
        .unwrap_or_else(|| runtime_dir.join(format!("{id}.shm")));
    if !shm_path.exists() {
        fs::write(&shm_path, b"FAKE_SHM_BACKING").unwrap();
    }

    run_real_api(dir.path(), |client1| client1.shutdown_daemon()).await;
    wait_for_child_exit_zero(&mut d1, Duration::from_secs(5), "daemon1 detach", &log_path).await;
    assert!(sock_path.exists());
    assert!(pid_path.exists());
    assert!(shm_path.exists());

    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let busy_listen = occupied.local_addr().unwrap().to_string();

    for bad_listen in ["badhost:47620", "0.0.0.0:47620", busy_listen.as_str()] {
        let out = Command::new(env!("CARGO_BIN_EXE_openvmmsd"))
            .arg("--data-dir")
            .arg(dir.path())
            .arg("--listen")
            .arg(bad_listen)
            .output()
            .expect("invoking failing openvmmsd start");
        assert!(
            !out.status.success(),
            "expected openvmmsd --listen {bad_listen} to fail"
        );
        assert!(
            sock_path.exists(),
            "failing start with --listen {bad_listen} removed .sock file"
        );
        assert!(
            pid_path.exists(),
            "failing start with --listen {bad_listen} removed .pid file"
        );
        assert!(
            shm_path.exists(),
            "failing start with --listen {bad_listen} removed .shm file"
        );
        assert!(
            TtrpcClient::new(&sock_path).is_socket_accepting(),
            "running VM socket died during failed start with --listen {bad_listen}"
        );
        assert!(!runtime_dir.join("teardown_called").exists());
    }
    drop(occupied);

    // Valid restart re-adopts the untouched running VM cleanly.
    let mut d2 = spawn_real_daemon(dir.path(), "127.0.0.1:0", &log_path);
    wait_for_real_daemon_health(dir.path(), &log_path).await;
    let vm2 = run_real_api(dir.path(), |client2| client2.get_vm("PreserveFiles-VM")).await;
    assert_eq!(vm2.runtime.status, VmStatus::Running);
    assert!(shm_path.exists());

    run_real_api(dir.path(), |client2| client2.shutdown_daemon_with_options(true)).await;
    wait_for_child_exit_zero(&mut d2, Duration::from_secs(5), "daemon2 stop-vms", &log_path).await;
}

async fn test_disconnecting_mid_disk_hotplug_and_port_forward_still_commits_config() {
    let dir = tempfile::tempdir().unwrap();
    let store = RegistryStore::new(dir.path()).unwrap();
    let fw_path = dir.path().join("MSVM.fd");
    fs::write(&fw_path, b"FAKE_UEFI").unwrap();

    let runtime_dir = dir.path().join("run");
    fs::create_dir_all(&runtime_dir).unwrap();
    let mut host = store.load_or_init_host_config().unwrap();
    host.runtime_dir = runtime_dir.clone();
    host.openvmm_bin = Some(std::env::current_exe().unwrap());
    host.uefi_firmware_path = Some(fw_path);
    store.save_host_config(&host).unwrap();
    write_stub_mode(&runtime_dir, "slow_modify_resource", &[]);

    let sup = Arc::new(Supervisor::new(dir.path(), Some("127.0.0.1:0")).unwrap());
    let mut default_nic = NicAttachment::default_consomme();
    let nic_id = default_nic.id;
    default_nic.mac_address = "00:15:5d:00:01:02".into();
    let created = sup
        .create_vm(CreateVmPayload {
            name: "DetachRpc-VM".into(),
            preset: VmPreset::Custom,
            vcpus: Some(2),
            memory_mb: Some(1024),
            fastdesk: Some(FastDeskSettings {
                enabled: false,
                ..Default::default()
            }),
            nics: Some(vec![default_nic]),
            ..Default::default()
        })
        .unwrap();
    let vm_id = created.config.id;
    let started = sup.start_vm(&vm_id.to_string()).await.unwrap();
    assert_eq!(started.runtime.status, VmStatus::Running);

    let vm_id_str = vm_id.to_string();
    let modify_sentinel = runtime_dir.join("modify_resource_entered");

    // 1. Hot-plug a disk, drop the caller future mid-ModifyResource, verify detached task commits it.
    let hotplug_disk = dir.path().join("hotplug.img");
    fs::write(&hotplug_disk, vec![0u8; 65536]).unwrap();
    {
        let sup_attach = Arc::clone(&sup);
        let disk_path = hotplug_disk.clone();
        let attach_fut = sup_attach.attach_disk(
            &vm_id_str,
            AttachDiskPayload {
                path: disk_path,
                lun: Some(2),
                read_only: false,
                format: Some(DiskFormat::Raw),
            },
        );
        tokio::pin!(attach_fut);
        tokio::select! {
            res = &mut attach_fut => panic!("attach_disk finished before ModifyResource delay: {res:?}"),
            _ = wait_for_file(&modify_sentinel) => {}
        }
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let vm = sup.get_vm(&vm_id_str).unwrap();
        if vm.config.disks.iter().any(|d| d.lun == 2 && d.path == hotplug_disk) {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "detached attach_disk never committed");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let persisted_after_disk = RegistryStore::open(dir.path())
        .load_all_vms()
        .unwrap()
        .into_iter()
        .find(|v| v.id == vm_id)
        .unwrap();
    assert!(
        persisted_after_disk
            .disks
            .iter()
            .any(|d| d.lun == 2 && d.path == hotplug_disk)
    );

    // 2. Add a port forward, drop the caller future mid-ModifyResource, verify detached task commits it.
    let _ = fs::remove_file(&modify_sentinel);
    let pf = PortForward {
        protocol: IpProto::Udp,
        host_addr: "127.0.0.1".into(),
        host_port: 15353,
        guest_port: 53,
    };
    {
        let sup_pf = Arc::clone(&sup);
        let pf_clone = pf.clone();
        let pf_fut = sup_pf.add_port_forward(&vm_id_str, nic_id, pf_clone);
        tokio::pin!(pf_fut);
        tokio::select! {
            res = &mut pf_fut => panic!("add_port_forward finished before ModifyResource delay: {res:?}"),
            _ = wait_for_file(&modify_sentinel) => {}
        }
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let vm = sup.get_vm(&vm_id.to_string()).unwrap();
        let has_pf = vm.config.nics.iter().any(|n| {
            matches!(&n.backend, NicBackendConfig::Consomme { port_forwards, .. } if port_forwards.contains(&pf))
        });
        if has_pf {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "detached add_port_forward never committed"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let persisted_after_pf = RegistryStore::open(dir.path())
        .load_all_vms()
        .unwrap()
        .into_iter()
        .find(|v| v.id == vm_id)
        .unwrap();
    assert!(persisted_after_pf.nics.iter().any(|n| {
        matches!(&n.backend, NicBackendConfig::Consomme { port_forwards, .. } if port_forwards.contains(&pf))
    }));

    sup.shutdown_all_vms().await;
}

async fn test_slow_reclaim_start_does_not_clobber_replacement_start() {
    let dir = tempfile::tempdir().unwrap();
    let daemon = TestDaemonHandle::spawn(dir.path().to_path_buf())
        .await
        .unwrap();
    let client = test_http_client();
    let fw_path = dir.path().join("MSVM.fd");
    fs::write(&fw_path, b"FAKE_UEFI").unwrap();
    let runtime_dir = configure_stub_host(&client, &daemon, dir.path(), &fw_path).await;
    write_stub_mode(&runtime_dir, "normal", &[]);

    let resp = client
        .post(format!("{}/api/v1/vms", daemon.base_url))
        .bearer_auth(&daemon.token)
        .json(&CreateVmPayload {
            name: "ReclaimRace-VM".into(),
            description: String::new(),
            preset: VmPreset::Custom,
            vcpus: Some(2),
            memory_mb: Some(1024),
            fastdesk: Some(FastDeskSettings {
                enabled: true,
                use_shm: true,
                ..Default::default()
            }),
            nics: Some(vec![]),
            ..Default::default()
        })
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    let created: VmSummary = resp.json().await.unwrap();
    let id = created.config.id;
    let sock_path = runtime_dir.join(format!("{id}.sock"));
    let pid_path = sock_path.with_extension("pid");
    let shm_path = runtime_dir.join(format!("vram-{id}.shm"));

    // Plant a slow leftover RPC listener on `<id>.sock` that stalls during TeardownVM.
    let leftover_listener = Socket::new(Domain::UNIX, Type::STREAM, None).unwrap();
    leftover_listener.bind(&SockAddr::unix(&sock_path).unwrap()).unwrap();
    leftover_listener.listen(16).unwrap();
    let _ = leftover_listener.set_nonblocking(true);
    let mut dead_child = Command::new(std::env::current_exe().unwrap())
        .arg("--noop-exit")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let dead_pid = dead_child.id();
    let _ = dead_child.wait();
    fs::write(&pid_path, format!("{dead_pid}\n1")).unwrap();
    fs::write(&shm_path, b"STALE_SHM").unwrap();

    let teardown_entered = Arc::new(AtomicBool::new(false));
    let stop_leftover = Arc::new(AtomicBool::new(false));
    let teardown_entered_srv = Arc::clone(&teardown_entered);
    let stop_leftover_srv = Arc::clone(&stop_leftover);
    let leftover_srv = std::thread::spawn(move || {
        let srv_deadline = std::time::Instant::now() + Duration::from_secs(30);
        while !stop_leftover_srv.load(Ordering::Acquire)
            && std::time::Instant::now() < srv_deadline
        {
            match leftover_listener.accept() {
                Ok((mut conn, _)) => {
                    let _ = conn.set_nonblocking(false);
                    let te = Arc::clone(&teardown_entered_srv);
                    std::thread::spawn(move || {
                        while let Ok((stream_id, msg_type, bytes)) = read_frame(&mut conn) {
                            if msg_type != MESSAGE_TYPE_REQUEST {
                                break;
                            }
                            let Ok(req) = WireRequest::decode(bytes.as_slice()) else {
                                break;
                            };
                            match req.method.as_str() {
                                "PropertiesVM" => {
                                    let payload = vmservice::PropertiesVmResponse {
                                        state: vmservice::VmState::Running as i32,
                                        ..Default::default()
                                    }
                                    .encode_to_vec();
                                    send_ttrpc_ok(&mut conn, stream_id, payload);
                                }
                                "TeardownVM" => {
                                    te.store(true, Ordering::Release);
                                    std::thread::sleep(Duration::from_millis(600));
                                    send_ttrpc_ok(&mut conn, stream_id, Vec::new());
                                }
                                "Quit" => {
                                    send_ttrpc_ok(&mut conn, stream_id, Vec::new());
                                    break;
                                }
                                _ => send_ttrpc_ok(&mut conn, stream_id, Vec::new()),
                            }
                        }
                    });
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(15));
                }
                Err(_) => break,
            }
        }
    });

    let base_url = daemon.base_url.clone();
    let token = daemon.token.clone();
    let start1 = tokio::spawn(async move {
        test_http_client()
            .post(format!("{base_url}/api/v1/vms/ReclaimRace-VM/start"))
            .bearer_auth(token)
            .send()
            .await
    });

    let enter_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while !teardown_entered.load(Ordering::Acquire) {
        assert!(
            tokio::time::Instant::now() < enter_deadline,
            "start1 did not reach leftover socket TeardownVM"
        );
        tokio::time::sleep(Duration::from_millis(15)).await;
    }

    // Cancel start1 via stop, drop the leftover listener, and immediately start replacement VM.
    assert_eq!(
        client
            .post(format!("{}/api/v1/vms/ReclaimRace-VM/stop", daemon.base_url))
            .bearer_auth(&daemon.token)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    stop_leftover.store(true, Ordering::Release);

    let start2_resp = client
        .post(format!("{}/api/v1/vms/ReclaimRace-VM/start", daemon.base_url))
        .bearer_auth(&daemon.token)
        .send()
        .await
        .unwrap();
    assert_eq!(start2_resp.status(), 200);
    let started2: VmSummary = start2_resp.json().await.unwrap();
    assert_eq!(started2.runtime.status, VmStatus::Running);
    let pid2 = started2.runtime.pid.expect("replacement VM must have PID");

    let _ = start1.await;
    let _ = leftover_srv.join();
    tokio::time::sleep(Duration::from_millis(700)).await;

    // Start #1's aborted/slow reclaim must NOT have deleted or truncated Start #2's files!
    let check: VmSummary = client
        .get(format!("{}/api/v1/vms/ReclaimRace-VM", daemon.base_url))
        .bearer_auth(&daemon.token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(check.runtime.status, VmStatus::Running);
    assert_eq!(check.runtime.pid, Some(pid2));
    assert!(sock_path.exists(), "replacement .sock was deleted by stale start");
    assert!(pid_path.exists(), "replacement .pid was deleted by stale start");
    assert!(shm_path.exists(), "replacement .shm was deleted by stale start");
    assert_eq!(
        openvmms_core::process::read_pid_file(&pid_path).map(|(p, _)| p),
        Some(pid2),
        ".pid no longer matches replacement VM"
    );
    assert!(
        fs::metadata(&shm_path).unwrap().len() >= 8 * 1024 * 1024,
        "replacement .shm was truncated or shrunken"
    );
    assert!(TtrpcClient::new(&sock_path).is_socket_accepting());

    assert_eq!(
        client
            .post(format!("{}/api/v1/vms/ReclaimRace-VM/stop", daemon.base_url))
            .bearer_auth(&daemon.token)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    daemon.shutdown().await;
}

async fn test_concurrent_pause_and_resume_preserve_actual_vm_state() {
    let dir = tempfile::tempdir().unwrap();
    let daemon = TestDaemonHandle::spawn(dir.path().to_path_buf())
        .await
        .unwrap();
    let client = test_http_client();
    let fw_path = dir.path().join("MSVM.fd");
    fs::write(&fw_path, b"FAKE_UEFI").unwrap();
    let runtime_dir = configure_stub_host(&client, &daemon, dir.path(), &fw_path).await;
    write_stub_mode(&runtime_dir, "concurrent_power", &[]);

    let created = create_stub_vm(&client, &daemon, "ConcurrentPower-VM").await;
    let sock_path = runtime_dir.join(format!("{}.sock", created.config.id));
    assert_eq!(
        client
            .post(format!("{}/api/v1/vms/ConcurrentPower-VM/start", daemon.base_url))
            .bearer_auth(&daemon.token)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );

    let base_url1 = daemon.base_url.clone();
    let token1 = daemon.token.clone();
    let pause_fut = tokio::spawn(async move {
        test_http_client()
            .post(format!("{base_url1}/api/v1/vms/ConcurrentPower-VM/pause"))
            .bearer_auth(token1)
            .send()
            .await
            .unwrap()
    });
    wait_for_file(&runtime_dir.join("power_pause_entered")).await;

    // Issue ResumeVM concurrently while PauseVM is still running in the stub; per-VM
    // serialization must order them so both OpenVMM and daemon end up Running.
    let resume_resp = client
        .post(format!("{}/api/v1/vms/ConcurrentPower-VM/resume", daemon.base_url))
        .bearer_auth(&daemon.token)
        .send()
        .await
        .unwrap();
    assert_eq!(resume_resp.status(), 200);
    assert_eq!(pause_fut.await.unwrap().status(), 200);

    let vm: VmSummary = client
        .get(format!("{}/api/v1/vms/ConcurrentPower-VM", daemon.base_url))
        .bearer_auth(&daemon.token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(vm.runtime.status, VmStatus::Running);
    let props = TtrpcClient::new(&sock_path)
        .properties_vm(vmservice::PropertiesVmRequest { types: vec![] })
        .await
        .unwrap();
    assert_eq!(props.state, vmservice::VmState::Running as i32);

    assert_eq!(
        client
            .post(format!("{}/api/v1/vms/ConcurrentPower-VM/shutdown", daemon.base_url))
            .bearer_auth(&daemon.token)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        client
            .post(format!("{}/api/v1/vms/ConcurrentPower-VM/stop", daemon.base_url))
            .bearer_auth(&daemon.token)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    daemon.shutdown().await;
}

async fn test_runtime_dir_change_is_rejected_while_vm_is_starting() {
    let dir = tempfile::tempdir().unwrap();
    let daemon = TestDaemonHandle::spawn(dir.path().to_path_buf())
        .await
        .unwrap();
    let client = test_http_client();
    let fw_path = dir.path().join("MSVM.fd");
    fs::write(&fw_path, b"FAKE_UEFI").unwrap();
    let runtime_dir = configure_stub_host(&client, &daemon, dir.path(), &fw_path).await;
    write_stub_mode(&runtime_dir, "slow_create", &[]);

    create_stub_vm(&client, &daemon, "StartingHostCfg-VM").await;

    let base_url = daemon.base_url.clone();
    let token = daemon.token.clone();
    let start_task = tokio::spawn(async move {
        test_http_client()
            .post(format!("{base_url}/api/v1/vms/StartingHostCfg-VM/start"))
            .bearer_auth(token)
            .send()
            .await
            .unwrap()
    });
    wait_for_file(&runtime_dir.join("create_entered")).await;

    let mut cfg: HostConfig = client
        .get(format!("{}/api/v1/host/config", daemon.base_url))
        .bearer_auth(&daemon.token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    cfg.runtime_dir = dir.path().join("run-new");
    let put_resp = client
        .put(format!("{}/api/v1/host/config", daemon.base_url))
        .bearer_auth(&daemon.token)
        .json(&cfg)
        .send()
        .await
        .unwrap();
    assert_eq!(put_resp.status(), 409);

    assert_eq!(start_task.await.unwrap().status(), 200);
    assert_eq!(
        client
            .post(format!("{}/api/v1/vms/StartingHostCfg-VM/stop", daemon.base_url))
            .bearer_auth(&daemon.token)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    daemon.shutdown().await;
}

async fn test_start_vm_rejected_during_daemon_shutdown_and_detach_never_kills_vm() {
    let dir = tempfile::tempdir().unwrap();
    let store = RegistryStore::new(dir.path()).unwrap();
    let fw_path = dir.path().join("MSVM.fd");
    fs::write(&fw_path, b"FAKE_UEFI").unwrap();

    let runtime_dir = dir.path().join("run");
    fs::create_dir_all(&runtime_dir).unwrap();
    let mut host = store.load_or_init_host_config().unwrap();
    host.runtime_dir = runtime_dir.clone();
    host.openvmm_bin = Some(std::env::current_exe().unwrap());
    host.uefi_firmware_path = Some(fw_path);
    store.save_host_config(&host).unwrap();
    write_stub_mode(&runtime_dir, "normal", &[]);

    let sup = Supervisor::new(dir.path(), Some("127.0.0.1:0")).unwrap();
    let running_vm = sup
        .create_vm(CreateVmPayload {
            name: "DetachAlive-VM".into(),
            preset: VmPreset::Custom,
            vcpus: Some(2),
            memory_mb: Some(1024),
            fastdesk: Some(FastDeskSettings {
                enabled: false,
                ..Default::default()
            }),
            nics: Some(vec![]),
            ..Default::default()
        })
        .unwrap();
    let blocked_vm = sup
        .create_vm(CreateVmPayload {
            name: "BlockedDuringShutdown-VM".into(),
            preset: VmPreset::Custom,
            vcpus: Some(2),
            memory_mb: Some(1024),
            fastdesk: Some(FastDeskSettings {
                enabled: false,
                ..Default::default()
            }),
            nics: Some(vec![]),
            ..Default::default()
        })
        .unwrap();

    let started = sup.start_vm(&running_vm.config.id.to_string()).await.unwrap();
    assert_eq!(started.runtime.status, VmStatus::Running);
    let sock_path = runtime_dir.join(format!("{}.sock", running_vm.config.id));

    sup.mark_shutting_down();
    let err = sup
        .start_vm(&blocked_vm.config.id.to_string())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("shutting down"),
        "expected shutdown rejection error, got: {err:#}"
    );

    sup.detach_all_vms().await;
    drop(sup);

    assert!(
        !runtime_dir.join("teardown_called").exists(),
        "detach killed running VM"
    );
    assert!(
        TtrpcClient::new(&sock_path).is_socket_accepting(),
        "detached VM socket closed on supervisor drop"
    );

    touch(&runtime_dir.join("exit_now"));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while TtrpcClient::new(&sock_path).is_socket_accepting() {
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let _ = fs::remove_file(&sock_path);
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--noop-exit") {
        return;
    }
    if args.iter().any(|a| a == "--rpc") {
        run_fake_openvmm_server(&args);
        return;
    }

    let filter = args
        .iter()
        .skip(1)
        .find(|a| !a.starts_with('-'))
        .cloned();

    let rt = tokio::runtime::Runtime::new().unwrap();
    let tests: Vec<(&str, std::pin::Pin<Box<dyn std::future::Future<Output = ()>>>)> = vec![
        (
            "dropping_start_vm_future_does_not_leave_vm_stuck_in_starting",
            Box::pin(test_dropping_start_vm_future_does_not_leave_vm_stuck_in_starting()),
        ),
        (
            "stopping_vm_during_startup_then_restarting_preserves_replacement_vm",
            Box::pin(test_stopping_vm_during_startup_then_restarting_preserves_replacement_vm()),
        ),
        (
            "concurrent_stop_requests_wait_for_cleanup_and_preserve_restarted_vm",
            Box::pin(test_concurrent_stop_requests_wait_for_cleanup_and_preserve_restarted_vm()),
        ),
        (
            "delayed_pause_and_resume_rpcs_do_not_break_concurrent_stop_and_restart",
            Box::pin(test_delayed_pause_and_resume_rpcs_do_not_break_concurrent_stop_and_restart()),
        ),
        (
            "wait_vm_transient_error_does_not_teardown_healthy_vm",
            Box::pin(test_wait_vm_transient_error_does_not_teardown_healthy_vm()),
        ),
        (
            "readopted_vm_whose_initial_properties_failed_gets_monitored_and_transitions_on_exit",
            Box::pin(test_readopted_vm_whose_initial_properties_failed_gets_monitored_and_transitions_on_exit()),
        ),
        (
            "daemon_stop_without_stop_vms_preserves_running_vm_and_readopts_on_restart",
            Box::pin(test_daemon_stop_without_stop_vms_preserves_running_vm_and_readopts_on_restart()),
        ),
        (
            "real_daemon_process_exits_on_default_stop_and_sigterm_while_vm_running_and_readopts_cleanly",
            Box::pin(test_real_daemon_process_exits_on_default_stop_and_sigterm_while_vm_running_and_readopts_cleanly()),
        ),
        (
            "failed_daemon_start_preserves_running_vm_runtime_files",
            Box::pin(test_failed_daemon_start_preserves_running_vm_runtime_files()),
        ),
        (
            "disconnecting_mid_disk_hotplug_and_port_forward_still_commits_config",
            Box::pin(test_disconnecting_mid_disk_hotplug_and_port_forward_still_commits_config()),
        ),
        (
            "slow_reclaim_start_does_not_clobber_replacement_start",
            Box::pin(test_slow_reclaim_start_does_not_clobber_replacement_start()),
        ),
        (
            "concurrent_pause_and_resume_preserve_actual_vm_state",
            Box::pin(test_concurrent_pause_and_resume_preserve_actual_vm_state()),
        ),
        (
            "runtime_dir_change_is_rejected_while_vm_is_starting",
            Box::pin(test_runtime_dir_change_is_rejected_while_vm_is_starting()),
        ),
        (
            "start_vm_rejected_during_daemon_shutdown_and_detach_never_kills_vm",
            Box::pin(test_start_vm_rejected_during_daemon_shutdown_and_detach_never_kills_vm()),
        ),
    ];

    let mut ran = 0usize;
    for (name, fut) in tests {
        if let Some(ref f) = filter
            && !name.contains(f)
        {
            continue;
        }
        eprintln!("running lifecycle_races::{name} ...");
        rt.block_on(async {
            tokio::time::timeout(Duration::from_secs(30), fut)
                .await
                .unwrap_or_else(|_| panic!("lifecycle_races::{name} timed out after 30s"));
        });
        eprintln!("  ok");
        ran += 1;
    }
    println!("lifecycle_races: {ran} passed; 0 failed");
}
