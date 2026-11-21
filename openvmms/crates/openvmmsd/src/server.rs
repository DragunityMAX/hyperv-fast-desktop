use crate::supervisor::ApiErrorKind;
use crate::supervisor::Supervisor;
use axum::Json;
use axum::Router;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::Request;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::http::header::AUTHORIZATION;
use axum::middleware;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::response::sse::Event;
use axum::response::sse::KeepAlive;
use axum::response::sse::Sse;
use axum::routing::delete;
use axum::routing::get;
use axum::routing::post;
use futures_util::Stream;
use openvmms_core::models::ApiErrorBody;
use openvmms_core::models::AttachDiskPayload;
use openvmms_core::models::CreateDiskPayload;
use openvmms_core::models::CreateVmPayload;
use openvmms_core::models::DaemonEvent;
use openvmms_core::models::DiskInfo;
use openvmms_core::models::HostConfig;
use openvmms_core::models::HostInfo;
use openvmms_core::models::InspectDiskPayload;
use openvmms_core::models::MountIsoPayload;
use openvmms_core::models::PortForwardPayload;
use openvmms_core::models::ShutdownVmPayload;
use openvmms_core::models::UpdateVmPayload;
use openvmms_core::models::VirtualSwitchInfo;
use openvmms_core::models::VmSummary;
use serde::Deserialize;
use serde::Serialize;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use uuid::Uuid;

struct CancelOnDrop(Arc<AtomicBool>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

#[derive(Clone)]
pub struct AppState {
    pub supervisor: Arc<Supervisor>,
    pub token: String,
    pub bound_addr: SocketAddr,
}

struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn map_anyhow(err: anyhow::Error) -> Self {
        let status = match err
            .chain()
            .find_map(|cause| cause.downcast_ref::<ApiErrorKind>())
        {
            Some(ApiErrorKind::NotFound(_)) => StatusCode::NOT_FOUND,
            Some(ApiErrorKind::Conflict(_)) => StatusCode::CONFLICT,
            Some(ApiErrorKind::BadRequest(_)) => StatusCode::BAD_REQUEST,
            Some(ApiErrorKind::Internal(_)) | None => StatusCode::INTERNAL_SERVER_ERROR,
        };
        Self {
            status,
            message: format!("{err:#}"),
        }
    }

    fn internal(err: impl ToString) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: err.to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(ApiErrorBody {
                error: self.message,
            }),
        )
            .into_response()
    }
}

pub fn build_router(supervisor: Arc<Supervisor>, token: String, bound_addr: SocketAddr) -> Router {
    let state = AppState {
        supervisor,
        token,
        bound_addr,
    };

    let protected = Router::new()
        .route("/api/v1/events", get(events_handler))
        .route("/api/v1/host/info", get(host_info))
        .route(
            "/api/v1/host/config",
            get(get_host_config).put(put_host_config),
        )
        .route("/api/v1/host/switches", get(list_switches))
        .route("/api/v1/daemon/shutdown", post(daemon_shutdown))
        .route("/api/v1/disks/create", post(create_disk))
        .route("/api/v1/disks/inspect", post(inspect_disk))
        .route("/api/v1/vms", get(list_vms).post(create_vm))
        .route(
            "/api/v1/vms/{id}",
            get(get_vm).put(update_vm).delete(delete_vm),
        )
        .route("/api/v1/vms/{id}/start", post(start_vm))
        .route("/api/v1/vms/{id}/stop", post(stop_vm))
        .route("/api/v1/vms/{id}/shutdown", post(shutdown_vm))
        .route("/api/v1/vms/{id}/pause", post(pause_vm))
        .route("/api/v1/vms/{id}/resume", post(resume_vm))
        .route("/api/v1/vms/{id}/reset", post(reset_vm))
        .route("/api/v1/vms/{id}/disks", post(attach_disk))
        .route("/api/v1/vms/{id}/disks/{lun}", delete(detach_disk))
        .route("/api/v1/vms/{id}/iso", post(mount_iso).delete(eject_iso_default))
        .route("/api/v1/vms/{id}/iso/{lun}", delete(eject_iso_lun))
        .route(
            "/api/v1/vms/{id}/nics/{nic_id}/ports",
            post(add_port_forward).delete(remove_port_forward),
        )
        .route("/api/v1/vms/{id}/logs", get(vm_logs))
        .route_layer(middleware::from_fn_with_state(state.clone(), require_auth));

    Router::new()
        .route("/api/v1/health", get(health))
        .merge(protected)
        .with_state(state)
}

async fn require_auth(
    State(state): State<AppState>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let header_token = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "));

    if header_token.is_some_and(|t| constant_time_eq(t.as_bytes(), state.token.as_bytes())) {
        Ok(next.run(request).await)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[derive(Deserialize)]
struct HealthQuery {
    challenge: Option<String>,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    service: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    auth_proof: Option<String>,
}

async fn health(
    State(state): State<AppState>,
    Query(query): Query<HealthQuery>,
) -> Json<HealthResponse> {
    let auth_proof = query
        .challenge
        .as_deref()
        .filter(|c| !c.is_empty())
        .map(|c| openvmms_core::models::compute_health_auth_proof(&state.token, state.bound_addr, c));
    Json(HealthResponse {
        status: "ok",
        service: "openvmmsd",
        auth_proof,
    })
}

async fn events_handler(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.supervisor.subscribe();
    let shutdown_rx = state.supervisor.subscribe_shutdown();
    let stream = futures_util::stream::unfold(
        (rx, shutdown_rx, false),
        |(mut rx, mut shutdown_rx, done)| async move {
            if done || *shutdown_rx.borrow() {
                return None;
            }
            loop {
                tokio::select! {
                    item = rx.recv() => {
                        match item {
                            Ok(event) => {
                                let is_stopping = matches!(event, DaemonEvent::DaemonStopping);
                                if let Ok(json) = serde_json::to_string(&event) {
                                    return Some((
                                        Ok::<Event, Infallible>(Event::default().data(json)),
                                        (rx, shutdown_rx, is_stopping),
                                    ));
                                }
                                if is_stopping {
                                    return None;
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                        }
                    }
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            while let Ok(event) = rx.try_recv() {
                                if matches!(event, DaemonEvent::DaemonStopping)
                                    && let Ok(json) = serde_json::to_string(&event)
                                {
                                    return Some((
                                        Ok::<Event, Infallible>(Event::default().data(json)),
                                        (rx, shutdown_rx, true),
                                    ));
                                }
                            }
                            return None;
                        }
                    }
                }
            }
        },
    );
    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    )
}

async fn host_info(State(state): State<AppState>) -> Result<Json<HostInfo>, ApiError> {
    tokio::task::spawn_blocking(move || state.supervisor.host_info())
        .await
        .map(Json)
        .map_err(ApiError::internal)
}

async fn get_host_config(State(state): State<AppState>) -> Json<HostConfig> {
    Json(state.supervisor.host_config())
}

async fn put_host_config(
    State(state): State<AppState>,
    Json(cfg): Json<HostConfig>,
) -> Result<impl IntoResponse, ApiError> {
    let updated = state
        .supervisor
        .update_host_config(cfg)
        .await
        .map_err(ApiError::map_anyhow)?;
    Ok(([(axum::http::header::CONNECTION, "close")], Json(updated)))
}

async fn list_switches(
    State(state): State<AppState>,
) -> Result<Json<Vec<VirtualSwitchInfo>>, ApiError> {
    tokio::task::spawn_blocking(move || state.supervisor.list_switches())
        .await
        .map(Json)
        .map_err(ApiError::internal)
}

#[derive(Deserialize, Default)]
struct DaemonShutdownQuery {
    #[serde(default)]
    stop_vms: bool,
}

async fn daemon_shutdown(
    State(state): State<AppState>,
    Query(query): Query<DaemonShutdownQuery>,
) -> StatusCode {
    state
        .supervisor
        .request_shutdown_with_options(query.stop_vms);
    StatusCode::ACCEPTED
}

async fn create_disk(
    State(state): State<AppState>,
    Json(req): Json<CreateDiskPayload>,
) -> Result<Json<DiskInfo>, ApiError> {
    let cancel_flag = Arc::new(AtomicBool::new(false));
    let _cancel_guard = CancelOnDrop(Arc::clone(&cancel_flag));
    tokio::task::spawn_blocking(move || state.supervisor.create_disk_cancellable(req, &cancel_flag))
        .await
        .map_err(ApiError::internal)?
        .map(Json)
        .map_err(ApiError::map_anyhow)
}

async fn inspect_disk(
    State(state): State<AppState>,
    Json(req): Json<InspectDiskPayload>,
) -> Result<Json<DiskInfo>, ApiError> {
    tokio::task::spawn_blocking(move || state.supervisor.inspect_disk(&req.path))
        .await
        .map_err(ApiError::internal)?
        .map(Json)
        .map_err(ApiError::map_anyhow)
}

async fn list_vms(State(state): State<AppState>) -> Json<Vec<VmSummary>> {
    Json(state.supervisor.list_vms())
}

async fn create_vm(
    State(state): State<AppState>,
    Json(req): Json<CreateVmPayload>,
) -> Result<(StatusCode, Json<VmSummary>), ApiError> {
    let cancel_flag = Arc::new(AtomicBool::new(false));
    let _cancel_guard = CancelOnDrop(Arc::clone(&cancel_flag));
    let summary =
        tokio::task::spawn_blocking(move || state.supervisor.create_vm_cancellable(req, &cancel_flag))
            .await
            .map_err(ApiError::internal)?
            .map_err(ApiError::map_anyhow)?;
    Ok((StatusCode::CREATED, Json(summary)))
}

async fn get_vm(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<VmSummary>, ApiError> {
    state
        .supervisor
        .get_vm_with_live_properties(&id)
        .await
        .map(Json)
        .map_err(ApiError::map_anyhow)
}

async fn update_vm(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<UpdateVmPayload>,
) -> Result<Json<VmSummary>, ApiError> {
    tokio::task::spawn_blocking(move || state.supervisor.update_vm(&id, req))
        .await
        .map_err(ApiError::internal)?
        .map(Json)
        .map_err(ApiError::map_anyhow)
}

#[derive(Deserialize, Default)]
struct DeleteVmQuery {
    #[serde(default)]
    delete_disks: bool,
}

async fn delete_vm(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(query): Query<DeleteVmQuery>,
) -> Result<StatusCode, ApiError> {
    tokio::task::spawn_blocking(move || state.supervisor.delete_vm(&id, query.delete_disks))
        .await
        .map_err(ApiError::internal)?
        .map(|()| StatusCode::NO_CONTENT)
        .map_err(ApiError::map_anyhow)
}

async fn start_vm(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<VmSummary>, ApiError> {
    state
        .supervisor
        .start_vm(&id)
        .await
        .map(Json)
        .map_err(ApiError::map_anyhow)
}

async fn stop_vm(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<VmSummary>, ApiError> {
    state
        .supervisor
        .stop_vm(&id)
        .await
        .map(Json)
        .map_err(ApiError::map_anyhow)
}

async fn shutdown_vm(
    State(state): State<AppState>,
    Path(id): Path<String>,
    payload: Option<Json<ShutdownVmPayload>>,
) -> Result<Json<VmSummary>, ApiError> {
    let req = payload.map(|p| p.0).unwrap_or_default();
    state
        .supervisor
        .shutdown_vm(&id, req)
        .await
        .map(Json)
        .map_err(ApiError::map_anyhow)
}

async fn pause_vm(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<VmSummary>, ApiError> {
    state
        .supervisor
        .pause_vm(&id)
        .await
        .map(Json)
        .map_err(ApiError::map_anyhow)
}

async fn resume_vm(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<VmSummary>, ApiError> {
    state
        .supervisor
        .resume_vm(&id)
        .await
        .map(Json)
        .map_err(ApiError::map_anyhow)
}

async fn reset_vm(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<VmSummary>, ApiError> {
    state
        .supervisor
        .reset_vm(&id)
        .await
        .map(Json)
        .map_err(ApiError::map_anyhow)
}

async fn attach_disk(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<AttachDiskPayload>,
) -> Result<Json<VmSummary>, ApiError> {
    state
        .supervisor
        .attach_disk(&id, req)
        .await
        .map(Json)
        .map_err(ApiError::map_anyhow)
}

async fn detach_disk(
    State(state): State<AppState>,
    Path((id, lun)): Path<(String, u32)>,
) -> Result<Json<VmSummary>, ApiError> {
    state
        .supervisor
        .detach_disk(&id, lun)
        .await
        .map(Json)
        .map_err(ApiError::map_anyhow)
}

async fn mount_iso(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(req): Json<MountIsoPayload>,
) -> Result<Json<VmSummary>, ApiError> {
    state
        .supervisor
        .mount_iso(&id, req)
        .await
        .map(Json)
        .map_err(ApiError::map_anyhow)
}

async fn eject_iso_default(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<VmSummary>, ApiError> {
    state
        .supervisor
        .eject_iso(&id, None)
        .await
        .map(Json)
        .map_err(ApiError::map_anyhow)
}

async fn eject_iso_lun(
    State(state): State<AppState>,
    Path((id, lun)): Path<(String, u32)>,
) -> Result<Json<VmSummary>, ApiError> {
    state
        .supervisor
        .eject_iso(&id, Some(lun))
        .await
        .map(Json)
        .map_err(ApiError::map_anyhow)
}

async fn add_port_forward(
    State(state): State<AppState>,
    Path((id, nic_id)): Path<(String, Uuid)>,
    Json(req): Json<PortForwardPayload>,
) -> Result<Json<VmSummary>, ApiError> {
    state
        .supervisor
        .add_port_forward(&id, nic_id, req.port)
        .await
        .map(Json)
        .map_err(ApiError::map_anyhow)
}

async fn remove_port_forward(
    State(state): State<AppState>,
    Path((id, nic_id)): Path<(String, Uuid)>,
    Json(req): Json<PortForwardPayload>,
) -> Result<Json<VmSummary>, ApiError> {
    state
        .supervisor
        .remove_port_forward(&id, nic_id, req.port)
        .await
        .map(Json)
        .map_err(ApiError::map_anyhow)
}

#[derive(Deserialize)]
struct LogQuery {
    #[serde(default = "default_tail")]
    tail: usize,
}

fn default_tail() -> usize {
    200
}

#[derive(Serialize)]
struct LogResponse {
    content: String,
}

async fn vm_logs(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<LogQuery>,
) -> Result<Json<LogResponse>, ApiError> {
    let content = tokio::task::spawn_blocking(move || state.supervisor.vm_logs(&id, q.tail))
        .await
        .map_err(ApiError::internal)?
        .map_err(ApiError::map_anyhow)?;
    Ok(Json(LogResponse { content }))
}

#[cfg(test)]
mod tests {
    use crate::TestDaemonHandle;
    use openvmms_core::models::CreateDiskPayload;
    use openvmms_core::models::CreateVmPayload;
    use openvmms_core::models::DiskFormat;
    use openvmms_core::models::DiskKind;
    use openvmms_core::models::IpProto;
    use openvmms_core::models::MountIsoPayload;
    use openvmms_core::models::NicBackendConfig;
    use openvmms_core::models::PortForward;
    use openvmms_core::models::PortForwardPayload;
    use openvmms_core::models::UpdateVmPayload;
    use openvmms_core::models::VmPreset;
    use openvmms_core::models::VmStatus;
    use openvmms_core::models::VmSummary;
    use std::fs;

    fn test_http_client() -> reqwest::Client {
        reqwest::Client::builder().no_proxy().build().unwrap()
    }

    #[tokio::test]
    async fn daemon_rest_api_crud_disk_iso_port_forward_and_auth() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = TestDaemonHandle::spawn(dir.path().to_path_buf())
            .await
            .unwrap();

        let client = test_http_client();

        // Health is unauthenticated.
        let health = client
            .get(format!("{}/api/v1/health", daemon.base_url))
            .send()
            .await
            .unwrap();
        assert_eq!(health.status(), 200);

        // Protected route rejects missing token.
        let unauth = client
            .get(format!("{}/api/v1/vms", daemon.base_url))
            .send()
            .await
            .unwrap();
        assert_eq!(unauth.status(), 401);

        // Create a raw disk.
        let disk_path = dir.path().join("win11.img");
        let disk_resp = client
            .post(format!("{}/api/v1/disks/create", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&CreateDiskPayload {
                path: disk_path.clone(),
                size_bytes: 32 * 1024 * 1024,
                format: DiskFormat::Raw,
                fixed: false,
            })
            .send()
            .await
            .unwrap();
        assert_eq!(disk_resp.status(), 200);

        // Create a dummy ISO file.
        let iso_path = dir.path().join("installer.iso");
        fs::write(&iso_path, b"ISO9660_TEST").unwrap();

        // Create VM.
        let create_resp = client
            .post(format!("{}/api/v1/vms", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&CreateVmPayload {
                name: "Dev-Win11".into(),
                description: "Test VM".into(),
                preset: VmPreset::Windows11,
                vcpus: Some(4),
                memory_mb: Some(8192),
                nested_virt: None,
                uefi_firmware: None,
                secure_boot_enabled: None,
                secure_boot_template: None,
                tpm_enabled: None,
                fastdesk: None,
                disks: vec![],
                create_disk: Some(CreateDiskPayload {
                    path: dir.path().join("os.img"),
                    size_bytes: 16 * 1024 * 1024,
                    format: DiskFormat::Raw,
                    fixed: false,
                }),
                iso_path: Some(iso_path.clone()),
                nics: None,
            })
            .send()
            .await
            .unwrap();
        assert_eq!(create_resp.status(), 201);
        let vm: VmSummary = create_resp.json().await.unwrap();
        assert_eq!(vm.runtime.status, VmStatus::Off);
        assert_eq!(vm.config.disks.len(), 2);
        assert_eq!(vm.config.disks[1].kind, DiskKind::Dvd);

        // Update VM.
        let update_resp = client
            .put(format!("{}/api/v1/vms/Dev-Win11", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&UpdateVmPayload {
                vcpus: Some(6),
                memory_mb: Some(12288),
                ..Default::default()
            })
            .send()
            .await
            .unwrap();
        assert_eq!(update_resp.status(), 200);
        let vm: VmSummary = update_resp.json().await.unwrap();
        assert_eq!(vm.config.vcpus, 6);
        assert_eq!(vm.config.memory_mb, 12288);

        // Eject and re-mount ISO.
        let eject_resp = client
            .delete(format!("{}/api/v1/vms/Dev-Win11/iso", daemon.base_url))
            .bearer_auth(&daemon.token)
            .send()
            .await
            .unwrap();
        assert_eq!(eject_resp.status(), 200);
        let vm: VmSummary = eject_resp.json().await.unwrap();
        assert_eq!(vm.config.disks.len(), 1);

        let mount_resp = client
            .post(format!("{}/api/v1/vms/Dev-Win11/iso", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&MountIsoPayload {
                path: iso_path,
                lun: None,
            })
            .send()
            .await
            .unwrap();
        assert_eq!(mount_resp.status(), 200);
        let vm: VmSummary = mount_resp.json().await.unwrap();
        assert_eq!(vm.config.disks.len(), 2);

        // Add & remove port forward on NIC 0.
        let nic_id = vm.config.nics[0].id;
        let pf = PortForward {
            protocol: IpProto::Tcp,
            host_addr: "127.0.0.1".into(),
            host_port: 3389,
            guest_port: 3389,
        };
        let pf_add = client
            .post(format!(
                "{}/api/v1/vms/Dev-Win11/nics/{nic_id}/ports",
                daemon.base_url
            ))
            .bearer_auth(&daemon.token)
            .json(&PortForwardPayload { port: pf.clone() })
            .send()
            .await
            .unwrap();
        assert_eq!(pf_add.status(), 200);
        let vm: VmSummary = pf_add.json().await.unwrap();
        match &vm.config.nics[0].backend {
            NicBackendConfig::Consomme { port_forwards, .. } => assert_eq!(port_forwards.len(), 1),
            other => panic!("unexpected nic backend: {other:?}"),
        }

        let pf_del = client
            .delete(format!(
                "{}/api/v1/vms/Dev-Win11/nics/{nic_id}/ports",
                daemon.base_url
            ))
            .bearer_auth(&daemon.token)
            .json(&PortForwardPayload { port: pf })
            .send()
            .await
            .unwrap();
        assert_eq!(pf_del.status(), 200);

        // Delete VM.
        let del = client
            .delete(format!(
                "{}/api/v1/vms/Dev-Win11?delete_disks=true",
                daemon.base_url
            ))
            .bearer_auth(&daemon.token)
            .send()
            .await
            .unwrap();
        assert_eq!(del.status(), 204);

        daemon.shutdown().await;
    }

    #[tokio::test]
    async fn real_openvmm_ttrpc_handshake_when_binary_present() {
        let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let repo_root = manifest_dir.join("../../..");
        let bin_name = if cfg!(windows) { "openvmm.exe" } else { "openvmm" };
        let openvmm_bin = [
            repo_root.join("openvmm/target/debug").join(bin_name),
            repo_root.join("openvmm/target/release").join(bin_name),
        ]
        .into_iter()
        .find(|p| p.is_file());

        let Some(openvmm_bin) = openvmm_bin else {
            if std::env::var_os("OPENVMM_REQUIRE_REAL_BINARY").is_some() {
                panic!(
                    "OPENVMM_REQUIRE_REAL_BINARY is set, but real openvmm binary was not found under {}",
                    repo_root.display()
                );
            }
            eprintln!("skipping real_openvmm_ttrpc_handshake: openvmm binary not built yet");
            return;
        };

        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("openvmm.sock");
        let mut child = std::process::Command::new(&openvmm_bin)
            .arg("--rpc")
            .arg(format!("path={},transport=ttrpc", sock.display()))
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();

        let client = openvmms_core::ttrpc::TtrpcClient::new(&sock);
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if sock.exists() && client.ping().await.is_ok() {
                break;
            }
            if let Some(status) = child.try_wait().unwrap() {
                panic!("openvmm --rpc exited prematurely with {status}");
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for openvmm --rpc socket"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        let props = client
            .properties_vm(openvmms_core::vmservice::PropertiesVmRequest { types: vec![] })
            .await
            .unwrap();
        assert_eq!(
            props.state,
            openvmms_core::vmservice::VmState::Uninitialized as i32
        );
        assert!(props.fastdesk_listen_addr.is_none());

        let caps = client.capabilities_vm().await.unwrap();
        assert!(!caps.supported_resources.is_empty());

        let _ = client.quit().await;
        let status = child.wait().unwrap();
        assert!(status.success(), "openvmm --rpc exited with {status}");
    }

    #[tokio::test]
    async fn real_openvmm_wait_vm_cancels_on_disconnect_and_reattaches() {
        use openvmms_core::ttrpc::{TtrpcClient, WaitVmOutcome};
        use openvmms_core::vmservice;

        let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let repo_root = manifest_dir.join("../../..");
        let bin_name = if cfg!(windows) { "openvmm.exe" } else { "openvmm" };
        let openvmm_bin = [
            repo_root.join("openvmm/target/release").join(bin_name),
            repo_root.join("openvmm/target/debug").join(bin_name),
        ]
        .into_iter()
        .find(|p| p.is_file());
        let msvm_fd = repo_root.join(
            "openvmm/.packages/hyperv.uefi.mscoreuefi.x64.RELEASE/MsvmX64/RELEASE_VS2022/FV/MSVM.fd",
        );
        #[cfg(target_os = "linux")]
        let kvm_ready = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/kvm")
            .is_ok();
        #[cfg(not(target_os = "linux"))]
        let kvm_ready = cfg!(windows);

        let missing_reason = if openvmm_bin.is_none() {
            Some(format!(
                "real openvmm binary not found under {}",
                repo_root.display()
            ))
        } else if !msvm_fd.is_file() {
            Some(format!("UEFI firmware not found at {}", msvm_fd.display()))
        } else if !kvm_ready {
            Some("/dev/kvm is not accessible read/write".into())
        } else {
            None
        };
        if let Some(reason) = missing_reason {
            if std::env::var_os("OPENVMM_REQUIRE_REAL_KVM_TEST").is_some() {
                panic!(
                    "OPENVMM_REQUIRE_REAL_KVM_TEST is set, cannot run real WaitVM test: {reason}"
                );
            }
            eprintln!("skipping real_openvmm_wait_vm_cancels_on_disconnect_and_reattaches: {reason}");
            return;
        }
        let openvmm_bin = openvmm_bin.unwrap();

        struct ChildKillGuard(Option<std::process::Child>);
        impl Drop for ChildKillGuard {
            fn drop(&mut self) {
                if let Some(mut c) = self.0.take() {
                    let _ = c.kill();
                    let _ = c.wait();
                }
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("openvmm-wait.sock");
        let disk_path = dir.path().join("boot.img");
        fs::write(&disk_path, vec![0u8; 1024 * 1024]).unwrap();
        let vmgs_path = dir.path().join("state.vmgs");

        let child = std::process::Command::new(&openvmm_bin)
            .arg("--rpc")
            .arg(format!("path={},transport=ttrpc", sock.display()))
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let mut child_guard = ChildKillGuard(Some(child));

        let client = TtrpcClient::new(&sock);
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if sock.exists() && client.ping().await.is_ok() {
                break;
            }
            if let Some(status) = child_guard.0.as_mut().unwrap().try_wait().unwrap() {
                panic!("openvmm --rpc exited prematurely with {status}");
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for openvmm --rpc socket"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        let mut cfg = openvmms_core::models::VmConfig::from_preset(
            "RealWaitVm",
            VmPreset::Custom,
            dir.path(),
        );
        cfg.vcpus = 1;
        cfg.memory_mb = 512;
        cfg.tpm_enabled = false;
        cfg.fastdesk.enabled = false;
        cfg.nics.clear();
        cfg.vmgs_path = Some(vmgs_path);
        cfg.disks = vec![openvmms_core::models::DiskAttachment {
            lun: 0,
            kind: DiskKind::HardDisk,
            path: disk_path,
            read_only: false,
            format: DiskFormat::Raw,
        }];
        let create_req =
            crate::supervisor::build_create_vm_request(&cfg, &msvm_fd, None).unwrap();
        if let Err(err) = client.create_vm(create_req).await {
            let msg = format!("{err:#}");
            let lower = msg.to_ascii_lowercase();
            let hypervisor_missing = cfg!(windows)
                && (lower.contains("hypervisor")
                    || lower.contains("whp")
                    || lower.contains("partition")
                    || lower.contains("virtualization")
                    || lower.contains("not present")
                    || lower.contains("not enabled"));
            if hypervisor_missing && std::env::var_os("OPENVMM_REQUIRE_REAL_KVM_TEST").is_none() {
                eprintln!(
                    "skipping rest of real_openvmm_wait_vm_cancels_on_disconnect_and_reattaches after spawning openvmm.exe --rpc (WHP unavailable): {msg}"
                );
                let _ = client.quit().await;
                let status = child_guard.0.take().unwrap().wait().unwrap();
                assert!(status.success(), "openvmm --rpc exited with {status}");
                return;
            }
            panic!("CreateVM RPC failed: {msg}");
        }
        client.resume_vm().await.unwrap();

        let props = client
            .properties_vm(vmservice::PropertiesVmRequest { types: vec![] })
            .await
            .unwrap();
        assert_eq!(props.state, vmservice::VmState::Running as i32);

        // 1. Task 1 holds the single WaitVM slot on the real openvmm process.
        let (cancel_tx1, cancel_rx1) = tokio::sync::watch::channel(false);
        let task1 = tokio::spawn({
            let c = client.clone();
            async move { c.wait_vm_cancellable(cancel_rx1).await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        assert!(!task1.is_finished(), "Task 1 exited before probe1");

        let (probe1_cancel_tx, probe1_cancel_rx) = tokio::sync::watch::channel(false);
        let probe1_err = tokio::select! {
            r = client.wait_vm_cancellable(probe1_cancel_rx) => r.unwrap_err(),
            _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {
                let _ = probe1_cancel_tx.send(true);
                panic!("probe1 did not fail with 'wait VM already in flight'");
            }
        };
        assert!(
            format!("{probe1_err:#}").contains("wait VM already in flight"),
            "unexpected probe1 error: {probe1_err:#}"
        );

        // 2. Cancelling Task 1 shuts down its socket, cancelling WaitVM in real openvmm.
        let _ = cancel_tx1.send(true);
        let outcome1 = tokio::time::timeout(std::time::Duration::from_secs(2), task1)
            .await
            .expect("Task 1 did not unblock on cancel")
            .unwrap()
            .unwrap();
        assert_eq!(outcome1, WaitVmOutcome::Cancelled);

        // 3. Re-attaching Task 2 succeeds directly without hitting "wait VM already in flight".
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        let (_cancel_tx2, cancel_rx2) = tokio::sync::watch::channel(false);
        let task2 = tokio::spawn({
            let c = client.clone();
            async move { c.wait_vm_cancellable(cancel_rx2).await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        assert!(
            !task2.is_finished(),
            "Task 2 exited prematurely instead of holding WaitVM"
        );

        let (probe2_cancel_tx, probe2_cancel_rx) = tokio::sync::watch::channel(false);
        let probe2_err = tokio::select! {
            r = client.wait_vm_cancellable(probe2_cancel_rx) => r.unwrap_err(),
            _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {
                let _ = probe2_cancel_tx.send(true);
                panic!("probe2 did not fail with 'wait VM already in flight'");
            }
        };
        assert!(
            format!("{probe2_err:#}").contains("wait VM already in flight"),
            "unexpected probe2 error: {probe2_err:#}"
        );

        // 4. Tearing down the VM resolves Task 2's WaitVM cleanly.
        client.teardown_vm().await.unwrap();
        let wait2_err = tokio::time::timeout(std::time::Duration::from_secs(5), task2)
            .await
            .expect("Task 2 did not finish after teardown_vm")
            .unwrap()
            .unwrap_err();
        let wait2_msg = format!("{wait2_err:#}");
        assert!(
            wait2_msg.contains("VM torn down"),
            "expected 'VM torn down' from Task 2, got: {wait2_msg}"
        );
        assert!(
            !wait2_msg.contains("wait VM already in flight"),
            "Task 2 failed with stale WaitVM conflict: {wait2_msg}"
        );

        let _ = client.quit().await;
        let status = child_guard.0.take().unwrap().wait().unwrap();
        assert!(status.success(), "openvmm --rpc exited with {status}");
    }

    #[tokio::test]
    async fn delete_vm_without_delete_disks_preserves_default_and_vm_dir_disks() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = TestDaemonHandle::spawn(dir.path().to_path_buf())
            .await
            .unwrap();
        let client = test_http_client();

        // Create VM with a default created disk (empty path -> <data_dir>/disks/...).
        let create_resp = client
            .post(format!("{}/api/v1/vms", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&CreateVmPayload {
                name: "Preserve-Disks-VM".into(),
                description: String::new(),
                preset: VmPreset::Custom,
                vcpus: Some(2),
                memory_mb: Some(1024),
                nested_virt: None,
                uefi_firmware: None,
                secure_boot_enabled: None,
                secure_boot_template: None,
                tpm_enabled: None,
                fastdesk: None,
                disks: vec![],
                create_disk: Some(CreateDiskPayload {
                    path: std::path::PathBuf::new(),
                    size_bytes: 4 * 1024 * 1024,
                    format: DiskFormat::Raw,
                    fixed: false,
                }),
                iso_path: None,
                nics: None,
            })
            .send()
            .await
            .unwrap();
        assert_eq!(create_resp.status(), 201);
        let vm: VmSummary = create_resp.json().await.unwrap();
        let default_disk = vm.config.disks[0].path.clone();
        assert!(default_disk.exists());
        assert!(default_disk.starts_with(dir.path().join("disks")));

        // Also place a disk explicitly inside vms/<id>/ and attach it.
        let inside_vm_dir_disk = dir
            .path()
            .join("vms")
            .join(vm.config.id.to_string())
            .join("extra.img");
        fs::write(&inside_vm_dir_disk, b"EXTRA_DISK").unwrap();
        let attach_resp = client
            .post(format!(
                "{}/api/v1/vms/{}/disks",
                daemon.base_url, vm.config.id
            ))
            .bearer_auth(&daemon.token)
            .json(&openvmms_core::models::AttachDiskPayload {
                path: inside_vm_dir_disk.clone(),
                lun: Some(1),
                read_only: false,
                format: Some(DiskFormat::Raw),
            })
            .send()
            .await
            .unwrap();
        assert_eq!(attach_resp.status(), 200);

        // Delete WITHOUT delete_disks -> both disks must remain intact.
        let del = client
            .delete(format!("{}/api/v1/vms/{}", daemon.base_url, vm.config.id))
            .bearer_auth(&daemon.token)
            .send()
            .await
            .unwrap();
        assert_eq!(del.status(), 204);
        assert!(
            default_disk.exists(),
            "default disk {} should be preserved when delete_disks=false",
            default_disk.display()
        );
        assert!(
            inside_vm_dir_disk.exists(),
            "disk inside vm dir {} should be preserved when delete_disks=false",
            inside_vm_dir_disk.display()
        );

        daemon.shutdown().await;
    }

    #[tokio::test]
    async fn mount_iso_rejects_non_dvd_lun_without_detaching_os_disk() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = TestDaemonHandle::spawn(dir.path().to_path_buf())
            .await
            .unwrap();
        let client = test_http_client();

        let os_disk = dir.path().join("os.img");
        fs::write(&os_disk, b"OS_DISK").unwrap();
        let iso_path = dir.path().join("install.iso");
        fs::write(&iso_path, b"ISO_DATA").unwrap();

        let create_resp = client
            .post(format!("{}/api/v1/vms", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&CreateVmPayload {
                name: "Lun-Guard-VM".into(),
                description: String::new(),
                preset: VmPreset::Custom,
                vcpus: Some(2),
                memory_mb: Some(1024),
                nested_virt: None,
                uefi_firmware: None,
                secure_boot_enabled: None,
                secure_boot_template: None,
                tpm_enabled: None,
                fastdesk: None,
                disks: vec![openvmms_core::models::DiskAttachment {
                    lun: 0,
                    kind: DiskKind::HardDisk,
                    path: os_disk.clone(),
                    read_only: false,
                    format: DiskFormat::Raw,
                }],
                create_disk: None,
                iso_path: None,
                nics: None,
            })
            .send()
            .await
            .unwrap();
        assert_eq!(create_resp.status(), 201);

        // Attempt to mount ISO onto LUN 0 (occupied by HardDisk) -> must fail!
        let bad_mount = client
            .post(format!("{}/api/v1/vms/Lun-Guard-VM/iso", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&MountIsoPayload {
                path: iso_path.clone(),
                lun: Some(0),
            })
            .send()
            .await
            .unwrap();
        assert_eq!(bad_mount.status(), 409);

        // Verify LUN 0 is still the OS HardDisk.
        let vm: VmSummary = client
            .get(format!("{}/api/v1/vms/Lun-Guard-VM", daemon.base_url))
            .bearer_auth(&daemon.token)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(vm.config.disks.len(), 1);
        assert_eq!(vm.config.disks[0].lun, 0);
        assert_eq!(vm.config.disks[0].kind, DiskKind::HardDisk);
        assert_eq!(vm.config.disks[0].path, os_disk);

        // Mounting ISO with lun: None succeeds onto free LUN 1.
        let ok_mount = client
            .post(format!("{}/api/v1/vms/Lun-Guard-VM/iso", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&MountIsoPayload {
                path: iso_path,
                lun: None,
            })
            .send()
            .await
            .unwrap();
        assert_eq!(ok_mount.status(), 200);
        let vm: VmSummary = ok_mount.json().await.unwrap();
        assert_eq!(vm.config.disks.len(), 2);
        assert_eq!(vm.config.disks[1].lun, 1);
        assert_eq!(vm.config.disks[1].kind, DiskKind::Dvd);

        daemon.shutdown().await;
    }

    #[tokio::test]
    async fn sse_events_stream_closes_promptly_on_daemon_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = TestDaemonHandle::spawn(dir.path().to_path_buf())
            .await
            .unwrap();
        let client = test_http_client();

        // Open a persistent SSE stream using bearer authentication.
        let mut sse_resp = client
            .get(format!("{}/api/v1/events", daemon.base_url))
            .bearer_auth(&daemon.token)
            .send()
            .await
            .unwrap();
        assert_eq!(sse_resp.status(), 200);

        // Request daemon shutdown via API while the SSE stream is open.
        let shutdown_resp = client
            .post(format!("{}/api/v1/daemon/shutdown", daemon.base_url))
            .bearer_auth(&daemon.token)
            .send()
            .await
            .unwrap();
        assert_eq!(shutdown_resp.status(), 202);

        // Both the SSE stream and the server task must exit within 2 seconds.
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            let mut saw_stopping = false;
            while let Some(chunk) = sse_resp.chunk().await.unwrap() {
                let text = String::from_utf8_lossy(&chunk);
                if text.contains("daemon_stopping") {
                    saw_stopping = true;
                }
            }
            assert!(saw_stopping, "expected daemon_stopping SSE event before EOF");
            daemon.shutdown().await;
        })
        .await
        .expect("SSE stream and daemon did not shut down within 2s");
    }

    #[tokio::test]
    async fn updating_host_config_listen_addr_serves_new_port_on_same_daemon() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = TestDaemonHandle::spawn(dir.path().to_path_buf())
            .await
            .unwrap();
        let client = test_http_client();

        // Create a VM on the original port so we can confirm state continuity on the new port.
        let create_resp = client
            .post(format!("{}/api/v1/vms", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&CreateVmPayload {
                name: "PortSwitch-VM".into(),
                description: String::new(),
                preset: VmPreset::Custom,
                vcpus: Some(2),
                memory_mb: Some(1024),
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
            .send()
            .await
            .unwrap();
        assert_eq!(create_resp.status(), 201);

        let mut cfg: openvmms_core::models::HostConfig = client
            .get(format!("{}/api/v1/host/config", daemon.base_url))
            .bearer_auth(&daemon.token)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        // Invalid listen address must fail without overwriting config.
        let mut bad_cfg = cfg.clone();
        bad_cfg.listen_addr = "not-a-socket-addr".into();
        let bad_resp = client
            .put(format!("{}/api/v1/host/config", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&bad_cfg)
            .send()
            .await
            .unwrap();
        assert_eq!(bad_resp.status(), 400);

        // Bind a fresh ephemeral port on the same daemon.
        cfg.listen_addr = "127.0.0.1:0".into();
        let put_resp = client
            .put(format!("{}/api/v1/host/config", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&cfg)
            .send()
            .await
            .unwrap();
        assert_eq!(put_resp.status(), 200);
        let updated: openvmms_core::models::HostConfig = put_resp.json().await.unwrap();
        let new_url = format!("http://{}", updated.listen_addr);
        assert_ne!(new_url, daemon.base_url);

        // ApiClient::from_data_dir resolves the persisted new listen_addr and talks to the SAME daemon.
        let data_dir = dir.path().to_path_buf();
        let (health_ok, vm_names): (bool, Vec<String>) = tokio::task::spawn_blocking(move || {
            let api = openvmms_core::api_client::ApiClient::from_data_dir(Some(&data_dir), None)
                .unwrap();
            assert_eq!(api.base_url(), new_url);
            api.health().unwrap();
            let vms = api.list_vms().unwrap();
            (
                true,
                vms.into_iter().map(|v| v.config.name).collect(),
            )
        })
        .await
        .unwrap();
        assert!(health_ok);
        assert_eq!(vm_names, vec!["PortSwitch-VM".to_string()]);

        // Previous listener must stop accepting new connections once the port changes.
        let old_addr: std::net::SocketAddr = daemon
            .base_url
            .trim_start_matches("http://")
            .parse()
            .unwrap();
        let mut old_closed = false;
        for _ in 0..40 {
            if tokio::net::TcpStream::connect(old_addr).await.is_err() {
                old_closed = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        assert!(
            old_closed,
            "old HTTP listener on {old_addr} kept accepting connections after port change"
        );

        daemon.shutdown().await;
    }

    #[tokio::test]
    async fn attach_disk_while_vm_starting_is_rejected_with_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = TestDaemonHandle::spawn(dir.path().to_path_buf())
            .await
            .unwrap();
        let client = test_http_client();

        let create_resp = client
            .post(format!("{}/api/v1/vms", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&CreateVmPayload {
                name: "Starting-VM".into(),
                description: String::new(),
                preset: VmPreset::Custom,
                vcpus: Some(2),
                memory_mb: Some(1024),
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
            .send()
            .await
            .unwrap();
        assert_eq!(create_resp.status(), 201);
        let created: VmSummary = create_resp.json().await.unwrap();

        daemon
            .supervisor
            .set_vm_status_for_test(created.config.id, openvmms_core::models::VmStatus::Starting);

        let disk_path = dir.path().join("extra.img");
        fs::write(&disk_path, b"DISK").unwrap();
        let attach_resp = client
            .post(format!("{}/api/v1/vms/Starting-VM/disks", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&openvmms_core::models::AttachDiskPayload {
                path: disk_path,
                lun: Some(0),
                read_only: false,
                format: Some(DiskFormat::Raw),
            })
            .send()
            .await
            .unwrap();
        assert_eq!(attach_resp.status(), 409);

        let iso_path = dir.path().join("boot.iso");
        fs::write(&iso_path, b"ISO").unwrap();
        let iso_resp = client
            .post(format!("{}/api/v1/vms/Starting-VM/iso", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&MountIsoPayload {
                path: iso_path,
                lun: None,
            })
            .send()
            .await
            .unwrap();
        assert_eq!(iso_resp.status(), 409);

        let vm: VmSummary = client
            .get(format!("{}/api/v1/vms/Starting-VM", daemon.base_url))
            .bearer_auth(&daemon.token)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(vm.config.disks.is_empty());

        daemon
            .supervisor
            .set_vm_status_for_test(created.config.id, openvmms_core::models::VmStatus::Off);
        daemon.shutdown().await;
    }

    #[tokio::test]
    async fn delayed_pause_resume_result_does_not_clobber_stop_or_restart() {
        use openvmms_core::models::VmStatus;
        let dir = tempfile::tempdir().unwrap();
        let daemon = TestDaemonHandle::spawn(dir.path().to_path_buf())
            .await
            .unwrap();
        let created = daemon
            .supervisor
            .create_vm(CreateVmPayload {
                name: "Race-VM".into(),
                description: String::new(),
                preset: VmPreset::Custom,
                vcpus: Some(2),
                memory_mb: Some(1024),
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
        let id = created.config.id;
        let sup = &daemon.supervisor;
        sup.set_vm_status_for_test(id, VmStatus::Running);

        // Pause RPC issued while Running, then a stop begins before the reply lands.
        let rpc_gen = sup.generation_for_test(id);
        sup.advance_lifecycle_for_test(id, VmStatus::Stopping);
        assert!(sup.commit_power_transition(id, rpc_gen, "pause", VmStatus::Paused).is_err());
        assert_eq!(sup.get_vm("Race-VM").unwrap().runtime.status, VmStatus::Stopping);
        assert!(sup.commit_power_transition(id, rpc_gen, "resume", VmStatus::Running).is_err());
        assert_eq!(sup.get_vm("Race-VM").unwrap().runtime.status, VmStatus::Stopping);

        // Replacement VM is now Running; the stale reply must not pause it either.
        sup.advance_lifecycle_for_test(id, VmStatus::Running);
        assert!(sup.commit_power_transition(id, rpc_gen, "pause", VmStatus::Paused).is_err());
        assert_eq!(sup.get_vm("Race-VM").unwrap().runtime.status, VmStatus::Running);

        // A reply from the current lifecycle generation still applies.
        let cur = sup.generation_for_test(id);
        sup.commit_power_transition(id, cur, "pause", VmStatus::Paused).unwrap();
        assert_eq!(sup.get_vm("Race-VM").unwrap().runtime.status, VmStatus::Paused);

        sup.set_vm_status_for_test(id, VmStatus::Off);
        daemon.shutdown().await;
    }

    #[tokio::test]
    async fn update_vm_clears_firmware_on_null_and_preserves_when_omitted() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = TestDaemonHandle::spawn(dir.path().to_path_buf())
            .await
            .unwrap();
        let client = test_http_client();

        let fw_path = dir.path().join("custom-msvm.fd");
        let create_resp = client
            .post(format!("{}/api/v1/vms", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&CreateVmPayload {
                name: "Firmware-VM".into(),
                description: String::new(),
                preset: VmPreset::Custom,
                vcpus: Some(2),
                memory_mb: Some(1024),
                nested_virt: None,
                uefi_firmware: Some(fw_path.clone()),
                secure_boot_enabled: None,
                secure_boot_template: None,
                tpm_enabled: None,
                fastdesk: None,
                disks: vec![],
                create_disk: None,
                iso_path: None,
                nics: None,
            })
            .send()
            .await
            .unwrap();
        assert_eq!(create_resp.status(), 201);
        let created: VmSummary = create_resp.json().await.unwrap();
        assert_eq!(created.config.uefi_firmware.as_ref(), Some(&fw_path));

        // Omitting uefi_firmware must preserve the existing override.
        let keep_resp = client
            .put(format!("{}/api/v1/vms/Firmware-VM", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&serde_json::json!({ "description": "updated" }))
            .send()
            .await
            .unwrap();
        assert_eq!(keep_resp.status(), 200);
        let kept: VmSummary = keep_resp.json().await.unwrap();
        assert_eq!(kept.config.description, "updated");
        assert_eq!(kept.config.uefi_firmware.as_ref(), Some(&fw_path));

        // Explicit JSON null must clear the override back to None.
        let clear_resp = client
            .put(format!("{}/api/v1/vms/Firmware-VM", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&serde_json::json!({ "uefi_firmware": null }))
            .send()
            .await
            .unwrap();
        assert_eq!(clear_resp.status(), 200);
        let cleared: VmSummary = clear_resp.json().await.unwrap();
        assert_eq!(cleared.config.uefi_firmware, None);

        daemon.shutdown().await;
    }

    #[tokio::test]
    async fn failed_create_vm_does_not_leave_created_disk_behind_and_retry_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = TestDaemonHandle::spawn(dir.path().to_path_buf())
            .await
            .unwrap();
        let client = test_http_client();

        let disk_path = dir.path().join("retry-os.img");
        for bad_vcpus in [0, 512] {
            let bad_resp = client
                .post(format!("{}/api/v1/vms", daemon.base_url))
                .bearer_auth(&daemon.token)
                .json(&CreateVmPayload {
                    name: "Validate-First-VM".into(),
                    description: String::new(),
                    preset: VmPreset::Custom,
                    vcpus: Some(bad_vcpus),
                    memory_mb: Some(1024),
                    nested_virt: None,
                    uefi_firmware: None,
                    secure_boot_enabled: None,
                    secure_boot_template: None,
                    tpm_enabled: None,
                    fastdesk: None,
                    disks: vec![],
                    create_disk: Some(CreateDiskPayload {
                        path: disk_path.clone(),
                        size_bytes: 16 * 1024 * 1024,
                        format: DiskFormat::Raw,
                        fixed: false,
                    }),
                    iso_path: None,
                    nics: None,
                })
                .send()
                .await
                .unwrap();
            assert!(
                bad_resp.status().is_client_error(),
                "expected 4xx error for bad vcpus={bad_vcpus}, got {}",
                bad_resp.status()
            );
            let err_body = bad_resp.text().await.unwrap();
            assert!(
                err_body.contains("vcpus must be between 1 and 256"),
                "unexpected error body: {err_body}"
            );
            assert!(
                !disk_path.exists(),
                "failed create_vm with vcpus={bad_vcpus} left disk {} behind",
                disk_path.display()
            );
        }

        // Exercise post-disk-creation rollback: `create_disk_cancellable` succeeds
        // and creates `disk_path`, then `save_vm` fails because `vms/` is blocked
        // by a regular file; the newly created disk must be removed cleanly.
        let vms_dir = dir.path().join("vms");
        fs::remove_dir(&vms_dir).unwrap();
        fs::write(&vms_dir, b"blocker").unwrap();
        let rollback_resp = client
            .post(format!("{}/api/v1/vms", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&CreateVmPayload {
                name: "Validate-First-VM".into(),
                description: String::new(),
                preset: VmPreset::Custom,
                vcpus: Some(4),
                memory_mb: Some(1024),
                nested_virt: None,
                uefi_firmware: None,
                secure_boot_enabled: None,
                secure_boot_template: None,
                tpm_enabled: None,
                fastdesk: None,
                disks: vec![],
                create_disk: Some(CreateDiskPayload {
                    path: disk_path.clone(),
                    size_bytes: 16 * 1024 * 1024,
                    format: DiskFormat::Raw,
                    fixed: false,
                }),
                iso_path: None,
                nics: None,
            })
            .send()
            .await
            .unwrap();
        assert!(
            !rollback_resp.status().is_success(),
            "expected failure when save_vm cannot create vms/<id>"
        );
        assert!(
            !disk_path.exists(),
            "post-disk-creation rollback failed to remove {}",
            disk_path.display()
        );
        fs::remove_file(&vms_dir).unwrap();
        fs::create_dir_all(&vms_dir).unwrap();

        // Retrying with corrected state using the same disk path must now succeed.
        let ok_resp = client
            .post(format!("{}/api/v1/vms", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&CreateVmPayload {
                name: "Validate-First-VM".into(),
                description: String::new(),
                preset: VmPreset::Custom,
                vcpus: Some(4),
                memory_mb: Some(1024),
                nested_virt: None,
                uefi_firmware: None,
                secure_boot_enabled: None,
                secure_boot_template: None,
                tpm_enabled: None,
                fastdesk: None,
                disks: vec![],
                create_disk: Some(CreateDiskPayload {
                    path: disk_path.clone(),
                    size_bytes: 16 * 1024 * 1024,
                    format: DiskFormat::Raw,
                    fixed: false,
                }),
                iso_path: None,
                nics: None,
            })
            .send()
            .await
            .unwrap();
        assert_eq!(ok_resp.status(), 201);
        let vm: VmSummary = ok_resp.json().await.unwrap();
        assert_eq!(vm.config.disks.len(), 1);
        assert_eq!(vm.config.disks[0].path, disk_path);
        assert!(disk_path.exists());

        daemon.shutdown().await;
    }

    #[tokio::test]
    async fn standalone_create_disk_rejects_incompatible_extension() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = TestDaemonHandle::spawn(dir.path().to_path_buf())
            .await
            .unwrap();
        let client = test_http_client();

        for (name, format) in [
            ("disk.vhdx", DiskFormat::Raw),
            ("disk.vhd", DiskFormat::Vhdx),
            ("disk.iso", DiskFormat::Vhdx),
            ("disk.vmdk", DiskFormat::Raw),
        ] {
            let path = dir.path().join(name);
            let resp = client
                .post(format!("{}/api/v1/disks/create", daemon.base_url))
                .bearer_auth(&daemon.token)
                .json(&CreateDiskPayload {
                    path: path.clone(),
                    size_bytes: 16 * 1024 * 1024,
                    format,
                    fixed: false,
                })
                .send()
                .await
                .unwrap();
            assert_eq!(resp.status(), 400, "{name} as {format:?} should fail with 400");
            assert!(!path.exists(), "{name} must not be created");
        }

        let valid_path = dir.path().join("disk.img");
        let ok_resp = client
            .post(format!("{}/api/v1/disks/create", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&CreateDiskPayload {
                path: valid_path.clone(),
                size_bytes: 16 * 1024 * 1024,
                format: DiskFormat::Raw,
                fixed: false,
            })
            .send()
            .await
            .unwrap();
        assert_eq!(ok_resp.status(), 200);
        assert!(valid_path.exists());

        daemon.shutdown().await;
    }

    #[tokio::test]
    async fn delete_disks_preserves_disk_referenced_by_another_vm() {
        let dir = tempfile::tempdir().unwrap();
        let shared_disk = dir.path().join("shared.img");
        let private_disk = dir.path().join("private.img");
        fs::write(&shared_disk, b"SHARED").unwrap();
        fs::write(&private_disk, b"PRIVATE").unwrap();

        let mk_disk = |lun: u32, path: std::path::PathBuf| openvmms_core::models::DiskAttachment {
            lun,
            kind: DiskKind::HardDisk,
            path,
            read_only: false,
            format: DiskFormat::Raw,
        };

        // Pre-seed two persisted VM configs referencing the same image to verify
        // `delete_vm(delete_disks=true)` preserves disks still referenced elsewhere.
        let store = openvmms_core::store::RegistryStore::new(dir.path()).unwrap();
        let mut owner1 = openvmms_core::models::VmConfig::from_preset(
            "VM-Owner-1",
            VmPreset::Custom,
            &store.vm_dir(uuid::Uuid::new_v4()),
        );
        owner1.disks = vec![
            mk_disk(0, shared_disk.clone()),
            mk_disk(1, private_disk.clone()),
        ];
        store.save_vm(&owner1).unwrap();

        let mut owner2 = openvmms_core::models::VmConfig::from_preset(
            "VM-Owner-2",
            VmPreset::Custom,
            &store.vm_dir(uuid::Uuid::new_v4()),
        );
        owner2.disks = vec![mk_disk(0, shared_disk.clone())];
        store.save_vm(&owner2).unwrap();

        let daemon = TestDaemonHandle::spawn(dir.path().to_path_buf())
            .await
            .unwrap();
        let client = test_http_client();

        let del = client
            .delete(format!(
                "{}/api/v1/vms/VM-Owner-1?delete_disks=true",
                daemon.base_url
            ))
            .bearer_auth(&daemon.token)
            .send()
            .await
            .unwrap();
        assert_eq!(del.status(), 204);
        assert!(shared_disk.exists(), "shared disk must not be deleted");
        assert!(!private_disk.exists(), "unshared private disk should be deleted");

        daemon.shutdown().await;
    }

    #[tokio::test]
    async fn host_config_rejects_non_loopback_listen_addr_and_runtime_dir_change_while_active() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = TestDaemonHandle::spawn(dir.path().to_path_buf())
            .await
            .unwrap();
        let client = test_http_client();

        let base_cfg: openvmms_core::models::HostConfig = client
            .get(format!("{}/api/v1/host/config", daemon.base_url))
            .bearer_auth(&daemon.token)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        let mut non_loopback = base_cfg.clone();
        non_loopback.listen_addr = "0.0.0.0:47620".into();
        assert_eq!(
            client
                .put(format!("{}/api/v1/host/config", daemon.base_url))
                .bearer_auth(&daemon.token)
                .json(&non_loopback)
                .send()
                .await
                .unwrap()
                .status(),
            400
        );

        let mut changed_data = base_cfg.clone();
        changed_data.data_dir = dir.path().join("other-data");
        assert_eq!(
            client
                .put(format!("{}/api/v1/host/config", daemon.base_url))
                .bearer_auth(&daemon.token)
                .json(&changed_data)
                .send()
                .await
                .unwrap()
                .status(),
            400
        );

        let created: VmSummary = client
            .post(format!("{}/api/v1/vms", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&CreateVmPayload {
                name: "Active-RuntimeDir-VM".into(),
                preset: VmPreset::Custom,
                vcpus: Some(2),
                memory_mb: Some(512),
                ..Default::default()
            })
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        daemon
            .supervisor
            .set_vm_status_for_test(created.config.id, VmStatus::Running);
        let mut changed_runtime = base_cfg.clone();
        changed_runtime.runtime_dir = dir.path().join("alt-runtime");
        assert_eq!(
            client
                .put(format!("{}/api/v1/host/config", daemon.base_url))
                .bearer_auth(&daemon.token)
                .json(&changed_runtime)
                .send()
                .await
                .unwrap()
                .status(),
            409
        );

        daemon
            .supervisor
            .set_vm_status_for_test(created.config.id, VmStatus::Off);
        assert_eq!(
            client
                .put(format!("{}/api/v1/host/config", daemon.base_url))
                .bearer_auth(&daemon.token)
                .json(&changed_runtime)
                .send()
                .await
                .unwrap()
                .status(),
            200
        );

        daemon.shutdown().await;
    }

    #[tokio::test]
    async fn listen_override_does_not_overwrite_config_toml() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = TestDaemonHandle::spawn(dir.path().to_path_buf())
            .await
            .unwrap();

        let store = openvmms_core::store::RegistryStore::new(dir.path()).unwrap();
        let persisted = store.load_or_init_host_config().unwrap();
        assert_eq!(
            persisted.listen_addr,
            openvmms_core::models::DEFAULT_DAEMON_LISTEN
        );
        let runtime_addr = store.read_runtime_listen_addr().unwrap();
        assert_eq!(format!("http://{runtime_addr}"), daemon.base_url);

        let data_dir = dir.path().to_path_buf();
        let expected_url = daemon.base_url.clone();
        tokio::task::spawn_blocking(move || {
            let api = openvmms_core::api_client::ApiClient::from_data_dir(Some(&data_dir), None)
                .unwrap();
            assert_eq!(api.base_url(), expected_url);
            api.health().unwrap();
        })
        .await
        .unwrap();

        daemon.shutdown().await;
        assert!(store.read_runtime_listen_addr().is_none());
        assert_eq!(
            store.load_or_init_host_config().unwrap().listen_addr,
            openvmms_core::models::DEFAULT_DAEMON_LISTEN
        );
    }

    #[test]
    fn api_error_status_codes_use_typed_errors_not_substrings() {
        use super::ApiError;
        use axum::http::StatusCode;
        // Wrap each typed error with misleading substrings to ensure text never drives status codes.
        let not_found = ApiError::map_anyhow(
            crate::supervisor::err_not_found("missing item")
                .context("cannot proceed: already exists"),
        );
        assert_eq!(not_found.status, StatusCode::NOT_FOUND);

        let conflict = ApiError::map_anyhow(
            crate::supervisor::err_conflict("duplicate state").context("not found wrapper"),
        );
        assert_eq!(conflict.status, StatusCode::CONFLICT);

        let bad_req = ApiError::map_anyhow(
            crate::supervisor::err_bad_request("invalid size").context("cannot allocate; not found"),
        );
        assert_eq!(bad_req.status, StatusCode::BAD_REQUEST);

        let internal = ApiError::map_anyhow(anyhow::anyhow!("not found / cannot / already exists"));
        assert_eq!(internal.status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn create_vm_with_iso_in_disks_list_normalizes_to_readonly_dvd() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = TestDaemonHandle::spawn(dir.path().to_path_buf())
            .await
            .unwrap();
        let client = test_http_client();

        let iso_path = dir.path().join("installer.iso");
        fs::write(&iso_path, b"ISO").unwrap();

        let resp = client
            .post(format!("{}/api/v1/vms", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&CreateVmPayload {
                name: "Iso-Normalize-VM".into(),
                preset: VmPreset::Custom,
                vcpus: Some(2),
                memory_mb: Some(512),
                disks: vec![openvmms_core::models::DiskAttachment {
                    lun: 0,
                    kind: DiskKind::HardDisk,
                    path: iso_path,
                    read_only: false,
                    format: DiskFormat::Raw,
                }],
                ..Default::default()
            })
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 201);
        let vm: VmSummary = resp.json().await.unwrap();
        assert_eq!(vm.config.disks[0].kind, DiskKind::Dvd);
        assert!(vm.config.disks[0].read_only);
        assert_eq!(vm.config.disks[0].format, DiskFormat::Iso);

        daemon.shutdown().await;
    }

    #[tokio::test]
    async fn auth_middleware_rejects_missing_wrong_empty_truncated_and_query_tokens_across_all_protected_routes() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = TestDaemonHandle::spawn(dir.path().to_path_buf())
            .await
            .unwrap();
        let client = test_http_client();
        let truncated = &daemon.token[..daemon.token.len().saturating_sub(1)];

        let routes: Vec<(reqwest::Method, &str)> = vec![
            (reqwest::Method::GET, "/api/v1/events"),
            (reqwest::Method::GET, "/api/v1/host/info"),
            (reqwest::Method::GET, "/api/v1/host/config"),
            (reqwest::Method::PUT, "/api/v1/host/config"),
            (reqwest::Method::GET, "/api/v1/host/switches"),
            (reqwest::Method::POST, "/api/v1/daemon/shutdown"),
            (reqwest::Method::POST, "/api/v1/disks/create"),
            (reqwest::Method::POST, "/api/v1/disks/inspect"),
            (reqwest::Method::GET, "/api/v1/vms"),
            (reqwest::Method::POST, "/api/v1/vms"),
            (
                reqwest::Method::GET,
                "/api/v1/vms/00000000-0000-0000-0000-000000000000",
            ),
            (
                reqwest::Method::PUT,
                "/api/v1/vms/00000000-0000-0000-0000-000000000000",
            ),
            (
                reqwest::Method::DELETE,
                "/api/v1/vms/00000000-0000-0000-0000-000000000000",
            ),
            (
                reqwest::Method::POST,
                "/api/v1/vms/00000000-0000-0000-0000-000000000000/start",
            ),
            (
                reqwest::Method::POST,
                "/api/v1/vms/00000000-0000-0000-0000-000000000000/stop",
            ),
            (
                reqwest::Method::POST,
                "/api/v1/vms/00000000-0000-0000-0000-000000000000/shutdown",
            ),
            (
                reqwest::Method::POST,
                "/api/v1/vms/00000000-0000-0000-0000-000000000000/pause",
            ),
            (
                reqwest::Method::POST,
                "/api/v1/vms/00000000-0000-0000-0000-000000000000/resume",
            ),
            (
                reqwest::Method::POST,
                "/api/v1/vms/00000000-0000-0000-0000-000000000000/reset",
            ),
            (
                reqwest::Method::POST,
                "/api/v1/vms/00000000-0000-0000-0000-000000000000/disks",
            ),
            (
                reqwest::Method::DELETE,
                "/api/v1/vms/00000000-0000-0000-0000-000000000000/disks/0",
            ),
            (
                reqwest::Method::POST,
                "/api/v1/vms/00000000-0000-0000-0000-000000000000/iso",
            ),
            (
                reqwest::Method::DELETE,
                "/api/v1/vms/00000000-0000-0000-0000-000000000000/iso",
            ),
            (
                reqwest::Method::DELETE,
                "/api/v1/vms/00000000-0000-0000-0000-000000000000/iso/0",
            ),
            (
                reqwest::Method::POST,
                "/api/v1/vms/00000000-0000-0000-0000-000000000000/nics/00000000-0000-0000-0000-000000000000/ports",
            ),
            (
                reqwest::Method::DELETE,
                "/api/v1/vms/00000000-0000-0000-0000-000000000000/nics/00000000-0000-0000-0000-000000000000/ports",
            ),
            (
                reqwest::Method::GET,
                "/api/v1/vms/00000000-0000-0000-0000-000000000000/logs",
            ),
        ];

        for (method, path) in &routes {
            for case in ["missing", "wrong", "empty", "truncated", "query"] {
                let req = match case {
                    "missing" => client.request(method.clone(), format!("{}{path}", daemon.base_url)),
                    "wrong" => client
                        .request(method.clone(), format!("{}{path}", daemon.base_url))
                        .bearer_auth("definitely-not-the-valid-token"),
                    "empty" => client
                        .request(method.clone(), format!("{}{path}", daemon.base_url))
                        .header(reqwest::header::AUTHORIZATION, "Bearer "),
                    "truncated" => client
                        .request(method.clone(), format!("{}{path}", daemon.base_url))
                        .bearer_auth(truncated),
                    "query" => client.request(
                        method.clone(),
                        format!("{}{path}?token={}", daemon.base_url, daemon.token),
                    ),
                    _ => unreachable!(),
                };
                let resp = req.send().await.unwrap();
                assert_eq!(
                    resp.status(),
                    401,
                    "route {method} {path} ({case}) must return 401"
                );
            }
        }

        let health = client
            .get(format!("{}/api/v1/health", daemon.base_url))
            .send()
            .await
            .unwrap();
        assert_eq!(health.status(), 200);

        daemon.shutdown().await;
    }

    #[tokio::test]
    async fn attach_and_create_vm_reject_duplicate_writable_disk_and_vhdx_magic_as_raw() {
        let dir = tempfile::tempdir().unwrap();
        let daemon = TestDaemonHandle::spawn(dir.path().to_path_buf())
            .await
            .unwrap();
        let client = test_http_client();

        let disk_a = dir.path().join("disk-a.img");
        let disk_b = dir.path().join("disk-b.img");
        let shared_ro = dir.path().join("shared-ro.img");
        let disguised_vhdx = dir.path().join("checkpoint.dat");
        fs::write(&disk_a, vec![0u8; 4096]).unwrap();
        fs::write(&disk_b, vec![0u8; 4096]).unwrap();
        fs::write(&shared_ro, vec![0u8; 4096]).unwrap();
        fs::write(&disguised_vhdx, b"vhdxfile\0\0\0\0\0\0\0\0").unwrap();

        // 1. Creating VM-1 with disguised VHDX marked as raw fails cleanly.
        let bad_create = client
            .post(format!("{}/api/v1/vms", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&CreateVmPayload {
                name: "BadMagic-VM".into(),
                preset: VmPreset::Custom,
                vcpus: Some(2),
                memory_mb: Some(512),
                disks: vec![openvmms_core::models::DiskAttachment {
                    lun: 0,
                    kind: DiskKind::HardDisk,
                    path: disguised_vhdx.clone(),
                    read_only: false,
                    format: DiskFormat::Raw,
                }],
                ..Default::default()
            })
            .send()
            .await
            .unwrap();
        assert_eq!(bad_create.status(), 400);

        // 2. Create VM-1 owning writable disk_a + read-only shared_ro.
        let vm1_resp = client
            .post(format!("{}/api/v1/vms", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&CreateVmPayload {
                name: "DiskGuard-1".into(),
                preset: VmPreset::Custom,
                vcpus: Some(2),
                memory_mb: Some(512),
                disks: vec![
                    openvmms_core::models::DiskAttachment {
                        lun: 0,
                        kind: DiskKind::HardDisk,
                        path: disk_a.clone(),
                        read_only: false,
                        format: DiskFormat::Raw,
                    },
                    openvmms_core::models::DiskAttachment {
                        lun: 2,
                        kind: DiskKind::HardDisk,
                        path: shared_ro.clone(),
                        read_only: true,
                        format: DiskFormat::Raw,
                    },
                ],
                ..Default::default()
            })
            .send()
            .await
            .unwrap();
        assert_eq!(vm1_resp.status(), 201);

        // 3. Re-attaching disk_a on another LUN of VM-1 fails with 409 Conflict.
        let dup_same_vm = client
            .post(format!("{}/api/v1/vms/DiskGuard-1/disks", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&openvmms_core::models::AttachDiskPayload {
                path: disk_a.clone(),
                lun: Some(3),
                read_only: false,
                format: Some(DiskFormat::Raw),
            })
            .send()
            .await
            .unwrap();
        assert_eq!(dup_same_vm.status(), 409);

        // 4. Creating VM-2 with writable disk_a fails with 409 Conflict.
        let conflict_create = client
            .post(format!("{}/api/v1/vms", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&CreateVmPayload {
                name: "DiskGuard-2".into(),
                preset: VmPreset::Custom,
                vcpus: Some(2),
                memory_mb: Some(512),
                disks: vec![openvmms_core::models::DiskAttachment {
                    lun: 0,
                    kind: DiskKind::HardDisk,
                    path: disk_a.clone(),
                    read_only: false,
                    format: DiskFormat::Raw,
                }],
                ..Default::default()
            })
            .send()
            .await
            .unwrap();
        assert_eq!(conflict_create.status(), 409);

        // 5. Create VM-2 with disk_b + read-only shared_ro -> 201 (both RO allowed).
        let vm2_resp = client
            .post(format!("{}/api/v1/vms", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&CreateVmPayload {
                name: "DiskGuard-2".into(),
                preset: VmPreset::Custom,
                vcpus: Some(2),
                memory_mb: Some(512),
                disks: vec![
                    openvmms_core::models::DiskAttachment {
                        lun: 0,
                        kind: DiskKind::HardDisk,
                        path: disk_b.clone(),
                        read_only: false,
                        format: DiskFormat::Raw,
                    },
                    openvmms_core::models::DiskAttachment {
                        lun: 1,
                        kind: DiskKind::HardDisk,
                        path: shared_ro,
                        read_only: true,
                        format: DiskFormat::Raw,
                    },
                ],
                ..Default::default()
            })
            .send()
            .await
            .unwrap();
        assert_eq!(vm2_resp.status(), 201);

        // 6. Attaching disk_a (writable or read-only) to VM-2 fails with 409 Conflict.
        let cross_attach = client
            .post(format!("{}/api/v1/vms/DiskGuard-2/disks", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&openvmms_core::models::AttachDiskPayload {
                path: disk_a,
                lun: Some(2),
                read_only: true,
                format: Some(DiskFormat::Raw),
            })
            .send()
            .await
            .unwrap();
        assert_eq!(cross_attach.status(), 409);

        // 7. Attaching disguised_vhdx with inferred format (format: None) fails with 400.
        let inferred_vhdx = client
            .post(format!("{}/api/v1/vms/DiskGuard-2/disks", daemon.base_url))
            .bearer_auth(&daemon.token)
            .json(&openvmms_core::models::AttachDiskPayload {
                path: disguised_vhdx,
                lun: Some(3),
                read_only: false,
                format: None,
            })
            .send()
            .await
            .unwrap();
        assert_eq!(inferred_vhdx.status(), 400);

        daemon.shutdown().await;
    }
}
