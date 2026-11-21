use crate::models::ApiErrorBody;
use crate::models::AttachDiskPayload;
use crate::models::CreateDiskPayload;
use crate::models::CreateVmPayload;
use crate::models::DEFAULT_DAEMON_LISTEN;
use crate::models::DiskInfo;
use crate::models::HostConfig;
use crate::models::HostInfo;
use crate::models::InspectDiskPayload;
use crate::models::MountIsoPayload;
use crate::models::PortForward;
use crate::models::PortForwardPayload;
use crate::models::ShutdownVmPayload;
use crate::models::UpdateVmPayload;
use crate::models::VirtualSwitchInfo;
use crate::models::VmSummary;
use crate::models::default_data_dir;
use crate::store::RegistryStore;
use anyhow::Context;
use anyhow::bail;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::path::Path;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::time::Duration;
use uuid::Uuid;

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(180);

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct PreRequestConnectError(pub String);

pub fn is_pre_request_connect_error(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause.is::<PreRequestConnectError>()
            || cause
                .downcast_ref::<reqwest::Error>()
                .is_some_and(reqwest::Error::is_connect)
    })
}

#[derive(Debug, Clone)]
pub struct ApiClient {
    base_url: String,
    token: String,
    client: reqwest::blocking::Client,
}

#[derive(serde::Deserialize)]
struct HealthCheckBody {
    status: String,
    #[serde(default)]
    service: Option<String>,
    #[serde(default)]
    auth_proof: Option<String>,
}

impl ApiClient {
    pub fn new(base_url: impl Into<String>, token: impl Into<String>) -> anyhow::Result<Self> {
        let base_url = validate_daemon_base_url(&base_url.into())?;
        let client = reqwest::blocking::Client::builder()
            .no_proxy()
            .connect_timeout(Duration::from_secs(10))
            // Disable reqwest's 30 s default client timeout so `post_no_timeout` (fixed disk
            // creation) can wait arbitrarily long; every other call sets its own timeout.
            .timeout(None)
            .build()
            .context("building HTTP client")?;
        Ok(Self {
            base_url,
            token: token.into(),
            client,
        })
    }

    pub fn from_data_dir(
        data_dir: Option<&Path>,
        daemon_url_override: Option<&str>,
    ) -> anyhow::Result<Self> {
        let dir = data_dir
            .map(resolve_client_path)
            .unwrap_or_else(default_data_dir);
        let store = RegistryStore::open(&dir);
        let cfg = store.load_host_config_or_default()?;
        let token = store.read_token()?;
        let base_url = if let Some(u) = daemon_url_override {
            normalize_base_url(u)
        } else if store.is_daemon_locked()
            && let Some(runtime_addr) = store.read_runtime_listen_addr()
        {
            normalize_base_url(&runtime_addr)
        } else {
            normalize_base_url(&cfg.listen_addr)
        };
        Self::new(base_url, token)
    }

    /// Resolves a client strictly from the live daemon's `daemon.addr` while the daemon
    /// holds `daemon.lock`, without falling back to `config.toml`'s configured `listen_addr`.
    pub fn from_runtime_addr(data_dir: Option<&Path>) -> anyhow::Result<Option<Self>> {
        let dir = data_dir
            .map(resolve_client_path)
            .unwrap_or_else(default_data_dir);
        let store = RegistryStore::open(&dir);
        if !store.is_daemon_locked() {
            return Ok(None);
        }
        let Some(runtime_addr) = store.read_runtime_listen_addr() else {
            return Ok(None);
        };
        let token = store.read_token()?;
        Ok(Some(Self::new(normalize_base_url(&runtime_addr), token)?))
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn token(&self) -> &str {
        &self.token
    }

    pub fn health(&self) -> anyhow::Result<()> {
        let challenge = Uuid::new_v4().to_string();
        let resp = self
            .client
            .get(format!("{}/api/v1/health", self.base_url))
            .query(&[("challenge", challenge.as_str())])
            .timeout(Duration::from_secs(2))
            .send()
            .context("connecting to openvmmsd")?;
        if !resp.status().is_success() {
            bail!("openvmmsd health returned {}", resp.status());
        }
        let remote_addr = resp
            .remote_addr()
            .context("cannot verify daemon health proof without remote socket address")?;
        let body: HealthCheckBody = resp
            .json()
            .context("decoding openvmmsd health response")?;
        if body.status != "ok" {
            bail!("openvmmsd health status was '{}'", body.status);
        }
        if let Some(service) = body.service.as_deref()
            && service != "openvmmsd"
        {
            bail!("unexpected service '{service}' at {}", self.base_url);
        }
        if !self.token.is_empty() {
            let expected =
                crate::models::compute_health_auth_proof(&self.token, remote_addr, &challenge);
            let proof_ok = body
                .auth_proof
                .as_deref()
                .is_some_and(|p| constant_time_eq(p.as_bytes(), expected.as_bytes()));
            if !proof_ok {
                bail!(
                    "daemon at {} failed health authentication challenge; refusing to send bearer token",
                    self.base_url
                );
            }
        }
        Ok(())
    }

    fn ensure_verified(&self) -> anyhow::Result<()> {
        if !self.token.is_empty() {
            self.health().map_err(|err| {
                if err.chain().any(|cause| cause.is::<reqwest::Error>()) {
                    anyhow::Error::new(PreRequestConnectError(format!("{err:#}")))
                } else {
                    err
                }
            })?;
        }
        Ok(())
    }

    pub fn host_info(&self) -> anyhow::Result<HostInfo> {
        self.get("/api/v1/host/info")
    }

    pub fn host_config(&self) -> anyhow::Result<HostConfig> {
        self.get("/api/v1/host/config")
    }

    pub fn update_host_config(&self, cfg: &HostConfig) -> anyhow::Result<HostConfig> {
        let mut resolved = cfg.clone();
        if !resolved.data_dir.as_os_str().is_empty() {
            resolved.data_dir = resolve_client_path(&resolved.data_dir);
        }
        if !resolved.runtime_dir.as_os_str().is_empty() {
            resolved.runtime_dir = resolve_client_path(&resolved.runtime_dir);
        }
        resolved.openvmm_bin = resolved.openvmm_bin.as_deref().map(resolve_client_path);
        resolved.fastdesk_viewer_bin = resolved
            .fastdesk_viewer_bin
            .as_deref()
            .map(resolve_client_path);
        resolved.openvmm_img_bin = resolved.openvmm_img_bin.as_deref().map(resolve_client_path);
        resolved.uefi_firmware_path = resolved
            .uefi_firmware_path
            .as_deref()
            .map(resolve_client_path);
        self.put("/api/v1/host/config", &resolved)
    }

    pub fn list_switches(&self) -> anyhow::Result<Vec<VirtualSwitchInfo>> {
        self.get("/api/v1/host/switches")
    }

    pub fn list_vms(&self) -> anyhow::Result<Vec<VmSummary>> {
        self.get("/api/v1/vms")
    }

    pub fn get_vm(&self, id_or_name: &str) -> anyhow::Result<VmSummary> {
        self.get(&format!("/api/v1/vms/{}", url_segment(id_or_name)))
    }

    pub fn create_vm(&self, req: &CreateVmPayload) -> anyhow::Result<VmSummary> {
        let mut resolved = req.clone();
        resolved.uefi_firmware = resolved.uefi_firmware.as_deref().map(resolve_client_path);
        resolved.iso_path = resolved.iso_path.as_deref().map(resolve_client_path);
        for d in &mut resolved.disks {
            d.path = resolve_client_path(&d.path);
        }
        let has_create_disk = resolved.create_disk.is_some();
        if let Some(cd) = &mut resolved.create_disk
            && !cd.path.as_os_str().is_empty()
        {
            cd.path = resolve_client_path(&cd.path);
        }
        if has_create_disk {
            self.post_no_timeout("/api/v1/vms", &resolved)
        } else {
            self.post("/api/v1/vms", &resolved)
        }
    }

    pub fn update_vm(&self, id_or_name: &str, req: &UpdateVmPayload) -> anyhow::Result<VmSummary> {
        let mut resolved = req.clone();
        if let Some(fw_opt) = &resolved.uefi_firmware {
            resolved.uefi_firmware = Some(match fw_opt {
                Some(fw) if !fw.as_os_str().is_empty() => Some(resolve_client_path(fw)),
                _ => None,
            });
        }
        if let Some(disks) = &mut resolved.disks {
            for d in disks {
                d.path = resolve_client_path(&d.path);
            }
        }
        self.put(&format!("/api/v1/vms/{}", url_segment(id_or_name)), &resolved)
    }

    pub fn delete_vm(&self, id_or_name: &str, delete_disks: bool) -> anyhow::Result<()> {
        self.ensure_verified()?;
        let suffix = if delete_disks {
            "?delete_disks=true"
        } else {
            ""
        };
        let url = format!("{}/api/v1/vms/{}{}", self.base_url, url_segment(id_or_name), suffix);
        let resp = self
            .client
            .delete(url)
            .bearer_auth(&self.token)
            .timeout(DEFAULT_REQUEST_TIMEOUT)
            .send()
            .context("DELETE VM request failed")?;
        decode_empty_response(resp)
    }

    pub fn start_vm(&self, id_or_name: &str) -> anyhow::Result<VmSummary> {
        self.post_empty(&format!("/api/v1/vms/{}/start", url_segment(id_or_name)))
    }

    pub fn stop_vm(&self, id_or_name: &str) -> anyhow::Result<VmSummary> {
        self.post_empty(&format!("/api/v1/vms/{}/stop", url_segment(id_or_name)))
    }

    pub fn shutdown_vm(
        &self,
        id_or_name: &str,
        req: &ShutdownVmPayload,
    ) -> anyhow::Result<VmSummary> {
        self.post(
            &format!("/api/v1/vms/{}/shutdown", url_segment(id_or_name)),
            req,
        )
    }

    pub fn pause_vm(&self, id_or_name: &str) -> anyhow::Result<VmSummary> {
        self.post_empty(&format!("/api/v1/vms/{}/pause", url_segment(id_or_name)))
    }

    pub fn resume_vm(&self, id_or_name: &str) -> anyhow::Result<VmSummary> {
        self.post_empty(&format!("/api/v1/vms/{}/resume", url_segment(id_or_name)))
    }

    pub fn reset_vm(&self, id_or_name: &str) -> anyhow::Result<VmSummary> {
        self.post_empty(&format!("/api/v1/vms/{}/reset", url_segment(id_or_name)))
    }

    pub fn attach_disk(
        &self,
        id_or_name: &str,
        req: &AttachDiskPayload,
    ) -> anyhow::Result<VmSummary> {
        let mut resolved = req.clone();
        resolved.path = resolve_client_path(&resolved.path);
        self.post(
            &format!("/api/v1/vms/{}/disks", url_segment(id_or_name)),
            &resolved,
        )
    }

    pub fn detach_disk(&self, id_or_name: &str, lun: u32) -> anyhow::Result<VmSummary> {
        self.delete_json(&format!(
            "/api/v1/vms/{}/disks/{}",
            url_segment(id_or_name),
            lun
        ))
    }

    pub fn mount_iso(&self, id_or_name: &str, req: &MountIsoPayload) -> anyhow::Result<VmSummary> {
        let mut resolved = req.clone();
        resolved.path = resolve_client_path(&resolved.path);
        self.post(
            &format!("/api/v1/vms/{}/iso", url_segment(id_or_name)),
            &resolved,
        )
    }

    pub fn eject_iso(&self, id_or_name: &str, lun: Option<u32>) -> anyhow::Result<VmSummary> {
        let path = match lun {
            Some(l) => format!("/api/v1/vms/{}/iso/{l}", url_segment(id_or_name)),
            None => format!("/api/v1/vms/{}/iso", url_segment(id_or_name)),
        };
        self.delete_json(&path)
    }

    pub fn add_port_forward(
        &self,
        id_or_name: &str,
        nic_id: Uuid,
        port: PortForward,
    ) -> anyhow::Result<VmSummary> {
        self.post(
            &format!(
                "/api/v1/vms/{}/nics/{nic_id}/ports",
                url_segment(id_or_name)
            ),
            &PortForwardPayload { port },
        )
    }

    pub fn remove_port_forward(
        &self,
        id_or_name: &str,
        nic_id: Uuid,
        port: PortForward,
    ) -> anyhow::Result<VmSummary> {
        self.ensure_verified()?;
        let url = format!(
            "{}/api/v1/vms/{}/nics/{nic_id}/ports",
            self.base_url,
            url_segment(id_or_name)
        );
        let resp = self
            .client
            .delete(url)
            .bearer_auth(&self.token)
            .json(&PortForwardPayload { port })
            .timeout(DEFAULT_REQUEST_TIMEOUT)
            .send()
            .context("DELETE port forward request failed")?;
        decode_json_response(resp)
    }

    pub fn vm_logs(&self, id_or_name: &str, tail: usize) -> anyhow::Result<String> {
        #[derive(serde::Deserialize)]
        struct LogResponse {
            content: String,
        }
        let resp: LogResponse = self.get(&format!(
            "/api/v1/vms/{}/logs?tail={tail}",
            url_segment(id_or_name)
        ))?;
        Ok(resp.content)
    }

    pub fn create_disk(&self, req: &CreateDiskPayload) -> anyhow::Result<DiskInfo> {
        let mut resolved = req.clone();
        resolved.path = resolve_client_path(&resolved.path);
        self.post_no_timeout("/api/v1/disks/create", &resolved)
    }

    pub fn inspect_disk(&self, path: &Path) -> anyhow::Result<DiskInfo> {
        self.post(
            "/api/v1/disks/inspect",
            &InspectDiskPayload {
                path: resolve_client_path(path),
            },
        )
    }

    pub fn shutdown_daemon(&self) -> anyhow::Result<()> {
        self.shutdown_daemon_with_options(false)
    }

    pub fn shutdown_daemon_with_options(&self, stop_vms: bool) -> anyhow::Result<()> {
        self.ensure_verified()?;
        let url = if stop_vms {
            format!("{}/api/v1/daemon/shutdown?stop_vms=true", self.base_url)
        } else {
            format!("{}/api/v1/daemon/shutdown", self.base_url)
        };
        let resp = self
            .client
            .post(url)
            .bearer_auth(&self.token)
            .timeout(DEFAULT_REQUEST_TIMEOUT)
            .send()
            .context("sending daemon shutdown")?;
        decode_empty_response(resp)
    }

    fn get<T: DeserializeOwned>(&self, path: &str) -> anyhow::Result<T> {
        self.ensure_verified()?;
        let resp = self
            .client
            .get(format!("{}{path}", self.base_url))
            .bearer_auth(&self.token)
            .timeout(DEFAULT_REQUEST_TIMEOUT)
            .send()
            .with_context(|| format!("GET {path}"))?;
        decode_json_response(resp)
    }

    fn post<B: Serialize, T: DeserializeOwned>(&self, path: &str, body: &B) -> anyhow::Result<T> {
        self.ensure_verified()?;
        let resp = self
            .client
            .post(format!("{}{path}", self.base_url))
            .bearer_auth(&self.token)
            .json(body)
            .timeout(DEFAULT_REQUEST_TIMEOUT)
            .send()
            .with_context(|| format!("POST {path}"))?;
        decode_json_response(resp)
    }

    fn post_no_timeout<B: Serialize, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> anyhow::Result<T> {
        self.ensure_verified()?;
        let resp = self
            .client
            .post(format!("{}{path}", self.base_url))
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .with_context(|| format!("POST {path}"))?;
        decode_json_response(resp)
    }

    fn post_empty<T: DeserializeOwned>(&self, path: &str) -> anyhow::Result<T> {
        self.ensure_verified()?;
        let resp = self
            .client
            .post(format!("{}{path}", self.base_url))
            .bearer_auth(&self.token)
            .timeout(DEFAULT_REQUEST_TIMEOUT)
            .send()
            .with_context(|| format!("POST {path}"))?;
        decode_json_response(resp)
    }

    fn put<B: Serialize, T: DeserializeOwned>(&self, path: &str, body: &B) -> anyhow::Result<T> {
        self.ensure_verified()?;
        let resp = self
            .client
            .put(format!("{}{path}", self.base_url))
            .bearer_auth(&self.token)
            .json(body)
            .timeout(DEFAULT_REQUEST_TIMEOUT)
            .send()
            .with_context(|| format!("PUT {path}"))?;
        decode_json_response(resp)
    }

    fn delete_json<T: DeserializeOwned>(&self, path: &str) -> anyhow::Result<T> {
        self.ensure_verified()?;
        let resp = self
            .client
            .delete(format!("{}{path}", self.base_url))
            .bearer_auth(&self.token)
            .timeout(DEFAULT_REQUEST_TIMEOUT)
            .send()
            .with_context(|| format!("DELETE {path}"))?;
        decode_json_response(resp)
    }
}

pub fn resolve_client_path(path: &Path) -> PathBuf {
    if path.as_os_str().is_empty() {
        return PathBuf::new();
    }
    let joined = if path.is_absolute() {
        path.to_path_buf()
    } else if let Ok(cwd) = std::env::current_dir() {
        cwd.join(path)
    } else {
        path.to_path_buf()
    };
    normalize_client_path(&joined)
}

#[cfg(any(windows, test))]
pub(crate) fn strip_verbatim_prefix_str(s: &str) -> Option<String> {
    if s.get(..8).is_some_and(|p| p.eq_ignore_ascii_case(r"\\?\UNC\")) {
        return Some(format!(r"\\{}", &s[8..]));
    }
    if let Some(rest) = s.strip_prefix(r"\\?\")
        && rest.as_bytes().first().is_some_and(u8::is_ascii_alphabetic)
        && rest.as_bytes().get(1) == Some(&b':')
    {
        return Some(rest.to_string());
    }
    None
}

fn strip_verbatim_prefix(path: PathBuf) -> PathBuf {
    #[cfg(windows)]
    {
        if let Some(s) = path.to_str()
            && let Some(stripped) = strip_verbatim_prefix_str(s)
        {
            return PathBuf::from(stripped);
        }
    }
    path
}

fn normalize_client_path(path: &Path) -> PathBuf {
    use std::path::Component;
    let mut out = PathBuf::new();
    for comp in path.components() {
        match comp {
            Component::CurDir => {}
            Component::ParentDir => {
                // Canonicalize the prefix before resolving `..` so symlinks expand to their target's parent rather than being collapsed lexically.
                if !out.as_os_str().is_empty()
                    && let Ok(canon) = std::fs::canonicalize(&out)
                {
                    out = strip_verbatim_prefix(canon);
                    out.pop();
                } else {
                    out.push(comp.as_os_str());
                }
            }
            other => out.push(other.as_os_str()),
        }
    }
    if out.as_os_str().is_empty() {
        path.to_path_buf()
    } else {
        out
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

fn validate_daemon_base_url(raw: &str) -> anyhow::Result<String> {
    let trimmed = raw.trim().trim_end_matches('/');
    if trimmed
        .get(..8)
        .is_some_and(|p| p.eq_ignore_ascii_case("https://"))
    {
        bail!(
            "HTTPS daemon URLs are not supported; openvmmsd serves plain HTTP on loopback (use http://)"
        );
    }
    if let Some(rest) = trimmed
        .get(..7)
        .filter(|p| p.eq_ignore_ascii_case("http://"))
        .map(|_| &trimmed[7..])
    {
        if rest.is_empty() {
            bail!("invalid daemon URL '{raw}': missing host:port");
        }
        return Ok(format!("http://{rest}"));
    }
    if trimmed.contains("://") {
        bail!("unsupported daemon URL scheme in '{raw}'; expected http://");
    }
    if trimmed.is_empty() {
        bail!("daemon URL cannot be empty");
    }
    Ok(trimmed.to_string())
}

pub fn normalize_base_url(listen_or_url: &str) -> String {
    let trimmed = listen_or_url.trim().trim_end_matches('/');
    if let Some(rest) = trimmed
        .get(..7)
        .filter(|p| p.eq_ignore_ascii_case("http://"))
        .map(|_| &trimmed[7..])
    {
        return format!("http://{rest}");
    }
    if trimmed
        .get(..8)
        .is_some_and(|p| p.eq_ignore_ascii_case("https://"))
        || trimmed.contains("://")
    {
        return trimmed.to_string();
    }
    let addr = if trimmed.is_empty() {
        DEFAULT_DAEMON_LISTEN
    } else {
        trimmed
    };
    format!("http://{addr}")
}

fn url_segment(s: &str) -> String {
    if s == "." {
        return "%2E".to_string();
    }
    if s == ".." {
        return "%2E%2E".to_string();
    }
    let mut out = String::with_capacity(s.len());
    for b in s.as_bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(*b as char);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
}

fn decode_json_response<T: DeserializeOwned>(resp: reqwest::blocking::Response) -> anyhow::Result<T> {
    let status = resp.status();
    if status.is_success() {
        resp.json::<T>().context("decoding daemon JSON response")
    } else {
        let text = resp.text().unwrap_or_default();
        if let Ok(err) = serde_json::from_str::<ApiErrorBody>(&text) {
            bail!("daemon error ({status}): {}", err.error);
        }
        bail!("daemon error ({status}): {text}");
    }
}

fn decode_empty_response(resp: reqwest::blocking::Response) -> anyhow::Result<()> {
    let status = resp.status();
    if status.is_success() {
        Ok(())
    } else {
        let text = resp.text().unwrap_or_default();
        if let Ok(err) = serde_json::from_str::<ApiErrorBody>(&text) {
            bail!("daemon error ({status}): {}", err.error);
        }
        bail!("daemon error ({status}): {text}");
    }
}

pub fn find_openvmmsd_binary() -> anyhow::Result<PathBuf> {
    let bin_name = if cfg!(windows) {
        "openvmmsd.exe"
    } else {
        "openvmmsd"
    };
    if let Ok(exe) = std::env::current_exe()
        && let Some(dir) = exe.parent()
    {
        let sibling = dir.join(bin_name);
        if sibling.is_file() {
            return Ok(sibling);
        }
        // Test binaries reside in target/<profile>/deps while workspace binaries live one level up.
        if let Some(parent) = dir.parent() {
            let up = parent.join(bin_name);
            if up.is_file() {
                return Ok(up);
            }
        }
    }
    if let Some(path_var) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&path_var) {
            let p = dir.join(bin_name);
            if p.is_file() {
                return Ok(p);
            }
        }
    }
    bail!("could not locate {bin_name} binary next to current executable or in PATH");
}

/// Resolves `fastdesk-viewer` locally from client config, executable siblings, or `PATH`
/// without trusting binary paths supplied over HTTP by a daemon.
pub fn resolve_local_viewer_binary(data_dir: Option<&Path>) -> anyhow::Result<PathBuf> {
    let dir = data_dir
        .map(resolve_client_path)
        .unwrap_or_else(default_data_dir);
    let configured = RegistryStore::open(&dir)
        .load_host_config_or_default()
        .unwrap_or_default();
    crate::models::resolve_host_paths(&configured)
        .fastdesk_viewer_bin
        .context("fastdesk-viewer binary not found next to current executable or in PATH")
}

pub fn ensure_daemon_running(
    data_dir: Option<&Path>,
    daemon_url_override: Option<&str>,
) -> anyhow::Result<ApiClient> {
    static SPAWN_STDIO_LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();

    if let Some(url) = daemon_url_override {
        let _ = crate::models::parse_loopback_listen_addr(url)?;
    }
    if let Ok(client) = ApiClient::from_data_dir(data_dir, daemon_url_override)
        && client.health().is_ok()
    {
        return Ok(client);
    }

    let dir = data_dir
        .map(resolve_client_path)
        .unwrap_or_else(default_data_dir);
    let store = RegistryStore::open(&dir);
    let cfg = store.load_host_config_or_default()?;

    if store.is_daemon_locked() {
        if let Some(url) = daemon_url_override {
            bail!(
                "cannot auto-start openvmmsd at {url}: another openvmmsd instance is already running for data directory {} (listening on {})",
                dir.display(),
                cfg.listen_addr
            );
        }
        let wait_deadline = std::time::Instant::now() + Duration::from_secs(5);
        while store.is_daemon_locked() {
            if let Ok(client) = ApiClient::from_data_dir(Some(&dir), None)
                && client.health().is_ok()
            {
                return Ok(client);
            }
            if std::time::Instant::now() >= wait_deadline {
                let latest_cfg = store.load_host_config_or_default().unwrap_or(cfg);
                bail!(
                    "another openvmmsd instance holds the lock for {} but is not healthy at {}",
                    dir.display(),
                    latest_cfg.listen_addr
                );
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    let daemon_bin = find_openvmmsd_binary()?;

    let mut cmd = Command::new(&daemon_bin);
    cmd.arg("--data-dir").arg(&dir);
    if let Some(url) = daemon_url_override {
        let listen = crate::models::parse_loopback_listen_addr(url)?.to_string();
        cmd.arg("--listen").arg(listen);
    } else {
        let listen = match std::net::TcpListener::bind(&cfg.listen_addr) {
            Ok(probe) => {
                drop(probe);
                cfg.listen_addr.as_str()
            }
            Err(err) if err.kind() == std::io::ErrorKind::AddrInUse => "127.0.0.1:0",
            Err(_) => cfg.listen_addr.as_str(),
        };
        cmd.arg("--listen").arg(listen);
    }
    let dir_existed = dir.exists();
    if !dir_existed {
        let _ = std::fs::create_dir_all(&dir);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700));
        }
    }
    if dir.is_dir() {
        cmd.current_dir(&dir);
    }
    let log_file_path = dir.join("openvmmsd.log");
    if let Ok(log_file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_file_path)
    {
        let err_file = log_file.try_clone().ok();
        cmd.stdout(Stdio::from(log_file));
        if let Some(ef) = err_file {
            cmd.stderr(Stdio::from(ef));
        }
    } else {
        cmd.stdout(Stdio::null()).stderr(Stdio::null());
    }
    cmd.stdin(Stdio::null());

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        cmd.process_group(0);
    }

    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const DETACHED_PROCESS: u32 = 0x0000_0008;
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        cmd.creation_flags(DETACHED_PROCESS | CREATE_NO_WINDOW);
    }

    let mut child = {
        let _spawn_guard = SPAWN_STDIO_LOCK
            .get_or_init(|| std::sync::Mutex::new(()))
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        #[cfg(windows)]
        let _stdio_no_inherit = ClearStdioInheritGuard::acquire();
        cmd.spawn()
            .with_context(|| format!("spawning {}", daemon_bin.display()))?
    };

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait()? {
            while store.is_daemon_locked() && std::time::Instant::now() < deadline {
                if let Ok(client) = ApiClient::from_data_dir(Some(&dir), daemon_url_override)
                    && client.health().is_ok()
                {
                    return Ok(client);
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            if let Ok(client) = ApiClient::from_data_dir(Some(&dir), daemon_url_override)
                && client.health().is_ok()
            {
                return Ok(client);
            }
            bail!(
                "openvmmsd exited prematurely with {status} (see {})",
                log_file_path.display()
            );
        }
        if let Ok(client) = ApiClient::from_data_dir(Some(&dir), daemon_url_override)
            && client.health().is_ok()
        {
            return Ok(client);
        }
        if std::time::Instant::now() >= deadline {
            bail!(
                "timed out waiting for openvmmsd to become healthy (see {})",
                log_file_path.display()
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[cfg(windows)]
struct ClearStdioInheritGuard {
    saved: Vec<(windows_sys::Win32::Foundation::HANDLE, u32)>,
}

#[cfg(windows)]
impl ClearStdioInheritGuard {
    fn acquire() -> Self {
        use windows_sys::Win32::Foundation::{
            GetHandleInformation, HANDLE_FLAG_INHERIT, INVALID_HANDLE_VALUE, SetHandleInformation,
        };
        use windows_sys::Win32::System::Console::{
            GetStdHandle, STD_ERROR_HANDLE, STD_INPUT_HANDLE, STD_OUTPUT_HANDLE,
        };

        let mut saved = Vec::with_capacity(3);
        for id in [STD_INPUT_HANDLE, STD_OUTPUT_HANDLE, STD_ERROR_HANDLE] {
            let h = unsafe { GetStdHandle(id) };
            if h.is_null() || h == INVALID_HANDLE_VALUE {
                continue;
            }
            let mut flags = 0u32;
            if unsafe { GetHandleInformation(h, &mut flags) } != 0
                && (flags & HANDLE_FLAG_INHERIT) != 0
            {
                if unsafe { SetHandleInformation(h, HANDLE_FLAG_INHERIT, 0) } != 0 {
                    saved.push((h, flags & HANDLE_FLAG_INHERIT));
                }
            }
        }
        Self { saved }
    }
}

#[cfg(windows)]
impl Drop for ClearStdioInheritGuard {
    fn drop(&mut self) {
        use windows_sys::Win32::Foundation::{HANDLE_FLAG_INHERIT, SetHandleInformation};
        for &(h, prev) in &self.saved {
            unsafe {
                SetHandleInformation(h, HANDLE_FLAG_INHERIT, prev);
            }
        }
    }
}

pub fn launch_fastdesk_viewer(
    viewer_bin: &Path,
    endpoint: &str,
    vm_name: &str,
    no_shm: bool,
) -> anyhow::Result<Child> {
    let mut cmd = Command::new(viewer_bin);
    cmd.arg("--connect")
        .arg(endpoint)
        .arg("--title")
        .arg(format!("{vm_name} - FastDesk"));
    if no_shm {
        cmd.arg("--no-shm");
    }
    cmd.spawn()
        .with_context(|| format!("launching fastdesk-viewer {}", viewer_bin.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_client_path_resolves_relative_and_preserves_empty() {
        assert_eq!(resolve_client_path(Path::new("")), PathBuf::new());
        let dir = tempfile::tempdir().unwrap();
        let sub = dir.path().join("sub");
        std::fs::create_dir_all(&sub).unwrap();
        let resolved = resolve_client_path(&sub.join("..").join("disk.img"));
        assert_eq!(
            resolved,
            strip_verbatim_prefix(std::fs::canonicalize(dir.path()).unwrap()).join("disk.img")
        );
        assert!(resolved.is_absolute());

        // Non-existent intermediate directory should keep `..` instead of collapsing it away.
        let nonexistent = resolve_client_path(&dir.path().join("missing").join("..").join("disk.img"));
        assert_eq!(
            nonexistent,
            dir.path().join("missing").join("..").join("disk.img")
        );
    }

    #[cfg(unix)]
    #[test]
    fn resolve_client_path_preserves_symlink_parent_semantics() {
        let dir = tempfile::tempdir().unwrap();
        let real_parent = dir.path().join("real_parent");
        let real_sub = real_parent.join("sub");
        std::fs::create_dir_all(&real_sub).unwrap();

        let wrong_disk = dir.path().join("disk.img");
        let right_disk = real_parent.join("disk.img");
        std::fs::write(&wrong_disk, b"WRONG").unwrap();
        std::fs::write(&right_disk, b"RIGHT").unwrap();

        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&real_sub, &link).unwrap();

        let resolved = resolve_client_path(&link.join("..").join("disk.img"));
        assert_eq!(resolved, std::fs::canonicalize(&right_disk).unwrap());
        assert_ne!(resolved, std::fs::canonicalize(&wrong_disk).unwrap());
    }

    #[test]
    fn strip_verbatim_prefix_preserves_unc_root_and_volume_paths() {
        assert_eq!(
            strip_verbatim_prefix_str(r"\\?\UNC\server\share\dir\disk.vhdx").as_deref(),
            Some(r"\\server\share\dir\disk.vhdx")
        );
        assert_eq!(
            strip_verbatim_prefix_str(r"\\?\unc\server\share\disk.vhdx").as_deref(),
            Some(r"\\server\share\disk.vhdx")
        );
        assert_eq!(
            strip_verbatim_prefix_str(r"\\?\C:\vms\disk.vhdx").as_deref(),
            Some(r"C:\vms\disk.vhdx")
        );
        assert_eq!(
            strip_verbatim_prefix_str(r"\\?\Volume{12345678-1234-1234-1234-1234567890ab}\disk.vhdx"),
            None
        );
    }

    fn accept_with_timeout(
        listener: &std::net::TcpListener,
        timeout: Duration,
    ) -> std::net::TcpStream {
        listener.set_nonblocking(true).unwrap();
        let deadline = std::time::Instant::now() + timeout;
        loop {
            match listener.accept() {
                Ok((stream, _)) => {
                    stream.set_nonblocking(false).unwrap();
                    stream
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .unwrap();
                    return stream;
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "mock TcpListener accept timed out after {timeout:?}"
                    );
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(e) => panic!("mock TcpListener accept failed: {e}"),
            }
        }
    }

    #[test]
    fn client_refuses_to_send_bearer_token_to_unauthenticated_impostor_health() {
        use std::io::{Read, Write};
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let saw_auth = Arc::new(AtomicBool::new(false));
        let saw_auth_srv = Arc::clone(&saw_auth);

        let srv = std::thread::spawn(move || {
            let mut stream = accept_with_timeout(&listener, Duration::from_secs(5));
            let mut buf = [0u8; 2048];
            let n = stream.read(&mut buf).unwrap_or(0);
            let req = String::from_utf8_lossy(&buf[..n]);
            if req.to_ascii_lowercase().contains("authorization:") {
                saw_auth_srv.store(true, Ordering::SeqCst);
            }
            let body = r#"{"status":"ok","service":"openvmmsd"}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.flush();
        });

        let client = ApiClient::new(format!("http://{addr}"), "secret-bearer-token").unwrap();
        let err = client.list_vms().unwrap_err();
        assert!(
            format!("{err:#}").contains("failed health authentication challenge"),
            "{err:#}"
        );
        assert!(
            !saw_auth.load(Ordering::SeqCst),
            "bearer token must never be transmitted to an impostor health listener"
        );
        srv.join().unwrap();
    }

    #[test]
    fn client_rechallenges_and_refuses_token_to_port_squatter_after_daemon_stops() {
        use std::io::{Read, Write};
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let token = "secret-bearer-token";

        // Genuine daemon handles one health challenge and one authenticated GET /api/v1/vms, then exits.
        let genuine_srv = std::thread::spawn(move || {
            for _ in 0..2 {
                let mut stream = accept_with_timeout(&listener, Duration::from_secs(5));
                let mut buf = [0u8; 4096];
                let n = stream.read(&mut buf).unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);
                let first_line = req.lines().next().unwrap_or("");
                let body = if first_line.starts_with("GET /api/v1/health?") {
                    let query = first_line
                        .split_whitespace()
                        .nth(1)
                        .and_then(|p| p.split_once('?'))
                        .map(|(_, q)| q)
                        .unwrap_or("");
                    let challenge = query
                        .split('&')
                        .filter_map(|kv| kv.split_once('='))
                        .find(|(k, _)| *k == "challenge")
                        .map(|(_, v)| v)
                        .unwrap_or("");
                    let proof = crate::models::compute_health_auth_proof(token, addr, challenge);
                    format!(
                        r#"{{"status":"ok","service":"openvmmsd","auth_proof":"{proof}"}}"#
                    )
                } else {
                    "[]".to_string()
                };
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream.write_all(resp.as_bytes()).unwrap();
                stream.flush().unwrap();
            }
        });

        let client = ApiClient::new(format!("http://{addr}"), token).unwrap();
        assert!(client.list_vms().unwrap().is_empty());
        genuine_srv.join().unwrap();

        // Port squatter binds the freed port on the same address.
        let squatter = std::net::TcpListener::bind(addr).unwrap();
        let saw_auth = Arc::new(AtomicBool::new(false));
        let saw_auth_srv = Arc::clone(&saw_auth);
        let squatter_srv = std::thread::spawn(move || {
            let mut stream = accept_with_timeout(&squatter, Duration::from_secs(5));
            let mut buf = [0u8; 2048];
            let n = stream.read(&mut buf).unwrap_or(0);
            let req = String::from_utf8_lossy(&buf[..n]);
            if req.to_ascii_lowercase().contains("authorization:") {
                saw_auth_srv.store(true, Ordering::SeqCst);
            }
            let body = r#"{"status":"ok","service":"openvmmsd"}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(resp.as_bytes());
            let _ = stream.flush();
        });

        // The long-lived client must re-challenge before sending the bearer token again.
        let err = client.list_vms().unwrap_err();
        assert!(
            format!("{err:#}").contains("failed health authentication challenge"),
            "{err:#}"
        );
        assert!(
            !saw_auth.load(Ordering::SeqCst),
            "long-lived client leaked bearer token to port squatter"
        );
        squatter_srv.join().unwrap();
    }

    #[test]
    fn client_rejects_relayed_health_proof_from_different_address() {
        use std::io::{Read, Write};
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };

        let real_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let real_addr = real_listener.local_addr().unwrap();
        let token = "secret-bearer-token";

        // Genuine daemon responds to one relayed health check with proof bound to `real_addr`.
        let real_srv = std::thread::spawn(move || {
            let mut stream = accept_with_timeout(&real_listener, Duration::from_secs(5));
            let mut buf = [0u8; 4096];
            let n = stream.read(&mut buf).unwrap_or(0);
            let req = String::from_utf8_lossy(&buf[..n]);
            let first_line = req.lines().next().unwrap_or("");
            let query = first_line
                .split_whitespace()
                .nth(1)
                .and_then(|p| p.split_once('?'))
                .map(|(_, q)| q)
                .unwrap_or("");
            let challenge = query
                .split('&')
                .filter_map(|kv| kv.split_once('='))
                .find(|(k, _)| *k == "challenge")
                .map(|(_, v)| v)
                .unwrap_or("");
            let proof = crate::models::compute_health_auth_proof(token, real_addr, challenge);
            let body = format!(r#"{{"status":"ok","service":"openvmmsd","auth_proof":"{proof}"}}"#);
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(resp.as_bytes()).unwrap();
            stream.flush().unwrap();
        });

        // Relay server on a different loopback port forwards `/api/v1/health?challenge=...`
        // to the real daemon and returns the exact response body verbatim.
        let relay_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let relay_addr = relay_listener.local_addr().unwrap();
        let saw_auth = Arc::new(AtomicBool::new(false));
        let saw_auth_relay = Arc::clone(&saw_auth);
        let relay_srv = std::thread::spawn(move || {
            let mut stream = accept_with_timeout(&relay_listener, Duration::from_secs(5));
            let mut buf = [0u8; 4096];
            let n = stream.read(&mut buf).unwrap_or(0);
            let req = String::from_utf8_lossy(&buf[..n]);
            if req.to_ascii_lowercase().contains("authorization:") {
                saw_auth_relay.store(true, Ordering::SeqCst);
            }
            let first_line = req.lines().next().unwrap_or("");
            let path_and_query = first_line.split_whitespace().nth(1).unwrap_or("/api/v1/health");
            let relayed = reqwest::blocking::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap()
                .get(format!("http://{real_addr}{path_and_query}"))
                .send()
                .unwrap()
                .text()
                .unwrap();
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                relayed.len(),
                relayed
            );
            stream.write_all(resp.as_bytes()).unwrap();
            stream.flush().unwrap();
        });

        let client = ApiClient::new(format!("http://{relay_addr}"), token).unwrap();
        let err = client.list_vms().unwrap_err();
        assert!(
            format!("{err:#}").contains("failed health authentication challenge"),
            "{err:#}"
        );
        assert!(
            !saw_auth.load(Ordering::SeqCst),
            "client transmitted bearer token to health relay!"
        );
        real_srv.join().unwrap();
        relay_srv.join().unwrap();
    }

    #[test]
    fn client_bypasses_http_and_all_proxy_for_loopback_and_preserves_data_dir_mode() {
        use std::io::{Read, Write};

        // If invoked as child probe with proxy env vars set, verify ApiClient still reaches loopback.
        if let Ok(target_url) = std::env::var("OPENVMMS_PROXY_TEST_URL") {
            let token = std::env::var("OPENVMMS_PROXY_TEST_TOKEN").unwrap();
            let client = ApiClient::new(target_url, token).unwrap();
            client.health().unwrap();
            return;
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let dir = tempfile::tempdir().unwrap();
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
            assert!(ApiClient::from_data_dir(Some(dir.path()), None).is_err());
            let mode = std::fs::metadata(dir.path()).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o755, "ApiClient::from_data_dir must not chmod --data-dir");
            assert!(!dir.path().join("vms").exists());
            assert!(!dir.path().join("disks").exists());
            assert!(!dir.path().join("config.toml").exists());
        }

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let token = "proxy-test-token";
        let srv = std::thread::spawn(move || {
            let mut stream = accept_with_timeout(&listener, Duration::from_secs(5));
            let mut buf = [0u8; 4096];
            let n = stream.read(&mut buf).unwrap_or(0);
            let req = String::from_utf8_lossy(&buf[..n]);
            let first_line = req.lines().next().unwrap_or("");
            let query = first_line
                .split_whitespace()
                .nth(1)
                .and_then(|p| p.split_once('?'))
                .map(|(_, q)| q)
                .unwrap_or("");
            let challenge = query
                .split('&')
                .filter_map(|kv| kv.split_once('='))
                .find(|(k, _)| *k == "challenge")
                .map(|(_, v)| v)
                .unwrap_or("");
            let proof = crate::models::compute_health_auth_proof(token, addr, challenge);
            let body = format!(r#"{{"status":"ok","service":"openvmmsd","auth_proof":"{proof}"}}"#);
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(resp.as_bytes()).unwrap();
            stream.flush().unwrap();
        });

        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("api_client::tests::client_bypasses_http_and_all_proxy_for_loopback_and_preserves_data_dir_mode")
            .env("OPENVMMS_PROXY_TEST_URL", format!("http://{addr}"))
            .env("OPENVMMS_PROXY_TEST_TOKEN", token)
            .env("HTTP_PROXY", "http://127.0.0.1:1")
            .env("http_proxy", "http://127.0.0.1:1")
            .env("ALL_PROXY", "http://127.0.0.1:1")
            .env("all_proxy", "http://127.0.0.1:1")
            .env_remove("NO_PROXY")
            .env_remove("no_proxy")
            .status()
            .unwrap();
        assert!(status.success(), "child proxy bypass probe failed: {status}");
        srv.join().unwrap();
    }

    #[test]
    fn disk_creating_requests_do_not_hit_reqwest_30s_default_timeout() {
        use crate::models::{DiskFormat, DiskInfo, VmConfig, VmPreset, VmRuntimeState};
        use std::io::{Read, Write};

        fn read_http_request(stream: &mut std::net::TcpStream) -> String {
            let mut buf = Vec::with_capacity(4096);
            let mut tmp = [0u8; 1024];
            let header_end = loop {
                let n = stream.read(&mut tmp).unwrap_or(0);
                if n == 0 {
                    return String::from_utf8_lossy(&buf).into_owned();
                }
                buf.extend_from_slice(&tmp[..n]);
                if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    break pos + 4;
                }
            };
            let header_str = String::from_utf8_lossy(&buf[..header_end]);
            let content_len = header_str
                .lines()
                .find_map(|line| {
                    let (k, v) = line.split_once(':')?;
                    k.trim()
                        .eq_ignore_ascii_case("content-length")
                        .then(|| v.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or(0);
            while buf.len() < header_end + content_len {
                let n = stream.read(&mut tmp).unwrap_or(0);
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&tmp[..n]);
            }
            String::from_utf8_lossy(&buf[..header_end]).into_owned()
        }

        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let token = "slow-disk-test-token";

        let srv = std::thread::spawn(move || {
            let mut workers = Vec::new();
            for _ in 0..4 {
                let mut stream = accept_with_timeout(&listener, Duration::from_secs(10));
                workers.push(std::thread::spawn(move || {
                    let req = read_http_request(&mut stream);
                    let first_line = req.lines().next().unwrap_or("");
                    let (status_line, body) = if first_line.starts_with("GET /api/v1/health?") {
                        let query = first_line
                            .split_whitespace()
                            .nth(1)
                            .and_then(|p| p.split_once('?'))
                            .map(|(_, q)| q)
                            .unwrap_or("");
                        let challenge = query
                            .split('&')
                            .filter_map(|kv| kv.split_once('='))
                            .find(|(k, _)| *k == "challenge")
                            .map(|(_, v)| v)
                            .unwrap_or("");
                        let proof = crate::models::compute_health_auth_proof(token, addr, challenge);
                        (
                            "HTTP/1.1 200 OK",
                            format!(
                                r#"{{"status":"ok","service":"openvmmsd","auth_proof":"{proof}"}}"#
                            ),
                        )
                    } else if first_line.starts_with("POST /api/v1/disks/create ") {
                        std::thread::sleep(Duration::from_secs(31));
                        let disk_info = DiskInfo {
                            path: PathBuf::from("/tmp/slow-fixed.img"),
                            format: "raw".into(),
                            disk_type: "fixed".into(),
                            virtual_size_bytes: 16 * 1024 * 1024,
                            file_size_bytes: 16 * 1024 * 1024,
                            block_size_bytes: None,
                            sector_size_bytes: Some(512),
                            parent_path: None,
                            read_only: false,
                        };
                        ("HTTP/1.1 200 OK", serde_json::to_string(&disk_info).unwrap())
                    } else if first_line.starts_with("POST /api/v1/vms ") {
                        std::thread::sleep(Duration::from_secs(31));
                        let summary = VmSummary {
                            config: VmConfig::from_preset(
                                "Slow-Disk-VM",
                                VmPreset::Custom,
                                Path::new("/tmp/slow-vm"),
                            ),
                            runtime: VmRuntimeState::default(),
                        };
                        (
                            "HTTP/1.1 201 Created",
                            serde_json::to_string(&summary).unwrap(),
                        )
                    } else {
                        panic!("unexpected HTTP request in mock server: {first_line}");
                    };

                    let resp = format!(
                        "{status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    stream.write_all(resp.as_bytes()).unwrap();
                    stream.flush().unwrap();
                }));
            }
            for w in workers {
                w.join().unwrap();
            }
        });

        let client = ApiClient::new(format!("http://{addr}"), token).unwrap();
        let disk_client = client.clone();
        let disk_thread = std::thread::spawn(move || {
            disk_client.create_disk(&CreateDiskPayload {
                path: PathBuf::from("/tmp/slow-fixed.img"),
                size_bytes: 16 * 1024 * 1024,
                format: DiskFormat::Raw,
                fixed: true,
            })
        });
        let vm_thread = std::thread::spawn(move || {
            client.create_vm(&CreateVmPayload {
                name: "Slow-Disk-VM".into(),
                create_disk: Some(CreateDiskPayload {
                    path: PathBuf::from("/tmp/slow-vm-disk.img"),
                    size_bytes: 16 * 1024 * 1024,
                    format: DiskFormat::Raw,
                    fixed: true,
                }),
                ..Default::default()
            })
        });

        let disk_info = disk_thread.join().unwrap().expect("create_disk timed out");
        assert_eq!(disk_info.disk_type, "fixed");
        let vm_summary = vm_thread.join().unwrap().expect("create_vm timed out");
        assert_eq!(vm_summary.config.name, "Slow-Disk-VM");
        srv.join().unwrap();
    }

    #[test]
    fn url_segment_encodes_dot_segments() {
        assert_eq!(url_segment("."), "%2E");
        assert_eq!(url_segment(".."), "%2E%2E");
        assert_eq!(url_segment("vm-1.test"), "vm-1.test");
    }

    #[test]
    fn https_and_unsupported_daemon_urls_are_rejected() {
        for url in [
            "https://127.0.0.1:47620",
            "HTTPS://localhost:47620",
            "ftp://127.0.0.1:47620",
        ] {
            let err = ApiClient::new(url, "tok").unwrap_err();
            assert!(
                err.to_string().contains("HTTPS") || err.to_string().contains("unsupported"),
                "{url}: {err:#}"
            );
            let dir = tempfile::tempdir().unwrap();
            let store = RegistryStore::new(dir.path()).unwrap();
            let _ = store.load_or_create_token().unwrap();
            assert!(
                ApiClient::from_data_dir(Some(dir.path()), Some(url)).is_err(),
                "from_data_dir accepted {url}"
            );
            assert!(
                ensure_daemon_running(Some(dir.path()), Some(url)).is_err(),
                "ensure_daemon_running accepted {url}"
            );
        }

        let client = ApiClient::new("HTTP://127.0.0.1:47620/", "tok").unwrap();
        assert_eq!(client.base_url(), "http://127.0.0.1:47620");
    }
}
