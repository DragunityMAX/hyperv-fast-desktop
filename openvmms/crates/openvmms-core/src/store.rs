use crate::models::HostConfig;
use crate::models::VmConfig;
use crate::models::default_runtime_dir;
use anyhow::Context;
use anyhow::bail;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct RegistryStore {
    data_dir: PathBuf,
}

impl RegistryStore {
    /// Returns a lightweight store handle without creating directories or altering permissions.
    pub fn open(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
        }
    }

    pub fn new(data_dir: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let data_dir = data_dir.into();
        let vms_dir = data_dir.join("vms");
        let disks_dir = data_dir.join("disks");
        let data_existed = data_dir.exists();
        let vms_existed = vms_dir.exists();
        let disks_existed = disks_dir.exists();

        fs::create_dir_all(&vms_dir)
            .with_context(|| format!("creating VM store dir under {}", data_dir.display()))?;
        fs::create_dir_all(&disks_dir)
            .with_context(|| format!("creating disks dir under {}", data_dir.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if !data_existed {
                let _ = fs::set_permissions(&data_dir, fs::Permissions::from_mode(0o700));
            }
            if !vms_existed {
                let _ = fs::set_permissions(&vms_dir, fs::Permissions::from_mode(0o700));
            }
            if !disks_existed {
                let _ = fs::set_permissions(&disks_dir, fs::Permissions::from_mode(0o700));
            }
        }
        #[cfg(not(unix))]
        let _ = (data_existed, vms_existed, disks_existed);
        Ok(Self { data_dir })
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn host_config_path(&self) -> PathBuf {
        self.data_dir.join("config.toml")
    }

    pub fn token_path(&self) -> PathBuf {
        self.data_dir.join("daemon.token")
    }

    pub fn lock_path(&self) -> PathBuf {
        self.data_dir.join("daemon.lock")
    }

    pub fn runtime_addr_path(&self) -> PathBuf {
        self.data_dir.join("daemon.addr")
    }

    pub fn write_runtime_listen_addr(&self, addr: &str) -> anyhow::Result<()> {
        atomic_write(&self.runtime_addr_path(), addr.as_bytes(), true)
    }

    pub fn read_runtime_listen_addr(&self) -> Option<String> {
        let raw = fs::read_to_string(self.runtime_addr_path()).ok()?;
        let trimmed = raw.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    }

    pub fn clear_runtime_listen_addr(&self) {
        let _ = fs::remove_file(self.runtime_addr_path());
    }

    /// Acquires an exclusive non-blocking lock on `<data_dir>/daemon.lock` for the
    /// lifetime of the returned `File`, preventing two `openvmmsd` instances from
    /// managing the same registry simultaneously.
    pub fn try_acquire_daemon_lock(&self) -> anyhow::Result<fs::File> {
        let path = self.lock_path();
        let mut opts = fs::OpenOptions::new();
        opts.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            opts.mode(0o600);
        }
        let file = opts
            .open(&path)
            .with_context(|| format!("opening daemon lock at {}", path.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = file.set_permissions(fs::Permissions::from_mode(0o600));
        }

        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            let fl = libc::flock {
                l_type: libc::F_WRLCK as _,
                l_whence: libc::SEEK_SET as _,
                l_start: 0,
                l_len: 0,
                l_pid: 0,
            };
            let rc = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_OFD_SETLK, &fl) };
            if rc == 0 {
                return Ok(file);
            }
            let err = std::io::Error::last_os_error();
            if matches!(err.raw_os_error(), Some(libc::EAGAIN | libc::EACCES)) {
                bail!(
                    "another openvmmsd instance is already running for data directory {}",
                    self.data_dir.display()
                );
            }
            if err.raw_os_error() != Some(libc::EINVAL) {
                return Err(err).with_context(|| format!("acquiring daemon lock at {}", path.display()));
            }
        }

        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(100);
        loop {
            match file.try_lock() {
                Ok(()) => {
                    #[cfg(windows)]
                    {
                        set_path_owner_only_windows(&path)?;
                    }
                    return Ok(file);
                }
                Err(fs::TryLockError::WouldBlock) => {
                    if std::time::Instant::now() >= deadline {
                        bail!(
                            "another openvmmsd instance is already running for data directory {}",
                            self.data_dir.display()
                        );
                    }
                    std::thread::sleep(std::time::Duration::from_millis(2));
                }
                Err(fs::TryLockError::Error(err)) => {
                    return Err(err)
                        .with_context(|| format!("acquiring daemon lock at {}", path.display()));
                }
            }
        }
    }

    /// Returns `true` if another open file handle currently holds `<data_dir>/daemon.lock`.
    pub fn is_daemon_locked(&self) -> bool {
        let path = self.lock_path();
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            let Ok(file) = fs::File::open(&path) else {
                return false;
            };
            let mut fl = libc::flock {
                l_type: libc::F_WRLCK as _,
                l_whence: libc::SEEK_SET as _,
                l_start: 0,
                l_len: 0,
                l_pid: 0,
            };
            if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_OFD_GETLK, &mut fl) } == 0 {
                return fl.l_type != libc::F_UNLCK as libc::c_short;
            }
        }
        let Ok(file) = fs::File::open(&path) else {
            return false;
        };
        match file.try_lock_shared() {
            Ok(()) => {
                let _ = file.unlock();
                false
            }
            Err(fs::TryLockError::WouldBlock) => true,
            Err(fs::TryLockError::Error(_)) => false,
        }
    }

    pub fn vms_dir(&self) -> PathBuf {
        self.data_dir.join("vms")
    }

    pub fn disks_dir(&self) -> PathBuf {
        self.data_dir.join("disks")
    }

    pub fn vm_dir(&self, id: Uuid) -> PathBuf {
        self.vms_dir().join(id.to_string())
    }

    pub fn vm_config_path(&self, id: Uuid) -> PathBuf {
        self.vm_dir(id).join("vm.toml")
    }

    pub fn vm_log_path(&self, id: Uuid) -> PathBuf {
        self.vm_dir(id).join("logs").join("openvmm.log")
    }

    pub fn load_host_config_or_default(&self) -> anyhow::Result<HostConfig> {
        let path = self.host_config_path();
        if path.exists() {
            let text = fs::read_to_string(&path)
                .with_context(|| format!("reading {}", path.display()))?;
            let mut cfg: HostConfig = toml::from_str(&text)
                .with_context(|| format!("parsing {}", path.display()))?;
            if cfg.data_dir.as_os_str().is_empty() {
                cfg.data_dir = self.data_dir.clone();
            }
            if cfg.runtime_dir.as_os_str().is_empty() {
                cfg.runtime_dir = default_runtime_dir(&self.data_dir);
            }
            Ok(cfg)
        } else {
            Ok(HostConfig {
                data_dir: self.data_dir.clone(),
                runtime_dir: default_runtime_dir(&self.data_dir),
                ..Default::default()
            })
        }
    }

    pub fn load_or_init_host_config(&self) -> anyhow::Result<HostConfig> {
        let path = self.host_config_path();
        if path.exists() {
            self.load_host_config_or_default()
        } else {
            let cfg = HostConfig {
                data_dir: self.data_dir.clone(),
                runtime_dir: default_runtime_dir(&self.data_dir),
                ..Default::default()
            };
            self.save_host_config(&cfg)?;
            Ok(cfg)
        }
    }

    pub fn save_host_config(&self, cfg: &HostConfig) -> anyhow::Result<()> {
        let text = toml::to_string_pretty(cfg).context("serializing host config")?;
        atomic_write(&self.host_config_path(), text.as_bytes(), false)
    }

    pub fn load_or_create_token(&self) -> anyhow::Result<String> {
        let path = self.token_path();
        if path.exists() {
            let token = fs::read_to_string(&path)
                .with_context(|| format!("reading {}", path.display()))?
                .trim()
                .to_string();
            if !token.is_empty() {
                return Ok(token);
            }
        }
        let a = Uuid::new_v4().as_u128();
        let b = Uuid::new_v4().as_u128();
        let token = format!("{a:032x}{b:032x}");
        atomic_write(&path, token.as_bytes(), true)?;
        Ok(token)
    }

    pub fn read_token(&self) -> anyhow::Result<String> {
        let path = self.token_path();
        let token = fs::read_to_string(&path)
            .with_context(|| format!("reading daemon token at {}", path.display()))?
            .trim()
            .to_string();
        if token.is_empty() {
            bail!("daemon token file {} is empty", path.display());
        }
        Ok(token)
    }

    pub fn load_all_vms(&self) -> anyhow::Result<Vec<VmConfig>> {
        let vms_dir = self.vms_dir();
        if !vms_dir.exists() {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for entry in fs::read_dir(&vms_dir)
            .with_context(|| format!("reading {}", vms_dir.display()))?
        {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let cfg_path = entry.path().join("vm.toml");
            if !cfg_path.is_file() {
                continue;
            }
            let text = match fs::read_to_string(&cfg_path) {
                Ok(t) => t,
                Err(err) => {
                    tracing::warn!(path = %cfg_path.display(), error = %err, "skipping unreadable vm.toml");
                    continue;
                }
            };
            let vm: VmConfig = match toml::from_str(&text) {
                Ok(v) => v,
                Err(err) => {
                    tracing::warn!(path = %cfg_path.display(), error = %err, "skipping corrupt vm.toml");
                    continue;
                }
            };
            out.push(vm);
        }
        out.sort_by(|a, b| a.name.to_lowercase().cmp(&b.name.to_lowercase()).then(a.id.cmp(&b.id)));
        Ok(out)
    }

    pub fn save_vm(&self, vm: &VmConfig) -> anyhow::Result<()> {
        vm.validate()?;
        let vm_dir = self.vm_dir(vm.id);
        fs::create_dir_all(vm_dir.join("logs"))
            .with_context(|| format!("creating VM directory {}", vm_dir.display()))?;
        let text = toml::to_string_pretty(vm).context("serializing VM config")?;
        atomic_write(&self.vm_config_path(vm.id), text.as_bytes(), false)
    }

    pub fn delete_vm_dir(&self, id: Uuid) -> anyhow::Result<()> {
        let vm_dir = self.vm_dir(id);
        if !vm_dir.exists() {
            return Ok(());
        }
        // Only remove OpenVMMS's own metadata, state, and log files so any
        // user disk images residing inside vm_dir are preserved unless
        // explicitly deleted by the caller.
        for file_name in ["vm.toml", "state.vmgs"] {
            let p = vm_dir.join(file_name);
            if p.is_file() {
                fs::remove_file(&p)
                    .with_context(|| format!("removing {}", p.display()))?;
            }
        }
        let logs_dir = vm_dir.join("logs");
        if logs_dir.is_dir() {
            fs::remove_dir_all(&logs_dir)
                .with_context(|| format!("removing {}", logs_dir.display()))?;
        }
        // Remove the VM directory only if it is now empty.
        let _ = fs::remove_dir(&vm_dir);
        Ok(())
    }
}

pub fn atomic_write(path: &Path, data: &[u8], owner_only: bool) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .context("target path has no parent directory")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("creating directory {}", parent.display()))?;
    let mut tmp = tempfile::NamedTempFile::new_in(parent)
        .with_context(|| format!("creating temp file in {}", parent.display()))?;
    #[cfg(unix)]
    if owner_only {
        use std::os::unix::fs::PermissionsExt;
        tmp.as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))
            .context("setting owner-only permissions")?;
    }
    #[cfg(windows)]
    if owner_only {
        set_path_owner_only_windows(tmp.path())
            .context("setting owner-only Windows DACL")?;
    }
    #[cfg(not(any(unix, windows)))]
    let _ = owner_only;
    tmp.write_all(data)
        .with_context(|| format!("writing temp file for {}", path.display()))?;
    tmp.as_file()
        .sync_all()
        .with_context(|| format!("syncing temp file for {}", path.display()))?;
    tmp.persist(path)
        .map_err(|e| e.error)
        .with_context(|| format!("persisting {}", path.display()))?;
    Ok(())
}

#[cfg(windows)]
fn current_user_token_buf()
-> anyhow::Result<Vec<std::mem::MaybeUninit<windows_sys::Win32::Security::TOKEN_USER>>> {
    use std::mem::MaybeUninit;
    use windows_sys::Win32::Foundation::{CloseHandle, ERROR_INSUFFICIENT_BUFFER, GetLastError, HANDLE};
    use windows_sys::Win32::Security::{GetTokenInformation, TOKEN_QUERY, TOKEN_USER, TokenUser};
    use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

    struct HandleGuard(HANDLE);
    impl Drop for HandleGuard {
        fn drop(&mut self) {
            unsafe {
                CloseHandle(self.0);
            }
        }
    }

    let mut raw_token: HANDLE = std::ptr::null_mut();
    let ok = unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut raw_token) };
    if ok == 0 {
        return Err(std::io::Error::last_os_error()).context("OpenProcessToken failed");
    }
    let _token = HandleGuard(raw_token);

    let mut needed = 0u32;
    let ok = unsafe {
        GetTokenInformation(
            raw_token,
            TokenUser,
            std::ptr::null_mut(),
            0,
            &mut needed,
        )
    };
    if ok != 0 || unsafe { GetLastError() } != ERROR_INSUFFICIENT_BUFFER || needed == 0 {
        return Err(std::io::Error::last_os_error()).context("GetTokenInformation size query failed");
    }
    let elem_size = std::mem::size_of::<TOKEN_USER>();
    let count = (needed as usize).div_ceil(elem_size).max(1);
    let mut buf = vec![MaybeUninit::<TOKEN_USER>::uninit(); count];
    let buf_bytes = u32::try_from(count * elem_size).context("TOKEN_USER buffer overflow")?;
    let mut returned = 0u32;
    let ok = unsafe {
        GetTokenInformation(
            raw_token,
            TokenUser,
            buf.as_mut_ptr().cast(),
            buf_bytes,
            &mut returned,
        )
    };
    if ok == 0 {
        return Err(std::io::Error::last_os_error()).context("GetTokenInformation(TokenUser) failed");
    }
    anyhow::ensure!(
        (returned as usize) >= elem_size,
        "GetTokenInformation(TokenUser) returned truncated buffer ({returned} bytes)"
    );
    Ok(buf)
}

#[cfg(windows)]
fn set_path_owner_only_windows(path: &Path) -> anyhow::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Foundation::{ERROR_SUCCESS, GENERIC_ALL, LocalFree};
    use windows_sys::Win32::Security::Authorization::{
        EXPLICIT_ACCESS_W, GRANT_ACCESS, NO_MULTIPLE_TRUSTEE, SE_FILE_OBJECT, SetEntriesInAclW,
        SetNamedSecurityInfoW, TRUSTEE_IS_SID, TRUSTEE_IS_USER, TRUSTEE_W,
    };
    use windows_sys::Win32::Security::{
        ACL, DACL_SECURITY_INFORMATION, NO_INHERITANCE, PROTECTED_DACL_SECURITY_INFORMATION,
    };

    struct LocalFreeAcl(*mut ACL);
    impl Drop for LocalFreeAcl {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe {
                    LocalFree(self.0.cast());
                }
            }
        }
    }

    let token_buf = current_user_token_buf()?;
    let token_user = unsafe { token_buf[0].assume_init_ref() };
    let user_sid = token_user.User.Sid;

    let ea = EXPLICIT_ACCESS_W {
        grfAccessPermissions: GENERIC_ALL,
        grfAccessMode: GRANT_ACCESS,
        grfInheritance: NO_INHERITANCE,
        Trustee: TRUSTEE_W {
            pMultipleTrustee: std::ptr::null_mut(),
            MultipleTrusteeOperation: NO_MULTIPLE_TRUSTEE,
            TrusteeForm: TRUSTEE_IS_SID,
            TrusteeType: TRUSTEE_IS_USER,
            ptstrName: user_sid.cast(),
        },
    };

    let mut new_dacl: *mut ACL = std::ptr::null_mut();
    let err = unsafe { SetEntriesInAclW(1, &ea, std::ptr::null(), &mut new_dacl) };
    if err != ERROR_SUCCESS {
        return Err(std::io::Error::from_raw_os_error(err as i32))
            .context("SetEntriesInAclW failed");
    }
    let _acl_guard = LocalFreeAcl(new_dacl);

    let wide: Vec<u16> = path
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    let err = unsafe {
        SetNamedSecurityInfoW(
            wide.as_ptr(),
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            new_dacl.cast_const(),
            std::ptr::null(),
        )
    };
    if err != ERROR_SUCCESS {
        return Err(std::io::Error::from_raw_os_error(err as i32))
            .with_context(|| format!("SetNamedSecurityInfoW failed for {}", path.display()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::VmPreset;

    #[test]
    fn host_and_vm_persistence_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store = RegistryStore::new(dir.path()).unwrap();

        let token1 = store.load_or_create_token().unwrap();
        let token2 = store.load_or_create_token().unwrap();
        assert_eq!(token1, token2);
        assert_eq!(token1.len(), 64);

        let host_cfg = store.load_or_init_host_config().unwrap();
        assert_eq!(host_cfg.data_dir, dir.path());

        let vm_dir = store.vm_dir(Uuid::new_v4());
        let vm = VmConfig::from_preset("Win11-Test", VmPreset::Windows11, &vm_dir);
        store.save_vm(&vm).unwrap();

        // Even if a user disk file happens to reside inside vm_dir, deleting
        // VM metadata with delete_vm_dir must preserve that disk file.
        let disk_in_vm_dir = store.vm_dir(vm.id).join("disk0.img");
        fs::write(&disk_in_vm_dir, b"PRESERVE_ME").unwrap();

        let loaded = store.load_all_vms().unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, vm.id);
        assert_eq!(loaded[0].name, "Win11-Test");

        store.delete_vm_dir(vm.id).unwrap();
        assert!(store.load_all_vms().unwrap().is_empty());
        assert!(disk_in_vm_dir.exists());
        assert!(!store.vm_config_path(vm.id).exists());
    }

    #[test]
    fn daemon_lock_prevents_duplicate_acquisition() {
        let dir = tempfile::tempdir().unwrap();
        let store = RegistryStore::new(dir.path()).unwrap();
        assert!(!store.is_daemon_locked());

        let lock = store.try_acquire_daemon_lock().unwrap();
        assert!(store.is_daemon_locked());
        let err = store.try_acquire_daemon_lock().unwrap_err();
        assert!(err.to_string().contains("already running"), "{err:#}");

        drop(lock);
        assert!(!store.is_daemon_locked());
        let _reacquired = store.try_acquire_daemon_lock().unwrap();
    }

    #[test]
    fn corrupt_vm_toml_is_skipped_on_load() {
        let dir = tempfile::tempdir().unwrap();
        let store = RegistryStore::new(dir.path()).unwrap();

        let good_dir = store.vm_dir(Uuid::new_v4());
        let good = VmConfig::from_preset("Valid-VM", VmPreset::LinuxDesktop, &good_dir);
        store.save_vm(&good).unwrap();

        let bad_id = Uuid::new_v4();
        let bad_dir = store.vm_dir(bad_id);
        fs::create_dir_all(&bad_dir).unwrap();
        fs::write(bad_dir.join("vm.toml"), "not = [valid toml").unwrap();

        let loaded = store.load_all_vms().unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, good.id);
    }

    #[cfg(unix)]
    #[test]
    fn token_is_0600_and_existing_data_dir_permissions_are_preserved() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        fs::set_permissions(dir.path(), fs::Permissions::from_mode(0o755)).unwrap();

        let store = RegistryStore::new(dir.path()).unwrap();
        let mode = fs::metadata(dir.path()).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o755, "pre-existing --data-dir permissions must not be overwritten");

        let _ = store.load_or_create_token().unwrap();
        let tok_mode = fs::metadata(store.token_path()).unwrap().permissions().mode() & 0o777;
        assert_eq!(tok_mode, 0o600);
    }

    #[cfg(windows)]
    #[test]
    fn token_has_protected_owner_only_dacl_on_windows() {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Foundation::{ERROR_SUCCESS, LocalFree};
        use windows_sys::Win32::Security::Authorization::{GetNamedSecurityInfoW, SE_FILE_OBJECT};
        use windows_sys::Win32::Security::{
            ACCESS_ALLOWED_ACE, ACL, ACL_SIZE_INFORMATION, AclSizeInformation,
            DACL_SECURITY_INFORMATION, EqualSid, GetAce, GetAclInformation,
            GetSecurityDescriptorControl, PSECURITY_DESCRIPTOR, SE_DACL_PROTECTED,
        };

        let dir = tempfile::tempdir().unwrap();
        let store = RegistryStore::new(dir.path()).unwrap();
        let _ = store.load_or_create_token().unwrap();

        let wide: Vec<u16> = store
            .token_path()
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect();
        let mut dacl: *mut ACL = std::ptr::null_mut();
        let mut sd: PSECURITY_DESCRIPTOR = std::ptr::null_mut();
        let err = unsafe {
            GetNamedSecurityInfoW(
                wide.as_ptr(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut dacl,
                std::ptr::null_mut(),
                &mut sd,
            )
        };
        assert_eq!(err, ERROR_SUCCESS);
        struct LocalFreeSd(PSECURITY_DESCRIPTOR);
        impl Drop for LocalFreeSd {
            fn drop(&mut self) {
                if !self.0.is_null() {
                    unsafe {
                        LocalFree(self.0);
                    }
                }
            }
        }
        let _sd_guard = LocalFreeSd(sd);

        let mut control = 0u16;
        let mut rev = 0u32;
        assert_ne!(unsafe { GetSecurityDescriptorControl(sd, &mut control, &mut rev) }, 0);
        assert_ne!(control & SE_DACL_PROTECTED, 0, "daemon.token DACL must be protected from inheritance");

        let mut size_info = ACL_SIZE_INFORMATION {
            AceCount: 0,
            AclBytesInUse: 0,
            AclBytesFree: 0,
        };
        assert_ne!(
            unsafe {
                GetAclInformation(
                    dacl,
                    (&mut size_info as *mut ACL_SIZE_INFORMATION).cast(),
                    std::mem::size_of::<ACL_SIZE_INFORMATION>() as u32,
                    AclSizeInformation,
                )
            },
            0
        );
        assert_eq!(size_info.AceCount, 1, "expected exactly 1 ACE on daemon.token");

        let mut ace_ptr: *mut core::ffi::c_void = std::ptr::null_mut();
        assert_ne!(unsafe { GetAce(dacl, 0, &mut ace_ptr) }, 0);
        let ace = unsafe { &*(ace_ptr.cast::<ACCESS_ALLOWED_ACE>()) };
        let ace_sid = (&raw const ace.SidStart).cast::<core::ffi::c_void>().cast_mut();

        let token_buf = current_user_token_buf().unwrap();
        let token_user = unsafe { token_buf[0].assume_init_ref() };
        assert_ne!(unsafe { EqualSid(ace_sid, token_user.User.Sid) }, 0);
    }

    #[test]
    fn is_daemon_locked_never_blocks_acquire() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };

        let dir = tempfile::tempdir().unwrap();
        let store = RegistryStore::new(dir.path()).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let mut readers = Vec::new();
        for _ in 0..4 {
            let store = store.clone();
            let stop = Arc::clone(&stop);
            readers.push(std::thread::spawn(move || {
                while !stop.load(Ordering::Acquire) {
                    let _ = store.is_daemon_locked();
                }
            }));
        }

        for _ in 0..200 {
            let lock = store
                .try_acquire_daemon_lock()
                .expect("is_daemon_locked probe blocked try_acquire_daemon_lock");
            assert!(store.is_daemon_locked());
            drop(lock);
        }

        stop.store(true, Ordering::Release);
        for r in readers {
            r.join().unwrap();
        }
    }
}
