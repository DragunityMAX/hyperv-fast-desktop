//! Process identity verification and safe termination helpers for adopted
//! `openvmm` worker processes.
//!
//! Storing only a numeric PID in `<vm>.pid` is vulnerable to PID reuse after a
//! daemon restart or crash. These helpers pair the PID with the kernel's
//! process start timestamp and hold an OS handle (`pidfd` on Linux, `HANDLE` on
//! Windows) opened *before* checking identity, so termination signals always
//! target the verified process instance.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(target_os = "linux")]
use std::os::unix::io::{AsRawFd, FromRawFd, OwnedFd, RawFd};

#[cfg(windows)]
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};

/// Live handle to a previously spawned `openvmm` process whose start timestamp
/// and executable identity have already been verified.
#[derive(Debug)]
pub struct AdoptedProcess {
    pid: u32,
    #[cfg(unix)]
    expected_start: Option<u64>,
    #[cfg(unix)]
    expected_bin: Option<PathBuf>,
    #[cfg(target_os = "linux")]
    pidfd: Option<OwnedFd>,
    #[cfg(windows)]
    handle: OwnedHandle,
}

impl AdoptedProcess {
    /// Returns the OS process ID of the adopted process.
    pub fn pid(&self) -> u32 {
        self.pid
    }

    /// Returns `true` if this exact process instance is still alive.
    pub fn is_alive(&self) -> bool {
        #[cfg(target_os = "linux")]
        {
            if let Some(fd) = &self.pidfd {
                let mut pfd = libc::pollfd {
                    fd: fd.as_raw_fd(),
                    events: libc::POLLIN,
                    revents: 0,
                };
                // A pidfd becomes readable once the process exits and waits to
                // be reaped (or has already been reaped).
                let rc = unsafe { libc::poll(&mut pfd, 1, 0) };
                if rc == 0 {
                    return true;
                }
                if rc > 0 && (pfd.revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR)) != 0 {
                    return false;
                }
            }
            if let Some(expected) = self.expected_start {
                return process_start_token(self.pid) == Some(expected);
            }
            is_pid_alive_raw(self.pid)
        }

        #[cfg(windows)]
        {
            use windows_sys::Win32::Foundation::{HANDLE, WAIT_TIMEOUT};
            use windows_sys::Win32::System::Threading::WaitForSingleObject;
            let rc = unsafe { WaitForSingleObject(self.handle.as_raw_handle() as HANDLE, 0) };
            rc == WAIT_TIMEOUT
        }

        #[cfg(all(unix, not(target_os = "linux")))]
        {
            is_pid_alive_raw(self.pid)
        }
    }

    /// Gracefully asks the verified process to exit (`SIGTERM` on Unix) and
    /// escalates to forced termination (`SIGKILL` / `TerminateProcess`) if it
    /// does not exit within a short grace window.
    pub fn terminate(&self) {
        #[cfg(target_os = "linux")]
        {
            if let Some(fd) = &self.pidfd {
                let rc = unsafe {
                    libc::syscall(
                        libc::SYS_pidfd_send_signal,
                        fd.as_raw_fd(),
                        libc::SIGTERM,
                        std::ptr::null::<libc::siginfo_t>(),
                        0 as libc::c_uint,
                    )
                };
                if rc == 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::ENOSYS) {
                    for _ in 0..10 {
                        if !self.is_alive() {
                            return;
                        }
                        std::thread::sleep(std::time::Duration::from_millis(50));
                    }
                    let _ = unsafe {
                        libc::syscall(
                            libc::SYS_pidfd_send_signal,
                            fd.as_raw_fd(),
                            libc::SIGKILL,
                            std::ptr::null::<libc::siginfo_t>(),
                            0 as libc::c_uint,
                        )
                    };
                    return;
                }
            }

            // Fallback when pidfd_open / pidfd_send_signal are unavailable:
            // re-verify right before each signal.
            if !self.matches_current_identity() {
                return;
            }
            unsafe {
                libc::kill(self.pid as libc::pid_t, libc::SIGTERM);
            }
            for _ in 0..10 {
                if !self.is_alive() {
                    return;
                }
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            if self.matches_current_identity() {
                unsafe {
                    libc::kill(self.pid as libc::pid_t, libc::SIGKILL);
                }
            }
        }

        #[cfg(windows)]
        {
            use windows_sys::Win32::Foundation::HANDLE;
            use windows_sys::Win32::System::Threading::{TerminateProcess, WaitForSingleObject};
            let handle = self.handle.as_raw_handle() as HANDLE;
            let _ = unsafe { TerminateProcess(handle, 1) };
            let _ = unsafe { WaitForSingleObject(handle, 2_000) };
        }

        #[cfg(all(unix, not(target_os = "linux")))]
        {
            if !self.matches_current_identity() {
                return;
            }
            unsafe {
                libc::kill(self.pid as libc::pid_t, libc::SIGTERM);
            }
            for _ in 0..10 {
                if !is_pid_alive_raw(self.pid) {
                    return;
                }
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            if self.matches_current_identity() {
                unsafe {
                    libc::kill(self.pid as libc::pid_t, libc::SIGKILL);
                }
            }
        }
    }

    #[cfg(unix)]
    fn matches_current_identity(&self) -> bool {
        if let Some(expected) = self.expected_start
            && process_start_token(self.pid) != Some(expected)
        {
            return false;
        }
        verify_linux_or_unix_identity(self.pid, self.expected_bin.as_deref())
    }
}

/// Writes `<pid>\n<start_token>` (or `<pid>` if no start token can be queried)
/// to `path`, creating/replacing the file with owner-only permissions on Unix.
pub fn write_pid_file(path: &Path, pid: u32) -> std::io::Result<()> {
    let content = match process_start_token(pid) {
        Some(tok) => format!("{pid}\n{tok}"),
        None => pid.to_string(),
    };

    #[cfg(unix)]
    {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let mut f = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)?;
        f.write_all(content.as_bytes())
    }
    #[cfg(not(unix))]
    {
        fs::write(path, content)
    }
}

/// Parses `<pid>[\n<start_token>]` from a `.pid` file.
pub fn read_pid_file(pid_path: &Path) -> Option<(u32, Option<u64>)> {
    let raw = fs::read_to_string(pid_path).ok()?;
    let mut lines = raw.lines();
    let pid = lines.next()?.trim().parse::<u32>().ok()?;
    if pid == 0 {
        return None;
    }
    let start_token = match lines.next() {
        Some(line) if !line.trim().is_empty() => Some(line.trim().parse::<u64>().ok()?),
        _ => None,
    };
    Some((pid, start_token))
}

/// Returns the kernel start timestamp token for `pid`, used to detect PID reuse.
pub fn process_start_token(pid: u32) -> Option<u64> {
    if pid == 0 {
        return None;
    }

    #[cfg(target_os = "linux")]
    {
        let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        let rparen = stat.rfind(')')?;
        let after = stat.get(rparen + 1..)?;
        // Field 3 (state) is index 0 after ')'; field 22 (starttime) is index 19.
        after.split_whitespace().nth(19)?.parse::<u64>().ok()
    }

    #[cfg(windows)]
    {
        use windows_sys::Win32::Foundation::{CloseHandle, FILETIME};
        use windows_sys::Win32::System::Threading::{
            GetProcessTimes, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
        };
        let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
        if handle.is_null() {
            return None;
        }
        let mut creation = FILETIME {
            dwLowDateTime: 0,
            dwHighDateTime: 0,
        };
        let mut exit = creation;
        let mut kernel = creation;
        let mut user = creation;
        let ok = unsafe {
            GetProcessTimes(handle, &mut creation, &mut exit, &mut kernel, &mut user) != 0
        };
        unsafe { CloseHandle(handle) };
        if !ok {
            return None;
        }
        Some(((creation.dwHighDateTime as u64) << 32) | (creation.dwLowDateTime as u64))
    }

    #[cfg(all(unix, not(target_os = "linux")))]
    {
        let _ = pid;
        None
    }
}

/// Opens an OS handle to the PID stored in `pid_path`, verifies its start token
/// and executable identity, and returns an `AdoptedProcess` handle.
///
/// The OS handle is opened *before* checking the start time and image name so
/// PID reuse in between check and action cannot redirect signals to an unrelated
/// process. Returns `None` if the pid file is missing/malformed, the process
/// has exited, the start token does not match, or the process is not `openvmm`
/// (or `expected_bin`).
pub fn open_verified_pid(pid_path: &Path, expected_bin: Option<&Path>) -> Option<Arc<AdoptedProcess>> {
    let (pid, expected_start) = read_pid_file(pid_path)?;
    if pid <= 1 {
        return None;
    }

    #[cfg(target_os = "linux")]
    {
        // Open pidfd BEFORE validating /proc so any subsequent signal targets
        // this exact kernel task, even if the PID wraps later.
        let pidfd = open_pidfd(pid);
        if let Some(expected) = expected_start
            && process_start_token(pid) != Some(expected)
        {
            return None;
        }
        if !verify_linux_or_unix_identity(pid, expected_bin) {
            return None;
        }
        if pidfd.is_none() && !is_pid_alive_raw(pid) {
            return None;
        }
        Some(Arc::new(AdoptedProcess {
            pid,
            expected_start,
            expected_bin: expected_bin.map(Path::to_path_buf),
            pidfd,
        }))
    }

    #[cfg(windows)]
    {
        use windows_sys::Win32::Foundation::{FILETIME, HANDLE, WAIT_TIMEOUT};
        use windows_sys::Win32::System::Threading::{
            GetProcessTimes, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE,
            PROCESS_TERMINATE, QueryFullProcessImageNameW, WaitForSingleObject,
        };

        let raw = unsafe {
            OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_TERMINATE | PROCESS_SYNCHRONIZE,
                0,
                pid,
            )
        };
        if raw.is_null() {
            return None;
        }
        let handle = unsafe { OwnedHandle::from_raw_handle(raw as std::os::windows::io::RawHandle) };
        let h = handle.as_raw_handle() as HANDLE;

        if unsafe { WaitForSingleObject(h, 0) } != WAIT_TIMEOUT {
            return None;
        }

        if let Some(expected) = expected_start {
            let mut creation = FILETIME {
                dwLowDateTime: 0,
                dwHighDateTime: 0,
            };
            let mut exit = creation;
            let mut kernel = creation;
            let mut user = creation;
            if unsafe { GetProcessTimes(h, &mut creation, &mut exit, &mut kernel, &mut user) == 0 } {
                return None;
            }
            let actual = ((creation.dwHighDateTime as u64) << 32) | (creation.dwLowDateTime as u64);
            if actual != expected {
                return None;
            }
        }

        let mut buf = vec![0u16; 32768];
        let mut len = buf.len() as u32;
        if unsafe { QueryFullProcessImageNameW(h, 0, buf.as_mut_ptr(), &mut len) == 0 } {
            return None;
        }
        let exe_path = PathBuf::from(String::from_utf16_lossy(&buf[..len as usize]));
        if !windows_image_matches(&exe_path, expected_bin) {
            return None;
        }

        Some(Arc::new(AdoptedProcess {
            pid,
            handle,
        }))
    }

    #[cfg(all(unix, not(target_os = "linux")))]
    {
        if !is_pid_alive_raw(pid) {
            return None;
        }
        if !verify_linux_or_unix_identity(pid, expected_bin) {
            return None;
        }
        Some(Arc::new(AdoptedProcess {
            pid,
            expected_start,
            expected_bin: expected_bin.map(Path::to_path_buf),
        }))
    }
}

#[cfg(target_os = "linux")]
fn open_pidfd(pid: u32) -> Option<OwnedFd> {
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid as libc::pid_t, 0 as libc::c_uint) };
    if fd >= 0 {
        Some(unsafe { OwnedFd::from_raw_fd(fd as RawFd) })
    } else {
        None
    }
}

#[cfg(unix)]
fn is_pid_alive_raw(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    let rc = unsafe { libc::kill(pid as libc::pid_t, 0) };
    if rc == 0 {
        return true;
    }
    let err = std::io::Error::last_os_error().raw_os_error();
    err == Some(libc::EPERM)
}

#[cfg(unix)]
fn verify_linux_or_unix_identity(pid: u32, expected_bin: Option<&Path>) -> bool {
    #[cfg(target_os = "linux")]
    {
        let exe_path = match fs::read_link(format!("/proc/{pid}/exe")) {
            Ok(p) => p,
            Err(_) => return false,
        };
        let exe_str = exe_path.to_string_lossy();
        let clean_exe_str = exe_str.strip_suffix(" (deleted)").unwrap_or(&exe_str);
        let exe_name = Path::new(clean_exe_str)
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");

        if exe_name == "openvmm" || exe_name == "openvmm.exe" {
            return true;
        }

        if let Some(exp) = expected_bin
            && let Some(exp_name) = exp.file_name().and_then(|n| n.to_str())
            && !exp_name.is_empty()
        {
            let cmdline_bytes = fs::read(format!("/proc/{pid}/cmdline")).unwrap_or_default();
            let args: Vec<String> = cmdline_bytes
                .split(|&b| b == 0)
                .filter(|s| !s.is_empty())
                .map(|s| String::from_utf8_lossy(s).into_owned())
                .collect();
            let has_rpc_flag = args.iter().any(|a| a == "--rpc");
            if exe_name == exp_name && (exp_name == "openvmm" || has_rpc_flag) {
                return true;
            }
            if has_rpc_flag
                && args
                    .iter()
                    .any(|a| Path::new(a).file_name().and_then(|n| n.to_str()) == Some(exp_name))
            {
                return true;
            }
        }
        false
    }

    #[cfg(all(unix, not(target_os = "linux")))]
    {
        let _ = (pid, expected_bin);
        false
    }
}

#[cfg(windows)]
fn windows_image_matches(exe_path: &Path, expected_bin: Option<&Path>) -> bool {
    let Some(exe_name) = exe_path.file_name().and_then(|n| n.to_str()) else {
        return false;
    };
    if exe_name.eq_ignore_ascii_case("openvmm.exe") || exe_name.eq_ignore_ascii_case("openvmm") {
        return true;
    }
    if let Some(exp) = expected_bin
        && let Some(exp_name) = exp.file_name().and_then(|n| n.to_str())
        && !exp_name.is_empty()
        && exe_name.eq_ignore_ascii_case(exp_name)
    {
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_verified_pid_rejects_mismatched_start_time_and_non_openvmm_process() {
        let dir = tempfile::tempdir().unwrap();
        let pid_path = dir.path().join("vm.pid");
        let self_pid = std::process::id();

        let real_start = process_start_token(self_pid);
        let wrong_start = real_start.unwrap_or(100).wrapping_add(1);
        fs::write(&pid_path, format!("{self_pid}\n{wrong_start}")).unwrap();

        // Mismatched start token must be rejected even if the binary name matched.
        assert!(open_verified_pid(&pid_path, Some(Path::new("openvmm"))).is_none());

        // Matching start token on a non-openvmm live PID (the unit test runner
        // itself, which does not pass `--rpc`) must also be rejected without
        // sending any signal or killing it.
        write_pid_file(&pid_path, self_pid).unwrap();
        assert!(open_verified_pid(&pid_path, Some(Path::new("openvmm"))).is_none());
        assert!(open_verified_pid(&pid_path, None).is_none());

        // Garbage or dead PID is rejected.
        fs::write(&pid_path, "not-a-pid").unwrap();
        assert!(open_verified_pid(&pid_path, None).is_none());
    }
}
