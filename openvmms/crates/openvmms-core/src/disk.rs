use crate::models::CreateDiskPayload;
use crate::models::DiskFormat;
use crate::models::DiskInfo;
use anyhow::Context;
use anyhow::bail;
use std::fs;
use std::io::Read;
#[cfg(target_os = "linux")]
use std::io::Seek;
#[cfg(target_os = "linux")]
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::process::Stdio;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

/// Returns `true` if `path` starts with the 8-byte VHDX container signature (`b"vhdxfile"`).
pub fn file_has_vhdx_signature(path: &Path) -> bool {
    let Ok(mut file) = fs::File::open(path) else {
        return false;
    };
    let mut magic = [0u8; 8];
    file.read_exact(&mut magic).is_ok() && &magic == b"vhdxfile"
}

/// Validates that `path` has a file name and does not collide with another disk format's
/// reserved extension.
pub fn validate_disk_path_for_format(path: &Path, format: DiskFormat) -> anyhow::Result<()> {
    if path.as_os_str().is_empty() || path.file_name().is_none() {
        bail!("disk path must specify a file name");
    }
    let lower = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase());
    match format {
        DiskFormat::Iso => bail!("cannot create an ISO file; specify Raw or Vhdx format"),
        DiskFormat::Vhd => bail!("creating VHD1 disks is not supported; specify Raw or Vhdx format"),
        DiskFormat::Vhdx => {
            if lower.as_deref() != Some("vhdx") {
                bail!(
                    "disk path {} is incompatible with vhdx format: VHDX disks must use the .vhdx extension",
                    path.display(),
                );
            }
        }
        DiskFormat::Raw => {
            if matches!(
                lower.as_deref(),
                Some("vhdx" | "avhdx" | "vhd" | "avhd" | "vmdk" | "vmgs" | "iso")
            ) {
                bail!(
                    "disk path {} is incompatible with raw format: extension .{} is reserved for another disk format (use e.g. .img or .raw)",
                    path.display(),
                    lower.as_deref().unwrap_or_default()
                );
            }
        }
    }
    Ok(())
}

/// Adjusts mismatched disk extensions so GUI defaults match the requested disk format.
pub fn sync_disk_path_extension(path: &mut PathBuf, format: DiskFormat) {
    if path.as_os_str().is_empty() || path.file_name().is_none() {
        return;
    }
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase());
    let raw_ext = path.extension().and_then(|e| e.to_str());
    match format {
        DiskFormat::Raw if validate_disk_path_for_format(path, format).is_ok() => return,
        DiskFormat::Vhdx if raw_ext == Some("vhdx") => return,
        DiskFormat::Vhd if raw_ext == Some("vhd") => return,
        _ => {}
    }
    match (format, ext.as_deref()) {
        (DiskFormat::Raw, _) => {
            path.set_extension("img");
        }
        (
            DiskFormat::Vhdx,
            None | Some("vhdx" | "vhd" | "img" | "raw" | "iso" | "vmdk" | "vmgs"),
        ) => {
            path.set_extension("vhdx");
        }
        (DiskFormat::Vhdx, Some(_)) => {
            // Preserve unrelated dotted name segments (e.g. `win.v2`) and append.
            path.as_mut_os_string().push(".vhdx");
        }
        (DiskFormat::Vhd, _) => {
            path.set_extension("vhd");
        }
        (DiskFormat::Iso, _) => {}
    }
}

/// Removes a disk file this operation created. Callers must only invoke this on
/// paths they reserved with `create_new`, so another writer's file is never removed.
fn remove_owned_disk(path: &Path) {
    let _ = fs::remove_file(path);
}

pub fn create_disk(
    req: &CreateDiskPayload,
    openvmm_img_bin: Option<&Path>,
) -> anyhow::Result<DiskInfo> {
    create_disk_cancellable(req, openvmm_img_bin, &AtomicBool::new(false))
}

pub fn create_disk_cancellable(
    req: &CreateDiskPayload,
    openvmm_img_bin: Option<&Path>,
    cancelled: &AtomicBool,
) -> anyhow::Result<DiskInfo> {
    if cancelled.load(Ordering::Acquire) {
        bail!("disk creation cancelled");
    }
    if req.size_bytes < 512 || req.size_bytes % 512 != 0 {
        bail!("disk size must be at least 512 bytes and a multiple of 512 bytes");
    }
    validate_disk_path_for_format(&req.path, req.format)?;
    let vhdx_img_bin = if req.format == DiskFormat::Vhdx {
        Some(openvmm_img_bin.context(
            "openvmm-img binary not found; configure openvmm_img_bin in host settings or create a raw disk",
        )?)
    } else {
        None
    };
    if let Some(parent) = req.path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("creating disk parent directory {}", parent.display()))?;
    }

    // Atomically reserve the path. Exactly one concurrent creator can succeed here, and
    // from this point on the file belongs to this operation, so failure cleanup is safe.
    let mut file = match fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&req.path)
    {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            bail!("disk file already exists: {}", req.path.display());
        }
        Err(err) => {
            return Err(err).with_context(|| format!("creating disk {}", req.path.display()));
        }
    };

    match vhdx_img_bin {
        None => {
            let sized = if req.fixed {
                allocate_fixed_raw(&mut file, req.size_bytes, cancelled)
                    .with_context(|| format!("allocating fixed raw disk {}", req.path.display()))
            } else {
                file.set_len(req.size_bytes)
                    .with_context(|| format!("sizing raw disk {}", req.path.display()))
            };
            drop(file);
            let res = sized.and_then(|()| inspect_disk(&req.path, openvmm_img_bin));
            if res.is_err() {
                remove_owned_disk(&req.path);
            }
            res
        }
        Some(img_bin) => {
            drop(file);
            let mut cmd = Command::new(img_bin);
            // `--force` overwrites only the placeholder this operation reserved above.
            cmd.arg("create")
                .arg(&req.path)
                .arg("--format")
                .arg("vhdx")
                .arg("--size")
                .arg(req.size_bytes.to_string())
                .arg("--force")
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            if req.fixed {
                cmd.arg("--format-options").arg("allocation=fixed");
            }
            let mut child = match cmd.spawn() {
                Ok(child) => child,
                Err(err) => {
                    remove_owned_disk(&req.path);
                    return Err(err).with_context(|| format!("running {}", img_bin.display()));
                }
            };
            let stdout_handle = child.stdout.take().map(|mut pipe| {
                std::thread::spawn(move || {
                    let mut buf = Vec::new();
                    let _ = pipe.read_to_end(&mut buf);
                    buf
                })
            });
            let stderr_handle = child.stderr.take().map(|mut pipe| {
                std::thread::spawn(move || {
                    let mut buf = Vec::new();
                    let _ = pipe.read_to_end(&mut buf);
                    buf
                })
            });

            let status = loop {
                if cancelled.load(Ordering::Acquire) {
                    let _ = child.kill();
                    let _ = child.wait();
                    if let Some(h) = stdout_handle {
                        let _ = h.join();
                    }
                    if let Some(h) = stderr_handle {
                        let _ = h.join();
                    }
                    remove_owned_disk(&req.path);
                    bail!("disk creation cancelled");
                }
                match child.try_wait() {
                    Ok(Some(status)) => break status,
                    Ok(None) => std::thread::sleep(Duration::from_millis(50)),
                    Err(err) => {
                        let _ = child.kill();
                        let _ = child.wait();
                        if let Some(h) = stdout_handle {
                            let _ = h.join();
                        }
                        if let Some(h) = stderr_handle {
                            let _ = h.join();
                        }
                        remove_owned_disk(&req.path);
                        return Err(err).with_context(|| format!("waiting for {}", img_bin.display()));
                    }
                }
            };
            let stdout_bytes = stdout_handle
                .and_then(|h| h.join().ok())
                .unwrap_or_default();
            let stderr_bytes = stderr_handle
                .and_then(|h| h.join().ok())
                .unwrap_or_default();
            if !status.success() {
                remove_owned_disk(&req.path);
                let stderr = String::from_utf8_lossy(&stderr_bytes);
                let stdout = String::from_utf8_lossy(&stdout_bytes);
                bail!(
                    "openvmm-img create failed ({status}): {}{}",
                    stderr.trim(),
                    if stdout.trim().is_empty() {
                        String::new()
                    } else {
                        format!("\n{}", stdout.trim())
                    }
                );
            }
            let res = inspect_disk(&req.path, Some(img_bin));
            if res.is_err() {
                remove_owned_disk(&req.path);
            }
            res
        }
    }
}

fn allocate_fixed_raw(
    file: &mut fs::File,
    size_bytes: u64,
    cancelled: &AtomicBool,
) -> anyhow::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        const FALLOC_CHUNK: u64 = 64 * 1024 * 1024;
        if let Ok(total_i64) = libc::off_t::try_from(size_bytes) {
            let mut offset = 0u64;
            let mut fallback = false;
            while offset < size_bytes {
                if cancelled.load(Ordering::Acquire) {
                    bail!("disk creation cancelled");
                }
                let len = (size_bytes - offset).min(FALLOC_CHUNK) as libc::off_t;
                let rc = unsafe { libc::fallocate(file.as_raw_fd(), 0, offset as libc::off_t, len) };
                if rc == 0 {
                    offset += len as u64;
                    continue;
                }
                let err = std::io::Error::last_os_error();
                if err.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                if offset == 0
                    && matches!(
                        err.raw_os_error(),
                        Some(libc::EOPNOTSUPP | libc::ENOSYS | libc::EINVAL)
                    )
                {
                    fallback = true;
                    break;
                }
                return Err(err.into());
            }
            if !fallback {
                let _ = total_i64;
                file.sync_all()?;
                return Ok(());
            }
            file.seek(SeekFrom::Start(0))?;
        }
    }
    write_fixed_raw_zeros(file, size_bytes, cancelled)
}

fn write_fixed_raw_zeros(
    file: &mut fs::File,
    size_bytes: u64,
    cancelled: &AtomicBool,
) -> anyhow::Result<()> {
    let chunk = vec![0u8; 1024 * 1024];
    let mut written = 0u64;
    while written < size_bytes {
        if cancelled.load(Ordering::Acquire) {
            bail!("disk creation cancelled");
        }
        let n = (size_bytes - written).min(chunk.len() as u64) as usize;
        file.write_all(&chunk[..n])?;
        written += n as u64;
    }
    file.sync_all()?;
    Ok(())
}

pub fn inspect_disk(path: &Path, openvmm_img_bin: Option<&Path>) -> anyhow::Result<DiskInfo> {
    let meta = fs::metadata(path)
        .with_context(|| format!("reading disk metadata for {}", path.display()))?;
    if !meta.is_file() {
        bail!("not a regular file: {}", path.display());
    }
    let file_size_bytes = meta.len();
    let read_only = meta.permissions().readonly();
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase());

    let is_vhdx = ext.as_deref() == Some("vhdx");
    if is_vhdx {
        let img_bin = openvmm_img_bin.with_context(|| {
            format!(
                "cannot inspect VHDX metadata for {}: openvmm-img binary not found; configure openvmm_img_bin in host settings",
                path.display()
            )
        })?;
        let output = Command::new(img_bin)
            .arg("info")
            .arg(path)
            .arg("--json")
            .output()
            .with_context(|| format!("running {}", img_bin.display()))?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            bail!(
                "openvmm-img info failed ({}): {}{}",
                output.status,
                stderr.trim(),
                if stdout.trim().is_empty() {
                    String::new()
                } else {
                    format!("\n{}", stdout.trim())
                }
            );
        }
        let val: serde_json::Value = serde_json::from_slice(&output.stdout)
            .context("parsing openvmm-img info JSON output")?;
        let virtual_size_bytes = val["disk_size"]
            .as_u64()
            .context("missing disk_size in openvmm-img info JSON output")?;
        let disk_type = val["type"].as_str().unwrap_or("vhdx").to_string();
        let block_size_bytes = val["block_size"].as_u64().and_then(|v| u32::try_from(v).ok());
        let sector_size_bytes = val["logical_sector_size"]
            .as_u64()
            .and_then(|v| u32::try_from(v).ok());
        let parent_path = val["relative_path"]
            .as_str()
            .or_else(|| val["absolute_win32_path"].as_str())
            .map(ToOwned::to_owned);
        return Ok(DiskInfo {
            path: path.to_path_buf(),
            format: "vhdx".to_string(),
            disk_type,
            virtual_size_bytes,
            file_size_bytes,
            block_size_bytes,
            sector_size_bytes,
            parent_path,
            read_only,
        });
    }

    let (format, disk_type) = match ext.as_deref() {
        Some("iso") => ("iso".to_string(), "read-only image".to_string()),
        Some("vhd") => ("vhd".to_string(), "vhd".to_string()),
        _ => ("raw".to_string(), "sparse/flat".to_string()),
    };

    Ok(DiskInfo {
        path: path.to_path_buf(),
        format,
        disk_type,
        virtual_size_bytes: file_size_bytes,
        file_size_bytes,
        block_size_bytes: None,
        sector_size_bytes: Some(512),
        parent_path: None,
        read_only,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_inspect_raw_disk() {
        let dir = tempfile::tempdir().unwrap();
        let disk_path = dir.path().join("test.img");
        let info = create_disk(
            &CreateDiskPayload {
                path: disk_path.clone(),
                size_bytes: 16 * 1024 * 1024,
                format: DiskFormat::Raw,
                fixed: false,
            },
            None,
        )
        .unwrap();
        assert_eq!(info.format, "raw");
        assert_eq!(info.virtual_size_bytes, 16 * 1024 * 1024);
        assert!(disk_path.exists());
    }

    #[test]
    fn create_disk_rejects_format_extension_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        for (name, format) in [
            ("disk.vhdx", DiskFormat::Raw),
            ("disk.VHDX", DiskFormat::Raw),
            ("disk.vhd", DiskFormat::Raw),
            ("disk.vmdk", DiskFormat::Raw),
            ("disk.vmgs", DiskFormat::Raw),
            ("disk.iso", DiskFormat::Raw),
            ("data.img", DiskFormat::Vhdx),
            ("disk.vhd", DiskFormat::Vhdx),
            ("disk.vmdk", DiskFormat::Vhdx),
            ("disk.vmgs", DiskFormat::Vhdx),
            ("disk.iso", DiskFormat::Vhdx),
        ] {
            let path = dir.path().join(name);
            let err = create_disk(
                &CreateDiskPayload {
                    path: path.clone(),
                    size_bytes: 16 * 1024 * 1024,
                    format,
                    fixed: false,
                },
                Some(Path::new("/nonexistent/openvmm-img")),
            )
            .unwrap_err();
            assert!(
                err.to_string().contains("incompatible with"),
                "{name}: {err:#}"
            );
            assert!(!path.exists(), "{name} must not be created");
        }

        let vhd_err = create_disk(
            &CreateDiskPayload {
                path: dir.path().join("legacy.vhd"),
                size_bytes: 16 * 1024 * 1024,
                format: DiskFormat::Vhd,
                fixed: false,
            },
            Some(Path::new("/nonexistent/openvmm-img")),
        )
        .unwrap_err();
        assert!(
            vhd_err.to_string().contains("VHD1"),
            "{vhd_err:#}"
        );

        let empty_err = create_disk(
            &CreateDiskPayload {
                path: PathBuf::new(),
                size_bytes: 16 * 1024 * 1024,
                format: DiskFormat::Raw,
                fixed: false,
            },
            Some(Path::new("/nonexistent/openvmm-img")),
        )
        .unwrap_err();
        assert!(
            empty_err.to_string().contains("must specify a file name"),
            "{empty_err:#}"
        );
    }

    #[test]
    fn failed_create_never_removes_another_writers_file() {
        let dir = tempfile::tempdir().unwrap();
        for (name, format) in [("other.vhdx", DiskFormat::Vhdx), ("other.img", DiskFormat::Raw)] {
            let path = dir.path().join(name);
            fs::write(&path, b"owned by another writer").unwrap();
            let err = create_disk(
                &CreateDiskPayload {
                    path: path.clone(),
                    size_bytes: 16 * 1024 * 1024,
                    format,
                    fixed: false,
                },
                // A binary that always fails, so any cleanup path would be exercised.
                Some(Path::new("/nonexistent/openvmm-img")),
            )
            .unwrap_err();
            assert!(err.to_string().contains("already exists"), "{err:#}");
            assert_eq!(fs::read(&path).unwrap(), b"owned by another writer");
        }
    }

    #[test]
    fn failed_vhdx_tool_removes_only_reserved_placeholder() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mine.vhdx");
        create_disk(
            &CreateDiskPayload {
                path: path.clone(),
                size_bytes: 16 * 1024 * 1024,
                format: DiskFormat::Vhdx,
                fixed: false,
            },
            Some(Path::new("/nonexistent/openvmm-img")),
        )
        .unwrap_err();
        assert!(!path.exists());
    }

    #[test]
    fn sync_extension_matches_inferred_format() {
        let cases = [
            ("a.vhdx", DiskFormat::Raw, "a.img"),
            ("a.VHDX", DiskFormat::Raw, "a.img"),
            ("a.vmdk", DiskFormat::Raw, "a.img"),
            ("a.vmgs", DiskFormat::Raw, "a.img"),
            ("a.iso", DiskFormat::Raw, "a.img"),
            ("a.img", DiskFormat::Raw, "a.img"),
            ("a", DiskFormat::Raw, "a"),
            ("a.img", DiskFormat::Vhdx, "a.vhdx"),
            ("a", DiskFormat::Vhdx, "a.vhdx"),
            ("win.v2", DiskFormat::Vhdx, "win.v2.vhdx"),
            ("a.vhd", DiskFormat::Vhdx, "a.vhdx"),
            ("a.VHDX", DiskFormat::Vhdx, "a.vhdx"),
        ];
        for (input, format, expected) in cases {
            let mut p = PathBuf::from(input);
            sync_disk_path_extension(&mut p, format);
            assert_eq!(p, PathBuf::from(expected), "{input} as {format:?}");
            assert_eq!(DiskFormat::from_path(&p), format);
            validate_disk_path_for_format(&p, format).unwrap();
        }
    }

    #[test]
    fn concurrent_create_disk_preserves_winners_file() {
        use std::sync::Arc;
        use std::sync::Barrier;

        let dir = tempfile::tempdir().unwrap();
        let raw_path = dir.path().join("race.img");
        let size_bytes = 16 * 1024 * 1024;
        let barrier = Arc::new(Barrier::new(8));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let path = raw_path.clone();
                let b = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    b.wait();
                    create_disk(
                        &CreateDiskPayload {
                            path,
                            size_bytes,
                            format: DiskFormat::Raw,
                            fixed: false,
                        },
                        None,
                    )
                })
            })
            .collect();

        let mut ok_count = 0usize;
        let mut err_count = 0usize;
        for h in handles {
            match h.join().unwrap() {
                Ok(_) => ok_count += 1,
                Err(err) => {
                    assert!(err.to_string().contains("already exists"), "{err:#}");
                    err_count += 1;
                }
            }
        }
        assert_eq!(ok_count, 1);
        assert_eq!(err_count, 7);
        assert_eq!(fs::metadata(&raw_path).unwrap().len(), size_bytes);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let stub_bin = dir.path().join("stub-openvmm-img.sh");
            fs::write(
                &stub_bin,
                "#!/bin/sh\nset -eu\ncmd=\"$1\"\npath=\"$2\"\nif [ \"$cmd\" = \"create\" ]; then\n  sleep 0.05\n  printf \"VHDX_WINNER_PAYLOAD\" > \"$path\"\nelif [ \"$cmd\" = \"info\" ]; then\n  printf '{\"disk_size\":16777216,\"type\":\"dynamic\",\"block_size\":33554432,\"logical_sector_size\":512}\\n'\nelse\n  exit 1\nfi\n",
            )
            .unwrap();
            fs::set_permissions(&stub_bin, fs::Permissions::from_mode(0o755)).unwrap();

            let vhdx_path = dir.path().join("race.vhdx");
            let vhdx_barrier = Arc::new(Barrier::new(8));
            let vhdx_handles: Vec<_> = (0..8)
                .map(|_| {
                    let path = vhdx_path.clone();
                    let bin = stub_bin.clone();
                    let b = Arc::clone(&vhdx_barrier);
                    std::thread::spawn(move || {
                        b.wait();
                        create_disk(
                            &CreateDiskPayload {
                                path,
                                size_bytes,
                                format: DiskFormat::Vhdx,
                                fixed: false,
                            },
                            Some(&bin),
                        )
                    })
                })
                .collect();

            let mut vhdx_ok = 0usize;
            let mut vhdx_err = 0usize;
            for h in vhdx_handles {
                match h.join().unwrap() {
                    Ok(_) => vhdx_ok += 1,
                    Err(err) => {
                        assert!(err.to_string().contains("already exists"), "{err:#}");
                        vhdx_err += 1;
                    }
                }
            }
            assert_eq!(vhdx_ok, 1);
            assert_eq!(vhdx_err, 7);
            assert_eq!(fs::read(&vhdx_path).unwrap(), b"VHDX_WINNER_PAYLOAD");

            // Case-insensitive .VHDX succeeds as VHDX, .vhd classifies as Vhd, and .img
            // starting with `vhdxfile` bytes stays Raw (never format-sniffed).
            let vhdx_stub = dir.path().join("vhdx-stub-openvmm-img.sh");
            fs::write(
                &vhdx_stub,
                "#!/bin/sh\nset -eu\ncmd=\"$1\"\npath=\"$2\"\nif [ \"$cmd\" = \"create\" ]; then\n  printf \"vhdxfileDUMMY\" > \"$path\"\nelif [ \"$cmd\" = \"info\" ]; then\n  printf '{\"disk_size\":16777216,\"type\":\"dynamic\",\"block_size\":33554432,\"logical_sector_size\":512}\\n'\nelse\n  exit 1\nfi\n",
            )
            .unwrap();
            fs::set_permissions(&vhdx_stub, fs::Permissions::from_mode(0o755)).unwrap();

            let upper_vhdx = dir.path().join("Disk.VHDX");
            let created = create_disk(
                &CreateDiskPayload {
                    path: upper_vhdx.clone(),
                    size_bytes,
                    format: DiskFormat::Vhdx,
                    fixed: false,
                },
                Some(&vhdx_stub),
            )
            .unwrap();
            assert_eq!(created.format, "vhdx");
            assert_eq!(created.virtual_size_bytes, size_bytes);
            assert_eq!(DiskFormat::from_path(&upper_vhdx), DiskFormat::Vhdx);

            assert_eq!(DiskFormat::from_path(Path::new("legacy.vhd")), DiskFormat::Vhd);

            let raw_with_magic = dir.path().join("raw-with-magic.img");
            fs::write(&raw_with_magic, b"vhdxfileDUMMY").unwrap();
            assert_eq!(DiskFormat::from_path(&raw_with_magic), DiskFormat::Raw);
            let inspected = inspect_disk(&raw_with_magic, Some(&vhdx_stub)).unwrap();
            assert_eq!(inspected.format, "raw");
        }
    }

    #[test]
    fn inspect_disk_vhdx_returns_error_when_tool_missing_or_fails() {
        let dir = tempfile::tempdir().unwrap();
        let vhdx = dir.path().join("locked.vhdx");
        fs::write(&vhdx, b"SMALL_HEADER").unwrap();

        let no_bin_err = inspect_disk(&vhdx, None).unwrap_err();
        assert!(
            no_bin_err.to_string().contains("openvmm-img binary not found"),
            "{no_bin_err:#}"
        );

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let failing_stub = dir.path().join("fail-img.sh");
            fs::write(
                &failing_stub,
                "#!/bin/sh\necho 'sharing violation' >&2\nexit 1\n",
            )
            .unwrap();
            fs::set_permissions(&failing_stub, fs::Permissions::from_mode(0o755)).unwrap();

            let err = inspect_disk(&vhdx, Some(&failing_stub)).unwrap_err();
            assert!(
                err.to_string().contains("openvmm-img info failed"),
                "{err:#}"
            );
            assert!(err.to_string().contains("sharing violation"), "{err:#}");
        }
    }

    #[test]
    fn fixed_raw_disk_allocates_blocks_and_cancellation_cleans_placeholder() {
        let dir = tempfile::tempdir().unwrap();
        let fixed_path = dir.path().join("fixed.img");
        let sparse_path = dir.path().join("sparse.img");
        let size = 4 * 1024 * 1024;

        let fixed_info = create_disk(
            &CreateDiskPayload {
                path: fixed_path.clone(),
                size_bytes: size,
                format: DiskFormat::Raw,
                fixed: true,
            },
            None,
        )
        .unwrap();
        assert_eq!(fixed_info.virtual_size_bytes, size);
        assert_eq!(fs::metadata(&fixed_path).unwrap().len(), size);
        let mut head = [1u8; 4096];
        fs::File::open(&fixed_path)
            .unwrap()
            .read_exact(&mut head)
            .unwrap();
        assert!(head.iter().all(|&b| b == 0), "fixed raw disk must read back as zeros");

        let _sparse_info = create_disk(
            &CreateDiskPayload {
                path: sparse_path.clone(),
                size_bytes: size,
                format: DiskFormat::Raw,
                fixed: false,
            },
            None,
        )
        .unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let fixed_blocks = fs::metadata(&fixed_path).unwrap().blocks();
            let sparse_blocks = fs::metadata(&sparse_path).unwrap().blocks();
            assert!(
                fixed_blocks * 512 >= size,
                "expected fixed raw disk to allocate >= {size} bytes, got {} bytes",
                fixed_blocks * 512
            );
            assert_eq!(sparse_blocks, 0, "expected sparse raw disk to have 0 allocated blocks");
        }

        // Pre-cancelled raw creation must fail and leave no file behind.
        let cancelled_raw = dir.path().join("cancelled.img");
        let flag = AtomicBool::new(true);
        let err = create_disk_cancellable(
            &CreateDiskPayload {
                path: cancelled_raw.clone(),
                size_bytes: size,
                format: DiskFormat::Raw,
                fixed: true,
            },
            None,
            &flag,
        )
        .unwrap_err();
        assert!(err.to_string().contains("cancelled"), "{err:#}");
        assert!(!cancelled_raw.exists());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            use std::sync::Arc;
            let slow_stub = dir.path().join("slow-img.sh");
            let started_flag = dir.path().join("slow_started");
            fs::write(
                &slow_stub,
                format!(
                    "#!/bin/sh\ntouch '{}'\nsleep 5\n",
                    started_flag.display()
                ),
            )
            .unwrap();
            fs::set_permissions(&slow_stub, fs::Permissions::from_mode(0o755)).unwrap();

            let vhdx_cancel_path = dir.path().join("cancelled.vhdx");
            let cancel_flag = Arc::new(AtomicBool::new(false));
            let cancel_flag_worker = Arc::clone(&cancel_flag);
            let p = vhdx_cancel_path.clone();
            let handle = std::thread::spawn(move || {
                create_disk_cancellable(
                    &CreateDiskPayload {
                        path: p,
                        size_bytes: size,
                        format: DiskFormat::Vhdx,
                        fixed: true,
                    },
                    Some(&slow_stub),
                    &cancel_flag_worker,
                )
            });
            let deadline = std::time::Instant::now() + Duration::from_secs(3);
            while !started_flag.exists() && std::time::Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(20));
            }
            cancel_flag.store(true, Ordering::Release);
            let res = handle.join().unwrap();
            assert!(res.is_err());
            assert!(!vhdx_cancel_path.exists());
        }
    }

    #[test]
    fn create_and_inspect_real_vhdx_disk_when_binary_present() {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let repo_root = manifest_dir.join("../../..");
        let img_name = if cfg!(windows) {
            "openvmm-img.exe"
        } else {
            "openvmm-img"
        };
        let img_bin = [
            repo_root.join("openvmm/target/release").join(img_name),
            repo_root.join("openvmm/target/debug").join(img_name),
        ]
        .into_iter()
        .find(|p| p.is_file());

        let Some(img_bin) = img_bin else {
            if std::env::var_os("OPENVMM_REQUIRE_REAL_BINARY").is_some() {
                panic!(
                    "OPENVMM_REQUIRE_REAL_BINARY is set, but real {img_name} binary was not found under {}",
                    repo_root.display()
                );
            }
            eprintln!("skipping create_and_inspect_real_vhdx_disk: {img_name} not built yet");
            return;
        };

        let dir = tempfile::tempdir().unwrap();
        let dyn_path = dir.path().join("dynamic.vhdx");
        let dyn_info = create_disk(
            &CreateDiskPayload {
                path: dyn_path.clone(),
                size_bytes: 16 * 1024 * 1024,
                format: DiskFormat::Vhdx,
                fixed: false,
            },
            Some(&img_bin),
        )
        .unwrap();
        assert_eq!(dyn_info.format, "vhdx");
        assert_eq!(dyn_info.virtual_size_bytes, 16 * 1024 * 1024);
        assert!(file_has_vhdx_signature(&dyn_path));

        let fixed_path = dir.path().join("fixed.vhdx");
        let fixed_info = create_disk(
            &CreateDiskPayload {
                path: fixed_path.clone(),
                size_bytes: 8 * 1024 * 1024,
                format: DiskFormat::Vhdx,
                fixed: true,
            },
            Some(&img_bin),
        )
        .unwrap();
        assert_eq!(fixed_info.format, "vhdx");
        assert_eq!(fixed_info.virtual_size_bytes, 8 * 1024 * 1024);
        assert!(fixed_info.file_size_bytes >= 8 * 1024 * 1024);
        assert!(file_has_vhdx_signature(&fixed_path));
    }
}
