# Vendored OpenVMM Patches

`openvmm/` vendors a fork of [`microsoft/openvmm`](https://github.com/microsoft/openvmm) starting from upstream commit `c4419be15ab702194f0a25c294bd2f6187804824`. Changes made inside vendored upstream crates are tracked here as named patches so they can be audited, preserved, or dropped cleanly during upstream syncs.

## Patch Inventory

### `0001-mesh-rpc-cancel-pending-rpcs-on-client-disconnect.patch`
- **Target:** `openvmm/support/mesh/mesh_rpc/src/server.rs`
- **Summary:** Cancels the connection `CancelContext` and skips writing responses in `mesh_rpc::Server::serve_connection` when the ttrpc client socket disconnects. On Linux, read EOF alone (such as a client half-close via `shutdown(SHUT_WR)`) keeps the connection open until `send_task` finishes draining in-flight replies, cancelling only if `EPOLLHUP`/`EPOLLERR` or a read/write error occurs. (On non-Linux targets, read EOF cancels pending RPCs immediately.)
- **Behavioral impact:** Affects every ttrpc service hosted by `mesh_rpc::Server`—any client that disconnects before receiving a reply cancels its pending RPCs on that connection via `CancelContext` and drops their responses. In particular, if `openvmmsd` restarts or disconnects while a `SaveVm`, `RestoreVm`, or `ShutdownVm` RPC is running, that operation is now cancelled instead of finishing.
- **Why carried:** Required for `openvmmsd` daemon restarts without stopping VMs (`WaitVM` holds a single per-VM waiter slot in `VmService`, so closing `openvmmsd`'s socket must release `WaitVM` for the next daemon to re-adopt the VM cleanly).

### `0002-disk-writable-image-locking-and-vhdx-open.patch`
- **Targets:** `openvmm/openvmm/openvmm_helpers/src/disk.rs`, `openvmm/vm/devices/storage/disklayer_vhdx/src/chain.rs`
- **Summary:** Adds `open_vhdx_disk` (to open VHDX files regardless of extension) and `open_raw_disk_file` (for raw images without format sniffing), and acquires exclusive writable disk locks (`flock` via `File::try_lock()` on Unix, and `FILE_SHARE_READ` without `FILE_SHARE_WRITE` via `OpenOptionsExt::share_mode(0x0000_0001)` on Windows) when opening writable raw/VHD1/VMGS/VHDX disk files.
- **Behavioral impact:** Concurrent writable opens of the same disk image fail cleanly with `"already locked for writing by another process"` on both Unix and Windows instead of silently allowing two writers to corrupt the image.
- **Why carried:** Prevents concurrent disk corruption when attaching or starting multiple VMs against the same writable disk image, and supports extension-agnostic VHDX opens for non-`.vhdx` images (e.g. `.avhdx`) and explicit raw opens for `.img` files that happen to contain a `vhdxfile` header signature.

### `0003-vmservice-ttrpc-fastdesk-dvd-reset-tpm-extensions.patch`
- **Targets:**
  - `openvmm/openvmm/openvmm_ttrpc_vmservice/src/vmservice.proto`
  - `openvmm/openvmm/openvmm_entry/src/lib.rs`
  - `openvmm/openvmm/openvmm_entry/src/ttrpc/mod.rs`
  - `openvmm/workers/fastdesk_worker/src/lib.rs`
  - `openvmm/workers/fastdesk_worker_defs/src/lib.rs`
- **Summary:** Adds `FastDeskConfig` (loopback TCP or loopback `HV_GUID_LOOPBACK` / `VMADDR_CID_LOCAL` vsock binding in ttrpc, while preserving wildcard `vsock_any` for the standalone `--fastdesk-vsock-port` CLI flag), `PropertiesVMResponse.fastdesk_listen_addr` / `fastdesk_shm_path`, `ResetVM`, `SCSIDisk.is_dvd`, `DevicesConfig.enable_scsi_controller`, `VMConfig.tpm_enabled`, and Windows `DioBackend` auto-port creation via `new_switch_port`. Explicit `SCSI_DISK_TYPE_VHDX` uses `open_vhdx_disk`, while `SCSI_DISK_TYPE_PHYSICAL` preserves `open_disk_type` extension dispatch.
- **Why carried:** Exposes FastDesk, DVD/ISO hot-plug, reset, vTPM 2.0, and Hyper-V switch auto-port control over `vmservice.VM` ttrpc for OpenVMMS (`openvmmsd`).

## Verifying and Re-applying

From the repository root:

```bash
# Confirm all patches match the current vendored tree:
git apply -R --check patches/0001-mesh-rpc-cancel-pending-rpcs-on-client-disconnect.patch
git apply -R --check patches/0002-disk-writable-image-locking-and-vhdx-open.patch
git apply -R --check patches/0003-vmservice-ttrpc-fastdesk-dvd-reset-tpm-extensions.patch

# Apply to a clean upstream openvmm checkout (stripping the leading `openvmm/` path segment):
cd openvmm && for p in ../patches/*.patch; do git apply -p2 "$p"; done
```
