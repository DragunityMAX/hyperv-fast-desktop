// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Worker IDs and mesh-serializable launch parameters for `fastdesk_worker`.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use mesh::MeshPayload;
use mesh_worker::WorkerId;
use std::net::TcpListener;

/// Default maximum number of concurrent viewer sessions when `max_clients == 0`.
pub const DEFAULT_MAX_CLIENTS: usize = 16;

/// Launch parameters for the event-driven FastDesk display/input worker.
#[derive(MeshPayload)]
pub struct FastDeskParameters<T> {
    /// Socket listener for viewer connections (TCP or `VmListener`).
    pub listener: T,
    /// Shared framebuffer view used to inspect display format and read dirty pixels.
    pub framebuffer: framebuffer::FramebufferAccess,
    /// Optional path to the host shared-memory file backing VRAM for zero-copy local viewers.
    pub shm_path: Option<String>,
    /// Channel used to forward keyboard and mouse events into the guest.
    pub input_send: mesh::Sender<input_core::InputData>,
    /// Channel receiving dirty rectangle hints from `SynthVideo`.
    pub dirty_recv: Option<mesh::Receiver<Vec<video_core::DirtyRect>>>,
    /// Channel receiving hardware cursor shape and position updates from `SynthVideo`.
    pub cursor_recv: Option<mesh::Receiver<video_core::CursorUpdate>>,
    /// Maximum number of concurrent viewer sessions.
    pub max_clients: usize,
}

/// Worker ID for the TCP-listening FastDesk worker.
pub const FASTDESK_WORKER_TCP: WorkerId<FastDeskParameters<TcpListener>> =
    WorkerId::new("FastDeskWorkerTcp");

/// Worker ID for the `AF_HYPERV` / `AF_VSOCK` FastDesk worker.
#[cfg(any(windows, target_os = "linux"))]
pub const FASTDESK_WORKER_VMSOCKET: WorkerId<FastDeskParameters<vmsocket::VmListener>> =
    WorkerId::new("FastDeskWorkerVmSocket");
