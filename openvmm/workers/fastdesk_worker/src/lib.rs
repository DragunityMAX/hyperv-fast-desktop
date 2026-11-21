// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! Event-driven low-latency FastDesk display and input worker.
//!
//! Replaces VNC's 30ms polling timer with immediate wakeup on `SYNTHVID_DIRT`
//! and hardware cursor updates, supporting lossless LZ4 dirty-tile streaming
//! over TCP/`AF_HYPERV`/`AF_VSOCK` as well as zero-copy shared memory mode.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use anyhow::Context;
use anyhow::anyhow;
use fastdesk_proto::BTN_WHEEL_DOWN;
use fastdesk_proto::BTN_WHEEL_UP;
use fastdesk_proto::CAP_ALL;
use fastdesk_proto::ClientMsg;
use fastdesk_proto::DirtyTileTracker;
use fastdesk_proto::EncodedRect;
use fastdesk_proto::RectEncoding;
use fastdesk_proto::ServerMsg;
use fastdesk_proto::TileRect;
use fastdesk_proto::decode_client_msg;
use fastdesk_proto::encode_bgra_rect;
use fastdesk_proto::encode_server_msg;
use fastdesk_worker_defs::FASTDESK_WORKER_TCP;
#[cfg(any(windows, target_os = "linux"))]
use fastdesk_worker_defs::FASTDESK_WORKER_VMSOCKET;
pub use fastdesk_worker_defs::DEFAULT_MAX_CLIENTS;
use fastdesk_worker_defs::FastDeskParameters;
use futures::AsyncReadExt;
use futures::AsyncWriteExt;
use futures::FutureExt;
use futures::StreamExt;
use input_core::InputData;
use input_core::KeyboardData;
use input_core::MouseData;
use mesh::message::MeshField;
use mesh_worker::Worker;
use mesh_worker::WorkerId;
use mesh_worker::WorkerRpc;
use pal_async::local::LocalDriver;
use pal_async::local::block_with_io;
use pal_async::socket::Listener;
use pal_async::socket::PolledSocket;
use pal_async::timer::PolledTimer;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::future::Future;
use std::net::TcpListener;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;
use tracing_helpers::AnyhowValueExt;
use video_core::CursorPosition;
use video_core::CursorShape;
use video_core::CursorUpdate;
use video_core::DirtyRect;

/// Event-driven FastDesk display/input worker supporting TCP and `vmsocket`.
pub struct FastDeskWorker<T: Listener> {
    listener: T,
    view: ViewWrapper,
    shm_path: Option<String>,
    input_send: mesh::Sender<InputData>,
    dirty_recv: Option<mesh::Receiver<Vec<DirtyRect>>>,
    cursor_recv: Option<mesh::Receiver<CursorUpdate>>,
    max_clients: usize,
}

impl Worker for FastDeskWorker<TcpListener> {
    type Parameters = FastDeskParameters<TcpListener>;
    type State = FastDeskParameters<TcpListener>;
    const ID: WorkerId<Self::Parameters> = FASTDESK_WORKER_TCP;

    fn new(params: Self::Parameters) -> anyhow::Result<Self> {
        Self::new_inner(params)
    }

    fn restart(state: Self::State) -> anyhow::Result<Self> {
        Self::new(state)
    }

    fn run(self, rpc_recv: mesh::Receiver<WorkerRpc<Self::State>>) -> anyhow::Result<()> {
        self.run_inner(rpc_recv)
    }
}

#[cfg(any(windows, target_os = "linux"))]
impl Worker for FastDeskWorker<vmsocket::VmListener> {
    type Parameters = FastDeskParameters<vmsocket::VmListener>;
    type State = FastDeskParameters<vmsocket::VmListener>;
    const ID: WorkerId<Self::Parameters> = FASTDESK_WORKER_VMSOCKET;

    fn new(params: Self::Parameters) -> anyhow::Result<Self> {
        Self::new_inner(params)
    }

    fn restart(state: Self::State) -> anyhow::Result<Self> {
        Self::new(state)
    }

    fn run(self, rpc_recv: mesh::Receiver<WorkerRpc<Self::State>>) -> anyhow::Result<()> {
        self.run_inner(rpc_recv)
    }
}

impl<T: 'static + Listener + MeshField + Send> FastDeskWorker<T> {
    fn new_inner(params: FastDeskParameters<T>) -> anyhow::Result<Self> {
        let max_clients = if params.max_clients == 0 {
            DEFAULT_MAX_CLIENTS
        } else {
            params.max_clients
        };
        Ok(Self {
            listener: params.listener,
            view: ViewWrapper(
                params
                    .framebuffer
                    .view()
                    .context("failed to map framebuffer")?,
            ),
            shm_path: params.shm_path,
            input_send: params.input_send,
            dirty_recv: params.dirty_recv,
            cursor_recv: params.cursor_recv,
            max_clients,
        })
    }

    fn run_inner(
        self,
        mut rpc_recv: mesh::Receiver<WorkerRpc<FastDeskParameters<T>>>,
    ) -> anyhow::Result<()> {
        block_with_io(async |driver| {
            tracing::info!(
                address = ?self.listener.local_addr().unwrap(),
                shm_path = ?self.shm_path,
                "FastDesk server listening"
            );

            let listener = PolledSocket::new(&driver, self.listener)?;
            let mut server = MultiClientServer::new(
                listener,
                self.view,
                self.shm_path,
                self.input_send,
                self.dirty_recv,
                self.cursor_recv,
                self.max_clients,
            );

            let rpc = loop {
                let r = futures::select! {
                    r = rpc_recv.recv().fuse() => r,
                    r = server.process(&driver).fuse() => break r.map(|_| None)?,
                };
                match r {
                    Ok(message) => match message {
                        WorkerRpc::Stop => break None,
                        WorkerRpc::Inspect(deferred) => deferred.inspect(&server),
                        WorkerRpc::Restart(response) => break Some(response),
                    },
                    Err(_) => break None,
                }
            };
            if let Some(rpc) = rpc {
                server.abort_all_clients().await;
                let mut view = Arc::try_unwrap(server.view)
                    .expect("all clients terminated")
                    .into_inner();
                if let Some(format_recv) = server.format_recv.take() {
                    view.0.restore_format_recv(format_recv);
                }
                let state = FastDeskParameters {
                    listener: server.listener.into_inner(),
                    framebuffer: view.0.access(),
                    shm_path: server.shm_path,
                    input_send: server.input_send,
                    dirty_recv: server.dirty_recv,
                    cursor_recv: server.cursor_recv,
                    max_clients: server.max_clients,
                };
                rpc.complete(Ok(state));
            }
            Ok(())
        })
    }
}

#[derive(Debug)]
struct ViewWrapper(framebuffer::View);

#[derive(Default)]
struct SharedCursorState {
    shape_seq: u64,
    shape: Option<Arc<CursorShape>>,
    pos_seq: u64,
    pos: Option<CursorPosition>,
}

const MAX_HELD_KEYS_PER_CLIENT: usize = 512;

#[derive(Default)]
struct SharedInputState {
    key_holders: HashMap<u16, usize>,
    client_buttons: HashMap<u64, u8>,
}

impl SharedInputState {
    fn combined_buttons(&self) -> u8 {
        self.client_buttons
            .values()
            .copied()
            .fold(0u8, |acc, mask| acc | mask)
    }
}

struct ClientInputTracker {
    client_id: u64,
    input_send: mesh::Sender<InputData>,
    shared: Arc<Mutex<SharedInputState>>,
    held_keys: Vec<u16>,
    held_buttons: u8,
    last_pos: (u16, u16),
}

impl ClientInputTracker {
    fn new(
        client_id: u64,
        input_send: mesh::Sender<InputData>,
        shared: Arc<Mutex<SharedInputState>>,
    ) -> Self {
        Self {
            client_id,
            input_send,
            shared,
            held_keys: Vec::new(),
            held_buttons: 0,
            last_pos: (0, 0),
        }
    }

    fn handle_key(&mut self, scancode: u16, make: bool) {
        if make {
            if self.held_keys.contains(&scancode) {
                self.input_send
                    .send(InputData::Keyboard(KeyboardData { code: scancode, make: true }));
                return;
            }
            if self.held_keys.len() >= MAX_HELD_KEYS_PER_CLIENT {
                tracing::warn!(
                    client_id = self.client_id,
                    scancode,
                    "ignoring key make exceeding held key limit"
                );
                return;
            }
            self.held_keys.push(scancode);
            {
                let mut shared = self.shared.lock();
                *shared.key_holders.entry(scancode).or_insert(0) += 1;
            }
            self.input_send
                .send(InputData::Keyboard(KeyboardData { code: scancode, make: true }));
        } else {
            let should_send = {
                let mut shared = self.shared.lock();
                if let Some(pos) = self.held_keys.iter().position(|&k| k == scancode) {
                    self.held_keys.swap_remove(pos);
                    match shared.key_holders.get_mut(&scancode) {
                        Some(count) if *count > 1 => {
                            *count -= 1;
                            false
                        }
                        Some(_) => {
                            shared.key_holders.remove(&scancode);
                            true
                        }
                        None => true,
                    }
                } else {
                    !shared.key_holders.contains_key(&scancode)
                }
            };
            if should_send {
                self.input_send
                    .send(InputData::Keyboard(KeyboardData { code: scancode, make: false }));
            }
        }
    }

    fn handle_pointer(&mut self, button_mask: u8, x: u16, y: u16, wheel_delta: i16) {
        let btns = button_mask & 0x07;
        self.held_buttons = btns;
        self.last_pos = (x, y);
        let effective_btns = {
            let mut shared = self.shared.lock();
            if btns != 0 {
                shared.client_buttons.insert(self.client_id, btns);
            } else {
                shared.client_buttons.remove(&self.client_id);
            }
            shared.combined_buttons()
        };

        self.input_send.send(InputData::Mouse(MouseData {
            button_mask: effective_btns,
            x,
            y,
        }));
        if wheel_delta != 0 {
            let steps = wheel_delta.clamp(-8, 8);
            let wheel_bit = if steps > 0 {
                BTN_WHEEL_UP
            } else {
                BTN_WHEEL_DOWN
            };
            for _ in 0..steps.unsigned_abs() {
                self.input_send.send(InputData::Mouse(MouseData {
                    button_mask: effective_btns | wheel_bit,
                    x,
                    y,
                }));
            }
            self.input_send.send(InputData::Mouse(MouseData {
                button_mask: effective_btns,
                x,
                y,
            }));
        }
    }

    fn cleanup(&mut self) {
        if self.held_keys.is_empty() && self.held_buttons == 0 {
            return;
        }
        let mut keys_to_release = Vec::new();
        let mut mouse_release = None;
        {
            let mut shared = self.shared.lock();
            for scancode in self.held_keys.drain(..).rev() {
                match shared.key_holders.get_mut(&scancode) {
                    Some(count) if *count > 1 => {
                        *count -= 1;
                    }
                    Some(_) => {
                        shared.key_holders.remove(&scancode);
                        keys_to_release.push(scancode);
                    }
                    None => {
                        keys_to_release.push(scancode);
                    }
                }
            }
            if self.held_buttons != 0 {
                self.held_buttons = 0;
                let before = shared.combined_buttons();
                shared.client_buttons.remove(&self.client_id);
                let after = shared.combined_buttons();
                if after != before {
                    mouse_release = Some((after, self.last_pos));
                }
            }
        }
        for code in keys_to_release {
            self.input_send
                .send(InputData::Keyboard(KeyboardData { code, make: false }));
        }
        if let Some((button_mask, (x, y))) = mouse_release {
            self.input_send
                .send(InputData::Mouse(MouseData { button_mask, x, y }));
        }
    }
}

impl Drop for ClientInputTracker {
    fn drop(&mut self) {
        self.cleanup();
    }
}

struct ClientHandle {
    id: u64,
    dirty_send: async_channel::Sender<Arc<Vec<DirtyRect>>>,
    wakeup_send: async_channel::Sender<()>,
    missed_dirty: Arc<AtomicBool>,
    _abort_send: mesh::OneshotSender<()>,
}

struct MultiClientServer<T: Listener> {
    listener: PolledSocket<T>,
    view: Arc<Mutex<ViewWrapper>>,
    format_recv: Option<mesh::Receiver<video_core::FramebufferFormat>>,
    shm_path: Option<String>,
    input_send: mesh::Sender<InputData>,
    input_state: Arc<Mutex<SharedInputState>>,
    dirty_recv: Option<mesh::Receiver<Vec<DirtyRect>>>,
    cursor_recv: Option<mesh::Receiver<CursorUpdate>>,
    cursor_state: Arc<Mutex<SharedCursorState>>,
    device_dirty_seen: Arc<AtomicBool>,
    client_handles: Vec<ClientHandle>,
    clients: unicycle::FuturesUnordered<Pin<Box<dyn Future<Output = u64>>>>,
    next_client_id: u64,
    max_clients: usize,
    start_time: Instant,
}

impl<T: Listener> MultiClientServer<T> {
    fn new(
        listener: PolledSocket<T>,
        mut view: ViewWrapper,
        shm_path: Option<String>,
        input_send: mesh::Sender<InputData>,
        dirty_recv: Option<mesh::Receiver<Vec<DirtyRect>>>,
        cursor_recv: Option<mesh::Receiver<CursorUpdate>>,
        max_clients: usize,
    ) -> Self {
        let format_recv = view.0.take_format_recv();
        let vram_size = view.0.len();
        let base_offset = view.0.file_offset();
        match view.0.format() {
            Some(fmt) if validate_display_format(&fmt, vram_size, base_offset).is_some() => {}
            Some(fmt) => {
                tracing::warn!(
                    width = fmt.width,
                    height = fmt.height,
                    bytes_per_line = fmt.bytes_per_line,
                    offset = fmt.offset,
                    vram_size,
                    "ignoring invalid initial framebuffer format"
                );
                view.0
                    .set_cached_format(fallback_framebuffer_format(vram_size));
            }
            None => {
                view.0
                    .set_cached_format(fallback_framebuffer_format(vram_size));
            }
        }
        let max_clients = if max_clients == 0 {
            DEFAULT_MAX_CLIENTS
        } else {
            max_clients
        };
        Self {
            listener,
            view: Arc::new(Mutex::new(view)),
            format_recv,
            shm_path,
            input_send,
            input_state: Arc::new(Mutex::new(SharedInputState::default())),
            dirty_recv,
            cursor_recv,
            cursor_state: Arc::new(Mutex::new(SharedCursorState::default())),
            device_dirty_seen: Arc::new(AtomicBool::new(false)),
            client_handles: Vec::new(),
            clients: unicycle::FuturesUnordered::new(),
            next_client_id: 0,
            max_clients,
            start_time: Instant::now(),
        }
    }

    fn reset_dirty_reporting(&mut self) {
        self.device_dirty_seen.store(false, Ordering::Release);
        for c in &self.client_handles {
            c.missed_dirty.store(true, Ordering::Release);
            let _ = c.wakeup_send.try_send(());
        }
    }

    fn apply_format_change(&mut self, format: video_core::FramebufferFormat) {
        {
            let mut guard = self.view.lock();
            let vram_size = guard.0.len();
            let base_offset = guard.0.file_offset();
            if validate_display_format(&format, vram_size, base_offset).is_none() {
                tracing::warn!(
                    width = format.width,
                    height = format.height,
                    bytes_per_line = format.bytes_per_line,
                    offset = format.offset,
                    vram_size,
                    "ignoring invalid framebuffer format"
                );
                return;
            }
            guard.0.set_cached_format(format);
        }
        if let Some(recv) = &mut self.dirty_recv {
            loop {
                match recv.try_recv() {
                    Ok(_) => {}
                    Err(mesh::TryRecvError::Empty) => break,
                    Err(mesh::TryRecvError::Closed | mesh::TryRecvError::Error(_)) => {
                        self.dirty_recv = None;
                        break;
                    }
                }
            }
        }
        self.reset_dirty_reporting();
    }

    fn drain_format_updates(&mut self) -> bool {
        let mut latest_valid = None;
        let mut channel_closed = false;
        if let Some(recv) = &mut self.format_recv {
            let guard = self.view.lock();
            let vram_size = guard.0.len();
            let base_offset = guard.0.file_offset();
            loop {
                match recv.try_recv() {
                    Ok(fmt) => {
                        if validate_display_format(&fmt, vram_size, base_offset).is_some() {
                            latest_valid = Some(fmt);
                        } else {
                            tracing::warn!(
                                width = fmt.width,
                                height = fmt.height,
                                bytes_per_line = fmt.bytes_per_line,
                                offset = fmt.offset,
                                vram_size,
                                "ignoring invalid framebuffer format"
                            );
                        }
                    }
                    Err(mesh::TryRecvError::Empty) => break,
                    Err(mesh::TryRecvError::Closed | mesh::TryRecvError::Error(_)) => {
                        channel_closed = true;
                        break;
                    }
                }
            }
        }
        if channel_closed {
            self.format_recv = None;
        }
        if let Some(fmt) = latest_valid {
            self.apply_format_change(fmt);
            true
        } else if channel_closed {
            self.reset_dirty_reporting();
            true
        } else {
            false
        }
    }

    async fn process(&mut self, driver: &LocalDriver) -> anyhow::Result<()> {
        enum Event<A> {
            Accepted(A),
            ClientDone(u64),
            DirtyRects(Vec<DirtyRect>),
            Cursor(CursorUpdate),
            FormatChanged(video_core::FramebufferFormat),
            FallbackTick,
        }

        let mut timer = PolledTimer::new(driver);

        loop {
            let listener = &mut self.listener;
            let clients = &mut self.clients;
            let dirty_recv = &mut self.dirty_recv;
            let cursor_recv = &mut self.cursor_recv;
            let format_recv = &mut self.format_recv;
            let fallback_active =
                !self.device_dirty_seen.load(Ordering::Acquire) && !self.client_handles.is_empty();

            let dirty_fut = async {
                match dirty_recv {
                    Some(recv) => recv.recv().await,
                    None => std::future::pending().await,
                }
            };
            let cursor_fut = async {
                match cursor_recv {
                    Some(recv) => recv.recv().await,
                    None => std::future::pending().await,
                }
            };
            let format_fut = async {
                match format_recv {
                    Some(recv) => recv.recv().await,
                    None => std::future::pending().await,
                }
            };
            let client_done = async {
                if clients.is_empty() {
                    std::future::pending().await
                } else {
                    clients.select_next_some().await
                }
            };
            let fallback_fut = async {
                if fallback_active {
                    timer.sleep(Duration::from_millis(16)).await;
                } else {
                    std::future::pending().await
                }
            };

            let event = futures::select! {
                accept = listener.accept().fuse() => {
                    let (socket, addr) = accept?;
                    Event::Accepted((socket, addr))
                }
                id = client_done.fuse() => Event::ClientDone(id),
                msg = dirty_fut.fuse() => match msg {
                    Ok(rects) => Event::DirtyRects(rects),
                    Err(_) => {
                        tracing::warn!("device dirty channel closed, enabling fallback diff");
                        self.dirty_recv = None;
                        self.reset_dirty_reporting();
                        continue;
                    }
                },
                msg = cursor_fut.fuse() => match msg {
                    Ok(update) => Event::Cursor(update),
                    Err(_) => {
                        self.cursor_recv = None;
                        continue;
                    }
                },
                msg = format_fut.fuse() => match msg {
                    Ok(fmt) => {
                        let mut latest_valid = None;
                        {
                            let guard = self.view.lock();
                            let vram_size = guard.0.len();
                            let base_offset = guard.0.file_offset();
                            if validate_display_format(&fmt, vram_size, base_offset).is_some() {
                                latest_valid = Some(fmt);
                            } else {
                                tracing::warn!(
                                    width = fmt.width,
                                    height = fmt.height,
                                    bytes_per_line = fmt.bytes_per_line,
                                    offset = fmt.offset,
                                    vram_size,
                                    "ignoring invalid framebuffer format"
                                );
                            }
                            if let Some(recv) = &mut self.format_recv {
                                while let Ok(newer) = recv.try_recv() {
                                    if validate_display_format(&newer, vram_size, base_offset)
                                        .is_some()
                                    {
                                        latest_valid = Some(newer);
                                    } else {
                                        tracing::warn!(
                                            width = newer.width,
                                            height = newer.height,
                                            bytes_per_line = newer.bytes_per_line,
                                            offset = newer.offset,
                                            vram_size,
                                            "ignoring invalid framebuffer format"
                                        );
                                    }
                                }
                            }
                        }
                        if let Some(valid) = latest_valid {
                            Event::FormatChanged(valid)
                        } else {
                            continue;
                        }
                    }
                    Err(_) => {
                        self.format_recv = None;
                        self.reset_dirty_reporting();
                        continue;
                    }
                },
                _ = fallback_fut.fuse() => Event::FallbackTick,
            };

            match event {
                Event::Accepted((socket, remote_addr)) => {
                    if self.client_handles.len() >= self.max_clients {
                        tracing::warn!(
                            addr = ?remote_addr,
                            max = self.max_clients,
                            "FastDesk client rejected, limit reached"
                        );
                        continue;
                    }
                    let sock: socket2::Socket = socket.into();
                    let _ = sock.set_tcp_nodelay(true);
                    match PolledSocket::new(driver, sock) {
                        Ok(socket) => self.spawn_client(socket, remote_addr),
                        Err(e) => {
                            tracing::error!(
                                error = %e,
                                "failed to register FastDesk client socket"
                            );
                        }
                    }
                }
                Event::ClientDone(id) => {
                    self.client_handles.retain(|c| c.id != id);
                    tracing::info!(
                        id,
                        count = self.client_handles.len(),
                        "FastDesk client disconnected"
                    );
                }
                Event::DirtyRects(first_rects) => {
                    if self.drain_format_updates() {
                        continue;
                    }
                    let mut rects = Vec::new();
                    let mut saw_reset = false;
                    let mut saw_dirty_after_reset = false;
                    let mut dirty_closed = false;
                    let mut process_batch = |batch: Vec<DirtyRect>| {
                        if batch.is_empty() {
                            rects.clear();
                            saw_reset = true;
                            saw_dirty_after_reset = false;
                        } else {
                            rects.extend(batch);
                            saw_dirty_after_reset = true;
                        }
                    };
                    process_batch(first_rects);
                    if let Some(recv) = &mut self.dirty_recv {
                        loop {
                            match recv.try_recv() {
                                Ok(more) => process_batch(more),
                                Err(mesh::TryRecvError::Empty) => break,
                                Err(mesh::TryRecvError::Closed | mesh::TryRecvError::Error(_)) => {
                                    dirty_closed = true;
                                    break;
                                }
                            }
                        }
                    }
                    if self.drain_format_updates() {
                        if dirty_closed {
                            self.dirty_recv = None;
                        }
                        continue;
                    }
                    if dirty_closed {
                        tracing::warn!("device dirty channel closed, enabling fallback diff");
                        self.dirty_recv = None;
                        self.reset_dirty_reporting();
                        continue;
                    }
                    if saw_reset && !saw_dirty_after_reset {
                        self.reset_dirty_reporting();
                        continue;
                    }
                    if !rects.is_empty() {
                        self.device_dirty_seen.store(true, Ordering::Release);
                        let rects = Arc::new(rects);
                        for c in &self.client_handles {
                            if saw_reset {
                                c.missed_dirty.store(true, Ordering::Release);
                            }
                            if c.dirty_send.try_send(Arc::clone(&rects)).is_err() {
                                c.missed_dirty.store(true, Ordering::Release);
                            }
                            let _ = c.wakeup_send.try_send(());
                        }
                    }
                }
                Event::Cursor(first_update) => {
                    let mut updates = vec![first_update];
                    if let Some(recv) = &mut self.cursor_recv {
                        while let Ok(more) = recv.try_recv() {
                            updates.push(more);
                        }
                    }
                    {
                        let mut state = self.cursor_state.lock();
                        for update in updates {
                            match update {
                                CursorUpdate::Position(pos) => {
                                    state.pos = Some(pos);
                                    state.pos_seq += 1;
                                }
                                CursorUpdate::Shape(shape) => {
                                    state.shape = Some(Arc::new(shape));
                                    state.shape_seq += 1;
                                }
                            }
                        }
                    }
                    for c in &self.client_handles {
                        let _ = c.wakeup_send.try_send(());
                    }
                }
                Event::FormatChanged(fmt) => {
                    self.apply_format_change(fmt);
                }
                Event::FallbackTick => {
                    let _ = self.drain_format_updates();
                    for c in &self.client_handles {
                        let _ = c.wakeup_send.try_send(());
                    }
                }
            }
        }
    }

    fn spawn_client(
        &mut self,
        socket: PolledSocket<socket2::Socket>,
        remote_addr: impl std::fmt::Debug,
    ) {
        let id = self.next_client_id;
        self.next_client_id += 1;
        tracing::info!(
            id,
            addr = ?remote_addr,
            count = self.client_handles.len() + 1,
            "FastDesk client connected"
        );

        let (dirty_send, dirty_recv) = async_channel::bounded(32);
        let (wakeup_send, wakeup_recv) = async_channel::bounded(1);
        let missed_dirty = Arc::new(AtomicBool::new(false));
        let (abort_send, abort_recv) = mesh::oneshot();

        self.client_handles.push(ClientHandle {
            id,
            dirty_send,
            wakeup_send,
            missed_dirty: missed_dirty.clone(),
            _abort_send: abort_send,
        });

        let ctx = ClientContext {
            client_id: id,
            socket,
            view: self.view.clone(),
            shm_path: self.shm_path.clone(),
            input_send: self.input_send.clone(),
            input_state: self.input_state.clone(),
            dirty_recv,
            cursor_state: self.cursor_state.clone(),
            wakeup_recv,
            missed_dirty,
            device_dirty_seen: self.device_dirty_seen.clone(),
            start_time: self.start_time,
        };

        let fut = Box::pin(async move {
            let r = futures::select! {
                r = run_client(ctx).fuse() => r,
                _ = abort_recv.fuse() => Err(anyhow!("FastDesk connection aborted")),
            };
            if let Err(err) = r {
                tracing::debug!(error = err.as_error(), id, "FastDesk client ended");
            }
            id
        });
        self.clients.push(fut);
    }

    async fn abort_all_clients(&mut self) {
        self.client_handles.clear();
        while self.clients.next().await.is_some() {}
    }
}

impl<T: Listener> inspect::Inspect for MultiClientServer<T> {
    fn inspect(&self, req: inspect::Request<'_>) {
        let mut resp = req.respond();
        resp.display_debug("local_addr", &self.listener.get().local_addr().unwrap());
        resp.field("client_count", self.client_handles.len());
        resp.field("shm_path", self.shm_path.as_deref());
        resp.field(
            "device_dirty_seen",
            self.device_dirty_seen.load(Ordering::Relaxed),
        );
    }
}

struct ClientContext {
    client_id: u64,
    socket: PolledSocket<socket2::Socket>,
    view: Arc<Mutex<ViewWrapper>>,
    shm_path: Option<String>,
    input_send: mesh::Sender<InputData>,
    input_state: Arc<Mutex<SharedInputState>>,
    dirty_recv: async_channel::Receiver<Arc<Vec<DirtyRect>>>,
    cursor_state: Arc<Mutex<SharedCursorState>>,
    wakeup_recv: async_channel::Receiver<()>,
    missed_dirty: Arc<AtomicBool>,
    device_dirty_seen: Arc<AtomicBool>,
    start_time: Instant,
}

fn validate_display_format(
    fmt: &video_core::FramebufferFormat,
    vram_size: usize,
    base_offset: u64,
) -> Option<(u16, u16, u32, u32)> {
    let w = u16::try_from(fmt.width).ok().filter(|&w| w > 0)?;
    let h = u16::try_from(fmt.height).ok().filter(|&h| h > 0)?;
    let row_bytes = (w as usize).checked_mul(4)?;
    if fmt.bytes_per_line < row_bytes {
        return None;
    }
    let stride = u32::try_from(fmt.bytes_per_line).ok()?;
    let surface_bytes = (h as usize - 1)
        .checked_mul(fmt.bytes_per_line)
        .and_then(|rows| rows.checked_add(row_bytes))?;
    if !fmt
        .offset
        .checked_add(surface_bytes)
        .is_some_and(|end| end <= vram_size)
    {
        return None;
    }
    let wire_offset = base_offset
        .checked_add(fmt.offset as u64)
        .and_then(|o| u32::try_from(o).ok())?;
    Some((w, h, stride, wire_offset))
}

fn fallback_framebuffer_format(vram_size: usize) -> video_core::FramebufferFormat {
    if 1024usize * 768 * 4 <= vram_size {
        video_core::FramebufferFormat {
            width: 1024,
            height: 768,
            bytes_per_line: 1024 * 4,
            offset: 0,
        }
    } else {
        let max_pixels = (vram_size / 4).max(1);
        let width = max_pixels.isqrt().clamp(1, 1024);
        let height = (max_pixels / width).clamp(1, 768);
        video_core::FramebufferFormat {
            width,
            height,
            bytes_per_line: width * 4,
            offset: 0,
        }
    }
}

fn current_display_dims(view: &Arc<Mutex<ViewWrapper>>) -> (u16, u16, u32, u32, usize) {
    let mut guard = view.lock();
    let vram_size = guard.0.len();
    let base_offset = guard.0.file_offset();
    if let Some(fmt) = guard.0.format() {
        if let Some((w, h, stride, offset)) = validate_display_format(&fmt, vram_size, base_offset) {
            return (w, h, stride, offset, vram_size);
        }
        tracing::warn!(
            width = fmt.width,
            height = fmt.height,
            bytes_per_line = fmt.bytes_per_line,
            offset = fmt.offset,
            vram_size,
            "invalid framebuffer format in view, falling back to safe bounds"
        );
    }
    let fallback = fallback_framebuffer_format(vram_size);
    guard.0.set_cached_format(fallback);
    let (w, h, stride, offset) = validate_display_format(&fallback, vram_size, base_offset)
        .unwrap_or((1, 1, 4, base_offset.min(u32::MAX as u64) as u32));
    (w, h, stride, offset, vram_size)
}

fn encode_cursor_shape_msg(shape: &CursorShape) -> ServerMsg {
    ServerMsg::CursorShape {
        width: shape.width,
        height: shape.height,
        hotspot_x: shape.hotspot_x,
        hotspot_y: shape.hotspot_y,
        lz4_bgra: lz4_flex::compress_prepend_size(&shape.argb_pixels),
    }
}

fn emit_pending_cursor_updates(
    cursor_state: &Mutex<SharedCursorState>,
    sent_shape_seq: &mut u64,
    sent_pos_seq: &mut u64,
    out_buf: &mut Vec<u8>,
) -> Result<(), fastdesk_proto::ProtoError> {
    let (shape, pos) = {
        let state = cursor_state.lock();
        let shape = if state.shape_seq != *sent_shape_seq {
            *sent_shape_seq = state.shape_seq;
            state.shape.clone()
        } else {
            None
        };
        let pos = if state.pos_seq != *sent_pos_seq {
            *sent_pos_seq = state.pos_seq;
            state.pos
        } else {
            None
        };
        (shape, pos)
    };

    if let Some(shape) = shape {
        encode_server_msg(&encode_cursor_shape_msg(&shape), out_buf)?;
    }
    if let Some(pos) = pos {
        encode_server_msg(
            &ServerMsg::CursorPosition {
                visible: pos.is_visible,
                x: pos.x,
                y: pos.y,
            },
            out_buf,
        )?;
    }
    Ok(())
}

const MAX_PENDING_PONGS: usize = 16;

struct ClientControlState {
    force_full: bool,
    use_shm: bool,
    pending_pongs: VecDeque<u64>,
}

async fn yield_now() {
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if yielded {
            std::task::Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    })
    .await;
}

async fn read_client_frame(
    socket: &mut (impl futures::AsyncRead + Unpin),
) -> anyhow::Result<ClientMsg> {
    let mut hdr = [0u8; 5];
    socket.read_exact(&mut hdr).await?;
    let (msg_type, payload_len) = fastdesk_proto::decode_frame_header(&hdr)?;
    let mut payload = vec![0u8; payload_len];
    socket.read_exact(&mut payload).await?;
    Ok(decode_client_msg(msg_type, &payload)?)
}

async fn read_client_frame_or_eof(
    socket: &mut (impl futures::AsyncRead + Unpin),
    payload: &mut Vec<u8>,
) -> anyhow::Result<Option<ClientMsg>> {
    let mut msg_type = [0u8; 1];
    loop {
        match socket.read(&mut msg_type).await {
            Ok(0) => return Ok(None),
            Ok(_) => break,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => return Ok(None),
            Err(e) => return Err(e.into()),
        }
    }
    let mut len_buf = [0u8; 4];
    socket.read_exact(&mut len_buf).await?;
    let payload_len = u32::from_le_bytes(len_buf) as usize;
    if payload_len > fastdesk_proto::MAX_FRAME_PAYLOAD_BYTES {
        anyhow::bail!("client frame payload too large: {payload_len}");
    }
    payload.resize(payload_len, 0);
    socket.read_exact(payload).await?;
    let msg = decode_client_msg(msg_type[0], payload)?;
    if payload.capacity() > 64 * 1024 {
        *payload = Vec::new();
    }
    Ok(Some(msg))
}

async fn run_client(mut ctx: ClientContext) -> anyhow::Result<()> {
    let mut input_tracker =
        ClientInputTracker::new(ctx.client_id, ctx.input_send.clone(), ctx.input_state.clone());

    let (mut width, mut height, mut stride, mut offset, vram_size) =
        current_display_dims(&ctx.view);

    let mut out_buf = Vec::with_capacity(64 * 1024);
    encode_server_msg(
        &ServerMsg::ServerHello {
            capabilities: CAP_ALL,
            width,
            height,
            stride,
            offset,
            vram_size: vram_size.min(u32::MAX as usize) as u32,
            shm_path: ctx.shm_path.clone(),
        },
        &mut out_buf,
    )?;
    ctx.socket.write_all(&out_buf).await?;

    let shm_available = ctx.shm_path.is_some();
    let use_shm = match read_client_frame(&mut ctx.socket).await? {
        ClientMsg::ClientHello { prefer_shm, .. } => prefer_shm && shm_available,
        other => anyhow::bail!("expected ClientHello, got {other:?}"),
    };

    let (mut read_half, mut write_half) = ctx.socket.split();
    let control = Arc::new(Mutex::new(ClientControlState {
        force_full: true,
        use_shm,
        pending_pongs: VecDeque::new(),
    }));
    let (ctrl_tx, ctrl_rx) = async_channel::bounded::<()>(1);

    let reader_control = Arc::clone(&control);
    let reader_fut = async move {
        let mut payload = Vec::new();
        while let Some(msg) = read_client_frame_or_eof(&mut read_half, &mut payload).await? {
            match msg {
                ClientMsg::ClientHello { .. } => {}
                ClientMsg::KeyEvent { scancode, make } => {
                    input_tracker.handle_key(scancode, make);
                }
                ClientMsg::PointerEvent {
                    button_mask,
                    x,
                    y,
                    wheel_delta,
                } => {
                    input_tracker.handle_pointer(button_mask, x, y, wheel_delta);
                }
                ClientMsg::RequestFullFrame { allow_shm } => {
                    {
                        let mut ctrl = reader_control.lock();
                        ctrl.force_full = true;
                        ctrl.use_shm = allow_shm && shm_available;
                    }
                    let _ = ctrl_tx.try_send(());
                }
                ClientMsg::Ping { client_time_us } => {
                    {
                        let mut ctrl = reader_control.lock();
                        if ctrl.pending_pongs.len() >= MAX_PENDING_PONGS {
                            ctrl.pending_pongs.pop_front();
                        }
                        ctrl.pending_pongs.push_back(client_time_us);
                    }
                    let _ = ctrl_tx.try_send(());
                }
            }
        }
        Ok::<(), anyhow::Error>(())
    };

    let view = ctx.view;
    let shm_path = ctx.shm_path;
    let dirty_recv = ctx.dirty_recv;
    let cursor_state = ctx.cursor_state;
    let wakeup_recv = ctx.wakeup_recv;
    let missed_dirty = ctx.missed_dirty;
    let device_dirty_seen = ctx.device_dirty_seen;
    let start_time = ctx.start_time;
    let writer_control = control;

    let writer_fut = async move {
        let mut tracker = DirtyTileTracker::new(width, height);
        let mut merged_rects: Vec<TileRect> = Vec::new();
        let mut raw_scratch: Vec<u8> = Vec::new();
        let mut prev_fb: Vec<u8> = Vec::new();
        let mut cur_fb: Vec<u8> = Vec::new();
        let mut frame_seq = 0u64;
        let mut sent_shape_seq = 0u64;
        let mut sent_pos_seq = 0u64;
        let mut force_full = false;

        loop {
            let mut display_wakeup = false;
            let mut pending_pongs = VecDeque::new();
            let mut use_shm_now;

            loop {
                while ctrl_rx.try_recv().is_ok() {}
                while wakeup_recv.try_recv().is_ok() {
                    display_wakeup = true;
                }
                if missed_dirty.load(Ordering::Acquire) {
                    display_wakeup = true;
                }
                {
                    let mut ctrl = writer_control.lock();
                    if std::mem::take(&mut ctrl.force_full) {
                        force_full = true;
                        display_wakeup = true;
                    }
                    use_shm_now = ctrl.use_shm;
                    pending_pongs.append(&mut ctrl.pending_pongs);
                }
                if force_full {
                    display_wakeup = true;
                }
                if display_wakeup || !pending_pongs.is_empty() {
                    break;
                }

                enum WakeEvent {
                    Display(Result<(), async_channel::RecvError>),
                    Control(Result<(), async_channel::RecvError>),
                }
                let ev = futures::select! {
                    w = Box::pin(wakeup_recv.recv()).fuse() => WakeEvent::Display(w),
                    c = Box::pin(ctrl_rx.recv()).fuse() => WakeEvent::Control(c),
                };
                match ev {
                    WakeEvent::Display(Ok(())) => {
                        display_wakeup = true;
                    }
                    WakeEvent::Display(Err(_)) | WakeEvent::Control(Err(_)) => return Ok(()),
                    WakeEvent::Control(Ok(())) => {}
                }
            }

            if !display_wakeup {
                out_buf.clear();
                for client_time_us in pending_pongs {
                    encode_server_msg(
                        &ServerMsg::Pong {
                            client_time_us,
                            host_time_us: start_time.elapsed().as_micros() as u64,
                        },
                        &mut out_buf,
                    )?;
                }
                if !out_buf.is_empty() {
                    write_half.write_all(&out_buf).await?;
                    yield_now().await;
                }
                continue;
            }

            out_buf.clear();

            // 1. Emit latest hardware cursor shape and position if changed.
            emit_pending_cursor_updates(
                &cursor_state,
                &mut sent_shape_seq,
                &mut sent_pos_seq,
                &mut out_buf,
            )?;

            // 2. Check resolution changes.
            let (new_w, new_h, new_stride, new_offset, _) = current_display_dims(&view);
            if new_w != width || new_h != height || new_stride != stride || new_offset != offset {
                width = new_w;
                height = new_h;
                stride = new_stride;
                offset = new_offset;
                tracker.resize(width, height);
                prev_fb.clear();
                cur_fb.clear();
                force_full = true;
                encode_server_msg(
                    &ServerMsg::Resolution {
                        width,
                        height,
                        stride,
                        offset,
                        shm_path: shm_path.clone(),
                    },
                    &mut out_buf,
                )?;
            }

            // 3. Collect dirty regions.
            tracker.clear();
            let mut saw_rects = false;
            while let Ok(rects) = dirty_recv.try_recv() {
                saw_rects = true;
                for r in rects.iter() {
                    tracker.mark_rect(r.left, r.top, r.right, r.bottom);
                }
            }

            let missed = missed_dirty.swap(false, Ordering::AcqRel);
            let need_full = force_full || missed;
            if need_full {
                tracker.mark_all();
                force_full = false;
                if !device_dirty_seen.load(Ordering::Acquire) {
                    let line_bytes = width as usize * 4;
                    let total_bytes = line_bytes * height as usize;
                    prev_fb.resize(total_bytes, 0);
                    let mut guard = view.lock();
                    for y in 0..height {
                        let row =
                            &mut prev_fb[y as usize * line_bytes..(y as usize + 1) * line_bytes];
                        guard.0.read_line(y, row);
                    }
                }
            } else if !saw_rects && !device_dirty_seen.load(Ordering::Acquire) {
                let line_bytes = width as usize * 4;
                let total_bytes = line_bytes * height as usize;
                cur_fb.resize(total_bytes, 0);
                {
                    let mut guard = view.lock();
                    for y in 0..height {
                        let row =
                            &mut cur_fb[y as usize * line_bytes..(y as usize + 1) * line_bytes];
                        guard.0.read_line(y, row);
                    }
                }
                if prev_fb.len() != total_bytes {
                    tracker.mark_all();
                } else {
                    tracker.diff_frames(&prev_fb, &cur_fb, line_bytes);
                }
                std::mem::swap(&mut prev_fb, &mut cur_fb);
            } else if device_dirty_seen.load(Ordering::Acquire) && !prev_fb.is_empty() {
                prev_fb = Vec::new();
                cur_fb = Vec::new();
            }

            // 4. Encode dirty rectangles (SHM zero-copy vs LZ4 dirty-column read),
            // splitting into multiple FrameBatch messages if needed to stay within
            // MAX_RECTS_PER_BATCH and MAX_FRAME_PAYLOAD_BYTES.
            if !tracker.is_empty() {
                tracker.merge_into(&mut merged_rects);

                const FRAME_BATCH_HEADER_BYTES: usize = 18;
                const ENCODED_RECT_HEADER_BYTES: usize = 13;

                let host_time_us = start_time.elapsed().as_micros() as u64;
                let mut batch_rects: Vec<EncodedRect> = Vec::new();
                let mut batch_payload_bytes = FRAME_BATCH_HEADER_BYTES;

                let mut push_rect =
                    |rect: EncodedRect, out_buf: &mut Vec<u8>| -> anyhow::Result<()> {
                        let rect_wire_bytes = ENCODED_RECT_HEADER_BYTES + rect.data.len();
                        if !batch_rects.is_empty()
                            && (batch_rects.len() >= fastdesk_proto::MAX_RECTS_PER_BATCH
                                || batch_payload_bytes + rect_wire_bytes
                                    > fastdesk_proto::MAX_FRAME_PAYLOAD_BYTES)
                        {
                            frame_seq += 1;
                            encode_server_msg(
                                &ServerMsg::FrameBatch {
                                    frame_seq,
                                    host_time_us,
                                    rects: std::mem::take(&mut batch_rects),
                                },
                                out_buf,
                            )?;
                            batch_payload_bytes = FRAME_BATCH_HEADER_BYTES;
                        }
                        batch_payload_bytes += rect_wire_bytes;
                        batch_rects.push(rect);
                        Ok(())
                    };

                if use_shm_now {
                    for r in &merged_rects {
                        push_rect(
                            EncodedRect {
                                x: r.x,
                                y: r.y,
                                w: r.w,
                                h: r.h,
                                encoding: RectEncoding::ShmDirtyOnly,
                                data: Vec::new(),
                            },
                            &mut out_buf,
                        )?;
                    }
                } else {
                    let max_rect_raw_bytes = fastdesk_proto::MAX_FRAME_PAYLOAD_BYTES
                        - FRAME_BATCH_HEADER_BYTES
                        - ENCODED_RECT_HEADER_BYTES;
                    for r in &merged_rects {
                        let row_bytes = r.w as usize * 4;
                        if row_bytes == 0 || r.h == 0 {
                            continue;
                        }
                        let max_rows = (max_rect_raw_bytes / row_bytes)
                            .max(1)
                            .min(u16::MAX as usize) as u16;
                        let mut y_off = 0u16;
                        while y_off < r.h {
                            let strip_h = (r.h - y_off).min(max_rows);
                            raw_scratch.resize(row_bytes * strip_h as usize, 0);
                            {
                                let mut guard = view.lock();
                                for dy in 0..strip_h {
                                    let dst = &mut raw_scratch
                                        [dy as usize * row_bytes..(dy as usize + 1) * row_bytes];
                                    guard.0.read_rect_row(r.x, r.y + y_off + dy, r.w, dst);
                                }
                            }
                            let (encoding, data) = encode_bgra_rect(&raw_scratch);
                            push_rect(
                                EncodedRect {
                                    x: r.x,
                                    y: r.y + y_off,
                                    w: r.w,
                                    h: strip_h,
                                    encoding,
                                    data,
                                },
                                &mut out_buf,
                            )?;
                            y_off += strip_h;
                        }
                    }
                }

                if !batch_rects.is_empty() {
                    frame_seq += 1;
                    encode_server_msg(
                        &ServerMsg::FrameBatch {
                            frame_seq,
                            host_time_us,
                            rects: batch_rects,
                        },
                        &mut out_buf,
                    )?;
                }
            }

            for client_time_us in pending_pongs {
                encode_server_msg(
                    &ServerMsg::Pong {
                        client_time_us,
                        host_time_us: start_time.elapsed().as_micros() as u64,
                    },
                    &mut out_buf,
                )?;
            }

            if !out_buf.is_empty() {
                write_half.write_all(&out_buf).await?;
                yield_now().await;
            }
        }
    };

    let mut reader_fut = std::pin::pin!(reader_fut.fuse());
    let mut writer_fut = std::pin::pin!(writer_fut.fuse());
    futures::select! {
        r = reader_fut => r,
        r = writer_fut => r,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fastdesk_proto::BTN_LEFT;
    use fastdesk_proto::BTN_WHEEL_UP;
    use fastdesk_proto::decode_bgra_rect;
    use fastdesk_proto::read_server_msg;
    use fastdesk_proto::write_client_msg;
    use framebuffer::FRAMEBUFFER_SIZE;
    use sparse_mmap::SparseMapping;
    use sparse_mmap::alloc_shared_memory;
    use sparse_mmap::new_mappable_from_file;
    use std::net::SocketAddr;
    use std::net::TcpStream;
    use std::thread;
    use std::thread::JoinHandle;
    use video_core::FramebufferFormat;

    struct TestServer {
        addr: SocketAddr,
        vram: SparseMapping,
        input_recv: mesh::Receiver<InputData>,
        dirty_send: mesh::Sender<Vec<DirtyRect>>,
        cursor_send: mesh::Sender<CursorUpdate>,
        format_send: mesh::Sender<FramebufferFormat>,
        stop_send: Option<mesh::OneshotSender<()>>,
        join: Option<JoinHandle<anyhow::Result<()>>>,
    }

    impl TestServer {
        fn stop(mut self) {
            if let Some(stop) = self.stop_send.take() {
                stop.send(());
            }
            if let Some(join) = self.join.take() {
                let res = join.join().unwrap();
                assert!(res.is_ok(), "{res:?}");
            }
        }
    }

    fn start_test_server(
        width: u16,
        height: u16,
        shm_file: Option<&std::path::Path>,
    ) -> TestServer {
        start_test_server_with_offset(width, height, 0, shm_file)
    }

    fn start_test_server_with_offset(
        width: u16,
        height: u16,
        initial_offset: usize,
        shm_file: Option<&std::path::Path>,
    ) -> TestServer {
        start_test_server_with_vram(width, height, initial_offset, FRAMEBUFFER_SIZE, shm_file)
    }

    fn start_test_server_with_vram(
        width: u16,
        height: u16,
        initial_offset: usize,
        vram_size: usize,
        shm_file: Option<&std::path::Path>,
    ) -> TestServer {
        start_test_server_with_max_clients(
            width,
            height,
            initial_offset,
            vram_size,
            shm_file,
            8,
        )
    }

    fn start_test_server_with_max_clients(
        width: u16,
        height: u16,
        initial_offset: usize,
        vram_size: usize,
        shm_file: Option<&std::path::Path>,
        max_clients: usize,
    ) -> TestServer {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let (vram, shm_path_str) = if let Some(path) = shm_file {
            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)
                .unwrap();
            f.set_len(vram_size as u64).unwrap();
            let m = new_mappable_from_file(&f, true, false).unwrap();
            (m, Some(path.to_string_lossy().into_owned()))
        } else {
            (alloc_shared_memory(vram_size, "fastdesk-test").unwrap(), None)
        };
        let writer_vram = vram.try_clone().unwrap();
        let (fb, access) = framebuffer::framebuffer(vram, vram_size, 0).unwrap();
        let format_send = fb.format_send();
        format_send.send(FramebufferFormat {
            width: width as usize,
            height: height as usize,
            bytes_per_line: width as usize * 4,
            offset: initial_offset,
        });

        let mapping = SparseMapping::new(vram_size).unwrap();
        mapping
            .map_file(0, vram_size, &writer_vram, 0, true)
            .unwrap();

        let (input_send, input_recv) = mesh::channel();
        let (dirty_send, dirty_recv) = mesh::channel();
        let (cursor_send, cursor_recv) = mesh::channel();
        let (stop_send, stop_recv) = mesh::oneshot();

        let join = thread::spawn(move || {
            block_with_io(async |driver| -> anyhow::Result<()> {
                let view = ViewWrapper(access.view().unwrap());
                let mut server = MultiClientServer::new(
                    PolledSocket::new(&driver, listener)?,
                    view,
                    shm_path_str,
                    input_send,
                    Some(dirty_recv),
                    Some(cursor_recv),
                    max_clients,
                );
                futures::select! {
                    r = server.process(&driver).fuse() => r,
                    _ = stop_recv.fuse() => Ok(()),
                }
            })
        });

        TestServer {
            addr,
            vram: mapping,
            input_recv,
            dirty_send,
            cursor_send,
            format_send,
            stop_send: Some(stop_send),
            join: Some(join),
        }
    }

    fn connect_client(addr: SocketAddr, prefer_shm: bool) -> (TcpStream, ServerMsg) {
        let mut stream = TcpStream::connect(addr).unwrap();
        stream.set_nodelay(true).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let hello = read_server_msg(&mut stream).unwrap();
        write_client_msg(
            &mut stream,
            &ClientMsg::ClientHello {
                capabilities: CAP_ALL,
                prefer_shm,
            },
        )
        .unwrap();
        (stream, hello)
    }

    #[test]
    fn event_driven_dirty_lz4_cursor_and_input() {
        let mut server = start_test_server(64, 64, None);
        let (mut stream, hello) = connect_client(server.addr, false);
        assert!(matches!(
            hello,
            ServerMsg::ServerHello {
                width: 64,
                height: 64,
                shm_path: None,
                ..
            }
        ));

        // Initial full frame.
        let initial = read_server_msg(&mut stream).unwrap();
        match initial {
            ServerMsg::FrameBatch { rects, .. } => {
                assert_eq!(rects.len(), 1);
                assert_eq!(rects[0].w, 64);
                assert_eq!(rects[0].h, 64);
            }
            other => panic!("expected initial FrameBatch, got {other:?}"),
        }

        // Send a dirty rect + VRAM pixel write; verify immediate wakeup (< 25ms) & exact pixels.
        let px = [0x11u8, 0x22, 0x33, 0xff];
        let row_bytes = 64 * 4;
        for y in 16..32usize {
            for x in 16..32usize {
                server
                    .vram
                    .write_at(y * row_bytes + x * 4, &px)
                    .unwrap();
            }
        }
        let sent_at = Instant::now();
        server.dirty_send.send(vec![DirtyRect {
            left: 16,
            top: 16,
            right: 32,
            bottom: 32,
        }]);

        let update = read_server_msg(&mut stream).unwrap();
        let elapsed = sent_at.elapsed();
        assert!(
            elapsed < Duration::from_millis(25),
            "expected immediate event wakeup, took {elapsed:?}"
        );
        match update {
            ServerMsg::FrameBatch { rects, .. } => {
                assert_eq!(rects.len(), 1);
                assert_eq!((rects[0].x, rects[0].y, rects[0].w, rects[0].h), (16, 16, 16, 16));
                let decoded = decode_bgra_rect(rects[0].encoding, &rects[0].data, 16 * 16 * 4).unwrap();
                assert_eq!(&decoded[0..4], &px);
                assert_eq!(&decoded[1020..1024], &px);
            }
            other => panic!("expected dirty FrameBatch, got {other:?}"),
        }

        // Hardware cursor shape and position broadcast.
        server.cursor_send.send(CursorUpdate::Shape(CursorShape {
            width: 2,
            height: 2,
            hotspot_x: 1,
            hotspot_y: 0,
            argb_pixels: vec![0xaa; 16],
        }));
        server.cursor_send.send(CursorUpdate::Position(CursorPosition {
            is_visible: true,
            x: 25,
            y: 30,
        }));

        let mut got_shape = false;
        let mut got_pos = false;
        for _ in 0..2 {
            match read_server_msg(&mut stream).unwrap() {
                ServerMsg::CursorShape {
                    width: 2,
                    height: 2,
                    hotspot_x: 1,
                    hotspot_y: 0,
                    lz4_bgra,
                } => {
                    let pixels = lz4_flex::decompress_size_prepended(&lz4_bgra).unwrap();
                    assert_eq!(pixels, vec![0xaa; 16]);
                    got_shape = true;
                }
                ServerMsg::CursorPosition {
                    visible: true,
                    x: 25,
                    y: 30,
                } => got_pos = true,
                other => panic!("unexpected cursor msg: {other:?}"),
            }
        }
        assert!(got_shape && got_pos);

        // Input events (keyboard scancode + mouse + wheel).
        write_client_msg(
            &mut stream,
            &ClientMsg::KeyEvent {
                scancode: 0x1e,
                make: true,
            },
        )
        .unwrap();
        write_client_msg(
            &mut stream,
            &ClientMsg::PointerEvent {
                button_mask: BTN_LEFT,
                x: 1000,
                y: 2000,
                wheel_delta: 2,
            },
        )
        .unwrap();

        let mut received = Vec::new();
        for _ in 0..5 {
            for _ in 0..100 {
                if let Ok(ev) = server.input_recv.try_recv() {
                    received.push(ev);
                    break;
                }
                thread::sleep(Duration::from_millis(2));
            }
        }
        assert!(matches!(
            received[0],
            InputData::Keyboard(KeyboardData {
                code: 0x1e,
                make: true
            })
        ));
        assert!(matches!(
            received[1],
            InputData::Mouse(MouseData {
                button_mask: BTN_LEFT,
                x: 1000,
                y: 2000
            })
        ));
        assert!(matches!(
            received[2],
            InputData::Mouse(MouseData {
                button_mask,
                ..
            }) if button_mask == (BTN_LEFT | BTN_WHEEL_UP)
        ));
        assert!(matches!(
            received[4],
            InputData::Mouse(MouseData {
                button_mask: BTN_LEFT,
                ..
            })
        ));

        drop(stream);
        server.stop();
    }

    #[test]
    fn shm_zerocopy_mode_emits_no_pixel_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let shm_file = dir.path().join("vram.shm");
        let server = start_test_server(64, 64, Some(&shm_file));
        let (mut stream, hello) = connect_client(server.addr, true);

        match hello {
            ServerMsg::ServerHello {
                shm_path: Some(p), ..
            } => assert_eq!(p, shm_file.to_string_lossy()),
            other => panic!("expected shm_path in ServerHello, got {other:?}"),
        }

        // Initial frame uses ShmDirtyOnly with empty data.
        match read_server_msg(&mut stream).unwrap() {
            ServerMsg::FrameBatch { rects, .. } => {
                assert_eq!(rects.len(), 1);
                assert_eq!(rects[0].encoding, RectEncoding::ShmDirtyOnly);
                assert!(rects[0].data.is_empty());
            }
            other => panic!("expected ShmDirtyOnly FrameBatch, got {other:?}"),
        }

        // Subsequent dirty rect emits ShmDirtyOnly and pixel write is visible in shm_file.
        let px = [0xde, 0xad, 0xbe, 0xff];
        server.vram.write_at((4 * 64 + 4) * 4, &px).unwrap();
        server.dirty_send.send(vec![DirtyRect {
            left: 0,
            top: 0,
            right: 16,
            bottom: 16,
        }]);
        match read_server_msg(&mut stream).unwrap() {
            ServerMsg::FrameBatch { rects, .. } => {
                assert_eq!(rects.len(), 1);
                assert_eq!(rects[0].encoding, RectEncoding::ShmDirtyOnly);
                assert!(rects[0].data.is_empty());
            }
            other => panic!("expected ShmDirtyOnly FrameBatch, got {other:?}"),
        }
        let disk_bytes = std::fs::read(&shm_file).unwrap();
        let off = (4 * 64 + 4) * 4;
        assert_eq!(&disk_bytes[off..off + 4], &px);

        drop(stream);
        server.stop();
    }

    #[test]
    fn shm_and_lz4_respect_nonzero_surface_offset() {
        let dir = tempfile::tempdir().unwrap();
        let shm_file = dir.path().join("vram-offset.shm");
        let server = start_test_server_with_offset(64, 64, 4096, Some(&shm_file));

        // Write sentinel garbage at byte 0 and the real pixel inside the offset primary surface.
        let garbage = [0xaa, 0xbb, 0xcc, 0xdd];
        let expected_px = [0x34, 0x56, 0x78, 0xff];
        server.vram.write_at(0, &garbage).unwrap();
        let surface_rel_off = (4 * 64 + 4) * 4;
        server
            .vram
            .write_at(4096 + surface_rel_off, &expected_px)
            .unwrap();

        // LZ4 client observes offset in ServerHello and receives pixels from 4096+.
        let (mut lz4_stream, lz4_hello) = connect_client(server.addr, false);
        assert!(matches!(
            lz4_hello,
            ServerMsg::ServerHello {
                width: 64,
                height: 64,
                stride: 256,
                offset: 4096,
                ..
            }
        ));
        match read_server_msg(&mut lz4_stream).unwrap() {
            ServerMsg::FrameBatch { rects, .. } => {
                assert_eq!(rects.len(), 1);
                let decoded =
                    decode_bgra_rect(rects[0].encoding, &rects[0].data, 64 * 64 * 4).unwrap();
                assert_eq!(&decoded[surface_rel_off..surface_rel_off + 4], &expected_px);
            }
            other => panic!("expected initial LZ4 FrameBatch, got {other:?}"),
        }

        // SHM client receives offset in ServerHello and reads from offset + surface_rel_off.
        let (mut shm_stream, shm_hello) = connect_client(server.addr, true);
        let shm_offset = match shm_hello {
            ServerMsg::ServerHello {
                offset,
                shm_path: Some(_),
                ..
            } => {
                assert_eq!(offset, 4096);
                offset as usize
            }
            other => panic!("expected ServerHello with offset and shm_path, got {other:?}"),
        };
        match read_server_msg(&mut shm_stream).unwrap() {
            ServerMsg::FrameBatch { rects, .. } => {
                assert_eq!(rects.len(), 1);
                assert_eq!(rects[0].encoding, RectEncoding::ShmDirtyOnly);
            }
            other => panic!("expected initial SHM FrameBatch, got {other:?}"),
        }
        let disk_bytes = std::fs::read(&shm_file).unwrap();
        assert_eq!(&disk_bytes[0..4], &garbage);
        assert_eq!(
            &disk_bytes[shm_offset + surface_rel_off..shm_offset + surface_rel_off + 4],
            &expected_px
        );

        // Dynamic surface offset change emits Resolution with new offset.
        server.format_send.send(FramebufferFormat {
            width: 64,
            height: 64,
            bytes_per_line: 64 * 4,
            offset: 8192,
        });
        server.dirty_send.send(vec![DirtyRect {
            left: 0,
            top: 0,
            right: 16,
            bottom: 16,
        }]);
        match read_server_msg(&mut shm_stream).unwrap() {
            ServerMsg::Resolution {
                width: 64,
                height: 64,
                stride: 256,
                offset: 8192,
                ..
            } => {}
            other => panic!("expected Resolution with offset 8192, got {other:?}"),
        }
        match read_server_msg(&mut shm_stream).unwrap() {
            ServerMsg::FrameBatch { .. } => {}
            other => panic!("expected FrameBatch after Resolution, got {other:?}"),
        }

        drop(lz4_stream);
        drop(shm_stream);
        server.stop();
    }

    #[test]
    fn shm_fallback_persists_across_subsequent_frames() {
        let dir = tempfile::tempdir().unwrap();
        let shm_file = dir.path().join("vram-fallback.shm");
        let server = start_test_server(64, 64, Some(&shm_file));
        let (mut stream, _) = connect_client(server.addr, true);

        // Initial frame uses SHM.
        match read_server_msg(&mut stream).unwrap() {
            ServerMsg::FrameBatch { rects, .. } => {
                assert_eq!(rects[0].encoding, RectEncoding::ShmDirtyOnly);
            }
            other => panic!("expected initial SHM FrameBatch, got {other:?}"),
        }

        // Client triggers fallback to pixel mode.
        write_client_msg(&mut stream, &ClientMsg::RequestFullFrame { allow_shm: false }).unwrap();
        match read_server_msg(&mut stream).unwrap() {
            ServerMsg::FrameBatch { rects, .. } => {
                assert!(!rects.is_empty());
                assert!(
                    rects
                        .iter()
                        .all(|r| r.encoding != RectEncoding::ShmDirtyOnly)
                );
            }
            other => panic!("expected pixel fallback FrameBatch, got {other:?}"),
        }

        // Subsequent dirty updates must stay in pixel mode (not revert to ShmDirtyOnly).
        let px1 = [0x12, 0x34, 0x56, 0xff];
        server.vram.write_at((16 * 64 + 16) * 4, &px1).unwrap();
        server.dirty_send.send(vec![DirtyRect {
            left: 16,
            top: 16,
            right: 32,
            bottom: 32,
        }]);
        match read_server_msg(&mut stream).unwrap() {
            ServerMsg::FrameBatch { rects, .. } => {
                assert_eq!(rects.len(), 1);
                assert_ne!(rects[0].encoding, RectEncoding::ShmDirtyOnly);
                let decoded =
                    decode_bgra_rect(rects[0].encoding, &rects[0].data, 16 * 16 * 4).unwrap();
                assert_eq!(&decoded[0..4], &px1);
            }
            other => panic!("expected first post-fallback pixel FrameBatch, got {other:?}"),
        }

        let px2 = [0x78, 0x9a, 0xbc, 0xff];
        server.vram.write_at((32 * 64 + 32) * 4, &px2).unwrap();
        server.dirty_send.send(vec![DirtyRect {
            left: 32,
            top: 32,
            right: 48,
            bottom: 48,
        }]);
        match read_server_msg(&mut stream).unwrap() {
            ServerMsg::FrameBatch { rects, .. } => {
                assert_eq!(rects.len(), 1);
                assert_ne!(rects[0].encoding, RectEncoding::ShmDirtyOnly);
                let decoded =
                    decode_bgra_rect(rects[0].encoding, &rects[0].data, 16 * 16 * 4).unwrap();
                assert_eq!(&decoded[0..4], &px2);
            }
            other => panic!("expected second post-fallback pixel FrameBatch, got {other:?}"),
        }

        drop(stream);
        server.stop();
    }

    #[test]
    fn fragmented_checkerboard_2560x1440_splits_batches_without_disconnect() {
        let dir = tempfile::tempdir().unwrap();
        let shm_file = dir.path().join("vram-2560.shm");
        let server = start_test_server_with_vram(
            2560,
            1440,
            0,
            framebuffer::FASTDESK_FRAMEBUFFER_SIZE,
            Some(&shm_file),
        );
        // Write sentinel pixels before clients connect so fallback polling sees no
        // spurious changes prior to the 7,200-rect DIRT burst.
        let px_first = [0xaa, 0xbb, 0xcc, 0xff];
        let px_last = [0x11, 0x22, 0x33, 0xff];
        server.vram.write_at(0, &px_first).unwrap();
        server
            .vram
            .write_at((1424usize * 2560 + 2544) * 4, &px_last)
            .unwrap();

        let (mut lz4_stream, _) = connect_client(server.addr, false);
        let (mut shm_stream, _) = connect_client(server.addr, true);

        // Drain initial full frames.
        assert!(matches!(
            read_server_msg(&mut lz4_stream).unwrap(),
            ServerMsg::FrameBatch { .. }
        ));
        assert!(matches!(
            read_server_msg(&mut shm_stream).unwrap(),
            ServerMsg::FrameBatch { .. }
        ));

        // Build all 7,200 16x16 checkerboard dirty rects at 2560x1440 (> MAX_RECTS_PER_BATCH = 4096).
        let mut dirty_rects = Vec::with_capacity(7200);
        for ty in 0..90i32 {
            for tx in 0..160i32 {
                if (tx + ty) % 2 == 0 {
                    dirty_rects.push(DirtyRect {
                        left: tx * 16,
                        top: ty * 16,
                        right: (tx + 1) * 16,
                        bottom: (ty + 1) * 16,
                    });
                }
            }
        }
        assert_eq!(dirty_rects.len(), 7200);
        server.dirty_send.send(dirty_rects);

        // LZ4 client receives 2 split FrameBatches totaling 7,200 rects with exact pixels.
        let mut lz4_batches = Vec::new();
        let mut lz4_total_rects = 0usize;
        let mut prev_seq = 0u64;
        let mut saw_first = false;
        let mut saw_last = false;
        while lz4_total_rects < 7200 {
            match read_server_msg(&mut lz4_stream).unwrap() {
                ServerMsg::FrameBatch {
                    frame_seq, rects, ..
                } => {
                    assert!(frame_seq > prev_seq);
                    prev_seq = frame_seq;
                    lz4_batches.push(rects.len());
                    lz4_total_rects += rects.len();
                    for r in &rects {
                        assert!(r.encoding != RectEncoding::ShmDirtyOnly);
                        if (r.x, r.y, r.w, r.h) == (0, 0, 16, 16) {
                            let decoded =
                                decode_bgra_rect(r.encoding, &r.data, 16 * 16 * 4).unwrap();
                            assert_eq!(&decoded[0..4], &px_first);
                            saw_first = true;
                        }
                        if (r.x, r.y, r.w, r.h) == (2544, 1424, 16, 16) {
                            let decoded =
                                decode_bgra_rect(r.encoding, &r.data, 16 * 16 * 4).unwrap();
                            assert_eq!(&decoded[0..4], &px_last);
                            saw_last = true;
                        }
                    }
                }
                other => panic!("expected LZ4 FrameBatch, got {other:?}"),
            }
        }
        assert_eq!(lz4_batches, vec![4096, 3104]);
        assert!(saw_first && saw_last);

        // SHM client also receives 2 split ShmDirtyOnly batches totaling 7,200 rects.
        let mut shm_batches = Vec::new();
        let mut shm_total_rects = 0usize;
        prev_seq = 0;
        while shm_total_rects < 7200 {
            match read_server_msg(&mut shm_stream).unwrap() {
                ServerMsg::FrameBatch {
                    frame_seq, rects, ..
                } => {
                    assert!(frame_seq > prev_seq);
                    prev_seq = frame_seq;
                    assert!(rects.iter().all(|r| r.encoding == RectEncoding::ShmDirtyOnly));
                    shm_batches.push(rects.len());
                    shm_total_rects += rects.len();
                }
                other => panic!("expected SHM FrameBatch, got {other:?}"),
            }
        }
        assert_eq!(shm_batches, vec![4096, 3104]);

        // Verify both connections remain alive and responsive after the fragmented burst.
        for (stream, ts) in [(&mut lz4_stream, 999u64), (&mut shm_stream, 1000u64)] {
            write_client_msg(stream, &ClientMsg::Ping { client_time_us: ts }).unwrap();
            assert!(matches!(
                read_server_msg(stream).unwrap(),
                ServerMsg::Pong { client_time_us, .. } if client_time_us == ts
            ));
        }

        drop(lz4_stream);
        drop(shm_stream);
        server.stop();
    }

    #[test]
    fn format_and_vram_updates_reach_viewer_after_dirty_reporting_starts() {
        let server = start_test_server(64, 64, None);
        let (mut stream, _) = connect_client(server.addr, false);
        stream
            .set_read_timeout(Some(Duration::from_millis(800)))
            .unwrap();

        // Initial full frame.
        assert!(matches!(
            read_server_msg(&mut stream).unwrap(),
            ServerMsg::FrameBatch { .. }
        ));

        // Send one DIRT message so device_dirty_seen becomes true.
        let px1 = [0x12, 0x34, 0x56, 0xff];
        server.vram.write_at((16 * 64 + 16) * 4, &px1).unwrap();
        server.dirty_send.send(vec![DirtyRect {
            left: 16,
            top: 16,
            right: 32,
            bottom: 32,
        }]);
        match read_server_msg(&mut stream).unwrap() {
            ServerMsg::FrameBatch { rects, .. } => {
                assert_eq!(rects.len(), 1);
            }
            other => panic!("expected dirty FrameBatch, got {other:?}"),
        }

        write_client_msg(&mut stream, &ClientMsg::Ping { client_time_us: 100 }).unwrap();
        assert!(matches!(
            read_server_msg(&mut stream).unwrap(),
            ServerMsg::Pong {
                client_time_us: 100,
                ..
            }
        ));

        // Without sending any DirtyRect, change format/offset and write px2 at the new surface.
        let px2 = [0xde, 0xad, 0xbe, 0xff];
        server
            .vram
            .write_at(4096 + (8 * 96 + 8) * 4, &px2)
            .unwrap();
        server.format_send.send(FramebufferFormat {
            width: 96,
            height: 80,
            bytes_per_line: 96 * 4,
            offset: 4096,
        });

        match read_server_msg(&mut stream).unwrap() {
            ServerMsg::Resolution {
                width: 96,
                height: 80,
                stride: 384,
                offset: 4096,
                shm_path: None,
            } => {}
            other => panic!("expected Resolution 96x80@4096, got {other:?}"),
        }
        match read_server_msg(&mut stream).unwrap() {
            ServerMsg::FrameBatch { rects, .. } => {
                let mut found = false;
                for r in &rects {
                    if r.x <= 8 && 8 < r.x + r.w && r.y <= 8 && 8 < r.y + r.h {
                        let decoded = decode_bgra_rect(
                            r.encoding,
                            &r.data,
                            r.w as usize * r.h as usize * 4,
                        )
                        .unwrap();
                        let off = ((8 - r.y as usize) * r.w as usize + (8 - r.x as usize)) * 4;
                        assert_eq!(&decoded[off..off + 4], &px2);
                        found = true;
                    }
                }
                assert!(found, "expected px2 in post-Resolution FrameBatch");
            }
            other => panic!("expected post-Resolution FrameBatch, got {other:?}"),
        }

        // Still without sending any DirtyRect, write px3 and verify fallback polling detects it.
        let px3 = [0xfe, 0xdc, 0xba, 0xff];
        server
            .vram
            .write_at(4096 + (24 * 96 + 24) * 4, &px3)
            .unwrap();
        match read_server_msg(&mut stream).unwrap() {
            ServerMsg::FrameBatch { rects, .. } => {
                let mut found = false;
                for r in &rects {
                    if r.x <= 24 && 24 < r.x + r.w && r.y <= 24 && 24 < r.y + r.h {
                        let decoded = decode_bgra_rect(
                            r.encoding,
                            &r.data,
                            r.w as usize * r.h as usize * 4,
                        )
                        .unwrap();
                        let off = ((24 - r.y as usize) * r.w as usize + (24 - r.x as usize)) * 4;
                        assert_eq!(&decoded[off..off + 4], &px3);
                        found = true;
                    }
                }
                assert!(found, "expected px3 in fallback FrameBatch");
            }
            other => panic!("expected fallback FrameBatch, got {other:?}"),
        }

        drop(stream);
        server.stop();
    }

    #[test]
    fn cursor_backpressure_during_delayed_handshake_preserves_latest_state() {
        let server = start_test_server(64, 64, None);
        let mut stream = TcpStream::connect(server.addr).unwrap();
        stream.set_nodelay(true).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        // Reading ServerHello ensures spawn_client has already registered the client.
        assert!(matches!(
            read_server_msg(&mut stream).unwrap(),
            ServerMsg::ServerHello { .. }
        ));

        // Burst >64 cursor updates before sending ClientHello.
        server.cursor_send.send(CursorUpdate::Shape(CursorShape {
            width: 2,
            height: 2,
            hotspot_x: 0,
            hotspot_y: 0,
            argb_pixels: vec![0x11; 16],
        }));
        server.cursor_send.send(CursorUpdate::Shape(CursorShape {
            width: 4,
            height: 4,
            hotspot_x: 2,
            hotspot_y: 3,
            argb_pixels: vec![0x77; 64],
        }));
        for i in 0..200 {
            server.cursor_send.send(CursorUpdate::Position(CursorPosition {
                is_visible: true,
                x: i,
                y: i,
            }));
        }
        server.cursor_send.send(CursorUpdate::Position(CursorPosition {
            is_visible: false,
            x: 19,
            y: 27,
        }));

        thread::sleep(Duration::from_millis(25));
        write_client_msg(
            &mut stream,
            &ClientMsg::ClientHello {
                capabilities: CAP_ALL,
                prefer_shm: false,
            },
        )
        .unwrap();

        let expected_shape_1 = (4u16, 4u16, 2u16, 3u16, vec![0x77u8; 64]);
        let expected_pos_1 = (false, 19i32, 27i32);
        let mut got_frame = false;
        let mut last_shape = None;
        let mut last_pos = None;
        while !(got_frame
            && last_shape.as_ref() == Some(&expected_shape_1)
            && last_pos == Some(expected_pos_1))
        {
            match read_server_msg(&mut stream).unwrap() {
                ServerMsg::FrameBatch { .. } => got_frame = true,
                ServerMsg::CursorShape {
                    width,
                    height,
                    hotspot_x,
                    hotspot_y,
                    lz4_bgra,
                } => {
                    let pixels = lz4_flex::decompress_size_prepended(&lz4_bgra).unwrap();
                    last_shape = Some((width, height, hotspot_x, hotspot_y, pixels));
                }
                ServerMsg::CursorPosition { visible, x, y } => {
                    last_pos = Some((visible, x, y));
                }
                other => panic!("unexpected pre-handshake cursor msg: {other:?}"),
            }
        }

        write_client_msg(&mut stream, &ClientMsg::Ping { client_time_us: 1 }).unwrap();
        assert!(matches!(
            read_server_msg(&mut stream).unwrap(),
            ServerMsg::Pong {
                client_time_us: 1,
                ..
            }
        ));

        // Post-handshake burst: new shape + >64 positions must converge to final state.
        server.cursor_send.send(CursorUpdate::Shape(CursorShape {
            width: 2,
            height: 2,
            hotspot_x: 1,
            hotspot_y: 1,
            argb_pixels: vec![0xcc; 16],
        }));
        for i in 0..200 {
            server.cursor_send.send(CursorUpdate::Position(CursorPosition {
                is_visible: true,
                x: 100 + i,
                y: 100 + i,
            }));
        }
        server.cursor_send.send(CursorUpdate::Position(CursorPosition {
            is_visible: true,
            x: 50,
            y: 55,
        }));

        let expected_shape_2 = (2u16, 2u16, 1u16, 1u16, vec![0xccu8; 16]);
        let expected_pos_2 = (true, 50i32, 55i32);
        while !(last_shape.as_ref() == Some(&expected_shape_2) && last_pos == Some(expected_pos_2)) {
            match read_server_msg(&mut stream).unwrap() {
                ServerMsg::CursorShape {
                    width,
                    height,
                    hotspot_x,
                    hotspot_y,
                    lz4_bgra,
                } => {
                    let pixels = lz4_flex::decompress_size_prepended(&lz4_bgra).unwrap();
                    last_shape = Some((width, height, hotspot_x, hotspot_y, pixels));
                }
                ServerMsg::CursorPosition { visible, x, y } => {
                    last_pos = Some((visible, x, y));
                }
                other => panic!("unexpected post-handshake cursor msg: {other:?}"),
            }
        }

        write_client_msg(&mut stream, &ClientMsg::Ping { client_time_us: 2 }).unwrap();
        assert!(matches!(
            read_server_msg(&mut stream).unwrap(),
            ServerMsg::Pong {
                client_time_us: 2,
                ..
            }
        ));

        drop(stream);
        server.stop();
    }

    fn recv_input_with_timeout(
        recv: &mut mesh::Receiver<InputData>,
        timeout: Duration,
    ) -> Option<InputData> {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if let Ok(ev) = recv.try_recv() {
                return Some(ev);
            }
            thread::sleep(Duration::from_millis(2));
        }
        None
    }

    #[test]
    fn rejects_oversized_and_malformed_framebuffer_formats_without_oom() {
        // Invalid initial 65535x65535 format against 8 MiB VRAM falls back to safe bounds.
        let server = start_test_server_with_vram(65535, 65535, 0, FRAMEBUFFER_SIZE, None);
        let (mut stream, hello) = connect_client(server.addr, false);
        assert!(matches!(
            hello,
            ServerMsg::ServerHello {
                width: 1024,
                height: 768,
                stride: 4096,
                offset: 0,
                ..
            }
        ));
        assert!(matches!(
            read_server_msg(&mut stream).unwrap(),
            ServerMsg::FrameBatch { .. }
        ));

        // Transition to a valid 64x64 mode.
        server.format_send.send(FramebufferFormat {
            width: 64,
            height: 64,
            bytes_per_line: 64 * 4,
            offset: 0,
        });
        assert!(matches!(
            read_server_msg(&mut stream).unwrap(),
            ServerMsg::Resolution {
                width: 64,
                height: 64,
                stride: 256,
                offset: 0,
                ..
            }
        ));
        assert!(matches!(
            read_server_msg(&mut stream).unwrap(),
            ServerMsg::FrameBatch { .. }
        ));

        // Send a series of malformed/OOB formats that must all be rejected.
        for bad in [
            FramebufferFormat {
                width: 65535,
                height: 65535,
                bytes_per_line: 65535 * 4,
                offset: 0,
            },
            FramebufferFormat {
                width: 0,
                height: 64,
                bytes_per_line: 256,
                offset: 0,
            },
            FramebufferFormat {
                width: 64,
                height: 0,
                bytes_per_line: 256,
                offset: 0,
            },
            FramebufferFormat {
                width: 1024,
                height: 65535,
                bytes_per_line: 4,
                offset: 0,
            },
            FramebufferFormat {
                width: 1920,
                height: 1080,
                bytes_per_line: 1920 * 4,
                offset: FRAMEBUFFER_SIZE - 4096,
            },
            FramebufferFormat {
                width: 1024,
                height: 1024,
                bytes_per_line: usize::MAX,
                offset: 0,
            },
        ] {
            server.format_send.send(bad);
        }

        // Send Ping and confirm Pong arrives without any Resolution message.
        thread::sleep(Duration::from_millis(30));
        write_client_msg(&mut stream, &ClientMsg::Ping { client_time_us: 77 }).unwrap();
        loop {
            match read_server_msg(&mut stream).unwrap() {
                ServerMsg::Pong {
                    client_time_us: 77, ..
                } => break,
                ServerMsg::FrameBatch { .. } => {}
                ServerMsg::Resolution {
                    width,
                    height,
                    stride,
                    offset,
                    ..
                } => panic!("unexpected Resolution {width}x{height} stride={stride} offset={offset}"),
                other => panic!("unexpected msg after invalid formats: {other:?}"),
            }
        }

        // Valid format followed immediately by invalid format must keep the valid 80x80 format.
        server.format_send.send(FramebufferFormat {
            width: 80,
            height: 80,
            bytes_per_line: 80 * 4,
            offset: 0,
        });
        server.format_send.send(FramebufferFormat {
            width: 65535,
            height: 65535,
            bytes_per_line: 65535 * 4,
            offset: 0,
        });
        loop {
            match read_server_msg(&mut stream).unwrap() {
                ServerMsg::Resolution {
                    width,
                    height,
                    stride,
                    offset,
                    ..
                } => {
                    assert_eq!((width, height, stride, offset), (80, 80, 320, 0));
                    break;
                }
                ServerMsg::FrameBatch { .. } => {}
                other => panic!("unexpected msg waiting for 80x80 Resolution: {other:?}"),
            }
        }
        assert!(matches!(
            read_server_msg(&mut stream).unwrap(),
            ServerMsg::FrameBatch { .. }
        ));

        drop(stream);
        server.stop();
    }

    #[test]
    fn disconnect_releases_held_keys_and_mouse_buttons() {
        let mut server = start_test_server(64, 64, None);

        // Single-client abrupt disconnect releases held keys (reverse order) and mouse button.
        let (mut s1, _) = connect_client(server.addr, false);
        assert!(matches!(
            read_server_msg(&mut s1).unwrap(),
            ServerMsg::FrameBatch { .. }
        ));

        write_client_msg(
            &mut s1,
            &ClientMsg::KeyEvent {
                scancode: 0x1d,
                make: true,
            },
        )
        .unwrap();
        write_client_msg(
            &mut s1,
            &ClientMsg::KeyEvent {
                scancode: 0x1e,
                make: true,
            },
        )
        .unwrap();
        write_client_msg(
            &mut s1,
            &ClientMsg::PointerEvent {
                button_mask: BTN_LEFT,
                x: 1200,
                y: 3400,
                wheel_delta: 0,
            },
        )
        .unwrap();

        assert!(matches!(
            recv_input_with_timeout(&mut server.input_recv, Duration::from_secs(1)),
            Some(InputData::Keyboard(KeyboardData {
                code: 0x1d,
                make: true
            }))
        ));
        assert!(matches!(
            recv_input_with_timeout(&mut server.input_recv, Duration::from_secs(1)),
            Some(InputData::Keyboard(KeyboardData {
                code: 0x1e,
                make: true
            }))
        ));
        assert!(matches!(
            recv_input_with_timeout(&mut server.input_recv, Duration::from_secs(1)),
            Some(InputData::Mouse(MouseData {
                button_mask: BTN_LEFT,
                x: 1200,
                y: 3400
            }))
        ));

        drop(s1);

        assert!(matches!(
            recv_input_with_timeout(&mut server.input_recv, Duration::from_secs(1)),
            Some(InputData::Keyboard(KeyboardData {
                code: 0x1e,
                make: false
            }))
        ));
        assert!(matches!(
            recv_input_with_timeout(&mut server.input_recv, Duration::from_secs(1)),
            Some(InputData::Keyboard(KeyboardData {
                code: 0x1d,
                make: false
            }))
        ));
        assert!(matches!(
            recv_input_with_timeout(&mut server.input_recv, Duration::from_secs(1)),
            Some(InputData::Mouse(MouseData {
                button_mask: 0,
                x: 1200,
                y: 3400
            }))
        ));

        // Multi-client overlapping holds: disconnecting client A preserves client B's held input
        // until client B also disconnects.
        let (mut ca, _) = connect_client(server.addr, false);
        let (mut cb, _) = connect_client(server.addr, false);
        assert!(matches!(
            read_server_msg(&mut ca).unwrap(),
            ServerMsg::FrameBatch { .. }
        ));
        assert!(matches!(
            read_server_msg(&mut cb).unwrap(),
            ServerMsg::FrameBatch { .. }
        ));

        write_client_msg(
            &mut ca,
            &ClientMsg::KeyEvent {
                scancode: 0x1d,
                make: true,
            },
        )
        .unwrap();
        write_client_msg(
            &mut ca,
            &ClientMsg::PointerEvent {
                button_mask: BTN_LEFT,
                x: 500,
                y: 600,
                wheel_delta: 0,
            },
        )
        .unwrap();
        assert!(recv_input_with_timeout(&mut server.input_recv, Duration::from_secs(1)).is_some());
        assert!(recv_input_with_timeout(&mut server.input_recv, Duration::from_secs(1)).is_some());

        write_client_msg(
            &mut cb,
            &ClientMsg::KeyEvent {
                scancode: 0x1d,
                make: true,
            },
        )
        .unwrap();
        write_client_msg(
            &mut cb,
            &ClientMsg::KeyEvent {
                scancode: 0x2a,
                make: true,
            },
        )
        .unwrap();
        write_client_msg(
            &mut cb,
            &ClientMsg::PointerEvent {
                button_mask: BTN_LEFT | fastdesk_proto::BTN_RIGHT,
                x: 700,
                y: 800,
                wheel_delta: 0,
            },
        )
        .unwrap();
        for _ in 0..3 {
            assert!(
                recv_input_with_timeout(&mut server.input_recv, Duration::from_secs(1)).is_some()
            );
        }

        // Dropping client A must NOT release 0x1d or BTN_LEFT while client B still holds them.
        drop(ca);
        assert!(recv_input_with_timeout(&mut server.input_recv, Duration::from_millis(50)).is_none());

        // Dropping client B releases 0x2a, 0x1d, and clears mouse buttons.
        drop(cb);
        assert!(matches!(
            recv_input_with_timeout(&mut server.input_recv, Duration::from_secs(1)),
            Some(InputData::Keyboard(KeyboardData {
                code: 0x2a,
                make: false
            }))
        ));
        assert!(matches!(
            recv_input_with_timeout(&mut server.input_recv, Duration::from_secs(1)),
            Some(InputData::Keyboard(KeyboardData {
                code: 0x1d,
                make: false
            }))
        ));
        assert!(matches!(
            recv_input_with_timeout(&mut server.input_recv, Duration::from_secs(1)),
            Some(InputData::Mouse(MouseData { button_mask: 0, .. }))
        ));

        server.stop();
    }

    #[test]
    fn queued_dirty_before_format_reset_preserves_fallback_polling() {
        let server = start_test_server(64, 64, None);
        let (mut stream, _) = connect_client(server.addr, false);
        stream
            .set_read_timeout(Some(Duration::from_millis(800)))
            .unwrap();

        // Drain initial full frame.
        assert!(matches!(
            read_server_msg(&mut stream).unwrap(),
            ServerMsg::FrameBatch { .. }
        ));

        // Repeat across several rounds so both select! ready orderings are exercised.
        for round in 1..=10u8 {
            // Re-enable device_dirty_seen with a dirty rect first.
            let px_init = [0x10, round, 0x20, 0xff];
            server.vram.write_at((8 * 64 + 8) * 4, &px_init).unwrap();
            server.dirty_send.send(vec![DirtyRect {
                left: 0,
                top: 0,
                right: 16,
                bottom: 16,
            }]);
            assert!(matches!(
                read_server_msg(&mut stream).unwrap(),
                ServerMsg::FrameBatch { .. }
            ));

            // Queue a dirty rect followed immediately by a same-dimension format reset.
            let px_pre = [0x01, round, 0x02, 0xff];
            server.vram.write_at((16 * 64 + 16) * 4, &px_pre).unwrap();
            server.dirty_send.send(vec![DirtyRect {
                left: 16,
                top: 16,
                right: 32,
                bottom: 32,
            }]);
            server.format_send.send(FramebufferFormat {
                width: 64,
                height: 64,
                bytes_per_line: 64 * 4,
                offset: 0,
            });

            // Wait for the reset to settle and drain all queued frames via Ping/Pong.
            thread::sleep(Duration::from_millis(25));
            let ping_id = 1000 + round as u64;
            write_client_msg(
                &mut stream,
                &ClientMsg::Ping {
                    client_time_us: ping_id,
                },
            )
            .unwrap();
            loop {
                match read_server_msg(&mut stream).unwrap() {
                    ServerMsg::Pong { client_time_us, .. } if client_time_us == ping_id => break,
                    ServerMsg::FrameBatch { .. } | ServerMsg::Resolution { .. } => {}
                    other => panic!("unexpected msg during format reset barrier: {other:?}"),
                }
            }

            // Write a sentinel pixel without sending any DirtyRect; fallback polling must deliver it.
            let px_fallback = [0xaa, round, 0x55, 0xff];
            server
                .vram
                .write_at((40 * 64 + 40) * 4, &px_fallback)
                .unwrap();
            let mut found = false;
            for _ in 0..8 {
                match read_server_msg(&mut stream).unwrap() {
                    ServerMsg::FrameBatch { rects, .. } => {
                        for r in &rects {
                            if r.x <= 40 && 40 < r.x + r.w && r.y <= 40 && 40 < r.y + r.h {
                                let decoded = decode_bgra_rect(
                                    r.encoding,
                                    &r.data,
                                    r.w as usize * r.h as usize * 4,
                                )
                                .unwrap();
                                let off =
                                    ((40 - r.y as usize) * r.w as usize + (40 - r.x as usize)) * 4;
                                if decoded[off..off + 4] == px_fallback {
                                    found = true;
                                }
                            }
                        }
                        if found {
                            break;
                        }
                    }
                    other => panic!("unexpected msg waiting for fallback FrameBatch: {other:?}"),
                }
            }
            assert!(
                found,
                "round {round}: fallback polling stopped after queued dirty + format reset"
            );
        }

        // Also verify in-band empty Vec<DirtyRect> reset barrier preserves fallback polling.
        server.dirty_send.send(vec![DirtyRect {
            left: 0,
            top: 0,
            right: 16,
            bottom: 16,
        }]);
        assert!(matches!(
            read_server_msg(&mut stream).unwrap(),
            ServerMsg::FrameBatch { .. }
        ));
        server.dirty_send.send(vec![DirtyRect {
            left: 16,
            top: 16,
            right: 32,
            bottom: 32,
        }]);
        server.dirty_send.send(Vec::new());
        thread::sleep(Duration::from_millis(25));
        write_client_msg(
            &mut stream,
            &ClientMsg::Ping {
                client_time_us: 2000,
            },
        )
        .unwrap();
        loop {
            match read_server_msg(&mut stream).unwrap() {
                ServerMsg::Pong {
                    client_time_us: 2000,
                    ..
                } => break,
                ServerMsg::FrameBatch { .. } => {}
                other => panic!("unexpected msg during in-band reset barrier: {other:?}"),
            }
        }

        let px_inband = [0xfe, 0xca, 0xef, 0xff];
        server.vram.write_at((48 * 64 + 48) * 4, &px_inband).unwrap();
        let mut found_inband = false;
        for _ in 0..8 {
            match read_server_msg(&mut stream).unwrap() {
                ServerMsg::FrameBatch { rects, .. } => {
                    for r in &rects {
                        if r.x <= 48 && 48 < r.x + r.w && r.y <= 48 && 48 < r.y + r.h {
                            let decoded = decode_bgra_rect(
                                r.encoding,
                                &r.data,
                                r.w as usize * r.h as usize * 4,
                            )
                            .unwrap();
                            let off =
                                ((48 - r.y as usize) * r.w as usize + (48 - r.x as usize)) * 4;
                            if decoded[off..off + 4] == px_inband {
                                found_inband = true;
                            }
                        }
                    }
                    if found_inband {
                        break;
                    }
                }
                other => panic!("unexpected msg waiting for in-band fallback FrameBatch: {other:?}"),
            }
        }
        assert!(found_inband, "in-band dirty reset did not re-enable fallback polling");

        drop(stream);
        server.stop();
    }

    #[test]
    fn input_releases_not_blocked_by_backpressured_frame_writes() {
        let mut server = start_test_server(1280, 720, None);

        // Shrink client SO_RCVBUF so server write_all() saturates TCP buffers quickly when client
        // stops reading frames.
        let sock = socket2::Socket::new(
            socket2::Domain::IPV4,
            socket2::Type::STREAM,
            None,
        )
        .unwrap();
        sock.set_recv_buffer_size(16 * 1024).unwrap();
        sock.connect(&server.addr.into()).unwrap();
        let mut stream: TcpStream = sock.into();
        stream.set_nodelay(true).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        let _hello = read_server_msg(&mut stream).unwrap();
        write_client_msg(
            &mut stream,
            &ClientMsg::ClientHello {
                capabilities: CAP_ALL,
                prefer_shm: false,
            },
        )
        .unwrap();
        assert!(matches!(
            read_server_msg(&mut stream).unwrap(),
            ServerMsg::FrameBatch { .. }
        ));

        // Press Ctrl and Left mouse button while reader is idle.
        write_client_msg(
            &mut stream,
            &ClientMsg::KeyEvent {
                scancode: 0x1d,
                make: true,
            },
        )
        .unwrap();
        write_client_msg(
            &mut stream,
            &ClientMsg::PointerEvent {
                button_mask: BTN_LEFT,
                x: 100,
                y: 100,
                wheel_delta: 0,
            },
        )
        .unwrap();
        assert!(matches!(
            recv_input_with_timeout(&mut server.input_recv, Duration::from_secs(1)),
            Some(InputData::Keyboard(KeyboardData {
                code: 0x1d,
                make: true
            }))
        ));
        assert!(matches!(
            recv_input_with_timeout(&mut server.input_recv, Duration::from_secs(1)),
            Some(InputData::Mouse(MouseData {
                button_mask: BTN_LEFT,
                ..
            }))
        ));

        // Fill VRAM with high-entropy incompressible data and trigger multiple full-screen dirty
        // bursts without reading from `stream`, backpressuring `write_half.write_all()`.
        let frame_bytes = 1280usize * 720 * 4;
        let mut noise = vec![0u8; frame_bytes];
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        for chunk in noise.chunks_exact_mut(8) {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            chunk.copy_from_slice(&state.to_le_bytes());
        }
        for round in 0..6u8 {
            noise[0] = round;
            server.vram.write_at(0, &noise).unwrap();
            server.dirty_send.send(vec![DirtyRect {
                left: 0,
                top: 0,
                right: 1280,
                bottom: 720,
            }]);
            thread::sleep(Duration::from_millis(8));
        }

        // Send key-up and button release while the outbound frame pipe is stalled; input must
        // arrive promptly without waiting for frame reads to resume.
        write_client_msg(
            &mut stream,
            &ClientMsg::KeyEvent {
                scancode: 0x1d,
                make: false,
            },
        )
        .unwrap();
        write_client_msg(
            &mut stream,
            &ClientMsg::PointerEvent {
                button_mask: 0,
                x: 100,
                y: 100,
                wheel_delta: 0,
            },
        )
        .unwrap();

        assert!(matches!(
            recv_input_with_timeout(&mut server.input_recv, Duration::from_millis(200)),
            Some(InputData::Keyboard(KeyboardData {
                code: 0x1d,
                make: false
            }))
        ));
        assert!(matches!(
            recv_input_with_timeout(&mut server.input_recv, Duration::from_millis(200)),
            Some(InputData::Mouse(MouseData { button_mask: 0, .. }))
        ));

        // Even while frame transmission remains stalled, disconnecting releases any held keys.
        write_client_msg(
            &mut stream,
            &ClientMsg::KeyEvent {
                scancode: 0x2a,
                make: true,
            },
        )
        .unwrap();
        assert!(matches!(
            recv_input_with_timeout(&mut server.input_recv, Duration::from_millis(200)),
            Some(InputData::Keyboard(KeyboardData {
                code: 0x2a,
                make: true
            }))
        ));
        drop(stream);
        assert!(matches!(
            recv_input_with_timeout(&mut server.input_recv, Duration::from_millis(200)),
            Some(InputData::Keyboard(KeyboardData {
                code: 0x2a,
                make: false
            }))
        ));

        server.stop();
    }

    #[test]
    fn zero_max_clients_uses_default_and_accepts_connections() {
        let server = start_test_server_with_max_clients(64, 64, 0, FRAMEBUFFER_SIZE, None, 0);
        let (mut stream1, hello1) = connect_client(server.addr, false);
        assert!(matches!(hello1, ServerMsg::ServerHello { .. }));
        assert!(matches!(
            read_server_msg(&mut stream1).unwrap(),
            ServerMsg::FrameBatch { .. }
        ));
        let (mut stream2, hello2) = connect_client(server.addr, false);
        assert!(matches!(hello2, ServerMsg::ServerHello { .. }));
        assert!(matches!(
            read_server_msg(&mut stream2).unwrap(),
            ServerMsg::FrameBatch { .. }
        ));
        drop(stream1);
        drop(stream2);
        server.stop();
    }
}
