// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

//! TTRPC server.

use crate::message::MESSAGE_TYPE_REQUEST;
use crate::message::MESSAGE_TYPE_RESPONSE;
use crate::message::ReadResult;
use crate::message::Request;
use crate::message::Response;
use crate::message::TooLongError;
use crate::message::read_message;
use crate::message::write_message;
use crate::rpc::ProtocolError;
use crate::rpc::status_from_err;
use crate::service::Code;
use crate::service::DecodedRpc;
use crate::service::GenericRpc;
use crate::service::ServiceRpc;
use crate::service::ServiceRpcError;
use crate::service::Status;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use futures::stream::FusedStream;
use futures_concurrency::future::TryJoin;
use futures_concurrency::stream::Merge;
use mesh::CancelContext;
use mesh::MeshPayload;
use mesh::local_node::Port;
use pal_async::driver::Driver;
#[cfg(target_os = "linux")]
use pal_async::interest::PollEvents;
use pal_async::socket::AsSockRef;
use pal_async::socket::Listener;
#[cfg(target_os = "linux")]
use pal_async::socket::PollReadyExt;
use pal_async::socket::PolledSocket;
use std::collections::HashMap;
use std::io::Read;
use std::io::Write;
use std::pin::Pin;
use std::task::ready;
use unicycle::FuturesUnordered;

/// A ttrpc server.
#[derive(Debug, Default)]
pub struct Server {
    services: HashMap<&'static str, mesh::Sender<(CancelContext, GenericRpc)>>,
}

/// A receiver for RPC requests for a given service.
///
/// Returned by [`Server::add_service`].
#[derive(MeshPayload)]
#[mesh(bound = "T: ServiceRpc")]
pub struct RpcReceiver<T>(mesh::Receiver<(CancelContext, DecodedRpc<T>)>);

impl<T: ServiceRpc> RpcReceiver<T> {
    /// Returns a disconnected stream, useful for when a service is dynamically
    /// not registered.
    pub fn disconnected() -> Self {
        let (_send, recv) = mesh::channel();
        Self(recv)
    }
}

impl<T: ServiceRpc> Stream for RpcReceiver<T> {
    type Item = (CancelContext, T);

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        while let Some((ctx, rpc)) = ready!(Pin::new(&mut this.0).poll_next(cx)) {
            match rpc {
                DecodedRpc::Rpc(rpc) => return Some((ctx, rpc)).into(),
                DecodedRpc::Err { rpc, err } => {
                    rpc.fail(err);
                }
            }
        }
        None.into()
    }
}

impl<T: ServiceRpc> FusedStream for RpcReceiver<T> {
    fn is_terminated(&self) -> bool {
        self.0.is_terminated()
    }
}

impl GenericRpc {
    fn fail(self, err: ServiceRpcError) {
        let status = match err {
            ServiceRpcError::UnknownMethod => Status {
                code: Code::Unimplemented.into(),
                message: format!("unknown method {}", self.method),
                details: Vec::new(),
            },
            ServiceRpcError::InvalidInput(error) => status_from_err(Code::InvalidArgument, error),
        };
        self.respond_status(status);
    }
}

impl Server {
    /// Creates a new ttrpc server.
    pub fn new() -> Self {
        Self {
            services: Default::default(),
        }
    }

    /// Adds or updates a channel for receiving service requests.
    pub fn add_service<T: ServiceRpc>(&mut self) -> RpcReceiver<T> {
        let (send, recv) = mesh::channel();
        self.services.insert(T::NAME, Port::from(send).into());
        RpcReceiver(recv)
    }

    /// Runs the server using the ttrpc transport, listening on `listener` and
    /// servicing connections until `cancel`.
    pub async fn run(
        &mut self,
        driver: &(impl Driver + ?Sized),
        listener: impl Listener,
        cancel: mesh::OneshotReceiver<()>,
    ) -> anyhow::Result<()> {
        let mut listener = PolledSocket::new(driver, listener)?;
        let mut tasks = FuturesUnordered::new();
        let mut cancel = cancel.fuse();
        loop {
            let conn = futures::select! { // merge semantics
                r = listener.accept().fuse() => r,
                _ = tasks.next() => continue,
                _ = cancel => break,
            };
            if let Ok(conn) = conn.and_then(|(conn, _)| PolledSocket::new(driver, conn)) {
                tasks.push(async {
                    let _ = self.serve_connection(conn).await.map_err(|err| {
                        tracing::error!(
                            error = err.as_ref() as &dyn std::error::Error,
                            "connection error"
                        )
                    });
                });
            }
        }
        Ok(())
    }

    /// Runs the server, servicing a single connection `conn`.
    pub async fn run_single(
        &mut self,
        driver: &(impl Driver + ?Sized),
        conn: impl AsSockRef + Read + Write,
    ) -> anyhow::Result<()> {
        self.serve_connection(PolledSocket::new(driver, conn)?)
            .await
    }

    /// Services a single, already-accepted connection using the ttrpc protocol.
    ///
    /// This is useful when the caller owns the accept loop, for example to
    /// dispatch a connection to ttrpc or another protocol based on a sniffed
    /// prefix byte.
    pub async fn serve_connection(
        &self,
        stream: PolledSocket<impl AsSockRef + Read + Write>,
    ) -> anyhow::Result<()> {
        let (mut reader, mut writer) = stream.split();
        let (stream_send, mut stream_recv) = mesh::channel();
        let (ctx, cancel) = CancelContext::new().with_cancel();
        let cancel = parking_lot::Mutex::new(cancel);
        let peer_closed = std::sync::atomic::AtomicBool::new(false);
        let (send_done_tx, send_done_rx) = mesh::oneshot::<()>();
        let recv_task = async {
            let res: anyhow::Result<()> = {
                let stream_send = stream_send; // drop on EOF/error so send_task can finish draining
                async {
                    while let Some(message) = read_message(&mut reader).await? {
                        let (send, recv) = mesh::oneshot::<Result<Vec<u8>, Status>>();
                        stream_send.send((message.stream_id, recv));

                        let handle = handle_message(message).and_then(|request| {
                            let service = self.services.get(request.service.as_str()).ok_or_else(|| {
                                status_from_err(
                                    Code::Unimplemented,
                                    anyhow::anyhow!("unknown service {}", request.service),
                                )
                            })?;

                            let ctx = if request.timeout_nano == 0 {
                                ctx.clone()
                            } else {
                                ctx.with_timeout(std::time::Duration::from_nanos(request.timeout_nano))
                            };

                            Ok(move |port| {
                                service.send((
                                    ctx,
                                    GenericRpc {
                                        method: request.method,
                                        data: request.payload,
                                        port,
                                    },
                                ));
                            })
                        });

                        match handle {
                            Ok(handle) => handle(send.into()),
                            Err(err) => send.send(Err(err)),
                        }
                    }
                    Ok(())
                }
                .await
            };
            if res.is_err() {
                drop(send_done_rx);
                peer_closed.store(true, std::sync::atomic::Ordering::Release);
                cancel.lock().cancel();
                return res;
            }
            // A clean read EOF can be either a half-close (`shutdown(SHUT_WR)`),
            // where the peer still expects replies, or a full socket close.
            // On Linux, wait for `POLLHUP`/`POLLERR` vs `send_task` completing
            // so half-closed clients still receive in-flight replies while
            // full disconnects cancel pending RPCs immediately.
            #[cfg(target_os = "linux")]
            {
                futures::select! {
                    _ = reader.wait_ready(PollEvents::HUP | PollEvents::ERR).fuse() => {
                        peer_closed.store(true, std::sync::atomic::Ordering::Release);
                        cancel.lock().cancel();
                    }
                    _ = send_done_rx.fuse() => {}
                }
            }
            #[cfg(not(target_os = "linux"))]
            {
                drop(send_done_rx);
                peer_closed.store(true, std::sync::atomic::Ordering::Release);
                cancel.lock().cancel();
            }
            Ok(())
        };
        let send_task = async {
            let mut responses = FuturesUnordered::new();
            enum Event<T> {
                Request((u32, mesh::OneshotReceiver<Result<Vec<u8>, Status>>)),
                Response(T),
            }
            while let Some(event) = (
                (&mut stream_recv).map(Event::Request),
                (&mut responses).map(Event::Response),
            )
                .merge()
                .next()
                .await
            {
                match event {
                    Event::Request((stream_id, recv)) => {
                        let recv = recv.map(move |r| {
                            (
                                stream_id,
                                match r {
                                    Ok(Ok(payload)) => Response::Payload(payload),
                                    Ok(Err(status)) => Response::Status(status),
                                    Err(err) => {
                                        Response::Status(status_from_err(Code::Internal, err))
                                    }
                                },
                            )
                        });
                        responses.push(recv);
                    }
                    Event::Response((stream_id, payload)) => {
                        if !peer_closed.load(std::sync::atomic::Ordering::Acquire) {
                            if let Err(err) = write_message(
                                &mut writer,
                                stream_id,
                                MESSAGE_TYPE_RESPONSE,
                                &mesh::payload::encode(payload),
                            )
                            .await
                            {
                                peer_closed.store(true, std::sync::atomic::Ordering::Release);
                                cancel.lock().cancel();
                                return Err(err);
                            }
                        }
                    }
                }
            }
            send_done_tx.send(());
            anyhow::Result::<_>::Ok(())
        };
        (recv_task, send_task).try_join().await?;
        Ok(())
    }
}

fn handle_message(message: ReadResult) -> Result<Request, Status> {
    if message.stream_id % 2 != 1 {
        return Err(status_from_err(
            Code::InvalidArgument,
            ProtocolError::EvenStreamId,
        ));
    }

    match message.message_type {
        MESSAGE_TYPE_REQUEST => {
            let payload = message.payload.map_err(|err @ TooLongError { .. }| {
                status_from_err(Code::ResourceExhausted, err)
            })?;
            let request = mesh::payload::decode::<Request>(&payload)
                .map_err(|err| status_from_err(Code::InvalidArgument, err))?;

            tracing::debug!(
                stream_id = message.stream_id,
                service = %request.service,
                method = %request.method,
                timeout = request.timeout_nano / 1000 / 1000,
                "message",
            );

            Ok(request)
        }
        ty => Err(status_from_err(
            Code::InvalidArgument,
            ProtocolError::InvalidMessageType(ty),
        )),
    }
}

#[cfg(feature = "grpc")]
mod grpc {
    use super::Server;
    use crate::rpc::status_from_err;
    use crate::service::Code;
    use crate::service::GenericRpc;
    use crate::service::Status;
    use anyhow::Context as _;
    use futures::AsyncRead as _;
    use futures::AsyncWrite;
    use futures::FutureExt;
    use futures::StreamExt;
    use futures_concurrency::stream::Merge;
    use h2::RecvStream;
    use h2::server::SendResponse;
    use http::HeaderMap;
    use http::HeaderValue;
    use mesh::CancelContext;
    use pal_async::driver::Driver;
    use pal_async::socket::AsSockRef;
    use pal_async::socket::Listener;
    use pal_async::socket::PolledSocket;
    use prost::bytes::Bytes;
    use std::io::Read;
    use std::io::Write;
    use std::pin::Pin;
    use std::task::ready;
    use thiserror::Error;
    use unicycle::FuturesUnordered;

    #[derive(Debug, Error)]
    enum RequestError {
        #[error("http error")]
        Http(#[from] http::Error),
        #[error("http2 error")]
        H2(#[from] h2::Error),
        #[error("unreachable")]
        Status(http::StatusCode),
        #[error("invalid message header")]
        InvalidHeader,
    }

    impl From<http::StatusCode> for RequestError {
        fn from(status: http::StatusCode) -> Self {
            RequestError::Status(status)
        }
    }

    impl Server {
        /// Runs the server using the gRPC transport, listening on `listener` and servicing connections until
        /// `cancel`.
        pub async fn run_grpc(
            &mut self,
            driver: &(impl Driver + ?Sized),
            listener: impl Listener,
            cancel: mesh::OneshotReceiver<()>,
        ) -> anyhow::Result<()> {
            let mut listener = PolledSocket::new(driver, listener)?;
            let mut tasks = FuturesUnordered::new();
            let mut cancel = cancel.fuse();
            loop {
                let conn = futures::select! { // merge semantics
                    r = listener.accept().fuse() => r,
                    _ = tasks.next() => continue,
                    _ = cancel => break,
                };
                if let Ok(conn) = conn.and_then(|(conn, _)| PolledSocket::new(driver, conn)) {
                    tasks.push(async {
                        let _ = self.serve_connection_grpc(conn).await.map_err(|err| {
                            tracing::error!(
                                error = err.as_ref() as &dyn std::error::Error,
                                "connection error"
                            )
                        });
                    });
                }
            }
            Ok(())
        }

        /// Services a single, already-accepted connection using the gRPC
        /// (HTTP/2) protocol.
        ///
        /// This is useful when the caller owns the accept loop, for example to
        /// dispatch a connection to gRPC or another protocol based on a sniffed
        /// prefix byte.
        pub async fn serve_connection_grpc(
            &self,
            stream: PolledSocket<impl AsSockRef + Read + Write>,
        ) -> anyhow::Result<()> {
            struct Wrap<T>(T);

            impl<T: AsSockRef + Read> tokio::io::AsyncRead for Wrap<PolledSocket<T>> {
                fn poll_read(
                    self: Pin<&mut Self>,
                    cx: &mut std::task::Context<'_>,
                    buf: &mut tokio::io::ReadBuf<'_>,
                ) -> std::task::Poll<std::io::Result<()>> {
                    let n = ready!(
                        Pin::new(&mut self.get_mut().0).poll_read(cx, buf.initialize_unfilled())
                    )?;
                    buf.advance(n);
                    std::task::Poll::Ready(Ok(()))
                }
            }

            impl<T: AsSockRef + Write> tokio::io::AsyncWrite for Wrap<PolledSocket<T>> {
                fn poll_write(
                    self: Pin<&mut Self>,
                    cx: &mut std::task::Context<'_>,
                    buf: &[u8],
                ) -> std::task::Poll<Result<usize, std::io::Error>> {
                    Pin::new(&mut self.get_mut().0).poll_write(cx, buf)
                }

                fn poll_flush(
                    self: Pin<&mut Self>,
                    cx: &mut std::task::Context<'_>,
                ) -> std::task::Poll<Result<(), std::io::Error>> {
                    Pin::new(&mut self.get_mut().0).poll_flush(cx)
                }

                fn poll_shutdown(
                    self: Pin<&mut Self>,
                    cx: &mut std::task::Context<'_>,
                ) -> std::task::Poll<Result<(), std::io::Error>> {
                    Pin::new(&mut self.get_mut().0).poll_close(cx)
                }
            }

            let mut conn = h2::server::handshake(Wrap(stream))
                .await
                .context("failed http2 handshake")?;

            let mut tasks = FuturesUnordered::new();

            loop {
                enum Event<A, B> {
                    Accept(A),
                    Task(Result<(), B>),
                }

                let r = (
                    (&mut conn).map(Event::Accept),
                    (&mut tasks).map(Event::Task),
                )
                    .merge()
                    .next()
                    .await;

                let (req, mut resp) = match r {
                    None => break,
                    Some(Event::Task(r)) => {
                        r?;
                        continue;
                    }
                    Some(Event::Accept(r)) => r.context("failed http2 stream accept")?,
                };

                let task = async move {
                    match self.handle_request(req, &mut resp).await {
                        Err(RequestError::Status(status)) => {
                            tracing::debug!(status = status.as_u16(), "request error");
                            resp.send_response(
                                http::Response::builder().status(status).body(())?,
                                true,
                            )?;
                            Ok(())
                        }
                        r => r,
                    }
                };
                tasks.push(task);
            }

            std::future::poll_fn(|cx| conn.poll_closed(cx)).await?;
            Ok(())
        }

        async fn handle_request(
            &self,
            req: http::Request<RecvStream>,
            resp: &mut SendResponse<Bytes>,
        ) -> Result<(), RequestError> {
            tracing::debug!(url = %req.uri(), "rpc request");

            if req.method() != http::Method::POST {
                Err(http::StatusCode::METHOD_NOT_ALLOWED)?
            }
            let content_type = req.headers().get("content-type");
            match content_type.map(|v| v.as_bytes()) {
                Some(b"application/grpc" | b"application/grpc+proto") => {}
                _ => Err(http::StatusCode::UNSUPPORTED_MEDIA_TYPE)?,
            }

            let response =
                http::Response::builder().header("content-type", "application/grpc+proto");

            let ctx = if let Some(timeout) = req.headers().get("grpc-timeout") {
                let timeout = timeout
                    .to_str()
                    .map_err(|_| http::StatusCode::BAD_REQUEST)?;
                let mul = match timeout
                    .bytes()
                    .last()
                    .ok_or(http::StatusCode::BAD_REQUEST)?
                {
                    b'H' => std::time::Duration::from_hours(1),
                    b'M' => std::time::Duration::from_mins(1),
                    b'S' => std::time::Duration::from_secs(1),
                    b'm' => std::time::Duration::from_millis(1),
                    b'u' => std::time::Duration::from_micros(1),
                    b'n' => std::time::Duration::from_nanos(1),
                    _ => Err(http::StatusCode::BAD_REQUEST)?,
                };
                let timeout = timeout[..timeout.len() - 1]
                    .parse::<u32>()
                    .map_err(|_| http::StatusCode::BAD_REQUEST)?;
                CancelContext::new().with_timeout(mul * timeout)
            } else {
                CancelContext::new()
            };

            let (head, body) = req.into_parts();
            let path = head.uri.path();
            let path = path.strip_prefix('/').ok_or(http::StatusCode::NOT_FOUND)?;
            let (service, method) = path.split_once('/').ok_or(http::StatusCode::NOT_FOUND)?;

            // No returning HTTP status code errors after this point.
            let mut resp = resp.send_response(response.body(())?, false)?;

            let result = self.invoke_rpc(service, method, body, ctx).await?;

            let mut trailers = HeaderMap::new();
            match result {
                Ok(data) => {
                    tracing::debug!(service, method, "rpc success");

                    let mut buf = Vec::with_capacity(5 + data.len());
                    buf.push(0);
                    buf.extend(&(data.len() as u32).to_be_bytes());
                    buf.extend(data);
                    resp.send_data(buf.into(), false)?;
                    trailers.insert("grpc-status", const { HeaderValue::from_static("0") });
                }
                Err(status) => {
                    tracing::debug!(service, method, ?status, "rpc error");

                    trailers.insert("grpc-status", status.code.into());
                    trailers.insert(
                        "grpc-message",
                        urlencoding::encode(&status.message)
                            .into_owned()
                            .try_into()
                            .unwrap(),
                    );
                    trailers.insert(
                        "grpc-status-details-bin",
                        base64::Engine::encode(
                            &base64::engine::general_purpose::STANDARD,
                            prost::Message::encode_to_vec(&status),
                        )
                        .try_into()
                        .unwrap(),
                    );
                }
            }
            resp.send_trailers(trailers)?;
            Ok(())
        }

        async fn invoke_rpc(
            &self,
            service: &str,
            method: &str,
            mut body: RecvStream,
            ctx: CancelContext,
        ) -> Result<Result<Vec<u8>, Status>, RequestError> {
            let Some(service) = self.services.get(service) else {
                return Ok(Err(Status {
                    code: Code::Unimplemented.into(),
                    message: format!("unknown service {}", service),
                    details: Vec::new(),
                }));
            };

            // For now, only non-stream RPCs are supported, so read the first
            // message and ignore the rest.
            //
            // FUTURE: change the `GenericRpc` type to include channels for
            // streams.

            let mut buf = Vec::new();

            // Read data frames until the header is complete.
            while buf.len() < 5 {
                let data = body.data().await.ok_or(RequestError::InvalidHeader)??;
                buf.extend(&data);
                body.flow_control().release_capacity(data.len()).unwrap();
            }
            let hdr = buf.get(0..5).ok_or(RequestError::InvalidHeader)?;
            if hdr[0] != 0 {
                // Compression was not advertised as supported, so the client
                // should not send compressed messages.
                return Err(RequestError::InvalidHeader);
            }
            let len = u32::from_be_bytes(hdr[1..5].try_into().unwrap()) as usize;

            buf.drain(..5);
            while buf.len() < len {
                let data = body.data().await.ok_or(RequestError::InvalidHeader)??;
                buf.extend(&data);
                body.flow_control().release_capacity(data.len()).unwrap();
            }

            let (send, recv) = mesh::oneshot();

            let rpc = GenericRpc {
                method: method.to_owned(),
                data: buf,
                port: send.into(),
            };

            service.send((ctx, rpc));

            Ok(recv
                .await
                .unwrap_or_else(|err| Err(status_from_err(Code::Internal, err))))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Client;
    use crate::Server;
    use crate::client::ExistingConnection;
    use crate::message::MESSAGE_TYPE_REQUEST;
    use crate::message::write_message;
    use crate::rpc::status_from_err;
    use crate::service::Code;
    use crate::service::ServiceRpc;
    use futures::StreamExt;
    use futures::executor::block_on;
    use pal_async::DefaultPool;
    use pal_async::local::block_with_io;
    use pal_async::socket::PolledSocket;
    use test_with_tracing::test;

    #[expect(clippy::allow_attributes)]
    mod items {
        include!(concat!(env!("OUT_DIR"), "/ttrpc.example.v1.rs"));
    }

    #[test]
    fn client_server() {
        let (c, s) = unix_socket::UnixStream::pair().unwrap();
        let mut server = Server::new();
        let mut recv = server.add_service::<items::Example>();
        let server_thread = std::thread::spawn(move || {
            block_with_io(async |driver| server.run_single(&driver, s).await)
        });

        let client_thread = std::thread::spawn(move || {
            DefaultPool::run_with(async |driver| {
                let client = Client::new(
                    &driver,
                    ExistingConnection::new(PolledSocket::new(&driver, c).unwrap()),
                );
                let response = client
                    .call()
                    .start(
                        items::Example::Method1,
                        items::Method1Request {
                            foo: "abc".to_string(),
                            bar: "def".to_string(),
                        },
                    )
                    .await
                    .unwrap();

                assert_eq!(&response.foo, "abc123");
                assert_eq!(&response.bar, "def456");

                let status = client
                    .call()
                    .start_raw(items::Example::NAME, "unknown", Vec::new())
                    .await
                    .unwrap_err();

                assert_eq!(status.code, Code::Unimplemented as i32);

                client.shutdown().await;
            })
        });

        block_on(async {
            let (_, req) = recv.next().await.unwrap();
            match req {
                items::Example::Method1(input, resp) => {
                    assert_eq!(&input.foo, "abc");
                    assert_eq!(&input.bar, "def");
                    resp.send(Ok(items::Method1Response {
                        foo: input.foo + "123",
                        bar: input.bar + "456",
                    }));
                }
                _ => panic!("{:?}", req),
            }

            assert!(recv.next().await.is_none());
        });

        client_thread.join().unwrap();
        server_thread.join().unwrap().unwrap();
    }

    #[test]
    fn client_disconnect_cancels_pending_rpc_context() {
        let (c, s) = unix_socket::UnixStream::pair().unwrap();
        let mut server = Server::new();
        let mut recv = server.add_service::<items::Example>();
        let server_thread = std::thread::spawn(move || {
            block_with_io(async |driver| server.run_single(&driver, s).await)
        });

        let client_thread = std::thread::spawn(move || {
            DefaultPool::run_with(async |driver| {
                let mut sock = PolledSocket::new(&driver, c).unwrap();
                let req = crate::message::Request {
                    service: items::Example::NAME.to_string(),
                    method: "Method1".to_string(),
                    payload: mesh::payload::encode(items::Method1Request {
                        foo: "a".to_string(),
                        bar: "b".to_string(),
                    }),
                    timeout_nano: 0,
                    metadata: Vec::new(),
                };
                write_message(
                    &mut sock,
                    1,
                    MESSAGE_TYPE_REQUEST,
                    &mesh::payload::encode(req),
                )
                .await
                .unwrap();
                // Disconnect before the server responds so the pending RPC must observe cancellation.
                drop(sock);
            })
        });

        block_on(async {
            let (mut ctx, req) = recv.next().await.unwrap();
            match req {
                items::Example::Method1(_, resp) => {
                    let mut timeout_ctx =
                        mesh::CancelContext::new().with_timeout(std::time::Duration::from_secs(5));
                    timeout_ctx
                        .until_cancelled(ctx.cancelled())
                        .await
                        .expect("timed out waiting for client disconnect to cancel pending RPC");
                    resp.send(Err(status_from_err(
                        Code::Cancelled,
                        anyhow::anyhow!("client disconnected"),
                    )));
                }
                _ => panic!("{:?}", req),
            }
            assert!(recv.next().await.is_none());
        });

        client_thread.join().unwrap();
        server_thread.join().unwrap().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn client_half_close_receives_in_flight_reply() {
        use crate::message::MESSAGE_TYPE_RESPONSE;
        use crate::message::read_message;

        let (c, s) = unix_socket::UnixStream::pair().unwrap();
        let mut server = Server::new();
        let mut recv = server.add_service::<items::Example>();
        let server_thread = std::thread::spawn(move || {
            block_with_io(async |driver| server.run_single(&driver, s).await)
        });

        let (req_seen_tx, req_seen_rx) = std::sync::mpsc::channel::<()>();
        let client_thread = std::thread::spawn(move || {
            DefaultPool::run_with(async |driver| {
                let mut sock = PolledSocket::new(&driver, c).unwrap();
                let req = crate::message::Request {
                    service: items::Example::NAME.to_string(),
                    method: "Method1".to_string(),
                    payload: mesh::payload::encode(items::Method1Request {
                        foo: "half".to_string(),
                        bar: "close".to_string(),
                    }),
                    timeout_nano: 0,
                    metadata: Vec::new(),
                };
                write_message(
                    &mut sock,
                    1,
                    MESSAGE_TYPE_REQUEST,
                    &mesh::payload::encode(req),
                )
                .await
                .unwrap();
                sock.get().shutdown(std::net::Shutdown::Write).unwrap();
                // Wait until the server has observed the request and delayed slightly
                // before sending the response back to the half-closed client.
                req_seen_rx
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
                let msg = read_message(&mut sock).await.unwrap().unwrap();
                assert_eq!(msg.stream_id, 1);
                assert_eq!(msg.message_type, MESSAGE_TYPE_RESPONSE);
                let resp =
                    mesh::payload::decode::<crate::message::Response>(&msg.payload.unwrap())
                        .unwrap();
                match resp {
                    crate::message::Response::Payload(bytes) => {
                        let decoded = mesh::payload::decode::<items::Method1Response>(&bytes)
                            .unwrap();
                        assert_eq!(decoded.foo, "half-ok");
                        assert_eq!(decoded.bar, "close-ok");
                    }
                    crate::message::Response::Status(s) => panic!("unexpected status {s:?}"),
                }
                drop(sock);
            })
        });

        block_on(async {
            let (mut ctx, req) = recv.next().await.unwrap();
            match req {
                items::Example::Method1(input, resp) => {
                    req_seen_tx.send(()).unwrap();
                    std::thread::sleep(std::time::Duration::from_millis(50));
                    assert!(
                        !ctx.is_cancelled(),
                        "half-closed client must not cancel pending RPC"
                    );
                    resp.send(Ok(items::Method1Response {
                        foo: input.foo + "-ok",
                        bar: input.bar + "-ok",
                    }));
                }
                _ => panic!("{:?}", req),
            }
            assert!(recv.next().await.is_none());
        });

        client_thread.join().unwrap();
        server_thread.join().unwrap().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn client_half_close_then_full_close_cancels_pending_rpc_context() {
        let (c, s) = unix_socket::UnixStream::pair().unwrap();
        let mut server = Server::new();
        let mut recv = server.add_service::<items::Example>();
        let server_thread = std::thread::spawn(move || {
            block_with_io(async |driver| server.run_single(&driver, s).await)
        });

        let (req_seen_tx, req_seen_rx) = std::sync::mpsc::channel::<()>();
        let client_thread = std::thread::spawn(move || {
            DefaultPool::run_with(async |driver| {
                let mut sock = PolledSocket::new(&driver, c).unwrap();
                let req = crate::message::Request {
                    service: items::Example::NAME.to_string(),
                    method: "Method1".to_string(),
                    payload: mesh::payload::encode(items::Method1Request {
                        foo: "a".to_string(),
                        bar: "b".to_string(),
                    }),
                    timeout_nano: 0,
                    metadata: Vec::new(),
                };
                write_message(
                    &mut sock,
                    1,
                    MESSAGE_TYPE_REQUEST,
                    &mesh::payload::encode(req),
                )
                .await
                .unwrap();
                sock.get().shutdown(std::net::Shutdown::Write).unwrap();
                req_seen_rx
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
                // Full close after half-close must raise POLLHUP and cancel the RPC.
                drop(sock);
            })
        });

        block_on(async {
            let (mut ctx, req) = recv.next().await.unwrap();
            match req {
                items::Example::Method1(_, resp) => {
                    req_seen_tx.send(()).unwrap();
                    let mut timeout_ctx =
                        mesh::CancelContext::new().with_timeout(std::time::Duration::from_secs(5));
                    timeout_ctx
                        .until_cancelled(ctx.cancelled())
                        .await
                        .expect("timed out waiting for full close to cancel pending RPC");
                    resp.send(Err(status_from_err(
                        Code::Cancelled,
                        anyhow::anyhow!("client closed socket"),
                    )));
                }
                _ => panic!("{:?}", req),
            }
            assert!(recv.next().await.is_none());
        });

        client_thread.join().unwrap();
        server_thread.join().unwrap().unwrap();
    }
}
