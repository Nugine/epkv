use epkv_utils::chan;
use epkv_utils::clone;
use epkv_utils::codec;

use std::future::Future;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use futures_util::pin_mut;
use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;

use tokio::net::{TcpListener, TcpStream};
use tokio::spawn;
use tokio::sync::Semaphore;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{self, error::Elapsed};

use anyhow::{anyhow, Context as _, Result};
use tracing::{debug, error, warn};
use wgp::Working;

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcRequest<A> {
    pub rpc_id: u64,
    pub args: A,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcResponse<O> {
    pub rpc_id: u64,
    pub output: O,
}

pub struct RpcConnection<A, O> {
    next_rpc_id: AtomicU64,
    op_tx: mpsc::Sender<Operation<A, O>>,
    task: JoinHandle<()>,
}

enum Operation<A, O> {
    Send {
        rpc_id: u64,
        callback: oneshot::Sender<O>,
        args: A,
    },
    Cancel {
        rpc_id: u64,
    },
}

impl<A, O> Drop for RpcConnection<A, O> {
    #[inline]
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RpcClientConfig {
    pub max_frame_length: usize,
    pub op_chan_size: usize,
    pub forward_chan_size: usize,
}

impl<A, O> RpcConnection<A, O>
where
    A: Serialize + Send + Unpin + 'static,
    O: DeserializeOwned + Send + Unpin + 'static,
{
    #[inline]
    pub async fn connect(remote_addr: SocketAddr, config: &RpcClientConfig) -> Result<Self> {
        let max_frame_length = config.max_frame_length;
        let op_chan_size = config.op_chan_size;
        let forward_chan_size = config.forward_chan_size;

        let conn = <Connection<A, O>>::connect(remote_addr, max_frame_length, forward_chan_size).await?;
        let (op_tx, op_rx) = mpsc::channel(op_chan_size);
        let task = spawn(conn.driver(op_rx));
        let next_rpc_id = AtomicU64::new(1);
        Ok(Self { next_rpc_id, op_tx, task })
    }

    #[inline]
    pub async fn call(&self, args: A) -> Result<O> {
        let (callback, handle) = oneshot::channel();
        let rpc_id = self.next_rpc_id.fetch_add(1, Ordering::Relaxed);
        let op = Operation::Send { rpc_id, args, callback };

        let send_result = chan::send(&self.op_tx, op).await;
        assert!(send_result.is_ok(), "driver task has been dropped");

        let cancel_guard = scopeguard::guard((), |()| {
            let tx = self.op_tx.clone();
            spawn(async move {
                let op = Operation::Cancel { rpc_id };
                let _ = chan::send(&tx, op).await;
            });
        });

        let result = match handle.await {
            Ok(output) => Ok(output),
            Err(_) => Err(anyhow!("rpc failed: no response")),
        };

        scopeguard::ScopeGuard::into_inner(cancel_guard);

        result
    }

    #[inline]
    pub async fn call_timeout(self, args: A, timeout: Duration) -> Result<Result<O>, Elapsed> {
        time::timeout(timeout, self.call(args)).await
    }
}

struct Connection<A, O> {
    write_tx: mpsc::Sender<RpcRequest<A>>,
    read_rx: mpsc::Receiver<RpcResponse<O>>,
    forward_read_task: JoinHandle<()>,
    forward_write_task: JoinHandle<()>,
}

impl<A, O> Drop for Connection<A, O> {
    fn drop(&mut self) {
        self.forward_read_task.abort();
        self.forward_write_task.abort();
    }
}

type InflightMap<O> = fnv::FnvHashMap<u64, oneshot::Sender<O>>;

impl<A, O> Connection<A, O>
where
    A: Serialize + Send + Unpin + 'static,
    O: DeserializeOwned + Send + Unpin + 'static,
{
    async fn connect(
        remote_addr: SocketAddr,
        max_frame_length: usize,
        forward_chan_size: usize,
    ) -> Result<Self> {
        let stream = TcpStream::connect(remote_addr)
            .await
            .with_context(|| format!("failed to connect to remote addr {remote_addr}"))?;

        let (reader, writer) = stream.into_split();
        let remote_rx = codec::bytes_stream(reader, max_frame_length);
        let remote_tx = codec::bytes_sink(writer, max_frame_length);

        let (read_tx, read_rx) = mpsc::channel::<RpcResponse<O>>(forward_chan_size);
        let (write_tx, write_rx) = mpsc::channel::<RpcRequest<A>>(forward_chan_size);

        let forward_read_task: JoinHandle<()> = spawn(async move {
            pin_mut!(remote_rx);
            'forward: while let Some(result) = remote_rx.next().await {
                let bytes = match result {
                    Ok(x) => x,
                    Err(err) => {
                        error!(?err, "remote rx error");
                        break 'forward;
                    }
                };
                let item = match codec::deserialize_owned::<RpcResponse<O>>(&bytes) {
                    Ok(x) => x,
                    Err(err) => {
                        error!(?err, "codec deserialize error");
                        break 'forward;
                    }
                };
                match chan::send(&read_tx, item).await {
                    Ok(()) => {}
                    Err(_) => {
                        error!("forward_read: read_tx error");
                        break 'forward;
                    }
                };
            }
        });

        let forward_write_task: JoinHandle<()> = spawn(async move {
            let mut write_rx = write_rx;
            pin_mut!(remote_tx);
            'forward: while let Some(mut item) = write_rx.recv().await {
                loop {
                    let bytes = match codec::serialize(&item) {
                        Ok(x) => x,
                        Err(err) => {
                            error!(?err, "codec serialize error");
                            break 'forward;
                        }
                    };
                    match remote_tx.feed(bytes).await {
                        Ok(()) => {}
                        Err(err) => {
                            error!(?err, "remote tx feed error");
                            break 'forward;
                        }
                    }
                    match write_rx.try_recv() {
                        Ok(x) => item = x,
                        Err(_) => break,
                    }
                }
                match remote_tx.flush().await {
                    Ok(()) => {}
                    Err(err) => {
                        error!(?err, "remote tx flush error");
                        break 'forward;
                    }
                }
            }
        });

        Ok(Connection { write_tx, read_rx, forward_read_task, forward_write_task })
    }

    #[allow(clippy::arithmetic_side_effects)] // tokio::select!
    async fn driver(mut self, mut op_rx: mpsc::Receiver<Operation<A, O>>) {
        let mut inflight = <InflightMap<O>>::default();
        let read_rx = &mut self.read_rx;
        let write_tx = &mut self.write_tx;
        'drive: loop {
            tokio::select! {
                Some(mut op) = op_rx.recv() => loop {
                    match op {
                        Operation::Send {
                            rpc_id,
                            args,
                            callback,
                        } => {
                            inflight.insert(rpc_id, callback);
                            let req = RpcRequest { rpc_id, args };
                            if chan::send(write_tx, req).await.is_err() {
                                error!("client_driver: write_tx send error");
                                break 'drive
                            }
                        }
                        Operation::Cancel {
                            rpc_id,
                        } => {
                            inflight.remove(&rpc_id);
                        }
                    }
                    match op_rx.try_recv() {
                        Ok(x) => op = x,
                        Err(_) => break,
                    }
                },
                Some(mut res) = read_rx.recv() => loop {
                    match inflight.remove(&res.rpc_id) {
                        Some(callback) => {
                            let _ = callback.send(res.output);
                        }
                        None => {
                            warn!(rpc_id=?res.rpc_id, "unexpected rpc id");
                            break;
                        }
                    }
                    match read_rx.try_recv() {
                        Ok(x) => res = x,
                        Err(_) => break,
                    }
                },
                else => break 'drive,
            }
        }
    }
}

pub trait Service<A: Send + 'static>: Send + Sync + 'static {
    type Output: Send + 'static;

    type Future<'a>: Future<Output = Result<Self::Output>> + Send + 'a;

    fn call<'a>(self: &'a Arc<Self>, args: A) -> Self::Future<'a>;

    fn needs_stop(&self) -> bool;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcServerConfig {
    pub max_frame_length: usize,
    pub max_task_num: usize,
}

#[inline]
pub async fn serve<S, A>(
    service: Arc<S>,
    listener: TcpListener,
    config: RpcServerConfig,
    working: Working,
) -> Result<()>
where
    S: Service<A>,
    A: DeserializeOwned + Send + 'static,
    <S as Service<A>>::Output: Serialize + Send + 'static,
{
    let max_frame_length = config.max_frame_length;
    let limit = Arc::new(Semaphore::new(config.max_task_num));

    loop {
        if service.needs_stop() {
            break;
        }

        let (tcp, _) = listener.accept().await.inspect_err(|err| error!(?err, "tcp accept error"))?;

        if service.needs_stop() {
            break;
        }

        let (reader, writer) = tcp.into_split();
        let mut remote_stream = codec::bytes_stream(reader, max_frame_length);
        let mut remote_sink = codec::bytes_sink(writer, max_frame_length);
        let (res_tx, mut res_rx) = mpsc::unbounded_channel::<RpcResponse<S::Output>>();

        {
            clone!(service, working, limit);
            spawn(async move {
                if service.needs_stop() {
                    return Ok(());
                }
                while let Some(result) = remote_stream.next().await {
                    if service.needs_stop() {
                        debug!("drop rpc request because of waiting shutdown");
                        break;
                    }

                    let bytes = result.inspect_err(|err| error!(?err, "remote rx error"))?;

                    let permit = limit.clone().acquire_owned().await.unwrap();

                    clone!(service, working, res_tx);
                    spawn(async move {
                        let req = match codec::deserialize_owned::<RpcRequest<A>>(&bytes) {
                            Ok(req) => req,
                            Err(err) => {
                                error!(?err, "codec deserialize error");
                                return;
                            }
                        };

                        let output = match service.call(req.args).await {
                            Ok(output) => output,
                            Err(err) => {
                                error!(?err, "service call failed");
                                return;
                            }
                        };

                        let rpc_id = req.rpc_id;
                        let res = RpcResponse { rpc_id, output };
                        let _ = res_tx.send(res);

                        drop(working);
                        drop(permit);
                    });
                }

                anyhow::Result::<()>::Ok(())
            })
        };

        clone!(working);
        spawn(async move {
            while let Some(res) = res_rx.recv().await {
                let bytes = match codec::serialize(&res) {
                    Ok(x) => x,
                    Err(err) => {
                        error!(?err, "codec serialize error");
                        continue;
                    }
                };
                match remote_sink.send(bytes).await {
                    Ok(()) => {}
                    Err(err) => {
                        error!(?err, "remote tx error");
                        break;
                    }
                }
            }
            drop(working);
        });
    }

    Ok(())
}
