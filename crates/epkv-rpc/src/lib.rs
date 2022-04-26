use epkv_utils::codec::{self, bytes_sink, bytes_stream};

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use futures_util::pin_mut;
use futures_util::sink::SinkExt;
use futures_util::stream::StreamExt;

use tokio::net::TcpStream;
use tokio::spawn;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{self, error::Elapsed};

use anyhow::{anyhow, Context as _, Result};
use tracing::{error, warn};

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
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl<A, O> RpcConnection<A, O>
where
    A: Serialize + Send + Unpin + 'static,
    O: DeserializeOwned + Send + Unpin + 'static,
{
    pub async fn connect(
        remote_addr: SocketAddr,
        max_frame_length: usize,
        op_chan_size: usize,
        forward_chan_size: usize,
    ) -> Result<Self> {
        let conn = <Connection<A, O>>::connect(remote_addr, max_frame_length, forward_chan_size).await?;
        let (op_tx, op_rx) = mpsc::channel(op_chan_size);
        let task = spawn(conn.driver(op_rx));
        let next_rpc_id = AtomicU64::new(1);
        Ok(Self { next_rpc_id, op_tx, task })
    }

    pub async fn call(&self, args: A) -> Result<O> {
        let (callback, handle) = oneshot::channel();
        let rpc_id = self.next_rpc_id.fetch_add(1, Ordering::Relaxed);
        let op = Operation::Send { rpc_id, args, callback };

        let send_result = self.op_tx.send(op).await;
        assert!(send_result.is_ok(), "driver task has been dropped");

        let cancel_guard = scopeguard::guard((), |()| {
            let tx = self.op_tx.clone();
            spawn(async move {
                let op = Operation::Cancel { rpc_id };
                let _ = tx.send(op).await;
            });
        });

        let result = match handle.await {
            Ok(output) => Ok(output),
            Err(_) => Err(anyhow!("rpc failed: no response")),
        };

        let _ = scopeguard::ScopeGuard::into_inner(cancel_guard);

        result
    }

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
            .with_context(|| format!("failed to connect to remote address {remote_addr}"))?;

        let (reader, writer) = stream.into_split();
        let remote_rx: _ = bytes_stream(reader, max_frame_length);
        let remote_tx: _ = bytes_sink(writer, max_frame_length);

        let (read_tx, read_rx): _ = mpsc::channel::<RpcResponse<O>>(forward_chan_size);
        let (write_tx, write_rx): _ = mpsc::channel::<RpcRequest<A>>(forward_chan_size);

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
                let item = match codec::deserialize_owned::<RpcResponse<O>>(&*bytes) {
                    Ok(x) => x,
                    Err(err) => {
                        error!(?err, "codec deserialize error");
                        break 'forward;
                    }
                };
                match read_tx.send(item).await {
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

    #[allow(clippy::integer_arithmetic)] // tokio::select!
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
                            if write_tx.send(req).await.is_err() {
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
