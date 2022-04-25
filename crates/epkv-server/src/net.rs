use crate::config::NetworkConfig;

use epkv_epaxos::id::ReplicaId;
use epkv_epaxos::msg::Message;
use epkv_epaxos::net::Network;

use epkv_utils::codec;
use epkv_utils::lock::{with_read_lock, with_write_lock};
use epkv_utils::vecmap::VecMap;
use epkv_utils::vecset::VecSet;

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::join_all;
use futures_util::{Sink, SinkExt, Stream, StreamExt, TryStreamExt};
use parking_lot::RwLock;
use serde::Serialize;
use tokio::net::{TcpListener, TcpStream};
use tokio::spawn;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_util::codec::LengthDelimitedCodec;
use tracing::{debug, error};

pub struct Connection {
    tx: mpsc::Sender<Bytes>,
    task: Option<JoinHandle<()>>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        if let Some(ref task) = self.task {
            task.abort();
        }
    }
}

impl Listener {
    pub async fn recv(&mut self) -> Option<Bytes> {
        self.rx.recv().await
    }
}

pub struct Listener {
    rx: mpsc::Receiver<Bytes>,
    task: Option<JoinHandle<()>>,
}

impl Drop for Listener {
    fn drop(&mut self) {
        if let Some(ref task) = self.task {
            task.abort();
        }
    }
}

pub struct TcpNetwork {
    conns: RwLock<VecMap<ReplicaId, Connection>>,
    config: NetworkConfig,
}

impl<C> Network<C> for TcpNetwork
where
    C: Serialize,
{
    fn broadcast(&self, targets: VecSet<ReplicaId>, msg: Message<C>) {
        if targets.is_empty() {
            return;
        }
        if targets.len() == 1 {
            return self.send_one(targets.as_slice()[0], msg);
        }

        let msg_bytes = codec::serialize(&msg).expect("message should be able to be serialized");

        let mut txs = Vec::with_capacity(targets.len());
        with_read_lock(&self.conns, |conns: _| {
            conns.apply(&targets, |conn| txs.push(conn.tx.clone()));
        });

        spawn(async move {
            let futures: _ = txs.iter().map(|tx: _| tx.send(msg_bytes.clone()));
            let _ = join_all(futures).await;
        });
    }

    fn send_one(&self, target: ReplicaId, msg: Message<C>) {
        let msg_bytes = codec::serialize(&msg).expect("message should be able to be serialized");
        let tx: _ = with_read_lock(&self.conns, |conns: _| {
            conns.get(&target).map(|conn| conn.tx.clone())
        });
        if let Some(tx) = tx {
            spawn(async move {
                let _ = tx.send(msg_bytes).await;
            });
        }
    }

    fn register_peer(&self, rid: ReplicaId, address: SocketAddr) {
        with_write_lock(&self.conns, |conns: _| {
            let _ = conns.init_with(rid, || Self::spawn_connector(address, &self.config));
        })
    }
}

fn bytes_stream<R>(
    reader: R,
    max_frame_length: usize,
) -> impl Stream<Item = io::Result<Bytes>> + Send + Unpin + 'static
where
    R: tokio::io::AsyncRead + Send + Unpin + 'static,
{
    LengthDelimitedCodec::builder()
        .max_frame_length(max_frame_length)
        .new_read(reader)
        .map_ok(|bytes| bytes.freeze())
}

fn bytes_sink<W>(
    writer: W,
    max_frame_length: usize,
) -> impl Sink<Bytes, Error = io::Error> + Send + Unpin + 'static
where
    W: tokio::io::AsyncWrite + Send + Unpin + 'static,
{
    LengthDelimitedCodec::builder().max_frame_length(max_frame_length).new_write(writer)
}

impl TcpNetwork {
    pub fn spawn_connector(addr: SocketAddr, config: &NetworkConfig) -> Connection {
        let chan_size = config.outbound_chan_size;
        let reconnect_interval = Duration::from_micros(config.reconnect_interval_us);
        let max_frame_length = config.max_frame_length;

        let (tx, rx) = mpsc::channel::<Bytes>(chan_size);

        let task = spawn(async move {
            let mut rx = rx;

            'drive: loop {
                let mut sink = loop {
                    match TcpStream::connect(addr).await {
                        Ok(tcp) => {
                            break bytes_sink(tcp, max_frame_length);
                        }
                        Err(err) => {
                            error!(?err, ?addr, "failed to reconnect");
                            sleep(reconnect_interval).await;
                        }
                    }
                };
                debug!(?addr, "tcp connection established");

                'forward: loop {
                    let mut item = match rx.recv().await {
                        Some(x) => x,
                        None => break 'drive,
                    };

                    loop {
                        match sink.feed(item).await {
                            Ok(()) => {}
                            Err(err) => {
                                error!(?err, "tcp connection error");
                                break 'forward;
                            }
                        }

                        match rx.try_recv() {
                            Ok(x) => item = x,
                            Err(_) => break,
                        }
                    }

                    match sink.flush().await {
                        Ok(()) => {}
                        Err(err) => {
                            error!(?err, "tcp connection error");
                            break 'forward;
                        }
                    }
                }
            }
        });

        Connection { tx, task: Some(task) }
    }

    pub async fn spawn_listener(addr: SocketAddr, config: &NetworkConfig) -> io::Result<Listener> {
        let listener = TcpListener::bind(addr).await?;

        let chan_size = config.inbound_chan_size;
        let max_frame_length = config.max_frame_length;

        let (tx, rx) = mpsc::channel::<Bytes>(chan_size);

        let task = spawn(async move {
            loop {
                let (tcp, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(err) => {
                        error!(?err, "tcp listener error");
                        break;
                    }
                };
                if tx.is_closed() {
                    break;
                }
                let mut stream = bytes_stream(tcp, max_frame_length);
                let tx = tx.clone();
                spawn(async move {
                    while let Some(result) = stream.next().await {
                        let item = match result {
                            Ok(x) => x,
                            Err(err) => {
                                error!(?err, "tcp stream error");
                                break;
                            }
                        };
                        match tx.send(item).await {
                            Ok(()) => {}
                            Err(_) => break,
                        }
                    }
                });
            }
        });

        Ok(Listener { rx, task: Some(task) })
    }
}
