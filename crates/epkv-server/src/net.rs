use crate::config::NetworkConfig;

use epkv_epaxos::id::ReplicaId;
use epkv_epaxos::msg::Message;
use epkv_epaxos::net::Network;

use epkv_utils::codec::{self, bytes_sink, bytes_stream};
use epkv_utils::lock::{with_read_lock, with_write_lock};
use epkv_utils::vecmap::VecMap;
use epkv_utils::vecset::VecSet;

use std::io;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use futures_util::future::join_all;
use futures_util::{SinkExt, StreamExt};
use parking_lot::RwLock;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::net::{TcpListener, TcpStream};
use tokio::spawn;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::sleep;
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

impl<C> Listener<C>
where
    C: DeserializeOwned,
{
    pub async fn recv(&mut self) -> Option<Result<Message<C>>> {
        let bytes = self.rx.recv().await?;
        Some(codec::deserialize_owned(&*bytes))
    }
}

pub struct Listener<C> {
    rx: mpsc::Receiver<Bytes>,
    task: Option<JoinHandle<()>>,
    _marker: PhantomData<fn() -> C>,
}

impl<C> Drop for Listener<C> {
    fn drop(&mut self) {
        if let Some(ref task) = self.task {
            task.abort();
        }
    }
}

pub struct TcpNetwork<C> {
    conns: RwLock<VecMap<ReplicaId, Connection>>,
    config: NetworkConfig,
    _marker: PhantomData<fn(C) -> C>,
}

impl<C> Network<C> for TcpNetwork<C>
where
    C: Serialize + 'static,
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

    fn register_peer(&self, rid: ReplicaId, addr: SocketAddr) {
        with_write_lock(&self.conns, |conns: _| {
            let _ = conns.init_with(rid, || Self::spawn_connector(addr, &self.config));
        })
    }
}

impl<C> TcpNetwork<C> {
    #[must_use]
    pub fn new(config: &NetworkConfig) -> Self {
        Self {
            conns: RwLock::new(VecMap::new()),
            config: config.clone(),
            _marker: PhantomData,
        }
    }

    #[must_use]
    pub fn spawn_connector(addr: SocketAddr, config: &NetworkConfig) -> Connection {
        let chan_size = config.outbound_chan_size;
        let reconnect_interval = Duration::from_micros(config.reconnect_interval_us);
        let max_frame_length = config.max_frame_length;

        let (tx, rx) = mpsc::channel::<Bytes>(chan_size);

        let task = spawn(async move {
            let mut rx = rx;

            'drive: loop {
                let mut sink = loop {
                    // FIXME: check rx.is_closed()
                    // https://github.com/tokio-rs/tokio/issues/4638
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

    pub async fn spawn_listener(addr: SocketAddr, config: &NetworkConfig) -> io::Result<Listener<C>> {
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

        Ok(Listener { rx, task: Some(task), _marker: PhantomData })
    }
}
