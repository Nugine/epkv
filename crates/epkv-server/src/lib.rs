#![forbid(unsafe_code)]
#![deny(
    clippy::all,
    clippy::as_conversions,
    clippy::float_arithmetic,
    clippy::integer_arithmetic,
    clippy::must_use_candidate
)]
#![warn(clippy::todo, clippy::dbg_macro)]
//
#![feature(generic_associated_types)]
#![feature(type_alias_impl_trait)]

pub mod config;
pub mod net;

// -----------------------------------------------------------------------------

use self::config::Config;
use self::net::{Listener, TcpNetwork};

use epkv_epaxos::replica::{Replica, ReplicaMeta};
use epkv_protocol::{cs, rpc, sm};

use epkv_rocks::cmd::{BatchedCommand, Command, CommandKind, CommandNotify, Del, Get, MutableCommand, Set};
use epkv_rocks::data_db::DataDb;
use epkv_rocks::log_db::LogDb;

use epkv_utils::asc::Asc;
use epkv_utils::atomic_flag::AtomicFlag;
use epkv_utils::lock::with_mutex;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use parking_lot::Mutex as SyncMutex;
use tokio::net::TcpListener;
use tokio::spawn;
use tokio::sync::mpsc;
use tracing::debug;
use tracing::error;
use wgp::WaitGroup;

type EpkvReplica = Replica<BatchedCommand, Arc<LogDb>, Arc<DataDb>, TcpNetwork<BatchedCommand>>;

pub struct Server {
    replica: Arc<EpkvReplica>,

    config: Config,

    cmd_tx: mpsc::Sender<Command>,

    metrics: SyncMutex<Metrics>,

    is_waiting_shutdown: AtomicFlag,
    waitgroup: WaitGroup,
}

#[derive(Clone)]
struct Metrics {
    single_cmd_count: u64,
    batched_cmd_count: u64,
}

impl Server {
    pub async fn run(config: Config) -> Result<()> {
        let replica = {
            let log_store = LogDb::new(&config.log_db.path)?;
            let data_store = DataDb::new(&config.data_db.path)?;
            let net = TcpNetwork::new(&config.network);

            let (rid, epoch, peers): _ = {
                let remote_addr = config.server.monitor_addr;
                let monitor = sm::Monitor::connect(remote_addr, &config.rpc_client).await?;

                let public_peer_addr = config.server.public_peer_addr;
                let output = monitor.register(sm::RegisterArgs { public_peer_addr }).await?;

                debug!(?output, "monitor register");

                (output.rid, output.epoch, output.peers)
            };

            let public_peer_addr = config.server.public_peer_addr;

            let meta = ReplicaMeta {
                rid,
                epoch,
                peers,
                public_peer_addr,
                config: config.replica.clone(),
            };

            EpkvReplica::new(meta, log_store, data_store, net).await?
        };

        let (cmd_tx, cmd_rx) = mpsc::channel(config.batching.chan_size);

        let server = {
            let metrics = SyncMutex::new(Metrics { single_cmd_count: 0, batched_cmd_count: 0 });

            let is_waiting_shutdown = AtomicFlag::new(false);
            let waitgroup = WaitGroup::new();

            Arc::new(Server {
                replica,
                config,
                cmd_tx,
                metrics,
                is_waiting_shutdown,
                waitgroup,
            })
        };

        let mut bg_tasks = Vec::new();

        {
            let this = Arc::clone(&server);

            let listener = {
                let addr = this.config.server.listen_peer_addr;
                TcpListener::bind(addr).await.with_context(|| format!("failed to bind to {addr}"))?
            };

            let listener = TcpNetwork::spawn_listener(listener, &this.config.network);
            bg_tasks.push(spawn(this.serve_peer(listener)));
        }

        {
            let this = Arc::clone(&server);
            let listener = TcpListener::bind(this.config.server.listen_client_addr).await?;
            bg_tasks.push(spawn(this.serve_client(listener)));
        }

        {
            let this = Arc::clone(&server);
            bg_tasks.push(spawn(this.cmd_batcher(cmd_rx)))
        }

        {
            let this = Arc::clone(&server);
            bg_tasks.push(spawn(this.interval_probe_rtt()));
        }

        {
            let this = Arc::clone(&server);
            bg_tasks.push(spawn(this.interval_clear_key_map()));
        }

        {
            let this = Arc::clone(&server);
            bg_tasks.push(spawn(this.interval_save_bounds()));
        }

        {
            let this = Arc::clone(&server);
            bg_tasks.push(spawn(this.interval_broadcast_bounds()));
        }

        {
            tokio::signal::ctrl_c().await?;
        }

        {
            server.is_waiting_shutdown.set(true);
            for task in &bg_tasks {
                task.abort();
            }
            drop(bg_tasks);

            let task_count = server.waitgroup.count();
            debug!(?task_count, "waiting running tasks");
            server.waitgroup.wait().await;
        }

        server.shutdown().await?;

        Ok(())
    }

    async fn serve_peer(self: Arc<Self>, mut listener: Listener<BatchedCommand>) -> Result<()> {
        {
            let this = Arc::clone(&self);
            spawn(async move {
                if let Err(err) = this.replica.run_sync_known().await {
                    error!(?err, "run_sync_known");
                }
            });
        }

        {
            let this = Arc::clone(&self);
            spawn(async move {
                if let Err(err) = this.replica.run_join().await {
                    error!(?err, "run_join")
                }
            });
        }

        while let Some(result) = listener.recv().await {
            if self.is_waiting_shutdown.get() {
                break;
            }
            match result {
                Ok(msg) => {
                    let this = Arc::clone(&self);
                    spawn(async move {
                        if let Err(err) = this.replica.handle_message(msg).await {
                            error!(?err, "handle_message");
                        }
                    });
                }
                Err(err) => {
                    error!(?err, "listener recv");
                    continue;
                }
            }
        }

        Ok(())
    }

    async fn serve_client(self: Arc<Self>, listener: TcpListener) -> Result<()> {
        let config = self.config.rpc_server.clone();
        let working = self.waitgroup.working();
        rpc::serve(self, listener, config, working).await
    }

    async fn shutdown(self: Arc<Self>) -> Result<()> {
        // do what?
        Ok(())
    }
}

type HandleClientRpcFuture<'a> = impl Future<Output = Result<cs::Output>> + Send + 'a;

impl rpc::Service<cs::Args> for Server {
    type Output = cs::Output;

    type Future<'a> = HandleClientRpcFuture<'a>;

    fn call<'a>(self: &'a Arc<Self>, args: cs::Args) -> Self::Future<'a> {
        self.handle_client_rpc(args)
    }

    fn needs_stop(&self) -> bool {
        self.is_waiting_shutdown.get()
    }
}

impl Server {
    async fn handle_client_rpc(self: &Arc<Self>, args: cs::Args) -> Result<cs::Output> {
        match args {
            cs::Args::Get(args) => self.client_rpc_get(args).await.map(cs::Output::Get),
            cs::Args::Set(args) => self.client_rpc_set(args).await.map(cs::Output::Set),
            cs::Args::Del(args) => self.client_rpc_del(args).await.map(cs::Output::Del),
            cs::Args::GetMetrics(args) => self.client_rpc_get_metrics(args).await.map(cs::Output::GetMetrics),
        }
    }

    async fn client_rpc_get(self: &Arc<Self>, args: cs::GetArgs) -> Result<cs::GetOutput> {
        let (tx, mut rx) = mpsc::channel(1);
        let cmd = Command::from_mutable(MutableCommand {
            kind: CommandKind::Get(Get { key: args.key, tx: Some(tx) }),
            notify: None,
        });
        self.cmd_tx.send(cmd).await.map_err(|_| anyhow!("failed to send command"))?;
        match rx.recv().await {
            Some(value) => Ok(cs::GetOutput { value }),
            None => Err(anyhow!("failed to receive command output")),
        }
    }

    async fn client_rpc_set(self: &Arc<Self>, args: cs::SetArgs) -> Result<cs::SetOutput> {
        let notify = Asc::new(CommandNotify::new());
        let cmd = Command::from_mutable(MutableCommand {
            kind: CommandKind::Set(Set { key: args.key, value: args.value }),
            notify: Some(Asc::clone(&notify)),
        });
        self.cmd_tx.send(cmd).await.map_err(|_| anyhow!("failed to send command"))?;
        notify.wait_committed().await;
        Ok(cs::SetOutput {})
    }

    async fn client_rpc_del(self: &Arc<Self>, args: cs::DelArgs) -> Result<cs::DelOutput> {
        let notify = Asc::new(CommandNotify::new());
        let cmd = Command::from_mutable(MutableCommand {
            kind: CommandKind::Del(Del { key: args.key }),
            notify: Some(Asc::clone(&notify)),
        });
        self.cmd_tx.send(cmd).await.map_err(|_| anyhow!("failed to send command"))?;
        notify.wait_committed().await;
        Ok(cs::DelOutput {})
    }

    async fn client_rpc_get_metrics(self: &Arc<Self>, _: cs::GetMetricsArgs) -> Result<cs::GetMetricsOutput> {
        let network = self.replica.network().metrics();
        let server = with_mutex(&self.metrics, |m: _| m.clone());

        Ok(cs::GetMetricsOutput {
            network_msg_total_size: network.msg_total_size,
            network_msg_count: network.msg_count,
            server_single_cmd_count: server.single_cmd_count,
            server_batched_cmd_count: server.batched_cmd_count,
        })
    }
}

impl Server {
    async fn cmd_batcher(self: Arc<Self>, mut rx: mpsc::Receiver<Command>) -> Result<()> {
        let initial_capacity = self.config.batching.batch_initial_capacity;
        let max_size = self.config.batching.batch_max_size;

        loop {
            if self.is_waiting_shutdown.get() {
                break;
            }

            let cmd = match rx.recv().await {
                Some(cmd) => cmd,
                None => break,
            };

            let mut batch = Vec::<Command>::with_capacity(initial_capacity);
            batch.push(cmd);

            loop {
                if batch.len() >= max_size {
                    break;
                }

                match rx.try_recv() {
                    Ok(cmd) => batch.push(cmd),
                    Err(_) => break,
                }
            }

            {
                let cnt = u64::try_from(batch.len()).unwrap();
                with_mutex(&self.metrics, |m| {
                    m.single_cmd_count = m.single_cmd_count.wrapping_add(cnt);
                    m.batched_cmd_count = m.batched_cmd_count.wrapping_add(1);
                });
            }

            let this = Arc::clone(&self);
            spawn(async move {
                let cmd = BatchedCommand::from_vec(batch);
                if let Err(err) = this.handle_batched_command(cmd).await {
                    error!(?err, "handle batched command")
                }
            });
        }

        Ok(())
    }

    async fn handle_batched_command(self: &Arc<Self>, cmd: BatchedCommand) -> Result<()> {
        self.replica.run_propose(cmd).await
    }
}

impl Server {
    async fn interval_probe_rtt(self: Arc<Self>) -> Result<()> {
        let mut interval = {
            let duration = Duration::from_micros(self.config.interval.probe_rtt_interval_us);
            tokio::time::interval(duration)
        };

        loop {
            interval.tick().await;

            if self.is_waiting_shutdown.get() {
                break;
            }

            let this = Arc::clone(&self);
            spawn(async move {
                if let Err(err) = this.replica.run_probe_rtt().await {
                    error!(?err, "interval probe rtt")
                }
            });
        }

        Ok(())
    }

    async fn interval_clear_key_map(self: Arc<Self>) -> Result<()> {
        let mut interval = {
            let duration = Duration::from_micros(self.config.interval.clear_key_map_interval_us);
            tokio::time::interval(duration)
        };

        loop {
            interval.tick().await;

            if self.is_waiting_shutdown.get() {
                break;
            }

            let this = Arc::clone(&self);
            spawn(async move {
                this.replica.run_clear_key_map().await;
            });
        }

        Ok(())
    }

    async fn interval_save_bounds(self: Arc<Self>) -> Result<()> {
        let mut interval = {
            let duration = Duration::from_micros(self.config.interval.save_bounds_interval_us);
            tokio::time::interval(duration)
        };

        loop {
            interval.tick().await;

            if self.is_waiting_shutdown.get() {
                break;
            }

            let this = Arc::clone(&self);
            spawn(async move {
                if let Err(err) = this.replica.run_save_bounds().await {
                    error!(?err, "interval save bounds")
                }
            });
        }

        Ok(())
    }

    async fn interval_broadcast_bounds(self: Arc<Self>) -> Result<()> {
        let mut interval = {
            let duration = Duration::from_micros(self.config.interval.broadcast_bounds_interval_us);
            tokio::time::interval(duration)
        };

        loop {
            interval.tick().await;

            if self.is_waiting_shutdown.get() {
                break;
            }

            let this = Arc::clone(&self);
            spawn(async move {
                if let Err(err) = this.replica.run_broadcast_bounds().await {
                    error!(?err, "interval broadcast bounds")
                }
            });
        }

        Ok(())
    }
}
