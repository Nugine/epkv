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
use epkv_rocks::cmd::BatchedCommand;
use epkv_rocks::data_db::DataDb;
use epkv_rocks::log_db::LogDb;
use epkv_utils::atomic_flag::AtomicFlag;

use std::future::Future;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use tokio::net::TcpListener;
use tokio::spawn;
use tracing::debug;
use tracing::error;
use wgp::WaitGroup;

type EpkvReplica = Replica<BatchedCommand, Arc<LogDb>, Arc<DataDb>, TcpNetwork<BatchedCommand>>;

pub struct Server {
    replica: Arc<EpkvReplica>,

    config: Config,

    is_waiting_shutdown: AtomicFlag,
    waitgroup: WaitGroup,
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

        let server = {
            let is_waiting_shutdown = AtomicFlag::new(false);
            let waitgroup = WaitGroup::new();
            Arc::new(Server { replica, config, is_waiting_shutdown, waitgroup })
        };

        let mut bg_tasks = Vec::new();

        {
            let this = Arc::clone(&server);
            let addr = this.config.server.listen_peer_addr;
            let config = &this.config.network;
            let listener = TcpNetwork::spawn_listener(addr, config).await?;
            bg_tasks.push(spawn(this.serve_peer(listener)));
        }

        {
            let this = Arc::clone(&server);
            let listener = TcpListener::bind(this.config.server.listen_client_addr).await?;
            bg_tasks.push(spawn(this.serve_client(listener)));
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
            _ => Err(anyhow!("unexpected rpc method")),
        }
    }

    async fn client_rpc_get(self: &Arc<Self>, args: cs::GetArgs) -> Result<cs::GetOutput> {
        todo!()
    }

    async fn client_rpc_set(self: &Arc<Self>, args: cs::SetArgs) -> Result<cs::SetOutput> {
        todo!()
    }

    async fn client_rpc_del(self: &Arc<Self>, args: cs::DelArgs) -> Result<cs::DelOutput> {
        todo!()
    }
}
