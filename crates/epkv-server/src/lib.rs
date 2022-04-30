#![forbid(unsafe_code)]
#![deny(
    clippy::all,
    clippy::as_conversions,
    clippy::float_arithmetic,
    clippy::integer_arithmetic,
    clippy::must_use_candidate
)]
#![warn(clippy::todo, clippy::dbg_macro)]

pub mod config;
pub mod net;

// -----------------------------------------------------------------------------

use self::config::Config;
use self::net::{Listener, TcpNetwork};

use epkv_epaxos::replica::{Replica, ReplicaMeta};
use epkv_protocol::sm;
use epkv_rocks::cmd::BatchedCommand;
use epkv_rocks::data_db::DataDb;
use epkv_rocks::log_db::LogDb;
use epkv_utils::atomic_flag::AtomicFlag;
use epkv_utils::clone;

use std::sync::Arc;

use anyhow::Result;
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

        let serve_peer_task = {
            let this = Arc::clone(&server);
            let addr = this.config.server.listen_peer_addr;
            let config = &this.config.network;
            let listener = TcpNetwork::spawn_listener(addr, config).await?;

            clone!(server);
            spawn(server.serve_peer(listener))
        };

        let serve_client_task = {
            clone!(server);
            spawn(server.serve_client())
        };

        {
            tokio::signal::ctrl_c().await?;
        }

        {
            server.is_waiting_shutdown.set(true);
            serve_peer_task.abort();
            serve_client_task.abort();

            let task_count = server.waitgroup.count();
            debug!(?task_count, "waiting running tasks");
            server.waitgroup.wait().await;
        }

        server.shutdown().await;

        Ok(())
    }

    async fn serve_peer(self: Arc<Self>, mut listener: Listener<BatchedCommand>) {
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
    }

    async fn serve_client(self: Arc<Self>) {
        todo!()
    }

    async fn shutdown(self: Arc<Self>) {
        todo!()
    }
}
