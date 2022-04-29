#![forbid(unsafe_code)]
#![deny(
    clippy::all,
    clippy::as_conversions,
    clippy::float_arithmetic,
    clippy::integer_arithmetic,
    clippy::must_use_candidate
)]
#![warn(clippy::todo)]

pub mod config;
pub mod net;

// -----------------------------------------------------------------------------

use self::config::Config;
use self::net::TcpNetwork;

use epkv_epaxos::replica::{Replica, ReplicaMeta};
use epkv_protocol::sm;
use epkv_rocks::cmd::BatchedCommand;
use epkv_rocks::data_db::DataDb;
use epkv_rocks::log_db::LogDb;
use epkv_utils::clone;

use std::sync::atomic::{AtomicBool, Ordering::*};
use std::sync::Arc;

use anyhow::Result;
use futures_util::pin_mut;
use tokio::spawn;
use tracing::debug;
use wgp::WaitGroup;

type EpkvReplica = Replica<BatchedCommand, Arc<LogDb>, Arc<DataDb>, TcpNetwork>;

pub struct Server {
    replica: Arc<EpkvReplica>,
    waiting_shutdown: AtomicBool,
    waitgroup: WaitGroup,
}

impl Server {
    fn is_waiting_shutdown(&self) -> bool {
        self.waiting_shutdown.load(SeqCst)
    }

    fn set_waiting_shutdown(&self) {
        self.waiting_shutdown.store(true, SeqCst);
    }
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
            let waiting_shutdown = AtomicBool::new(false);
            let waitgroup = WaitGroup::new();

            Arc::new(Server { replica, waiting_shutdown, waitgroup })
        };

        let serve_peer_task = {
            clone!(server);
            spawn(server.serve_peer())
        };

        let serve_client_task = {
            clone!(server);
            spawn(server.serve_client())
        };

        {
            let shutdown_signal: _ = tokio::signal::ctrl_c();
            pin_mut!(shutdown_signal);
            shutdown_signal.await?
        }

        {
            server.set_waiting_shutdown();
            serve_peer_task.abort();
            serve_client_task.abort();

            let task_count = server.waitgroup.count();
            debug!(?task_count, "waiting running tasks");
            server.waitgroup.wait().await;
        }

        server.shutdown().await;

        Ok(())
    }

    async fn serve_peer(self: Arc<Self>) {
        todo!()
    }

    async fn serve_client(self: Arc<Self>) {
        todo!()
    }

    async fn shutdown(self: Arc<Self>) {
        todo!()
    }
}
