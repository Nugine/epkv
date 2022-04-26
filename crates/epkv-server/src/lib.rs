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

pub struct Server {
    log_db: Arc<LogDb>,
    data_db: Arc<DataDb>,
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
        let log_db = LogDb::new(&config.log_db.path)?;

        debug!("log_db is opened");

        let data_db = DataDb::new(&config.data_db.path)?;

        debug!("data_db is opened");

        let waiting_shutdown = AtomicBool::new(false);
        let waitgroup = WaitGroup::new();

        let server = Arc::new(Server { log_db, data_db, waiting_shutdown, waitgroup });

        {
            clone!(server);
            spawn(server.serve_peer())
        };

        {
            clone!(server);
            spawn(server.serve_client());
        };

        {
            let shutdown_signal: _ = tokio::signal::ctrl_c();
            pin_mut!(shutdown_signal);
            shutdown_signal.await?
        }

        {
            server.set_waiting_shutdown();

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
