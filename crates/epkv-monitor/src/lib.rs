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

// ------------------------------------------------------------------------------------------------

use self::config::Config;

use epkv_epaxos::addr_map::AddrMap;
use epkv_epaxos::id::{Epoch, Head, ReplicaId};
use epkv_protocol::{rpc, sm};
use epkv_utils::atomic_flag::AtomicFlag;

use std::fs;
use std::future::Future;
use std::ops::Not;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use camino::Utf8Path;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::spawn;
use tokio::sync::Mutex;
use tracing::{debug, error, warn};
use wgp::WaitGroup;

pub struct Monitor {
    state: Mutex<State>,
    dirty: AtomicFlag,

    config: Config,

    is_waiting_shutdown: AtomicFlag,
    waitgroup: WaitGroup,
}

#[derive(Debug, Serialize, Deserialize)]
struct State {
    rid_head: Head<ReplicaId>,
    epoch_head: Head<Epoch>,
    addr_map: AddrMap,
}

impl Default for State {
    fn default() -> Self {
        Self {
            rid_head: Head::new(ReplicaId::ZERO),
            epoch_head: Head::new(Epoch::ZERO),
            addr_map: AddrMap::new(),
        }
    }
}

impl State {
    fn load_or_create(path: &Utf8Path) -> Result<Self> {
        if path.exists() {
            let content = fs::read(path)?;
            Ok(serde_json::from_slice(&content)?)
        } else {
            let state = Self::default();
            state.save(path)?;
            Ok(state)
        }
    }

    fn save(&self, path: &Utf8Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let content = serde_json::to_vec(self)?;
        fs::write(path, content)?;
        Ok(())
    }
}

impl Monitor {
    pub async fn run(config: Config) -> Result<()> {
        let state = {
            let path = config.state_path.as_ref();
            State::load_or_create(path)?
        };
        debug!(?state);

        let monitor = {
            let state = Mutex::new(state);
            let dirty = AtomicFlag::new(false);

            let is_waiting_shutdown = AtomicFlag::new(false);
            let waitgroup = WaitGroup::new();

            Arc::new(Monitor { state, dirty, config, is_waiting_shutdown, waitgroup })
        };

        let mut bg_tasks = Vec::new();

        {
            let listener = TcpListener::bind(monitor.config.listen_rpc_addr).await?;
            let this = Arc::clone(&monitor);
            bg_tasks.push(spawn(this.serve_rpc(listener)));
        };

        {
            let this = Arc::clone(&monitor);
            bg_tasks.push(spawn(this.interval_save_state()));
        }

        {
            tokio::signal::ctrl_c().await?;
        }

        {
            monitor.is_waiting_shutdown.set(true);
            for task in &bg_tasks {
                task.abort();
            }
            drop(bg_tasks);

            let task_count = monitor.waitgroup.count();
            debug!(?task_count, "waiting running tasks");
            monitor.waitgroup.wait().await;
        }

        monitor.shutdown().await?;

        Ok(())
    }

    async fn serve_rpc(self: Arc<Self>, listener: TcpListener) -> Result<()> {
        let config = self.config.rpc_server.clone();
        let working = self.waitgroup.working();
        rpc::serve(self, listener, config, working).await
    }

    async fn interval_save_state(self: Arc<Self>) -> Result<()> {
        let mut interval = tokio::time::interval(Duration::from_micros(self.config.save_state_interval_us));

        loop {
            interval.tick().await;

            if self.is_waiting_shutdown.get() {
                break;
            }

            if self.dirty.get().not() {
                continue;
            }

            let this = Arc::clone(&self);
            let working = self.waitgroup.working();
            spawn(async move {
                if let Err(err) = this.run_save_state().await {
                    error!(?err, "interval save state");
                }
                drop(working);
            });
        }

        Ok(())
    }

    async fn shutdown(self: &Arc<Self>) -> Result<()> {
        self.run_save_state().await
    }
}

type HandleRpcFuture<'a> = impl Future<Output = Result<sm::Output>> + Send + 'a;

impl rpc::Service<sm::Args> for Monitor {
    type Output = sm::Output;

    type Future<'a> = HandleRpcFuture<'a>;

    fn call<'a>(self: &'a Arc<Self>, args: sm::Args) -> Self::Future<'a> {
        self.handle_rpc(args)
    }

    fn needs_stop(&self) -> bool {
        self.is_waiting_shutdown.get()
    }
}

impl Monitor {
    async fn handle_rpc(self: &Arc<Self>, args: sm::Args) -> Result<sm::Output> {
        match args {
            sm::Args::Register(args) => self.rpc_register(args).await.map(sm::Output::Register),
        }
    }

    async fn rpc_register(self: &Arc<Self>, args: sm::RegisterArgs) -> Result<sm::RegisterOutput> {
        let mut guard = self.state.lock().await;
        let s = &mut *guard;

        let rid = s.rid_head.gen_next();
        let epoch = s.epoch_head.gen_next();

        let mut peers = s.addr_map.map().clone();

        let prev_rid = s.addr_map.update(rid, args.public_peer_addr);

        if let Some(prev_rid) = prev_rid {
            let _ = peers.remove(&prev_rid);
        }

        self.dirty.set(true);

        drop(guard);

        let output = sm::RegisterOutput { rid, epoch, peers, prev_rid };
        Ok(output)
    }

    async fn run_save_state(&self) -> Result<()> {
        let guard = self.state.lock().await;
        let s = &*guard;
        if self.dirty.get() {
            s.save(&self.config.state_path)?;
            debug!(state=?s);
            self.dirty.set(false);
        }
        drop(guard);
        Ok(())
    }
}
