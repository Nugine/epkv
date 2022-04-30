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

use epkv_epaxos::id::{Epoch, Head, ReplicaId};
use epkv_protocol::{rpc, sm};
use epkv_utils::atomic_flag::AtomicFlag;
use epkv_utils::vecmap::VecMap;

use std::collections::HashMap;
use std::fs;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use camino::Utf8Path;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::spawn;
use tokio::sync::Mutex;
use tracing::{debug, warn};
use wgp::WaitGroup;

pub struct Monitor {
    state: Mutex<State>,

    config: Config,

    is_waiting_shutdown: AtomicFlag,
    waitgroup: WaitGroup,
}

#[derive(Debug, Serialize, Deserialize)]
struct State {
    rid_head: Head<ReplicaId>,
    epoch_head: Head<Epoch>,
    addr_map: VecMap<ReplicaId, SocketAddr>,
    addr_rev_map: HashMap<SocketAddr, ReplicaId>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            rid_head: Head::new(ReplicaId::ZERO),
            epoch_head: Head::new(Epoch::ZERO),
            addr_map: VecMap::new(),
            addr_rev_map: HashMap::new(),
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

            let is_waiting_shutdown = AtomicFlag::new(false);
            let waitgroup = WaitGroup::new();

            Arc::new(Monitor { state, config, is_waiting_shutdown, waitgroup })
        };

        let serve_rpc_task = {
            let listener = TcpListener::bind(monitor.config.listen_rpc_addr).await?;
            let this = Arc::clone(&monitor);
            spawn(this.serve_rpc(listener))
        };

        {
            tokio::signal::ctrl_c().await?;
        }

        {
            monitor.is_waiting_shutdown.set(true);
            serve_rpc_task.abort();

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

    async fn shutdown(&self) -> Result<()> {
        let guard = self.state.lock().await;
        let s = &*guard;
        s.save(&self.config.state_path)?;
        drop(guard);
        Ok(())
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
            _ => Err(anyhow!("unexpected rpc method")),
        }
    }

    async fn rpc_register(self: &Arc<Self>, args: sm::RegisterArgs) -> Result<sm::RegisterOutput> {
        let mut guard = self.state.lock().await;
        let s = &mut *guard;

        let rid = s.rid_head.gen_next();
        let epoch = s.epoch_head.gen_next();

        let mut peers = s.addr_map.clone();

        let opt = s.addr_map.insert(rid, args.public_peer_addr);
        assert!(opt.is_none());

        let prev_rid = s.addr_rev_map.insert(args.public_peer_addr, rid);

        if let Some(prev_rid) = prev_rid {
            let _ = s.addr_map.remove(&prev_rid);
            let _ = peers.remove(&prev_rid);
        }

        drop(guard);

        let output = sm::RegisterOutput { rid, epoch, peers, prev_rid };
        Ok(output)
    }

    async fn interval_save_state(&self) -> Result<()> {
        todo!()
    }
}
