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

// ------------------------------------------------------------------------------------------------

use std::fs;
use std::net::SocketAddr;
use std::ops::Not;
use std::sync::Arc;

use self::config::Config;

use epkv_epaxos::id::{Epoch, Head, ReplicaId};
use epkv_utils::vecmap::VecMap;

use anyhow::Result;
use camino::Utf8Path;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::debug;

pub struct Monitor {
    state: Mutex<State>,
}

#[derive(Debug, Serialize, Deserialize)]
struct State {
    rid_head: Head<ReplicaId>,
    epoch_head: Head<Epoch>,
    addr_map: VecMap<ReplicaId, SocketAddr>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            rid_head: Head::new(ReplicaId::ZERO),
            epoch_head: Head::new(Epoch::ZERO),
            addr_map: VecMap::new(),
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
        todo!()
    }

    async fn serve_rpc(self: Arc<Self>) {
        todo!()
    }

    async fn shutdown(self: Arc<Self>) {
        todo!()
    }
}
