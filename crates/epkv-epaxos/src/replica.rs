// TODO

mod config;
mod meta;

use self::config::ReplicaConfig;
use self::meta::ReplicaMeta;

use crate::types::{Effect, Epoch, Join, LogStore, Message, ReplicaId};

use epkv_utils::vecset::VecSet;

use anyhow::{ensure, Result};
use tokio::sync::RwLock;

pub struct Replica<S: LogStore> {
    rid: ReplicaId,
    config: ReplicaConfig,
    state: RwLock<State<S>>,
}

struct State<S> {
    meta: ReplicaMeta,
    store: S,
    joining: Option<VecSet<ReplicaId>>,
}

impl<S> State<S> {
    fn is_joining(&self) -> bool {
        self.joining.is_some()
    }
}

impl<S: LogStore> Replica<S> {
    pub fn new(
        rid: ReplicaId,
        epoch: Epoch,
        peers: &[ReplicaId],
        store: S,
        config: ReplicaConfig,
    ) -> Result<Self> {
        let cluster_size = peers.len().wrapping_add(1);
        ensure!(peers.iter().all(|&p| p != rid));
        ensure!(cluster_size >= 3 && cluster_size % 2 == 1);

        let meta = ReplicaMeta::new(epoch, peers);
        let state = RwLock::new(State {
            meta,
            store,
            joining: None,
        });
        Ok(Self { rid, config, state })
    }

    pub async fn handle_message(&self, msg: Message<S::Command>) -> Result<Effect<S::Command>> {
        match msg {
            Message::PreAccept(_) => todo!(),
            Message::PreAcceptOk(_) => todo!(),
            Message::PreAcceptDiff(_) => todo!(),
            Message::Accept(_) => todo!(),
            Message::AcceptOk(_) => todo!(),
            Message::Commit(_) => todo!(),
            Message::Prepare(_) => todo!(),
            Message::PrepareOk(_) => todo!(),
            Message::PrepareNack(_) => todo!(),
            Message::Join(_) => todo!(),
            Message::JoinOk(_) => todo!(),
        }
    }

    pub async fn start_phase_join(&self) -> Result<Effect<S::Command>> {
        let mut state_guard = self.state.write().await;
        let state = &mut *state_guard;

        state.joining = Some(VecSet::new());

        let msg = Message::<S::Command>::Join(Join {
            sender: self.rid,
            epoch: state.meta.epoch(),
        });
        let targets = state.meta.all_peers();
        Ok(Effect::broadcast(targets, msg))
    }
}
