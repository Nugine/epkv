// TODO

mod config;
mod meta;

use self::config::ReplicaConfig;
use self::meta::ReplicaMeta;

use crate::types::*;

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
            Message::PreAccept(msg) => self.handle_pre_accept(msg).await,
            Message::PreAcceptOk(msg) => self.handle_pre_accept_ok(msg).await,
            Message::PreAcceptDiff(msg) => self.handle_pre_accept_diff(msg).await,
            Message::Accept(msg) => self.handle_accept().await,
            Message::AcceptOk(msg) => self.handle_accept_ok().await,
            Message::Commit(msg) => self.handle_commit().await,
            Message::Prepare(msg) => self.handle_prepare().await,
            Message::PrepareOk(msg) => self.handle_prepare_ok().await,
            Message::PrepareNack(msg) => self.handle_prepare_nack().await,
            Message::Join(msg) => self.handle_join(msg).await,
            Message::JoinOk(msg) => self.handle_join_ok(msg).await,
            Message::Leave(msg) => self.handle_leave(msg).await,
        }
    }

    pub async fn start_joining(&self) -> Result<Effect<S::Command>> {
        let mut guard = self.state.write().await;
        let state = &mut *guard;

        state.joining = Some(VecSet::new());

        let targets = state.meta.all_peers();
        let msg: _ = Message::Join(Join {
            sender: self.rid,
            epoch: state.meta.epoch(),
        });
        Ok(Effect::broadcast(targets, msg))
    }

    async fn handle_join_ok(&self, msg: JoinOk) -> Result<Effect<S::Command>> {
        let mut guard = self.state.write().await;
        let state = &mut *guard;

        let joining = match state.joining {
            Some(ref mut s) => s,
            None => return Ok(Effect::empty()),
        };

        let _ = joining.insert(msg.sender);

        let cluster_size = state.meta.cluster_size();

        if joining.len() > cluster_size / 2 {
            state.joining = None;
        }

        Ok(Effect::empty())
    }

    async fn handle_join(&self, msg: Join) -> Result<Effect<S::Command>> {
        let mut guard = self.state.write().await;
        let state = &mut *guard;

        state.meta.add_peer(msg.sender);
        state.meta.update_epoch(msg.epoch);

        let target = msg.sender;
        let msg = Message::JoinOk(JoinOk { sender: self.rid });
        Ok(Effect::reply(target, msg))
    }

    async fn handle_leave(&self, msg: Leave) -> Result<Effect<S::Command>> {
        let mut guard = self.state.write().await;
        let state = &mut *guard;

        state.meta.remove_peer(msg.sender);

        Ok(Effect::empty())
    }

    async fn handle_pre_accept(&self, msg: PreAccept<S::Command>) -> Result<Effect<S::Command>> {
        todo!()
    }

    async fn handle_pre_accept_ok(&self, msg: PreAcceptOk) -> Result<Effect<S::Command>> {
        todo!()
    }

    async fn handle_pre_accept_diff(&self, msg: PreAcceptDiff) -> Result<Effect<S::Command>> {
        todo!()
    }

    async fn handle_accept(&self) -> Result<Effect<S::Command>> {
        todo!()
    }

    async fn handle_accept_ok(&self) -> Result<Effect<S::Command>> {
        todo!()
    }

    async fn handle_commit(&self) -> Result<Effect<S::Command>> {
        todo!()
    }

    async fn handle_prepare(&self) -> Result<Effect<S::Command>> {
        todo!()
    }

    async fn handle_prepare_ok(&self) -> Result<Effect<S::Command>> {
        todo!()
    }

    async fn handle_prepare_nack(&self) -> Result<Effect<S::Command>> {
        todo!()
    }
}
