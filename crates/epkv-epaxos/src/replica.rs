// TODO

mod config;
mod meta;
mod space;

use self::config::ReplicaConfig;
use self::meta::ReplicaMeta;
use self::space::Space;

use crate::types::*;

use epkv_utils::vecset::VecSet;

use anyhow::{ensure, Result};
use tokio::sync::RwLock;

pub struct Replica<S: LogStore> {
    rid: ReplicaId,
    config: ReplicaConfig,
    state: RwLock<State<S>>,
}

struct State<S: LogStore> {
    meta: ReplicaMeta,
    space: Space<S>,
    joining: Option<VecSet<ReplicaId>>,
}

impl<S: LogStore> State<S> {
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
        let space = Space::new(rid, store)?;
        let state = RwLock::new(State {
            meta,
            space,
            joining: None,
        });
        Ok(Self { rid, config, state })
    }

    pub async fn handle_message(&self, msg: Message<S::Command>) -> Result<Effect<S::Command>> {
        match msg {
            Message::PreAccept(msg) => self.handle_pre_accept(msg).await,
            Message::PreAcceptOk(msg) => self.handle_pre_accept_ok(msg).await,
            Message::PreAcceptDiff(msg) => self.handle_pre_accept_diff(msg).await,
            Message::Accept(msg) => self.handle_accept(msg).await,
            Message::AcceptOk(msg) => self.handle_accept_ok(msg).await,
            Message::Commit(msg) => self.handle_commit(msg).await,
            Message::Prepare(msg) => self.handle_prepare(msg).await,
            Message::PrepareOk(msg) => self.handle_prepare_ok(msg).await,
            Message::PrepareNack(msg) => self.handle_prepare_nack(msg).await,
            Message::PrepareUnchosen(msg) => self.handle_prepare_unchosen(msg).await,
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

    pub async fn propose(&self, cmd: S::Command) -> Result<Effect<S::Command>> {
        let mut guard = self.state.write().await;
        let state = &mut *guard;

        let id = InstanceId(self.rid, state.space.generate_lid());
        let pbal = Ballot(state.meta.epoch(), Round::ZERO, self.rid);
        let acc = Acc::with_capacity(1);

        drop(guard);
        self.start_phase_pre_accept(id, pbal, cmd, acc).await
    }

    async fn start_phase_pre_accept(
        &self,
        id: InstanceId,
        pbal: Ballot,
        cmd: S::Command,
        acc: Acc,
    ) -> Result<Effect<S::Command>> {
        todo!()
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

    async fn handle_accept(&self, msg: Accept<S::Command>) -> Result<Effect<S::Command>> {
        todo!()
    }

    async fn handle_accept_ok(&self, msg: AcceptOk) -> Result<Effect<S::Command>> {
        todo!()
    }

    async fn handle_commit(&self, msg: Commit<S::Command>) -> Result<Effect<S::Command>> {
        todo!()
    }

    async fn handle_prepare(&self, msg: Prepare) -> Result<Effect<S::Command>> {
        todo!()
    }

    async fn handle_prepare_ok(&self, msg: PrepareOk<S::Command>) -> Result<Effect<S::Command>> {
        todo!()
    }

    async fn handle_prepare_nack(&self, msg: PrepareNack) -> Result<Effect<S::Command>> {
        todo!()
    }

    async fn handle_prepare_unchosen(&self, msg: PrepareUnchosen) -> Result<Effect<S::Command>> {
        todo!()
    }
}
