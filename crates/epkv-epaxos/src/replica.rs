use crate::cmd::CommandLike;
use crate::config::ReplicaConfig;
use crate::id::*;
use crate::msg::*;
use crate::net::Network;
use crate::state::State;
use crate::store::LogStore;

use epkv_utils::time::LocalInstant;
use epkv_utils::vecset::VecSet;

use anyhow::{ensure, Result};
use tokio::sync::Mutex as AsyncMutex;

pub struct Replica<C, S, N>
where
    C: CommandLike,
    S: LogStore<C>,
    N: Network<C>,
{
    rid: ReplicaId,
    config: ReplicaConfig,

    epoch: AtomicEpoch,
    state: AsyncMutex<State<C, S>>,

    net: N,
}

impl<C, S, N> Replica<C, S, N>
where
    C: CommandLike,
    S: LogStore<C>,
    N: Network<C>,
{
    pub async fn new(
        rid: ReplicaId,
        epoch: Epoch,
        peers: VecSet<ReplicaId>,
        config: ReplicaConfig,
        store: S,
        net: N,
    ) -> Result<Self> {
        let cluster_size = peers.len().wrapping_add(1);
        ensure!(peers.iter().all(|&p| p != rid));
        ensure!(cluster_size >= 3);

        let epoch = AtomicEpoch::new(epoch);
        let state = AsyncMutex::new(State::new(rid, store, peers).await?);

        Ok(Self { rid, config, state, epoch, net })
    }

    pub fn config(&self) -> &ReplicaConfig {
        &self.config
    }

    pub async fn handle_message(&self, msg: Message<C>, time: LocalInstant) -> Result<()> {
        match msg {
            Message::PreAccept(msg) => {
                self.handle_preaccept(msg).await //
            }
            Message::PreAcceptOk(msg) => {
                self.handle_preaccept_reply(PreAcceptReply::Ok(msg)).await //
            }
            Message::PreAcceptDiff(msg) => {
                self.handle_preaccept_reply(PreAcceptReply::Diff(msg)).await
            }
            Message::Accept(msg) => {
                self.handle_accept(msg).await //
            }
            Message::AcceptOk(msg) => {
                self.handle_accept_reply(AcceptReply::Ok(msg)).await //
            }
            Message::Commit(msg) => {
                self.handle_commit(msg).await //
            }
            Message::Prepare(msg) => {
                self.handle_prepare(msg).await //
            }
            Message::PrepareOk(msg) => {
                self.handle_prepare_reply(PrepareReply::Ok(msg)).await //
            }
            Message::PrepareNack(msg) => {
                self.handle_prepare_reply(PrepareReply::Nack(msg)).await //
            }
            Message::PrepareUnchosen(msg) => {
                self.handle_prepare_reply(PrepareReply::Unchosen(msg)).await
            }
            Message::Join(msg) => {
                self.handle_join(msg).await //
            }
            Message::JoinOk(msg) => {
                self.handle_join_ok(msg).await //
            }
            Message::Leave(msg) => {
                self.handle_leave(msg).await //
            }
            Message::ProbeRtt(msg) => {
                self.handle_probe_rtt(msg).await //
            }
            Message::ProbeRttOk(msg) => {
                self.handle_probe_rtt_ok(msg, time).await //
            }
            Message::AskLog(msg) => {
                self.handle_ask_log(msg).await //
            }
            Message::SyncLog(msg) => {
                self.handle_sync_log(msg).await //
            }
            Message::SyncLogOk(msg) => {
                self.handle_sync_log_ok(msg).await //
            }
            Message::PeerBounds(msg) => {
                self.handle_peer_bounds(msg).await //
            }
        }
    }

    async fn handle_preaccept(&self, msg: PreAccept<C>) -> Result<()> {
        todo!()
    }

    async fn handle_preaccept_reply(&self, msg: PreAcceptReply) -> Result<()> {
        todo!()
    }

    async fn handle_accept(&self, msg: Accept<C>) -> Result<()> {
        todo!()
    }

    async fn handle_accept_reply(&self, msg: AcceptReply) -> Result<()> {
        todo!()
    }

    async fn handle_commit(&self, msg: Commit<C>) -> Result<()> {
        todo!()
    }

    async fn handle_prepare(&self, msg: Prepare) -> Result<()> {
        todo!()
    }

    async fn handle_prepare_reply(&self, msg: PrepareReply<C>) -> Result<()> {
        todo!()
    }

    async fn handle_join(&self, msg: Join) -> Result<()> {
        todo!()
    }

    async fn handle_join_ok(&self, msg: JoinOk) -> Result<()> {
        todo!()
    }

    async fn handle_leave(&self, msg: Leave) -> Result<()> {
        todo!()
    }

    async fn handle_probe_rtt(&self, msg: ProbeRtt) -> Result<()> {
        todo!()
    }

    async fn handle_probe_rtt_ok(&self, msg: ProbeRttOk, time: LocalInstant) -> Result<()> {
        todo!()
    }

    async fn handle_ask_log(&self, msg: AskLog) -> Result<()> {
        todo!()
    }

    async fn handle_sync_log(&self, msg: SyncLog<C>) -> Result<()> {
        todo!()
    }

    async fn handle_sync_log_ok(&self, msg: SyncLogOk) -> Result<()> {
        todo!()
    }

    async fn handle_peer_bounds(&self, msg: PeerBounds) -> Result<()> {
        todo!()
    }
}
