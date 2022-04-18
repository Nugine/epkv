use crate::cmd::CommandLike;
use crate::config::ReplicaConfig;
use crate::deps::Deps;
use crate::id::*;
use crate::ins::Instance;
use crate::msg::*;
use crate::net::broadcast_preaccept;
use crate::net::Network;
use crate::state::State;
use crate::status::Status;
use crate::store::LogStore;
use crate::store::UpdateMode;

use epkv_utils::clone;
use epkv_utils::cmp::max_assign;
use epkv_utils::vecset::VecSet;
use rand::Rng;
use tokio::spawn;
use tokio::time::sleep;
use tracing::error;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{ensure, Result};
use dashmap::DashMap;
use tokio::sync::mpsc;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::MutexGuard as AsyncMutexGuard;

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
    propose_tx: DashMap<InstanceId, mpsc::Sender<Message<C>>>,

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
    ) -> Result<Arc<Self>> {
        let cluster_size = peers.len().wrapping_add(1);
        ensure!(peers.iter().all(|&p| p != rid));
        ensure!(cluster_size >= 3);

        let epoch = AtomicEpoch::new(epoch);
        let state = AsyncMutex::new(State::new(rid, store, peers).await?);
        let cb_propose = DashMap::new();

        Ok(Arc::new(Self {
            rid,
            config,
            state,
            epoch,
            propose_tx: cb_propose,
            net,
        }))
    }

    pub fn config(&self) -> &ReplicaConfig {
        &self.config
    }

    pub async fn handle_message(self: &Arc<Self>, msg: Message<C>) -> Result<()> {
        match msg {
            Message::PreAccept(msg) => {
                self.handle_preaccept(msg).await //
            }
            Message::PreAcceptOk(PreAcceptOk { id, .. }) => {
                self.resume_propose(id, msg).await //
            }
            Message::PreAcceptDiff(PreAcceptDiff { id, .. }) => {
                self.resume_propose(id, msg).await //
            }
            Message::Accept(msg) => {
                self.handle_accept(msg).await //
            }
            Message::AcceptOk(AcceptOk { id, .. }) => {
                self.resume_propose(id, msg).await //
            }
            Message::Commit(msg) => {
                self.handle_commit(msg).await //
            }
            Message::Prepare(msg) => {
                self.handle_prepare(msg).await //
            }
            Message::PrepareOk(PrepareOk { id, .. }) => {
                self.resume_propose(id, msg).await //
            }
            Message::PrepareNack(PrepareNack { id, .. }) => {
                self.resume_propose(id, msg).await //
            }
            Message::PrepareUnchosen(PrepareUnchosen { id, .. }) => {
                self.resume_propose(id, msg).await //
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
                self.handle_probe_rtt_ok(msg).await //
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

    pub async fn propose(self: &Arc<Self>, cmd: C) -> Result<()> {
        let mut guard = self.state.lock().await;
        let s = &mut *guard;

        let id = InstanceId(self.rid, s.lid_head.gen_next());
        let pbal = Ballot(Round::ZERO, self.rid);
        let acc = VecSet::<ReplicaId>::with_capacity(1);

        self.start_phase_preaccept(guard, id, pbal, Some(cmd), acc).await
    }

    async fn start_phase_preaccept(
        self: &Arc<Self>,
        mut guard: AsyncMutexGuard<'_, State<C, S>>,
        id: InstanceId,
        pbal: Ballot,
        cmd: Option<C>,
        mut acc: VecSet<ReplicaId>,
    ) -> Result<()> {
        let s = &mut *guard;

        s.log.load(id).await?;

        let (cmd, mode) = match cmd {
            Some(cmd) => (cmd, UpdateMode::Full),
            None => {
                let ins: _ = s.log.get_cached_ins(id).expect("instance should exist");
                (ins.cmd.clone(), UpdateMode::Partial)
            }
        };

        let (seq, deps) = s.log.calc_attributes(id, &cmd.keys());

        let abal = pbal;
        let status = Status::PreAccepted;
        let _ = acc.insert(self.rid);

        {
            clone!(cmd, deps, acc);
            let ins = Instance { pbal, cmd, seq, deps, abal, status, acc };
            s.log.save(id, ins, mode).await?;
        }

        let quorum = s.peers.cluster_size().wrapping_sub(2);
        let selected_peers = s.peers.select(quorum, &acc);

        let rx = {
            let (tx, rx) = mpsc::channel(quorum);
            self.propose_tx.insert(id, tx);
            rx
        };

        // let avg_rtt = s.peers.get_avg_rtt();

        drop(guard);

        {
            clone!(deps, acc);
            let sender = self.rid;
            let epoch = self.epoch.load();
            broadcast_preaccept(
                &self.net,
                selected_peers.acc,
                selected_peers.others,
                PreAccept { sender, epoch, id, pbal, cmd: Some(cmd), seq, deps, acc },
            );
        }

        // if pbal.0 == Round::ZERO {
        //     let conf = &self.config.recover_timeout;
        //     let duration = conf.with(avg_rtt, |d| {
        //         let rate: f64 = rand::thread_rng().gen_range(3.0..5.0);
        //         #[allow(clippy::float_arithmetic)]
        //         let delta = Duration::from_secs_f64(d.as_secs_f64() * rate);
        //         conf.default + delta
        //     });
        //     let this = Arc::clone(self);
        //     spawn(async move {
        //         sleep(duration).await;
        //         if let Err(err) = this.recover(id).await {
        //             error!(?err);
        //         }
        //     });
        // }

        {
            let this = Arc::clone(self);
            spawn(async move {
                if let Err(err) = this.end_phase_preaccept(rx, seq, deps, acc).await {
                    error!(?err);
                }
            });
        }

        Ok(())
    }

    async fn handle_preaccept(self: &Arc<Self>, msg: PreAccept<C>) -> Result<()> {
        if msg.epoch < self.epoch.load() {
            return Ok(());
        }

        let mut guard = self.state.lock().await;
        let s = &mut *guard;

        let id = msg.id;
        let pbal = msg.pbal;

        s.log.load(id).await?;

        if s.log.should_ignore_pbal(id, pbal) {
            return Ok(());
        }
        if s.log.should_ignore_status(id, pbal, Status::PreAccepted) {
            return Ok(());
        }

        let (cmd, mode) = match msg.cmd {
            Some(cmd) => (cmd, UpdateMode::Full),
            None => {
                let ins: _ = s.log.get_cached_ins(id).expect("instance should exist");
                (ins.cmd.clone(), UpdateMode::Partial)
            }
        };

        let (mut seq, mut deps) = s.log.calc_attributes(id, &cmd.keys());
        max_assign(&mut seq, msg.seq);
        deps.merge(&msg.deps);

        let is_changed = seq != msg.seq || deps != msg.deps;

        let abal = pbal;
        let status = Status::PreAccepted;

        let mut acc = msg.acc;
        let _ = acc.insert(self.rid);

        {
            clone!(deps);
            let ins: _ = Instance { pbal, cmd, seq, deps, abal, status, acc };
            s.log.save(id, ins, mode).await?
        }

        drop(guard);

        {
            let target = msg.sender;
            let sender = self.rid;
            let epoch = self.epoch.load();
            self.net.send_one(
                target,
                if is_changed {
                    Message::PreAcceptDiff(PreAcceptDiff { sender, epoch, id, pbal, seq, deps })
                } else {
                    Message::PreAcceptOk(PreAcceptOk { sender, epoch, id, pbal })
                },
            );
        }
        Ok(())
    }

    async fn end_phase_preaccept(
        self: &Arc<Self>,
        rx: mpsc::Receiver<Message<C>>,
        seq: Seq,
        deps: Deps,
        acc: VecSet<ReplicaId>,
    ) -> Result<()> {
        todo!()
    }

    async fn resume_propose(self: &Arc<Self>, id: InstanceId, msg: Message<C>) -> Result<()> {
        let tx = self.propose_tx.get(&id).as_deref().cloned();
        if let Some(tx) = tx {
            let _ = tx.send(msg).await;
        }
        Ok(())
    }

    async fn handle_accept(self: &Arc<Self>, msg: Accept<C>) -> Result<()> {
        todo!()
    }

    async fn handle_commit(self: &Arc<Self>, msg: Commit<C>) -> Result<()> {
        todo!()
    }

    async fn recover(&self, id: InstanceId) -> Result<()> {
        todo!()
    }

    async fn handle_prepare(self: &Arc<Self>, msg: Prepare) -> Result<()> {
        todo!()
    }

    async fn handle_join(self: &Arc<Self>, msg: Join) -> Result<()> {
        todo!()
    }

    async fn handle_join_ok(self: &Arc<Self>, msg: JoinOk) -> Result<()> {
        todo!()
    }

    async fn handle_leave(self: &Arc<Self>, msg: Leave) -> Result<()> {
        todo!()
    }

    async fn handle_probe_rtt(self: &Arc<Self>, msg: ProbeRtt) -> Result<()> {
        todo!()
    }

    async fn handle_probe_rtt_ok(self: &Arc<Self>, msg: ProbeRttOk) -> Result<()> {
        todo!()
    }

    async fn handle_ask_log(self: &Arc<Self>, msg: AskLog) -> Result<()> {
        todo!()
    }

    async fn handle_sync_log(self: &Arc<Self>, msg: SyncLog<C>) -> Result<()> {
        todo!()
    }

    async fn handle_sync_log_ok(self: &Arc<Self>, msg: SyncLogOk) -> Result<()> {
        todo!()
    }

    async fn handle_peer_bounds(self: &Arc<Self>, msg: PeerBounds) -> Result<()> {
        let mut guard = self.state.lock().await;
        let s = &mut *guard;

        if let Some(bounds) = msg.committed_up_to {
            s.peer_status_bounds.set_committed(msg.sender, bounds);
        }

        drop(guard);

        Ok(())
    }
}
