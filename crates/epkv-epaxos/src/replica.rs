use crate::cmd::CommandLike;
use crate::config::ReplicaConfig;
use crate::deps::Deps;
use crate::id::*;
use crate::ins::Instance;
use crate::msg::*;
use crate::net::broadcast_accept;
use crate::net::broadcast_commit;
use crate::net::broadcast_preaccept;
use crate::net::Network;
use crate::state::State;
use crate::status::Status;
use crate::store::LogStore;
use crate::store::UpdateMode;

use epkv_utils::chan::recv_timeout;
use epkv_utils::clone;
use epkv_utils::cmp::max_assign;
use epkv_utils::vecset::VecSet;

use std::ops::Not;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{ensure, Result};
use dashmap::DashMap;
use rand::Rng;
use tokio::spawn;
use tokio::sync::mpsc;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::MutexGuard as AsyncMutexGuard;
use tokio::time::sleep;
use tokio::time::timeout;
use tracing::error;

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
        // } // TODO

        {
            let this = Arc::clone(self);
            spawn(async move {
                if let Err(err) = this.end_phase_preaccept(id, rx, seq, deps, acc).await {
                    error!(?id, ?err);
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

    async fn resume_propose(self: &Arc<Self>, id: InstanceId, msg: Message<C>) -> Result<()> {
        let tx = self.propose_tx.get(&id).as_deref().cloned();
        if let Some(tx) = tx {
            let _ = tx.send(msg).await;
        }
        Ok(())
    }

    async fn end_phase_preaccept(
        self: &Arc<Self>,
        id: InstanceId,
        mut rx: mpsc::Receiver<Message<C>>,
        mut seq: Seq,
        mut deps: Deps,
        mut acc: VecSet<ReplicaId>,
    ) -> Result<()> {
        let mut received: VecSet<ReplicaId> = VecSet::new();
        let mut all_same = true;

        loop {
            let t = {
                let mut guard = self.state.lock().await;
                let s = &mut *guard;
                let avg_rtt = s.peers.get_avg_rtt();
                drop(guard);
                let conf = &self.config.preaccept_timeout;
                conf.with(avg_rtt, |d| d / 2)
            };

            match recv_timeout(&mut rx, t).await {
                Ok(Some(msg)) => {
                    let msg = match PreAcceptReply::convert(msg) {
                        Some(m) => m,
                        None => continue,
                    };

                    match msg {
                        PreAcceptReply::Ok(ref msg) => assert_eq!(id, msg.id),
                        PreAcceptReply::Diff(ref msg) => assert_eq!(id, msg.id),
                    }

                    let msg_epoch = match msg {
                        PreAcceptReply::Ok(ref msg) => msg.epoch,
                        PreAcceptReply::Diff(ref msg) => msg.epoch,
                    };

                    if msg_epoch < self.epoch.load() {
                        continue;
                    }

                    let mut guard = self.state.lock().await;
                    let s = &mut *guard;

                    let pbal = match msg {
                        PreAcceptReply::Ok(ref msg) => msg.pbal,
                        PreAcceptReply::Diff(ref msg) => msg.pbal,
                    };

                    s.log.load(id).await?;

                    if s.log.should_ignore_pbal(id, pbal) {
                        continue;
                    }

                    let ins: _ = s.log.get_cached_ins(id).expect("instance should exist");

                    if ins.status != Status::PreAccepted {
                        continue;
                    }

                    let cluster_size = s.peers.cluster_size();

                    {
                        let msg_sender = match msg {
                            PreAcceptReply::Ok(ref msg) => msg.sender,
                            PreAcceptReply::Diff(ref msg) => msg.sender,
                        };
                        if received.insert(msg_sender).is_some() {
                            continue;
                        }
                        let _ = acc.insert(msg_sender);
                    }

                    match msg {
                        PreAcceptReply::Ok(_) => {}
                        PreAcceptReply::Diff(msg) => {
                            let mut new_seq = msg.seq;
                            let mut new_deps = msg.deps;

                            max_assign(&mut new_seq, seq);
                            new_deps.merge(&deps);

                            if received.len() > 1 && (new_seq != seq || new_deps != deps) {
                                all_same = false;
                            }

                            seq = new_seq;
                            deps = new_deps;
                        }
                    }

                    if received.len() < cluster_size / 2 {
                        continue;
                    }

                    let which_path = if all_same {
                        if pbal.0 == Round::ZERO && received.len() >= cluster_size.wrapping_sub(2) {
                            Some(true)
                        } else {
                            None
                        }
                    } else {
                        Some(false)
                    };

                    let is_fast_path = match which_path {
                        None => continue,
                        Some(f) => f,
                    };

                    let cmd = None;
                    let _ = self.propose_tx.remove(&id);

                    if is_fast_path {
                        return self.start_phase_commit(guard, id, pbal, cmd, seq, deps, acc).await;
                    } else {
                        return self.start_phase_accept(guard, id, pbal, cmd, seq, deps, acc).await;
                    }
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }

        {
            let mut guard = self.state.lock().await;
            let s = &mut *guard;

            let cluster_size = s.peers.cluster_size();
            if received.len() < cluster_size / 2 {
                return Ok(());
            }

            s.log.load(id).await?;
            let pbal = s.log.get_cached_pbal(id).expect("pbal should exist");

            let cmd = None;
            let _ = self.propose_tx.remove(&id);

            self.start_phase_accept(guard, id, pbal, cmd, seq, deps, acc).await
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_phase_accept(
        self: &Arc<Self>,
        mut guard: AsyncMutexGuard<'_, State<C, S>>,
        id: InstanceId,
        pbal: Ballot,
        cmd: Option<C>,
        seq: Seq,
        deps: Deps,
        acc: VecSet<ReplicaId>,
    ) -> Result<()> {
        let s = &mut *guard;

        let abal = pbal;
        let status = Status::Accepted;

        let quorum = s.peers.cluster_size() / 2;
        let selected_peers = s.peers.select(quorum, &acc);

        let (cmd, mode) = match cmd {
            Some(cmd) => (cmd, UpdateMode::Full),
            None => {
                s.log.load(id).await?;
                let ins: _ = s.log.get_cached_ins(id).expect("instance should exist");
                (ins.cmd.clone(), UpdateMode::Partial)
            }
        };

        {
            clone!(cmd, deps, acc);
            let ins: _ = Instance { pbal, cmd, seq, deps, abal, status, acc };
            s.log.save(id, ins, mode).await?;
        }

        let rx = {
            let (tx, rx) = mpsc::channel(quorum);
            self.propose_tx.insert(id, tx);
            rx
        };

        drop(guard);

        {
            clone!(acc);
            let sender = self.rid;
            let epoch = self.epoch.load();
            broadcast_accept(
                &self.net,
                selected_peers.acc,
                selected_peers.others,
                Accept { sender, epoch, id, pbal, cmd: Some(cmd), seq, deps, acc },
            );
        }

        {
            let this = Arc::clone(self);
            spawn(async move {
                if let Err(err) = this.end_phase_accept(id, rx, acc).await {
                    error!(?id, ?err);
                }
            });
        }

        Ok(())
    }

    async fn handle_accept(self: &Arc<Self>, msg: Accept<C>) -> Result<()> {
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
        if s.log.should_ignore_status(id, pbal, Status::Accepted) {
            return Ok(());
        }

        let abal = pbal;
        let status = Status::Accepted;

        let mut acc = msg.acc;
        let _ = acc.insert(self.rid);

        let seq = msg.seq;
        let deps = msg.deps;

        let (cmd, mode) = match msg.cmd {
            Some(cmd) => (cmd, UpdateMode::Full),
            None => {
                let ins: _ = s.log.get_cached_ins(id).expect("instance should exist");
                (ins.cmd.clone(), UpdateMode::Partial)
            }
        };

        {
            let ins: _ = Instance { pbal, cmd, seq, deps, abal, status, acc };
            s.log.save(id, ins, mode).await?;
        }

        drop(guard);

        {
            let target = msg.sender;
            let sender = self.rid;
            let epoch = self.epoch.load();
            self.net.send_one(
                target,
                Message::AcceptOk(AcceptOk { sender, epoch, id, pbal }),
            );
        }
        Ok(())
    }

    async fn end_phase_accept(
        self: &Arc<Self>,
        id: InstanceId,
        mut rx: mpsc::Receiver<Message<C>>,
        mut acc: VecSet<ReplicaId>,
    ) -> Result<()> {
        let mut received = VecSet::new();

        while let Some(msg) = rx.recv().await {
            let msg = match AcceptReply::convert(msg) {
                Some(m) => m,
                None => continue,
            };

            let AcceptReply::Ok(msg) = msg;

            if msg.epoch < self.epoch.load() {
                continue;
            }

            let mut guard = self.state.lock().await;
            let s = &mut *guard;

            assert_eq!(id, msg.id);

            let pbal = msg.pbal;

            s.log.load(id).await?;

            if s.log.should_ignore_pbal(id, pbal) {
                continue;
            }

            let ins: _ = s.log.get_cached_ins(id).expect("instance should exist");

            if ins.status != Status::Accepted {
                continue;
            }

            let seq = ins.seq;
            let deps = ins.deps.clone();

            {
                if received.insert(msg.sender).is_some() {
                    continue;
                }
                let _ = acc.insert(msg.sender);
            }

            let cluster_size = s.peers.cluster_size();

            if received.len() < cluster_size / 2 {
                continue;
            }

            let _ = self.propose_tx.remove(&id);

            let cmd = None;
            return self.start_phase_commit(guard, id, pbal, cmd, seq, deps, acc).await;
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_phase_commit(
        self: &Arc<Self>,
        mut guard: AsyncMutexGuard<'_, State<C, S>>,
        id: InstanceId,
        pbal: Ballot,
        cmd: Option<C>,
        seq: Seq,
        deps: Deps,
        acc: VecSet<ReplicaId>,
    ) -> Result<()> {
        let s = &mut *guard;

        let abal = pbal;
        let status = Status::Committed;

        let quorum = s.peers.cluster_size().wrapping_sub(1);
        let selected_peers = s.peers.select(quorum, &acc);

        let (cmd, mode) = match cmd {
            Some(cmd) => (cmd, UpdateMode::Full),
            None => {
                s.log.load(id).await?;
                let ins: _ = s.log.get_cached_ins(id).expect("instance should exist");
                (ins.cmd.clone(), UpdateMode::Partial)
            }
        };

        {
            clone!(cmd, deps, acc);
            let ins: _ = Instance { pbal, cmd, seq, deps, abal, status, acc };
            s.log.save(id, ins, mode).await?;
        }

        drop(guard);

        cmd.notify_committed();

        {
            let sender = self.rid;
            let epoch = self.epoch.load();
            clone!(cmd, deps);
            broadcast_commit(
                &self.net,
                selected_peers.acc,
                selected_peers.others,
                Commit { sender, epoch, id, pbal, cmd: Some(cmd), seq, deps, acc },
            );
        }

        {
            let this = Arc::clone(self);
            spawn(async move {
                if let Err(err) = this.execute(id, cmd, seq, deps).await {
                    error!(?id, ?err)
                }
            });
        }

        Ok(())
    }

    async fn handle_commit(self: &Arc<Self>, msg: Commit<C>) -> Result<()> {
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

        if s.log.should_ignore_status(id, pbal, Status::Committed) {
            return Ok(());
        }

        let (cmd, mode) = match msg.cmd {
            Some(cmd) => (cmd, UpdateMode::Full),
            None => {
                s.log.load(id).await?;
                let ins: _ = s.log.get_cached_ins(id).expect("instance should exist");
                (ins.cmd.clone(), UpdateMode::Partial)
            }
        };

        let (status, exec) = match s.log.get_cached_ins(id) {
            Some(ins) if ins.status > Status::Committed => (ins.status, false),
            _ => (Status::Committed, true),
        };

        let seq = msg.seq;
        let deps = msg.deps;

        let abal = pbal;

        let mut acc = msg.acc;
        let _ = acc.insert(self.rid);

        {
            clone!(cmd, deps);
            let ins: _ = Instance { pbal, cmd, seq, deps, abal, status, acc };
            s.log.save(id, ins, mode).await?
        }

        drop(guard);

        if exec {
            let this = Arc::clone(self);
            spawn(async move {
                if let Err(err) = this.execute(id, cmd, seq, deps).await {
                    error!(?id, ?err)
                }
            });
        }
        Ok(())
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
        let mut guard = self.state.lock().await;
        let s = &mut *guard;

        s.peers.remove(msg.sender);

        drop(guard);

        Ok(())
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

    async fn execute(self: &Arc<Self>, id: InstanceId, cmd: C, seq: Seq, deps: Deps) -> Result<()> {
        todo!()
    }
}
