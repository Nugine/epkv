use crate::acc::{Acc, MutableAcc};
use crate::bounds::PeerStatusBounds;
use crate::cmd::CommandLike;
use crate::config::ReplicaConfig;
use crate::deps::Deps;
use crate::exec::ExecNotify;
use crate::graph::{DepsQueue, Graph, InsNode, LocalGraph};
use crate::id::*;
use crate::ins::Instance;
use crate::log::Log;
use crate::msg::*;
use crate::net::{self, Network};
use crate::peers::Peers;
use crate::status::{ExecStatus, Status};
use crate::store::{DataStore, LogStore, UpdateMode};

use epkv_utils::asc::Asc;
use epkv_utils::cast::NumericCast;
use epkv_utils::chan::recv_timeout;
use epkv_utils::clone;
use epkv_utils::cmp::max_assign;
use epkv_utils::flag_group::FlagGroup;
use epkv_utils::iter::{iter_mut_deref, map_collect};
use epkv_utils::lock::with_mutex;
use epkv_utils::time::LocalInstant;
use epkv_utils::vecmap::VecMap;
use epkv_utils::vecset::VecSet;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::mem;
use std::net::SocketAddr;
use std::ops::Not;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::*;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{ensure, Result};
use dashmap::DashMap;
use futures_util::future::join_all;
use parking_lot::Mutex as SyncMutex;
use rand::Rng;
use tokio::spawn;
use tokio::sync::mpsc;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::MutexGuard as AsyncMutexGuard;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::{debug, error};

pub struct Replica<C, L, D, N>
where
    C: CommandLike,
    L: LogStore<C>,
    D: DataStore<C>,
    N: Network<C>,
{
    rid: ReplicaId,
    public_peer_addr: SocketAddr,
    config: ReplicaConfig,

    epoch: AtomicEpoch,
    state: AsyncMutex<State<C, L>>,

    propose_tx: DashMap<InstanceId, mpsc::Sender<Message<C>>>,
    join_tx: SyncMutex<Option<mpsc::Sender<JoinOk>>>,
    sync_tx: DashMap<SyncId, mpsc::Sender<SyncLogOk>>,

    graph: Graph<C>,
    data_store: D,

    network: N,

    recovering: DashMap<InstanceId, JoinHandle<()>>,

    metrics: SyncMutex<Metrics>,

    probe_rtt_countdown: AtomicU64,
}

struct State<C, L>
where
    C: CommandLike,
    L: LogStore<C>,
{
    peers: Peers,

    log: Log<C, L>,

    peer_status_bounds: PeerStatusBounds,

    lid_head: Head<LocalInstanceId>,

    sync_id_head: Head<SyncId>,
}

#[derive(Debug, Clone)]
pub struct Metrics {
    pub preaccept_fast_path: u64,
    pub preaccept_slow_path: u64,
    pub recover_nop_count: u64,
    pub recover_success_count: u64,
}

pub struct ReplicaMeta {
    pub rid: ReplicaId,
    pub epoch: Epoch,
    pub peers: VecMap<ReplicaId, SocketAddr>,
    pub public_peer_addr: SocketAddr,
    pub config: ReplicaConfig,
}

impl<C, L, D, N> Replica<C, L, D, N>
where
    C: CommandLike,
    L: LogStore<C>,
    D: DataStore<C>,
    N: Network<C>,
{
    pub async fn new(meta: ReplicaMeta, mut log_store: L, data_store: D, network: N) -> Result<Arc<Self>> {
        let rid = meta.rid;
        let public_peer_addr = meta.public_peer_addr;
        let epoch = meta.epoch;
        let peers = meta.peers;
        let config = meta.config;

        let addr_set: VecSet<_> = map_collect(&peers, |&(_, a)| a);
        ensure!(peers.iter().all(|&(p, a)| p != rid && a != public_peer_addr));
        ensure!(addr_set.len() == peers.len());

        let epoch = AtomicEpoch::new(epoch);

        let (attr_bounds, status_bounds) = log_store.load_bounds().await?;
        let status_bounds: _ = Asc::new(SyncMutex::new(status_bounds));

        let state = {
            let peers_set: VecSet<_> = map_collect(&peers, |&(p, _)| p);
            let peers = Peers::new(rid, peers_set);

            let lid_head =
                Head::new(attr_bounds.max_lids.get(&rid).copied().unwrap_or(LocalInstanceId::ZERO));

            let sync_id_head = Head::new(SyncId::ZERO);

            let log = Log::new(log_store, attr_bounds, Asc::clone(&status_bounds));

            let peer_status_bounds = PeerStatusBounds::new();

            AsyncMutex::new(State { peers, lid_head, sync_id_head, log, peer_status_bounds })
        };

        let propose_tx = DashMap::new();
        let join_tx = SyncMutex::new(None);
        let sync_tx = DashMap::new();

        let graph = Graph::new(status_bounds);

        for &(p, a) in &peers {
            network.join(p, a);
        }

        let recovering = DashMap::new();

        let metrics = SyncMutex::new(Metrics {
            preaccept_fast_path: 0,
            preaccept_slow_path: 0,
            recover_nop_count: 0,
            recover_success_count: 0,
        });

        let probe_rtt_countdown = AtomicU64::new(config.optimization.probe_rtt_per_msg_count);

        Ok(Arc::new(Self {
            rid,
            public_peer_addr,
            config,
            state,
            epoch,
            propose_tx,
            join_tx,
            sync_tx,
            graph,
            data_store,
            network,
            recovering,
            metrics,
            probe_rtt_countdown,
        }))
    }

    #[inline]
    pub fn config(&self) -> &ReplicaConfig {
        &self.config
    }

    #[inline]
    pub fn network(&self) -> &N {
        &self.network
    }

    #[inline]
    pub fn metrics(&self) -> Metrics {
        with_mutex(&self.metrics, |m| m.clone())
    }

    #[inline]
    pub fn data_store(&self) -> &D {
        &self.data_store
    }

    #[tracing::instrument(skip_all, fields(rid = ?self.rid, epoch = ?self.epoch.load()))]
    pub async fn handle_message(self: &Arc<Self>, msg: Message<C>) -> Result<()> {
        debug!(msg_variant_name = ?msg.variant_name());

        {
            let countdown = self.probe_rtt_countdown.fetch_sub(1, Relaxed).saturating_sub(1);
            if countdown == 0 {
                let this = Arc::clone(self);
                spawn(async move {
                    debug!("probe_rtt_countdown");
                    if let Err(err) = this.run_probe_rtt().await {
                        error!(?err)
                    }
                });
                let count = self.config.optimization.probe_rtt_per_msg_count;
                self.probe_rtt_countdown.store(count, Relaxed);
            }
        }

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
                self.resume_join(msg).await //
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
                self.resume_sync(msg).await //
            }
            Message::PeerBounds(msg) => {
                self.handle_peer_bounds(msg).await //
            }
        }
    }

    pub async fn run_propose(self: &Arc<Self>, cmd: C) -> Result<()> {
        let mut guard = self.state.lock().await;
        let s = &mut *guard;

        let id = InstanceId(self.rid, s.lid_head.gen_next());
        let pbal = Ballot(Round::ZERO, self.rid);
        let acc = Acc::from_mutable(MutableAcc::with_capacity(1));

        debug!(?id, "run_propose");

        self.phase_preaccept(guard, id, pbal, Some(cmd), acc).await
    }

    #[tracing::instrument(skip_all, fields(id = ?id))]
    async fn phase_preaccept(
        self: &Arc<Self>,
        mut guard: AsyncMutexGuard<'_, State<C, L>>,
        id: InstanceId,
        pbal: Ballot,
        cmd: Option<C>,
        acc: Acc,
    ) -> Result<()> {
        debug!("phase_preaccept");

        let (mut rx, mut seq, mut deps, mut acc, targets) = {
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
            let deps = Deps::from_mutable(deps);

            let abal = pbal;
            let status = Status::PreAccepted;

            let acc = {
                let mut acc = acc;
                acc.cow_insert(self.rid);
                acc
            };

            {
                clone!(cmd, deps, acc);
                let ins = Instance { pbal, cmd, seq, deps, abal, status, acc };
                s.log.save(id, ins, mode).await?;
            }

            let quorum = s.peers.cluster_size().wrapping_sub(2);
            let selected_peers = s.peers.select(quorum, acc.as_ref());

            let rx = {
                let (tx, rx) = mpsc::channel(quorum);
                self.propose_tx.insert(id, tx);
                rx
            };

            let avg_rtt = s.peers.get_avg_rtt();

            drop(guard);

            let targets = selected_peers.to_merged();

            {
                debug!(?selected_peers, "broadcast preaccept");

                clone!(deps, acc);
                let sender = self.rid;
                let epoch = self.epoch.load();

                let msg: _ = PreAccept { sender, epoch, id, pbal, cmd: Some(cmd), seq, deps, acc };

                if self.config.optimization.enable_acc {
                    net::broadcast_preaccept(&self.network, selected_peers.acc, selected_peers.others, msg);
                } else {
                    self.network.broadcast(targets.clone(), Message::PreAccept(msg))
                }
            }

            if pbal.0 == Round::ZERO {
                self.spawn_recover_timeout(id, avg_rtt);
            }

            (rx, seq, deps.into_mutable(), acc.into_mutable(), targets)
        };

        {
            let mut received: VecSet<ReplicaId> = VecSet::new();
            let mut all_same = true;

            let avg_rtt = {
                let mut guard = self.state.lock().await;
                let s = &mut *guard;
                s.peers.get_avg_rtt()
            };
            let t = {
                let conf = &self.config.preaccept_timeout;
                let default = Duration::from_micros(conf.default_us);
                conf.with(avg_rtt, |d| d * 2 + default)
            };
            debug!(?avg_rtt, timeout=?t, "calc preaccept timeout");

            loop {
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

                        {
                            let msg_epoch = match msg {
                                PreAcceptReply::Ok(ref msg) => msg.epoch,
                                PreAcceptReply::Diff(ref msg) => msg.epoch,
                            };

                            if msg_epoch < self.epoch.load() {
                                continue;
                            }
                        }

                        debug!("received preaccept reply");

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
                            acc.insert(msg_sender);
                        }

                        match msg {
                            PreAcceptReply::Ok(_) => {}
                            PreAcceptReply::Diff(msg) => {
                                let mut new_seq = msg.seq;
                                let mut new_deps = msg.deps.into_mutable();

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

                        let deps = Deps::from_mutable(deps);
                        let acc = Acc::from_mutable(acc);

                        with_mutex(&self.metrics, |m| {
                            if is_fast_path {
                                m.preaccept_fast_path = m.preaccept_fast_path.wrapping_add(1);
                            } else {
                                m.preaccept_slow_path = m.preaccept_slow_path.wrapping_add(1);
                            }
                        });

                        if is_fast_path {
                            debug!("fast path");
                            return self.phase_commit(guard, id, pbal, cmd, seq, deps, acc).await;
                        } else {
                            debug!("slow path");
                            return self.phase_accept(guard, id, pbal, cmd, seq, deps, acc).await;
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
                    debug!("preaccept timeout: not enough replies");

                    let no_reply_targets = {
                        let mut all_targets = targets;
                        all_targets.difference_copied(&received);
                        all_targets
                    };

                    s.peers.set_inf_rtt(&no_reply_targets);

                    return Ok(());
                }

                debug!("preaccept timeout: goto slow path");

                s.log.load(id).await?;
                let pbal = s.log.get_cached_pbal(id).expect("pbal should exist");

                let _ = self.propose_tx.remove(&id);

                let cmd = None;
                let deps = Deps::from_mutable(deps);
                let acc = Acc::from_mutable(acc);

                with_mutex(&self.metrics, |m| {
                    m.preaccept_slow_path = m.preaccept_slow_path.wrapping_add(1);
                });

                self.phase_accept(guard, id, pbal, cmd, seq, deps, acc).await
            }
        }
    }

    #[tracing::instrument(skip_all, fields(id = ?msg.id))]
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

        let (seq, deps) = {
            let (mut seq, mut deps) = s.log.calc_attributes(id, &cmd.keys());
            max_assign(&mut seq, msg.seq);
            deps.merge(msg.deps.as_ref());
            (seq, Deps::from_mutable(deps))
        };

        let is_changed = seq != msg.seq || deps != msg.deps;

        let abal = pbal;
        let status = Status::PreAccepted;

        let mut acc = msg.acc;
        acc.cow_insert(self.rid);

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
            self.network.send_one(
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

    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(skip_all, fields(id = ?id))]
    async fn phase_accept(
        self: &Arc<Self>,
        mut guard: AsyncMutexGuard<'_, State<C, L>>,
        id: InstanceId,
        pbal: Ballot,
        cmd: Option<C>,
        seq: Seq,
        deps: Deps,
        acc: Acc,
    ) -> Result<()> {
        debug!("phase_accept");

        let (mut rx, mut acc) = {
            let s = &mut *guard;

            let abal = pbal;
            let status = Status::Accepted;

            let quorum = s.peers.cluster_size() / 2;
            let selected_peers = s.peers.select(quorum, acc.as_ref());

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
                debug!(?selected_peers, "broadcast accept");

                clone!(acc);
                let sender = self.rid;
                let epoch = self.epoch.load();

                let msg = Accept { sender, epoch, id, pbal, cmd: Some(cmd), seq, deps, acc };

                if self.config.optimization.enable_acc {
                    net::broadcast_accept(&self.network, selected_peers.acc, selected_peers.others, msg);
                } else {
                    let targets = selected_peers.into_merged();
                    self.network.broadcast(targets, Message::Accept(msg));
                }
            }

            (rx, acc.into_mutable())
        };

        {
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

                debug!("received accept reply");

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
                    acc.insert(msg.sender);
                }

                let cluster_size = s.peers.cluster_size();

                if received.len() < cluster_size / 2 {
                    continue;
                }

                let _ = self.propose_tx.remove(&id);

                let cmd = None;
                let acc = Acc::from_mutable(acc);
                return self.phase_commit(guard, id, pbal, cmd, seq, deps, acc).await;
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip_all, fields(id = ?msg.id))]
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
        acc.cow_insert(self.rid);

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
            self.network.send_one(target, Message::AcceptOk(AcceptOk { sender, epoch, id, pbal }));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(skip_all, fields(id = ?id))]
    async fn phase_commit(
        self: &Arc<Self>,
        mut guard: AsyncMutexGuard<'_, State<C, L>>,
        id: InstanceId,
        pbal: Ballot,
        cmd: Option<C>,
        seq: Seq,
        deps: Deps,
        acc: Acc,
    ) -> Result<()> {
        debug!("phase_commit");

        let s = &mut *guard;

        let abal = pbal;
        let status = Status::Committed;

        let quorum = s.peers.cluster_size().wrapping_sub(1);
        let selected_peers = s.peers.select(quorum, acc.as_ref());

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
            debug!(?selected_peers, "broadcast commit");

            let sender = self.rid;
            let epoch = self.epoch.load();
            clone!(cmd, deps);

            let msg: _ = Commit { sender, epoch, id, pbal, cmd: Some(cmd), seq, deps, acc };

            if self.config.optimization.enable_acc {
                net::broadcast_commit(&self.network, selected_peers.acc, selected_peers.others, msg);
            } else {
                let targets = selected_peers.into_merged();
                self.network.broadcast(targets, Message::Commit(msg));
            }
        }

        {
            self.spawn_execute(id, cmd, seq, deps, status)
        }

        Ok(())
    }

    #[tracing::instrument(skip_all, fields(id = ?msg.id))]
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

        let status = match s.log.get_cached_ins(id) {
            Some(ins) if ins.status > Status::Committed => ins.status,
            _ => Status::Committed,
        };

        let seq = msg.seq;
        let deps = msg.deps;

        let abal = pbal;

        let mut acc = msg.acc;
        acc.cow_insert(self.rid);

        {
            clone!(cmd, deps);
            let ins: _ = Instance { pbal, cmd, seq, deps, abal, status, acc };
            s.log.save(id, ins, mode).await?
        }

        drop(guard);

        self.spawn_execute(id, cmd, seq, deps, status);

        Ok(())
    }

    #[tracing::instrument(skip_all, fields(id = ?id))]
    async fn run_recover(self: &Arc<Self>, id: InstanceId) -> Result<()> {
        loop {
            debug!("run_recover");

            let mut rx = {
                let mut guard = self.state.lock().await;
                let s = &mut *guard;

                s.log.load(id).await?;

                if let Some(ins) = s.log.get_cached_ins(id) {
                    if ins.status >= Status::Committed {
                        let cmd = ins.cmd.clone();
                        let seq = ins.seq;
                        let deps = ins.deps.clone();
                        let status = ins.status;
                        let _ = self.graph.init_node(id, cmd, seq, deps, status);
                        return Ok(());
                    }
                }

                let pbal = match s.log.get_cached_pbal(id) {
                    Some(Ballot(rnd, _)) => Ballot(rnd.add_one(), self.rid),
                    None => Ballot(Round::ZERO, self.rid),
                };

                let known = matches!(s.log.get_cached_ins(id), Some(ins) if ins.cmd.is_nop().not());

                let mut targets = s.peers.select_all();

                let rx = {
                    let (tx, rx) = mpsc::channel(targets.len());
                    self.propose_tx.insert(id, tx);
                    rx
                };

                drop(guard);

                let _ = targets.insert(self.rid);

                {
                    let sender = self.rid;
                    let epoch = self.epoch.load();
                    self.network.broadcast(
                        targets,
                        Message::Prepare(Prepare { sender, epoch, id, pbal, known }),
                    );

                    let this = Arc::clone(self);
                    spawn(async move {
                        if let Err(err) =
                            this.handle_prepare(Prepare { sender, epoch, id, pbal, known }).await
                        {
                            error!(?id, ?err)
                        }
                    });
                }

                rx
            };

            {
                let mut received: VecSet<ReplicaId> = VecSet::new();

                let mut max_abal: Option<Ballot> = None;
                let mut cmd: Option<C> = None;

                // (sender, seq, deps, status, acc)
                let mut tuples: Vec<(ReplicaId, Seq, Deps, Status, Acc)> = Vec::new();

                // adaptive?
                let timeout = Duration::from_micros(self.config.recover_timeout.default_us);

                while let Ok(Some(msg)) = recv_timeout(&mut rx, timeout).await {
                    let msg = match PrepareReply::convert(msg) {
                        Some(m) => m,
                        None => continue,
                    };

                    let msg_epoch = match msg {
                        PrepareReply::Ok(ref msg) => msg.epoch,
                        PrepareReply::Nack(ref msg) => msg.epoch,
                        PrepareReply::Unchosen(ref msg) => msg.epoch,
                    };

                    if msg_epoch < self.epoch.load() {
                        continue;
                    }

                    debug!(received_len = ?received.len(), "receive new prepare reply");

                    let mut guard = self.state.lock().await;
                    let s = &mut *guard;

                    match msg {
                        PrepareReply::Ok(ref msg) => assert_eq!(id, msg.id),
                        PrepareReply::Nack(ref msg) => assert_eq!(id, msg.id),
                        PrepareReply::Unchosen(ref msg) => assert_eq!(id, msg.id),
                    };

                    s.log.load(id).await?;
                    if let PrepareReply::Ok(ref msg) = msg {
                        if s.log.should_ignore_pbal(id, msg.pbal) {
                            continue;
                        }
                    }

                    match msg {
                        PrepareReply::Unchosen(msg) => {
                            let _ = received.insert(msg.sender);
                        }
                        PrepareReply::Nack(msg) => {
                            s.log.save_pbal(id, msg.pbal).await?;

                            let avg_rtt = s.peers.get_avg_rtt();

                            let _ = self.propose_tx.remove(&id);

                            drop(guard);

                            self.spawn_nack_recover_timeout(id, avg_rtt);
                            return Ok(());
                        }
                        PrepareReply::Ok(msg) => {
                            let _ = received.insert(msg.sender);

                            let is_max_abal = match max_abal {
                                Some(ref mut max_abal) => match Ord::cmp(&msg.abal, max_abal) {
                                    Ordering::Less => false,
                                    Ordering::Equal => true,
                                    Ordering::Greater => {
                                        *max_abal = msg.abal;
                                        cmd = None;
                                        tuples.clear();
                                        true
                                    }
                                },
                                None => {
                                    max_abal = Some(msg.abal);
                                    true
                                }
                            };
                            if is_max_abal.not() {
                                continue;
                            }
                            cmd = msg.cmd;
                            tuples.push((msg.sender, msg.seq, msg.deps, msg.status, msg.acc));
                        }
                    }

                    let cluster_size = s.peers.cluster_size();
                    if received.len() <= cluster_size / 2 {
                        continue;
                    }

                    let max_abal = match max_abal {
                        Some(b) => b,
                        None => continue,
                    };

                    let _ = self.propose_tx.remove(&id);

                    let pbal = s.log.get_cached_pbal(id).expect("pbal should exist");

                    debug!(?pbal, "recover succeeded");
                    with_mutex(&self.metrics, |m| {
                        m.recover_success_count = m.recover_success_count.wrapping_add(1);
                    });

                    let acc = {
                        let mut acc = match s.log.get_cached_ins(id) {
                            Some(ins) => MutableAcc::clone(ins.acc.as_ref()),
                            None => MutableAcc::default(),
                        };
                        for (_, _, _, _, a) in tuples.iter() {
                            acc.union(a.as_ref());
                        }
                        Acc::from_mutable(acc)
                    };

                    for &mut (_, seq, ref mut deps, status, _) in tuples.iter_mut() {
                        if status >= Status::Committed {
                            let deps = mem::take(deps);
                            return self.phase_commit(guard, id, pbal, cmd, seq, deps, acc).await;
                        } else if status == Status::Accepted {
                            let deps = mem::take(deps);
                            return self.phase_accept(guard, id, pbal, cmd, seq, deps, acc).await;
                        }
                    }

                    tuples.retain(|t| t.3 == Status::PreAccepted);

                    let enable_accept = max_abal.0 == Round::ZERO
                        && tuples.len() >= cluster_size / 2
                        && tuples.iter().all(|t| t.0 != id.0);

                    if enable_accept {
                        #[allow(clippy::mutable_key_type)]
                        let mut buckets: HashMap<(Seq, &mut Deps), usize> = HashMap::new();
                        for &mut (_, seq, ref mut deps, _, _) in tuples.iter_mut() {
                            let cnt = buckets.entry((seq, deps)).or_default();
                            *cnt = cnt.wrapping_add(1);
                        }
                        let mut max_cnt_attr = None;
                        let mut max_cnt = 0;
                        for (attr, cnt) in buckets {
                            if cnt > max_cnt {
                                max_cnt_attr = Some(attr);
                                max_cnt = cnt;
                            }
                        }
                        if max_cnt >= cluster_size / 2 {
                            if let Some(attr) = max_cnt_attr {
                                let seq = attr.0;
                                let deps = mem::take(attr.1);
                                return self.phase_accept(guard, id, pbal, cmd, seq, deps, acc).await;
                            }
                        }
                    }

                    if tuples.is_empty().not() {
                        return self.phase_preaccept(guard, id, pbal, cmd, acc).await;
                    }

                    let (cmd, acc) = match s.log.get_cached_ins(id) {
                        Some(_) => (None, acc),
                        None => {
                            with_mutex(&self.metrics, |m| {
                                m.recover_nop_count = m.recover_nop_count.wrapping_add(1);
                            });
                            (Some(C::create_nop()), Acc::default())
                        }
                    };

                    return self.phase_preaccept(guard, id, pbal, cmd, acc).await;
                }
            }

            {
                // adaptive?
                let timeout = Duration::from_micros(self.config.recover_timeout.default_us);
                sleep(timeout).await
            }
        }
    }

    // fn spawn_recover_immediately(self: &Arc<Self>, id: InstanceId) {
    //     let this = Arc::clone(self);
    //     let task = spawn(async move {
    //         if let Err(err) = this.run_recover(id).await {
    //             error!(?id, ?err);
    //         }
    //         this.recovering.remove(&id);
    //     });
    //     if let Some(prev) = self.recovering.insert(id, task) {
    //         prev.abort();
    //     }
    // }

    #[allow(clippy::float_arithmetic)]
    fn spawn_recover_timeout(self: &Arc<Self>, id: InstanceId, avg_rtt: Option<Duration>) {
        let conf = &self.config.recover_timeout;
        let duration = conf.with(avg_rtt, |d| {
            let rate: f64 = rand::thread_rng().gen_range(4.0..6.0);
            let delta = Duration::from_secs_f64(d.as_secs_f64() * rate);
            Duration::from_micros(conf.default_us) + delta
        });

        if let dashmap::mapref::entry::Entry::Vacant(e) = self.recovering.entry(id) {
            let this = Arc::clone(self);
            let task = spawn(async move {
                sleep(duration).await;
                if let Err(err) = this.run_recover(id).await {
                    error!(?id, ?err);
                }
                this.recovering.remove(&id);
            });
            e.insert(task);
        }
    }

    #[allow(clippy::float_arithmetic)]
    fn spawn_nack_recover_timeout(self: &Arc<Self>, id: InstanceId, avg_rtt: Option<Duration>) {
        let conf = &self.config.recover_timeout;
        let duration = conf.with(avg_rtt, |d| {
            let rate: f64 = rand::thread_rng().gen_range(1.0..4.0);
            Duration::from_secs_f64(d.as_secs_f64() * rate)
        });
        if let dashmap::mapref::entry::Entry::Vacant(e) = self.recovering.entry(id) {
            let this = Arc::clone(self);
            let task = spawn(async move {
                sleep(duration).await;
                if let Err(err) = this.run_recover(id).await {
                    error!(?id, ?err);
                }
                this.recovering.remove(&id);
            });
            e.insert(task);
        }
    }

    #[tracing::instrument(skip_all, fields(id = ?msg.id))]
    async fn handle_prepare(self: &Arc<Self>, msg: Prepare) -> Result<()> {
        if msg.epoch < self.epoch.load() {
            return Ok(());
        }

        debug!(id =?msg.id, "handle_prepare");

        let mut guard = self.state.lock().await;
        let s = &mut *guard;

        let id = msg.id;

        s.log.load(id).await?;

        let epoch = self.epoch.load();

        let reply: Result<Message<C>> = async {
            if let Some(pbal) = s.log.get_cached_pbal(id) {
                if pbal >= msg.pbal {
                    let sender = self.rid;
                    return Ok(Message::PrepareNack(PrepareNack { sender, epoch, id, pbal }));
                }
            }

            let pbal = msg.pbal;

            s.log.save_pbal(id, pbal).await?;

            let ins: _ = match s.log.get_cached_ins(id) {
                Some(ins) => ins,
                None => {
                    let sender = self.rid;
                    return Ok(Message::PrepareUnchosen(PrepareUnchosen { sender, epoch, id }));
                }
            };

            let cmd = if msg.known && ins.cmd.is_nop().not() {
                None
            } else {
                Some(ins.cmd.clone())
            };

            let seq = ins.seq;
            let deps = ins.deps.clone();
            let abal = ins.abal;
            let status = ins.status;
            let acc = ins.acc.clone();

            let sender = self.rid;
            Ok(Message::PrepareOk(PrepareOk {
                sender,
                epoch,
                id,
                pbal,
                cmd,
                seq,
                deps,
                abal,
                status,
                acc,
            }))
        }
        .await;

        let reply = reply?;

        let target = msg.sender;
        if target == self.rid {
            self.spawn_handle_message(reply);
        } else {
            self.network.send_one(target, reply)
        }

        Ok(())
    }

    fn spawn_handle_message(self: &Arc<Self>, msg: Message<C>) {
        let this = Arc::clone(self);
        spawn(async move {
            if let Err(err) = this.handle_message(msg).await {
                error!(?err)
            }
        });
    }

    pub async fn run_join(self: &Arc<Self>) -> Result<bool> {
        debug!("run_join");

        let mut rx = {
            let mut guard = self.state.lock().await;
            let s = &mut *guard;

            let targets = s.peers.select_all();

            drop(guard);

            // 1 -> 2
            if targets.is_empty() {
                return Ok(true);
            }

            // 2 -> 3
            if targets.len() == 1 {
                let target = targets.as_slice()[0];
                let sender = self.rid;
                let epoch = self.epoch.load();
                let addr = self.public_peer_addr;
                self.network.send_one(target, Message::Join(Join { sender, epoch, addr }));
                return Ok(true);
            }

            let rx = {
                let (tx, rx) = mpsc::channel(targets.len());
                let _ = self.join_tx.lock().insert(tx);
                rx
            };
            {
                let sender = self.rid;
                let epoch = self.epoch.load();
                let addr = self.public_peer_addr;
                self.network.broadcast(targets, Message::Join(Join { sender, epoch, addr }));
            }
            rx
        };

        {
            let join_timeout = Duration::from_micros(self.config.join_timeout.default_us);

            let mut received = VecSet::new();
            while let Ok(Some(msg)) = recv_timeout(&mut rx, join_timeout).await {
                let _ = received.insert(msg.sender);

                let mut guard = self.state.lock().await;
                let s = &mut *guard;

                let cluster_size = s.peers.cluster_size();

                drop(guard);

                if received.len() > cluster_size / 2 {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    #[tracing::instrument(skip_all, fields(?msg))]
    async fn handle_join(self: &Arc<Self>, msg: Join) -> Result<()> {
        let mut guard = self.state.lock().await;
        let s = &mut *guard;

        if let Some(prev) = self.network.join(msg.sender, msg.addr) {
            s.peers.remove(prev);
        }

        s.peers.add(msg.sender);

        self.epoch.update_max(msg.epoch);

        drop(guard);

        {
            let target = msg.sender;
            self.network.send_one(target, Message::JoinOk(JoinOk { sender: self.rid }));
        }

        Ok(())
    }

    async fn resume_join(self: &Arc<Self>, msg: JoinOk) -> Result<()> {
        let tx = self.join_tx.lock().clone();
        if let Some(tx) = tx {
            let _ = tx.send(msg).await;
        }
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(?msg))]
    async fn handle_leave(self: &Arc<Self>, msg: Leave) -> Result<()> {
        let mut guard = self.state.lock().await;
        let s = &mut *guard;

        s.peers.remove(msg.sender);

        drop(guard);

        Ok(())
    }

    pub async fn run_probe_rtt(&self) -> Result<()> {
        let mut guard = self.state.lock().await;
        let s = &mut *guard;

        let targets = s.peers.select_all();

        drop(guard);

        debug!(?targets, "run_probe_rtt");

        let sender = self.rid;
        let time = LocalInstant::now();
        self.network.broadcast(targets, Message::ProbeRtt(ProbeRtt { sender, time }));
        Ok(())
    }

    async fn handle_probe_rtt(self: &Arc<Self>, msg: ProbeRtt) -> Result<()> {
        let sender = self.rid;
        let target = msg.sender;
        let time = msg.time;
        self.network.send_one(target, Message::ProbeRttOk(ProbeRttOk { sender, time }));
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn handle_probe_rtt_ok(self: &Arc<Self>, msg: ProbeRttOk) -> Result<()> {
        let time = LocalInstant::now();
        let peer = msg.sender;
        let rtt = time.saturating_duration_since(msg.time);

        debug!(?peer, ?rtt);

        {
            let mut guard = self.state.lock().await;
            let s = &mut *guard;
            s.peers.set_rtt(peer, rtt);
        }

        Ok(())
    }

    pub async fn run_sync_known(self: &Arc<Self>) -> Result<()> {
        let mut guard = self.state.lock().await;
        let s = &mut *guard;

        s.log.update_bounds();
        let known_up_to = s.log.known_up_to();

        let target = match s.peers.select_one() {
            Some(t) => t,
            None => return Ok(()),
        };

        drop(guard);

        {
            let sender = self.rid;
            let addr = self.public_peer_addr;
            self.network.send_one(target, Message::AskLog(AskLog { sender, addr, known_up_to }));
        }

        Ok(())
    }

    async fn handle_ask_log(self: &Arc<Self>, msg: AskLog) -> Result<()> {
        self.network.join(msg.sender, msg.addr);

        let mut guard = self.state.lock().await;
        let s = &mut *guard;

        s.log.update_bounds();
        let local_known_up_to = s.log.known_up_to();

        let target = msg.sender;
        let sender = self.rid;
        let sync_id = SyncId::ZERO;
        let send_log = |instances| {
            self.network.send_one(target, Message::SyncLog(SyncLog { sender, sync_id, instances }))
        };

        let conf = &self.config.sync_limits;
        let limit: usize = conf.max_instance_num.numeric_cast();

        for &(rid, lower) in msg.known_up_to.iter() {
            let higher = match local_known_up_to.get(&rid) {
                Some(&h) => h,
                None => continue,
            };

            let mut instances: Vec<(InstanceId, Instance<C>)> = Vec::new();

            for lid in LocalInstanceId::range_inclusive(lower.add_one(), higher) {
                let id = InstanceId(rid, lid);

                s.log.load(id).await?;

                if let Some(ins) = s.log.get_cached_ins(id) {
                    instances.push((id, ins.clone()));
                    if instances.len() >= limit {
                        send_log(mem::take(&mut instances))
                    }
                }
            }

            if instances.is_empty().not() {
                send_log(instances)
            }
        }

        drop(guard);

        Ok(())
    }

    pub async fn run_sync_committed(self: &Arc<Self>) -> Result<bool> {
        let (rxs, quorum) = {
            let mut guard = self.state.lock().await;
            let s = &mut *guard;

            s.log.update_bounds();

            let local_bounds = s.log.committed_up_to();
            let peer_bounds = s.peer_status_bounds.committed_up_to();

            let mut rxs = Vec::new();

            let targets = s.peers.select_all();
            let sender = self.rid;
            let mut send_log = |s: &mut State<_, _>, instances| {
                let sync_id = s.sync_id_head.gen_next();
                let (tx, rx) = mpsc::channel(targets.len());
                let _ = self.sync_tx.insert(sync_id, tx);
                rxs.push((sync_id, rx));

                clone!(targets);
                self.network.broadcast(targets, Message::SyncLog(SyncLog { sender, sync_id, instances }))
            };

            for &(rid, higher) in local_bounds.iter() {
                let lower: _ = peer_bounds.get(&rid).copied().unwrap_or(LocalInstanceId::ZERO);

                let conf = &self.config.sync_limits;
                let limit: usize = conf.max_instance_num.numeric_cast();

                let mut instances: _ = <Vec<(InstanceId, Instance<C>)>>::new();

                for lid in LocalInstanceId::range_inclusive(lower.add_one(), higher) {
                    let id: _ = InstanceId(rid, lid);

                    s.log.load(id).await?;

                    let ins = match s.log.get_cached_ins(id) {
                        Some(ins) if ins.status >= Status::Committed => ins,
                        _ => continue,
                    };

                    instances.push((id, ins.clone()));

                    if instances.len() >= limit {
                        send_log(s, mem::take(&mut instances));
                    }
                }

                if instances.is_empty().not() {
                    send_log(s, instances);
                }
            }

            drop(guard);

            (rxs, targets.len())
        };

        let mut handles = Vec::with_capacity(rxs.len());
        let mut sync_ids = Vec::with_capacity(rxs.len());
        for (sync_id, mut rx) in rxs {
            let this = Arc::clone(self);
            let handle = spawn(async move {
                let mut received: VecSet<ReplicaId> = VecSet::new();
                while let Some(msg) = rx.recv().await {
                    let _ = received.insert(msg.sender);
                    if received.len() >= quorum / 2 {
                        break;
                    }
                }
                let _ = this.sync_tx.remove(&sync_id);
                received.len() >= quorum / 2
            });
            handles.push(handle);
            sync_ids.push(sync_id);
        }

        let _guard: _ = scopeguard::guard_on_success(sync_ids, |sync_ids: _| {
            for sync_id in sync_ids {
                let _ = self.sync_tx.remove(&sync_id);
            }
        });

        let ans = join_all(handles).await;
        let is_succeeded = ans.iter().all(|ret| matches!(ret, Ok(true)));

        Ok(is_succeeded)
    }

    async fn handle_sync_log(self: &Arc<Self>, msg: SyncLog<C>) -> Result<()> {
        let mut guard = self.state.lock().await;
        let s = &mut *guard;

        for (id, mut ins) in msg.instances {
            s.log.load(id).await?;
            match s.log.get_cached_ins(id) {
                None => {
                    s.log.save(id, ins, UpdateMode::Full).await?;
                }
                Some(saved_ins) => {
                    if saved_ins.status < Status::Committed && ins.status >= Status::Committed {
                        max_assign(&mut ins.pbal, saved_ins.pbal);
                        max_assign(&mut ins.abal, saved_ins.abal);
                        ins.status = Status::Committed;

                        ins.acc.cow_insert(self.rid);
                        let mode = if saved_ins.cmd.is_nop() != ins.cmd.is_nop() {
                            UpdateMode::Full
                        } else {
                            UpdateMode::Partial
                        };

                        {
                            let cmd = ins.cmd.clone();
                            let seq = ins.seq;
                            let deps = ins.deps.clone();
                            let status = ins.status;

                            s.log.save(id, ins, mode).await?;
                            self.spawn_execute(id, cmd, seq, deps, status);
                        }
                    }
                }
            }
        }

        drop(guard);

        if msg.sync_id != SyncId::ZERO {
            let target = msg.sender;
            let sender = self.rid;
            let sync_id = msg.sync_id;
            self.network.send_one(target, Message::SyncLogOk(SyncLogOk { sender, sync_id }));
        }

        Ok(())
    }

    async fn resume_sync(self: &Arc<Self>, msg: SyncLogOk) -> Result<()> {
        let tx = self.sync_tx.get(&msg.sync_id).as_deref().cloned();
        if let Some(tx) = tx {
            let _ = tx.send(msg).await;
        }
        Ok(())
    }

    pub async fn run_broadcast_bounds(self: &Arc<Self>) -> Result<()> {
        let mut guard = self.state.lock().await;
        let s = &mut *guard;

        let committed_up_to = s.log.committed_up_to();
        if committed_up_to.is_empty() {
            return Ok(());
        }

        let targets = s.peers.select_all();

        drop(guard);

        {
            let sender = self.rid;
            self.network.broadcast(
                targets,
                Message::PeerBounds(PeerBounds { sender, committed_up_to: Some(committed_up_to) }),
            )
        }

        Ok(())
    }

    async fn handle_peer_bounds(self: &Arc<Self>, msg: PeerBounds) -> Result<()> {
        debug!(?msg, "handle_peer_bounds");

        let mut guard = self.state.lock().await;
        let s = &mut *guard;

        if let Some(bounds) = msg.committed_up_to {
            s.peer_status_bounds.set_committed(msg.sender, bounds);
        }

        drop(guard);

        Ok(())
    }

    fn spawn_execute(self: &Arc<Self>, id: InstanceId, cmd: C, seq: Seq, deps: Deps, status: Status) {
        if let Some((_, task)) = self.recovering.remove(&id) {
            task.abort()
        }

        match Ord::cmp(&status, &Status::Committed) {
            Ordering::Less => panic!("unexpected status: {:?}", status),
            Ordering::Equal => {}
            Ordering::Greater => return,
        }

        let _ = self.graph.init_node(id, cmd, seq, deps, status);

        let this = Arc::clone(self);
        spawn(async move {
            if let Err(err) = this.run_execute(id).await {
                error!(?id, ?err)
            }
        });
    }

    #[tracing::instrument(skip_all, fields(id = ?id))]
    async fn run_execute(self: &Arc<Self>, id: InstanceId) -> Result<()> {
        debug!("run_execute");

        let _executing = match self.graph.executing(id) {
            Some(exec) => exec,
            None => return Ok(()),
        };

        let mut local_graph = LocalGraph::new();

        debug!("wait graph");

        {
            let _row_guard = self.graph.lock_row(id.0).await;

            let mut q = DepsQueue::from_single(id);

            while let Some(id) = q.pop() {
                if local_graph.contains_node(id) {
                    continue;
                }

                debug!("bfs waiting node {:?}", id);

                let node = match self.graph.wait_node(id).await {
                    Some(node) => node,
                    None => continue, // executed node
                };

                {
                    let guard = node.status.lock();
                    let status = *guard;
                    if status > ExecStatus::Committed {
                        continue;
                    }
                }

                local_graph.add_node(id, Asc::clone(&node));

                let InstanceId(rid, lid) = id;

                let wm = self.graph.watermark(rid);

                {
                    let start = LocalInstanceId::from(wm.level().saturating_add(1));
                    let end = lid.sub_one();

                    if start <= end {
                        let mut guard = self.state.lock().await;
                        let s = &mut *guard;
                        let avg_rtt = s.peers.get_avg_rtt();
                        drop(guard);

                        for l in LocalInstanceId::range_inclusive(start, end) {
                            let id = InstanceId(rid, l);
                            self.spawn_recover_timeout(id, avg_rtt)
                        }
                    }
                }

                wm.until(lid.raw_value()).wait().await;

                for d in node.deps.elements() {
                    if local_graph.contains_node(d) {
                        continue;
                    }
                    q.push(d);
                }
            }
        }

        let local_graph_nodes_count = local_graph.nodes_count();
        debug!(local_graph_nodes_count, "tarjan scc");

        if local_graph_nodes_count == 0 {
            return Ok(()); // ins executed
        } else if local_graph_nodes_count == 1 {
            // common case
            let node = local_graph.get_node(id).cloned().unwrap();
            let this = Arc::clone(self);
            spawn(async move {
                if let Err(err) = this.run_execute_single_node(id, node).await {
                    error!(?err);
                }
            });
        } else {
            let mut scc_list = local_graph.tarjan_scc(id);
            for scc in &mut scc_list {
                scc.sort_by_key(|&(InstanceId(rid, lid), ref node): _| (node.seq, lid, rid));
            }
            assert!(scc_list.is_empty().not());

            scc_list.retain(|scc| {
                let mut needs_issue = true;

                let mut stack = Vec::with_capacity(scc.len());

                for (_, node) in scc {
                    let guard = node.status.lock();
                    stack.push(guard);
                }

                for status in iter_mut_deref(&mut stack) {
                    needs_issue = *status == ExecStatus::Committed;
                    *status = ExecStatus::Issuing;
                }

                while let Some(guard) = stack.pop() {
                    drop(guard);
                }

                needs_issue
            });

            let flag_group = FlagGroup::new(scc_list.len());

            for (idx, scc) in scc_list.into_iter().enumerate() {
                clone!(flag_group);
                let this = Arc::clone(self);
                spawn(async move {
                    if let Err(err) = this.run_execute_scc(scc, flag_group, idx).await {
                        error!(?err);
                    }
                });
            }
        }
        Ok(())
    }

    async fn run_execute_single_node(self: &Arc<Self>, id: InstanceId, node: Asc<InsNode<C>>) -> Result<()> {
        let notify = Asc::new(ExecNotify::new());
        {
            clone!(notify);
            let cmd = node.cmd.clone();
            self.data_store.issue(id, cmd, notify).await?;
        }
        notify.wait_issued().await;

        let prev_status = {
            let mut guard = self.state.lock().await;
            let s = &mut *guard;
            let status = notify.status();
            s.log.update_status(id, status).await?;
            match status {
                Status::Issued => *node.status.lock() = ExecStatus::Issued,
                Status::Executed => *node.status.lock() = ExecStatus::Executed,
                _ => {}
            }
            status
        };
        notify.wait_executed().await;

        if prev_status < Status::Executed {
            let mut guard = self.state.lock().await;
            let s = &mut *guard;
            s.log.update_status(id, Status::Executed).await?;
            *node.status.lock() = ExecStatus::Executed;
        }

        self.graph.retire_node(id);
        if let Some((_, task)) = self.recovering.remove(&id) {
            task.abort()
        }

        {
            let mut guard = self.state.lock().await;
            let s = &mut *guard;
            s.log.retire_instance(id);
            debug!(?id, "retire instance");
        }

        Ok(())
    }

    async fn run_execute_scc(
        self: &Arc<Self>,
        scc: Vec<(InstanceId, Asc<InsNode<C>>)>,
        flag_group: FlagGroup,
        idx: usize,
    ) -> Result<()> {
        if idx > 0 {
            flag_group.wait(idx.wrapping_sub(1)).await;
        }

        let mut handles = Vec::with_capacity(scc.len());

        for &(id, ref node) in &scc {
            let notify = Asc::new(ExecNotify::new());
            {
                clone!(notify);
                let cmd = node.cmd.clone();
                self.data_store.issue(id, cmd, notify).await?;
            }
            handles.push(notify)
        }

        for n in &handles {
            n.wait_issued().await;
        }

        flag_group.set(idx);

        let mut prev_status = Vec::with_capacity(handles.len());

        {
            let mut guard = self.state.lock().await;
            let s = &mut *guard;
            for (&(id, ref node), n) in scc.iter().zip(handles.iter()) {
                let status = n.status();
                s.log.update_status(id, status).await?;
                match status {
                    Status::Issued => *node.status.lock() = ExecStatus::Issued,
                    Status::Executed => *node.status.lock() = ExecStatus::Executed,
                    _ => {}
                };
                prev_status.push(status);
            }
        }

        for n in &handles {
            n.wait_executed().await;
        }

        {
            let mut guard = self.state.lock().await;
            let s = &mut *guard;
            for (&(id, ref node), &prev) in scc.iter().zip(prev_status.iter()) {
                if Status::Executed > prev {
                    s.log.update_status(id, Status::Executed).await?;
                    *node.status.lock() = ExecStatus::Executed;
                }
            }
        }

        for &(id, _) in &scc {
            self.graph.retire_node(id);
            if let Some((_, task)) = self.recovering.remove(&id) {
                task.abort()
            }
        }

        {
            let mut guard = self.state.lock().await;
            let s = &mut *guard;
            for &(id, _) in &scc {
                s.log.retire_instance(id);
                debug!(?id, "retire instance");
            }
        }

        Ok(())
    }

    pub async fn run_clear_key_map(self: &Arc<Self>) {
        let mut guard = self.state.lock().await;
        let s = &mut *guard;
        let garbage = s.log.clear_key_map();
        drop(guard);
        drop(garbage);
    }

    pub async fn run_save_bounds(self: &Arc<Self>) -> Result<()> {
        let mut guard = self.state.lock().await;
        let s = &mut *guard;
        s.log.save_bounds().await
    }
}
