use crate::acc::{Acc, MutableAcc};
use crate::bounds::{PeerStatusBounds, SavedStatusBounds};
use crate::cmd::CommandLike;
use crate::config::ReplicaConfig;
use crate::deps::Deps;
use crate::exec::ExecNotify;
use crate::graph::{DepsQueue, Graph, InsNode, LocalGraph};
use crate::id::*;
use crate::ins::Instance;
use crate::log::{InsGuard, Log};
use crate::msg::*;
use crate::net::{self, Network};
use crate::peers::Peers;
use crate::status::{ExecStatus, Status};
use crate::store::{DataStore, LogStore, UpdateMode};

use epkv_utils::chan::{self, recv_timeout};
use epkv_utils::clone;
use epkv_utils::cmp::max_assign;
use epkv_utils::flag_group::FlagGroup;
use epkv_utils::iter::map_collect;
use epkv_utils::lock::with_mutex;
use epkv_utils::time::LocalInstant;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut, Not};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::*;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{mem, ops};

use anyhow::{ensure, Result};
use asc::Asc;
use dashmap::DashMap;
use fnv::FnvHashSet;
use futures_util::future::join_all;
use numeric_cast::NumericCast;
use ordered_vecmap::VecMap;
use ordered_vecmap::VecSet;
use parking_lot::Mutex as SyncMutex;
use rand::Rng;
use tokio::spawn;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::MutexGuard as AsyncMutexGuard;
use tokio::sync::{mpsc, Semaphore};
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
    state: AsyncMutex<State>,
    log: Log<C, L>,

    propose_tx: DashMap<InstanceId, mpsc::Sender<Message<C>>>,
    join_tx: SyncMutex<Option<mpsc::Sender<JoinOk>>>,
    sync_tx: DashMap<SyncId, mpsc::Sender<SyncLogOk>>,

    graph: Graph<C>,
    data_store: Arc<D>,

    network: N,

    recovering: DashMap<InstanceId, JoinHandle<()>>,
    executing: DashMap<InstanceId, JoinHandle<()>>,
    exec_row_locks: DashMap<ReplicaId, Arc<AsyncMutex<()>>>,
    executing_limit: Arc<Semaphore>,

    metrics: SyncMutex<Metrics>,

    probe_rtt_countdown: AtomicU64,
}

struct State {
    peers: Peers,

    peer_status_bounds: PeerStatusBounds,

    lid_head: Head<LocalInstanceId>,

    sync_id_head: Head<SyncId>,
}

struct StateGuard<'a> {
    guard: AsyncMutexGuard<'a, State>,
    t1: Instant,
}

impl Deref for StateGuard<'_> {
    type Target = State;

    fn deref(&self) -> &Self::Target {
        &*self.guard
    }
}

impl DerefMut for StateGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut *self.guard
    }
}

impl Drop for StateGuard<'_> {
    fn drop(&mut self) {
        debug!(elapsed_us = ?self.t1.elapsed().as_micros(), "unlock state");
    }
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
    pub async fn new(
        meta: ReplicaMeta,
        log_store: Arc<L>,
        data_store: Arc<D>,
        network: N,
    ) -> Result<Arc<Self>> {
        let rid = meta.rid;
        let public_peer_addr = meta.public_peer_addr;
        let epoch = meta.epoch;
        let peers = meta.peers;
        let config = meta.config;

        let addr_set: VecSet<_> = map_collect(&peers, |&(_, a)| a);
        ensure!(peers.iter().all(|&(p, a)| p != rid && a != public_peer_addr));
        ensure!(addr_set.len() == peers.len());

        let epoch = AtomicEpoch::new(epoch);

        let (attr_bounds, status_bounds) = log_store.load_bounds().await??;
        let status_bounds: _ = Asc::new(SyncMutex::new(status_bounds));

        let state = {
            let peers_set: VecSet<_> = map_collect(&peers, |&(p, _)| p);
            let peers = Peers::new(peers_set);

            let lid_head =
                Head::new(attr_bounds.max_lids.get(&rid).copied().unwrap_or(LocalInstanceId::ZERO));

            let sync_id_head = Head::new(SyncId::ZERO);

            let peer_status_bounds = PeerStatusBounds::new();

            AsyncMutex::new(State { peers, lid_head, sync_id_head, peer_status_bounds })
        };

        let log = Log::new(log_store, attr_bounds, Asc::clone(&status_bounds));

        let propose_tx = DashMap::new();
        let join_tx = SyncMutex::new(None);
        let sync_tx = DashMap::new();

        let graph = Graph::new(status_bounds);

        for &(p, a) in &peers {
            network.join(p, a);
        }

        let recovering = DashMap::new();

        let executing = DashMap::new();
        let exec_row_locks = DashMap::new();

        let executing_limit = Arc::new(Semaphore::new(
            config.execution_limits.max_task_num.numeric_cast(),
        ));

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
            log,
            epoch,
            propose_tx,
            join_tx,
            sync_tx,
            graph,
            data_store,
            network,
            recovering,
            executing,
            exec_row_locks,
            executing_limit,
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

    #[inline]
    pub fn rid(&self) -> ReplicaId {
        self.rid
    }

    #[inline]
    #[must_use]
    pub fn dump_saved_status_bounds(&self) -> SavedStatusBounds {
        self.log.saved_status_bounds()
    }

    fn countdown_probe_rtt(self: &Arc<Self>) {
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

    #[tracing::instrument(skip_all, fields(rid = ?self.rid, epoch = ?self.epoch.load()))]
    pub async fn handle_message(self: &Arc<Self>, msg: Message<C>) -> Result<()> {
        debug!(msg_variant_name = ?msg.variant_name());

        self.countdown_probe_rtt();

        let t0 = Instant::now();

        let result = match msg {
            Message::PreAccept(msg) => {
                Box::pin(self.handle_preaccept(msg)).await //
            }
            Message::PreAcceptOk(PreAcceptOk { id, .. }) => {
                Box::pin(self.resume_propose(id, msg)).await //
            }
            Message::PreAcceptDiff(PreAcceptDiff { id, .. }) => {
                Box::pin(self.resume_propose(id, msg)).await //
            }
            Message::Accept(msg) => {
                Box::pin(self.handle_accept(msg)).await //
            }
            Message::AcceptOk(AcceptOk { id, .. }) => {
                Box::pin(self.resume_propose(id, msg)).await //
            }
            Message::Commit(msg) => {
                Box::pin(self.handle_commit(msg)).await //
            }
            Message::Prepare(msg) => {
                Box::pin(self.handle_prepare(msg)).await //
            }
            Message::PrepareOk(PrepareOk { id, .. }) => {
                Box::pin(self.resume_propose(id, msg)).await //
            }
            Message::PrepareNack(PrepareNack { id, .. }) => {
                Box::pin(self.resume_propose(id, msg)).await //
            }
            Message::PrepareUnchosen(PrepareUnchosen { id, .. }) => {
                Box::pin(self.resume_propose(id, msg)).await //
            }
            Message::Join(msg) => {
                Box::pin(self.handle_join(msg)).await //
            }
            Message::JoinOk(msg) => {
                Box::pin(self.resume_join(msg)).await //
            }
            Message::Leave(msg) => {
                Box::pin(self.handle_leave(msg)).await //
            }
            Message::ProbeRtt(msg) => {
                Box::pin(self.handle_probe_rtt(msg)).await //
            }
            Message::ProbeRttOk(msg) => {
                Box::pin(self.handle_probe_rtt_ok(msg)).await //
            }
            Message::AskLog(msg) => {
                Box::pin(self.handle_ask_log(msg)).await //
            }
            Message::SyncLog(msg) => {
                Box::pin(self.handle_sync_log(msg)).await //
            }
            Message::SyncLogOk(msg) => {
                Box::pin(self.resume_sync(msg)).await //
            }
            Message::PeerBounds(msg) => {
                Box::pin(self.handle_peer_bounds(msg)).await //
            }
        };

        debug!(elapsed_us=?t0.elapsed().as_micros());

        result
    }

    async fn lock_state(&self) -> StateGuard<'_> {
        debug!("start to lock state");
        let t0 = Instant::now();
        let guard = self.state.lock().await;
        debug!(elapsed_us = ?t0.elapsed().as_micros(), "locked state");
        let t1 = Instant::now();
        StateGuard { guard, t1 }
    }

    fn random_time(duration: Duration, rate_range: ops::Range<f64>) -> Duration {
        let rate: f64 = rand::thread_rng().gen_range(rate_range);
        duration.mul_f64(rate)
    }

    fn insert_propose_chan(&self, id: InstanceId, chan_size: usize) -> mpsc::Receiver<Message<C>> {
        let (tx, rx) = mpsc::channel(chan_size);
        self.propose_tx.insert(id, tx);
        debug!(?id, "insert_propose_chan");
        rx
    }

    fn remove_propose_chan(&self, id: InstanceId) {
        let _ = self.propose_tx.remove(&id);
        debug!(?id, "remove_propose_chan");
    }

    async fn determine_update_mode(&self, id: InstanceId, cmd: Option<C>) -> Result<(C, UpdateMode)> {
        if let Some(cmd) = cmd {
            return Ok((cmd, UpdateMode::Full));
        }
        self.log.load(id).await?;
        let cmd: _ = self.log.with_cached_ins(id, |ins: _| ins.unwrap().cmd.clone()).await;
        Ok((cmd, UpdateMode::Partial))
    }

    async fn is_status_changed(&self, id: InstanceId, expected: Status) -> bool {
        self.log
            .with_cached_ins(id, |ins: _| {
                let ins = ins.unwrap();
                ins.status != expected
            })
            .await
    }

    #[tracing::instrument(skip_all, fields(?id))]
    async fn resume_propose(self: &Arc<Self>, id: InstanceId, msg: Message<C>) -> Result<()> {
        debug!(?id, reply_variant_name=?msg.variant_name());
        let tx = self.propose_tx.get(&id).as_deref().cloned();
        if let Some(tx) = tx {
            let _ = chan::send(&tx, msg).await;
        }
        Ok(())
    }

    async fn with<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        let mut guard = self.lock_state().await;
        f(&mut *guard)
    }

    #[tracing::instrument(skip_all, fields(rid=?self.rid))]
    pub async fn run_propose(self: &Arc<Self>, cmd: C) -> Result<()> {
        let id = self.with(|s| InstanceId(self.rid, s.lid_head.gen_next())).await;

        let pbal = Ballot(Round::ZERO, self.rid);
        let acc = Acc::from_mutable(MutableAcc::with_capacity(1));

        debug!(?id, "run_propose");

        let ins_guard = self.log.lock_instance(id).await;

        Box::pin(self.phase_preaccept(ins_guard, id, pbal, Some(cmd), acc)).await
    }

    #[tracing::instrument(skip_all, fields(id = ?id, pbal=?pbal))]
    async fn phase_preaccept(
        self: &Arc<Self>,
        ins_guard: InsGuard,
        id: InstanceId,
        pbal: Ballot,
        cmd: Option<C>,
        acc: Acc,
    ) -> Result<()> {
        debug!("phase_preaccept");

        let (mut rx, mut seq, mut deps, mut acc, targets) = {
            self.log.load(id).await?;

            let (cmd, mode) = self.determine_update_mode(id, cmd).await?;

            let calc_t0 = Instant::now();

            let (seq, deps) =
                self.log.calc_and_update_attributes(id, cmd.keys(), Seq::ZERO, &Deps::default()).await;

            debug!(?id, ?seq, ?deps, elapsed_us=?calc_t0.elapsed().as_micros(), "calc_attributes");

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
                self.log.save(id, ins, mode, Some(false)).await?;
            }

            let quorum;
            let selected_peers;
            let avg_rtt;
            {
                let mut guard = self.state.lock().await;
                let s = &mut *guard;

                quorum = s.peers.cluster_size().wrapping_sub(2);
                selected_peers = s.peers.select(quorum, acc.as_ref());
                avg_rtt = s.peers.get_avg_rtt();
            }

            let rx = self.insert_propose_chan(id, quorum);

            drop(ins_guard);

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

            self.spawn_recover_timeout(id, avg_rtt);

            (rx, seq, deps.into_mutable(), acc.into_mutable(), targets)
        };

        {
            let mut received: VecSet<ReplicaId> = VecSet::new();
            let mut all_same = true;

            let avg_rtt = {
                let mut guard = self.lock_state().await;
                let s = &mut *guard;
                s.peers.get_avg_rtt()
            };
            let conf = &self.config.preaccept_timeout;
            let t = conf.with(avg_rtt, |d| {
                let base = Self::random_time(Duration::from_micros(conf.default_us), 0.5..1.5);
                let delta = Self::random_time(d, 2.0..5.0);
                base + delta
            });
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

                        let pbal = match msg {
                            PreAcceptReply::Ok(ref msg) => msg.pbal,
                            PreAcceptReply::Diff(ref msg) => msg.pbal,
                        };

                        let ins_guard = self.log.lock_instance(id).await;

                        self.log.load(id).await?;

                        if self.log.should_ignore_pbal(id, pbal).await {
                            continue;
                        }

                        if self.is_status_changed(id, Status::PreAccepted).await {
                            break;
                        }

                        let cluster_size = self.with(|s| s.peers.cluster_size()).await;

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

                        let deps = Deps::from_mutable(deps);
                        let acc = Acc::from_mutable(acc);

                        debug!(?id, ?seq, ?deps, "ins attributes");

                        with_mutex(&self.metrics, |m| {
                            if is_fast_path {
                                m.preaccept_fast_path = m.preaccept_fast_path.wrapping_add(1);
                            } else {
                                m.preaccept_slow_path = m.preaccept_slow_path.wrapping_add(1);
                            }
                        });

                        if is_fast_path {
                            debug!("fast path");
                            return Box::pin(self.phase_commit(ins_guard, id, pbal, cmd, seq, deps, acc))
                                .await;
                        } else {
                            debug!("slow path");
                            return Box::pin(self.phase_accept(ins_guard, id, pbal, cmd, seq, deps, acc))
                                .await;
                        }
                    }
                    Ok(None) => break,
                    Err(_) => {
                        let mut guard = self.lock_state().await;
                        let s = &mut *guard;

                        let cluster_size = s.peers.cluster_size();
                        if received.len() < cluster_size / 2 {
                            debug!("preaccept timeout: not enough replies");

                            let no_reply_targets = {
                                let mut all_targets = targets;
                                all_targets.difference_copied_inplace(&received);
                                all_targets
                            };

                            s.peers.set_inf_rtt(&no_reply_targets);
                            drop(guard);
                            break;
                        }

                        drop(guard);

                        debug!("preaccept timeout: goto slow path");

                        let ins_guard = self.log.lock_instance(id).await;

                        self.log.load(id).await?;
                        let pbal = self.log.get_cached_pbal(id).await.expect("pbal should exist");

                        let cmd = None;
                        let deps = Deps::from_mutable(deps);
                        let acc = Acc::from_mutable(acc);

                        with_mutex(&self.metrics, |m| {
                            m.preaccept_slow_path = m.preaccept_slow_path.wrapping_add(1);
                        });

                        return Box::pin(self.phase_accept(ins_guard, id, pbal, cmd, seq, deps, acc)).await;
                    }
                }
            }
        }

        debug!("phase preaccept failed");
        self.remove_propose_chan(id);
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(sender=?msg.sender, id = ?msg.id, pbal = ?msg.pbal))]
    async fn handle_preaccept(self: &Arc<Self>, msg: PreAccept<C>) -> Result<()> {
        if msg.epoch < self.epoch.load() {
            return Ok(());
        }

        debug!(seq=?msg.seq, deps=?msg.deps);

        let id = msg.id;
        let pbal = msg.pbal;

        let ins_guard = self.log.lock_instance(id).await;

        self.log.load(id).await?;

        if self.log.should_ignore_pbal(id, pbal).await {
            return Ok(());
        }
        if self.log.should_ignore_status(id, pbal, Status::PreAccepted).await {
            return Ok(());
        }

        let (cmd, mode) = self.determine_update_mode(id, msg.cmd).await?;

        let (seq, deps) = self.log.calc_and_update_attributes(id, cmd.keys(), msg.seq, &msg.deps).await;

        debug!(?id, ?seq, ?deps, "ins attributes");

        let is_changed = seq != msg.seq || deps != msg.deps;

        let abal = pbal;
        let status = Status::PreAccepted;

        let mut acc = msg.acc;
        acc.cow_insert(self.rid);

        {
            clone!(deps);
            let ins: _ = Instance { pbal, cmd, seq, deps, abal, status, acc };
            self.log.save(id, ins, mode, Some(false)).await?
        }

        drop(ins_guard);

        let mut guard = self.state.lock().await;
        let s = &mut *guard;
        let avg_rtt = s.peers.get_avg_rtt();
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

        self.spawn_recover_timeout(id, avg_rtt);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(skip_all, fields(id = ?id))]
    async fn phase_accept(
        self: &Arc<Self>,
        ins_guard: InsGuard,
        id: InstanceId,
        pbal: Ballot,
        cmd: Option<C>,
        seq: Seq,
        deps: Deps,
        acc: Acc,
    ) -> Result<()> {
        debug!("phase_accept");

        let (mut rx, mut acc) = {
            let abal = pbal;
            let status = Status::Accepted;

            let (cmd, mode) = self.determine_update_mode(id, cmd).await?;

            {
                clone!(cmd, deps, acc);
                let ins: _ = Instance { pbal, cmd, seq, deps, abal, status, acc };
                self.log.save(id, ins, mode, None).await?;
            }

            let mut guard = self.state.lock().await;
            let s = &mut *guard;

            let quorum = s.peers.cluster_size() / 2;
            let selected_peers = s.peers.select(quorum, acc.as_ref());

            let avg_rtt = s.peers.get_avg_rtt();

            drop(guard);

            let rx = self.insert_propose_chan(id, quorum);

            drop(ins_guard);

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

            self.spawn_recover_timeout(id, avg_rtt);

            (rx, acc.into_mutable())
        };

        {
            let mut received = VecSet::new();

            let timeout = Self::random_time(
                Duration::from_micros(self.config.accept_timeout.default_us),
                0.75..1.25,
            );

            while let Ok(Some(msg)) = recv_timeout(&mut rx, timeout).await {
                let msg = match AcceptReply::convert(msg) {
                    Some(m) => m,
                    None => continue,
                };

                let AcceptReply::Ok(msg) = msg;

                if msg.epoch < self.epoch.load() {
                    continue;
                }

                debug!("received accept reply");

                assert_eq!(id, msg.id);

                let ins_guard = self.log.lock_instance(id).await;

                let pbal = msg.pbal;

                self.log.load(id).await?;

                if self.log.should_ignore_pbal(id, pbal).await {
                    continue;
                }

                if self.is_status_changed(id, Status::Accepted).await {
                    break;
                }

                {
                    if received.insert(msg.sender).is_some() {
                        continue;
                    }
                    acc.insert(msg.sender);
                }

                let mut guard = self.state.lock().await;
                let s = &mut *guard;
                let cluster_size = s.peers.cluster_size();
                drop(guard);

                if received.len() < cluster_size / 2 {
                    continue;
                }

                let cmd = None;
                let acc = Acc::from_mutable(acc);

                let (seq, deps) = self
                    .log
                    .with_cached_ins(id, |ins| {
                        let ins = ins.unwrap();
                        (ins.seq, ins.deps.clone())
                    })
                    .await;

                return Box::pin(self.phase_commit(ins_guard, id, pbal, cmd, seq, deps, acc)).await;
            }
        }

        debug!("phase accept failed");
        self.remove_propose_chan(id);
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(sender=?msg.sender, id = ?msg.id, pbal = ?msg.pbal))]
    async fn handle_accept(self: &Arc<Self>, msg: Accept<C>) -> Result<()> {
        if msg.epoch < self.epoch.load() {
            return Ok(());
        }

        let id = msg.id;
        let pbal = msg.pbal;

        let ins_guard = self.log.lock_instance(id).await;

        self.log.load(id).await?;

        if self.log.should_ignore_pbal(id, pbal).await {
            return Ok(());
        }
        if self.log.should_ignore_status(id, pbal, Status::Accepted).await {
            return Ok(());
        }

        let abal = pbal;
        let status = Status::Accepted;

        let mut acc = msg.acc;
        acc.cow_insert(self.rid);

        let seq = msg.seq;
        let deps = msg.deps;

        let (cmd, mode) = self.determine_update_mode(id, msg.cmd).await?;

        {
            let ins: _ = Instance { pbal, cmd, seq, deps, abal, status, acc };
            self.log.save(id, ins, mode, None).await?;
        }

        drop(ins_guard);

        let mut guard = self.lock_state().await;
        let s = &mut *guard;

        let avg_rtt = s.peers.get_avg_rtt();

        drop(guard);

        {
            let target = msg.sender;
            let sender = self.rid;
            let epoch = self.epoch.load();
            self.network.send_one(target, Message::AcceptOk(AcceptOk { sender, epoch, id, pbal }));
        }

        self.spawn_recover_timeout(id, avg_rtt);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(skip_all, fields(id = ?id))]
    async fn phase_commit(
        self: &Arc<Self>,
        ins_guard: InsGuard,
        id: InstanceId,
        pbal: Ballot,
        cmd: Option<C>,
        seq: Seq,
        deps: Deps,
        acc: Acc,
    ) -> Result<()> {
        debug!("phase_commit");

        let abal = pbal;
        let status = Status::Committed;

        let (cmd, mode) = self.determine_update_mode(id, cmd).await?;

        let mut guard = self.state.lock().await;
        let s = &mut *guard;

        let quorum = s.peers.cluster_size().wrapping_sub(1);
        let selected_peers = s.peers.select(quorum, acc.as_ref());

        drop(guard);

        {
            clone!(cmd, deps, acc);
            let ins: _ = Instance { pbal, cmd, seq, deps, abal, status, acc };
            self.log.save(id, ins, mode, None).await?;
        }

        drop(ins_guard);

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
            self.graph.sync_watermark(id.0);
            self.spawn_execute(id, cmd, seq, deps, status);
        }

        Ok(())
    }

    #[tracing::instrument(skip_all, fields(sender=?msg.sender, id = ?msg.id, pbal = ?msg.pbal))]
    async fn handle_commit(self: &Arc<Self>, msg: Commit<C>) -> Result<()> {
        if msg.epoch < self.epoch.load() {
            return Ok(());
        }

        let id = msg.id;
        let pbal = msg.pbal;

        let ins_guard = self.log.lock_instance(id).await;

        self.log.load(id).await?;

        if self.log.should_ignore_pbal(id, pbal).await {
            return Ok(());
        }

        if self.log.should_ignore_status(id, pbal, Status::Committed).await {
            return Ok(());
        }

        let (cmd, mode) = self.determine_update_mode(id, msg.cmd).await?;

        let status = self.log.with_cached_ins(id, |ins|match ins {
                Some(ins) if ins.status >= Status::Committed => {
                    if ins.seq != msg.seq || ins.deps != msg.deps {
                        debug!(?id, ins_seq=?ins.seq, msg_seq=?msg.seq, ins_deps=?ins.deps, msg_deps=?msg.deps,"consistency incorrect");
                        assert_eq!(ins.seq, msg.seq);
                        assert_eq!(ins.deps, msg.deps);
                    }
                    ins.status
                }
                _ => Status::Committed,
            }).await;

        let seq = msg.seq;
        let deps = msg.deps;

        let abal = pbal;

        let mut acc = msg.acc;
        acc.cow_insert(self.rid);

        {
            clone!(cmd, deps);
            let ins: _ = Instance { pbal, cmd, seq, deps, abal, status, acc };
            self.log.save(id, ins, mode, None).await?
        }

        drop(ins_guard);

        self.graph.sync_watermark(id.0);
        self.spawn_execute(id, cmd, seq, deps, status);

        Ok(())
    }

    #[tracing::instrument(skip_all, fields(rid = ?self.rid, id = ?id))]
    async fn run_recover(self: &Arc<Self>, id: InstanceId) -> Result<()> {
        let mut is_not_first = false;
        loop {
            debug!("run_recover");

            if is_not_first {
                let base = Duration::from_micros(self.config.recover_timeout.default_us);
                let timeout = Self::random_time(base, 0.5..1.5);
                debug!("delay recover in {:?}", timeout);
                sleep(timeout).await;
            }
            is_not_first = true;

            while self.propose_tx.contains_key(&id) {
                let base = Duration::from_micros(self.config.recover_timeout.default_us);
                let timeout = Self::random_time(base, 0.5..1.5);
                debug!("delay recover in {:?}", timeout);
                sleep(timeout).await;
            }

            let mut rx = {
                let ins_guard = self.log.lock_instance(id).await;

                if self.propose_tx.contains_key(&id) {
                    continue;
                }

                self.log.load(id).await?;

                let is_committed = self
                    .log
                    .with_cached_ins(id, |ins| {
                        if let Some(ins) = ins {
                            if ins.status >= Status::Committed {
                                let cmd = ins.cmd.clone();
                                let seq = ins.seq;
                                let deps = ins.deps.clone();
                                let status = ins.status;
                                return Some((id, cmd, seq, deps, status));
                            }
                        }
                        None
                    })
                    .await;

                if let Some((id, cmd, seq, deps, status)) = is_committed {
                    self.graph.init_node(id, cmd, seq, deps, status);
                    return Ok(());
                }

                let pbal = match self.log.get_cached_pbal(id).await {
                    Some(Ballot(rnd, _)) => Ballot(rnd.add_one(), self.rid),
                    None => Ballot(Round::ZERO, self.rid),
                };

                debug!(?pbal);

                let known = self
                    .log
                    .with_cached_ins(id, |ins: _| matches!(ins, Some(ins) if ins.cmd.is_nop().not()))
                    .await;

                let mut guard = self.state.lock().await;
                let s = &mut *guard;

                let targets = s.peers.select_all();

                drop(guard);

                let rx = self.insert_propose_chan(id, targets.len());

                drop(ins_guard);

                {
                    let sender = self.rid;
                    let epoch = self.epoch.load();

                    debug!(?epoch, ?id, ?pbal, ?known, "broadcast prepare");

                    self.network.broadcast(
                        targets,
                        Message::Prepare(Prepare { sender, epoch, id, pbal, known }),
                    );

                    self.spawn_handle_prepare(Prepare { sender, epoch, id, pbal, known });
                }

                rx
            };

            {
                let mut received: VecSet<ReplicaId> = VecSet::new();

                let mut max_abal: Option<Ballot> = None;
                let mut cmd: Option<C> = None;

                // (sender, seq, deps, status, acc)
                let mut tuples: Vec<(ReplicaId, Seq, Deps, Status, Acc)> = Vec::new();

                let mut has_one_commit: bool = false;

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

                    match msg {
                        PrepareReply::Ok(ref msg) => assert_eq!(id, msg.id),
                        PrepareReply::Nack(ref msg) => assert_eq!(id, msg.id),
                        PrepareReply::Unchosen(ref msg) => assert_eq!(id, msg.id),
                    };

                    let ins_guard = self.log.lock_instance(id).await;
                    self.log.load(id).await?;
                    if let PrepareReply::Ok(ref msg) = msg {
                        if self.log.should_ignore_pbal(id, msg.pbal).await {
                            continue;
                        }
                    }

                    match msg {
                        PrepareReply::Unchosen(msg) => {
                            let _ = received.insert(msg.sender);
                        }
                        PrepareReply::Nack(msg) => {
                            self.log.save_pbal(id, msg.pbal).await?;
                            drop(ins_guard);
                            break;
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

                            has_one_commit = msg.status >= Status::Committed;
                        }
                    }

                    debug!(?id, received_len = ?received.len(), "received prepare reply: tuples: {:?}", tuples);

                    let mut guard = self.lock_state().await;
                    let s = &mut *guard;

                    let cluster_size = s.peers.cluster_size();

                    drop(guard);

                    if has_one_commit.not() && received.len() <= cluster_size / 2 {
                        continue;
                    }

                    let max_abal = match max_abal {
                        Some(b) => b,
                        None => continue,
                    };

                    let pbal = self.log.get_cached_pbal(id).await.expect("pbal should exist");

                    debug!(?pbal, ?tuples, "recover succeeded");
                    with_mutex(&self.metrics, |m| {
                        m.recover_success_count = m.recover_success_count.wrapping_add(1);
                    });

                    let _ = self.recovering.remove(&id);

                    let acc = {
                        let mut acc = self
                            .log
                            .with_cached_ins(id, |ins| match ins {
                                Some(ins) => MutableAcc::clone(ins.acc.as_ref()),
                                None => MutableAcc::default(),
                            })
                            .await;
                        for (_, _, _, _, a) in tuples.iter() {
                            acc.union(a.as_ref());
                        }
                        Acc::from_mutable(acc)
                    };

                    for &mut (_, seq, ref mut deps, status, _) in tuples.iter_mut() {
                        if status < Status::Committed {
                            continue;
                        }
                        let deps = mem::take(deps);
                        return Box::pin(self.phase_commit(ins_guard, id, pbal, cmd, seq, deps, acc)).await;
                    }

                    for &mut (_, seq, ref mut deps, status, _) in tuples.iter_mut() {
                        if status != Status::Accepted {
                            continue;
                        }
                        let deps = mem::take(deps);
                        return Box::pin(self.phase_accept(ins_guard, id, pbal, cmd, seq, deps, acc)).await;
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
                                return Box::pin({
                                    self.phase_accept(ins_guard, id, pbal, cmd, seq, deps, acc)
                                })
                                .await;
                            }
                        }
                    }

                    if tuples.is_empty().not() {
                        return Box::pin(self.phase_preaccept(ins_guard, id, pbal, cmd, acc)).await;
                    }

                    let (cmd, acc) = self
                        .log
                        .with_cached_ins(id, |ins| match ins {
                            Some(_) => (None, acc),
                            None => (Some(C::create_nop()), Acc::default()),
                        })
                        .await;

                    if cmd.is_some() {
                        with_mutex(&self.metrics, |m| {
                            m.recover_nop_count = m.recover_nop_count.wrapping_add(1);
                        });
                    }

                    return Box::pin(self.phase_preaccept(ins_guard, id, pbal, cmd, acc)).await;
                }
            }

            self.remove_propose_chan(id);
        }
    }

    #[allow(clippy::float_arithmetic)]
    fn spawn_recover_timeout(self: &Arc<Self>, id: InstanceId, avg_rtt: Option<Duration>) {
        if let dashmap::mapref::entry::Entry::Vacant(e) = self.recovering.entry(id) {
            let conf = &self.config.recover_timeout;
            let duration = conf.with(avg_rtt, |d| {
                let base = Self::random_time(Duration::from_micros(conf.default_us), 0.5..1.5);
                let delta = Self::random_time(d, 4.0..6.0);
                base + delta
            });

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

        debug!(?msg);

        let id = msg.id;

        let ins_guard = self.log.lock_instance(id).await;
        self.log.load(id).await?;

        let epoch = self.epoch.load();

        let reply: Result<Message<C>> = async {
            if let Some(pbal) = self.log.get_cached_pbal(id).await {
                if pbal >= msg.pbal {
                    let sender = self.rid;
                    return Ok(Message::PrepareNack(PrepareNack { sender, epoch, id, pbal }));
                }
            }

            let pbal = msg.pbal;

            self.log.save_pbal(id, pbal).await?;

            self.log
                .with_cached_ins(id, |ins| {
                    let ins = match ins {
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
                })
                .await
        }
        .await;

        drop(ins_guard);

        let reply = reply?;

        let target = msg.sender;
        if target == self.rid {
            Box::pin(self.resume_propose(id, reply)).await?
        } else {
            self.network.send_one(target, reply)
        }

        Ok(())
    }

    fn spawn_handle_prepare(self: &Arc<Self>, msg: Prepare) {
        let this = Arc::clone(self);
        spawn(async move {
            let id = msg.id;
            if let Err(err) = this.handle_prepare(msg).await {
                error!(?id, ?err)
            }
        });
    }

    #[tracing::instrument(skip_all, fields(rid=?self.rid))]
    pub async fn run_join(self: &Arc<Self>) -> Result<bool> {
        debug!("run_join");

        let mut rx = {
            let mut guard = self.lock_state().await;
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

                let mut guard = self.lock_state().await;
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
        let mut guard = self.lock_state().await;
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
            let _ = chan::send(&tx, msg).await;
        }
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(?msg))]
    async fn handle_leave(self: &Arc<Self>, msg: Leave) -> Result<()> {
        let mut guard = self.lock_state().await;
        let s = &mut *guard;

        s.peers.remove(msg.sender);

        drop(guard);

        Ok(())
    }

    #[tracing::instrument(skip_all, fields(rid=?self.rid))]
    pub async fn run_probe_rtt(&self) -> Result<()> {
        let mut guard = self.lock_state().await;
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
            let mut guard = self.lock_state().await;
            let s = &mut *guard;
            s.peers.set_rtt(peer, rtt);
        }

        Ok(())
    }

    pub async fn run_sync_known(self: &Arc<Self>) -> Result<()> {
        self.log.update_bounds();
        let known_up_to = self.log.known_up_to();

        let mut guard = self.lock_state().await;
        let s = &mut *guard;

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

        self.log.update_bounds();
        let local_known_up_to = self.log.known_up_to();

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

                let ins_guard = self.log.lock_instance(id).await;
                self.log.load(id).await?;
                let ins = self.log.with_cached_ins(id, |ins| ins.cloned()).await;

                drop(ins_guard);

                if let Some(ins) = ins {
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

        Ok(())
    }

    pub async fn run_sync_committed(self: &Arc<Self>) -> Result<bool> {
        let (rxs, quorum) = {
            self.log.update_bounds();
            let local_bounds = self.log.committed_up_to();

            let mut guard = self.lock_state().await;
            let s = &mut *guard;
            let peer_bounds = s.peer_status_bounds.committed_up_to();

            let targets = s.peers.select_all();

            drop(guard);

            let sender = self.rid;
            let mut rxs = Vec::new();
            let mut send_log = |s: &mut State, instances| {
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
                    let _ins_guard = self.log.lock_instance(id).await;

                    self.log.load(id).await?;

                    let ins = self
                        .log
                        .with_cached_ins(id, |ins| match ins {
                            Some(ins) if ins.status >= Status::Committed => Some(ins.clone()),
                            _ => None,
                        })
                        .await;

                    if let Some(ins) = ins {
                        instances.push((id, ins.clone()));

                        if instances.len() >= limit {
                            let mut guard = self.state.lock().await;
                            let s = &mut *guard;
                            send_log(s, mem::take(&mut instances));
                        }
                    }
                }

                if instances.is_empty().not() {
                    let mut guard = self.state.lock().await;
                    let s = &mut *guard;
                    send_log(s, instances);
                }
            }

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
        for (id, mut ins) in msg.instances {
            let _ins_guard = self.log.lock_instance(id).await;

            self.log.load(id).await?;

            let saved_pbal = self.log.get_cached_pbal(id).await;

            let opt = self
                .log
                .with_cached_ins(id, |saved_ins| match saved_ins {
                    None => match saved_pbal {
                        Some(saved_pbal) if saved_pbal > ins.abal => None,
                        _ => Some((UpdateMode::Full, ins.status >= Status::Committed)),
                    },
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

                            Some((mode, true))
                        } else {
                            None
                        }
                    }
                })
                .await;

            if let Some((mode, needs_exec)) = opt {
                if needs_exec {
                    let cmd = ins.cmd.clone();
                    let seq = ins.seq;
                    let deps = ins.deps.clone();
                    let status = ins.status;

                    self.log.save(id, ins, mode, None).await?;
                    self.spawn_execute(id, cmd, seq, deps, status);
                } else {
                    self.log.save(id, ins, mode, None).await?;
                }
            }
        }

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
            let _ = chan::send(&tx, msg).await;
        }
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(rid=?self.rid))]
    pub async fn run_broadcast_bounds(self: &Arc<Self>) -> Result<()> {
        let mut guard = self.lock_state().await;
        let s = &mut *guard;

        let targets = s.peers.select_all();

        drop(guard);

        let bounds = self.log.saved_status_bounds();
        let committed_up_to = Some(bounds.committed_up_to);
        let executed_up_to = Some(bounds.executed_up_to);

        {
            let sender = self.rid;
            self.network.broadcast(
                targets,
                Message::PeerBounds(PeerBounds { sender, committed_up_to, executed_up_to }),
            )
        }

        Ok(())
    }

    async fn handle_peer_bounds(self: &Arc<Self>, msg: PeerBounds) -> Result<()> {
        debug!(?msg, "handle_peer_bounds");

        let mut guard = self.lock_state().await;
        let s = &mut *guard;

        if let Some(bounds) = msg.committed_up_to {
            s.peer_status_bounds.set_committed(msg.sender, bounds);
        }

        if let Some(bounds) = msg.executed_up_to {
            s.peer_status_bounds.set_executed(msg.sender, bounds)
        }

        drop(guard);

        Ok(())
    }

    fn spawn_execute(self: &Arc<Self>, id: InstanceId, cmd: C, seq: Seq, deps: Deps, status: Status) {
        debug!(?id, ?seq, ?deps, ?status, "spawn_execute");

        match Ord::cmp(&status, &Status::Committed) {
            Ordering::Less => panic!("unexpected status: {:?}", status),
            Ordering::Equal => {}
            Ordering::Greater => return,
        }

        self.remove_propose_chan(id);
        if let Some((_, task)) = self.recovering.remove(&id) {
            task.abort()
        }

        self.executing.entry(id).or_insert_with(|| {
            self.graph.init_node(id, cmd, seq, deps, status);

            let this = Arc::clone(self);
            spawn(async move {
                let permit = this.executing_limit.acquire().await.unwrap();
                if let Err(err) = this.run_execute(id).await {
                    error!(?id, ?err)
                }
                drop(permit);
            })
        });

        debug!(?id, "spawned executing task");
    }

    fn init_row_lock(&self, rid: ReplicaId) -> Arc<AsyncMutex<()>> {
        self.exec_row_locks.entry(rid).or_insert_with(|| Arc::new(AsyncMutex::new(()))).clone()
    }

    #[tracing::instrument(skip_all, fields(id = ?root))]
    async fn run_execute(self: &Arc<Self>, root: InstanceId) -> Result<()> {
        let mut local_graph = LocalGraph::new();

        debug!("wait graph");

        {
            debug!("start to lock row {:?}", root.0);
            let row_lock = self.init_row_lock(root.0);
            let row_guard = row_lock.lock_owned().await;
            debug!("locked row {:?}", root.0);

            let mut vis: _ = FnvHashSet::<InstanceId>::default();
            let mut q = DepsQueue::new(root);
            let mut spawn_recover_up_to = VecMap::<ReplicaId, LocalInstanceId>::new();

            let t0 = Instant::now();

            while let Some(id) = q.pop() {
                if vis.contains(&id) {
                    continue;
                }
                vis.insert(id);

                let node = match self.graph.try_get_node(id) {
                    Ok(Some(node)) => node,
                    Ok(None) => continue, // executed node
                    Err(_) => {
                        // needs wait
                        self.spawn_recover_timeout(id, None);

                        debug!("bfs waiting node {:?}", id);

                        let node = match self.graph.wait_node(id).await {
                            Some(node) => node,
                            None => continue, // executed node
                        };

                        if let Some((_, task)) = self.recovering.remove(&id) {
                            task.abort()
                        }

                        node
                    }
                };

                {
                    let needs_skip = node.estatus(|es| *es > ExecStatus::Committed);
                    if needs_skip {
                        continue;
                    }
                }

                if id.1 > LocalInstanceId::ONE {
                    let InstanceId(rid, lid) = id;

                    let wm = self.graph.watermark(rid);

                    let mut start = LocalInstanceId::from(wm.level().saturating_add(1));
                    let up_to = spawn_recover_up_to.entry(rid).or_insert(start);
                    start = start.max(up_to.add_one());

                    let end = lid.sub_one();

                    if start <= end {
                        for l in LocalInstanceId::range_inclusive(start, end) {
                            let id = InstanceId(rid, l);
                            if vis.contains(&id).not() {
                                self.spawn_recover_timeout(id, None)
                            }
                        }
                        let _ = spawn_recover_up_to.insert(rid, lid);
                    }

                    let cnt = lid.raw_value().saturating_sub(start.raw_value());
                    debug!(?rid, level=?wm.level(), ?lid, ?start, ?end, ?cnt, "bfs waiting watermark");
                    wm.until(lid.raw_value()).wait().await;
                }

                {
                    let needs_skip = node.estatus(|es| *es > ExecStatus::Committed);
                    if needs_skip {
                        continue;
                    }
                }

                debug!(?id, seq=?node.seq, deps=?node.deps, "local graph add node");
                local_graph.add_node(id, Asc::clone(&node));

                for d in node.deps.elements() {
                    if vis.contains(&d) {
                        continue;
                    }
                    q.push(d);
                }

                {
                    let elapsed = t0.elapsed();
                    if elapsed > Duration::from_secs(1) {
                        debug!(elapsed_us = ?elapsed.as_micros(), "bfs too slow")
                    }
                }
            }

            let _ = self.executing.remove(&root);

            debug!("unlock row {:?}", root.0);
            drop(row_guard)
        }

        let local_graph_nodes_count = local_graph.nodes_count();
        debug!(local_graph_nodes_count, "tarjan scc");

        if local_graph_nodes_count == 0 {
            return Ok(()); // ins executed
        } else if local_graph_nodes_count == 1 {
            // common case
            let node = local_graph.get_node(root).cloned().unwrap();
            drop(local_graph);

            {
                let needs_issue = node.estatus(|es| {
                    if *es == ExecStatus::Committed {
                        *es = ExecStatus::Issuing;
                        debug!(id=?root, "mark issuing");
                        true
                    } else {
                        false
                    }
                });
                if needs_issue.not() {
                    return Ok(());
                }
            }

            let this = Arc::clone(self);
            spawn(async move {
                if let Err(err) = this.run_execute_single_node(root, node).await {
                    error!(?err);
                }
            });
        } else {
            let mut scc_list = local_graph.tarjan_scc(root);
            for scc in &mut scc_list {
                scc.sort_by_key(|&(InstanceId(rid, lid), ref node): _| (node.seq, lid, rid));
            }

            let scc_total_len: usize = scc_list.iter().map(|scc: _| scc.len()).sum();
            assert_eq!(scc_total_len, local_graph.nodes_count());
            assert!(scc_list.is_empty().not());

            debug!(root=?root, scc_list_len=?scc_list.len());

            let scc_list = {
                let mut ans = Vec::with_capacity(scc_list.len());
                for scc in scc_list {
                    let mut stack = Vec::with_capacity(scc.len());

                    for &(id, ref node) in &scc {
                        let guard = node.lock_estatus();
                        stack.push((id, guard));
                    }

                    debug!("locked scc");

                    let mut needs_issue = None;
                    for &mut (id, ref mut guard) in stack.iter_mut().rev() {
                        let es = &mut **guard;
                        let flag = *es == ExecStatus::Committed;
                        if let Some(prev) = needs_issue {
                            if prev != flag {
                                debug!(?root, ?id, status = ?*es, "scc marking incorrect: {:?}", scc);
                                panic!("scc marking incorrect")
                            }
                        }
                        needs_issue = Some(flag);
                        if flag {
                            *es = ExecStatus::Issuing;
                            debug!(?id, "mark issuing")
                        }
                    }

                    debug!("unlock scc");

                    while let Some((_, guard)) = stack.pop() {
                        drop(guard);
                    }
                    drop(stack);

                    let needs_issue = needs_issue.unwrap();
                    if needs_issue {
                        for &(id, _) in &scc {
                            if id == root {
                                continue;
                            }
                            let task = self.executing.remove(&id);
                            if let Some((_, task)) = task {
                                task.abort();
                                debug!("abort executing task {:?}", id)
                            }
                        }
                        ans.push(scc)
                    } else {
                        debug!(root=?root, scc_len=?scc.len(), "not needs issue: {:?}", scc)
                    }
                }
                ans
            };

            debug!(root=?root, scc_list_len=?scc_list.len());

            let flag_group = FlagGroup::new(scc_list.len());

            for (idx, scc) in scc_list.into_iter().enumerate() {
                clone!(flag_group);
                let this = Arc::clone(self);
                spawn(async move {
                    if let Err(err) = this.run_execute_scc(scc, flag_group, idx, root).await {
                        error!(?err);
                    }
                });
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(?id))]
    async fn run_execute_single_node(self: &Arc<Self>, id: InstanceId, node: Asc<InsNode<C>>) -> Result<()> {
        {
            let status = node.estatus(|es| *es);
            assert_eq!(status, ExecStatus::Issuing);
        }

        let notify = Asc::new(ExecNotify::new());
        {
            clone!(notify);
            let cmd = node.cmd.clone();
            self.data_store.issue(id, cmd, notify).await?;
        }
        notify.wait_issued().await;

        let prev_status = {
            let status = notify.status();

            {
                let _ins_guard = self.log.lock_instance(id).await;
                self.log.update_status(id, status).await?;
            }

            node.estatus(|es| match status {
                Status::Issued => *es = ExecStatus::Issued,
                Status::Executed => *es = ExecStatus::Executed,
                _ => {}
            });

            status
        };

        debug!(?prev_status);

        notify.wait_executed().await;

        if prev_status < Status::Executed {
            {
                let _ins_guard = self.log.lock_instance(id).await;
                self.log.update_status(id, Status::Executed).await?;
            }
            node.estatus(|es| *es = ExecStatus::Executed);
        }

        self.retire_executed_instances(std::iter::once(id)).await;

        Ok(())
    }

    #[tracing::instrument(skip_all, fields(root=?root))]
    async fn run_execute_scc(
        self: &Arc<Self>,
        scc: Vec<(InstanceId, Asc<InsNode<C>>)>,
        flag_group: FlagGroup,
        idx: usize,
        root: InstanceId,
    ) -> Result<()> {
        if idx > 0 {
            let prev = idx.wrapping_sub(1);
            debug!("waiting prev={}", prev);
            flag_group.wait(prev).await;
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
        let mut is_all_executed = true;
        {
            for (&(id, ref node), n) in scc.iter().zip(handles.iter()) {
                let status = n.status();

                {
                    let _ins_guard = self.log.lock_instance(id).await;
                    self.log.update_status(id, status).await?;
                }
                node.estatus(|es| {
                    match status {
                        Status::Issued => *es = ExecStatus::Issued,
                        Status::Executed => *es = ExecStatus::Executed,
                        _ => {}
                    };
                });

                is_all_executed &= status == Status::Executed;
                prev_status.push(status);
            }
        }

        if is_all_executed.not() {
            for n in &handles {
                n.wait_executed().await;
            }

            for (&(id, ref node), &prev) in scc.iter().zip(prev_status.iter()) {
                if prev < Status::Executed {
                    {
                        let _ins_guard = self.log.lock_instance(id).await;
                        self.log.update_status(id, Status::Executed).await?;
                    }
                    node.estatus(|es| *es = ExecStatus::Executed);
                }
            }
        }

        self.retire_executed_instances(scc.iter().map(|&(id, _)| id)).await;

        Ok(())
    }

    async fn retire_executed_instances(&self, iter: impl Iterator<Item = InstanceId> + Clone) {
        for id in iter.clone() {
            self.graph.retire_node(id);
            if let Some((_, task)) = self.recovering.remove(&id) {
                task.abort()
            }
        }
        for id in iter {
            let _ins_guard = self.log.lock_instance(id).await;
            self.log.retire_instance(id).await;
        }
        debug!("retire instances")
    }

    #[tracing::instrument(skip_all, fields(rid=?self.rid))]
    pub async fn run_clear_key_map(self: &Arc<Self>) {
        debug!("run_clear_key_map");
        let garbage = self.log.clear_key_map().await;
        drop(garbage);
    }

    #[tracing::instrument(skip_all, fields(rid=?self.rid))]
    pub async fn run_save_bounds(self: &Arc<Self>) -> Result<()> {
        self.log.save_bounds().await
    }
}
