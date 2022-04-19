use crate::bounds::PeerStatusBounds;
use crate::cmd::CommandLike;
use crate::config::ReplicaConfig;
use crate::deps::Deps;
use crate::graph::{DepsQueue, Graph};
use crate::id::*;
use crate::ins::Instance;
use crate::log::Log;
use crate::msg::*;
use crate::net::{broadcast_accept, broadcast_commit, broadcast_preaccept, Network};
use crate::peers::Peers;
use crate::status::Status;
use crate::store::{DataStore, LogStore, UpdateMode};

use epkv_utils::asc::Asc;
use epkv_utils::chan::recv_timeout;
use epkv_utils::clone;
use epkv_utils::cmp::max_assign;
use epkv_utils::iter::map_collect;
use epkv_utils::time::LocalInstant;
use epkv_utils::vecmap::VecMap;
use epkv_utils::vecset::VecSet;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::mem;
use std::net::SocketAddr;
use std::ops::Not;
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
use tokio::time::sleep;
use tracing::error;

pub struct Replica<C, L, D, N>
where
    C: CommandLike,
    L: LogStore<C>,
    D: DataStore<C>,
    N: Network<C>,
{
    rid: ReplicaId,
    address: SocketAddr,
    config: ReplicaConfig,

    epoch: AtomicEpoch,
    state: AsyncMutex<State<C, L>>,

    propose_tx: DashMap<InstanceId, mpsc::Sender<Message<C>>>,
    join_tx: SyncMutex<Option<mpsc::Sender<JoinOk>>>,
    sync_tx: DashMap<SyncId, mpsc::Sender<SyncLogOk>>,

    graph: Graph<C>,
    data_store: D,

    net: N,
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

pub struct ReplicaBuilder<C, L, D, N> {
    pub rid: Option<ReplicaId>,
    pub address: Option<SocketAddr>,
    pub epoch: Option<Epoch>,
    pub peers: Option<VecMap<ReplicaId, SocketAddr>>,
    pub config: Option<ReplicaConfig>,
    pub log_store: Option<L>,
    pub data_store: Option<D>,
    pub net: Option<N>,
    _marker: PhantomData<C>,
}

impl<C, L, D, N> ReplicaBuilder<C, L, D, N>
where
    C: CommandLike,
    L: LogStore<C>,
    D: DataStore<C>,
    N: Network<C>,
{
    pub async fn build(self) -> Result<Arc<Replica<C, L, D, N>>> {
        Replica::new(self).await
    }
}

impl<C, L, D, N> Replica<C, L, D, N>
where
    C: CommandLike,
    L: LogStore<C>,
    D: DataStore<C>,
    N: Network<C>,
{
    #[must_use]
    pub fn builder() -> ReplicaBuilder<C, L, D, N> {
        ReplicaBuilder {
            rid: None,
            address: None,
            epoch: None,
            peers: None,
            config: None,
            log_store: None,
            data_store: None,
            net: None,
            _marker: PhantomData,
        }
    }

    async fn new(builder: ReplicaBuilder<C, L, D, N>) -> Result<Arc<Self>> {
        let rid = builder.rid.unwrap();
        let address = builder.address.unwrap();
        let epoch = builder.epoch.unwrap();
        let peers = builder.peers.unwrap();
        let config = builder.config.unwrap();
        let mut log_store = builder.log_store.unwrap();
        let data_store = builder.data_store.unwrap();
        let net = builder.net.unwrap();

        let cluster_size = peers.len().wrapping_add(1);
        let addr_set: VecSet<_> = map_collect(&peers, |&(_, a)| a);
        ensure!(cluster_size >= 3);
        ensure!(peers.iter().all(|&(p, a)| p != rid && a != address));
        ensure!(addr_set.len() == peers.len());

        let epoch = AtomicEpoch::new(epoch);

        let attr_bounds: _ = log_store.load_attr_bounds().await?;
        let status_bounds: _ = Asc::new(SyncMutex::new(log_store.load_status_bounds().await?));

        let state = {
            let peers_set: VecSet<_> = map_collect(&peers, |&(p, _)| p);
            let peers = Peers::new(peers_set);

            let lid_head =
                Head::new(attr_bounds.max_lids.get(&rid).copied().unwrap_or(LocalInstanceId::ZERO));

            let sync_id_head = Head::new(SyncId::ZERO);

            let log = Log::new(log_store, attr_bounds, status_bounds.asc_clone());

            let peer_status_bounds = PeerStatusBounds::new();

            AsyncMutex::new(State { peers, lid_head, sync_id_head, log, peer_status_bounds })
        };

        let propose_tx = DashMap::new();
        let join_tx = SyncMutex::new(None);
        let sync_tx = DashMap::new();

        let graph = Graph::new(status_bounds);

        for &(p, a) in &peers {
            net.register_peer(p, a)
        }

        Ok(Arc::new(Self {
            rid,
            address,
            config,
            state,
            epoch,
            propose_tx,
            join_tx,
            sync_tx,
            graph,
            data_store,
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
        let acc = VecSet::<ReplicaId>::with_capacity(1);

        self.phase_preaccept(guard, id, pbal, Some(cmd), acc).await
    }

    async fn phase_preaccept(
        self: &Arc<Self>,
        mut guard: AsyncMutexGuard<'_, State<C, L>>,
        id: InstanceId,
        pbal: Ballot,
        cmd: Option<C>,
        mut acc: VecSet<ReplicaId>,
    ) -> Result<()> {
        let (mut rx, mut seq, mut deps, mut acc) = {
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

            let avg_rtt = s.peers.get_avg_rtt();

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

            if pbal.0 == Round::ZERO {
                self.spawn_recover_timeout(id, avg_rtt);
            }

            (rx, seq, deps, acc)
        };

        {
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
                            if pbal.0 == Round::ZERO
                                && received.len() >= cluster_size.wrapping_sub(2)
                            {
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
                            return self.phase_commit(guard, id, pbal, cmd, seq, deps, acc).await;
                        } else {
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
                    return Ok(());
                }

                s.log.load(id).await?;
                let pbal = s.log.get_cached_pbal(id).expect("pbal should exist");

                let cmd = None;
                let _ = self.propose_tx.remove(&id);

                self.phase_accept(guard, id, pbal, cmd, seq, deps, acc).await
            }
        }
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

    // async fn end_phase_preaccept(
    //     self: &Arc<Self>,
    //     id: InstanceId,
    //     mut rx: mpsc::Receiver<Message<C>>,
    //     mut seq: Seq,
    //     mut deps: Deps,
    //     mut acc: VecSet<ReplicaId>,
    // ) -> Result<()> {

    // }

    #[allow(clippy::too_many_arguments)]
    async fn phase_accept(
        self: &Arc<Self>,
        mut guard: AsyncMutexGuard<'_, State<C, L>>,
        id: InstanceId,
        pbal: Ballot,
        cmd: Option<C>,
        seq: Seq,
        deps: Deps,
        acc: VecSet<ReplicaId>,
    ) -> Result<()> {
        let (mut rx, mut acc) = {
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

            (rx, acc)
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
                return self.phase_commit(guard, id, pbal, cmd, seq, deps, acc).await;
            }
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

    #[allow(clippy::too_many_arguments)]
    async fn phase_commit(
        self: &Arc<Self>,
        mut guard: AsyncMutexGuard<'_, State<C, L>>,
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
                if let Err(err) = this.run_execute(id, cmd, seq, deps).await {
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
                if let Err(err) = this.run_execute(id, cmd, seq, deps).await {
                    error!(?id, ?err)
                }
            });
        }
        Ok(())
    }

    async fn run_recover(self: &Arc<Self>, id: InstanceId) -> Result<()> {
        let mut rx = {
            let mut guard = self.state.lock().await;
            let s = &mut *guard;

            s.log.load(id).await?;

            if let Some(ins) = s.log.get_cached_ins(id) {
                if ins.status >= Status::Committed {
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

            let avg_rtt = s.peers.get_avg_rtt();

            drop(guard);

            let _ = targets.insert(self.rid);

            {
                let sender = self.rid;
                let epoch = self.epoch.load();

                self.net.broadcast(
                    targets,
                    Message::Prepare(Prepare { sender, epoch, id, pbal, known }),
                )
            }

            self.spawn_recover_timeout(id, avg_rtt);

            rx
        };

        {
            let mut received: VecSet<ReplicaId> = VecSet::new();

            let mut max_abal: Option<Ballot> = None;
            let mut cmd: Option<C> = None;

            // (sender, seq, deps, status, acc)
            let mut tuples: Vec<(ReplicaId, Seq, Deps, Status, VecSet<ReplicaId>)> = Vec::new();

            while let Some(msg) = rx.recv().await {
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

                let mut acc = match s.log.get_cached_ins(id) {
                    Some(ins) => ins.acc.clone(),
                    None => VecSet::new(),
                };
                for (_, _, _, _, a) in tuples.iter() {
                    acc.union_copied(a);
                }

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

                let cmd = match s.log.get_cached_ins(id) {
                    Some(_) => None,
                    None => {
                        acc = VecSet::new();
                        Some(C::create_nop())
                    }
                };

                return self.phase_preaccept(guard, id, pbal, cmd, acc).await;
            }
        }

        Ok(())
    }

    #[allow(clippy::float_arithmetic)]
    fn spawn_recover_timeout(self: &Arc<Self>, id: InstanceId, avg_rtt: Option<Duration>) {
        let conf = &self.config.recover_timeout;
        let duration = conf.with(avg_rtt, |d| {
            let rate: f64 = rand::thread_rng().gen_range(4.0..6.0);
            let delta = Duration::from_secs_f64(d.as_secs_f64() * rate);
            conf.default + delta
        });
        let this = Arc::clone(self);
        spawn(async move {
            sleep(duration).await;
            if let Err(err) = this.run_recover(id).await {
                error!(?id, ?err);
            }
        });
    }

    #[allow(clippy::float_arithmetic)]
    fn spawn_nack_recover_timeout(self: &Arc<Self>, id: InstanceId, avg_rtt: Option<Duration>) {
        let conf = &self.config.recover_timeout;
        let duration = conf.with(avg_rtt, |d| {
            let rate: f64 = rand::thread_rng().gen_range(1.0..4.0);
            Duration::from_secs_f64(d.as_secs_f64() * rate)
        });
        let this = Arc::clone(self);
        spawn(async move {
            sleep(duration).await;
            if let Err(err) = this.run_recover(id).await {
                error!(?id, ?err);
            }
        });
    }

    async fn handle_prepare(self: &Arc<Self>, msg: Prepare) -> Result<()> {
        if msg.epoch < self.epoch.load() {
            return Ok(());
        }

        let mut guard = self.state.lock().await;
        let s = &mut *guard;

        let id = msg.id;

        s.log.load(id).await?;

        let epoch = self.epoch.load();

        if let Some(pbal) = s.log.get_cached_pbal(id) {
            if pbal >= msg.pbal {
                drop(guard);

                let target = msg.sender;
                let sender = self.rid;
                self.net.send_one(
                    target,
                    Message::PrepareNack(PrepareNack { sender, epoch, id, pbal }),
                );
                return Ok(());
            }
        }

        let pbal = msg.pbal;

        s.log.save_pbal(id, pbal).await?;

        let ins: _ = match s.log.get_cached_ins(id) {
            Some(ins) => ins,
            None => {
                drop(guard);

                let target = msg.sender;
                let sender = self.rid;
                self.net.send_one(
                    target,
                    Message::PrepareUnchosen(PrepareUnchosen { sender, epoch, id }),
                );
                return Ok(());
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

        drop(guard);

        let target = msg.sender;
        let sender = self.rid;
        self.net.send_one(
            target,
            Message::PrepareOk(PrepareOk {
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
            }),
        );
        Ok(())
    }

    pub async fn run_join(self: &Arc<Self>) -> Result<bool> {
        let mut rx = {
            let mut guard = self.state.lock().await;
            let s = &mut *guard;

            let targets = s.peers.select_all();

            drop(guard);

            let rx = {
                let (tx, rx) = mpsc::channel(targets.len());
                let _ = self.join_tx.lock().insert(tx);
                rx
            };
            {
                let sender = self.rid;
                let epoch = self.epoch.load();
                let address = self.address;
                self.net.broadcast(targets, Message::Join(Join { sender, epoch, address }));
            }
            rx
        };

        {
            let mut received = VecSet::new();
            while let Some(msg) = rx.recv().await {
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

    async fn handle_join(self: &Arc<Self>, msg: Join) -> Result<()> {
        let mut guard = self.state.lock().await;
        let s = &mut *guard;

        s.peers.add(msg.sender);
        self.epoch.update_max(msg.epoch);

        drop(guard);

        {
            let target = msg.sender;
            self.net.register_peer(target, msg.address);
            self.net.send_one(target, Message::JoinOk(JoinOk { sender: self.rid }));
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

        let sender = self.rid;
        let time = LocalInstant::now();
        self.net.broadcast(targets, Message::ProbeRtt(ProbeRtt { sender, time }));
        Ok(())
    }

    async fn handle_probe_rtt(self: &Arc<Self>, msg: ProbeRtt) -> Result<()> {
        let target = msg.sender;
        let sender = self.rid;
        let time = msg.time;
        self.net.send_one(target, Message::ProbeRttOk(ProbeRttOk { sender, time }));
        Ok(())
    }

    async fn handle_probe_rtt_ok(self: &Arc<Self>, msg: ProbeRttOk) -> Result<()> {
        let time = LocalInstant::now();
        let peer = msg.sender;
        let rtt = time.saturating_duration_since(msg.time);

        let mut guard = self.state.lock().await;
        let s = &mut *guard;

        s.peers.set_rtt(peer, rtt);

        drop(guard);

        Ok(())
    }

    pub async fn run_sync_known(self: &Arc<Self>) -> Result<()> {
        let mut guard = self.state.lock().await;
        let s = &mut *guard;

        s.log.update_bounds();
        let known_up_to = s.log.known_up_to();

        let target = s.peers.select_one().expect("peer should exist");

        drop(guard);

        {
            let sender = self.rid;
            let targets = VecSet::from_single(target);
            self.net.broadcast(targets, Message::AskLog(AskLog { sender, known_up_to }));
        }

        Ok(())
    }

    async fn handle_ask_log(self: &Arc<Self>, msg: AskLog) -> Result<()> {
        let mut guard = self.state.lock().await;
        let s = &mut *guard;

        s.log.update_bounds();
        let local_known_up_to = s.log.known_up_to();

        let target = msg.sender;
        let sender = self.rid;
        let sync_id = SyncId::ZERO;
        let send_log = |instances| {
            self.net.send_one(
                target,
                Message::SyncLog(SyncLog { sender, sync_id, instances }),
            )
        };

        let conf = &self.config.sync_limits;
        let limit: usize = conf.max_instance_num.try_into().expect("usize overflow");

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
                self.net.broadcast(
                    targets,
                    Message::SyncLog(SyncLog { sender, sync_id, instances }),
                )
            };

            for &(rid, higher) in local_bounds.iter() {
                let lower: _ = peer_bounds.get(&rid).copied().unwrap_or(LocalInstanceId::ZERO);

                let conf = &self.config.sync_limits;
                let limit: usize = conf.max_instance_num.try_into().expect("usize overflow");

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

                        let _ = ins.acc.insert(self.rid);
                        let mode = if saved_ins.cmd.is_nop() != ins.cmd.is_nop() {
                            UpdateMode::Full
                        } else {
                            UpdateMode::Partial
                        };

                        let (cmd, seq, deps) = (ins.cmd.clone(), ins.seq, ins.deps.clone());
                        s.log.save(id, ins, mode).await?;

                        let this = Arc::clone(self);
                        spawn(async move {
                            if let Err(err) = this.run_execute(id, cmd, seq, deps).await {
                                error!(?id, ?err);
                            }
                        });
                    }
                }
            }
        }

        drop(guard);

        if msg.sync_id != SyncId::ZERO {
            let target = msg.sender;
            let sender = self.rid;
            let sync_id = msg.sync_id;
            self.net.send_one(target, Message::SyncLogOk(SyncLogOk { sender, sync_id }));
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

    async fn handle_peer_bounds(self: &Arc<Self>, msg: PeerBounds) -> Result<()> {
        let mut guard = self.state.lock().await;
        let s = &mut *guard;

        if let Some(bounds) = msg.committed_up_to {
            s.peer_status_bounds.set_committed(msg.sender, bounds);
        }

        drop(guard);

        Ok(())
    }

    async fn run_execute(
        self: &Arc<Self>,
        id: InstanceId,
        cmd: C,
        seq: Seq,
        deps: Deps,
    ) -> Result<()> {
        let root = self.graph.init_node(id, cmd, seq, deps);

        let _executing = match self.graph.executing(id) {
            Some(exec) => exec,
            None => return Ok(()),
        };

        {
            let _row_guard = self.graph.lock_row(id.0).await;

            let mut q = DepsQueue::from_single(id);

            while let Some(u_id) = q.pop() {
                // TODO

                // for v_id in u_node.deps {
                //      // TODO
                //      if ... {
                //         q.push(v_id);
                //      }
                // }
            }
        }

        todo!()
    }
}
