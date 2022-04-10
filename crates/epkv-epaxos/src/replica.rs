mod config;
mod peers;
mod state;
mod temporary;

pub use self::config::ReplicaConfig;
use self::state::State;
use self::temporary::{Accepting, PreAccepting, Preparing, Temporary};

use crate::types::*;

use epkv_utils::clone;
use epkv_utils::cmp::max_assign;
use epkv_utils::vecset::VecSet;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::mem;
use std::ops::Not;
use std::time::Duration;

use anyhow::{ensure, Result};
use rand::Rng;
use tokio::sync::{Mutex, MutexGuard};

pub struct Replica<S: LogStore> {
    rid: ReplicaId,
    config: ReplicaConfig,
    state: Mutex<State<S>>,
    epoch: AtomicEpoch,
}

impl<S: LogStore> Replica<S> {
    pub async fn new(
        rid: ReplicaId,
        store: S,
        epoch: Epoch,
        peers: VecSet<ReplicaId>,
        config: ReplicaConfig,
    ) -> Result<Self> {
        let cluster_size = peers.len().wrapping_add(1);
        ensure!(peers.iter().all(|&p| p != rid));
        ensure!(cluster_size >= 3);

        let state = Mutex::new(State::new(rid, store, peers).await?);
        let epoch = AtomicEpoch::new(epoch);

        Ok(Self { rid, config, state, epoch })
    }

    pub async fn handle_message(&self, msg: Message<S::Command>) -> Result<Effect<S::Command>> {
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
        }
    }

    pub async fn propose(&self, cmd: S::Command) -> Result<Effect<S::Command>> {
        let mut guard = self.state.lock().await;
        let state = &mut *guard;

        let id = InstanceId(self.rid, state.generate_lid());
        let pbal = Ballot(Round::ZERO, self.rid);
        let acc = VecSet::<ReplicaId>::with_capacity(1);

        self.start_phase_preaccept(guard, id, pbal, Some(cmd), acc).await
    }

    async fn start_phase_preaccept(
        &self,
        mut guard: MutexGuard<'_, State<S>>,
        id: InstanceId,
        pbal: Ballot,
        cmd: Option<S::Command>,
        mut acc: VecSet<ReplicaId>,
    ) -> Result<Effect<S::Command>> {
        let state = &mut *guard;

        state.load(id).await?;

        let (cmd, mode) = match cmd {
            Some(cmd) => (cmd, UpdateMode::Full),
            None => {
                let ins: _ = state.get_cached_ins(id).expect("instance should exist");
                (ins.cmd.clone(), UpdateMode::Partial)
            }
        };

        let (seq, deps) = state.calc_attributes(id, &cmd.keys());

        let abal = pbal;
        let status = Status::PreAccepted;
        let _ = acc.insert(self.rid);

        {
            clone!(cmd, deps, acc);
            let ins = Instance { pbal, cmd, seq, deps, abal, status, acc };
            state.save(id, ins, mode).await?;
        }

        let quorum = state.peers.cluster_size().wrapping_sub(2);
        let selected_peers = state.peers.select(quorum, &acc);

        {
            clone!(deps, acc);
            let received = VecSet::new();
            let temp = PreAccepting { received, seq, deps, all_same: true, acc };
            let _ = state.temporaries.insert(id, Temporary::PreAccepting(temp));
        }

        drop(guard);

        let mut effect = Effect::new();
        {
            let sender = self.rid;
            let epoch = self.epoch.load();
            effect.broadcast_preaccept(
                selected_peers.acc,
                selected_peers.others,
                PreAccept { sender, epoch, id, pbal, cmd: Some(cmd), seq, deps, acc },
            );
        }
        Ok(effect)
    }

    async fn handle_preaccept(&self, msg: PreAccept<S::Command>) -> Result<Effect<S::Command>> {
        if msg.epoch < self.epoch.load() {
            return Ok(Effect::new());
        }

        let mut guard = self.state.lock().await;
        let state = &mut *guard;

        let id = msg.id;
        let pbal = msg.pbal;

        state.load(id).await?;

        if state.should_ignore_pbal(id, pbal) {
            return Ok(Effect::new());
        }
        if state.should_ignore_status(id, pbal, Status::PreAccepted) {
            return Ok(Effect::new());
        }

        let (cmd, mode) = match msg.cmd {
            Some(cmd) => (cmd, UpdateMode::Full),
            None => {
                let ins: _ = state.get_cached_ins(id).expect("instance should exist");
                (ins.cmd.clone(), UpdateMode::Partial)
            }
        };

        let (mut seq, mut deps) = state.calc_attributes(id, &cmd.keys());
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
            state.save(id, ins, mode).await?
        }

        drop(guard);

        let mut effect = Effect::new();
        {
            let target = msg.sender;
            let sender = self.rid;
            let epoch = self.epoch.load();
            effect.reply(
                target,
                if is_changed {
                    Message::PreAcceptDiff(PreAcceptDiff { sender, epoch, id, pbal, seq, deps })
                } else {
                    Message::PreAcceptOk(PreAcceptOk { sender, epoch, id, pbal })
                },
            );
        }
        Ok(effect)
    }

    async fn handle_preaccept_reply(&self, msg: PreAcceptReply) -> Result<Effect<S::Command>> {
        let msg_epoch = match msg {
            PreAcceptReply::Ok(ref msg) => msg.epoch,
            PreAcceptReply::Diff(ref msg) => msg.epoch,
        };

        if msg_epoch < self.epoch.load() {
            return Ok(Effect::new());
        }

        let mut guard = self.state.lock().await;
        let state = &mut *guard;

        let (id, pbal) = match msg {
            PreAcceptReply::Ok(ref msg) => (msg.id, msg.pbal),
            PreAcceptReply::Diff(ref msg) => (msg.id, msg.pbal),
        };

        state.load(id).await?;

        if state.should_ignore_pbal(id, pbal) {
            return Ok(Effect::new());
        }

        let ins: _ = state.get_cached_ins(id).expect("instance should exist");

        if ins.status != Status::PreAccepted {
            return Ok(Effect::new());
        }

        let temp = match state.temporaries.get_mut(&id) {
            Some(Temporary::PreAccepting(t)) => t,
            _ => return Ok(Effect::new()),
        };

        {
            let msg_sender = match msg {
                PreAcceptReply::Ok(ref msg) => msg.sender,
                PreAcceptReply::Diff(ref msg) => msg.sender,
            };
            if temp.received.insert(msg_sender).is_some() {
                return Ok(Effect::new());
            }
            let _ = temp.acc.insert(msg_sender);
        }

        match msg {
            PreAcceptReply::Ok(_) => {}
            PreAcceptReply::Diff(msg) => {
                let mut seq = msg.seq;
                let mut deps = msg.deps;
                max_assign(&mut seq, temp.seq);
                deps.merge(&temp.deps);
                if temp.received.len() > 1 && (seq != temp.seq || deps != temp.deps) {
                    temp.all_same = false;
                }
                temp.seq = seq;
                temp.deps = deps;
            }
        }

        let cluster_size = state.peers.cluster_size();

        if temp.received.len() < cluster_size / 2 {
            return Ok(Effect::new());
        }

        let which_path = if temp.all_same {
            if pbal.0 == Round::ZERO && temp.received.len() >= cluster_size.wrapping_sub(2) {
                Some(true)
            } else {
                None
            }
        } else {
            Some(false)
        };

        match which_path {
            Some(is_fast_path) => {
                let cmd = None;
                let seq = temp.seq;
                let deps = mem::take(&mut temp.deps);
                let acc = mem::take(&mut temp.acc);
                let _ = state.temporaries.remove(&id);

                if is_fast_path {
                    self.start_phase_commit(guard, id, pbal, cmd, seq, deps, acc).await
                } else {
                    self.start_phase_accept(guard, id, pbal, cmd, seq, deps, acc).await
                }
            }
            None => {
                let conf = &self.config.fastpath_timeout;
                let duration = if conf.enable_adaptive {
                    state.peers.get_avg_rtt().unwrap_or(conf.default)
                } else {
                    conf.default
                };
                drop(guard);
                let mut effect = Effect::new();
                effect.timeout(duration, TimeoutKind::PreAcceptFastPath { id });
                Ok(effect)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_phase_accept(
        &self,
        mut guard: MutexGuard<'_, State<S>>,
        id: InstanceId,
        pbal: Ballot,
        cmd: Option<S::Command>,
        seq: Seq,
        deps: Deps,
        acc: VecSet<ReplicaId>,
    ) -> Result<Effect<S::Command>> {
        let state = &mut *guard;

        let abal = pbal;
        let status = Status::Accepted;

        let quorum = state.peers.cluster_size() / 2;
        let selected_peers = state.peers.select(quorum, &acc);

        let (cmd, mode) = match cmd {
            Some(cmd) => (cmd, UpdateMode::Full),
            None => {
                state.load(id).await?;
                let ins: _ = state.get_cached_ins(id).expect("instance should exist");
                (ins.cmd.clone(), UpdateMode::Partial)
            }
        };

        {
            clone!(cmd, deps, acc);
            let ins: _ = Instance { pbal, cmd, seq, deps, abal, status, acc };
            state.save(id, ins, mode).await?;
        }

        {
            clone!(acc);
            let received = VecSet::new();
            let temp = Accepting { received, acc };
            let _ = state.temporaries.insert(id, Temporary::Accepting(temp));
        }

        drop(guard);

        let mut effect = Effect::new();
        {
            let sender = self.rid;
            let epoch = self.epoch.load();
            effect.broadcast_accept(
                selected_peers.acc,
                selected_peers.others,
                Accept { sender, epoch, id, pbal, cmd: Some(cmd), seq, deps, acc },
            );
        }
        Ok(effect)
    }

    async fn handle_accept(&self, msg: Accept<S::Command>) -> Result<Effect<S::Command>> {
        if msg.epoch < self.epoch.load() {
            return Ok(Effect::new());
        }

        let mut guard = self.state.lock().await;
        let state = &mut *guard;

        let id = msg.id;
        let pbal = msg.pbal;

        state.load(id).await?;

        if state.should_ignore_pbal(id, pbal) {
            return Ok(Effect::new());
        }
        if state.should_ignore_status(id, pbal, Status::Accepted) {
            return Ok(Effect::new());
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
                let ins: _ = state.get_cached_ins(id).expect("instance should exist");
                (ins.cmd.clone(), UpdateMode::Partial)
            }
        };

        {
            let ins: _ = Instance { pbal, cmd, seq, deps, abal, status, acc };
            state.save(id, ins, mode).await?;
        }

        drop(guard);

        let mut effect = Effect::new();
        {
            let target = msg.sender;
            let sender = self.rid;
            let epoch = self.epoch.load();
            effect.reply(
                target,
                Message::AcceptOk(AcceptOk { sender, epoch, id, pbal }),
            );
        }
        Ok(effect)
    }

    async fn handle_accept_reply(&self, msg: AcceptReply) -> Result<Effect<S::Command>> {
        let AcceptReply::Ok(msg) = msg;

        if msg.epoch < self.epoch.load() {
            return Ok(Effect::new());
        }

        let mut guard = self.state.lock().await;
        let state = &mut *guard;

        let id = msg.id;
        let pbal = msg.pbal;

        state.load(id).await?;

        if state.should_ignore_pbal(id, pbal) {
            return Ok(Effect::new());
        }

        let ins: _ = state.get_cached_ins(id).expect("instance should exist");

        if ins.status != Status::Accepted {
            return Ok(Effect::new());
        }

        let seq = ins.seq;
        let deps = ins.deps.clone();

        let temp = match state.temporaries.get_mut(&id) {
            Some(Temporary::Accepting(t)) => t,
            _ => return Ok(Effect::new()),
        };

        {
            if temp.received.insert(msg.sender).is_some() {
                return Ok(Effect::new());
            }
            let _ = temp.acc.insert(msg.sender);
        }

        let cluster_size = state.peers.cluster_size();

        if temp.received.len() < cluster_size / 2 {
            return Ok(Effect::new());
        }

        let acc = mem::take(&mut temp.acc);
        let _ = state.temporaries.remove(&id);

        let cmd = None;
        self.start_phase_commit(guard, id, pbal, cmd, seq, deps, acc).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_phase_commit(
        &self,
        mut guard: MutexGuard<'_, State<S>>,
        id: InstanceId,
        pbal: Ballot,
        cmd: Option<S::Command>,
        seq: Seq,
        deps: Deps,
        acc: VecSet<ReplicaId>,
    ) -> Result<Effect<S::Command>> {
        let state = &mut *guard;

        let abal = pbal;
        let status = Status::Committed;

        let quorum = state.peers.cluster_size().wrapping_sub(1);
        let selected_peers = state.peers.select(quorum, &acc);

        let (cmd, mode) = match cmd {
            Some(cmd) => (cmd, UpdateMode::Full),
            None => {
                state.load(id).await?;
                let ins: _ = state.get_cached_ins(id).expect("instance should exist");
                (ins.cmd.clone(), UpdateMode::Partial)
            }
        };

        {
            clone!(cmd, deps, acc);
            let ins: _ = Instance { pbal, cmd, seq, deps, abal, status, acc };
            state.save(id, ins, mode).await?;
        }

        drop(guard);

        let mut effect = Effect::new();

        if let Some(n) = cmd.notify_committed() {
            effect.notify(n);
        }

        {
            let sender = self.rid;
            let epoch = self.epoch.load();
            clone!(cmd, deps);
            effect.broadcast_commit(
                selected_peers.acc,
                selected_peers.others,
                Commit { sender, epoch, id, pbal, cmd: Some(cmd), seq, deps, acc },
            );
        }

        {
            effect.execution(id, cmd, seq, deps);
        }

        Ok(effect)
    }

    async fn handle_commit(&self, msg: Commit<S::Command>) -> Result<Effect<S::Command>> {
        if msg.epoch < self.epoch.load() {
            return Ok(Effect::new());
        }

        let mut guard = self.state.lock().await;
        let state = &mut *guard;

        let id = msg.id;
        let pbal = msg.pbal;

        state.load(id).await?;

        if state.should_ignore_pbal(id, pbal) {
            return Ok(Effect::new());
        }

        if state.should_ignore_status(id, pbal, Status::Committed) {
            return Ok(Effect::new());
        }

        let (cmd, mode) = match msg.cmd {
            Some(cmd) => (cmd, UpdateMode::Full),
            None => {
                state.load(id).await?;
                let ins: _ = state.get_cached_ins(id).expect("instance should exist");
                (ins.cmd.clone(), UpdateMode::Partial)
            }
        };

        let (status, exec) = match state.get_cached_ins(id) {
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
            state.save(id, ins, mode).await?
        }

        drop(guard);

        let mut effect = Effect::new();
        if exec {
            effect.execution(id, cmd, seq, deps)
        }
        Ok(effect)
    }

    async fn recover(&self, id: InstanceId) -> Result<Effect<S::Command>> {
        let mut guard = self.state.lock().await;
        let state = &mut *guard;

        state.load(id).await?;

        let pbal = match state.get_cached_pbal(id) {
            Some(Ballot(rnd, _)) => Ballot(rnd.add_one(), self.rid),
            None => Ballot(Round::ZERO, self.rid),
        };

        let known = matches!(state.get_cached_ins(id), Some(ins) if ins.cmd.is_nop().not());

        let mut targets = state.peers.select_all();

        {
            let received = VecSet::new();
            let temp: _ = Preparing { received, max_abal: None, cmd: None, tuples: Vec::new() };
            let _ = state.temporaries.insert(id, Temporary::Preparing(temp));
        }

        drop(guard);

        let _ = targets.insert(self.rid);

        let mut effect = Effect::new();
        {
            let sender = self.rid;
            let epoch = self.epoch.load();

            effect.broadcast(
                targets,
                Message::Prepare(Prepare { sender, epoch, id, pbal, known }),
            )
        }

        Ok(effect)
    }

    async fn handle_prepare(&self, msg: Prepare) -> Result<Effect<S::Command>> {
        if msg.epoch < self.epoch.load() {
            return Ok(Effect::new());
        }

        let mut guard = self.state.lock().await;
        let state = &mut *guard;

        let id = msg.id;

        state.load(id).await?;

        let epoch = self.epoch.load();

        if let Some(pbal) = state.get_cached_pbal(id) {
            if pbal >= msg.pbal {
                drop(guard);
                let mut effect = Effect::new();
                let target = msg.sender;
                let sender = self.rid;
                effect.reply(
                    target,
                    Message::PrepareNack(PrepareNack { sender, epoch, id, pbal }),
                );
                return Ok(effect);
            }
        }

        let pbal = msg.pbal;

        state.save_pbal(id, pbal).await?;

        let ins: _ = match state.get_cached_ins(id) {
            Some(ins) => ins,
            None => {
                drop(guard);
                let mut effect = Effect::new();
                let target = msg.sender;
                let sender = self.rid;
                effect.reply(
                    target,
                    Message::PrepareUnchosen(PrepareUnchosen { sender, epoch, id }),
                );
                return Ok(effect);
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

        let mut effect = Effect::new();
        let target = msg.sender;
        let sender = self.rid;
        effect.reply(
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
        Ok(effect)
    }

    async fn handle_prepare_reply(
        &self,
        msg: PrepareReply<S::Command>,
    ) -> Result<Effect<S::Command>> {
        let msg_epoch = match msg {
            PrepareReply::Ok(ref msg) => msg.epoch,
            PrepareReply::Nack(ref msg) => msg.epoch,
            PrepareReply::Unchosen(ref msg) => msg.epoch,
        };

        if msg_epoch < self.epoch.load() {
            return Ok(Effect::new());
        }

        let mut guard = self.state.lock().await;
        let state = &mut *guard;

        let id = match msg {
            PrepareReply::Ok(ref msg) => msg.id,
            PrepareReply::Nack(ref msg) => msg.id,
            PrepareReply::Unchosen(ref msg) => msg.id,
        };

        state.load(id).await?;
        if let PrepareReply::Ok(ref msg) = msg {
            if state.should_ignore_pbal(id, msg.pbal) {
                return Ok(Effect::new());
            }
        }

        let temp: _ = match state.temporaries.get_mut(&id) {
            Some(Temporary::Preparing(t)) => t,
            _ => return Ok(Effect::new()),
        };

        match msg {
            PrepareReply::Unchosen(msg) => {
                let _ = temp.received.insert(msg.sender);
            }
            PrepareReply::Nack(msg) => {
                state.save_pbal(id, msg.pbal).await?;

                let conf = &self.config.recover;
                let duration = if conf.enable_adaptive {
                    match state.peers.get_avg_rtt() {
                        Some(d) => {
                            let rate: f64 = rand::thread_rng().gen_range(1.0..4.0);
                            #[allow(clippy::float_arithmetic)]
                            Duration::from_secs_f64(d.as_secs_f64() * rate)
                        }
                        None => conf.default,
                    }
                } else {
                    conf.default
                };

                let _ = state.temporaries.remove(&id);

                drop(guard);

                let mut effect = Effect::new();
                effect.timeout(duration, TimeoutKind::Recover { id });
                return Ok(effect);
            }
            PrepareReply::Ok(msg) => {
                let _ = temp.received.insert(msg.sender);

                let is_max_abal = match temp.max_abal {
                    Some(ref mut max_abal) => match Ord::cmp(&msg.abal, max_abal) {
                        Ordering::Less => false,
                        Ordering::Equal => true,
                        Ordering::Greater => {
                            *max_abal = msg.abal;
                            temp.cmd = None;
                            temp.tuples.clear();
                            true
                        }
                    },
                    None => {
                        temp.max_abal = Some(msg.abal);
                        true
                    }
                };
                if is_max_abal.not() {
                    return Ok(Effect::new());
                }
                temp.cmd = msg.cmd;
                temp.tuples.push((msg.sender, msg.seq, msg.deps, msg.status, msg.acc));
            }
        }

        let cluster_size = state.peers.cluster_size();
        if temp.received.len() <= cluster_size / 2 {
            return Ok(Effect::new());
        }

        let max_abal = match temp.max_abal {
            Some(b) => b,
            None => return Ok(Effect::new()),
        };

        let cmd = temp.cmd.take();
        let mut tuples = mem::take(&mut temp.tuples);
        let _ = state.temporaries.remove(&id);

        let pbal = state.get_cached_pbal(id).expect("pbal should exist");

        let mut acc = match state.get_cached_ins(id) {
            Some(ins) => ins.acc.clone(),
            None => VecSet::new(),
        };
        for (_, _, _, _, a) in tuples.iter() {
            acc.union_copied(a);
        }

        for &mut (_, seq, ref mut deps, status, _) in tuples.iter_mut() {
            if status >= Status::Committed {
                let deps = mem::take(deps);
                return self.start_phase_commit(guard, id, pbal, cmd, seq, deps, acc).await;
            } else if status == Status::Accepted {
                let deps = mem::take(deps);
                return self.start_phase_accept(guard, id, pbal, cmd, seq, deps, acc).await;
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
                    return self.start_phase_accept(guard, id, pbal, cmd, seq, deps, acc).await;
                }
            }
        }

        if let Some(t) = tuples.first_mut() {
            return self.start_phase_preaccept(guard, id, pbal, cmd, acc).await;
        }

        let cmd = match state.get_cached_ins(id) {
            Some(_) => None,
            None => {
                acc = VecSet::new();
                Some(S::Command::create_nop())
            }
        };

        self.start_phase_preaccept(guard, id, pbal, cmd, acc).await
    }

    pub async fn start_joining(&self) -> Result<Effect<S::Command>> {
        let mut guard = self.state.lock().await;
        let state = &mut *guard;

        state.joining = Some(VecSet::new());

        let targets = state.peers.select_all();

        drop(guard);

        let mut effect = Effect::new();
        let sender = self.rid;
        effect.broadcast(targets, Message::JoinOk(JoinOk { sender }));
        Ok(effect)
    }

    async fn handle_join(&self, msg: Join) -> Result<Effect<S::Command>> {
        let mut guard = self.state.lock().await;
        let state = &mut *guard;

        state.peers.add(msg.sender);
        self.epoch.update_max(msg.epoch);

        drop(guard);

        let mut effect = Effect::new();
        {
            let target = msg.sender;
            effect.reply(target, Message::JoinOk(JoinOk { sender: self.rid }));
        }
        Ok(effect)
    }

    async fn handle_join_ok(&self, msg: JoinOk) -> Result<Effect<S::Command>> {
        let mut guard = self.state.lock().await;
        let state = &mut *guard;

        let mut effect = Effect::new();

        if let Some(ref mut j) = state.joining {
            let _ = j.insert(msg.sender);
            if j.len() > state.peers.cluster_size() / 2 {
                state.joining = None;
                effect.join_finished = true;
            }
        }

        drop(guard);

        Ok(effect)
    }

    async fn handle_leave(&self, msg: Leave) -> Result<Effect<S::Command>> {
        let mut guard = self.state.lock().await;
        let state = &mut *guard;

        state.peers.remove(msg.sender);

        drop(guard);

        Ok(Effect::new())
    }
}
