mod config;
mod peers;

pub use self::config::ReplicaConfig;
use self::peers::Peers;

use crate::types::*;

use epkv_utils::cmp::max_assign;
use epkv_utils::vecmap::VecMap;
use epkv_utils::vecset::VecSet;

use std::collections::HashMap;
use std::ops::Not;

use anyhow::ensure;
use anyhow::Result;
use fnv::FnvHashMap;
use tokio::sync::Mutex;

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

        let state = Mutex::new(State::new(rid, store, epoch, peers).await?);
        let epoch = AtomicEpoch::new(epoch);

        Ok(Self {
            rid,
            config,
            state,
            epoch,
        })
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

    pub async fn propose(&self, cmd: S::Command) -> Result<Effect<S::Command>> {
        let mut guard = self.state.lock().await;
        let state = &mut *guard;

        let id = InstanceId(self.rid, state.generate_lid());

        drop(guard);

        let pbal = Ballot(Round::ZERO, self.rid);
        let acc = VecSet::<ReplicaId>::with_capacity(1);

        self.start_phase_pre_accept(id, pbal, cmd, acc).await
    }

    async fn start_phase_pre_accept(
        &self,
        id: InstanceId,
        pbal: Ballot,
        cmd: S::Command,
        mut acc: VecSet<ReplicaId>,
    ) -> Result<Effect<S::Command>> {
        let keys = cmd.keys();

        let mut guard = self.state.lock().await;
        let state = &mut *guard;

        let (seq, deps) = state.calc_attributes(id, &keys);

        let abal = pbal;
        let status = Status::PreAccepted;
        let _ = acc.insert(self.rid);

        let ins = Instance {
            pbal,
            cmd: cmd.clone(),
            seq,
            deps: deps.clone(),
            abal,
            status,
            acc: acc.clone(),
        };

        state.save(id, ins).await?;

        let quorum = state.peers.cluster_size().wrapping_sub(2);
        let selected_peers = state.peers.select(quorum, &acc);

        drop(guard);

        let msg = PreAccept {
            sender: self.rid,
            epoch: self.epoch.load(),
            id,
            pbal,
            cmd: Some(cmd),
            seq,
            deps,
            acc,
        };

        Ok(Effect::broadcast_pre_accept(
            selected_peers.acc,
            selected_peers.others,
            msg,
        ))
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

    async fn handle_join(&self, msg: Join) -> Result<Effect<S::Command>> {
        let mut guard = self.state.lock().await;
        let state = &mut *guard;

        state.peers.add(msg.sender);
        self.epoch.update_max(msg.epoch);

        drop(guard);

        Ok(Effect::reply(
            msg.sender,
            Message::JoinOk(JoinOk { sender: self.rid }),
        ))
    }

    async fn handle_join_ok(&self, msg: JoinOk) -> Result<Effect<S::Command>> {
        todo!()
    }

    async fn handle_leave(&self, msg: Leave) -> Result<Effect<S::Command>> {
        let mut guard = self.state.lock().await;
        let state = &mut *guard;

        state.peers.remove(msg.sender);

        drop(guard);

        Ok(Effect::empty())
    }
}

struct State<S: LogStore> {
    peers: Peers,

    store: S,

    lid_head: LocalInstanceId,

    max_key_map: HashMap<CommandKey<S>, MaxKey>,
    max_lid_map: VecMap<ReplicaId, MaxLid>,
    max_seq: MaxSeq,

    ins_cache: FnvHashMap<InstanceId, Instance<S::Command>>,
    pbal_cache: FnvHashMap<InstanceId, Ballot>,
}

type CommandKey<S> = <<S as LogStore>::Command as CommandLike>::Key;

struct MaxKey {
    seq: Seq,
    lids: VecMap<ReplicaId, LocalInstanceId>,
}

struct MaxLid {
    checkpoint: LocalInstanceId,
    any: LocalInstanceId,
}

struct MaxSeq {
    checkpoint: Seq,
    any: Seq,
}

impl<S: LogStore> State<S> {
    async fn new(rid: ReplicaId, store: S, epoch: Epoch, peers: VecSet<ReplicaId>) -> Result<Self> {
        let peers = Peers::new(peers);

        let attr_bounds = store.load_attr_bounds().await?;

        let lid_head = attr_bounds
            .max_lids
            .get(&rid)
            .copied()
            .unwrap_or(LocalInstanceId::ZERO);

        let max_key_map = HashMap::new();

        let max_lid_map = attr_bounds
            .max_lids
            .iter()
            .copied()
            .map(|(rid, lid)| {
                let max_lid = MaxLid {
                    checkpoint: lid,
                    any: lid,
                };
                (rid, max_lid)
            })
            .collect();

        let max_seq = MaxSeq {
            checkpoint: attr_bounds.max_seq,
            any: attr_bounds.max_seq,
        };

        let ins_cache = FnvHashMap::default();
        let pbal_cache = FnvHashMap::default();

        Ok(Self {
            peers,
            store,
            lid_head,
            max_key_map,
            max_lid_map,
            max_seq,
            ins_cache,
            pbal_cache,
        })
    }

    fn generate_lid(&mut self) -> LocalInstanceId {
        self.lid_head = self.lid_head.add_one();
        self.lid_head
    }

    fn calc_attributes(&self, id: InstanceId, keys: &Keys<S::Command>) -> (Seq, Deps) {
        let mut deps = Deps::with_capacity(self.max_lid_map.len());
        let mut seq = Seq::ZERO;
        let InstanceId(rid, lid) = id;

        match keys {
            Keys::Bounded(ref keys) => {
                let conflicts: _ = keys.iter().filter_map(|k: _| self.max_key_map.get(k));
                for m in conflicts {
                    let others: _ = m.lids.iter().filter(|(r, _)| *r != rid);
                    for &(r, l) in others {
                        deps.insert(InstanceId(r, l));
                    }
                    max_assign(&mut seq, m.seq);
                }
                let others: _ = self.max_lid_map.iter().filter(|(r, _)| *r != rid);
                for &(r, ref m) in others {
                    deps.insert(InstanceId(r, m.checkpoint));
                }
                max_assign(&mut seq, self.max_seq.checkpoint);
            }
            Keys::Unbounded => {
                let others: _ = self.max_lid_map.iter().filter(|(r, _)| *r != rid);
                for &(r, ref m) in others {
                    deps.insert(InstanceId(r, m.any));
                }
                max_assign(&mut seq, self.max_seq.any);
            }
        }
        if lid > LocalInstanceId::ONE {
            deps.insert(InstanceId(rid, lid.sub_one()));
        }
        seq = seq.add_one();
        (seq, deps)
    }

    async fn save(&mut self, id: InstanceId, ins: Instance<S::Command>) -> Result<()> {
        self.store.save_instance(id, &ins).await?;
        let _ = self.ins_cache.insert(id, ins);
        let _ = self.pbal_cache.remove(&id);
        Ok(())
    }

    async fn load(&mut self, id: InstanceId) -> Result<()> {
        if self.ins_cache.contains_key(&id).not() {
            if let Some(ins) = self.store.load_instance(id).await? {
                let _ = self.ins_cache.insert(id, ins);
                let _ = self.pbal_cache.remove(&id);
            } else if self.pbal_cache.contains_key(&id).not() {
                if let Some(pbal) = self.store.load_pbal(id).await? {
                    let _ = self.pbal_cache.insert(id, pbal);
                }
            }
        }
        Ok(())
    }

    fn get_cached_pbal(&self, id: InstanceId) -> Option<Ballot> {
        if let Some(ins) = self.ins_cache.get(&id) {
            return Some(ins.pbal);
        }
        self.pbal_cache.get(&id).copied()
    }

    async fn should_ignore(
        &mut self,
        id: InstanceId,
        pbal: Ballot,
        next_status: Status,
    ) -> Result<bool> {
        self.load(id).await?;

        if let Some(saved_pbal) = self.get_cached_pbal(id) {
            if saved_pbal != pbal {
                return Ok(true);
            }
        }

        if let Some(ins) = self.ins_cache.get(&id) {
            let abal = ins.abal;
            let status = ins.status;

            if (pbal, next_status) <= (abal, status) {
                return Ok(true);
            }
        }

        Ok(false)
    }
}
