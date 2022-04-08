mod config;
mod meta;

pub use self::config::ReplicaConfig;
use self::meta::Meta;

use crate::types::*;

use epkv_utils::cmp::max_assign;
use epkv_utils::vecmap::VecMap;
use epkv_utils::vecset::VecSet;

use std::collections::HashMap;

use anyhow::ensure;
use anyhow::Result;
use fnv::FnvHashMap;
use tokio::sync::Mutex;

pub struct Replica<S: LogStore> {
    rid: ReplicaId,
    config: ReplicaConfig,
    state: Mutex<State<S>>,
}

impl<S: LogStore> Replica<S> {
    pub async fn new(
        rid: ReplicaId,
        store: S,
        epoch: Epoch,
        peers: &VecSet<ReplicaId>,
        config: ReplicaConfig,
    ) -> Result<Self> {
        let cluster_size = peers.len().wrapping_add(1);
        ensure!(peers.iter().all(|&p| p != rid));
        ensure!(cluster_size >= 3);

        let state = State::new(rid, store, epoch, peers).await?;
        let state = Mutex::new(state);
        Ok(Self { rid, config, state })
    }

    pub async fn propose(&self, cmd: S::Command) -> Result<Effect<S::Command>> {
        let mut guard = self.state.lock().await;
        let state = &mut *guard;

        let id = InstanceId(self.rid, state.generate_lid());
        let pbal = Ballot(state.meta.epoch(), Round::ZERO, self.rid);
        let acc = VecSet::<ReplicaId>::with_capacity(1);

        drop(guard);

        self.start_phase_pre_accept(id, pbal, cmd, acc).await
    }

    async fn start_phase_pre_accept(
        &self,
        id: InstanceId,
        pbal: Ballot,
        cmd: <S as LogStore>::Command,
        acc: VecSet<ReplicaId>,
    ) -> Result<Effect<S::Command>> {
        todo!()
    }
}

struct State<S: LogStore> {
    meta: Meta,

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
    async fn new(
        rid: ReplicaId,
        store: S,
        epoch: Epoch,
        peers: &VecSet<ReplicaId>,
    ) -> Result<Self> {
        let meta = Meta::new(epoch, peers);

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
            meta,
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
}
