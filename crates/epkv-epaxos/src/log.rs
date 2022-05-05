use crate::bounds::{AttrBounds, SavedStatusBounds, StatusBounds};
use crate::cmd::{CommandLike, Keys};
use crate::deps::MutableDeps;
use crate::id::{Ballot, InstanceId, LocalInstanceId, ReplicaId, Seq};
use crate::ins::Instance;
use crate::status::Status;
use crate::store::{LogStore, UpdateMode};

use std::collections::{hash_map, HashMap};
use std::mem;
use std::ops::Not;

use epkv_utils::asc::Asc;
use epkv_utils::clone;
use epkv_utils::cmp::max_assign;
use epkv_utils::iter::{copied_map_collect, map_collect};
use epkv_utils::lock::with_mutex;
use epkv_utils::vecmap::VecMap;

use anyhow::Result;
use fnv::FnvHashMap;
use parking_lot::Mutex as SyncMutex;

pub struct Log<C, L>
where
    C: CommandLike,
    L: LogStore<C>,
{
    log_store: L,

    max_key_map: HashMap<C::Key, MaxKey>,
    max_lid_map: VecMap<ReplicaId, MaxLid>,
    max_seq: MaxSeq,

    status_bounds: Asc<SyncMutex<StatusBounds>>,

    ins_cache: FnvHashMap<InstanceId, Instance<C>>,
    pbal_cache: FnvHashMap<InstanceId, Ballot>,
}

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

impl<C, L> Log<C, L>
where
    C: CommandLike,
    L: LogStore<C>,
{
    pub fn new(log_store: L, attr_bounds: AttrBounds, status_bounds: Asc<SyncMutex<StatusBounds>>) -> Self {
        let max_key_map = HashMap::new();

        let max_lid_map = copied_map_collect(attr_bounds.max_lids.iter(), |(rid, lid)| {
            let max_lid = MaxLid { checkpoint: lid, any: lid };
            (rid, max_lid)
        });

        let max_seq = MaxSeq { checkpoint: attr_bounds.max_seq, any: attr_bounds.max_seq };

        let ins_cache = FnvHashMap::default();
        let pbal_cache = FnvHashMap::default();

        Self {
            log_store,
            max_key_map,
            max_lid_map,
            max_seq,
            status_bounds,
            ins_cache,
            pbal_cache,
        }
    }

    pub fn calc_attributes(&self, id: InstanceId, keys: &Keys<C>) -> (Seq, MutableDeps) {
        let mut deps = MutableDeps::with_capacity(self.max_lid_map.len());
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

    fn update_attrs(&mut self, id: InstanceId, keys: Keys<C>, seq: Seq) {
        let InstanceId(rid, lid) = id;

        match keys {
            Keys::Bounded(keys) => {
                for k in keys.into_iter() {
                    match self.max_key_map.entry(k) {
                        hash_map::Entry::Occupied(mut e) => {
                            let m = e.get_mut();
                            max_assign(&mut m.seq, seq);
                            m.lids.update(rid, |l| max_assign(l, lid), || lid);
                        }
                        hash_map::Entry::Vacant(e) => {
                            let mut lids = VecMap::new();
                            let _ = lids.insert(rid, lid);
                            e.insert(MaxKey { seq, lids });
                        }
                    }
                }

                self.max_lid_map.update(
                    rid,
                    |m| max_assign(&mut m.any, lid),
                    || MaxLid { checkpoint: lid, any: lid },
                );

                max_assign(&mut self.max_seq.any, seq);
            }
            Keys::Unbounded => {
                self.max_lid_map.update(
                    rid,
                    |m| {
                        max_assign(&mut m.checkpoint, lid);
                        max_assign(&mut m.any, lid);
                    },
                    || MaxLid { checkpoint: lid, any: lid },
                );

                max_assign(&mut self.max_seq.checkpoint, seq);
                max_assign(&mut self.max_seq.any, seq);
            }
        }
    }

    pub fn clear_key_map(&mut self) -> impl Send + Sync + 'static {
        let garbage = mem::take(&mut self.max_key_map);
        for (_, m) in &mut self.max_lid_map {
            m.checkpoint = m.any;
        }
        {
            let m = &mut self.max_seq;
            m.checkpoint = m.any;
        }
        garbage
    }

    pub async fn save(&mut self, id: InstanceId, ins: Instance<C>, mode: UpdateMode) -> Result<()> {
        let needs_update_attrs = if let Some(saved) = self.ins_cache.get(&id) {
            saved.seq != ins.seq || saved.deps != ins.deps
        } else {
            true
        };

        {
            clone!(ins);
            self.log_store.save(id, ins, mode).await?;
        }

        if needs_update_attrs {
            self.update_attrs(id, ins.cmd.keys(), ins.seq);
        }

        self.status_bounds.lock().set(id, ins.status);

        let _ = self.ins_cache.insert(id, ins);
        let _ = self.pbal_cache.remove(&id);
        Ok(())
    }

    pub async fn load(&mut self, id: InstanceId) -> Result<()> {
        if self.ins_cache.contains_key(&id).not() {
            if let Some(ins) = self.log_store.load(id).await? {
                self.status_bounds.lock().set(id, ins.status);
                let _ = self.ins_cache.insert(id, ins);
                let _ = self.pbal_cache.remove(&id);
            } else if self.pbal_cache.contains_key(&id).not() {
                if let Some(pbal) = self.log_store.load_pbal(id).await? {
                    let _ = self.pbal_cache.insert(id, pbal);
                }
            }
        }
        Ok(())
    }

    pub async fn save_pbal(&mut self, id: InstanceId, pbal: Ballot) -> Result<()> {
        self.log_store.save_pbal(id, pbal).await?;

        match self.ins_cache.get_mut(&id) {
            Some(ins) => {
                ins.pbal = pbal;
            }
            None => {
                let _ = self.pbal_cache.insert(id, pbal);
            }
        }

        Ok(())
    }

    pub async fn update_status(&mut self, id: InstanceId, status: Status) -> Result<()> {
        self.log_store.update_status(id, status).await?;
        if let Some(ins) = self.ins_cache.get_mut(&id) {
            ins.status = status;
        }
        self.status_bounds.lock().set(id, status);
        Ok(())
    }

    pub fn get_cached_pbal(&self, id: InstanceId) -> Option<Ballot> {
        if let Some(ins) = self.ins_cache.get(&id) {
            return Some(ins.pbal);
        }
        self.pbal_cache.get(&id).copied()
    }

    pub fn get_cached_ins(&self, id: InstanceId) -> Option<&Instance<C>> {
        self.ins_cache.get(&id)
    }

    pub fn should_ignore_pbal(&self, id: InstanceId, pbal: Ballot) -> bool {
        if let Some(saved_pbal) = self.get_cached_pbal(id) {
            if saved_pbal != pbal {
                return true;
            }
        }

        false
    }

    pub fn should_ignore_status(&self, id: InstanceId, pbal: Ballot, next_status: Status) -> bool {
        if let Some(ins) = self.ins_cache.get(&id) {
            let abal = ins.abal;
            let status = ins.status;

            if (pbal, next_status) <= (abal, status) {
                return true;
            }
        }
        false
    }

    pub fn update_bounds(&mut self) {
        self.status_bounds.lock().update_bounds();
    }

    pub fn known_up_to(&self) -> VecMap<ReplicaId, LocalInstanceId> {
        self.status_bounds.lock().known_up_to()
    }

    pub fn committed_up_to(&self) -> VecMap<ReplicaId, LocalInstanceId> {
        self.status_bounds.lock().committed_up_to()
    }

    pub fn executed_up_to(&self) -> VecMap<ReplicaId, LocalInstanceId> {
        self.status_bounds.lock().executed_up_to()
    }

    pub async fn save_bounds(&mut self) -> Result<()> {
        let saved_status_bounds = with_mutex(&self.status_bounds, |status_bounds: _| {
            status_bounds.update_bounds();
            SavedStatusBounds {
                known_up_to: status_bounds.known_up_to(),
                committed_up_to: status_bounds.committed_up_to(),
                executed_up_to: status_bounds.executed_up_to(),
            }
        });
        let attr_bounds = AttrBounds {
            max_seq: self.max_seq.any,
            max_lids: map_collect(&self.max_lid_map, |&(rid, ref m)| (rid, m.any)),
        };
        self.log_store.save_bounds(attr_bounds, saved_status_bounds).await?;
        Ok(())
    }

    pub fn retire_instance(&mut self, id: InstanceId) {
        if self.ins_cache.remove(&id).is_none() {
            self.pbal_cache.remove(&id);
        }
    }
}
