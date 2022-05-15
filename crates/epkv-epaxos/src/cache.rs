use crate::bounds::AttrBounds;
use crate::cmd::{CommandLike, Keys};
use crate::deps::MutableDeps;
use crate::id::{Ballot, InstanceId, LocalInstanceId, ReplicaId, Seq};
use crate::ins::Instance;

use epkv_utils::cmp::max_assign;
use epkv_utils::iter::{copied_map_collect, map_collect};

use std::collections::{hash_map, HashMap};
use std::mem;

use fnv::FnvHashMap;
use ordered_vecmap::VecMap;

pub struct LogCache<C>
where
    C: CommandLike,
{
    ins_cache: VecMap<ReplicaId, FnvHashMap<LocalInstanceId, Instance<C>>>,
    pbal_cache: FnvHashMap<InstanceId, Ballot>,

    max_key_map: HashMap<C::Key, MaxKey>,
    max_lid_map: VecMap<ReplicaId, MaxLid>,
    max_seq: MaxSeq,
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

impl<C> LogCache<C>
where
    C: CommandLike,
{
    #[must_use]
    pub fn new(attr_bounds: AttrBounds) -> Self {
        let max_key_map = HashMap::new();

        let max_lid_map = copied_map_collect(attr_bounds.max_lids.iter(), |(rid, lid)| {
            let max_lid = MaxLid { checkpoint: lid, any: lid };
            (rid, max_lid)
        });

        let max_seq = MaxSeq { checkpoint: attr_bounds.max_seq, any: attr_bounds.max_seq };

        let ins_cache = VecMap::new();
        let pbal_cache = FnvHashMap::default();

        Self { ins_cache, pbal_cache, max_key_map, max_lid_map, max_seq }
    }

    pub fn calc_attributes(&self, id: InstanceId, keys: &C::Keys) -> (Seq, MutableDeps) {
        let mut deps = MutableDeps::with_capacity(self.max_lid_map.len());
        let mut seq = Seq::ZERO;
        let InstanceId(rid, lid) = id;

        if keys.is_unbounded() {
            let others: _ = self.max_lid_map.iter().filter(|(r, _)| *r != rid);
            for &(r, ref m) in others {
                deps.insert(InstanceId(r, m.any));
            }
            max_assign(&mut seq, self.max_seq.any);
        } else {
            keys.for_each(|k| {
                if let Some(m) = self.max_key_map.get(k) {
                    let others: _ = m.lids.iter().filter(|(r, _)| *r != rid);
                    for &(r, l) in others {
                        deps.insert(InstanceId(r, l));
                    }
                    max_assign(&mut seq, m.seq);
                }
            });
            let others: _ = self.max_lid_map.iter().filter(|(r, _)| *r != rid);
            for &(r, ref m) in others {
                if m.checkpoint > LocalInstanceId::ZERO {
                    deps.insert(InstanceId(r, m.checkpoint));
                }
            }
            max_assign(&mut seq, self.max_seq.checkpoint);
        }
        if lid > LocalInstanceId::ONE {
            deps.insert(InstanceId(rid, lid.sub_one()));
        }
        seq = seq.add_one();
        (seq, deps)
    }

    pub fn update_attrs(&mut self, id: InstanceId, keys: C::Keys, seq: Seq) {
        let InstanceId(rid, lid) = id;

        if keys.is_unbounded() {
            self.max_lid_map
                .entry(rid)
                .and_modify(|m| {
                    max_assign(&mut m.checkpoint, lid);
                    max_assign(&mut m.any, lid);
                })
                .or_insert_with(|| MaxLid { checkpoint: lid, any: lid });

            max_assign(&mut self.max_seq.checkpoint, seq);
            max_assign(&mut self.max_seq.any, seq);
        } else {
            keys.for_each(|k| match self.max_key_map.entry(k.clone()) {
                hash_map::Entry::Occupied(mut e) => {
                    let m = e.get_mut();
                    max_assign(&mut m.seq, seq);
                    m.lids.entry(rid).and_modify(|l| max_assign(l, lid)).or_insert(lid);
                }
                hash_map::Entry::Vacant(e) => {
                    let lids = VecMap::from_single(rid, lid);
                    e.insert(MaxKey { seq, lids });
                }
            });

            self.max_lid_map
                .entry(rid)
                .and_modify(|m| max_assign(&mut m.any, lid))
                .or_insert_with(|| MaxLid { checkpoint: LocalInstanceId::ZERO, any: lid });

            max_assign(&mut self.max_seq.any, seq);
        }
    }

    pub fn clear_key_map(&mut self) -> impl Send + Sync + 'static {
        let cap = self.max_key_map.capacity() / 2;
        let new_key_map = HashMap::with_capacity(cap);

        let garbage = mem::replace(&mut self.max_key_map, new_key_map);

        for (_, m) in &mut self.max_lid_map {
            m.checkpoint = m.any;
        }
        {
            let m = &mut self.max_seq;
            m.checkpoint = m.any;
        }

        garbage
    }

    #[must_use]
    pub fn get_ins(&self, id: InstanceId) -> Option<&Instance<C>> {
        let row = self.ins_cache.get(&id.0)?;
        row.get(&id.1)
    }

    #[must_use]
    pub fn get_mut_ins(&mut self, id: InstanceId) -> Option<&mut Instance<C>> {
        let row = self.ins_cache.get_mut(&id.0)?;
        row.get_mut(&id.1)
    }

    #[must_use]
    pub fn contains_ins(&self, id: InstanceId) -> bool {
        match self.ins_cache.get(&id.0) {
            Some(row) => row.contains_key(&id.1),
            None => false,
        }
    }

    pub fn insert_ins(&mut self, id: InstanceId, ins: Instance<C>) {
        let row: _ = self.ins_cache.entry(id.0).or_insert_with(FnvHashMap::default);
        if row.insert(id.1, ins).is_none() {
            self.pbal_cache.remove(&id);
        }
    }

    pub fn insert_orphan_pbal(&mut self, id: InstanceId, pbal: Ballot) {
        self.pbal_cache.insert(id, pbal);
    }

    #[must_use]
    pub fn contains_orphan_pbal(&self, id: InstanceId) -> bool {
        self.pbal_cache.contains_key(&id)
    }

    #[must_use]
    pub fn get_pbal(&self, id: InstanceId) -> Option<Ballot> {
        if let Some(ins) = self.get_ins(id) {
            return Some(ins.pbal);
        }
        self.pbal_cache.get(&id).copied()
    }

    pub fn remove_ins(&mut self, id: InstanceId) {
        if let Some(row) = self.ins_cache.get_mut(&id.0) {
            if row.remove(&id.1).is_none() {
                self.pbal_cache.remove(&id);
            }
        }
    }

    #[must_use]
    pub fn calc_attr_bounds(&self) -> AttrBounds {
        AttrBounds {
            max_seq: self.max_seq.any,
            max_lids: map_collect(&self.max_lid_map, |&(rid, ref m)| (rid, m.any)),
        }
    }
}
