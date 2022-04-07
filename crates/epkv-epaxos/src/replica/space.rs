use crate::types::*;

use epkv_utils::cmp::max_assign;
use epkv_utils::vecmap::VecMap;

use std::collections::HashMap;

use anyhow::Result;

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

pub struct Space<S: LogStore> {
    max_lid: LocalInstanceId,

    max_key_map: HashMap<CommandKey<S>, MaxKey>,
    max_lid_map: VecMap<ReplicaId, MaxLid>,
    max_seq: MaxSeq,

    store: S,
}

impl<S: LogStore> Space<S> {
    pub fn new(rid: ReplicaId, store: S) -> Result<Self> {
        let attr_bounds = store.load_attr_bounds()?;

        let max_lid = attr_bounds
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

        Ok(Self {
            max_lid,
            max_key_map,
            max_lid_map,
            max_seq,
            store,
        })
    }

    pub fn generate_lid(&mut self) -> LocalInstanceId {
        let lid = self.max_lid.add_one();
        self.max_lid = lid;
        lid
    }

    pub fn calc_attributes(&self, id: InstanceId, keys: &Keys<S::Command>) -> (Seq, Deps) {
        let mut deps = Deps::with_capacity(self.max_lid_map.len());
        let mut seq = Seq::ZERO;
        let InstanceId(rid, lid) = id;

        match keys {
            Keys::Bounded(ref keys) => {
                let conflicts: _ = keys.iter().filter_map(|k: _| self.max_key_map.get(k));
                for m in conflicts {
                    let others: _ = m.lids.iter().copied().filter(|&(r, _)| r != rid);
                    for (r, l) in others {
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
}
