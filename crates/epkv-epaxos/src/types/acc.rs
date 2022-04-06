use super::id::ReplicaId;

use epkv_utils::vecset::VecSet;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Acc(VecSet<ReplicaId>);

impl Acc {
    pub fn with_capacity(cap: usize) -> Self {
        Self(VecSet::with_capacity(cap))
    }

    pub fn insert(&mut self, rid: ReplicaId) {
        let _ = self.0.insert(rid);
    }

    pub fn union(&mut self, other: &Self) {
        self.0.union_copied(&other.0);
    }
}
