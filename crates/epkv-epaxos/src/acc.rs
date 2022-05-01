use crate::id::ReplicaId;

use epkv_utils::asc::Asc;
use epkv_utils::vecset::VecSet;

use std::hash::Hash;

use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MutableAcc(VecSet<ReplicaId>);

impl MutableAcc {
    #[must_use]
    pub fn with_capacity(cap: usize) -> Self {
        Self(VecSet::with_capacity(cap))
    }

    pub fn insert(&mut self, id: ReplicaId) {
        let _ = self.0.insert(id);
    }

    pub fn union(&mut self, other: &Self) {
        self.0.union_copied(&other.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Acc(Asc<MutableAcc>);

impl Acc {
    #[inline]
    fn as_inner(&self) -> &MutableAcc {
        &*self.0
    }

    #[must_use]
    pub fn from_mutable(acc: MutableAcc) -> Self {
        Self(Asc::new(acc))
    }

    #[must_use]
    pub fn into_mutable(self) -> MutableAcc {
        match Asc::try_into_inner(self.0) {
            Ok(a) => a,
            Err(a) => MutableAcc::clone(&*a),
        }
    }

    pub fn cow_insert(&mut self, id: ReplicaId) {
        let acc = Asc::make_mut(&mut self.0);
        acc.insert(id);
    }
}

impl PartialEq for Acc {
    fn eq(&self, other: &Self) -> bool {
        Asc::ptr_eq(&self.0, &other.0) || self.as_inner() == other.as_inner()
    }
}

impl Eq for Acc {}

impl Hash for Acc {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_inner().hash(state);
    }
}

impl AsRef<MutableAcc> for Acc {
    fn as_ref(&self) -> &MutableAcc {
        self.as_inner()
    }
}

static EMPTY: Lazy<Acc> = Lazy::new(|| Acc(Asc::new(MutableAcc(VecSet::new()))));

impl Default for Acc {
    fn default() -> Self {
        Acc::clone(&*EMPTY)
    }
}

impl AsRef<VecSet<ReplicaId>> for MutableAcc {
    fn as_ref(&self) -> &VecSet<ReplicaId> {
        &self.0
    }
}

impl AsRef<VecSet<ReplicaId>> for Acc {
    fn as_ref(&self) -> &VecSet<ReplicaId> {
        &self.as_inner().0
    }
}