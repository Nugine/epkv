use crate::id::{InstanceId, LocalInstanceId, ReplicaId};

use epkv_utils::asc::Asc;
use epkv_utils::cmp::max_assign;
use epkv_utils::vecmap::VecMap;

use std::hash::Hash;

use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MutableDeps(VecMap<ReplicaId, LocalInstanceId>);

impl MutableDeps {
    #[must_use]
    pub fn with_capacity(cap: usize) -> Self {
        Self(VecMap::with_capacity(cap))
    }

    pub fn insert(&mut self, id: InstanceId) {
        let InstanceId(rid, lid) = id;
        self.0.update(rid, |prev| max_assign(prev, lid), || lid);
    }

    pub fn merge(&mut self, other: &Self) {
        self.0.merge_copied_with(&other.0, |v1, v2| v1.max(v2))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Deps(Asc<MutableDeps>);

impl Deps {
    #[inline]
    fn as_inner(&self) -> &MutableDeps {
        &*self.0
    }

    #[must_use]
    pub fn from_mutable(deps: MutableDeps) -> Self {
        Self(Asc::new(deps))
    }

    #[must_use]
    pub fn into_mutable(self) -> MutableDeps {
        match Asc::try_into_inner(self.0) {
            Ok(d) => d,
            Err(a) => MutableDeps::clone(&*a),
        }
    }
}

impl PartialEq for Deps {
    fn eq(&self, other: &Self) -> bool {
        Asc::ptr_eq(&self.0, &other.0) || self.as_inner() == other.as_inner()
    }
}

impl Eq for Deps {}

impl Hash for Deps {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_inner().hash(state);
    }
}

impl AsRef<MutableDeps> for Deps {
    fn as_ref(&self) -> &MutableDeps {
        self.as_inner()
    }
}

static EMPTY: Lazy<Deps> = Lazy::new(|| Deps(Asc::new(MutableDeps(VecMap::new()))));

impl Default for Deps {
    fn default() -> Self {
        let empty = &*EMPTY;
        empty.clone()
    }
}

impl Deps {
    pub fn elements(&self) -> impl Iterator<Item = InstanceId> + '_ {
        self.as_inner().0.iter().copied().map(|(rid, lid)| InstanceId(rid, lid))
    }
}
