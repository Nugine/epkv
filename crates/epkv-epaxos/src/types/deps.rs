use super::id::{InstanceId, LocalInstanceId, ReplicaId};

use epkv_utils::cmp::max_assign;
use epkv_utils::vecmap::VecMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Deps(VecMap<ReplicaId, LocalInstanceId>);

impl Deps {
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
