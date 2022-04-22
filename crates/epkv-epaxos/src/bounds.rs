use crate::id::{InstanceId, LocalInstanceId, ReplicaId, Seq};
use crate::status::Status;

use epkv_utils::onemap::OneMap;
use epkv_utils::vecmap::VecMap;

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct AttrBounds {
    pub max_seq: Seq,
    pub max_lids: VecMap<ReplicaId, LocalInstanceId>,
}

pub struct StatusBounds(VecMap<ReplicaId, StatusMap>);

#[derive(Default)]
pub struct StatusMap {
    pub known: OneMap,
    pub committed: OneMap,
    pub executed: OneMap,
}

impl AsRef<VecMap<ReplicaId, StatusMap>> for StatusBounds {
    fn as_ref(&self) -> &VecMap<ReplicaId, StatusMap> {
        &self.0
    }
}

impl AsMut<VecMap<ReplicaId, StatusMap>> for StatusBounds {
    fn as_mut(&mut self) -> &mut VecMap<ReplicaId, StatusMap> {
        &mut self.0
    }
}

impl StatusBounds {
    #[must_use]
    pub fn from_maps(maps: VecMap<ReplicaId, StatusMap>) -> Self {
        Self(maps)
    }

    pub fn set(&mut self, id: InstanceId, status: Status) {
        let InstanceId(rid, lid) = id;
        let (_, m) = self.0.init_with(rid, StatusMap::default);
        m.known.set(lid.raw_value());
        if status >= Status::Committed {
            m.committed.set(lid.raw_value());
        }
        if status >= Status::Executed {
            m.executed.set(lid.raw_value());
        }
    }

    pub fn update_bounds(&mut self) {
        self.0.iter_mut().for_each(|(_, m)| {
            m.known.update_bound();
            m.committed.update_bound();
            m.executed.update_bound();
        })
    }

    #[must_use]
    pub fn known_up_to(&self) -> VecMap<ReplicaId, LocalInstanceId> {
        self.0.iter().map(|&(r, ref m)| (r, LocalInstanceId::from(m.known.bound()))).collect()
    }

    #[must_use]
    pub fn committed_up_to(&self) -> VecMap<ReplicaId, LocalInstanceId> {
        self.0.iter().map(|&(r, ref m)| (r, LocalInstanceId::from(m.committed.bound()))).collect()
    }

    #[must_use]
    pub fn executed_up_to(&self) -> VecMap<ReplicaId, LocalInstanceId> {
        self.0.iter().map(|&(r, ref m)| (r, LocalInstanceId::from(m.executed.bound()))).collect()
    }
}

#[derive(Default)]
pub struct PeerStatusBounds {
    committed: VecMap<ReplicaId, VecMap<ReplicaId, LocalInstanceId>>,
}

impl PeerStatusBounds {
    #[must_use]
    pub const fn new() -> Self {
        Self { committed: VecMap::new() }
    }

    pub fn set_committed(&mut self, rid: ReplicaId, bounds: VecMap<ReplicaId, LocalInstanceId>) {
        let _ = self.committed.insert(rid, bounds);
    }

    #[must_use]
    pub fn committed_up_to(&self) -> VecMap<ReplicaId, LocalInstanceId> {
        let mut ans = VecMap::new();
        for (_, m) in self.committed.iter() {
            ans.merge_copied_with(m, |lhs, rhs| lhs.min(rhs))
        }
        ans
    }
}
