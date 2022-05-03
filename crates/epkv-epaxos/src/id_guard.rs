use crate::id::InstanceId;

use epkv_utils::asc::Asc;

use std::ops::Not;

use dashmap::DashSet;

pub struct IdGuard {
    id_set: Asc<DashSet<InstanceId>>,
    id: InstanceId,
}

impl IdGuard {
    #[must_use]
    pub fn new(id_set: Asc<DashSet<InstanceId>>, id: InstanceId) -> Option<Self> {
        let is_new = id_set.insert(id);
        is_new.then(|| Self { id_set, id })
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.id_set.contains(&self.id).not()
    }
}

impl Drop for IdGuard {
    fn drop(&mut self) {
        self.id_set.remove(&self.id);
    }
}
