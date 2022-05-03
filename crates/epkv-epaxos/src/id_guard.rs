use crate::id::InstanceId;

use dashmap::DashSet;

pub struct IdGuard<'a> {
    id_set: &'a DashSet<InstanceId>,
    id: InstanceId,
}

impl<'a> IdGuard<'a> {
    #[must_use]
    pub fn new(id_set: &'a DashSet<InstanceId>, id: InstanceId) -> Option<Self> {
        let is_new = id_set.insert(id);
        is_new.then(|| Self { id_set, id })
    }
}

impl Drop for IdGuard<'_> {
    fn drop(&mut self) {
        self.id_set.remove(&self.id);
    }
}
