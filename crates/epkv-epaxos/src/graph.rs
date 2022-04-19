use crate::bounds::StatusBounds;
use crate::deps::Deps;
use crate::id::InstanceId;
use crate::id::LocalInstanceId;
use crate::id::ReplicaId;
use crate::id::Seq;
use crate::status::ExecStatus;

use epkv_utils::asc::Asc;
use epkv_utils::cmp::max_assign;
use epkv_utils::vecmap::VecMap;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use dashmap::DashSet;
use parking_lot::Mutex as SyncMutex;

pub struct Graph<C> {
    nodes: DashMap<InstanceId, Asc<Node<C>>>,
    status_bounds: Asc<SyncMutex<StatusBounds>>,
    executing: DashSet<InstanceId>,
}

pub struct Node<C> {
    pub cmd: C,
    pub seq: Seq,
    pub deps: Deps,
    pub status: SyncMutex<ExecStatus>,
}

impl<C> Graph<C> {
    #[must_use]
    pub fn new(status_bounds: Asc<SyncMutex<StatusBounds>>) -> Self {
        let nodes = DashMap::new();
        let executing = DashSet::new();
        Self { nodes, status_bounds, executing }
    }

    #[must_use]
    pub fn init_node(&self, id: InstanceId, cmd: C, seq: Seq, deps: Deps) -> Asc<Node<C>> {
        match self.nodes.entry(id) {
            Entry::Occupied(e) => e.get().clone(),
            Entry::Vacant(e) => {
                let status: _ = SyncMutex::new(ExecStatus::Committed);
                let node = Asc::new(Node { cmd, seq, deps, status });
                e.insert(node).clone()
            }
        }
        // TODO
    }

    #[must_use]
    pub fn find_node(&self, id: InstanceId) -> Option<Asc<Node<C>>> {
        self.nodes.get(&id).as_deref().cloned()
        // TODO
    }

    pub fn retire_node(&self, id: InstanceId) {
        let _ = self.nodes.remove(&id);
        // TODO
    }

    #[must_use]
    pub fn executing(&self, id: InstanceId) -> Option<Executing<'_>> {
        Executing::new(&self.executing, id)
    }
}

pub struct Executing<'a> {
    id_set: &'a DashSet<InstanceId>,
    id: InstanceId,
}

impl<'a> Executing<'a> {
    fn new(id_set: &'a DashSet<InstanceId>, id: InstanceId) -> Option<Self> {
        let is_new = id_set.insert(id);
        is_new.then(|| Self { id_set, id })
    }
}

impl Drop for Executing<'_> {
    fn drop(&mut self) {
        self.id_set.remove(&self.id);
    }
}

pub struct DepsQueue(VecMap<ReplicaId, LocalInstanceId>);

impl DepsQueue {
    #[must_use]
    pub fn from_single(InstanceId(rid, lid): InstanceId) -> Self {
        Self(VecMap::from_single(rid, lid))
    }

    pub fn push(&mut self, InstanceId(rid, lid): InstanceId) {
        self.0.update(rid, |v: _| max_assign(v, lid), || lid);
    }

    #[must_use]
    pub fn pop(&mut self) -> Option<InstanceId> {
        self.0.pop_max().map(|(rid, lid)| InstanceId(rid, lid))
    }
}
