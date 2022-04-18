use crate::deps::Deps;
use crate::id::InstanceId;
use crate::id::Seq;
use crate::status::ExecStatus;

use dashmap::mapref::entry::Entry;
use epkv_utils::asc::Asc;

use dashmap::DashMap;
use parking_lot::Mutex as SyncMutex;

pub struct Graph<C> {
    nodes: DashMap<InstanceId, Asc<Node<C>>>,
}

pub struct Node<C> {
    pub cmd: C,
    pub seq: Seq,
    pub deps: Deps,
    pub status: SyncMutex<ExecStatus>,
}

impl<C> Graph<C> {
    #[must_use]
    pub fn new() -> Self {
        Self { nodes: DashMap::new() }
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
    }

    #[inline]
    #[must_use]
    pub fn find_node(&self, id: InstanceId) -> Option<Asc<Node<C>>> {
        self.nodes.get(&id).as_deref().cloned()
    }

    #[inline]
    pub fn retire_node(&self, id: InstanceId) {
        let _ = self.nodes.remove(&id);
    }
}

impl<C> Default for Graph<C> {
    fn default() -> Self {
        Self::new()
    }
}
