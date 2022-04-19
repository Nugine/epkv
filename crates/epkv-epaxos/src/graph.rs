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
use epkv_utils::watermark::WaterMark;

use std::collections::hash_map;
use std::sync::Arc;

use dashmap::DashMap;
use dashmap::DashSet;
use fnv::FnvHashMap;
use parking_lot::Mutex as SyncMutex;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::OwnedMutexGuard as OwnedAsyncMutexGuard;

pub struct Graph<C> {
    nodes: DashMap<InstanceId, Asc<Node<C>>>,
    status_bounds: Asc<SyncMutex<StatusBounds>>,
    executing: DashSet<InstanceId>,
    row_locks: DashMap<ReplicaId, Arc<AsyncMutex<()>>>,
    watermarks: DashMap<ReplicaId, Asc<WaterMark>>,
}

pub struct Node<C> {
    pub cmd: C,
    pub seq: Seq,
    pub deps: Deps,
    pub status: SyncMutex<ExecStatus>,
}

pub struct RowGuard(OwnedAsyncMutexGuard<()>);

impl<C> Graph<C> {
    #[must_use]
    pub fn new(status_bounds: Asc<SyncMutex<StatusBounds>>) -> Self {
        let nodes = DashMap::new();
        let executing = DashSet::new();
        let row_locks = DashMap::new();
        let watermarks = DashMap::new();
        Self { nodes, status_bounds, executing, row_locks, watermarks }
    }

    #[must_use]
    pub fn init_node(&self, id: InstanceId, cmd: C, seq: Seq, deps: Deps) -> Asc<Node<C>> {
        let gen = || {
            let status: _ = SyncMutex::new(ExecStatus::Committed);
            Asc::new(Node { cmd, seq, deps, status })
        };
        self.nodes.entry(id).or_insert_with(gen).clone()

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

    pub async fn lock_row(&self, rid: ReplicaId) -> RowGuard {
        let gen = || Arc::new(AsyncMutex::new(()));
        let mutex: Arc<_> = self.row_locks.entry(rid).or_insert_with(gen).clone();
        RowGuard(mutex.lock_owned().await)
    }

    #[must_use]
    pub fn watermark(&self, rid: ReplicaId) -> Asc<WaterMark> {
        let mut guard = self.status_bounds.lock();
        let status_bounds = &mut *guard;
        let bound = status_bounds.maps.get(&rid).map(|m| m.committed.bound());
        drop(guard);

        let gen = || Asc::new(WaterMark::new(bound.unwrap_or(0)));
        self.watermarks.entry(rid).or_insert_with(gen).clone()
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

pub struct LocalGraph<C> {
    nodes: FnvHashMap<InstanceId, Asc<Node<C>>>,
}

impl<C> LocalGraph<C> {
    #[must_use]
    pub fn new() -> Self {
        Self { nodes: FnvHashMap::default() }
    }

    #[must_use]
    pub fn add_node(&mut self, id: InstanceId, node: Asc<Node<C>>) -> bool {
        match self.nodes.entry(id) {
            hash_map::Entry::Occupied(_) => false,
            hash_map::Entry::Vacant(e) => {
                e.insert(node);
                true
            }
        }
    }
}

impl<C> Default for LocalGraph<C> {
    fn default() -> Self {
        Self::new()
    }
}
