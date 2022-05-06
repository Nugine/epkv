use crate::bounds::StatusBounds;
use crate::deps::Deps;
use crate::id::InstanceId;
use crate::id::LocalInstanceId;
use crate::id::ReplicaId;
use crate::id::Seq;
use crate::status::ExecStatus;
use crate::status::Status;

use epkv_utils::asc::Asc;
use epkv_utils::cmp::max_assign;
use epkv_utils::cmp::min_assign;
use epkv_utils::lock::with_mutex;
use epkv_utils::vecmap::VecMap;
use epkv_utils::vecset::VecSet;
use epkv_utils::watermark::WaterMark;

use std::fmt;
use std::hash::Hash;
use std::ops::Not;

use dashmap::DashMap;
use fnv::{FnvHashMap, FnvHashSet};
use parking_lot::Mutex as SyncMutex;
use parking_lot::MutexGuard as SyncMutexGuard;
use rand::prelude::SliceRandom;
use tokio::sync::Notify;

pub struct Graph<C> {
    nodes: DashMap<InstanceId, Node<C>>,
    status_bounds: Asc<SyncMutex<StatusBounds>>,
    watermarks: DashMap<ReplicaId, Asc<WaterMark>>,
}

enum Node<C> {
    InGraph(Asc<InsNode<C>>),
    Waiting(Asc<Notify>),
}

pub struct InsNode<C> {
    pub cmd: C,
    pub seq: Seq,
    pub deps: Deps,
    status: SyncMutex<ExecStatus>,
}

impl<C> InsNode<C> {
    pub fn estatus<R>(&self, f: impl FnOnce(&mut ExecStatus) -> R) -> R {
        let mut guard = self.status.lock();
        f(&mut *guard)
    }

    pub fn lock_estatus(&self) -> SyncMutexGuard<'_, ExecStatus> {
        self.status.lock()
    }
}

impl<C> fmt::Debug for InsNode<C> {
    #[track_caller]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // locking status here will cause reentry deadlock
        f.debug_struct("InsNode")
            .field("seq", &self.seq)
            .field("deps", &self.deps)
            .finish_non_exhaustive()
    }
}

impl<C> Graph<C> {
    #[must_use]
    pub fn new(status_bounds: Asc<SyncMutex<StatusBounds>>) -> Self {
        let nodes = DashMap::new();
        let watermarks = DashMap::new();
        Self { nodes, status_bounds, watermarks }
    }

    pub fn init_node(&self, id: InstanceId, cmd: C, seq: Seq, deps: Deps, status: Status) {
        let exec_status = match status {
            Status::Committed => ExecStatus::Committed,
            Status::Issued => ExecStatus::Issued,
            Status::Executed => ExecStatus::Executed,
            _ => panic!("unexpected status: {:?}", status),
        };

        let gen: _ = || Asc::new(InsNode { cmd, seq, deps, status: SyncMutex::new(exec_status) });
        match self.nodes.entry(id) {
            dashmap::mapref::entry::Entry::Occupied(mut e) => match e.get_mut() {
                Node::InGraph(_) => {}
                Node::Waiting(n) => {
                    n.notify_waiters();
                    e.insert(Node::InGraph(gen()));
                }
            },
            dashmap::mapref::entry::Entry::Vacant(e) => {
                e.insert(Node::InGraph(gen()));
            }
        }

        self.sync_watermark(id.0);
    }

    pub fn sync_watermark(&self, rid: ReplicaId) {
        let committed_up_to = with_mutex(&self.status_bounds, |status_bounds: _| {
            let m = status_bounds.as_mut().get_mut(&rid)?;
            m.committed.update_bound();
            Some(m.committed.bound())
        });

        if let Some(lv) = committed_up_to {
            let wm = self.watermark(rid);
            wm.bump_level(lv)
        }
    }

    pub async fn wait_node(&self, id: InstanceId) -> Option<Asc<InsNode<C>>> {
        if self.is_executed(id) {
            return None;
        }

        let n = match self.nodes.entry(id) {
            dashmap::mapref::entry::Entry::Occupied(e) => match e.get() {
                Node::InGraph(node) => return Some(Asc::clone(node)),
                Node::Waiting(notify) => Asc::clone(notify),
            },
            dashmap::mapref::entry::Entry::Vacant(e) => {
                let notify = Asc::new(Notify::new());
                e.insert(Node::Waiting(Asc::clone(&notify)));
                notify
            }
        };

        loop {
            n.notified().await;
            if self.is_executed(id) {
                return None;
            }
            if let Some(Node::InGraph(node)) = self.nodes.get(&id).as_deref() {
                return Some(Asc::clone(node));
            }
        }
    }

    fn is_executed(&self, id: InstanceId) -> bool {
        let guard = self.status_bounds.lock();
        let status_bounds = &*guard;
        let InstanceId(rid, lid) = id;
        if let Some(m) = status_bounds.as_ref().get(&rid) {
            if m.executed.is_set(lid.raw_value()) {
                return true;
            }
        }
        false
    }

    pub fn retire_node(&self, id: InstanceId) {
        if let Some((_, Node::Waiting(n))) = self.nodes.remove(&id) {
            n.notify_waiters()
        }
    }

    #[must_use]
    pub fn watermark(&self, rid: ReplicaId) -> Asc<WaterMark> {
        let bound = with_mutex(&self.status_bounds, |status_bounds: _| {
            status_bounds.as_ref().get(&rid).map(|m: _| m.committed.bound())
        });
        let gen = || Asc::new(WaterMark::new(bound.unwrap_or(0)));
        self.watermarks.entry(rid).or_insert_with(gen).clone()
    }
}

pub struct DepsQueue {
    map: VecMap<ReplicaId, LocalInstanceId>,
    rows: VecSet<ReplicaId>,
}

impl DepsQueue {
    #[must_use]
    pub fn new(InstanceId(rid, lid): InstanceId) -> Self {
        Self {
            map: VecMap::from_single(rid, lid),
            rows: VecSet::from_single(rid),
        }
    }

    pub fn push(&mut self, InstanceId(rid, lid): InstanceId) {
        self.map.update(
            rid,
            |v: _| max_assign(v, lid),
            || {
                let _ = self.rows.insert(rid);
                lid
            },
        );
    }

    #[must_use]
    pub fn pop(&mut self) -> Option<InstanceId> {
        let row = self.rows.as_slice().choose(&mut rand::thread_rng()).copied()?;
        let _ = self.rows.remove(&row);
        self.map.remove(&row).map(|lid| InstanceId(row, lid))
    }
}

pub trait GraphId: Copy + Eq + Hash + fmt::Debug {}

pub trait GraphNode: Clone + fmt::Debug {
    type Id: GraphId;
    fn deps_for_each(&self, f: impl FnMut(Self::Id));
}

#[derive(Debug)]
pub struct LocalGraph<I, N> {
    nodes: FnvHashMap<I, N>,
}

impl<I, N> LocalGraph<I, N> {
    #[must_use]
    pub fn new() -> Self {
        Self { nodes: FnvHashMap::default() }
    }
}

impl<I, N> LocalGraph<I, N>
where
    I: GraphId,
{
    #[must_use]
    pub fn nodes_count(&self) -> usize {
        self.nodes.len()
    }

    // #[must_use]
    // pub fn contains_node(&self, id: I) -> bool {
    //     self.nodes.contains_key(&id)
    // }

    pub fn add_node(&mut self, id: I, node: N) {
        self.nodes.entry(id).or_insert(node);
    }

    pub fn get_node(&self, id: I) -> Option<&N> {
        self.nodes.get(&id)
    }

    #[must_use]
    pub fn tarjan_scc(&self, root: I) -> Vec<Vec<(I, N)>>
    where
        N: GraphNode<Id = I>,
    {
        let mut tarjan = TarjanScc::new(&self.nodes);
        tarjan.run(root);
        tarjan.ans
    }
}

impl<I, N> Default for LocalGraph<I, N> {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphId for InstanceId {}

impl<C> GraphNode for Asc<InsNode<C>> {
    type Id = InstanceId;
    fn deps_for_each(&self, mut f: impl FnMut(Self::Id)) {
        for id in self.deps.elements() {
            f(id)
        }
    }
}

struct TarjanScc<'a, I, N> {
    nodes: &'a FnvHashMap<I, N>,
    cnt: usize,
    attr: FnvHashMap<I, TarjanSccAttr>,
    stack: Stack<I>,
    ans: Vec<Vec<(I, N)>>,
}

#[derive(Debug)]
struct Stack<I> {
    stk: Vec<I>,
    set: FnvHashSet<I>,
}

impl<I> Stack<I>
where
    I: Copy + Eq + Hash,
{
    fn new() -> Self {
        Self { stk: Vec::new(), set: FnvHashSet::default() }
    }
    fn push(&mut self, val: I) {
        self.stk.push(val);
        self.set.insert(val);
    }
    fn pop(&mut self) -> Option<I> {
        let val = self.stk.pop();
        if let Some(ref val) = val {
            self.set.remove(val);
        }
        val
    }
    fn contains(&mut self, val: I) -> bool {
        self.set.contains(&val)
    }
}

#[derive(Debug)]
struct TarjanSccAttr {
    dfn: usize,
    low: usize,
}

impl<'a, I, N> TarjanScc<'a, I, N>
where
    I: GraphId,
    N: GraphNode<Id = I>,
{
    fn new(nodes: &'a FnvHashMap<I, N>) -> Self {
        Self {
            nodes,
            cnt: 0,
            attr: FnvHashMap::default(),
            stack: Stack::new(),
            ans: Vec::new(),
        }
    }

    /// <https://oi-wiki.org/graph/scc/#tarjan_1>
    fn run(&mut self, u: I) {
        let node = self
            .nodes
            .get(&u)
            .unwrap_or_else(|| panic!("cannot find node {:?} in local graph {:?}", u, self.nodes));

        let idx = self.cnt.wrapping_add(1);
        self.cnt = idx;

        let _ = self.attr.insert(u, TarjanSccAttr { dfn: idx, low: idx });

        self.stack.push(u);

        node.deps_for_each(|v| {
            if self.nodes.contains_key(&v).not() {
                return; // the node is omitted
            }

            if self.attr.contains_key(&v).not() {
                self.run(v);
                let low_v: _ = self.attr[&v].low;
                let low_u: _ = &mut self.attr.get_mut(&u).unwrap().low;
                min_assign(low_u, low_v);
            } else if self.stack.contains(v) {
                let dfn_v: _ = self.attr[&v].dfn;
                let low_u: _ = &mut self.attr.get_mut(&u).unwrap().low;
                min_assign(low_u, dfn_v);
            } else {
                // other
            }
        });

        let u_attr = &self.attr[&u];
        if u_attr.dfn == u_attr.low {
            let mut scc = Vec::new();
            while let Some(x) = self.stack.pop() {
                let node = self.nodes[&x].clone();
                scc.push((x, node));
                if x == u {
                    break;
                }
            }
            assert!(scc.is_empty().not());
            self.ans.push(scc);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use epkv_utils::iter::map_collect;
    use epkv_utils::vecset::VecSet;

    use std::fmt;

    #[derive(Debug, Clone)]
    struct TestNode {
        deps: VecSet<u64>,
    }

    impl GraphId for u64 {}

    impl GraphNode for TestNode {
        type Id = u64;
        fn deps_for_each(&self, mut f: impl FnMut(Self::Id)) {
            for &id in &self.deps {
                f(id)
            }
        }
    }

    fn build_graph(edges: &[(u64, u64)]) -> LocalGraph<u64, TestNode> {
        let mut nodes: FnvHashMap<u64, TestNode> = FnvHashMap::default();

        for &(u, v) in edges {
            let gen: _ = || TestNode { deps: VecSet::new() };

            nodes.entry(v).or_insert_with(gen);
            let u: _ = nodes.entry(u).or_insert_with(gen);

            let is_new_edge = u.deps.insert(v).is_none();
            assert!(is_new_edge, "duplicate edge");
        }

        LocalGraph { nodes }
    }

    fn assert_scc<I, N>(ans: &[Vec<(I, N)>], expected: &[&[I]])
    where
        I: Copy + Eq + Ord + fmt::Debug,
    {
        let mut ans: Vec<Vec<I>> = map_collect(ans, |scc| map_collect(scc, |&(id, _)| id));
        assert_eq!(ans.len(), expected.len());
        for (a, e) in ans.iter_mut().zip(expected.iter().copied()) {
            a.sort();
            assert_eq!(a, e);
        }
    }

    #[test]
    fn cycle_2() {
        let graph: _ = build_graph(&[
            (1, 2), //
            (2, 1), //
        ]);

        let ans: _ = graph.tarjan_scc(2);
        assert_scc(&ans, &[&[1, 2]]);
    }

    #[test]
    fn cycle_3() {
        let graph: _ = build_graph(&[
            (1, 2), //
            (2, 3), //
            (3, 1), //
        ]);

        let ans: _ = graph.tarjan_scc(3);
        assert_scc(&ans, &[&[1, 2, 3]]);
    }

    #[test]
    fn linear() {
        let graph: _ = build_graph(&[
            (4, 3), //
            (3, 2), //
            (2, 1), //
        ]);

        let ans: _ = graph.tarjan_scc(4);
        assert_scc(&ans, &[&[1], &[2], &[3], &[4]]);
    }

    #[test]
    fn linear_and_cycle() {
        let graph: _ = build_graph(&[
            (702, 701), //
            (703, 702), //
            (802, 801), //
            (803, 802), //
            (701, 801), //
            (702, 802), //
            (703, 803), //
            (801, 702), //
            (802, 703), //
            (704, 703), //
            (804, 704), //
            (704, 804), //
            (804, 803), //
            (805, 804), //
        ]);

        let ans: _ = graph.tarjan_scc(805);
        assert_scc(&ans, &[&[701, 702, 703, 801, 802, 803], &[704, 804], &[805]]);
    }

    #[test]
    fn omitted_node() {
        let mut graph = build_graph(&[
            (202, 201), //
            (301, 202), //
        ]);
        graph.nodes.remove(&201);

        let ans: _ = graph.tarjan_scc(301);
        assert_scc(&ans, &[&[202], &[301]]);
    }
}
