use crate::types::{Ballot, Deps, ReplicaId, Seq, Status};

use epkv_utils::vecset::VecSet;

pub enum Temporary<C> {
    PreAccepting(PreAccepting),
    Accepting(Accepting),
    Preparing(Preparing<C>),
}

pub struct PreAccepting {
    pub received: VecSet<ReplicaId>,
    pub seq: Seq,
    pub deps: Deps,
    pub all_same: bool,
    pub acc: VecSet<ReplicaId>,
}

pub struct Accepting {
    pub received: VecSet<ReplicaId>,
    pub acc: VecSet<ReplicaId>,
}

pub struct Preparing<C> {
    pub received: VecSet<ReplicaId>,
    pub max_abal: Option<Ballot>,
    pub cmd: Option<C>,
    /// (sender, seq, deps, status, acc)
    pub tuples: Vec<(ReplicaId, Seq, Deps, Status, VecSet<ReplicaId>)>,
}
