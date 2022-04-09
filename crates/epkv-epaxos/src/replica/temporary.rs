use crate::types::{Deps, ReplicaId, Seq};

use epkv_utils::vecset::VecSet;

pub enum Temporary {
    PreAccepting(PreAccepting),
    Accepting(Accepting),
    Preparing(Preparing),
}

pub struct PreAccepting {
    pub received: VecSet<ReplicaId>,
    pub seq: Seq,
    pub deps: Deps,
    pub all_same: bool,
    pub acc: VecSet<ReplicaId>,
}

pub struct Accepting {}
pub struct Preparing {}
