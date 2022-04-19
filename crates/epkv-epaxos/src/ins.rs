use crate::deps::MutableDeps;
use crate::id::{Ballot, ReplicaId, Seq};
use crate::status::Status;

use epkv_utils::vecset::VecSet;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instance<C> {
    pub pbal: Ballot,
    pub cmd: C,
    pub seq: Seq,
    pub deps: MutableDeps,
    pub abal: Ballot,
    pub status: Status,
    pub acc: VecSet<ReplicaId>,
}
