use crate::acc::Acc;
use crate::deps::Deps;
use crate::id::{Ballot, Seq};
use crate::status::Status;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instance<C> {
    pub pbal: Ballot,
    pub cmd: C,
    pub seq: Seq,
    pub deps: Deps,
    pub abal: Ballot,
    pub status: Status,
    pub acc: Acc,
}
