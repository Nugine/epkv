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

#[cfg(test)]
mod tests {
    use super::*;

    use std::mem;

    #[test]
    fn instance_size() {
        let baseline_type_size = mem::size_of::<Instance<()>>();
        assert_eq!(baseline_type_size, 64); // track instance size
    }
}
