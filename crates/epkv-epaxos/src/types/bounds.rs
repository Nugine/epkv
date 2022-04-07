use super::{LocalInstanceId, ReplicaId, Seq};

use epkv_utils::vecmap::VecMap;

pub struct AttrBounds {
    pub max_seq: Seq,
    pub max_lids: VecMap<ReplicaId, LocalInstanceId>,
}
