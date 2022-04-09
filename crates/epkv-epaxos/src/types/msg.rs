use super::deps::Deps;
use super::id::{Ballot, InstanceId, ReplicaId, Seq};
use super::ins::Status;
use super::Epoch;

use epkv_utils::vecset::VecSet;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreAccept<C> {
    pub sender: ReplicaId,
    pub epoch: Epoch,
    pub id: InstanceId,
    pub pbal: Ballot,
    pub cmd: Option<C>,
    pub seq: Seq,
    pub deps: Deps,
    pub acc: VecSet<ReplicaId>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PreAcceptOk {
    pub sender: ReplicaId,
    pub epoch: Epoch,
    pub id: InstanceId,
    pub pbal: Ballot,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PreAcceptDiff {
    pub sender: ReplicaId,
    pub epoch: Epoch,
    pub id: InstanceId,
    pub pbal: Ballot,
    pub seq: Seq,
    pub deps: Deps,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Accept<C> {
    pub sender: ReplicaId,
    pub epoch: Epoch,
    pub id: InstanceId,
    pub pbal: Ballot,
    pub cmd: Option<C>,
    pub seq: Seq,
    pub deps: Deps,
    pub acc: VecSet<ReplicaId>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AcceptOk {
    pub sender: ReplicaId,
    pub epoch: Epoch,
    pub id: InstanceId,
    pub pbal: Ballot,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Commit<C> {
    pub sender: ReplicaId,
    pub epoch: Epoch,
    pub id: InstanceId,
    pub pbal: Ballot,
    pub cmd: Option<C>,
    pub seq: Seq,
    pub deps: Deps,
    pub acc: VecSet<ReplicaId>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Prepare {
    pub sender: ReplicaId,
    pub epoch: Epoch,
    pub id: InstanceId,
    pub pbal: Ballot,
    pub known: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PrepareNack {
    pub sender: ReplicaId,
    pub epoch: Epoch,
    pub id: InstanceId,
    pub pbal: Ballot,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PrepareUnchosen {
    pub sender: ReplicaId,
    pub epoch: Epoch,
    pub id: InstanceId,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PrepareOk<C> {
    pub sender: ReplicaId,
    pub epoch: Epoch,
    pub id: InstanceId,
    pub pbal: Ballot,
    pub cmd: Option<C>,
    pub seq: Seq,
    pub deps: Deps,
    pub abal: Ballot,
    pub status: Status,
    pub acc: VecSet<ReplicaId>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Join {
    pub sender: ReplicaId,
    pub epoch: Epoch,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JoinOk {
    pub sender: ReplicaId,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Leave {
    pub sender: ReplicaId,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Message<C> {
    PreAccept(PreAccept<C>),
    PreAcceptOk(PreAcceptOk),
    PreAcceptDiff(PreAcceptDiff),
    Accept(Accept<C>),
    AcceptOk(AcceptOk),
    Commit(Commit<C>),
    Prepare(Prepare),
    PrepareOk(PrepareOk<C>),
    PrepareNack(PrepareNack),
    PrepareUnchosen(PrepareUnchosen),
    Join(Join),
    JoinOk(JoinOk),
    Leave(Leave),
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::types::{Epoch, LocalInstanceId, Round};

    use epkv_utils::codec;

    use std::mem;

    #[test]
    fn message_size() {
        {
            let baseline_type_size = mem::size_of::<Message<()>>();
            assert_eq!(baseline_type_size, 136); // track message type size
        }

        {
            let epoch = Epoch::ONE;
            let rid = ReplicaId::from(101);
            let lid = LocalInstanceId::from(1024);
            let id = InstanceId(rid, lid);
            let pbal = Ballot(Round::ZERO, rid);
            let cmd = None;
            let seq = Seq::from(1);
            let deps = Deps::with_capacity(3);
            let mut acc = VecSet::with_capacity(3);
            let _ = acc.insert(rid);

            let preaccept = Message::<()>::PreAccept(PreAccept {
                sender: rid,
                epoch,
                id,
                pbal,
                cmd,
                seq,
                deps,
                acc,
            });
            let msg_size = codec::serialized_size(&preaccept).unwrap();
            assert_eq!(msg_size, 14); // track message size
        }
    }
}
