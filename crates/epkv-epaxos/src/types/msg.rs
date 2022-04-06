use super::acc::Acc;
use super::deps::Deps;
use super::id::{Ballot, InstanceId, ReplicaId, Seq};
use super::ins::InstanceStatus;
use super::Epoch;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct PreAccept<C> {
    pub sender: ReplicaId,
    pub id: InstanceId,
    pub pbal: Ballot,
    pub cmd: Option<C>,
    pub seq: Seq,
    pub deps: Deps,
    pub acc: Acc,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PreAcceptOk {
    pub sender: ReplicaId,
    pub id: InstanceId,
    pub pbal: Ballot,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PreAcceptDiff {
    pub sender: ReplicaId,
    pub id: InstanceId,
    pub pbal: Ballot,
    pub seq: Seq,
    pub deps: Deps,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Accept<C> {
    pub sender: ReplicaId,
    pub id: InstanceId,
    pub pbal: Ballot,
    pub cmd: Option<C>,
    pub seq: Seq,
    pub deps: Deps,
    pub acc: Acc,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AcceptOk {
    pub sender: ReplicaId,
    pub id: InstanceId,
    pub pbal: Ballot,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Commit<C> {
    pub sender: ReplicaId,
    pub id: InstanceId,
    pub pbal: Ballot,
    pub cmd: Option<C>,
    pub seq: Seq,
    pub deps: Deps,
    pub acc: Acc,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Prepare {
    pub sender: ReplicaId,
    pub id: InstanceId,
    pub pbal: Ballot,
    pub known: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PrepareNack {
    pub sender: ReplicaId,
    pub id: InstanceId,
    pub pbal: Ballot,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PrepareUnchosen {
    pub sender: ReplicaId,
    pub id: InstanceId,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PrepareOk<C> {
    pub sender: ReplicaId,
    pub id: InstanceId,
    pub pbal: Ballot,
    pub cmd: Option<C>,
    pub seq: Seq,
    pub deps: Deps,
    pub abal: Ballot,
    pub status: InstanceStatus,
    pub acc: Acc,
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
    Join(Join),
    JoinOk(JoinOk),
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::codec;
    use crate::types::{Epoch, LocalInstanceId, Round};

    use std::mem;

    #[test]
    fn message_size() {
        {
            let baseline_type_size = mem::size_of::<Message<()>>();
            assert_eq!(baseline_type_size, 144); // track message type size
        }

        {
            let rid = ReplicaId::from(101);
            let lid = LocalInstanceId::from(1024);
            let id = InstanceId(rid, lid);
            let pbal = Ballot(Epoch::from(1), Round::ZERO, rid);
            let cmd = None;
            let seq = Seq::from(1);
            let deps = Deps::with_capacity(3);
            let mut acc = Acc::with_capacity(3);
            acc.insert(rid);

            let pre_accept = Message::<()>::PreAccept(PreAccept {
                sender: rid,
                id,
                pbal,
                cmd,
                seq,
                deps,
                acc,
            });
            let msg_size = codec::serialized_size(&pre_accept).unwrap();
            assert_eq!(msg_size, 14); // track message size
        }
    }
}
