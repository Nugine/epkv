use crate::acc::Acc;
use crate::deps::Deps;
use crate::id::*;
use crate::ins::Instance;
use crate::status::Status;

use epkv_utils::time::LocalInstant;
use epkv_utils::vecmap::VecMap;

use std::net::SocketAddr;

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
    pub acc: Acc,
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
    pub acc: Acc,
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
    pub acc: Acc,
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
    pub acc: Acc,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Join {
    pub sender: ReplicaId,
    pub epoch: Epoch,
    pub addr: SocketAddr,
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
pub struct ProbeRtt {
    pub sender: ReplicaId,
    pub time: LocalInstant,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProbeRttOk {
    pub sender: ReplicaId,
    pub time: LocalInstant,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AskLog {
    pub sender: ReplicaId,
    pub addr: SocketAddr,
    pub known_up_to: VecMap<ReplicaId, LocalInstanceId>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SyncLog<C> {
    pub sender: ReplicaId,
    pub sync_id: SyncId,
    pub instances: Vec<(InstanceId, Instance<C>)>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SyncLogOk {
    pub sender: ReplicaId,
    pub sync_id: SyncId,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PeerBounds {
    pub sender: ReplicaId,
    pub committed_up_to: Option<VecMap<ReplicaId, LocalInstanceId>>,
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
    ProbeRtt(ProbeRtt),
    ProbeRttOk(ProbeRttOk),
    AskLog(AskLog),
    SyncLog(SyncLog<C>),
    SyncLogOk(SyncLogOk),
    PeerBounds(PeerBounds),
}

pub enum PreAcceptReply {
    Ok(PreAcceptOk),
    Diff(PreAcceptDiff),
}

pub enum AcceptReply {
    Ok(AcceptOk),
}

pub enum PrepareReply<C> {
    Ok(PrepareOk<C>),
    Nack(PrepareNack),
    Unchosen(PrepareUnchosen),
}

impl PreAcceptReply {
    pub fn convert<C>(msg: Message<C>) -> Option<Self> {
        match msg {
            Message::PreAcceptOk(msg) => Some(Self::Ok(msg)),
            Message::PreAcceptDiff(msg) => Some(Self::Diff(msg)),
            _ => None,
        }
    }
}

impl AcceptReply {
    pub fn convert<C>(msg: Message<C>) -> Option<Self> {
        match msg {
            Message::AcceptOk(msg) => Some(Self::Ok(msg)),
            _ => None,
        }
    }
}

impl<C> PrepareReply<C> {
    pub fn convert(msg: Message<C>) -> Option<Self> {
        match msg {
            Message::PrepareOk(msg) => Some(Self::Ok(msg)),
            Message::PrepareNack(msg) => Some(Self::Nack(msg)),
            Message::PrepareUnchosen(msg) => Some(Self::Unchosen(msg)),
            _ => None,
        }
    }
}

impl<C> Message<C> {
    pub fn variant_name(&self) -> &'static str {
        match self {
            Message::PreAccept(_) => "PreAccept",
            Message::PreAcceptOk(_) => "PreAcceptOk",
            Message::PreAcceptDiff(_) => "PreAcceptDiff",
            Message::Accept(_) => "Accept",
            Message::AcceptOk(_) => "AcceptOk",
            Message::Commit(_) => "Commit",
            Message::Prepare(_) => "Prepare",
            Message::PrepareOk(_) => "PrepareOk",
            Message::PrepareNack(_) => "PrepareNack",
            Message::PrepareUnchosen(_) => "PrepareUnchosen",
            Message::Join(_) => "Join",
            Message::JoinOk(_) => "JoinOk",
            Message::Leave(_) => "Leave",
            Message::ProbeRtt(_) => "ProbeRtt",
            Message::ProbeRttOk(_) => "ProbeRttOk",
            Message::AskLog(_) => "AskLog",
            Message::SyncLog(_) => "SyncLog",
            Message::SyncLogOk(_) => "SyncLogOk",
            Message::PeerBounds(_) => "PeerBounds",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::deps::MutableDeps;
    use crate::id::{Epoch, LocalInstanceId, Round};

    use epkv_utils::codec;

    use std::mem;

    #[test]
    fn message_size() {
        {
            let baseline_type_size = mem::size_of::<Message<()>>();
            assert_eq!(baseline_type_size, 104); // track message type size
        }

        {
            let epoch = Epoch::ONE;
            let rid = ReplicaId::from(101);
            let lid = LocalInstanceId::from(1024);
            let id = InstanceId(rid, lid);
            let pbal = Ballot(Round::ZERO, rid);
            let cmd = None;
            let seq = Seq::from(1);
            let deps = Deps::from_mutable(MutableDeps::with_capacity(3));
            let acc = Acc::from_iter([rid]);

            let preaccept =
                Message::<()>::PreAccept(PreAccept { sender: rid, epoch, id, pbal, cmd, seq, deps, acc });
            let msg_size = codec::serialized_size(&preaccept).unwrap();
            assert_eq!(msg_size, 14); // track message size
        }
    }
}
