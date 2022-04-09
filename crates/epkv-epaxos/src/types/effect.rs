use super::*;

use epkv_utils::vecset::VecSet;

use std::ops::Not;
use std::time::Duration;

pub struct Effect<C: CommandLike> {
    pub broadcasts: Vec<Broadcast<C>>,
    pub replies: Vec<Reply<C>>,
    pub notifies: Vec<C::Notify>,
    pub timeouts: Vec<Timeout>,
}

pub struct Broadcast<C> {
    pub targets: VecSet<ReplicaId>,
    pub msg: Message<C>,
}

pub struct Reply<C> {
    pub target: ReplicaId,
    pub msg: Message<C>,
}

pub struct Timeout {
    pub duration: Duration,
    pub kind: TimeoutKind,
}

pub enum TimeoutKind {
    PreAcceptFastPath { id: InstanceId },
}

impl<C: CommandLike> Default for Effect<C> {
    fn default() -> Self {
        Self::new()
    }
}

impl<C: CommandLike> Effect<C> {
    #[must_use]
    pub const fn new() -> Self {
        Effect {
            broadcasts: Vec::new(),
            replies: Vec::new(),
            notifies: Vec::new(),
            timeouts: Vec::new(),
        }
    }

    pub fn broadcast(&mut self, targets: VecSet<ReplicaId>, msg: Message<C>) {
        self.broadcasts.push(Broadcast { targets, msg })
    }

    pub fn reply(&mut self, target: ReplicaId, msg: Message<C>) {
        self.replies.push(Reply { target, msg })
    }

    pub fn timeout(&mut self, duration: Duration, kind: TimeoutKind) {
        self.timeouts.push(Timeout { duration, kind })
    }

    pub fn broadcast_preaccept(
        &mut self,
        acc: VecSet<ReplicaId>,
        others: VecSet<ReplicaId>,
        msg: PreAccept<C>,
    ) {
        assert!(msg.cmd.is_some());
        self.broadcasts.reserve(2);
        self.broadcasts.push(Broadcast {
            targets: acc,
            msg: Message::PreAccept(PreAccept {
                sender: msg.sender,
                epoch: msg.epoch,
                id: msg.id,
                pbal: msg.pbal,
                cmd: None,
                seq: msg.seq,
                deps: msg.deps.clone(),
                acc: msg.acc.clone(),
            }),
        });
        if others.is_empty().not() {
            self.broadcasts
                .push(Broadcast { targets: others, msg: Message::PreAccept(msg) });
        }
    }
}
