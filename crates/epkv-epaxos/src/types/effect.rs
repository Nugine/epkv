use super::*;

use epkv_utils::vecset::VecSet;

use std::ops::Not;

pub struct Effect<C: CommandLike> {
    pub broadcasts: Vec<Broadcast<C>>,
    pub replies: Vec<Reply<C>>,
    pub notifies: Vec<C::Notify>,
}

pub struct Broadcast<C> {
    pub targets: VecSet<ReplicaId>,
    pub msg: Message<C>,
}

pub struct Reply<C> {
    pub target: ReplicaId,
    pub msg: Message<C>,
}

impl<C: CommandLike> Effect<C> {
    #[must_use]
    pub fn empty() -> Self {
        Effect {
            broadcasts: Vec::new(),
            replies: Vec::new(),
            notifies: Vec::new(),
        }
    }

    #[must_use]
    pub fn broadcast(targets: VecSet<ReplicaId>, msg: Message<C>) -> Self {
        Effect {
            broadcasts: vec![Broadcast { targets, msg }],
            ..Self::empty()
        }
    }

    #[must_use]
    pub fn reply(target: ReplicaId, msg: Message<C>) -> Self {
        Effect {
            replies: vec![Reply { target, msg }],
            ..Self::empty()
        }
    }

    #[must_use]
    pub fn broadcast_pre_accept(
        acc: VecSet<ReplicaId>,
        others: VecSet<ReplicaId>,
        msg: PreAccept<C>,
    ) -> Self {
        assert!(msg.cmd.is_some());
        let mut broadcasts = Vec::with_capacity(2);
        broadcasts.push(Broadcast {
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
            broadcasts.push(Broadcast {
                targets: others,
                msg: Message::PreAccept(msg),
            });
        }
        Effect {
            broadcasts,
            ..Self::empty()
        }
    }
}
