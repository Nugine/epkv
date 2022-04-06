use super::CommandLike;

use crate::types::{Message, ReplicaId};

pub struct Effect<C: CommandLike> {
    pub broadcasts: Vec<Broadcast<C>>,
    pub replies: Vec<Reply<C>>,
    pub notifies: Vec<C::Notify>,
}

pub struct Broadcast<C> {
    pub targets: Vec<ReplicaId>,
    pub msg: Message<C>,
}

pub struct Reply<C> {
    pub target: ReplicaId,
    pub reply: Message<C>,
}

impl<C: CommandLike> Effect<C> {
    pub fn broadcast(targets: Vec<ReplicaId>, msg: Message<C>) -> Self {
        Effect {
            broadcasts: vec![Broadcast { targets, msg }],
            replies: Vec::new(),
            notifies: Vec::new(),
        }
    }
}
