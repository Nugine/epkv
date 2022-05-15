use crate::id::ReplicaId;
use crate::msg::{Accept, Commit, Message, PreAccept};

use std::net::SocketAddr;
use std::ops::Not;

use ordered_vecmap::VecSet;

pub trait Network<C>: Send + Sync + 'static {
    fn broadcast(&self, targets: VecSet<ReplicaId>, msg: Message<C>);
    fn send_one(&self, target: ReplicaId, msg: Message<C>);
    fn join(&self, rid: ReplicaId, addr: SocketAddr) -> Option<ReplicaId>;
    fn leave(&self, rid: ReplicaId);
}

pub fn broadcast_preaccept<C>(
    network: &impl Network<C>,
    acc: VecSet<ReplicaId>,
    others: VecSet<ReplicaId>,
    msg: PreAccept<C>,
) {
    if acc.is_empty().not() {
        network.broadcast(
            acc,
            Message::PreAccept(PreAccept {
                sender: msg.sender,
                epoch: msg.epoch,
                id: msg.id,
                pbal: msg.pbal,
                cmd: None,
                seq: msg.seq,
                deps: msg.deps.clone(),
                acc: msg.acc.clone(),
            }),
        );
    }
    if others.is_empty().not() {
        assert!(msg.cmd.is_some());
        network.broadcast(others, Message::PreAccept(msg));
    }
}

pub fn broadcast_accept<C>(
    network: &impl Network<C>,
    acc: VecSet<ReplicaId>,
    others: VecSet<ReplicaId>,
    msg: Accept<C>,
) {
    if acc.is_empty().not() {
        network.broadcast(
            acc,
            Message::Accept(Accept {
                sender: msg.sender,
                epoch: msg.epoch,
                id: msg.id,
                pbal: msg.pbal,
                cmd: None,
                seq: msg.seq,
                deps: msg.deps.clone(),
                acc: msg.acc.clone(),
            }),
        );
    }
    if others.is_empty().not() {
        assert!(msg.cmd.is_some());
        network.broadcast(others, Message::Accept(msg));
    }
}

pub fn broadcast_commit<C>(
    network: &impl Network<C>,
    acc: VecSet<ReplicaId>,
    others: VecSet<ReplicaId>,
    msg: Commit<C>,
) {
    if acc.is_empty().not() {
        network.broadcast(
            acc,
            Message::Commit(Commit {
                sender: msg.sender,
                epoch: msg.epoch,
                id: msg.id,
                pbal: msg.pbal,
                cmd: None,
                seq: msg.seq,
                deps: msg.deps.clone(),
                acc: msg.acc.clone(),
            }),
        );
    }
    if others.is_empty().not() {
        assert!(msg.cmd.is_some());
        network.broadcast(others, Message::Commit(msg));
    }
}
