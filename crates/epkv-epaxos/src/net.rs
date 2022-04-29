use crate::id::ReplicaId;
use crate::msg::{Accept, Commit, Message, PreAccept};

use epkv_utils::vecset::VecSet;

use std::net::SocketAddr;
use std::ops::Not;

pub trait Network<C>: Send + Sync + 'static {
    fn broadcast(&self, targets: VecSet<ReplicaId>, msg: Message<C>);
    fn send_one(&self, target: ReplicaId, msg: Message<C>);
    fn register_peer(&self, rid: ReplicaId, addr: SocketAddr);
}

pub fn broadcast_preaccept<C>(
    net: &impl Network<C>,
    acc: VecSet<ReplicaId>,
    others: VecSet<ReplicaId>,
    msg: PreAccept<C>,
) {
    net.broadcast(
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
    if others.is_empty().not() {
        assert!(msg.cmd.is_some());
        net.broadcast(others, Message::PreAccept(msg));
    }
}

pub fn broadcast_accept<C>(
    net: &impl Network<C>,
    acc: VecSet<ReplicaId>,
    others: VecSet<ReplicaId>,
    msg: Accept<C>,
) {
    net.broadcast(
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
    if others.is_empty().not() {
        assert!(msg.cmd.is_some());
        net.broadcast(others, Message::Accept(msg));
    }
}

pub fn broadcast_commit<C>(
    net: &impl Network<C>,
    acc: VecSet<ReplicaId>,
    others: VecSet<ReplicaId>,
    msg: Commit<C>,
) {
    net.broadcast(
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
    if others.is_empty().not() {
        assert!(msg.cmd.is_some());
        net.broadcast(others, Message::Commit(msg));
    }
}
