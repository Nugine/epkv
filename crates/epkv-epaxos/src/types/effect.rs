use super::*;

use epkv_utils::vecset::VecSet;

use std::net::SocketAddr;
use std::ops::Not;
use std::time::Duration;

pub trait Effect<C: CommandLike>: Send + Sync + 'static {
    fn broadcast(&self, targets: VecSet<ReplicaId>, msg: Message<C>);
    fn send(&self, target: ReplicaId, msg: Message<C>);
    fn cmd_notify(&self, notify: C::Notify);
    fn set_timeout(&self, duration: Duration, event: TimeoutEvent);
    fn join_finished(&self);
    fn sync_finished(&self);
    fn start_execution(&self, id: InstanceId, cmd: C, seq: Seq, deps: Deps);
    fn register_peer(&self, rid: ReplicaId, address: SocketAddr);
    fn ins_issued(&self, id: InstanceId);
    fn ins_executed(&self, id: InstanceId);
}

pub enum TimeoutEvent {
    PreAccept { id: InstanceId },
    Recover { id: InstanceId },
}

pub fn broadcast_preaccept<C: CommandLike>(
    effect: &impl Effect<C>,
    acc: VecSet<ReplicaId>,
    others: VecSet<ReplicaId>,
    msg: PreAccept<C>,
) {
    effect.broadcast(
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
        effect.broadcast(others, Message::PreAccept(msg));
    }
}

pub fn broadcast_accept<C: CommandLike>(
    effect: &impl Effect<C>,
    acc: VecSet<ReplicaId>,
    others: VecSet<ReplicaId>,
    msg: Accept<C>,
) {
    effect.broadcast(
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
        effect.broadcast(others, Message::Accept(msg));
    }
}

pub fn broadcast_commit<C: CommandLike>(
    effect: &impl Effect<C>,
    acc: VecSet<ReplicaId>,
    others: VecSet<ReplicaId>,
    msg: Commit<C>,
) {
    effect.broadcast(
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
        effect.broadcast(others, Message::Commit(msg));
    }
}
