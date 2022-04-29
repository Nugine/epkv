use epkv_epaxos::id::{Epoch, ReplicaId};
use epkv_utils::vecmap::VecMap;

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcArgs {
    Register(RegisterArgs),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RpcOutput {
    Register(RegisterOutput),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterArgs {
    pub addr: SocketAddr,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterOutput {
    pub rid: ReplicaId,
    pub epoch: Epoch,
    pub peers: VecMap<ReplicaId, SocketAddr>,
}
