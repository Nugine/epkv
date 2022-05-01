//! server calls monitor

use crate::rpc::{RpcClientConfig, RpcConnection};

use epkv_epaxos::id::{Epoch, ReplicaId};
use epkv_utils::vecmap::VecMap;

use std::net::SocketAddr;

use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
// #[non_exhaustive]
pub enum Args {
    Register(RegisterArgs),
}

#[derive(Debug, Serialize, Deserialize)]
// #[non_exhaustive]
pub enum Output {
    Register(RegisterOutput),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterArgs {
    pub public_peer_addr: SocketAddr,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterOutput {
    pub rid: ReplicaId,
    pub epoch: Epoch,
    pub peers: VecMap<ReplicaId, SocketAddr>,
    pub prev_rid: Option<ReplicaId>,
}

pub struct Monitor {
    conn: RpcConnection<Args, Output>,
}

impl Monitor {
    pub async fn connect(remote_addr: SocketAddr, config: &RpcClientConfig) -> Result<Self> {
        let conn: _ = RpcConnection::connect(remote_addr, config).await?;
        Ok(Self { conn })
    }

    pub async fn register(&self, args: RegisterArgs) -> Result<RegisterOutput> {
        let output = self.conn.call(Args::Register(args)).await?;
        match output {
            Output::Register(ans) => Ok(ans),
            // _ => Err(anyhow!("unexpected rpc output type")),
        }
    }
}
