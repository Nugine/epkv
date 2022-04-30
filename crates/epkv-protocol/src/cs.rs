//! client calls server

use crate::rpc::{RpcClientConfig, RpcConnection};

use std::net::SocketAddr;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Args {
    Get(GetArgs),
    Set(SetArgs),
    Del(DelArgs),
}

#[derive(Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Output {
    Get(GetOutput),
    Set(SetOutput),
    Del(DelOutput),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetArgs {
    pub key: Bytes,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GetOutput {
    pub value: Option<Bytes>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SetArgs {
    pub key: Bytes,
    pub value: Bytes,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SetOutput {}

#[derive(Debug, Serialize, Deserialize)]
pub struct DelArgs {
    pub key: Bytes,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DelOutput {}

pub struct Server {
    conn: RpcConnection<Args, Output>,
}

impl Server {
    pub async fn connect(remote_addr: SocketAddr, config: &RpcClientConfig) -> Result<Self> {
        let conn: _ = RpcConnection::connect(remote_addr, config).await?;
        Ok(Self { conn })
    }

    pub async fn get(&self, args: GetArgs) -> Result<GetOutput> {
        let output = self.conn.call(Args::Get(args)).await?;
        match output {
            Output::Get(output) => Ok(output),
            _ => Err(anyhow!("unexpected rpc output type")),
        }
    }

    pub async fn set(&self, args: SetArgs) -> Result<SetOutput> {
        let output = self.conn.call(Args::Set(args)).await?;
        match output {
            Output::Set(output) => Ok(output),
            _ => Err(anyhow!("unexpected rpc output type")),
        }
    }

    pub async fn del(&self, args: DelArgs) -> Result<DelOutput> {
        let output = self.conn.call(Args::Del(args)).await?;
        match output {
            Output::Del(output) => Ok(output),
            _ => Err(anyhow!("unexpected rpc output type")),
        }
    }
}
