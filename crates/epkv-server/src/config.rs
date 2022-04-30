use epkv_epaxos::config::ReplicaConfig;
use epkv_protocol::rpc::{RpcClientConfig, RpcServerConfig};

use std::net::SocketAddr;

use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Config {
    pub server: ServerConfig,
    pub replica: ReplicaConfig,
    pub network: NetworkConfig,
    pub log_db: LogDbConfig,
    pub data_db: DataDbConfig,
    pub rpc_client: RpcClientConfig,
    pub rpc_server: RpcServerConfig,
}

#[derive(Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ServerConfig {
    pub listen_peer_addr: SocketAddr,
    pub listen_client_addr: SocketAddr,
    pub monitor_addr: SocketAddr,
    pub public_peer_addr: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    pub inbound_chan_size: usize,
    pub outbound_chan_size: usize,
    pub reconnect_interval_us: u64,
    pub max_frame_length: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LogDbConfig {
    pub path: Utf8PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DataDbConfig {
    pub path: Utf8PathBuf,
}
