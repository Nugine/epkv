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
    pub batching: BatchingConfig,
    pub interval: IntervalConfig,
}

#[derive(Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ServerConfig {
    pub listen_peer_addr: SocketAddr,
    pub listen_client_addr: SocketAddr,
    pub monitor_addr: SocketAddr,
    pub public_peer_addr: SocketAddr,
    pub msg_task_limit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    pub inbound_chan_size: usize,
    pub outbound_chan_size: usize,
    pub initial_reconnect_timeout_us: u64,
    pub max_reconnect_timeout_us: u64,
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

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchingConfig {
    pub chan_size: usize,
    pub batch_initial_capacity: usize,
    pub batch_max_size: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IntervalConfig {
    pub probe_rtt_interval_us: u64,
    pub clear_key_map_interval_us: u64,
    pub save_bounds_interval_us: u64,
    pub broadcast_bounds_interval_us: u64,
}
