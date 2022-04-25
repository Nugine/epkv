use epkv_epaxos::config::ReplicaConfig;

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Config {
    pub server: ServerConfig,
    pub replica: ReplicaConfig,
}

#[derive(Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ServerConfig {
    pub listen_peer_addr: SocketAddr,
    pub listen_client_addr: SocketAddr,
    pub monitor_addr: SocketAddr,
}
