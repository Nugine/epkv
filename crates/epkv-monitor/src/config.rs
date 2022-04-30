use std::net::SocketAddr;

use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub state_path: Utf8PathBuf,
    pub listen_rpc_addr: SocketAddr,
}
