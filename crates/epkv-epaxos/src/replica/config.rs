use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ReplicaConfig {
    pub fastpath_timeout: FastPathTimeout,
    pub recover: Recover,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FastPathTimeout {
    pub default: Duration,
    pub enable_adaptive: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Recover {
    pub default: Duration,
    pub enable_adaptive: bool,
}
