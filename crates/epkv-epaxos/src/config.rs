use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ReplicaConfig {
    pub preaccept_timeout: PreAcceptTimeout,
    pub recover_timeout: RecoverTimeout,
    pub sync_limits: SyncLimits,
    pub join_timeout: JoinTimeout,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreAcceptTimeout {
    /// default timeout, in microseconds
    pub default_us: u64,
    pub enable_adaptive: bool,
}

impl PreAcceptTimeout {
    pub fn with(&self, avg_rtt: Option<Duration>, f: impl FnOnce(Duration) -> Duration) -> Duration {
        let default = Duration::from_micros(self.default_us);
        if self.enable_adaptive {
            avg_rtt.map_or(default, f)
        } else {
            default
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoverTimeout {
    /// default timeout, in microseconds
    pub default_us: u64,
    pub enable_adaptive: bool,
}

impl RecoverTimeout {
    pub fn with(&self, avg_rtt: Option<Duration>, f: impl FnOnce(Duration) -> Duration) -> Duration {
        let default = Duration::from_micros(self.default_us);
        if self.enable_adaptive {
            avg_rtt.map_or(default, f)
        } else {
            default
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncLimits {
    pub max_instance_num: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinTimeout {
    pub default_us: u64,
}
