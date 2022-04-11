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

impl FastPathTimeout {
    pub fn with(
        &self,
        avg_rtt: Option<Duration>,
        f: impl FnOnce(Duration) -> Duration,
    ) -> Duration {
        if self.enable_adaptive {
            match avg_rtt {
                Some(d) => f(d),
                None => self.default,
            }
        } else {
            self.default
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Recover {
    pub default: Duration,
    pub enable_adaptive: bool,
}

impl Recover {
    pub fn with(
        &self,
        avg_rtt: Option<Duration>,
        f: impl FnOnce(Duration) -> Duration,
    ) -> Duration {
        if self.enable_adaptive {
            match avg_rtt {
                Some(d) => f(d),
                None => self.default,
            }
        } else {
            self.default
        }
    }
}
