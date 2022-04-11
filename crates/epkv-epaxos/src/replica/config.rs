use std::time::Duration;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ReplicaConfig {
    pub preaccept_timeout: PreAcceptTimeout,
    pub recover_timeout: RecoverTimeout,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PreAcceptTimeout {
    pub default: Duration,
    pub enable_adaptive: bool,
}

impl PreAcceptTimeout {
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
pub struct RecoverTimeout {
    pub default: Duration,
    pub enable_adaptive: bool,
}

impl RecoverTimeout {
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
