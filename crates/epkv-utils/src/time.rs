use std::time::{Duration, Instant};

use numeric_cast::NumericCast;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct LocalInstant(u64);

static ANCHOR: Lazy<Instant> = Lazy::new(Instant::now);

impl LocalInstant {
    #[cfg(test)]
    #[must_use]
    pub const fn mock(val: u64) -> Self {
        Self(val)
    }

    #[inline]
    #[must_use]
    pub fn now() -> Self {
        let anchor = *ANCHOR;
        let duration = Instant::now().duration_since(anchor);
        Self(duration.as_nanos().numeric_cast())
    }

    #[inline]
    #[must_use]
    pub const fn saturating_duration_since(&self, earlier: Self) -> Duration {
        let nanos = self.0.saturating_sub(earlier.0);
        Duration::from_nanos(nanos)
    }
}
