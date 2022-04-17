use std::time::Duration;

use minstant::{Anchor, Instant};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct LocalInstant(u64);

static ANCHOR: Lazy<Anchor> = Lazy::new(Anchor::new);

impl LocalInstant {
    #[cfg(test)]
    #[must_use]
    pub const fn mock(val: u64) -> Self {
        Self(val)
    }

    #[inline]
    #[must_use]
    pub fn now() -> Self {
        let anchor = &*ANCHOR;
        Self(Instant::now().as_unix_nanos(anchor))
    }

    #[inline]
    #[must_use]
    pub const fn saturating_duration_since(&self, earlier: Self) -> Duration {
        let nanos = self.0.saturating_sub(earlier.0);
        Duration::from_nanos(nanos)
    }
}
