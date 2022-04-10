use std::sync::atomic::{AtomicBool, Ordering};

pub struct AtomicFlag(AtomicBool);

impl AtomicFlag {
    #[inline]
    #[must_use]
    pub const fn new(val: bool) -> Self {
        Self(AtomicBool::new(val))
    }

    #[inline]
    pub fn set(&self, val: bool) {
        self.0.store(val, Ordering::Relaxed);
    }

    #[inline]
    #[must_use]
    pub fn load(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}
