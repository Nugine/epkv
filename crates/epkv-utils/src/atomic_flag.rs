use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;

pub struct AtomicFlag(AtomicBool);

impl AtomicFlag {
    #[inline]
    #[must_use]
    pub const fn new(val: bool) -> Self {
        Self(AtomicBool::new(val))
    }

    #[inline]
    pub fn set(&self, val: bool) {
        self.0.store(val, SeqCst);
    }

    #[inline]
    #[must_use]
    pub fn get(&self) -> bool {
        self.0.load(SeqCst)
    }
}
