use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;

pub struct AtomicFlag(AtomicBool);

impl AtomicFlag {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self(AtomicBool::new(false))
    }

    #[inline]
    pub fn set(&self) {
        self.0.store(true, SeqCst);
    }

    #[inline]
    #[must_use]
    pub fn get(&self) -> bool {
        self.0.load(SeqCst)
    }
}
