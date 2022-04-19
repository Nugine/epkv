use crate::asc::Asc;
use crate::radixmap::RadixMap;

use std::sync::atomic::{AtomicU64, Ordering::*};

use parking_lot::Mutex;
use tokio::sync::Notify;

pub struct WaterMark {
    level: AtomicU64,
    queue: Mutex<RadixMap<Asc<Notify>>>,
}

impl WaterMark {
    #[inline]
    #[must_use]
    pub const fn new(lv: u64) -> Self {
        Self {
            level: AtomicU64::new(lv),
            queue: Mutex::new(RadixMap::new()),
        }
    }

    #[inline]
    pub fn bump_level(&self, lv: u64) {
        let prev = self.level.fetch_max(lv, Relaxed);
        if prev < lv {
            self.flush_queue(lv);
        }
    }

    fn flush_queue(&self, lv: u64) {
        let mut guard = self.queue.lock();
        let q = &mut *guard;
        q.drain_less_equal(lv, |_, n| n.notify_waiters())
    }

    #[inline]
    #[must_use]
    pub fn level(&self) -> u64 {
        self.level.load(SeqCst)
    }

    #[inline]
    #[must_use]
    pub fn until(&self, lv: u64) -> WaterMarkUntil<'_> {
        let mut guard = self.queue.lock();
        let q = &mut *guard;
        let (_, n) = q.init_with(lv, || Asc::new(Notify::new()));
        let notify = n.asc_clone();
        drop(guard);
        WaterMarkUntil { watermark: self, until: lv, notify }
    }
}

pub struct WaterMarkUntil<'a> {
    watermark: &'a WaterMark,
    until: u64,
    notify: Asc<Notify>,
}

impl WaterMarkUntil<'_> {
    #[inline]
    #[must_use]
    pub fn level(&self) -> u64 {
        self.watermark.level.load(SeqCst)
    }

    #[inline]
    pub async fn wait(&self) {
        loop {
            if self.level() >= self.until {
                return;
            }
            self.notify.notified().await;
            if self.level() >= self.until {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::clone;

    use tokio::task::spawn;

    #[tokio::test(flavor = "current_thread")]
    async fn tokio() {
        let wm = Asc::new(WaterMark::new(0));
        {
            clone!(wm);
            wm.until(0).wait().await;
        }
        assert_eq!(wm.level(), 0);

        let h1 = {
            clone!(wm);
            spawn(async move { wm.until(10000).wait().await })
        };

        let h2 = {
            clone!(wm);
            spawn(async move { wm.until(100).wait().await })
        };

        wm.bump_level(200);
        h2.await.unwrap();
        assert_eq!(wm.level(), 200);

        wm.bump_level(10000);
        h1.await.unwrap();
        assert_eq!(wm.level(), 10000);
    }
}
