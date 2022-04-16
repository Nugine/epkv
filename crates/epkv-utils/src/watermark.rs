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
    pub fn until(this: Asc<Self>, lv: u64) -> Token {
        let mut guard = this.queue.lock();
        let q = &mut *guard;
        let (_, n) = q.init_with(lv, || Asc::new(Notify::new()));
        let notify = n.asc_clone();
        drop(guard);
        Token { watermark: this, until: lv, notify }
    }
}

pub struct Token {
    watermark: Asc<WaterMark>,
    until: u64,
    notify: Asc<Notify>,
}

impl Token {
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

pub trait WaterMarkExt {
    fn until(self, lv: u64) -> Token;
}

impl WaterMarkExt for &'_ Asc<WaterMark> {
    #[inline]
    fn until(self, lv: u64) -> Token {
        WaterMark::until(self.asc_clone(), lv)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::task::spawn;

    #[tokio::test(flavor = "current_thread")]
    async fn tokio() {
        let wm = Asc::new(WaterMark::new(0));
        wm.until(0).wait().await;
        assert_eq!(wm.level(), 0);

        let h1 = {
            let tk = wm.until(10000);
            spawn(async move { tk.wait().await })
        };

        let h2 = {
            let tk = wm.until(100);
            spawn(async move { tk.wait().await })
        };

        wm.bump_level(200);
        h2.await.unwrap();
        assert_eq!(wm.level(), 200);

        wm.bump_level(10000);
        h1.await.unwrap();
        assert_eq!(wm.level(), 10000);
    }
}
