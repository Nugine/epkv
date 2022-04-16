use crate::asc::Asc;
use crate::radixmap::RadixMap;
use crate::vecmap::VecMap;

use std::borrow::Borrow;
use std::sync::atomic::{AtomicU64, Ordering::*};

use parking_lot::{Mutex, RwLock};
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
    pub fn until(this: Asc<Self>, lv: u64) -> WaterMarkToken {
        let mut guard = this.queue.lock();
        let q = &mut *guard;
        let (_, n) = q.init_with(lv, || Asc::new(Notify::new()));
        let notify = n.asc_clone();
        drop(guard);
        WaterMarkToken { watermark: this, until: lv, notify }
    }
}

pub struct WaterMarkToken {
    watermark: Asc<WaterMark>,
    until: u64,
    notify: Asc<Notify>,
}

impl WaterMarkToken {
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

pub struct MultiWaterMark<K: Ord> {
    map: RwLock<VecMap<K, Asc<WaterMark>>>,
}

impl<K: Ord> MultiWaterMark<K> {
    #[inline]
    pub fn new<I>(levels: I) -> Self
    where
        I: IntoIterator<Item = (K, u64)>,
    {
        let gen: _ = |(k, lv)| (k, Asc::new(WaterMark::new(lv)));
        let map: VecMap<_, _> = levels.into_iter().map(gen).collect();
        Self { map: RwLock::new(map) }
    }

    #[inline]
    pub fn insert(&self, key: K, lv: u64) {
        let mut guard = self.map.write();
        let m = &mut *guard;
        let _ = m.insert(key, Asc::new(WaterMark::new(lv)));
    }

    #[inline]
    pub fn remove<Q>(&self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut guard = self.map.write();
        let m = &mut *guard;
        let _ = m.remove(key);
    }

    #[inline]
    pub fn bump_many(&self, levels: impl IntoIterator<Item = (K, u64)>) {
        let guard = self.map.read();
        let m = &*guard;
        for (key, lv) in levels {
            if let Some(wm) = m.get(&key) {
                wm.bump_level(lv)
            }
        }
    }

    #[inline]
    #[must_use]
    pub fn until<Q>(&self, key: &Q, lv: u64) -> Option<WaterMarkToken>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let wm = {
            let guard = self.map.read();
            let m = &*guard;
            m.get(key)?.asc_clone()
        };
        Some(WaterMark::until(wm, lv))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::clone;

    use tokio::task::spawn;

    #[tokio::test(flavor = "current_thread")]
    async fn single() {
        let wm = Asc::new(WaterMark::new(0));
        {
            clone!(wm);
            WaterMark::until(wm, 0).wait().await;
        }
        assert_eq!(wm.level(), 0);

        let h1 = {
            clone!(wm);
            let tk = WaterMark::until(wm, 10000);
            spawn(async move { tk.wait().await })
        };

        let h2 = {
            clone!(wm);
            let tk = WaterMark::until(wm, 100);
            spawn(async move { tk.wait().await })
        };

        wm.bump_level(200);
        h2.await.unwrap();
        assert_eq!(wm.level(), 200);

        wm.bump_level(10000);
        h1.await.unwrap();
        assert_eq!(wm.level(), 10000);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn multi() {
        let m = Asc::new(MultiWaterMark::<u32>::new([(1, 1), (2, 1)]));

        let h1 = {
            clone!(m);
            spawn(async move {
                m.until(&1, 2).unwrap().wait().await;
            })
        };

        let h2 = {
            clone!(m);
            spawn(async move {
                m.until(&2, 2).unwrap().wait().await;
            })
        };

        m.insert(3, 1);

        let h3 = {
            clone!(m);
            spawn(async move {
                m.until(&3, 1).unwrap().wait().await;
            })
        };

        {
            clone!(m);
            spawn(async move { m.bump_many([(1, 3), (2, 2)]) });
        }

        h2.await.unwrap();
        h3.await.unwrap();
        h1.await.unwrap();

        m.remove(&3);
    }
}
