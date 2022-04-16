use crate::vecmap::VecMap;

use std::borrow::Borrow;
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub struct MultiSemaphore<K: Ord> {
    map: RwLock<VecMap<K, Arc<Semaphore>>>,
}

impl<K: Ord> MultiSemaphore<K> {
    #[inline]
    pub fn new<I>(permits: I) -> Self
    where
        I: IntoIterator<Item = (K, usize)>,
    {
        let gen: _ = |(k, p)| (k, Arc::new(Semaphore::new(p)));
        let map: VecMap<_, _> = permits.into_iter().map(gen).collect();
        Self { map: RwLock::new(map) }
    }

    #[inline]
    pub fn insert(&self, key: K, permits: usize) {
        let mut guard = self.map.write();
        let m = &mut *guard;
        let _ = m.insert(key, Arc::new(Semaphore::new(permits)));
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
    pub async fn acquire<Q>(&self, key: &Q) -> Option<OwnedSemaphorePermit>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let sem = {
            let guard = self.map.read();
            let m = &*guard;
            m.get(key)?.clone()
        };
        Some(sem.acquire_owned().await.unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::asc::Asc;
    use crate::clone;

    use tokio::task::spawn;

    #[tokio::test(flavor = "current_thread")]
    async fn tokio() {
        let m = Asc::new(MultiSemaphore::<u32>::new([(1, 1), (2, 1)]));

        let p1 = m.acquire(&1).await.unwrap();

        let h1 = {
            clone!(m);
            spawn(async move {
                let p = m.acquire(&1).await;
                drop(p);
            })
        };

        let h2 = {
            clone!(m);
            spawn(async move {
                let p = m.acquire(&2).await;
                drop(p);
            })
        };

        m.insert(3, 1);

        let h3 = {
            clone!(m);
            spawn(async move {
                let p = m.acquire(&3).await;
                drop(p);
            })
        };

        h2.await.unwrap();
        h3.await.unwrap();

        m.remove(&3);

        drop(p1);
        h1.await.unwrap();
    }
}
