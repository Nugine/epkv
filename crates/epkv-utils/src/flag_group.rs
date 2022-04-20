use std::sync::atomic::{AtomicBool, Ordering::*};
use std::sync::Arc;
use std::task::Poll;

use futures_util::future::poll_fn;
use futures_util::task::AtomicWaker;

#[derive(Clone)]
pub struct FlagGroup(Arc<[Flag]>);

struct Flag {
    value: AtomicBool,
    waker: AtomicWaker,
}

impl FlagGroup {
    #[inline]
    #[must_use]
    pub fn new(num: usize) -> Self {
        let mut flags = Vec::with_capacity(num);
        flags.resize_with(num, || Flag {
            value: AtomicBool::new(false),
            waker: AtomicWaker::new(),
        });
        Self(Arc::from(flags))
    }

    #[inline]
    pub fn set(&self, idx: usize) {
        let flag = &self.0[idx];
        flag.value.store(true, SeqCst);
        flag.waker.wake();
    }

    #[inline]
    pub async fn wait(&self, idx: usize) {
        let flag = &self.0[idx];
        poll_fn(|cx| {
            if flag.value.load(SeqCst) {
                return Poll::Ready(());
            }
            flag.waker.register(cx.waker());
            if flag.value.load(SeqCst) {
                return Poll::Ready(());
            }
            Poll::Pending
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tokio::spawn;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn tokio() {
        let n = 10;
        let flag_group = FlagGroup::new(n);

        let order = Arc::new(Mutex::new(Vec::<usize>::new()));

        let mut handles = Vec::new();
        for i in (0..n).rev() {
            clone!(flag_group, order);
            handles.push(spawn(async move {
                if i > 0 {
                    flag_group.wait(i - 1).await
                }
                order.lock().await.push(i);
                flag_group.set(i);
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        let mut guard = order.lock().await;
        let order = &mut *guard;

        assert_eq!(order.len(), n);
        for (i, x) in order.iter().copied().enumerate() {
            assert_eq!(x, i);
        }
    }
}
