use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering::*;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use futures_util::future::poll_fn;
use futures_util::task::AtomicWaker;

pub struct Stepper {
    state: AtomicU8,
    waker: AtomicWaker,
}

impl Stepper {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self { state: AtomicU8::new(0), waker: AtomicWaker::new() }
    }

    fn register_waker(&self, waker: &Waker) {
        self.waker.register(waker);
    }

    #[inline]
    pub fn state(&self) -> u8 {
        self.state.load(SeqCst)
    }

    #[inline]
    pub fn poll_state(&self, cx: &mut Context<'_>, s: u8) -> Poll<()> {
        if self.state() >= s {
            return Poll::Ready(());
        }
        self.register_waker(cx.waker());
        if self.state() >= s {
            return Poll::Ready(());
        }
        Poll::Pending
    }

    #[inline]
    pub fn set_state(&self, s: u8) {
        self.state.fetch_max(s, Relaxed);
        self.waker.wake();
    }

    #[inline]
    pub async fn wait_state(&self, s: u8) {
        poll_fn(|cx| self.poll_state(cx, s)).await;
    }
}

impl Default for Stepper {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use asc::Asc;

    use tokio::spawn;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn simple() {
        let n = Asc::new(Stepper::new());
        assert_eq!(n.state(), 0);

        {
            let n = Asc::clone(&n);
            spawn(async move {
                n.set_state(1);
                sleep(Duration::from_millis(10)).await;
                n.set_state(2);
            });
        }

        n.wait_state(1).await;
        assert_eq!(n.state(), 1);

        n.wait_state(2).await;
        assert_eq!(n.state(), 2);
    }
}
