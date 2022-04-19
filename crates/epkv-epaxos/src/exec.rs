use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering::*;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use futures_util::future::poll_fn;
use futures_util::task::AtomicWaker;

pub struct ExecNotify {
    state: AtomicU8,
    waker: AtomicWaker,
}

impl ExecNotify {
    const INIT: u8 = 0;
    const ISSUED: u8 = 1;
    const EXECUTED: u8 = 2;

    #[must_use]
    pub fn new() -> Self {
        Self { state: AtomicU8::new(Self::INIT), waker: AtomicWaker::new() }
    }

    fn register_waker(&self, waker: &Waker) {
        self.waker.register(waker);
    }

    fn state(&self) -> u8 {
        self.state.load(SeqCst)
    }

    fn poll_state(&self, cx: &mut Context<'_>, s: u8) -> Poll<()> {
        if self.state() >= s {
            return Poll::Ready(());
        }
        self.register_waker(cx.waker());
        if self.state() >= s {
            return Poll::Ready(());
        }
        Poll::Pending
    }

    fn set_state(&self, s: u8) {
        self.state.fetch_max(s, Relaxed);
        self.waker.wake();
    }

    pub fn notify_issued(&self) {
        self.set_state(Self::ISSUED);
    }
    pub fn notify_executed(&self) {
        self.set_state(Self::EXECUTED);
    }

    pub async fn wait_issued(&self) {
        poll_fn(|cx| self.poll_state(cx, Self::ISSUED)).await;
    }
    pub async fn wait_executed(&self) {
        poll_fn(|cx| self.poll_state(cx, Self::EXECUTED)).await;
    }
}

impl Default for ExecNotify {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use epkv_utils::asc::Asc;

    use tokio::spawn;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn simple() {
        let n = Asc::new(ExecNotify::new());
        assert_eq!(n.state(), ExecNotify::INIT);

        {
            let n = Asc::clone(&n);
            spawn(async move {
                n.notify_issued();
                sleep(Duration::from_millis(50)).await;
                n.notify_executed();
            });
        }

        n.wait_issued().await;
        assert_eq!(n.state(), ExecNotify::ISSUED);

        n.wait_executed().await;
        assert_eq!(n.state(), ExecNotify::EXECUTED);
    }
}
