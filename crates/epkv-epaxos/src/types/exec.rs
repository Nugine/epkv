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

const INIT: u8 = 0;
const ISSUED: u8 = 1;
const EXECUTED: u8 = 2;

impl ExecNotify {
    #[must_use]
    pub fn new() -> Self {
        Self { state: AtomicU8::new(INIT), waker: AtomicWaker::new() }
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
        self.set_state(ISSUED);
    }
    pub fn notify_executed(&self) {
        self.set_state(EXECUTED);
    }

    pub async fn wait_issued(&self) {
        poll_fn(|cx| self.poll_state(cx, ISSUED)).await;
    }
    pub async fn wait_executed(&self) {
        poll_fn(|cx| self.poll_state(cx, EXECUTED)).await;
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
        assert_eq!(n.state(), INIT);

        {
            let n = n.asc_clone();
            spawn(async move {
                n.notify_issued();
                sleep(Duration::from_millis(50)).await;
                n.notify_executed();
            });
        }

        n.wait_issued().await;
        assert_eq!(n.state(), ISSUED);

        n.wait_executed().await;
        assert_eq!(n.state(), EXECUTED);
    }
}
