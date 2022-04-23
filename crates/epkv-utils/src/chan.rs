use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::error::Elapsed;
use tokio::time::timeout;

#[inline]
pub async fn recv_timeout<T>(rx: &mut mpsc::Receiver<T>, t: Duration) -> Result<Option<T>, Elapsed> {
    if let Ok(val) = rx.try_recv() {
        return Ok(Some(val));
    }
    timeout(t, rx.recv()).await
}
