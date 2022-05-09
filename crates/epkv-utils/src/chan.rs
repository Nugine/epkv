use std::future::Future;
use std::time::Duration;

use futures_util::FutureExt;

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

#[inline]
pub fn send<T: Send>(tx: &mpsc::Sender<T>, val: T) -> impl Future<Output = Result<(), T>> + Send + '_ {
    tx.reserve().map(move |result| match result {
        Ok(permit) => {
            permit.send(val);
            Ok(())
        }
        Err(_) => Err(val),
    })
}
