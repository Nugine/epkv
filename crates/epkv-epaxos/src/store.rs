use crate::bounds::{AttrBounds, SavedStatusBounds, StatusBounds};
use crate::exec::ExecNotify;
use crate::id::{Ballot, InstanceId};
use crate::ins::Instance;
use crate::status::Status;

use std::future::Future;
use std::sync::Arc;

use anyhow::Result;
use asc::Asc;
use tokio::sync::oneshot;

pub trait LogStore<C>: Send + Sync + 'static {
    fn save(
        self: &Arc<Self>,
        id: InstanceId,
        ins: Instance<C>,
        mode: UpdateMode,
    ) -> oneshot::Receiver<Result<()>>;

    fn load(self: &Arc<Self>, id: InstanceId) -> oneshot::Receiver<Result<Option<Instance<C>>>>;

    fn save_pbal(self: &Arc<Self>, id: InstanceId, pbal: Ballot) -> oneshot::Receiver<Result<()>>;

    fn load_pbal(self: &Arc<Self>, id: InstanceId) -> oneshot::Receiver<Result<Option<Ballot>>>;

    fn save_bounds(
        self: &Arc<Self>,
        attr_bounds: AttrBounds,
        status_bounds: SavedStatusBounds,
    ) -> oneshot::Receiver<Result<()>>;

    fn load_bounds(self: &Arc<Self>) -> oneshot::Receiver<Result<(AttrBounds, StatusBounds)>>;

    fn update_status(self: &Arc<Self>, id: InstanceId, status: Status) -> oneshot::Receiver<Result<()>>;
}

#[derive(Debug, Clone, Copy)]
pub enum UpdateMode {
    Full,
    Partial,
}

pub trait DataStore<C>: Send + Sync + 'static {
    fn issue(
        self: &Arc<Self>,
        id: InstanceId,
        cmd: C,
        notify: Asc<ExecNotify>,
    ) -> impl Future<Output = Result<()>> + Send;
}
