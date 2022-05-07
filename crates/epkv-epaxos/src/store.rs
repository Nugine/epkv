use crate::bounds::{AttrBounds, SavedStatusBounds, StatusBounds};
use crate::exec::ExecNotify;
use crate::id::{Ballot, InstanceId};
use crate::ins::Instance;
use crate::status::Status;

use epkv_utils::asc::Asc;

use std::future::Future;
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::oneshot;

pub trait LogStore<C>: Send + Sync + 'static {
    fn save(&mut self, id: InstanceId, ins: Instance<C>, mode: UpdateMode) -> oneshot::Receiver<Result<()>>;

    fn load(&mut self, id: InstanceId) -> oneshot::Receiver<Result<Option<Instance<C>>>>;

    fn save_pbal(&mut self, id: InstanceId, pbal: Ballot) -> oneshot::Receiver<Result<()>>;

    fn load_pbal(&mut self, id: InstanceId) -> oneshot::Receiver<Result<Option<Ballot>>>;

    fn save_bounds(
        &mut self,
        attr_bounds: AttrBounds,
        status_bounds: SavedStatusBounds,
    ) -> oneshot::Receiver<Result<()>>;

    fn load_bounds(&mut self) -> oneshot::Receiver<Result<(AttrBounds, StatusBounds)>>;

    fn update_status(&mut self, id: InstanceId, status: Status) -> oneshot::Receiver<Result<()>>;
}

#[derive(Debug, Clone, Copy)]
pub enum UpdateMode {
    Full,
    Partial,
}

pub trait DataStore<C>: Send + Sync + 'static {
    type Future<'a>: Future<Output = Result<()>> + Send + 'a;
    fn issue<'a>(self: &'a Arc<Self>, id: InstanceId, cmd: C, notify: Asc<ExecNotify>) -> Self::Future<'a>;
}
