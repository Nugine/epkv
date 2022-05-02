use crate::bounds::{AttrBounds, SavedStatusBounds, StatusBounds};
use crate::exec::ExecNotify;
use crate::id::{Ballot, InstanceId};
use crate::ins::Instance;
use crate::status::Status;

use epkv_utils::asc::Asc;

use std::future::Future;

use anyhow::Result;

pub trait LogStore<C>: Send + Sync + 'static {
    type SaveFuture<'a>: Future<Output = Result<()>> + Send + 'a;
    fn save(&mut self, id: InstanceId, ins: Instance<C>, mode: UpdateMode) -> Self::SaveFuture<'_>;

    type LoadFuture<'a>: Future<Output = Result<Option<Instance<C>>>> + Send + 'a;
    fn load(&mut self, id: InstanceId) -> Self::LoadFuture<'_>;

    type SavePbalFuture<'a>: Future<Output = Result<()>> + Send + 'a;
    fn save_pbal(&mut self, id: InstanceId, pbal: Ballot) -> Self::SavePbalFuture<'_>;

    type LoadPbalFuture<'a>: Future<Output = Result<Option<Ballot>>> + Send + 'a;
    fn load_pbal(&mut self, id: InstanceId) -> Self::LoadPbalFuture<'_>;

    type SaveBoundsFuture<'a>: Future<Output = Result<()>> + Send + 'a;
    fn save_bounds(
        &mut self,
        attr_bounds: AttrBounds,
        status_bounds: SavedStatusBounds,
    ) -> Self::SaveBoundsFuture<'_>;

    type LoadBoundsFuture<'a>: Future<Output = Result<(AttrBounds, StatusBounds)>> + Send + 'a;
    fn load_bounds(&mut self) -> Self::LoadBoundsFuture<'_>;

    type UpdateStatusFuture<'a>: Future<Output = Result<()>> + Send + 'a;
    fn update_status(&mut self, id: InstanceId, status: Status) -> Self::UpdateStatusFuture<'_>;
}

#[derive(Debug)]
pub enum UpdateMode {
    Full,
    Partial,
}

pub trait DataStore<C>: Send + Sync + 'static {
    type Future<'a>: Future<Output = Result<()>> + Send + 'a;
    fn issue(&self, id: InstanceId, cmd: C, notify: Asc<ExecNotify>) -> Self::Future<'_>;
}
