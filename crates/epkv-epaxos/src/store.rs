use crate::bounds::{AttrBounds, StatusBounds};
use crate::exec::ExecNotify;
use crate::id::{Ballot, InstanceId};
use crate::ins::Instance;
use crate::status::Status;

use epkv_utils::asc::Asc;

use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait LogStore<C>: Send + Sync + 'static {
    async fn save(&mut self, id: InstanceId, ins: &Instance<C>, mode: UpdateMode) -> Result<()>;
    async fn load(&mut self, id: InstanceId) -> Result<Option<Instance<C>>>;

    async fn load_pbal(&mut self, id: InstanceId) -> Result<Option<Ballot>>;
    async fn save_pbal(&mut self, id: InstanceId, pbal: Ballot) -> Result<()>;

    async fn load_attr_bounds(&mut self) -> Result<AttrBounds>;
    async fn load_status_bounds(&mut self) -> Result<StatusBounds>;

    async fn update_status(&mut self, id: InstanceId, status: Status) -> Result<()>;
}

pub enum UpdateMode {
    Full,
    Partial,
}

#[async_trait]
pub trait DataStore<C>: Send + Sync + 'static {
    async fn issue(&self, id: InstanceId, cmd: C, notify: Asc<ExecNotify>) -> Result<()>;
}
