use super::*;

use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait LogStore: Send + Sync + 'static {
    type Command: CommandLike;

    async fn save_full(&mut self, id: InstanceId, ins: &Instance<Self::Command>) -> Result<()>;
    async fn load_full(&mut self, id: InstanceId) -> Result<Option<Instance<Self::Command>>>;

    async fn load_pbal(&mut self, id: InstanceId) -> Result<Option<Ballot>>;
    async fn save_pbal(&mut self, id: InstanceId, pbal: Ballot) -> Result<()>;

    async fn save_partial(&mut self, id: InstanceId, ins: &PartialInstance) -> Result<()>;

    async fn load_attr_bounds(&mut self) -> Result<AttrBounds>;
}
