use super::*;

use anyhow::Result;

pub trait LogStore: Send + Sync + 'static {
    type Command: CommandLike;

    fn save_instance(&self, id: InstanceId, ins: &Instance<Self::Command>) -> Result<()>;
    fn load_instance(&self, id: InstanceId) -> Result<Option<Instance<Self::Command>>>;

    fn load_pbal(&self, id: InstanceId) -> Result<Option<Ballot>>;
    fn save_pbal(&self, id: InstanceId, pbal: Ballot) -> Result<()>;

    fn save_partial_instance(&self, id: InstanceId, ins: &PartialInstance) -> Result<()>;

    fn load_attr_bounds(&self) -> Result<AttrBounds>;
}
