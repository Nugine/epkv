use super::id::{Ballot, InstanceId};
use super::ins::Instance;
use super::{CommandLike, Deps, Seq};

use anyhow::Result;

pub trait LogStore: Send + Sync + 'static {
    type Command: CommandLike;
    fn load_instance(&self, id: InstanceId) -> Result<Option<Instance<Self::Command>>>;
    fn save_instance(&self, id: InstanceId, instance: Instance<Self::Command>) -> Result<()>;
    fn load_pbal(&self, id: InstanceId) -> Result<Option<Ballot>>;
    fn save_pbal(&self, id: InstanceId, pbal: Ballot) -> Result<()>;
    fn calc_deps(&self, id: InstanceId, cmd: &Self::Command) -> Result<Deps>;
    fn calc_seq(&self, id: InstanceId, cmd: &Self::Command) -> Result<Seq>;
}
