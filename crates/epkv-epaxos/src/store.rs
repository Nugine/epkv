use crate::id::{Ballot, InstanceId};
use crate::ins::Instance;

use anyhow::Result;

pub trait LogStore<C>: Send + Sync + 'static {
    fn load_instance(&mut self, id: InstanceId) -> Result<Option<Instance<C>>>;
    fn save_instance(&mut self, id: InstanceId, instance: Instance<C>) -> Result<()>;
    fn load_pbal(&mut self, id: InstanceId) -> Result<Option<Ballot>>;
    fn save_pbal(&mut self, id: InstanceId, pbal: Ballot) -> Result<()>;
}
