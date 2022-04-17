use epkv_utils::vecset::VecSet;

use std::hash::Hash;

use serde::de::DeserializeOwned;
use serde::Serialize;

pub trait CommandLike
where
    Self: Serialize + DeserializeOwned,
    Self: Send + Sync + 'static,
    Self: Clone,
{
    type Key: Eq + Ord + Hash + Send + Sync + 'static;

    fn keys(&self) -> Keys<Self>;

    fn is_nop(&self) -> bool;

    fn create_nop() -> Self;

    fn create_fence() -> Self;

    fn notify_committed(&self);

    fn notify_executed(&self);
}

pub enum Keys<C: CommandLike> {
    Bounded(VecSet<C::Key>),
    Unbounded,
}
