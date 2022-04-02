use epkv_utils::vecset::VecSet;

use std::hash::Hash;

use serde::de::DeserializeOwned;
use serde::Serialize;

pub trait CommandLike
where
    Self: Serialize + DeserializeOwned,
    Self: Send + Sync + 'static,
{
    type Key: Eq + Ord + Hash + Send + Sync + 'static;

    fn keys(&self) -> Keys<Self>;

    fn has_unbounded_keys(&self) -> bool;

    fn is_nop(&self) -> bool;

    fn create_nop() -> Self
    where
        Self: Sized;

    fn create_fence() -> Self
    where
        Self: Sized;
}

pub enum Keys<C: CommandLike> {
    Bounded(VecSet<C::Key>),
    Unbounded,
}
