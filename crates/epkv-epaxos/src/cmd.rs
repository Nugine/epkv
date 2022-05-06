use std::hash::Hash;

use serde::de::DeserializeOwned;
use serde::Serialize;

pub trait CommandLike
where
    Self: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    type Key: Clone + Eq + Ord + Hash + Send + Sync + 'static;

    type Keys: Keys<Key = Self::Key>;

    fn keys(&self) -> Self::Keys;

    fn is_nop(&self) -> bool;

    fn create_nop() -> Self;

    fn create_fence() -> Self;

    fn notify_committed(&self);

    fn notify_executed(&self);
}

pub trait Keys {
    type Key;
    fn is_unbounded(&self) -> bool;
    fn for_each(&self, f: impl FnMut(&Self::Key));
}
