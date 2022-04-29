use super::single::{Command, MutableCommand};

use epkv_epaxos::cmd::{CommandLike, Keys};
use epkv_utils::vecset::VecSet;

use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct BatchedCommand(Arc<[Command]>);

impl BatchedCommand {
    #[must_use]
    pub fn from_vec(v: Vec<Command>) -> Self {
        Self(Arc::from(v))
    }

    #[inline]
    #[must_use]
    pub fn as_slice(&self) -> &[Command] {
        &*self.0
    }
}

impl<'de> Deserialize<'de> for BatchedCommand {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: ::serde::de::Deserializer<'de>,
    {
        <Vec<Command>>::deserialize(deserializer).map(Self::from_vec)
    }
}

impl Serialize for BatchedCommand {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ::serde::ser::Serializer,
    {
        <[Command]>::serialize(&*self.0, serializer)
    }
}

impl CommandLike for BatchedCommand {
    type Key = Bytes;

    fn keys(&self) -> Keys<Self> {
        let this = self.as_slice();
        let is_fence = this.iter().all(|c| c.as_ref().is_fence());
        if is_fence {
            return Keys::Unbounded;
        }
        let mut keys = Vec::with_capacity(this.len());
        for c in this {
            c.as_ref().fill_keys(&mut keys);
        }
        Keys::Bounded(VecSet::from_vec(keys))
    }

    fn is_nop(&self) -> bool {
        self.as_slice().iter().all(|c| c.as_ref().is_nop())
    }

    fn create_nop() -> Self {
        let nop = Command::from_mutable(MutableCommand::create_nop());
        Self::from_vec(vec![nop])
    }

    fn create_fence() -> Self {
        let fence = Command::from_mutable(MutableCommand::create_fence());
        Self::from_vec(vec![fence])
    }

    fn notify_committed(&self) {
        self.as_slice().iter().for_each(|c| c.as_ref().notify_committed());
    }

    fn notify_executed(&self) {
        self.as_slice().iter().for_each(|c| c.as_ref().notify_executed());
    }
}
