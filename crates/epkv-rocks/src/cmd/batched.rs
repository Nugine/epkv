use super::*;

use epkv_epaxos::cmd::{CommandLike, Keys};

use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct BatchedCommand {
    cmds: Arc<[MutableCommand]>,
    meta: BatchedMeta,
}

#[derive(Clone)]
pub enum BatchedMeta {
    Nop,
    Unbounded,
    Bounded(Arc<[Bytes]>),
}

fn calc_meta(cmds: &[MutableCommand]) -> BatchedMeta {
    let mut is_all_nop = true;
    let mut keys = Vec::with_capacity(cmds.len());
    for cmd in cmds.iter() {
        match cmd.kind {
            super::CommandKind::Get(Get { ref key, .. }) => {
                keys.push(key.clone());
                is_all_nop = false;
            }
            super::CommandKind::Set(Set { ref key, .. }) => {
                keys.push(key.clone());
                is_all_nop = false;
            }
            super::CommandKind::Del(Del { ref key, .. }) => {
                keys.push(key.clone());
                is_all_nop = false
            }
            super::CommandKind::Nop(_) => {}
            super::CommandKind::Fence(_) => return BatchedMeta::Unbounded,
        }
    }
    if is_all_nop {
        return BatchedMeta::Nop;
    }
    keys.sort_unstable();
    keys.dedup_by(|x, first| x == first);
    BatchedMeta::Bounded(Arc::from(keys))
}

impl BatchedCommand {
    #[must_use]
    pub fn from_vec(v: Vec<MutableCommand>) -> Self {
        let cmds = Arc::from(v);
        let meta = calc_meta(&cmds);
        Self { cmds, meta }
    }

    #[inline]
    #[must_use]
    pub fn as_slice(&self) -> &[MutableCommand] {
        &self.cmds
    }
}

impl<'de> Deserialize<'de> for BatchedCommand {
    #[inline]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: ::serde::de::Deserializer<'de>,
    {
        <Vec<MutableCommand>>::deserialize(deserializer).map(Self::from_vec)
    }
}

impl Serialize for BatchedCommand {
    #[inline]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: ::serde::ser::Serializer,
    {
        <[MutableCommand]>::serialize(&*self.cmds, serializer)
    }
}

impl CommandLike for BatchedCommand {
    type Key = Bytes;

    type Keys = BatchedMeta;

    fn keys(&self) -> BatchedMeta {
        self.meta.clone()
    }

    fn is_nop(&self) -> bool {
        matches!(self.meta, BatchedMeta::Nop)
    }

    fn create_nop() -> Self {
        let nop = MutableCommand::create_nop();
        Self { cmds: Arc::from([nop]), meta: BatchedMeta::Nop }
    }

    fn create_fence() -> Self {
        let fence = MutableCommand::create_fence();
        Self { cmds: Arc::from([fence]), meta: BatchedMeta::Unbounded }
    }

    fn notify_committed(&self) {
        self.as_slice().iter().for_each(|c| c.notify_committed());
    }

    fn notify_executed(&self) {
        self.as_slice().iter().for_each(|c| c.notify_executed());
    }
}

impl Keys for BatchedMeta {
    type Key = Bytes;

    fn is_unbounded(&self) -> bool {
        matches!(self, BatchedMeta::Unbounded)
    }

    fn for_each(&self, f: impl FnMut(&Self::Key)) {
        if let BatchedMeta::Bounded(keys) = self {
            keys.iter().for_each(f);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::mem;

    #[test]
    fn batched_command_type_size() {
        assert_eq!(mem::size_of::<BatchedCommand>(), 40);
    }
}
