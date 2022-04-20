use crate::key::Key;
use crate::value::Value;

use epkv_utils::asc::Asc;

use std::sync::Arc;

use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct BatchedCommand(Arc<[Command]>);

impl BatchedCommand {
    #[must_use]
    pub fn from_vec(v: Vec<Command>) -> Self {
        Self(Arc::from(v))
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

#[derive(Clone, Serialize, Deserialize)]
pub struct Command(Asc<MutableCommand>);

impl Command {
    pub fn from_mutable(cmd: MutableCommand) -> Self {
        Self(Asc::new(cmd))
    }

    #[must_use]
    pub fn into_mutable(self) -> MutableCommand {
        match Asc::try_into_inner(self.0) {
            Ok(cmd) => cmd,
            Err(this) => MutableCommand::clone(&*this),
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct MutableCommand {
    pub kind: CommandKind,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum CommandKind {
    Get(Get),
    Set(Set),
    Del(Del),
    Nop(Nop),
    Fence(Fence),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Get {
    pub key: Key,
    pub value: Value,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Set {
    pub key: Key,
    pub value: Value,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Del {
    pub key: Key,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Nop {}

#[derive(Clone, Serialize, Deserialize)]
pub struct Fence {}

#[cfg(test)]
mod tests {
    use std::mem;

    use super::*;

    #[test]
    fn cmd_size() {
        let cmd_type_size = mem::size_of::<MutableCommand>();
        assert_eq!(cmd_type_size, 80); // track cmd type size

        let cmd_type_size = mem::size_of::<Command>();
        assert_eq!(cmd_type_size, 8); // track cmd type size
    }
}
