use crate::key::Key;
use crate::value::Value;

use epkv_utils::asc::Asc;

use serde::{Deserialize, Serialize};

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
