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

    fn as_slice(&self) -> &[Command] {
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

#[derive(Clone, Serialize, Deserialize)]
pub struct Command(Asc<MutableCommand>);

impl Command {
    fn as_inner(&self) -> &MutableCommand {
        &*self.0
    }

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

impl AsRef<MutableCommand> for Command {
    fn as_ref(&self) -> &MutableCommand {
        self.as_inner()
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct MutableCommand {
    pub kind: CommandKind,
}

impl MutableCommand {
    fn is_fence(&self) -> bool {
        matches!(self.kind, CommandKind::Fence(_))
    }

    fn is_nop(&self) -> bool {
        matches!(self.kind, CommandKind::Nop(_))
    }

    fn create_nop() -> Self {
        Self { kind: CommandKind::Nop(Nop {}) }
    }

    fn create_fence() -> Self {
        Self { kind: CommandKind::Fence(Fence {}) }
    }
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

mod epaxos {
    use super::*;

    use epkv_epaxos::cmd::{CommandLike, Keys};
    use epkv_utils::vecset::VecSet;

    impl CommandLike for BatchedCommand {
        type Key = Key;

        fn keys(&self) -> Keys<Self> {
            let this = self.as_slice();
            let is_fence = this.iter().all(|c| c.as_inner().is_fence());
            if is_fence {
                return Keys::Unbounded;
            }
            let mut keys = Vec::with_capacity(this.len());
            for c in this {
                match c.as_inner().kind {
                    CommandKind::Get(ref c) => keys.push(c.key.clone()),
                    CommandKind::Set(ref c) => keys.push(c.key.clone()),
                    CommandKind::Del(ref c) => keys.push(c.key.clone()),
                    CommandKind::Nop(_) => {}
                    CommandKind::Fence(_) => {}
                }
            }
            Keys::Bounded(VecSet::from_vec(keys))
        }

        fn is_nop(&self) -> bool {
            self.as_slice().iter().all(|c| c.as_inner().is_nop())
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
            todo!()
        }

        fn notify_executed(&self) {
            todo!()
        }
    }
}

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
