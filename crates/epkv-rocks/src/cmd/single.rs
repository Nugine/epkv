use super::kinds::*;
use super::notify::CommandNotify;

use asc::Asc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

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
        match Asc::try_unwrap(self.0) {
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
    #[serde(skip)]
    pub notify: Option<Asc<CommandNotify>>,
}

impl MutableCommand {
    pub fn is_fence(&self) -> bool {
        matches!(self.kind, CommandKind::Fence(_))
    }

    pub fn is_nop(&self) -> bool {
        matches!(self.kind, CommandKind::Nop(_))
    }

    #[must_use]
    pub fn create_nop() -> Self {
        Self { kind: CommandKind::Nop(Nop {}), notify: None }
    }

    #[must_use]
    pub fn create_fence() -> Self {
        Self { kind: CommandKind::Fence(Fence {}), notify: None }
    }

    pub fn fill_keys(&self, keys: &mut Vec<Bytes>) {
        match self.kind {
            CommandKind::Get(ref c) => keys.push(c.key.clone()),
            CommandKind::Set(ref c) => keys.push(c.key.clone()),
            CommandKind::Del(ref c) => keys.push(c.key.clone()),
            CommandKind::Nop(_) => {}
            CommandKind::Fence(_) => {}
        }
    }

    pub fn notify_committed(&self) {
        if let Some(ref n) = self.notify {
            n.notify_committed();
        }
    }

    pub fn notify_executed(&self) {
        if let Some(ref n) = self.notify {
            n.notify_executed();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::value::Value;

    use super::*;

    use epkv_utils::bytes_str::BytesStr;
    use epkv_utils::codec;

    use std::mem;

    #[test]
    fn cmd_size() {
        {
            let cmd_type_size = mem::size_of::<MutableCommand>();
            assert_eq!(cmd_type_size, 80); // track cmd type size
        }
        {
            let cmd_type_size = mem::size_of::<Command>();
            assert_eq!(cmd_type_size, 8); // track cmd type size
        }
        {
            let value = Value::Str(BytesStr::from("world".to_owned()));

            let key = Bytes::from("key".to_owned());
            let value = codec::serialize(&value).unwrap();

            let cmd = Command::from_mutable(MutableCommand {
                kind: CommandKind::Set(Set { key, value }),
                notify: None,
            });
            let cmd_codec_size = codec::serialized_size(&cmd).unwrap();
            assert_eq!(cmd_codec_size, 13);
        }
    }
}
