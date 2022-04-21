//! <https://github.com/tikv/rust-rocksdb>

use crate::cmd::{CommandKind, Del, Get, Set};
use crate::error::RocksDbErrorExt;
use crate::kv::BytesValue;

use std::sync::Arc;

use anyhow::Result;
use camino::Utf8Path;
use rocksdb::{Writable, DB};

pub struct DataDb {
    db: DB,
}

impl DataDb {
    pub fn new(path: &Utf8Path) -> Result<Arc<Self>> {
        let db = DB::open_default(path.as_ref()).cvt()?;
        Ok(Arc::new(Self { db }))
    }

    pub fn execute(self: &Arc<Self>, cmd: CommandKind) -> Result<()> {
        match cmd {
            CommandKind::Get(cmd) => self.execute_get(cmd),
            CommandKind::Set(cmd) => self.execute_set(cmd),
            CommandKind::Del(cmd) => self.execute_del(cmd),
            CommandKind::Nop(_) => Ok(()),
            CommandKind::Fence(_) => Ok(()),
        }
    }

    pub fn execute_get(self: &Arc<Self>, cmd: Get) -> Result<()> {
        let ans = self.db.get(cmd.key.as_ref()).cvt()?;
        let ans = ans.map(|v| BytesValue::from_bytes(&*v));
        if let Some(tx) = cmd.tx {
            let _ = tx.blocking_send(ans);
        }
        Ok(())
    }

    pub fn execute_set(self: &Arc<Self>, cmd: Set) -> Result<()> {
        self.db.put(cmd.key.as_ref(), cmd.value.as_ref()).cvt()?;
        Ok(())
    }

    pub fn execute_del(self: &Arc<Self>, cmd: Del) -> Result<()> {
        self.db.delete(cmd.key.as_ref()).cvt()?;
        Ok(())
    }
}
