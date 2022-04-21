//! <https://github.com/tikv/rust-rocksdb>

use crate::cmd::{CommandKind, Del, Get, Set};
use crate::error::RocksDbErrorExt;

use std::sync::Arc;

use epkv_utils::codec;

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
        todo!()
    }

    pub fn execute_set(self: &Arc<Self>, cmd: Set) -> Result<()> {
        let value = codec::serialize(&cmd.value)?;
        self.db.put(cmd.key.as_ref(), value.as_ref()).cvt()?;
        Ok(())
    }

    pub fn execute_del(self: &Arc<Self>, cmd: Del) -> Result<()> {
        todo!()
    }
}
