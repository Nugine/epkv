use crate::error::RocksDbErrorExt;

use std::sync::Arc;

use anyhow::Result;
use camino::Utf8Path;
use rocksdb::DB;

pub struct LogDb {
    db: DB,
}

impl LogDb {
    pub fn new(path: &Utf8Path) -> Result<Arc<Self>> {
        let db = DB::open_default(path.as_ref()).cvt()?;
        Ok(Arc::new(Self { db }))
    }
}
