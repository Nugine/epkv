use crate::error::RocksDbErrorExt;

use std::sync::Arc;

use rocksdb::DB;

use anyhow::Result;
use camino::Utf8Path;

pub struct DataDb {
    db: DB,
}

impl DataDb {
    pub fn new(path: &Utf8Path) -> Result<Arc<Self>> {
        let db = DB::open_default(path.as_ref()).cvt()?;
        Ok(Arc::new(Self { db }))
    }
}
