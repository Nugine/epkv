use crate::types::LogStore;

use anyhow::Result;

pub struct Space<S: LogStore> {
    store: S,
}

impl<S: LogStore> Space<S> {
    pub fn new(store: S) -> Result<Self> {
        todo!()
    }
}
