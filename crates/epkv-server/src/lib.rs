pub mod config;
pub mod net;

// -----------------------------------------------------------------------------

use std::sync::Arc;

use anyhow::Result;

pub struct Server {}

impl Server {
    pub async fn new() -> Result<Arc<Self>> {
        todo!()
    }
}
