pub mod config;
pub mod net;

// -----------------------------------------------------------------------------

use self::config::Config;

use anyhow::Result;

pub struct Server {}

impl Server {
    pub async fn run(config: Config) -> Result<()> {
        todo!()
    }
}
