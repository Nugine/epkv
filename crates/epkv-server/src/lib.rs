pub mod config;
pub mod net;

// -----------------------------------------------------------------------------

use self::config::Config;

use epkv_rocks::data_db::DataDb;
use epkv_rocks::log_db::LogDb;

use std::sync::Arc;

use anyhow::Result;
use tracing::debug;

pub struct Server {
    log_db: Arc<LogDb>,
    data_db: Arc<DataDb>,
}

impl Server {
    pub async fn run(config: Config) -> Result<()> {
        let log_db = LogDb::new(&config.log_db.path)?;

        debug!("log_db is opened");

        let data_db = DataDb::new(&config.data_db.path)?;

        debug!("data_db is opened");

        let server = Arc::new(Server { log_db, data_db });

        todo!()
    }
}
