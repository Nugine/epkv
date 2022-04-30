#![forbid(unsafe_code)]
#![deny(
    clippy::all,
    clippy::as_conversions,
    clippy::float_arithmetic,
    clippy::integer_arithmetic,
    clippy::must_use_candidate
)]
#![warn(clippy::todo)]

pub mod config;

// ------------------------------------------------------------------------------------------------

use self::config::Config;

use anyhow::Result;

pub struct Monitor {}

impl Monitor {
    pub async fn run(config: Config) -> Result<()> {
        todo!()
    }
}
