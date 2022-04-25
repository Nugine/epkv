#![forbid(unsafe_code)]
#![deny(
    clippy::all,
    clippy::as_conversions,
    clippy::float_arithmetic,
    clippy::integer_arithmetic,
    clippy::must_use_candidate
)]
#![warn(clippy::todo)]

use epkv_server::config::Config;
use epkv_server::Server;
use epkv_utils::tracing::setup_tracing;

use std::fs;

use anyhow::Result;
use camino::Utf8PathBuf;
use clap::StructOpt;
use tracing::debug;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Debug, clap::Parser)]
struct Opt {
    #[clap(long)]
    config: Utf8PathBuf,
}

fn main() -> Result<()> {
    let opt = Opt::parse();

    setup_tracing();

    let config: Config = {
        let content = fs::read_to_string(&opt.config)?;
        toml::from_str(&content)?
    };

    debug!(?config);

    run(config)
}

#[tokio::main]
async fn run(config: Config) -> Result<()> {
    Server::run(config).await
}
