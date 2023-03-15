#![forbid(unsafe_code)]
#![deny(
    clippy::all,
    clippy::as_conversions,
    clippy::float_arithmetic,
    clippy::integer_arithmetic,
    clippy::must_use_candidate
)]
#![warn(clippy::todo, clippy::dbg_macro)]

use epkv_server::config::Config;
use epkv_server::Server;
use epkv_utils::config::read_config_file;
use epkv_utils::tracing::setup_tracing;

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

    let config: Config = read_config_file(&opt.config)?;

    debug!(?config);

    run(config)
}

#[allow(clippy::redundant_async_block)]
#[tokio::main]
async fn run(config: Config) -> Result<()> {
    Server::run(config).await
}
