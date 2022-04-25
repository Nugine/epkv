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

use std::fs;

use anyhow::Result;
use camino::Utf8PathBuf;
use clap::StructOpt;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Debug, clap::Parser)]
struct Opt {
    #[clap(long)]
    config: Utf8PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::parse();
    let config: Config = {
        let content = fs::read_to_string(&opt.config)?;
        toml::from_str(&content)?
    };
    Server::run(config).await
}
