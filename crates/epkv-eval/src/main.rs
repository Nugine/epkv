#![forbid(unsafe_code)]
#![deny(
    clippy::all,
    clippy::as_conversions,
    clippy::float_arithmetic,
    clippy::integer_arithmetic,
    clippy::must_use_candidate
)]
#![warn(clippy::todo, clippy::dbg_macro)]

use epkv_eval::client::{self, ClientOpt};
use epkv_eval::cluster::{self, ClusterOpt};

use anyhow::Result;
use clap::StructOpt;

#[derive(Debug, clap::Parser)]
enum Opt {
    #[clap(subcommand)]
    Cluster(ClusterOpt),

    #[clap(subcommand)]
    Client(ClientOpt),
}

fn main() -> Result<()> {
    let opt = Opt::parse();
    run(opt)
}

#[tokio::main]
async fn run(opt: Opt) -> Result<()> {
    match opt {
        Opt::Cluster(cluster_opt) => cluster::run(cluster_opt)?,
        Opt::Client(client_opt) => client::run(client_opt).await?,
    }
    Ok(())
}
