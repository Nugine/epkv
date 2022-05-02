#![forbid(unsafe_code)]
#![deny(
    clippy::all,
    clippy::as_conversions,
    clippy::float_arithmetic,
    clippy::integer_arithmetic,
    clippy::must_use_candidate
)]
#![warn(clippy::todo, clippy::dbg_macro)]

use anyhow::Result;
use camino::Utf8PathBuf;
use clap::StructOpt;
use epkv_eval::cluster_generate;

#[derive(Debug, clap::Parser)]
enum Opt {
    #[clap(subcommand)]
    Cluster(ClusterOpt),
}

#[derive(Debug, clap::Subcommand)]
enum ClusterOpt {
    Generate {
        #[clap(long)]
        config: Utf8PathBuf,
        #[clap(long)]
        target: Utf8PathBuf,
    },
}

fn main() -> Result<()> {
    let opt = Opt::parse();
    match opt {
        Opt::Cluster(cluster_opt) => match cluster_opt {
            ClusterOpt::Generate { config, target } => {
                cluster_generate::run(&config, &target)?;
            }
        },
    };
    Ok(())
}
