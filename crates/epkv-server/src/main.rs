#![forbid(unsafe_code)]
#![deny(
    clippy::all,
    clippy::as_conversions,
    clippy::float_arithmetic,
    clippy::integer_arithmetic,
    clippy::must_use_candidate
)]
#![warn(clippy::todo)]

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use camino::Utf8PathBuf;
use clap::StructOpt;

#[derive(Debug, clap::Parser)]
struct Opt {
    #[clap(long)]
    config: Utf8PathBuf,
}

fn main() {
    let opt = Opt::parse();
    println!("{:?}", opt);
}
