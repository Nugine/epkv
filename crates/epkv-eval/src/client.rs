use epkv_protocol::cs;
use epkv_utils::display::display_bytes;

use std::net::SocketAddr;
use std::time::Instant;

use anyhow::Result;

#[derive(Debug, clap::Args)]
pub struct Opt {
    #[clap(long)]
    pub server: SocketAddr,

    #[clap(long)]
    pub debug_time: bool,

    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Debug, clap::Subcommand)]
pub enum Command {
    GetMetrics {},
    Get { key: String },
    Set { key: String, value: String },
    Del { key: String },
}

pub async fn run(opt: Opt) -> Result<()> {
    let server = {
        let remote_addr = opt.server;
        let rpc_client_config = crate::default_rpc_client_config();
        cs::Server::connect(remote_addr, &rpc_client_config).await?
    };

    let t0 = opt.debug_time.then(Instant::now);

    match opt.cmd {
        Command::GetMetrics { .. } => {
            let args = cs::GetMetricsArgs {};
            let output = server.get_metrics(args).await?;
            println!("{}", crate::pretty_json(&output)?);
        }
        Command::Get { key, .. } => {
            let args = cs::GetArgs { key: key.into() };
            let output = server.get(args).await?;
            if let Some(val) = output.value {
                println!("{}", display_bytes(&val))
            }
        }
        Command::Set { key, value, .. } => {
            let args = cs::SetArgs { key: key.into(), value: value.into() };
            let output = server.set(args).await?;
            let cs::SetOutput {} = output;
        }
        Command::Del { key, .. } => {
            let args = cs::DelArgs { key: key.into() };
            let output = server.del(args).await?;
            let cs::DelOutput {} = output;
        }
    }

    let t1 = Instant::now();

    if let Some(t0) = t0 {
        #[allow(clippy::arithmetic_side_effects)]
        let duration = t1 - t0;
        eprintln!("time: {duration:?}");
    }

    Ok(())
}
