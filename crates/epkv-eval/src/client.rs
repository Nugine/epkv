use epkv_protocol::cs;
use epkv_protocol::rpc::RpcClientConfig;
use epkv_utils::display::display_bytes;
use epkv_utils::utf8;

use std::net::SocketAddr;
use std::time::Instant;

use anyhow::Result;
use serde::Serialize;

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

fn default_rpc_client_config() -> RpcClientConfig {
    RpcClientConfig {
        max_frame_length: 16777216, // 16 MiB
        op_chan_size: 1024,
        forward_chan_size: 1024,
    }
}

pub async fn run(opt: Opt) -> Result<()> {
    let server = {
        let remote_addr = opt.server;
        let rpc_client_config = default_rpc_client_config();
        cs::Server::connect(remote_addr, &rpc_client_config).await?
    };

    let t0 = opt.debug_time.then(Instant::now);

    match opt.cmd {
        Command::GetMetrics { .. } => {
            let args = cs::GetMetricsArgs {};
            let output = server.get_metrics(args).await?;
            println!("{}", pretty_json(&output)?);
        }
        Command::Get { key, .. } => {
            let args = cs::GetArgs { key: key.into() };
            let output = server.get(args).await?;
            match output.value {
                Some(val) => println!("{}", display_bytes(&*val)),
                None => {}
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
        let duration = t1 - t0;
        eprintln!("time: {:?}", duration);
    }

    Ok(())
}

fn pretty_json<T: Serialize>(value: &T) -> Result<String> {
    let mut buf = Vec::new();
    let formatter = serde_json::ser::PrettyFormatter::with_indent("    ".as_ref());
    let mut serializer: _ = serde_json::Serializer::with_formatter(&mut buf, formatter.clone());
    value.serialize(&mut serializer)?;
    let ans = utf8::vec_to_string(buf)?;
    Ok(ans)
}
