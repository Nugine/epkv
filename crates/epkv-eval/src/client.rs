use epkv_protocol::cs;
use epkv_protocol::rpc::RpcClientConfig;
use epkv_utils::utf8;
use serde::Serialize;

use std::net::SocketAddr;

use anyhow::Result;

#[derive(Debug, clap::Subcommand)]
pub enum ClientOpt {
    GetMetrics {
        #[clap(long)]
        server: SocketAddr,
    },
}

fn default_rpc_client_config() -> RpcClientConfig {
    RpcClientConfig {
        max_frame_length: 16777216, // 16 MiB
        op_chan_size: 1024,
        forward_chan_size: 1024,
    }
}

pub async fn run(opt: ClientOpt) -> Result<()> {
    let remote_addr = match opt {
        ClientOpt::GetMetrics { server } => server,
    };
    let rpc_client_config = default_rpc_client_config();
    let server = cs::Server::connect(remote_addr, &rpc_client_config).await?;

    match opt {
        ClientOpt::GetMetrics { .. } => {
            let output = server.get_metrics(cs::GetMetricsArgs {}).await?;
            println!("{}", pretty_json(&output)?);
        }
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
