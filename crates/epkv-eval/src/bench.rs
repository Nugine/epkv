use epkv_protocol::cs;
use epkv_utils::config::read_config_file;
use futures_util::future::join_all;
use serde_json::json;

use std::collections::BTreeMap;
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::time::Instant;

use anyhow::{ensure, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use serde::{Deserialize, Serialize};

#[derive(Debug, clap::Args)]
pub struct Opt {
    #[clap(long)]
    config: Utf8PathBuf,

    #[clap(long)]
    target: Utf8PathBuf,

    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Debug, clap::Subcommand)]
pub enum Command {
    Case1 {
        key_size: usize,
        value_size: usize,
        cmd_count: usize,
        batch_size: usize,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub servers: BTreeMap<String, RemoteServerConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RemoteServerConfig {
    pub ip: IpAddr,
    pub client_port: u16,
}

pub async fn run(opt: Opt) -> Result<()> {
    let config: Config = read_config_file(&opt.config)
        .with_context(|| format!("failed to read config file {}", opt.config))?;

    fs::create_dir_all(&opt.target)?;

    match opt.cmd {
        Command::Case1 { key_size, value_size, cmd_count, batch_size } => {
            case1(&config, &opt.target, key_size, value_size, cmd_count, batch_size).await?
        }
    }

    Ok(())
}

pub async fn case1(
    config: &Config,
    target_dir: &Utf8Path,
    key_size: usize,
    value_size: usize,
    cmd_count: usize,
    batch_size: usize,
) -> Result<()> {
    #[allow(clippy::integer_arithmetic)]
    {
        ensure!(cmd_count % batch_size == 0);
    }

    let (server_name, server) = {
        let first = config.servers.iter().next().unwrap();
        let server_name = first.0.as_str();
        let client_addr = SocketAddr::from((first.1.ip, first.1.client_port));
        let rpc_client_config = crate::default_rpc_client_config();
        let server = cs::Server::connect(client_addr, &rpc_client_config).await?;
        (server_name, server)
    };

    let key = crate::random_bytes(key_size);
    let value = crate::random_bytes(value_size);
    let args = move || cs::SetArgs { key: key.clone(), value: value.clone() };

    let t0 = Instant::now();

    for _ in 0..(cmd_count.wrapping_div(batch_size)) {
        let futures: _ = (0..batch_size).map(|_| server.set(args()));
        let results: _ = join_all(futures).await;
        for result in results {
            result?;
        }
    }

    let t1 = Instant::now();

    #[allow(clippy::float_arithmetic)]
    let time_ms = (t1 - t0).as_secs_f64() * 1000.0;

    let metrics = server.get_metrics(cs::GetMetricsArgs {}).await?;

    {
        let result = json!({
            "server_name": server_name,
            "metrics": metrics,
            "time_ms": time_ms,
        });

        let result_path = target_dir.join("case1.json");

        let content = crate::pretty_json(&result)?;

        fs::write(&result_path, content)
            .with_context(|| format!("failed to write result file {result_path}"))?;
    }

    Ok(())
}
