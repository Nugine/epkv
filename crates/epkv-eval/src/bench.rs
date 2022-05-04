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
    output: Utf8PathBuf,

    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Debug, clap::Subcommand)]
pub enum Command {
    Case1 {
        #[clap(long)]
        key_size: usize,
        #[clap(long)]
        value_size: usize,
        #[clap(long)]
        cmd_count: usize,
        #[clap(long)]
        batch_size: usize,
    },
    Case2 {
        #[clap(long)]
        key_size: usize,
        #[clap(long)]
        value_size: usize,
        #[clap(long)]
        cmd_count: usize,
        #[clap(long)]
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

    if let Some(parent) = opt.output.parent() {
        fs::create_dir_all(parent)?;
    }

    match opt.cmd {
        Command::Case1 { key_size, value_size, cmd_count, batch_size } => {
            case1(&config, &opt.output, key_size, value_size, cmd_count, batch_size).await?
        }
        Command::Case2 { key_size, value_size, cmd_count, batch_size } => {
            case2(&config, &opt.output, key_size, value_size, cmd_count, batch_size).await?
        }
    }

    Ok(())
}

pub async fn case1(
    config: &Config,
    output: &Utf8Path,
    key_size: usize,
    value_size: usize,
    cmd_count: usize,
    batch_size: usize,
) -> Result<()> {
    #[allow(clippy::integer_arithmetic)]
    {
        ensure!(cmd_count % batch_size == 0);
    }

    let server = {
        let first = config.servers.iter().next().unwrap();
        let remote_addr = SocketAddr::from((first.1.ip, first.1.client_port));
        let rpc_client_config = crate::default_rpc_client_config();
        cs::Server::connect(remote_addr, &rpc_client_config).await?
    };

    let key = crate::random_bytes(key_size);
    let value = crate::random_bytes(value_size);
    let args = || cs::SetArgs { key: key.clone(), value: value.clone() };

    let cluster_metrics_before = get_cluster_metrics(config).await?;

    let t0 = Instant::now();

    for _ in 0..(cmd_count.wrapping_div(batch_size)) {
        let futures: _ = (0..batch_size).map(|_| server.set(args()));
        let results: _ = join_all(futures).await;
        for result in results {
            result?;
        }
    }

    let t1 = Instant::now();

    drop(server);

    #[allow(clippy::float_arithmetic)]
    let time_ms = (t1 - t0).as_secs_f64() * 1000.0;

    let cluster_metrics_after = get_cluster_metrics(config).await?;

    let diff = diff_cluster_metrics(&cluster_metrics_before, &cluster_metrics_after)?;

    let result = json!({
        "args": {
            "key_size": key_size,
            "value_size": value_size,
            "cmd_count": cmd_count,
            "batch_size": batch_size,
        },
        "time_ms": time_ms,
        "cluster_metrics": {
            "before": cluster_metrics_before,
            "after": cluster_metrics_after,
        },
        "diff": diff,
    });

    save_result(output, &result)?;

    Ok(())
}

pub async fn case2(
    config: &Config,
    output: &Utf8Path,
    key_size: usize,
    value_size: usize,
    cmd_count: usize,
    batch_size: usize,
) -> Result<()> {
    #[allow(clippy::integer_arithmetic)]
    {
        ensure!(cmd_count % batch_size == 0);
    }

    let server = {
        let first = config.servers.iter().next().unwrap();
        let remote_addr = SocketAddr::from((first.1.ip, first.1.client_port));
        let rpc_client_config = crate::default_rpc_client_config();
        cs::Server::connect(remote_addr, &rpc_client_config).await?
    };

    let key = crate::random_bytes(key_size);
    let value = crate::random_bytes(value_size);

    server.set(cs::SetArgs { key: key.clone(), value: value.clone() }).await?;

    let cluster_metrics_before = get_cluster_metrics(config).await?;

    let t0 = Instant::now();

    for _ in 0..(cmd_count.wrapping_div(batch_size)) {
        let futures: _ = (0..batch_size).map(|_| server.get(cs::GetArgs { key: key.clone() }));
        let results: _ = join_all(futures).await;
        for result in results {
            result?;
        }
    }

    let t1 = Instant::now();

    drop(server);

    #[allow(clippy::float_arithmetic)]
    let time_ms = (t1 - t0).as_secs_f64() * 1000.0;

    let cluster_metrics_after = get_cluster_metrics(config).await?;

    let diff = diff_cluster_metrics(&cluster_metrics_before, &cluster_metrics_after)?;

    let result = json!({
        "args": {
            "key_size": key_size,
            "value_size": value_size,
            "cmd_count": cmd_count,
            "batch_size": batch_size,
        },
        "time_ms": time_ms,
        "cluster_metrics": {
            "before": cluster_metrics_before,
            "after": cluster_metrics_after,
        },
        "diff": diff
    });

    save_result(output, &result)?;

    Ok(())
}

async fn get_cluster_metrics(config: &Config) -> Result<BTreeMap<String, cs::GetMetricsOutput>> {
    let rpc_client_config = crate::default_rpc_client_config();
    let mut map = BTreeMap::new();
    for (name, c) in &config.servers {
        let remote_addr = SocketAddr::from((c.ip, c.client_port));
        let server = cs::Server::connect(remote_addr, &rpc_client_config).await?;
        let metrics = server.get_metrics(cs::GetMetricsArgs {}).await?;
        map.insert(name.to_owned(), metrics);
    }
    Ok(map)
}

#[allow(clippy::integer_arithmetic, clippy::float_arithmetic, clippy::as_conversions)]
fn diff_cluster_metrics(
    before: &BTreeMap<String, cs::GetMetricsOutput>,
    after: &BTreeMap<String, cs::GetMetricsOutput>,
) -> Result<serde_json::Value> {
    let mut msg_count = 0;
    let mut msg_total_size = 0;
    let mut batched_cmd_count = 0;
    let mut single_cmd_count = 0;

    for (name, rhs) in after {
        let lhs = &before[name];
        msg_count += rhs.network_msg_count - lhs.network_msg_count;
        msg_total_size += rhs.network_msg_total_size - lhs.network_msg_total_size;
        batched_cmd_count += rhs.server_batched_cmd_count - lhs.server_batched_cmd_count;
        single_cmd_count += rhs.server_single_cmd_count - lhs.server_single_cmd_count;
    }

    let avg_transmission_per_single_cmd = msg_total_size as f64 / single_cmd_count as f64;
    let avg_transmission_per_batched_cmd = msg_total_size as f64 / batched_cmd_count as f64;
    let avg_transmission_per_msg = msg_total_size as f64 / msg_count as f64;
    let avg_msg_count_per_single_cmd = msg_count as f64 / single_cmd_count as f64;
    let avg_msg_count_per_batched_cmd = msg_count as f64 / batched_cmd_count as f64;

    Ok(json!({
        "msg_count": msg_count,
        "msg_total_size": msg_total_size,
        "batched_cmd_count": batched_cmd_count,
        "single_cmd_count": single_cmd_count,
        "avg_transmission_per_single_cmd": avg_transmission_per_single_cmd,
        "avg_transmission_per_batched_cmd": avg_transmission_per_batched_cmd,
        "avg_transmission_per_msg": avg_transmission_per_msg,
        "avg_msg_count_per_single_cmd": avg_msg_count_per_single_cmd,
        "avg_msg_count_per_batched_cmd": avg_msg_count_per_batched_cmd,
    }))
}

fn save_result(output: &Utf8Path, value: &serde_json::Value) -> Result<()> {
    let content = crate::pretty_json(&value)?;

    println!("{}", content);

    fs::write(output, content).with_context(|| format!("failed to write result file {output}"))?;

    Ok(())
}
