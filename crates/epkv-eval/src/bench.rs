use epkv_protocol::cs;
use epkv_utils::asc::Asc;
use epkv_utils::cast::NumericCast;
use epkv_utils::clone;
use epkv_utils::config::read_config_file;

use std::collections::BTreeMap;
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{ensure, Context, Result};
use bytes::Bytes;
use camino::{Utf8Path, Utf8PathBuf};
use crossbeam_queue::ArrayQueue;
use futures_util::future::join_all;
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::spawn;
use wgp::WaitGroup;

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
    Case1(Case1),
    Case2(Case2),
    Case3(Case3),
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

impl Config {
    fn iter_remote_addr(&self) -> impl Iterator<Item = (String, SocketAddr)> + '_ {
        self.servers.iter().map(|(name, c)| {
            let remote_addr = SocketAddr::from((c.ip, c.client_port));
            (name.to_owned(), remote_addr)
        })
    }
}

#[derive(Debug, Serialize, Deserialize, clap::Args)]
pub struct Case1 {
    #[clap(long)]
    key_size: usize,
    #[clap(long)]
    value_size: usize,
    #[clap(long)]
    cmd_count: usize,
    #[clap(long)]
    batch_size: usize,
}

#[derive(Debug, Serialize, Deserialize, clap::Args)]
pub struct Case2 {
    #[clap(long)]
    key_size: usize,
    #[clap(long)]
    value_size: usize,
    #[clap(long)]
    cmd_count: usize,
    #[clap(long)]
    batch_size: usize,
}

#[derive(Debug, Serialize, Deserialize, clap::Args)]
pub struct Case3 {
    #[clap(long)]
    value_size: usize,
    #[clap(long)]
    cmd_count: usize,
    #[clap(long)]
    conflict_rate: usize,
}

pub async fn run(opt: Opt) -> Result<()> {
    let config: Config = read_config_file(&opt.config)
        .with_context(|| format!("failed to read config file {}", opt.config))?;

    if let Some(parent) = opt.output.parent() {
        fs::create_dir_all(parent)?;
    }

    let result = match opt.cmd {
        Command::Case1(args) => case1(&config, args).await?,
        Command::Case2(args) => case2(&config, args).await?,
        Command::Case3(args) => case3(&config, args).await?,
    };

    save_result(&opt.output, &result)?;

    Ok(())
}

pub async fn case1(config: &Config, args: Case1) -> Result<serde_json::Value> {
    #[allow(clippy::integer_arithmetic)]
    {
        ensure!(args.cmd_count % args.batch_size == 0);
    }

    let server = {
        let first = config.servers.iter().next().unwrap();
        let remote_addr = SocketAddr::from((first.1.ip, first.1.client_port));
        let rpc_client_config = crate::default_rpc_client_config();
        cs::Server::connect(remote_addr, &rpc_client_config).await?
    };

    let key = crate::random_bytes(args.key_size);
    let value = crate::random_bytes(args.value_size);

    let cluster_metrics_before = get_cluster_metrics(config).await?;

    let t0 = Instant::now();

    for _ in 0..(args.cmd_count.wrapping_div(args.batch_size)) {
        let set_args = || cs::SetArgs { key: key.clone(), value: value.clone() };
        let futures: _ = (0..args.batch_size).map(|_| server.set(set_args()));
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
        "args": args,
        "time_ms": time_ms,
        "cluster_metrics": {
            "before": cluster_metrics_before,
            "after": cluster_metrics_after,
        },
        "diff": diff,
    });

    Ok(result)
}

pub async fn case2(config: &Config, args: Case2) -> Result<serde_json::Value> {
    #[allow(clippy::integer_arithmetic)]
    {
        ensure!(args.cmd_count % args.batch_size == 0);
    }

    let server = {
        let first = config.servers.iter().next().unwrap();
        let remote_addr = SocketAddr::from((first.1.ip, first.1.client_port));
        let rpc_client_config = crate::default_rpc_client_config();
        cs::Server::connect(remote_addr, &rpc_client_config).await?
    };

    let key = crate::random_bytes(args.key_size);
    let value = crate::random_bytes(args.value_size);

    server.set(cs::SetArgs { key: key.clone(), value: value.clone() }).await?;

    let cluster_metrics_before = get_cluster_metrics(config).await?;

    let t0 = Instant::now();

    for _ in 0..(args.cmd_count.wrapping_div(args.batch_size)) {
        let futures: _ = (0..args.batch_size).map(|_| server.get(cs::GetArgs { key: key.clone() }));
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
        "args": args,
        "time_ms": time_ms,
        "cluster_metrics": {
            "before": cluster_metrics_before,
            "after": cluster_metrics_after,
        },
        "diff": diff
    });

    Ok(result)
}

async fn get_cluster_metrics(config: &Config) -> Result<BTreeMap<String, cs::GetMetricsOutput>> {
    let rpc_client_config = crate::default_rpc_client_config();
    let mut map = BTreeMap::new();
    for (name, remote_addr) in config.iter_remote_addr() {
        let server = cs::Server::connect(remote_addr, &rpc_client_config).await?;
        let metrics = server.get_metrics(cs::GetMetricsArgs {}).await?;
        map.insert(name, metrics);
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
    let mut preaccept_fast_path = 0;
    let mut preaccept_slow_path = 0;
    let mut recover_nop_count = 0;
    let mut recover_success_count = 0;

    for (name, rhs) in after {
        let lhs = &before[name];
        msg_count += rhs.network_msg_count - lhs.network_msg_count;
        msg_total_size += rhs.network_msg_total_size - lhs.network_msg_total_size;
        batched_cmd_count += rhs.server_batched_cmd_count - lhs.server_batched_cmd_count;
        single_cmd_count += rhs.server_single_cmd_count - lhs.server_single_cmd_count;
        preaccept_fast_path += rhs.replica_preaccept_fast_path - lhs.replica_preaccept_fast_path;
        preaccept_slow_path += rhs.replica_preaccept_slow_path - lhs.replica_preaccept_slow_path;
        recover_nop_count += rhs.replica_recover_nop_count - lhs.replica_recover_nop_count;
        recover_success_count += rhs.replica_recover_success_count - lhs.replica_recover_success_count;
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
        "preaccept_fast_path": preaccept_fast_path,
        "preaccept_slow_path": preaccept_slow_path,
        "recover_nop_count": recover_nop_count,
        "recover_success_count": recover_success_count,
        "avg": {
            "avg_transmission_per_single_cmd": avg_transmission_per_single_cmd,
            "avg_transmission_per_batched_cmd": avg_transmission_per_batched_cmd,
            "avg_transmission_per_msg": avg_transmission_per_msg,
            "avg_msg_count_per_single_cmd": avg_msg_count_per_single_cmd,
            "avg_msg_count_per_batched_cmd": avg_msg_count_per_batched_cmd,
        }
    }))
}

fn save_result(output: &Utf8Path, value: &serde_json::Value) -> Result<()> {
    let content = crate::pretty_json(&value)?;

    println!("{}", content);

    fs::write(output, content).with_context(|| format!("failed to write result file {output}"))?;

    Ok(())
}

pub async fn case3(config: &Config, args: Case3) -> Result<serde_json::Value> {
    #[allow(clippy::integer_arithmetic)]
    {
        ensure!(args.cmd_count % config.servers.len() == 0);
        ensure!((0..=100).contains(&args.conflict_rate));
    }

    let servers = {
        let rpc_client_config = crate::default_rpc_client_config();
        let mut servers = Vec::new();
        for (_, remote_addr) in config.iter_remote_addr() {
            let server = cs::Server::connect(remote_addr, &rpc_client_config).await?;
            servers.push(Asc::new(server));
        }
        servers
    };

    let common_key = rand::random::<u64>();
    let unique_key_gen = Asc::new(AtomicU64::new(common_key));
    let value = crate::random_bytes(args.value_size);

    let latency_us_queue: _ = Asc::new(ArrayQueue::<u64>::new(args.cmd_count));

    let wg = WaitGroup::new();

    {
        clone!(latency_us_queue);
        spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                println!("received {}", latency_us_queue.len());
            }
        });
    }

    let cluster_metrics_before = get_cluster_metrics(config).await?;

    let t0 = Instant::now();

    let mut tasks = Vec::with_capacity(args.cmd_count);

    for server in servers {
        let server_cmd_count = args.cmd_count.wrapping_div(config.servers.len());
        let rate = args.conflict_rate;

        for _ in 0..server_cmd_count {
            let key = if rate == 0 {
                unique_key_gen.fetch_add(1, Ordering::Relaxed).wrapping_add(1)
            } else if rate == 100 {
                common_key
            } else {
                let magic: usize = rand::thread_rng().gen_range(0..100);
                if magic < rate {
                    common_key
                } else {
                    unique_key_gen.fetch_add(1, Ordering::Relaxed).wrapping_add(1)
                }
            };

            let key = Bytes::copy_from_slice(&key.to_be_bytes());
            let value = value.clone();
            clone!(server);
            tasks.push((server, key, value));
        }
    }

    for (server, key, value) in tasks {
        clone!(latency_us_queue);
        let working = wg.working();

        spawn(async move {
            let t0 = Instant::now();
            server.set(cs::SetArgs { key, value }).await.unwrap();
            let t1 = Instant::now();
            latency_us_queue.push((t1 - t0).as_micros().numeric_cast()).unwrap();
            drop(working);
        });
    }

    wg.wait_owned().await;

    let t1 = Instant::now();

    let cluster_metrics_after = get_cluster_metrics(config).await?;

    #[allow(clippy::float_arithmetic)]
    let time_ms = (t1 - t0).as_secs_f64() * 1000.0;

    let diff = diff_cluster_metrics(&cluster_metrics_before, &cluster_metrics_after)?;

    let latency_us = {
        let mut v = Vec::with_capacity(args.cmd_count);
        for _ in 0..args.cmd_count {
            v.push(latency_us_queue.pop().unwrap());
        }
        v.sort_unstable();
        v
    };

    #[allow(clippy::integer_arithmetic, clippy::float_arithmetic, clippy::as_conversions)]
    let latency = {
        let min = latency_us.first().copied().unwrap();
        let p25 = latency_us[args.cmd_count / 4];
        let p50 = latency_us[args.cmd_count / 2];
        let p75 = latency_us[args.cmd_count * 3 / 4];
        let p99 = latency_us[args.cmd_count * 99 / 100];
        let p999 = latency_us[args.cmd_count * 999 / 1000];
        let max = latency_us.last().copied().unwrap();

        json!({
            "min": min as f64 / 1000.0, // ms
            "p25": p25 as f64 / 1000.0, // ms
            "p50": p50 as f64 / 1000.0, // ms
            "p75": p75 as f64 / 1000.0, // ms
            "p99": p99 as f64 / 1000.0, // ms
            "p999": p999 as f64 / 1000.0, // ms
            "max": max as f64 / 1000.0, // ms
        })
    };

    #[allow(clippy::float_arithmetic, clippy::as_conversions)]
    let estimated_qps = args.cmd_count as f64 / time_ms * 1000.0;

    let result = json!({
        "args": args,
        "time_ms": time_ms,
        "cluster_metrics": {
            "before": cluster_metrics_before,
            "after": cluster_metrics_after,
        },
        "diff": diff,
        "latency": latency,
        "estimated_qps": estimated_qps,
    });

    Ok(result)
}
