use bytes::Bytes;
use crossbeam_queue::ArrayQueue;
use epkv_protocol::cs;
use epkv_utils::asc::Asc;
use epkv_utils::cast::NumericCast;
use epkv_utils::clone;
use epkv_utils::config::read_config_file;
use rand::Rng;

use std::collections::BTreeMap;
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use anyhow::{ensure, Context, Result};
use camino::{Utf8Path, Utf8PathBuf};
use futures_util::future::join_all;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::spawn;
use tokio::task::JoinHandle;

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
    batch_size: usize,
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

    for (name, rhs) in after {
        let lhs = &before[name];
        msg_count += rhs.network_msg_count - lhs.network_msg_count;
        msg_total_size += rhs.network_msg_total_size - lhs.network_msg_total_size;
        batched_cmd_count += rhs.server_batched_cmd_count - lhs.server_batched_cmd_count;
        single_cmd_count += rhs.server_single_cmd_count - lhs.server_single_cmd_count;
        preaccept_fast_path += rhs.replica_preaccept_fast_path - lhs.replica_preaccept_fast_path;
        preaccept_slow_path += rhs.replica_preaccept_slow_path - lhs.replica_preaccept_slow_path;
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
        ensure!(args.cmd_count % (config.servers.len() * args.batch_size) == 0);
        ensure!((0..=100).contains(&args.conflict_rate));
    }

    let servers = {
        let rpc_client_config = crate::default_rpc_client_config();
        let mut servers = Vec::new();
        for (_, remote_addr) in config.iter_remote_addr() {
            let server = cs::Server::connect(remote_addr, &rpc_client_config).await?;
            servers.push(server);
        }
        servers
    };

    let common_key = rand::random::<u64>();
    let unique_key_gen = Asc::new(AtomicU64::new(common_key));
    let value = crate::random_bytes(args.value_size);

    let latency_us_queue: _ = Asc::new(ArrayQueue::<u64>::new(args.cmd_count));

    let mut tasks: Vec<JoinHandle<Result<()>>> = Vec::with_capacity(servers.len());

    let cluster_metrics_before = get_cluster_metrics(config).await?;

    let t0 = Instant::now();

    for server in servers {
        let cmd_count = args.cmd_count.wrapping_div(config.servers.len());
        let batch_size = args.batch_size;
        let rate = args.conflict_rate;

        clone!(unique_key_gen, value, latency_us_queue);

        let task = spawn(async move {
            for _ in 0..(cmd_count.wrapping_div(batch_size)) {
                let futures: _ = (0..batch_size).map(|_| {
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

                    let server = &server;
                    let latency_us_queue = &*latency_us_queue;

                    let t0 = Instant::now();
                    async move {
                        server.set(cs::SetArgs { key, value }).await?;
                        let t1 = Instant::now();
                        latency_us_queue.push((t1 - t0).as_micros().numeric_cast()).unwrap();
                        <Result<()>>::Ok(())
                    }
                });
                let results: _ = join_all(futures).await;
                for result in results {
                    result?;
                }
            }

            Ok(())
        });
        tasks.push(task);
    }
    for task in tasks {
        task.await?.unwrap();
    }

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

    #[allow(clippy::integer_arithmetic)]
    let latency = {
        let min = latency_us.first().unwrap();
        let q1 = latency_us[args.cmd_count / 4];
        let q2 = latency_us[args.cmd_count / 2];
        let q3 = latency_us[args.cmd_count * 3 / 4];
        let max = latency_us.last().unwrap();
        let p99 = latency_us[args.cmd_count * 99 / 100];

        json!({
            "min": min,
            "q1": q1,
            "q2": q2,
            "q3": q3,
            "max": max,
            "p99": p99,
        })
    };

    let result = json!({
        "args": args,
        "time_ms": time_ms,
        "cluster_metrics": {
            "before": cluster_metrics_before,
            "after": cluster_metrics_after,
        },
        "diff": diff,
        "latency": latency,
    });

    Ok(result)
}
