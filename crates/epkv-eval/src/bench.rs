use epkv_protocol::cs;
use epkv_utils::clone;
use epkv_utils::config::read_config_file;

use std::collections::BTreeMap;
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{ensure, Context, Result};
use asc::Asc;
use bytes::Bytes;
use camino::{Utf8Path, Utf8PathBuf};
use crossbeam_queue::{ArrayQueue, SegQueue};
use futures_util::future::join_all;
use numeric_cast::NumericCast;
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
    Case4,
    Case5(Case5),
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
        Command::Case4 => {
            let cluster_metrics: _ = get_cluster_metrics(&config).await?;
            serde_json::to_value(&cluster_metrics)?
        }
        Command::Case5(args) => case5(&config, args).await?,
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
    let mut executed_single_cmd_count = 0;
    let mut executed_batched_cmd_count = 0;

    for (name, rhs) in after {
        let lhs = &before[name];
        msg_count += rhs.network_msg_count - lhs.network_msg_count;
        msg_total_size += rhs.network_msg_total_size - lhs.network_msg_total_size;
        batched_cmd_count += rhs.proposed_batched_cmd_count - lhs.proposed_batched_cmd_count;
        single_cmd_count += rhs.proposed_single_cmd_count - lhs.proposed_single_cmd_count;
        preaccept_fast_path += rhs.replica_preaccept_fast_path - lhs.replica_preaccept_fast_path;
        preaccept_slow_path += rhs.replica_preaccept_slow_path - lhs.replica_preaccept_slow_path;
        recover_nop_count += rhs.replica_recover_nop_count - lhs.replica_recover_nop_count;
        recover_success_count += rhs.replica_recover_success_count - lhs.replica_recover_success_count;
        executed_single_cmd_count += rhs.executed_single_cmd_count - lhs.executed_single_cmd_count;
        executed_batched_cmd_count += rhs.executed_batched_cmd_count - lhs.executed_batched_cmd_count;
    }

    let avg_transmission_per_single_cmd = msg_total_size as f64 / single_cmd_count as f64;
    let avg_transmission_per_batched_cmd = msg_total_size as f64 / batched_cmd_count as f64;
    let avg_transmission_per_msg = msg_total_size as f64 / msg_count as f64;
    let avg_msg_count_per_single_cmd = msg_count as f64 / single_cmd_count as f64;
    let avg_msg_count_per_batched_cmd = msg_count as f64 / batched_cmd_count as f64;

    Ok(json!({
        "cluster": {
            "msg_count": msg_count,
            "msg_total_size": msg_total_size,
            "batched_cmd_count": batched_cmd_count,
            "single_cmd_count": single_cmd_count,
            "preaccept_fast_path": preaccept_fast_path,
            "preaccept_slow_path": preaccept_slow_path,
            "recover_nop_count": recover_nop_count,
            "recover_success_count": recover_success_count,
            "executed_single_cmd_count": executed_single_cmd_count,
            "executed_batched_cmd_count": executed_batched_cmd_count,
        },
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

#[allow(clippy::integer_arithmetic, clippy::float_arithmetic, clippy::as_conversions)]
pub async fn case3(config: &Config, args: Case3) -> Result<serde_json::Value> {
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

    let mut gen = &RandomCmds::new(args.value_size, args.conflict_rate);

    let latency_us_queue: _ = Asc::new(ArrayQueue::<u64>::new(args.cmd_count));

    let wg = WaitGroup::new();

    let mut tasks = Vec::with_capacity(args.cmd_count);

    for server in servers {
        let server_cmd_count = args.cmd_count.wrapping_div(config.servers.len());

        for _ in 0..server_cmd_count {
            let cs::SetArgs { key, value } = gen.next().unwrap();
            clone!(server);
            tasks.push((server, key, value));
        }
    }
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

    let min = latency_us.first().copied().unwrap();
    let p25 = latency_us[args.cmd_count / 4];
    let p50 = latency_us[args.cmd_count / 2];
    let p75 = latency_us[args.cmd_count * 3 / 4];
    let p99 = latency_us[args.cmd_count * 99 / 100];
    let p99_9 = latency_us[args.cmd_count * 999 / 1000];
    let max = latency_us.last().copied().unwrap();

    let latency = {
        let min_ms = min as f64 / 1000.0;
        let p25_ms = p25 as f64 / 1000.0; // ms
        let p50_ms = p50 as f64 / 1000.0; // ms
        let p75_ms = p75 as f64 / 1000.0; // ms
        let p99_ms = p99 as f64 / 1000.0; // ms
        let p99_9_ms = p99_9 as f64 / 1000.0; // ms
        let max_ms = max as f64 / 1000.0;
        json!({
            "min": min_ms,
            "p25": p25_ms,
            "p50": p50_ms,
            "p75": p75_ms,
            "p99": p99_ms,
            "p99_9": p99_9_ms,
            "max": max_ms,
        })
    };

    let estimated_qps = {
        let by_p25 = (args.cmd_count / 4) as f64 / (p25 as f64) * 1e6;
        let by_p50 = (args.cmd_count / 2) as f64 / (p50 as f64) * 1e6;
        let by_p75 = (args.cmd_count * 3 / 4) as f64 / (p75 as f64) * 1e6;
        let by_wall_time = args.cmd_count as f64 / time_ms * 1000.0;
        json!({
            "by_p25": by_p25,
            "by_p50": by_p50,
            "by_p75": by_p75,
            "by_wall_time": by_wall_time,
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
        "estimated_qps": estimated_qps,
    });

    Ok(result)
}

#[derive(Debug, Serialize, Deserialize, clap::Args)]
pub struct Case5 {
    #[clap(long)]
    server_name: String,
    #[clap(long)]
    value_size: usize,
    #[clap(long)]
    conflict_rate: usize,
    #[clap(long)]
    interval_ms: u64,
    #[clap(long)]
    cmds_per_interval: usize,
}

#[allow(clippy::integer_arithmetic)]
pub async fn case5(config: &Config, args: Case5) -> Result<serde_json::Value> {
    {
        ensure!((0..=100).contains(&args.conflict_rate));
    }
    let server = {
        let conf = &config.servers[&args.server_name];
        let remote_addr = SocketAddr::from((conf.ip, conf.client_port));
        let rpc_client_config = crate::default_rpc_client_config();
        Arc::new(cs::Server::connect(remote_addr, &rpc_client_config).await?)
    };

    let completed = Arc::new(AtomicU64::new(0));
    let result_queue = Arc::new(SegQueue::<serde_json::Value>::new());
    {
        clone!(server, completed, result_queue);
        spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            let t0 = Instant::now();
            let mut prev_completed = 0;
            loop {
                interval.tick().await;

                let completed = completed.load(Ordering::SeqCst);
                let t = t0.elapsed();
                let time_us: u64 = t.as_micros().numeric_cast();

                let metrics = server.get_metrics(cs::GetMetricsArgs {}).await.unwrap();
                let delta = completed - prev_completed;

                result_queue.push(json!({
                    "time_us": time_us,
                    "completed": completed,
                    "completed_delta": delta,
                    "metrics": metrics,
                }));

                let time_s = t.as_secs_f64();
                println!("time: {time_s:>10.6}s, completed: {completed:>10}, delta: {delta:>10}");
                prev_completed = completed;
            }
        });
    }
    {
        let interval = tokio::time::interval(Duration::from_millis(args.interval_ms));
        let count = args.cmds_per_interval;
        let gen = RandomCmds::new(args.value_size, args.conflict_rate);
        spawn(async move {
            let mut interval = interval;
            let mut cmds = gen.take(count).collect::<Vec<_>>();
            loop {
                interval.tick().await;

                clone!(server, completed);
                spawn(async move {
                    let futures: _ = cmds.into_iter().map(|set_args| {
                        let server = &server;
                        let completed = &completed;
                        async move {
                            let result = server.set(set_args).await;
                            completed.fetch_add(1, Ordering::Relaxed);
                            result
                        }
                    });
                    let results: _ = join_all(futures).await;
                    for result in results {
                        result.unwrap();
                    }
                });

                cmds = gen.take(count).collect::<Vec<_>>();
            }
        });
    }
    {
        tokio::signal::ctrl_c().await.unwrap();
    }
    let mut data = Vec::with_capacity(result_queue.len());
    while let Some(result) = result_queue.pop() {
        data.push(result);
    }
    let result = json!({
        "args": args,
        "data": data,
    });
    Ok(result)
}

struct RandomCmds {
    rate: usize,
    value: Bytes,
    common_key: u64,
    unique_key_gen: AtomicU64,
}

impl RandomCmds {
    fn new(value_size: usize, conflict_rate: usize) -> Self {
        let common_key = rand::random::<u64>();
        let unique_key_gen = AtomicU64::new(common_key);
        let value = crate::random_bytes(value_size);
        Self { rate: conflict_rate, value, common_key, unique_key_gen }
    }
}

impl Iterator for &'_ RandomCmds {
    type Item = cs::SetArgs;
    #[allow(clippy::integer_arithmetic)]
    fn next(&mut self) -> Option<cs::SetArgs> {
        let key = if self.rate == 0 {
            self.unique_key_gen.fetch_add(1, Ordering::Relaxed) + 1
        } else if self.rate == 100 {
            self.common_key
        } else {
            let magic: usize = rand::thread_rng().gen_range(0..100);
            if magic < self.rate {
                self.common_key
            } else {
                self.unique_key_gen.fetch_add(1, Ordering::Relaxed) + 1
            }
        };
        let key = Bytes::copy_from_slice(&key.to_be_bytes());
        let value = self.value.clone();
        Some(cs::SetArgs { key, value })
    }
}
