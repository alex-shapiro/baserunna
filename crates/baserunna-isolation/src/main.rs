//! Tenant isolation harness for the node HTTP API.
//!
//! Verifies that a node continues serving a victim tenant while an attacker runs a heavy workload.
//!
//! Usage:
//!
//! ```text
//! isolation --node http://127.0.0.1:8080 --cgroup /sys/fs/cgroup/iso/node \
//!     --workload <name> [--variant small|attack] --out result.json [--node-pid PID]
//! ```
//!
//! The cost benchmarks `event_scan`, `kv_throughput`, and `idle_cost` take no `--variant`.
//!
//! Before setup, the harness records idle node memory. Next, the harness runs three phases:
//! - Phase A: victim workload alone.
//! - Phase B: attacker workload alone, with cgroup memory sampled every 10 ms.
//! - Phase C: attacker and victim workloads together.
//!
//! The output JSON contains all measurements and pass-criterion verdicts.

mod cgroup;
mod event_scan;
mod http;
mod idle_cost;
mod kv_throughput;
mod victim;
mod workloads;

use cgroup::{MemorySampler, NodeCgroup};
use http::{NodeApi, RequestRecord};
use serde_json::{Value as Json, json};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};
use workloads::{AttackContext, AttackOutcome, Round, Variant};

const MIB: u64 = 1 << 20;
/// Expected maximum time for the node to answer a request.
const REQUEST_DEADLINE: Duration = Duration::from_secs(10);
/// Latest time for the node to reject an over-limit request. Criterion 4 fails after this time.
const REJECTION_LIMIT_MS: f64 = 2.0 * REQUEST_DEADLINE.as_millis() as f64;
const VICTIM_ALONE_DURATION: Duration = Duration::from_secs(15);
/// Combined request rate of all victim clients, below the per-user rate limit.
const VICTIM_RPS: f64 = 400.0;
const EVENT_SCAN_ROUNDS: usize = 20;
const KV_CLIENTS: usize = 256;

#[tokio::main]
async fn main() {
    let cli = parse_args();
    let started = Instant::now();
    let mut report = json!({
        "workload": cli.workload, "variant": cli.variant, "node": cli.node_url, "cgroup": cli.cgroup.0,
        "request_deadline_ms": REQUEST_DEADLINE.as_millis() as u64,
    });
    let monitor =
        NodeMonitor { pid: cli.node_pid, cgroup: cli.cgroup.clone(), oom_kills_before: cli.cgroup.node_oom_kills() };
    if let Err(err) = run(&cli, &monitor, &mut report).await {
        eprintln!("isolation: {err}");
        report["error"] = json!(err);
    }
    report["node_after"] = monitor.report();
    report["total_s"] = json!(started.elapsed().as_secs_f64());
    let text = serde_json::to_string_pretty(&report).expect("json");
    std::fs::write(&cli.output_path, text + "\n")
        .unwrap_or_else(|err| panic!("cannot write {}: {err}", cli.output_path));
    eprintln!("isolation: wrote {}", cli.output_path);
}

async fn run(cli: &CliArgs, monitor: &NodeMonitor, report: &mut Json) -> Result<(), String> {
    let client_timeout = 6 * REQUEST_DEADLINE + Duration::from_secs(60);
    let api = Arc::new(NodeApi::new(&cli.node_url, client_timeout));
    api.wait_healthy(Duration::from_secs(60)).await?;
    let cost_report = match cli.workload.as_str() {
        "idle_cost" => Some(idle_cost::run(&api, &cli.cgroup).await?),
        "kv_throughput" => Some(kv_throughput::run(&api, &cli.node_url, &cli.cgroup, KV_CLIENTS).await?),
        "event_scan" => {
            let owner = api.create_owner("attacker@iso.test", "attacker").await?;
            Some(event_scan::run(&api, &owner, &cli.cgroup, EVENT_SCAN_ROUNDS).await?)
        }
        _ => None,
    };
    if let Some(cost_report) = cost_report {
        report[cli.workload.as_str()] = cost_report;
        report["node_running"] = json!(monitor.is_running());
        return Ok(());
    }
    let attacker = api.create_owner("attacker@iso.test", "attacker").await?;
    let workload = workloads::by_name(&cli.workload).ok_or_else(|| format!("unknown workload {:?}", cli.workload))?;
    let idle_node_bytes = cli.cgroup.memory_current_bytes().ok_or("cannot read the node's memory.current")?;
    report["idle_node_bytes"] = json!(idle_node_bytes);
    let variant = cli.variant.ok_or("this workload requires --variant")?;
    let victim_owner = api.create_owner("victim@iso.test", "victim").await?;
    victim::prepare(&api, &victim_owner).await?;
    let ctx = AttackContext { api: api.clone(), attacker, variant, cgroup: cli.cgroup.clone() };
    let prepare_started = Instant::now();
    let prepare_report = json!({
        "alone": workload.prepare(&ctx, Round::Alone).await?,
        "with_victim": workload.prepare(&ctx, Round::WithVictim).await?,
        "seconds": prepare_started.elapsed().as_secs_f64(),
    });
    report["prepare"] = prepare_report;
    if !monitor.is_running() {
        report["conditions"] = evaluate_criteria(monitor, None, None, None, variant);
        return Err("node stopped during setup".into());
    }

    let stop_victim = Arc::new(AtomicBool::new(false));
    let victim_task = tokio::spawn(victim::run(api.clone(), victim_owner.clone(), stop_victim.clone(), VICTIM_RPS));
    tokio::time::sleep(VICTIM_ALONE_DURATION).await;
    stop_victim.store(true, std::sync::atomic::Ordering::Relaxed);
    let victim_alone = victim_task.await.map_err(|err| err.to_string())?;
    report["phase_a_victim_alone"] = json!(victim_alone);

    let sampler = MemorySampler::start(cli.cgroup.clone());
    let phase_b_started = Instant::now();
    let phase_b_result = workload.attack(&ctx, Round::Alone).await;
    let phase_b_memory = sampler.finish();
    let phase_b_outcome = phase_b_result?;
    report["phase_b_attacker_alone"] = json!({
        "seconds": phase_b_started.elapsed().as_secs_f64(),
        "memory": phase_b_memory,
        "attack": attack_summary(&phase_b_outcome),
    });

    let mut phase_c_outcome = None;
    let mut victim_during_attack = None;
    if monitor.is_running() {
        let stop_victim = Arc::new(AtomicBool::new(false));
        let victim_task = tokio::spawn(victim::run(api.clone(), victim_owner, stop_victim.clone(), VICTIM_RPS));
        let sampler = MemorySampler::start(cli.cgroup.clone());
        let phase_c_started = Instant::now();
        let phase_c_result = workload.attack(&ctx, Round::WithVictim).await;
        // Run the victim for at least 2 seconds so short attacks produce enough latency samples.
        tokio::time::sleep(Duration::from_secs(2).saturating_sub(phase_c_started.elapsed())).await;
        stop_victim.store(true, std::sync::atomic::Ordering::Relaxed);
        let victim_stats = victim_task.await.map_err(|err| err.to_string())?;
        let phase_c_memory = sampler.finish();
        let phase_c_attack = phase_c_result?;
        report["phase_c_attack_with_victim"] = json!({
            "seconds": phase_c_started.elapsed().as_secs_f64(),
            "memory": phase_c_memory,
            "victim": victim_stats,
            "attack": attack_summary(&phase_c_attack),
        });
        phase_c_outcome = Some(phase_c_attack);
        victim_during_attack = Some(victim_stats);
    }
    report["phase_b_growth_from_idle_mib"] =
        json!(phase_b_memory.peak_bytes.saturating_sub(idle_node_bytes) as f64 / MIB as f64);
    report["conditions"] = evaluate_criteria(
        monitor,
        Some(&victim_alone),
        victim_during_attack.as_ref(),
        Some((&phase_b_outcome, phase_c_outcome.as_ref())),
        variant,
    );
    Ok(())
}

/// Evaluates the five milestone 0 pass criteria from `ISOLATION_PLAN.md`.
///
/// Sets a criterion to `null` if a required phase did not run or if the criterion does not apply
/// to the variant.
fn evaluate_criteria(
    monitor: &NodeMonitor,
    victim_alone: Option<&victim::VictimStats>,
    victim_during_attack: Option<&victim::VictimStats>,
    attacks: Option<(&AttackOutcome, Option<&AttackOutcome>)>,
    variant: Variant,
) -> Json {
    let node_running = monitor.is_running();
    let victim_succeeds = victim_during_attack.map(|stats| stats.failures == 0);
    let victim_latency = match (victim_alone, victim_during_attack) {
        (Some(alone), Some(attacked)) => {
            let p99_limit_ms = (2.0 * alone.p99_ms).max(alone.p99_ms + 50.0);
            Some(json!({"pass": attacked.p99_ms <= p99_limit_ms, "p99_limit_ms": p99_limit_ms,
                "p99_alone_ms": alone.p99_ms, "p99_attacked_ms": attacked.p99_ms}))
        }
        _ => None,
    };
    let all_requests: Vec<&RequestRecord> = attacks
        .map(|(phase_b, phase_c)| {
            phase_b.requests.iter().chain(phase_c.into_iter().flat_map(|outcome| outcome.requests.iter())).collect()
        })
        .unwrap_or_default();
    let rejections_are_http_errors = attacks.map(|_| {
        all_requests.iter().all(|record| {
            record.expected
                || (record.status.is_some_and(|code| code >= 400) && record.elapsed_ms <= REJECTION_LIMIT_MS)
        })
    });
    let small_variant_succeeds = (variant == Variant::Small).then(|| {
        attacks.is_some_and(|(phase_b, phase_c)| {
            phase_b.completed
                && phase_c.is_some_and(|outcome| outcome.completed)
                && all_requests.iter().all(|record| record.expected)
        }) && node_running
    });
    let pass = node_running
        && victim_succeeds == Some(true)
        && victim_latency.as_ref().is_some_and(|stats| stats["pass"] == json!(true))
        && rejections_are_http_errors == Some(true)
        && small_variant_succeeds != Some(false);
    json!({
        "1_node_running": node_running,
        "2_victim_requests_succeed": victim_succeeds,
        "3_victim_p99_within_limit": victim_latency,
        "4_rejections_are_http_errors": rejections_are_http_errors,
        "5_small_variant_succeeds": small_variant_succeeds,
        "pass": pass,
    })
}

fn attack_summary(outcome: &AttackOutcome) -> Json {
    let dropped_requests: Vec<&RequestRecord> =
        outcome.requests.iter().filter(|record| record.status.is_none()).collect();
    let rejected_requests: Vec<&RequestRecord> = outcome
        .requests
        .iter()
        .filter(|record| record.status.is_some_and(|code| code >= 400) && !record.expected)
        .collect();
    let late_rejections: Vec<&RequestRecord> =
        rejected_requests.iter().copied().filter(|record| record.elapsed_ms > REJECTION_LIMIT_MS).collect();
    json!({
        "requests": outcome.requests,
        "completed": outcome.completed,
        "notes": outcome.notes,
        "expected_responses": outcome.requests.iter().filter(|record| record.expected).count(),
        "rejections": rejected_requests.len(),
        "late_rejections": late_rejections.len(),
        "dropped_connections": dropped_requests.len(),
        "unexpected_success_statuses": outcome.requests.iter().filter(|record| !record.expected && record.status.is_some_and(|code| code < 400)).count(),
    })
}

/// Tracks node process liveness and new OOM kills in the node leaf cgroup.
struct NodeMonitor {
    pid: Option<u32>,
    cgroup: NodeCgroup,
    oom_kills_before: Option<u64>,
}

impl NodeMonitor {
    fn is_alive(&self) -> bool {
        self.pid.is_none_or(|pid| std::path::Path::new(&format!("/proc/{pid}")).exists() && !is_zombie(pid))
    }

    /// Returns true if the node process is alive and the kernel has not stopped it.
    fn is_running(&self) -> bool {
        self.is_alive() && self.new_oom_kills() == 0
    }

    fn new_oom_kills(&self) -> u64 {
        match (self.oom_kills_before, self.cgroup.node_oom_kills()) {
            (Some(before), Some(after)) => after.saturating_sub(before),
            _ => 0,
        }
    }

    fn report(&self) -> Json {
        json!({"pid": self.pid, "process_alive": self.is_alive(), "oom_kills": self.new_oom_kills(),
            "cgroup_exists": self.cgroup.exists()})
    }
}

fn is_zombie(pid: u32) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .ok()
        .and_then(|stat| stat.rsplit_once(')').map(|(_, rest)| rest.trim_start().starts_with('Z')))
        .unwrap_or(false)
}

struct CliArgs {
    node_url: String,
    cgroup: NodeCgroup,
    workload: String,
    variant: Option<Variant>,
    output_path: String,
    node_pid: Option<u32>,
}

fn parse_args() -> CliArgs {
    let mut cli = CliArgs {
        node_url: "http://127.0.0.1:8080".into(),
        cgroup: NodeCgroup("/sys/fs/cgroup/iso/node".into()),
        workload: String::new(),
        variant: None,
        output_path: String::new(),
        node_pid: None,
    };
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let value = args.next().unwrap_or_else(|| usage());
        match flag.as_str() {
            "--node" => cli.node_url = value,
            "--cgroup" => cli.cgroup = NodeCgroup(value.into()),
            "--workload" => cli.workload = value,
            "--variant" => cli.variant = Some(Variant::parse(&value).unwrap_or_else(|| usage())),
            "--out" => cli.output_path = value,
            "--node-pid" => cli.node_pid = value.parse().ok(),
            _ => usage(),
        }
    }
    if cli.workload.is_empty() || cli.output_path.is_empty() {
        usage();
    }
    cli
}

fn usage() -> ! {
    eprintln!(
        "usage: isolation --node URL --cgroup DIR --workload NAME [--variant small|attack] --out FILE [--node-pid PID]\n\
         workloads: {} event_scan kv_throughput idle_cost",
        workloads::NAMES.join(" ")
    );
    std::process::exit(2)
}

/// Returns the value at `quantile` of `sorted`, or 0 if `sorted` is empty.
fn percentile(sorted: &[f64], quantile: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    sorted[((sorted.len() as f64 * quantile).ceil() as usize).clamp(1, sorted.len()) - 1]
}
