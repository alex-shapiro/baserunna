//! Tenant isolation harness: checks whether a node continues to serve a victim tenant while an
//! attacker tenant runs a heavy workload, through the node's HTTP API (ISOLATION_PLAN.md,
//! milestone 0).
//!
//! Usage:
//!
//! ```text
//! isolation --node http://127.0.0.1:8080 --cgroup /sys/fs/cgroup/iso/node \
//!     --workload <name> --variant small|attack --out result.json [--node-pid PID]
//! isolation --node ... --cgroup ... --workload event_scan --out result.json [--rounds 20]
//! isolation --node ... --cgroup ... --workload kv_throughput --out result.json [--clients 64 | --find-saturation]
//! isolation --node ... --cgroup ... --workload idle_cost --out result.json
//! ```
//!
//! A workload run records the idle node's `memory.current` before any setup, and then runs three
//! phases. Phase A runs the victim alone. Phase B runs the attacker alone and samples the node
//! cgroup's `memory.current` every 10 ms. Phase C runs the attacker and the victim together. The
//! output JSON has every measurement and the result of each pass criterion.

mod cgroup;
mod event_scan;
mod http;
mod idle_cost;
mod kv_throughput;
mod victim;
mod workloads;

use cgroup::{MemorySampler, NodeCgroup};
use http::{Api, RequestRecord};
use serde_json::{Value as Json, json};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};
use workloads::{AttackContext, AttackOutcome, Round, Variant};

const MIB: u64 = 1 << 20;

struct Args {
    node: String,
    cgroup: NodeCgroup,
    workload: String,
    variant: Option<Variant>,
    out: String,
    node_pid: Option<u32>,
    deadline: Duration,
    victim_alone: Duration,
    victim_rps: f64,
    rounds: usize,
    clients: usize,
    find_saturation: bool,
}

fn usage() -> ! {
    eprintln!(
        "usage: isolation --node URL --cgroup DIR --workload NAME [--variant small|attack] --out FILE \
         [--node-pid PID] [--deadline-ms 10000] [--victim-secs 15] [--victim-rps 400] [--rounds 20] [--clients 64] [--find-saturation]\n\
         workloads: {} event_scan kv_throughput idle_cost",
        workloads::NAMES.join(" ")
    );
    std::process::exit(2)
}

fn args() -> Args {
    let mut a = Args {
        node: "http://127.0.0.1:8080".into(),
        cgroup: NodeCgroup("/sys/fs/cgroup/iso/node".into()),
        workload: String::new(),
        variant: None,
        out: String::new(),
        node_pid: None,
        deadline: Duration::from_millis(10_000),
        victim_alone: Duration::from_secs(15),
        victim_rps: 400.0,
        rounds: 20,
        clients: 64,
        find_saturation: false,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        if flag == "--find-saturation" {
            a.find_saturation = true;
            continue;
        }
        let v = it.next().unwrap_or_else(|| usage());
        match flag.as_str() {
            "--node" => a.node = v,
            "--cgroup" => a.cgroup = NodeCgroup(v.into()),
            "--workload" => a.workload = v,
            "--variant" => a.variant = Some(Variant::parse(&v).unwrap_or_else(|| usage())),
            "--out" => a.out = v,
            "--node-pid" => a.node_pid = v.parse().ok(),
            "--deadline-ms" => a.deadline = Duration::from_millis(v.parse().unwrap_or_else(|_| usage())),
            "--victim-secs" => a.victim_alone = Duration::from_secs(v.parse().unwrap_or_else(|_| usage())),
            "--victim-rps" => a.victim_rps = v.parse().unwrap_or_else(|_| usage()),
            "--rounds" => a.rounds = v.parse().unwrap_or_else(|_| usage()),
            "--clients" => a.clients = v.parse().unwrap_or_else(|_| usage()),
            _ => usage(),
        }
    }
    if a.workload.is_empty() || a.out.is_empty() {
        usage();
    }
    a
}

/// Node liveness: its process exists and its leaf cgroup has no new OOM kills.
struct NodeWatch {
    pid: Option<u32>,
    cgroup: NodeCgroup,
    oom_kills_before: Option<u64>,
}

impl NodeWatch {
    fn alive(&self) -> bool {
        self.pid.is_none_or(|p| std::path::Path::new(&format!("/proc/{p}")).exists() && !is_zombie(p))
    }

    fn new_oom_kills(&self) -> u64 {
        match (self.oom_kills_before, self.cgroup.node_oom_kills()) {
            (Some(a), Some(b)) => b.saturating_sub(a),
            _ => 0,
        }
    }

    fn report(&self) -> Json {
        json!({"pid": self.pid, "process_alive": self.alive(), "oom_kills": self.new_oom_kills(),
            "cgroup_exists": self.cgroup.exists()})
    }
}

fn is_zombie(pid: u32) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .ok()
        .and_then(|s| s.rsplit_once(')').map(|(_, rest)| rest.trim_start().starts_with('Z')))
        .unwrap_or(false)
}

fn attack_summary(o: &AttackOutcome, deadline: Duration) -> Json {
    let dropped: Vec<&RequestRecord> = o.requests.iter().filter(|r| r.status.is_none()).collect();
    let refused: Vec<&RequestRecord> =
        o.requests.iter().filter(|r| r.status.is_some_and(|s| s >= 400) && !r.expected).collect();
    let late: Vec<&RequestRecord> =
        refused.iter().copied().filter(|r| r.elapsed_ms > 2.0 * deadline.as_secs_f64() * 1000.0).collect();
    json!({
        "requests": o.requests,
        "completed": o.completed,
        "notes": o.notes,
        "expected_responses": o.requests.iter().filter(|r| r.expected).count(),
        "refusals": refused.len(),
        "late_refusals": late.len(),
        "dropped_connections": dropped.len(),
        "unexpected_success_statuses": o.requests.iter().filter(|r| !r.expected && r.status.is_some_and(|s| s < 400)).count(),
    })
}

#[tokio::main]
async fn main() {
    let a = args();
    let started = Instant::now();
    let mut out = json!({
        "workload": a.workload, "variant": a.variant, "node": a.node, "cgroup": a.cgroup.0,
        "request_deadline_ms": a.deadline.as_millis() as u64,
    });
    let watch = NodeWatch { pid: a.node_pid, cgroup: a.cgroup.clone(), oom_kills_before: a.cgroup.node_oom_kills() };
    if let Err(e) = run(&a, &watch, &mut out).await {
        eprintln!("isolation: {e}");
        out["error"] = json!(e);
    }
    out["node_after"] = watch.report();
    out["total_s"] = json!(started.elapsed().as_secs_f64());
    let text = serde_json::to_string_pretty(&out).expect("json");
    std::fs::write(&a.out, text + "\n").unwrap_or_else(|e| panic!("cannot write {}: {e}", a.out));
    eprintln!("isolation: wrote {}", a.out);
}

async fn run(a: &Args, watch: &NodeWatch, out: &mut Json) -> Result<(), String> {
    let client_timeout = 6 * a.deadline + Duration::from_secs(60);
    let api = Arc::new(Api::new(&a.node, client_timeout));
    api.wait_healthy(Duration::from_secs(60)).await?;
    if a.workload == "idle_cost" {
        out["idle_cost"] = idle_cost::run(&api, &a.cgroup).await?;
        out["node_survived"] = json!(watch.alive() && watch.new_oom_kills() == 0);
        return Ok(());
    }
    if a.workload == "kv_throughput" {
        out["kv_throughput"] = kv_throughput::run(&api, &a.node, &a.cgroup, a.clients, a.find_saturation).await?;
        out["node_survived"] = json!(watch.alive() && watch.new_oom_kills() == 0);
        return Ok(());
    }
    let attacker = api.create_owner("attacker@iso.test", "attacker").await?;
    if a.workload == "event_scan" {
        out["event_scan"] = event_scan::run(&api, &attacker, &a.cgroup, a.rounds).await?;
        out["node_survived"] = json!(watch.alive() && watch.new_oom_kills() == 0);
        return Ok(());
    }
    let workload = workloads::by_name(&a.workload).ok_or_else(|| format!("unknown workload {:?}", a.workload))?;
    let idle_node_bytes = a.cgroup.memory_current().ok_or("cannot read the node's memory.current")?;
    out["idle_node_bytes"] = json!(idle_node_bytes);
    let variant = a.variant.ok_or("--variant is required for this workload")?;
    let victim_owner = api.create_owner("victim@iso.test", "victim").await?;
    victim::prepare(&api, &victim_owner).await?;
    let cx = AttackContext { api: api.clone(), attacker, variant, cgroup: a.cgroup.clone() };
    let prep_started = Instant::now();
    let prepare = json!({
        "alone": workload.prepare(&cx, Round::Alone).await?,
        "with_victim": workload.prepare(&cx, Round::WithVictim).await?,
        "seconds": prep_started.elapsed().as_secs_f64(),
    });
    out["prepare"] = prepare;
    if !watch.alive() || watch.new_oom_kills() > 0 {
        out["conditions"] = conditions(watch, None, None, None, variant, a.deadline);
        return Err("the node died during setup".into());
    }

    let stop = Arc::new(AtomicBool::new(false));
    let victim_task = tokio::spawn(victim::run(api.clone(), victim_owner.clone(), stop.clone(), a.victim_rps));
    tokio::time::sleep(a.victim_alone).await;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let victim_alone = victim_task.await.map_err(|e| e.to_string())?;
    out["phase_a_victim_alone"] = json!(victim_alone);

    let sampler = MemorySampler::start(a.cgroup.clone());
    let phase_b_started = Instant::now();
    let alone = workload.attack(&cx, Round::Alone).await;
    let memory = sampler.finish();
    let alone = alone?;
    out["phase_b_attacker_alone"] = json!({
        "seconds": phase_b_started.elapsed().as_secs_f64(),
        "memory": memory,
        "attack": attack_summary(&alone, a.deadline),
    });

    let mut with_victim = None;
    let mut victim_attacked = None;
    if watch.alive() && watch.new_oom_kills() == 0 {
        let stop = Arc::new(AtomicBool::new(false));
        let victim_task = tokio::spawn(victim::run(api.clone(), victim_owner, stop.clone(), a.victim_rps));
        let sampler = MemorySampler::start(a.cgroup.clone());
        let phase_c_started = Instant::now();
        let attack = workload.attack(&cx, Round::WithVictim).await;
        // Keep the victim running for at least 2 s so that a short attack still yields samples.
        tokio::time::sleep(Duration::from_secs(2).saturating_sub(phase_c_started.elapsed())).await;
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let victim_stats = victim_task.await.map_err(|e| e.to_string())?;
        let memory = sampler.finish();
        let attack = attack?;
        out["phase_c_attack_with_victim"] = json!({
            "seconds": phase_c_started.elapsed().as_secs_f64(),
            "memory": memory,
            "victim": victim_stats,
            "attack": attack_summary(&attack, a.deadline),
        });
        with_victim = Some(attack);
        victim_attacked = Some(victim_stats);
    }
    out["phase_b_growth_from_idle_mib"] = json!(memory.peak_bytes.saturating_sub(idle_node_bytes) as f64 / MIB as f64);
    out["conditions"] = conditions(
        watch,
        Some(&victim_alone),
        victim_attacked.as_ref(),
        Some((&alone, with_victim.as_ref())),
        variant,
        a.deadline,
    );
    Ok(())
}

/// Evaluates the five pass criteria in ISOLATION_PLAN.md, milestone 0. A criterion is `null` when
/// a phase that it needs did not run, or when it does not apply to the variant.
fn conditions(
    watch: &NodeWatch,
    victim_alone: Option<&victim::VictimStats>,
    victim_attacked: Option<&victim::VictimStats>,
    attacks: Option<(&AttackOutcome, Option<&AttackOutcome>)>,
    variant: Variant,
    deadline: Duration,
) -> Json {
    let survived = watch.alive() && watch.new_oom_kills() == 0;
    let victim_succeeds = victim_attacked.map(|v| v.failures == 0);
    let victim_latency = match (victim_alone, victim_attacked) {
        (Some(alone), Some(attacked)) => {
            let limit = (2.0 * alone.p99_ms).max(alone.p99_ms + 50.0);
            Some(json!({"pass": attacked.p99_ms <= limit, "p99_limit_ms": limit,
                "p99_alone_ms": alone.p99_ms, "p99_attacked_ms": attacked.p99_ms}))
        }
        _ => None,
    };
    let all_requests: Vec<&RequestRecord> = attacks
        .map(|(b, c)| b.requests.iter().chain(c.into_iter().flat_map(|c| c.requests.iter())).collect())
        .unwrap_or_default();
    let limit_ms = 2.0 * deadline.as_secs_f64() * 1000.0;
    let refusals_are_http_errors = attacks.map(|_| {
        all_requests.iter().all(|r| r.expected || (r.status.is_some_and(|s| s >= 400) && r.elapsed_ms <= limit_ms))
    });
    let small = (variant == Variant::Small).then(|| {
        attacks.is_some_and(|(b, c)| {
            b.completed && c.is_some_and(|c| c.completed) && all_requests.iter().all(|r| r.expected)
        }) && survived
    });
    let pass = survived
        && victim_succeeds == Some(true)
        && victim_latency.as_ref().is_some_and(|v| v["pass"] == json!(true))
        && refusals_are_http_errors == Some(true)
        && small != Some(false);
    json!({
        "1_node_survives": survived,
        "2_victim_requests_succeed": victim_succeeds,
        "3_victim_p99_within_limit": victim_latency,
        "4_refusals_are_http_errors": refusals_are_http_errors,
        "5_small_variant_succeeds": small,
        "pass": pass,
    })
}
