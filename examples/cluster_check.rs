//! Failover workload and consistency checker for a live object store.
//!
//! Runs multiple nodes against a shared store while killing and restarting nodes at random.
//! Concurrent workers execute counter increments with idempotency keys and bank transfers with
//! commit IDs across random nodes, then verify that all committed writes are durable, strong reads
//! observe all settled writes, and total account balances remain constant.
//!
//! Run: `BASERUNNA_STORE=s3://bucket[/prefix] cargo run --release --example cluster_check -- [seconds]`

#[path = "common/mod.rs"]
mod common;

use baserunna::client::{ApiError, Client};
use baserunna::store::{DynStore, MemStore, Retry, list_all};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const NODES: usize = 3;
const COUNTERS: usize = 4;
const TELLERS: usize = 2;
const ACCOUNTS: usize = 5;
const INITIAL_BALANCE: i64 = 100;
const DB: &str = "check";

struct Cluster {
    open: Box<dyn Fn() -> DynStore + Send + Sync>,
    nodes: Mutex<Vec<Option<common::Isolated>>>,
    urls: Mutex<Vec<Option<String>>>,
    token: Mutex<String>,
    rng: AtomicU64,
    kills: AtomicU64,
}

impl Cluster {
    fn random(&self, n: u64) -> u64 {
        let mut x = self.rng.fetch_add(0x9E37_79B9_7F4A_7C15, SeqCst);
        x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        (x ^ (x >> 31)) % n
    }

    fn random_client(&self) -> Client {
        let urls: Vec<String> = self.urls.lock().unwrap().iter().flatten().cloned().collect();
        let url = &urls[self.random(urls.len() as u64) as usize];
        Client::new(url).with_token(&self.token.lock().unwrap())
    }

    async fn start(&self, slot: usize) {
        let node = common::start_isolated((self.open)(), &format!("node{slot}")).await;
        self.urls.lock().unwrap()[slot] = Some(node.url.clone());
        self.nodes.lock().unwrap()[slot] = Some(node);
    }

    fn kill(&self, slot: usize) {
        self.urls.lock().unwrap()[slot] = None;
        if let Some(n) = self.nodes.lock().unwrap()[slot].take() {
            n.kill();
            self.kills.fetch_add(1, SeqCst);
        }
    }
}

async fn send_request(c: &Cluster, method: &str, path: &str, body: Option<Value>) -> Result<Value, ApiError> {
    let client = c.random_client();
    match tokio::time::timeout(Duration::from_secs(20), client.call(method, path, body)).await {
        Ok(r) => r,
        Err(_) => Err(ApiError { status: 0, code: "timeout".into(), message: "timed out".into(), body: Value::Null }),
    }
}

async fn call_with_retry(c: &Cluster, method: &str, path: &str, body: Option<Value>) -> Result<Value, String> {
    let started = Instant::now();
    loop {
        match send_request(c, method, path, body.clone()).await {
            Ok(v) => return Ok(v),
            Err(e) if e.status == 0 || e.retryable() || e.status == 503 => {
                if started.elapsed() > Duration::from_secs(120) {
                    return Err(format!("{method} {path} kept failing: {e}"));
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(e) => return Err(format!("{method} {path} failed: {e}")),
        }
    }
}

async fn resolve_commit(c: &Cluster, id: &str) -> Result<bool, String> {
    let v = call_with_retry(c, "GET", &format!("/v1/dbs/{DB}/commits/{id}"), None).await?;
    match v["status"].as_str() {
        Some("committed") => Ok(true),
        Some("not_committed") => Ok(false),
        _ => Err(format!("unexpected commit status {v}")),
    }
}

fn parse_i64(v: &Value) -> Option<i64> {
    v.as_i64().or_else(|| v.as_str()?.parse().ok())
}

async fn strong_get(c: &Cluster, key: &str) -> Result<Option<i64>, ApiError> {
    let r = send_request(c, "POST", &format!("/v1/dbs/{DB}/txn"), Some(json!({"ops": [{"op": "get", "key": key}]})))
        .await?;
    Ok(parse_i64(&r["results"][0]["value"]))
}

#[derive(Default)]
struct Stats {
    commits: AtomicU64,
    unknown: AtomicU64,
    reads: AtomicU64,
}

async fn increment_worker(
    c: Arc<Cluster>,
    i: usize,
    stop: Arc<AtomicBool>,
    t: Arc<Stats>,
) -> Result<BTreeSet<u64>, String> {
    let mut done = BTreeSet::new();
    let mut n = 0;
    while !stop.load(SeqCst) {
        n += 1;
        let key = format!("w{i}-{n}");
        let body = json!({
            "ops": [{"op": "increment", "key": format!("w{i}"), "by": 1}, {"op": "put", "key": format!("log/w{i}/{n:06}"), "value": "1"}],
            "idempotency_key": key,
        });
        let applied = match send_request(&c, "POST", &format!("/v1/dbs/{DB}/txn"), Some(body)).await {
            Ok(r) if r["committed"] == true => true,
            Ok(r) => return Err(format!("increment {key} was not committed: {r}")),
            Err(_) => {
                t.unknown.fetch_add(1, SeqCst);
                resolve_commit(&c, &format!("key:{key}")).await?
            }
        };
        if applied {
            done.insert(n);
            t.commits.fetch_add(1, SeqCst);
        }
        if let Ok(v) = strong_get(&c, &format!("w{i}")).await {
            t.reads.fetch_add(1, SeqCst);
            let want = done.len() as i64;
            if v.unwrap_or(0) != want {
                return Err(format!("a strong read of w{i} returned {v:?} after {want} settled increments"));
            }
        }
    }
    Ok(done)
}

fn account_key(i: usize) -> String {
    format!("acct/{i}")
}

async fn transfer_worker(c: Arc<Cluster>, stop: Arc<AtomicBool>, t: Arc<Stats>) -> Result<(), String> {
    let base = format!("/v1/dbs/{DB}");
    while !stop.load(SeqCst) {
        let (a, b) = (c.random(ACCOUNTS as u64) as usize, c.random(ACCOUNTS as u64) as usize);
        if a == b {
            continue;
        }
        let reads = json!([{"op": "get", "key": account_key(a)}, {"op": "get", "key": account_key(b)}]);
        let Ok(r) = send_request(&c, "POST", &format!("{base}/transactions"), Some(json!({"ops": reads}))).await else {
            continue;
        };
        let (id, cid) =
            (r["txn_id"].as_str().unwrap_or("").to_string(), r["commit_id"].as_str().unwrap_or("").to_string());
        let (Some(ba), Some(bb)) = (parse_i64(&r["results"][0]["value"]), parse_i64(&r["results"][1]["value"])) else {
            return Err(format!("missing balances: {r}"));
        };
        let amount = 1 + c.random(20) as i64;
        if ba < amount {
            let _ = send_request(&c, "DELETE", &format!("{base}/transactions/{id}"), None).await;
            continue;
        }
        let writes = json!([
            {"op": "put", "key": account_key(a), "value": (ba - amount).to_string()},
            {"op": "put", "key": account_key(b), "value": (bb + amount).to_string()},
        ]);
        let applied =
            match send_request(&c, "POST", &format!("{base}/transactions/{id}/commit"), Some(json!({"ops": writes})))
                .await
            {
                Ok(_) => true,
                Err(e) if e.status == 409 || e.status == 404 => false,
                Err(e) if e.status == 0 || e.status == 503 || e.retryable() => {
                    t.unknown.fetch_add(1, SeqCst);
                    resolve_commit(&c, &cid).await?
                }
                Err(e) => return Err(format!("commit failed: {e}")),
            };
        if applied {
            t.commits.fetch_add(1, SeqCst);
        }
    }
    Ok(())
}

async fn read_balances(c: &Cluster) -> Result<Vec<i64>, String> {
    let body = json!({"ops": [{"op": "scan", "prefix": "acct/", "limit": 100}]});
    let r = call_with_retry(c, "POST", &format!("/v1/dbs/{DB}/txn"), Some(body)).await?;
    let items = r["results"][0]["items"].as_array().ok_or_else(|| format!("bad scan: {r}"))?;
    Ok(items.iter().map(|it| parse_i64(&it["value"]).unwrap_or(i64::MIN)).collect())
}

fn verify_balances(b: &[i64]) -> Result<(), String> {
    if b.len() != ACCOUNTS || b.iter().sum::<i64>() != INITIAL_BALANCE * ACCOUNTS as i64 || b.iter().any(|x| *x < 0) {
        return Err(format!(
            "balances {b:?} do not conserve {} in {ACCOUNTS} accounts",
            INITIAL_BALANCE * ACCOUNTS as i64
        ));
    }
    Ok(())
}

async fn audit_worker(c: Arc<Cluster>, stop: Arc<AtomicBool>, t: Arc<Stats>) -> Result<(), String> {
    while !stop.load(SeqCst) {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let body = json!({"ops": [{"op": "scan", "prefix": "acct/", "limit": 100}]});
        if let Ok(r) = send_request(&c, "POST", &format!("/v1/dbs/{DB}/txn"), Some(body)).await {
            let items = r["results"][0]["items"].as_array().cloned().unwrap_or_default();
            verify_balances(&items.iter().map(|it| parse_i64(&it["value"]).unwrap_or(i64::MIN)).collect::<Vec<_>>())?;
            t.reads.fetch_add(1, SeqCst);
        }
    }
    Ok(())
}

async fn kill_and_restart_nodes(c: Arc<Cluster>, stop: Arc<AtomicBool>) {
    while !stop.load(SeqCst) {
        tokio::time::sleep(Duration::from_millis(3_000 + c.random(4_000))).await;
        if stop.load(SeqCst) {
            break;
        }
        let slot = c.random(NODES as u64) as usize;
        c.kill(slot);
        tokio::time::sleep(Duration::from_millis(500 + c.random(3_000))).await;
        c.start(slot).await;
    }
}

async fn run_check(c: Arc<Cluster>, seconds: u64) -> Result<(), String> {
    for slot in 0..NODES {
        c.start(slot).await;
    }
    let first = Client::new(c.urls.lock().unwrap()[0].as_ref().unwrap());
    let user = first.signup("check@example.com", "cluster-check-1").await.map_err(|e| e.to_string())?;
    *c.token.lock().unwrap() = user.token().unwrap_or_default().to_string();
    user.create_db(DB).await.map_err(|e| e.to_string())?;
    let init: Vec<Value> = (0..ACCOUNTS)
        .map(|i| json!({"op": "put", "key": account_key(i), "value": INITIAL_BALANCE.to_string()}))
        .collect();
    call_with_retry(&c, "POST", &format!("/v1/dbs/{DB}/txn"), Some(json!({"ops": init, "idempotency_key": "init"})))
        .await?;
    tokio::time::sleep(Duration::from_millis(1_500)).await;

    let (stop, tally) = (Arc::new(AtomicBool::new(false)), Arc::new(Stats::default()));
    let counters: Vec<_> =
        (0..COUNTERS).map(|i| tokio::spawn(increment_worker(c.clone(), i, stop.clone(), tally.clone()))).collect();
    let mut others: Vec<_> =
        (0..TELLERS).map(|_| tokio::spawn(transfer_worker(c.clone(), stop.clone(), tally.clone()))).collect();
    others.push(tokio::spawn(audit_worker(c.clone(), stop.clone(), tally.clone())));
    let chaos = tokio::spawn(kill_and_restart_nodes(c.clone(), stop.clone()));
    tokio::time::sleep(Duration::from_secs(seconds)).await;
    stop.store(true, SeqCst);
    let _ = chaos.await;
    let mut done = Vec::new();
    for t in counters {
        done.push(t.await.map_err(|e| e.to_string())??);
    }
    for t in others {
        t.await.map_err(|e| e.to_string())??;
    }

    for (i, done) in done.iter().enumerate() {
        let value = loop {
            match strong_get(&c, &format!("w{i}")).await {
                Ok(v) => break v.unwrap_or(0),
                Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
            }
        };
        if value != done.len() as i64 {
            return Err(format!("counter w{i} is {value}, but {} increments committed", done.len()));
        }
        let mut logged = BTreeSet::new();
        let mut cursor = Value::Null;
        loop {
            let scan = json!({"op": "scan", "prefix": format!("log/w{i}/"), "limit": 1000, "keys_only": true, "cursor": cursor});
            let r = call_with_retry(&c, "POST", &format!("/v1/dbs/{DB}/txn"), Some(json!({"ops": [scan]}))).await?;
            let page = &r["results"][0];
            for it in page["items"].as_array().into_iter().flatten() {
                logged.extend(it["key"].as_str().and_then(|k| k.rsplit('/').next()?.parse::<u64>().ok()));
            }
            cursor = page["next_cursor"].clone();
            if cursor.is_null() {
                break;
            }
        }
        if logged != *done {
            let missing: Vec<_> = done.difference(&logged).collect();
            let extra: Vec<_> = logged.difference(done).collect();
            return Err(format!("w{i}: committed increments {missing:?} are missing and {extra:?} were not committed"));
        }
    }
    verify_balances(&read_balances(&c).await?)?;
    println!(
        "ok: {} commits ({} with unknown outcomes, settled), {} checked reads, {} node kills",
        tally.commits.load(SeqCst),
        tally.unknown.load(SeqCst),
        tally.reads.load(SeqCst),
        c.kills.load(SeqCst)
    );
    Ok(())
}

#[tokio::main]
async fn main() {
    let seconds: u64 = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(60);
    let url = std::env::var("BASERUNNA_STORE").ok().filter(|u| !u.is_empty() && u != "mem:");
    let nonce = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis());
    let (open, scratch): (Box<dyn Fn() -> DynStore + Send + Sync>, Option<DynStore>) = match &url {
        None => {
            let s: DynStore = Arc::new(MemStore::with_system_clock());
            (Box::new(move || s.clone()), None)
        }
        Some(url) => {
            let prefix = format!("{}/cluster-check-{nonce:x}", url.trim_end_matches('/'));
            let open = move || -> DynStore {
                Arc::new(baserunna::store::CloudStore::from_url(&prefix).expect("BASERUNNA_STORE"))
            };
            let scratch = open();
            (Box::new(open), Some(scratch))
        }
    };
    let c = Arc::new(Cluster {
        open,
        nodes: Mutex::new((0..NODES).map(|_| None).collect()),
        urls: Mutex::new(vec![None; NODES]),
        token: Mutex::new(String::new()),
        rng: AtomicU64::new(nonce as u64),
        kills: AtomicU64::new(0),
    });
    let result = run_check(c.clone(), seconds).await;
    for slot in 0..NODES {
        c.kill(slot);
    }
    if let Some(s) = scratch {
        match list_all(&*s, "", &Retry::default()).await {
            Ok(objects) => {
                for o in objects {
                    let _ = Retry::default().run(|| s.delete(&o.key)).await;
                }
            }
            Err(e) => eprintln!("cleanup failed: {e}"),
        }
    }
    if let Err(e) = result {
        eprintln!("FAILED: {e}");
        std::process::exit(1);
    }
}
