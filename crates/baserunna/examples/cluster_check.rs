//! Failover workload and consistency checker for a live object store.
//!
//! Runs multiple nodes against a shared store while killing and restarting nodes at random.
//! Concurrent workers increment counters with idempotency keys and transfer funds across random
//! nodes. The checker verifies that committed writes are durable, strong reads observe settled
//! writes, and total account balances are conserved.
//!
//! Run: `BASERUNNA_STORE=s3://bucket[/prefix] cargo run --release --example cluster_check -- [seconds]`

#[path = "common/mod.rs"]
mod common;

use baserunna::store::{DynStore, MemStore, Retry, list_all};
use baserunna_client::{
    Client, Db, Error, HttpTransport, KeyRange, Op, OpResult, RetryPolicy, TransportError, Value, api,
};
use parking_lot::Mutex;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::SeqCst};
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
    clients: Mutex<Vec<Option<Client>>>,
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

    fn random_db(&self) -> Db {
        let clients: Vec<Client> = self.clients.lock().iter().flatten().cloned().collect();
        let client = &clients[self.random(clients.len() as u64) as usize];
        client.with_token(&self.token.lock()).db(DB)
    }

    async fn start(&self, slot: usize) {
        let node = common::start_isolated((self.open)(), &format!("node{slot}")).await;
        // The checker retries across random nodes, so each client sends at most one attempt per request.
        let policy = RetryPolicy { attempts: 1, ..RetryPolicy::default() };
        self.clients.lock()[slot] = Some(Client::new(HttpTransport::new(&node.url)).with_retry_policy(policy));
        self.nodes.lock()[slot] = Some(node);
    }

    fn kill(&self, slot: usize) {
        self.clients.lock()[slot] = None;
        if let Some(n) = self.nodes.lock()[slot].take() {
            n.kill();
            self.kills.fetch_add(1, SeqCst);
        }
    }
}

/// Sends `request` to a random node with a 20 s timeout.
async fn send_request<T, F>(c: &Cluster, request: impl FnOnce(Db) -> F) -> Result<T, Error>
where
    F: Future<Output = Result<T, Error>>,
{
    match tokio::time::timeout(Duration::from_secs(20), request(c.random_db())).await {
        Ok(r) => r,
        Err(_) => Err(TransportError::TimedOut("request timed out after 20 s".into()).into()),
    }
}

/// Returns true if `e` is a transient transport or server error.
fn is_retryable(e: &Error) -> bool {
    match e {
        Error::Transport(_) => true,
        Error::Api { status, error, .. } => *status == 503 || error.retryable,
        Error::Foreign { status, .. } => *status == 503,
        _ => false,
    }
}

/// Extracts an optional response from `r`.
/// Rejects malformed responses and ignores other errors.
fn optional_response<T>(what: &str, r: Result<T, Error>) -> Result<Option<T>, String> {
    match r {
        Ok(v) => Ok(Some(v)),
        Err(e @ (Error::Malformed { .. } | Error::Decode { .. })) => Err(format!("{what}: {e}")),
        Err(_) => Ok(None),
    }
}

async fn call_with_retry<T, F>(c: &Cluster, what: &str, request: impl Fn(Db) -> F) -> Result<T, String>
where
    F: Future<Output = Result<T, Error>>,
{
    let started = Instant::now();
    loop {
        match send_request(c, &request).await {
            Ok(v) => return Ok(v),
            Err(e) if is_retryable(&e) => {
                if started.elapsed() > Duration::from_secs(120) {
                    return Err(format!("{what} kept failing: {e}"));
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(e) => return Err(format!("{what} failed: {e}")),
        }
    }
}

async fn resolve_commit(c: &Cluster, id: &str) -> Result<bool, String> {
    let status = call_with_retry(c, &format!("GET commit {id}"), |db| async move {
        db.client().send(&api::GetCommit { db: DB.into(), id: id.into() }, &api::NoBody {}).await
    })
    .await?;
    Ok(matches!(status, api::CommitStatus::Committed { .. }))
}

fn parse_i64(v: &Value) -> Option<i64> {
    std::str::from_utf8(v.data()?).ok()?.parse().ok()
}

async fn strong_get(c: &Cluster, key: &str) -> Result<Option<i64>, Error> {
    let entry = send_request(c, |db| async move { db.read_one(Op::get(key.to_owned())).await?.entry() }).await?;
    Ok(entry.and_then(|e| parse_i64(&e.value)))
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
        let key = &format!("w{i}-{n}");
        let ops = &[Op::increment(format!("w{i}"), 1), Op::put(format!("log/w{i}/{n:06}"), "1")];
        let applied = match send_request(&c, |db| async move { db.txn(ops, Some(key)).await }).await {
            Ok(r) if r.commit_id.is_some() => true,
            Ok(r) => return Err(format!("increment {key} was not committed: {r:?}")),
            Err(_) => {
                t.unknown.fetch_add(1, SeqCst);
                resolve_commit(&c, &format!("key:{key}")).await?
            }
        };
        if applied {
            done.insert(n);
            t.commits.fetch_add(1, SeqCst);
        }
        if let Some(v) = optional_response(&format!("strong read of w{i}"), strong_get(&c, &format!("w{i}")).await)? {
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

fn balance(r: &api::OpResult) -> Option<i64> {
    r.decode::<api::GetResult>().ok()?.value?.parse().ok()
}

/// Transfers funds between random accounts in interactive transactions.
/// Calls the transaction endpoints directly so each step can reach a different node.
async fn transfer_worker(c: Arc<Cluster>, stop: Arc<AtomicBool>, t: Arc<Stats>) -> Result<(), String> {
    while !stop.load(SeqCst) {
        let (a, b) = (c.random(ACCOUNTS as u64) as usize, c.random(ACCOUNTS as u64) as usize);
        if a == b {
            continue;
        }
        let reads = vec![api::Op::Get { key: account_key(a) }, api::Op::Get { key: account_key(b) }];
        let begin = &api::BeginRequest { ops: reads, ..api::BeginRequest::default() };
        let begun =
            send_request(&c, |db| async move { db.client().send(&api::BeginTxn { db: DB.into() }, begin).await });
        let Ok(r) = begun.await else {
            continue;
        };
        let (Some(ba), Some(bb)) = (r.results.first().and_then(balance), r.results.get(1).and_then(balance)) else {
            return Err(format!("missing balances: {:?}", r.results));
        };
        let id = &r.txn_id;
        let amount = 1 + c.random(20) as i64;
        if ba < amount {
            let abort = |db: Db| async move {
                db.client().send(&api::AbortTxn { db: DB.into(), id: id.clone() }, &api::NoBody {}).await
            };
            let _ = send_request(&c, abort).await;
            continue;
        }
        let put = |i: usize, v: i64| api::Op::Put {
            key: account_key(i),
            value: Some(v.to_string()),
            blob_id: None,
            ttl_ms: None,
        };
        let writes = &api::CommitRequest { ops: vec![put(a, ba - amount), put(b, bb + amount)] };
        let commit =
            |db: Db| async move { db.client().send(&api::CommitTxn { db: DB.into(), id: id.clone() }, writes).await };
        let applied = match send_request(&c, commit).await {
            Ok(_) => true,
            Err(Error::Api { status: 404 | 409, .. }) => false,
            Err(e) if is_retryable(&e) => {
                t.unknown.fetch_add(1, SeqCst);
                resolve_commit(&c, &r.commit_id).await?
            }
            Err(e) => return Err(format!("commit failed: {e}")),
        };
        if applied {
            t.commits.fetch_add(1, SeqCst);
        }
    }
    Ok(())
}

async fn scan_balances(db: Db) -> Result<Vec<i64>, Error> {
    let page = db.read_one(Op::scan("acct/")).await?.scan_page()?;
    Ok(page.entries.iter().map(|e| parse_i64(&e.value).unwrap_or(i64::MIN)).collect())
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
        if let Some(balances) = optional_response("audit scan", send_request(&c, scan_balances).await)? {
            verify_balances(&balances)?;
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
    let first = c.clients.lock()[0].clone().expect("node 0 is running");
    let session = first.sign_up("check@example.com", "cluster-check-1").await.map_err(|e| e.to_string())?;
    *c.token.lock() = session.token.clone();
    first.with_token(&session.token).create_db(DB, None).await.map_err(|e| e.to_string())?;
    let init = &(0..ACCOUNTS).map(|i| Op::put(account_key(i), INITIAL_BALANCE.to_string())).collect::<Vec<_>>();
    call_with_retry(&c, "initial balances", |db| async move { db.txn(init, Some("init")).await }).await?;
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
            return Err(format!("counter w{i} is {value}, but {} increments were committed", done.len()));
        }
        let mut logged = BTreeSet::new();
        let prefix = format!("log/w{i}/");
        let mut scan = Op::Scan {
            range: KeyRange::Prefix(prefix.clone().into()),
            limit: api::PAGE_MAX,
            reverse: false,
            keys_only: true,
            cursor: None,
        };
        loop {
            let op = &scan;
            let page =
                call_with_retry(&c, &format!("scan {prefix}"), |db| async move { db.read_one(op.clone()).await })
                    .await?;
            if let OpResult::Scan(p) = &page {
                for e in &p.entries {
                    logged.extend(
                        std::str::from_utf8(&e.key).ok().and_then(|k| k.rsplit('/').next()?.parse::<u64>().ok()),
                    );
                }
            }
            match scan.next_page(&page) {
                Some(next) => scan = next,
                None => break,
            }
        }
        if logged != *done {
            let missing: Vec<_> = done.difference(&logged).collect();
            let extra: Vec<_> = logged.difference(done).collect();
            return Err(format!("w{i}: committed increments {missing:?} are missing and {extra:?} were not committed"));
        }
    }
    verify_balances(&call_with_retry(&c, "scan acct/", scan_balances).await?)?;
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
        clients: Mutex::new(vec![None; NODES]),
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
