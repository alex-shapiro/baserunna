//! `watch`: a writer commits puts (some expiring), deletes and large range
//! deletes while a watcher follows the change feed with long polls, both
//! through random nodes, under faults.
//!
//! Every change the watcher receives must be what the writer wrote at that
//! version: a value it put, or the deletion of a key it deleted, deleted a
//! range around, or put with a time to live. A cursor may expire only once
//! a transaction lifetime has passed since the request that caught up to
//! it. Once the writer stops and every value it wrote with a time to live
//! has expired, the watcher's state built from the feed alone must equal
//! the database's.

use super::super::Sim;
use super::super::client::{Client, Resp, ZOMBIE_WAIT_MS};
use serde_json::{Value as Json, json};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

const BASE: &str = "/v1/dbs/watch";

/// One write of a committed transaction.
#[derive(Clone, Debug, PartialEq)]
enum Write {
    Put { key: String, value: String, ttl: bool },
    Delete(String),
    DeleteRange(String),
}

/// Committed writes by version.
type Log = Arc<Mutex<BTreeMap<u64, Vec<Write>>>>;

/// Commits `ops` (with an idempotency key) until the outcome is definitive.
/// Returns the version it committed at and whether an earlier attempt may
/// have applied too: one whose outcome was lost, when its commit record may
/// have expired before the retry that succeeded.
async fn commit(c: &Client, ops: Json, key: &str) -> Result<(u64, bool), String> {
    let body = json!({"ops": ops, "idempotency_key": key});
    let first = c.sim.now_ms();
    let mut ambiguous = false;
    loop {
        match c.call("POST", &format!("{BASE}/txn"), Some(body.clone())).await {
            Ok(r) if r.status == 200 => {
                let v = r.json["version"].as_u64().ok_or("no version")?;
                let slack = 2 * c.sim.cfg.engine.clock_sync_max_error_ms;
                let expired = c.sim.now_ms() - first + slack >= c.sim.cfg.engine.commit_retention_ms;
                // Even a replay may follow an attempt that applied a second
                // time after the first one's record expired.
                return Ok((v, ambiguous && expired));
            }
            Ok(r) if r.code() == "conflict" || r.retryable_not_applied() => c.pause(50).await,
            Ok(r) if r.status != 503 && r.status != 429 => return Err(format!("write: {} {}", r.status, r.json)),
            r => {
                ambiguous |= r.as_ref().map_or(true, |r| r.outcome_unknown());
                c.pause(ZOMBIE_WAIT_MS).await
            }
        }
    }
}

async fn writer(c: Client, log: Log, steps: usize) -> Result<(), String> {
    let mut prev = 0;
    for step in 0..steps {
        let k = format!("k/{}", c.rng().below(6));
        let roll = c.rng().below(10);
        let (ops, writes) = match roll {
            0..=4 => {
                let value = format!("v{step}");
                let ttl = c.rng().chance(0.3);
                let mut op = json!({"op": "put", "key": k, "value": value});
                if ttl {
                    op["ttl_ms"] = json!(500 + c.rng().below(3_000));
                }
                (json!([op]), vec![Write::Put { key: k, value, ttl }])
            }
            5 | 6 => (json!([{"op": "delete", "key": k}]), vec![Write::Delete(k)]),
            7 | 8 => {
                // Enough keys that deleting them takes a range tombstone.
                let ops: Vec<Json> = (0..8)
                    .map(|i| json!({"op": "put", "key": format!("r/{i}"), "value": format!("r{step}")}))
                    .collect();
                let writes =
                    (0..8).map(|i| Write::Put { key: format!("r/{i}"), value: format!("r{step}"), ttl: false });
                (Json::Array(ops), writes.collect())
            }
            _ => (json!([{"op": "delete_range", "prefix": "r/"}]), vec![Write::DeleteRange("r/".into())]),
        };
        let (v, maybe_twice) = commit(&c, ops, &format!("w{step}")).await?;
        // The writer is the database's only writer, so an earlier application
        // of this step is the only thing that can commit between its previous
        // step's version and this one.
        let from = if maybe_twice { prev + 1 } else { v };
        {
            let mut l = log.lock().unwrap_or_else(|e| e.into_inner());
            for u in from..=v {
                l.entry(u).or_insert_with(|| writes.clone());
            }
        }
        prev = v;
        let d = c.rng().below(400);
        c.pause(d).await;
    }
    Ok(())
}

/// Verifies that a change from the feed matches a write in `log`.
fn explained(ch: &Json, log: &BTreeMap<u64, Vec<Write>>) -> Result<(), String> {
    let v = ch["version"].as_u64().ok_or("no version")?;
    let writes = log.get(&v).ok_or_else(|| format!("{ch}: no write committed as version {v}"))?;
    let ok = if let Some(r) = ch.get("deleted_range") {
        writes.iter().any(
            |w| matches!(w, Write::DeleteRange(p) if r["start"].as_str().is_some_and(|s| s.starts_with(p.as_str()))),
        )
    } else {
        let key = ch["key"].as_str().unwrap_or_default();
        writes.iter().any(|w| match w {
            Write::Put { key: k, value, ttl } if k == key => {
                ch["value"].as_str() == Some(value.as_str()) || (*ttl && ch["deleted"] == true)
            }
            Write::Delete(k) => k == key && ch["deleted"] == true,
            Write::DeleteRange(p) => key.starts_with(p.as_str()) && ch["deleted"] == true,
            _ => false,
        })
    };
    match ok {
        true => Ok(()),
        false => Err(format!("{ch} is not what version {v} wrote: {writes:?}")),
    }
}

type State = BTreeMap<String, (String, Option<u64>)>;

async fn read_all(c: &Client) -> (State, String) {
    let r = c.call_retry("GET", &format!("{BASE}/kv?limit=1000"), None).await;
    let items = r.json["items"].as_array().cloned().unwrap_or_default();
    let state = items
        .iter()
        .filter_map(|it| {
            Some((it["key"].as_str()?.to_string(), (it["value"].as_str()?.to_string(), it["expires_at"].as_u64())))
        })
        .collect();
    (state, r.json["version"].as_u64().unwrap_or(0).to_string())
}

struct Watcher {
    c: Client,
    cursor: String,
    /// Start time of the last request that caught up with the feed; no
    /// deletion after `cursor` committed before this timestamp.
    cursor_at: u64,
    state: State,
    received: Vec<Json>,
    lifetime: u64,
    eps: u64,
}

impl Watcher {
    async fn start(c: Client, lifetime: u64, eps: u64) -> Watcher {
        let mut w = Watcher {
            c,
            cursor: String::new(),
            cursor_at: 0,
            state: State::new(),
            received: Vec::new(),
            lifetime,
            eps,
        };
        w.resync().await;
        w
    }

    async fn resync(&mut self) {
        self.cursor_at = self.c.sim.now_ms();
        (self.state, self.cursor) = read_all(&self.c).await;
    }

    /// Polls the change feed once, returning `Some(true)` if caught up or
    /// `None` on transient error.
    async fn poll(&mut self, wait_ms: u64) -> Result<Option<bool>, String> {
        let at = self.c.sim.now_ms();
        let p = format!("{BASE}/changes?after={}&limit={}&wait_ms={wait_ms}", self.cursor, 1 + self.c.rng().below(8));
        let Ok(r): Result<Resp, _> = self.c.call("GET", &p, None).await else { return Ok(None) };
        match r.status {
            200 => {}
            410 => {
                if at + self.eps < self.cursor_at + self.lifetime {
                    return Err(format!(
                        "cursor {} from {} expired at {at}, within a transaction lifetime",
                        self.cursor, self.cursor_at
                    ));
                }
                self.resync().await;
                return Ok(Some(false));
            }
            _ if r.status >= 500 || r.status == 429 => return Ok(None),
            _ => return Err(format!("feed: {} {}", r.status, r.json)),
        }
        let changes = r.json["changes"].as_array().cloned().unwrap_or_default();
        for ch in &changes {
            if let Some(range) = ch.get("deleted_range") {
                let start = range["start"].as_str().unwrap_or_default().to_string();
                let end = range["end"].as_str().map(|e| e.to_string());
                self.state.retain(|k, _| !(*k >= start && end.as_ref().is_none_or(|e| k < e)));
            } else if let Some(k) = ch["key"].as_str() {
                match ch["value"].as_str() {
                    Some(v) if ch["deleted"] != true => {
                        self.state.insert(k.to_string(), (v.to_string(), ch["expires_at"].as_u64()))
                    }
                    _ => self.state.remove(k),
                };
            }
        }
        self.received.extend(changes.iter().cloned());
        self.cursor = r.json["next_cursor"].as_str().ok_or("no next_cursor")?.to_string();
        let more = r.json["more"] == true;
        if !more {
            self.cursor_at = at;
        }
        Ok(Some(changes.is_empty() && !more))
    }
}

pub async fn run(sim: Arc<Sim>, steps: usize, chaos: bool) -> Result<(), String> {
    let owner = Client::signup(&sim, "watch", "watch@watch.test", "password-123").await?;
    owner.create_db("watch").await?;
    let token = owner.token.clone().unwrap_or_default();
    let log: Log = Arc::default();
    let done = Arc::new(AtomicBool::new(false));
    let w = {
        let (sim, token, log, done) = (sim.clone(), token.clone(), log.clone(), done.clone());
        tokio::spawn(async move {
            let r = writer(Client::with_token(&sim, "writer", &token), log, steps).await;
            done.store(true, Ordering::SeqCst);
            r
        })
    };
    let lifetime = sim.cfg.engine.txn_max_lifetime_ms;
    let eps = 2 * sim.cfg.engine.clock_sync_max_error_ms;
    let mut watcher = Watcher::start(Client::with_token(&sim, "watcher", &token), lifetime, eps).await;
    let phase = chaos.then(|| super::chaos_phase(&sim));
    let mut err = None;
    while !done.load(Ordering::SeqCst) {
        let wait = watcher.c.rng().below(3_000);
        if let Err(e) = watcher.poll(wait).await {
            err = Some(e);
            break;
        }
        // Periodically pause longer than the tombstone retention window to exercise cursor expiry.
        if watcher.c.rng().chance(0.02) {
            watcher.c.pause(lifetime + 10_000).await;
        }
    }
    match w.await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => err = err.or(Some(e)),
        Err(e) => err = err.or(Some(format!("writer panicked: {e}"))),
    }
    if let Some(e) = err {
        return Err(e);
    }
    super::settle(&sim, phase).await;
    // Wait for all TTL entries to expire before comparing state.
    watcher.c.pause(4_000 + eps).await;
    while watcher.poll(0).await? != Some(true) {
        watcher.c.pause(100).await;
    }
    watcher.state.retain(|_, (_, e)| e.is_none());
    let (state, _) = read_all(&watcher.c).await;
    if watcher.state != state {
        return Err(format!("the watcher's state {:?} differs from the database's {state:?}", watcher.state));
    }
    let log = log.lock().unwrap_or_else(|e| e.into_inner()).clone();
    for ch in &watcher.received {
        explained(ch, &log)?;
    }
    if watcher.received.is_empty() {
        return Err("the watcher received nothing".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn changes_must_be_explained_by_the_log() {
        let mut log = BTreeMap::new();
        log.insert(3, vec![Write::Put { key: "k/1".into(), value: "a".into(), ttl: true }]);
        log.insert(4, vec![Write::Delete("k/2".into())]);
        log.insert(5, vec![Write::DeleteRange("r/".into())]);
        for ok in [
            json!({"version": 3, "key": "k/1", "value": "a"}),
            json!({"version": 3, "key": "k/1", "deleted": true}),
            json!({"version": 4, "key": "k/2", "deleted": true}),
            json!({"version": 5, "key": "r/3", "deleted": true}),
            json!({"version": 5, "deleted_range": {"start": "r/", "end": "r0"}}),
        ] {
            explained(&ok, &log).unwrap();
        }
        for bad in [
            json!({"version": 3, "key": "k/1", "value": "b"}),
            json!({"version": 4, "key": "k/1", "deleted": true}),
            json!({"version": 6, "key": "k/1", "value": "a"}),
            json!({"version": 4, "deleted_range": {"start": "r/", "end": null}}),
        ] {
            assert!(explained(&bad, &log).is_err(), "{bad}");
        }
    }
}
