//! `history`: concurrent clients run random multi-key transactions (one-shot
//! and interactive, serializable and snapshot-isolated, strong and stale
//! reads) on a small set of keys, and the whole history is checked at the
//! end, with no workload-specific model:
//!
//! - Committed write transactions, replayed in commit-version order, must
//!   explain every value and version that every read observed: a
//!   serializable transaction read the state just before its commit, a
//!   read-only one the state at its version, a snapshot-isolated one the
//!   state at some earlier version. Interactive transactions, committed or
//!   not, read the state at their read version.
//! - Real-time order: a transaction that began after another one finished
//!   is serialized after it (strict serializability).
//! - First committer wins: no transaction committed a write to a key that a
//!   snapshot-isolated transaction wrote between its snapshot and its commit.
//!
//! Every outcome is known from the data: each write transaction also writes
//! a marker key of its own, so the markers left at the end tell which
//! transactions committed, and at which version. Outcomes reported to
//! clients must agree with them, including those of ambiguous commits
//! resolved through the fenced commit status (SPEC §6.2).

use super::super::Sim;
use super::super::client::{Client, Status, ZOMBIE_WAIT_MS};
use crate::util::rng::Rng;
use serde_json::{Value as Json, json};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, PartialEq)]
pub enum Op {
    Get { key: usize },
    Put { key: usize, value: String },
    Delete { key: usize },
    Increment { key: usize, by: i64 },
    Scan { start: usize, end: usize, limit: usize, reverse: bool },
    DeleteRange { start: usize, end: usize },
    Check { key: usize, exists: bool },
}

impl Op {
    fn is_write(&self) -> bool {
        matches!(self, Op::Put { .. } | Op::Delete { .. } | Op::Increment { .. } | Op::DeleteRange { .. })
    }

    fn json(&self) -> Json {
        match self {
            Op::Get { key } => json!({"op": "get", "key": name(*key)}),
            Op::Put { key, value } => json!({"op": "put", "key": name(*key), "value": value}),
            Op::Delete { key } => json!({"op": "delete", "key": name(*key)}),
            Op::Increment { key, by } => json!({"op": "increment", "key": name(*key), "by": by}),
            Op::Scan { start, end, limit, reverse } => {
                json!({"op": "scan", "start": name(*start), "end": name(*end), "limit": limit, "reverse": reverse})
            }
            Op::DeleteRange { start, end } => json!({"op": "delete_range", "start": name(*start), "end": name(*end)}),
            Op::Check { key, exists } => json!({"op": "check", "key": name(*key), "exists": exists}),
        }
    }
}

fn name(key: usize) -> String {
    format!("h/{key:03}")
}

fn key_of(s: &str) -> Option<usize> {
    s.strip_prefix("h/")?.parse().ok()
}

/// A value as a read reported it: `version` is `None` for the
/// transaction's own write.
#[derive(Clone, Debug, PartialEq)]
pub struct Seen {
    pub value: String,
    pub version: Option<u64>,
}

/// What an operation's result reported.
#[derive(Clone, Debug, PartialEq)]
pub enum Observed {
    Get(Option<Seen>),
    Scan { items: Vec<(usize, Seen)>, more: bool },
    Increment(i64),
    Done,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Isolation {
    Serializable,
    Snapshot,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// A write transaction committed at this version.
    Committed(u64),
    /// A read-only transaction, serialized at this version.
    Read(u64),
    NotCommitted,
    /// A write transaction whose outcome its client could not learn.
    Unknown,
}

#[derive(Clone, Debug)]
pub struct Txn {
    pub client: String,
    /// The marker key a write transaction also writes.
    pub marker: String,
    pub start: u64,
    pub end: u64,
    pub isolation: Isolation,
    /// Bounded staleness of a read-only transaction's snapshot, if not strong.
    pub stale_ms: Option<u64>,
    /// The read version an interactive transaction began at.
    pub read_version: Option<u64>,
    pub ops: Vec<Op>,
    /// Results of the operations whose responses arrived, in order.
    pub results: Vec<Observed>,
    pub outcome: Outcome,
}

impl Txn {
    fn writes(&self) -> bool {
        self.ops.iter().any(Op::is_write)
    }
}

/// The value of every key over time: `(version, value)` in version order,
/// `None` for a deletion.
#[derive(Default)]
struct Model {
    keys: BTreeMap<usize, Vec<(u64, Option<String>)>>,
}

impl Model {
    fn at(&self, key: usize, pos: u64) -> Option<(String, u64)> {
        let hist = self.keys.get(&key)?;
        let i = hist.partition_point(|(v, _)| *v <= pos);
        let (v, val) = hist.get(i.checked_sub(1)?)?;
        val.clone().map(|x| (x, *v))
    }

    fn live(&self, pos: u64, start: usize, end: usize) -> Vec<(usize, (String, u64))> {
        self.keys.range(start..end).filter_map(|(k, _)| Some((*k, self.at(*k, pos)?))).collect()
    }
}

/// The state a transaction sees: the model at `pos` under its own writes.
struct View<'a> {
    model: &'a Model,
    pos: u64,
    own: BTreeMap<usize, Option<String>>,
}

impl View<'_> {
    fn get(&self, key: usize) -> Option<Seen> {
        match self.own.get(&key) {
            Some(v) => v.clone().map(|value| Seen { value, version: None }),
            None => self.model.at(key, self.pos).map(|(value, v)| Seen { value, version: Some(v) }),
        }
    }

    fn range(&self, start: usize, end: usize) -> Vec<(usize, Seen)> {
        let mut m: BTreeMap<usize, Seen> = self
            .model
            .live(self.pos, start, end)
            .into_iter()
            .map(|(k, (value, v))| (k, Seen { value, version: Some(v) }))
            .collect();
        for (k, w) in self.own.range(start..end) {
            match w {
                Some(value) => m.insert(*k, Seen { value: value.clone(), version: None }),
                None => m.remove(k),
            };
        }
        m.into_iter().collect()
    }

    /// Applies `op`, returning what its result should report, or an error
    /// if the transaction must fail at it.
    fn apply(&mut self, op: &Op) -> Result<Observed, String> {
        Ok(match op {
            Op::Get { key } => Observed::Get(self.get(*key)),
            Op::Put { key, value } => {
                self.own.insert(*key, Some(value.clone()));
                Observed::Done
            }
            Op::Delete { key } => {
                self.own.insert(*key, None);
                Observed::Done
            }
            Op::Increment { key, by } => {
                let cur = match self.get(*key) {
                    Some(s) => s.value.parse::<i64>().map_err(|_| format!("increment of {:?}", s.value))?,
                    None => 0,
                };
                self.own.insert(*key, Some((cur + by).to_string()));
                Observed::Increment(cur + by)
            }
            Op::Scan { start, end, limit, reverse } => {
                let mut all = self.range(*start, *end);
                if *reverse {
                    all.reverse();
                }
                let more = all.len() > *limit;
                all.truncate(*limit);
                Observed::Scan { items: all, more }
            }
            Op::DeleteRange { start, end } => {
                for (k, _) in self.range(*start, *end) {
                    self.own.insert(k, None);
                }
                Observed::Done
            }
            Op::Check { key, exists } => {
                if self.get(*key).is_some() != *exists {
                    return Err(format!("check {} exists={exists} fails", name(*key)));
                }
                Observed::Done
            }
        })
    }
}

/// Replays `t`'s operations at `pos` and compares them with what it observed.
/// `exact_more` says whether scans' `more` flags are comparable: a scan's
/// validation covers only the interval it returned, so whether more keys
/// followed may change between its snapshot and its commit.
fn replay(model: &Model, t: &Txn, pos: u64, exact_more: bool) -> Result<BTreeMap<usize, Option<String>>, String> {
    let mut view = View { model, pos, own: BTreeMap::new() };
    // An uncommitted transaction failed after the last result it received.
    let n = match t.outcome {
        Outcome::NotCommitted | Outcome::Unknown => t.results.len(),
        _ => t.ops.len(),
    };
    for (i, op) in t.ops[..n].iter().enumerate() {
        let want = view.apply(op).map_err(|e| format!("op {i}: {e}"))?;
        let Some(got) = t.results.get(i) else { continue };
        let same = match (&want, got) {
            (Observed::Scan { items: a, more: m1 }, Observed::Scan { items: b, more: m2 }) => {
                a == b && (!exact_more || m1 == m2)
            }
            _ => want == *got,
        };
        if !same {
            return Err(format!("op {i} {op:?} observed {got:?}, but the state at {pos} gives {want:?}"));
        }
    }
    Ok(view.own)
}

/// Checks a complete history (see the module documentation).
pub fn check(history: &[Txn]) -> Result<(), String> {
    let mut writers: Vec<(u64, &Txn)> = history
        .iter()
        .filter_map(|t| match t.outcome {
            Outcome::Committed(v) => Some((v, t)),
            _ => None,
        })
        .collect();
    writers.sort_by_key(|(v, _)| *v);
    if let Some(w) = writers.windows(2).find(|w| w[0].0 == w[1].0) {
        return Err(format!("{} and {} both committed at version {}", w[0].1.client, w[1].1.client, w[0].0));
    }
    let mut model = Model::default();
    let mut write_sets: Vec<(u64, Vec<usize>)> = Vec::new();
    for (v, t) in &writers {
        // A serializable transaction read the state just before its commit.
        // Snapshot-isolated ones are checked below; their writes are the
        // same at either position unless first-committer-wins failed, which
        // that check reports.
        let mut ops_only = (*t).clone();
        if t.isolation == Isolation::Snapshot {
            // Its checks read its snapshot, which may differ here.
            ops_only.results.clear();
            ops_only.ops.retain(|o| !matches!(o, Op::Check { .. }));
        }
        let own = replay(&model, &ops_only, v - 1, false)
            .map_err(|e| format!("{} committed at {v}: {e}\n  {:?}", t.client, t.ops))?;
        // Only effective writes count: deleting a missing key changes
        // nothing, and the engine may not even write it.
        let own: Vec<(usize, Option<String>)> =
            own.into_iter().filter(|(k, val)| val.is_some() || model.at(*k, v - 1).is_some()).collect();
        write_sets.push((*v, own.iter().map(|(k, _)| *k).collect()));
        for (k, val) in own {
            model.keys.entry(k).or_default().push((*v, val));
        }
    }
    // The earliest position each transaction may take: after everything
    // that finished before it began.
    let pos = |t: &Txn| match t.outcome {
        Outcome::Committed(v) | Outcome::Read(v) => Some(v),
        Outcome::NotCommitted | Outcome::Unknown => None,
    };
    // Versions only grow, so whatever a finished transaction saw, even a
    // stale read, bounds what strong ones that began later see. A stale read
    // is bounded only by strong transactions that finished its staleness
    // before it began.
    let mut done: Vec<(u64, u64, bool)> =
        history.iter().filter_map(|t| Some((t.end, pos(t)?, t.stale_ms.is_some()))).collect();
    done.sort();
    let floor = |before: u64, strong_only: bool| -> u64 {
        let n = done.partition_point(|(end, _, _)| *end < before);
        done[..n].iter().filter(|(_, _, stale)| !(strong_only && *stale)).map(|(_, p, _)| *p).max().unwrap_or(0)
    };
    for t in history {
        let lo = match t.stale_ms {
            Some(s) => floor(t.start.saturating_sub(s), true),
            None => floor(t.start, false),
        };
        let who = || format!("{} [{}, {}] {:?}", t.client, t.start, t.end, t.outcome);
        if let Some(p) = pos(t)
            && p < lo
        {
            return Err(format!("{} is serialized at {p}, before version {lo} that finished before it began", who()));
        }
        if let Some(rv) = t.read_version {
            // A serializable interactive transaction begins on its node's
            // latest version without waiting for an in-flight write (SPEC
            // §5.5), so only its commit is ordered in real time.
            if rv < lo && t.isolation == Isolation::Snapshot {
                return Err(format!("{} began at read version {rv}, before version {lo} that finished first", who()));
            }
            replay(&model, t, rv, true).map_err(|e| format!("{} at its read version {rv}: {e}", who()))?;
        }
        match (t.outcome, t.isolation) {
            (Outcome::Read(r), _) => {
                // A revalidated read-only transaction reports the later
                // version its reads were validated at.
                let exact = t.read_version.is_none_or(|rv| rv == r);
                replay(&model, t, r, exact).map_err(|e| format!("{}: {e}", who()))?;
            }
            (Outcome::Committed(v), Isolation::Serializable) => {
                replay(&model, t, v - 1, false).map_err(|e| format!("{}: {e}", who()))?;
            }
            (Outcome::Committed(v), Isolation::Snapshot) => {
                let snap = match t.read_version {
                    Some(rv) => rv,
                    None => (lo..v)
                        .rev()
                        .find(|r| replay(&model, t, *r, false).is_ok())
                        .ok_or_else(|| format!("{}: no snapshot in [{lo}, {v}) explains its reads", who()))?,
                };
                let mine: Vec<usize> =
                    write_sets.iter().find(|(w, _)| *w == v).map(|(_, k)| k.clone()).unwrap_or_default();
                for (w, keys) in &write_sets {
                    if *w > snap && *w < v && keys.iter().any(|k| mine.contains(k)) {
                        return Err(format!(
                            "{}: committed over version {w}, which wrote one of its keys after its snapshot {snap}",
                            who()
                        ));
                    }
                }
            }
            (Outcome::NotCommitted | Outcome::Unknown, _) => {}
        }
    }
    Ok(())
}

/// Knobs that shape one run of the workload.
#[derive(Clone, Copy, Debug)]
pub struct Shape {
    pub keys: usize,
    pub clients: usize,
    pub txns: usize,
    pub max_ops: usize,
    /// Weights of get, put, delete, increment, scan, delete_range and check.
    pub weights: [u64; 7],
    /// Share of interactive transactions.
    pub interactive: f64,
    pub snapshot_isolation: f64,
    pub stale_reads: f64,
    /// Share of operations on the first key, which is then hot.
    pub hot: f64,
    /// Share of ambiguous commits resolved through the commit status rather
    /// than only from the markers.
    pub settle: f64,
    /// Share of ambiguous one-shot transactions retried with their
    /// idempotency key rather than resolved.
    pub retry: f64,
}

impl Shape {
    pub fn draw(k: &super::super::swarm::Knobs) -> Shape {
        let mut weights = [
            k.weight("history.get"),
            k.weight("history.put"),
            k.weight("history.delete"),
            k.weight("history.increment"),
            k.weight("history.scan"),
            k.weight("history.delete_range"),
            k.weight("history.check"),
        ];
        if weights.iter().all(|w| *w == 0) {
            weights = [1; 7];
        }
        Shape {
            keys: k.log_range("history.keys", 1, 40) as usize,
            clients: k.range("history.clients", 2, 8) as usize,
            txns: k.range("history.txns", 10, 40) as usize,
            max_ops: k.log_range("history.max_ops", 1, 24) as usize,
            weights,
            interactive: k.pick("history.interactive", &[0.0, 0.3, 0.7]),
            snapshot_isolation: k.pick("history.snapshot_isolation", &[0.0, 0.3, 0.8]),
            stale_reads: k.pick("history.stale_reads", &[0.0, 0.2]),
            hot: k.pick("history.hot", &[0.0, 0.3, 0.8]),
            settle: k.pick("history.settle", &[0.0, 0.5, 1.0]),
            retry: k.pick("history.retry", &[0.0, 0.5, 1.0]),
        }
    }
}

struct Gen<'a> {
    shape: &'a Shape,
    rng: Rng,
    uid: u64,
}

impl Gen<'_> {
    fn key(&mut self) -> usize {
        if self.rng.chance(self.shape.hot) { 0 } else { self.rng.usize(self.shape.keys) }
    }

    fn range(&mut self) -> (usize, usize) {
        let a = self.rng.usize(self.shape.keys + 1);
        let b = self.rng.usize(self.shape.keys + 1);
        (a.min(b), a.max(b) + 1)
    }

    fn ops(&mut self, read_only: bool) -> Vec<Op> {
        let n = 1 + self.rng.usize(self.shape.max_ops);
        let mut w = self.shape.weights;
        if read_only {
            for i in [1, 2, 3, 5] {
                w[i] = 0;
            }
            if w.iter().all(|x| *x == 0) {
                w[0] = 1;
            }
        }
        (0..n)
            .map(|i| match super::super::swarm::weighted(&mut self.rng, &w).unwrap_or(0) {
                0 => Op::Get { key: self.key() },
                1 => Op::Put { key: self.key(), value: format!("{}", self.uid * 64 + i as u64) },
                2 => Op::Delete { key: self.key() },
                3 => Op::Increment { key: self.key(), by: self.rng.range(0, 20) as i64 - 5 },
                4 => {
                    let (start, end) = self.range();
                    Op::Scan { start, end, limit: 1 + self.rng.usize(8), reverse: self.rng.chance(0.3) }
                }
                5 => {
                    let (start, end) = self.range();
                    Op::DeleteRange { start, end }
                }
                _ => Op::Check { key: self.key(), exists: self.rng.chance(0.5) },
            })
            .collect()
    }
}

fn parse_result(op: &Op, r: &Json) -> Result<Observed, String> {
    let seen = |j: &Json| -> Result<Seen, String> {
        let value = j["value"].as_str().ok_or(format!("no value in {j}"))?.to_string();
        Ok(Seen { value, version: j["version"].as_u64() })
    };
    Ok(match op {
        Op::Get { .. } => Observed::Get(if r["found"] == true { Some(seen(r)?) } else { None }),
        Op::Scan { .. } => {
            let mut items = Vec::new();
            for it in r["items"].as_array().ok_or(format!("no items in {r}"))? {
                let k = it["key"].as_str().and_then(key_of).ok_or(format!("odd key in {it}"))?;
                items.push((k, seen(it)?));
            }
            Observed::Scan { items, more: !r["next_cursor"].is_null() }
        }
        Op::Increment { .. } => Observed::Increment(r["value"].as_i64().ok_or(format!("no value in {r}"))?),
        _ => Observed::Done,
    })
}

fn parse_results(ops: &[Op], r: &Json) -> Result<Vec<Observed>, String> {
    let rs = r.as_array().ok_or(format!("no results: {r}"))?;
    if rs.len() != ops.len() {
        return Err(format!("{} results for {} ops: {r}", rs.len(), ops.len()));
    }
    ops.iter().zip(rs).map(|(o, r)| parse_result(o, r)).collect()
}

fn ops_json(ops: &[Op]) -> Json {
    Json::Array(ops.iter().map(Op::json).collect())
}

/// `ops` as a request sends them, followed by `t`'s marker if `last` is
/// the request that commits a write transaction.
fn request_ops(t: &Txn, ops: &[Op], last: bool) -> Json {
    let mut j = ops_json(ops);
    if last && t.writes() {
        j.as_array_mut().into_iter().for_each(|a| a.push(json!({"op": "put", "key": t.marker, "value": "1"})));
    }
    j
}

/// Parses the results of `ops`, ignoring a trailing marker's.
fn parse_sent(t: &Txn, ops: &[Op], r: &Json, last: bool) -> Result<Vec<Observed>, String> {
    let mut r = r.clone();
    if last && t.writes() {
        let missing = format!("no results: {r}");
        r.as_array_mut().ok_or(missing)?.pop();
    }
    parse_results(ops, &r)
}

/// Resolves an ambiguous commit through the fenced commit status, if
/// `resolve`; the markers settle what it cannot.
async fn resolve(c: &Client, db: &str, id: &str, t: &Txn, resolve: bool) -> Result<Outcome, String> {
    if !resolve {
        return Ok(Outcome::Unknown);
    }
    c.pause(ZOMBIE_WAIT_MS).await;
    Ok(match c.commit_status(db, id, t.start).await? {
        Status::Committed(v) => Outcome::Committed(v),
        Status::NotCommitted => Outcome::NotCommitted,
        Status::Unknown => Outcome::Unknown,
    })
}

fn definite_failure(status: u16, r: &super::super::client::Resp) -> bool {
    matches!(status, 404 | 409 | 412 | 429) || r.retryable_not_applied()
}

/// Runs `t` in one request. After an ambiguous failure, `retry` resends it
/// with the same idempotency key while its commit record would still
/// deduplicate it, first at once, racing the earlier attempt, then after
/// waiting it out; otherwise, or past that, the outcome is resolved.
async fn one_shot(c: &Client, db: &str, t: &mut Txn, key: String, settle: bool, retry: bool) -> Result<(), String> {
    let writes = t.writes();
    let mut body = json!({"ops": request_ops(t, &t.ops, true)});
    if t.isolation == Isolation::Snapshot {
        body["isolation"] = json!("snapshot");
    }
    if let Some(s) = t.stale_ms {
        body["consistency"] = json!("snapshot");
        body["max_staleness_ms"] = json!(s);
    }
    if writes {
        body["idempotency_key"] = json!(key);
    }
    let e = &c.sim.cfg.engine;
    let dedup_until = t.start + e.commit_retention_ms - 2 * e.clock_sync_max_error_ms;
    let (mut ambiguous, mut raced) = (false, false);
    loop {
        let r = c.call("POST", &format!("/v1/dbs/{db}/txn"), Some(body.clone())).await;
        // A retry that raced the earlier attempt can conflict with it; a
        // failed precondition is definite even then, because the server
        // fences the earlier attempt before answering 412 (SPEC §6.2).
        let unknown = match &r {
            Ok(r) => r.outcome_unknown() || (ambiguous && r.status == 503) || (raced && r.status == 409),
            Err(e) => *e != super::super::NetError::Refused || ambiguous,
        };
        if writes && unknown {
            raced = retry && !ambiguous;
            if !raced {
                c.pause(ZOMBIE_WAIT_MS).await;
            }
            if retry && c.sim.now_ms() < dedup_until {
                ambiguous = true;
                continue;
            }
            t.outcome = resolve(c, db, &format!("key:{key}"), t, settle).await?;
            return Ok(());
        }
        t.outcome = match r {
            Ok(r) if r.status == 200 => {
                let v = r.json["version"].as_u64().ok_or(format!("no version: {}", r.json))?;
                if r.json["replayed"] != true {
                    t.results = parse_sent(t, &t.ops, &r.json["results"], true)?;
                }
                if writes { Outcome::Committed(v) } else { Outcome::Read(v) }
            }
            // A retry is deduplicated by the earlier attempt's commit record,
            // which no attempt can add once the zombie wait is over: its
            // definite failure is the transaction's.
            Ok(r) if definite_failure(r.status, &r) => Outcome::NotCommitted,
            Ok(r) if !writes && r.status == 503 => Outcome::NotCommitted,
            Err(_) => Outcome::NotCommitted,
            Ok(r) => return Err(format!("{}: one-shot {}: {}", c.name, r.status, r.json)),
        };
        return Ok(());
    }
}

async fn interactive(c: &Client, db: &str, t: &mut Txn, rng: &mut Rng, settle: bool) -> Result<(), String> {
    let base = format!("/v1/dbs/{db}/transactions");
    // The operations go out in up to three calls: with begin, alone, and with commit.
    let n = t.ops.len();
    let a = rng.usize(n + 1);
    let b = a + rng.usize(n - a + 1);
    let mut begin = json!({"ops": ops_json(&t.ops[..a])});
    if t.isolation == Isolation::Snapshot {
        begin["isolation"] = json!("snapshot");
    }
    t.outcome = Outcome::NotCommitted;
    let r = match c.call("POST", &base, Some(begin)).await {
        Ok(r) if r.status == 201 => r,
        Ok(r) if definite_failure(r.status, &r) || r.status == 503 || r.status == 400 => return Ok(()),
        Ok(r) => return Err(format!("{}: begin {}: {}", c.name, r.status, r.json)),
        Err(_) => return Ok(()),
    };
    let id = r.json["txn_id"].as_str().ok_or("no txn_id")?.to_string();
    let cid = r.json["commit_id"].as_str().ok_or("no commit_id")?.to_string();
    t.read_version = Some(r.json["read_version"].as_u64().ok_or("no read_version")?);
    t.results = parse_results(&t.ops[..a], &r.json["results"])?;
    if b > a {
        let r = c.call("POST", &format!("{base}/{id}/ops"), Some(json!({"ops": ops_json(&t.ops[a..b])}))).await;
        match r {
            Ok(r) if r.status == 200 => t.results.extend(parse_results(&t.ops[a..b], &r.json["results"])?),
            // A failed call ends the transaction.
            _ => return Ok(()),
        }
    }
    if rng.chance(0.1) {
        let _ = c.call("DELETE", &format!("{base}/{id}"), None).await;
        return Ok(());
    }
    let r =
        c.call("POST", &format!("{base}/{id}/commit"), Some(json!({"ops": request_ops(t, &t.ops[b..], true)}))).await;
    t.outcome = match r {
        Ok(r) if r.status == 200 => {
            let v = r.json["version"].as_u64().ok_or(format!("no version: {}", r.json))?;
            let rest = parse_sent(t, &t.ops[b..], &r.json["results"], true)?;
            t.results.extend(rest);
            if t.writes() { Outcome::Committed(v) } else { Outcome::Read(v) }
        }
        Ok(r) if definite_failure(r.status, &r) || r.status == 400 => Outcome::NotCommitted,
        Ok(r) if r.outcome_unknown() => resolve(c, db, &cid, t, settle).await?,
        Err(super::super::NetError::Refused) => Outcome::NotCommitted,
        Err(_) => resolve(c, db, &cid, t, settle).await?,
        Ok(r) => return Err(format!("{}: commit {}: {}", c.name, r.status, r.json)),
    };
    Ok(())
}

pub async fn run(sim: Arc<Sim>, shape: Shape, chaos: bool) -> Result<(), String> {
    let owner = Client::signup(&sim, "hist", "hist@history.test", "password-123").await?;
    let db = "hist";
    owner.create_db(db).await?;
    let token = owner.token.clone().unwrap_or_default();
    let history: Arc<Mutex<Vec<Txn>>> = Arc::new(Mutex::new(Vec::new()));
    let phase = chaos.then(|| super::chaos_phase(&sim));
    let mut hs = Vec::new();
    for ci in 0..shape.clients {
        let c = Client::with_token(&sim, &format!("hist{ci}"), &token);
        let history = history.clone();
        let sim = sim.clone();
        hs.push(tokio::spawn(async move {
            let mut g = Gen { shape: &shape, rng: sim.fork_rng(), uid: 0 };
            for n in 0..shape.txns {
                g.uid = (ci as u64) << 32 | n as u64;
                let pause = g.rng.below(200);
                c.pause(pause).await;
                let interactive_txn = g.rng.chance(shape.interactive);
                let read_only = g.rng.chance(0.3);
                let stale = read_only && !interactive_txn && g.rng.chance(shape.stale_reads);
                let isolation = match g.rng.chance(shape.snapshot_isolation) && !stale {
                    true => Isolation::Snapshot,
                    false => Isolation::Serializable,
                };
                let mut t = Txn {
                    client: format!("{}#{n}", c.name),
                    marker: format!("m/{ci}/{n}"),
                    start: sim.now_ms(),
                    end: 0,
                    isolation,
                    stale_ms: stale.then(|| 50 + g.rng.below(2_000)),
                    read_version: None,
                    ops: g.ops(read_only),
                    results: Vec::new(),
                    outcome: Outcome::NotCommitted,
                };
                let mut rng = g.rng.fork();
                let settle = rng.chance(shape.settle);
                match interactive_txn {
                    true => interactive(&c, db, &mut t, &mut rng, settle).await?,
                    false => {
                        let retry = rng.chance(shape.retry);
                        one_shot(&c, db, &mut t, format!("{}-{n}", c.name), settle, retry).await?
                    }
                }
                t.end = sim.now_ms();
                sim.trace.event(&format!("history {t:?}"));
                history.lock().unwrap_or_else(|e| e.into_inner()).push(t);
            }
            Ok::<(), String>(())
        }));
    }
    let mut err = None;
    for h in hs {
        if let Err(e) = h.await.map_err(|e| e.to_string()).and_then(|r| r) {
            err = err.or(Some(e));
        }
    }
    if let Some(e) = err {
        return Err(e);
    }
    super::settle(&sim, phase).await;
    let markers = owner.paginate(&format!("/v1/dbs/{db}/kv?prefix=m/&limit=100"), "items").await?;
    let mut markers: BTreeMap<String, u64> = markers
        .iter()
        .map(|m| Some((m["key"].as_str()?.to_string(), m["version"].as_u64()?)))
        .collect::<Option<_>>()
        .ok_or("odd marker")?;
    let mut h = history.lock().unwrap_or_else(|e| e.into_inner()).clone();
    for t in h.iter_mut().filter(|t| t.writes()) {
        let applied = markers.remove(&t.marker);
        t.outcome = match (t.outcome, applied) {
            (Outcome::Committed(v), Some(m)) if v == m => t.outcome,
            (Outcome::NotCommitted, None) | (Outcome::Unknown, None) => Outcome::NotCommitted,
            (Outcome::Unknown, Some(m)) => {
                // It may have committed after its client gave up on it, so
                // it orders nothing that began later.
                t.end = u64::MAX;
                Outcome::Committed(m)
            }
            (o, m) => return Err(format!("{} was reported {o:?} but its marker shows {m:?}", t.client)),
        };
    }
    if let Some(m) = markers.keys().next() {
        return Err(format!("marker {m} of no write transaction"));
    }
    check(&h).map_err(|e| format!("history of {} transactions: {e}", h.len()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn txn(start: u64, end: u64, ops: Vec<Op>, results: Vec<Observed>, outcome: Outcome) -> Txn {
        Txn {
            client: format!("c@{start}"),
            marker: format!("m/{start}"),
            start,
            end,
            isolation: Isolation::Serializable,
            stale_ms: None,
            read_version: None,
            ops,
            results,
            outcome,
        }
    }

    fn put(key: usize, value: &str) -> Op {
        Op::Put { key, value: value.into() }
    }

    fn got(value: &str, version: u64) -> Observed {
        Observed::Get(Some(Seen { value: value.into(), version: Some(version) }))
    }

    fn get(key: usize) -> Op {
        Op::Get { key }
    }

    #[test]
    fn accepts_serializable_histories() {
        let h = [
            txn(0, 10, vec![put(1, "a")], vec![Observed::Done], Outcome::Committed(1)),
            txn(5, 20, vec![get(1), put(2, "b")], vec![got("a", 1), Observed::Done], Outcome::Committed(2)),
            txn(21, 30, vec![get(2), get(1)], vec![got("b", 2), got("a", 1)], Outcome::Read(2)),
            txn(3, 4, vec![get(1)], vec![Observed::Get(None)], Outcome::Read(0)),
            txn(22, 23, vec![put(1, "z")], vec![], Outcome::NotCommitted),
            txn(24, 40, vec![Op::Increment { key: 3, by: 4 }, get(3)], vec![], Outcome::Committed(3)),
            txn(41, 42, vec![get(3)], vec![got("4", 3)], Outcome::Read(3)),
        ];
        check(&h).unwrap();
    }

    #[test]
    fn rejects_anomalies() {
        let w1 = txn(0, 10, vec![put(1, "a")], vec![Observed::Done], Outcome::Committed(1));
        let stale = txn(11, 12, vec![get(1)], vec![Observed::Get(None)], Outcome::Read(0));
        assert!(check(&[w1.clone(), stale]).unwrap_err().contains("before version 1"));
        let lost_update = [
            w1.clone(),
            txn(11, 30, vec![get(1), put(1, "b")], vec![got("a", 1), Observed::Done], Outcome::Committed(2)),
            txn(11, 30, vec![get(1), put(1, "c")], vec![got("a", 1), Observed::Done], Outcome::Committed(3)),
        ];
        assert!(check(&lost_update).unwrap_err().contains("the state at 2"));
        let phantom = [
            w1.clone(),
            txn(
                11,
                12,
                vec![Op::Scan { start: 0, end: 5, limit: 5, reverse: false }],
                vec![Observed::Scan { items: vec![], more: false }],
                Outcome::Read(1),
            ),
        ];
        assert!(check(&phantom).is_err());
        let twice = [w1.clone(), txn(0, 10, vec![put(2, "a")], vec![], Outcome::Committed(1))];
        assert!(check(&twice).unwrap_err().contains("both committed"));
        let invented = [w1.clone(), txn(11, 12, vec![get(1)], vec![got("q", 1)], Outcome::Read(1))];
        assert!(check(&invented).is_err());
        let mut si = txn(11, 30, vec![put(1, "c")], vec![Observed::Done], Outcome::Committed(3));
        si.isolation = Isolation::Snapshot;
        si.read_version = Some(1);
        let fcw = [w1.clone(), txn(11, 20, vec![put(1, "b")], vec![], Outcome::Committed(2)), si.clone()];
        assert!(check(&fcw).unwrap_err().contains("after its snapshot"));
        let mut opaque = txn(11, 30, vec![get(1)], vec![Observed::Get(None)], Outcome::NotCommitted);
        opaque.read_version = Some(1);
        assert!(check(&[w1.clone(), opaque]).unwrap_err().contains("read version 1"));
        let mut skew_ok = si;
        skew_ok.ops = vec![get(2), put(3, "x")];
        skew_ok.results = vec![Observed::Get(None), Observed::Done];
        check(&[w1, skew_ok]).unwrap();
    }
}
