//! An event table dropped and recreated under the same name, and columns
//! added and dropped on it, while bulk and transactional inserts, seals,
//! merges and queries run on it.
//!
//! Schema changes take no idempotency key, so one whose response was lost
//! has an unknown version. Each is kept as the range of versions it may have
//! committed at: above the highest version seen before it was sent, and
//! below any write sent after it settled. An answer is checked only when
//! every schema change and insert falls on a known side of its version; it
//! must hold exactly the rows inserted into the generation of the table
//! that existed then, with the values of the column that existed then.

use super::super::Sim;
use super::super::client::Client;
use super::events::{Settled, settle};
use serde_json::{Value as Json, json};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

const DB: &str = "/v1/dbs/events";
const TABLE: &str = "/v1/dbs/events/events/churn";
const HOUR: i64 = 3_600_000;

fn definition() -> Json {
    json!({
        "name": "churn",
        "columns": [{"name": "time", "type": "time"}, {"name": "uid", "type": "string", "nullable": false},
            {"name": "k", "type": "int"}],
        "time_column": "time", "order_by": ["k", "time"], "partition": "hour",
    })
}

/// The default of added column `c{n}`, which events written before it read.
fn default_of(n: u32) -> i64 {
    1_000 + n as i64
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Kind {
    Create,
    Drop,
    AddCol(u32),
    DropCol(u32),
}

/// A schema change, which committed at a version in `(lo, hi]`; `hi` is
/// bounded by later writes when the response was lost.
#[derive(Clone, Debug)]
struct Ddl {
    kind: Kind,
    lo: u64,
    hi: Option<u64>,
    /// When the change had certainly committed.
    done_ms: u64,
}

struct Insert {
    sent_ms: u64,
    /// `None` when the outcome stayed unknown.
    version: Option<u64>,
    uids: Vec<String>,
}

struct Answer {
    sent_ms: u64,
    strong: bool,
    col: Option<u32>,
    aggregate: bool,
    json: Json,
}

#[derive(Default)]
struct World {
    ddl: Vec<Ddl>,
    /// Each event's `k`, and the added column it gave a value for.
    evs: BTreeMap<String, (i64, Option<(u32, i64)>)>,
    inserts: Vec<Insert>,
    /// Deletes by uid, with their versions, `None` when unknown.
    deletes: Vec<(Option<u64>, Vec<String>)>,
    /// The highest version any response showed.
    seen: u64,
    /// The added column, once its addition was acknowledged.
    col: Option<u32>,
    /// Whether the churner still changes the schema; inserts and queries
    /// run until it stops, and then a few more.
    churning: bool,
    answers: Vec<Answer>,
}

type Shared = Arc<Mutex<World>>;

fn world(w: &Shared) -> std::sync::MutexGuard<'_, World> {
    w.lock().unwrap_or_else(|e| e.into_inner())
}

/// The table at some version: whether it exists, the index of the schema
/// change that created its generation, and its added column.
struct State {
    exists: bool,
    create: usize,
    col: Option<u32>,
}

impl World {
    fn see(&mut self, j: &Json) {
        if let Some(v) = j["version"].as_u64() {
            self.seen = self.seen.max(v);
        }
    }

    /// The versions schema change `i` may have committed at, `(lo, hi]`.
    fn bounds(&self, i: usize) -> (u64, u64) {
        let d = &self.ddl[i];
        let later = || {
            self.inserts
                .iter()
                .filter(|x| x.sent_ms > d.done_ms)
                .filter_map(|x| x.version)
                .map(|v| v - 1)
                .min()
                .unwrap_or(u64::MAX)
        };
        (d.lo, d.hi.unwrap_or_else(later))
    }

    /// Whether schema change `i` is visible at version `v`, if known; a
    /// strong read sent after the change settled sees it.
    fn applied(&self, i: usize, v: u64, strong_sent: Option<u64>) -> Option<bool> {
        if strong_sent.is_some_and(|s| s > self.ddl[i].done_ms) {
            return Some(true);
        }
        let (lo, hi) = self.bounds(i);
        if hi <= v {
            Some(true)
        } else if lo >= v {
            Some(false)
        } else {
            None
        }
    }

    /// Whether schema change `i` committed after a write at version `w`,
    /// if known. Schema changes commit in order.
    fn after(&self, i: usize, w: u64) -> Option<bool> {
        if (0..=i).any(|j| self.bounds(j).0 >= w) {
            return Some(true);
        }
        if self.bounds(i).1 < w { Some(false) } else { None }
    }

    fn state_at(&self, v: u64, strong_sent: Option<u64>) -> Option<State> {
        let mut st = State { exists: false, create: 0, col: None };
        for (i, d) in self.ddl.iter().enumerate() {
            if !self.applied(i, v, strong_sent)? {
                break;
            }
            match d.kind {
                Kind::Create => st = State { exists: true, create: i, col: None },
                Kind::Drop => st = State { exists: false, create: i, col: None },
                Kind::AddCol(n) => st.col = Some(n),
                Kind::DropCol(_) => st.col = None,
            }
        }
        Some(st)
    }

    /// The uids the table holds at `v` in generation `st`, or `None` if an
    /// insert's place is unknown.
    fn rows_at(&self, v: u64, st: &State) -> Option<BTreeSet<&String>> {
        let mut out = BTreeSet::new();
        for x in &self.inserts {
            match x.version {
                None => return None,
                Some(w) if w <= v => {
                    if !self.after(st.create, w)? {
                        out.extend(&x.uids);
                    }
                }
                Some(_) => {}
            }
        }
        for (d, uids) in &self.deletes {
            match d {
                None => return None,
                Some(d) if *d <= v => out.retain(|u| !uids.contains(u)),
                Some(_) => {}
            }
        }
        Some(out)
    }

    /// The value event `uid` reads for added column `c{n}`.
    fn value(&self, uid: &str, n: u32) -> i64 {
        match self.evs.get(uid).and_then(|e| e.1) {
            Some((m, x)) if m == n => x,
            _ => default_of(n),
        }
    }

    fn check(&self, a: &Answer) -> Result<bool, String> {
        let v = a.json["version"].as_u64().ok_or(format!("churn answer without a version: {}", a.json))?;
        let Some(st) = self.state_at(v, a.strong.then_some(a.sent_ms)) else { return Ok(false) };
        if !st.exists {
            return Err(format!("churn answered at v{v}, when the table was dropped: {}", a.json));
        }
        if a.col.is_some() && a.col != st.col {
            return Err(format!("churn answered for column {:?} at v{v}, which had column {:?}", a.col, st.col));
        }
        let Some(uids) = self.rows_at(v, &st) else { return Ok(false) };
        let got = a.json["rows"].as_array().cloned().unwrap_or_default();
        let want: Vec<Json> = if a.aggregate {
            let k: i64 = uids.iter().map(|u| self.evs[*u].0).sum();
            let mut row = vec![json!(uids.len()), json!(k)];
            if let Some(n) = a.col {
                row.push(json!(uids.iter().map(|u| self.value(u, n)).sum::<i64>()));
            }
            vec![Json::Array(row)]
        } else {
            uids.iter()
                .map(|u| match a.col {
                    Some(n) => json!([u, self.value(u, n)]),
                    None => json!([u]),
                })
                .collect()
        };
        let num = |j: &Json| -> Json {
            match j {
                Json::Array(r) => Json::Array(r.iter().map(|x| x.as_f64().map_or(x.clone(), |f| json!(f))).collect()),
                _ => j.clone(),
            }
        };
        let (got, want): (Vec<Json>, Vec<Json>) = (got.iter().map(num).collect(), want.iter().map(num).collect());
        if got != want {
            return Err(format!(
                "churn at v{v} (generation {}, column {:?}): got {got:?}\nwant {want:?}",
                st.create, st.col
            ));
        }
        Ok(true)
    }
}

/// Whether a task that ran `step` of at least `steps` steps, `after` of
/// them since the churner stopped, is done. It stops at `10 * steps`
/// regardless, so a churner that cannot commit does not keep it going.
fn done(w: &Shared, step: usize, steps: usize, after: &mut usize) -> bool {
    if !world(w).churning {
        *after += 1;
    }
    (step >= steps && *after > 3) || step >= 10 * steps.max(10)
}

async fn inserter(c: Client, w: Shared, base: i64, steps: usize, transactional: bool) -> Result<(), String> {
    let mut after = 0;
    for step in 0.. {
        if done(&w, step, steps, &mut after) {
            break;
        }
        let col = world(&w).col;
        let n = 1 + c.rng().usize(8);
        let mut rows = Vec::new();
        let mut evs = Vec::new();
        for i in 0..n {
            let mut r = c.rng();
            let uid = format!("{}-{}", c.name, step * 100 + i);
            let k = r.below(5) as i64;
            let mut row = json!({"uid": uid, "time": base + r.below(12 * HOUR as u64) as i64, "k": k});
            let given = col.filter(|_| r.chance(0.7)).map(|n| (n, r.below(50) as i64));
            if let Some((n, x)) = given {
                row[format!("c{n}")] = json!(x);
            }
            rows.push(row);
            evs.push((uid, (k, given)));
        }
        let uids: Vec<String> = evs.iter().map(|e| e.0.clone()).collect();
        world(&w).evs.extend(evs);
        let (path, body, ndjson) = if transactional {
            (format!("{DB}/txn"), json!({"ops": [{"op": "insert_events", "table": "churn", "rows": rows}]}), false)
        } else {
            let nd = c.rng().chance(0.5);
            (format!("{TABLE}/rows"), if nd { json!(rows) } else { json!({"rows": rows}) }, nd)
        };
        let sent_ms = c.sim.now_ms();
        match settle(&c, &path, ndjson, &body, &format!("{}-{step}", c.name)).await? {
            Settled::Done(j) => {
                let v = j["version"].as_u64().ok_or(format!("{}: no version in {j}", c.name))?;
                let mut g = world(&w);
                g.see(&j);
                g.inserts.push(Insert { sent_ms, version: Some(v), uids });
            }
            Settled::Unknown => world(&w).inserts.push(Insert { sent_ms, version: None, uids }),
            // The table was dropped, or the column named was dropped, before the insert.
            Settled::Rejected(r) if r.status == 404 => {}
            Settled::Rejected(r) if r.status == 400 && r.json.to_string().contains("unknown column") => {}
            Settled::Rejected(r) => return Err(format!("{}: churn insert refused: {} {}", c.name, r.status, r.json)),
        }
        let ms = c.rng().below(400);
        c.pause(ms).await;
    }
    Ok(())
}

/// Deletes a few acknowledged events, of any generation, by uid.
async fn deleter(c: Client, w: Shared, steps: usize) -> Result<(), String> {
    let mut after = 0;
    for step in 0.. {
        if done(&w, step, steps, &mut after) {
            break;
        }
        let ms = 500 + c.rng().below(2_000);
        c.pause(ms).await;
        let victims: Vec<String> = {
            let g = world(&w);
            let acked: Vec<&String> = g.inserts.iter().filter(|x| x.version.is_some()).flat_map(|x| &x.uids).collect();
            let mut r = c.rng();
            match acked.is_empty() {
                true => continue,
                false => (0..1 + r.usize(4)).map(|_| acked[r.usize(acked.len())].clone()).collect(),
            }
        };
        let body = json!({"ops": [{"op": "delete_events", "table": "churn",
            "where": {"col": "uid", "op": "in", "value": victims}}]});
        let d = match settle(&c, &format!("{DB}/txn"), false, &body, &format!("{}-{step}", c.name)).await? {
            Settled::Done(j) => {
                world(&w).see(&j);
                Some(j["version"].as_u64().ok_or(format!("{}: no version in {j}", c.name))?)
            }
            Settled::Unknown => None,
            Settled::Rejected(r) if r.status == 404 => continue,
            Settled::Rejected(r) => return Err(format!("{}: churn delete refused: {} {}", c.name, r.status, r.json)),
        };
        world(&w).deletes.push((d, victims));
    }
    Ok(())
}

/// Runs a schema change, recording the versions it may have committed at.
/// `again` is the status a repeat of an earlier attempt that took effect gets.
async fn ddl(c: &Client, w: &Shared, kind: Kind, method: &str, path: &str, body: Option<Json>) -> Result<(), String> {
    let (ok, again) = match kind {
        Kind::Create => (201, 409),
        Kind::Drop | Kind::DropCol(_) => (200, 404),
        Kind::AddCol(_) => (200, 409),
    };
    let lo = world(w).seen;
    let (r, ambiguous) = c.settle(method, path, body).await?;
    let done_ms = c.sim.now_ms();
    let mut g = world(w);
    let hi = match r.status {
        s if s == ok => {
            g.see(&r.json);
            let v = r.json["version"].as_u64().ok_or(format!("churn {kind:?} without a version: {}", r.json))?;
            g.ddl.push(Ddl { kind, lo: v - 1, hi: Some(v), done_ms });
            return Ok(());
        }
        s if s == again && ambiguous => None,
        s => return Err(format!("churn {kind:?}: {s} {}", r.json)),
    };
    g.ddl.push(Ddl { kind, lo, hi, done_ms });
    Ok(())
}

/// Drops and recreates the table, and adds and drops a column, one change
/// at a time.
async fn churner(c: Client, w: Shared, steps: usize) -> Result<(), String> {
    let r = churn(&c, &w, steps).await;
    world(&w).churning = false;
    r
}

async fn churn(c: &Client, w: &Shared, steps: usize) -> Result<(), String> {
    let (c, w) = (c, w.clone());
    let mut next = 1u32;
    let mut col: Option<u32> = None;
    for _ in 0..steps {
        let ms = 1_000 + c.rng().below(3_000);
        c.pause(ms).await;
        let recreate = c.rng().chance(0.4);
        if recreate {
            world(&w).col = None;
            ddl(c, &w, Kind::Drop, "DELETE", TABLE, None).await?;
            ddl(c, &w, Kind::Create, "POST", &format!("{DB}/events"), Some(definition())).await?;
            col = None;
        } else if let Some(n) = col {
            world(&w).col = None;
            ddl(c, &w, Kind::DropCol(n), "DELETE", &format!("{TABLE}/columns/c{n}"), None).await?;
            col = None;
        } else {
            let n = next;
            next += 1;
            let spec = json!({"name": format!("c{n}"), "type": "int", "default": default_of(n)});
            ddl(c, &w, Kind::AddCol(n), "POST", &format!("{TABLE}/columns"), Some(spec)).await?;
            world(&w).col = Some(n);
            col = Some(n);
        }
    }
    Ok(())
}

fn query(c: &Client, col: Option<u32>) -> (Json, bool) {
    let mut r = c.rng();
    let strong = r.chance(0.3);
    let aggregate = r.chance(0.4);
    let mut q = json!({"from": i64::MIN / 4, "to": i64::MAX / 4});
    if aggregate {
        let mut aggs = json!({"a_n": {"fn": "count"}, "b_k": {"fn": "sum", "of": "k"}});
        if let Some(n) = col {
            aggs["c_x"] = json!({"fn": "sum", "of": format!("c{n}")});
        }
        q["aggregates"] = aggs;
    } else {
        let mut cols = vec![json!("uid")];
        cols.extend(col.map(|n| json!(format!("c{n}"))));
        q["columns"] = Json::Array(cols);
        q["order_by"] = json!(["uid"]);
        q["limit"] = json!(10_000);
    }
    if strong {
        q["consistency"] = json!("strong");
    }
    (q, aggregate)
}

async fn querier(c: Client, w: Shared, steps: usize) -> Result<(), String> {
    let mut after = 0;
    for step in 0.. {
        if done(&w, step, steps, &mut after) {
            break;
        }
        let ms = 200 + c.rng().below(800);
        c.pause(ms).await;
        let col = world(&w).col.filter(|_| c.rng().chance(0.8));
        let (q, aggregate) = query(&c, col);
        let sent_ms = c.sim.now_ms();
        match c.call("POST", &format!("{TABLE}/rows/query"), Some(q.clone())).await {
            Ok(r) if r.status == 200 => {
                if r.json["next_cursor"].is_string() {
                    continue;
                }
                let strong = q["consistency"] == "strong";
                let mut g = world(&w);
                g.see(&r.json);
                g.answers.push(Answer { sent_ms, strong, col, aggregate, json: r.json });
            }
            Ok(r) if r.status == 503 || r.status == 429 || r.status == 404 => {}
            Ok(r) if r.status == 400 && col.is_some_and(|n| r.json.to_string().contains(&format!("c{n}"))) => {}
            Ok(r) => return Err(format!("{}: churn query {q} failed: {} {}", c.name, r.status, r.json)),
            Err(_) => {}
        }
    }
    Ok(())
}

async fn setup(c: &Client, w: &Shared) -> Result<(), String> {
    ddl(c, w, Kind::Create, "POST", &format!("{DB}/events"), Some(definition())).await
}

/// The churn workload's tasks.
pub struct Churn {
    w: Shared,
}

impl Churn {
    pub async fn start(
        sim: &Arc<Sim>,
        client: impl Fn(&str) -> Client,
        base: i64,
        steps: usize,
    ) -> Result<(Churn, Vec<tokio::task::JoinHandle<Result<(), String>>>), String> {
        let w: Shared = Arc::default();
        world(&w).churning = true;
        setup(&client("churn-setup"), &w).await?;
        sim.trace.event("events: churning table churn");
        let tasks = vec![
            tokio::spawn(inserter(client("chb"), w.clone(), base, steps, false)),
            tokio::spawn(inserter(client("cht"), w.clone(), base, steps / 2, true)),
            tokio::spawn(churner(client("churner"), w.clone(), steps / 3 + 1)),
            tokio::spawn(deleter(client("chd"), w.clone(), steps / 4)),
            tokio::spawn(querier(client("chq"), w.clone(), steps)),
        ];
        Ok((Churn { w }, tasks))
    }

    /// Checks every answer whose version the model places, then the final
    /// table: rows of earlier generations are gone, every row acknowledged
    /// since the last creation is present, and every row reads its value of
    /// the added column.
    pub async fn check(&self, c: &Client) -> Result<(), String> {
        let mut checked = 0;
        {
            let g = world(&self.w);
            for a in &g.answers {
                checked += g.check(a)? as usize;
            }
        }
        let col = world(&self.w).col;
        let sent_ms = c.sim.now_ms();
        let mut got: BTreeMap<String, Option<i64>> = BTreeMap::new();
        let mut cursor: Option<String> = None;
        let mut version = 0;
        loop {
            let mut cols = vec![json!("uid")];
            cols.extend(col.map(|n| json!(format!("c{n}"))));
            let mut q = json!({"from": i64::MIN / 4, "to": i64::MAX / 4, "columns": cols, "order_by": ["uid"],
                "limit": 500, "consistency": "strong"});
            if let Some(cur) = &cursor {
                q["cursor"] = json!(cur);
            }
            let r = c.call_retry("POST", &format!("{TABLE}/rows/query"), Some(q)).await;
            if r.status != 200 {
                return Err(format!("churn final read: {} {}", r.status, r.json));
            }
            version = version.max(r.json["version"].as_u64().unwrap_or(0));
            for row in r.json["rows"].as_array().into_iter().flatten() {
                let u = row[0].as_str().unwrap_or_default().to_string();
                if got.insert(u.clone(), row[1].as_i64()).is_some() {
                    return Err(format!("churn final read holds {u} twice"));
                }
            }
            match r.json["next_cursor"].as_str() {
                Some(n) => cursor = Some(n.to_string()),
                None => break,
            }
        }
        let g = world(&self.w);
        let st = g.state_at(version, Some(sent_ms)).ok_or("churn: final state unknown")?;
        if !st.exists || st.col != col {
            return Err(format!("churn: final table exists {} with column {:?}, expected {col:?}", st.exists, st.col));
        }
        let (mut must, mut may) = (BTreeSet::new(), BTreeSet::new());
        for x in &g.inserts {
            match x.version.map(|w| g.after(st.create, w)) {
                Some(Some(true)) => {}
                Some(Some(false)) => must.extend(x.uids.iter()),
                _ => may.extend(x.uids.iter()),
            }
        }
        for u in g.deletes.iter().filter(|d| d.0.is_none()).flat_map(|d| &d.1) {
            if must.remove(u) {
                may.insert(u);
            }
        }
        for u in g.deletes.iter().filter(|d| d.0.is_some()).flat_map(|d| &d.1) {
            must.remove(u);
            may.remove(u);
        }
        let missing: Vec<&&String> = must.iter().filter(|u| !got.contains_key(**u)).take(5).collect();
        let unexpected: Vec<&String> = got.keys().filter(|u| !must.contains(u) && !may.contains(u)).take(5).collect();
        if !missing.is_empty() || !unexpected.is_empty() {
            return Err(format!(
                "churn table (generation {}) holds the wrong rows at v{version}: missing {missing:?}, unexpected \
                 {unexpected:?} (rows of a dropped generation, or of no insert)",
                st.create
            ));
        }
        if let Some(n) = col {
            for (u, x) in &got {
                if *x != Some(g.value(u, n)) {
                    return Err(format!("churn row {u} reads c{n} = {x:?}, expected {}", g.value(u, n)));
                }
            }
        }
        c.sim.trace.event(&format!(
            "events: churn {} rows after {} schema changes, {checked} of {} answers checked",
            got.len(),
            g.ddl.len(),
            g.answers.len()
        ));
        Ok(())
    }
}
