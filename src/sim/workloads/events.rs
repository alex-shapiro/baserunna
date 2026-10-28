//! Simulation workload for event tables.
//!
//! Concurrent clients bulk insert events (JSON and NDJSON, with idempotency
//! keys) and insert them in transactions together with row upserts, delete
//! events, add a column mid-run, and query at random snapshots, while seals,
//! merges and bulk binds run on whichever nodes hold leases under faults.
//! Every query is checked against a reference model that evaluates it naively
//! over the rows committed at the response's version, and at quiescence the
//! table must hold exactly the rows the model says, with retention honored.

use super::super::Sim;
use super::super::client::{Client, Resp, Status};
use bytes::Bytes;
use serde_json::{Value as Json, json};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

const DB: &str = "/v1/dbs/events";
const HOUR: i64 = 3_600_000;
/// Default of the column added mid-run, which older rows report.
const EXTRA_DEFAULT: i64 = 7;
const KINDS: [&str; 4] = ["view", "click", "buy", "visit"];

#[derive(Clone, Debug, PartialEq)]
struct Ev {
    uid: String,
    time: i64,
    kind: String,
    g: i64,
    v: Option<f64>,
    props: Json,
    distinct_id: String,
    extra: Option<i64>,
}

impl Ev {
    fn json(&self) -> Json {
        let mut j = json!({
            "uid": self.uid, "time": self.time, "kind": self.kind, "g": self.g,
            "props": self.props, "distinct_id": self.distinct_id,
        });
        if let Some(v) = self.v {
            j["v"] = json!(v);
        }
        if let Some(x) = self.extra {
            j["extra"] = json!(x);
        }
        j
    }
}

/// A committed write: its version, or `None` when its outcome stayed unknown.
#[derive(Clone, Debug)]
enum Write {
    Insert(Vec<String>),
    Delete(Vec<String>),
    Person(String, String),
}

#[derive(Default)]
struct World {
    events: BTreeMap<String, Ev>,
    /// Writes known to be committed, with their versions.
    committed: Vec<(u64, Write)>,
    /// Writes whose outcome is unknown (commit records expired).
    unknown: Vec<Write>,
    /// The version from which queries may name the added column.
    extra_since: Option<u64>,
    /// Queries answered: the query, its answer, and the time it was sent.
    answers: Vec<(Json, Json)>,
}

type Shared = Arc<Mutex<World>>;

fn world(w: &Shared) -> std::sync::MutexGuard<'_, World> {
    w.lock().unwrap_or_else(|e| e.into_inner())
}

/// The outcome of an idempotent write.
enum Settled {
    Done(Json),
    Unknown,
    Rejected(Resp),
}

/// Retries an idempotent write until its outcome is known, rotating the
/// key when an unapplied attempt was fenced.
async fn settle(c: &Client, path: &str, ndjson: bool, body: &Json, key: &str) -> Result<Settled, String> {
    let mut key = key.to_string();
    let mut first = c.sim.now_ms();
    for attempt in 0..200 {
        let (bytes, ct) = match (ndjson, body) {
            (true, Json::Array(rows)) => {
                (Bytes::from(rows.iter().map(|r| r.to_string() + "\n").collect::<String>()), "application/x-ndjson")
            }
            _ => {
                let mut b = body.clone();
                if b.get("ops").is_some() {
                    b["idempotency_key"] = json!(key);
                }
                (Bytes::from(b.to_string()), "application/json")
            }
        };
        let headers = vec![("content-type", ct.to_string()), ("idempotency-key", key.clone())];
        let (ambiguous, aborted) = match c.send("POST", path, &headers, bytes).await {
            Ok(raw) => {
                let json = serde_json::from_slice(&raw.body).unwrap_or(Json::Null);
                let r = Resp { status: raw.status, json, raw };
                match r.status {
                    200 | 201 => return Ok(Settled::Done(r.json)),
                    503 if r.outcome_unknown() => (true, false),
                    503 | 429 => (false, false),
                    409 if r.code() == "conflict" => (false, false),
                    409 if r.code() == "aborted" => (false, true),
                    _ => return Ok(Settled::Rejected(r)),
                }
            }
            Err(_) => (true, false),
        };
        if !ambiguous && !aborted {
            c.pause(200).await;
            continue;
        }
        if ambiguous {
            match c.commit_status("events", &format!("key:{key}"), first).await? {
                Status::Committed(j) => return Ok(Settled::Done(j)),
                Status::Unknown => return Ok(Settled::Unknown),
                Status::NotCommitted => {}
            }
        }
        (key, first) = (format!("{key}.{attempt}"), c.sim.now_ms());
    }
    Err(format!("{}: POST {path} never settled", c.name))
}

fn event(c: &Client, base: i64, n: usize, extra: bool) -> Ev {
    let mut r = c.rng();
    let mut props = serde_json::Map::new();
    for k in 0..r.usize(4) {
        let key = format!("p{}", (k + r.usize(3)) % 5);
        props.insert(key, if r.chance(0.5) { json!(r.below(4)) } else { json!(format!("s{}", r.below(3))) });
    }
    Ev {
        uid: format!("{}-{n}", c.name),
        time: base + r.below(72 * HOUR as u64) as i64,
        kind: KINDS[r.usize(KINDS.len())].to_string(),
        g: r.below(10) as i64,
        v: r.chance(0.8).then(|| (r.below(400) as f64 - 100.0) / 4.0),
        props: Json::Object(props),
        distinct_id: format!("d{}", r.below(8)),
        extra: (extra && r.chance(0.7)).then(|| r.below(50) as i64),
    }
}

async fn inserter(c: Client, w: Shared, base: i64, steps: usize, transactional: bool) -> Result<(), String> {
    for step in 0..steps {
        let extra = world(&w).extra_since.is_some();
        let n = 1 + c.rng().usize(12);
        let evs: Vec<Ev> = (0..n).map(|i| event(&c, base, step * 100 + i, extra)).collect();
        let rows: Vec<Json> = evs.iter().map(Ev::json).collect();
        let key = format!("{}-{step}", c.name);
        let person = transactional.then(|| {
            let mut r = c.rng();
            (format!("d{}", r.below(8)), format!("person{}", r.below(4)))
        });
        let (path, body, ndjson) = match &person {
            Some((d, p)) => (
                format!("{DB}/txn"),
                json!({"ops": [
                    {"op": "insert_events", "table": "e", "rows": rows},
                    {"op": "upsert_row", "table": "persons", "row": {"distinct_id": d, "person_id": p}},
                ]}),
                false,
            ),
            None => {
                let nd = c.rng().chance(0.5);
                (format!("{DB}/events/e/rows"), if nd { json!(rows) } else { json!({"rows": rows}) }, nd)
            }
        };
        let uids: Vec<String> = evs.iter().map(|e| e.uid.clone()).collect();
        {
            let mut g = world(&w);
            for e in &evs {
                g.events.insert(e.uid.clone(), e.clone());
            }
        }
        match settle(&c, &path, ndjson, &body, &key).await? {
            Settled::Done(j) => {
                let v = j["version"].as_u64().ok_or(format!("{}: no version in {j}", c.name))?;
                let mut g = world(&w);
                g.committed.push((v, Write::Insert(uids)));
                if let Some((d, p)) = person {
                    g.committed.push((v, Write::Person(d, p)));
                }
            }
            Settled::Unknown => {
                let mut g = world(&w);
                g.unknown.push(Write::Insert(uids));
                if let Some((d, p)) = person {
                    g.unknown.push(Write::Person(d, p));
                }
            }
            Settled::Rejected(r) => return Err(format!("{}: insert refused: {} {}", c.name, r.status, r.json)),
        }
        let ms = c.rng().below(300);
        c.pause(ms).await;
    }
    Ok(())
}

async fn deleter(c: Client, w: Shared, steps: usize) -> Result<(), String> {
    for step in 0..steps {
        let ms = 500 + c.rng().below(1_500);
        c.pause(ms).await;
        let victims: Vec<String> = {
            let g = world(&w);
            let live: Vec<&String> = g
                .committed
                .iter()
                .filter_map(|(_, wr)| match wr {
                    Write::Insert(u) => Some(u),
                    _ => None,
                })
                .flatten()
                .collect();
            let mut r = c.rng();
            (0..r.usize(6)).filter_map(|_| (!live.is_empty()).then(|| live[r.usize(live.len())].clone())).collect()
        };
        if victims.is_empty() {
            continue;
        }
        let body = json!({"ops": [{"op": "delete_events", "table": "e", "where": {"col": "uid", "op": "in", "value": victims}}]});
        match settle(&c, &format!("{DB}/txn"), false, &body, &format!("{}-{step}", c.name)).await? {
            Settled::Done(j) => {
                let v = j["version"].as_u64().ok_or(format!("{}: no version in {j}", c.name))?;
                world(&w).committed.push((v, Write::Delete(victims)));
            }
            Settled::Unknown => world(&w).unknown.push(Write::Delete(victims)),
            Settled::Rejected(r) => return Err(format!("{}: delete refused: {} {}", c.name, r.status, r.json)),
        }
    }
    Ok(())
}

/// Builds a random query whose answer the model can compute.
fn random_query(c: &Client, base: i64, extra: bool, uid: Option<String>) -> Json {
    let mut r = c.rng();
    let from =
        base + r.below(60) as i64 * HOUR - 6 * HOUR + if r.chance(0.5) { r.below(HOUR as u64) as i64 } else { 0 };
    let to = from + (1 + r.below(80)) as i64 * HOUR;
    let mut q = json!({"from": from, "to": to});
    let filters = [
        json!({"col": "kind", "op": "eq", "value": KINDS[r.usize(4)]}),
        json!({"col": "kind", "op": "in", "value": [KINDS[r.usize(4)], KINDS[r.usize(4)]]}),
        json!({"col": "g", "op": "lt", "value": r.below(10)}),
        json!({"col": "g", "op": "ge", "value": r.below(10)}),
        json!({"col": "props", "path": [format!("p{}", r.below(5))], "op": "eq", "value": r.below(4)}),
        json!({"col": "distinct_id", "op": "eq", "value": format!("d{}", r.below(8))}),
        json!({"col": "uid", "op": "prefix", "value": format!("bulk{}", r.below(2))}),
        json!({"not": {"col": "v", "op": "exists"}}),
        json!({"col": "uid", "op": "eq", "value": uid.unwrap_or_else(|| format!("bulk{}-{}", r.below(2), r.below(400)))}),
    ];
    if r.chance(0.6) {
        q["where"] = filters[r.usize(filters.len())].clone();
    }
    if r.chance(0.2) {
        q["consistency"] = json!("strong");
    }
    if r.chance(0.2) {
        q["columns"] = json!(["uid", "g", "kind"]);
        q["order_by"] = json!([{"col": "uid", "desc": r.chance(0.5)}]);
        q["limit"] = json!(1 + r.usize(10));
        return q;
    }
    let groups = [
        json!([]),
        json!(["kind"]),
        json!(["g"]),
        json!([{"bucket": {"col": "time"}, "interval": "6h"}]),
        json!([{"col": "props", "path": ["p1"], "as": "p1"}]),
        json!([{"lookup": "person"}]),
    ];
    let group = groups[r.usize(groups.len())].clone();
    if group == json!([{"lookup": "person"}]) {
        q["lookups"] = json!({"person": {"table": "persons", "key": {"col": "distinct_id"}, "value": "person_id",
            "default": {"col": "distinct_id"}}});
    }
    q["group_by"] = group;
    let mut aggs = json!({"n": {"fn": "count"}, "sum_g": {"fn": "sum", "of": "g"}, "min_v": {"fn": "min", "of": "v"},
        "max_v": {"fn": "max", "of": "v"}, "uids": {"fn": "uniq_exact", "of": "uid"},
        "buys": {"fn": "count", "where": {"col": "kind", "op": "eq", "value": "buy"}}, "avg_v": {"fn": "avg", "of": "v"}});
    if extra {
        aggs["extra"] = json!({"fn": "sum", "of": "extra"});
    }
    q["aggregates"] = aggs;
    if r.chance(0.2) {
        q["then"] = json!({"aggregates": {"groups": {"fn": "count"}, "total": {"fn": "sum", "of": "n"}}});
    }
    q
}

/// The rows visible at version `v`, and the persons table then, or `None`
/// if an unknown write makes the snapshot uncertain.
fn snapshot(g: &World, v: u64) -> Option<(Vec<&Ev>, BTreeMap<String, String>)> {
    if !g.unknown.is_empty() {
        return None;
    }
    let mut writes: Vec<&(u64, Write)> = g.committed.iter().filter(|(cv, _)| *cv <= v).collect();
    writes.sort_by_key(|(cv, _)| *cv);
    let mut live = BTreeSet::new();
    let mut persons = BTreeMap::new();
    for (_, wr) in writes {
        match wr {
            Write::Insert(u) => live.extend(u.iter().cloned()),
            Write::Delete(u) => u.iter().for_each(|x| {
                live.remove(x);
            }),
            Write::Person(d, p) => {
                persons.insert(d.clone(), p.clone());
            }
        }
    }
    Some((live.iter().filter_map(|u| g.events.get(u)).collect(), persons))
}

fn holds(e: &Ev, f: &Json) -> bool {
    if let Some(n) = f.get("not") {
        return !holds(e, n);
    }
    let col = f["col"].as_str().unwrap_or("");
    let val: Json = match col {
        "kind" => json!(e.kind),
        "g" => json!(e.g),
        "uid" => json!(e.uid),
        "distinct_id" => json!(e.distinct_id),
        "v" => e.v.map_or(Json::Null, |v| json!(v)),
        "props" => e.props.get(f["path"][0].as_str().unwrap_or("")).cloned().unwrap_or(Json::Null),
        _ => Json::Null,
    };
    if val.is_null() {
        return false;
    }
    let num = |a: &Json, b: &Json| a.as_f64().zip(b.as_f64());
    match f["op"].as_str().unwrap_or("") {
        "exists" => true,
        "eq" => val == f["value"] || num(&val, &f["value"]).is_some_and(|(a, b)| a == b),
        "in" => f["value"].as_array().is_some_and(|a| a.contains(&val)),
        "lt" => num(&val, &f["value"]).is_some_and(|(a, b)| a < b),
        "ge" => num(&val, &f["value"]).is_some_and(|(a, b)| a >= b),
        "prefix" => val.as_str().zip(f["value"].as_str()).is_some_and(|(a, p)| a.starts_with(p)),
        _ => false,
    }
}

/// The model's answer to aggregate or rows query `q` over `rows`.
fn model_answer(q: &Json, rows: &[&Ev], persons: &BTreeMap<String, String>) -> Vec<Vec<Json>> {
    let (from, to) = (q["from"].as_i64().unwrap_or(0), q["to"].as_i64().unwrap_or(0));
    let sel: Vec<&&Ev> =
        rows.iter().filter(|e| e.time >= from && e.time < to && q.get("where").is_none_or(|f| holds(e, f))).collect();
    if let Some(cols) = q.get("columns") {
        let desc = q["order_by"][0]["desc"].as_bool().unwrap_or(false);
        let mut uids: Vec<&&Ev> = sel;
        uids.sort_by(|a, b| a.uid.cmp(&b.uid));
        if desc {
            uids.reverse();
        }
        let _ = cols;
        return uids
            .iter()
            .take(q["limit"].as_u64().unwrap_or(100) as usize)
            .map(|e| vec![json!(e.uid), json!(e.g), json!(e.kind)])
            .collect();
    }
    let key = |e: &Ev| -> Json {
        match &q["group_by"][0] {
            Json::String(s) if s == "kind" => json!(e.kind),
            Json::String(s) if s == "g" => json!(e.g),
            Json::Object(o) if o.contains_key("bucket") => json!(e.time.div_euclid(6 * HOUR) * 6 * HOUR),
            Json::Object(o) if o.contains_key("lookup") => {
                json!(persons.get(&e.distinct_id).cloned().unwrap_or_else(|| e.distinct_id.clone()))
            }
            Json::Object(o) if o.contains_key("path") => e.props.get("p1").cloned().unwrap_or(Json::Null),
            _ => Json::Null,
        }
    };
    let grouped = q["group_by"].as_array().is_some_and(|a| !a.is_empty());
    let mut groups: BTreeMap<String, (Json, Vec<&Ev>)> = BTreeMap::new();
    for e in sel {
        let k = key(e);
        groups.entry(k.to_string()).or_insert_with(|| (k.clone(), Vec::new())).1.push(e);
    }
    if !grouped && groups.is_empty() {
        groups.insert(String::new(), (Json::Null, Vec::new()));
    }
    let names: Vec<String> = q["aggregates"].as_object().map(|o| o.keys().cloned().collect()).unwrap_or_default();
    if q.get("then").is_some() {
        let total: usize = groups.values().map(|(_, es)| es.len()).sum();
        return vec![vec![json!(groups.len()), json!(total)]];
    }
    groups
        .values()
        .map(|(k, es)| {
            let mut row = if grouped { vec![k.clone()] } else { Vec::new() };
            let vs: Vec<f64> = es.iter().filter_map(|e| e.v).collect();
            for n in &names {
                row.push(match n.as_str() {
                    "n" => json!(es.len()),
                    "sum_g" => json!(es.iter().map(|e| e.g).sum::<i64>()),
                    "min_v" => vs.iter().copied().reduce(f64::min).map_or(Json::Null, |v| json!(v)),
                    "max_v" => vs.iter().copied().reduce(f64::max).map_or(Json::Null, |v| json!(v)),
                    "avg_v" if vs.is_empty() => Json::Null,
                    "avg_v" => json!(vs.iter().sum::<f64>() / vs.len() as f64),
                    "uids" => json!(es.len()),
                    "buys" => json!(es.iter().filter(|e| e.kind == "buy").count()),
                    "extra" => json!(es.iter().map(|e| e.extra.unwrap_or(EXTRA_DEFAULT)).sum::<i64>()),
                    _ => Json::Null,
                });
            }
            row
        })
        .collect()
}

/// Rows compared as sets, with numbers compared by value.
fn normalize(rows: &[Vec<Json>], ordered: bool) -> Vec<String> {
    fn num(v: &Json) -> Json {
        match v {
            Json::Number(n) => n.as_f64().map_or(v.clone(), |f| json!(f)),
            Json::Array(a) => Json::Array(a.iter().map(num).collect()),
            _ => v.clone(),
        }
    }
    let mut out: Vec<String> = rows.iter().map(|r| Json::Array(r.iter().map(num).collect()).to_string()).collect();
    if !ordered {
        out.sort();
    }
    out
}

async fn querier(c: Client, w: Shared, base: i64, steps: usize) -> Result<(), String> {
    for _ in 0..steps {
        let ms = 200 + c.rng().below(800);
        c.pause(ms).await;
        let (extra, uid) = {
            let g = world(&w);
            let i = c.rng().below(g.events.len().max(1) as u64) as usize;
            (g.extra_since.is_some(), g.events.keys().nth(i).cloned())
        };
        let q = random_query(&c, base, extra, uid);
        let r = c.call("POST", &format!("{DB}/events/e/query"), Some(q.clone())).await;
        match r {
            Ok(r) if r.status == 200 => world(&w).answers.push((q, r.json)),
            Ok(r) if r.status == 503 || r.status == 429 => {}
            Ok(r) if r.status == 400 && r.json.to_string().contains("extra") => {}
            Ok(r) => return Err(format!("{}: query {q} failed: {} {}", c.name, r.status, r.json)),
            Err(_) => {}
        }
    }
    Ok(())
}

fn check_answers(g: &World) -> Result<usize, String> {
    let mut checked = 0;
    for (q, a) in &g.answers {
        let v = a["version"].as_u64().ok_or(format!("answer without a version: {a}"))?;
        if q["aggregates"].get("extra").is_some() && g.extra_since.is_none_or(|s| v < s) {
            continue;
        }
        let Some((rows, persons)) = snapshot(g, v) else { continue };
        let want = model_answer(q, &rows, &persons);
        let got: Vec<Vec<Json>> = a["rows"]
            .as_array()
            .ok_or(format!("answer without rows: {a}"))?
            .iter()
            .map(|r| r.as_array().cloned().unwrap_or_default())
            .collect();
        let ordered = q.get("columns").is_some();
        if normalize(&got, ordered) != normalize(&want, ordered) {
            return Err(format!("query {q} at v{v}\n got {got:?}\nwant {want:?}"));
        }
        checked += 1;
    }
    Ok(checked)
}

/// Reads every live row's uid at a strong snapshot.
async fn all_uids(c: &Client) -> Result<(BTreeSet<String>, u64), String> {
    let mut uids = BTreeSet::new();
    let mut cursor: Option<String> = None;
    let mut version = 0;
    loop {
        let mut q = json!({"from": i64::MIN / 4, "to": i64::MAX / 4, "columns": ["uid"], "order_by": ["uid"],
            "limit": 500, "consistency": "strong"});
        if let Some(cur) = &cursor {
            q["cursor"] = json!(cur);
        }
        let r = c.call_retry("POST", &format!("{DB}/events/e/query"), Some(q)).await;
        if r.status != 200 {
            return Err(format!("final read: {} {}", r.status, r.json));
        }
        version = version.max(r.json["version"].as_u64().unwrap_or(0));
        for row in r.json["rows"].as_array().into_iter().flatten() {
            uids.insert(row[0].as_str().unwrap_or_default().to_string());
        }
        match r.json["next_cursor"].as_str() {
            Some(n) => cursor = Some(n.to_string()),
            None => return Ok((uids, version)),
        }
    }
}

async fn setup(c: &Client) -> Result<(), String> {
    c.create_db("events").await?;
    let table = json!({
        "name": "e",
        "columns": [
            {"name": "time", "type": "time"}, {"name": "uid", "type": "string", "nullable": false},
            {"name": "kind", "type": "string"}, {"name": "g", "type": "int"}, {"name": "v", "type": "float"},
            {"name": "props", "type": "json"}, {"name": "distinct_id", "type": "string"},
        ],
        "time_column": "time", "order_by": ["kind", "time"], "partition": "day",
        "skip_indexes": [{"column": "uid", "type": "bloom"}],
    });
    let old = json!({
        "name": "old", "columns": [{"name": "time", "type": "time"}, {"name": "n", "type": "int"}],
        "time_column": "time", "order_by": ["time"], "partition": "hour", "retention_ms": 2 * HOUR,
    });
    for (path, body) in [
        (format!("{DB}/events"), table),
        (format!("{DB}/events"), old),
        (
            format!("{DB}/tables"),
            json!({"name": "persons", "columns": [{"name": "distinct_id", "type": "string", "nullable": false},
                {"name": "person_id", "type": "string"}], "primary_key": ["distinct_id"]}),
        ),
    ] {
        let r = c.call_settled("POST", &path, Some(body)).await;
        if r.status != 201 && r.status != 409 {
            return Err(format!("setup {path}: {} {}", r.status, r.json));
        }
    }
    Ok(())
}

pub async fn run(sim: Arc<Sim>, steps: usize, chaos: bool) -> Result<(), String> {
    let owner = Client::signup(&sim, "events", "events@events.test", "password-123").await?;
    setup(&owner).await?;
    let token = owner.token.clone().unwrap_or_default();
    let client = |name: &str| Client::with_token(&sim, name, &token);
    let base = (sim.now_ms() as i64).div_euclid(HOUR) * HOUR;
    let w: Shared = Arc::default();
    let phase = chaos.then(|| super::chaos_phase(&sim));
    let mut tasks = Vec::new();
    for i in 0..2 {
        tasks.push(tokio::spawn(inserter(client(&format!("bulk{i}")), w.clone(), base, steps, false)));
    }
    tasks.push(tokio::spawn(inserter(client("txn"), w.clone(), base, steps / 2, true)));
    tasks.push(tokio::spawn(deleter(client("del"), w.clone(), steps / 4)));
    tasks.push(tokio::spawn(querier(client("q0"), w.clone(), base, steps)));
    let alter = {
        let (c, w) = (client("alter"), w.clone());
        tokio::spawn(async move {
            let ms = 5_000 + c.rng().below(10_000);
            c.pause(ms).await;
            let col = json!({"name": "extra", "type": "int", "default": EXTRA_DEFAULT});
            let r = c.call_settled("POST", &format!("{DB}/events/e/columns"), Some(col)).await;
            match r.status {
                200 => {
                    world(&w).extra_since = r.json["version"].as_u64();
                    Ok(())
                }
                409 => Ok(()),
                s => Err(format!("add column: {s} {}", r.json)),
            }
        })
    };
    tasks.push(alter);
    let old = {
        let c = client("old");
        tokio::spawn(async move {
            for i in 0..4 {
                let now = c.sim.now_ms() as i64;
                let rows = json!({"rows": [{"time": now - 30 * HOUR, "n": i}, {"time": now + 30 * HOUR, "n": i}],
                    "idempotency_key": format!("old-{i}")});
                let r = c.call_settled("POST", &format!("{DB}/events/old/rows"), Some(rows)).await;
                if r.status != 201 && r.status != 200 {
                    return Err(format!("old rows: {} {}", r.status, r.json));
                }
            }
            Ok(())
        })
    };
    tasks.push(old);
    let mut err = None;
    for t in tasks {
        match t.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => err = err.or(Some(e)),
            Err(e) => err = err.or(Some(format!("task panicked: {e}"))),
        }
    }
    super::settle(&sim, phase).await;
    if let Some(e) = err {
        return Err(e);
    }
    let c = client("checker");
    let (uids, version) = all_uids(&c).await?;
    let mut g = world(&w);
    let unknown = std::mem::take(&mut g.unknown);
    let certain = unknown.is_empty();
    for u in unknown {
        match u {
            Write::Insert(us) => {
                let present = us.iter().filter(|u| uids.contains(*u)).count();
                let deleted =
                    |u: &String| g.committed.iter().any(|(_, wr)| matches!(wr, Write::Delete(d) if d.contains(u)));
                if present != 0 && us.iter().any(|u| !uids.contains(u) && !deleted(u)) {
                    return Err(format!("an insert of unknown outcome is partly present: {us:?}"));
                }
                if present > 0 {
                    g.committed.push((0, Write::Insert(us)));
                }
            }
            Write::Delete(us) => {
                if us.iter().all(|u| !uids.contains(u)) {
                    g.committed.push((version, Write::Delete(us)));
                }
            }
            Write::Person(..) => {}
        }
    }
    let (rows, _) = snapshot(&g, version).ok_or("unresolved writes")?;
    let want: BTreeSet<String> = rows.iter().map(|e| e.uid.clone()).collect();
    if want != uids {
        let missing: Vec<&String> = want.difference(&uids).take(5).collect();
        let extra: Vec<&String> = uids.difference(&want).take(5).collect();
        return Err(format!("the table holds the wrong rows at v{version}: missing {missing:?}, unexpected {extra:?}"));
    }
    let checked = if certain { check_answers(&g)? } else { 0 };
    sim.trace.event(&format!("events: {} rows, {checked} answers checked", uids.len()));
    drop(g);
    check_retention(&c).await?;
    tour(&c).await
}

/// Exercises the schema endpoints on a scratch table and checks their effects.
async fn tour(c: &Client) -> Result<(), String> {
    let t = format!("{DB}/events/tour");
    let expect = |r: &Resp, status: u16, what: &str| -> Result<(), String> {
        if r.status == status { Ok(()) } else { Err(format!("tour {what}: {} {}", r.status, r.json)) }
    };
    let def = json!({"name": "tour", "columns": [{"name": "time", "type": "time", "default": "now"},
        {"name": "who", "type": "string"}, {"name": "n", "type": "int"}], "time_column": "time", "order_by": ["time"]});
    let r = c.call_settled("POST", &format!("{DB}/events"), Some(def)).await;
    if r.status != 201 && r.status != 409 {
        return Err(format!("tour create: {} {}", r.status, r.json));
    }
    let r = c.call_settled("POST", &format!("{t}/skip-indexes"), Some(json!({"column": "who", "type": "bloom"}))).await;
    if r.status != 200 && r.status != 409 {
        return Err(format!("tour add skip index: {} {}", r.status, r.json));
    }
    let r = c.call_settled("DELETE", &format!("{t}/columns/who"), None).await;
    expect(&r, 400, "drop an indexed column")?;
    let r = c.call_settled("DELETE", &format!("{t}/skip-indexes/who"), None).await;
    if r.status != 200 && r.status != 404 {
        return Err(format!("tour drop skip index: {} {}", r.status, r.json));
    }
    let r = c.call_settled("DELETE", &format!("{t}/columns/who"), None).await;
    if r.status != 200 && r.status != 404 {
        return Err(format!("tour drop column: {} {}", r.status, r.json));
    }
    let r = c.call_settled("PATCH", &t, Some(json!({"retention_ms": 86_400_000}))).await;
    expect(&r, 200, "set retention")?;
    if r.json["retention_ms"] != json!(86_400_000) || r.json["columns"].to_string().contains("\"who\"") {
        return Err(format!("tour schema after changes: {}", r.json));
    }
    let rows = json!({"rows": [{"n": 1}, {"n": 2}, {"n": 3}], "idempotency_key": "tour-rows"});
    let r = c.call_settled("POST", &format!("{t}/rows"), Some(rows)).await;
    if r.status != 201 && r.status != 200 {
        return Err(format!("tour insert: {} {}", r.status, r.json));
    }
    let del = json!({"ops": [{"op": "delete_events", "table": "tour", "where": {"col": "n", "op": "ge", "value": 2}}],
        "idempotency_key": "tour-delete"});
    let r = c.call_settled("POST", &format!("{DB}/txn"), Some(del)).await;
    expect(&r, 200, "delete in a transaction")?;
    let r = c
        .call_settled("POST", &format!("{t}/delete"), Some(json!({"where": {"col": "n", "op": "eq", "value": 3}})))
        .await;
    expect(&r, 200, "delete")?;
    let r = c.call_retry("GET", &t, None).await;
    expect(&r, 200, "describe")?;
    if r.json["rows"] != json!(1) {
        return Err(format!("tour rows after deletes: {}", r.json));
    }
    let r = c.call_retry("GET", &format!("{DB}/events"), None).await;
    expect(&r, 200, "list")?;
    if !r.json["tables"].as_array().is_some_and(|ts| ts.iter().any(|x| x["name"] == "tour")) {
        return Err(format!("tour missing from the list: {}", r.json));
    }
    let r = c.call_settled("DELETE", &t, None).await;
    if r.status != 200 && r.status != 404 {
        return Err(format!("tour drop: {} {}", r.status, r.json));
    }
    let r = c.call_retry("GET", &t, None).await;
    expect(&r, 404, "describe a dropped table")
}

/// Parts of the retained table past their retention are dropped by
/// maintenance after the next commit, and newer rows stay.
async fn check_retention(c: &Client) -> Result<(), String> {
    let deadline = c.sim.now_ms() + 120_000;
    for round in 0.. {
        let rows = json!({"rows": [{"time": c.sim.now_ms(), "n": 99}], "idempotency_key": format!("check-{round}")});
        let r = c.call_settled("POST", &format!("{DB}/events/old/rows"), Some(rows)).await;
        if r.status != 201 && r.status != 200 {
            return Err(format!("old rows: {} {}", r.status, r.json));
        }
        c.pause(3_000).await;
        let now = c.sim.now_ms() as i64;
        let q = json!({"from": i64::MIN / 4, "to": i64::MAX / 4, "consistency": "strong",
            "aggregates": {"stale": {"fn": "count", "where": {"col": "time", "op": "lt", "value": now - 3 * HOUR}},
                           "future": {"fn": "count", "where": {"col": "time", "op": "gt", "value": now + 20 * HOUR}}}});
        let a = c.call_retry("POST", &format!("{DB}/events/old/query"), Some(q)).await;
        let col = |n: &str| {
            let cols = a.json["columns"].as_array().cloned().unwrap_or_default();
            cols.iter().position(|c| c == n).and_then(|i| a.json["rows"][0][i].as_u64())
        };
        let (stale, future) = (col("stale"), col("future"));
        if future != Some(4) {
            return Err(format!("rows within retention were lost: {}", a.json));
        }
        if stale == Some(0) {
            return Ok(());
        }
        if c.sim.now_ms() > deadline {
            return Err(format!("rows past retention were never dropped: {}", a.json));
        }
    }
    Ok(())
}
