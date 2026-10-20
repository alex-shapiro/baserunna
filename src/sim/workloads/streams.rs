//! Simulation workload for stream appends, reads, consumers, long-poll followers, and retention.
//!
//! Concurrent clients append records with idempotency keys, query ranges and filters, follow
//! streams via long polling and server-sent events, advance consumer cursors in transactions,
//! and trim records under fault injection. At quiescence, all recorded reads, follower logs,
//! consumer cursors, and stream states are verified against the committed append history.

use super::super::Sim;
use super::super::client::{Client, Resp, Status};
use bytes::Bytes;
use serde_json::{Value as Json, json};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

const DB: &str = "/v1/dbs/streams";
const LEVELS: [&str; 3] = ["info", "warn", "error"];
const RET_BYTES: u64 = 3_000;
const RET_MS: u64 = 40_000;

/// Expected record content and append time window.
#[derive(Clone, Debug, PartialEq)]
struct Expect {
    body: Json,
    headers: Json,
    window: (u64, u64),
}

#[derive(Clone, Debug)]
struct Read {
    stream: String,
    spec: Json,
    resp: Json,
    window: (u64, u64),
}

#[derive(Default)]
struct World {
    log: BTreeMap<String, BTreeMap<u64, Expect>>,
    /// Acknowledged appends: `(stream, next_seq, ack_time_ms)`.
    acked: Vec<(String, u64, u64)>,
    /// Sent appends: `(stream, record_count, start_time_ms)`.
    sent: Vec<(String, u64, u64)>,
    reads: Vec<Read>,
    /// Acknowledged explicit trims: `(stream, before_seq, ack_time_ms)`.
    trims: Vec<(String, u64, u64)>,
    /// Conditional appends: `(writer, step) -> seq`.
    cas: BTreeMap<(String, u64), u64>,
    /// Appends with unknown outcomes.
    uncertain: Vec<Uncertain>,
    /// Count of `tx_writer` transactions with unknown outcomes.
    tx_unknown: u64,
}

/// Append with an unknown outcome, resolved by inspecting records read from the stream.
#[derive(Debug)]
struct Uncertain {
    stream: String,
    records: Vec<Expect>,
    cas: Option<((String, u64), u64)>,
}

type Shared = Arc<Mutex<World>>;

fn world(w: &Shared) -> std::sync::MutexGuard<'_, World> {
    w.lock().unwrap_or_else(|e| e.into_inner())
}

/// Outcome of an idempotent request.
enum Settled {
    Answer(Resp),
    /// Committed on an earlier attempt whose response was lost.
    Applied(Json),
    /// Commit record retention expired before the outcome was determined.
    Unknown,
}

/// Retries a request with idempotency key `key` (in `headers` or in a JSON
/// `body`) until it receives a definitive response. On an ambiguous failure,
/// resolves the commit at once; a status check aborts an unapplied commit for
/// good, so the request is retried under a new key, as it is if an earlier
/// attempt's failed precondition resolved the key.
async fn settle_call(
    c: &Client,
    method: &str,
    path: &str,
    headers: &[(&str, String)],
    body: Bytes,
    key: &str,
) -> Result<Settled, String> {
    let (mut key, mut headers, mut body) = (key.to_string(), headers.to_vec(), body);
    let mut first = c.sim.now_ms();
    for attempt in 0..200 {
        let (ambiguous, aborted) = match c.send(method, path, &headers, body.clone()).await {
            Ok(raw) => {
                let json = serde_json::from_slice(&raw.body).unwrap_or(Json::Null);
                let r = Resp { status: raw.status, json, raw };
                match r.status {
                    503 if r.outcome_unknown() => (true, false),
                    503 | 429 => (false, false),
                    409 if r.code() == "conflict" => (false, false),
                    409 if r.code() == "aborted" => (false, true),
                    _ => return Ok(Settled::Answer(r)),
                }
            }
            Err(_) => (true, false),
        };
        if !ambiguous && !aborted {
            c.pause(200).await;
            continue;
        }
        if ambiguous {
            match c.commit_status("streams", &format!("key:{key}"), first).await? {
                Status::Committed(j) => return Ok(Settled::Applied(j)),
                Status::Unknown => return Ok(Settled::Unknown),
                Status::NotCommitted => {}
            }
        }
        let fresh = format!("{key}.{attempt}");
        for (_, v) in headers.iter_mut().filter(|(h, _)| *h == "idempotency-key") {
            *v = fresh.clone();
        }
        let quoted = |k: &str| format!("\"idempotency_key\":{}", Json::from(k));
        if let Ok(text) = std::str::from_utf8(&body)
            && text.contains(&quoted(&key))
        {
            body = Bytes::from(text.replace(&quoted(&key), &quoted(&fresh)));
        }
        (key, first) = (fresh, c.sim.now_ms());
    }
    Err(format!("{}: {method} {path} never settled", c.name))
}

#[derive(Clone, Copy, Debug)]
enum Format {
    Json,
    Ndjson,
    Text,
}

fn record(name: &str, step: usize, j: usize, pad: usize) -> (Json, Json) {
    let lvl = LEVELS[(step + j) % 3];
    (json!({"w": name, "i": step, "j": j, "lvl": lvl, "pad": "x".repeat(pad)}), json!({"w": name}))
}

/// Appends `k` records to `stream` and records their sequence numbers.
async fn append(
    c: &Client,
    w: &Shared,
    stream: &str,
    step: usize,
    k: usize,
    format: Format,
    expect_seq: Option<u64>,
) -> Result<Option<u64>, String> {
    let pad = c.rng().below(if k > 10 { 150 } else { 60 }) as usize;
    let recs: Vec<(Json, Json)> = (0..k).map(|j| record(&c.name, step, j, pad)).collect();
    let (ct, body, expect): (&str, Vec<u8>, Vec<(Json, Json)>) = match format {
        Format::Json => {
            let rs: Vec<Json> = recs.iter().map(|(b, h)| json!({"json": b, "headers": h})).collect();
            ("application/json", json!({"records": rs}).to_string().into_bytes(), recs.clone())
        }
        Format::Ndjson => {
            let lines: String = recs.iter().map(|(b, _)| format!("{b}\n")).collect();
            ("application/x-ndjson", lines.into_bytes(), recs.iter().map(|(b, _)| (b.clone(), Json::Null)).collect())
        }
        Format::Text => {
            let texts: Vec<String> = (0..k).map(|j| format!("{}:{step}:{j}", c.name)).collect();
            let body = texts.join("\r\n").into_bytes();
            ("text/plain", body, texts.into_iter().map(|t| (Json::String(t), Json::Null)).collect())
        }
    };
    let key = format!("{}-{stream}-{step}", c.name);
    let start = c.sim.now_ms();
    world(w).sent.push((stream.to_string(), k as u64, start));
    let headers = [("content-type", ct.to_string()), ("idempotency-key", key.clone())];
    let query = match (expect_seq, stream.starts_with("chat:")) {
        (Some(n), _) => format!("?expect_seq={n}"),
        (None, true) => "?create=true".to_string(),
        (None, false) => String::new(),
    };
    let path = format!("{DB}/streams/{stream}/records{query}");
    let settled = settle_call(c, "POST", &path, &headers, Bytes::from(body), &key).await?;
    let end = c.sim.now_ms();
    let expect: Vec<Expect> = expect
        .into_iter()
        .map(|(b, headers)| {
            let body = match format {
                Format::Text => json!({"value": b}),
                _ => json!({"json": b}),
            };
            Expect { body, headers, window: (start, end) }
        })
        .collect();
    let first = match settled {
        Settled::Applied(s) => s["appends"][0].as_u64().ok_or_else(|| format!("status without appends: {s}"))?,
        Settled::Answer(r) if r.status == 412 => return Ok(None),
        Settled::Unknown => {
            let cas = expect_seq.map(|n| ((c.name.clone(), step as u64), n));
            world(w).uncertain.push(Uncertain { stream: stream.to_string(), records: expect, cas });
            return Ok(None);
        }
        Settled::Answer(r) if r.status == 200 => {
            let first = r.json["first_seq"].as_u64().ok_or("missing first_seq")?;
            if r.json["count"] != k || r.json["next_seq"].as_u64() != Some(first + k as u64) {
                return Err(format!("append of {k} to {stream}: {}", r.json));
            }
            first
        }
        Settled::Answer(r) => return Err(format!("append to {stream}: {} {}", r.status, r.json)),
    };
    let mut g = world(w);
    insert(&mut g, stream, first, expect)?;
    g.acked.push((stream.to_string(), first + k as u64, end));
    Ok(Some(first))
}

/// Resolves uncertain appends by matching their records against observed stream reads.
fn resolve(w: &mut World, seen: &BTreeMap<String, BTreeMap<u64, Json>>) -> Result<(), String> {
    for u in std::mem::take(&mut w.uncertain) {
        let mut firsts = BTreeSet::new();
        for (seq, r) in seen.get(&u.stream).into_iter().flatten() {
            for (j, e) in (0..).zip(&u.records) {
                if *seq >= j && *r == shape(e, *seq) {
                    firsts.insert(seq - j);
                }
            }
        }
        let Some(&first) = firsts.first() else { continue };
        if firsts.len() > 1 {
            return Err(format!("an append to {} applied at each of {firsts:?}", u.stream));
        }
        if let Some((by, expected)) = u.cas {
            if first != expected {
                return Err(format!("an append expecting {expected} took {first}"));
            }
            w.cas.insert(by, first);
        }
        insert(w, &u.stream, first, u.records)?;
    }
    Ok(())
}

fn insert(w: &mut World, stream: &str, first: u64, records: Vec<Expect>) -> Result<(), String> {
    let log = w.log.entry(stream.to_string()).or_default();
    for (seq, e) in (first..).zip(records) {
        if let Some(old) = log.insert(seq, e) {
            return Err(format!("{stream}: seq {seq} appended twice (first {old:?})"));
        }
    }
    Ok(())
}

async fn appender(c: Client, w: Shared, streams: Vec<String>, steps: usize) -> Result<(), String> {
    for step in 0..steps {
        let stream = streams[c.rng().usize(streams.len())].clone();
        let bulk = c.rng().chance(0.08);
        let k = if bulk { 30 + c.rng().usize(30) } else { 1 + c.rng().usize(4) };
        let format = c.rng().below(20);
        let format = match format {
            0..=11 => Format::Json,
            12..=16 => Format::Ndjson,
            _ => Format::Text,
        };
        append(&c, &w, &stream, step, k, format, None).await?;
        let d = c.rng().below(300);
        c.pause(d).await;
    }
    Ok(())
}

/// Appends to `cas` using conditional `expect_seq` checks.
async fn cas_writer(c: Client, w: Shared, steps: usize) -> Result<(), String> {
    for step in 0..steps {
        let info = c.call_retry("GET", &format!("{DB}/streams/cas"), None).await;
        let next = info.json["next_seq"].as_u64().ok_or_else(|| format!("cas info: {}", info.json))?;
        if let Some(first) = append(&c, &w, "cas", step, 1, Format::Json, Some(next)).await? {
            if first != next {
                return Err(format!("an append expecting {next} took {first}"));
            }
            world(&w).cas.insert((c.name.clone(), step as u64), first);
        }
        let d = c.rng().below(200);
        c.pause(d).await;
    }
    Ok(())
}

/// Appends to `tx` and increments a counter in the same transaction.
async fn tx_writer(c: Client, w: Shared, steps: usize) -> Result<(), String> {
    for step in 0..steps {
        let body = json!({
            "ops": [
                {"op": "increment", "key": "tx/n", "by": 1},
                {"op": "append", "stream": "tx", "records": [{"json": {"step": step}}]},
            ],
            "idempotency_key": format!("tx-{step}"),
        });
        let headers = [("content-type", "application/json".to_string())];
        let key = format!("tx-{step}");
        let r =
            match settle_call(&c, "POST", &format!("{DB}/txn"), &headers, Bytes::from(body.to_string()), &key).await? {
                Settled::Applied(s) => Some(s),
                Settled::Answer(r) if r.status == 200 => Some(r.json),
                Settled::Unknown => None,
                Settled::Answer(r) => return Err(format!("tx append: {} {}", r.status, r.json)),
            };
        match r {
            Some(r) if r["appends"].as_array().is_none_or(|a| a.len() != 1) => {
                return Err(format!("tx append: {r}"));
            }
            Some(r) if !r["results"].is_null() && r["results"][1]["first_seq"] != r["appends"][0] => {
                return Err(format!("tx append results: {r}"));
            }
            Some(_) => {}
            None => world(&w).tx_unknown += 1,
        }
        let check = json!({"ops": [{"op": "get", "key": "tx/n"}, {"op": "stream_info", "stream": "tx"}]});
        let r = c.call_retry("POST", &format!("{DB}/txn"), Some(check)).await;
        let n: u64 = r.json["results"][0]["value"].as_str().and_then(|v| v.parse().ok()).unwrap_or(0);
        if r.status != 200 || r.json["results"][1]["next_seq"] != n {
            return Err(format!("the counter and its stream disagree: {}", r.json));
        }
        let d = c.rng().below(400);
        c.pause(d).await;
    }
    Ok(())
}

async fn reader(c: Client, w: Shared, done: Arc<AtomicBool>) -> Result<(), String> {
    while !done.load(Ordering::SeqCst) {
        let streams = ["log", "chat:0", "chat:1", "ret", "cas"];
        let stream = streams[c.rng().usize(streams.len())];
        let mut q: Vec<(String, String)> = Vec::new();
        let mut spec = serde_json::Map::new();
        let start = c.rng().below(5);
        match start {
            0 => spec.insert("from_seq".into(), json!(c.rng().below(120))),
            1 => spec.insert("tail".into(), json!(c.rng().below(20))),
            2 => {
                let t = c.sim.now_ms().saturating_sub(c.rng().below(20_000));
                spec.insert("from_time".into(), json!(t))
            }
            _ => None,
        };
        if c.rng().chance(0.2) {
            spec.insert("until_seq".into(), json!(c.rng().below(150)));
        }
        if c.rng().chance(0.3) {
            spec.insert("reverse".into(), json!(true));
        }
        let most = if c.rng().chance(0.5) { 8 } else { 300 };
        spec.insert("limit".into(), json!(1 + c.rng().below(most)));
        let filtered = c.rng().chance(0.25);
        let start = c.sim.now_ms();
        let r = if filtered {
            let lvl = LEVELS[c.rng().usize(3)];
            spec.insert("filter".into(), json!({"field": "lvl", "op": "eq", "value": lvl}));
            spec.insert("scan_limit".into(), json!(1 + c.rng().below(40)));
            c.call("POST", &format!("{DB}/streams/{stream}/records/query"), Some(Json::Object(spec.clone()))).await
        } else {
            for (k, v) in &spec {
                q.push((k.clone(), v.to_string()));
            }
            let qs: Vec<String> = q.iter().map(|(k, v)| format!("{k}={v}")).collect();
            c.get(&format!("{DB}/streams/{stream}/records?{}", qs.join("&"))).await
        };
        let end = c.sim.now_ms();
        match r {
            Ok(r) if r.status == 200 => world(&w).reads.push(Read {
                stream: stream.into(),
                spec: Json::Object(spec),
                resp: r.json,
                window: (start, end),
            }),
            Ok(r) if r.status == 404 && stream.starts_with("chat:") => {}
            Ok(r) if r.status >= 500 || r.status == 429 => {}
            Ok(r) => return Err(format!("read {stream} {spec:?}: {} {}", r.status, r.json)),
            Err(_) => {}
        }
        let d = c.rng().below(500);
        c.pause(d).await;
    }
    Ok(())
}

/// Follows `stream` from sequence 0 using long polling or server-sent events.
async fn follower(c: Client, stream: &str, events: bool, done: Arc<AtomicBool>) -> Result<Vec<(u64, Json)>, String> {
    let mut got: Vec<(u64, Json)> = Vec::new();
    let mut cursor = 0u64;
    let mut idle = 0;
    loop {
        let finished = done.load(Ordering::SeqCst);
        let wait = c.rng().below(if finished { 300 } else { 8_000 });
        let limit = 1 + c.rng().below(50);
        let mut path = format!("{DB}/streams/{stream}/records?from_seq={cursor}&wait_ms={wait}&limit={limit}");
        if events {
            path.push_str("&format=sse");
        }
        let r = match c.get(&path).await {
            Ok(r) if r.status == 200 => r,
            Ok(r) if r.status == 404 && stream.starts_with("chat:") && !finished => continue,
            Ok(r) if r.status >= 500 || r.status == 429 || r.status == 404 => continue,
            Ok(r) => return Err(format!("follow {stream}: {} {}", r.status, r.json)),
            Err(_) => continue,
        };
        let records: Vec<Json> = match events {
            true => parse_events(&r.raw.body)?,
            false => r.json["records"].as_array().cloned().unwrap_or_default(),
        };
        for rec in &records {
            let seq = rec["seq"].as_u64().ok_or("record without seq")?;
            if seq != cursor {
                return Err(format!("{stream} follower at {cursor} received {seq}"));
            }
            got.push((seq, rec.clone()));
            cursor += 1;
        }
        if !events && r.json["next_seq"].as_u64() != Some(cursor) {
            return Err(format!("{stream} follower: next_seq {} after {cursor}", r.json["next_seq"]));
        }
        idle = if records.is_empty() { idle + 1 } else { 0 };
        if finished && idle >= 3 {
            return Ok(got);
        }
    }
}

/// Parses records from a server-sent event stream.
fn parse_events(body: &[u8]) -> Result<Vec<Json>, String> {
    let text = std::str::from_utf8(body).map_err(|_| "event stream not UTF-8")?;
    let mut out = Vec::new();
    for ev in text.split("\n\n").filter(|e| !e.trim().is_empty()) {
        let (mut id, mut data, mut kind) = (None, None, "message");
        for line in ev.lines() {
            if let Some(v) = line.strip_prefix("id: ") {
                id = v.parse::<u64>().ok();
            } else if let Some(v) = line.strip_prefix("data: ") {
                data = Some(v);
            } else if let Some(v) = line.strip_prefix("event: ") {
                kind = v;
            } else if !line.starts_with(':') {
                return Err(format!("invalid event line {line:?}"));
            }
        }
        match (kind, id, data) {
            ("error", _, _) => break,
            ("message", Some(id), Some(d)) => {
                let rec: Json = serde_json::from_str(d).map_err(|e| format!("event data: {e}"))?;
                if rec["seq"] != id {
                    return Err(format!("event {id} has mismatched seq: {rec}"));
                }
                out.push(rec);
            }
            (_, None, None) => {}
            _ => return Err(format!("malformed event {ev:?}")),
        }
    }
    Ok(out)
}

/// Reads from `log` using a consumer cursor and advances the cursor in the same transaction.
async fn consumer(c: Client, w: Shared, done: Arc<AtomicBool>) -> Result<(), String> {
    let mut idle = 0;
    loop {
        let finished = done.load(Ordering::SeqCst);
        let r = c.call("POST", &format!("{DB}/transactions"), Some(json!({}))).await;
        let Ok(r) = r.filter_status(201) else {
            c.pause(200).await;
            continue;
        };
        let (id, cid) = (r.json["txn_id"].as_str().unwrap_or_default().to_string(), r.json["commit_id"].clone());
        let ops =
            json!({"ops": [{"op": "read_stream", "stream": "log", "consumer": "proc", "limit": 1 + c.rng().below(6)}]});
        let start = c.sim.now_ms();
        let Ok(r) = c.call("POST", &format!("{DB}/transactions/{id}/ops"), Some(ops)).await.filter_status(200) else {
            continue;
        };
        let read = r.json["results"][0].clone();
        let records = read["records"].as_array().cloned().unwrap_or_default();
        world(&w).reads.push(Read {
            stream: "log".into(),
            spec: json!({"consumer": "proc"}),
            resp: read.clone(),
            window: (start, c.sim.now_ms()),
        });
        if records.is_empty() {
            let _ = c.call("DELETE", &format!("{DB}/transactions/{id}"), None).await;
            idle += 1;
            if finished && idle >= 3 {
                return Ok(());
            }
            c.pause(300).await;
            continue;
        }
        idle = 0;
        let next = read["next_seq"].clone();
        let ops = json!({"ops": [
            {"op": "increment", "key": "proc/count", "by": records.len()},
            {"op": "set_consumer", "stream": "log", "consumer": "proc", "seq": next},
        ]});
        if c.call("POST", &format!("{DB}/transactions/{id}/ops"), Some(ops)).await.filter_status(200).is_err() {
            continue;
        }
        match c.call("POST", &format!("{DB}/transactions/{id}/commit"), None).await {
            Ok(r) if r.status == 200 || r.status == 409 || r.status == 404 => {}
            Ok(r) if r.retryable_not_applied() || r.status == 429 => {}
            _ => {
                c.fence("streams", cid.as_str().unwrap_or_default()).await?;
            }
        }
    }
}

trait FilterStatus {
    fn filter_status(self, want: u16) -> Result<Resp, ()>;
}

impl FilterStatus for Result<Resp, super::super::NetError> {
    fn filter_status(self, want: u16) -> Result<Resp, ()> {
        match self {
            Ok(r) if r.status == want => Ok(r),
            _ => Err(()),
        }
    }
}

/// Appends to `ret` and periodically trims it.
async fn trimmer(c: Client, w: Shared, steps: usize) -> Result<(), String> {
    for step in 0..steps {
        if c.rng().chance(0.15) {
            let info = c.call_retry("GET", &format!("{DB}/streams/ret"), None).await;
            let next = info.json["next_seq"].as_u64().unwrap_or(0);
            let before = next.saturating_sub(c.rng().below(8));
            let r =
                c.call_settled("POST", &format!("{DB}/streams/ret/trim"), Some(json!({"before_seq": before}))).await;
            if r.status != 200 {
                return Err(format!("trim: {} {}", r.status, r.json));
            }
            world(&w).trims.push(("ret".into(), before, c.sim.now_ms()));
        }
        let k = 1 + c.rng().usize(3);
        append(&c, &w, "ret", step, k, Format::Json, None).await?;
        let d = c.rng().below(800);
        c.pause(d).await;
    }
    Ok(())
}

/// Deletes and recreates a stream while appending to it.
async fn lifecycle(c: Client, rounds: usize) -> Result<(), String> {
    for round in 0..rounds {
        for i in 0..3u64 {
            let path = format!("{DB}/streams/tmp/records");
            let mut attempt = 0;
            let first = loop {
                // A status check left the key of an attempt of unknown outcome aborted.
                let key = format!("tmp-{round}-{i}-{attempt}");
                attempt += 1;
                let body = json!({"records": [{"value": format!("{round}.{i}")}], "idempotency_key": key});
                match settle_call(&c, "POST", &path, &[], Bytes::from(body.to_string()), &key).await? {
                    Settled::Applied(s) => break s["appends"][0].clone(),
                    Settled::Answer(r) if r.status == 200 => break r.json["first_seq"].clone(),
                    // As the sole writer to `tmp`, the stream length determines whether the append was applied.
                    Settled::Unknown => {
                        let info = c.call_retry("GET", &format!("{DB}/streams/tmp"), None).await;
                        match info.json["next_seq"].as_u64() {
                            Some(n) if n == i + 1 => break json!(i),
                            Some(n) if n == i => {}
                            _ => return Err(format!("tmp after an unknown append {i}: {}", info.json)),
                        }
                    }
                    Settled::Answer(r) => return Err(format!("append to tmp: {} {}", r.status, r.json)),
                }
            };
            if first != i {
                return Err(format!("append {i} to a recreated stream took seq {first}"));
            }
        }
        let r = c.call_settled("DELETE", &format!("{DB}/streams/tmp"), None).await;
        if r.status != 200 && r.status != 404 {
            return Err(format!("delete tmp: {} {}", r.status, r.json));
        }
        let r = c.call_retry("GET", &format!("{DB}/streams/tmp/records"), None).await;
        if r.status != 404 {
            return Err(format!("a deleted stream answered {} {}", r.status, r.json));
        }
        let r = c.call_settled("POST", &format!("{DB}/streams"), Some(json!({"name": "tmp"}))).await;
        if r.status != 201 && r.status != 409 {
            return Err(format!("recreate tmp: {} {}", r.status, r.json));
        }
        let info = c.call_retry("GET", &format!("{DB}/streams/tmp"), None).await;
        if info.json["next_seq"] != 0 {
            return Err(format!("a recreated stream does not start over: {}", info.json));
        }
        let body = json!({"retention_ms": 3_600_000, "max_bytes": null});
        let r = c.call_settled("PATCH", &format!("{DB}/streams/tmp"), Some(body)).await;
        if r.status != 200 || r.json["retention_ms"] != 3_600_000 || !r.json["max_bytes"].is_null() {
            return Err(format!("configure tmp: {} {}", r.status, r.json));
        }
        let path = format!("{DB}/streams/tmp/consumers/c{round}");
        let r = c.call_settled("PUT", &path, Some(json!({"seq": 1}))).await;
        if r.status != 400 {
            return Err(format!("a consumer set past the stream's end: {} {}", r.status, r.json));
        }
        let r = c.call_settled("PUT", &path, Some(json!({"seq": 0}))).await;
        if r.status != 200 {
            return Err(format!("set a consumer: {} {}", r.status, r.json));
        }
        let list = c.call_retry("GET", &format!("{DB}/streams/tmp/consumers"), None).await;
        let names: Vec<&str> =
            list.json["consumers"].as_array().into_iter().flatten().filter_map(|x| x["name"].as_str()).collect();
        if names != [format!("c{round}")] {
            return Err(format!("a recreated stream's consumers: {}", list.json));
        }
        let (r, retried) = c.settle("DELETE", &path, None).await?;
        if r.status != 200 || (r.json["existed"] != true && !retried) {
            return Err(format!("delete a consumer: {} {}", r.status, r.json));
        }
        let streams = c.call_retry("GET", &format!("{DB}/streams?prefix=t&limit=10"), None).await;
        let listed: Vec<&str> =
            streams.json["streams"].as_array().into_iter().flatten().filter_map(|x| x["name"].as_str()).collect();
        if listed != ["tmp", "tx"] {
            return Err(format!("streams starting with t: {listed:?}"));
        }
    }
    Ok(())
}

/// Formats `e` as a read response record at `seq`.
fn shape(e: &Expect, seq: u64) -> Json {
    let mut j = e.body.clone();
    j["seq"] = json!(seq);
    if !e.headers.is_null() {
        j["headers"] = e.headers.clone();
    }
    j
}

fn strip_time(r: &Json) -> Json {
    let mut r = r.clone();
    if let Some(o) = r.as_object_mut() {
        o.remove("time");
    }
    r
}

fn strip_records(resp: &Json) -> Json {
    let mut r = resp.clone();
    let seqs: Vec<Json> =
        resp["records"].as_array().into_iter().flatten().map(|r| json!([r["seq"], r["time"]])).collect();
    r["records"] = Json::Array(seqs);
    r
}

/// Returns the starting sequence number of a `from_time` read.
fn time_start(rd: &Read) -> Option<u64> {
    if !rd.spec["filter"].is_null() {
        return None;
    }
    let first = rd.resp["records"].as_array().and_then(|r| r.first()).and_then(|r| r["seq"].as_u64());
    match (rd.spec["reverse"] == true, rd.resp["done"] == true) {
        (false, _) => first.or_else(|| rd.resp["next_seq"].as_u64()),
        (true, true) if rd.spec["until_seq"].is_null() => rd.resp["next_seq"].as_u64(),
        (true, _) => None,
    }
}

fn check_times(log: &BTreeMap<String, BTreeMap<u64, Expect>>, reads: &[Read], eps: u64) -> Result<(), String> {
    let mut times: BTreeMap<(String, u64), u64> = BTreeMap::new();
    for rd in reads {
        for r in rd.resp["records"].as_array().into_iter().flatten() {
            let (seq, t) = (r["seq"].as_u64().unwrap_or(u64::MAX), r["time"].as_u64().unwrap_or(0));
            if let Some(old) = times.insert((rd.stream.clone(), seq), t)
                && old != t
            {
                return Err(format!("{} record {seq} read at times {old} and {t}", rd.stream));
            }
            let e = log
                .get(&rd.stream)
                .and_then(|l| l.get(&seq))
                .ok_or_else(|| format!("{} {seq} read but never appended", rd.stream))?;
            if t + eps < e.window.0 || t > e.window.1 + eps {
                return Err(format!("{} record {seq} has time {t} outside its append's {:?}", rd.stream, e.window));
            }
        }
    }
    let mut prev: Option<(&(String, u64), u64)> = None;
    for (k, t) in &times {
        if let Some((pk, pt)) = prev
            && pk.0 == k.0
            && pt > *t
        {
            return Err(format!("{} times decrease from {} ({pt}) to {} ({t})", k.0, pk.1, k.1));
        }
        prev = Some((k, *t));
    }
    // A `from_time` read starts at the first record with `time >= from_time`.
    for rd in reads.iter().filter(|r| r.resp.get("first_seq").is_some()) {
        let (Some(t), Some(lo)) = (rd.spec["from_time"].as_u64(), time_start(rd)) else { continue };
        let first = rd.resp["first_seq"].as_u64().unwrap_or(0);
        // Records appended after the read may have an earlier timestamp within the clock error bound.
        let tail = rd.resp["tail_seq"].as_u64().unwrap_or(0);
        if let Some(at) = times.get(&(rd.stream.clone(), lo)).filter(|_| lo < tail)
            && *at < t
        {
            return Err(format!(
                "{} read from time {t} began at {lo}, of time {at}: {} {:?} {}",
                rd.stream,
                rd.spec,
                rd.window,
                strip_records(&rd.resp)
            ));
        }
        if lo > first
            && let Some(before) = times.get(&(rd.stream.clone(), lo - 1))
            && *before >= t
        {
            return Err(format!(
                "{} read from time {t} began at {lo}, after {} of time {before}: {} {:?} {}",
                rd.stream,
                lo - 1,
                rd.spec,
                rd.window,
                strip_records(&rd.resp)
            ));
        }
    }
    Ok(())
}

fn lvl_matches(spec: &Json, e: &Expect) -> bool {
    spec["filter"].is_null() || e.body["json"]["lvl"] == spec["filter"]["value"]
}

/// Verifies a recorded read against committed records, acknowledged appends, and trims.
fn check_read(w: &World, rd: &Read) -> Result<(), String> {
    let log = w.log.get(&rd.stream).cloned().unwrap_or_default();
    let spec = &rd.spec;
    let resp = &rd.resp;
    let records = resp["records"].as_array().cloned().unwrap_or_default();
    let reverse = spec["reverse"] == true;
    let seqs: Vec<u64> = records.iter().map(|r| r["seq"].as_u64().unwrap_or(u64::MAX)).collect();
    for (r, s) in records.iter().zip(&seqs) {
        let e = log
            .get(s)
            .ok_or_else(|| format!("{} {s} read but never appended ({spec} at {:?})", rd.stream, rd.window))?;
        if strip_time(r) != shape(e, *s) {
            return Err(format!("{} {s} read as {r}, appended as {}", rd.stream, shape(e, *s)));
        }
        if !lvl_matches(spec, e) {
            return Err(format!("{} {s} does not match the filter {}", rd.stream, spec["filter"]));
        }
    }
    let consumer = !spec["consumer"].is_null();
    let (first, tail) = (resp["first_seq"].as_u64(), resp["tail_seq"].as_u64());
    if let (Some(first), Some(tail)) = (first, tail) {
        let acked = w.acked.iter().filter(|(s, _, at)| *s == rd.stream && *at < rd.window.0).map(|(_, n, _)| *n).max();
        if acked.is_some_and(|n| tail < n) {
            return Err(format!(
                "{} read at {:?} found tail {tail}, before the acknowledged {acked:?}",
                rd.stream, rd.window
            ));
        }
        let sent: u64 =
            w.sent.iter().filter(|(s, _, at)| *s == rd.stream && *at < rd.window.1).map(|(_, k, _)| *k).sum();
        if tail > sent {
            return Err(format!("{} read found tail {tail} with only {sent} records sent", rd.stream));
        }
        let trimmed =
            w.trims.iter().filter(|(s, _, at)| *s == rd.stream && *at < rd.window.0).map(|(_, b, _)| *b).max();
        if trimmed.is_some_and(|b| first < b.min(tail)) {
            return Err(format!("{} read found first {first} after a trim to {trimmed:?}", rd.stream));
        }
        if first > tail {
            return Err(format!("{} first {first} past tail {tail}", rd.stream));
        }
        if spec["filter"].is_null() {
            let start = match (spec["from_seq"].as_u64(), spec["tail"].as_u64()) {
                (Some(s), _) => Some(s.max(first)),
                (_, Some(n)) => Some(tail.saturating_sub(n).max(first)),
                _ if spec["from_time"].is_null() => Some(first),
                _ => None,
            };
            let hi = spec["until_seq"].as_u64().unwrap_or(u64::MAX).min(tail);
            let want: Vec<u64> = match (reverse, start) {
                (false, Some(lo)) => (lo..hi.max(lo)).take(seqs.len()).collect(),
                (true, Some(lo)) => (lo..hi.max(lo)).rev().take(seqs.len()).collect(),
                (false, None) => seqs.first().map_or(Vec::new(), |f| (*f..f + seqs.len() as u64).collect()),
                (true, None) => (hi.saturating_sub(seqs.len() as u64)..hi).rev().collect(),
            };
            if seqs != want {
                return Err(format!("{} read {spec} gave {seqs:?}, not {want:?}", rd.stream));
            }
            let limit = spec["limit"].as_u64().unwrap_or(100) as usize;
            let all = match start {
                Some(lo) => hi.saturating_sub(lo) as usize,
                None => seqs.len(),
            };
            if start.is_some() && seqs.len() < limit.min(all) {
                return Err(format!("{} read {spec} stopped early: {seqs:?} of {all}", rd.stream));
            }
        }
    }
    // Filtered forward reads must include all matching records in the scanned range.
    if !spec["filter"].is_null() && !reverse {
        let lo = seqs.first().copied();
        let next = resp["next_seq"].as_u64().unwrap_or(0);
        if let Some(lo) = lo {
            let want: Vec<u64> = log.range(lo..next).filter(|(_, e)| lvl_matches(spec, e)).map(|(s, _)| *s).collect();
            if seqs != want {
                return Err(format!("{} query {spec} found {seqs:?} where {want:?} match", rd.stream));
            }
        }
    }
    if consumer {
        let next = resp["next_seq"].as_u64().unwrap_or(0);
        if seqs.windows(2).any(|p| p[1] != p[0] + 1) || seqs.last().is_some_and(|l| l + 1 != next) {
            return Err(format!("consumer read {seqs:?} next {next}"));
        }
    }
    Ok(())
}

async fn read_stream_fully(c: &Client, stream: &str) -> Result<(u64, Vec<Json>), String> {
    let mut out = Vec::new();
    let mut from = 0;
    let mut first = None;
    loop {
        let r = c.call_retry("GET", &format!("{DB}/streams/{stream}/records?from_seq={from}&limit=1000"), None).await;
        if r.status != 200 {
            return Err(format!("read {stream}: {} {}", r.status, r.json));
        }
        first.get_or_insert(r.json["first_seq"].as_u64().unwrap_or(0));
        out.extend(r.json["records"].as_array().cloned().unwrap_or_default());
        from = r.json["next_seq"].as_u64().unwrap_or(0);
        if r.json["done"] == true {
            return Ok((first.unwrap_or(0), out));
        }
    }
}

pub async fn run(sim: Arc<Sim>, steps: usize, chaos: bool) -> Result<(), String> {
    let owner = Client::signup(&sim, "streams", "streams@streams.test", "password-123").await?;
    owner.create_db("streams").await?;
    for (name, extra) in [
        ("log", json!({})),
        ("cas", json!({})),
        ("tx", json!({})),
        ("tmp", json!({})),
        ("ret", json!({"max_bytes": RET_BYTES, "retention_ms": RET_MS})),
    ] {
        let mut body = extra;
        body["name"] = json!(name);
        let r = owner.call_settled("POST", &format!("{DB}/streams"), Some(body)).await;
        if r.status != 201 && r.status != 409 {
            return Err(format!("create {name}: {} {}", r.status, r.json));
        }
    }
    let token = owner.token.clone().unwrap_or_default();
    let client = |name: &str| Client::with_token(&sim, name, &token);
    let w: Shared = Arc::default();
    let done = Arc::new(AtomicBool::new(false));
    let phase = chaos.then(|| super::chaos_phase(&sim));
    let mut writers = Vec::new();
    let streams = vec!["log".to_string(), "log".to_string(), "chat:0".to_string(), "chat:1".to_string()];
    for i in 0..3 {
        writers.push(tokio::spawn(appender(client(&format!("a{i}")), w.clone(), streams.clone(), steps)));
    }
    for i in 0..2 {
        writers.push(tokio::spawn(cas_writer(client(&format!("cas{i}")), w.clone(), steps / 3)));
    }
    writers.push(tokio::spawn(tx_writer(client("tx"), w.clone(), steps / 3)));
    writers.push(tokio::spawn(trimmer(client("trim"), w.clone(), steps / 2)));
    writers.push(tokio::spawn(lifecycle(client("life"), 3)));
    let readers: Vec<_> =
        (0..2).map(|i| tokio::spawn(reader(client(&format!("r{i}")), w.clone(), done.clone()))).collect();
    let processor = tokio::spawn(consumer(client("proc"), w.clone(), done.clone()));
    let follow_log = tokio::spawn(follower(client("follow-log"), "log", false, done.clone()));
    let follow_chat = tokio::spawn(follower(client("follow-chat"), "chat:0", true, done.clone()));
    let mut err = None;
    for h in writers {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => err = err.or(Some(e)),
            Err(e) => err = err.or(Some(format!("writer panicked: {e}"))),
        }
    }
    super::settle(&sim, phase).await;
    done.store(true, Ordering::SeqCst);
    for h in readers {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => err = err.or(Some(e)),
            Err(e) => err = err.or(Some(format!("reader panicked: {e}"))),
        }
    }
    if let Err(e) = processor.await.map_err(|e| e.to_string()).and_then(|r| r) {
        err = err.or(Some(format!("consumer: {e}")));
    }
    let mut followed = Vec::new();
    for (stream, h) in [("log", follow_log), ("chat:0", follow_chat)] {
        match h.await {
            Ok(Ok(got)) => followed.push((stream, got)),
            Ok(Err(e)) => err = err.or(Some(e)),
            Err(e) => err = err.or(Some(format!("follower panicked: {e}"))),
        }
    }
    if let Some(e) = err {
        return Err(e);
    }
    let c = client("checker");
    // Wait for the oldest records to age past the retention window, then trigger a retention trim.
    c.pause(RET_MS).await;
    append(&c, &w, "ret", 1_000_000, 1, Format::Json, None).await?;
    c.pause(2_000).await;
    let eps = 2 * sim.cfg.engine.clock_sync_max_error_ms;
    let due = crate::engine::Retention { retention_ms: Some(RET_MS), max_bytes: Some(RET_BYTES) }.due_at();
    let (due_ms, due_bytes) = (due.retention_ms.unwrap_or(RET_MS), due.max_bytes.unwrap_or(RET_BYTES));
    let deadline = c.sim.now_ms() + 60_000;
    loop {
        let info = c.call_retry("GET", &format!("{DB}/streams/ret"), None).await;
        let bytes = info.json["bytes"].as_u64().unwrap_or(u64::MAX);
        let fresh = info.json["first_time"].as_u64().is_none_or(|t| t + due_ms + 5_000 + eps >= c.sim.now_ms());
        if bytes <= due_bytes && fresh {
            break;
        }
        if c.sim.now_ms() > deadline {
            return Err(format!("ret is past its retention: {}", info.json));
        }
        c.pause(1_000).await;
    }
    let mut finals = Vec::new();
    for stream in ["log", "chat:0", "chat:1", "cas", "ret"] {
        finals.push((stream, read_stream_fully(&c, stream).await?));
    }
    let (log, cas, tx_unknown) = {
        let mut g = world(&w);
        let mut seen: BTreeMap<String, BTreeMap<u64, Json>> = BTreeMap::new();
        let read = g
            .reads
            .iter()
            .flat_map(|rd| rd.resp["records"].as_array().into_iter().flatten().map(|r| (&rd.stream[..], r)));
        let follows = followed.iter().flat_map(|(s, got)| got.iter().map(|(_, r)| (*s, r)));
        let last = finals.iter().flat_map(|(s, (_, records))| records.iter().map(|r| (*s, r)));
        for (stream, r) in read.chain(follows).chain(last) {
            if let Some(seq) = r["seq"].as_u64() {
                seen.entry(stream.to_string()).or_default().insert(seq, strip_time(r));
            }
        }
        resolve(&mut g, &seen)?;
        for rd in &g.reads {
            check_read(&g, rd)?;
        }
        check_times(&g.log, &g.reads, 2 * sim.cfg.engine.clock_sync_max_error_ms)?;
        (g.log.clone(), g.cas.clone(), g.tx_unknown)
    };
    for (stream, got) in &followed {
        let want: Vec<Json> = log.get(*stream).into_iter().flatten().map(|(s, e)| shape(e, *s)).collect();
        let got: Vec<Json> = got.iter().map(|(_, r)| strip_time(r)).collect();
        if got != want {
            return Err(format!(
                "the {stream} follower received {} records, not the {} appended",
                got.len(),
                want.len()
            ));
        }
    }
    for (stream, (first, records)) in finals {
        let want: Vec<Json> =
            log.get(stream).into_iter().flatten().filter(|(s, _)| **s >= first).map(|(s, e)| shape(e, *s)).collect();
        let got: Vec<Json> = records.iter().map(strip_time).collect();
        if got != want {
            return Err(format!("{stream} from {first}: {} records, model {}", got.len(), want.len()));
        }
        if stream != "ret" && first != 0 {
            return Err(format!("{stream} was trimmed to {first}"));
        }
    }
    let wins: Vec<u64> = cas.values().copied().collect();
    let cas_log: Vec<u64> = log.get("cas").map(|l| l.keys().copied().collect()).unwrap_or_default();
    let mut sorted = wins.clone();
    sorted.sort();
    if sorted != cas_log {
        return Err(format!("appends expecting seqs won {sorted:?}, the stream holds {cas_log:?}"));
    }
    let n = c.call_retry("GET", &format!("{DB}/kv/proc%2Fcount"), None).await;
    let processed: u64 = String::from_utf8_lossy(&n.raw.body).parse().unwrap_or(0);
    let cursor = c.call_retry("GET", &format!("{DB}/streams/log/consumers/proc"), None).await;
    if cursor.json["seq"] != processed {
        return Err(format!("the consumer counted {processed} records with its cursor at {}", cursor.json));
    }
    if processed != log.get("log").map_or(0, |l| l.len() as u64) {
        return Err(format!("the consumer processed {processed} of {} records", log.get("log").map_or(0, |l| l.len())));
    }
    let r = c
        .call_retry(
            "POST",
            &format!("{DB}/txn"),
            Some(json!({"ops": [{"op": "get", "key": "tx/n"}, {"op": "stream_info", "stream": "tx"}]})),
        )
        .await;
    let n: u64 = r.json["results"][0]["value"].as_str().and_then(|v| v.parse().ok()).unwrap_or(0);
    let sent = (steps / 3) as u64;
    if r.json["results"][1]["next_seq"] != n || n > sent || n + tx_unknown < sent {
        return Err(format!("tx counted {n} appends: {}", r.json));
    }
    check_integrity(&sim).await
}

async fn check_integrity(sim: &Arc<Sim>) -> Result<(), String> {
    let n = sim.up_nodes().first().copied().ok_or("all nodes are down")?;
    let node = sim.node(n).ok_or("node is down")?.node.clone();
    let c = Client::login(sim, "integrity", "streams@streams.test", "password-123").await?;
    let r = c.call_retry("GET", DB, None).await;
    let id = r.json["id"].as_str().ok_or("missing database id")?.to_string();
    let db = node.engine.db(&id);
    let mut t = crate::node::retry_unavailable(|| {
        db.begin(crate::engine::Isolation::Serializable, crate::engine::Consistency::Strong)
    })
    .await
    .map_err(|e| e.to_string())?;
    crate::stream::check_integrity(&mut t).await.map_err(|e| format!("stream integrity: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn world_with(records: &[(u64, &str)], acked: u64) -> World {
        let mut w = World::default();
        let log = w.log.entry("s".into()).or_default();
        for (seq, v) in records {
            log.insert(*seq, Expect { body: json!({"value": v}), headers: Json::Null, window: (100, 200) });
        }
        w.acked.push(("s".into(), acked, 200));
        w.sent.push(("s".into(), records.len() as u64, 50));
        w
    }

    fn read(spec: Json, records: &[(u64, &str, u64)], first: u64, tail: u64, at: u64) -> Read {
        let rs: Vec<Json> = records.iter().map(|(s, v, t)| json!({"seq": s, "time": t, "value": v})).collect();
        Read {
            stream: "s".into(),
            spec,
            resp: json!({"records": rs, "first_seq": first, "tail_seq": tail, "next_seq": 0, "done": true}),
            window: (at, at + 10),
        }
    }

    #[test]
    fn checkers_reject_invalid_reads() {
        let w = world_with(&[(0, "a"), (1, "b"), (2, "c")], 3);
        let ok = read(json!({"from_seq": 0, "limit": 10}), &[(0, "a", 150), (1, "b", 150), (2, "c", 160)], 0, 3, 300);
        check_read(&w, &ok).unwrap();
        check_times(&w.log, std::slice::from_ref(&ok), 0).unwrap();
        for (why, bad) in [
            ("a gap", read(json!({"from_seq": 0}), &[(0, "a", 150), (2, "c", 150)], 0, 3, 300)),
            (
                "a foreign record",
                read(json!({"from_seq": 0}), &[(0, "z", 150), (1, "b", 150), (2, "c", 150)], 0, 3, 300),
            ),
            ("a lost append", read(json!({"from_seq": 0}), &[(0, "a", 150)], 0, 1, 300)),
            ("the future", read(json!({"from_seq": 0}), &[(0, "a", 150)], 0, 9, 300)),
            ("stopping early", read(json!({"from_seq": 0, "limit": 5}), &[(0, "a", 150)], 0, 3, 300)),
            ("reversed wrongly", read(json!({"reverse": true}), &[(0, "a", 150), (1, "b", 150)], 0, 3, 300)),
        ] {
            assert!(check_read(&w, &bad).is_err(), "{why}");
        }
        let mut trimmed = w;
        trimmed.trims.push(("s".into(), 2, 250));
        let undone = read(json!({"from_seq": 0}), &[(1, "b", 150), (2, "c", 150)], 1, 3, 300);
        assert!(check_read(&trimmed, &undone).is_err());
        let w = world_with(&[(0, "a"), (1, "b")], 2);
        for (why, rs) in [
            ("times out of order", vec![(0, "a", 180), (1, "b", 120)]),
            ("a time outside its window", vec![(0, "a", 900)]),
        ] {
            let rd = read(json!({}), &rs, 0, 2, 300);
            assert!(check_times(&w.log, &[rd], 0).is_err(), "{why}");
        }
        let a = read(json!({}), &[(0, "a", 150)], 0, 2, 300);
        let b = read(json!({}), &[(0, "a", 151)], 0, 2, 300);
        assert!(check_times(&w.log, &[a, b], 0).is_err());
    }

    #[test]
    fn uncertain_appends_are_resolved_from_observed_records() {
        let expect = |v: &str| Expect { body: json!({"value": v}), headers: Json::Null, window: (100, 200) };
        let seen = |records: &[(u64, &str)]| {
            let rs = records.iter().map(|(s, v)| (*s, json!({"seq": s, "value": v}))).collect();
            BTreeMap::from([("s".to_string(), rs)])
        };
        let uncertain = |cas| Uncertain { stream: "s".into(), records: vec![expect("x"), expect("y")], cas };
        let mut w = world_with(&[(0, "a")], 1);
        w.uncertain =
            vec![uncertain(Some((("c".into(), 7), 1))), Uncertain { records: vec![expect("z")], ..uncertain(None) }];
        resolve(&mut w, &seen(&[(0, "a"), (2, "y")])).unwrap();
        let log: Vec<(u64, Json)> = w.log["s"].iter().map(|(s, e)| (*s, e.body["value"].clone())).collect();
        assert_eq!(log, [(0, json!("a")), (1, json!("x")), (2, json!("y"))]);
        assert_eq!(w.cas[&("c".to_string(), 7)], 1);
        for (why, cas, records) in [
            ("applied twice", None, &[(1, "x"), (5, "y")][..]),
            ("over a known record", None, &[(1, "y")][..]),
            ("not where it expected", Some((("c".into(), 7), 3)), &[(1, "x")][..]),
        ] {
            let mut w = world_with(&[(0, "a")], 1);
            w.uncertain = vec![uncertain(cas)];
            assert!(resolve(&mut w, &seen(records)).is_err(), "{why}");
        }
    }

    #[test]
    fn events_are_parsed() {
        let body = b"id: 3\ndata: {\"seq\":3,\"time\":1}\n\n: keepalive\n\nid: 4\ndata: {\"seq\":4,\"time\":1}\n\n";
        assert_eq!(parse_events(body).unwrap().len(), 2);
        assert!(parse_events(b"id: 3\ndata: {\"seq\":4}\n\n").is_err());
        assert_eq!(parse_events(b"event: error\ndata: {}\n\n").unwrap().len(), 0);
    }
}
