//! Events inserted near an event table's retention boundary while its
//! retention changes and maintenance drops expired partitions.
//!
//! Retention is judged by the store's clock, which the simulator's clock
//! is, within the clock-sync error. An event is at risk once any retention
//! that may have been in effect put it within [`SLACK_MS`] of the boundary:
//! it may be skipped by queries and dropped by maintenance. Every other
//! acknowledged event must be in every answer at or after its version, no
//! answer may hold an event no insert wrote, and after healing no query may
//! return an event clearly past the final retention.

use super::super::Sim;
use super::super::client::Client;
use super::events::{Settled, settle};
use serde_json::{Value as Json, json};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

const DB: &str = "/v1/dbs/events";
const TABLE: &str = "/v1/dbs/events/events/ret";
const MINUTE: i64 = 60_000;
const HOUR: i64 = 3_600_000;
/// Retentions the table switches among.
const RETENTIONS: [i64; 3] = [90 * MINUTE, 2 * HOUR, 150 * MINUTE];
/// Clock error and scheduling slack around the retention boundary.
const SLACK_MS: i64 = 3_000;
/// How long a replaced retention may still be applied: a maintenance round
/// plans drops from a schema snapshot and commits them without reading the
/// retention again (TODO.md, "Re-check retention when dropping expired
/// parts").
const LAG_MS: i64 = 60_000;

/// A retention setting: when it was requested, and when it had certainly
/// taken effect.
struct Change {
    retention: i64,
    sent_ms: i64,
    done_ms: i64,
}

struct Insert {
    version: Option<u64>,
    uids: Vec<String>,
}

struct Answer {
    recv_ms: i64,
    version: u64,
    uids: BTreeSet<String>,
}

#[derive(Default)]
struct World {
    times: BTreeMap<String, i64>,
    inserts: Vec<Insert>,
    changes: Vec<Change>,
    answers: Vec<Answer>,
}

type Shared = Arc<Mutex<World>>;

fn world(w: &Shared) -> std::sync::MutexGuard<'_, World> {
    w.lock().unwrap_or_else(|e| e.into_inner())
}

impl World {
    /// The latest boundary any retention in effect up to `upto` drew: events
    /// before it, plus slack, may have been skipped or dropped.
    fn risk_line(&self, upto: i64) -> i64 {
        let mut line = i64::MIN;
        for (i, c) in self.changes.iter().enumerate() {
            if c.sent_ms > upto {
                break;
            }
            let end = match self.changes.get(i + 1) {
                Some(n) if n.done_ms + LAG_MS < upto => n.done_ms + LAG_MS,
                _ => upto,
            };
            line = line.max(end - c.retention);
        }
        line + SLACK_MS
    }

    fn current(&self) -> i64 {
        self.changes.last().map_or(2 * HOUR, |c| c.retention)
    }
}

async fn inserter(c: Client, w: Shared, steps: usize, transactional: bool) -> Result<(), String> {
    for step in 0..steps {
        let retention = world(&w).current();
        let now = c.sim.now_ms() as i64;
        let n = 1 + c.rng().usize(6);
        let mut rows = Vec::new();
        let mut times = Vec::new();
        for i in 0..n {
            let mut r = c.rng();
            let boundary = now - retention;
            let time = match r.below(10) {
                0..4 => boundary + r.below(120_000) as i64 - 60_000,
                4..8 => boundary + r.below(40 * MINUTE as u64) as i64 - 20 * MINUTE,
                _ => now - r.below(retention as u64 / 2) as i64,
            };
            let uid = format!("{}-{}", c.name, step * 100 + i);
            rows.push(json!({"uid": uid, "time": time, "n": r.below(100)}));
            times.push((uid, time));
        }
        let uids: Vec<String> = times.iter().map(|t| t.0.clone()).collect();
        world(&w).times.extend(times);
        let (path, body, ndjson) = if transactional {
            (format!("{DB}/txn"), json!({"ops": [{"op": "insert_events", "table": "ret", "rows": rows}]}), false)
        } else {
            let nd = c.rng().chance(0.5);
            (format!("{TABLE}/rows"), if nd { json!(rows) } else { json!({"rows": rows}) }, nd)
        };
        let version = match settle(&c, &path, ndjson, &body, &format!("{}-{step}", c.name)).await? {
            Settled::Done(j) => Some(j["version"].as_u64().ok_or(format!("{}: no version in {j}", c.name))?),
            Settled::Unknown => None,
            Settled::Rejected(r) => return Err(format!("{}: ret insert refused: {} {}", c.name, r.status, r.json)),
        };
        world(&w).inserts.push(Insert { version, uids });
        let ms = c.rng().below(500);
        c.pause(ms).await;
    }
    Ok(())
}

async fn retainer(c: Client, w: Shared, steps: usize) -> Result<(), String> {
    for _ in 0..steps {
        let ms = 2_000 + c.rng().below(6_000);
        c.pause(ms).await;
        let retention = RETENTIONS[c.rng().usize(RETENTIONS.len())];
        let sent_ms = c.sim.now_ms() as i64;
        let (r, _) = c.settle("PATCH", TABLE, Some(json!({"retention_ms": retention}))).await?;
        if r.status != 200 || r.json["retention_ms"] != json!(retention) {
            return Err(format!("ret set retention: {} {}", r.status, r.json));
        }
        let done_ms = c.sim.now_ms() as i64;
        world(&w).changes.push(Change { retention, sent_ms, done_ms });
    }
    Ok(())
}

fn rows_query(strong: bool) -> Json {
    let mut q = json!({"from": i64::MIN / 4, "to": i64::MAX / 4, "columns": ["uid"], "order_by": ["uid"],
        "limit": 10_000});
    if strong {
        q["consistency"] = json!("strong");
    }
    q
}

/// The uids of a rows answer, which holds each event at most once.
fn distinct_uids(answer: &Json) -> Result<BTreeSet<String>, String> {
    let mut uids = BTreeSet::new();
    for u in answer["rows"].as_array().into_iter().flatten().filter_map(|x| x[0].as_str()) {
        if !uids.insert(u.to_string()) {
            return Err(format!("ret answer at v{} holds {u} twice", answer["version"]));
        }
    }
    Ok(uids)
}

async fn querier(c: Client, w: Shared, steps: usize) -> Result<(), String> {
    for _ in 0..steps {
        let ms = 300 + c.rng().below(1_200);
        c.pause(ms).await;
        let strong = c.rng().chance(0.4);
        let r = c.call("POST", &format!("{TABLE}/rows/query"), Some(rows_query(strong))).await;
        let recv_ms = c.sim.now_ms() as i64;
        match r {
            Ok(r) if r.status == 200 && !r.json["next_cursor"].is_string() => {
                let version = r.json["version"].as_u64().ok_or(format!("ret answer without a version: {}", r.json))?;
                let uids = distinct_uids(&r.json)?;
                world(&w).answers.push(Answer { recv_ms, version, uids });
            }
            Ok(r) if r.status == 200 || r.status == 503 || r.status == 429 => {}
            Ok(r) => return Err(format!("{}: ret query failed: {} {}", c.name, r.status, r.json)),
            Err(_) => {}
        }
    }
    Ok(())
}

pub struct Retention {
    w: Shared,
}

impl Retention {
    pub async fn start(
        sim: &Arc<Sim>,
        client: impl Fn(&str) -> Client,
        steps: usize,
    ) -> Result<(Retention, Vec<tokio::task::JoinHandle<Result<(), String>>>), String> {
        let c = client("ret-setup");
        let def = json!({
            "name": "ret", "columns": [{"name": "time", "type": "time"},
                {"name": "uid", "type": "string", "nullable": false}, {"name": "n", "type": "int"}],
            "time_column": "time", "order_by": ["time"], "partition": "hour", "retention_ms": 2 * HOUR,
        });
        let r = c.call_settled("POST", &format!("{DB}/events"), Some(def)).await;
        if r.status != 201 && r.status != 409 {
            return Err(format!("ret create: {} {}", r.status, r.json));
        }
        let w: Shared = Arc::default();
        world(&w).changes.push(Change { retention: 2 * HOUR, sent_ms: 0, done_ms: 0 });
        sim.trace.event("events: inserting at the retention boundary of table ret");
        let tasks = vec![
            tokio::spawn(inserter(client("rtb"), w.clone(), steps, false)),
            tokio::spawn(inserter(client("rtt"), w.clone(), steps / 2, true)),
            tokio::spawn(retainer(client("retainer"), w.clone(), steps / 4 + 1)),
            tokio::spawn(querier(client("rtq"), w.clone(), steps)),
        ];
        Ok((Retention { w }, tasks))
    }

    fn check_answer(g: &World, a: &Answer, final_read: bool) -> Result<(), String> {
        let line = g.risk_line(a.recv_ms);
        for u in &a.uids {
            if !g.times.contains_key(u) {
                return Err(format!("ret answer at v{} holds {u}, which no insert wrote", a.version));
            }
        }
        for x in &g.inserts {
            if x.version.is_none_or(|w| w > a.version) {
                continue;
            }
            for u in &x.uids {
                if !a.uids.contains(u) && g.times[u] >= line {
                    return Err(format!(
                        "ret answer at v{} lacks {u} (time {}), acknowledged at v{:?}, {} ms within retention \
                         (risk line {line}, read at {}{})",
                        a.version,
                        g.times[u],
                        x.version,
                        g.times[u] - line + SLACK_MS,
                        a.recv_ms,
                        if final_read { ", final" } else { "" }
                    ));
                }
            }
        }
        Ok(())
    }

    /// Checks the answers collected under faults, then a strong read after
    /// healing, which must also skip events clearly past the final retention.
    pub async fn check(&self, c: &Client) -> Result<(), String> {
        let sent_ms = c.sim.now_ms() as i64;
        let r = c.call_retry("POST", &format!("{TABLE}/rows/query"), Some(rows_query(true))).await;
        let recv_ms = c.sim.now_ms() as i64;
        if r.status != 200 || r.json["next_cursor"].is_string() {
            return Err(format!("ret final read: {} {}", r.status, r.json));
        }
        let version = r.json["version"].as_u64().unwrap_or(0);
        let uids = distinct_uids(&r.json)?;
        let g = world(&self.w);
        for a in &g.answers {
            Self::check_answer(&g, a, false)?;
        }
        let last = Answer { recv_ms, version, uids };
        Self::check_answer(&g, &last, true)?;
        let past = sent_ms - g.current() - SLACK_MS;
        if let Some(u) = last.uids.iter().find(|u| g.times[*u] < past) {
            return Err(format!("ret final read returned {u} at {}, past retention (before {past})", g.times[u]));
        }
        c.sim.trace.event(&format!(
            "events: ret {} rows after {} retention changes, {} answers checked",
            last.uids.len(),
            g.changes.len() - 1,
            g.answers.len()
        ));
        Ok(())
    }
}
