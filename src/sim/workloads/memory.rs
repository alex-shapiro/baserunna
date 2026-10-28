//! `memory`: many databases under a memory budget too small to hold them all.
//!
//! Writers fill the tails of their databases through random nodes, long polls wait on
//! databases that nodes unload while they wait, and interactive transactions hold write sets
//! until nodes refuse new ones. Every strong read must return the settled writes, every long
//! poll must observe the write it waits for, and refused transactions must be admitted once
//! memory is released.

use super::super::Sim;
use super::super::SimConfig;
use super::super::client::{Client, Status};
use super::super::swarm::{Knobs, Profile, world};
use serde_json::{Value as Json, json};
use std::collections::BTreeMap;
use std::sync::Arc;

/// A budget that holds the working set of a few databases, including the segment metadata of
/// the largest.
pub fn config(k: &Knobs) -> SimConfig {
    let mut c = world(k, Profile { nodes: (1, 2), ..Profile::FAULTY });
    c.engine.tail_max_bytes = c.engine.tail_max_bytes.min(2048);
    let floor = (64 * c.engine.tail_max_bytes as u64).max(256 * 1024);
    c.engine.memory_bytes = k.log_range("memory.bytes", floor, floor * 3 / 2) as usize;
    c.node.engine = c.engine.clone();
    c
}

fn value(c: &Client, tag: &str) -> String {
    let n = 200 + c.rng().usize(1300);
    format!("{tag}:{}", "m".repeat(n))
}

/// Commits a write with an idempotency key and settles its outcome through its commit status,
/// or, once its commit record may have expired, by reading the key: each written value is
/// unique and has a single writer, and the status check fences later attempts.
async fn settled_put(c: &Client, db: &str, key: &str, value: &str, id: &str) -> Result<Option<u64>, String> {
    let body = json!({"ops": [{"op": "put", "key": key, "value": value}], "idempotency_key": id});
    let began = c.sim.now_ms();
    match c.call("POST", &format!("/v1/dbs/{db}/txn"), Some(body)).await {
        Ok(r) if r.status == 200 => return Ok(r.json["version"].as_u64()),
        Ok(r) if r.status != 503 && r.status != 429 && r.status != 409 => {
            return Err(format!("put {db}/{key}: {} {}", r.status, r.json));
        }
        _ => {}
    }
    match c.commit_status(db, &format!("key:{id}"), began).await? {
        s @ Status::Committed(_) => Ok(s.version()),
        Status::NotCommitted => Ok(None),
        Status::Unknown => Ok((strong_get(c, db, key).await?.as_deref() == Some(value)).then_some(0)),
    }
}

async fn strong_get(c: &Client, db: &str, key: &str) -> Result<Option<String>, String> {
    let r = c.call_retry("POST", &format!("/v1/dbs/{db}/txn"), Some(json!({"ops": [{"op": "get", "key": key}]}))).await;
    match r.status {
        200 => Ok(r.json["results"][0]["value"].as_str().map(str::to_string)),
        s => Err(format!("get {db}/{key}: {s} {}", r.json)),
    }
}

/// Writes to databases `dbs` and checks each settled write with a strong read.
async fn writer(c: Client, dbs: Vec<String>, steps: usize) -> Result<BTreeMap<(String, String), String>, String> {
    let mut model = BTreeMap::new();
    for step in 0..steps {
        let db = dbs[c.rng().usize(dbs.len())].clone();
        let key = format!("k{}", c.rng().below(6));
        let v = value(&c, &format!("{}-{step}", c.name));
        if settled_put(&c, &db, &key, &v, &format!("{}-{step}", c.name)).await?.is_some() {
            model.insert((db.clone(), key.clone()), v);
        }
        let want = model.get(&(db.clone(), key.clone()));
        let got = strong_get(&c, &db, &key).await?;
        if got.as_ref() != want {
            return Err(format!(
                "{db}/{key}: read {:?} after settled writes, want {:?}",
                got.map(|g| g.len()),
                want.map(|w| w.len())
            ));
        }
        let d = c.rng().below(200);
        c.pause(d).await;
    }
    Ok(model)
}

/// Long-polls the change feed of `db` while the same client writes to it, and requires the
/// poll to return that write.
async fn poller(c: Client, db: String, rounds: usize) -> Result<(), String> {
    for round in 0..rounds {
        let r = c.call_retry("GET", &format!("/v1/dbs/{db}/changes?after=0&limit=1000"), None).await;
        let after = r.json["next_cursor"].as_str().ok_or_else(|| format!("changes: {}", r.json))?.to_string();
        let poll = {
            let c = Client::with_token(&c.sim, &format!("{}-poll", c.name), c.token.as_deref().unwrap_or(""));
            let (db, after) = (db.clone(), after.clone());
            tokio::spawn(async move {
                loop {
                    let path = format!("/v1/dbs/{db}/changes?after={after}&wait_ms=20000");
                    match c.call("GET", &path, None).await {
                        Ok(r) if r.status == 200 && r.json["changes"].as_array().is_some_and(|a| !a.is_empty()) => {
                            return Ok(r.json);
                        }
                        Ok(r) if r.status == 200 || r.status == 503 => {}
                        Ok(r) => return Err(format!("poll {db}: {} {}", r.status, r.json)),
                        Err(_) => c.pause(100).await,
                    }
                }
            })
        };
        let pause = c.rng().below(3_000);
        c.pause(pause).await;
        let v = value(&c, &format!("{db}-{round}"));
        let Some(_) = settled_put(&c, &db, "watched", &v, &format!("{db}-{round}")).await? else {
            poll.abort();
            continue;
        };
        let got = poll.await.map_err(|e| format!("poll task: {e}"))??;
        let seen = got["changes"].as_array().into_iter().flatten().any(|ch| ch["value"].as_str() == Some(v.as_str()));
        if !seen {
            return Err(format!("a long poll on {db} returned {got} instead of the write it waited for"));
        }
    }
    Ok(())
}

/// Holds interactive transactions with large write sets on `db`, retrying refusals until
/// admitted, and commits or aborts them.
async fn holder(c: Client, db: String, rounds: usize) -> Result<BTreeMap<(String, String), String>, String> {
    let mut model = BTreeMap::new();
    let base = format!("/v1/dbs/{db}/transactions");
    for round in 0..rounds {
        let started = c.sim.now_ms();
        let (id, cid) = loop {
            if c.sim.now_ms() - started > 600_000 {
                return Err(format!("{db}: an interactive transaction was refused for 10 minutes"));
            }
            match c.call("POST", &base, Some(json!({}))).await {
                Ok(r) if r.status == 201 => {
                    break (
                        r.json["txn_id"].as_str().unwrap_or("").to_string(),
                        r.json["commit_id"].as_str().unwrap_or("").to_string(),
                    );
                }
                Ok(r) if r.status == 503 || r.status == 429 || r.status == 409 => c.pause(500).await,
                Ok(r) => return Err(format!("begin {db}: {} {}", r.status, r.json)),
                Err(_) => c.pause(200).await,
            }
        };
        let key = format!("t{}", round % 3);
        let v = value(&c, &format!("{db}-{round}"));
        // Values below the inline limit stay in the write set rather than being staged as blobs.
        let inline = c.sim.cfg.engine.value_inline_max - 16;
        let pads = 80 * 1024 / inline;
        let pad: Vec<Json> =
            (0..pads).map(|i| json!({"op": "put", "key": format!("pad{i}"), "value": "p".repeat(inline)})).collect();
        let mut ops = vec![json!({"op": "put", "key": key, "value": v})];
        ops.extend(pad);
        let ops = json!({"ops": ops});
        let added = match c.call("POST", &format!("{base}/{id}/ops"), Some(ops)).await {
            Ok(r) if r.status == 200 || r.status == 503 || r.status == 404 => r.status == 200,
            Ok(r) => return Err(format!("ops {db}: {} {}", r.status, r.json)),
            Err(_) => false,
        };
        let hold = 1_000 + c.rng().below(4_000);
        c.pause(hold).await;
        if !added || c.rng().chance(0.3) {
            let _ = c.call("DELETE", &format!("{base}/{id}"), None).await;
            continue;
        }
        let began = c.sim.now_ms();
        let committed = match c.call("POST", &format!("{base}/{id}/commit"), Some(json!({}))).await {
            Ok(r) if r.status == 200 => true,
            Ok(r) if r.status == 404 || r.status == 409 || r.retryable_not_applied() => false,
            Ok(r) if r.status != 503 => return Err(format!("commit {db}: {} {}", r.status, r.json)),
            _ => match c.commit_status(&db, &cid, began).await? {
                Status::Committed(_) => true,
                Status::NotCommitted => false,
                Status::Unknown => strong_get(&c, &db, &key).await?.as_deref() == Some(v.as_str()),
            },
        };
        if committed {
            model.insert((db.clone(), key), v);
        }
    }
    Ok(model)
}

pub async fn run(sim: Arc<Sim>, chaos: bool) -> Result<(), String> {
    let k = &sim.knobs;
    let (writers, pollers, holders) = (
        k.range("memory.writers", 1, 3) as usize,
        k.range("memory.pollers", 1, 4) as usize,
        k.range("memory.holders", 4, 8) as usize,
    );
    let dbs_per_writer = k.range("memory.dbs_per_writer", 6, 16) as usize;
    let steps = k.range("memory.steps", 20, 60) as usize;
    let owner = Client::signup(&sim, "memory", "memory@sim.test", "password-123").await?;
    let token = owner.token.clone().unwrap_or_default();
    let mut names = Vec::new();
    for w in 0..writers {
        names.push((0..dbs_per_writer).map(|i| format!("w{w}x{i}")).collect::<Vec<_>>());
    }
    let polled: Vec<String> = (0..pollers).map(|i| format!("p{i}")).collect();
    let held: Vec<String> = (0..holders).map(|i| format!("h{i}")).collect();
    for db in names.iter().flatten().chain(&polled).chain(&held) {
        owner.create_db(db).await?;
    }
    let phase = chaos.then(|| super::chaos_phase(&sim));
    let client = |name: String| Client::with_token(&sim, &name, &token);
    let writes: Vec<_> = names
        .iter()
        .enumerate()
        .map(|(i, dbs)| tokio::spawn(writer(client(format!("w{i}")), dbs.clone(), steps)))
        .collect();
    let polls: Vec<_> =
        polled.iter().map(|db| tokio::spawn(poller(client(format!("{db}-poller")), db.clone(), 3))).collect();
    let holds: Vec<_> =
        held.iter().map(|db| tokio::spawn(holder(client(format!("{db}-holder")), db.clone(), steps / 4 + 1))).collect();
    let mut model = BTreeMap::new();
    for t in writes.into_iter().chain(holds) {
        model.extend(t.await.map_err(|e| format!("task: {e}"))??);
    }
    for t in polls {
        t.await.map_err(|e| format!("task: {e}"))??;
    }
    super::settle(&sim, phase).await;
    for ((db, key), want) in &model {
        let got = strong_get(&owner, db, key).await?;
        if got.as_deref() != Some(want.as_str()) {
            return Err(format!("{db}/{key}: final read {:?}, want {} bytes", got.map(|g| g.len()), want.len()));
        }
    }
    Ok(())
}
