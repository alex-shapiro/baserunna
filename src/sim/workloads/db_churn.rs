//! `db_churn`: creates, uses, deletes, and recreates databases under a tiny
//! cache and fast handle eviction. Every read must match the database's
//! model, deleted names must return 404, recreated databases must start
//! empty, and at quiescence idle handles must be released and caches must
//! stay within budget.

use super::super::client::{Client, ZOMBIE_WAIT_MS};
use super::super::{Sim, SimConfig};
use serde_json::{Value as Json, json};
use std::collections::BTreeMap;
use std::sync::Arc;

pub fn config(base: SimConfig) -> SimConfig {
    let mut c = base;
    c.engine.cache_bytes = 16 * 1024;
    c.node.db_idle_evict_ms = 2_000;
    c.node.engine = c.engine.clone();
    c
}

/// `None`: the database does not exist.
type Model = BTreeMap<String, Option<BTreeMap<String, String>>>;

/// Runs a one-shot transaction to a definitive response.
async fn txn(c: &Client, db: &str, body: Json) -> Result<super::super::client::Resp, String> {
    let path = format!("/v1/dbs/{db}/txn");
    loop {
        match c.call("POST", &path, Some(body.clone())).await {
            Ok(r) if r.outcome_unknown() => c.pause(ZOMBIE_WAIT_MS).await,
            Ok(r) if r.status == 503 || (r.status == 409 && r.code() == "conflict") => c.pause(100).await,
            Ok(r) => return Ok(r),
            Err(_) => c.pause(ZOMBIE_WAIT_MS).await,
        }
    }
}

async fn check_db(c: &Client, db: &str, want: &Option<BTreeMap<String, String>>, ctx: &str) -> Result<(), String> {
    let r = c.call_retry("GET", &format!("/v1/dbs/{db}/kv?limit=1000"), None).await;
    match want {
        None if r.status == 404 => Ok(()),
        None => Err(format!("{ctx}: deleted database {db} returned {} {}", r.status, r.json)),
        Some(m) => {
            let items = r.json["items"].as_array().ok_or(format!("{ctx}: scan of {db}: {} {}", r.status, r.json))?;
            let got: BTreeMap<String, String> = items
                .iter()
                .filter_map(|it| Some((it["key"].as_str()?.to_string(), it["value"].as_str()?.to_string())))
                .collect();
            if got != *m {
                return Err(format!("{ctx}: {db} holds {got:?}, model {m:?}"));
            }
            Ok(())
        }
    }
}

async fn client(c: Client, k: usize, dbs: usize, steps: usize) -> Result<(Model, Client), String> {
    let mut rng = c.sim.fork_rng();
    let mut model: Model = (0..dbs).map(|i| (format!("churn{k}x{i}"), None)).collect();
    let names: Vec<String> = model.keys().cloned().collect();
    for step in 0..steps {
        let ctx = format!("client {k} step {step}");
        if rng.chance(0.1) {
            // Long enough for idle handles to be evicted.
            c.pause(c.sim.cfg.node.db_idle_evict_ms + 6_000).await;
        } else {
            c.pause(rng.below(200)).await;
        }
        let db = names[rng.usize(names.len())].clone();
        let exists = model[&db].is_some();
        match rng.below(10) {
            _ if !exists => {
                c.create_db(&db).await?;
                model.insert(db.clone(), Some(BTreeMap::new()));
                check_db(&c, &db, &model[&db], &format!("{ctx} (recreated)")).await?;
            }
            0 => {
                let r = c.call_settled("DELETE", &format!("/v1/dbs/{db}"), None).await;
                if r.status != 200 && r.status != 404 {
                    return Err(format!("{ctx}: delete {db}: {} {}", r.status, r.json));
                }
                model.insert(db.clone(), None);
            }
            1..=4 => {
                let key = format!("k{}", rng.below(6));
                let value = format!("v{step}");
                let body = json!({"ops": [{"op": "put", "key": key, "value": value}], "idempotency_key": format!("c{k}s{step}")});
                let r = txn(&c, &db, body).await?;
                if r.status != 200 {
                    return Err(format!("{ctx}: put into {db}: {} {}", r.status, r.json));
                }
                if let Some(Some(m)) = model.get_mut(&db) {
                    m.insert(key, value);
                }
            }
            5 => {
                let key = format!("k{}", rng.below(6));
                let body = json!({"ops": [{"op": "delete", "key": key}], "idempotency_key": format!("c{k}s{step}")});
                let r = txn(&c, &db, body).await?;
                if r.status != 200 {
                    return Err(format!("{ctx}: delete in {db}: {} {}", r.status, r.json));
                }
                if let Some(Some(m)) = model.get_mut(&db) {
                    m.remove(&key);
                }
            }
            6 | 7 => {
                let key = format!("k{}", rng.below(6));
                let r = txn(&c, &db, json!({"ops": [{"op": "get", "key": key}]})).await?;
                let want = model[&db].as_ref().and_then(|m| m.get(&key));
                let got = r.json["results"][0]["value"].as_str();
                if r.status != 200 || got != want.map(|s| s.as_str()) {
                    return Err(format!("{ctx}: get {db}/{key}: {} {} (model {want:?})", r.status, r.json));
                }
            }
            _ => check_db(&c, &db, &model[&db], &ctx).await?,
        }
    }
    Ok((model, c))
}

pub async fn run(sim: Arc<Sim>, clients: usize, dbs: usize, steps: usize, chaos: bool) -> Result<(), String> {
    let phase = chaos.then(|| super::chaos_phase(&sim));
    let mut hs = Vec::new();
    for k in 0..clients {
        let c = Client::signup(&sim, &format!("churn{k}"), &format!("churn{k}@churn.test"), "password-123").await?;
        hs.push(tokio::spawn(client(c, k, dbs, steps)));
    }
    let mut done = Vec::new();
    let mut err = None;
    for h in hs {
        match h.await.map_err(|e| e.to_string()).and_then(|r| r) {
            Ok(x) => done.push(x),
            Err(e) => err = err.or(Some(e)),
        }
    }
    if let Some(e) = err {
        return Err(e);
    }
    super::settle(&sim, phase).await;
    super::check_integrity(&sim).await?;
    for (model, c) in &done {
        for (db, want) in model {
            check_db(c, db, want, "final").await?;
        }
    }
    sim.sleep(sim.cfg.node.db_idle_evict_ms + 10_000).await;
    for i in sim.up_nodes() {
        let Some(n) = sim.node(i) else { continue };
        let open = n.node.engine.open_count();
        let cache = n.node.engine.shared.cache.stats();
        // Heartbeats keep the system database open, and background tasks may
        // still hold a user database handle.
        if open > 3 {
            return Err(format!("n{i} keeps {open} database handles open while idle"));
        }
        if cache.bytes > sim.cfg.engine.cache_bytes {
            return Err(format!("n{i} cache holds {} bytes, budget {}", cache.bytes, sim.cfg.engine.cache_bytes));
        }
    }
    Ok(())
}
