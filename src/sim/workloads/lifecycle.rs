//! `lifecycle`: create, use, delete and recreate databases under faults.
//! Deleted data is never visible again (on any node), names are reusable at
//! once, and deleted databases are eventually purged from storage.

use super::super::Sim;
use super::super::client::Client;
use serde_json::json;
use std::sync::Arc;

pub async fn run(sim: Arc<Sim>, cycles: usize, chaos: bool) -> Result<(), String> {
    let c = Client::signup(&sim, "life", "life@cycle.test", "password-123").await?;
    let chaos_h = chaos.then(|| super::spawn_chaos(&sim));
    let mut old_ids = Vec::new();
    for cycle in 0..cycles {
        c.create_db("temp").await?;
        let info = c.call_retry("GET", "/v1/dbs/temp", None).await;
        let id = info.json["id"].as_str().ok_or(format!("db info: {}", info.json))?.to_string();
        // A fresh database is empty even though the name was used before.
        let r = c.call_retry("GET", "/v1/dbs/temp/kv?limit=10", None).await;
        if r.json["items"].as_array().is_some_and(|a| !a.is_empty()) {
            return Err(format!("cycle {cycle}: recreated database not empty: {}", r.json));
        }
        let ops: Vec<_> =
            (0..5).map(|i| json!({"op": "put", "key": format!("k{i}"), "value": format!("c{cycle}")})).collect();
        let r = c
            .call_settled(
                "POST",
                "/v1/dbs/temp/txn",
                Some(json!({"ops": ops, "idempotency_key": format!("fill-{cycle}")})),
            )
            .await;
        if r.status != 200 {
            return Err(format!("fill: {} {}", r.status, r.json));
        }
        let r = c.call_settled("DELETE", "/v1/dbs/temp", None).await;
        if r.status != 200 && r.status != 404 {
            return Err(format!("delete db: {} {}", r.status, r.json));
        }
        // Gone on every node.
        for i in sim.up_nodes() {
            let mut cc = Client::with_token(&sim, "probe", c.token.as_deref().unwrap_or_default());
            cc.pin = Some(i);
            let r = cc.call_retry("GET", "/v1/dbs/temp/kv/k0", None).await;
            if r.status != 404 {
                return Err(format!("deleted db readable via n{i}: {} {}", r.status, r.json));
            }
        }
        old_ids.push(id);
    }
    if let Some(h) = chaos_h {
        h.abort();
    }
    super::recover(&sim).await;
    super::check_db_counts(&sim).await?;
    // Wait for two purge passes (grace periods) plus reaper intervals.
    sim.sleep(2 * sim.cfg.node.db_delete_grace_ms + 3 * 60_000).await;
    let keys = sim.mem.keys();
    for id in &old_ids {
        let prefix = format!("dbs/{id}/");
        if let Some(k) = keys.iter().find(|k| k.starts_with(&prefix)) {
            return Err(format!("deleted database {id} not purged: {k} remains"));
        }
    }
    let r = c.call_retry("GET", "/v1/dbs", None).await;
    if r.json["databases"].as_array().is_some_and(|a| !a.is_empty()) {
        return Err(format!("databases remain: {}", r.json));
    }
    Ok(())
}
