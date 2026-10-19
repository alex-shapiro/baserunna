//! `lifecycle`: creates, uses, deletes, and recreates databases under faults.
//! Deleted data is never visible again on any node, names are reusable at
//! once, and deleted databases are eventually purged from storage.

use super::super::Sim;
use super::super::client::Client;
use serde_json::json;
use std::sync::Arc;

pub async fn run(sim: Arc<Sim>, cycles: usize, chaos: bool) -> Result<(), String> {
    let c = Client::signup(&sim, "life", "life@cycle.test", "password-123").await?;
    let phase = chaos.then(|| super::chaos_phase(&sim));
    let mut old_ids = Vec::new();
    for cycle in 0..cycles {
        c.create_db("temp").await?;
        let info = c.call_retry("GET", "/v1/dbs/temp", None).await;
        let id = info.json["id"].as_str().ok_or(format!("db info: {}", info.json))?.to_string();
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
        for i in sim.up_nodes() {
            let mut cc = Client::with_token(&sim, "probe", c.token.as_deref().unwrap_or_default());
            cc.pin = Some(i);
            // A node that crashes mid-probe serves nothing, and chaos may never restart it.
            while sim.node(i).is_some() {
                match cc.call("GET", "/v1/dbs/temp/kv/k0", None).await {
                    Ok(r) if r.status == 404 => break,
                    Ok(r) if r.status != 503 => {
                        return Err(format!("deleted db readable via n{i}: {} {}", r.status, r.json));
                    }
                    _ => sim.sleep(200).await,
                }
            }
        }
        old_ids.push(id);
    }
    super::settle(&sim, phase).await;
    super::check_integrity(&sim).await?;
    super::check_purged(&sim, &old_ids).await?;
    let r = c.call_retry("GET", "/v1/dbs", None).await;
    if r.json["databases"].as_array().is_some_and(|a| !a.is_empty()) {
        return Err(format!("databases remain: {}", r.json));
    }
    Ok(())
}
