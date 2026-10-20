//! Interactive transactions that are abandoned, aborted, or committed after a
//! delay while leases move between nodes.

use super::super::Sim;
use super::super::client::{Client, Status};
use serde_json::{Value as Json, json};
use std::collections::BTreeSet;
use std::sync::Arc;

const BASE: &str = "/v1/dbs/limbo";

#[derive(Default)]
struct Tally {
    committed: BTreeSet<String>,
    /// Marker keys and commit IDs of abandoned or aborted transactions.
    dropped: Vec<(String, String)>,
    /// Transaction IDs left open without committing or aborting.
    abandoned_ids: Vec<String>,
}

enum Fate {
    Abandon,
    Abort,
    CommitAfter(u64),
    /// Checks the commit's status, which aborts it, and then tries to commit.
    ResolveThenCommit,
}

async fn one(c: &Client, marker: &str, isolation: &str, fate: Fate, tally: &mut Tally) -> Result<(), String> {
    let began = c.sim.now_ms();
    let r = c.call("POST", &format!("{BASE}/transactions"), Some(json!({"isolation": isolation}))).await;
    let Ok(r) = r else { return Ok(()) };
    if r.status != 201 {
        return if r.status == 503 { Ok(()) } else { Err(format!("begin: {} {}", r.status, r.json)) };
    }
    let id = r.json["txn_id"].as_str().unwrap_or_default().to_string();
    let cid = r.json["commit_id"].as_str().unwrap_or_default().to_string();
    let ops_path = format!("{BASE}/transactions/{id}/ops");
    let read = c.call("POST", &ops_path, Some(json!({"ops": [{"op": "get", "key": "ctr"}]}))).await;
    let n: i64 = match read {
        Ok(r) if r.ok() => r.json["results"][0]["value"].as_str().and_then(|v| v.parse().ok()).unwrap_or(0),
        _ => return Ok(()),
    };
    let write = json!({"ops": [
        {"op": "put", "key": "ctr", "value": (n + 1).to_string()},
        {"op": "put", "key": marker, "value": "x"},
    ]});
    let fate = match c.call("POST", &ops_path, Some(write)).await {
        Ok(r) if r.ok() => fate,
        Ok(_) => {
            tally.dropped.push((marker.to_string(), cid));
            return Ok(());
        }
        // Abandon the transaction if the network call fails so partially applied ops are not committed.
        Err(_) => Fate::Abandon,
    };
    let wait = match fate {
        Fate::Abandon => {
            tally.dropped.push((marker.to_string(), cid));
            tally.abandoned_ids.push(id);
            return Ok(());
        }
        Fate::Abort => {
            let _ = c.call("DELETE", &format!("{BASE}/transactions/{id}"), None).await;
            tally.dropped.push((marker.to_string(), cid));
            return Ok(());
        }
        Fate::CommitAfter(ms) => ms,
        Fate::ResolveThenCommit => {
            if !matches!(c.commit_status("limbo", &cid, began).await?, Status::NotCommitted | Status::Unknown) {
                return Err(format!("{marker}: an uncommitted transaction resolved as committed"));
            }
            match c.call("POST", &format!("{BASE}/transactions/{id}/commit"), Some(json!({}))).await {
                Ok(r) if r.ok() => return Err(format!("{marker}: a commit applied after its status check aborted it")),
                Ok(r) if r.status == 409 && r.code() == "aborted" => {}
                Ok(r) if r.status == 404 || r.retryable_not_applied() || r.outcome_unknown() => {}
                Ok(r) => return Err(format!("commit after abort: {} {}", r.status, r.json)),
                Err(_) => {}
            }
            tally.dropped.push((marker.to_string(), cid));
            return Ok(());
        }
    };
    c.pause(wait).await;
    let committed = match c.call("POST", &format!("{BASE}/transactions/{id}/commit"), Some(json!({}))).await {
        Ok(r) if r.ok() => true,
        Ok(r) if r.status == 404 || r.retryable_not_applied() || (r.status == 409 && r.code() == "conflict") => false,
        Ok(r) if !r.outcome_unknown() => return Err(format!("commit: {} {}", r.status, r.json)),
        _ => c.resolve("limbo", &cid, began, marker).await?,
    };
    if committed {
        tally.committed.insert(marker.to_string());
    } else {
        tally.dropped.push((marker.to_string(), cid));
    }
    Ok(())
}

async fn client(c: Client, k: usize, txns: usize) -> Result<Tally, String> {
    let mut tally = Tally::default();
    let mut rng = c.sim.fork_rng();
    let idle = c.sim.cfg.node.txn_idle_ms;
    for i in 0..txns {
        c.pause(rng.below(400)).await;
        let isolation = if rng.chance(0.5) { "snapshot" } else { "serializable" };
        let fate = match rng.below(7) {
            0 | 1 => Fate::Abandon,
            2 => Fate::Abort,
            3 => Fate::ResolveThenCommit,
            _ if rng.chance(0.6) => Fate::CommitAfter(rng.below(500)),
            _ => Fate::CommitAfter(rng.below(2 * idle)),
        };
        one(&c, &format!("m/{k}/{i}"), isolation, fate, &mut tally).await?;
    }
    Ok(tally)
}

pub async fn run(sim: Arc<Sim>, clients: usize, txns: usize, chaos: bool) -> Result<(), String> {
    let owner = Client::signup(&sim, "limbo", "limbo@limbo.test", "password-123").await?;
    owner.create_db("limbo").await?;
    let token = owner.token.clone().unwrap_or_default();
    let phase = chaos.then(|| super::chaos_phase(&sim));
    let hs: Vec<_> = (0..clients)
        .map(|k| tokio::spawn(client(Client::with_token(&sim, &format!("limbo{k}"), &token), k, txns)))
        .collect();
    let mut tallies = Vec::new();
    for h in hs {
        tallies.push(h.await.map_err(|e| e.to_string())??);
    }
    super::settle(&sim, phase).await;
    sim.sleep(sim.cfg.node.txn_idle_ms + 10_000).await;
    let info = owner.call_retry("GET", BASE, None).await;
    let id = info.json["id"].as_str().ok_or(format!("db info: {}", info.json))?.to_string();
    for i in sim.up_nodes() {
        let Some(n) = sim.node(i) else { continue };
        let open = n.node.open_txn_count_for_db(&id).await;
        if open != 0 {
            return Err(format!("n{i} still holds {open} open transactions"));
        }
    }
    let committed: BTreeSet<String> = tallies.iter().flat_map(|t| t.committed.iter().cloned()).collect();
    let r = owner.call_retry("GET", &format!("{BASE}/kv?prefix=m/&limit=1000&keys_only=true"), None).await;
    let markers: BTreeSet<String> = r.json["items"]
        .as_array()
        .ok_or(format!("scan: {}", r.json))?
        .iter()
        .filter_map(|it| it["key"].as_str().map(|s| s.to_string()))
        .collect();
    if markers != committed {
        let ghosts: Vec<_> = markers.difference(&committed).collect();
        let lost: Vec<_> = committed.difference(&markers).collect();
        return Err(format!(
            "markers differ from commits: uncommitted but visible {ghosts:?}, committed but missing {lost:?}"
        ));
    }
    let r = owner.call_retry("POST", &format!("{BASE}/txn"), Some(json!({"ops": [{"op": "get", "key": "ctr"}]}))).await;
    let ctr: usize = r.json["results"][0]["value"].as_str().and_then(|v| v.parse().ok()).unwrap_or(0);
    if ctr != committed.len() {
        return Err(format!("lost update: counter is {ctr} after {} committed increments", committed.len()));
    }
    for t in &tallies {
        for (marker, cid) in &t.dropped {
            let st = owner.call_retry("GET", &format!("{BASE}/commits/{cid}"), None).await;
            if st.json["status"] != "not_committed" {
                return Err(format!("{marker}: dropped transaction status is {}", st.json));
            }
        }
        for id in &t.abandoned_ids {
            let r = owner.call_retry("POST", &format!("{BASE}/transactions/{id}/commit"), Some(Json::Null)).await;
            if r.status != 404 {
                return Err(format!("abandoned transaction {id} is still active: {} {}", r.status, r.json));
            }
        }
    }
    Ok(())
}
