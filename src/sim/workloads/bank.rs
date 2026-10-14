//! Concurrent transfers via interactive serializable transactions. Verifies
//! that total balance is conserved, no account goes negative under continuous
//! strong and snapshot reads, and final balances match committed transfers.

use super::super::Sim;
use super::super::client::Client;
use serde_json::json;
use std::sync::{Arc, Mutex};

const INITIAL: i64 = 100;

fn acct(i: usize) -> String {
    format!("acct/{i:03}")
}

async fn balances(c: &Client, db: &str, consistency: &str) -> Option<Vec<i64>> {
    let r = c
        .post(
            &format!("/v1/dbs/{db}/txn"),
            json!({"ops": [{"op": "scan", "prefix": "acct/", "limit": 1000}], "consistency": consistency, "max_staleness_ms": 2000}),
        )
        .await
        .ok()?;
    if r.status != 200 {
        return None;
    }
    let items = r.json["results"][0]["items"].as_array()?;
    Some(items.iter().map(|it| it["value"].as_str().and_then(|v| v.parse().ok()).unwrap_or(i64::MIN)).collect())
}

fn audit(b: &[i64], accounts: usize) -> Result<(), String> {
    if b.len() != accounts {
        return Err(format!("auditor saw {} accounts", b.len()));
    }
    if b.iter().sum::<i64>() != INITIAL * accounts as i64 || b.iter().any(|x| *x < 0) {
        return Err(format!("invariant violated: balances {b:?}"));
    }
    Ok(())
}

async fn transfer(c: &Client, db: &str, a: usize, b: usize, amt: i64) -> Result<bool, String> {
    let base = format!("/v1/dbs/{db}");
    // Writes fail while the lease holder is partitioned from reachable nodes;
    // retry across a long virtual deadline.
    let deadline = c.sim.now_ms() + 30 * 60_000;
    while c.sim.now_ms() < deadline {
        let d = c.rng().below(50);
        c.pause(d).await;
        let Ok(r) = c.post(&format!("{base}/transactions"), json!({})).await else { continue };
        if r.status != 201 {
            c.pause(100).await;
            continue;
        }
        let id = r.json["txn_id"].as_str().unwrap_or_default().to_string();
        let cid = r.json["commit_id"].as_str().unwrap_or_default().to_string();
        let ops = json!({"ops": [{"op": "get", "key": acct(a)}, {"op": "get", "key": acct(b)}]});
        let Ok(r) = c.post(&format!("{base}/transactions/{id}/ops"), ops).await else { continue };
        if r.status != 200 {
            continue;
        }
        let bal = |i: usize| r.json["results"][i]["value"].as_str().and_then(|v| v.parse::<i64>().ok());
        let (Some(ba), Some(bb)) = (bal(0), bal(1)) else {
            return Err(format!("missing balance: {}", r.json));
        };
        if ba < amt {
            let _ = c.call("DELETE", &format!("{base}/transactions/{id}"), None).await;
            return Ok(false);
        }
        let ops = json!({"ops": [
            {"op": "put", "key": acct(a), "value": (ba - amt).to_string()},
            {"op": "put", "key": acct(b), "value": (bb + amt).to_string()},
        ]});
        let Ok(r) = c.post(&format!("{base}/transactions/{id}/ops"), ops).await else { continue };
        if r.status != 200 {
            continue;
        }
        match c.post(&format!("{base}/transactions/{id}/commit"), json!({})).await {
            Ok(r) if r.status == 200 => return Ok(true),
            Ok(r) if r.status == 409 || r.status == 404 || r.retryable_not_applied() => {
                let d = c.rng().below(200) + 10;
                c.pause(d).await;
                continue;
            }
            Ok(r) if r.status != 503 => return Err(format!("commit: {} {}", r.status, r.json)),
            _ => {
                // Ambiguous: wait out zombies, then query fenced commit status.
                c.pause(super::super::client::ZOMBIE_WAIT_MS).await;
                let st = c.call_retry("GET", &format!("{base}/commits/{cid}"), None).await;
                match st.json["status"].as_str() {
                    Some("committed") => return Ok(true),
                    Some("not_committed") => continue,
                    _ => return Err(format!("status: {}", st.json)),
                }
            }
        }
    }
    Err("transfer never completed".into())
}

pub async fn run(sim: Arc<Sim>, accounts: usize, clients: usize, transfers: usize, chaos: bool) -> Result<(), String> {
    let owner = Client::signup(&sim, "bank", "bank@bank.test", "password-123").await?;
    let db = "bank";
    owner.create_db(db).await?;
    let token = owner.token.clone().unwrap_or_default();
    let init: Vec<_> =
        (0..accounts).map(|i| json!({"op": "put", "key": acct(i), "value": INITIAL.to_string()})).collect();
    let r = owner
        .call_retry("POST", &format!("/v1/dbs/{db}/txn"), Some(json!({"ops": init, "idempotency_key": "init"})))
        .await;
    if r.status != 200 {
        return Err(format!("init failed: {}", r.json));
    }
    let applied: Arc<Mutex<Vec<(usize, usize, i64)>>> = Arc::new(Mutex::new(Vec::new()));
    let chaos_h = chaos.then(|| super::spawn_chaos(&sim));
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let auditor = {
        let c = Client::with_token(&sim, "auditor", &token);
        let done = done.clone();
        tokio::spawn(async move {
            let mut checks = 0;
            while !done.load(std::sync::atomic::Ordering::SeqCst) {
                let d = 300 + c.rng().below(700);
                c.pause(d).await;
                let strong = c.rng().chance(0.5);
                let mode = if strong { "strong" } else { "snapshot" };
                if let Some(b) = balances(&c, db, mode).await {
                    audit(&b, accounts).map_err(|e| format!("{e} ({mode} read)"))?;
                    checks += 1;
                }
            }
            Ok::<usize, String>(checks)
        })
    };
    let mut hs = Vec::new();
    for ci in 0..clients {
        let c = Client::with_token(&sim, &format!("teller{ci}"), &token);
        let applied = applied.clone();
        hs.push(tokio::spawn(async move {
            for _ in 0..transfers {
                let (a, b, amt) = {
                    let mut r = c.rng();
                    let a = r.usize(accounts);
                    let b = (a + 1 + r.usize(accounts - 1)) % accounts;
                    (a, b, 1 + r.below(40) as i64)
                };
                if transfer(&c, db, a, b, amt).await? {
                    applied.lock().unwrap_or_else(|e| e.into_inner()).push((a, b, amt));
                }
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
    if let Some(h) = chaos_h {
        h.abort();
    }
    done.store(true, std::sync::atomic::Ordering::SeqCst);
    let audits = auditor.await.map_err(|e| e.to_string())??;
    if let Some(e) = err {
        return Err(e);
    }
    super::recover(&sim).await;
    let mut want = vec![INITIAL; accounts];
    for (a, b, amt) in applied.lock().unwrap_or_else(|e| e.into_inner()).iter() {
        want[*a] -= amt;
        want[*b] += amt;
    }
    let got = loop {
        if let Some(b) = balances(&owner, db, "strong").await {
            break b;
        }
        owner.pause(500).await;
    };
    if got != want {
        return Err(format!("final balances {got:?} != expected {want:?}"));
    }
    if audits == 0 && transfers > 0 {
        return Err("auditor never completed a check".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_rejects_broken_ledgers() {
        audit(&[100, 100, 100], 3).unwrap();
        audit(&[0, 150, 150], 3).unwrap();
        assert!(audit(&[100, 100], 3).is_err());
        assert!(audit(&[100, 100, 101], 3).is_err());
        assert!(audit(&[-1, 151, 150], 3).is_err());
    }
}
