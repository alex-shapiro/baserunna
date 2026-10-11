//! Real-world example: a ledger with concurrent money transfers.
//!
//! Shows interactive serializable transactions (read balances, decide,
//! write), automatic conflict detection with client retries, idempotency
//! keys that make retried requests safe, and an invariant check (money is
//! never created or destroyed).
//!
//! Run: `cargo run --example bank_ledger`

#[path = "common/mod.rs"]
mod common;

use baserunna::client::Client;
use baserunna::meta::SignupPolicy;
use common::step;
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const ACCOUNTS: usize = 8;

async fn transfer(
    c: &Client,
    from: usize,
    to: usize,
    amount: i64,
    conflicts: &AtomicU64,
) -> Result<bool, Box<dyn std::error::Error>> {
    loop {
        let t = c.post("/v1/dbs/ledger/transactions", json!({})).await?;
        let id = t["txn_id"].as_str().unwrap().to_string();
        let base = format!("/v1/dbs/ledger/transactions/{id}");
        let r = c
            .post(
                &format!("{base}/ops"),
                json!({"ops": [
                    {"op": "get", "key": format!("acct/{from}")},
                    {"op": "get", "key": format!("acct/{to}")}
                ]}),
            )
            .await?;
        let bal = |i: usize| r["results"][i]["value"].as_str().unwrap().parse::<i64>().unwrap();
        let (a, b) = (bal(0), bal(1));
        if a < amount {
            c.delete(&base).await?;
            return Ok(false); // insufficient funds
        }
        c.post(
            &format!("{base}/ops"),
            json!({"ops": [
                {"op": "put", "key": format!("acct/{from}"), "value": (a - amount).to_string()},
                {"op": "put", "key": format!("acct/{to}"), "value": (b + amount).to_string()},
                {"op": "put", "key": format!("log/{id}"), "value": format!("{from}->{to}:{amount}")}
            ]}),
        )
        .await?;
        match c.post(&format!("{base}/commit"), json!({})).await {
            Ok(_) => return Ok(true),
            // Another transfer touched the same accounts first: retry
            // with fresh balances. (Serializable isolation detected it.)
            Err(e) if e.code == "conflict" => {
                conflicts.fetch_add(1, Ordering::Relaxed);
            }
            Err(e) => return Err(e.into()),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = common::start(common::shared_store(), "bank", SignupPolicy::FirstUserOnly).await;
    let teller = Client::new(&server.url).signup("treasury@bank.example", "very-secret-1").await?;
    teller.create_db("ledger").await?;

    step(1, "Open accounts with 1000 each (one atomic transaction)");
    let ops: Vec<_> =
        (0..ACCOUNTS).map(|i| json!({"op": "put", "key": format!("acct/{i}"), "value": "1000"})).collect();
    let r = teller.txn("ledger", json!(ops)).await?;
    println!("    committed at version {}", r["version"]);

    step(2, "8 tellers run 25 random transfers each, concurrently");
    let conflicts = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::new();
    for t in 0..8u64 {
        let c = teller.clone();
        let conflicts = conflicts.clone();
        tasks.push(tokio::spawn(async move {
            let mut done = 0;
            let mut x = t * 7919 + 1;
            for _ in 0..25 {
                x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                let from = (x >> 33) as usize % ACCOUNTS;
                let to = (from + 1 + (x >> 40) as usize % (ACCOUNTS - 1)) % ACCOUNTS;
                let amount = 1 + (x >> 50) as i64 % 300;
                if transfer(&c, from, to, amount, &conflicts).await.map_err(|e| e.to_string())? {
                    done += 1;
                }
            }
            Ok::<u32, String>(done)
        }));
    }
    let mut total = 0;
    for t in tasks {
        total += t.await??;
    }
    println!("    {total} transfers committed, {} conflicts detected and retried", conflicts.load(Ordering::Relaxed));

    step(3, "Audit: a consistent snapshot of all balances sums to the initial total");
    let r = teller.txn("ledger", json!([{"op": "scan", "prefix": "acct/", "limit": 100}])).await?;
    let balances: Vec<i64> = r["results"][0]["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|it| it["value"].as_str().unwrap().parse().unwrap())
        .collect();
    let sum: i64 = balances.iter().sum();
    println!("    balances {balances:?}");
    println!("    total = {sum} (expected {})", 1000 * ACCOUNTS as i64);
    assert_eq!(sum, 1000 * ACCOUNTS as i64);
    assert!(balances.iter().all(|b| *b >= 0));

    step(4, "Idempotency keys: a retried request is applied once");
    let body = json!({"ops": [{"op": "increment", "key": "fees/collected", "by": 25}], "idempotency_key": "invoice-2026-0042"});
    let first = teller.post("/v1/dbs/ledger/txn", body.clone()).await?;
    let retry = teller.post("/v1/dbs/ledger/txn", body).await?;
    println!("    first: version {} value {}", first["version"], first["results"][0]["value"]);
    println!("    retry: version {} replayed={}", retry["version"], retry["replayed"]);
    let r = teller.txn("ledger", json!([{"op": "get", "key": "fees/collected"}])).await?;
    println!("    fees/collected = {} (applied exactly once)", r["results"][0]["value"]);

    step(5, "Conditional writes: check-and-set fails cleanly on a stale version");
    let r = teller.txn("ledger", json!([{"op": "get", "key": "acct/0"}])).await?;
    let v = r["results"][0]["version"].as_u64().unwrap();
    let ok = teller.txn("ledger", json!([{"op": "check", "key": "acct/0", "version": v}, {"op": "put", "key": "acct/0/frozen", "value": "yes"}])).await;
    let stale = teller.txn("ledger", json!([{"op": "check", "key": "acct/0", "version": v - 1}, {"op": "put", "key": "acct/0/frozen", "value": "no"}])).await;
    println!("    with current version: {}", if ok.is_ok() { "applied" } else { "rejected" });
    println!("    with stale version:   {}", stale.err().map(|e| e.to_string()).unwrap_or_default());

    server.stop().await;
    println!("\ndone.");
    Ok(())
}
