//! Horizontal scaling and failover example.
//!
//! Three stateless nodes share an object store. Any node accepts any request
//! and forwards it to the database's lease holder. If a node is killed, the
//! remaining nodes serve reads immediately. Writes resume after the dead
//! node's lease expires.
//!
//! Run: `cargo run --example multi_node`

#[path = "common/mod.rs"]
mod common;

use baserunna_client::{Client, Error, HttpTransport, Op, OpResult};
use common::step;
use std::time::{Duration, Instant};

/// Formats the counter value from a `Get` or `Increment` result.
fn counter_value(result: OpResult) -> Result<String, Error> {
    Ok(match result {
        OpResult::Increment(v) => v.to_string(),
        r => r.entry()?.and_then(|e| Some(String::from_utf8_lossy(e.value.data()?).into_owned())).unwrap_or_default(),
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = common::shared_store();
    step(1, "Start three nodes on one shared object store");
    let mut nodes = Vec::new();
    for i in 0..3 {
        let s = common::start_isolated(store.clone(), &format!("node{i}")).await;
        println!("    {} at {}", s.id, s.url);
        nodes.push(Some(s));
    }
    let client = |i: usize| Client::new(HttpTransport::new(&nodes[i].as_ref().unwrap().url));
    let token = client(0).sign_up("sre@example.com", "pager-duty-1").await?.token;
    let clients: Vec<Client> = (0..3).map(|i| client(i).with_token(&token)).collect();
    // Wait for heartbeats to share cluster membership.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    step(2, "Create databases through different nodes; write via any node");
    for i in 0..6 {
        clients[i % 3].create_db(&format!("app{i}"), None).await?;
        clients[(i + 1) % 3].db(&format!("app{i}")).write(&[Op::increment("counter", 1)]).await?;
    }
    for i in 0..6 {
        let info = clients[2].db(&format!("app{i}")).info().await?;
        println!("    app{i} is owned by {}", info.stats.lease_holder.as_deref().unwrap_or("-"));
    }

    step(3, "Kill the owner of app0 without any cleanup");
    let owner = clients[0].db("app0").info().await?.stats.lease_holder.unwrap_or_default();
    let victim = (0..3).find(|i| nodes[*i].as_ref().unwrap().id == owner).unwrap_or(0);
    nodes[victim].take().unwrap().kill();
    println!("    killed {owner}");
    let remaining = clients[(victim + 1) % 3].db("app0");

    step(4, "Reads keep working immediately; writes resume once the lease expires");
    let t = Instant::now();
    let r = remaining.read_one(Op::get("counter")).await?;
    println!("    read after {:?}: counter = {}", t.elapsed(), counter_value(r)?);
    let t = Instant::now();
    loop {
        match remaining.txn(&[Op::increment("counter", 1)], None).await {
            Ok(r) => {
                let r = r.results.unwrap_or_default().swap_remove(0);
                println!("    write accepted after {:?}: counter = {}", t.elapsed(), counter_value(r)?);
                break;
            }
            Err(e) if e.is_transient(false) => tokio::time::sleep(Duration::from_millis(250)).await,
            Err(e) => return Err(e.into()),
        }
    }

    step(5, "Every committed write is preserved");
    for i in 0..6 {
        let r = remaining.client().db(&format!("app{i}")).read_one(Op::get("counter")).await?;
        println!("    app{i}: counter = {}", counter_value(r)?);
    }

    for s in nodes.into_iter().flatten() {
        s.kill();
    }
    println!("\ndone.");
    Ok(())
}
