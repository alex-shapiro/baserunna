//! Horizontal scaling and failover example.
//!
//! Three stateless nodes share an object store. Any node accepts any request
//! and forwards it to the database's lease holder. When a node is killed, the
//! remaining nodes continue serving reads immediately and resume writes once
//! the dead node's lease expires.
//!
//! Run: `cargo run --example multi_node`

#[path = "common/mod.rs"]
mod common;

use baserunna::client::Client;
use common::step;
use serde_json::json;
use std::time::{Duration, Instant};

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
    let url = |i: usize, nodes: &Vec<Option<common::Isolated>>| nodes[i].as_ref().unwrap().url.clone();
    let user = Client::new(&url(0, &nodes)).signup("sre@example.com", "pager-duty-1").await?;
    let token = user.token().unwrap().to_string();
    let clients: Vec<Client> = (0..3).map(|i| Client::new(&url(i, &nodes)).with_token(&token)).collect();
    tokio::time::sleep(Duration::from_millis(1500)).await; // let heartbeats spread membership

    step(2, "Create databases through different nodes; write via any node");
    for i in 0..6 {
        clients[i % 3].create_db(&format!("app{i}")).await?;
        clients[(i + 1) % 3].txn(&format!("app{i}"), json!([{"op": "increment", "key": "counter", "by": 1}])).await?;
    }
    for i in 0..6 {
        let info = clients[2].get(&format!("/v1/dbs/app{i}")).await?;
        println!("    app{i} is owned by {}", info["stats"]["lease_holder"].as_str().unwrap_or("-"));
    }

    step(3, "Kill the owner of app0 without any cleanup");
    let owner = clients[0].get("/v1/dbs/app0").await?["stats"]["lease_holder"].as_str().unwrap_or("").to_string();
    let victim = (0..3).find(|i| nodes[*i].as_ref().unwrap().id == owner).unwrap_or(0);
    nodes[victim].take().unwrap().kill();
    println!("    killed {owner}");
    let survivor = &clients[(victim + 1) % 3];

    step(4, "Reads keep working immediately; writes resume once the lease expires");
    let t = Instant::now();
    let r = survivor.txn("app0", json!([{"op": "get", "key": "counter"}])).await?;
    println!("    read after {:?}: counter = {}", t.elapsed(), r["results"][0]["value"]);
    let t = Instant::now();
    loop {
        match survivor.txn("app0", json!([{"op": "increment", "key": "counter", "by": 1}])).await {
            Ok(r) => {
                println!("    write accepted after {:?}: counter = {}", t.elapsed(), r["results"][0]["value"]);
                break;
            }
            Err(e) if e.retryable() => tokio::time::sleep(Duration::from_millis(250)).await,
            Err(e) => return Err(e.into()),
        }
    }

    step(5, "Every committed write survived");
    for i in 0..6 {
        let r = survivor.txn(&format!("app{i}"), json!([{"op": "get", "key": "counter"}])).await?;
        println!("    app{i}: counter = {}", r["results"][0]["value"]);
    }

    for s in nodes.into_iter().flatten() {
        s.kill();
    }
    println!("\ndone.");
    Ok(())
}
