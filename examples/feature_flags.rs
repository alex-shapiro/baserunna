//! Read-heavy feature-flag and configuration store example.
//!
//! Strong reads are served from the lease holder's memory without object-store
//! requests, compare-and-set prevents concurrent admins from overwriting each
//! other, and `snapshot` reads trade bounded staleness for local reads on any
//! node. Temporary overrides expire automatically via TTLs, and application
//! servers follow flag updates through the change feed.
//!
//! Run: `cargo run --example feature_flags`

#[path = "common/mod.rs"]
mod common;

use baserunna::client::Client;
use baserunna::meta::SignupPolicy;
use bytes::Bytes;
use common::step;
use serde_json::json;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = common::start(common::shared_store(), "flags", SignupPolicy::FirstUserOnly).await;
    let admin = Client::new(&server.url).signup("platform@example.com", "flags-admin-1").await?;
    admin.create_db("flags").await?;

    step(1, "Seed flags (values are JSON documents stored as bytes)");
    let flags = [
        ("checkout.new_flow", json!({"enabled": true, "rollout": 0.25})),
        ("search.semantic", json!({"enabled": false})),
        ("ui.dark_mode", json!({"enabled": true, "rollout": 1.0})),
    ];
    for (k, v) in &flags {
        admin.put_bytes("flags", k, Bytes::from(v.to_string()), "application/json").await?;
    }

    step(2, "A read key for application servers");
    let k = admin.post("/v1/api-keys", json!({"name": "app-servers", "db": "flags", "read_only": true})).await?;
    let app = admin.with_token(k["key"].as_str().unwrap());

    step(3, "Hot read path: 2000 strong (linearizable) reads");
    let t = Instant::now();
    for i in 0..2000 {
        let key = flags[i % flags.len()].0;
        let v = app.get_bytes("flags", key, None).await?.unwrap();
        let _: serde_json::Value = serde_json::from_slice(&v)?;
    }
    let el = t.elapsed();
    let info = admin.get("/v1/dbs/flags").await?;
    println!("    {:.0} reads/s over HTTP from one sequential client", 2000.0 / el.as_secs_f64());
    println!(
        "    lease holder {}: while it holds the lease, strong reads are answered from its in-memory snapshot with no object-store requests",
        info["stats"]["lease_holder"]
    );

    step(4, "Compare-and-set: two admins edit the same flag concurrently");
    let path = "/v1/dbs/flags/kv/checkout.new_flow";
    let (_, headers, _) = admin.raw("GET", path, &[], Bytes::new()).await?;
    let etag = headers.get("etag").unwrap().to_str()?.to_string();
    println!("    both admins read version {etag}");
    let a = admin
        .raw("PUT", path, &[("if-match", &etag)], Bytes::from(json!({"enabled": true, "rollout": 0.5}).to_string()))
        .await?;
    let b = admin.raw("PUT", path, &[("if-match", &etag)], Bytes::from(json!({"enabled": false}).to_string())).await?;
    println!("    admin A (rollout to 50%): HTTP {}", a.0);
    println!("    admin B (disable):        HTTP {} -> must re-read and retry", b.0);

    step(5, "Create-only writes: If-None-Match: *");
    let r = admin.raw("PUT", "/v1/dbs/flags/kv/ui.dark_mode", &[("if-none-match", "*")], Bytes::from("{}")).await?;
    println!("    overwrite an existing flag with If-None-Match: HTTP {}", r.0);

    step(6, "Snapshot reads (bounded staleness) and a consistent multi-key read");
    let r = app
        .post(
            "/v1/dbs/flags/txn",
            json!({"ops": [{"op": "scan", "prefix": "", "limit": 100}], "consistency": "snapshot", "max_staleness_ms": 1000}),
        )
        .await?;
    for it in r["results"][0]["items"].as_array().unwrap() {
        println!("    {} = {} (v{})", it["key"].as_str().unwrap(), it["value"].as_str().unwrap(), it["version"]);
    }

    step(7, "A temporary override that expires by itself");
    let path = "/v1/dbs/flags/kv/override%2Fsearch.semantic";
    let (st, _, _) = admin
        .raw("PUT", &format!("{path}?ttl_ms=1500"), &[], Bytes::from(json!({"enabled": true}).to_string()))
        .await?;
    let (_, headers, _) = admin.raw("GET", path, &[], Bytes::new()).await?;
    println!(
        "    override written (HTTP {st}), expires at {:?} (cluster time, ms)",
        headers.get("x-baserunna-expires-at")
    );
    tokio::time::sleep(std::time::Duration::from_millis(1_600)).await;
    let gone = app.get_bytes("flags", "override/search.semantic", None).await?;
    println!(
        "    after 1.6 s: {}",
        if gone.is_none() { "expired, the flag's own value applies again" } else { "still there?!" }
    );

    step(8, "App servers follow flag changes with a long poll");
    let info = admin.get("/v1/dbs/flags").await?;
    let since = info["stats"]["version"].to_string();
    let watcher = {
        let app = app.clone();
        tokio::spawn(async move { app.changes("flags", &since, 10_000).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    admin
        .put_bytes("flags", "ui.dark_mode", Bytes::from(json!({"enabled": false}).to_string()), "application/json")
        .await?;
    let page = watcher.await??;
    for ch in page["changes"].as_array().unwrap() {
        println!(
            "    v{}: {} = {}",
            ch["version"],
            ch["key"].as_str().unwrap(),
            ch["value"].as_str().unwrap_or("(deleted)")
        );
    }
    println!("    continue from cursor {}", page["next_cursor"]);

    server.stop().await;
    println!("\ndone.");
    Ok(())
}
