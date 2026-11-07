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

use baserunna::meta::SignupPolicy;
use baserunna_client::api::{self, ErrorCode, Expect, PutOptions, Written};
use baserunna_client::{Change, ChangesRequest, Client, Error, HttpTransport, KeyRange, Op};
use bytes::Bytes;
use common::step;
use serde_json::json;
use std::time::{Duration, Instant};

fn json_value() -> PutOptions {
    PutOptions { content_type: Some("application/json".into()), ..PutOptions::default() }
}

/// Describes the outcome of a conditional write, propagating errors other than a failed
/// precondition.
fn conditional(r: Result<Written, Error>) -> Result<String, Error> {
    match r {
        Ok(w) => Ok(format!("written at version {}", w.version)),
        Err(e) if e.code() == Some(ErrorCode::PreconditionFailed) => Ok(format!("rejected ({e})")),
        Err(e) => Err(e),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = common::start(common::shared_store(), "flags", SignupPolicy::FirstUserOnly).await;
    let anon = Client::new(HttpTransport::new(&server.url));
    let admin = anon.with_token(&anon.sign_up("platform@example.com", "flags-admin-1").await?.token);
    admin.create_db("flags", None).await?;
    let flags_db = admin.db("flags");

    step(1, "Seed flags (values are JSON documents stored as bytes)");
    let flags = [
        ("checkout.new_flow", json!({"enabled": true, "rollout": 0.25})),
        ("search.semantic", json!({"enabled": false})),
        ("ui.dark_mode", json!({"enabled": true, "rollout": 1.0})),
    ];
    for (k, v) in &flags {
        flags_db.put_value(*k, v.to_string(), json_value()).await?;
    }

    step(2, "A read key for application servers");
    let key =
        api::NewApiKey { name: "app-servers".into(), db: Some("flags".into()), read_only: true, rate_limit: None };
    let app = admin.with_token(&admin.create_api_key(&key).await?.key);
    let app_db = app.db("flags");

    step(3, "Hot read path: 2000 strong (linearizable) reads");
    let t = Instant::now();
    for i in 0..2000 {
        let key = flags[i % flags.len()].0;
        let v = app_db.get_value(key, None).await?.expect("flags were seeded");
        let _: serde_json::Value = serde_json::from_slice(&v.content.data)?;
    }
    let el = t.elapsed();
    let info = flags_db.info().await?;
    println!("    {:.0} reads/s over HTTP from one sequential client", 2000.0 / el.as_secs_f64());
    println!(
        "    lease holder {:?}: while it holds the lease, strong reads are answered from its in-memory snapshot with no object-store requests",
        info.stats.lease_holder.unwrap_or_default()
    );

    step(4, "Compare-and-set: two admins edit the same flag concurrently");
    let read = flags_db.get_value("checkout.new_flow", None).await?.expect("the flag was seeded");
    println!("    both admins read version {}", read.version);
    let cas = PutOptions { expect: Some(Expect::Version(read.version)), ..json_value() };
    let a = flags_db.put_value("checkout.new_flow", json!({"enabled": true, "rollout": 0.5}).to_string(), cas.clone());
    let a = conditional(a.await)?;
    let b = conditional(flags_db.put_value("checkout.new_flow", json!({"enabled": false}).to_string(), cas).await)?;
    println!("    admin A (rollout to 50%): {a}");
    println!("    admin B (disable):        {b} -> must re-read and retry");

    step(5, "Create-only writes: If-None-Match: *");
    let create = PutOptions { expect: Some(Expect::Absent), ..json_value() };
    let r = conditional(flags_db.put_value("ui.dark_mode", "{}", create).await)?;
    println!("    overwrite an existing flag with If-None-Match: {r}");

    step(6, "Snapshot reads (bounded staleness) and a consistent multi-key read");
    let page = app
        .db("flags")
        .read_snapshot(&[Op::scan("")], Duration::from_millis(1000))
        .await?
        .swap_remove(0)
        .scan_page()?;
    for e in page.entries {
        let value = e.value.data().map(|d| String::from_utf8_lossy(d).into_owned()).unwrap_or_default();
        println!("    {} = {value} (v{})", String::from_utf8_lossy(&e.key), e.version.unwrap_or_default());
    }

    step(7, "A temporary override that expires by itself");
    let ttl = PutOptions { ttl_ms: Some(1500), ..json_value() };
    let w = flags_db.put_value("override/search.semantic", json!({"enabled": true}).to_string(), ttl).await?;
    let v = flags_db.get_value("override/search.semantic", None).await?.expect("the override was just written");
    println!("    override written at version {}, expires at {:?} (cluster time, ms)", w.version, v.expires_at);
    tokio::time::sleep(Duration::from_millis(1_600)).await;
    let gone = app_db.get_value("override/search.semantic", None).await?;
    println!(
        "    after 1.6 s: {}",
        if gone.is_none() { "expired, the flag's own value applies again" } else { "still there?!" }
    );

    step(8, "App servers follow flag changes with a long poll");
    let since = flags_db.info().await?.stats.version.to_string();
    let watcher = {
        let req = ChangesRequest {
            range: KeyRange::Prefix(Bytes::new()),
            after: since,
            limit: api::PAGE_MAX,
            wait_ms: 10_000,
            max_staleness_ms: None,
        };
        let app_db = app_db.clone();
        tokio::spawn(async move { app_db.changes(&req).await })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    flags_db.put_value("ui.dark_mode", json!({"enabled": false}).to_string(), json_value()).await?;
    let page = watcher.await??;
    for ch in &page.changes {
        let text = |b: &Bytes| String::from_utf8_lossy(b).into_owned();
        match ch {
            Change::Put { key, version, value } => {
                let value = value.data().map_or("(blob)".into(), text);
                println!("    v{version}: {} = {value}", text(key));
            }
            Change::Delete { key, version } => println!("    v{version}: {} = (deleted)", text(key)),
            Change::DeleteRange { start, end, version } => {
                println!("    v{version}: deleted {}..{}", text(start), end.as_ref().map_or(String::new(), text));
            }
        }
    }
    println!("    continue from cursor {}", page.cursor);

    server.stop().await;
    println!("\ndone.");
    Ok(())
}
