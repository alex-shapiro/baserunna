//! Application logs in a stream.
//!
//! Services ship JSON lines straight from their log files; the stream keeps
//! a day of them within a size cap. Queries find the latest errors newest
//! first, filtered by field, by bounded scans that say where to continue;
//! `tail -f` is a forward read that waits for new lines; and a log shipper
//! forwards every line exactly once by advancing its cursor in the same
//! transaction that records what it shipped.
//!
//! Run: `cargo run --example app_logs`

#[path = "common/mod.rs"]
mod common;

use baserunna::client::Client;
use baserunna::meta::SignupPolicy;
use bytes::Bytes;
use common::step;
use serde_json::{Value, json};

fn log_lines(start: usize, n: usize) -> Bytes {
    let services = ["api", "billing", "search"];
    let mut out = String::new();
    for i in start..start + n {
        let level = if i % 17 == 0 {
            "error"
        } else if i % 5 == 0 {
            "warn"
        } else {
            "info"
        };
        let line = json!({
            "level": level,
            "service": services[i % 3],
            "msg": if level == "error" { format!("upstream timeout after {} ms", 100 + i % 900) } else { format!("handled request {i}") },
            "http": {"status": if level == "error" { 503 } else { 200 }, "ms": i % 250},
        });
        out.push_str(&line.to_string());
        out.push('\n');
    }
    Bytes::from(out)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = common::start(common::shared_store(), "logs", SignupPolicy::FirstUserOnly).await;
    let admin = Client::new(&server.url).signup("ops@example.com", "logs-admin-1").await?;
    admin.create_db("ops").await?;

    step(1, "A stream that keeps a day of logs, at most 256 MiB");
    let s = admin.create_stream("ops", "app", json!({"retention_ms": 86_400_000, "max_bytes": 256 << 20})).await?;
    println!("    {s}");

    step(2, "Ship log files: 20 batches of 500 JSON lines, as NDJSON");
    let t = std::time::Instant::now();
    for b in 0..20 {
        let r = admin.append_lines("ops", "app", log_lines(b * 500, 500), true).await?;
        if b % 5 == 0 {
            println!("    batch {b}: records {}..{}", r["first_seq"], r["next_seq"]);
        }
    }
    println!("    10,000 lines in {:.0} ms (one commit, one PUT, per batch)", t.elapsed().as_secs_f64() * 1e3);

    step(3, "The latest errors, newest first, by a bounded scan");
    let mut until: Option<u64> = None;
    let mut found = Vec::new();
    while found.len() < 5 {
        let mut q = json!({"reverse": true, "limit": 5, "scan_limit": 2000,
            "filter": {"and": [{"field": "level", "op": "eq", "value": "error"}, {"field": "http.status", "op": "ge", "value": 500}]}});
        if let Some(u) = until {
            q["until_seq"] = json!(u);
        }
        let page = admin.query_stream("ops", "app", q).await?;
        found.extend(page["records"].as_array().cloned().unwrap_or_default());
        println!(
            "    page: {} matches, continue before {}",
            page["records"].as_array().map_or(0, Vec::len),
            page["next_seq"]
        );
        if page["done"] == true {
            break;
        }
        until = page["next_seq"].as_u64();
    }
    for r in found.iter().take(5) {
        println!(
            "    #{} {}: {}",
            r["seq"],
            r["json"]["service"].as_str().unwrap_or(""),
            r["json"]["msg"].as_str().unwrap_or("")
        );
    }

    step(4, "Text search over the last 1000 lines");
    let r = admin
        .query_stream("ops", "app", json!({"tail": 1000, "filter": {"op": "contains", "value": "timeout after 9"}}))
        .await?;
    println!("    {} lines mention a timeout of 900 ms or more", r["records"].as_array().map_or(0, Vec::len));

    step(5, "tail -f: a forward read that waits for the next lines");
    let info = admin.get("/v1/dbs/ops/streams/app").await?;
    let next = info["next_seq"].as_u64().unwrap_or(0);
    let follower = {
        let c = admin.clone();
        tokio::spawn(async move { c.read_stream("ops", "app", &format!("from_seq={next}&wait_ms=10000")).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    admin.append_lines("ops", "app", log_lines(10_000, 3), true).await?;
    let page = follower.await??;
    for r in page["records"].as_array().unwrap() {
        println!(
            "    #{} [{}] {}",
            r["seq"],
            r["json"]["level"].as_str().unwrap_or(""),
            r["json"]["msg"].as_str().unwrap_or("")
        );
    }
    println!(
        "    (curl -N '…/streams/app/records?from_seq={}&format=sse&wait_ms=300000' streams them as events)",
        page["next_seq"]
    );

    step(6, "A log shipper forwards every line exactly once");
    let mut shipped = 0;
    loop {
        // Read a batch from the cursor, "ship" it, and advance the cursor and
        // a counter of shipped lines atomically: a crash anywhere repeats
        // nothing and skips nothing.
        let begin = admin.post("/v1/dbs/ops/transactions", json!({})).await?;
        let id = begin["txn_id"].as_str().unwrap().to_string();
        let ops = json!({"ops": [{"op": "read_stream", "stream": "app", "consumer": "shipper", "limit": 1000}]});
        let r = admin.post(&format!("/v1/dbs/ops/transactions/{id}/ops"), ops).await?;
        let read = &r["results"][0];
        let n = read["records"].as_array().map_or(0, Vec::len);
        if n == 0 {
            admin.delete(&format!("/v1/dbs/ops/transactions/{id}")).await?;
            break;
        }
        let ops = json!({"ops": [
            {"op": "increment", "key": "shipped", "by": n},
            {"op": "set_consumer", "stream": "app", "consumer": "shipper", "seq": read["next_seq"]},
        ]});
        admin.post(&format!("/v1/dbs/ops/transactions/{id}/ops"), ops).await?;
        admin.post(&format!("/v1/dbs/ops/transactions/{id}/commit"), json!({})).await?;
        shipped += n;
    }
    let info: Value = admin.get("/v1/dbs/ops/streams/app").await?;
    println!("    shipped {shipped} lines; consumers: {}", info["consumers"]);

    step(7, "Trim what the shipper has forwarded");
    let cursor = info["consumers"][0]["seq"].as_u64().unwrap_or(0);
    admin.post("/v1/dbs/ops/streams/app/trim", json!({"before_seq": cursor.saturating_sub(100)})).await?;
    let info = admin.get("/v1/dbs/ops/streams/app").await?;
    println!("    the stream now keeps records {}..{} ({} bytes)", info["first_seq"], info["next_seq"], info["bytes"]);

    server.stop().await;
    println!("\ndone.");
    Ok(())
}
