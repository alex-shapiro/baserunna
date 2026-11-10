//! Application logs in a stream.
//!
//! Services append JSON lines from their log files into a stream with a time
//! and size cap. Bounded scans filter by field and return the newest errors
//! first with a continuation sequence number. Forward reads wait for new
//! lines like `tail -f`. A log shipper forwards each line once by advancing
//! its consumer cursor in the same transaction that records the batch.
//!
//! Run: `cargo run --example app_logs`

#[path = "common/mod.rs"]
mod common;

use baserunna::meta::SignupPolicy;
use baserunna_client::{Client, HttpTransport, Op, OpResult, api};
use bytes::Bytes;
use common::step;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Serialize, Deserialize)]
struct LogLine {
    level: String,
    service: String,
    msg: String,
    http: Http,
}

#[derive(Debug, Serialize, Deserialize)]
struct Http {
    status: u16,
    ms: u64,
}

fn parse_log_line(r: &api::StreamRecord) -> Result<LogLine, serde_json::Error> {
    serde_json::from_value(r.json.clone().unwrap_or_default())
}

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
        let line = LogLine {
            level: level.into(),
            service: services[i % 3].into(),
            msg: if level == "error" {
                format!("upstream timeout after {} ms", 100 + i % 900)
            } else {
                format!("handled request {i}")
            },
            http: Http { status: if level == "error" { 503 } else { 200 }, ms: (i % 250) as u64 },
        };
        out.push_str(&serde_json::to_string(&line).expect("log lines serialize"));
        out.push('\n');
    }
    Bytes::from(out)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = common::start(common::shared_store(), "logs", SignupPolicy::FirstUserOnly).await;
    let anon = Client::new(HttpTransport::new(&server.url));
    let session = anon.sign_up("ops@example.com", "logs-admin-1").await?;
    let admin = anon.with_token(&session.token);
    admin.create_db("ops", None).await?;
    let db = admin.db("ops");

    step(1, "A stream that keeps a day of logs, at most 256 MiB");
    let new = api::NewStream { name: "app".into(), retention_ms: Some(86_400_000), max_bytes: Some(256 << 20) };
    let s = db.create_stream(&new).await?.stream;
    println!(
        "    {}: retention {} ms, max {} bytes, records {}..{}",
        s.name,
        s.retention_ms.unwrap_or_default(),
        s.max_bytes.unwrap_or_default(),
        s.state.first_seq,
        s.state.next_seq
    );

    step(2, "Ship log files: 20 batches of 500 JSON lines, as NDJSON");
    let t = std::time::Instant::now();
    for b in 0..20 {
        let r = db.append_lines("app", log_lines(b * 500, 500)).await?;
        if b % 5 == 0 {
            println!("    batch {b}: records {}..{}", r.first_seq, r.next_seq);
        }
    }
    println!("    10,000 lines in {:.0} ms (one commit, one PUT, per batch)", t.elapsed().as_secs_f64() * 1e3);

    step(3, "The latest errors, newest first, by a bounded scan");
    let errors = json!({"and": [
        {"field": "level", "op": "eq", "value": "error"},
        {"field": "http.status", "op": "ge", "value": 500},
    ]});
    let mut until: Option<u64> = None;
    let mut found = Vec::new();
    while found.len() < 5 {
        let q = api::RecordQuery {
            reverse: true,
            limit: Some(5),
            scan_limit: Some(2000),
            filter: Some(errors.clone()),
            until_seq: until,
            ..Default::default()
        };
        let page = db.query_records("app", &q).await?;
        println!("    page: {} matches, continue before {}", page.records.len(), page.next_seq);
        found.extend(page.records);
        if page.done {
            break;
        }
        until = Some(page.next_seq);
    }
    for r in found.iter().take(5) {
        let line = parse_log_line(r)?;
        println!("    #{} {}: {}", r.seq, line.service, line.msg);
    }

    step(4, "Text search over the last 1000 lines");
    let q = api::RecordQuery {
        tail: Some(1000),
        filter: Some(json!({"op": "contains", "value": "timeout after 9"})),
        ..Default::default()
    };
    let r = db.query_records("app", &q).await?;
    println!("    {} lines mention a timeout of 900 ms or more", r.records.len());

    step(5, "tail -f: a forward read that waits for the next lines");
    let next = db.stream_info("app").await?.stream.state.next_seq;
    let follower = {
        let db = db.clone();
        let q = api::ReadRecordsQuery { from_seq: Some(next), wait_ms: Some(10_000), ..Default::default() };
        tokio::spawn(async move { db.read_records("app", q).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    db.append_lines("app", log_lines(10_000, 3)).await?;
    let page = follower.await??;
    for r in &page.records {
        let line = parse_log_line(r)?;
        println!("    #{} [{}] {}", r.seq, line.level, line.msg);
    }
    println!(
        "    (curl -N '…/streams/app/records?from_seq={}&format=sse&wait_ms=300000' streams them as events)",
        page.next_seq
    );

    step(6, "A log shipper forwards every line exactly once");
    let read = Op::ReadStream(api::ReadStream {
        stream: "app".into(),
        consumer: Some("shipper".into()),
        limit: Some(1000),
        ..Default::default()
    });
    let mut shipped = 0;
    loop {
        // Advance the consumer cursor and the shipped counter in the same
        // transaction so a crash cannot duplicate or skip a batch.
        let mut txn = db.begin();
        let OpResult::ReadStream(page) = txn.run_one(read.clone()).await? else {
            return Err("read_stream returns a page of records".into());
        };
        let n = page.records.len();
        if n == 0 {
            txn.abort().await;
            break;
        }
        txn.write([Op::increment("shipped", n as i64), Op::set_consumer("app", "shipper", page.next_seq)]);
        txn.commit().await?;
        shipped += n;
    }
    let info = db.stream_info("app").await?;
    let consumers: Vec<String> =
        info.consumers.iter().map(|c| format!("{} at #{} with {} unread", c.name, c.seq, c.lag)).collect();
    println!("    shipped {shipped} lines; consumers: {consumers:?}");

    step(7, "Trim what the shipper has forwarded");
    let cursor = info.consumers.first().map_or(0, |c| c.seq);
    db.write(&[Op::trim_stream("app", cursor.saturating_sub(100))]).await?;
    let s = db.stream_info("app").await?.stream.state;
    println!("    the stream now keeps records {}..{} ({} bytes)", s.first_seq, s.next_seq, s.bytes);

    server.stop().await;
    println!("\ndone.");
    Ok(())
}
