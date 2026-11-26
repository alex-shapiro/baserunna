//! Distinct count of a 60 KB string column, from `long_string_queries_respect_memory_limit`
//! (tests/event_query_limits.rs). The attack queries 8 days of 8 bulk inserts each, as the test's
//! largest round does.
//!
//! The small variant differs from the test's 1-day round. It queries 1 day of 1 insert (500 rows,
//! 30 MB of strings) instead of 8 inserts. Reason: over real time, today's node keeps the 8 inserts
//! of a day as 2 parts of 2,000 rows, and it refuses the 1-day query with 413 `too_large` because
//! the query needs more than the 256 MiB node memory budget. A small variant must succeed on a
//! correct node, so it must read less data than the budget.

use super::{AttackContext, AttackOutcome, Round, Variant, Workload, concurrent_posts};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use serde_json::{Value as Json, json};
use std::time::{Duration, Instant};

const DAY_MS: i64 = 86_400_000;
const DAYS_FROM_MS: i64 = 1_740_000_000_000 / DAY_MS * DAY_MS;
const BLOB_BYTES: usize = 60_000;
const ROWS_PER_INSERT: usize = 500;
/// Bulk inserts per day. `event_merge_fan_in` parts merge into a single part.
const INSERTS_PER_DAY: usize = 8;
const TABLE: &str = "blobs";

fn inserts_per_day(v: Variant) -> usize {
    match v {
        Variant::Small => 1,
        Variant::Attack => INSERTS_PER_DAY,
    }
}

fn days(v: Variant) -> usize {
    match v {
        Variant::Small => 1,
        Variant::Attack => 8,
    }
}

/// Unique string of `BLOB_BYTES` characters drawn from 2 letters.
fn blob(row: u64) -> String {
    let mut s = format!("{row:016x}");
    let mut x = row.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    while s.len() < BLOB_BYTES {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        for k in 0..64 {
            s.push(if (x >> k) & 1 == 0 { 'a' } else { 'b' });
        }
    }
    s.truncate(BLOB_BYTES);
    s
}

/// Waits until the table's part count stays the same for 3 s, at most 20 s, and returns the parts.
async fn settle_parts(cx: &AttackContext) -> Result<Json, String> {
    let (api, token, db) = (&cx.api, &cx.attacker.token, &cx.attacker.db);
    let started = Instant::now();
    let mut parts = api.part_count(token, db, TABLE).await?;
    let mut unchanged = 0;
    while unchanged < 6 && started.elapsed() < Duration::from_secs(20) {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let now = api.part_count(token, db, TABLE).await?;
        unchanged = if now == parts { unchanged + 1 } else { 0 };
        parts = now;
    }
    let info = api.table_info(token, db, TABLE).await?;
    let parts = info["parts"].as_array().into_iter().flatten();
    Ok(parts
        .map(|p| json!({"rows": p["rows"], "raw_bytes": p["raw_bytes"], "bytes": p["bytes"], "level": p["level"]}))
        .collect())
}

pub struct LongStrings;

impl Workload for LongStrings {
    fn prepare<'a>(&'a self, cx: &'a AttackContext, round: Round) -> BoxFuture<'a, Result<Json, String>> {
        Box::pin(async move {
            if round == Round::WithVictim {
                return Ok(json!({"reuses": "the table of the first round"}));
            }
            let (api, token, db) = (&cx.api, &cx.attacker.token, &cx.attacker.db);
            api.create_event_table(
                token,
                db,
                json!({"name": TABLE, "columns": [{"name": "time", "type": "time"}, {"name": "s", "type": "string"}],
                    "time_column": "time", "order_by": ["time"]}),
            )
            .await?;
            let started = Instant::now();
            let days = days(cx.variant);
            let mut row = 0u64;
            for day in 0..days {
                for _ in 0..inserts_per_day(cx.variant) {
                    let batch: Vec<String> = (0..ROWS_PER_INSERT)
                        .map(|_| {
                            row += 1;
                            let time = DAYS_FROM_MS + day as i64 * DAY_MS + row as i64 * 1000;
                            format!(r#"{{"time": {time}, "s": "{}"}}"#, blob(row))
                        })
                        .collect();
                    api.bulk_insert(token, db, TABLE, format!("[{}]", batch.join(","))).await?;
                }
            }
            let inserted_s = started.elapsed().as_secs_f64();
            let parts = settle_parts(cx).await?;
            Ok(json!({"days": days, "rows": row, "insert_s": inserted_s, "parts": parts,
                "setup_s": started.elapsed().as_secs_f64()}))
        })
    }

    fn attack<'a>(&'a self, cx: &'a AttackContext, _round: Round) -> BoxFuture<'a, Result<AttackOutcome, String>> {
        Box::pin(async move {
            let days = days(cx.variant);
            let q = json!({"from": DAYS_FROM_MS, "to": DAYS_FROM_MS + days as i64 * DAY_MS,
                "aggregates": {"u": {"fn": "uniq", "of": {"col": "s"}}}});
            let path = format!("/v1/dbs/{}/events/{TABLE}/rows/query", cx.attacker.db);
            let requests = concurrent_posts(
                cx,
                vec![(format!("uniq over {days} days"), path, Bytes::from(q.to_string()))],
                &[200],
            )
            .await;
            Ok(AttackOutcome { requests, completed: true, notes: json!({"days": days}) })
        })
    }
}
