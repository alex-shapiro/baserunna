//! Distinct-count query over a 60 KB string column.
//!
//! The attack variant loads 8 days with 8 bulk inserts per day, then queries all 8 days.
//! The small variant loads 1 day with 1 bulk insert of 500 rows, or 30 MB of strings.
//! On `main`, a full 8-insert day exceeds the 256 MiB node memory budget, so the node returns
//! HTTP 413.
//! A single bulk insert fits within that budget, so a healthy node completes the small variant.

use super::{AttackContext, AttackOutcome, Round, Variant, Workload, concurrent_posts};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use serde_json::{Value as Json, json};
use std::time::{Duration, Instant};

const DAY_MS: i64 = 86_400_000;
const FIRST_DAY_START_MS: i64 = 1_740_000_000_000 / DAY_MS * DAY_MS;
const STRING_BYTES: usize = 60_000;
const ROWS_PER_INSERT: usize = 500;
/// Bulk inserts per day. The node merges `event_merge_fan_in` parts into a single part.
const INSERTS_PER_DAY: usize = 8;
const EVENT_TABLE: &str = "blobs";

fn inserts_per_day(variant: Variant) -> usize {
    match variant {
        Variant::Small => 1,
        Variant::Attack => INSERTS_PER_DAY,
    }
}

fn day_count(variant: Variant) -> usize {
    match variant {
        Variant::Small => 1,
        Variant::Attack => 8,
    }
}

/// Generates a unique `STRING_BYTES` string from the letters `a` and `b`.
fn unique_string(row_index: u64) -> String {
    let mut text = format!("{row_index:016x}");
    let mut state = row_index.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    while text.len() < STRING_BYTES {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        for bit in 0..64 {
            text.push(if (state >> bit) & 1 == 0 { 'a' } else { 'b' });
        }
    }
    text.truncate(STRING_BYTES);
    text
}

/// Polls the table for up to 20 seconds until the node holds the same part count for 3 seconds.
/// Returns metadata for the resulting parts.
async fn settle_parts(ctx: &AttackContext) -> Result<Json, String> {
    let (api, token, db) = (&ctx.api, &ctx.attacker.token, &ctx.attacker.db);
    let started = Instant::now();
    let mut part_count = api.part_count(token, db, EVENT_TABLE).await?;
    let mut stable_polls = 0;
    while stable_polls < 6 && started.elapsed() < Duration::from_secs(20) {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let current_part_count = api.part_count(token, db, EVENT_TABLE).await?;
        stable_polls = if current_part_count == part_count { stable_polls + 1 } else { 0 };
        part_count = current_part_count;
    }
    let table_info = api.table_info(token, db, EVENT_TABLE).await?;
    let parts = table_info["parts"].as_array().into_iter().flatten();
    Ok(parts
        .map(|part| {
            json!({"rows": part["rows"], "raw_bytes": part["raw_bytes"], "bytes": part["bytes"], "level": part["level"]})
        })
        .collect())
}

pub struct LongStrings;

impl Workload for LongStrings {
    fn prepare<'a>(&'a self, ctx: &'a AttackContext, round: Round) -> BoxFuture<'a, Result<Json, String>> {
        Box::pin(async move {
            if round == Round::WithVictim {
                return Ok(json!({"reuses": "the table of the first round"}));
            }
            let (api, token, db) = (&ctx.api, &ctx.attacker.token, &ctx.attacker.db);
            api.create_event_table(
                token,
                db,
                json!({"name": EVENT_TABLE, "columns": [{"name": "time", "type": "time"}, {"name": "s", "type": "string"}],
                    "time_column": "time", "order_by": ["time"]}),
            )
            .await?;
            let started = Instant::now();
            let days = day_count(ctx.variant);
            let mut row_count = 0u64;
            for day in 0..days {
                for _ in 0..inserts_per_day(ctx.variant) {
                    let batch: Vec<String> = (0..ROWS_PER_INSERT)
                        .map(|_| {
                            row_count += 1;
                            let time = FIRST_DAY_START_MS + day as i64 * DAY_MS + row_count as i64 * 1000;
                            format!(r#"{{"time": {time}, "s": "{}"}}"#, unique_string(row_count))
                        })
                        .collect();
                    api.bulk_insert(token, db, EVENT_TABLE, format!("[{}]", batch.join(","))).await?;
                }
            }
            let inserted_s = started.elapsed().as_secs_f64();
            let parts = settle_parts(ctx).await?;
            Ok(json!({"days": days, "rows": row_count, "insert_s": inserted_s, "parts": parts,
                "setup_s": started.elapsed().as_secs_f64()}))
        })
    }

    fn attack<'a>(&'a self, ctx: &'a AttackContext, _round: Round) -> BoxFuture<'a, Result<AttackOutcome, String>> {
        Box::pin(async move {
            let days = day_count(ctx.variant);
            let query = json!({"from": FIRST_DAY_START_MS, "to": FIRST_DAY_START_MS + days as i64 * DAY_MS,
                "aggregates": {"u": {"fn": "uniq", "of": {"col": "s"}}}});
            let path = format!("/v1/dbs/{}/events/{EVENT_TABLE}/rows/query", ctx.attacker.db);
            let requests = concurrent_posts(
                ctx,
                vec![(format!("uniq over {days} days"), path, Bytes::from(query.to_string()))],
                &[200],
            )
            .await;
            Ok(AttackOutcome { requests, completed: true, notes: json!({"days": days}) })
        })
    }
}
