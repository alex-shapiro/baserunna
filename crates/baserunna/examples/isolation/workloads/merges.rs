//! Concurrent part merges in 1 or 16 event tables of 500,000 rows each, from
//! `event_merges_share_a_node_wide_limit` (tests/event_ingest_limits.rs).
//!
//! Each table gets 7 parts in setup. The attack inserts one row per table, which adds the 8th
//! part and starts a merge in every table at once. The round ends when every table has 1 part or
//! after [`MERGE_TIMEOUT`].

use super::{AttackContext, AttackOutcome, Round, Variant, Workload, concurrent_posts};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use serde_json::{Value as Json, json};
use std::time::{Duration, Instant};

const DAY_MS: i64 = 86_400_000;
const MERGE_DAY_MS: i64 = 1_749_000_000_000 / DAY_MS * DAY_MS;
const ROWS_PER_TABLE: usize = 500_000;
const PARTS_BEFORE_TRIGGER: usize = 7;
const MERGE_TIMEOUT: Duration = Duration::from_secs(40);
/// Tables that receive setup inserts at once.
const SETUP_CONCURRENCY: usize = 4;

fn tables(v: Variant) -> usize {
    match v {
        Variant::Small => 1,
        Variant::Attack => 16,
    }
}

fn db(round: Round) -> String {
    format!("merges{}", round.index())
}

fn small_event(i: usize) -> String {
    let time = MERGE_DAY_MS + (i as i64 * 7_919) % DAY_MS;
    format!(r#"{{"t":{time},"u":"u{:07}","e":"e{}"}}"#, i % 100_000, i % 8)
}

pub struct Merges;

impl Workload for Merges {
    fn prepare<'a>(&'a self, cx: &'a AttackContext, round: Round) -> BoxFuture<'a, Result<Json, String>> {
        Box::pin(async move {
            let started = Instant::now();
            let (api, token, db) = (&cx.api, &cx.attacker.token, db(round));
            api.create_db(token, &db).await?;
            let n = tables(cx.variant);
            for i in 0..n {
                let table = json!({"name": format!("ev{i}"), "columns": [{"name": "t", "type": "time"},
                    {"name": "u", "type": "string"}, {"name": "e", "type": "string"}], "time_column": "t", "order_by": ["t"]});
                api.create_event_table(token, &db, table).await?;
            }
            // Inserts into one table run one at a time, so that the insert buffer does not join them into one part.
            for tables in (0..n).collect::<Vec<_>>().chunks(SETUP_CONCURRENCY) {
                let tasks: Vec<_> = tables
                    .iter()
                    .map(|&i| {
                        let (api, token, db) = (cx.api.clone(), token.clone(), db.clone());
                        tokio::spawn(async move {
                            let per_part = ROWS_PER_TABLE.div_ceil(PARTS_BEFORE_TRIGGER);
                            for p in 0..PARTS_BEFORE_TRIGGER {
                                let first = i * ROWS_PER_TABLE + p * per_part;
                                let end = (i * ROWS_PER_TABLE + ROWS_PER_TABLE).min(first + per_part);
                                let body = format!("[{}]", (first..end).map(small_event).collect::<Vec<_>>().join(","));
                                api.bulk_insert(&token, &db, &format!("ev{i}"), body).await?;
                            }
                            Ok::<(), String>(())
                        })
                    })
                    .collect();
                for t in tasks {
                    t.await.map_err(|e| e.to_string())??;
                }
            }
            let mut parts = Vec::new();
            for i in 0..n {
                parts.push(api.part_count(token, &db, &format!("ev{i}")).await?);
            }
            Ok(json!({"db": db, "tables": n, "rows_per_table": ROWS_PER_TABLE, "parts_per_table": parts,
                "setup_s": started.elapsed().as_secs_f64()}))
        })
    }

    fn attack<'a>(&'a self, cx: &'a AttackContext, round: Round) -> BoxFuture<'a, Result<AttackOutcome, String>> {
        Box::pin(async move {
            let started = Instant::now();
            let (db, n) = (db(round), tables(cx.variant));
            let triggers = (0..n)
                .map(|i| {
                    let path = format!("/v1/dbs/{db}/events/ev{i}/rows");
                    (format!("trigger ev{i}"), path, Bytes::from(format!("[{}]", small_event(i))))
                })
                .collect();
            let requests = concurrent_posts(cx, triggers, &[201]).await;
            let mut pending: Vec<usize> = (0..n).collect();
            let mut poll_errors = Vec::new();
            while !pending.is_empty() && started.elapsed() < MERGE_TIMEOUT && poll_errors.len() < 3 {
                tokio::time::sleep(Duration::from_millis(250)).await;
                let mut still = Vec::new();
                for &i in &pending {
                    match cx.api.part_count(&cx.attacker.token, &db, &format!("ev{i}")).await {
                        Ok(1) => {}
                        Ok(_) => still.push(i),
                        Err(e) => {
                            still.push(i);
                            poll_errors.push(e);
                        }
                    }
                }
                pending = still;
            }
            Ok(AttackOutcome {
                requests,
                completed: pending.is_empty(),
                notes: json!({"tables": n, "merge_s": started.elapsed().as_secs_f64(), "unmerged_tables": pending, "poll_errors": poll_errors}),
            })
        })
    }
}
