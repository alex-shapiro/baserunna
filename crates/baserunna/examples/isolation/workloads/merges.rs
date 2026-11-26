//! Concurrent event part merges across 1 or 16 tables of 500,000 rows each.
//!
//! Setup populates 7 parts in each table.
//! The attack inserts a single row per table to create an 8th part and trigger all merges together.
//! The round ends once the node merges every table to 1 part, or after [`MERGE_TIMEOUT`].

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
/// Maximum tables populated concurrently during setup.
const SETUP_CONCURRENCY: usize = 4;

fn table_count(variant: Variant) -> usize {
    match variant {
        Variant::Small => 1,
        Variant::Attack => 16,
    }
}

fn db_name(round: Round) -> String {
    format!("merges{}", round.index())
}

fn small_event(i: usize) -> String {
    let time = MERGE_DAY_MS + (i as i64 * 7_919) % DAY_MS;
    format!(r#"{{"t":{time},"u":"u{:07}","e":"e{}"}}"#, i % 100_000, i % 8)
}

pub struct Merges;

impl Workload for Merges {
    fn prepare<'a>(&'a self, ctx: &'a AttackContext, round: Round) -> BoxFuture<'a, Result<Json, String>> {
        Box::pin(async move {
            let started = Instant::now();
            let (api, token, db) = (&ctx.api, &ctx.attacker.token, db_name(round));
            api.create_db(token, &db).await?;
            let count = table_count(ctx.variant);
            for i in 0..count {
                let table = json!({"name": format!("ev{i}"), "columns": [{"name": "t", "type": "time"},
                    {"name": "u", "type": "string"}, {"name": "e", "type": "string"}], "time_column": "t", "order_by": ["t"]});
                api.create_event_table(token, &db, table).await?;
            }
            // Insert sequentially into each table so the node creates separate parts.
            for chunk in (0..count).collect::<Vec<_>>().chunks(SETUP_CONCURRENCY) {
                let tasks: Vec<_> = chunk
                    .iter()
                    .map(|&table_index| {
                        let (api, token, db) = (ctx.api.clone(), token.clone(), db.clone());
                        tokio::spawn(async move {
                            let per_part = ROWS_PER_TABLE.div_ceil(PARTS_BEFORE_TRIGGER);
                            for part_index in 0..PARTS_BEFORE_TRIGGER {
                                let first = table_index * ROWS_PER_TABLE + part_index * per_part;
                                let end = (table_index * ROWS_PER_TABLE + ROWS_PER_TABLE).min(first + per_part);
                                let body = format!("[{}]", (first..end).map(small_event).collect::<Vec<_>>().join(","));
                                api.bulk_insert(&token, &db, &format!("ev{table_index}"), body).await?;
                            }
                            Ok::<(), String>(())
                        })
                    })
                    .collect();
                for task in tasks {
                    task.await.map_err(|err| err.to_string())??;
                }
            }
            let mut parts = Vec::new();
            for i in 0..count {
                parts.push(api.part_count(token, &db, &format!("ev{i}")).await?);
            }
            Ok(json!({"db": db, "tables": count, "rows_per_table": ROWS_PER_TABLE, "parts_per_table": parts,
                "setup_s": started.elapsed().as_secs_f64()}))
        })
    }

    fn attack<'a>(&'a self, ctx: &'a AttackContext, round: Round) -> BoxFuture<'a, Result<AttackOutcome, String>> {
        Box::pin(async move {
            let started = Instant::now();
            let (db, count) = (db_name(round), table_count(ctx.variant));
            let triggers = (0..count)
                .map(|i| {
                    let path = format!("/v1/dbs/{db}/events/ev{i}/rows");
                    (format!("trigger ev{i}"), path, Bytes::from(format!("[{}]", small_event(i))))
                })
                .collect();
            let requests = concurrent_posts(ctx, triggers, &[201]).await;
            let mut pending: Vec<usize> = (0..count).collect();
            let mut poll_errors = Vec::new();
            while !pending.is_empty() && started.elapsed() < MERGE_TIMEOUT && poll_errors.len() < 3 {
                tokio::time::sleep(Duration::from_millis(250)).await;
                let mut remaining = Vec::new();
                for &table_index in &pending {
                    match ctx.api.part_count(&ctx.attacker.token, &db, &format!("ev{table_index}")).await {
                        Ok(1) => {}
                        Ok(_) => remaining.push(table_index),
                        Err(err) => {
                            remaining.push(table_index);
                            poll_errors.push(err);
                        }
                    }
                }
                pending = remaining;
            }
            Ok(AttackOutcome {
                requests,
                completed: pending.is_empty(),
                notes: json!({"tables": count, "merge_s": started.elapsed().as_secs_f64(), "unmerged_tables": pending, "poll_errors": poll_errors}),
            })
        })
    }
}
