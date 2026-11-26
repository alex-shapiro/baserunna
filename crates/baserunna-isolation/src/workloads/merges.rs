//! Concurrent event part merges across 1 or 16 tables of 500,000 rows each.
//!
//! `prepare` creates 7 parts in each table.
//! Each attack round inserts 1 row per table to create an 8th part and start all merges at once.
//! The harness stops the round after the node merges every table to 1 part, or after
//! [`MERGE_TIMEOUT`].

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

pub struct Merges;

impl Workload for Merges {
    fn prepare<'a>(&'a self, ctx: &'a AttackContext, round: Round) -> BoxFuture<'a, Result<Json, String>> {
        Box::pin(async move {
            let started = Instant::now();
            let (api, token, db) = (&ctx.api, &ctx.attacker.token, db_name(round));
            api.create_db(token, &db).await?;
            let table_count = table_count(ctx.variant);
            for table_index in 0..table_count {
                api.create_event_table(token, &db, &format!("ev{table_index}"), "t", &["u", "e"]).await?;
            }
            // Insert sequentially into each table so the node creates separate parts.
            for table_batch in (0..table_count).collect::<Vec<_>>().chunks(SETUP_CONCURRENCY) {
                let tasks: Vec<_> = table_batch
                    .iter()
                    .map(|&table_index| {
                        let (api, token, db) = (ctx.api.clone(), token.clone(), db.clone());
                        tokio::spawn(async move {
                            let rows_per_part = ROWS_PER_TABLE.div_ceil(PARTS_BEFORE_TRIGGER);
                            for part_index in 0..PARTS_BEFORE_TRIGGER {
                                let row_start = table_index * ROWS_PER_TABLE + part_index * rows_per_part;
                                let row_end =
                                    (table_index * ROWS_PER_TABLE + ROWS_PER_TABLE).min(row_start + rows_per_part);
                                let body = format!(
                                    "[{}]",
                                    (row_start..row_end).map(format_event_row).collect::<Vec<_>>().join(",")
                                );
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
            for table_index in 0..table_count {
                parts.push(api.part_count(token, &db, &format!("ev{table_index}")).await?);
            }
            Ok(json!({"db": db, "tables": table_count, "rows_per_table": ROWS_PER_TABLE, "parts_per_table": parts,
                "setup_s": started.elapsed().as_secs_f64()}))
        })
    }

    fn attack<'a>(&'a self, ctx: &'a AttackContext, round: Round) -> BoxFuture<'a, Result<AttackOutcome, String>> {
        Box::pin(async move {
            let started = Instant::now();
            let (db, table_count) = (db_name(round), table_count(ctx.variant));
            let triggers = (0..table_count)
                .map(|table_index| {
                    let path = format!("/v1/dbs/{db}/events/ev{table_index}/rows");
                    (
                        format!("trigger ev{table_index}"),
                        path,
                        Bytes::from(format!("[{}]", format_event_row(table_index))),
                    )
                })
                .collect();
            let requests = concurrent_posts(ctx, triggers, &[201]).await;
            let mut unmerged_tables: Vec<usize> = (0..table_count).collect();
            let mut poll_errors = Vec::new();
            while !unmerged_tables.is_empty() && started.elapsed() < MERGE_TIMEOUT && poll_errors.len() < 3 {
                tokio::time::sleep(Duration::from_millis(250)).await;
                let mut still_unmerged = Vec::new();
                for &table_index in &unmerged_tables {
                    match ctx.api.part_count(&ctx.attacker.token, &db, &format!("ev{table_index}")).await {
                        Ok(1) => {}
                        Ok(_) => still_unmerged.push(table_index),
                        Err(err) => {
                            still_unmerged.push(table_index);
                            poll_errors.push(err);
                        }
                    }
                }
                unmerged_tables = still_unmerged;
            }
            Ok(AttackOutcome {
                requests,
                completed: unmerged_tables.is_empty(),
                notes: json!({"tables": table_count, "merge_s": started.elapsed().as_secs_f64(), "unmerged_tables": unmerged_tables, "poll_errors": poll_errors}),
            })
        })
    }
}

fn table_count(variant: Variant) -> usize {
    match variant {
        Variant::Small => 1,
        Variant::Attack => 16,
    }
}

fn db_name(round: Round) -> String {
    format!("merges{}", round.index())
}

fn format_event_row(row_index: usize) -> String {
    let time = MERGE_DAY_MS + (row_index as i64 * 7_919) % DAY_MS;
    format!(r#"{{"t":{time},"u":"u{:07}","e":"e{}"}}"#, row_index % 100_000, row_index % 8)
}
