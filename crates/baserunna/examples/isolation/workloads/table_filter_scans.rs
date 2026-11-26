//! Concurrent table filter queries that scan all rows without matching.
//!
//! In `table/mod.rs`, `table::query` reads rows in key order until it collects `limit` matches or
//! scans `MAX_EXAMINED_PER_QUERY` rows.
//! It also reloads and decodes each scanned row by primary key.
//! The per-user rate limit counts requests, not scanned rows. Concurrent queries therefore drive
//! high node CPU use.

use super::{AttackContext, AttackOutcome, Round, Variant, Workload};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use reqwest::Method;
use serde_json::{Value as Json, json};
use std::time::{Duration, Instant};

/// Row count matching `MAX_EXAMINED_PER_QUERY` and the single-request insert limit.
const ROW_COUNT: usize = 10_000;
/// Padding bytes added to each row to match a small JSON document.
const PAD_BYTES: usize = 200;
/// Concurrent attack clients, set to 8 per vCPU to keep all VM cores busy.
const ATTACK_CLIENTS: usize = 32;
/// Duration of each attack round. Keeps the attack active while victim requests run.
const ATTACK_DURATION: Duration = Duration::from_secs(10);
/// Sequential queries sent by a single client in the small variant.
const SMALL_QUERIES: usize = 10;
const TABLE_NAME: &str = "scanned";

pub struct TableFilterScans;

impl Workload for TableFilterScans {
    fn prepare<'a>(&'a self, ctx: &'a AttackContext, round: Round) -> BoxFuture<'a, Result<Json, String>> {
        Box::pin(async move {
            if round == Round::WithVictim {
                return Ok(json!({"reuses": "the table of the first round"}));
            }
            let db = &ctx.attacker.db;
            let table = json!({"name": TABLE_NAME, "primary_key": ["id"], "columns": [
                {"name": "id", "type": "int", "nullable": false}, {"name": "tag", "type": "string"},
                {"name": "pad", "type": "string"}]});
            ctx.api
                .send_expect(
                    Method::POST,
                    &format!("/v1/dbs/{db}/tables"),
                    &ctx.attacker.token,
                    table.to_string(),
                    &[200, 201],
                )
                .await?;
            let pad = "p".repeat(PAD_BYTES);
            let rows: Vec<Json> =
                (0..ROW_COUNT).map(|i| json!({"id": i, "tag": format!("t{}", i % 16), "pad": pad})).collect();
            let path = format!("/v1/dbs/{db}/tables/{TABLE_NAME}/rows");
            ctx.api
                .send_expect(Method::POST, &path, &ctx.attacker.token, json!({"rows": rows}).to_string(), &[201])
                .await?;
            Ok(json!({"rows": ROW_COUNT, "pad_bytes": PAD_BYTES}))
        })
    }

    fn attack<'a>(&'a self, ctx: &'a AttackContext, _round: Round) -> BoxFuture<'a, Result<AttackOutcome, String>> {
        Box::pin(async move {
            let query = json!({"filter": {"col": "tag", "op": "eq", "value": "unmatched_tag"}});
            let body = Bytes::from(query.to_string());
            let path = format!("/v1/dbs/{}/tables/{TABLE_NAME}/rows/query", ctx.attacker.db);
            let (client_count, deadline, max_queries) = match ctx.variant {
                Variant::Small => (1, None, SMALL_QUERIES),
                Variant::Attack => (ATTACK_CLIENTS, Some(Instant::now() + ATTACK_DURATION), usize::MAX),
            };
            let tasks: Vec<_> = (0..client_count)
                .map(|client_index| {
                    let (api, token, path, body) =
                        (ctx.api.clone(), ctx.attacker.token.clone(), path.clone(), body.clone());
                    tokio::spawn(async move {
                        let mut records = Vec::new();
                        while records.len() < max_queries && deadline.is_none_or(|end| Instant::now() < end) {
                            records.push(
                                api.send_timed(
                                    &format!("query {client_index}"),
                                    Method::POST,
                                    &path,
                                    &token,
                                    body.clone(),
                                    &[200],
                                )
                                .await,
                            );
                        }
                        records
                    })
                })
                .collect();
            let mut requests = Vec::new();
            for task in tasks {
                requests.extend(task.await.map_err(|err| err.to_string())?);
            }
            let completed_queries = requests.iter().filter(|record| record.expected).count();
            let notes =
                json!({"clients": client_count, "queries": requests.len(), "completed_queries": completed_queries});
            Ok(AttackOutcome { requests, completed: true, notes })
        })
    }
}
