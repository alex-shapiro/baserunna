//! Concurrent table filter queries that scan all rows without matching.
//!
//! In `table/mod.rs`, `table::query` reads rows in key order until it collects `limit` matches or
//! scans `MAX_EXAMINED_PER_QUERY` rows.
//! It also reloads and decodes each scanned row by primary key.
//! The per-user rate limit counts requests instead of scanned rows.
//! Concurrent queries cause high node CPU use.

use super::{AttackContext, AttackOutcome, Round, Workload, request_loop};
use baserunna_api::{ColumnType, CreateTable, NewColumn, NewTable};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use reqwest::Method;
use serde_json::{Value as Json, json};

/// Row count matching `MAX_EXAMINED_PER_QUERY` and the single-request insert limit.
const ROW_COUNT: usize = 10_000;
/// Padding bytes added to each row to match a small JSON document.
const PAD_BYTES: usize = 200;
/// Concurrent attack clients, set to 8 per vCPU to keep all VM cores busy.
const ATTACK_CLIENTS: usize = 32;
/// Sequential queries sent by a single client in the small variant.
const SMALL_QUERIES: usize = 10;
const TABLE_NAME: &str = "scanned";

pub struct TableFilterScans;

impl Workload for TableFilterScans {
    fn prepare<'a>(&'a self, ctx: &'a AttackContext, round: Round) -> BoxFuture<'a, Result<Json, String>> {
        Box::pin(async move {
            if round == Round::WithVictim {
                return Ok(Json::Null);
            }
            let db = &ctx.attacker.db;
            let table = NewTable {
                name: TABLE_NAME.into(),
                columns: vec![
                    NewColumn::new("id", ColumnType::Int),
                    NewColumn::nullable("tag", ColumnType::String),
                    NewColumn::nullable("pad", ColumnType::String),
                ],
                primary_key: vec!["id".into()],
                indexes: Vec::new(),
                if_not_exists: false,
            };
            ctx.api.call(&CreateTable { db: db.clone() }, &ctx.attacker.token, &table).await?;
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
            let body = Bytes::from(json!({"filter": {"col": "tag", "op": "eq", "value": "unmatched_tag"}}).to_string());
            let path = format!("/v1/dbs/{}/tables/{TABLE_NAME}/rows/query", ctx.attacker.db);
            let requests = request_loop(ctx, SMALL_QUERIES, ATTACK_CLIENTS, move |api, token, client_index| {
                let (path, body) = (path.clone(), body.clone());
                async move {
                    api.send_timed(&format!("query {client_index}"), Method::POST, &path, &token, body, &[200]).await
                }
            })
            .await?;
            let completed_queries = requests.iter().filter(|record| record.expected).count();
            let notes = json!({"queries": requests.len(), "completed_queries": completed_queries});
            Ok(AttackOutcome { requests, completed: true, notes })
        })
    }
}
