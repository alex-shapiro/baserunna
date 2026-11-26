//! Concurrent stream filter queries that scan all records without matching.
//!
//! In `stream/mod.rs`, `read` decodes records in sequence order until it collects `limit` matches
//! or scans `scan_limit` records.
//! A client can set `scan_limit` up to `SCAN_LIMIT_MAX`, so an unmatched filter forces maximum CPU
//! work per request.
//! The per-user rate limit counts requests instead of scanned records.
//! Concurrent queries cause high node CPU use.

use super::{AttackContext, AttackOutcome, Round, Workload, request_loop};
use baserunna_api::{AppendRecords, AppendRequest, Body, NewRecord, RecordQuery};
use futures_util::future::BoxFuture;
use reqwest::Method;
use serde_json::{Value as Json, json};

/// Record count matching `SCAN_LIMIT_MAX` so each query scans the full stream.
const RECORD_COUNT: usize = 100_000;
/// Records per append batch, sized near 5 MB to fit the 16 MiB transaction write limit.
const RECORDS_PER_APPEND: usize = 20_000;
/// Padding bytes added to each record to match a small JSON event.
const PAD_BYTES: usize = 200;
/// Concurrent attack clients, set to 8 per vCPU to keep all VM cores busy.
const ATTACK_CLIENTS: usize = 32;
/// Sequential queries sent by a single client in the small variant.
const SMALL_QUERIES: usize = 10;
const STREAM_NAME: &str = "events";

pub struct StreamFilterScans;

impl Workload for StreamFilterScans {
    fn prepare<'a>(&'a self, ctx: &'a AttackContext, round: Round) -> BoxFuture<'a, Result<Json, String>> {
        Box::pin(async move {
            if round == Round::WithVictim {
                return Ok(Json::Null);
            }
            let endpoint = AppendRecords { db: ctx.attacker.db.clone(), stream: STREAM_NAME.into() };
            let pad = "p".repeat(PAD_BYTES);
            for batch_start in (0..RECORD_COUNT).step_by(RECORDS_PER_APPEND) {
                let records = (batch_start..(batch_start + RECORDS_PER_APPEND).min(RECORD_COUNT))
                    .map(|i| NewRecord {
                        json: Some(json!({"id": i, "tag": format!("t{}", i % 16), "pad": pad})),
                        ..Default::default()
                    })
                    .collect();
                let append = AppendRequest { records, create: true, ..Default::default() };
                ctx.api.call(&endpoint, &ctx.attacker.token, &append).await?;
            }
            Ok(json!({"records": RECORD_COUNT, "pad_bytes": PAD_BYTES}))
        })
    }

    fn attack<'a>(&'a self, ctx: &'a AttackContext, _round: Round) -> BoxFuture<'a, Result<AttackOutcome, String>> {
        Box::pin(async move {
            let query = RecordQuery {
                from_seq: Some(0),
                scan_limit: Some(RECORD_COUNT),
                filter: Some(json!({"field": "tag", "op": "eq", "value": "unmatched_tag"})),
                ..Default::default()
            };
            let body = query.encode();
            let path = format!("/v1/dbs/{}/streams/{STREAM_NAME}/records/query", ctx.attacker.db);
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
