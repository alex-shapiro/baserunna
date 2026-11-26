//! Concurrent stream filter queries that scan all records without matching.
//!
//! In `stream/mod.rs`, `read` decodes records in sequence order until it collects `limit` matches
//! or scans `scan_limit` records.
//! A client can set `scan_limit` up to `SCAN_LIMIT_MAX`, so an unmatched filter forces maximum CPU
//! work per request.
//! Because the per-user rate limit counts requests rather than scanned records, concurrent queries
//! consume proportional node CPU.

use super::{AttackContext, AttackOutcome, Round, Variant, Workload};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use reqwest::Method;
use serde_json::{Value as Json, json};
use std::time::{Duration, Instant};

/// Record count matching `SCAN_LIMIT_MAX` so each query scans the full stream.
const RECORDS: usize = 100_000;
/// Records per append batch, sized near 5 MB to fit the 16 MiB transaction write limit.
const RECORDS_PER_APPEND: usize = 20_000;
/// Padding bytes added to each record to match a small JSON event.
const PAD_BYTES: usize = 200;
/// Concurrent attack clients, set to 8 per vCPU to keep all VM cores busy.
const ATTACK_CLIENTS: usize = 32;
/// Duration of each attack round so victim requests overlap with the load.
const ATTACK_DURATION: Duration = Duration::from_secs(10);
/// Sequential queries sent by a single client in the small variant.
const SMALL_QUERIES: usize = 10;
const STREAM: &str = "events";

pub struct StreamFilterScans;

impl Workload for StreamFilterScans {
    fn prepare<'a>(&'a self, ctx: &'a AttackContext, round: Round) -> BoxFuture<'a, Result<Json, String>> {
        Box::pin(async move {
            if round == Round::WithVictim {
                return Ok(json!({"reuses": "the stream of the first round"}));
            }
            let path = format!("/v1/dbs/{}/streams/{STREAM}/records", ctx.attacker.db);
            let pad = "p".repeat(PAD_BYTES);
            for first in (0..RECORDS).step_by(RECORDS_PER_APPEND) {
                let records: Vec<Json> = (first..(first + RECORDS_PER_APPEND).min(RECORDS))
                    .map(|i| json!({"json": {"id": i, "tag": format!("t{}", i % 16), "pad": pad}}))
                    .collect();
                let body = json!({"records": records, "create": true}).to_string();
                ctx.api.send_expect(Method::POST, &path, &ctx.attacker.token, body, &[200, 201]).await?;
            }
            Ok(json!({"records": RECORDS, "pad_bytes": PAD_BYTES}))
        })
    }

    fn attack<'a>(&'a self, ctx: &'a AttackContext, _round: Round) -> BoxFuture<'a, Result<AttackOutcome, String>> {
        Box::pin(async move {
            let query = json!({"from_seq": 0, "scan_limit": RECORDS,
                "filter": {"field": "tag", "op": "eq", "value": "no such tag"}});
            let body = Bytes::from(query.to_string());
            let path = format!("/v1/dbs/{}/streams/{STREAM}/records/query", ctx.attacker.db);
            let (clients, deadline, max_queries) = match ctx.variant {
                Variant::Small => (1, None, SMALL_QUERIES),
                Variant::Attack => (ATTACK_CLIENTS, Some(Instant::now() + ATTACK_DURATION), usize::MAX),
            };
            let tasks: Vec<_> = (0..clients)
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
            let notes = json!({"clients": clients, "queries": requests.len(), "completed_queries": completed_queries});
            Ok(AttackOutcome { requests, completed: true, notes })
        })
    }
}
