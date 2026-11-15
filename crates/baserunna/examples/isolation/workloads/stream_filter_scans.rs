//! Stream record queries whose filter matches no record, sent by many concurrent clients.
//!
//! A stream query reads records in sequence order. It decodes each record's body and tests it
//! against the filter, until it has `limit` matches or has examined `scan_limit` records
//! (`stream/mod.rs`, `read`). A client may set `scan_limit` up to `SCAN_LIMIT_MAX`. So a query
//! that matches nothing does the most work that one query may do, and the node's CPU time grows
//! with the number of such queries in flight. The per-user rate limit counts requests, not this
//! cost.

use super::{AttackContext, AttackOutcome, Round, Variant, Workload};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use reqwest::Method;
use serde_json::{Value as Json, json};
use std::time::{Duration, Instant};

/// Records in the stream: `SCAN_LIMIT_MAX`, so that each query examines every record.
const RECORDS: usize = 100_000;
/// Records per append: 20,000 records of about 250 bytes are 5 MB, inside the 16 MiB
/// transaction write limit that one append must fit.
const RECORDS_PER_APPEND: usize = 20_000;
/// Bytes of each record's text field, so that a record has the size of a small JSON event.
const PAD_BYTES: usize = 200;
/// Concurrent query clients in the attack: 8 per vCPU of the VM, so that the node always has
/// more queries in flight than CPUs, whatever the cost of one query.
const ATTACK_CLIENTS: usize = 32;
/// Duration of the attack in each round: long enough for many victim requests to meet it.
const ATTACK_DURATION: Duration = Duration::from_secs(10);
/// Queries in the small variant: one client sends them one after another, as an application
/// that searches a log would.
const SMALL_QUERIES: usize = 10;
const STREAM: &str = "events";

pub struct StreamFilterScans;

impl Workload for StreamFilterScans {
    fn prepare<'a>(&'a self, cx: &'a AttackContext, round: Round) -> BoxFuture<'a, Result<Json, String>> {
        Box::pin(async move {
            if round == Round::WithVictim {
                return Ok(json!({"reuses": "the stream of the first round"}));
            }
            let path = format!("/v1/dbs/{}/streams/{STREAM}/records", cx.attacker.db);
            let pad = "p".repeat(PAD_BYTES);
            for first in (0..RECORDS).step_by(RECORDS_PER_APPEND) {
                let records: Vec<Json> = (first..(first + RECORDS_PER_APPEND).min(RECORDS))
                    .map(|i| json!({"json": {"id": i, "tag": format!("t{}", i % 16), "pad": pad}}))
                    .collect();
                let body = json!({"records": records, "create": true}).to_string();
                cx.api.expect(Method::POST, &path, &cx.attacker.token, body, &[200, 201]).await?;
            }
            Ok(json!({"records": RECORDS, "pad_bytes": PAD_BYTES}))
        })
    }

    fn attack<'a>(&'a self, cx: &'a AttackContext, _round: Round) -> BoxFuture<'a, Result<AttackOutcome, String>> {
        Box::pin(async move {
            let query = json!({"from_seq": 0, "scan_limit": RECORDS,
                "filter": {"field": "tag", "op": "eq", "value": "no such tag"}});
            let body = Bytes::from(query.to_string());
            let path = format!("/v1/dbs/{}/streams/{STREAM}/records/query", cx.attacker.db);
            let (clients, deadline, max_queries) = match cx.variant {
                Variant::Small => (1, None, SMALL_QUERIES),
                Variant::Attack => (ATTACK_CLIENTS, Some(Instant::now() + ATTACK_DURATION), usize::MAX),
            };
            let tasks: Vec<_> = (0..clients)
                .map(|c| {
                    let (api, token, path, body) = (cx.api.clone(), cx.attacker.token.clone(), path.clone(), body.clone());
                    tokio::spawn(async move {
                        let mut out = Vec::new();
                        while out.len() < max_queries && deadline.is_none_or(|d| Instant::now() < d) {
                            out.push(api.timed(&format!("query {c}"), Method::POST, &path, &token, body.clone(), &[200]).await);
                        }
                        out
                    })
                })
                .collect();
            let mut requests = Vec::new();
            for t in tasks {
                requests.extend(t.await.map_err(|e| e.to_string())?);
            }
            let ok = requests.iter().filter(|r| r.expected).count();
            let notes = json!({"clients": clients, "queries": requests.len(), "completed_queries": ok});
            Ok(AttackOutcome { requests, completed: true, notes })
        })
    }
}
