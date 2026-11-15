//! Table queries whose filter matches no row, sent by many concurrent clients.
//!
//! A table query reads rows in key order and tests each against its filter until it has `limit`
//! matches or has examined `MAX_EXAMINED_PER_QUERY` rows (`table/mod.rs`, `table::query`). For
//! each examined row it also loads and decodes the row again by its primary key. So a query that
//! matches nothing does the most work that one query may do, and the node's CPU time grows with
//! the number of such queries in flight. The per-user rate limit counts requests, not this cost.

use super::{AttackContext, AttackOutcome, Round, Variant, Workload};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use reqwest::Method;
use serde_json::{Value as Json, json};
use std::time::{Duration, Instant};

/// Rows in the table: `MAX_EXAMINED_PER_QUERY`, so each query examines every row. One insert
/// request may write at most this many rows.
const ROWS: usize = 10_000;
/// Bytes of each row's text column, so that a row has the size of a small JSON document.
const PAD_BYTES: usize = 200;
/// Concurrent query clients in the attack: 8 per vCPU of the VM, so that the node always has
/// more queries in flight than CPUs, whatever the cost of one query.
const ATTACK_CLIENTS: usize = 32;
/// Duration of the attack in each round: long enough for many victim requests to meet it.
const ATTACK_DURATION: Duration = Duration::from_secs(10);
/// Queries in the small variant: one client sends them one after another, as an application
/// that pages through a table would.
const SMALL_QUERIES: usize = 10;
const TABLE: &str = "scanned";

pub struct TableFilterScans;

impl Workload for TableFilterScans {
    fn prepare<'a>(&'a self, cx: &'a AttackContext, round: Round) -> BoxFuture<'a, Result<Json, String>> {
        Box::pin(async move {
            if round == Round::WithVictim {
                return Ok(json!({"reuses": "the table of the first round"}));
            }
            let db = &cx.attacker.db;
            let table = json!({"name": TABLE, "primary_key": ["id"], "columns": [
                {"name": "id", "type": "int", "nullable": false}, {"name": "tag", "type": "string"},
                {"name": "pad", "type": "string"}]});
            cx.api.expect(Method::POST, &format!("/v1/dbs/{db}/tables"), &cx.attacker.token, table.to_string(), &[200, 201]).await?;
            let pad = "p".repeat(PAD_BYTES);
            let rows: Vec<Json> = (0..ROWS).map(|i| json!({"id": i, "tag": format!("t{}", i % 16), "pad": pad})).collect();
            let path = format!("/v1/dbs/{db}/tables/{TABLE}/rows");
            cx.api.expect(Method::POST, &path, &cx.attacker.token, json!({"rows": rows}).to_string(), &[201]).await?;
            Ok(json!({"rows": ROWS, "pad_bytes": PAD_BYTES}))
        })
    }

    fn attack<'a>(&'a self, cx: &'a AttackContext, _round: Round) -> BoxFuture<'a, Result<AttackOutcome, String>> {
        Box::pin(async move {
            let query = json!({"filter": {"col": "tag", "op": "eq", "value": "no such tag"}});
            let body = Bytes::from(query.to_string());
            let path = format!("/v1/dbs/{}/tables/{TABLE}/rows/query", cx.attacker.db);
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
