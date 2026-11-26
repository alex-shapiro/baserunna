//! Single-request transactions that repeat full scan pages of maximum-size inline values.
//!
//! `Op::Scan` in `ops.rs` returns up to `PAGE_MAX` key-value pairs per operation.
//! The transaction charges only the scanned range to its read set. It does not charge the returned
//! values.
//! `exec_ops` holds all operation results in memory until the handler serializes them into a single
//! HTTP response.
//! Repeating scan operations up to `MAX_TXN_OPS` multiplies per-request node memory use.

use super::{AttackContext, AttackOutcome, Round, Variant, Workload};
use baserunna_api::{Body, Op, Txn, TxnRequest};
use futures_util::future::BoxFuture;
use reqwest::Method;
use serde_json::{Value as Json, json};

/// Key count matching `PAGE_MAX`, the maximum page size of a scan operation.
const KEY_COUNT: usize = 1000;
/// Value size matching `value_inline_max`, so a full scan returns 32 MiB of values.
const VALUE_BYTES: usize = 32 << 10;
/// Puts per setup transaction, totaling 8 MiB within the 16 MiB transaction write limit.
const PUTS_PER_TXN: usize = 256;
/// Scan operations per attack transaction, totaling 512 MiB of response values.
const ATTACK_SCANS: usize = 16;
/// Concurrent attack transactions sent in each round.
const ATTACK_REQUESTS: usize = 4;
/// Scan operations per small transaction, returning a single full page.
const SMALL_SCANS: usize = 1;
const KEY_PREFIX: &str = "scan/";

pub struct TxnScans;

impl Workload for TxnScans {
    fn prepare<'a>(&'a self, ctx: &'a AttackContext, round: Round) -> BoxFuture<'a, Result<Json, String>> {
        Box::pin(async move {
            if round == Round::WithVictim {
                return Ok(Json::Null);
            }
            let value = "x".repeat(VALUE_BYTES);
            let endpoint = Txn { db: ctx.attacker.db.clone() };
            for batch_start in (0..KEY_COUNT).step_by(PUTS_PER_TXN) {
                let ops = (batch_start..(batch_start + PUTS_PER_TXN).min(KEY_COUNT))
                    .map(|i| Op::Put {
                        key: format!("{KEY_PREFIX}{i:06}"),
                        value: Some(value.clone()),
                        blob_id: None,
                        ttl_ms: None,
                    })
                    .collect();
                ctx.api.call(&endpoint, &ctx.attacker.token, &TxnRequest { ops, ..Default::default() }).await?;
            }
            Ok(json!({"keys": KEY_COUNT, "value_bytes": VALUE_BYTES}))
        })
    }

    fn attack<'a>(&'a self, ctx: &'a AttackContext, _round: Round) -> BoxFuture<'a, Result<AttackOutcome, String>> {
        Box::pin(async move {
            let (txn_count, scans_per_txn) = match ctx.variant {
                Variant::Small => (1, SMALL_SCANS),
                Variant::Attack => (ATTACK_REQUESTS, ATTACK_SCANS),
            };
            let scan = Op::Scan {
                prefix: Some(KEY_PREFIX.into()),
                start: None,
                end: None,
                limit: Some(KEY_COUNT),
                reverse: false,
                keys_only: false,
                cursor: None,
            };
            let body = TxnRequest { ops: vec![scan; scans_per_txn], ..Default::default() }.encode();
            let path = format!("/v1/dbs/{}/txn", ctx.attacker.db);
            let tasks: Vec<_> = (0..txn_count)
                .map(|i| {
                    let (api, token, path, body) =
                        (ctx.api.clone(), ctx.attacker.token.clone(), path.clone(), body.clone());
                    tokio::spawn(async move {
                        api.send_timed_drain(&format!("scans {i}"), Method::POST, &path, &token, body, &[200]).await
                    })
                })
                .collect();
            let (mut records, mut response_bytes) = (Vec::new(), Vec::new());
            for task in tasks {
                let (record, byte_count) = task.await.map_err(|err| err.to_string())?;
                records.push(record);
                response_bytes.push(byte_count);
            }
            let notes =
                json!({"concurrent_txns": txn_count, "scans_per_txn": scans_per_txn, "response_bytes": response_bytes});
            Ok(AttackOutcome { requests: records, completed: true, notes })
        })
    }
}
