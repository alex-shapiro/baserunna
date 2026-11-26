//! One-shot transactions that repeat a full scan page of maximum-size inline values.
//!
//! A `scan` op returns up to `PAGE_MAX` items with their values (`ops.rs`, `Op::Scan` in
//! `exec_one`). The transaction charges its read set only for the scanned range, not for the
//! values. `exec_ops` keeps every op result until the transaction ends, and the handler then
//! serializes all of them into one response body. So the node memory of one request grows with
//! the number of scan ops, and a transaction may have up to `MAX_TXN_OPS` ops.

use super::{AttackContext, AttackOutcome, Round, Variant, Workload};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use reqwest::Method;
use serde_json::{Value as Json, json};

/// Keys under the scanned prefix: `PAGE_MAX`, the largest page that one scan op returns.
const KEYS: usize = 1000;
/// Bytes of each value: `value_inline_max`, the largest value that the node stores inline.
/// One scan op then returns 32 MiB of values.
const VALUE_BYTES: usize = 32 << 10;
/// Puts per setup transaction: 8 MiB of writes, half of the 16 MiB transaction write limit.
const PUTS_PER_TXN: usize = 256;
/// Scan ops in one attack transaction: their values sum to 512 MiB, the tenant memory bound.
const ATTACK_SCANS: usize = 16;
/// Concurrent attack transactions, as in the other workloads that send concurrent requests.
const ATTACK_REQUESTS: usize = 4;
/// Scan ops in the small transaction: one full page, which is normal use of the API.
const SMALL_SCANS: usize = 1;
const PREFIX: &str = "scan/";

pub struct TxnScans;

impl Workload for TxnScans {
    fn prepare<'a>(&'a self, cx: &'a AttackContext, round: Round) -> BoxFuture<'a, Result<Json, String>> {
        Box::pin(async move {
            if round == Round::WithVictim {
                return Ok(json!({"reuses": "the keys of the first round"}));
            }
            let value = "x".repeat(VALUE_BYTES);
            let path = format!("/v1/dbs/{}/txn", cx.attacker.db);
            for first in (0..KEYS).step_by(PUTS_PER_TXN) {
                let ops: Vec<Json> = (first..(first + PUTS_PER_TXN).min(KEYS))
                    .map(|i| json!({"op": "put", "key": format!("{PREFIX}{i:06}"), "value": value}))
                    .collect();
                cx.api.expect(Method::POST, &path, &cx.attacker.token, json!({"ops": ops}).to_string(), &[200]).await?;
            }
            Ok(json!({"keys": KEYS, "value_bytes": VALUE_BYTES}))
        })
    }

    fn attack<'a>(&'a self, cx: &'a AttackContext, _round: Round) -> BoxFuture<'a, Result<AttackOutcome, String>> {
        Box::pin(async move {
            let (requests, scans) = match cx.variant {
                Variant::Small => (1, SMALL_SCANS),
                Variant::Attack => (ATTACK_REQUESTS, ATTACK_SCANS),
            };
            let scan = json!({"op": "scan", "prefix": PREFIX, "limit": KEYS});
            let body = Bytes::from(json!({"ops": vec![scan; scans]}).to_string());
            let path = format!("/v1/dbs/{}/txn", cx.attacker.db);
            let tasks: Vec<_> = (0..requests)
                .map(|i| {
                    let (api, token, path, body) =
                        (cx.api.clone(), cx.attacker.token.clone(), path.clone(), body.clone());
                    tokio::spawn(async move {
                        api.timed_drain(&format!("scans {i}"), Method::POST, &path, &token, body, &[200]).await
                    })
                })
                .collect();
            let (mut records, mut response_bytes) = (Vec::new(), Vec::new());
            for t in tasks {
                let (r, n) = t.await.map_err(|e| e.to_string())?;
                records.push(r);
                response_bytes.push(n);
            }
            let notes = json!({"concurrent_txns": requests, "scans_per_txn": scans, "response_bytes": response_bytes});
            Ok(AttackOutcome { requests: records, completed: true, notes })
        })
    }
}
