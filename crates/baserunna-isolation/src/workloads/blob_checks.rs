//! Concurrent value checks against a 256 MiB blob.
//!
//! Each check compares the full blob against a short string. The harness expects HTTP 412.

use super::{AttackContext, AttackOutcome, Round, Variant, Workload, concurrent_posts};
use baserunna_api::{Body, Op, TxnRequest};
use futures_util::future::BoxFuture;
use serde_json::{Value as Json, json};

const BLOB_BYTES: usize = 256 << 20;

pub struct BlobChecks;

impl Workload for BlobChecks {
    fn prepare<'a>(&'a self, ctx: &'a AttackContext, round: Round) -> BoxFuture<'a, Result<Json, String>> {
        Box::pin(async move {
            if round == Round::WithVictim {
                return Ok(Json::Null);
            }
            let started = std::time::Instant::now();
            ctx.api.put_value(&ctx.attacker.token, &ctx.attacker.db, "big", vec![b'x'; BLOB_BYTES]).await?;
            Ok(json!({"blob_bytes": BLOB_BYTES, "upload_s": started.elapsed().as_secs_f64()}))
        })
    }

    fn attack<'a>(&'a self, ctx: &'a AttackContext, _round: Round) -> BoxFuture<'a, Result<AttackOutcome, String>> {
        Box::pin(async move {
            let check_count = match ctx.variant {
                Variant::Small => 1,
                Variant::Attack => 4,
            };
            let check =
                Op::Check { key: "big".into(), exists: None, version: None, value: Some("not the blob".into()) };
            let request_body = TxnRequest { ops: vec![check], ..Default::default() }.encode();
            let path = format!("/v1/dbs/{}/txn", ctx.attacker.db);
            let batch = (0..check_count).map(|i| (format!("check {i}"), path.clone(), request_body.clone())).collect();
            let requests = concurrent_posts(ctx, batch, &[412]).await;
            Ok(AttackOutcome { completed: true, requests, notes: json!({"concurrent_checks": check_count}) })
        })
    }
}
