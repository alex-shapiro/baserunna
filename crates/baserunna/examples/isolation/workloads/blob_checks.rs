//! Concurrent value checks against a 256 MiB blob.
//!
//! Each check compares the full blob against a short string and expects HTTP 412.

use super::{AttackContext, AttackOutcome, Round, Variant, Workload, concurrent_posts};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use reqwest::Method;
use serde_json::{Value as Json, json};

const BLOB_BYTES: usize = 256 << 20;

pub struct BlobChecks;

impl Workload for BlobChecks {
    fn prepare<'a>(&'a self, ctx: &'a AttackContext, round: Round) -> BoxFuture<'a, Result<Json, String>> {
        Box::pin(async move {
            if round == Round::WithVictim {
                return Ok(json!({"reuses": "the blob of the first round"}));
            }
            let started = std::time::Instant::now();
            let path = format!("/v1/dbs/{}/kv/big", ctx.attacker.db);
            ctx.api.send_expect(Method::PUT, &path, &ctx.attacker.token, vec![b'x'; BLOB_BYTES], &[200, 201, 204]).await?;
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
                Bytes::from(json!({"ops": [{"op": "check", "key": "big", "value": "not the blob"}]}).to_string());
            let path = format!("/v1/dbs/{}/txn", ctx.attacker.db);
            let requests = (0..check_count).map(|i| (format!("check {i}"), path.clone(), check.clone())).collect();
            let requests = concurrent_posts(ctx, requests, &[412]).await;
            Ok(AttackOutcome { completed: true, requests, notes: json!({"concurrent_checks": check_count}) })
        })
    }
}
