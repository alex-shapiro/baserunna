//! Value checks on a 256 MiB blob. Each check compares the whole blob with a short value and
//! fails with 412.

use super::{AttackContext, AttackOutcome, Round, Variant, Workload, concurrent_posts};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use reqwest::Method;
use serde_json::{Value as Json, json};

const BLOB_BYTES: usize = 256 << 20;

pub struct BlobChecks;

impl Workload for BlobChecks {
    fn prepare<'a>(&'a self, cx: &'a AttackContext, round: Round) -> BoxFuture<'a, Result<Json, String>> {
        Box::pin(async move {
            if round == Round::WithVictim {
                return Ok(json!({"reuses": "the blob of the first round"}));
            }
            let started = std::time::Instant::now();
            let path = format!("/v1/dbs/{}/kv/big", cx.attacker.db);
            cx.api.expect(Method::PUT, &path, &cx.attacker.token, vec![b'x'; BLOB_BYTES], &[200, 201, 204]).await?;
            Ok(json!({"blob_bytes": BLOB_BYTES, "upload_s": started.elapsed().as_secs_f64()}))
        })
    }

    fn attack<'a>(&'a self, cx: &'a AttackContext, _round: Round) -> BoxFuture<'a, Result<AttackOutcome, String>> {
        Box::pin(async move {
            let n = match cx.variant {
                Variant::Small => 1,
                Variant::Attack => 4,
            };
            let check = Bytes::from(json!({"ops": [{"op": "check", "key": "big", "value": "not the blob"}]}).to_string());
            let path = format!("/v1/dbs/{}/txn", cx.attacker.db);
            let requests = (0..n).map(|i| (format!("check {i}"), path.clone(), check.clone())).collect();
            let requests = concurrent_posts(cx, requests, &[412]).await;
            Ok(AttackOutcome { completed: true, requests, notes: json!({"concurrent_checks": n}) })
        })
    }
}
