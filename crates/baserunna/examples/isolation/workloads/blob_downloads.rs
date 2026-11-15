//! Concurrent full downloads of one large KV value that the node stores as a blob.
//!
//! A `GET /kv/{key}` of a blob value streams the blob chunk by chunk (`api/body.rs`, `BlobBody`).
//! For each chunk the node sends one store `GET` and holds the whole chunk in memory until the
//! client has read it (`engine/blob.rs`, `read_chunk`). No memory charge or concurrency limit
//! applies, and the per-user rate limit counts requests, not bytes in flight. So node memory grows
//! with the number of downloads in flight, at one `blob_chunk_size` chunk each.

use super::{AttackContext, AttackOutcome, Round, Variant, Workload};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use reqwest::Method;
use serde_json::{Value as Json, json};
use std::time::{Duration, Instant};

/// Bytes of the blob: 3 chunks of the 8 MiB `blob_chunk_size`, so that each download sends more
/// than one chunk, and below the 32 MiB `json_body_max` so that one `PUT` stores it.
const BLOB_BYTES: usize = 24 << 20;
/// Concurrent download clients in the attack. With one 8 MiB chunk each, they hold 1 GiB, twice
/// the 512 MiB tenant memory bound.
const ATTACK_CLIENTS: usize = 128;
/// Duration of the attack in each round: long enough for many victim requests to meet it.
const ATTACK_DURATION: Duration = Duration::from_secs(10);
/// Downloads in the small variant: one client sends them one after another, as an application
/// that serves one file would.
const SMALL_DOWNLOADS: usize = 4;
const KEY: &str = "files/large.bin";

pub struct BlobDownloads;

impl Workload for BlobDownloads {
    fn prepare<'a>(&'a self, cx: &'a AttackContext, round: Round) -> BoxFuture<'a, Result<Json, String>> {
        Box::pin(async move {
            if round == Round::WithVictim {
                return Ok(json!({"reuses": "the blob of the first round"}));
            }
            let path = format!("/v1/dbs/{}/kv/{KEY}", cx.attacker.db);
            let body = Bytes::from(vec![b'b'; BLOB_BYTES]);
            cx.api.expect(Method::PUT, &path, &cx.attacker.token, body, &[200]).await?;
            Ok(json!({"blob_bytes": BLOB_BYTES}))
        })
    }

    fn attack<'a>(&'a self, cx: &'a AttackContext, _round: Round) -> BoxFuture<'a, Result<AttackOutcome, String>> {
        Box::pin(async move {
            let path = format!("/v1/dbs/{}/kv/{KEY}", cx.attacker.db);
            let (clients, deadline, max_downloads) = match cx.variant {
                Variant::Small => (1, None, SMALL_DOWNLOADS),
                Variant::Attack => (ATTACK_CLIENTS, Some(Instant::now() + ATTACK_DURATION), usize::MAX),
            };
            let tasks: Vec<_> = (0..clients)
                .map(|c| {
                    let (api, token, path) = (cx.api.clone(), cx.attacker.token.clone(), path.clone());
                    tokio::spawn(async move {
                        let (mut records, mut short) = (Vec::new(), 0usize);
                        while records.len() < max_downloads && deadline.is_none_or(|d| Instant::now() < d) {
                            let (mut r, n) =
                                api.timed_drain(&format!("download {c}"), Method::GET, &path, &token, Bytes::new(), &[200]).await;
                            if r.expected && n != BLOB_BYTES as u64 {
                                r.expected = false;
                                r.error = Some(format!("short download: {n} of {BLOB_BYTES} bytes"));
                                short += 1;
                            }
                            let dropped = r.status.is_none();
                            records.push(r);
                            if dropped {
                                break;
                            }
                        }
                        (records, short)
                    })
                })
                .collect();
            let (mut requests, mut short) = (Vec::new(), 0);
            for t in tasks {
                let (r, s) = t.await.map_err(|e| e.to_string())?;
                requests.extend(r);
                short += s;
            }
            let ok = requests.iter().filter(|r| r.expected).count();
            let notes = json!({"clients": clients, "downloads": requests.len(), "completed_downloads": ok, "short_downloads": short});
            Ok(AttackOutcome { requests, completed: true, notes })
        })
    }
}
