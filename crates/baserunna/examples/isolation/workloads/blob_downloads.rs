//! Concurrent full downloads of a 24 MiB KV blob.
//!
//! `BlobBody` in `api/body.rs` streams blob values chunk by chunk.
//! For each chunk, `read_chunk` in `engine/blob.rs` fetches the chunk from object storage and
//! buffers it in memory until the client reads it.
//! The node buffers a `blob_chunk_size` chunk in memory for each active download.

use super::{AttackContext, AttackOutcome, Round, Variant, Workload};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use reqwest::Method;
use serde_json::{Value as Json, json};
use std::time::{Duration, Instant};

/// Three 8 MiB chunks. Fits inside a 32 MiB PUT request.
const BLOB_BYTES: usize = 24 << 20;
/// Concurrent attack clients. Together they buffer up to 1 GiB of 8 MiB chunks.
const ATTACK_CLIENTS: usize = 128;
/// Duration of each attack round so the attack overlaps with victim requests.
const ATTACK_DURATION: Duration = Duration::from_secs(10);
/// Sequential downloads sent by a single client in the small variant.
const SMALL_DOWNLOADS: usize = 4;
const BLOB_KEY: &str = "files/large.bin";

pub struct BlobDownloads;

impl Workload for BlobDownloads {
    fn prepare<'a>(&'a self, ctx: &'a AttackContext, round: Round) -> BoxFuture<'a, Result<Json, String>> {
        Box::pin(async move {
            if round == Round::WithVictim {
                return Ok(json!({"reuses": "the blob of the first round"}));
            }
            let path = format!("/v1/dbs/{}/kv/{BLOB_KEY}", ctx.attacker.db);
            let body = Bytes::from(vec![b'b'; BLOB_BYTES]);
            ctx.api.send_expect(Method::PUT, &path, &ctx.attacker.token, body, &[200]).await?;
            Ok(json!({"blob_bytes": BLOB_BYTES}))
        })
    }

    fn attack<'a>(&'a self, ctx: &'a AttackContext, _round: Round) -> BoxFuture<'a, Result<AttackOutcome, String>> {
        Box::pin(async move {
            let path = format!("/v1/dbs/{}/kv/{BLOB_KEY}", ctx.attacker.db);
            let (client_count, deadline, max_downloads) = match ctx.variant {
                Variant::Small => (1, None, SMALL_DOWNLOADS),
                Variant::Attack => (ATTACK_CLIENTS, Some(Instant::now() + ATTACK_DURATION), usize::MAX),
            };
            let tasks: Vec<_> = (0..client_count)
                .map(|client_index| {
                    let (api, token, path) = (ctx.api.clone(), ctx.attacker.token.clone(), path.clone());
                    tokio::spawn(async move {
                        let (mut records, mut short_downloads) = (Vec::new(), 0usize);
                        while records.len() < max_downloads && deadline.is_none_or(|end| Instant::now() < end) {
                            let (mut record, downloaded_bytes) = api
                                .send_timed_drain(
                                    &format!("download {client_index}"),
                                    Method::GET,
                                    &path,
                                    &token,
                                    Bytes::new(),
                                    &[200],
                                )
                                .await;
                            if record.expected && downloaded_bytes != BLOB_BYTES as u64 {
                                record.expected = false;
                                record.error =
                                    Some(format!("short download: {downloaded_bytes} of {BLOB_BYTES} bytes"));
                                short_downloads += 1;
                            }
                            let dropped = record.status.is_none();
                            records.push(record);
                            if dropped {
                                break;
                            }
                        }
                        (records, short_downloads)
                    })
                })
                .collect();
            let (mut requests, mut short_downloads) = (Vec::new(), 0);
            for task in tasks {
                let (task_records, task_short_downloads) = task.await.map_err(|err| err.to_string())?;
                requests.extend(task_records);
                short_downloads += task_short_downloads;
            }
            let completed_downloads = requests.iter().filter(|record| record.expected).count();
            let notes = json!({
                "clients": client_count,
                "downloads": requests.len(),
                "completed_downloads": completed_downloads,
                "short_downloads": short_downloads,
            });
            Ok(AttackOutcome { requests, completed: true, notes })
        })
    }
}
