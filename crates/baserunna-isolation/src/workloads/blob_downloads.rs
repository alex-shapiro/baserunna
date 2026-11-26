//! Concurrent full downloads of a 24 MiB KV blob.
//!
//! `BlobBody` in `api/body.rs` streams blob values chunk by chunk.
//! For each chunk, `read_chunk` in `engine/blob.rs` fetches the chunk from object storage and
//! buffers it in memory until the client reads it.
//! The node buffers a `blob_chunk_size` chunk in memory for each active download.

use super::{AttackContext, AttackOutcome, Round, Workload, request_loop};
use bytes::Bytes;
use futures_util::future::BoxFuture;
use reqwest::Method;
use serde_json::{Value as Json, json};

/// Three 8 MiB chunks. Fits inside a 32 MiB PUT request.
const BLOB_BYTES: usize = 24 << 20;
/// Concurrent attack clients. Together they buffer up to 1 GiB of 8 MiB chunks.
const ATTACK_CLIENTS: usize = 128;
/// Sequential downloads sent by a single client in the small variant.
const SMALL_DOWNLOADS: usize = 4;
const BLOB_KEY: &str = "files/large.bin";
const SHORT_DOWNLOAD: &str = "short download";

pub struct BlobDownloads;

impl Workload for BlobDownloads {
    fn prepare<'a>(&'a self, ctx: &'a AttackContext, round: Round) -> BoxFuture<'a, Result<Json, String>> {
        Box::pin(async move {
            if round == Round::WithVictim {
                return Ok(Json::Null);
            }
            ctx.api.put_value(&ctx.attacker.token, &ctx.attacker.db, BLOB_KEY, vec![b'b'; BLOB_BYTES]).await?;
            Ok(json!({"blob_bytes": BLOB_BYTES}))
        })
    }

    fn attack<'a>(&'a self, ctx: &'a AttackContext, _round: Round) -> BoxFuture<'a, Result<AttackOutcome, String>> {
        Box::pin(async move {
            let path = format!("/v1/dbs/{}/kv/{BLOB_KEY}", ctx.attacker.db);
            let requests = request_loop(ctx, SMALL_DOWNLOADS, ATTACK_CLIENTS, move |api, token, client_index| {
                let path = path.clone();
                async move {
                    let label = format!("download {client_index}");
                    let (mut record, downloaded_bytes) =
                        api.send_timed_drain(&label, Method::GET, &path, &token, Bytes::new(), &[200]).await;
                    if record.expected && downloaded_bytes != BLOB_BYTES as u64 {
                        record.expected = false;
                        record.error = Some(format!("{SHORT_DOWNLOAD}: {downloaded_bytes} of {BLOB_BYTES} bytes"));
                    }
                    record
                }
            })
            .await?;
            let is_short =
                |error: &Option<String>| error.as_deref().is_some_and(|text| text.starts_with(SHORT_DOWNLOAD));
            let notes = json!({
                "downloads": requests.len(),
                "completed_downloads": requests.iter().filter(|record| record.expected).count(),
                "short_downloads": requests.iter().filter(|record| is_short(&record.error)).count(),
            });
            Ok(AttackOutcome { requests, completed: true, notes })
        })
    }
}
