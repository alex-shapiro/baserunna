//! Real-world example: a media library with large binary assets.
//!
//! Large files are streamed into out-of-band blobs (chunked objects in the
//! object store), referenced from table rows alongside searchable metadata,
//! and served back with HTTP range requests (e.g. for video seeking or
//! resumable downloads). Overwritten and deleted assets are garbage
//! collected automatically.
//!
//! Run: `cargo run --example asset_store`

#[path = "common/mod.rs"]
mod common;

use baserunna::client::Client;
use baserunna::meta::SignupPolicy;
use baserunna::util::{b64, sha256};
use bytes::Bytes;
use common::step;
use serde_json::json;

fn fake_image(seed: u8, len: usize) -> Bytes {
    // A deterministic stand-in for real file contents.
    let mut x: u32 = seed as u32 * 2654435761;
    Bytes::from(
        (0..len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 17;
                x ^= x << 5;
                x as u8
            })
            .collect::<Vec<u8>>(),
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = common::start(common::shared_store(), "media", SignupPolicy::FirstUserOnly).await;
    let me = Client::new(&server.url).signup("studio@example.com", "film-grain-42").await?;
    me.create_db("media").await?;
    me.post(
        "/v1/dbs/media/tables",
        json!({
            "name": "assets",
            "columns": [
                {"name": "id", "type": "string", "nullable": false},
                {"name": "title", "type": "string", "nullable": false},
                {"name": "kind", "type": "string", "nullable": false},
                {"name": "width", "type": "int"},
                {"name": "file", "type": "blob"}
            ],
            "primary_key": ["id"],
            "indexes": [{"name": "by_kind", "columns": ["kind", "title"]}]
        }),
    )
    .await?;

    step(1, "Upload three assets (staged blobs), then register them transactionally");
    let files = [("sunset", "photo", 3_000_000), ("intro", "video", 12_000_000), ("logo", "photo", 40_000)];
    let mut rows = Vec::new();
    for (i, (title, kind, size)) in files.iter().enumerate() {
        let data = fake_image(i as u8, *size);
        let ct = if *kind == "video" { "video/mp4" } else { "image/png" };
        let blob = me.stage_blob("media", data.clone(), ct).await?;
        println!("    staged {title}: {} bytes, sha256 {}…", blob["size"], &blob["sha256"].as_str().unwrap()[..16]);
        assert_eq!(blob["sha256"], json!(b64::hex(&sha256(&data))));
        rows.push(json!({"id": title, "title": title, "kind": kind, "width": 1920, "file": blob["blob_id"]}));
    }
    // All three rows appear atomically, or none do.
    me.post("/v1/dbs/media/tables/assets/rows", json!({"rows": rows})).await?;

    step(2, "Search metadata through the index; blobs come back as references");
    let r = me.post("/v1/dbs/media/tables/assets/rows/query", json!({"index": "by_kind", "eq": ["photo"]})).await?;
    for a in r["rows"].as_array().unwrap() {
        println!(
            "    {} -> blob {} ({} bytes, {})",
            a["title"], a["file"]["blob_id"], a["file"]["size"], a["file"]["content_type"]
        );
    }

    step(3, "Stream a download and verify it end to end");
    let intro = me.post("/v1/dbs/media/tables/assets/rows/get", json!({"pk": "intro"})).await?;
    let blob_id = intro["row"]["file"]["blob_id"].as_str().unwrap().to_string();
    let (st, headers, data) = me.raw("GET", &format!("/v1/dbs/media/blobs/{blob_id}"), &[], Bytes::new()).await?;
    println!(
        "    status {st}, {} bytes, content-type {:?}, intact: {}",
        data.len(),
        headers.get("content-type").unwrap(),
        b64::hex(&sha256(&data)) == intro["row"]["file"]["sha256"].as_str().unwrap()
    );

    step(4, "Seek into the video with a Range request (bytes 8,000,000-8,000,099)");
    let (st, headers, part) = me
        .raw("GET", &format!("/v1/dbs/media/blobs/{blob_id}"), &[("range", "bytes=8000000-8000099")], Bytes::new())
        .await?;
    let full = fake_image(1, 12_000_000);
    println!(
        "    status {st}, {:?}, matches: {}",
        headers.get("content-range").unwrap(),
        part == full.slice(8_000_000..8_000_100)
    );

    step(5, "Plain key-value API for files: PUT a 5 MB file, read a slice back");
    let doc = fake_image(9, 5_000_000);
    let v = me.put_bytes("media", "exports/report.pdf", doc.clone(), "application/pdf").await?;
    let slice = me.get_bytes("media", "exports/report.pdf", Some("bytes=-1000")).await?.unwrap();
    println!("    stored at version {v}; last 1000 bytes match: {}", slice == doc.slice(doc.len() - 1000..));

    step(6, "Replace and delete assets; unreferenced blobs are garbage collected later");
    let new_logo = me.stage_blob("media", fake_image(7, 50_000), "image/svg+xml").await?;
    me.post("/v1/dbs/media/tables/assets/rows/update", json!({"pk": "logo", "set": {"file": new_logo["blob_id"]}}))
        .await?;
    me.post("/v1/dbs/media/tables/assets/rows/delete", json!({"pk": "sunset"})).await?;
    let info = me.get("/v1/dbs/media").await?;
    println!("    database stats: {}", info["stats"]);
    println!("    (the old logo and the sunset photo become garbage; a background sweep deletes");
    println!("     them after the grace period, never while an open snapshot could still read them)");

    server.stop().await;
    println!("\ndone.");
    Ok(())
}
