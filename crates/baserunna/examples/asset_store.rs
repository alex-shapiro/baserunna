//! Media library example with large binary assets.
//!
//! Large files are streamed into out-of-band chunked blobs, referenced from
//! table rows alongside searchable metadata, and served with HTTP range
//! requests. Overwritten and deleted assets are garbage collected automatically.
//!
//! Run: `cargo run --example asset_store`

#[path = "common/mod.rs"]
mod common;

use baserunna::meta::SignupPolicy;
use baserunna::util::sha256;
use baserunna_client::api::{self, BlobMeta, ByteRange, ColumnType, NewColumn, NewIndex, NewTable, Selection, b64};
use baserunna_client::{Client, HttpTransport, Op, Row};
use bytes::Bytes;
use common::step;
use serde::{Deserialize, Serialize};
use serde_json::json;

/// Asset row as read: the `file` column holds the blob's metadata.
#[derive(Serialize, Deserialize)]
struct Asset {
    id: String,
    title: String,
    kind: String,
    width: Option<i64>,
    file: Option<BlobMeta>,
}

impl Row for Asset {
    const TABLE: &str = "assets";
}

/// Asset row as written: the `file` column takes the `blob_id` of a staged blob.
#[derive(Serialize, Deserialize)]
struct NewAsset {
    id: String,
    title: String,
    kind: String,
    width: Option<i64>,
    file: Option<String>,
}

impl Row for NewAsset {
    const TABLE: &str = Asset::TABLE;
}

fn fake_image(seed: u8, len: usize) -> Bytes {
    let mut x: u32 = (seed as u32).wrapping_mul(2654435761);
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
    let anon = Client::new(HttpTransport::new(&server.url));
    let me = anon.with_token(&anon.sign_up("studio@example.com", "film-grain-42").await?.token);
    me.create_db("media", None).await?;
    let media = me.db("media");
    media
        .create_table(&NewTable {
            name: Asset::TABLE.into(),
            columns: vec![
                NewColumn::new("id", ColumnType::String),
                NewColumn::new("title", ColumnType::String),
                NewColumn::new("kind", ColumnType::String),
                NewColumn::nullable("width", ColumnType::Int),
                NewColumn::nullable("file", ColumnType::Blob),
            ],
            primary_key: vec!["id".into()],
            indexes: vec![NewIndex {
                name: "by_kind".into(),
                columns: vec!["kind".into(), "title".into()],
                unique: false,
            }],
            if_not_exists: false,
        })
        .await?;

    step(1, "Upload three assets (staged blobs), then register them transactionally");
    let files = [("sunset", "photo", 3_000_000), ("intro", "video", 12_000_000), ("logo", "photo", 40_000)];
    let mut rows = Vec::new();
    for (i, (title, kind, size)) in files.iter().enumerate() {
        let data = fake_image(i as u8, *size);
        let ct = if *kind == "video" { "video/mp4" } else { "image/png" };
        let blob = media.stage_blob(data.clone(), ct).await?;
        println!("    staged {title}: {} bytes, sha256 {}…", blob.size, &blob.sha256[..16]);
        assert_eq!(blob.sha256, b64::hex(&sha256(&data)));
        rows.push(Op::insert(&NewAsset {
            id: title.to_string(),
            title: title.to_string(),
            kind: kind.to_string(),
            width: Some(1920),
            file: Some(blob.blob_id),
        }));
    }
    media.write(&rows).await?;

    step(2, "Search metadata through the index; blobs come back as references");
    let photos = Selection { index: Some("by_kind".into()), eq: vec![json!("photo")], ..Selection::default() };
    for a in media.read_one(Op::query_where::<Asset>(photos)).await?.rows::<Asset>()? {
        let f = a.file.unwrap_or_default();
        println!("    {:?} -> blob {:?} ({} bytes, {:?})", a.title, f.blob_id, f.size, f.content_type);
    }

    step(3, "Stream a download and verify it end to end");
    let intro = media.read_one(Op::get_row::<Asset>("intro")).await?.row::<Asset>()?.expect("intro was inserted");
    let file = intro.file.expect("intro has a file");
    let data = media.read_blob(&file.blob_id, None).await?;
    println!(
        "    {} bytes, content-type {:?}, intact: {}",
        data.data.len(),
        data.content_type.unwrap_or_default(),
        b64::hex(&sha256(&data.data)) == file.sha256
    );

    step(4, "Seek into the video with a Range request (bytes 8,000,000-8,000,099)");
    let part = media.read_blob(&file.blob_id, Some(ByteRange::Span { start: 8_000_000, end: 8_000_099 })).await?;
    let full = fake_image(1, 12_000_000);
    println!(
        "    content-range {:?}, matches: {}",
        part.content_range.unwrap_or_default(),
        part.data == full.slice(8_000_000..8_000_100)
    );

    step(5, "Plain key-value API for files: PUT a 5 MB file, read a slice back");
    let doc = fake_image(9, 5_000_000);
    let pdf = api::PutOptions { content_type: Some("application/pdf".into()), ..api::PutOptions::default() };
    let w = media.put_value("exports/report.pdf", doc.clone(), pdf).await?;
    let slice = media.get_value("exports/report.pdf", Some(ByteRange::Last(1000))).await?.expect("the file was put");
    println!(
        "    stored at version {}; last 1000 bytes match: {}",
        w.version,
        slice.content.data == doc.slice(doc.len() - 1000..)
    );

    step(6, "Replace and delete assets; unreferenced blobs are garbage collected later");
    let new_logo = media.stage_blob(fake_image(7, 50_000), "image/svg+xml").await?;
    media
        .write(&[Op::update::<NewAsset>("logo", &json!({"file": new_logo.blob_id})), Op::delete_row::<Asset>("sunset")])
        .await?;
    let info = media.info().await?;
    println!("    database stats: {}", serde_json::to_string(&info.stats)?);
    println!("    (the old logo and the sunset photo become garbage; a background sweep deletes");
    println!("     them after the grace period, never while an open snapshot could still read them)");

    server.stop().await;
    println!("\ndone.");
    Ok(())
}
