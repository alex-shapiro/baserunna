//! In-memory fake S3 server process for the isolation harness.
//!
//! Usage: `fake_s3_server [listen_addr]`.
//! The default listen address is `127.0.0.1:9000`.
//! On startup, the server prints `BASERUNNA_STORE` and `AWS_*` environment variables for the node.
//! Set `FAKE_S3_LATENCY_MS` to add a fixed millisecond delay to each request.

use baserunna_fake_s3::{BUCKET, FakeS3};

#[tokio::main]
async fn main() {
    let listen_addr = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1:9000".into());
    let server = FakeS3::start_on(&listen_addr).await;
    server.set_max_list_keys(1000);
    if let Some(latency_ms) = std::env::var("FAKE_S3_LATENCY_MS").ok().and_then(|value| value.parse().ok()) {
        server.set_latency(std::time::Duration::from_millis(latency_ms));
    }
    for (key, value) in server.config() {
        println!("{}={value}", key.to_uppercase());
    }
    println!("BASERUNNA_STORE=s3://{BUCKET}");
    std::future::pending::<()>().await;
}
