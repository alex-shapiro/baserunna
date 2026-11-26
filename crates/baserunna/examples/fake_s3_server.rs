//! Fake S3 server process that keeps objects in memory, for node tests that need a store in a
//! separate process.
//!
//! Usage: `fake_s3_server <listen addr>`, for example `fake_s3_server 127.0.0.1:9000`. The node
//! reaches it with `BASERUNNA_STORE=s3://bucket` and the `AWS_*` variables that the server prints.
//! `FAKE_S3_LATENCY_MS` delays every request by that many milliseconds.

#[path = "../tests/support/fake_s3.rs"]
#[allow(dead_code)]
mod fake_s3;

#[tokio::main]
async fn main() {
    let addr = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1:9000".into());
    let s3 = fake_s3::FakeS3::start_on(&addr).await;
    s3.set_max_list_keys(1000);
    if let Some(ms) = std::env::var("FAKE_S3_LATENCY_MS").ok().and_then(|v| v.parse().ok()) {
        s3.set_latency(std::time::Duration::from_millis(ms));
    }
    for (k, v) in s3.config() {
        println!("{}={v}", k.to_uppercase());
    }
    println!("BASERUNNA_STORE=s3://{}", fake_s3::BUCKET);
    std::future::pending::<()>().await;
}
