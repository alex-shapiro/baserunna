#![no_main]
//! The client never panics on a response, whatever its status, headers, and body.
use baserunna_client::{
    BoxFuture, ChangesRequest, Client, Expect, KeyRange, Op, Retry, Transport, TransportError,
};
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

struct Fixed(http::Response<Bytes>);

impl Transport for Fixed {
    fn send(&self, _: http::Request<Bytes>) -> BoxFuture<'_, Result<http::Response<Bytes>, TransportError>> {
        let mut r = http::Response::new(self.0.body().clone());
        *r.status_mut() = self.0.status();
        *r.headers_mut() = self.0.headers().clone();
        Box::pin(async move { Ok(r) })
    }
}

fuzz_target!(|data: &[u8]| {
    let [call, status, header, body @ ..] = data else { return };
    let status = [200u16, 201, 206, 400, 401, 404, 409, 412, 413, 429, 500, 503][usize::from(*status) % 12];
    let mut resp = http::Response::builder().status(status);
    if header & 1 == 1 {
        resp = resp.header("retry-after", (header >> 1).to_string());
    }
    if header & 2 == 2 {
        resp = resp.header("content-length", body.len().to_string());
    }
    let resp = resp.body(Bytes::copy_from_slice(body)).unwrap();
    let client = Client::new(Fixed(resp)).with_retry(Retry { attempts: 1, ..Retry::default() });
    let db = client.db("d");
    let ops = [
        Op::Get { key: Bytes::from_static(b"a") },
        Op::Scan { range: KeyRange::Prefix(Bytes::from_static(b"t/")), limit: 10, reverse: false, keys_only: false },
        Op::Check { key: Bytes::from_static(b"a"), expect: Expect::Absent },
    ];
    let changes = ChangesRequest {
        range: KeyRange::Prefix(Bytes::new()),
        after: "0".into(),
        limit: 10,
        wait_ms: 0,
        max_staleness_ms: None,
    };
    let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
    rt.block_on(async {
        match call % 6 {
            0 => drop(db.txn(&ops[..usize::from(*call / 6) % 4], None).await),
            1 => drop(db.changes(&changes).await),
            2 => drop(db.stage_blob(Bytes::from_static(b"x")).await),
            3 => drop(db.read_blob("b").await),
            4 => drop(client.me().await),
            _ => drop(client.limits().await),
        }
    });
});
