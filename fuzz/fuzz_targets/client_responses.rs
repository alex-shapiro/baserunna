#![no_main]
//! Verifies that `baserunna-client` does not panic on arbitrary HTTP responses.
use baserunna_client::{
    BoxFuture, ChangesRequest, Client, Expect, KeyRange, NewRecord, Op, OpResult, RetryPolicy, Row, Transport,
    TransportError, api,
};
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

#[derive(serde::Serialize, serde::Deserialize)]
struct Item {
    id: i64,
    label: Option<String>,
}

impl Row for Item {
    const TABLE: &'static str = "t";
}

/// Decodes `r` as each typed result its operation can hold.
fn decode(r: OpResult) {
    drop(r.clone().entry());
    drop(r.clone().scan_page());
    drop(r.clone().row::<Item>());
    drop(r.clone().rows::<Item>());
    drop(r.records::<Item>());
}

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
    let client = Client::new(Fixed(resp)).with_retry_policy(RetryPolicy { attempts: 1, ..RetryPolicy::default() });
    let db = client.db("d");
    let ops = [
        Op::Get { key: Bytes::from_static(b"a") },
        Op::Scan {
            range: KeyRange::Prefix(Bytes::from_static(b"t/")),
            limit: 10,
            reverse: false,
            keys_only: false,
            cursor: None,
        },
        Op::Check { key: Bytes::from_static(b"a"), expect: Expect::Absent },
        Op::Increment { key: Bytes::from_static(b"n"), by: 1 },
        Op::get_row::<Item>(1),
        Op::query_all::<Item>(),
        Op::update::<Item>(1, &serde_json::json!({"label": "x"})),
        Op::delete_all_rows::<Item>(),
        Op::Append { stream: "s".into(), records: vec![NewRecord::Bytes(Bytes::from_static(b"r"))], create: true },
        Op::read_stream("s", 0),
    ];
    let pick = &ops[..usize::from(*call / 10) % (ops.len() + 1)];
    let changes = ChangesRequest {
        range: KeyRange::Prefix(Bytes::new()),
        after: "0".into(),
        limit: 10,
        wait_ms: 0,
        max_staleness_ms: None,
    };
    let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
    rt.block_on(async {
        match call % 10 {
            0 => {
                if let Ok(out) = db.txn(pick, None).await {
                    out.results.into_iter().flatten().for_each(decode);
                }
            }
            1 => drop(db.changes(&changes).await),
            2 => drop(db.stage_blob(Bytes::from_static(b"x")).await),
            3 => drop(db.read_blob("b").await),
            4 => drop(client.me().await),
            5 => drop(client.limits().await),
            6 => {
                let mut txn = db.begin();
                if let Ok(rs) = txn.run(pick).await {
                    rs.into_iter().for_each(decode);
                }
                txn.write([Op::put("k", "v")]);
                drop(txn.commit().await);
            }
            7 => drop(db.write(pick).await),
            8 => drop(client.metrics().await),
            _ => drop(db.list_tables(api::ListTablesQuery::default()).await),
        }
    });
});
