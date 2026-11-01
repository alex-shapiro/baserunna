use super::*;
use crate::api::{ErrorCode, Outcome};
use parking_lot::Mutex;
use std::collections::BTreeMap;

#[derive(Default)]
struct Script {
    responses: Mutex<Vec<Result<http::Response<Bytes>, TransportError>>>,
    requests: Mutex<Vec<http::Request<Bytes>>>,
}

impl Transport for Script {
    fn send(&self, req: http::Request<Bytes>) -> BoxFuture<'_, Result<http::Response<Bytes>, TransportError>> {
        self.requests.lock().push(req);
        let mut r = self.responses.lock();
        let next = if r.len() > 1 { r.remove(0) } else { r[0].clone() };
        Box::pin(async move { next })
    }
}

fn resp(status: u16, headers: &[(&str, &str)], body: &str) -> Result<http::Response<Bytes>, TransportError> {
    let mut b = http::Response::builder().status(status);
    for (k, v) in headers {
        b = b.header(*k, *v);
    }
    Ok(b.body(Bytes::from(body.to_string())).unwrap())
}

fn script(responses: Vec<Result<http::Response<Bytes>, TransportError>>) -> (Client, Arc<Script>) {
    let s = Arc::new(Script { responses: Mutex::new(responses), ..Script::default() });
    (Client::new(s.clone()).with_token("t"), s)
}

fn sent(s: &Script) -> usize {
    s.requests.lock().len()
}

const ERR: &str = r#"{"error":{"code":"CODE","message":"m","retryable":true,"details":DETAILS}}"#;

fn api_err(status: u16, code: &str, details: &str) -> Result<http::Response<Bytes>, TransportError> {
    resp(status, &[], &ERR.replace("CODE", code).replace("DETAILS", details))
}

const GET: [Op; 1] = [Op::Get { key: Bytes::from_static(b"k") }];

fn put() -> Vec<Op> {
    vec![Op::Put { key: Bytes::from_static(b"k"), value: Bytes::from_static(b"v") }]
}

#[tokio::test(start_paused = true)]
async fn requests_include_endpoint_route_method_token_and_body() {
    let ok = r#"{"version":3,"committed":true,"commit_id":"00000000000000aa","results":[{}],"appends":[]}"#;
    let (c, s) = script(vec![resp(200, &[], ok)]);
    let out = c.db("team.tools").txn(&put(), Some("key-1")).await.unwrap();
    assert_eq!(
        out,
        TxnOutcome {
            version: 3,
            commit_id: Some("00000000000000aa".into()),
            appends: vec![],
            results: Some(vec![OpResult::Done])
        }
    );
    let r = &s.requests.lock()[0];
    assert_eq!((r.method().as_str(), r.uri().to_string()), ("POST", "/v1/dbs/team.tools/txn".to_string()));
    assert_eq!(r.headers()["authorization"], "Bearer t");
    assert_eq!(r.headers()["content-type"], "application/json");
    let body: serde_json::Value = serde_json::from_slice(r.body()).unwrap();
    let want = serde_json::json!({"ops": [{"op": "put", "key": "aw==", "value": "dg=="}], "encoding": "base64", "idempotency_key": "key-1"});
    assert_eq!(body, want);
}

#[tokio::test(start_paused = true)]
async fn empty_request_bodies_omit_content_type() {
    let limits = r#"{"txn_max_ops":1,"txn_max_write_bytes":1,"txn_max_read_bytes":1,"txn_max_open_per_user":1,"value_inline_max":1,"scan_limit_max":1,"long_poll_max_ms":1,"json_value_max":1,"json_body_max":1,"blob_max":1,"blob_stage_ttl_ms":1,"added_later":true}"#;
    let (c, s) = script(vec![resp(200, &[], limits)]);
    assert_eq!(c.limits().await.unwrap().txn_max_ops, 1, "unknown fields are ignored");
    let r = &s.requests.lock()[0];
    assert!(r.headers().get("content-type").is_none() && r.body().is_empty());
}

#[tokio::test(start_paused = true)]
async fn path_parameters_are_percent_encoded() {
    let (c, s) = script(vec![resp(200, &[], "{}")]);
    c.remove_member("a/b c", "u?#%").await.unwrap();
    let uri = s.requests.lock()[0].uri().to_string();
    assert_eq!(uri, "/v1/orgs/a%2Fb%20c/members/u%3F%23%25");
}

#[tokio::test(start_paused = true)]
async fn change_requests_encode_the_range_and_wait() {
    let page = r#"{"changes":[
        {"version":4,"key":"dC94","value":"aGk="},
        {"version":5,"key":"dC95","deleted":true},
        {"version":6,"key":"dC96","blob":{"blob_id":"b","size":3,"sha256":"00","content_type":"x"}},
        {"version":7,"deleted_range":{"start":"dC8=","end":null}},
        {"version":8,"deleted_range":{"start":"dC8=","end":"dDA="}}
    ],"more":true,"next_cursor":"8","version":8}"#;
    let (c, s) = script(vec![resp(200, &[], page)]);
    let req = ChangesRequest {
        range: KeyRange::Prefix(Bytes::from_static(b"t/")),
        after: "0".into(),
        limit: 50,
        wait_ms: 20_000,
        max_staleness_ms: Some(300_000),
    };
    let p = c.db("s").changes(&req).await.unwrap();
    let uri = s.requests.lock()[0].uri().to_string();
    assert_eq!(
        uri,
        "/v1/dbs/s/changes?after=0&encoding=base64&prefix=dC8%3D&limit=50&wait_ms=20000&consistency=snapshot&max_staleness_ms=300000"
    );
    let meta = api::BlobMeta { blob_id: "b".into(), size: 3, sha256: "00".into(), content_type: "x".into() };
    let k = |s: &'static [u8]| Bytes::from_static(s);
    assert_eq!(
        p,
        ChangePage {
            changes: vec![
                Change::Put { key: k(b"t/x"), version: 4, value: Value::Inline(k(b"hi")) },
                Change::Delete { key: k(b"t/y"), version: 5 },
                Change::Put { key: k(b"t/z"), version: 6, value: Value::Blob { meta, data: None } },
                Change::DeleteRange { start: k(b"t/"), end: None, version: 7 },
                Change::DeleteRange { start: k(b"t/"), end: Some(k(b"t0")), version: 8 },
            ],
            more: true,
            cursor: "8".into(),
        }
    );
}

#[tokio::test(start_paused = true)]
async fn results_are_decoded_by_the_type_of_their_operation() {
    let body = r#"{"version":9,"committed":false,"results":[
        {"found":true,"value":"aGk=","version":5},
        {"found":false},
        {"items":[{"key":"dC94","version":2},{"key":"dC95","version":3,"blob":{"blob_id":"b","size":9,"sha256":"00","content_type":"x"},"value":"YmxvYg=="}],"next_cursor":"dC95"}
    ]}"#;
    let (c, _) = script(vec![resp(200, &[], body)]);
    let ops = [
        Op::Get { key: "a".into() },
        Op::Get { key: "b".into() },
        Op::Scan { range: KeyRange::Prefix("t/".into()), limit: 2, reverse: false, keys_only: true, cursor: None },
    ];
    let out = c.db("s").txn(&ops, None).await.unwrap();
    let meta = api::BlobMeta { blob_id: "b".into(), size: 9, sha256: "00".into(), content_type: "x".into() };
    let entry = |k: &'static str, version, value| Entry { key: k.into(), version: Some(version), value };
    assert_eq!(
        out.results.unwrap(),
        [
            OpResult::Get(Some(entry("a", 5, Value::Inline("hi".into())))),
            OpResult::Get(None),
            OpResult::Scan(ScanPage {
                entries: vec![
                    entry("t/x", 2, Value::Inline(Bytes::new())),
                    entry("t/y", 3, Value::Blob { meta, data: Some("blob".into()) }),
                ],
                cursor: Some("dC95".into()),
            }),
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn replayed_transactions_omit_results() {
    let body =
        r#"{"version":4,"committed":true,"replayed":true,"commit_id":"0000000000000001","results":null,"appends":[]}"#;
    let (c, _) = script(vec![resp(200, &[], body)]);
    assert_eq!(c.db("s").txn(&put(), Some("k")).await.unwrap().results, None);
}

#[tokio::test(start_paused = true)]
async fn responses_that_do_not_match_their_operations_are_malformed() {
    for body in [
        r#"{"version":1,"committed":false,"results":[]}"#,
        r#"{"version":1,"committed":false,"results":null}"#,
        r#"{"version":1,"committed":false,"results":[{"found":true,"value":"not base64!","version":1}]}"#,
        r#"{"version":1,"committed":false,"results":[{"items":[]}]}"#,
        r#"{"unexpected":true}"#,
    ] {
        let (c, s) = script(vec![resp(200, &[], body)]);
        let e = c.db("s").txn(&GET, None).await.unwrap_err();
        assert!(matches!(e, Error::Malformed { .. }), "{body}: {e:?}");
        assert_eq!(sent(&s), 1, "{body}: a malformed response is not retried");
    }
}

#[tokio::test(start_paused = true)]
async fn api_errors_preserve_code_details_and_outcome() {
    let cases = [
        (412, "precondition_failed", r#"{"op_index":2}"#, Outcome::NotApplied, 1),
        (503, "unavailable", r#"{"outcome":"unknown"}"#, Outcome::Unknown, 4),
        (503, "unavailable", r#"{"outcome":"not_applied"}"#, Outcome::NotApplied, 4),
        (503, "unavailable", r#"{"lease_holder":"n2"}"#, Outcome::NotApplied, 4),
        (409, "conflict", "null", Outcome::NotApplied, 4),
        (409, "aborted", "null", Outcome::NotApplied, 1),
        (500, "internal", "null", Outcome::Unknown, 4),
        (400, "a_code_of_a_newer_server", "null", Outcome::Unknown, 1),
    ];
    for (status, code, details, outcome, attempts) in cases {
        let (c, s) = script(vec![api_err(status, code, details)]);
        let e = c.db("s").txn(&GET, None).await.unwrap_err();
        assert_eq!(e.outcome(), outcome, "{code} {details}");
        assert_eq!(sent(&s), attempts, "{code} {details}: attempts");
        let Error::Api { status: got, error, .. } = &e else { panic!("{e:?}") };
        assert_eq!(*got, status);
        if code == "precondition_failed" {
            assert_eq!(error.details.as_ref().and_then(|d| d.op_index), Some(2));
        }
        if code.starts_with('a') && code.len() > 10 {
            assert_eq!(error.code, ErrorCode::Unknown);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_write_without_an_idempotency_key_is_not_sent_twice_after_an_unknown_outcome() {
    let unknowns = [
        api_err(503, "unavailable", r#"{"outcome":"unknown"}"#),
        api_err(500, "internal", "null"),
        resp(502, &[], "<html>Bad Gateway</html>"),
        resp(200, &[], "<html>Sign in</html>"),
        Err(TransportError::Unknown("reset".into())),
    ];
    for r in unknowns {
        let (c, s) = script(vec![r.clone()]);
        let e = c.db("s").txn(&put(), None).await.unwrap_err();
        assert_eq!((e.outcome(), sent(&s)), (Outcome::Unknown, 1), "{r:?}");
        let (c, s) = script(vec![r.clone()]);
        c.db("s").txn(&put(), Some("key")).await.unwrap_err();
        assert_eq!(sent(&s), 4, "{r:?}: a write with a key is retried");
    }
    for r in [Err(TransportError::NotSent("refused".into())), resp(407, &[], "<html>Proxy</html>")] {
        let (c, s) = script(vec![r.clone()]);
        c.db("s").txn(&put(), None).await.unwrap_err();
        assert_eq!(sent(&s), 4, "{r:?}: a request that was not applied is retried");
    }
}

#[tokio::test(start_paused = true)]
async fn proxy_pages_are_foreign_and_retried_only_if_transient() {
    for (status, attempts) in [(401, 1), (403, 1), (404, 1), (413, 1), (407, 4), (408, 4), (429, 4), (502, 4), (200, 4)]
    {
        let (c, s) = script(vec![resp(status, &[], "<html>Error</html>")]);
        let e = c.db("s").txn(&GET, None).await.unwrap_err();
        assert!(matches!(e, Error::Foreign { status: got, .. } if got == status), "{status}: {e:?}");
        assert_eq!(sent(&s), attempts, "HTTP {status}");
    }
}

#[tokio::test(start_paused = true)]
async fn retries_back_off_and_follow_retry_after_up_to_a_bound() {
    let mut waits = std::collections::BTreeSet::new();
    for seed in 0..20u64 {
        let (c, _) = script(vec![resp(503, &[], "<html>Busy</html>")]);
        let n = std::sync::atomic::AtomicU64::new(seed);
        let c = c.with_entropy(move || n.fetch_add(0x9e37_79b9_7f4a_7c15, std::sync::atomic::Ordering::Relaxed));
        let began = tokio::time::Instant::now();
        c.db("s").txn(&GET, None).await.unwrap_err();
        let waited = began.elapsed().as_millis();
        assert!((3..=100 + 300 + 900).contains(&waited), "each delay is at most triple the last: {waited}");
        waits.insert(waited);
    }
    assert!(waits.len() > 10, "clients that fail together retry at different times: {waits:?}");

    let began = tokio::time::Instant::now();
    let (c, _) = script(vec![resp(503, &[("retry-after", "3")], "<html>Busy</html>")]);
    c.db("s").txn(&GET, None).await.unwrap_err();
    assert_eq!(began.elapsed().as_millis(), 9_000, "each of 3 retries waits 3 s");

    let began = tokio::time::Instant::now();
    let (c, _) = script(vec![api_err(429, "rate_limited", r#"{"retry_after_ms":99999999}"#)]);
    c.db("s").txn(&put(), None).await.unwrap_err();
    assert_eq!(began.elapsed().as_millis(), 30_000, "Retry-After is capped at 10 s");

    let (c, s) = script(vec![resp(503, &[], "<html>Busy</html>")]);
    let one = c.with_retry_policy(RetryPolicy { attempts: 1, ..RetryPolicy::default() });
    one.db("s").txn(&GET, None).await.unwrap_err();
    assert_eq!(sent(&s), 1);
}

#[tokio::test(start_paused = true)]
async fn a_request_that_times_out_is_not_sent_again() {
    let (c, s) = script(vec![Err(TransportError::TimedOut("deadline".into()))]);
    let e = c.db("s").txn(&GET, None).await.unwrap_err();
    assert_eq!((e.outcome(), sent(&s)), (Outcome::Unknown, 1));
}

#[tokio::test(start_paused = true)]
async fn a_truncated_body_is_a_network_failure_and_is_retried() {
    let short = resp(200, &[("content-length", "5")], "abc");
    let (c, s) = script(vec![short, resp(200, &[("content-length", "5")], "abcde")]);
    assert_eq!(c.db("s").read_blob("b").await.unwrap(), "abcde");
    assert_eq!(sent(&s), 2);
}

#[tokio::test(start_paused = true)]
async fn the_first_success_ends_the_retries() {
    let (c, s) = script(vec![
        Err(TransportError::NotSent("refused".into())),
        api_err(409, "conflict", "null"),
        resp(201, &[], r#"{"blob_id":"b","size":1,"sha256":"00","content_type":"application/octet-stream"}"#),
    ]);
    assert_eq!(c.db("s").stage_blob(Bytes::from_static(b"x")).await.unwrap().blob_id, "b");
    assert_eq!(sent(&s), 3);
    let r = &s.requests.lock()[2];
    assert_eq!(
        (r.headers()["content-type"].to_str().unwrap(), r.body().as_ref()),
        ("application/octet-stream", &b"x"[..])
    );
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn mutate(r: &mut Rng, mut b: Vec<u8>) -> Vec<u8> {
    const BYTES: &[u8] = b"{}[]\":,0a-=n";
    for _ in 0..1 + r.below(4) {
        let i = r.below(b.len() + 1);
        match r.below(3) {
            0 if i < b.len() => b[i] = BYTES[r.below(BYTES.len())],
            1 => b.insert(i, BYTES[r.below(BYTES.len())]),
            _ if i < b.len() => drop(b.remove(i)),
            _ => {}
        }
    }
    b
}

#[test]
fn arbitrary_responses_never_panic_the_client() {
    let rt = tokio::runtime::Builder::new_current_thread().enable_time().start_paused(true).build().unwrap();
    let valid = [
        r#"{"version":5,"committed":true,"results":[{"found":true,"value":"aGk=","version":5},{"items":[{"key":"dC94","value":"","version":2}],"next_cursor":null},{}]}"#,
        r#"{"changes":[{"key":"dC94","version":3,"value":"e30="},{"version":4,"deleted_range":{"start":"dC8=","end":null}}],"more":false,"next_cursor":"4","version":4}"#,
        r#"{"error":{"code":"precondition_failed","message":"x","retryable":false,"details":{"op_index":1}}}"#,
        r#"{"blob_id":"b","size":3,"sha256":"00","content_type":"x"}"#,
        r#"{"token":"t","user":{"id":"u","email":"e","email_verified":false,"is_admin":false,"disabled":false,"has_password":true,"google_linked":false,"created_at":1}}"#,
    ];
    let mut r = Rng(7);
    rt.block_on(async {
        for _ in 0..20_000 {
            let body = valid[r.below(valid.len())].as_bytes().to_vec();
            let body = if r.below(3) == 0 { body } else { mutate(&mut r, body) };
            let status = [200u16, 201, 400, 404, 409, 412, 429, 500, 503, 507][r.below(10)];
            let ra = ["1", "0", "-3", "99999999999999999999", "soon", " 2 "][r.below(6)];
            let mut b = http::Response::builder().status(status);
            if r.below(2) == 0 {
                b = b.header("retry-after", ra);
            }
            let body = String::from_utf8_lossy(&body).into_owned();
            let (c, _) = script(vec![Ok(b.body(Bytes::from(body)).unwrap())]);
            let c = c.with_retry_policy(RetryPolicy { attempts: 1, ..RetryPolicy::default() });
            let db = c.db("s");
            let ops = [
                Op::Get { key: "a".into() },
                Op::Scan {
                    range: KeyRange::Prefix("t/".into()),
                    limit: 10,
                    reverse: false,
                    keys_only: false,
                    cursor: None,
                },
                Op::Check { key: "a".into(), expect: Expect::Absent },
            ];
            let req = ChangesRequest {
                range: KeyRange::Prefix("t/".into()),
                after: "0".into(),
                limit: 10,
                wait_ms: 0,
                max_staleness_ms: None,
            };
            match r.below(5) {
                0 => drop(db.txn(&ops, None).await),
                1 => drop(db.changes(&req).await),
                2 => drop(db.stage_blob(Bytes::from_static(b"x")).await),
                3 => drop(c.sign_up("e", "p").await),
                _ => drop(db.read_blob("b").await),
            }
        }
    });
}

#[tokio::test(start_paused = true)]
async fn an_unknown_outcome_of_an_earlier_attempt_is_not_hidden_by_later_failures() {
    let lost = Err(TransportError::Unknown("response lost".into()));
    let refused = Err(TransportError::NotSent("refused".into()));
    let (c, s) = script(vec![lost.clone(), refused.clone(), refused.clone(), refused.clone()]);
    let e = c.db("s").txn(&put(), Some("key")).await.unwrap_err();
    assert_eq!((e, sent(&s)), (Error::Transport(TransportError::Unknown("response lost".into())), 4));

    let not_applied = api_err(503, "unavailable", r#"{"outcome":"not_applied"}"#);
    let (c, _) = script(vec![lost.clone(), not_applied.clone()]);
    assert_eq!(c.db("s").txn(&put(), Some("key")).await.unwrap_err().outcome(), Outcome::Unknown);

    let denied = api_err(401, "unauthenticated", "null");
    let (c, _) = script(vec![lost.clone(), denied]);
    let e = c.db("s").txn(&put(), Some("key")).await.unwrap_err();
    assert_eq!(e.outcome(), Outcome::Unknown, "a rejected retry does not resolve an earlier attempt");

    let resolved = api_err(412, "precondition_failed", r#"{"op_index":0}"#);
    let (c, _) = script(vec![lost, resolved]);
    let e = c.db("s").txn(&put(), Some("key")).await.unwrap_err();
    assert_eq!(e.code(), Some(ErrorCode::PreconditionFailed), "a definitive response resolves earlier attempts");

    let (c, _) = script(vec![refused]);
    assert_eq!(c.db("s").txn(&put(), Some("key")).await.unwrap_err().outcome(), Outcome::NotApplied);
}

#[tokio::test(start_paused = true)]
async fn ensure_db_retries_unknown_outcomes_and_accepts_an_existing_database() {
    let lost = Err(TransportError::Unknown("response lost".into()));
    let (c, s) = script(vec![lost.clone(), api_err(409, "already_exists", "null")]);
    c.ensure_db("a", None).await.unwrap();
    assert_eq!(sent(&s), 2);
    let (c, s) = script(vec![lost]);
    assert!(matches!(c.create_db("a", None).await, Err(Error::Transport(_))));
    assert_eq!(sent(&s), 1, "create_db is not repeated after an unknown outcome");
    let (c, _) = script(vec![api_err(403, "permission_denied", "null")]);
    assert_eq!(c.ensure_db("a", None).await.unwrap_err().code(), Some(ErrorCode::PermissionDenied));
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
struct Thing {
    id: String,
    n: u32,
}

impl Row for Thing {
    const TABLE: &'static str = "things";
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
struct Other {
    id: String,
}

impl Row for Other {
    const TABLE: &'static str = "others";
}

fn wire(op: &Op) -> serde_json::Value {
    serde_json::to_value(op.to_api()).unwrap()
}

fn requests(s: &Script) -> Vec<(String, String, serde_json::Value)> {
    let json = |b: &Bytes| serde_json::from_slice(b).unwrap_or(serde_json::Value::Null);
    let rs = s.requests.lock();
    rs.iter().map(|r| (r.method().to_string(), r.uri().to_string(), json(r.body()))).collect()
}

const BEGUN: &str = r#"{"txn_id":"t1","commit_id":"00000000000000c1","read_version":1,"expires_at":9,"idle_timeout_ms":9,"results":[{"found":true,"row":{"id":"a","n":1}}]}"#;
const COMMITTED: &str = r#"{"version":2,"commit_id":"00000000000000c1","appends":[],"results":[{}]}"#;

#[test]
fn row_and_stream_ops_serialize_as_the_api_takes_them() {
    use serde_json::json;
    let row = Thing { id: "a".into(), n: 1 };
    let cases = [
        (
            Op::insert_expiring(&row, Duration::from_millis(1500)),
            json!({"op": "insert_row", "table": "things", "row": {"id": "a", "n": 1}, "ttl_ms": 1500}),
        ),
        (
            Op::insert_expiring(&row, Duration::ZERO),
            json!({"op": "insert_row", "table": "things", "row": {"id": "a", "n": 1}, "ttl_ms": 1}),
        ),
        (Op::get_row::<Thing>("a"), json!({"op": "get_row", "table": "things", "pk": "a"})),
        (Op::get_row::<Thing>(["a", "b"]), json!({"op": "get_row", "table": "things", "pk": ["a", "b"]})),
        (
            Op::update::<Thing>("a", &json!({"n": 2})),
            json!({"op": "update_row", "table": "things", "pk": "a", "set": {"n": 2}}),
        ),
        (Op::delete_rows::<Thing>(["a"]), json!({"op": "delete_rows", "table": "things", "where": {"eq": ["a"]}})),
        (Op::delete_rows::<Thing>("a"), json!({"op": "delete_rows", "table": "things", "where": {"eq": ["a"]}})),
        (Op::query::<Thing>(()), json!({"op": "query_rows", "table": "things", "limit": 1000})),
        (
            Op::append("s", &[&row]),
            json!({"op": "append", "stream": "s", "records": [{"json": {"id": "a", "n": 1}}], "create": true}),
        ),
        (Op::read_stream("s", 7), json!({"op": "read_stream", "stream": "s", "from_seq": 7, "limit": 1000})),
        (Op::increment("k", -2), json!({"op": "increment", "key": "aw==", "by": -2})),
        (Op::scan("a/"), json!({"op": "scan", "prefix": "YS8=", "limit": 1000})),
    ];
    for (op, want) in cases {
        assert_eq!(wire(&op), want);
        let back: api::Op = serde_json::from_value(want).unwrap();
        assert_eq!(back, op.to_api(), "the server parses what the client sends");
    }
    assert!(Op::upsert(&row).is_write() && !Op::query::<Thing>(()).is_write() && !Op::read_stream("s", 0).is_write());
}

#[test]
fn next_page_follows_cursors_and_stream_positions() {
    let rows = |cursor: Option<&str>| {
        OpResult::QueryRows(RowPage { table: "things".into(), rows: vec![], cursor: cursor.map(Into::into) })
    };
    let q = Op::query::<Thing>(["a"]);
    assert_eq!(wire(&q.next_page(&rows(Some("c1"))).unwrap())["cursor"], "c1");
    assert_eq!(q.next_page(&rows(None)), None);
    let scan = Op::scan("p");
    let page = OpResult::Scan(ScanPage { entries: vec![], cursor: Some("k".into()) });
    assert_eq!(wire(&scan.next_page(&page).unwrap())["cursor"], "k");

    let records = |next_seq, done| OpResult::ReadStream(RecordPage { records: vec![], next_seq, done });
    let s = Op::read_stream("s", 0);
    assert_eq!(wire(&s.next_page(&records(7, false)).unwrap())["from_seq"], 7);
    assert_eq!(s.next_page(&records(7, true)), None);
    let tail = Op::ReadStream(api::ReadStream { stream: "s".into(), tail: Some(3), ..api::ReadStream::default() });
    let next = wire(&tail.next_page(&records(9, false)).unwrap());
    assert_eq!(
        (next["from_seq"].clone(), next.get("tail")),
        (serde_json::json!(9), None),
        "later pages start at a seq"
    );
    let back = Op::ReadStream(api::ReadStream { stream: "s".into(), reverse: true, ..api::ReadStream::default() });
    assert_eq!(wire(&back.next_page(&records(4, false)).unwrap())["until_seq"], 4, "reverse reads page backward");
    assert_eq!(Op::get("k").next_page(&rows(Some("c"))), None, "a result of another operation has no next page");
}

#[tokio::test(start_paused = true)]
async fn rows_and_records_decode_into_their_types_and_name_what_is_malformed() {
    let body = r#"{"version":1,"committed":false,"results":[
        {"found":true,"row":{"id":"a","n":2}},
        {"found":false},
        {"rows":[{"id":"b","n":3},{"id":"c"}],"next_cursor":null},
        {"records":[{"seq":0,"time":5,"json":{"id":"d","n":4}},{"seq":1,"time":6,"value":"aGk="}],"next_seq":2,"done":true}
    ]}"#;
    let (c, _) = script(vec![resp(200, &[], body)]);
    let ops = [Op::get_row::<Thing>("a"), Op::get_row::<Thing>("z"), Op::query::<Thing>(()), Op::read_stream("s", 0)];
    let [found, missing, rows, records]: [OpResult; 4] = c.db("s").read(&ops).await.unwrap().try_into().unwrap();
    assert_eq!(found.clone().row::<Thing>().unwrap(), Some(Thing { id: "a".into(), n: 2 }));
    assert_eq!(missing.row::<Thing>().unwrap(), None);
    let e = found.clone().row::<Other>().unwrap_err().to_string();
    assert!(e.contains("things rows") && e.contains("read as others rows"), "{e}");
    let e = rows.clone().rows::<Thing>().unwrap_err().to_string();
    assert!(e.contains("things") && e.contains("missing field `n`"), "{e}");
    assert!(found.entry().unwrap_err().to_string().contains("get result"), "a get_row result is not a get result");

    let OpResult::ReadStream(page) = &records else { panic!("{records:?}") };
    let text = Record { seq: 1, time_ms: 6, headers: BTreeMap::new(), body: RecordBody::Bytes("hi".into()) };
    assert_eq!(page.records[1], text);
    let e = records.records::<Thing>().unwrap_err().to_string();
    assert!(e.contains("not JSON"), "{e}");
}

#[tokio::test(start_paused = true)]
async fn a_transaction_begins_with_its_first_operations_and_commits_its_writes() {
    let (c, s) =
        script(vec![resp(201, &[], BEGUN), resp(200, &[], r#"{"results":[{"value":3}]}"#), resp(200, &[], COMMITTED)]);
    let db = c.db("s");
    let mut txn = db.begin();
    assert!(txn.run(&[]).await.unwrap().is_empty());
    assert_eq!(sent(&s), 0, "a transaction without operations sends nothing");
    let row = txn.run_one(Op::get_row::<Thing>("a")).await.unwrap().row::<Thing>().unwrap();
    assert_eq!(row, Some(Thing { id: "a".into(), n: 1 }));
    assert_eq!(txn.run_one(Op::increment("n", 1)).await.unwrap(), OpResult::Increment(3));
    txn.write([Op::delete_row::<Thing>("a")]);
    let done = txn.commit().await.unwrap().unwrap();
    assert_eq!((done.version, done.commit_id.as_str()), (2, "00000000000000c1"));
    let r = requests(&s);
    assert_eq!((r[0].0.as_str(), r[0].1.as_str()), ("POST", "/v1/dbs/s/transactions"));
    assert_eq!(r[0].2["ops"][0]["op"], "get_row");
    assert_eq!(r[0].2["encoding"], "base64");
    assert_eq!(r[1].1, "/v1/dbs/s/transactions/t1/ops");
    assert_eq!(r[2].1, "/v1/dbs/s/transactions/t1/commit");
    assert_eq!(r[2].2["ops"][0]["op"], "delete_row", "queued writes execute with the commit");
    let routes: Vec<_> = s.requests.lock().iter().map(|r| r.extensions().get::<Route>().unwrap().0).collect();
    assert_eq!(routes, [api::BeginTxn::ROUTE, api::TxnOps::ROUTE, api::CommitTxn::ROUTE]);
}

#[tokio::test(start_paused = true)]
async fn a_transaction_without_reads_commits_its_writes_in_one_idempotent_request() {
    let ok = r#"{"version":3,"committed":true,"commit_id":"00000000000000aa","results":[{}],"appends":[]}"#;
    let (c, s) = script(vec![resp(200, &[], ok)]);
    assert_eq!(c.db("s").begin().commit().await.unwrap(), None);
    assert_eq!(sent(&s), 0, "a transaction without writes sends nothing");
    let mut txn = c.db("s").begin();
    txn.write([Op::put("k", "v")]);
    assert_eq!(txn.commit().await.unwrap().unwrap().version, 3);
    let r = requests(&s);
    assert_eq!((r.len(), r[0].1.as_str()), (1, "/v1/dbs/s/txn"));
    assert_eq!(r[0].2["idempotency_key"].as_str().map(str::len), Some(32));
}

#[tokio::test(start_paused = true)]
async fn an_unknown_commit_outcome_is_resolved_by_its_commit_id() {
    let lost = Err(TransportError::Unknown("response lost".into()));
    let committed = r#"{"status":"committed","version":5,"commit_id":"00000000000000c1","appends":[3]}"#;
    let not_committed = r#"{"status":"not_committed","commit_id":"00000000000000c1","retention_ms":600000}"#;
    let commit = async |status| {
        let (c, s) = script(vec![resp(201, &[], BEGUN), lost.clone(), status]);
        let mut txn = c.db("s").begin();
        txn.run_one(Op::get_row::<Thing>("a")).await.unwrap();
        txn.write([Op::upsert(&Thing { id: "a".into(), n: 2 })]);
        let r = txn.commit().await;
        (r, requests(&s))
    };

    let (r, sent) = commit(resp(200, &[], committed)).await;
    assert_eq!(r.unwrap().unwrap(), Committed { version: 5, commit_id: "00000000000000c1".into(), appends: vec![3] });
    assert_eq!((sent.len(), sent[2].0.as_str(), sent[2].1.as_str()), (3, "GET", "/v1/dbs/s/commits/00000000000000c1"));

    let (r, _) = commit(resp(200, &[], not_committed)).await;
    let e = r.unwrap_err();
    assert!(matches!(&e, Error::NotApplied(cause) if cause.outcome() == Outcome::Unknown), "{e:?}");
    assert!(e.outcome() == Outcome::NotApplied && e.is_transient(false));

    let (r, sent) = commit(Err(TransportError::NotSent("down".into()))).await;
    assert_eq!(r.unwrap_err(), Error::Transport(TransportError::Unknown("response lost".into())));
    assert_eq!(sent.len(), 2 + 4, "an unresolved outcome stays unknown after the status retries");
}

#[tokio::test(start_paused = true)]
async fn a_read_only_commit_with_an_unknown_outcome_applied_nothing() {
    let (c, s) = script(vec![resp(201, &[], BEGUN), Err(TransportError::Unknown("lost".into()))]);
    let mut txn = c.db("s").begin();
    txn.run_one(Op::get_row::<Thing>("a")).await.unwrap();
    let e = txn.commit().await.unwrap_err();
    assert_eq!((e.outcome(), sent(&s)), (Outcome::NotApplied, 2), "{e:?}");
}

#[tokio::test(start_paused = true)]
async fn writes_apply_exactly_once_by_resolving_their_idempotency_key() {
    let lost = Err(TransportError::Unknown("response lost".into()));
    let committed = r#"{"status":"committed","version":5,"commit_id":"0000000000000001","appends":[]}"#;
    let (c, s) = script(vec![lost.clone(), lost.clone(), lost.clone(), lost, resp(200, &[], committed)]);
    assert_eq!(c.db("s").write(&put()).await.unwrap().unwrap().version, 5);
    let r = requests(&s);
    let key = r[0].2["idempotency_key"].as_str().unwrap().to_string();
    assert!(r[..4].iter().all(|x| x.2["idempotency_key"] == key.as_str()), "retries reuse the key");
    assert_eq!(r[4].1, format!("/v1/dbs/s/commits/key%3A{key}"));
    let (c, s) = script(vec![api_err(400, "invalid_argument", "null")]);
    assert_eq!(c.db("s").write(&put()).await.unwrap_err().code(), Some(ErrorCode::InvalidArgument));
    assert_eq!(sent(&s), 1, "a definitive failure is not resolved");
    let keys: std::collections::HashSet<String> = (0..100).map(|_| c.random_id()).collect();
    assert_eq!(keys.len(), 100, "keys are unique");
}

#[derive(Debug, PartialEq)]
enum AppError {
    Client(Error),
    Refused,
}

impl From<Error> for AppError {
    fn from(e: Error) -> AppError {
        AppError::Client(e)
    }
}

impl TxnError for AppError {
    fn client_error(&self) -> Option<&Error> {
        match self {
            AppError::Client(e) => Some(e),
            AppError::Refused => None,
        }
    }
}

/// Runs a read-then-write transaction loop and returns the attempts and the result.
async fn read_modify_write(c: &Client, refuse: bool) -> (u32, Result<u32, AppError>) {
    let db = c.db("s");
    let mut retry = db.retry();
    let mut attempts = 0;
    let r = loop {
        attempts += 1;
        let mut txn = db.begin();
        let r = async {
            let row = txn.run_one(Op::get_row::<Thing>("a")).await?.row::<Thing>()?;
            if refuse {
                return Err(AppError::Refused);
            }
            txn.write([Op::upsert(&Thing { id: "a".into(), n: 2 })]);
            Ok(row.map_or(0, |r| r.n))
        }
        .await;
        match retry.finish(txn, r).await {
            Ok(Some(n)) => break Ok(n),
            Ok(None) => {}
            Err(e) => break Err(e),
        }
    };
    (attempts, r)
}

#[tokio::test(start_paused = true)]
async fn retry_restarts_a_conflicting_transaction() {
    let (c, s) = script(vec![
        resp(201, &[], BEGUN),
        api_err(409, "conflict", "null"),
        resp(201, &[], BEGUN),
        resp(200, &[], COMMITTED),
    ]);
    assert_eq!(read_modify_write(&c, false).await, (2, Ok(1)));
    let paths: Vec<_> = requests(&s).into_iter().map(|r| r.1).collect();
    let commit = "/v1/dbs/s/transactions/t1/commit";
    assert_eq!(paths, ["/v1/dbs/s/transactions", commit, "/v1/dbs/s/transactions", commit]);
}

#[tokio::test(start_paused = true)]
async fn retry_gives_up_on_unresolved_commits_refusals_and_exhausted_attempts() {
    let lost = Err(TransportError::Unknown("response lost".into()));
    let down = Err(TransportError::NotSent("down".into()));
    let (c, s) = script(vec![resp(201, &[], BEGUN), lost, down]);
    let (attempts, r) = read_modify_write(&c, false).await;
    assert_eq!(attempts, 1, "an unknown commit outcome never runs the transaction again");
    assert!(matches!(r, Err(AppError::Client(e)) if e.outcome() == Outcome::Unknown));
    assert_eq!(requests(&s)[2].1, "/v1/dbs/s/commits/00000000000000c1");

    let (c, s) = script(vec![resp(201, &[], BEGUN), resp(200, &[], "{}")]);
    let (attempts, r) = read_modify_write(&c, true).await;
    assert_eq!((attempts, r), (1, Err(AppError::Refused)));
    let r = requests(&s);
    assert_eq!((r[1].0.as_str(), r[1].1.as_str()), ("DELETE", "/v1/dbs/s/transactions/t1"), "a refusal aborts");

    let conflicts = (0..3).flat_map(|_| [resp(201, &[], BEGUN), api_err(409, "conflict", "null")]).collect();
    let (c, s) = script(conflicts);
    let c = c.with_retry_policy(RetryPolicy { txn_attempts: 3, ..RetryPolicy::default() });
    let (attempts, r) = read_modify_write(&c, false).await;
    assert_eq!(attempts, 3);
    assert!(matches!(r, Err(AppError::Client(e)) if e.code() == Some(ErrorCode::Conflict)));
    assert_eq!(sent(&s), 3 * 2, "each attempt begins and commits once");
}

#[tokio::test(start_paused = true)]
async fn retry_restarts_a_transaction_that_the_server_lost() {
    let (c, s) = script(vec![
        resp(201, &[], BEGUN),
        api_err(404, "txn_not_found", "null"),
        resp(201, &[], BEGUN),
        resp(200, &[], r#"{"results":[{"value":1}]}"#),
        resp(200, &[], COMMITTED),
    ]);
    let db = c.db("s");
    let mut retry = db.retry();
    let mut attempts = 0;
    let n = loop {
        attempts += 1;
        let mut txn = db.begin();
        let r = async {
            txn.run_one(Op::get_row::<Thing>("a")).await?;
            let n = txn.run_one(Op::increment("n", 1)).await?;
            txn.write([Op::put("k", "v")]);
            Ok::<_, Error>(n)
        }
        .await;
        if let Some(n) = retry.finish(txn, r).await.unwrap() {
            break n;
        }
    };
    assert_eq!((attempts, n), (2, OpResult::Increment(1)));
    let methods: Vec<_> = requests(&s).into_iter().map(|r| r.0).collect();
    assert!(!methods.contains(&"DELETE".to_string()), "the server already aborted the lost transaction");
}

#[tokio::test(start_paused = true)]
async fn a_not_committed_status_older_than_its_retention_leaves_the_outcome_unknown() {
    let lost = Err(TransportError::Unknown("response lost".into()));
    let expired = r#"{"status":"not_committed","commit_id":"0000000000000001","retention_ms":1}"#;
    let (c, _) = script(vec![lost.clone(), lost.clone(), lost.clone(), lost, resp(200, &[], expired)]);
    let e = c.db("s").write(&put()).await.unwrap_err();
    assert_eq!(e.outcome(), Outcome::Unknown, "the commit record may have expired after it applied: {e:?}");
}

#[tokio::test(start_paused = true)]
async fn retry_does_not_restart_after_an_unknown_outcome_outside_its_transaction() {
    let (c, s) = script(vec![resp(200, &[], "{}")]);
    let db = c.db("s");
    let mut retry = db.retry();
    let lost = Error::Transport(TransportError::Unknown("lost".into()));
    let r: Result<Option<()>, Error> = retry.finish(db.begin(), Err(lost.clone())).await;
    assert_eq!((r, sent(&s)), (Err(lost), 0), "another request's write may have applied");
}

#[tokio::test(start_paused = true)]
async fn a_transaction_refuses_further_use_after_a_request_fails_and_is_aborted() {
    let (c, s) = script(vec![resp(201, &[], BEGUN), api_err(400, "invalid_argument", "null"), resp(200, &[], "{}")]);
    let mut txn = c.db("s").begin();
    txn.run_one(Op::get_row::<Thing>("a")).await.unwrap();
    let e = txn.run_one(Op::put("k", "v")).await.unwrap_err();
    assert_eq!(e.code(), Some(ErrorCode::InvalidArgument));
    assert_eq!(txn.run_one(Op::get("k")).await.unwrap_err(), e, "a failed transaction runs nothing more");
    txn.write([Op::put("k", "v")]);
    assert_eq!(txn.commit().await.unwrap_err(), e, "nor commits its queued writes");
    let r = requests(&s);
    assert_eq!(r.len(), 3);
    assert_eq!(
        (r[2].0.as_str(), r[2].1.as_str()),
        ("DELETE", "/v1/dbs/s/transactions/t1"),
        "the open transaction is aborted"
    );
}

#[tokio::test(start_paused = true)]
async fn a_replayed_read_is_malformed() {
    let replayed = r#"{"version":4,"committed":true,"replayed":true,"results":null}"#;
    let (c, _) = script(vec![resp(200, &[], replayed)]);
    assert!(matches!(c.db("s").read_one(Op::get("k")).await, Err(Error::Malformed { .. })));
}

#[tokio::test(start_paused = true)]
async fn retry_does_not_restart_after_an_unknown_outcome_outside_a_failed_transaction() {
    let (c, s) = script(vec![
        resp(201, &[], BEGUN),
        api_err(503, "unavailable", "null"),
        Err(TransportError::Unknown("lost".into())),
        Err(TransportError::NotSent("down".into())),
        resp(200, &[], "{}"),
    ]);
    let db = c.with_retry_policy(RetryPolicy { attempts: 1, ..RetryPolicy::default() }).db("s");
    let mut retry = db.retry();
    let mut txn = db.begin();
    txn.run_one(Op::get_row::<Thing>("a")).await.unwrap();
    assert!(txn.run_one(Op::get("k")).await.is_err());
    let outside = db.write(&put()).await.unwrap_err();
    assert_eq!(outside.outcome(), Outcome::Unknown);
    let r: Result<Option<()>, Error> = retry.finish(txn, Err(outside.clone())).await;
    assert_eq!(r, Err(outside), "the outside write may have applied, so the loop must not run it again");
    assert_eq!(sent(&s), 5);
}

#[tokio::test(start_paused = true)]
async fn table_responses_ignore_fields_added_by_newer_servers() {
    let body = r#"{"name":"items","columns":[{"name":"id","type":"int","nullable":false,"comment":"new"}],
        "primary_key":["id"],"indexes":[{"name":"i","columns":["id"],"unique":false,"state":"ready","x":1}],"y":2}"#;
    let (c, _) = script(vec![resp(200, &[], body)]);
    let got = c.db("app").get_table("items").await;
    assert!(got.is_ok(), "{got:?}");
}

#[tokio::test(start_paused = true)]
async fn a_rejected_begin_sends_no_abort() {
    let (c, s) = script(vec![api_err(400, "invalid_argument", "null"), resp(200, &[], "{}")]);
    let mut txn = c.db("s").begin();
    assert!(txn.run_one(Op::get("k")).await.is_err());
    txn.abort().await;
    assert_eq!(sent(&s), 1, "nothing began, so nothing is aborted");
}

#[tokio::test(start_paused = true)]
async fn table_listings_decode_column_types_and_index_states_added_by_newer_servers() {
    let body = r#"{"tables":[
        {"name":"old","columns":[{"name":"id","type":"int","nullable":false}],"primary_key":["id"],"indexes":[]},
        {"name":"new","columns":[{"name":"at","type":"timestamp","nullable":false}],"primary_key":["at"],
         "indexes":[{"name":"i","columns":["at"],"unique":false,"state":"failed"}]}
    ],"next_cursor":null,"version":3}"#;
    let (c, _) = script(vec![resp(200, &[], body)]);
    let page = c.db("app").list_tables(api::ListTablesQuery::default()).await.unwrap();
    assert_eq!(page.tables[1].columns[0].ty, api::ColumnType::Unknown);
    assert_eq!(page.tables[1].indexes[0].state, api::IndexState::Unknown);
}
