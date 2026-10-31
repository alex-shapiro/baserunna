use super::*;
use crate::api::{ErrorCode, Outcome};
use std::sync::Mutex;

/// Transport that returns scripted responses in order and records the requests.
#[derive(Default)]
struct Script {
    responses: Mutex<Vec<Result<http::Response<Bytes>, TransportError>>>,
    requests: Mutex<Vec<http::Request<Bytes>>>,
}

impl Transport for Script {
    fn send(&self, req: http::Request<Bytes>) -> BoxFuture<'_, Result<http::Response<Bytes>, TransportError>> {
        self.requests.lock().unwrap().push(req);
        let mut r = self.responses.lock().unwrap();
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
    (Client::with_transport(s.clone()).with_token("t"), s)
}

fn sent(s: &Script) -> usize {
    s.requests.lock().unwrap().len()
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
async fn requests_carry_the_route_method_token_and_body_of_their_endpoint() {
    let ok = r#"{"version":3,"committed":true,"commit_id":"00000000000000aa","results":[{}],"appends":[]}"#;
    let (c, s) = script(vec![resp(200, &[], ok)]);
    let out = c.db("team.tools").txn(&put(), Some("key-1")).await.unwrap();
    assert_eq!(
        out,
        TxnOutcome { version: 3, commit_id: Some("00000000000000aa".into()), results: Some(vec![OpResult::Done]) }
    );
    let r = &s.requests.lock().unwrap()[0];
    assert_eq!((r.method().as_str(), r.uri().to_string()), ("POST", "/v1/dbs/team.tools/txn".to_string()));
    assert_eq!(r.headers()["authorization"], "Bearer t");
    assert_eq!(r.headers()["content-type"], "application/json");
    let body: serde_json::Value = serde_json::from_slice(r.body()).unwrap();
    let want = serde_json::json!({"ops": [{"op": "put", "key": "aw==", "value": "dg=="}], "encoding": "base64", "idempotency_key": "key-1"});
    assert_eq!(body, want);
}

#[tokio::test(start_paused = true)]
async fn requests_without_a_body_send_no_content_type() {
    let limits = r#"{"txn_max_ops":1,"txn_max_write_bytes":1,"txn_max_read_bytes":1,"txn_max_open_per_user":1,"value_inline_max":1,"scan_limit_max":1,"long_poll_max_ms":1,"json_value_max":1,"json_body_max":1,"blob_max":1,"blob_stage_ttl_ms":1,"added_later":true}"#;
    let (c, s) = script(vec![resp(200, &[], limits)]);
    assert_eq!(c.limits().await.unwrap().txn_max_ops, 1, "unknown fields are ignored");
    let r = &s.requests.lock().unwrap()[0];
    assert!(r.headers().get("content-type").is_none() && r.body().is_empty());
}

#[tokio::test(start_paused = true)]
async fn path_parameters_are_percent_encoded() {
    let (c, s) = script(vec![resp(200, &[], "{}")]);
    c.remove_member("a/b c", "u?#%").await.unwrap();
    let uri = s.requests.lock().unwrap()[0].uri().to_string();
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
    let uri = s.requests.lock().unwrap()[0].uri().to_string();
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
        Op::Scan { range: KeyRange::Prefix("t/".into()), limit: 2, reverse: false, keys_only: true },
    ];
    let out = c.db("s").txn(&ops, None).await.unwrap();
    let meta = api::BlobMeta { blob_id: "b".into(), size: 9, sha256: "00".into(), content_type: "x".into() };
    let entry = |k: &'static str, version, value| Entry { key: k.into(), version, value };
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
async fn a_replayed_transaction_has_no_results() {
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
        r#"{"version":1,"committed":false,"results":[{"found":true,"value":"aGk=","version":null}]}"#,
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
async fn api_errors_carry_their_code_details_and_outcome() {
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
    let began = tokio::time::Instant::now();
    let (c, _) = script(vec![resp(503, &[], "<html>Busy</html>")]);
    c.db("s").txn(&GET, None).await.unwrap_err();
    assert_eq!(began.elapsed().as_millis(), 100 + 300 + 900, "backoff triples");

    let began = tokio::time::Instant::now();
    let (c, _) = script(vec![resp(503, &[("retry-after", "3")], "<html>Busy</html>")]);
    c.db("s").txn(&GET, None).await.unwrap_err();
    assert_eq!(began.elapsed().as_millis(), 9_000, "each of 3 retries waits 3 s");

    let began = tokio::time::Instant::now();
    let (c, _) = script(vec![api_err(429, "rate_limited", r#"{"retry_after_ms":99999999}"#)]);
    c.db("s").txn(&put(), None).await.unwrap_err();
    assert_eq!(began.elapsed().as_millis(), 30_000, "Retry-After is capped at 10 s");

    let (c, s) = script(vec![resp(503, &[], "<html>Busy</html>")]);
    let one = c.with_retry(Retry { attempts: 1, ..Retry::default() });
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
    let r = &s.requests.lock().unwrap()[2];
    assert_eq!(
        (r.headers()["content-type"].to_str().unwrap(), r.body().as_ref()),
        ("application/octet-stream", &b"x"[..])
    );
}

/// SplitMix64 generator for property tests.
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

/// Mutates `b` by replacing, inserting, or deleting bytes.
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
            let c = c.with_retry(Retry { attempts: 1, ..Retry::default() });
            let db = c.db("s");
            let ops = [
                Op::Get { key: "a".into() },
                Op::Scan { range: KeyRange::Prefix("t/".into()), limit: 10, reverse: false, keys_only: false },
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
