use super::*;
use serde_json::{Value as Json, json};
use tower::ServiceExt;

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

    fn text(&mut self) -> String {
        let pool = ["a", "Z", "0", "/", "%", "?", "#", " ", "+", "=", "&", ".", "~", "é", "😀", "\u{0}", "{x}"];
        (0..1 + self.below(6)).map(|_| pool[self.below(pool.len())]).collect()
    }

    fn opt(&mut self) -> Option<String> {
        (self.below(2) == 0).then(|| self.text())
    }
}

/// Route, number of path parameters, and the path of a request with given parameters.
type Route = (&'static str, usize, Box<dyn Fn(&[String]) -> String>);

fn routes() -> Vec<Route> {
    vec![
        (GetLimits::ROUTE, 0, Box::new(|_| GetLimits.path())),
        (SignUp::ROUTE, 0, Box::new(|_| SignUp.path())),
        (LogIn::ROUTE, 0, Box::new(|_| LogIn.path())),
        (GetMe::ROUTE, 0, Box::new(|_| GetMe.path())),
        (CreateOrg::ROUTE, 0, Box::new(|_| CreateOrg.path())),
        (AddMember::ROUTE, 1, Box::new(|p| AddMember { org: p[0].clone() }.path())),
        (RemoveMember::ROUTE, 2, Box::new(|p| RemoveMember { org: p[0].clone(), user: p[1].clone() }.path())),
        (CreateDb::ROUTE, 0, Box::new(|_| CreateDb.path())),
        (Txn::ROUTE, 1, Box::new(|p| Txn { db: p[0].clone() }.path())),
        (Changes::ROUTE, 1, Box::new(|p| Changes { db: p[0].clone(), query: ChangesQuery::default() }.path())),
        (StageBlob::ROUTE, 1, Box::new(|p| StageBlob { db: p[0].clone() }.path())),
        (ReadBlob::ROUTE, 2, Box::new(|p| ReadBlob { db: p[0].clone(), blob_id: p[1].clone() }.path())),
    ]
}

#[tokio::test]
async fn paths_route_back_to_their_parameters() {
    let mut r = Rng(1);
    for (route, n, path) in routes() {
        let app = match n {
            0 => axum::Router::new().route(route, axum::routing::any(|| async { "[]" })),
            _ => axum::Router::new().route(
                route,
                axum::routing::any(|axum::extract::Path(p): axum::extract::Path<Vec<String>>| async move {
                    serde_json::to_string(&p).unwrap()
                }),
            ),
        };
        for _ in 0..200 {
            let params: Vec<String> = (0..n).map(|_| r.text()).collect();
            let p = path(&params);
            let req = http::Request::builder().uri(&p).body(axum::body::Body::empty()).unwrap();
            let resp = app.clone().oneshot(req).await.unwrap();
            assert_eq!(resp.status(), 200, "{route}: {p}");
            let body = axum::body::to_bytes(resp.into_body(), 1 << 16).await.unwrap();
            let got: Vec<String> = serde_json::from_slice(&body).unwrap();
            assert_eq!(got, params, "{route}: {p}");
        }
    }
}

fn random_op(r: &mut Rng) -> Op {
    match r.below(9) {
        0 => Op::Get { key: r.text() },
        1 => Op::Scan {
            prefix: r.opt(),
            start: r.opt(),
            end: r.opt(),
            limit: (r.below(2) == 0).then(|| r.below(1001)),
            reverse: r.below(2) == 0,
            keys_only: r.below(2) == 0,
            cursor: r.opt(),
        },
        2 => Op::Put { key: r.text(), value: r.opt(), blob_id: r.opt(), ttl_ms: (r.below(2) == 0).then(|| r.next()) },
        3 => Op::Delete { key: r.text() },
        4 => Op::Copy { from: r.text(), to: r.text(), ttl_ms: None },
        5 => Op::DeleteRange { prefix: r.opt(), start: r.opt(), end: r.opt() },
        6 => Op::Check {
            key: r.text(),
            exists: (r.below(2) == 0).then(|| r.below(2) == 0),
            version: (r.below(2) == 0).then(|| r.next()),
            value: r.opt(),
        },
        7 => Op::ReadStream(ReadStream {
            stream: r.text(),
            spec: serde_json::from_value(json!({"from_seq": r.below(9), "limit": 3})).unwrap(),
        }),
        _ => Op::Increment { key: r.text(), by: r.next() as i64, ttl_ms: None },
    }
}

#[test]
fn ops_round_trip_through_json() {
    let mut r = Rng(2);
    for _ in 0..5_000 {
        let ops: Vec<Op> = (0..r.below(5)).map(|_| random_op(&mut r)).collect();
        let req = TxnRequest {
            ops,
            encoding: if r.below(2) == 0 { Encoding::Base64 } else { Encoding::Utf8 },
            idempotency_key: r.opt(),
            ..TxnRequest::default()
        };
        let back = TxnRequest::decode(req.encode()).unwrap();
        assert_eq!(back, req);
    }
}

#[test]
fn ops_omit_default_fields() {
    let check = Op::Check { key: "aw==".into(), exists: Some(false), version: None, value: None };
    assert_eq!(serde_json::to_value(&check).unwrap(), json!({"op": "check", "key": "aw==", "exists": false}));
    let scan = Op::Scan {
        prefix: None,
        start: Some("YQ==".into()),
        end: Some("Yg==".into()),
        limit: Some(10),
        reverse: false,
        keys_only: true,
        cursor: None,
    };
    let want = json!({"op": "scan", "start": "YQ==", "end": "Yg==", "limit": 10, "keys_only": true});
    assert_eq!(serde_json::to_value(&scan).unwrap(), want);
    assert_eq!(serde_json::to_value(TxnRequest::default()).unwrap(), json!({"ops": []}));
}

#[test]
fn requests_reject_unknown_fields_and_responses_ignore_them() {
    assert!(TxnRequest::decode(r#"{"ops":[],"nope":1}"#.into()).is_err());
    assert!(TxnRequest::decode(r#"{"ops":[{"op":"get","key":"k","nope":1}]}"#.into()).is_err());
    assert!(NewDb::decode(r#"{"name":"a","nope":1}"#.into()).is_err());
    let db = Db::decode(r#"{"id":"i","name":"a","org":null,"created_at":1,"new":true}"#.into()).unwrap();
    assert_eq!(db.org, None);
}

#[test]
fn nullable_fields_keep_null_apart_from_absent() {
    let r: DeletedRange = serde_json::from_value(json!({"start": "a", "end": null})).unwrap();
    assert_eq!(r.end, Some(None));
    let r: DeletedRange = serde_json::from_value(json!({"start": "a", "end_b64": "/w=="})).unwrap();
    assert_eq!((r.end, r.end_b64.as_deref()), (None, Some("/w==")));
    for j in [json!({"start": "a", "end": null}), json!({"start": "a", "end": "b"}), json!({"start_b64": "/w=="})] {
        let r: DeletedRange = serde_json::from_value(j.clone()).unwrap();
        assert_eq!(serde_json::to_value(r).unwrap(), j);
    }
    for j in [
        json!({"found": false}),
        json!({"found": true, "value": "v", "version": null}),
        json!({"found": true, "version": 3}),
    ] {
        let g: GetResult = serde_json::from_value(j.clone()).unwrap();
        assert_eq!(serde_json::to_value(g).unwrap(), j);
    }
}

#[test]
fn transaction_results_keep_their_json() {
    let body = r#"{"version":1,"committed":true,"commit_id":"00000000000000aa","results":[{"found":true,"value":"v","version":null},{}],"appends":[]}"#;
    let r = TxnResponse::decode(body.into()).unwrap();
    let results = r.results.as_ref().unwrap();
    let get: GetResult = results[0].decode().unwrap();
    assert_eq!(get.version, Some(None));
    assert_eq!(results[1].decode::<Empty>().unwrap(), Empty {});
    assert_eq!(serde_json::from_slice::<Json>(&r.encode()).unwrap(), serde_json::from_str::<Json>(body).unwrap());
    let typed = OpResult::Get(get.clone());
    assert_eq!(typed.decode::<GetResult>().unwrap(), get);
}

#[test]
fn error_codes_serialize_as_their_names() {
    use ErrorCode as C;
    let all = [
        C::InvalidArgument,
        C::Unauthenticated,
        C::PermissionDenied,
        C::NotFound,
        C::TxnNotFound,
        C::Conflict,
        C::AlreadyExists,
        C::Aborted,
        C::PreconditionFailed,
        C::TooLarge,
        C::CursorExpired,
        C::QuotaExceeded,
        C::RateLimited,
        C::Unavailable,
        C::Internal,
        C::Unknown,
    ];
    for c in all {
        assert_eq!(serde_json::to_value(c).unwrap(), json!(c.as_str()));
        assert_eq!(serde_json::from_value::<ErrorCode>(json!(c.as_str())).unwrap(), c);
    }
    assert_eq!(serde_json::from_value::<ErrorCode>(json!("from_the_future")).unwrap(), C::Unknown);
}

#[test]
fn change_queries_parse_flags_like_the_server() {
    for (q, keys_only) in
        [("after=0&keys_only", true), ("after=0&keys_only=1", true), ("after=0&keys_only=false", false)]
    {
        let parsed: ChangesQuery = serde_urlencoded::from_str(q).unwrap();
        assert_eq!(parsed.keys_only, keys_only, "{q}");
    }
    assert!(serde_urlencoded::from_str::<ChangesQuery>("after=0&keys_only=maybe").is_err());
    assert!(serde_urlencoded::from_str::<ChangesQuery>("limit=1").is_err(), "after is required");
    let q = ChangesQuery { after: "7".into(), prefix: Some("a+b/=".into()), ..ChangesQuery::default() };
    let back: ChangesQuery = serde_urlencoded::from_str(&serde_urlencoded::to_string(&q).unwrap()).unwrap();
    assert_eq!(back, q);
}
