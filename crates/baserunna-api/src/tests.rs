use super::*;
use serde_json::{Value as Json, json};
use tower::ServiceExt;

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

    fn maybe<T>(&mut self, f: impl FnOnce(&mut Rng) -> T) -> Option<T> {
        (self.below(2) == 0).then(|| f(self))
    }

    fn json(&mut self) -> Json {
        match self.below(5) {
            0 => Json::Null,
            _ => self.value(),
        }
    }

    /// Returns a non-null JSON value, because a `null` optional field decodes as absent.
    fn value(&mut self) -> Json {
        match self.below(4) {
            0 => json!(self.next() as i64),
            1 => json!(self.text()),
            2 => json!([self.text(), self.below(9)]),
            _ => json!({self.text(): self.below(2) == 0}),
        }
    }

    fn selection(&mut self) -> Selection {
        Selection {
            index: self.opt(),
            eq: (0..self.below(3)).map(|_| self.json()).collect(),
            range: self.maybe(|r| RangeSpec {
                gt: r.maybe(Rng::value),
                lte: r.maybe(Rng::value),
                ..RangeSpec::default()
            }),
            filter: self.maybe(|r| json!({"col": r.text(), "op": "eq", "value": r.json()})),
        }
    }
}

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
        (GetMetrics::ROUTE, 0, Box::new(|_| GetMetrics.path())),
        (ResetMetrics::ROUTE, 0, Box::new(|_| ResetMetrics.path())),
        (Txn::ROUTE, 1, Box::new(|p| Txn { db: p[0].clone() }.path())),
        (BeginTxn::ROUTE, 1, Box::new(|p| BeginTxn { db: p[0].clone() }.path())),
        (TxnOps::ROUTE, 2, Box::new(|p| TxnOps { db: p[0].clone(), id: p[1].clone() }.path())),
        (CommitTxn::ROUTE, 2, Box::new(|p| CommitTxn { db: p[0].clone(), id: p[1].clone() }.path())),
        (AbortTxn::ROUTE, 2, Box::new(|p| AbortTxn { db: p[0].clone(), id: p[1].clone() }.path())),
        (GetCommit::ROUTE, 2, Box::new(|p| GetCommit { db: p[0].clone(), id: p[1].clone() }.path())),
        (CreateTable::ROUTE, 1, Box::new(|p| CreateTable { db: p[0].clone() }.path())),
        (ListTables::ROUTE, 1, Box::new(|p| ListTables { db: p[0].clone(), query: ListTablesQuery::default() }.path())),
        (GetTable::ROUTE, 2, Box::new(|p| GetTable { db: p[0].clone(), table: p[1].clone() }.path())),
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
    let n = |r: &mut Rng| r.maybe(|r| r.next() >> 12);
    let ttl_ms = |r: &mut Rng| r.maybe(|r| 1 + r.next() % 1000);
    match r.below(20) {
        0 => Op::Get { key: r.text() },
        1 => Op::Scan {
            prefix: r.opt(),
            start: r.opt(),
            end: r.opt(),
            limit: r.maybe(|r| r.below(PAGE_MAX + 1)),
            reverse: r.below(2) == 0,
            keys_only: r.below(2) == 0,
            cursor: r.opt(),
        },
        2 => Op::Put { key: r.text(), value: r.opt(), blob_id: r.opt(), ttl_ms: ttl_ms(r) },
        3 => Op::Delete { key: r.text() },
        4 => Op::Copy { from: r.text(), to: r.text(), ttl_ms: ttl_ms(r) },
        5 => Op::DeleteRange { prefix: r.opt(), start: r.opt(), end: r.opt() },
        6 => Op::Check { key: r.text(), exists: r.maybe(|r| r.below(2) == 0), version: n(r), value: r.opt() },
        7 => Op::ReadStream(ReadStream {
            stream: r.text(),
            from_seq: n(r),
            tail: n(r),
            consumer: r.opt(),
            until_time: n(r),
            reverse: r.below(2) == 0,
            limit: r.maybe(|r| r.below(PAGE_MAX)),
            filter: r.maybe(Rng::value),
            ..ReadStream::default()
        }),
        8 => Op::GetRow { table: r.text(), pk: r.json() },
        9 => Op::InsertRow { table: r.text(), row: r.json(), ttl_ms: ttl_ms(r) },
        10 => Op::UpsertRow { table: r.text(), row: r.json(), ttl_ms: ttl_ms(r) },
        11 => Op::UpdateRow { table: r.text(), pk: r.json(), set: r.json(), ttl_ms: ttl_ms(r) },
        12 => Op::DeleteRow { table: r.text(), pk: r.json() },
        13 => Op::UpdateRows {
            table: r.text(),
            selection: r.selection(),
            set: r.json(),
            ttl_ms: ttl_ms(r),
            limit: r.maybe(|r| r.below(9)),
        },
        14 => Op::DeleteRows { table: r.text(), selection: r.selection(), limit: r.maybe(|r| r.below(9)) },
        15 => {
            let Selection { index, eq, range, filter } = r.selection();
            let (limit, reverse, cursor) = (r.maybe(|r| r.below(9)), r.below(2) == 0, r.opt());
            Op::QueryRows { table: r.text(), index, eq, range, filter, limit, reverse, cursor }
        }
        16 => Op::Append {
            stream: r.text(),
            records: (0..r.below(3))
                .map(|_| NewRecord {
                    value: r.opt(),
                    json: r.maybe(Rng::json),
                    headers: (0..r.below(2)).map(|_| (r.text(), r.text())).collect(),
                })
                .collect(),
            expect_seq: n(r),
            create: r.below(2) == 0,
        },
        17 => Op::StreamInfo { stream: r.text() },
        18 => Op::SetConsumer { stream: r.text(), consumer: r.text(), seq: r.next() },
        _ => Op::Increment { key: r.text(), by: r.next() as i64, ttl_ms: ttl_ms(r) },
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
fn transaction_bodies_round_trip_through_json() {
    let mut r = Rng(3);
    for _ in 0..2_000 {
        let ops: Vec<Op> = (0..r.below(4)).map(|_| random_op(&mut r)).collect();
        let begin = BeginRequest { encoding: Encoding::Base64, ops: ops.clone(), ..BeginRequest::default() };
        assert_eq!(BeginRequest::decode(begin.encode()).unwrap(), begin);
        let more = OpsRequest { ops: ops.clone() };
        assert_eq!(OpsRequest::decode(more.encode()).unwrap(), more);
        let commit = CommitRequest { ops };
        assert_eq!(CommitRequest::decode(commit.encode()).unwrap(), commit);
    }
    for empty in ["{}", "null"] {
        assert_eq!(CommitRequest::decode(empty.into()).unwrap(), CommitRequest::default(), "commit ops are optional");
    }
    assert!(OpsRequest::decode("{}".into()).is_err(), "ops requests name their ops");
    assert!(CommitRequest::decode(r#"{"ops":[],"nope":1}"#.into()).is_err());
}

#[test]
fn null_record_bodies_stay_apart_from_absent_ones() {
    let null: NewRecord = serde_json::from_value(json!({"json": null})).unwrap();
    assert_eq!(null.json, Some(Json::Null));
    assert_eq!(serde_json::to_value(&null).unwrap(), json!({"json": null}));
    let text: NewRecord = serde_json::from_value(json!({"value": "v"})).unwrap();
    assert_eq!((text.json, text.value.as_deref()), (None, Some("v")));
    let rec: StreamRecord = serde_json::from_value(json!({"seq": 1, "time": 2, "json": null})).unwrap();
    assert_eq!(rec.json, Some(Json::Null));
}

#[test]
fn row_results_decode_directly_into_row_types() {
    #[derive(Deserialize, Debug, PartialEq)]
    struct Thing {
        id: String,
    }
    let page = r#"{"rows":[{"id":"a"},{"id":"b"}],"next_cursor":null}"#;
    let r: RowsResult<Thing> = serde_json::from_str(page).unwrap();
    assert_eq!(r.rows, [Thing { id: "a".into() }, Thing { id: "b".into() }]);
    let missing: RowResult<Thing> = serde_json::from_str(r#"{"found":false}"#).unwrap();
    assert_eq!(missing.row, None);
    let raw: RowsResult<Box<serde_json::value::RawValue>> = serde_json::from_str(page).unwrap();
    assert_eq!(raw.rows[1].get(), r#"{"id":"b"}"#);
}

#[test]
fn results_serialize_as_the_server_has_always_written_them() {
    let cases = [
        (OpResult::GetRow(RowResult::default()), json!({"found": false})),
        (
            OpResult::GetRow(RowResult { found: true, row: Some(json!({"a": 1})), expires_at: Some(9) }),
            json!({"found": true, "row": {"a": 1}, "expires_at": 9}),
        ),
        (OpResult::QueryRows(RowsResult { rows: vec![], next_cursor: None }), json!({"rows": [], "next_cursor": null})),
        (OpResult::Append(AppendResult { count: 2, first_seq: Some(5) }), json!({"count": 2, "first_seq": 5})),
        (OpResult::GetConsumer(ConsumerResult { found: false, seq: None }), json!({"found": false})),
        (
            OpResult::StreamInfo(StreamState {
                first_seq: 0,
                next_seq: 0,
                bytes: 0,
                first_time: None,
                last_time: None,
            }),
            json!({"first_seq": 0, "next_seq": 0, "bytes": 0}),
        ),
        (
            OpResult::ReadStream(RecordsResult {
                records: vec![StreamRecord { seq: 3, time: 4, value: Some("aGk=".into()), ..StreamRecord::default() }],
                next_seq: 4,
                done: true,
            }),
            json!({"records": [{"seq": 3, "time": 4, "value": "aGk="}], "next_seq": 4, "done": true}),
        ),
        (OpResult::Increment(IncrementResult { value: -3 }), json!({"value": -3})),
        (OpResult::Done(Empty {}), json!({})),
    ];
    for (r, want) in cases {
        assert_eq!(serde_json::to_value(&r).unwrap(), want);
    }
}

#[test]
fn commit_statuses_are_tagged_by_status() {
    let done = CommitStatus::Committed { version: 7, commit_id: "00000000000000aa".into(), appends: vec![3] };
    let j = json!({"status": "committed", "version": 7, "commit_id": "00000000000000aa", "appends": [3]});
    assert_eq!(serde_json::to_value(&done).unwrap(), j);
    let not = r#"{"status":"not_committed","commit_id":"00000000000000aa","retention_ms":600000}"#;
    let want = CommitStatus::NotCommitted { commit_id: "00000000000000aa".into(), retention_ms: 600_000 };
    assert_eq!(CommitStatus::decode(not.into()).unwrap(), want);
}

#[test]
fn table_definitions_default_to_nullable_columns() {
    let t = NewTable::decode(r#"{"name":"t","columns":[{"name":"id","type":"string"}],"primary_key":["id"]}"#.into());
    let t = t.unwrap();
    assert!(t.columns[0].nullable && !t.if_not_exists);
    let def = NewTable {
        name: "t".into(),
        columns: vec![Column::new("id", ColumnType::String), Column::nullable("n", ColumnType::Int)],
        primary_key: vec!["id".into()],
        indexes: vec![],
        if_not_exists: true,
    };
    assert!(CreateTable::is_idempotent(&def));
    assert_eq!(NewTable::decode(def.encode()).unwrap(), def);
    for ty in ColumnType::ALL {
        assert_eq!(serde_json::to_value(ty).unwrap(), json!(ty.name()));
    }
    let created = r#"{"name":"t","columns":[],"primary_key":["id"],"indexes":[{"name":"i","columns":["n"],"unique":false,"state":"building"}],"version":4}"#;
    let c = CreatedTable::decode(created.into()).unwrap();
    assert_eq!((c.version, c.table.indexes[0].state), (4, IndexState::Building));
}

#[test]
fn metrics_omit_quantiles_of_empty_histograms() {
    let m = Metrics::decode(
        r#"{"node":"n","store":{},"commits":{"count":2,"p50_ms":1.5,"txns_per_batch":1.0,"max_txns_per_batch":1},"routes":{"GET /v1/limits":{"count":0}},"lag":{"count":0},"cpu":{"process_ms":null,"steal_ms":null}}"#.into(),
    )
    .unwrap();
    assert_eq!((m.commits.latency.count, m.commits.latency.p50_ms), (2, Some(1.5)));
    assert_eq!(serde_json::to_value(&m.lag).unwrap(), json!({"count": 0}));
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
