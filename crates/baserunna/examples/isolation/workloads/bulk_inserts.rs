//! Concurrent 30 MiB bulk event inserts, from `concurrent_bulk_inserts_respect_node_memory_budget`
//! (tests/event_ingest_limits.rs), for the `{}` and `user event` row shapes.

use super::{AttackContext, AttackOutcome, Round, Variant, Workload, concurrent_posts};
use crate::http::json_rows;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use serde_json::{Value as Json, json};

const BODY_BYTES: usize = 30 << 20;

type RowShape = (&'static str, fn(usize) -> String);

const ROW_SHAPES: &[RowShape] = &[
    ("empty", |_| "{}".to_string()),
    ("user_event", |i| {
        format!(r#"{{"time": {}, "user": "user-{:07}", "event": "e{}"}}"#, 1_749_000_000_000 + i as i64, i, i % 8)
    }),
];

fn table(round: Round, shape: &str) -> String {
    format!("bulk{}_{shape}", round.index())
}

pub struct BulkInserts;

impl Workload for BulkInserts {
    fn prepare<'a>(&'a self, cx: &'a AttackContext, round: Round) -> BoxFuture<'a, Result<Json, String>> {
        Box::pin(async move {
            for (shape, _) in ROW_SHAPES {
                let t = json!({"name": table(round, shape), "columns": [{"name": "time", "type": "time", "default": "now"},
                    {"name": "user", "type": "string"}, {"name": "event", "type": "string"}],
                    "time_column": "time", "order_by": ["time"]});
                cx.api.create_event_table(&cx.attacker.token, &cx.attacker.db, t).await?;
            }
            Ok(Json::Null)
        })
    }

    fn attack<'a>(&'a self, cx: &'a AttackContext, round: Round) -> BoxFuture<'a, Result<AttackOutcome, String>> {
        Box::pin(async move {
            let n = match cx.variant {
                Variant::Small => 1,
                Variant::Attack => 4,
            };
            let mut requests = Vec::new();
            let mut notes = Vec::new();
            for &(shape, row) in ROW_SHAPES {
                let (body, rows) = json_rows(BODY_BYTES, row);
                let body = Bytes::from(body);
                let path = format!("/v1/dbs/{}/events/{}/rows", cx.attacker.db, table(round, shape));
                let batch = (0..n).map(|i| (format!("{shape} insert {i}"), path.clone(), body.clone())).collect();
                requests.extend(concurrent_posts(cx, batch, &[201]).await);
                notes.push(json!({"shape": shape, "concurrent_inserts": n, "body_bytes": body.len(), "rows": rows}));
            }
            Ok(AttackOutcome { requests, completed: true, notes: Json::Array(notes) })
        })
    }
}
