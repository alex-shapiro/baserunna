//! Concurrent 30 MiB bulk event inserts, from `concurrent_bulk_inserts_respect_node_memory_budget`
//! (tests/event_ingest_limits.rs), for the `{}` and `user event` row shapes.
//!
//! As in the test, a 4 MiB insert first estimates the node memory per body byte, and the body
//! size is the smaller of 30 MiB and the size whose estimate is 1 GiB.

use crate::cgroup::MemorySampler;
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

#[derive(Default)]
pub struct BulkInserts {
    /// Body size of each row shape, from the 4 MiB calibration insert.
    body_bytes: std::sync::Mutex<Vec<usize>>,
}

impl Workload for BulkInserts {
    fn prepare<'a>(&'a self, cx: &'a AttackContext, round: Round) -> BoxFuture<'a, Result<Json, String>> {
        Box::pin(async move {
            for (shape, _) in ROW_SHAPES {
                let t = json!({"name": table(round, shape), "columns": [{"name": "time", "type": "time", "default": "now"},
                    {"name": "user", "type": "string"}, {"name": "event", "type": "string"}],
                    "time_column": "time", "order_by": ["time"]});
                cx.api.create_event_table(&cx.attacker.token, &cx.attacker.db, t).await?;
            }
            if round == Round::WithVictim {
                return Ok(Json::Null);
            }
            let mut notes = Vec::new();
            let mut sizes = Vec::new();
            for &(shape, row) in ROW_SHAPES {
                let table = format!("calibrate_{shape}");
                let t = json!({"name": table, "columns": [{"name": "time", "type": "time", "default": "now"},
                    {"name": "user", "type": "string"}, {"name": "event", "type": "string"}],
                    "time_column": "time", "order_by": ["time"]});
                cx.api.create_event_table(&cx.attacker.token, &cx.attacker.db, t).await?;
                let (small, _) = json_rows(4 << 20, row);
                let sampler = MemorySampler::start(cx.cgroup.clone());
                cx.api.bulk_insert(&cx.attacker.token, &cx.attacker.db, &table, small.clone()).await?;
                let growth = sampler.finish().growth_bytes;
                let per_byte = (growth as f64 / small.len() as f64).max(1.0);
                let body_bytes = BODY_BYTES.min(((1u64 << 30) as f64 / per_byte) as usize);
                notes.push(json!({"shape": shape, "node_bytes_per_body_byte": per_byte, "body_bytes": body_bytes}));
                sizes.push(body_bytes);
            }
            *self.body_bytes.lock().expect("body sizes") = sizes;
            Ok(Json::Array(notes))
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
            let sizes = self.body_bytes.lock().expect("body sizes").clone();
            for (&(shape, row), &size) in ROW_SHAPES.iter().zip(&sizes) {
                let (body, rows) = json_rows(size, row);
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
