//! Concurrent bulk event inserts, from `concurrent_bulk_inserts_respect_node_memory_budget`
//! (tests/event_ingest_limits.rs), for the `{}` and `user event` row shapes.
//!
//! The test sizes each body as the smaller of 30 MiB and the size whose estimated node memory is
//! 1 GiB. Here the body sizes are fixed, so every node under test gets the same attack. The attack
//! sizes are the sizes that this rule gave on today's node in the VM: 142 node bytes per body byte
//! for `{}` rows (7,560,000 bytes) and 20 for user event rows (30 MiB).
//!
//! The small variant sends one smaller body of each shape. Today's node completes it with growth
//! under the 512 MiB tenant bound, so a node that refuses it refuses a legitimate insert. A 256 KiB
//! probe insert reports the node bytes per body byte, as a measurement only.

use crate::cgroup::MemorySampler;
use super::{AttackContext, AttackOutcome, Round, Variant, Workload, concurrent_posts};
use crate::http::json_rows;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use serde_json::{Value as Json, json};

/// Row shape of the inserted bodies.
struct RowShape {
    name: &'static str,
    row: fn(usize) -> String,
    attack_body_bytes: usize,
    small_body_bytes: usize,
}

const ROW_SHAPES: &[RowShape] = &[
    RowShape { name: "empty", row: |_| "{}".to_string(), attack_body_bytes: 7_560_000, small_body_bytes: 512 << 10 },
    RowShape {
        name: "user_event",
        row: |i| format!(r#"{{"time": {}, "user": "user-{:07}", "event": "e{}"}}"#, 1_749_000_000_000 + i as i64, i, i % 8),
        attack_body_bytes: 30 << 20,
        small_body_bytes: 4 << 20,
    },
];

const PROBE_BODY_BYTES: usize = 256 << 10;

fn table(round: Round, shape: &str) -> String {
    format!("bulk{}_{shape}", round.index())
}

pub struct BulkInserts;

impl Workload for BulkInserts {
    fn prepare<'a>(&'a self, cx: &'a AttackContext, round: Round) -> BoxFuture<'a, Result<Json, String>> {
        Box::pin(async move {
            for shape in ROW_SHAPES {
                let t = json!({"name": table(round, shape.name), "columns": [{"name": "time", "type": "time", "default": "now"},
                    {"name": "user", "type": "string"}, {"name": "event", "type": "string"}],
                    "time_column": "time", "order_by": ["time"]});
                cx.api.create_event_table(&cx.attacker.token, &cx.attacker.db, t).await?;
            }
            if round == Round::WithVictim {
                return Ok(Json::Null);
            }
            let mut notes = Vec::new();
            for shape in ROW_SHAPES {
                let table = format!("probe_{}", shape.name);
                let t = json!({"name": table, "columns": [{"name": "time", "type": "time", "default": "now"},
                    {"name": "user", "type": "string"}, {"name": "event", "type": "string"}],
                    "time_column": "time", "order_by": ["time"]});
                cx.api.create_event_table(&cx.attacker.token, &cx.attacker.db, t).await?;
                let (probe, _) = json_rows(PROBE_BODY_BYTES, shape.row);
                let probe_len = probe.len();
                let sampler = MemorySampler::start(cx.cgroup.clone());
                cx.api.bulk_insert(&cx.attacker.token, &cx.attacker.db, &table, probe).await?;
                let growth = sampler.finish().growth_bytes;
                let per_byte = growth as f64 / probe_len as f64;
                notes.push(json!({"shape": shape.name, "probe_node_bytes_per_body_byte": per_byte}));
            }
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
            for shape in ROW_SHAPES {
                let size = match cx.variant {
                    Variant::Small => shape.small_body_bytes,
                    Variant::Attack => shape.attack_body_bytes,
                };
                let (body, rows) = json_rows(size, shape.row);
                let body = Bytes::from(body);
                let path = format!("/v1/dbs/{}/events/{}/rows", cx.attacker.db, table(round, shape.name));
                let batch = (0..n).map(|i| (format!("{} insert {i}", shape.name), path.clone(), body.clone())).collect();
                requests.extend(concurrent_posts(cx, batch, &[201]).await);
                notes.push(json!({"shape": shape.name, "concurrent_inserts": n, "body_bytes": body.len(), "rows": rows}));
            }
            Ok(AttackOutcome { requests, completed: true, notes: Json::Array(notes) })
        })
    }
}
