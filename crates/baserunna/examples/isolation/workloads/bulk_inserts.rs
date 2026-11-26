//! Concurrent bulk event inserts using empty `{}` and `user_event` row shapes.
//!
//! Fixed request body sizes give every node the same attack load:
//! - Empty rows: 7,560,000 bytes. On `main`, the node used about 142 bytes of memory per body byte.
//! - User event rows: 30 MiB. On `main`, the node used about 20 bytes of memory per body byte.
//!
//! The small variant sends a smaller insert for each shape. A healthy node must accept it.
//! Setup also runs a 256 KiB probe per shape to record memory amplification.

use super::{AttackContext, AttackOutcome, Round, Variant, Workload, concurrent_posts};
use crate::cgroup::MemorySampler;
use crate::http::json_rows;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use serde_json::{Value as Json, json};

/// Row formatter and body sizes for a bulk insert row shape.
struct RowShape {
    name: &'static str,
    format_row: fn(usize) -> String,
    attack_body_bytes: usize,
    small_body_bytes: usize,
}

const ROW_SHAPES: &[RowShape] = &[
    RowShape {
        name: "empty",
        format_row: |_| "{}".to_string(),
        attack_body_bytes: 7_560_000,
        small_body_bytes: 512 << 10,
    },
    RowShape {
        name: "user_event",
        format_row: |i| {
            format!(r#"{{"time": {}, "user": "user-{:07}", "event": "e{}"}}"#, 1_749_000_000_000 + i as i64, i, i % 8)
        },
        attack_body_bytes: 30 << 20,
        small_body_bytes: 4 << 20,
    },
];

const PROBE_BODY_BYTES: usize = 256 << 10;

fn table_name(round: Round, shape: &str) -> String {
    format!("bulk{}_{shape}", round.index())
}

pub struct BulkInserts;

impl Workload for BulkInserts {
    fn prepare<'a>(&'a self, ctx: &'a AttackContext, round: Round) -> BoxFuture<'a, Result<Json, String>> {
        Box::pin(async move {
            for shape in ROW_SHAPES {
                let table_schema = json!({"name": table_name(round, shape.name), "columns": [{"name": "time", "type": "time", "default": "now"},
                    {"name": "user", "type": "string"}, {"name": "event", "type": "string"}],
                    "time_column": "time", "order_by": ["time"]});
                ctx.api.create_event_table(&ctx.attacker.token, &ctx.attacker.db, table_schema).await?;
            }
            if round == Round::WithVictim {
                return Ok(Json::Null);
            }
            let mut notes = Vec::new();
            for shape in ROW_SHAPES {
                let table = format!("probe_{}", shape.name);
                let table_schema = json!({"name": table, "columns": [{"name": "time", "type": "time", "default": "now"},
                    {"name": "user", "type": "string"}, {"name": "event", "type": "string"}],
                    "time_column": "time", "order_by": ["time"]});
                ctx.api.create_event_table(&ctx.attacker.token, &ctx.attacker.db, table_schema).await?;
                let (probe_body, _) = json_rows(PROBE_BODY_BYTES, shape.format_row);
                let probe_len = probe_body.len();
                let sampler = MemorySampler::start(ctx.cgroup.clone());
                ctx.api.bulk_insert(&ctx.attacker.token, &ctx.attacker.db, &table, probe_body).await?;
                let growth_bytes = sampler.finish().growth_bytes;
                let node_bytes_per_body_byte = growth_bytes as f64 / probe_len as f64;
                notes.push(json!({"shape": shape.name, "probe_node_bytes_per_body_byte": node_bytes_per_body_byte}));
            }
            Ok(Json::Array(notes))
        })
    }

    fn attack<'a>(&'a self, ctx: &'a AttackContext, round: Round) -> BoxFuture<'a, Result<AttackOutcome, String>> {
        Box::pin(async move {
            let concurrent_inserts = match ctx.variant {
                Variant::Small => 1,
                Variant::Attack => 4,
            };
            let mut requests = Vec::new();
            let mut notes = Vec::new();
            for shape in ROW_SHAPES {
                let body_bytes = match ctx.variant {
                    Variant::Small => shape.small_body_bytes,
                    Variant::Attack => shape.attack_body_bytes,
                };
                let (body, row_count) = json_rows(body_bytes, shape.format_row);
                let body = Bytes::from(body);
                let path = format!("/v1/dbs/{}/events/{}/rows", ctx.attacker.db, table_name(round, shape.name));
                let batch = (0..concurrent_inserts)
                    .map(|i| (format!("{} insert {i}", shape.name), path.clone(), body.clone()))
                    .collect();
                requests.extend(concurrent_posts(ctx, batch, &[201]).await);
                notes.push(json!({
                    "shape": shape.name,
                    "concurrent_inserts": concurrent_inserts,
                    "body_bytes": body.len(),
                    "rows": row_count,
                }));
            }
            Ok(AttackOutcome { requests, completed: true, notes: Json::Array(notes) })
        })
    }
}
