//! Event table routes (SPEC §4.8, §10): tables, bulk inserts as JSON or
//! NDJSON, queries and deletes.

use super::data::{PathMap, commit_hex, param, split_consistency};
use super::{DbCtx, JsonBody, Params, consistency_of, with_read, with_txn};
use crate::engine::{Committed, Consistency};
use crate::error::{Error, Result};
use crate::events::exec;
use crate::events::query::{Filter, Query};
use crate::events::schema::SkipIndex;
use crate::events::{self, ColumnSpec, CreateEvents};
use crate::node::Node;
use axum::Router;
use axum::extract::{Extension, Path, Request, State};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use http::StatusCode;
use serde::Deserialize;
use serde_json::{Value as Json, json};
use std::sync::Arc;

type J = axum::Json<Json>;

/// Routes whose requests every node serves itself instead of forwarding
/// them to the lease holder: bulk inserts stage parts where they arrive,
/// and queries read snapshots.
pub(super) fn served_locally(method: &http::Method, path: &str) -> bool {
    let segs: Vec<&str> = path.trim_end_matches('/').split('/').collect();
    method == http::Method::POST
        && segs.len() == 7
        && segs[1] == "v1"
        && segs[2] == "dbs"
        && segs[4] == "events"
        && matches!(segs[6], "rows" | "query")
}

/// Whether a request is an event query, which reads a snapshot at most
/// [`QUERY_STALENESS_MS`] old unless it asks for strong consistency.
pub(super) fn is_query(method: &http::Method, path: &str) -> bool {
    served_locally(method, path) && path.trim_end_matches('/').ends_with("/query")
}

/// The staleness of an event query's snapshot by default.
pub(super) const QUERY_STALENESS_MS: u64 = 5_000;

pub fn routes() -> Router<Arc<Node>> {
    Router::new()
        .route("/v1/dbs/{db}/events", get(list).post(create))
        .route("/v1/dbs/{db}/events/{t}", get(info).patch(configure).delete(drop_table))
        .route("/v1/dbs/{db}/events/{t}/columns", post(add_column))
        .route("/v1/dbs/{db}/events/{t}/columns/{c}", delete(drop_column))
        .route("/v1/dbs/{db}/events/{t}/skip-indexes", post(add_skip_index))
        .route("/v1/dbs/{db}/events/{t}/skip-indexes/{c}", delete(drop_skip_index))
        .route("/v1/dbs/{db}/events/{t}/rows", post(insert))
        .route("/v1/dbs/{db}/events/{t}/query", post(query))
        .route("/v1/dbs/{db}/events/{t}/delete", post(delete_rows))
}

async fn list(Extension(ctx): Extension<DbCtx>, q: Params) -> Result<J> {
    let c = consistency_of(&q)?;
    let (ts, v) = with_read!(ctx.db, c, |t| events::list(t).await);
    Ok(axum::Json(json!({"tables": ts.iter().map(|s| s.to_json()).collect::<Vec<_>>(), "version": v})))
}

async fn create(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    JsonBody(def): JsonBody<CreateEvents>,
) -> Result<Response> {
    let (s, v) = with_txn!(node, ctx, |t| events::create(t, &def).await);
    let mut out = s.to_json();
    out["version"] = json!(v);
    Ok((StatusCode::CREATED, axum::Json(out)).into_response())
}

async fn info(Extension(ctx): Extension<DbCtx>, Path(p): PathMap, q: Params) -> Result<J> {
    let name = param(&p, "t")?.to_string();
    let c = consistency_of(&q)?;
    let (out, v) = with_read!(ctx.db, c, |t| {
        let s = events::get_schema(t, &name).await?;
        let st = events::state(t, s.id).await?;
        let refs = events::refs(t, s.id).await?;
        Ok::<_, Error>(events::table_json(&s, &st, &refs))
    });
    let mut out = out;
    out["version"] = json!(v);
    Ok(axum::Json(out))
}

async fn configure(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    Path(p): PathMap,
    JsonBody(b): JsonBody<Json>,
) -> Result<J> {
    let name = param(&p, "t")?.to_string();
    let o = b.as_object().ok_or_else(|| Error::invalid("body must be a JSON object"))?;
    if let Some(k) = o.keys().find(|k| *k != "retention_ms") {
        return Err(Error::invalid(format!("unknown field {k:?}")));
    }
    let retention = match o.get("retention_ms") {
        None => return Err(Error::invalid("set retention_ms to a number or null")),
        Some(Json::Null) => None,
        Some(v) => Some(v.as_u64().ok_or_else(|| Error::invalid("retention_ms must be a number or null"))?),
    };
    let (s, v) = with_txn!(node, ctx, |t| events::set_retention(t, &name, retention).await);
    let mut out = s.to_json();
    out["version"] = json!(v);
    Ok(axum::Json(out))
}

async fn drop_table(State(node): State<Arc<Node>>, Extension(ctx): Extension<DbCtx>, Path(p): PathMap) -> Result<J> {
    let name = param(&p, "t")?.to_string();
    let ((), v) = with_txn!(node, ctx, |t| events::drop(t, &name).await);
    Ok(axum::Json(json!({"version": v})))
}

async fn add_column(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    Path(p): PathMap,
    JsonBody(c): JsonBody<ColumnSpec>,
) -> Result<J> {
    let name = param(&p, "t")?.to_string();
    let (s, v) = with_txn!(node, ctx, |t| events::add_column(t, &name, &c).await);
    let mut out = s.to_json();
    out["version"] = json!(v);
    Ok(axum::Json(out))
}

async fn drop_column(State(node): State<Arc<Node>>, Extension(ctx): Extension<DbCtx>, Path(p): PathMap) -> Result<J> {
    let (name, col) = (param(&p, "t")?.to_string(), param(&p, "c")?.to_string());
    let (s, v) = with_txn!(node, ctx, |t| events::drop_column(t, &name, &col).await);
    let mut out = s.to_json();
    out["version"] = json!(v);
    Ok(axum::Json(out))
}

async fn add_skip_index(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    Path(p): PathMap,
    JsonBody(i): JsonBody<SkipIndex>,
) -> Result<J> {
    let name = param(&p, "t")?.to_string();
    let (s, v) = with_txn!(node, ctx, |t| events::add_skip_index(t, &name, &i).await);
    let mut out = s.to_json();
    out["version"] = json!(v);
    Ok(axum::Json(out))
}

async fn drop_skip_index(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    Path(p): PathMap,
) -> Result<J> {
    let (name, col) = (param(&p, "t")?.to_string(), param(&p, "c")?.to_string());
    let (s, v) = with_txn!(node, ctx, |t| events::drop_skip_index(t, &name, &col).await);
    let mut out = s.to_json();
    out["version"] = json!(v);
    Ok(axum::Json(out))
}

/// Parses an insert body: `{"rows": [...]}`, an array of events, or NDJSON.
fn parse_rows(content_type: &str, data: &[u8]) -> Result<(Vec<Json>, Option<String>)> {
    match content_type {
        "" | "application/json" => match serde_json::from_slice::<Json>(data) {
            Ok(Json::Array(rows)) => Ok((rows, None)),
            Ok(Json::Object(mut o)) => {
                let key = match o.remove("idempotency_key") {
                    None => None,
                    Some(Json::String(k)) => Some(k),
                    Some(_) => return Err(Error::invalid("idempotency_key is a string")),
                };
                let rows = match o.remove("rows") {
                    Some(Json::Array(r)) => r,
                    _ => return Err(Error::invalid("the body is {\"rows\": [...]}, an array of events, or NDJSON")),
                };
                if let Some(k) = o.keys().next() {
                    return Err(Error::invalid(format!("unknown field {k:?}")));
                }
                Ok((rows, key))
            }
            Ok(_) => Err(Error::invalid("the body is {\"rows\": [...]}, an array of events, or NDJSON")),
            Err(e) => Err(Error::invalid(format!("invalid JSON body: {e}"))),
        },
        "application/x-ndjson" | "application/jsonl" | "application/json-lines" => {
            let text = std::str::from_utf8(data).map_err(|_| Error::invalid("NDJSON must be UTF-8"))?;
            let text = text.strip_prefix('\u{feff}').unwrap_or(text);
            let rows = text
                .lines()
                .enumerate()
                .filter(|(_, l)| !l.trim().is_empty())
                .map(|(i, l)| serde_json::from_str(l).map_err(|e| Error::invalid(format!("line {}: {e}", i + 1))))
                .collect::<Result<Vec<Json>>>()?;
            Ok((rows, None))
        }
        other => Err(Error::invalid(format!(
            "unsupported content type {other:?}: use application/json or application/x-ndjson"
        ))),
    }
}

async fn insert(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    Path(p): PathMap,
    req: Request,
) -> Result<Response> {
    ctx.require_write()?;
    let name = param(&p, "t")?.to_string();
    let ct = req
        .headers()
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    let header_key = match req.headers().get("idempotency-key") {
        None => None,
        Some(v) => Some(v.to_str().map_err(|_| Error::invalid("Idempotency-Key must be visible ASCII"))?.to_string()),
    };
    let limit = node.cfg.max_json_body;
    let data = axum::body::to_bytes(req.into_body(), limit)
        .await
        .map_err(|_| Error::TooLarge(format!("bulk inserts are limited to {limit} bytes")))?;
    let (rows, body_key) = parse_rows(&ct, &data)?;
    let key = header_key.or(body_key);
    if rows.is_empty() {
        return Err(Error::invalid("insert at least one event"));
    }
    let commit_id = super::commit_id_for(&node, &ctx, key.as_deref())?;
    if let Some(c) = super::earlier_attempt(&ctx, commit_id, key.as_deref()).await? {
        return Ok(inserted(rows.len(), &c, true, commit_id));
    }
    let (schema, _) = with_read!(ctx.db, Consistency::Strong, |t| events::get_schema(t, &name).await);
    let valid = events::validate_rows(&schema, &rows, node.now_ms() as i64)?;
    let n = valid.len();
    let c = node
        .event_buffers()
        .submit(ctx.db.clone(), schema, valid, commit_id, ctx.deadline.instant())
        .await
        .map_err(|e| super::naming_commit(e, commit_id))?;
    Ok(inserted(n, &c, c.replayed, commit_id))
}

fn inserted(count: usize, c: &Committed, replayed: bool, commit_id: u64) -> Response {
    let status = if replayed { StatusCode::OK } else { StatusCode::CREATED };
    let body = json!({
        "count": count, "version": c.version, "time": c.time_ms, "commit_id": commit_hex(commit_id), "replayed": replayed,
    });
    (status, axum::Json(body)).into_response()
}

/// Splits a query body into the query and its consistency, which defaults
/// to a snapshot at most 5 s old so that queries do not load the lease holder.
fn query_consistency(b: Json) -> Result<(Json, Consistency)> {
    let explicit = b.get("consistency").is_some();
    let (b, c) = split_consistency(b)?;
    Ok((b, if explicit { c } else { Consistency::Snapshot { max_staleness_ms: QUERY_STALENESS_MS } }))
}

async fn query(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    Path(p): PathMap,
    JsonBody(b): JsonBody<Json>,
) -> Result<J> {
    let name = param(&p, "t")?.to_string();
    let (b, c) = query_consistency(b)?;
    let q = Query::parse(b)?;
    let limits = node.cfg.query_limits;
    let _slot = node.query_slot().await?;
    let run = async |c: Consistency| -> Result<(exec::Output, u64)> {
        let db = ctx.db.clone();
        Ok(with_read!(ctx.db, c, |t| {
            let s = events::get_schema(t, &name).await?;
            exec::run(db.shared(), &db.id, t, &s, &q, &limits).await
        }))
    };
    // A snapshot may predate the table or a column the caller just added.
    let (out, v) = match run(c).await {
        Err(Error::NotFound(_) | Error::InvalidArgument(_)) if matches!(c, Consistency::Snapshot { .. }) => {
            run(Consistency::Strong).await?
        }
        r => r?,
    };
    Ok(axum::Json(output_json(&out, v)))
}

/// The JSON form of a query's answer at version `v`.
pub fn output_json(out: &exec::Output, v: u64) -> Json {
    json!({
        "columns": out.columns,
        "rows": out.rows,
        "next_cursor": out.next_cursor,
        "version": v,
        "stats": out.stats,
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeleteBody {
    #[serde(default)]
    pub from: Option<Json>,
    #[serde(default)]
    pub to: Option<Json>,
    #[serde(rename = "where")]
    pub filter: Filter,
}

async fn delete_rows(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    Path(p): PathMap,
    JsonBody(b): JsonBody<DeleteBody>,
) -> Result<J> {
    let name = param(&p, "t")?.to_string();
    let (from, to) = events::time_bounds(&b.from, &b.to)?;
    let db = ctx.db.clone();
    let (n, v) = with_txn!(node, ctx, |t| events::maint::delete(&db, t, &name, from, to, &b.filter).await);
    Ok(axum::Json(json!({"deleted": n, "version": v})))
}

/// The body of `POST /internal/v1/events/bind`: a bind forwarded to the lease holder.
#[derive(serde::Serialize, Deserialize)]
pub(crate) struct ForwardedBind {
    pub db: String,
    /// The bind's binary encoding, as base64.
    pub bind: String,
}

/// Encodes each slot's outcome of a bind.
pub(crate) fn bind_results_json(rs: &[Result<Committed>]) -> Json {
    Json::Array(
        rs.iter()
            .map(|r| match r {
                Ok(c) => json!({"version": c.version, "time": c.time_ms, "replayed": c.replayed}),
                Err(e) => json!({"error": {"code": e.code(), "message": e.message(), "status": e.status(),
                    "details": match e { Error::Unavailable { outcome_unknown: true, .. } => json!({"outcome": "unknown"}), _ => json!({}) }}}),
            })
            .collect(),
    )
}

pub(crate) fn bind_results_from_json(j: &Json, n: usize) -> Result<Vec<Result<Committed>>> {
    let a = j.as_array().filter(|a| a.len() == n).ok_or_else(|| Error::internal("a forwarded bind's answer"))?;
    Ok(a.iter()
        .map(|r| match r.get("error") {
            Some(e) => Err(super::error_from_json(e["status"].as_u64().unwrap_or(500) as u16, &json!({"error": e}))),
            None => Ok(Committed {
                version: r["version"].as_u64().unwrap_or(0),
                time_ms: r["time"].as_u64().unwrap_or(0),
                replayed: r["replayed"].as_bool().unwrap_or(false),
                appends: Vec::new(),
            }),
        })
        .collect())
}

/// Handles `POST /internal/v1/events/bind` on the lease holder.
pub(super) async fn internal_bind(
    State(node): State<Arc<Node>>,
    axum::Json(b): axum::Json<ForwardedBind>,
) -> Result<J> {
    let data = crate::util::b64::decode(&b.bind).ok_or_else(|| Error::invalid("bad bind encoding"))?;
    let bind = crate::engine::events::Bind::decode(&data).map_err(|e| Error::invalid(e.to_string()))?;
    let db = node.engine.db(&b.db);
    db.open(false).await?;
    let rs = db.bind(bind).await?;
    Ok(axum::Json(bind_results_json(&rs)))
}
