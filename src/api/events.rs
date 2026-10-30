//! HTTP routes for event tables (SPEC §4.8, §10).

use super::data::{
    PathMap, commit_hex, content_type, idempotency_header, one_idempotency_key, param, split_consistency,
};
use super::{DbCtx, JsonBody, Params, consistency_of, name_paging, with_read, with_txn};
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

/// Returns `true` if a request is an event bulk insert or query, which can be
/// served by any node without forwarding to the lease holder.
pub(super) fn served_locally(method: &http::Method, path: &str) -> bool {
    matches!(event_rows_route(method, path).as_deref(), Some([] | ["query"]))
}

pub(super) fn is_query(method: &http::Method, path: &str) -> bool {
    matches!(event_rows_route(method, path).as_deref(), Some(["query"]))
}

fn event_rows_route<'a>(method: &http::Method, path: &'a str) -> Option<Vec<&'a str>> {
    let segs: Vec<&str> = path.trim_end_matches('/').split('/').collect();
    let route = method == http::Method::POST
        && segs.len() >= 7
        && segs[..3] == ["", "v1", "dbs"]
        && segs[4] == "events"
        && segs[6] == "rows";
    route.then(|| segs[7..].to_vec())
}

/// Default maximum snapshot staleness for event queries, in milliseconds.
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
        .route("/v1/dbs/{db}/events/{t}/rows/query", post(query))
        .route("/v1/dbs/{db}/events/{t}/rows/delete", post(delete_rows))
}

async fn list(Extension(ctx): Extension<DbCtx>, q: Params) -> Result<J> {
    let (prefix, limit, cursor) = name_paging(&q)?;
    let c = consistency_of(&q)?;
    let ((ts, next), v) = with_read!(ctx.db, c, |t| events::list(t, &prefix, limit, cursor.as_deref()).await);
    let tables: Vec<Json> = ts.iter().map(|s| s.to_json()).collect();
    Ok(axum::Json(json!({"tables": tables, "next_cursor": next, "version": v})))
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

const BODY_SHAPE: &str = "the body is {\"rows\": [...]}, an array of events, or NDJSON";

/// Streams events from a JSON or NDJSON insert body into `f` without buffering
/// the parsed array, and returns the body's `idempotency_key` if present.
fn each_event(content_type: &str, data: &[u8], f: &mut dyn FnMut(Json) -> Result<()>) -> Result<Option<String>> {
    crate::util::json::reject_inexact_integers(data).map_err(Error::invalid)?;
    match content_type {
        "" | "application/json" => {
            let mut failed = None;
            let mut de = serde_json::Deserializer::from_slice(data);
            let parsed = serde::de::DeserializeSeed::deserialize(Body { f, failed: &mut failed }, &mut de)
                .and_then(|key| de.end().map(|()| key));
            match (failed, parsed) {
                (Some(e), _) => Err(e),
                (None, Ok(key)) => Ok(key),
                (None, Err(e)) => Err(Error::invalid(format!("invalid JSON body: {e}"))),
            }
        }
        "application/x-ndjson" | "application/jsonl" | "application/json-lines" => {
            let text = std::str::from_utf8(data).map_err(|_| Error::invalid("NDJSON must be UTF-8"))?;
            let text = text.strip_prefix('\u{feff}').unwrap_or(text);
            for (i, l) in text.lines().enumerate().filter(|(_, l)| !l.trim().is_empty()) {
                f(serde_json::from_str(l).map_err(|e| Error::invalid(format!("line {}: {e}", i + 1)))?)?;
            }
            Ok(None)
        }
        other => Err(Error::invalid(format!(
            "unsupported content type {other:?}: use application/json or application/x-ndjson"
        ))),
    }
}

struct Events<'a> {
    f: &'a mut dyn FnMut(Json) -> Result<()>,
    failed: &'a mut Option<Error>,
}

impl<'de> serde::de::DeserializeSeed<'de> for Events<'_> {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> std::result::Result<(), D::Error> {
        d.deserialize_seq(self)
    }
}

impl<'de> serde::de::Visitor<'de> for Events<'_> {
    type Value = ();
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("an array of events")
    }
    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> std::result::Result<(), A::Error> {
        while let Some(v) = seq.next_element::<Json>()? {
            if let Err(e) = (self.f)(v) {
                *self.failed = Some(e);
                return Err(serde::de::Error::custom("rejected"));
            }
        }
        Ok(())
    }
}

struct Body<'a> {
    f: &'a mut dyn FnMut(Json) -> Result<()>,
    failed: &'a mut Option<Error>,
}

impl Body<'_> {
    fn serde_err<E: serde::de::Error>(self, e: Error) -> E {
        *self.failed = Some(e);
        E::custom("rejected")
    }
}

impl<'de> serde::de::DeserializeSeed<'de> for Body<'_> {
    type Value = Option<String>;
    fn deserialize<D: serde::Deserializer<'de>>(self, d: D) -> std::result::Result<Option<String>, D::Error> {
        d.deserialize_any(self)
    }
}

impl<'de> serde::de::Visitor<'de> for Body<'_> {
    type Value = Option<String>;
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(BODY_SHAPE)
    }
    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, seq: A) -> std::result::Result<Option<String>, A::Error> {
        serde::de::Visitor::visit_seq(Events { f: self.f, failed: self.failed }, seq).map(|()| None)
    }
    fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> std::result::Result<Option<String>, A::Error> {
        let (mut key, mut rows, mut keyed) = (None, false, false);
        while let Some(k) = map.next_key::<String>()? {
            if (k == "rows" && rows) || (k == "idempotency_key" && keyed) {
                return Err(self.serde_err(Error::invalid(format!("{k:?} appears twice"))));
            }
            match k.as_str() {
                "rows" => {
                    map.next_value_seed(Events { f: &mut *self.f, failed: &mut *self.failed })?;
                    rows = true;
                }
                "idempotency_key" => {
                    keyed = true;
                    match map.next_value::<Json>()? {
                        Json::Null => key = None,
                        Json::String(k) => key = Some(k),
                        _ => return Err(self.serde_err(Error::invalid("idempotency_key is a string"))),
                    }
                }
                other => return Err(self.serde_err(Error::invalid(format!("unknown field {other:?}")))),
            }
        }
        if !rows {
            return Err(self.serde_err(Error::invalid(BODY_SHAPE)));
        }
        Ok(key)
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
    let ct = content_type(req.headers());
    let header_key = idempotency_header(req.headers())?;
    let limit = node.cfg.max_json_body;
    let data = axum::body::to_bytes(req.into_body(), limit)
        .await
        .map_err(|_| Error::TooLarge(format!("bulk inserts are limited to {limit} bytes")))?;
    // Extract the idempotency key first so a replay returns before validating rows.
    let body_key = each_event(&ct, &data, &mut |_| Ok(()))?;
    let key = one_idempotency_key(header_key, body_key)?;
    let commit_id = super::commit_id_for(&node, &ctx, key.as_deref())?;
    if let Some(c) = super::earlier_attempt(&ctx, commit_id, key.as_deref()).await? {
        return inserted(&c, commit_id);
    }
    let (schema, _) = with_read!(ctx.db, Consistency::Strong, |t| events::get_schema(t, &name).await);
    let now = node.now_ms() as i64;
    // Encoded column defaults can expand small JSON objects up to ~5x.
    let mut valid = events::ValidRows::up_to((8 * data.len()).max(limit));
    each_event(&ct, &data, &mut |v| valid.push(&schema, &v, now))?;
    drop(data);
    if valid.is_empty() {
        return Err(Error::invalid("insert at least one event"));
    }
    let c = node
        .event_buffers()
        .submit(ctx.db.clone(), schema, valid, commit_id, ctx.deadline.instant())
        .await
        .map_err(|e| super::naming_commit(e, commit_id))?;
    inserted(&c, commit_id)
}

fn inserted(c: &Committed, commit_id: u64) -> Result<Response> {
    let Some(count) = c.inserted else {
        return Err(Error::invalid("the idempotency key was used by a request other than an event insert"));
    };
    let status = if c.replayed { StatusCode::OK } else { StatusCode::CREATED };
    let body = json!({
        "count": count, "version": c.version, "time": c.time_ms, "commit_id": commit_hex(commit_id),
        "replayed": c.replayed,
    });
    Ok((status, axum::Json(body)).into_response())
}

/// Extracts consistency settings from an event query body, defaulting to a 5 s snapshot.
fn query_consistency(mut b: Json) -> Result<(Json, Consistency)> {
    if !b.is_object() {
        return Err(Error::invalid("the query is a JSON object"));
    }
    if b.get("consistency").is_none_or(Json::is_null) {
        b["consistency"] = json!("snapshot");
        if b.get("max_staleness_ms").is_none_or(Json::is_null) {
            b["max_staleness_ms"] = json!(QUERY_STALENESS_MS);
        }
    }
    split_consistency(b)
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
    // Retry with strong consistency if the local snapshot predates a new table or column.
    let (out, v) = match run(c).await {
        Err(Error::NotFound(_) | Error::InvalidArgument(_)) if matches!(c, Consistency::Snapshot { .. }) => {
            run(Consistency::Strong).await?
        }
        r => r?,
    };
    Ok(axum::Json(output_json(&out, v)))
}

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

/// Request body for `POST /internal/v1/events/bind`.
#[derive(serde::Serialize, Deserialize)]
pub(crate) struct ForwardedBind {
    pub db: String,
    /// Base64-encoded [`crate::engine::events::Bind`].
    pub bind: String,
}

pub(crate) fn bind_results_json(rs: &[Result<Committed>]) -> Json {
    Json::Array(
        rs.iter()
            .map(|r| match r {
                Ok(c) => {
                    json!({"version": c.version, "time": c.time_ms, "replayed": c.replayed, "inserted": c.inserted})
                }
                Err(e) => {
                    let mut err = super::error_json(e);
                    err["status"] = json!(e.status());
                    json!({"error": err})
                }
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
                inserted: r["inserted"].as_u64(),
                appends: Vec::new(),
            }),
        })
        .collect())
}

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
