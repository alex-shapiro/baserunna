//! Database-scoped routes: databases, KV, blobs, transactions and tables.

use super::body::{BlobBody, Upload, parse_range, read_upload};
use super::{Auth, DbCtx, JsonBody, Params, consistency_of, percent_decode, with_read, with_txn};
use crate::engine::manifest::ObjKind;
use crate::engine::{Change, ChangePos, Consistency, Isolation, Value, blob};
use crate::error::{Error, Result};
use crate::meta::{self, MetaCmd};
use crate::node::{Node, OpenTxn};
use crate::ops::{self, Encoding, Op, OpCtx, exec_ops, user_key};
use crate::table::{self, BlobResolver, ColumnSpec, CreateTable, IndexSpec, InsertMode, Query, Selection};
use axum::Router;
use axum::body::Body;
use axum::extract::{Extension, Path, Request, State};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use http::{HeaderMap, HeaderValue, StatusCode};
use serde::Deserialize;
use serde_json::{Value as Json, json};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

type J = axum::Json<Json>;

pub fn routes() -> Router<Arc<Node>> {
    Router::new()
        .route("/v1/dbs/{db}", get(get_db).delete(delete_db))
        .route("/v1/dbs/{db}/kv", get(scan_kv))
        .route("/v1/dbs/{db}/changes", get(kv_changes))
        .route("/v1/dbs/{db}/kv/{*key}", get(get_kv).put(put_kv).delete(delete_kv))
        .route("/v1/dbs/{db}/blobs", post(stage_blob))
        .route("/v1/dbs/{db}/blobs/{blob_id}", get(get_blob))
        .route("/v1/dbs/{db}/txn", post(one_shot))
        .route("/v1/dbs/{db}/transactions", post(begin_txn))
        .route("/v1/dbs/{db}/transactions/{id}", delete(abort_txn))
        .route("/v1/dbs/{db}/transactions/{id}/ops", post(txn_ops))
        .route("/v1/dbs/{db}/transactions/{id}/commit", post(commit_txn))
        .route("/v1/dbs/{db}/commits/{id}", get(commit_status))
        .route("/v1/dbs/{db}/tables", get(list_tables).post(create_table))
        .route("/v1/dbs/{db}/tables/{t}", get(get_table).delete(drop_table))
        .route("/v1/dbs/{db}/tables/{t}/columns", post(add_column))
        .route("/v1/dbs/{db}/tables/{t}/columns/{c}", delete(drop_column))
        .route("/v1/dbs/{db}/tables/{t}/indexes", post(add_index))
        .route("/v1/dbs/{db}/tables/{t}/indexes/{i}", delete(drop_index))
        .route("/v1/dbs/{db}/tables/{t}/rows", post(insert_rows))
        .route("/v1/dbs/{db}/tables/{t}/rows/get", post(get_row))
        .route("/v1/dbs/{db}/tables/{t}/rows/update", post(update_row))
        .route("/v1/dbs/{db}/tables/{t}/rows/delete", post(delete_row))
        .route("/v1/dbs/{db}/tables/{t}/rows/query", post(query_rows))
        .route("/v1/dbs/{db}/tables/{t}/changes", get(row_changes))
}

type PathMap = Path<HashMap<String, String>>;

fn param<'a>(p: &'a HashMap<String, String>, k: &str) -> Result<&'a str> {
    p.get(k).map(|s| s.as_str()).ok_or_else(|| Error::invalid(format!("missing path parameter {k}")))
}

fn op_ctx<'a>(node: &Node, ctx: &'a DbCtx, enc: Encoding) -> OpCtx<'a> {
    OpCtx { db: &ctx.db, encoding: enc, read_only: ctx.principal.read_only(), json_value_max: node.cfg.json_value_max }
}

// ------------------------------------------------------------ databases

pub async fn list_dbs(State(node): State<Arc<Node>>, Auth(p): Auth) -> Result<J> {
    let mut t = node.sys_read_strong().await?;
    let mut dbs = meta::list_dbs(&mut t, &p.user.id).await?;
    if let Some(scope) = p.scoped_db() {
        dbs.retain(|d| d.id == scope);
    }
    let out: Vec<Json> = dbs.iter().map(|d| json!({"id": d.id, "name": d.name, "created_at": d.created_at})).collect();
    Ok(axum::Json(json!({"databases": out})))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateDb {
    name: String,
}

pub async fn create_db(
    State(node): State<Arc<Node>>,
    Auth(p): Auth,
    JsonBody(c): JsonBody<CreateDb>,
) -> Result<Response> {
    p.require_writable()?;
    p.require_unscoped()?;
    let create = MetaCmd::CreateDb {
        id: node.env.random_hex(10),
        owner_id: p.user.id.clone(),
        name: c.name.clone(),
        max_dbs: node.cfg.max_dbs_per_user,
    };
    let r = match node.run_meta(create.clone()).await {
        Err(Error::AlreadyExists(m)) => {
            // The name may belong to a database whose deletion stopped after
            // flagging its manifest deleted: finish it, then create.
            let existing = node.resolve_db_strong(&p, &c.name).await?;
            let db = node.open_db(&existing).await?;
            let deleted =
                db.read_version(Consistency::Strong).await.is_err() && db.current().is_some_and(|v| v.m.is_deleted());
            match deleted {
                true => {
                    node.run_meta(MetaCmd::DeleteDb { id: existing.id.clone() }).await?;
                    node.forget_db_name(&p.user.id, &c.name);
                    node.run_meta(create).await?
                }
                false => return Err(Error::AlreadyExists(m)),
            }
        }
        other => other?,
    };
    Ok((StatusCode::CREATED, axum::Json(r)).into_response())
}

async fn get_db(State(_node): State<Arc<Node>>, Extension(ctx): Extension<DbCtx>) -> Result<J> {
    let v = ctx.db.read_version(Consistency::Snapshot { max_staleness_ms: 5000 }).await?;
    let st = ctx.db.stats();
    Ok(axum::Json(json!({
        "id": ctx.rec.id,
        "name": ctx.rec.name,
        "created_at": ctx.rec.created_at,
        "stats": {
            "version": v.version(),
            "segments": st.as_ref().map(|s| s.segments),
            "segment_bytes": st.as_ref().map(|s| s.segment_bytes),
            "tail_bytes": st.as_ref().map(|s| s.tail_bytes),
            "lease_holder": st.as_ref().and_then(|s| s.lease_holder.clone()),
            "size_bytes": st.as_ref().map(|s| s.size_bytes),
            "quota_bytes": st.as_ref().and_then(|s| s.quota_bytes),
        }
    })))
}

async fn delete_db(State(node): State<Arc<Node>>, Extension(ctx): Extension<DbCtx>) -> Result<J> {
    ctx.principal.require_writable()?;
    ctx.principal.require_unscoped()?;
    ctx.db.mark_deleted().await?;
    node.run_meta(MetaCmd::DeleteDb { id: ctx.rec.id.clone() }).await?;
    node.forget_db_name(&ctx.rec.owner_id, &ctx.rec.name);
    node.engine.forget(&ctx.rec.id);
    Ok(axum::Json(json!({})))
}

// ------------------------------------------------------------------ kv

fn encoding_param(q: &Params) -> Result<Encoding> {
    match q.get("encoding").map(|s| s.as_str()) {
        None | Some("utf8") => Ok(Encoding::Utf8),
        Some("base64") => Ok(Encoding::Base64),
        Some(o) => Err(Error::invalid(format!("unknown encoding {o:?}"))),
    }
}

fn bool_param(q: &Params, k: &str) -> Result<bool> {
    match q.get(k).map(|s| s.as_str()) {
        None | Some("false") | Some("0") => Ok(false),
        Some("true") | Some("1") | Some("") => Ok(true),
        Some(o) => Err(Error::invalid(format!("bad boolean for {k}: {o:?}"))),
    }
}

async fn scan_kv(State(node): State<Arc<Node>>, Extension(ctx): Extension<DbCtx>, q: Params) -> Result<J> {
    let enc = encoding_param(&q)?;
    let op = Op::Scan {
        prefix: q.get("prefix").cloned(),
        start: q.get("start").cloned(),
        end: q.get("end").cloned(),
        limit: q.get("limit").map(|l| l.parse().map_err(|_| Error::invalid("bad limit"))).transpose()?,
        reverse: bool_param(&q, "reverse")?,
        keys_only: bool_param(&q, "keys_only")?,
        cursor: q.get("cursor").cloned(),
    };
    let c = consistency_of(&q)?;
    let octx = op_ctx(&node, &ctx, enc);
    let (mut r, v) = with_read!(ctx.db, c, |t| exec_ops(&octx, t, std::slice::from_ref(&op)).await);
    let mut out = r.pop().unwrap_or(Json::Null);
    out["version"] = json!(v);
    Ok(axum::Json(out))
}

/// The raw key from `/v1/dbs/{db}/kv/{key...}` (percent-decoded bytes).
fn raw_key(path: &str) -> Result<Vec<u8>> {
    let i = path.find("/kv/").ok_or_else(|| Error::invalid("missing key"))?;
    let k = percent_decode(&path[i + 4..]);
    if k.is_empty() {
        return Err(Error::invalid("missing key"));
    }
    Ok(k)
}

fn etag_version(h: &HeaderMap, name: http::HeaderName) -> Option<String> {
    h.get(name).and_then(|v| v.to_str().ok()).map(|s| s.trim().trim_start_matches("W/").trim_matches('"').to_string())
}

async fn get_kv(Extension(ctx): Extension<DbCtx>, q: Params, req: Request) -> Result<Response> {
    let key = user_key(&raw_key(req.uri().path())?)?;
    let c = consistency_of(&q)?;
    let snap = ctx.db.snapshot(c).await?;
    let Some(e) = snap.get(&key).await? else {
        return Err(Error::not_found("key not found"));
    };
    let range_h = req.headers().get(http::header::RANGE).and_then(|v| v.to_str().ok()).map(|s| s.to_string());
    let (size, ct) = match &e.value {
        Value::Inline(b) => (b.len() as u64, "application/octet-stream".to_string()),
        Value::Blob(r) => (r.size, r.content_type.clone()),
        Value::Tombstone => return Err(Error::not_found("key not found")),
    };
    let range = match parse_range(range_h.as_deref(), size) {
        Ok(r) => r,
        Err(_) => {
            let mut resp = StatusCode::RANGE_NOT_SATISFIABLE.into_response();
            if let Ok(v) = HeaderValue::from_str(&format!("bytes */{size}")) {
                resp.headers_mut().insert(http::header::CONTENT_RANGE, v);
            }
            return Ok(resp);
        }
    };
    let r = range.clone().unwrap_or(0..size);
    let body = match &e.value {
        Value::Inline(b) => Body::from(b.slice(r.start as usize..r.end as usize)),
        Value::Blob(bref) => Body::new(BlobBody::new(ctx.db.shared().clone(), &ctx.db.id, bref.clone(), r.clone())),
        Value::Tombstone => Body::empty(),
    };
    let mut resp = Response::new(body);
    if range.is_some() {
        *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
        if let Ok(v) = HeaderValue::from_str(&format!("bytes {}-{}/{}", r.start, r.end.saturating_sub(1), size)) {
            resp.headers_mut().insert(http::header::CONTENT_RANGE, v);
        }
    }
    let h = resp.headers_mut();
    h.insert(http::header::CONTENT_LENGTH, HeaderValue::from(r.end - r.start));
    h.insert(http::header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if let Ok(v) = HeaderValue::from_str(&ct) {
        h.insert(http::header::CONTENT_TYPE, v);
    }
    if let Ok(v) = HeaderValue::from_str(&format!("\"{}\"", e.seq)) {
        h.insert(http::header::ETAG, v);
    }
    h.insert("x-baserunna-version", HeaderValue::from(e.seq));
    if let Some(x) = e.expires {
        h.insert("x-baserunna-expires-at", HeaderValue::from(x));
    }
    Ok(resp)
}

async fn put_kv(State(node): State<Arc<Node>>, Extension(ctx): Extension<DbCtx>, q: Params, req: Request) -> Result<J> {
    ctx.principal.require_writable()?;
    let key = user_key(&raw_key(req.uri().path())?)?;
    let ttl_ms = q.get("ttl_ms").map(|v| v.parse::<u64>().map_err(|_| Error::invalid("bad ttl_ms"))).transpose()?;
    let if_match = etag_version(req.headers(), http::header::IF_MATCH);
    let if_none = etag_version(req.headers(), http::header::IF_NONE_MATCH);
    if if_none.as_deref().is_some_and(|v| v != "*") {
        return Err(Error::invalid("only If-None-Match: * is supported"));
    }
    let want_version: Option<u64> = match &if_match {
        None => None,
        Some(v) => Some(v.parse().map_err(|_| Error::invalid("If-Match must be a version"))?),
    };
    let ct = req
        .headers()
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();
    let db = ctx.db.clone();
    let inline_max = db.shared().cfg.value_inline_max;
    let up = read_upload(req.into_body(), inline_max, false, || db.blob_writer(&ct)).await?;
    let value = match up {
        Upload::Small(b) => Value::Inline(b),
        Upload::Blob(r) => Value::Blob(r),
    };
    let ((), v) = with_txn!(node, ctx, |t| {
        if want_version.is_some() || if_none.is_some() {
            let cur = t.get(&key).await?;
            if if_none.is_some() && cur.is_some() {
                return Err(Error::PreconditionFailed { message: "key already exists".into(), op_index: None });
            }
            if let Some(w) = want_version
                && cur.as_ref().and_then(|c| c.seq) != Some(w)
            {
                return Err(Error::PreconditionFailed { message: "version mismatch".into(), op_index: None });
            }
        }
        t.put_expiring(key.clone(), value.clone(), ops::expiry(ttl_ms)?)
    });
    Ok(axum::Json(json!({"version": v})))
}

async fn delete_kv(State(node): State<Arc<Node>>, Extension(ctx): Extension<DbCtx>, req: Request) -> Result<J> {
    let key = user_key(&raw_key(req.uri().path())?)?;
    let if_match = etag_version(req.headers(), http::header::IF_MATCH);
    let want: Option<u64> = match &if_match {
        None => None,
        Some(v) => Some(v.parse().map_err(|_| Error::invalid("If-Match must be a version"))?),
    };
    let (existed, v) = with_txn!(node, ctx, |t| {
        let cur = t.get(&key).await?;
        if let Some(w) = want
            && cur.as_ref().and_then(|c| c.seq) != Some(w)
        {
            return Err(Error::PreconditionFailed { message: "version mismatch".into(), op_index: None });
        }
        t.delete(key.clone())?;
        Ok::<bool, Error>(cur.is_some())
    });
    Ok(axum::Json(json!({"version": v, "existed": existed})))
}

// ---------------------------------------------------------------- blobs

async fn stage_blob(Extension(ctx): Extension<DbCtx>, req: Request) -> Result<Response> {
    ctx.principal.require_writable()?;
    let ct = req
        .headers()
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();
    let db = ctx.db.clone();
    let up = read_upload(req.into_body(), 0, true, || db.blob_writer(&ct)).await?;
    let Upload::Blob(r) = up else {
        return Err(Error::internal("expected a blob"));
    };
    Ok((StatusCode::CREATED, axum::Json(ops::blob_meta(&r))).into_response())
}

async fn get_blob(Extension(ctx): Extension<DbCtx>, Path(p): PathMap, req: Request) -> Result<Response> {
    let id = param(&p, "blob_id")?;
    let r = blob::load_staged(ctx.db.shared(), &ctx.db.id, id).await?;
    // A blob read by id may be unreferenced already, and collected soon.
    let doomed = ctx
        .db
        .read_version(Consistency::Strong)
        .await?
        .m
        .obsolete
        .iter()
        .any(|o| o.kind == ObjKind::Blob && o.id == r.id);
    let range_h = req.headers().get(http::header::RANGE).and_then(|v| v.to_str().ok());
    let range = match parse_range(range_h, r.size) {
        Ok(x) => x,
        Err(_) => return Ok(StatusCode::RANGE_NOT_SATISFIABLE.into_response()),
    };
    let rr = range.clone().unwrap_or(0..r.size);
    let mut body = BlobBody::new(ctx.db.shared().clone(), &ctx.db.id, r.clone(), rr.clone());
    if doomed {
        body = body.pinned_first();
    }
    let mut resp = Response::new(Body::new(body));
    if range.is_some() {
        *resp.status_mut() = StatusCode::PARTIAL_CONTENT;
        if let Ok(v) = HeaderValue::from_str(&format!("bytes {}-{}/{}", rr.start, rr.end.saturating_sub(1), r.size)) {
            resp.headers_mut().insert(http::header::CONTENT_RANGE, v);
        }
    }
    let h = resp.headers_mut();
    h.insert(http::header::CONTENT_LENGTH, HeaderValue::from(rr.end - rr.start));
    h.insert(http::header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if let Ok(v) = HeaderValue::from_str(&r.content_type) {
        h.insert(http::header::CONTENT_TYPE, v);
    }
    Ok(resp)
}

// --------------------------------------------------------- transactions

#[derive(Deserialize, Clone, Copy, Default)]
#[serde(rename_all = "lowercase")]
enum IsolationParam {
    #[default]
    Serializable,
    Snapshot,
}

impl From<IsolationParam> for Isolation {
    fn from(i: IsolationParam) -> Isolation {
        match i {
            IsolationParam::Serializable => Isolation::Serializable,
            IsolationParam::Snapshot => Isolation::Snapshot,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OneShot {
    ops: Vec<Op>,
    #[serde(default)]
    encoding: Encoding,
    #[serde(default)]
    isolation: IsolationParam,
    #[serde(default)]
    idempotency_key: Option<String>,
    #[serde(default)]
    consistency: Option<String>,
    #[serde(default)]
    max_staleness_ms: Option<u64>,
}

fn consistency_from(c: Option<&str>, staleness: Option<u64>) -> Result<Consistency> {
    let mut m = std::collections::BTreeMap::new();
    if let Some(c) = c {
        m.insert("consistency".to_string(), c.to_string());
    }
    if let Some(s) = staleness {
        m.insert("max_staleness_ms".to_string(), s.to_string());
    }
    consistency_of(&m)
}

fn commit_id_for_key(db_id: &str, key: &str) -> u64 {
    let mut d = Vec::with_capacity(db_id.len() + key.len() + 1);
    d.extend_from_slice(db_id.as_bytes());
    d.push(0);
    d.extend_from_slice(key.as_bytes());
    let h = crate::util::sha256(&d);
    let mut a = [0u8; 8];
    a.copy_from_slice(&h[..8]);
    u64::from_be_bytes(a)
}

fn commit_hex(id: u64) -> String {
    format!("{id:016x}")
}

async fn one_shot(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    JsonBody(req): JsonBody<OneShot>,
) -> Result<J> {
    let writes = req.ops.iter().any(|o| o.is_write());
    if writes {
        ctx.principal.require_writable()?;
    }
    if let Some(k) = &req.idempotency_key
        && (k.is_empty() || k.len() > 200)
    {
        return Err(Error::invalid("idempotency_key must be 1-200 characters"));
    }
    let commit_id = match &req.idempotency_key {
        Some(k) => commit_id_for_key(&ctx.rec.id, k),
        None => node.env.random_u64(),
    };
    if req.idempotency_key.is_some()
        && let Some(v) = ctx.db.commit_status(commit_id).await?
    {
        return Ok(axum::Json(json!({
            "version": v, "committed": true, "replayed": true,
            "commit_id": commit_hex(commit_id), "results": Json::Null,
        })));
    }
    let deadline = ctx.deadline;
    let c = consistency_from(req.consistency.as_deref(), req.max_staleness_ms)?;
    if writes && c != Consistency::Strong {
        return Err(Error::invalid("transactions with writes must use strong consistency"));
    }
    let octx = op_ctx(&node, &ctx, req.encoding);
    let mut attempt = 0;
    loop {
        let mut t = ctx.db.begin(req.isolation.into(), c).await?;
        t.commit_id = commit_id;
        let results = match exec_ops(&octx, &mut t, &req.ops).await {
            Ok(r) => r,
            // A retried idempotent request may fail a precondition because
            // an earlier attempt already applied it: check (fenced).
            Err(e @ Error::PreconditionFailed { .. }) if req.idempotency_key.is_some() => {
                if let Some(v) = ctx.db.commit_status_fenced(commit_id).await? {
                    return Ok(axum::Json(json!({
                        "version": v, "committed": true, "replayed": true,
                        "commit_id": commit_hex(commit_id), "results": Json::Null,
                    })));
                }
                return Err(e);
            }
            Err(e) => return Err(e),
        };
        if t.is_read_only() {
            return Ok(axum::Json(json!({
                "version": t.read_version(), "committed": false, "results": results,
            })));
        }
        let mut creq = t.into_request();
        creq.deadline = Some(deadline);
        match ctx.db.commit_detailed(creq).await {
            Ok(c) if c.replayed => {
                return Ok(axum::Json(json!({
                    "version": c.version, "committed": true, "replayed": true,
                    "commit_id": commit_hex(commit_id), "results": Json::Null,
                })));
            }
            Ok(c) => {
                return Ok(axum::Json(json!({
                    "version": c.version, "committed": true, "commit_id": commit_hex(commit_id), "results": results,
                })));
            }
            Err(Error::Conflict(_)) if attempt < node.cfg.txn_retries => {
                attempt += 1;
                tokio::time::sleep(Duration::from_millis(5 * (1 << attempt) + node.env.random_below(10))).await;
            }
            Err(Error::Unavailable { message, retry_after_ms, outcome_unknown: true }) => {
                return Err(Error::Unavailable {
                    message: format!("{message} (commit_id {})", commit_hex(commit_id)),
                    retry_after_ms,
                    outcome_unknown: true,
                });
            }
            Err(e) => return Err(e),
        }
    }
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct Begin {
    #[serde(default)]
    isolation: IsolationParam,
    #[serde(default)]
    encoding: Encoding,
}

async fn begin_txn(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    JsonBody(b): JsonBody<Begin>,
) -> Result<Response> {
    let t = ctx.db.begin(b.isolation.into(), Consistency::Strong).await?;
    let id = node.env.random_hex(16);
    let rv = t.read_version();
    let cid = t.commit_id;
    let now = Instant::now();
    let entry = OpenTxn {
        db_id: ctx.rec.id.clone(),
        user_id: ctx.principal.user.id.clone(),
        txn: Some(t),
        created: now,
        last_used: now,
        encoding: b.encoding,
    };
    node.txns.lock().unwrap_or_else(|e| e.into_inner()).insert(id.clone(), Arc::new(tokio::sync::Mutex::new(entry)));
    Ok((
        StatusCode::CREATED,
        axum::Json(json!({
            "txn_id": id,
            "commit_id": commit_hex(cid),
            "read_version": rv,
            "expires_at": node.now_ms() + node.cfg.txn_max_ms,
            "idle_timeout_ms": node.cfg.txn_idle_ms,
        })),
    )
        .into_response())
}

fn get_open(node: &Node, ctx: &DbCtx, id: &str) -> Result<Arc<tokio::sync::Mutex<OpenTxn>>> {
    node.txns
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(id)
        .cloned()
        .ok_or(Error::TxnNotFound)
        // Ownership is checked by the caller under the entry lock.
        .inspect(|_| {
            let _ = ctx;
        })
}

fn remove_open(node: &Node, id: &str) {
    node.txns.lock().unwrap_or_else(|e| e.into_inner()).remove(id);
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OpsBody {
    ops: Vec<Op>,
}

async fn txn_ops(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    Path(p): PathMap,
    JsonBody(b): JsonBody<OpsBody>,
) -> Result<J> {
    let id = param(&p, "id")?.to_string();
    let entry = get_open(&node, &ctx, &id)?;
    let mut g = entry.lock().await;
    if g.db_id != ctx.rec.id || g.user_id != ctx.principal.user.id {
        return Err(Error::TxnNotFound);
    }
    let started = g.created;
    if started.elapsed() > Duration::from_millis(node.cfg.txn_max_ms) {
        drop(g);
        remove_open(&node, &id);
        return Err(Error::TxnNotFound);
    }
    g.last_used = Instant::now();
    let enc = g.encoding;
    let octx = op_ctx(&node, &ctx, enc);
    let Some(t) = g.txn.as_mut() else {
        return Err(Error::TxnNotFound);
    };
    match exec_ops(&octx, t, &b.ops).await {
        Ok(results) => Ok(axum::Json(json!({"results": results}))),
        Err(e) => {
            g.txn = None;
            drop(g);
            remove_open(&node, &id);
            Err(e)
        }
    }
}

async fn commit_txn(State(node): State<Arc<Node>>, Extension(ctx): Extension<DbCtx>, Path(p): PathMap) -> Result<J> {
    let id = param(&p, "id")?.to_string();
    let entry = get_open(&node, &ctx, &id)?;
    let mut g = entry.lock().await;
    if g.db_id != ctx.rec.id || g.user_id != ctx.principal.user.id {
        return Err(Error::TxnNotFound);
    }
    let t = g.txn.take().ok_or(Error::TxnNotFound)?;
    drop(g);
    remove_open(&node, &id);
    if !t.is_read_only() {
        ctx.principal.require_writable()?;
    }
    let cid = t.commit_id;
    let mut creq = t.into_request();
    creq.deadline = Some(ctx.deadline);
    let v = ctx.db.commit(creq).await.map_err(|e| match e {
        Error::Unavailable { message, retry_after_ms, outcome_unknown: true } => Error::Unavailable {
            message: format!("{message} (commit_id {})", commit_hex(cid)),
            retry_after_ms,
            outcome_unknown: true,
        },
        other => other,
    })?;
    Ok(axum::Json(json!({"version": v, "commit_id": commit_hex(cid)})))
}

async fn abort_txn(State(node): State<Arc<Node>>, Extension(ctx): Extension<DbCtx>, Path(p): PathMap) -> Result<J> {
    let id = param(&p, "id")?.to_string();
    let entry = get_open(&node, &ctx, &id)?;
    let g = entry.lock().await;
    if g.db_id != ctx.rec.id || g.user_id != ctx.principal.user.id {
        return Err(Error::TxnNotFound);
    }
    drop(g);
    remove_open(&node, &id);
    Ok(axum::Json(json!({})))
}

async fn commit_status(Extension(ctx): Extension<DbCtx>, Path(p): PathMap) -> Result<J> {
    let id = param(&p, "id")?;
    let cid = match id.strip_prefix("key:") {
        Some(k) => commit_id_for_key(&ctx.rec.id, k),
        None if id.len() == 16 => u64::from_str_radix(id, 16).map_err(|_| bad_commit_id())?,
        None => return Err(bad_commit_id()),
    };
    Ok(axum::Json(match ctx.db.commit_status_fenced(cid).await? {
        Some(v) => json!({"status": "committed", "version": v, "commit_id": commit_hex(cid)}),
        None => json!({"status": "not_committed", "commit_id": commit_hex(cid)}),
    }))
}

fn bad_commit_id() -> Error {
    Error::invalid("commit id must be 16 hex characters or key:<idempotency key>")
}

// ---------------------------------------------------------- change feeds

/// The longest a change feed request waits for a change.
const WATCH_WAIT_MAX_MS: u64 = 20_000;

/// The common parameters of change feeds: where to start (`after`, a
/// version or a `next_cursor`), `limit` and `wait_ms`.
fn feed_params(q: &Params) -> Result<(ChangePos, usize, Duration)> {
    let after = q.get("after").ok_or_else(|| Error::invalid("after is required (a version or a next_cursor)"))?;
    let after = ChangePos::decode(after).ok_or_else(|| Error::invalid("bad after"))?;
    let limit = match q.get("limit") {
        None => 100,
        Some(l) => l
            .parse()
            .ok()
            .filter(|l| (1..=ops::MAX_SCAN_LIMIT).contains(l))
            .ok_or_else(|| Error::invalid(format!("limit must be in 1..={}", ops::MAX_SCAN_LIMIT)))?,
    };
    let wait = match q.get("wait_ms") {
        None => 0,
        Some(w) => w
            .parse()
            .ok()
            .filter(|w| *w <= WATCH_WAIT_MAX_MS)
            .ok_or_else(|| Error::invalid(format!("wait_ms must be in 0..={WATCH_WAIT_MAX_MS}")))?,
    };
    Ok((after, limit, Duration::from_millis(wait)))
}

/// Run `$read` (with `$t: &mut Txn`, evaluating to `Result<(page, more)>`)
/// on a snapshot until it finds a change or `$wait` passes. Between tries
/// it waits for this node to install a newer version, re-reading the
/// manifest every `watch_poll_ms` in case another node commits.
macro_rules! watch {
    ($node:expr, $db:expr, $c:expr, $wait:expr, |$t:ident| $read:expr) => {{
        let until_ = Instant::now() + $wait;
        loop {
            let (out_, v_) = with_read!($db, $c, |$t| $read);
            let left_ = until_.saturating_duration_since(Instant::now());
            if out_.found() || left_.is_zero() {
                break (out_, v_);
            }
            let poll_ = Duration::from_millis($node.cfg.watch_poll_ms);
            $db.wait_for_change(v_, left_.min(poll_)).await;
        }
    }};
}

/// A page of a feed and whether it holds any change.
trait Found {
    fn found(&self) -> bool;
}

impl Found for (Vec<Json>, bool, ChangePos) {
    fn found(&self) -> bool {
        !self.0.is_empty()
    }
}

async fn kv_changes(State(node): State<Arc<Node>>, Extension(ctx): Extension<DbCtx>, q: Params) -> Result<J> {
    let enc = encoding_param(&q)?;
    let keys_only = bool_param(&q, "keys_only")?;
    let range = ops::user_range(
        enc,
        q.get("prefix").map(|s| s.as_str()),
        q.get("start").map(|s| s.as_str()),
        q.get("end").map(|s| s.as_str()),
    )?;
    let (after, limit, wait) = feed_params(&q)?;
    let c = consistency_of(&q)?;
    let octx = op_ctx(&node, &ctx, enc);
    let ((changes, more, next), v) = watch!(node, ctx.db, c, wait, |t| {
        let page = t.snapshot.changes(&range, &after, limit).await?;
        let mut out = Vec::with_capacity(page.changes.len());
        for ch in &page.changes {
            out.push(kv_change_json(&octx, ch, t.read_time, keys_only).await?);
        }
        let next = match (page.more, page.changes.last()) {
            (true, Some(c)) => c.pos(),
            _ => after.clone().max(ChangePos::after_version(t.read_version())),
        };
        Ok::<_, Error>((out, page.more, next))
    });
    Ok(axum::Json(json!({"changes": changes, "more": more, "next_cursor": next.encode(), "version": v})))
}

/// A change to user keys as JSON: `{"version", "key", value fields,
/// "expires_at"?}`, `{"version", "key", "deleted": true}` (also for a value
/// expired at `at`), or `{"version", "deleted_range": {"start", "end"}}`.
async fn kv_change_json(ctx: &OpCtx<'_>, ch: &Change, at: u64, keys_only: bool) -> Result<Json> {
    let mut m = serde_json::Map::new();
    m.insert("version".into(), json!(ch.seq()));
    match ch {
        Change::Entry(e) => {
            let e = e.clone().at(at);
            ops::put_key(ctx.encoding, "key", &e.key[1..], &mut m);
            match &e.value {
                Value::Tombstone => {
                    m.insert("deleted".into(), json!(true));
                }
                Value::Blob(r) if keys_only => {
                    m.insert("blob".into(), ops::blob_meta(r));
                }
                _ if keys_only => {}
                v => ops::value_json(ctx, v, &mut m).await?,
            }
            ops::put_expiry(e.expires, &mut m);
        }
        Change::DeleteRange(t) => {
            let mut r = serde_json::Map::new();
            ops::put_key(ctx.encoding, "start", &t.start[1..], &mut r);
            match &t.end {
                Some(e) if e.first() == Some(&table::SPACE_KV) => ops::put_key(ctx.encoding, "end", &e[1..], &mut r),
                _ => {
                    r.insert("end".into(), Json::Null);
                }
            }
            m.insert("deleted_range".into(), Json::Object(r));
        }
    }
    Ok(Json::Object(m))
}

async fn row_changes(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    Path(p): PathMap,
    q: Params,
) -> Result<J> {
    let name = param(&p, "t")?.to_string();
    let (after, limit, wait) = feed_params(&q)?;
    let c = consistency_of(&q)?;
    let ((changes, more, next), v) = watch!(node, ctx.db, c, wait, |t| {
        let r = table::row_changes(t, &name, &after, limit).await?;
        Ok::<_, Error>((r.changes, r.more, r.next))
    });
    Ok(axum::Json(json!({"changes": changes, "more": more, "next_cursor": next.encode(), "version": v})))
}

// --------------------------------------------------------------- tables

async fn list_tables(Extension(ctx): Extension<DbCtx>, q: Params) -> Result<J> {
    let c = consistency_of(&q)?;
    let (ts, v) = with_read!(ctx.db, c, |t| table::list_tables(t).await);
    let out: Vec<Json> = ts.iter().map(|s| s.to_json()).collect();
    Ok(axum::Json(json!({"tables": out, "version": v})))
}

async fn create_table(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    JsonBody(def): JsonBody<CreateTable>,
) -> Result<Response> {
    let (s, v) = with_txn!(node, ctx, |t| table::create_table(t, &def).await);
    let mut out = s.to_json();
    out["version"] = json!(v);
    Ok((StatusCode::CREATED, axum::Json(out)).into_response())
}

async fn get_table(Extension(ctx): Extension<DbCtx>, Path(p): PathMap, q: Params) -> Result<J> {
    let name = param(&p, "t")?.to_string();
    let c = consistency_of(&q)?;
    let (s, _) = with_read!(ctx.db, c, |t| table::get_schema(t, &name).await);
    Ok(axum::Json(s.to_json()))
}

async fn drop_table(State(node): State<Arc<Node>>, Extension(ctx): Extension<DbCtx>, Path(p): PathMap) -> Result<J> {
    let name = param(&p, "t")?.to_string();
    let ((), v) = with_txn!(node, ctx, |t| table::drop_table(t, &name).await);
    Ok(axum::Json(json!({"version": v})))
}

async fn add_column(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    Path(p): PathMap,
    JsonBody(c): JsonBody<ColumnSpec>,
) -> Result<J> {
    let name = param(&p, "t")?.to_string();
    let (s, v) = with_txn!(node, ctx, |t| table::add_column(t, &name, &c).await);
    let mut out = s.to_json();
    out["version"] = json!(v);
    Ok(axum::Json(out))
}

async fn drop_column(State(node): State<Arc<Node>>, Extension(ctx): Extension<DbCtx>, Path(p): PathMap) -> Result<J> {
    let name = param(&p, "t")?.to_string();
    let col = param(&p, "c")?.to_string();
    let (s, v) = with_txn!(node, ctx, |t| table::drop_column(t, &name, &col).await);
    let mut out = s.to_json();
    out["version"] = json!(v);
    Ok(axum::Json(out))
}

async fn add_index(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    Path(p): PathMap,
    JsonBody(i): JsonBody<IndexSpec>,
) -> Result<J> {
    ctx.principal.require_writable()?;
    let name = param(&p, "t")?.to_string();
    let opts = table::Backfill { batch_rows: node.cfg.index_backfill_batch_rows, ..Default::default() };
    let (s, v) = table::add_index(&ctx.db, &name, &i, opts).await?;
    let mut out = s.to_json();
    out["version"] = json!(v);
    Ok(axum::Json(out))
}

async fn drop_index(State(node): State<Arc<Node>>, Extension(ctx): Extension<DbCtx>, Path(p): PathMap) -> Result<J> {
    let name = param(&p, "t")?.to_string();
    let idx = param(&p, "i")?.to_string();
    let (s, v) = with_txn!(node, ctx, |t| table::drop_index(t, &name, &idx).await);
    let mut out = s.to_json();
    out["version"] = json!(v);
    Ok(axum::Json(out))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InsertBody {
    #[serde(default)]
    row: Option<Json>,
    #[serde(default)]
    rows: Option<Vec<Json>>,
    #[serde(default)]
    upsert: bool,
    #[serde(default)]
    ttl_ms: Option<u64>,
}

async fn insert_rows(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    Path(p): PathMap,
    JsonBody(b): JsonBody<InsertBody>,
) -> Result<Response> {
    let name = param(&p, "t")?.to_string();
    let rows: Vec<Json> = match (b.row, b.rows) {
        (Some(r), None) => vec![r],
        (None, Some(rs)) => rs,
        _ => return Err(Error::invalid("provide exactly one of row or rows")),
    };
    if rows.is_empty() || rows.len() > 10_000 {
        return Err(Error::invalid("insert 1..=10000 rows per request"));
    }
    let mode = if b.upsert { InsertMode::Upsert } else { InsertMode::Insert };
    let db = ctx.db.clone();
    let (n, v) = with_txn!(node, ctx, |t| {
        let blobs = BlobResolver { db: &db };
        let ttl = ops::expiry(b.ttl_ms)?;
        for r in &rows {
            table::insert_expiring(t, &blobs, &name, r, mode, ttl).await?;
        }
        Ok::<usize, Error>(rows.len())
    });
    let status = if b.upsert { StatusCode::OK } else { StatusCode::CREATED };
    Ok((status, axum::Json(json!({"count": n, "version": v}))).into_response())
}

fn split_consistency(mut body: Json) -> Result<(Json, Consistency)> {
    let (c, s) = match body.as_object_mut() {
        Some(o) => (
            o.remove("consistency").and_then(|v| v.as_str().map(|s| s.to_string())),
            o.remove("max_staleness_ms").and_then(|v| v.as_u64()),
        ),
        None => return Err(Error::invalid("body must be a JSON object")),
    };
    Ok((body, consistency_from(c.as_deref(), s)?))
}

async fn get_row(Extension(ctx): Extension<DbCtx>, Path(p): PathMap, JsonBody(b): JsonBody<Json>) -> Result<J> {
    let name = param(&p, "t")?.to_string();
    let (b, c) = split_consistency(b)?;
    let pk = b.get("pk").cloned().ok_or_else(|| Error::invalid("missing pk"))?;
    let (row, v) = with_read!(ctx.db, c, |t| table::get_expiring(t, &name, &pk).await);
    Ok(axum::Json(match row {
        Some((r, Some(e))) => json!({"found": true, "row": r, "expires_at": e, "version": v}),
        Some((r, None)) => json!({"found": true, "row": r, "version": v}),
        None => json!({"found": false, "version": v}),
    }))
}

/// Rows to change: one by primary key, or every row a selection matches
/// (at most `limit`).
enum Rows<'a> {
    One(&'a Json),
    Where(&'a Selection, Option<usize>),
}

fn target<'a>(pk: &'a Option<Json>, selection: &'a Option<Selection>, limit: Option<usize>) -> Result<Rows<'a>> {
    match (pk, selection) {
        (Some(pk), None) if limit.is_none() => Ok(Rows::One(pk)),
        (Some(_), None) => Err(Error::invalid("limit applies to where only")),
        (None, Some(_)) if limit == Some(0) => Err(Error::invalid("limit must be at least 1")),
        (None, Some(s)) => Ok(Rows::Where(s, limit)),
        _ => Err(Error::invalid("provide exactly one of pk or where")),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateBody {
    #[serde(default)]
    pk: Option<Json>,
    #[serde(default, rename = "where")]
    selection: Option<Selection>,
    #[serde(default)]
    limit: Option<usize>,
    set: Json,
    #[serde(default)]
    ttl_ms: Option<u64>,
}

async fn update_row(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    Path(p): PathMap,
    JsonBody(b): JsonBody<UpdateBody>,
) -> Result<J> {
    let name = param(&p, "t")?.to_string();
    let db = ctx.db.clone();
    let rows = target(&b.pk, &b.selection, b.limit)?;
    let (out, v) = with_txn!(node, ctx, |t| {
        let blobs = BlobResolver { db: &db };
        let e = ops::row_expiry(b.ttl_ms)?;
        match rows {
            Rows::One(pk) => {
                table::update_expiring(t, &blobs, &name, pk, &b.set, e).await.map(|found| json!({"found": found}))
            }
            Rows::Where(sel, limit) => {
                table::update_where(t, &blobs, &name, sel, &b.set, e, limit).await.map(|n| json!({"count": n}))
            }
        }
    });
    Ok(axum::Json(with_version(out, v)))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteBody {
    #[serde(default)]
    pk: Option<Json>,
    #[serde(default, rename = "where")]
    selection: Option<Selection>,
    #[serde(default)]
    limit: Option<usize>,
}

async fn delete_row(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    Path(p): PathMap,
    JsonBody(b): JsonBody<DeleteBody>,
) -> Result<J> {
    let name = param(&p, "t")?.to_string();
    let rows = target(&b.pk, &b.selection, b.limit)?;
    let (out, v) = with_txn!(node, ctx, |t| match rows {
        Rows::One(pk) => table::delete(t, &name, pk).await.map(|found| json!({"found": found})),
        Rows::Where(sel, limit) => table::delete_where(t, &name, sel, limit).await.map(|n| json!({"count": n})),
    });
    Ok(axum::Json(with_version(out, v)))
}

fn with_version(mut out: Json, v: u64) -> Json {
    out["version"] = json!(v);
    out
}

async fn query_rows(Extension(ctx): Extension<DbCtx>, Path(p): PathMap, JsonBody(b): JsonBody<Json>) -> Result<J> {
    let name = param(&p, "t")?.to_string();
    let (b, c) = split_consistency(b)?;
    let q: Query = serde_json::from_value(b).map_err(|e| Error::invalid(format!("invalid query: {e}")))?;
    let (r, v) = with_read!(ctx.db, c, |t| table::query(t, &name, &q).await);
    Ok(axum::Json(json!({"rows": r.rows, "next_cursor": r.next_cursor, "version": v})))
}
