//! Database-scoped routes: databases, KV, blobs, transactions and tables.

use super::body::{BlobBody, Upload, parse_range, read_upload};
use super::{
    Auth, DbCtx, Endpoints as _, Found, JsonBody, LONG_POLL_MAX_MS, Params, QueryOf, Ran, Reply, Req, consistency_of,
    naming_commit, page_limit, paging, percent_decode, run_txn, watch, with_read, with_txn,
};
use crate::engine::manifest::ObjKind;
use crate::engine::{Change, ChangeCursor, Committed, Consistency, Isolation, Txn, Value, blob};
use crate::error::{Error, Result};
use crate::meta::{self, DbRecord, DbRef, MetaCmd};
use crate::node::Node;
use crate::ops::{self, EncodingExt, Op, OpCtx, exec_ops, user_key};
use crate::table::{self, BlobResolver, InsertMode, Query, Selection};
use crate::txns::OpenTxn;
use axum::Router;
use axum::body::Body;
use axum::extract::{Extension, Path, Request, State};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use baserunna_api::{self as api, Endpoint as _};
use http::{HeaderMap, HeaderValue, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

type J = axum::Json<Json>;

pub fn routes() -> Router<Arc<Node>> {
    Router::new()
        .route("/v1/dbs/{db}", get(get_db).delete(delete_db))
        .route("/v1/dbs/{db}/kv", get(scan_kv))
        .endpoint::<api::Changes, _>(kv_changes)
        .route("/v1/dbs/{db}/kv/{*key}", get(get_kv).put(put_kv).delete(delete_kv))
        .route(api::StageBlob::ROUTE, post(stage_blob))
        .route(api::ReadBlob::ROUTE, get(get_blob))
        .endpoint::<api::Txn, _>(one_shot)
        .endpoint::<api::BeginTxn, _>(begin_txn)
        .endpoint::<api::AbortTxn, _>(abort_txn)
        .endpoint::<api::TxnOps, _>(txn_ops)
        .endpoint::<api::CommitTxn, _>(commit_txn)
        .endpoint::<api::GetCommit, _>(commit_status)
        .endpoint::<api::ListTables, _>(list_tables)
        .endpoint::<api::CreateTable, _>(create_table)
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

pub(super) type PathMap = Path<HashMap<String, String>>;

pub(super) fn param<'a>(p: &'a HashMap<String, String>, k: &str) -> Result<&'a str> {
    p.get(k).map(|s| s.as_str()).ok_or_else(|| Error::invalid(format!("missing path parameter {k}")))
}

fn op_ctx<'a>(node: &Node, ctx: &'a DbCtx, enc: api::Encoding) -> OpCtx<'a> {
    OpCtx {
        db: &ctx.db,
        encoding: enc,
        access: ctx.access,
        json_value_max: node.cfg.json_value_max,
        query_limits: node.cfg.query_limits,
    }
}

#[derive(Serialize)]
struct DbSummary<'a> {
    id: &'a str,
    name: &'a str,
    org: Option<&'a str>,
    created_at: i64,
}

impl<'a> DbSummary<'a> {
    fn new(db: &'a DbRecord, org: Option<&'a str>) -> DbSummary<'a> {
        DbSummary { id: &db.id, name: &db.name, org, created_at: db.created_at }
    }
}

async fn org_name(t: &mut Txn, owner_id: &str) -> Result<Option<String>> {
    if !meta::is_org_id(owner_id) {
        return Ok(None);
    }
    let org = meta::get_org(t, owner_id).await?;
    Ok(Some(org.ok_or_else(|| Error::internal(format!("database owner {owner_id} is missing")))?.name))
}

/// Formats the display reference for `d` (`name` or `org.name`).
pub async fn db_ref(t: &mut Txn, d: &DbRecord) -> Result<String> {
    Ok(DbRef::format(org_name(t, &d.owner_id).await?.as_deref(), &d.name))
}

/// Lists databases for the authenticated user or the specified `org`.
pub async fn list_dbs(State(node): State<Arc<Node>>, Auth(p): Auth, q: Params) -> Result<J> {
    let (limit, cursor) = paging(&q)?;
    let org = q.get("org");
    let mut t = node.sys_read_strong().await?;
    let (dbs, next_cursor) = match (p.scoped_db(), org) {
        (Some(_), Some(_)) => return Err(Error::PermissionDenied("this API key is limited to one database".into())),
        (Some(scope), None) => (meta::get_db(&mut t, scope).await?.into_iter().collect(), None),
        (None, Some(name)) => {
            let owner = node.resolve_org(&p, name).await?.0.id;
            let page = meta::dbs_page(&mut t, &owner, limit, cursor).await?;
            (page.items, page.next_cursor)
        }
        (None, None) => {
            let page = meta::dbs_page(&mut t, &p.user.id, limit, cursor).await?;
            (page.items, page.next_cursor)
        }
    };
    let mut out = Vec::with_capacity(dbs.len());
    for db in &dbs {
        let org = org_name(&mut t, &db.owner_id).await?;
        out.push(to_json(&DbSummary::new(db, org.as_deref()))?);
    }
    Ok(axum::Json(json!({"databases": out, "next_cursor": next_cursor})))
}

fn to_json<T: Serialize>(v: &T) -> Result<Json> {
    serde_json::to_value(v).map_err(|e| Error::internal(e.to_string()))
}

/// Creates a database owned by the authenticated user or the specified `org`.
pub async fn create_db(
    State(node): State<Arc<Node>>,
    Auth(p): Auth,
    Req(c): Req<api::CreateDb>,
) -> Result<Reply<api::CreateDb>> {
    p.require_writable()?;
    p.require_unscoped()?;
    let owner_id = match &c.org {
        Some(o) => node.resolve_org(&p, o).await?.0.id,
        None => p.user.id.clone(),
    };
    let create = MetaCmd::CreateDb {
        id: node.env.random_hex(10),
        owner_id,
        actor: p.user.id.clone(),
        name: c.name.clone(),
        max_dbs: node.cfg.max_dbs_per_owner,
    };
    let db: DbRecord = match node.run_meta_as(create.clone()).await {
        Err(Error::AlreadyExists(m)) => {
            // If a prior deletion stopped after marking the manifest deleted, finish it and retry.
            let (existing, _) = node.resolve_db_strong(&p, &DbRef::format(c.org.as_deref(), &c.name)).await?;
            let db = node.open_db(&existing).await?;
            let deleted =
                db.read_version(Consistency::Strong).await.is_err() && db.current().is_some_and(|v| v.m.is_deleted());
            if !deleted {
                return Err(Error::AlreadyExists(m));
            }
            node.run_meta(MetaCmd::DeleteDb { id: existing.id.clone() }).await?;
            node.forget_db_name(&existing.owner_id, &c.name);
            node.run_meta_as(create).await?
        }
        other => other?,
    };
    Ok(Reply::new(api::Db { id: db.id, name: db.name, org: c.org, created_at: db.created_at }))
}

async fn get_db(State(node): State<Arc<Node>>, Extension(ctx): Extension<DbCtx>) -> Result<J> {
    let v = ctx.db.read_version(Consistency::Snapshot { max_staleness_ms: 5000 }).await?;
    let mut t = node.sys_read().await?;
    Ok(axum::Json(json!({
        "id": ctx.rec.id,
        "name": ctx.rec.name,
        "org": org_name(&mut t, &ctx.rec.owner_id).await?,
        "role": ctx.access.role,
        "created_at": ctx.rec.created_at,
        "stats": ctx.db.stats(&v),
    })))
}

async fn delete_db(State(node): State<Arc<Node>>, Extension(ctx): Extension<DbCtx>) -> Result<J> {
    ctx.principal.require_unscoped()?;
    node.access_strong(&ctx.principal, &ctx.rec).await?.require_admin()?;
    ctx.db.mark_deleted().await?;
    node.env.crash_point().await;
    // Ignore `NotFound` if a concurrent repair already removed the metadata record after the manifest was marked deleted.
    match node.run_meta(MetaCmd::DeleteDb { id: ctx.rec.id.clone() }).await {
        Ok(_) | Err(Error::NotFound(_)) => {}
        Err(e) => {
            return Err(Error::Unavailable { message: e.message(), retry_after_ms: None, outcome_unknown: true });
        }
    }
    node.forget_db_name(&ctx.rec.owner_id, &ctx.rec.name);
    node.engine.forget(&ctx.rec.id);
    Ok(axum::Json(json!({})))
}

pub(super) fn encoding_param(q: &Params) -> Result<api::Encoding> {
    match q.get("encoding").map(|s| s.as_str()) {
        None | Some("utf8") => Ok(api::Encoding::Utf8),
        Some("base64") => Ok(api::Encoding::Base64),
        Some(o) => Err(Error::invalid(format!("unknown encoding {o:?}"))),
    }
}

pub(super) fn bool_param(q: &Params, k: &str) -> Result<bool> {
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
    let mut out = r.pop().map_or(Ok(Json::Null), |r| to_json(&r))?;
    out["version"] = json!(v);
    Ok(axum::Json(out))
}

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
    untrusted_content(h);
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
    ctx.require_write()?;
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

/// Adds headers that prevent browsers from executing user-supplied content in the console's origin.
pub fn untrusted_content(h: &mut HeaderMap) {
    h.insert(http::header::X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    h.insert(http::header::CONTENT_SECURITY_POLICY, HeaderValue::from_static("sandbox; default-src 'none'"));
}

async fn stage_blob(Extension(ctx): Extension<DbCtx>, req: Request) -> Result<Reply<api::StageBlob>> {
    ctx.require_write()?;
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
    Ok(Reply::new(ops::blob_meta(&r)))
}

async fn get_blob(Extension(ctx): Extension<DbCtx>, Path(p): PathMap, req: Request) -> Result<Response> {
    let id = param(&p, "blob_id")?;
    let r = blob::load_staged(ctx.db.shared(), &ctx.db.id, id).await?;
    // Pin obsolete blobs immediately so GC does not collect them during the read.
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
    untrusted_content(h);
    h.insert(http::header::CONTENT_LENGTH, HeaderValue::from(rr.end - rr.start));
    h.insert(http::header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    if let Ok(v) = HeaderValue::from_str(&r.content_type) {
        h.insert(http::header::CONTENT_TYPE, v);
    }
    Ok(resp)
}

fn isolation(i: api::Isolation) -> Isolation {
    match i {
        api::Isolation::Serializable => Isolation::Serializable,
        api::Isolation::Snapshot => Isolation::Snapshot,
    }
}

fn consistency_from(c: Option<api::Consistency>, max_staleness_ms: Option<u64>) -> Consistency {
    match c {
        None | Some(api::Consistency::Strong) => Consistency::Strong,
        Some(api::Consistency::Snapshot) => {
            Consistency::Snapshot { max_staleness_ms: max_staleness_ms.unwrap_or(5000) }
        }
    }
}

pub(super) fn commit_id_for_key(db_id: &str, key: &str) -> u64 {
    let mut d = Vec::with_capacity(db_id.len() + key.len() + 1);
    d.extend_from_slice(db_id.as_bytes());
    d.push(0);
    d.extend_from_slice(key.as_bytes());
    let h = crate::util::sha256(&d);
    let mut a = [0u8; 8];
    a.copy_from_slice(&h[..8]);
    u64::from_be_bytes(a)
}

/// Extracts the lowercase MIME type from `Content-Type` without parameters.
pub(super) fn content_type(h: &HeaderMap) -> String {
    let ct = h.get(http::header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).unwrap_or("");
    ct.split(';').next().unwrap_or("").trim().to_ascii_lowercase()
}

pub(super) fn idempotency_header(h: &HeaderMap) -> Result<Option<String>> {
    match h.get("idempotency-key") {
        None => Ok(None),
        Some(v) => match v.to_str() {
            Ok(k) => Ok(Some(k.to_string())),
            Err(_) => Err(Error::invalid("Idempotency-Key must be visible ASCII")),
        },
    }
}

/// Returns the idempotency key from the header or body, or an error if both are present and differ.
pub(super) fn one_idempotency_key(header: Option<String>, body: Option<String>) -> Result<Option<String>> {
    match (header, body) {
        (Some(h), Some(b)) if h != b => {
            Err(Error::invalid("the Idempotency-Key header and the body's idempotency_key differ"))
        }
        (h, b) => Ok(h.or(b)),
    }
}

pub(super) fn commit_hex(id: u64) -> String {
    format!("{id:016x}")
}

async fn one_shot(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    Req(req): Req<api::Txn>,
) -> Result<Reply<api::Txn>> {
    let ops = ops::parse_ops(req.ops)?;
    let writes = ops.iter().any(|o| o.is_write());
    if writes {
        ctx.require_write()?;
    }
    let c = consistency_from(req.consistency, req.max_staleness_ms);
    if writes && c != Consistency::Strong {
        return Err(Error::invalid("transactions with writes must use strong consistency"));
    }
    let octx = op_ctx(&node, &ctx, req.encoding);
    // Ignore idempotency keys on read-only requests so a failed check cannot abort a write key without write permission.
    let key = req.idempotency_key.as_deref().filter(|_| writes);
    let (commit_id, ran) = run_txn!(node, ctx, isolation(req.isolation), c, key, |t| exec_ops(&octx, t, &ops).await);
    node.env.crash_point().await;
    let reply = |version: u64, committed: bool, replayed: bool, results, appends| {
        let commit_id = committed.then(|| commit_hex(commit_id));
        Reply::new(api::TxnResponse { version, committed, commit_id, replayed, results, appends })
    };
    Ok(match ran {
        Ran::Read { out, version } => reply(version, false, false, Some(out), None),
        Ran::Committed { mut out, commit } => {
            ops::fill_appends(&ops, &mut out, &commit.appends);
            reply(commit.version, true, false, Some(out), Some(first_seqs(&commit)))
        }
        Ran::Replayed(c) if c.inserted.is_some() => {
            return Err(Error::invalid("the idempotency key was used by an event bulk insert"));
        }
        Ran::Replayed(c) => reply(c.version, true, true, None, Some(first_seqs(&c))),
    })
}

fn first_seqs(c: &Committed) -> Vec<u64> {
    c.appends.iter().map(|a| a.first_seq).collect()
}

async fn begin_txn(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    Req(b): Req<api::BeginTxn>,
) -> Result<Reply<api::BeginTxn>> {
    let ops = ops::parse_ops(b.ops)?;
    let user = &ctx.principal.user.id;
    node.txns.admit_begin(user, &ctx.db.id)?;
    let t = match isolation(b.isolation) {
        crate::engine::Isolation::Serializable => ctx.db.begin_optimistic().await?,
        iso => ctx.db.begin(iso, Consistency::Strong).await?,
    };
    let (rv, cid) = (t.read_version(), t.commit_id);
    let id = match b.txn_id {
        Some(id) if id.len() == 32 && id.bytes().all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f')) => id,
        Some(_) => return Err(Error::invalid("txn_id must be 32 lowercase hex digits")),
        None => node.env.random_hex(16),
    };
    let mut txn = node.txns.open(id.clone(), user, &ctx.rec.id, t, b.encoding)?;
    let results = exec_ops(&op_ctx(&node, &ctx, b.encoding), txn.txn(), &ops).await;
    let results = txn.finish(results)?;
    Ok(Reply::new(api::Begun {
        txn_id: id,
        commit_id: commit_hex(cid),
        read_version: rv,
        expires_at: node.now_ms() + node.cfg.txn_max_ms,
        idle_timeout_ms: node.cfg.txn_idle_ms,
        results,
    }))
}

fn get_open(node: &Node, ctx: &DbCtx, id: &str) -> Result<Arc<OpenTxn>> {
    node.txns.get(id, &ctx.principal.user.id, &ctx.rec.id)
}

async fn txn_ops(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    Path(p): PathMap,
    Req(b): Req<api::TxnOps>,
) -> Result<Reply<api::TxnOps>> {
    let id = param(&p, "id")?.to_string();
    let ops = ops::parse_ops(b.ops)?;
    let open = get_open(&node, &ctx, &id)?;
    node.txns.admit(&open.user_id, &open.db_id)?;
    let mut txn = node.txns.lock(open).await?;
    let results = exec_ops(&op_ctx(&node, &ctx, txn.encoding()), txn.txn(), &ops).await;
    let results = txn.finish(results).map_err(|e| match e.retryable() {
        true => Error::TxnNotFound(format!(
            "transaction was aborted because a request on it failed ({}); retry it",
            e.message()
        )),
        false => e,
    })?;
    Ok(Reply::new(api::OpsResponse { results }))
}

async fn commit_txn(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    Path(p): PathMap,
    Req(b): Req<api::CommitTxn>,
) -> Result<Reply<api::CommitTxn>> {
    let id = param(&p, "id")?.to_string();
    let ops = ops::parse_ops(b.ops)?;
    let txn = node.txns.lock(get_open(&node, &ctx, &id)?).await?;
    let enc = txn.encoding();
    let mut t = txn.take();
    let mut results = exec_ops(&op_ctx(&node, &ctx, enc), &mut t, &ops).await?;
    if !t.is_read_only() {
        ctx.require_write()?;
    }
    let cid = t.commit_id;
    if t.needs_revalidation(!t.is_read_only()) {
        let Some(version) = ctx.db.revalidate(t).await? else {
            return Err(Error::Conflict("transaction conflicts with a concurrent commit".into()));
        };
        return Ok(Reply::new(api::Committed { version, commit_id: commit_hex(cid), appends: Vec::new(), results }));
    }
    let mut creq = t.into_request();
    creq.deadline = Some(ctx.deadline.instant());
    let c = ctx.db.commit_detailed(creq).await.map_err(|e| naming_commit(e, cid))?;
    let own_appends = ops.iter().filter(|o| matches!(o, Op::Append { .. })).count();
    ops::fill_appends(&ops, &mut results, &c.appends[c.appends.len().saturating_sub(own_appends)..]);
    Ok(Reply::new(api::Committed { version: c.version, commit_id: commit_hex(cid), appends: first_seqs(&c), results }))
}

async fn abort_txn(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    Path(p): PathMap,
    _: Req<api::AbortTxn>,
) -> Result<Reply<api::AbortTxn>> {
    let id = param(&p, "id")?.to_string();
    let open = get_open(&node, &ctx, &id).inspect_err(|_| node.txns.abort_unopened(&id))?;
    drop(node.txns.lock(open).await?.take());
    Ok(Reply::new(api::Empty {}))
}

async fn commit_status(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    Path(p): PathMap,
    _: Req<api::GetCommit>,
) -> Result<Reply<api::GetCommit>> {
    ctx.require_write()?;
    let id = param(&p, "id")?;
    let cid = match id.strip_prefix("key:") {
        Some(k) => commit_id_for_key(&ctx.rec.id, k),
        None if id.len() == 16 => u64::from_str_radix(id, 16).map_err(|_| bad_commit_id())?,
        None => return Err(bad_commit_id()),
    };
    Ok(Reply::new(match ctx.db.resolve_commit(cid).await? {
        Some(c) => {
            api::CommitStatus::Committed { version: c.version, commit_id: commit_hex(cid), appends: first_seqs(&c) }
        }
        None => api::CommitStatus::NotCommitted {
            commit_id: commit_hex(cid),
            retention_ms: node.cfg.engine.commit_retention_ms,
        },
    }))
}

fn bad_commit_id() -> Error {
    Error::invalid("commit id must be 16 hex characters or key:<idempotency key>")
}

fn feed_params(q: &Params) -> Result<(ChangeCursor, usize, Duration)> {
    let after = q.get("after").ok_or_else(|| Error::invalid("after is required (a version or a next_cursor)"))?;
    let limit = q.get("limit").map(|l| l.parse().map_err(|_| limit_error())).transpose()?;
    let wait = q.get("wait_ms").map(|w| w.parse().map_err(|_| wait_error())).transpose()?;
    feed_args(after, limit, wait)
}

/// Validates the cursor, page size, and wait time of a change-feed read.
fn feed_args(after: &str, limit: Option<usize>, wait_ms: Option<u64>) -> Result<(ChangeCursor, usize, Duration)> {
    let after = ChangeCursor::decode(after).ok_or_else(|| Error::invalid("bad after"))?;
    let limit = limit.unwrap_or(100);
    if !(1..=ops::MAX_SCAN_LIMIT).contains(&limit) {
        return Err(limit_error());
    }
    let wait = wait_ms.unwrap_or(0);
    if wait > LONG_POLL_MAX_MS {
        return Err(wait_error());
    }
    Ok((after, limit, Duration::from_millis(wait)))
}

fn limit_error() -> Error {
    Error::invalid(format!("limit must be in 1..={}", ops::MAX_SCAN_LIMIT))
}

fn wait_error() -> Error {
    Error::invalid(format!("wait_ms must be in 0..={LONG_POLL_MAX_MS}"))
}

impl Found for (Vec<Json>, bool, ChangeCursor) {
    fn found(&self) -> bool {
        !self.0.is_empty()
    }
}

impl Found for (Vec<api::Change>, bool, ChangeCursor) {
    fn found(&self) -> bool {
        !self.0.is_empty()
    }
}

async fn kv_changes(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    QueryOf(q): QueryOf<api::ChangesQuery>,
    _: Req<api::Changes>,
) -> Result<Reply<api::Changes>> {
    let enc = q.encoding.unwrap_or_default();
    let range = ops::user_range(enc, q.prefix.as_deref(), q.start.as_deref(), q.end.as_deref())?;
    let (after, limit, wait) = feed_args(&q.after, q.limit, q.wait_ms)?;
    let c = consistency_from(q.consistency, q.max_staleness_ms);
    let octx = op_ctx(&node, &ctx, enc);
    let ((changes, more, next), version) = watch!(node, ctx.db, c, wait, |t| {
        let page = t.snapshot().changes(&range, &after, limit).await?;
        let mut out = Vec::with_capacity(page.changes.len());
        for ch in &page.changes {
            out.push(kv_change(&octx, ch, t.read_time, q.keys_only).await?);
        }
        Ok::<_, Error>((out, page.more, page.next))
    });
    Ok(Reply::new(api::ChangePage { changes, more, next_cursor: next.encode(), version }))
}

async fn kv_change(ctx: &OpCtx<'_>, ch: &Change, at: u64, keys_only: bool) -> Result<api::Change> {
    let mut out = api::Change { version: ch.seq(), ..api::Change::default() };
    match ch {
        Change::Entry(e) => {
            let e = e.clone().at(at);
            (out.key, out.key_b64) = ctx.encoding.text(&e.key[1..]);
            match &e.value {
                Value::Tombstone => out.deleted = true,
                v => {
                    let f = ops::value_fields(ctx, v, keys_only).await?;
                    (out.value, out.value_b64, out.blob) = (f.value, f.value_b64, f.blob);
                }
            }
            out.expires_at = e.expires;
        }
        Change::DeleteRange(t) => {
            let (start, start_b64) = ctx.encoding.text(&t.start[1..]);
            let (end, end_b64) = match &t.end {
                Some(e) if e.first() == Some(&table::SPACE_KV) => {
                    let (end, end_b64) = ctx.encoding.text(&e[1..]);
                    (end.map(Some), end_b64)
                }
                _ => (Some(None), None),
            };
            out.deleted_range = Some(api::DeletedRange { start, start_b64, end, end_b64 });
        }
    }
    Ok(out)
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

async fn list_tables(
    Extension(ctx): Extension<DbCtx>,
    QueryOf(q): QueryOf<api::ListTablesQuery>,
    _: Req<api::ListTables>,
) -> Result<Reply<api::ListTables>> {
    let limit = page_limit(q.limit)?;
    let (prefix, cursor) = (q.prefix.unwrap_or_default(), q.cursor.filter(|c| !c.is_empty()));
    let c = consistency_from(q.consistency, q.max_staleness_ms);
    let ((ts, next), v) = with_read!(ctx.db, c, |t| table::list_tables(t, &prefix, limit, cursor.as_deref()).await);
    let tables = ts.iter().map(|s| s.to_api()).collect();
    Ok(Reply::new(api::TablePage { tables, next_cursor: next, version: v }))
}

async fn create_table(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    Req(def): Req<api::CreateTable>,
) -> Result<Reply<api::CreateTable>> {
    let (s, version) = with_txn!(node, ctx, |t| table::create_table(t, &def).await);
    Ok(Reply::new(api::CreatedTable { table: s.to_api(), version }))
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
    JsonBody(c): JsonBody<api::Column>,
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
    JsonBody(i): JsonBody<api::NewIndex>,
) -> Result<J> {
    ctx.require_write()?;
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

pub(super) fn split_consistency(mut body: Json) -> Result<(Json, Consistency)> {
    let (c, s) = match body.as_object_mut() {
        Some(o) => (
            o.remove("consistency")
                .filter(|v| !v.is_null())
                .map(|v| v.as_str().map(str::to_string).ok_or_else(|| Error::invalid("consistency must be a string")))
                .transpose()?,
            o.remove("max_staleness_ms")
                .filter(|v| !v.is_null())
                .map(|v| v.as_u64().ok_or_else(|| Error::invalid("max_staleness_ms must be a number")))
                .transpose()?,
        ),
        None => return Err(Error::invalid("body must be a JSON object")),
    };
    let mut q = std::collections::BTreeMap::new();
    q.extend(c.map(|c| ("consistency".to_string(), c)));
    q.extend(s.map(|s| ("max_staleness_ms".to_string(), s.to_string())));
    Ok((body, consistency_of(&q)?))
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
