//! HTTP API router, extractors, and request middleware.

mod account;
pub mod body;
mod data;
pub mod openapi;
mod orgs;
mod streams;

pub use account::session_cookie;

use crate::cluster::{
    CLUSTER_KEY_HEADER, DEADLINE_HEADER, Delivery, FORWARD_ID_HEADER, FORWARD_TO_HEADER, HOPS_HEADER, ReplayedResponse,
};
use crate::engine::{Committed, Consistency, Db, Txn};
use crate::error::{Error, Result};
use crate::log::{self, REQUEST_ID_HEADER};
use crate::meta::{Access, DbRecord};
use crate::node::{Node, Principal};
use crate::timing;
use axum::Router;
use axum::body::Body;
use axum::extract::{DefaultBodyLimit, FromRequest, FromRequestParts, MatchedPath, Request, State};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, StatusCode};
use serde::de::DeserializeOwned;
use serde_json::{Value as Json, json};
use std::sync::Arc;

pub const SESSION_COOKIE: &str = "brs_session";

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let mut err = json!({
            "code": self.code(),
            "message": self.message(),
            "retryable": self.retryable(),
        });
        let mut retry_after = None;
        match &self {
            Error::PreconditionFailed { op_index: Some(i), .. } => err["details"] = json!({"op_index": i}),
            Error::NotOwner { holder, retry_after_ms } => {
                err["details"] = json!({"lease_holder": holder});
                retry_after = Some(*retry_after_ms);
            }
            Error::Unavailable { outcome_unknown, retry_after_ms, .. } => {
                err["details"] = json!({"outcome": if *outcome_unknown { "unknown" } else { "not_applied" }});
                retry_after = *retry_after_ms;
            }
            Error::RateLimited { retry_after_ms, .. } => {
                err["details"] = json!({"retry_after_ms": retry_after_ms});
                retry_after = Some(*retry_after_ms);
            }
            _ => {}
        }
        if let Error::Internal(m) = &self {
            crate::warn!("internal error: {m}");
        }
        let mut resp = (status, axum::Json(json!({"error": err}))).into_response();
        if let Some(ms) = retry_after {
            let secs = ms.div_ceil(1000).max(1);
            if let Ok(v) = HeaderValue::from_str(&secs.to_string()) {
                resp.headers_mut().insert(http::header::RETRY_AFTER, v);
            }
        }
        resp
    }
}

/// Reconstructs an [`Error`] from a JSON error response.
pub fn error_from_json(status: u16, j: &Json) -> Error {
    let e = &j["error"];
    let m = e["message"].as_str().unwrap_or("request failed").to_string();
    match e["code"].as_str().unwrap_or("") {
        "invalid_argument" => Error::InvalidArgument(m),
        "unauthenticated" => Error::Unauthenticated(m),
        "permission_denied" => Error::PermissionDenied(m),
        "not_found" => Error::NotFound(m),
        "txn_not_found" => Error::TxnNotFound,
        "conflict" => Error::Conflict(m),
        "already_exists" => Error::AlreadyExists(m),
        "precondition_failed" => {
            Error::PreconditionFailed { message: m, op_index: e["details"]["op_index"].as_u64().map(|i| i as usize) }
        }
        "too_large" => Error::TooLarge(m),
        "cursor_expired" => Error::CursorExpired(m),
        "quota_exceeded" => Error::QuotaExceeded(m),
        "rate_limited" => {
            Error::RateLimited { message: m, retry_after_ms: e["details"]["retry_after_ms"].as_u64().unwrap_or(1_000) }
        }
        "unavailable" => Error::Unavailable {
            message: m,
            retry_after_ms: Some(500),
            outcome_unknown: e["details"]["outcome"] == "unknown",
        },
        _ => Error::Internal(format!("{status}: {m}")),
    }
}

/// JSON body extractor that reports errors in the API's format.
pub struct JsonBody<T>(pub T);

impl<S: Send + Sync, T: DeserializeOwned> FromRequest<S> for JsonBody<T> {
    type Rejection = Error;

    async fn from_request(req: Request, state: &S) -> Result<Self> {
        let b = Bytes::from_request(req, state).await.map_err(|e| match e.status() {
            StatusCode::PAYLOAD_TOO_LARGE => Error::TooLarge("request body too large".into()),
            _ => Error::invalid(format!("could not read body: {e}")),
        })?;
        let b: &[u8] = if b.iter().all(|c| c.is_ascii_whitespace()) { b"{}" } else { &b };
        serde_json::from_slice(b).map(JsonBody).map_err(|e| Error::invalid(format!("invalid JSON body: {e}")))
    }
}

/// Extracts a bearer token or session cookie from `headers`.
pub fn credential(headers: &HeaderMap) -> Option<String> {
    if let Some(v) = headers.get(http::header::AUTHORIZATION).and_then(|v| v.to_str().ok())
        && let Some(t) = v.strip_prefix("Bearer ").or_else(|| v.strip_prefix("bearer "))
    {
        return Some(t.trim().to_string());
    }
    cookie(headers, SESSION_COOKIE)
}

pub fn cookie(headers: &HeaderMap, name: &str) -> Option<String> {
    for v in headers.get_all(http::header::COOKIE) {
        let Ok(s) = v.to_str() else { continue };
        for part in s.split(';') {
            if let Some((k, val)) = part.trim().split_once('=')
                && k == name
            {
                return Some(val.to_string());
            }
        }
    }
    None
}

/// Authenticated principal extractor.
pub struct Auth(pub Principal);

impl FromRequestParts<Arc<Node>> for Auth {
    type Rejection = Error;

    async fn from_request_parts(parts: &mut http::request::Parts, node: &Arc<Node>) -> Result<Self> {
        if let Some(p) = parts.extensions.get::<Principal>() {
            return Ok(Auth(p.clone()));
        }
        let t = credential(&parts.headers).ok_or_else(|| Error::Unauthenticated("missing credentials".into()))?;
        Ok(Auth(node.authenticate(&t).await?))
    }
}

/// The client address for a request, extracted from connection info or forwarded headers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientAddr(pub String);

impl<S: Send + Sync> FromRequestParts<S> for ClientAddr {
    type Rejection = Error;

    async fn from_request_parts(parts: &mut http::request::Parts, _: &S) -> Result<Self> {
        Ok(parts.extensions.get::<ClientAddr>().cloned().unwrap_or_else(|| ClientAddr("unknown".into())))
    }
}

/// Resolves the client IP, extracting the address `hops` positions from the end of
/// `X-Forwarded-For` if behind trusted proxies, or falling back to the peer socket.
fn client_addr(headers: &HeaderMap, peer: Option<std::net::SocketAddr>, hops: usize) -> String {
    if hops > 0 {
        let forwarded: Vec<&str> = headers
            .get_all("x-forwarded-for")
            .iter()
            .filter_map(|v| v.to_str().ok())
            .flat_map(|v| v.split(','))
            .map(str::trim)
            .filter(|a| !a.is_empty())
            .collect();
        // Ignore client-supplied headers before the trusted proxy hops.
        if let Some(a) = forwarded.len().checked_sub(hops).and_then(|i| forwarded.get(i)) {
            return a.to_string();
        }
    }
    peer.map_or_else(|| "unknown".into(), |p| p.ip().to_string())
}

async fn tag_client(State(node): State<Arc<Node>>, mut req: Request, next: Next) -> Response {
    if req.extensions().get::<ClientAddr>().is_none() {
        let peer = req.extensions().get::<axum::extract::ConnectInfo<std::net::SocketAddr>>().map(|c| c.0);
        let addr = client_addr(req.headers(), peer, node.cfg.limits.trusted_proxy_hops);
        req.extensions_mut().insert(ClientAddr(addr));
    }
    next.run(req).await
}

/// Context for database-scoped routes.
#[derive(Clone)]
pub struct DbCtx {
    pub principal: Principal,
    pub rec: DbRecord,
    pub access: Access,
    pub db: Arc<Db>,
    pub deadline: tokio::time::Instant,
}

impl DbCtx {
    pub fn require_write(&self) -> Result<()> {
        self.access.require_write()
    }
}

/// Response extension recording the peer node that handled a forwarded request.
#[derive(Clone)]
struct ForwardedTo(String);

/// Computes the request commit deadline, capped at `commit_timeout_ms` from now.
fn request_deadline(node: &Node, headers: &HeaderMap) -> tokio::time::Instant {
    let timeout = node.cfg.engine.commit_timeout_ms;
    let remaining = headers
        .get(DEADLINE_HEADER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .map_or(timeout, |d| d.saturating_sub(node.now_ms()).min(timeout));
    tokio::time::Instant::now() + std::time::Duration::from_millis(remaining)
}

pub fn percent_decode(s: &str) -> Vec<u8> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            let h = std::str::from_utf8(&b[i + 1..i + 3]).ok().and_then(|h| u8::from_str_radix(h, 16).ok());
            if let Some(v) = h {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    out
}

fn db_name_from_path(path: &str) -> Option<String> {
    let rest = path.strip_prefix("/v1/dbs/")?;
    let name = rest.split('/').next()?;
    if name.is_empty() {
        return None;
    }
    String::from_utf8(percent_decode(name)).ok()
}

fn clone_parts(req: &Request) -> http::request::Builder {
    let mut b = http::Request::builder().method(req.method().clone()).uri(req.uri().clone()).version(req.version());
    if let Some(h) = b.headers_mut() {
        *h = req.headers().clone();
    }
    b
}

const BUFFER_FORWARD_MAX: u64 = 1024 * 1024;

/// Authenticates database-scoped requests and forwards them to the lease holder or handles them locally.
async fn db_routing(State(node): State<Arc<Node>>, mut req: Request, next: Next) -> Response {
    let arrived = tokio::time::Instant::now();
    let res: Result<Response> = crate::engine::db::invoked_at(arrived, async {
        let deadline = request_deadline(&node, req.headers());
        let name = db_name_from_path(req.uri().path()).ok_or_else(|| Error::not_found("no such route"))?;
        let token = credential(req.headers()).ok_or_else(|| Error::Unauthenticated("missing credentials".into()))?;
        let principal = node.authenticate(&token).await?;
        let (mut rec, mut access) = node.resolve_db(&principal, &name).await?;
        let hops = hops(req.headers());
        if hops < 2
            && let Some((target, addr)) = node.route(&rec.id).await
        {
            let small = req.method() == http::Method::GET
                || req.method() == http::Method::DELETE
                || req
                    .headers()
                    .get(http::header::CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
                    .is_some_and(|n| n <= BUFFER_FORWARD_MAX);
            if small {
                let (parts, body) = req.into_parts();
                let bytes = axum::body::to_bytes(body, BUFFER_FORWARD_MAX as usize)
                    .await
                    .map_err(|e| Error::invalid(format!("could not read body: {e}")))?;
                let mut fwd = http::Request::builder()
                    .method(parts.method.clone())
                    .uri(parts.uri.clone())
                    .version(parts.version)
                    .body(Body::from(bytes.clone()))
                    .map_err(|e| Error::internal(e.to_string()))?;
                *fwd.headers_mut() = parts.headers.clone();
                fwd.headers_mut().insert(HOPS_HEADER, HeaderValue::from(hops + 1));
                tag_forward(&node, &mut fwd, &target, deadline);
                let idempotent = parts.method == http::Method::GET || parts.method == http::Method::HEAD;
                match node.services.transport.call(&addr, fwd).await {
                    Ok(resp) => return Ok(forwarded(resp, target)),
                    Err(te) if te.maybe_delivered && !idempotent => {
                        return Err(Error::Unavailable {
                            message: format!("forwarding to {target} failed: {}", te.message),
                            retry_after_ms: Some(1000),
                            outcome_unknown: true,
                        });
                    }
                    Err(_) => {
                        node.mark_suspect(&target);
                        req = Request::from_parts(parts, Body::from(bytes));
                    }
                }
            } else {
                let mut fwd = clone_parts(&req)
                    .body(std::mem::take(req.body_mut()))
                    .map_err(|e| Error::internal(e.to_string()))?;
                fwd.headers_mut().insert(HOPS_HEADER, HeaderValue::from(hops + 1));
                tag_forward(&node, &mut fwd, &target, deadline);
                return match node.services.transport.call(&addr, fwd).await {
                    Ok(resp) => Ok(forwarded(resp, target)),
                    Err(te) => {
                        node.mark_suspect(&target);
                        Err(Error::Unavailable {
                            message: format!("forwarding to {target} failed: {}", te.message),
                            retry_after_ms: Some(1000),
                            outcome_unknown: te.maybe_delivered,
                        })
                    }
                };
            }
        }
        // Re-resolve strongly if the cached database name points to a deleted or recreated database.
        let opened = match node.open_db(&rec).await {
            Ok(db) => match db.latest().await {
                Err(Error::NotFound(m)) => Err(m),
                Err(e) => return Err(e),
                Ok(_) => Ok(db),
            },
            Err(Error::NotFound(m)) => Err(m),
            Err(e) => return Err(e),
        };
        let db = match opened {
            Ok(db) => db,
            Err(m) if name.starts_with("id:") => return Err(Error::NotFound(m)),
            Err(m) => {
                let (fresh, fresh_access) = node.resolve_db_strong(&principal, &name).await?;
                if fresh.id == rec.id {
                    // Finish any interrupted deletion where the manifest was marked deleted before metadata removal.
                    match node.run_meta(crate::meta::MetaCmd::DeleteDb { id: rec.id.clone() }).await {
                        Ok(_) | Err(Error::NotFound(_)) => node.forget_db_name(&rec.owner_id, &rec.name),
                        Err(e) => return Err(e),
                    }
                    return Err(Error::NotFound(m));
                }
                (rec, access) = (fresh, fresh_access);
                node.open_db(&rec).await?
            }
        };
        req.extensions_mut().insert(principal.clone());
        req.extensions_mut().insert(DbCtx { principal, rec: rec.clone(), access, db, deadline });
        let resp = next.run(req).await;
        if resp.status() == StatusCode::NOT_FOUND {
            node.forget_db_name(&rec.owner_id, &rec.name);
        }
        Ok(resp)
    })
    .await;
    res.unwrap_or_else(|e| e.into_response())
}

fn forwarded(mut resp: Response, target: String) -> Response {
    resp.extensions_mut().insert(ForwardedTo(target));
    resp
}

fn tag_forward(node: &Node, req: &mut http::Request<Body>, target: &str, deadline: tokio::time::Instant) {
    if let Ok(v) = HeaderValue::from_str(&node.env.random_hex(16)) {
        req.headers_mut().insert(FORWARD_ID_HEADER, v);
    }
    if let Ok(v) = HeaderValue::from_str(target) {
        req.headers_mut().insert(FORWARD_TO_HEADER, v);
    }
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now()).as_millis() as u64;
    req.headers_mut().insert(DEADLINE_HEADER, HeaderValue::from(node.now_ms() + remaining));
}

fn hops(h: &HeaderMap) -> u32 {
    h.get(HOPS_HEADER).and_then(|v| v.to_str().ok()).and_then(|v| v.parse().ok()).unwrap_or(0)
}

/// Assigns an `x-request-id` header, runs the request in a scoped log context, and logs completion.
async fn request_context(State(node): State<Arc<Node>>, mut req: Request, next: Next) -> Response {
    let given = req.headers().get(REQUEST_ID_HEADER).filter(|v| v.to_str().is_ok_and(log::valid_request_id));
    let header = match given {
        Some(v) => v.clone(),
        None => HeaderValue::from_str(&node.env.random_hex(8)).unwrap_or(HeaderValue::from_static("0")),
    };
    req.headers_mut().insert(REQUEST_ID_HEADER, header.clone());
    let rid = header.to_str().unwrap_or_default();
    let ctx = log::Context { node: node.id.as_str().into(), request_id: rid.into() };
    let mut resp = log::scoped(ctx, async move {
        if !log::enabled(log::Level::Info) {
            return next.run(req).await;
        }
        let started = tokio::time::Instant::now();
        let method = req.method().to_string();
        let route = req.extensions().get::<MatchedPath>().map_or("unmatched", |m| m.as_str()).to_string();
        let hops = hops(req.headers());
        let tally = Arc::new(timing::Tally::default());
        let resp = timing::scoped(tally.clone(), next.run(req)).await;
        let mut fields = vec![
            ("method", json!(method)),
            ("route", json!(route)),
            ("status", json!(resp.status().as_u16())),
            ("ms", json!(started.elapsed().as_millis() as u64)),
            ("hops", json!(hops)),
        ];
        if let Some(ForwardedTo(t)) = resp.extensions().get::<ForwardedTo>() {
            fields.push(("forwarded_to", json!(t)));
        }
        if let Some(store) = tally.store_json() {
            fields.push(("store_ms", json!(tally.store_ms())));
            fields.push(("store", store));
        }
        if let (n @ 1.., wait) = tally.commits() {
            fields.push(("commits", json!(n)));
            fields.push(("commit_ms", json!(wait)));
        }
        if tally.read_wait_ms() > 0 {
            fields.push(("read_wait_ms", json!(tally.read_wait_ms())));
        }
        log::event(log::Level::Info, "request", &fields);
        resp
    })
    .await;
    resp.headers_mut().insert(REQUEST_ID_HEADER, header);
    resp
}

/// Deduplicates forwarded write requests and rejects requests addressed to a previous node instance.
async fn dedup_forwarded(State(node): State<Arc<Node>>, req: Request, next: Next) -> Response {
    let Some(id) = req.headers().get(FORWARD_ID_HEADER).and_then(|v| v.to_str().ok()) else {
        return next.run(req).await;
    };
    if req.method() == http::Method::GET || req.method() == http::Method::HEAD {
        return next.run(req).await;
    }
    if req.headers().get(FORWARD_TO_HEADER).and_then(|v| v.to_str().ok()) != Some(node.id.as_str()) {
        return Error::Unavailable {
            message: "forwarded to a node that is no longer running".into(),
            retry_after_ms: Some(100),
            outcome_unknown: false,
        }
        .into_response();
    }
    // Scope deduplication keys to caller credentials and request target.
    let mut scope = Vec::new();
    for h in [http::header::AUTHORIZATION, http::header::COOKIE] {
        for v in req.headers().get_all(h) {
            scope.extend_from_slice(v.as_bytes());
            scope.push(0);
        }
    }
    scope.extend_from_slice(req.method().as_str().as_bytes());
    scope.push(0);
    scope.extend_from_slice(req.uri().to_string().as_bytes());
    let key = format!("{id}:{}", crate::util::b64::hex(&crate::util::sha256(&scope)));
    match node.forwards.deliver(&key) {
        Delivery::First(slot) => {
            let (parts, body) = next.run(req).await.into_parts();
            match http_body_util::BodyExt::collect(body).await {
                Ok(b) => {
                    let r = ReplayedResponse { status: parts.status, headers: parts.headers, body: b.to_bytes() };
                    slot.finish(Some(r.clone()));
                    replay(r)
                }
                Err(e) => {
                    slot.finish(None);
                    unknown_outcome(format!("response failed: {e}"))
                }
            }
        }
        Delivery::Duplicate(wait) => match wait.await {
            Some(r) => replay(r),
            None => unknown_outcome("duplicate of a forwarded request with an unknown outcome".into()),
        },
    }
}

fn replay(r: ReplayedResponse) -> Response {
    let mut resp = Response::new(Body::from(r.body));
    *resp.status_mut() = r.status;
    *resp.headers_mut() = r.headers;
    resp
}

fn unknown_outcome(message: String) -> Response {
    Error::Unavailable { message, retry_after_ms: Some(1000), outcome_unknown: true }.into_response()
}

/// Requires a valid cluster key header on internal endpoints.
async fn internal_auth(State(node): State<Arc<Node>>, req: Request, next: Next) -> Response {
    let ok = match node.cluster_key() {
        None => false,
        Some(k) => req
            .headers()
            .get(CLUSTER_KEY_HEADER)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| crate::util::ct_eq(v.as_bytes(), k.as_bytes())),
    };
    if !ok {
        return Error::PermissionDenied("bad cluster key".into()).into_response();
    }
    next.run(req).await
}

pub fn consistency_of(q: &std::collections::BTreeMap<String, String>) -> Result<Consistency> {
    match q.get("consistency").map(|s| s.as_str()) {
        None | Some("strong") => Ok(Consistency::Strong),
        Some("snapshot") => Ok(Consistency::Snapshot {
            max_staleness_ms: q
                .get("max_staleness_ms")
                .map(|s| s.parse().map_err(|_| Error::invalid("bad max_staleness_ms")))
                .transpose()?
                .unwrap_or(5000),
        }),
        Some(o) => Err(Error::invalid(format!("unknown consistency {o:?}"))),
    }
}

/// How [`run_txn!`] ended.
pub(crate) enum Ran<T> {
    /// The transaction wrote nothing; it read at `version`.
    Read {
        out: T,
        version: u64,
    },
    Committed {
        out: T,
        commit: Committed,
    },
    /// An earlier attempt with the same idempotency key had committed.
    /// Never without a key.
    Replayed(Committed),
}

/// Runs `$body` in fresh transactions until what it wrote is committed,
/// retrying conflicts; evaluates to `(commit_id, Ran)`. With an idempotency
/// key (`$key`, an `Option<&str>`) the commit id derives from the key, and a
/// commit an earlier attempt applied is reported rather than applied again
/// (SPEC §6.2).
macro_rules! run_txn {
    ($node:expr, $ctx:expr, $isolation:expr, $consistency:expr, $key:expr, |$t:ident| $body:expr) => {{
        let (node_, ctx_, key_): (&crate::node::Node, &crate::api::DbCtx, Option<&str>) = (&$node, &$ctx, $key);
        let commit_id_ = crate::api::commit_id_for(node_, ctx_, key_)?;
        let ran_ = match crate::api::earlier_attempt(ctx_, commit_id_, key_).await? {
            Some(c) => crate::api::Ran::Replayed(c),
            None => {
                let mut attempt_ = 0u32;
                let mut settle_ = false;
                loop {
                    let mut txn_ = crate::api::begin_attempt(ctx_, $isolation, $consistency, settle_).await?;
                    txn_.commit_id = commit_id_;
                    let out_ = {
                        let $t: &mut crate::engine::Txn = &mut txn_;
                        async { $body }.await
                    };
                    let txn_ = match txn_ {
                        t if !t.needs_recheck(out_.is_ok() && !t.is_read_only()) => t,
                        t => match ctx_.db.recheck(t).await? {
                            Some(version) => match out_ {
                                Ok(out) => break crate::api::Ran::Read { out, version },
                                Err(e) => break crate::api::failed(ctx_, commit_id_, key_, e).await?,
                            },
                            None => {
                                settle_ = true;
                                continue;
                            }
                        },
                    };
                    let out_ = match out_ {
                        Ok(o) => o,
                        Err(e) => break crate::api::failed(ctx_, commit_id_, key_, e).await?,
                    };
                    if let Some(ran) = crate::api::finish(node_, ctx_, txn_, out_, key_, &mut attempt_).await? {
                        break ran;
                    }
                }
            }
        };
        (commit_id_, ran_)
    }};
}
pub(crate) use run_txn;

/// Uses [`Db::begin_write`] for strong serializable attempts unless `settle`.
pub(crate) async fn begin_attempt(
    ctx: &DbCtx,
    isolation: crate::engine::Isolation,
    c: Consistency,
    settle: bool,
) -> Result<Txn> {
    if c == Consistency::Strong && isolation == crate::engine::Isolation::Serializable && !settle {
        ctx.db.begin_write().await
    } else {
        ctx.db.begin(isolation, c).await
    }
}

pub(crate) fn commit_id_for(node: &Node, ctx: &DbCtx, key: Option<&str>) -> Result<u64> {
    match key {
        Some(k) if k.is_empty() || k.len() > 200 => Err(Error::invalid("idempotency keys are 1-200 characters")),
        Some(k) => Ok(data::commit_id_for_key(&ctx.rec.id, k)),
        None => Ok(node.env.random_u64()),
    }
}

/// The commit of an earlier attempt with the idempotency key, if one applied.
pub(crate) async fn earlier_attempt(ctx: &DbCtx, commit_id: u64, key: Option<&str>) -> Result<Option<Committed>> {
    match key {
        Some(_) => ctx.db.commit_status(commit_id).await,
        None => Ok(None),
    }
}

/// Commits `t`, which produced `out`; `None` if it conflicted and is to be
/// run again.
pub(crate) async fn finish<T>(
    node: &Node,
    ctx: &DbCtx,
    t: Txn,
    out: T,
    key: Option<&str>,
    attempt: &mut u32,
) -> Result<Option<Ran<T>>> {
    if t.is_read_only() {
        return Ok(Some(Ran::Read { out, version: t.read_version() }));
    }
    let commit_id = t.commit_id;
    let mut req = t.into_request();
    req.deadline = Some(ctx.deadline);
    match ctx.db.commit_detailed(req).await {
        // Without a key, only this attempt's own commit, landed by an
        // ambiguous manifest write the committer retried, can be replayed.
        Ok(c) if c.replayed && key.is_some() => Ok(Some(Ran::Replayed(c))),
        Ok(commit) => Ok(Some(Ran::Committed { out, commit })),
        Err(Error::Conflict(_)) if *attempt < node.cfg.txn_retries => {
            *attempt += 1;
            let backoff = 5 * (1 << *attempt) + node.env.random_below(10);
            tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
            Ok(None)
        }
        Err(e) => failed(ctx, commit_id, key, e).await.map(Some),
    }
}

/// Answers a failed transaction. A retried idempotent request can fail a
/// precondition, or an append its expected seq, because an earlier attempt
/// applied: a fenced status check tells.
pub(crate) async fn failed<T>(ctx: &DbCtx, commit_id: u64, key: Option<&str>, e: Error) -> Result<Ran<T>> {
    match e {
        Error::PreconditionFailed { .. } if key.is_some() => match ctx.db.commit_status_fenced(commit_id).await? {
            Some(c) => Ok(Ran::Replayed(c)),
            None => Err(e),
        },
        e => Err(naming_commit(e, commit_id)),
    }
}

/// Names the commit in an error that leaves its outcome unknown, so that
/// the client can resolve it (SPEC §6.2).
pub(crate) fn naming_commit(e: Error, commit_id: u64) -> Error {
    match e {
        Error::Unavailable { message, retry_after_ms, outcome_unknown: true } => Error::Unavailable {
            message: format!("{message} (commit_id {})", data::commit_hex(commit_id)),
            retry_after_ms,
            outcome_unknown: true,
        },
        other => other,
    }
}

/// Runs `$body` in a serializable transaction and commits, retrying on
/// conflict; evaluates to `(output, version)`.
macro_rules! with_txn {
    ($node:expr, $ctx:expr, |$t:ident| $body:expr) => {{
        $ctx.require_write()?;
        let (_, ran_) = crate::api::run_txn!(
            $node,
            $ctx,
            crate::engine::Isolation::Serializable,
            crate::engine::Consistency::Strong,
            None,
            |$t| $body
        );
        match ran_ {
            crate::api::Ran::Read { out, version } => (out, version),
            crate::api::Ran::Committed { out, commit } => (out, commit.version),
            crate::api::Ran::Replayed(_) => {
                return Err(crate::error::Error::internal("a transaction without an idempotency key replayed"));
            }
        }
    }};
}
pub(crate) use with_txn;

/// Runs `$body` on a read snapshot and returns `(output, read_version)`.
macro_rules! with_read {
    ($db:expr, $c:expr, |$t:ident| $body:expr) => {{
        let mut txn_ = $db.begin(crate::engine::Isolation::Serializable, $c).await?;
        let v_ = txn_.read_version();
        let out_ = {
            let $t: &mut crate::engine::Txn = &mut txn_;
            $body
        }?;
        (out_, v_)
    }};
}
pub(crate) use with_read;

/// What a long poll reads: whether it found what the poll waits for.
pub(crate) trait Found {
    fn found(&self) -> bool;
}

/// Runs `$read` on fresh snapshots until what it reads is [`Found`] or
/// `$wait` elapses; evaluates to `(output, read_version)`.
macro_rules! watch {
    ($node:expr, $db:expr, $c:expr, $wait:expr, |$t:ident| $read:expr) => {{
        let until_ = tokio::time::Instant::now() + $wait;
        loop {
            let (out_, v_) = crate::api::with_read!($db, $c, |$t| $read);
            let left_ = until_.saturating_duration_since(tokio::time::Instant::now());
            if crate::api::Found::found(&out_) || left_.is_zero() {
                break (out_, v_);
            }
            let poll_ = std::time::Duration::from_millis($node.cfg.watch_poll_ms);
            $db.wait_for_change(v_, left_.min(poll_)).await;
        }
    }};
}
pub(crate) use watch;

/// Adds `Connection: close` if the request body went unread, so a proxy
/// doesn't reuse a connection hyper is about to close.
async fn close_if_body_unread(req: Request, next: Next) -> Response {
    let (parts, body) = req.into_parts();
    let (tracked, read) = body::Tracked::new(body);
    let mut resp = next.run(Request::from_parts(parts, Body::new(tracked))).await;
    if !read.load(std::sync::atomic::Ordering::SeqCst) {
        resp.headers_mut().insert(http::header::CONNECTION, HeaderValue::from_static("close"));
    }
    resp
}

async fn healthz(State(node): State<Arc<Node>>) -> Response {
    if node.draining() {
        return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(json!({"ok": false, "draining": true}))).into_response();
    }
    axum::Json(json!({"ok": true, "node": node.status_json()})).into_response()
}

async fn openapi() -> Response {
    ([(http::header::CONTENT_TYPE, "application/json")], openapi::document_json()).into_response()
}

async fn not_found() -> Error {
    Error::not_found("no such route")
}

/// Builds the HTTP router for the node.
pub fn router(node: Arc<Node>) -> Router {
    let db_routes =
        data::routes().merge(streams::routes()).route_layer(middleware::from_fn_with_state(node.clone(), db_routing));
    let internal = Router::new()
        .route("/internal/v1/meta", post(account::internal_meta))
        .route_layer(middleware::from_fn_with_state(node.clone(), internal_auth));
    let r = Router::new()
        .route("/healthz", get(healthz))
        .route("/v1/openapi.json", get(openapi))
        .merge(account::routes())
        .merge(orgs::routes())
        .route("/v1/dbs", get(data::list_dbs).post(data::create_db))
        .merge(db_routes)
        .merge(internal)
        .merge(crate::console::routes())
        .fallback(not_found)
        .layer(middleware::from_fn_with_state(node.clone(), dedup_forwarded))
        .layer(middleware::from_fn_with_state(node.clone(), tag_client))
        .layer(DefaultBodyLimit::max(node.cfg.max_json_body))
        .layer(middleware::from_fn_with_state(node.clone(), request_context))
        .layer(middleware::from_fn(close_if_body_unread))
        .with_state(node.clone());
    node.set_router(r.clone());
    r
}

pub(crate) use axum::extract::Query as QueryParams;

pub(crate) type Params = QueryParams<std::collections::BTreeMap<String, String>>;

/// Extracts `limit` and `cursor` pagination parameters.
pub(crate) fn paging(q: &Params) -> Result<(usize, Option<String>)> {
    let limit = match q.get("limit") {
        None => crate::meta::PAGE_DEFAULT,
        Some(l) => l.parse().map_err(|_| Error::invalid("limit must be a number"))?,
    };
    Ok((limit, q.get("cursor").filter(|c| !c.is_empty()).cloned()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt;

    #[test]
    fn responses_close_connections_whose_request_body_went_unread() {
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        rt.block_on(async {
            // Constants keep these out of the documented-routes scan (tests/openapi.rs).
            const IGNORE: &str = "/ignore";
            const READ: &str = "/read";
            const FIRST_FRAME: &str = "/first-frame";
            let first_frame = |mut b: Body| async move {
                use http_body_util::BodyExt;
                let n = b.frame().await.and_then(|f| f.ok()?.into_data().ok()).map_or(0, |d| d.len());
                assert!(http_body::Body::is_end_stream(&b));
                n.to_string()
            };
            let app = Router::new()
                .route(IGNORE, axum::routing::any(|| async { StatusCode::TOO_MANY_REQUESTS }))
                .route(READ, axum::routing::post(|b: Bytes| async move { b.len().to_string() }))
                .route(FIRST_FRAME, axum::routing::post(first_frame))
                .layer(middleware::from_fn(close_if_body_unread));
            let close = |method: &str, path: &str, body: &'static str| {
                let req = http::Request::builder().method(method).uri(path).body(Body::from(body)).unwrap();
                let app = app.clone();
                async move { app.oneshot(req).await.unwrap().headers().get(http::header::CONNECTION).cloned() }
            };
            assert_eq!(close("POST", "/ignore", "unread").await, Some(HeaderValue::from_static("close")));
            assert_eq!(close("POST", "/read", "read").await, None);
            assert_eq!(close("POST", "/first-frame", "read").await, None);
            assert_eq!(close("GET", "/ignore", "").await, None);
        });
    }

    #[test]
    fn client_addresses_come_from_trusted_proxies_only() {
        let peer: std::net::SocketAddr = "10.0.0.9:443".parse().unwrap();
        let mut h = HeaderMap::new();
        assert_eq!(client_addr(&h, Some(peer), 1), "10.0.0.9", "no header: the peer");
        h.append("x-forwarded-for", HeaderValue::from_static("6.6.6.6, 1.2.3.4"));
        assert_eq!(client_addr(&h, Some(peer), 0), "10.0.0.9", "no trusted proxies: the header is ignored");
        assert_eq!(client_addr(&h, Some(peer), 1), "1.2.3.4", "appended by the proxy");
        h.append("x-forwarded-for", HeaderValue::from_static("5.5.5.5"));
        assert_eq!(client_addr(&h, Some(peer), 2), "1.2.3.4", "two proxies");
        assert_eq!(client_addr(&h, Some(peer), 4), "10.0.0.9", "fewer entries than proxies");
        assert_eq!(client_addr(&HeaderMap::new(), None, 0), "unknown");
    }
}
