//! Stream routes (SPEC §4.6, §10): streams, appends, reads as JSON,
//! NDJSON or server-sent events, queries, trims and consumers.

use super::data::{PathMap, bool_param, commit_hex, encoding_param, param, split_consistency};
use super::{DbCtx, Found, JsonBody, Params, Ran, consistency_of, paging, run_txn, watch, with_read, with_txn};
use crate::engine::{Committed, Consistency, Db, Isolation, Retention, StreamState};
use crate::error::{Error, Result};
use crate::node::Node;
use crate::ops::Encoding;
use crate::stream::{self, AppendTo, Body, NewRecord, ReadOut, ReadSpec};
use axum::Router;
use axum::extract::{Extension, Path, Request, State};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, StatusCode};
use serde::Deserialize;
use serde_json::{Value as Json, json};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::Instant;

type J = axum::Json<Json>;

const WAIT_MAX_MS: u64 = 20_000;
const EVENTS_WAIT_MAX_MS: u64 = 300_000;
const KEEPALIVE: Duration = Duration::from_secs(15);
/// Consumers stream info lists; the consumers route pages through the rest.
const INFO_CONSUMERS: usize = 100;

pub fn routes() -> Router<Arc<Node>> {
    Router::new()
        .route("/v1/dbs/{db}/streams", get(list).post(create))
        .route("/v1/dbs/{db}/streams/{s}", get(info).patch(configure).delete(delete))
        .route("/v1/dbs/{db}/streams/{s}/records", get(read).post(append))
        .route("/v1/dbs/{db}/streams/{s}/records/query", post(query))
        .route("/v1/dbs/{db}/streams/{s}/trim", post(trim))
        .route("/v1/dbs/{db}/streams/{s}/consumers", get(consumers))
        .route("/v1/dbs/{db}/streams/{s}/consumers/{c}", get(get_consumer).put(put_consumer).delete(delete_consumer))
}

fn number<T: std::str::FromStr>(q: &Params, k: &str) -> Result<Option<T>> {
    q.get(k).map(|v| v.parse().map_err(|_| Error::invalid(format!("{k} must be a number")))).transpose()
}

fn page_of(q: &Params) -> Result<(usize, Option<String>)> {
    let (limit, cursor) = paging(q)?;
    if !(1..=1000).contains(&limit) {
        return Err(Error::invalid("limit must be in 1..=1000"));
    }
    Ok((limit, cursor))
}

async fn list(Extension(ctx): Extension<DbCtx>, q: Params) -> Result<J> {
    let (limit, cursor) = page_of(&q)?;
    let prefix = q.get("prefix").cloned().unwrap_or_default();
    let c = consistency_of(&q)?;
    let ((page, next), v) = with_read!(ctx.db, c, |t| stream::list(t, &prefix, limit, cursor.as_deref()).await);
    let streams: Vec<Json> = page.iter().map(|(n, c, st)| stream::stream_json(n, c, st)).collect();
    Ok(axum::Json(json!({"streams": streams, "next_cursor": next, "version": v})))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateBody {
    name: String,
    #[serde(default)]
    retention_ms: Option<u64>,
    #[serde(default)]
    max_bytes: Option<u64>,
}

async fn create(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    JsonBody(b): JsonBody<CreateBody>,
) -> Result<Response> {
    let r = Retention { retention_ms: b.retention_ms, max_bytes: b.max_bytes };
    let (c, v) = with_txn!(node, ctx, |t| stream::create(t, &b.name, r, node.env.random_u64(), node.now_ms()).await);
    let mut out = stream::stream_json(&b.name, &c, &StreamState::new(r));
    out["version"] = json!(v);
    Ok((StatusCode::CREATED, axum::Json(out)).into_response())
}

async fn info(Extension(ctx): Extension<DbCtx>, Path(p): PathMap, q: Params) -> Result<J> {
    let name = param(&p, "s")?.to_string();
    let c = consistency_of(&q)?;
    let ((cat, st, (consumers, next)), v) = with_read!(ctx.db, c, |t| {
        let (cat, st) = stream::info(t, &name).await?;
        let consumers = stream::list_consumers(t, &name, INFO_CONSUMERS, None).await?;
        Ok::<_, Error>((cat, st, consumers))
    });
    let mut out = stream::stream_json(&name, &cat, &st);
    out["consumers"] = Json::Array(consumers.iter().map(|(n, x)| consumer_json(n, x, &st)).collect());
    out["consumers_next_cursor"] = json!(next);
    out["version"] = json!(v);
    Ok(axum::Json(out))
}

fn consumer_json(name: &str, c: &stream::Consumer, st: &StreamState) -> Json {
    json!({"name": name, "seq": c.seq, "lag": st.next_seq.saturating_sub(c.seq.max(st.first_seq)), "updated_at": c.updated_at})
}

/// Reads a field of a partial update: absent, `null`, or a number.
fn patch_field(o: &serde_json::Map<String, Json>, k: &str) -> Result<Option<Option<u64>>> {
    match o.get(k) {
        None => Ok(None),
        Some(Json::Null) => Ok(Some(None)),
        Some(v) => {
            v.as_u64().map(|x| Some(Some(x))).ok_or_else(|| Error::invalid(format!("{k} must be a number or null")))
        }
    }
}

async fn configure(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    Path(p): PathMap,
    JsonBody(b): JsonBody<Json>,
) -> Result<J> {
    let name = param(&p, "s")?.to_string();
    let o = b.as_object().ok_or_else(|| Error::invalid("body must be a JSON object"))?;
    if let Some(k) = o.keys().find(|k| *k != "retention_ms" && *k != "max_bytes") {
        return Err(Error::invalid(format!("unknown field {k:?}")));
    }
    let (retention_ms, max_bytes) = (patch_field(o, "retention_ms")?, patch_field(o, "max_bytes")?);
    let ((c, st), v) = with_txn!(node, ctx, |t| {
        let cur = stream::open(t, &name).await?;
        let r = Retention {
            retention_ms: retention_ms.unwrap_or(cur.retention_ms),
            max_bytes: max_bytes.unwrap_or(cur.max_bytes),
        };
        let c = stream::configure(t, &name, r).await?;
        let st = t.untracked().stream_state(c.id).await?.unwrap_or(StreamState::new(r));
        Ok::<_, Error>((c, StreamState { retention: r, ..st }))
    });
    let mut out = stream::stream_json(&name, &c, &st);
    out["version"] = json!(v);
    Ok(axum::Json(out))
}

async fn delete(State(node): State<Arc<Node>>, Extension(ctx): Extension<DbCtx>, Path(p): PathMap) -> Result<J> {
    let name = param(&p, "s")?.to_string();
    let ((), v) = with_txn!(node, ctx, |t| stream::delete(t, &name).await);
    Ok(axum::Json(json!({"version": v})))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AppendBody {
    records: Vec<NewRecord>,
    #[serde(default)]
    expect_seq: Option<u64>,
    #[serde(default)]
    create: bool,
    #[serde(default)]
    idempotency_key: Option<String>,
    #[serde(default)]
    encoding: Option<Encoding>,
}

fn content_type(h: &HeaderMap) -> String {
    let ct = h.get(http::header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).unwrap_or("");
    ct.split(';').next().unwrap_or("").trim().to_ascii_lowercase()
}

fn idempotency_header(h: &HeaderMap) -> Result<Option<String>> {
    match h.get("idempotency-key") {
        None => Ok(None),
        Some(v) => match v.to_str() {
            Ok(k) => Ok(Some(k.to_string())),
            Err(_) => Err(Error::invalid("Idempotency-Key must be visible ASCII")),
        },
    }
}

async fn append(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    Path(p): PathMap,
    q: Params,
    req: Request,
) -> Result<Response> {
    ctx.require_write()?;
    let name = param(&p, "s")?.to_string();
    let ct = content_type(req.headers());
    let mut key = idempotency_header(req.headers())?;
    let mut expect = number::<u64>(&q, "expect_seq")?;
    let mut create = bool_param(&q, "create")?;
    let limit = node.cfg.engine.txn_max_write_bytes + (1 << 20);
    let data = axum::body::to_bytes(req.into_body(), limit)
        .await
        .map_err(|_| Error::TooLarge(format!("appends are limited to {limit} bytes")))?;
    let bodies: Vec<Body> = match ct.as_str() {
        "" | "application/json" => {
            let b: AppendBody =
                serde_json::from_slice(&data).map_err(|e| Error::invalid(format!("invalid JSON body: {e}")))?;
            expect = expect.or(b.expect_seq);
            create |= b.create;
            key = key.or(b.idempotency_key);
            let enc = match b.encoding {
                Some(e) => e,
                None => encoding_param(&q)?,
            };
            b.records.into_iter().map(|r| r.into_body(enc)).collect::<Result<_>>()?
        }
        "application/x-ndjson" | "application/jsonl" | "application/json-lines" => stream::parse_lines(&data, true)?,
        "text/plain" => stream::parse_lines(&data, false)?,
        other => {
            return Err(Error::invalid(format!(
                "unsupported content type {other:?}: use application/json, application/x-ndjson or text/plain \
                 (curl --data-binary sends application/x-www-form-urlencoded unless given -H 'content-type: ...')"
            )));
        }
    };
    let record_max = ctx.db.shared().cfg.stream_record_max;
    let (commit_id, ran) = run_txn!(node, ctx, Isolation::Serializable, Consistency::Strong, key.as_deref(), |t| {
        let to = AppendTo {
            name: &name,
            create: create.then(|| (node.env.random_u64(), node.now_ms())),
            expect,
            op_index: None,
            record_max,
        };
        stream::append(t, &to, &bodies).await
    });
    match ran {
        Ran::Committed { commit, .. } => appended(&commit, false, commit_id),
        Ran::Replayed(commit) => appended(&commit, true, commit_id),
        Ran::Read { .. } => Err(Error::internal("an append committed nothing")),
    }
}

/// The answer to an append, from the commit that applied it.
fn appended(c: &Committed, replayed: bool, commit_id: u64) -> Result<Response> {
    let [a] = c.appends[..] else {
        return Err(Error::invalid("the idempotency key was used by a request other than an append"));
    };
    Ok(axum::Json(json!({
        "first_seq": a.first_seq, "next_seq": a.first_seq + a.count, "count": a.count, "time": c.time_ms,
        "version": c.version, "commit_id": commit_hex(commit_id), "replayed": replayed,
    }))
    .into_response())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Format {
    Json,
    Ndjson,
    Events,
}

fn format_of(q: &Params, h: &HeaderMap) -> Result<Format> {
    match q.get("format").map(String::as_str) {
        Some("json") => Ok(Format::Json),
        Some("ndjson") => Ok(Format::Ndjson),
        Some("sse") => Ok(Format::Events),
        Some(o) => Err(Error::invalid(format!("unknown format {o:?}: use json, ndjson or sse"))),
        None => {
            let accept = h.get(http::header::ACCEPT).and_then(|v| v.to_str().ok()).unwrap_or("");
            Ok(match accept {
                a if a.contains("text/event-stream") => Format::Events,
                a if a.contains("application/x-ndjson") => Format::Ndjson,
                _ => Format::Json,
            })
        }
    }
}

/// Reads once on a fresh snapshot.
async fn read_once(db: &Arc<Db>, c: Consistency, name: &str, spec: &ReadSpec) -> Result<(ReadOut, u64)> {
    Ok(with_read!(db, c, |t| stream::read(t, name, spec).await))
}

impl Found for ReadOut {
    fn found(&self) -> bool {
        !self.records.is_empty() || !self.open
    }
}

/// The parameters a read takes besides those of its [`ReadSpec`].
const READ_PARAMS: [&str; 5] = ["format", "wait_ms", "encoding", "consistency", "max_staleness_ms"];

/// The read a query string describes, every parameter not in
/// [`READ_PARAMS`] being a field of [`ReadSpec`].
fn read_spec(q: &Params) -> Result<ReadSpec> {
    let fields: Vec<(&String, &String)> = q.iter().filter(|(k, _)| !READ_PARAMS.contains(&k.as_str())).collect();
    let query = serde_urlencoded::to_string(fields).map_err(|e| Error::internal(e.to_string()))?;
    serde_urlencoded::from_str(&query).map_err(|e| Error::invalid(format!("invalid read parameters: {e}")))
}

async fn read(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    Path(p): PathMap,
    q: Params,
    headers: HeaderMap,
) -> Result<Response> {
    let name = param(&p, "s")?.to_string();
    let enc = encoding_param(&q)?;
    let c = consistency_of(&q)?;
    let format = format_of(&q, &headers)?;
    let mut spec = read_spec(&q)?;
    spec.validate()?;
    let wait_max = if format == Format::Events { EVENTS_WAIT_MAX_MS } else { WAIT_MAX_MS };
    let wait = number::<u64>(&q, "wait_ms")?.unwrap_or(0);
    if wait > wait_max {
        return Err(Error::invalid(format!("wait_ms must be in 0..={wait_max}")));
    }
    if wait > 0 && spec.reverse {
        return Err(Error::invalid("only forward reads wait"));
    }
    if format == Format::Events {
        if spec.reverse {
            return Err(Error::invalid("server-sent events read forward"));
        }
        let last = headers.get("last-event-id").and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<u64>().ok());
        if let Some(l) = last {
            spec = ReadSpec { from_seq: Some(l.saturating_add(1)), ..starting_anywhere(spec) };
        }
        // The first read answers errors with their status, before the
        // response commits to an event stream.
        let first = read_once(&ctx.db, c, &name, &spec).await?;
        let follow = Follow { db: ctx.db.clone(), c, name, enc, wait: Duration::from_millis(wait) };
        return Ok(events(node, follow, spec, first));
    }
    let (out, v) = watch!(node, ctx.db, c, Duration::from_millis(wait), |t| stream::read(t, &name, &spec).await);
    let records = out.records.iter().map(|(r, b)| stream::record_json(r, b, enc));
    if format == Format::Ndjson {
        let mut body = Vec::new();
        for r in records {
            body.extend_from_slice(r.to_string().as_bytes());
            body.push(b'\n');
        }
        let mut resp = Response::new(axum::body::Body::from(body));
        let h = resp.headers_mut();
        h.insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/x-ndjson"));
        h.insert("x-baserunna-next-seq", HeaderValue::from(out.next_seq));
        h.insert("x-baserunna-first-seq", HeaderValue::from(out.first_seq));
        h.insert("x-baserunna-tail-seq", HeaderValue::from(out.tail_seq));
        h.insert("x-baserunna-done", HeaderValue::from_static(if out.done { "true" } else { "false" }));
        h.insert("x-baserunna-version", HeaderValue::from(v));
        return Ok(resp);
    }
    Ok(axum::Json(read_json(records.collect(), &out, v)).into_response())
}

/// `spec` without where it starts.
fn starting_anywhere(spec: ReadSpec) -> ReadSpec {
    ReadSpec { from_seq: None, from_time: None, tail: None, consumer: None, ..spec }
}

fn read_json(records: Vec<Json>, out: &ReadOut, version: u64) -> Json {
    json!({
        "records": records, "next_seq": out.next_seq, "first_seq": out.first_seq,
        "tail_seq": out.tail_seq, "done": out.done, "version": version,
    })
}

/// A response body fed by a task, chunk by chunk.
struct ChannelBody(tokio::sync::mpsc::Receiver<Bytes>);

impl http_body::Body for ChannelBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<http_body::Frame<Bytes>, Self::Error>>> {
        self.get_mut().0.poll_recv(cx).map(|b| b.map(|b| Ok(http_body::Frame::data(b))))
    }
}

/// A stream followed as server-sent events.
struct Follow {
    db: Arc<Db>,
    c: Consistency,
    name: String,
    enc: Encoding,
    /// How long to wait for appends.
    wait: Duration,
}

/// Serves a forward read as server-sent events, starting with `first`, its
/// first page: records as they are appended for up to `f.wait`, until
/// `limit` records or the end of the read's range, or until the node drains
/// (the client resumes elsewhere with `Last-Event-ID`).
fn events(node: Arc<Node>, f: Follow, spec: ReadSpec, first: (ReadOut, u64)) -> Response {
    let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(16);
    let poll = Duration::from_millis(node.cfg.watch_poll_ms);
    let env = node.env.clone();
    env.spawn(async move {
        let Follow { db, c, name, enc, wait } = f;
        let until = Instant::now() + wait;
        let mut left = spec.limit.map(|l| l as u64);
        let mut spec = spec;
        let mut quiet_since = Instant::now();
        let mut page = Some(first);
        loop {
            let (out, v) = match page.take() {
                Some(p) => p,
                None => {
                    spec.limit =
                        Some(left.map_or(stream::READ_LIMIT_MAX, |l| (l as usize).min(stream::READ_LIMIT_MAX)));
                    match read_once(&db, c, &name, &spec).await {
                        Ok(r) => r,
                        Err(e) => {
                            let err = json!({"code": e.code(), "message": e.message(), "retryable": e.retryable()});
                            let _ = tx.send(Bytes::from(format!("event: error\ndata: {err}\n\n"))).await;
                            return;
                        }
                    }
                }
            };
            for (r, b) in &out.records {
                let ev = format!("id: {}\ndata: {}\n\n", r.seq, stream::record_json(r, b, enc));
                if tx.send(Bytes::from(ev)).await.is_err() {
                    return;
                }
            }
            if !out.records.is_empty() {
                quiet_since = Instant::now();
            }
            left = left.map(|l| l - out.records.len() as u64);
            spec = ReadSpec { from_seq: Some(out.next_seq), ..starting_anywhere(spec) };
            let remaining = until.saturating_duration_since(Instant::now());
            let ended = match out.open {
                true => remaining.is_zero(),
                false => out.done,
            };
            if left == Some(0) || ended || node.draining() {
                return;
            }
            if !out.open {
                continue;
            }
            if quiet_since.elapsed() >= KEEPALIVE {
                if tx.send(Bytes::from_static(b": keepalive\n\n")).await.is_err() {
                    return;
                }
                quiet_since = Instant::now();
            }
            db.wait_for_change(v, remaining.min(poll).min(KEEPALIVE)).await;
        }
    });
    let mut resp = Response::new(axum::body::Body::new(ChannelBody(rx)));
    let h = resp.headers_mut();
    h.insert(http::header::CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
    h.insert(http::header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    h.insert("x-accel-buffering", HeaderValue::from_static("no"));
    resp
}

async fn query(Extension(ctx): Extension<DbCtx>, Path(p): PathMap, JsonBody(b): JsonBody<Json>) -> Result<J> {
    let name = param(&p, "s")?.to_string();
    let (mut b, c) = split_consistency(b)?;
    let enc = match b.as_object_mut().and_then(|o| o.remove("encoding")) {
        None => Encoding::Utf8,
        Some(e) => serde_json::from_value(e).map_err(|_| Error::invalid("encoding is utf8 or base64"))?,
    };
    let spec: ReadSpec = serde_json::from_value(b).map_err(|e| Error::invalid(format!("invalid query: {e}")))?;
    let (out, v) = read_once(&ctx.db, c, &name, &spec).await?;
    let records = out.records.iter().map(|(r, b)| stream::record_json(r, b, enc)).collect();
    Ok(axum::Json(read_json(records, &out, v)))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TrimBody {
    #[serde(default)]
    before_seq: Option<u64>,
    #[serde(default)]
    before_time: Option<u64>,
}

async fn trim(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    Path(p): PathMap,
    JsonBody(b): JsonBody<TrimBody>,
) -> Result<J> {
    let name = param(&p, "s")?.to_string();
    let (_, v) = with_txn!(node, ctx, |t| stream::trim(t, &name, b.before_seq, b.before_time).await);
    Ok(axum::Json(json!({"version": v})))
}

async fn consumers(Extension(ctx): Extension<DbCtx>, Path(p): PathMap, q: Params) -> Result<J> {
    let name = param(&p, "s")?.to_string();
    let (limit, cursor) = page_of(&q)?;
    let c = consistency_of(&q)?;
    let ((st, (list, next)), v) = with_read!(ctx.db, c, |t| {
        let (_, st) = stream::info(t, &name).await?;
        Ok::<_, Error>((st, stream::list_consumers(t, &name, limit, cursor.as_deref()).await?))
    });
    let out: Vec<Json> = list.iter().map(|(n, x)| consumer_json(n, x, &st)).collect();
    Ok(axum::Json(json!({"consumers": out, "next_cursor": next, "version": v})))
}

async fn get_consumer(Extension(ctx): Extension<DbCtx>, Path(p): PathMap, q: Params) -> Result<J> {
    let (name, consumer) = (param(&p, "s")?.to_string(), param(&p, "c")?.to_string());
    let c = consistency_of(&q)?;
    let ((st, found), v) = with_read!(ctx.db, c, |t| {
        let (_, st) = stream::info(t, &name).await?;
        Ok::<_, Error>((st, stream::consumer(t, &name, &consumer).await?))
    });
    let x = found.ok_or_else(|| Error::not_found(format!("consumer {consumer:?} not found")))?;
    let mut out = consumer_json(&consumer, &x, &st);
    out["version"] = json!(v);
    Ok(axum::Json(out))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConsumerBody {
    seq: u64,
}

async fn put_consumer(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    Path(p): PathMap,
    JsonBody(b): JsonBody<ConsumerBody>,
) -> Result<J> {
    let (name, consumer) = (param(&p, "s")?.to_string(), param(&p, "c")?.to_string());
    let ((), v) = with_txn!(node, ctx, |t| stream::set_consumer(t, &name, &consumer, b.seq, node.now_ms()).await);
    Ok(axum::Json(json!({"version": v})))
}

async fn delete_consumer(
    State(node): State<Arc<Node>>,
    Extension(ctx): Extension<DbCtx>,
    Path(p): PathMap,
) -> Result<J> {
    let (name, consumer) = (param(&p, "s")?.to_string(), param(&p, "c")?.to_string());
    let (existed, v) = with_txn!(node, ctx, |t| stream::delete_consumer(t, &name, &consumer).await);
    Ok(axum::Json(json!({"version": v, "existed": existed})))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sim::cost::{World, call_router, run_paused};
    use tower::ServiceExt;

    const S: &str = "/v1/dbs/logs/streams/app";

    /// A node with database `logs` holding stream `app`; its router and a token.
    async fn world() -> (World, axum::Router, String) {
        let w = World::new(1, 0).await;
        let t = w.signup(0, "streams@api.test").await;
        w.create_db(0, &t, "logs").await;
        let router = w.node(0).router().expect("router");
        let r = call_router(&router, "POST", "/v1/dbs/logs/streams", &t, Bytes::from(r#"{"name":"app"}"#)).await;
        assert_eq!(r.status, 201);
        (w, router, t)
    }

    async fn send(
        router: &axum::Router,
        t: &str,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &str,
    ) -> (u16, Json) {
        let mut b = http::Request::builder().method(method).uri(path).header("authorization", format!("Bearer {t}"));
        for (k, v) in headers {
            b = b.header(*k, *v);
        }
        let resp = router.clone().oneshot(b.body(axum::body::Body::from(body.to_string())).unwrap()).await.unwrap();
        let status = resp.status().as_u16();
        let body = http_body_util::BodyExt::collect(resp.into_body()).await.unwrap().to_bytes();
        (status, serde_json::from_slice(&body).unwrap_or_else(|_| json!(String::from_utf8_lossy(&body))))
    }

    async fn append(router: &axum::Router, t: &str, values: &[&str], key: Option<&str>) -> (u16, Json) {
        let records: Vec<Json> = values.iter().map(|v| json!({"value": v})).collect();
        let body = json!({"records": records, "idempotency_key": key}).to_string();
        send(router, t, "POST", &format!("{S}/records"), &[], &body).await
    }

    #[test]
    fn event_streams_follow_appends_until_they_end() {
        run_paused(async {
            let (w, router, t) = world().await;
            append(&router, &t, &["a"], None).await;
            let path = format!("{S}/records?format=sse&wait_ms=5000");
            let follow = send(&router, &t, "GET", &path, &[], "");
            let appends = async {
                for v in ["b", "c"] {
                    tokio::time::sleep(Duration::from_millis(1_000)).await;
                    assert_eq!(append(&router, &t, &[v], None).await.0, 200);
                }
            };
            let ((status, body), ()) = tokio::join!(follow, appends);
            let ids: Vec<&str> = body.as_str().unwrap().lines().filter(|l| l.starts_with("id: ")).collect();
            assert_eq!((status, ids), (200, vec!["id: 0", "id: 1", "id: 2"]));
            let (status, body) =
                send(&router, &t, "GET", &format!("{S}/records?format=sse&until_seq=2"), &[], "").await;
            assert_eq!((status, body.as_str().unwrap().matches("id: ").count()), (200, 2), "a bounded read ends");
            let (status, _) = send(&router, &t, "GET", &format!("{S}/records?format=sse&reverse=true"), &[], "").await;
            assert_eq!(status, 400, "event streams read forward");
            let (status, body) =
                send(&router, &t, "GET", "/v1/dbs/logs/streams/nope/records?format=sse", &[], "").await;
            assert_eq!((status, &body["error"]["code"]), (404, &json!("not_found")));
            w.shutdown();
        });
    }

    #[test]
    fn appends_answer_from_their_commit() {
        run_paused(async {
            let (w, router, t) = world().await;
            let (_, first) = append(&router, &t, &["a"], Some("k")).await;
            let (_, again) = append(&router, &t, &["a", "b"], Some("k")).await;
            assert_eq!(first["time"], again["time"]);
            assert_eq!((again["replayed"].clone(), again["count"].clone()), (json!(true), json!(1)));
            let txn = json!({"ops": [{"op": "put", "key": "x", "value": "y"}], "idempotency_key": "t"}).to_string();
            assert_eq!(send(&router, &t, "POST", "/v1/dbs/logs/txn", &[], &txn).await.0, 200);
            assert_eq!(append(&router, &t, &["c"], Some("t")).await.0, 400, "a key used by another request");
            let ndjson = [("content-type", "application/x-ndjson")];
            let (status, r) = send(&router, &t, "POST", &format!("{S}/records"), &ndjson, "\n \n").await;
            assert_eq!((status, &r["count"], &r["first_seq"]), (200, &json!(0), &json!(1)));
            let (status, _) = send(&router, &t, "POST", &format!("{S}/records"), &ndjson, "\u{feff}{\"n\":1}\n").await;
            assert_eq!(status, 200);
            let (_, r) = send(&router, &t, "GET", &format!("{S}/records?tail=1"), &[], "").await;
            assert_eq!(r["records"][0]["json"], json!({"n": 1}));
            w.shutdown();
        });
    }

    #[test]
    fn reads_and_consumers_are_bounded() {
        run_paused(async {
            let (w, router, t) = world().await;
            append(&router, &t, &["a", "b", "c"], None).await;
            let (status, _) = send(&router, &t, "GET", &format!("{S}/records?max_bytes=999999999"), &[], "").await;
            assert_eq!(status, 400);
            let (status, _) = send(&router, &t, "GET", &format!("{S}/records?bogus=1"), &[], "").await;
            assert_eq!(status, 400, "unknown read parameters");
            let read = json!({"ops": [{"op": "read_stream", "stream": "app", "max_bytes": 1}]}).to_string();
            let (_, r) = send(&router, &t, "POST", "/v1/dbs/logs/txn", &[], &read).await;
            assert_eq!(r["results"][0]["records"].as_array().map(Vec::len), Some(1));
            for c in ["c1", "c2", "c3"] {
                let (status, _) = send(&router, &t, "PUT", &format!("{S}/consumers/{c}"), &[], r#"{"seq":1}"#).await;
                assert_eq!(status, 200);
            }
            let (status, _) = send(&router, &t, "PUT", &format!("{S}/consumers/c4"), &[], r#"{"seq":4}"#).await;
            assert_eq!(status, 400, "a cursor past the stream's end");
            let (_, page) = send(&router, &t, "GET", &format!("{S}/consumers?limit=2"), &[], "").await;
            let cursor = page["next_cursor"].as_str().expect("a next page").to_string();
            let (_, rest) = send(&router, &t, "GET", &format!("{S}/consumers?limit=2&cursor={cursor}"), &[], "").await;
            let names =
                |p: &Json| p["consumers"].as_array().unwrap().iter().map(|c| c["name"].clone()).collect::<Vec<_>>();
            assert_eq!((names(&page), names(&rest)), (vec![json!("c1"), json!("c2")], vec![json!("c3")]));
            assert!(rest["next_cursor"].is_null());
            w.shutdown();
        });
    }

    #[test]
    fn idempotency_keys_are_visible_ascii() {
        let mut h = HeaderMap::new();
        h.insert("idempotency-key", HeaderValue::from_bytes("clé".as_bytes()).unwrap());
        assert!(idempotency_header(&h).is_err());
        h.insert("idempotency-key", HeaderValue::from_static("k-1"));
        assert_eq!(idempotency_header(&h).unwrap().as_deref(), Some("k-1"));
    }
}
