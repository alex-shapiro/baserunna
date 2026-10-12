//! Cluster plumbing: node-to-node transport, membership and routing.
//! Routing only affects performance (cache locality, lease stability);
//! correctness rests on manifest CAS. See SPEC §7.

use crate::meta::NodeRecord;
use crate::store::BoxFut;
use crate::util::DetHashMap;
use axum::body::Body;
use bytes::Bytes;
use http::{Request, Response};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

pub const HOPS_HEADER: &str = "x-baserunna-hops";
pub const CLUSTER_KEY_HEADER: &str = "x-baserunna-cluster-key";
/// A random id the forwarding node gives each forwarded request, so the
/// receiver can execute a request that the network delivers twice only once.
pub const FORWARD_ID_HEADER: &str = "x-baserunna-forward-id";
/// The node id a forwarded request is meant for, so that a node restarted
/// at the same address refuses copies it can no longer deduplicate.
pub const FORWARD_TO_HEADER: &str = "x-baserunna-forward-to";
/// The forwarded request's commit deadline (wall-clock ms since the epoch).
pub const DEADLINE_HEADER: &str = "x-baserunna-deadline-ms";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransportError {
    pub message: String,
    /// False if the request certainly never reached the peer (connect
    /// failure), so it is safe to handle it elsewhere.
    pub maybe_delivered: bool,
}

/// Sends HTTP requests to peer nodes.
pub trait Transport: Send + Sync + 'static {
    /// `addr` is the peer's base URL (e.g. `http://10.0.0.5:8080`); the
    /// request URI holds only the path and query.
    fn call(&self, addr: &str, req: Request<Body>) -> BoxFut<'static, Result<Response<Body>, TransportError>>;
}

/// Real HTTP/1.1 transport.
pub struct HttpTransport {
    client: hyper_util::client::legacy::Client<hyper_util::client::legacy::connect::HttpConnector, Body>,
}

impl Default for HttpTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpTransport {
    pub fn new() -> HttpTransport {
        let mut conn = hyper_util::client::legacy::connect::HttpConnector::new();
        conn.set_connect_timeout(Some(std::time::Duration::from_secs(2)));
        conn.set_nodelay(true);
        let client = hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
            .pool_idle_timeout(std::time::Duration::from_secs(30))
            .build(conn);
        HttpTransport { client }
    }
}

impl Transport for HttpTransport {
    fn call(&self, addr: &str, mut req: Request<Body>) -> BoxFut<'static, Result<Response<Body>, TransportError>> {
        let pq = req.uri().path_and_query().map(|p| p.as_str().to_string()).unwrap_or_else(|| "/".into());
        let uri = format!("{}{}", addr.trim_end_matches('/'), pq);
        let client = self.client.clone();
        Box::pin(async move {
            *req.uri_mut() = uri.parse().map_err(|e: http::uri::InvalidUri| TransportError {
                message: e.to_string(),
                maybe_delivered: false,
            })?;
            req.headers_mut().remove(http::header::HOST);
            match client.request(req).await {
                Ok(resp) => Ok(resp.map(Body::new)),
                Err(e) => Err(TransportError { maybe_delivered: !e.is_connect(), message: e.to_string() }),
            }
        })
    }
}

/// A transport for single-node deployments: every call fails undelivered.
pub struct NoTransport;

impl Transport for NoTransport {
    fn call(&self, _addr: &str, _req: Request<Body>) -> BoxFut<'static, Result<Response<Body>, TransportError>> {
        Box::pin(async { Err(TransportError { message: "no peers".into(), maybe_delivered: false }) })
    }
}

/// A response kept for replay to duplicates of a forwarded request.
#[derive(Clone, Debug)]
pub struct ReplayedResponse {
    pub status: http::StatusCode,
    pub headers: http::HeaderMap,
    pub body: Bytes,
}

#[derive(Default)]
struct Slot {
    /// `Some(None)`: finished without a replayable response.
    result: Mutex<Option<Option<ReplayedResponse>>>,
    done: tokio::sync::Notify,
}

impl Slot {
    fn finish(&self, r: Option<ReplayedResponse>) {
        let mut g = self.result.lock().unwrap_or_else(|e| e.into_inner());
        if g.is_none() {
            *g = Some(r);
        }
        drop(g);
        self.done.notify_waiters();
    }

    async fn wait(&self) -> Option<ReplayedResponse> {
        loop {
            let notified = self.done.notified();
            let mut notified = std::pin::pin!(notified);
            notified.as_mut().enable();
            if let Some(r) = self.result.lock().unwrap_or_else(|e| e.into_inner()).clone() {
                return r;
            }
            notified.await;
        }
    }
}

/// Exactly-once execution of forwarded requests on the receiving node.
///
/// The first copy of a forwarded request runs; later copies wait for it and
/// get the same response. Ids are remembered for `retention`, far longer
/// than any network delay.
pub struct ForwardDedup {
    slots: Mutex<DetHashMap<String, (Instant, Arc<Slot>)>>,
    retention: Duration,
}

/// Handle of the copy that executes a forwarded request. Dropping it
/// without [`FirstCopy::finish`] (e.g. the handler was cancelled) tells
/// waiting duplicates that the outcome is unknown.
pub struct FirstCopy(Arc<Slot>);

impl FirstCopy {
    pub fn finish(self, r: Option<ReplayedResponse>) {
        self.0.finish(r);
    }
}

impl Drop for FirstCopy {
    fn drop(&mut self) {
        self.0.finish(None);
    }
}

pub enum Delivery {
    First(FirstCopy),
    /// Resolves to the first copy's response, or `None` if it has none to
    /// share.
    Duplicate(BoxFut<'static, Option<ReplayedResponse>>),
}

impl ForwardDedup {
    pub fn new(retention: Duration) -> ForwardDedup {
        ForwardDedup { slots: Mutex::new(DetHashMap::default()), retention }
    }

    pub fn deliver(&self, id: &str) -> Delivery {
        let mut g = self.slots.lock().unwrap_or_else(|e| e.into_inner());
        if g.len() > 100_000 {
            let keep = self.retention;
            g.retain(|_, (t, _)| t.elapsed() < keep);
        }
        if let Some((_, s)) = g.get(id) {
            let s = s.clone();
            return Delivery::Duplicate(Box::pin(async move { s.wait().await }));
        }
        let s = Arc::new(Slot::default());
        g.insert(id.to_string(), (Instant::now(), s.clone()));
        Delivery::First(FirstCopy(s))
    }

    /// Forget ids older than the retention period.
    pub fn expire(&self) {
        let keep = self.retention;
        self.slots.lock().unwrap_or_else(|e| e.into_inner()).retain(|_, (t, _)| t.elapsed() < keep);
    }
}

fn fnv(data: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01B3);
    }
    h ^= h >> 31;
    h = h.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    h ^ (h >> 29)
}

/// Rendezvous (highest random weight) hashing.
pub fn rendezvous<'a>(key: &str, nodes: &'a [NodeRecord]) -> Option<&'a NodeRecord> {
    nodes.iter().max_by_key(|n| {
        let mut b = Vec::with_capacity(n.id.len() + key.len() + 1);
        b.extend_from_slice(n.id.as_bytes());
        b.push(0);
        b.extend_from_slice(key.as_bytes());
        (fnv(&b), n.id.clone())
    })
}

/// Live members at `now_ms` given a heartbeat interval.
pub fn live(nodes: &[NodeRecord], now_ms: i64, heartbeat_ms: i64) -> Vec<NodeRecord> {
    let mut v: Vec<NodeRecord> =
        nodes.iter().filter(|n| n.heartbeat_at + 3 * heartbeat_ms >= now_ms).cloned().collect();
    v.sort_by(|a, b| a.id.cmp(&b.id));
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    fn n(id: &str) -> NodeRecord {
        NodeRecord { id: id.into(), addr: format!("http://{id}"), started_at: 0, heartbeat_at: 100 }
    }

    #[test]
    fn rendezvous_is_stable_and_balanced() {
        let nodes = vec![n("a"), n("b"), n("c")];
        let mut counts = std::collections::BTreeMap::new();
        for i in 0..3000 {
            let k = format!("db{i}");
            let o = rendezvous(&k, &nodes).unwrap().id.clone();
            // Removing a different node never moves the key.
            let others: Vec<NodeRecord> =
                nodes.iter().filter(|x| x.id == o || x.id != "b" || o == "b").cloned().collect();
            if o != "b" {
                assert_eq!(rendezvous(&k, &others).unwrap().id, o);
            }
            *counts.entry(o).or_insert(0) += 1;
        }
        for c in counts.values() {
            assert!(*c > 800, "unbalanced: {counts:?}");
        }
    }

    #[test]
    fn forwarded_requests_run_once() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
        rt.block_on(async {
            let d = ForwardDedup::new(Duration::from_secs(60));
            let (Delivery::First(first), Delivery::Duplicate(dup)) = (d.deliver("a"), d.deliver("a")) else {
                panic!("the first copy must run and the second wait");
            };
            let ok = ReplayedResponse {
                status: http::StatusCode::CREATED,
                headers: http::HeaderMap::new(),
                body: Bytes::from_static(b"done"),
            };
            first.finish(Some(ok));
            assert_eq!(
                dup.await.map(|r| (r.status, r.body)),
                Some((http::StatusCode::CREATED, Bytes::from_static(b"done")))
            );
            let Delivery::Duplicate(late) = d.deliver("a") else { panic!("a late copy must not run again") };
            assert!(late.await.is_some());
            // A first copy that never finished leaves its duplicates unsure.
            let (Delivery::First(first), Delivery::Duplicate(dup)) = (d.deliver("b"), d.deliver("b")) else {
                panic!("the first copy must run and the second wait");
            };
            drop(first);
            assert!(dup.await.is_none());
            assert!(matches!(d.deliver("c"), Delivery::First(_)));
        });
    }

    #[test]
    fn liveness() {
        let mut a = n("a");
        a.heartbeat_at = 0;
        let b = n("b");
        assert_eq!(live(&[a, b], 100, 10).len(), 1);
    }
}
