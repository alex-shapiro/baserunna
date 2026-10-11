//! Cluster plumbing: node-to-node transport, membership and routing.
//! Routing only affects performance (cache locality, lease stability);
//! correctness rests on manifest CAS. See SPEC §7.

use crate::meta::NodeRecord;
use crate::store::BoxFut;
use axum::body::Body;
use http::{Request, Response};

pub const HOPS_HEADER: &str = "x-baserunna-hops";
pub const CLUSTER_KEY_HEADER: &str = "x-baserunna-cluster-key";

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
    fn liveness() {
        let mut a = n("a");
        a.heartbeat_at = 0;
        let b = n("b");
        assert_eq!(live(&[a, b], 100, 10).len(), 1);
    }
}
