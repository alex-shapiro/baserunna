//! Latency histograms of a node's object-store calls, commit batches and API
//! requests, served by `/v1/admin/metrics`.

use crate::engine::ObjectClass;
use crate::timing::StoreOp;
use serde_json::{Map, Value as Json, json};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, RwLock};
use std::time::Duration;

/// Bucket `i` holds durations up to `FIRST_US * GROWTH^i` µs: 10 µs to
/// about 30 minutes with 25% resolution.
const BUCKETS: usize = 86;
const FIRST_US: f64 = 10.0;
const GROWTH: f64 = 1.25;

fn bucket(us: u64) -> usize {
    if us as f64 <= FIRST_US {
        return 0;
    }
    (((us as f64 / FIRST_US).ln() / GROWTH.ln()).ceil() as usize).min(BUCKETS - 1)
}

fn upper_us(i: usize) -> f64 {
    FIRST_US * GROWTH.powi(i as i32)
}

/// A latency histogram with lock-free recording.
pub struct Histogram {
    buckets: [AtomicU64; BUCKETS],
    count: AtomicU64,
    sum_us: AtomicU64,
    max_us: AtomicU64,
}

impl Default for Histogram {
    fn default() -> Self {
        Histogram {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum_us: AtomicU64::new(0),
            max_us: AtomicU64::new(0),
        }
    }
}

impl Histogram {
    pub fn record(&self, d: Duration) {
        let us = d.as_micros().min(u64::MAX as u128) as u64;
        self.buckets[bucket(us)].fetch_add(1, Relaxed);
        self.count.fetch_add(1, Relaxed);
        self.sum_us.fetch_add(us, Relaxed);
        self.max_us.fetch_max(us, Relaxed);
    }

    pub fn count(&self) -> u64 {
        self.count.load(Relaxed)
    }

    /// The upper bound of the bucket holding quantile `q`, in ms.
    fn quantile_ms(&self, counts: &[u64], total: u64, q: f64) -> f64 {
        let rank = ((total as f64) * q).ceil().max(1.0) as u64;
        let mut seen = 0;
        for (i, c) in counts.iter().enumerate() {
            seen += c;
            if seen >= rank {
                let max = self.max_us.load(Relaxed) as f64;
                return upper_us(i).min(max) / 1000.0;
            }
        }
        0.0
    }

    /// Count, mean, quantiles (accurate to a bucket, 25%) and max, in ms.
    pub fn json(&self) -> Json {
        let counts: Vec<u64> = self.buckets.iter().map(|b| b.load(Relaxed)).collect();
        let total: u64 = counts.iter().sum();
        if total == 0 {
            return json!({"count": 0});
        }
        let round = |ms: f64| (ms * 10.0).round() / 10.0;
        json!({
            "count": total,
            "mean_ms": round(self.sum_us.load(Relaxed) as f64 / total as f64 / 1000.0),
            "p50_ms": round(self.quantile_ms(&counts, total, 0.5)),
            "p90_ms": round(self.quantile_ms(&counts, total, 0.9)),
            "p99_ms": round(self.quantile_ms(&counts, total, 0.99)),
            "p999_ms": round(self.quantile_ms(&counts, total, 0.999)),
            "max_ms": round(self.max_us.load(Relaxed) as f64 / 1000.0),
        })
    }

    fn reset(&self) {
        for b in &self.buckets {
            b.store(0, Relaxed);
        }
        self.count.store(0, Relaxed);
        self.sum_us.store(0, Relaxed);
        self.max_us.store(0, Relaxed);
    }
}

/// Latency histograms of a node's object-store calls and commit batches,
/// kept by its engine.
#[derive(Default)]
pub struct StoreMetrics {
    store: [[Histogram; StoreOp::ALL.len()]; ObjectClass::ALL.len()],
    store_errors: [[AtomicU64; StoreOp::ALL.len()]; ObjectClass::ALL.len()],
    /// Commit batches that wrote a manifest, from dequeue to acknowledgement.
    commits: Histogram,
    batch_txns: AtomicU64,
    batch_txns_max: AtomicU64,
}

impl StoreMetrics {
    pub fn store_call(&self, class: ObjectClass, op: StoreOp, d: Duration, failed: bool) {
        self.store[class as usize][op as usize].record(d);
        if failed {
            self.store_errors[class as usize][op as usize].fetch_add(1, Relaxed);
        }
    }

    pub fn commit_batch(&self, d: Duration, txns: usize) {
        self.commits.record(d);
        self.batch_txns.fetch_add(txns as u64, Relaxed);
        self.batch_txns_max.fetch_max(txns as u64, Relaxed);
    }

    /// Adds `store` and `commits` fields to `out`.
    pub fn json_into(&self, out: &mut Map<String, Json>) {
        let mut store = Map::new();
        for class in ObjectClass::ALL {
            for op in StoreOp::ALL {
                let h = &self.store[class as usize][op as usize];
                if h.count() == 0 {
                    continue;
                }
                let mut v = h.json();
                v["errors"] = json!(self.store_errors[class as usize][op as usize].load(Relaxed));
                store.insert(format!("{}.{}", class.name(), op.name()), v);
            }
        }
        let mut commits = self.commits.json();
        let batches = self.commits.count().max(1) as f64;
        commits["txns_per_batch"] = json!((self.batch_txns.load(Relaxed) as f64 / batches * 10.0).round() / 10.0);
        commits["max_txns_per_batch"] = json!(self.batch_txns_max.load(Relaxed));
        out.insert("store".into(), Json::Object(store));
        out.insert("commits".into(), commits);
    }

    pub fn reset(&self) {
        self.store.iter().flatten().for_each(Histogram::reset);
        self.store_errors.iter().flatten().for_each(|e| e.store(0, Relaxed));
        self.commits.reset();
        self.batch_txns.store(0, Relaxed);
        self.batch_txns_max.store(0, Relaxed);
    }
}

/// Methods requests are counted under; any other method counts as `OTHER`.
const METHODS: [&str; 8] = ["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "OTHER"];

/// Latency histograms of a node's API requests and scheduling delays.
#[derive(Default)]
pub struct ServerMetrics {
    /// Per method in [`METHODS`], keyed by route template.
    routes: RwLock<[BTreeMap<String, Arc<Histogram>>; METHODS.len()]>,
    /// How late a periodic 100 ms timer fires: time the node had work it
    /// could not run, because its CPU was busy or taken away.
    lag: Histogram,
}

impl ServerMetrics {
    /// Records a request with `method` to the route template `route`, or to
    /// no route.
    pub fn request(&self, method: &str, route: Option<&str>, d: Duration) {
        let m = METHODS.iter().position(|x| *x == method).unwrap_or(METHODS.len() - 1);
        let route = route.unwrap_or("unmatched");
        if let Some(h) = self.routes.read().unwrap_or_else(|e| e.into_inner())[m].get(route) {
            return h.record(d);
        }
        let mut routes = self.routes.write().unwrap_or_else(|e| e.into_inner());
        routes[m].entry(route.to_string()).or_default().record(d);
    }

    /// Records how late the scheduler-lag probe's timer fired.
    pub fn lag(&self, late: Duration) {
        self.lag.record(late);
    }

    /// Adds `routes`, `lag` and `cpu` fields to `out`.
    pub fn json_into(&self, out: &mut Map<String, Json>) {
        let routes = self.routes.read().unwrap_or_else(|e| e.into_inner());
        let routes: Map<String, Json> = METHODS
            .iter()
            .zip(routes.iter())
            .flat_map(|(m, by_route)| by_route.iter().map(move |(r, h)| (format!("{m} {r}"), h.json())))
            .collect();
        out.insert("routes".into(), Json::Object(routes));
        out.insert("lag".into(), self.lag.json());
        out.insert("cpu".into(), cpu_json());
    }

    pub fn reset(&self) {
        self.routes.write().unwrap_or_else(|e| e.into_inner()).iter_mut().for_each(BTreeMap::clear);
        self.lag.reset();
    }
}

/// Cumulative CPU time of this process and the host's steal time (time the
/// hypervisor ran something else), in ms, where `/proc` reports them.
fn cpu_json() -> Json {
    // Linux reports both in clock ticks, which are 10 ms on every platform it runs on here.
    const TICK_MS: u64 = 10;
    let process = std::fs::read_to_string("/proc/self/stat").ok().and_then(|s| {
        // After the parenthesized command name come the state, then utime and
        // stime as the 12th and 13th fields; some fields between can be -1.
        let f: Vec<&str> = s.rsplit_once(')')?.1.split_whitespace().collect();
        Some((f.get(11)?.parse::<u64>().ok()? + f.get(12)?.parse::<u64>().ok()?) * TICK_MS)
    });
    let steal = std::fs::read_to_string("/proc/stat").ok().and_then(|s| {
        let cpu = s.lines().next()?.split_whitespace().skip(1).filter_map(|x| x.parse::<u64>().ok());
        Some(cpu.collect::<Vec<_>>().get(7)? * TICK_MS)
    });
    json!({"process_ms": process, "steal_ms": steal})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantiles_land_in_the_right_bucket() {
        let h = Histogram::default();
        for ms in 1..=100 {
            h.record(Duration::from_millis(ms));
        }
        let j = h.json();
        assert_eq!(j["count"], 100);
        let near = |k: &str, want: f64| {
            let got = j[k].as_f64().unwrap();
            assert!(got >= want && got <= want * 1.25 + 0.1, "{k}: {got} vs {want}");
        };
        near("p50_ms", 50.0);
        near("p90_ms", 90.0);
        near("p99_ms", 99.0);
        assert_eq!(j["max_ms"], 100.0);
        h.reset();
        assert_eq!(h.json()["count"], 0);
    }

    #[test]
    fn routes_are_bounded_by_method_and_template() {
        let m = ServerMetrics::default();
        for method in ["GET", "GET", "BREW", "QQ1"] {
            m.request(method, Some("/v1/dbs/{db}"), Duration::from_millis(1));
        }
        m.request("ZZ", None, Duration::from_millis(1));
        let mut out = Map::new();
        m.json_into(&mut out);
        let routes: Vec<_> =
            out["routes"].as_object().unwrap().iter().map(|(k, v)| (k.clone(), v["count"].clone())).collect();
        assert_eq!(
            routes,
            [
                ("GET /v1/dbs/{db}".into(), json!(2)),
                ("OTHER /v1/dbs/{db}".into(), json!(2)),
                ("OTHER unmatched".into(), json!(1))
            ]
        );
    }
}
