//! Storage engine write, space, and read amplification benchmarks.

use crate::engine::{Consistency, Engine, EngineConfig, Isolation};
use crate::env::Env;
use crate::store::{BoxFut, DynStore, GetOpts, GetResult, ListPage, MemStore, ObjectStore, PutMode, SResult};
use crate::util::rng::Rng;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

struct SegmentMeter {
    inner: DynStore,
    seg_put_bytes: AtomicU64,
}

impl ObjectStore for SegmentMeter {
    fn get<'a>(&'a self, key: &'a str, opts: GetOpts) -> BoxFut<'a, SResult<GetResult>> {
        self.inner.get(key, opts)
    }

    fn put<'a>(&'a self, key: &'a str, data: Bytes, mode: PutMode) -> BoxFut<'a, SResult<String>> {
        if key.contains("/seg/") {
            self.seg_put_bytes.fetch_add(data.len() as u64, Ordering::Relaxed);
        }
        self.inner.put(key, data, mode)
    }

    fn delete<'a>(&'a self, key: &'a str) -> BoxFut<'a, SResult<()>> {
        self.inner.delete(key)
    }

    fn list<'a>(&'a self, prefix: &'a str, after: Option<&'a str>, limit: usize) -> BoxFut<'a, SResult<ListPage>> {
        self.inner.list(prefix, after, limit)
    }

    fn time_precision_ms(&self) -> u64 {
        self.inner.time_precision_ms()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Workload {
    /// Load a key set, then overwrite random keys.
    UniformUpdate,
    /// Like `UniformUpdate`, with most updates going to a few keys.
    SkewedUpdate,
    /// Sequential inserts with increasing keys.
    Append,
    /// Sliding window: insert at the head, delete at the tail.
    Queue,
    /// Inserts, updates, and deletes of random keys.
    Mixed,
}

impl Workload {
    pub const ALL: [Workload; 5] =
        [Workload::UniformUpdate, Workload::SkewedUpdate, Workload::Append, Workload::Queue, Workload::Mixed];

    pub fn name(self) -> &'static str {
        match self {
            Workload::UniformUpdate => "uniform_update",
            Workload::SkewedUpdate => "skewed_update",
            Workload::Append => "append",
            Workload::Queue => "queue",
            Workload::Mixed => "mixed",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Amplification {
    pub workload: Workload,
    /// Key and value bytes of every committed write.
    pub logical_written: u64,
    /// Key and value bytes of live data at the end of the run.
    pub live_bytes: u64,
    /// Segment bytes written per logical byte.
    pub write_amp: f64,
    /// Mean manifest-referenced bytes per live byte across commits.
    pub space_amp_mean: f64,
    /// Peak manifest-referenced bytes per live byte across commits.
    pub space_amp_max: f64,
    /// Total stored bytes per live byte after compaction and GC.
    pub space_amp_settled: f64,
    /// Mean candidate segments per point lookup of a live key.
    pub read_amp: f64,
    pub segments: usize,
}

fn value(rng: &mut Rng, i: u64) -> Vec<u8> {
    const WORDS: &[&str] = &["alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india"];
    let mut s = format!("{{\"id\":{i},\"owner\":\"user-{}\",\"tags\":[", rng.below(500));
    for _ in 0..3 + rng.usize(4) {
        s.push_str(&format!("\"{}\",", rng.pick(WORDS)));
    }
    s.push_str(&format!("],\"score\":{},\"note\":\"", rng.below(1_000_000)));
    for _ in 0..8 + rng.usize(16) {
        s.push_str(rng.pick(WORDS));
        s.push(' ');
    }
    s.push_str("\"}");
    s.into_bytes()
}

fn key(i: u64) -> Bytes {
    Bytes::from(format!("\x01row/{i:010}"))
}

type Write = (u64, Option<Vec<u8>>);

/// Generates commit batches for `w`. Scale 1.0 corresponds to ~10 MB.
fn writes(w: Workload, scale: f64, rng: &mut Rng) -> Vec<Vec<Write>> {
    let n = ((50_000.0 * scale) as u64).max(100);
    let mut ops: Vec<Write> = Vec::new();
    match w {
        Workload::UniformUpdate | Workload::SkewedUpdate => {
            let mut order: Vec<u64> = (0..n).collect();
            rng.shuffle(&mut order);
            for i in order {
                ops.push((i, Some(value(rng, i))));
            }
            for _ in 0..2 * n {
                let k = if w == Workload::SkewedUpdate { (n as f64 * rng.unit().powi(4)) as u64 } else { rng.below(n) };
                ops.push((k, Some(value(rng, k))));
            }
        }
        Workload::Append => {
            for i in 0..3 * n {
                ops.push((i, Some(value(rng, i))));
            }
        }
        Workload::Queue => {
            let window = n / 2;
            for i in 0..3 * n {
                ops.push((i, Some(value(rng, i))));
                if i >= window {
                    ops.push((i - window, None));
                }
            }
        }
        Workload::Mixed => {
            let mut next = 0u64;
            let mut live: Vec<u64> = Vec::new();
            for _ in 0..3 * n {
                match rng.below(10) {
                    0..=4 => {
                        live.push(next);
                        ops.push((next, Some(value(rng, next))));
                        next += 1;
                    }
                    5..=7 if !live.is_empty() => {
                        let k = *rng.pick(&live);
                        ops.push((k, Some(value(rng, k))));
                    }
                    _ if !live.is_empty() => {
                        let k = live.swap_remove(rng.usize(live.len()));
                        ops.push((k, None));
                    }
                    _ => {}
                }
            }
        }
    }
    ops.chunks(50).map(|c| c.to_vec()).collect()
}

pub async fn measure(w: Workload, scale: f64, cfg: EngineConfig) -> Amplification {
    let origin = tokio::time::Instant::now();
    let env = Arc::new(Env::sim(42, 1_750_000_000_000, origin, 0));
    let env2 = env.clone();
    let mem = Arc::new(MemStore::new(Box::new(move || env2.now_ms())));
    let meter = Arc::new(SegmentMeter { inner: mem.clone(), seg_put_bytes: AtomicU64::new(0) });
    let eng = Engine::new(meter.clone(), env.clone(), cfg.clone(), "amp".into());
    let db = eng.db("amp");
    db.open(true).await.expect("open");
    let mut rng = Rng::new(7);
    let mut live: BTreeMap<u64, u64> = BTreeMap::new();
    let mut logical = 0u64;
    let (mut amp_sum, mut amp_max, mut samples) = (0.0, 0.0f64, 0u64);
    let (mut lookups, mut consulted) = (0u64, 0u64);
    for batch in writes(w, scale, &mut rng) {
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.expect("begin");
        for (k, v) in batch {
            let kb = key(k);
            logical += kb.len() as u64;
            match v {
                Some(v) => {
                    logical += v.len() as u64;
                    live.insert(k, (kb.len() + v.len()) as u64);
                    t.put(kb, crate::engine::Value::Inline(Bytes::from(v))).expect("put");
                }
                None => {
                    live.remove(&k);
                    t.delete(kb).expect("delete");
                }
            }
        }
        db.commit(t.into_request()).await.expect("commit");
        tokio::time::sleep(Duration::from_millis(100)).await;
        let v = db.current().expect("current");
        let live_bytes: u64 = live.values().sum();
        if live_bytes > 0 {
            let referenced = v.m.segments.iter().map(|s| s.size).sum::<u64>() + v.m.tail_bytes() as u64;
            let a = referenced as f64 / live_bytes as f64;
            amp_sum += a;
            amp_max = amp_max.max(a);
            samples += 1;
            let keys: Vec<u64> = live.keys().copied().collect();
            for _ in 0..20 {
                consulted += v.segments_for_key(&key(*rng.pick(&keys))) as u64;
                lookups += 1;
            }
        }
    }
    tokio::time::sleep(Duration::from_secs(60)).await;
    tokio::time::sleep(Duration::from_millis(cfg.gc_grace_ms + 1_000)).await;
    db.maintenance_now().await.expect("gc");
    tokio::time::sleep(Duration::from_secs(10)).await;
    let v = db.current().expect("current");
    let live_bytes: u64 = live.values().sum::<u64>().max(1);
    let stored: u64 = mem.keys().iter().filter_map(|k| mem.get_sync(k, &GetOpts::default()).ok()).map(|g| g.size).sum();
    eng.env().shutdown();
    Amplification {
        workload: w,
        logical_written: logical,
        live_bytes,
        write_amp: meter.seg_put_bytes.load(Ordering::Relaxed) as f64 / logical.max(1) as f64,
        space_amp_mean: amp_sum / samples.max(1) as f64,
        space_amp_max: amp_max,
        space_amp_settled: stored as f64 / live_bytes as f64,
        read_amp: consulted as f64 / lookups.max(1) as f64,
        segments: v.m.segments.len(),
    }
}

pub fn measure_all(scale: f64, cfg: &EngineConfig) -> Vec<Amplification> {
    super::cost::run_paused(async {
        let mut out = Vec::new();
        for w in Workload::ALL {
            out.push(measure(w, scale, cfg.clone()).await);
        }
        out
    })
}

pub fn table(rows: &[Amplification]) -> String {
    let mut s = format!(
        "{:<15} {:>9} {:>9} {:>7} {:>11} {:>10} {:>11} {:>7} {:>5}\n",
        "workload", "written", "live", "write", "space mean", "space max", "space settled", "read", "segs"
    );
    for a in rows {
        s.push_str(&format!(
            "{:<15} {:>7.1}MB {:>7.1}MB {:>6.2}x {:>10.2}x {:>9.2}x {:>12.2}x {:>7.2} {:>5}\n",
            a.workload.name(),
            a.logical_written as f64 / 1e6,
            a.live_bytes as f64 / 1e6,
            a.write_amp,
            a.space_amp_mean,
            a.space_amp_max,
            a.space_amp_settled,
            a.read_amp,
            a.segments,
        ));
    }
    s
}

/// Stream workloads (SPEC §5.11).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamWorkload {
    /// One busy stream of log lines, appended in batches.
    Log,
    /// Twenty feeds, appended to in turn.
    Feeds,
    /// Two hundred streams of messages, a few at a time on random streams.
    Chat,
}

impl StreamWorkload {
    pub const ALL: [StreamWorkload; 3] = [StreamWorkload::Log, StreamWorkload::Feeds, StreamWorkload::Chat];

    pub fn name(self) -> &'static str {
        match self {
            StreamWorkload::Log => "log",
            StreamWorkload::Feeds => "feeds",
            StreamWorkload::Chat => "chat",
        }
    }
}

#[derive(Clone, Debug)]
pub struct StreamAmplification {
    pub workload: StreamWorkload,
    /// Body bytes appended.
    pub logical_written: u64,
    /// Segment and chunk bytes written per body byte.
    pub write_amp: f64,
    /// Stored bytes per body byte after compaction and GC.
    pub space_amp_settled: f64,
    /// The share of records stored in sealed chunks at the end.
    pub sealed: f64,
    pub chunks: usize,
    pub segments: usize,
}

pub async fn measure_stream(w: StreamWorkload, scale: f64, cfg: EngineConfig) -> StreamAmplification {
    use crate::engine::stream::{self as es, Stored};
    let origin = tokio::time::Instant::now();
    let env = Arc::new(Env::sim(43, 1_750_000_000_000, origin, 0));
    let env2 = env.clone();
    let mem = Arc::new(MemStore::new(Box::new(move || env2.now_ms())));
    let meter = Arc::new(SegmentMeter { inner: mem.clone(), seg_put_bytes: AtomicU64::new(0) });
    let eng = Engine::new(meter.clone(), env.clone(), cfg.clone(), "amp".into());
    let db = eng.db("amp");
    db.open(true).await.expect("open");
    eng.shared.store_time_ms().await.expect("clock");
    let streams = match w {
        StreamWorkload::Log => 1,
        StreamWorkload::Feeds => 20,
        StreamWorkload::Chat => 200,
    };
    let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.expect("begin");
    for s in 0..streams {
        let r = crate::engine::Retention::default();
        crate::stream::create(&mut t, &format!("s{s}"), r, 1 + s as u64, 0).await.expect("create");
    }
    db.commit(t.into_request()).await.expect("create streams");
    let mut rng = Rng::new(11);
    let records = ((50_000.0 * scale) as usize).max(500);
    let per_commit = match w {
        StreamWorkload::Log => 50,
        StreamWorkload::Feeds | StreamWorkload::Chat => 25,
    };
    let mut logical = 0u64;
    for _ in 0..records / per_commit {
        let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.expect("begin");
        for _ in 0..per_commit {
            let s = rng.usize(streams);
            let body = crate::stream::Body::text(value(&mut rng, s as u64));
            let payload = body.encode();
            logical += payload.len() as u64;
            t.append(1 + s as u64, vec![payload], None, None).expect("append");
        }
        db.commit(t.into_request()).await.expect("commit");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::sleep(Duration::from_secs(60)).await;
    tokio::time::sleep(Duration::from_millis(cfg.gc_grace_ms + cfg.orphan_grace_ms + 1_000)).await;
    db.maintenance_now().await.expect("gc");
    db.sweep_now().await.expect("sweep");
    tokio::time::sleep(Duration::from_millis(cfg.gc_grace_ms + 1_000)).await;
    db.maintenance_now().await.expect("gc");
    tokio::time::sleep(Duration::from_secs(10)).await;
    let v = db.current().expect("current");
    let mut it = v.iter(&crate::engine::KeyRange::prefix(&[es::RECORDS]), false, false, None);
    let (mut sealed, mut loose, mut chunks) = (0u64, 0u64, 0);
    while let Some(e) = it.next().await.expect("scan") {
        match es::stored(&e).map(|(_, s)| s) {
            Some(Stored::Chunk(c)) => {
                sealed += c.last_seq - c.first_seq + 1;
                chunks += 1;
            }
            _ => loose += 1,
        }
    }
    let stored: u64 = mem.keys().iter().filter_map(|k| mem.get_sync(k, &GetOpts::default()).ok()).map(|g| g.size).sum();
    eng.env().shutdown();
    StreamAmplification {
        workload: w,
        logical_written: logical,
        write_amp: meter.seg_put_bytes.load(Ordering::Relaxed) as f64 / logical.max(1) as f64,
        space_amp_settled: stored as f64 / logical.max(1) as f64,
        sealed: sealed as f64 / (sealed + loose).max(1) as f64,
        chunks,
        segments: v.m.segments.len(),
    }
}

pub fn measure_streams(scale: f64, cfg: &EngineConfig) -> Vec<StreamAmplification> {
    super::cost::run_paused(async {
        let mut out = Vec::new();
        for w in StreamWorkload::ALL {
            out.push(measure_stream(w, scale, cfg.clone()).await);
        }
        out
    })
}

pub fn stream_table(rows: &[StreamAmplification]) -> String {
    let mut s = format!(
        "{:<8} {:>9} {:>7} {:>13} {:>7} {:>7} {:>5}\n",
        "workload", "written", "write", "space settled", "sealed", "chunks", "segs"
    );
    for a in rows {
        s.push_str(&format!(
            "{:<8} {:>7.1}MB {:>6.2}x {:>12.2}x {:>6.0}% {:>7} {:>5}\n",
            a.workload.name(),
            a.logical_written as f64 / 1e6,
            a.write_amp,
            a.space_amp_settled,
            100.0 * a.sealed,
            a.chunks,
            a.segments,
        ));
    }
    s
}
