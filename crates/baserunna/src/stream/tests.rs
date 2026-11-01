//! Stream model and unit tests.

use super::*;
use crate::engine::tests::{engine, rt};
use crate::engine::{Appended, Committed, Consistency, Db, Engine, EngineConfig, Isolation, Snapshot};
use crate::probe::{self, Probe};
use crate::util::prop;
use crate::util::rng::Rng;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

const NAMES: [&str; 4] = ["s0", "s1", "s2", "s3"];

async fn begin(db: &Arc<Db>) -> Txn {
    db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap()
}

async fn commit(db: &Arc<Db>, t: Txn) -> Result<Committed> {
    db.commit_detailed(t.into_request()).await
}

fn to<'a>(name: &'a str, expect: Option<u64>) -> AppendTo<'a> {
    AppendTo { name, create: None, expect, op_index: Some(0), record_max: 1 << 20 }
}

async fn create_stream(db: &Arc<Db>, name: &str, r: Retention) -> Committed {
    let mut t = begin(db).await;
    let id = db.shared().env.random_u64();
    create(&mut t, name, r, id, 0).await.unwrap();
    commit(db, t).await.unwrap()
}

async fn append_texts(db: &Arc<Db>, name: &str, texts: &[&str]) -> Committed {
    let mut t = begin(db).await;
    let bodies: Vec<Body> = texts.iter().map(|s| Body::text(Bytes::copy_from_slice(s.as_bytes()))).collect();
    append(&mut t, &to(name, None), &bodies).await.unwrap();
    commit(db, t).await.unwrap()
}

async fn read_all(db: &Arc<Db>, name: &str, spec: ReadSpec) -> ReadOut {
    let mut t = begin(db).await;
    read(&mut t, name, &spec).await.unwrap()
}

fn texts(out: &ReadOut) -> Vec<String> {
    out.records.iter().map(|(_, b)| String::from_utf8_lossy(&b.data).into_owned()).collect()
}

fn firsts(c: &Committed) -> Vec<u64> {
    c.appends.iter().map(|a| a.first_seq).collect()
}

fn seqs(out: &ReadOut) -> Vec<u64> {
    out.records.iter().map(|(r, _)| r.seq).collect()
}

async fn settle(db: &Arc<Db>) {
    for _ in 0..500 {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if !db.busy() {
            return;
        }
    }
    panic!("background work never settled");
}

async fn opened(eng: &Arc<Engine>, id: &str) -> Arc<Db> {
    let db = eng.db(id);
    db.open(true).await.unwrap();
    eng.shared.store_time_ms().await.unwrap();
    db
}

#[derive(Clone, Debug)]
struct MStream {
    records: Vec<(u64, Body)>,
    first: u64,
    retention: Retention,
    /// Maximum `first_seq` reachable by an in-flight background retention trim.
    allowed: u64,
}

impl MStream {
    fn next(&self) -> u64 {
        self.records.len() as u64
    }

    fn pos(&self, seq: u64) -> u64 {
        self.records[..seq as usize].iter().map(|(_, b)| b.encode().len() as u64).sum()
    }

    fn first_where(&self, f: impl Fn(u64, &(u64, Body)) -> bool) -> u64 {
        (self.first..self.next()).find(|s| f(*s, &self.records[*s as usize])).unwrap_or(self.next())
    }

    fn changed(&mut self, now: u64) {
        self.allowed = self.allowed.max(self.retention_bound(now + 1_000));
    }

    fn upper(&self, now: u64) -> u64 {
        self.allowed.max(self.retention_bound(now))
    }

    fn retention_bound(&self, now: u64) -> u64 {
        self.bound(self.retention, now)
    }

    fn due_bound(&self, now: u64) -> u64 {
        self.bound(self.retention.due_at(), now)
    }

    fn bound(&self, r: Retention, now: u64) -> u64 {
        let mut b = self.first;
        if let Some(m) = r.max_bytes {
            let end = self.pos(self.next());
            b = b.max(self.first_where(|s, _| self.pos(s) >= end.saturating_sub(m)));
        }
        if let Some(ms) = r.retention_ms {
            b = b.max(self.first_where(|_, (t, _)| *t >= now.saturating_sub(ms)));
        }
        b
    }
}

type Model = BTreeMap<usize, MStream>;

#[derive(Clone, Debug)]
enum Start {
    Seq(u64),
    Time(usize),
    Tail(u64),
    Default,
}

#[derive(Clone, Debug)]
enum Op {
    Append {
        s: usize,
        sizes: Vec<usize>,
        json: bool,
        expect: Option<i64>,
    },
    Trim {
        s: usize,
        back: u64,
    },
    Retain {
        s: usize,
        max_bytes: Option<u64>,
        retention_ms: Option<u64>,
    },
    Delete(usize),
    Create(usize),
    Put(usize),
    Settle,
    Advance(u64),
    Sweep,
    Snapshot,
    Read {
        s: usize,
        start: Start,
        until_back: Option<u64>,
        reverse: bool,
        limit: usize,
        filter: Option<u8>,
        scan: Option<usize>,
    },
}

fn gen_ops(r: &mut Rng) -> Vec<Op> {
    let n = 40 + r.usize(200);
    (0..n)
        .map(|_| {
            let s = r.usize(NAMES.len());
            match r.below(100) {
                0..=39 => {
                    let most = if r.chance(0.1) { 60 } else { 6 };
                    let k = 1 + r.usize(most);
                    let sizes =
                        (0..k).map(|_| if r.chance(0.05) { 3_000 + r.usize(3_000) } else { r.usize(200) }).collect();
                    let expect = r.chance(0.15).then(|| if r.chance(0.7) { 0 } else { r.range(0, 4) as i64 - 2 });
                    Op::Append { s, sizes, json: r.chance(0.3), expect }
                }
                40..=45 => Op::Trim { s, back: r.below(30) },
                46..=48 => Op::Retain {
                    s,
                    max_bytes: r.chance(0.5).then(|| r.range(200, 20_000)),
                    retention_ms: r.chance(0.3).then(|| r.range(1_000, 600_000)),
                },
                49..=50 => Op::Delete(s),
                51..=54 => Op::Create(s),
                55..=59 => Op::Put(r.usize(1_500)),
                60..=64 => Op::Settle,
                65..=67 => Op::Advance(if r.chance(0.5) { r.range(1, 5_000) } else { r.range(10_000, 900_000) }),
                68..=69 => Op::Sweep,
                70..=72 => Op::Snapshot,
                _ => {
                    let start = match r.below(4) {
                        0 => Start::Seq(r.below(80)),
                        1 => Start::Time(r.usize(100)),
                        2 => Start::Tail(r.below(30)),
                        _ => Start::Default,
                    };
                    let most = if r.chance(0.5) { 5 } else { 200 };
                    Op::Read {
                        s,
                        start,
                        until_back: r.chance(0.2).then(|| r.below(20)),
                        reverse: r.chance(0.4),
                        limit: 1 + r.usize(most),
                        filter: r.chance(0.2).then(|| r.below(3) as u8),
                        scan: r.chance(0.2).then(|| 1 + r.usize(20)),
                    }
                }
            }
        })
        .collect()
}

fn body(r: &mut Rng, size: usize, json: bool, i: usize) -> Body {
    let headers = vec![("k".to_string(), format!("v{}", i % 3))];
    let data = match json {
        true => Bytes::from(json!({"i": i, "pad": "x".repeat(size)}).to_string()),
        false => Bytes::from(r.bytes(size)),
    };
    Body { json, headers, data }
}

fn expected(m: &MStream, spec: &ReadSpec, first: u64, filter: Option<u8>) -> (Vec<u64>, u64, bool) {
    let next = m.next();
    let time_seek = |t: u64| (first..next).find(|s| m.records[*s as usize].0 >= t).unwrap_or(next);
    let lo = match (spec.from_seq, spec.from_time, spec.tail) {
        (Some(s), ..) => s,
        (_, Some(t), _) => time_seek(t),
        (_, _, Some(n)) => next.saturating_sub(n),
        _ => 0,
    }
    .max(first);
    let until = spec.until_seq;
    let hi = until.unwrap_or(u64::MAX).min(next);
    let mut all: Vec<u64> = (lo..hi.max(lo)).collect();
    if spec.reverse {
        all.reverse();
    }
    let matches = |s: u64| filter.is_none_or(|f| m.records[s as usize].1.header("k") == Some(&format!("v{f}")));
    let (limit, scan) = (spec.limit.unwrap_or(100), spec.scan_limit.unwrap_or(SCAN_LIMIT_DEFAULT));
    let (mut out, mut scanned, mut last) = (Vec::new(), 0, None);
    let mut it = all.into_iter();
    let exhausted = loop {
        if out.len() >= limit || scanned >= scan {
            break false;
        }
        let Some(s) = it.next() else { break true };
        scanned += 1;
        last = Some(s);
        if matches(s) {
            out.push(s);
        }
    };
    let next_seq = match (spec.reverse, exhausted, last) {
        (false, false, Some(l)) => l + 1,
        (false, _, _) => hi.max(lo),
        (true, false, Some(l)) => l,
        (true, _, _) => lo.min(hi),
    };
    (out, next_seq, exhausted)
}

async fn check_read(t: &mut Txn, m: &MStream, s: usize, op: &Op, now: u64) -> Result<u64, String> {
    let Op::Read { start, until_back, reverse, limit, filter, scan, .. } = op else { unreachable!() };
    let mut spec = ReadSpec { reverse: *reverse, limit: Some(*limit), scan_limit: *scan, ..Default::default() };
    match start {
        Start::Seq(x) => spec.from_seq = Some(*x),
        Start::Time(i) if !m.records.is_empty() => {
            spec.from_time = Some(m.records[i % m.records.len()].0 + (*i as u64 % 3) - 1)
        }
        Start::Tail(n) => spec.tail = Some(*n),
        _ => {}
    }
    spec.until_seq = until_back.map(|b| m.next().saturating_sub(b));
    spec.filter =
        filter.map(|f| serde_json::from_value(json!({"header": "k", "op": "eq", "value": format!("v{f}")})).unwrap());
    let out = read(t, NAMES[s], &spec).await.map_err(|e| format!("read {spec:?}: {e}"))?;
    if out.first_seq < m.first || out.first_seq > m.upper(now) || out.tail_seq != m.next() {
        return Err(format!(
            "{}: first {} tail {}, model first {} (retention allows {}) next {}",
            NAMES[s],
            out.first_seq,
            out.tail_seq,
            m.first,
            m.upper(now),
            m.next()
        ));
    }
    let (want, next_seq, done) = expected(m, &spec, out.first_seq, *filter);
    if seqs(&out) != want || out.next_seq != next_seq || out.done != done {
        return Err(format!(
            "{} {spec:?} from first {}: got {:?} next {} done {}, want {want:?} next {next_seq} done {done}",
            NAMES[s],
            out.first_seq,
            seqs(&out),
            out.next_seq,
            out.done
        ));
    }
    for (r, b) in &out.records {
        let (time, body) = &m.records[r.seq as usize];
        if r.time_ms != *time || b != body || r.pos != m.pos(r.seq) {
            return Err(format!("{} record {} differs from the model", NAMES[s], r.seq));
        }
    }
    Ok(out.first_seq)
}

async fn run(seed: u64, ops: &[Op]) -> Result<(), String> {
    let levels_inline_max = if seed.is_multiple_of(2) { 0 } else { 16 };
    let (eng, store, _env) = engine(seed, EngineConfig { levels_inline_max, ..EngineConfig::small_for_tests() });
    let db = opened(&eng, "st").await;
    let mut model = Model::new();
    let mut rng = Rng::new(seed ^ 0x5eed);
    let mut snapshots: Vec<(Instant, Snapshot, Model)> = Vec::new();
    let mut appended = 0;
    let e = |e: Error| e.to_string();
    for op in ops {
        let now = eng.shared.store_now_ms().unwrap_or(0);
        match op {
            Op::Append { s, sizes, json, expect } => {
                let bodies: Vec<Body> = sizes.iter().map(|n| body(&mut rng, *n, *json, appended + *n)).collect();
                appended += sizes.len();
                let mut t = begin(&db).await;
                let next = model.get(s).map_or(0, |m| m.next());
                let x = expect.map(|o| (next as i64 + o).max(0) as u64);
                let r = match append(&mut t, &to(NAMES[*s], x), &bodies).await {
                    Ok(_) => commit(&db, t).await,
                    Err(e) => Err(e),
                };
                match (model.get_mut(s), r) {
                    (None, Err(Error::NotFound(_))) => {}
                    (Some(m), Err(Error::PreconditionFailed { .. })) if x != Some(m.next()) => {}
                    (Some(m), Ok(c)) if x.is_none_or(|x| x == m.next()) => {
                        if c.appends != [Appended { first_seq: m.next(), count: bodies.len() as u64 }] {
                            return Err(format!("append to {} at {} got {:?}", NAMES[*s], m.next(), c.appends));
                        }
                        m.records.extend(bodies.into_iter().map(|b| (c.time_ms, b)));
                        m.changed(c.time_ms);
                    }
                    (m, r) => {
                        return Err(format!(
                            "append to {} ({:?} expecting {x:?}): {r:?}",
                            NAMES[*s],
                            m.map(|m| m.next())
                        ));
                    }
                }
            }
            Op::Trim { s, back } => {
                let mut t = begin(&db).await;
                let before = model.get(s).map_or(0, |m| m.next().saturating_sub(*back));
                let r = match trim(&mut t, NAMES[*s], Some(before), None).await {
                    Ok(_) => commit(&db, t).await.map(|_| ()),
                    Err(e) => Err(e),
                };
                match (model.get_mut(s), r) {
                    (None, Err(Error::NotFound(_))) => {}
                    (Some(m), Ok(())) => {
                        m.first = m.first.max(before.min(m.next()));
                        m.changed(now);
                    }
                    (m, r) => return Err(format!("trim of {}: {r:?} ({})", NAMES[*s], m.is_some())),
                }
            }
            Op::Retain { s, max_bytes, retention_ms } => {
                let r = Retention { retention_ms: *retention_ms, max_bytes: *max_bytes };
                let mut t = begin(&db).await;
                let res = match configure(&mut t, NAMES[*s], r).await {
                    Ok(_) => commit(&db, t).await.map(|_| ()),
                    Err(e) => Err(e),
                };
                match (model.get_mut(s), res) {
                    (None, Err(Error::NotFound(_))) => {}
                    (Some(m), Ok(())) => {
                        m.retention = r;
                        m.changed(now);
                    }
                    (m, res) => return Err(format!("configure {}: {res:?} ({})", NAMES[*s], m.is_some())),
                }
            }
            Op::Delete(s) => {
                let mut t = begin(&db).await;
                let r = match delete(&mut t, NAMES[*s]).await {
                    Ok(()) => commit(&db, t).await.map(|_| ()),
                    Err(e) => Err(e),
                };
                match (model.remove(s), r) {
                    (None, Err(Error::NotFound(_))) | (Some(_), Ok(())) => {}
                    (m, r) => return Err(format!("delete {}: {r:?} ({})", NAMES[*s], m.is_some())),
                }
            }
            Op::Create(s) => {
                let mut t = begin(&db).await;
                let r = match create(&mut t, NAMES[*s], Retention::default(), rng.next_u64(), now).await {
                    Ok(_) => commit(&db, t).await.map(|_| ()),
                    Err(e) => Err(e),
                };
                match (model.contains_key(s), r) {
                    (true, Err(Error::AlreadyExists(_))) => {}
                    (false, Ok(())) => {
                        let m = MStream { records: Vec::new(), first: 0, retention: Retention::default(), allowed: 0 };
                        model.insert(*s, m);
                    }
                    (exists, r) => return Err(format!("create {}: {r:?} (exists: {exists})", NAMES[*s])),
                }
            }
            Op::Put(n) => {
                let mut t = begin(&db).await;
                let k = Bytes::from(format!("\x01k{}", n % 7));
                t.put(k, Value::Inline(Bytes::from(vec![1u8; *n]))).map_err(e)?;
                commit(&db, t).await.map_err(e)?;
            }
            Op::Settle => {
                settle(&db).await;
                let mut t = begin(&db).await;
                let now = eng.shared.store_now_ms().unwrap_or(0);
                for (s, m) in model.iter_mut() {
                    let (_, st) = info(&mut t, NAMES[*s]).await.map_err(e)?;
                    let bytes_only = Retention { retention_ms: None, ..m.retention };
                    let bytes_bound = m.bound(bytes_only.due_at(), now);
                    if st.first_seq < bytes_bound || st.first_seq > m.upper(now) {
                        return Err(format!(
                            "{} settled at first {} for bytes bound {bytes_bound}, retention bound {}",
                            NAMES[*s],
                            st.first_seq,
                            m.upper(now)
                        ));
                    }
                    m.first = st.first_seq;
                    m.allowed = m.first;
                }
                let life = Duration::from_millis(eng.shared.cfg.txn_max_lifetime_ms);
                snapshots.retain(|(t, _, _)| t.elapsed() < life);
                for (_, snap, then) in &snapshots {
                    for (s, m) in then {
                        let mut t = Txn::new(snap.clone(), Isolation::Serializable, 0, usize::MAX);
                        let read = Op::Read {
                            s: *s,
                            start: Start::Default,
                            until_back: None,
                            reverse: false,
                            limit: 1000,
                            filter: None,
                            scan: None,
                        };
                        let mut m = m.clone();
                        let st = snap.version.stream_state(info(&mut t, NAMES[*s]).await.map_err(e)?.0.id).await;
                        m.first = st.map_err(e)?.map_or(m.first, |st| st.first_seq);
                        check_read(&mut t, &m, *s, &read, snap.time_ms)
                            .await
                            .map_err(|e| format!("old snapshot: {e}"))?;
                    }
                }
            }
            Op::Advance(ms) => {
                tokio::time::sleep(Duration::from_millis(*ms)).await;
                db.maintenance_now().await.map_err(e)?;
            }
            Op::Sweep => {
                db.maintenance_now().await.map_err(e)?;
                db.sweep_now().await.map_err(e)?;
                for m in model.values_mut() {
                    m.changed(now);
                }
            }
            Op::Snapshot => {
                let s = db.snapshot(Consistency::Strong).await.map_err(e)?;
                let mut then = model.clone();
                let mut t = Txn::new(s.clone(), Isolation::Serializable, 0, usize::MAX);
                for (i, m) in then.iter_mut() {
                    m.first = info(&mut t, NAMES[*i]).await.map_err(e)?.1.first_seq;
                }
                snapshots.push((Instant::now(), s, then));
            }
            Op::Read { s, .. } => {
                let mut t = begin(&db).await;
                match model.get_mut(s) {
                    None => match read(&mut t, NAMES[*s], &ReadSpec::default()).await {
                        Err(Error::NotFound(_)) => {}
                        r => return Err(format!("read of missing {}: {:?}", NAMES[*s], r.map(|o| seqs(&o)))),
                    },
                    Some(m) => {
                        let now = t.read_time;
                        m.first = check_read(&mut t, m, *s, op, now).await?;
                    }
                }
            }
        }
    }
    let swept_at = eng.shared.store_now_ms().unwrap_or(0);
    db.maintenance_now().await.map_err(e)?;
    db.sweep_now().await.map_err(e)?;
    settle(&db).await;
    let mut t = begin(&db).await;
    check_integrity(&mut t).await.map_err(|e| format!("integrity: {e}"))?;
    let now = t.read_time;
    for (s, m) in &mut model {
        let (_, st) = info(&mut t, NAMES[*s]).await.map_err(e)?;
        let (lower, upper) = (m.due_bound(swept_at), m.upper(now));
        if st.first_seq < lower || st.first_seq > upper {
            return Err(format!(
                "{} kept records from {} after a sweep, not {lower}..={upper}",
                NAMES[*s], st.first_seq
            ));
        }
        m.first = st.first_seq;
        let all = Op::Read {
            s: *s,
            start: Start::Default,
            until_back: None,
            reverse: false,
            limit: 1000,
            filter: None,
            scan: None,
        };
        check_read(&mut t, m, *s, &all, now).await?;
    }
    check_chunks(&db, &store).await
}

async fn check_chunks(db: &Arc<Db>, store: &crate::store::MemStore) -> Result<(), String> {
    let v = db.read_version(Consistency::Strong).await.map_err(|e| e.to_string())?;
    let range = KeyRange::prefix(&[es::RECORDS]);
    let mut it = v.iter(&range, false, false, None);
    let mut covered: BTreeMap<u64, u64> = BTreeMap::new();
    while let Some(e) = it.next().await.map_err(|e| e.to_string())? {
        let (id, stored) = es::stored(&e).ok_or("an undecodable stream entry")?;
        let (lo, hi) = match &stored {
            es::Stored::Record(r) => (r.seq, r.seq),
            es::Stored::Chunk(c) => {
                let g = store
                    .get_sync(&crate::engine::seg_key("st", &c.id), &Default::default())
                    .map_err(|e| format!("chunk {} missing: {e}", c.id))?;
                let entries = crate::engine::segment::read_all(&g.data).map_err(|e| e.to_string())?;
                let got: Vec<u64> =
                    entries.iter().filter_map(|e| es::parse_record_key(&e.key)).map(|(_, s)| s).collect();
                let want: Vec<u64> = (c.first_seq..=c.last_seq).collect();
                let bytes: u64 = entries
                    .iter()
                    .filter_map(es::stored)
                    .map(|(_, s)| match s {
                        es::Stored::Record(r) => r.payload.len() as u64,
                        es::Stored::Chunk(_) => u64::MAX / 4,
                    })
                    .sum();
                if got != want || bytes != c.bytes || g.data.len() as u64 != c.size {
                    return Err(format!("chunk {} holds {got:?} ({bytes} bytes), claims {c:?}", c.id));
                }
                (c.first_seq, c.last_seq)
            }
        };
        if covered.get(&id).is_some_and(|end| *end >= lo) {
            return Err(format!("stream {id}: entries overlap at {lo}"));
        }
        covered.insert(id, hi);
    }
    Ok(())
}

#[test]
fn prop_streams_match_model() {
    prop::check(
        "streams_match_model",
        40,
        gen_ops,
        |ops| prop::shrink_vec(ops),
        |ops| {
            let seed = ops.len() as u64;
            rt().block_on(run(seed, ops))
        },
    );
}

#[test]
fn records_get_dense_seqs_and_their_commit_times() {
    rt().block_on(async {
        let (eng, _, _) = engine(1, EngineConfig::small_for_tests());
        let db = opened(&eng, "d").await;
        create_stream(&db, "log", Retention::default()).await;
        let a = append_texts(&db, "log", &["a", "b"]).await;
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        let b = append_texts(&db, "log", &["c"]).await;
        assert_eq!((firsts(&a), firsts(&b)), (vec![0], vec![2]));
        let out = read_all(&db, "log", ReadSpec::default()).await;
        assert_eq!(texts(&out), ["a", "b", "c"]);
        let times: Vec<u64> = out.records.iter().map(|(r, _)| r.time_ms).collect();
        assert_eq!(times, [a.time_ms, a.time_ms, b.time_ms]);
        assert!(b.time_ms > a.time_ms);
        let (next, done, first, tail) = (out.next_seq, out.done, out.first_seq, out.tail_seq);
        assert_eq!((next, done, first, tail), (3, true, 0, 3));
        let late = read_all(&db, "log", ReadSpec { from_time: Some(a.time_ms + 1), ..Default::default() }).await;
        assert_eq!(texts(&late), ["c"]);
        let last = read_all(&db, "log", ReadSpec { tail: Some(2), reverse: true, ..Default::default() }).await;
        assert_eq!(texts(&last), ["c", "b"]);
    });
}

#[test]
fn concurrent_appends_share_a_commit_and_never_conflict() {
    rt().block_on(async {
        let (eng, _, _) = engine(2, EngineConfig::small_for_tests());
        let db = opened(&eng, "d").await;
        create_stream(&db, "s", Retention::default()).await;
        let mut reqs = Vec::new();
        for i in 0..20 {
            let mut t = begin(&db).await;
            append(&mut t, &to("s", None), &[Body::text(format!("r{i}"))]).await.unwrap();
            reqs.push(t.into_request());
        }
        let ws = db.current().unwrap().m.write_seq;
        let handles: Vec<_> = reqs
            .into_iter()
            .map(|r| {
                let db = db.clone();
                tokio::spawn(async move { db.commit_detailed(r).await.unwrap() })
            })
            .collect();
        let mut firsts: Vec<u64> = Vec::new();
        for h in handles {
            firsts.push(h.await.unwrap().appends[0].first_seq);
        }
        assert!(db.current().unwrap().m.write_seq - ws < 20, "the appends were batched");
        firsts.sort();
        assert_eq!(firsts, (0..20).collect::<Vec<_>>());
        assert_eq!(read_all(&db, "s", ReadSpec::default()).await.records.len(), 20);
    });
}

async fn reader(db: &Arc<Db>, name: &str, from: u64, limit: usize) -> Txn {
    let mut t = begin(db).await;
    read(&mut t, name, &ReadSpec { from_seq: Some(from), limit: Some(limit), ..Default::default() }).await.unwrap();
    t.put(Bytes::from_static(b"\x01out"), Value::Inline(Bytes::from_static(b"x"))).unwrap();
    t
}

#[test]
fn reads_conflict_with_appends_only_when_they_reached_the_end() {
    rt().block_on(async {
        let (eng, _, _) = engine(3, EngineConfig::small_for_tests());
        let db = opened(&eng, "d").await;
        create_stream(&db, "s", Retention::default()).await;
        append_texts(&db, "s", &["0", "1", "2", "3", "4"]).await;
        let early = reader(&db, "s", 0, 3).await;
        let to_end = reader(&db, "s", 2, 10).await;
        let mut info_txn = begin(&db).await;
        info(&mut info_txn, "s").await.unwrap();
        info_txn.put(Bytes::from_static(b"\x01x"), Value::Inline(Bytes::new())).unwrap();
        append_texts(&db, "s", &["5"]).await;
        assert!(commit(&db, early).await.is_ok(), "the records it read did not change");
        assert!(matches!(commit(&db, to_end).await, Err(Error::Conflict(_))), "a record arrived after its end");
        assert!(matches!(commit(&db, info_txn).await, Err(Error::Conflict(_))), "the stream's state changed");
    });
}

#[test]
fn reads_to_a_time_past_the_end_conflict_with_appends() {
    rt().block_on(async {
        let (eng, _, _) = engine(3, EngineConfig::small_for_tests());
        let db = opened(&eng, "d").await;
        create_stream(&db, "s", Retention::default()).await;
        append_texts(&db, "s", &["0", "1"]).await;
        let mut readers = Vec::new();
        for reverse in [false, true] {
            let mut t = begin(&db).await;
            let spec = ReadSpec { until_time: Some(u64::MAX / 2), reverse, ..Default::default() };
            let out = read(&mut t, "s", &spec).await.unwrap();
            assert_eq!((seqs(&out).len(), out.done), (2, true));
            t.put(Bytes::from_static(b"\x01x"), Value::Inline(Bytes::new())).unwrap();
            readers.push(t);
        }
        append_texts(&db, "s", &["2"]).await;
        for t in readers {
            assert!(matches!(commit(&db, t).await, Err(Error::Conflict(_))), "a record arrived before its end");
        }
    });
}

#[test]
fn transactions_see_their_own_trims() {
    rt().block_on(async {
        let (eng, _, _) = engine(4, EngineConfig::small_for_tests());
        let db = opened(&eng, "d").await;
        create_stream(&db, "s", Retention::default()).await;
        append_texts(&db, "s", &["0", "1", "2", "3"]).await;
        let mut t = begin(&db).await;
        trim(&mut t, "s", Some(2), None).await.unwrap();
        let out = read(&mut t, "s", &ReadSpec::default()).await.unwrap();
        assert_eq!((seqs(&out), out.first_seq), (vec![2, 3], 2));
        let (_, st) = info(&mut t, "s").await.unwrap();
        let rec = |s: u64| out.records.iter().find(|(r, _)| r.seq == s).map(|(r, _)| r.clone()).unwrap();
        assert_eq!((st.first_seq, st.first_pos, st.first_time), (2, rec(2).pos, rec(2).time_ms));
        trim(&mut t, "s", Some(9), None).await.unwrap();
        assert_eq!(info(&mut t, "s").await.unwrap().1.records(), 0);
        commit(&db, t).await.unwrap();
        assert!(read_all(&db, "s", ReadSpec::default()).await.records.is_empty());
    });
}

#[test]
fn retention_trims_skip_streams_whose_retention_changed() {
    rt().block_on(async {
        let (eng, _, _) = engine(5, EngineConfig::small_for_tests());
        let db = opened(&eng, "d").await;
        let before_reconfiguring = Retention { retention_ms: None, max_bytes: Some(10) };
        create_stream(&db, "s", Retention::default()).await;
        append_texts(&db, "s", &["0", "1", "2", "3"]).await;
        let id = {
            let mut t = begin(&db).await;
            info(&mut t, "s").await.unwrap().0.id
        };
        let mut t = begin(&db).await;
        t.stream_op(StreamOp::Trim { id, before: 3, retention: Some(before_reconfiguring) }).unwrap();
        commit(&db, t).await.unwrap();
        assert_eq!(read_all(&db, "s", ReadSpec::default()).await.first_seq, 0, "computed for another retention");
        let mut t = begin(&db).await;
        t.stream_op(StreamOp::Trim { id, before: 3, retention: Some(Retention::default()) }).unwrap();
        commit(&db, t).await.unwrap();
        assert_eq!(read_all(&db, "s", ReadSpec::default()).await.first_seq, 3);
    });
}

#[test]
fn batch_mates_are_validated_against_the_seqs_their_appends_received() {
    rt().block_on(async {
        let (eng, _, _) = engine(4, EngineConfig::small_for_tests());
        let db = opened(&eng, "d").await;
        create_stream(&db, "s", Retention::default()).await;
        append_texts(&db, "s", &["0", "1", "2"]).await;
        let mut app = begin(&db).await;
        append(&mut app, &to("s", None), &[Body::text("3")]).await.unwrap();
        let early = reader(&db, "s", 0, 2).await;
        let to_end = reader(&db, "s", 0, 10).await;
        let ws = db.current().unwrap().m.write_seq;
        let (a, b, c) = tokio::join!(commit(&db, app), commit(&db, early), commit(&db, to_end));
        assert_eq!(db.current().unwrap().m.write_seq, ws + 1, "one batch");
        assert!(a.is_ok() && b.is_ok(), "{a:?} {b:?}");
        assert!(matches!(c, Err(Error::Conflict(_))), "{c:?}");
    });
}

#[test]
fn trims_conflict_with_readers_of_what_they_trimmed() {
    rt().block_on(async {
        let (eng, _, _) = engine(5, EngineConfig::small_for_tests());
        let db = opened(&eng, "d").await;
        create_stream(&db, "s", Retention::default()).await;
        append_texts(&db, "s", &["0", "1", "2", "3", "4", "5"]).await;
        let kept = reader(&db, "s", 3, 2).await;
        let trimmed = reader(&db, "s", 1, 2).await;
        let mut t = begin(&db).await;
        trim(&mut t, "s", Some(3), None).await.unwrap();
        commit(&db, t).await.unwrap();
        assert!(commit(&db, kept).await.is_ok());
        assert!(matches!(commit(&db, trimmed).await, Err(Error::Conflict(_))));
        let out = read_all(&db, "s", ReadSpec { from_seq: Some(0), ..Default::default() }).await;
        assert_eq!((seqs(&out), out.first_seq), (vec![3, 4, 5], 3));
    });
}

#[test]
fn an_expected_seq_admits_one_of_competing_appends() {
    rt().block_on(async {
        let (eng, _, _) = engine(6, EngineConfig::small_for_tests());
        let db = opened(&eng, "d").await;
        create_stream(&db, "s", Retention::default()).await;
        append_texts(&db, "s", &["0"]).await;
        let mut reqs = Vec::new();
        for i in 0..5 {
            let mut t = begin(&db).await;
            append(&mut t, &to("s", Some(1)), &[Body::text(format!("x{i}"))]).await.unwrap();
            let db = db.clone();
            reqs.push(tokio::spawn(async move { commit(&db, t).await }));
        }
        let mut won = 0;
        for r in reqs {
            match r.await.unwrap() {
                Ok(c) => {
                    assert_eq!(firsts(&c), [1]);
                    won += 1;
                }
                Err(Error::PreconditionFailed { op_index, .. }) => assert_eq!(op_index, Some(0)),
                Err(e) => panic!("{e}"),
            }
        }
        assert_eq!(won, 1);
    });
}

#[test]
fn replayed_commits_report_the_seqs_they_received() {
    rt().block_on(async {
        let (eng, _, _) = engine(7, EngineConfig::small_for_tests());
        let db = opened(&eng, "d").await;
        create_stream(&db, "s", Retention::default()).await;
        append_texts(&db, "s", &["a"]).await;
        let req = |db: Arc<Db>| async move {
            let mut t = begin(&db).await;
            t.commit_id = 42;
            append(&mut t, &to("s", None), &[Body::text("b"), Body::text("c")]).await.unwrap();
            append(&mut t, &to("s", None), &[Body::text("d")]).await.unwrap();
            commit(&db, t).await.unwrap()
        };
        let first = req(db.clone()).await;
        let again = req(db.clone()).await;
        let want = vec![Appended { first_seq: 1, count: 2 }, Appended { first_seq: 3, count: 1 }];
        assert_eq!((&first.appends, first.replayed), (&want, false));
        assert_eq!((&again.appends, again.replayed, again.time_ms), (&want, true, first.time_ms));
        let status = db.commit_status(42).await.unwrap();
        assert!(matches!(status, Some(crate::engine::CommitState::Applied(c)) if c.appends == want));
        assert_eq!(read_all(&db, "s", ReadSpec::default()).await.records.len(), 4);
    });
}

#[test]
fn bulky_runs_are_sealed_and_read_across_chunks() {
    rt().block_on(async {
        let (eng, store, _) = engine(8, EngineConfig::small_for_tests());
        let db = opened(&eng, "st").await;
        create_stream(&db, "big", Retention::default()).await;
        create_stream(&db, "small", Retention::default()).await;
        let mut want = Vec::new();
        for i in 0..400 {
            let n = if i % 50 == 0 { 40 } else { 1 };
            let bodies: Vec<String> = (0..n).map(|j| format!("{i}.{j}:{}", "x".repeat(100))).collect();
            let refs: Vec<&str> = bodies.iter().map(String::as_str).collect();
            append_texts(&db, "big", &refs).await;
            want.extend(bodies);
            if i % 7 == 0 {
                append_texts(&db, "small", &["s"]).await;
            }
        }
        settle(&db).await;
        assert!(probe::count(Probe::ChunkSealedAtFlush) > 0 && probe::count(Probe::ChunkSealedInCompaction) > 0);
        let v = db.read_version(Consistency::Strong).await.unwrap();
        let (big, _) = info(&mut begin(&db).await, "big").await.unwrap();
        let mut it = v.iter(&es::records(big.id), false, false, None);
        let in_tail = |k: &Bytes| v.m.tail.binary_search_by(|e| e.key.cmp(k)).is_ok();
        let (mut sealed, mut run, min) = (0, 0, eng.shared.cfg.stream_seal_min_bytes);
        while let Some(e) = it.next().await.unwrap() {
            match es::stored(&e).unwrap().1 {
                es::Stored::Chunk(c) => {
                    assert!(run < min, "a run of {run} bytes was flushed unsealed");
                    (sealed, run) = (sealed + c.last_seq - c.first_seq + 1, 0);
                }
                es::Stored::Record(_) if !in_tail(&e.key) => run += e.size(),
                es::Stored::Record(_) => {}
            }
        }
        assert!(sealed > 500, "{sealed} records sealed");
        assert!(v.m.chunk_bytes.total() > 0);
        let mut got = Vec::new();
        let mut from = 0;
        loop {
            let out =
                read_all(&db, "big", ReadSpec { from_seq: Some(from), limit: Some(77), ..Default::default() }).await;
            got.extend(texts(&out));
            from = out.next_seq;
            if out.done {
                break;
            }
        }
        assert_eq!(got, want);
        let mut back = Vec::new();
        let mut until = None;
        loop {
            let spec = ReadSpec { until_seq: until, reverse: true, limit: Some(64), ..Default::default() };
            let out = read_all(&db, "big", spec).await;
            back.extend(texts(&out));
            until = Some(out.next_seq);
            if out.done {
                break;
            }
        }
        back.reverse();
        assert_eq!(back, want);
        let mut t = begin(&db).await;
        check_integrity(&mut t).await.unwrap();
        check_chunks(&db, &store).await.unwrap();
    });
}

#[test]
fn retention_trims_in_the_background() {
    rt().block_on(async {
        let (eng, _, _) = engine(9, EngineConfig::small_for_tests());
        let db = opened(&eng, "d").await;
        create_stream(&db, "bytes", Retention { retention_ms: None, max_bytes: Some(100) }).await;
        create_stream(&db, "age", Retention { retention_ms: Some(10_000), max_bytes: None }).await;
        for i in 0..20 {
            append_texts(&db, "bytes", &[&format!("{i:03}{}", "y".repeat(20))]).await;
        }
        append_texts(&db, "age", &["old"]).await;
        tokio::time::sleep(Duration::from_millis(20_000)).await;
        append_texts(&db, "age", &["new"]).await;
        settle(&db).await;
        let b = read_all(&db, "bytes", ReadSpec::default()).await;
        let kept: u64 = b.records.iter().map(|(r, _)| r.payload.len() as u64).sum();
        assert!(kept <= 106 && kept + 30 > 100, "kept {kept} bytes: {:?}", texts(&b));
        assert_eq!(texts(&read_all(&db, "age", ReadSpec::default()).await), ["new"]);
        assert!(probe::count(Probe::RetentionTrimmed) >= 2);
        tokio::time::sleep(Duration::from_millis(20_000)).await;
        db.maintenance_now().await.unwrap();
        db.sweep_now().await.unwrap();
        settle(&db).await;
        let out = read_all(&db, "age", ReadSpec::default()).await;
        assert!(out.records.is_empty() && out.first_seq == 2, "{:?}", texts(&out));
    });
}

#[test]
fn retention_trims_are_batched() {
    rt().block_on(async {
        let (eng, _, _) = engine(9, EngineConfig::small_for_tests());
        let db = opened(&eng, "d").await;
        create_stream(&db, "bytes", Retention { retention_ms: None, max_bytes: Some(16_000) }).await;
        create_stream(&db, "age", Retention { retention_ms: Some(16_000), max_bytes: None }).await;
        let trims = || probe::count(Probe::RetentionTrimmed);
        let line = |i: usize| format!("{i:04}{}", "y".repeat(21));
        for i in 0..800 {
            append_texts(&db, "bytes", &[&line(i)]).await;
            settle(&db).await;
        }
        let before = trims();
        for i in 0..80 {
            append_texts(&db, "bytes", &[&line(i)]).await;
            settle(&db).await;
        }
        assert!(trims() - before <= 3, "{} trims for 80 appends of 2,000 bytes", trims() - before);
        for i in 0..200 {
            append_texts(&db, "age", &[&line(i)]).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            settle(&db).await;
        }
        let (before, started) = (trims(), tokio::time::Instant::now());
        for i in 0..30 {
            append_texts(&db, "age", &[&line(i)]).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            settle(&db).await;
        }
        let secs = started.elapsed().as_secs();
        assert!(trims() - before <= secs + 1, "{} trims for 30 appends over {secs} s", trims() - before);
        let b = read_all(&db, "bytes", ReadSpec { reverse: true, limit: Some(1000), ..Default::default() }).await;
        let kept: u64 = b.records.iter().map(|(r, _)| r.payload.len() as u64).sum();
        assert!((15_000..=17_000).contains(&kept), "kept {kept} bytes");
    });
}

#[test]
fn deleted_streams_leave_nothing_and_start_over() {
    rt().block_on(async {
        let (eng, _, _) = engine(10, EngineConfig::small_for_tests());
        let db = opened(&eng, "d").await;
        create_stream(&db, "s", Retention::default()).await;
        for _ in 0..30 {
            append_texts(&db, "s", &["0123456789012345678901234567890123456789"]).await;
        }
        let mut t = begin(&db).await;
        set_consumer(&mut t, "s", "c", 7, 0).await.unwrap();
        commit(&db, t).await.unwrap();
        let mut t = begin(&db).await;
        delete(&mut t, "s").await.unwrap();
        commit(&db, t).await.unwrap();
        let mut t = begin(&db).await;
        assert!(matches!(read(&mut t, "s", &ReadSpec::default()).await, Err(Error::NotFound(_))));
        create_stream(&db, "s", Retention::default()).await;
        assert_eq!(firsts(&append_texts(&db, "s", &["again"]).await), [0]);
        let mut t = begin(&db).await;
        assert_eq!(consumer(&mut t, "s", "c").await.unwrap(), None);
        check_integrity(&mut t).await.unwrap();
        assert_eq!(texts(&read_all(&db, "s", ReadSpec::default()).await), ["again"]);
    });
}

#[test]
fn consumers_read_from_their_cursor() {
    rt().block_on(async {
        let (eng, _, _) = engine(11, EngineConfig::small_for_tests());
        let db = opened(&eng, "d").await;
        create_stream(&db, "s", Retention::default()).await;
        append_texts(&db, "s", &["a", "b", "c"]).await;
        let spec = ReadSpec { consumer: Some("w".into()), ..Default::default() };
        assert_eq!(texts(&read_all(&db, "s", spec.clone()).await), ["a", "b", "c"]);
        let mut t = begin(&db).await;
        let out = read(&mut t, "s", &ReadSpec { limit: Some(2), ..spec.clone() }).await.unwrap();
        set_consumer(&mut t, "s", "w", out.next_seq, 5).await.unwrap();
        commit(&db, t).await.unwrap();
        assert_eq!(texts(&read_all(&db, "s", spec).await), ["c"]);
        let mut t = begin(&db).await;
        let (list, next) = list_consumers(&mut t, "s", 10, None).await.unwrap();
        assert_eq!((list, next), (vec![("w".to_string(), Consumer { seq: 2, updated_at: 5 })], None));
    });
}

#[test]
fn bodies_and_lines() {
    let b = Body {
        json: true,
        headers: vec![("a".into(), "1".into()), ("b".into(), "".into())],
        data: Bytes::from_static(b"[1]"),
    };
    assert_eq!(Body::decode(&b.encode()).unwrap(), b);
    assert_eq!(b.header("b"), Some(""));
    let mut bad = b.clone();
    bad.headers.reverse();
    assert!(Body::decode(&bad.encode()).is_err());
    let lines = parse_lines(&Bytes::from_static(b"{\"a\":1}\r\n\n  \n[2]\n"), true).unwrap();
    assert_eq!(lines.iter().map(|b| &b.data[..]).collect::<Vec<_>>(), [&b"{\"a\":1}"[..], b"[2]"]);
    assert!(parse_lines(&Bytes::from_static(b"{}\nnope\n"), true).is_err());
    assert_eq!(parse_lines(&Bytes::from_static(b"one\ntwo"), false).unwrap().len(), 2);
    let r: api::NewRecord = serde_json::from_value(json!({"json": null})).unwrap();
    assert_eq!(record_body(r, api::Encoding::Utf8).unwrap().data, Bytes::from_static(b"null"));
    let both: api::NewRecord = serde_json::from_value(json!({"json": 1, "value": "x"})).unwrap();
    assert!(record_body(both, api::Encoding::Utf8).is_err());
    assert!(valid_name("app.api:prod-1") && !valid_name("a/b") && !valid_name("") && !valid_name(&"x".repeat(129)));
    let rec = Record { seq: 4, time_ms: 9, pos: 0, payload: Bytes::new() };
    let raw = Body::text(Bytes::from_static(b"\xff"));
    assert_eq!(record_json(&rec, &raw, api::Encoding::Utf8)["value_b64"], "/w==");
}

#[test]
fn fuzz_bodies_never_panic() {
    let good =
        Body { json: false, headers: vec![("k".into(), "v".into())], data: Bytes::from_static(b"data") }.encode();
    let mut r = Rng::new(23);
    for _ in 0..20_000 {
        let mut d = good.to_vec();
        for _ in 0..1 + r.usize(3) {
            let p = r.usize(d.len());
            d[p] = r.next_u32() as u8;
        }
        d.truncate(r.usize(d.len() + 1));
        if let Ok(b) = Body::decode(&Bytes::from(d)) {
            assert_eq!(Body::decode(&b.encode()).unwrap(), b);
        }
    }
}

fn gen_filter(r: &mut Rng, depth: u32) -> Json {
    let pick = |r: &mut Rng, vs: &[Json]| vs[r.usize(vs.len())].clone();
    match r.below(if depth > 2 { 1 } else { 5 }) {
        0 => {
            let op = *r.pick(&crate::filter::OPS);
            let value = match op {
                "exists" => None,
                "lt" | "le" | "gt" | "ge" => {
                    Some(pick(r, &[json!(1), json!(1.5), json!("v"), json!("x"), json!(true)]))
                }
                "prefix" => Some(pick(r, &[json!(""), json!("v"), json!("x")])),
                "in" => Some(json!([pick(r, &[json!(1), json!("v")]), pick(r, &[json!(2.0), json!("x")])])),
                _ => {
                    Some(pick(r, &[json!(1), json!(2.0), json!("v"), json!("x"), json!(["x", 1]), json!({"x": true})]))
                }
            };
            let mut f = match (r.below(8), op) {
                (0, "contains") => json!({}),
                (0 | 1, _) => json!({"header": "h"}),
                _ => json!({"field": *r.pick(&["a", "k", "tags", "tags.0", "n", "n.x", "missing"])}),
            };
            f["op"] = json!(op);
            if let Some(v) = value {
                f["value"] = if f.get("field").is_none() && f.get("header").is_none() { json!("v") } else { v };
            }
            f
        }
        1 => json!({"and": (0..r.usize(3)).map(|_| gen_filter(r, depth + 1)).collect::<Vec<_>>()}),
        2 => json!({"or": (0..r.usize(3)).map(|_| gen_filter(r, depth + 1)).collect::<Vec<_>>()}),
        _ => json!({"not": gen_filter(r, depth + 1)}),
    }
}

fn gen_body(r: &mut Rng) -> Body {
    let docs = [
        json!({"a": 1, "k": "v"}),
        json!({"a": 2.0, "tags": ["x", 1]}),
        json!({"a": null, "k": "w"}),
        json!({"a": "x", "n": {"x": true}}),
        json!({"tags": [], "n": {"x": true}}),
        json!([1, "v"]),
        json!("v"),
    ];
    let mut b = match r.usize(docs.len() + 1) {
        i if i < docs.len() => Body { json: true, headers: vec![], data: Bytes::from(docs[i].to_string()) },
        _ => Body::text(Bytes::from_static(b"plain v=1")),
    };
    if r.chance(0.6) {
        b.headers = vec![("h".into(), r.pick(&["v", "w", ""]).to_string())];
    }
    b
}

fn model_matches(f: &Json, b: &Body) -> bool {
    fn canon(v: &Json) -> Json {
        match v {
            Json::Number(n) => json!(n.as_f64().unwrap()),
            Json::Array(a) => Json::Array(a.iter().map(canon).collect()),
            Json::Object(o) => Json::Object(o.iter().map(|(k, v)| (k.clone(), canon(v))).collect()),
            _ => v.clone(),
        }
    }
    let o = f.as_object().unwrap();
    match (o.get("and"), o.get("or"), o.get("not")) {
        (Some(fs), ..) => return fs.as_array().unwrap().iter().all(|f| model_matches(f, b)),
        (_, Some(fs), _) => return fs.as_array().unwrap().iter().any(|f| model_matches(f, b)),
        (.., Some(f)) => return !model_matches(f, b),
        _ => {}
    }
    let value = canon(o.get("value").unwrap_or(&Json::Null));
    let target = match (o.get("field"), o.get("header")) {
        (Some(f), _) => {
            let doc: Option<Json> = b.json.then(|| serde_json::from_slice(&b.data).unwrap());
            doc.and_then(|d| d.pointer(&format!("/{}", f.as_str().unwrap().replace('.', "/"))).cloned())
        }
        (_, Some(h)) => b.header(h.as_str().unwrap()).map(Json::from),
        _ => {
            let needle = value.as_str().unwrap().as_bytes();
            return b.data.windows(needle.len()).any(|w| w == needle);
        }
    };
    let Some(t) = target.filter(|t| !t.is_null()).map(|t| canon(&t)) else { return false };
    let order = match (&t, &value) {
        (Json::Number(x), Json::Number(y)) => x.as_f64().unwrap().partial_cmp(&y.as_f64().unwrap()),
        (Json::String(x), Json::String(y)) => Some(x.cmp(y)),
        (Json::Bool(x), Json::Bool(y)) => Some(x.cmp(y)),
        _ => None,
    };
    use std::cmp::Ordering::*;
    match o["op"].as_str().unwrap() {
        "exists" => true,
        "eq" => t == value,
        "ne" => t != value,
        "lt" => order == Some(Less),
        "le" => order.is_some_and(|o| o != Greater),
        "gt" => order == Some(Greater),
        "ge" => order.is_some_and(|o| o != Less),
        "in" => value.as_array().unwrap().contains(&t),
        "prefix" => t.as_str().is_some_and(|s| s.starts_with(value.as_str().unwrap())),
        _ => match &t {
            Json::String(s) => value.as_str().is_some_and(|p| s.contains(p)),
            Json::Array(a) => a.contains(&value),
            _ => false,
        },
    }
}

#[test]
fn prop_filters_match_model() {
    prop::check(
        "filters_match_model",
        2000,
        |r| (gen_filter(r, 0), (0..8).map(|_| gen_body(r)).collect::<Vec<_>>()),
        prop::no_shrink,
        |(f, bodies)| {
            let filter: Filter = serde_json::from_value(f.clone()).map_err(|e| format!("{f}: {e}"))?;
            for b in bodies {
                if filter.matches(b) != model_matches(f, b) {
                    return Err(format!("{f} on {:?}: model says {}", b, model_matches(f, b)));
                }
            }
            Ok(())
        },
    );
}

#[test]
fn trims_by_time_conflict_only_with_appends_they_could_have_trimmed() {
    rt().block_on(async {
        let (eng, _, _) = engine(4, EngineConfig::small_for_tests());
        let db = opened(&eng, "d").await;
        create_stream(&db, "s", Retention::default()).await;
        append_texts(&db, "s", &["0", "1"]).await;
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        let late = append_texts(&db, "s", &["2"]).await;
        let mut found = begin(&db).await;
        trim(&mut found, "s", None, Some(late.time_ms)).await.unwrap();
        let mut past_end = begin(&db).await;
        trim(&mut past_end, "s", None, Some(u64::MAX / 2)).await.unwrap();
        assert!(!found.has_untracked_reads() && !past_end.has_untracked_reads());
        append_texts(&db, "s", &["3"]).await;
        commit(&db, found).await.unwrap();
        assert!(matches!(commit(&db, past_end).await, Err(Error::Conflict(_))));
        assert_eq!(texts(&read_all(&db, "s", ReadSpec::default()).await), ["2", "3"]);
    });
}
