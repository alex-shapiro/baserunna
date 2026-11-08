//! Simulation workload for list-append transactions over KV keys and streams, recorded as an
//! Elle history.
//!
//! Each key holds a list of unique integers. An even key is a KV key whose value is the list in
//! comma-separated decimals; interactive serializable transactions append to it with a
//! read-modify-write. An odd key is a stream with one record per element; one-shot and
//! interactive transactions append to it with the `append` op. A read returns the whole list.
//! Each key takes a bounded number of appends and is then replaced by a fresh key, so lists stay
//! short and version orders stay easy to infer.
//!
//! The checker in this module verifies the properties that need no dependency graph: every read
//! of a key is a prefix of the key's final value, every acknowledged append appears in the final
//! value exactly once, no failed append is ever read, and no read contains a value that was never
//! appended to its key. If `SIM_ELLE_DIR` is set, the run also writes its history in EDN to
//! `elle-<seed>.edn`, so that Elle can search the history for dependency cycles under strict
//! serializability.

use super::super::client::Client;
use super::super::{NetError, Sim};
use crate::util::rng::Rng;
use serde_json::{Value as Json, json};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};

const DB: &str = "elle";
/// Process IDs at and above this value perform the final reads.
const FINAL_PROCESS: u64 = 1_000_000;

/// A micro-operation of a list-append transaction.
#[derive(Clone, Debug, PartialEq)]
enum Mop {
    Read(u64, Option<Vec<u64>>),
    Append(u64, u64),
}

impl Mop {
    fn key(&self) -> u64 {
        match self {
            Mop::Read(k, _) | Mop::Append(k, _) => *k,
        }
    }
}

fn is_stream(key: u64) -> bool {
    key % 2 == 1
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Invoke,
    Ok,
    Fail,
    Info,
}

impl Kind {
    fn edn(self) -> &'static str {
        match self {
            Kind::Invoke => ":invoke",
            Kind::Ok => ":ok",
            Kind::Fail => ":fail",
            Kind::Info => ":info",
        }
    }
}

#[derive(Clone, Debug)]
struct Event {
    kind: Kind,
    process: u64,
    time_ms: u64,
    mops: Vec<Mop>,
}

/// Workload parameters.
#[derive(Clone, Copy, Debug)]
pub struct Shape {
    pub clients: usize,
    pub txns: usize,
    pub max_mops: usize,
    pub active_keys: usize,
    pub max_appends: u64,
    pub interactive: f64,
    pub hot: f64,
    pub max_pause_ms: u64,
}

impl Shape {
    pub fn draw(k: &super::super::swarm::Knobs) -> Shape {
        Shape {
            clients: k.range("elle.clients", 2, 10) as usize,
            txns: k.range("elle.txns", 20, 120) as usize,
            max_mops: k.log_range("elle.max_mops", 1, 6) as usize,
            active_keys: k.log_range("elle.active_keys", 2, 16) as usize,
            max_appends: k.log_range("elle.max_appends", 4, 48),
            interactive: k.pick("elle.interactive", &[0.2, 0.5, 0.8]),
            hot: k.pick("elle.hot", &[0.0, 0.3]),
            max_pause_ms: k.pick("elle.max_pause_ms", &[0, 20, 200]),
        }
    }
}

/// History and key allocation shared by all clients.
struct World {
    events: Vec<Event>,
    active: Vec<u64>,
    appends: BTreeMap<u64, u64>,
    next_key: u64,
    next_value: u64,
    next_process: u64,
    created: BTreeSet<u64>,
}

impl World {
    fn new(active_keys: usize, clients: usize) -> World {
        World {
            events: Vec::new(),
            active: (0..active_keys as u64).collect(),
            appends: BTreeMap::new(),
            next_key: active_keys as u64,
            next_value: 1,
            next_process: clients as u64,
            created: BTreeSet::new(),
        }
    }

    /// Generates a transaction. Appends to KV keys are generated only for interactive transactions,
    /// and reads of a stream after an append to it in the same transaction are dropped because a
    /// transaction does not see its own stream appends.
    fn generate(&mut self, rng: &mut Rng, shape: &Shape, interactive: bool) -> Vec<Mop> {
        let n = 1 + rng.usize(shape.max_mops);
        let mut mops = Vec::with_capacity(n);
        for _ in 0..n {
            let slot = if rng.chance(shape.hot) { 0 } else { rng.usize(self.active.len()) };
            let key = self.active[slot];
            if rng.chance(0.5) && (interactive || is_stream(key)) {
                let v = self.next_value;
                self.next_value += 1;
                mops.push(Mop::Append(key, v));
                let count = self.appends.entry(key).or_default();
                *count += 1;
                if *count >= shape.max_appends {
                    self.active[slot] = self.next_key;
                    self.next_key += 1;
                }
            } else {
                mops.push(Mop::Read(key, None));
            }
        }
        let mut appended = BTreeSet::new();
        mops.retain(|m| match m {
            Mop::Append(k, _) => {
                appended.insert(*k);
                true
            }
            Mop::Read(k, _) => !(is_stream(*k) && appended.contains(k)),
        });
        mops
    }
}

fn world(w: &Mutex<World>) -> std::sync::MutexGuard<'_, World> {
    w.lock().unwrap_or_else(|e| e.into_inner())
}

fn record(w: &Mutex<World>, sim: &Sim, kind: Kind, process: u64, mops: &[Mop]) {
    let e = Event { kind, process, time_ms: sim.now_ms(), mops: mops.to_vec() };
    world(w).events.push(e);
}

fn read_op(key: u64) -> Json {
    match is_stream(key) {
        true => json!({"op": "read_stream", "stream": format!("s{key}"), "from_seq": 0, "limit": 1000}),
        false => json!({"op": "get", "key": format!("k{key}")}),
    }
}

fn append_op(key: u64, v: u64) -> Json {
    json!({"op": "append", "stream": format!("s{key}"), "records": [{"value": v.to_string()}]})
}

fn put_op(key: u64, list: &[u64]) -> Json {
    let value = list.iter().map(u64::to_string).collect::<Vec<_>>().join(",");
    json!({"op": "put", "key": format!("k{key}"), "value": value})
}

/// Parses the list returned by a `get` or `read_stream` result.
fn parse_list(key: u64, r: &Json) -> Result<Option<Vec<u64>>, String> {
    let bad = || format!("unexpected result for key {key}: {r}");
    if is_stream(key) {
        let records = r["records"].as_array().ok_or_else(bad)?;
        if r["done"] != true {
            return Err(bad());
        }
        let list: Option<Vec<u64>> = records.iter().map(|x| x["value"].as_str()?.parse().ok()).collect();
        return list.map(Some).ok_or_else(bad);
    }
    if r["found"] != true {
        return Ok(None);
    }
    let s = r["value"].as_str().ok_or_else(bad)?;
    let list: Result<Vec<u64>, _> = s.split(',').map(str::parse).collect();
    list.map(Some).map_err(|_| bad())
}

/// Transaction outcome as seen by the client.
enum Done {
    Ok(Vec<Mop>),
    Fail,
    Info,
}

/// Classifies a failed response: `Some(Fail)` if it was certainly not applied, `Some(Info)` if
/// it may have been applied, and `None` if the status is unexpected.
fn failure(r: &Result<super::super::client::Resp, NetError>) -> Option<Done> {
    match r {
        Ok(r) if r.outcome_unknown() => Some(Done::Info),
        Ok(r) if matches!(r.status, 404 | 409 | 412 | 429) || r.retryable_not_applied() => Some(Done::Fail),
        Ok(_) => None,
        Err(NetError::Refused) => Some(Done::Fail),
        Err(_) => Some(Done::Info),
    }
}

fn unexpected(what: &str, r: &Result<super::super::client::Resp, NetError>) -> String {
    match r {
        Ok(r) => format!("{what}: unexpected {} {}", r.status, r.json),
        Err(e) => format!("{what}: {e:?}"),
    }
}

/// Fills the reads of `mops` from the results of `ops`, which correspond one-to-one.
fn fill(mops: &[Mop], results: &Json) -> Result<Vec<Mop>, String> {
    let rs = results.as_array().ok_or_else(|| format!("missing results: {results}"))?;
    if rs.len() != mops.len() {
        return Err(format!("{} results for {} ops", rs.len(), mops.len()));
    }
    mops.iter()
        .zip(rs)
        .map(|(m, r)| match m {
            Mop::Read(k, _) => Ok(Mop::Read(*k, parse_list(*k, r)?)),
            a => Ok(a.clone()),
        })
        .collect()
}

async fn one_shot(c: &Client, mops: &[Mop]) -> Result<Done, String> {
    let ops: Vec<Json> = mops
        .iter()
        .map(|m| match m {
            Mop::Read(k, _) => read_op(*k),
            Mop::Append(k, v) => append_op(*k, *v),
        })
        .collect();
    let r = c.call("POST", &format!("/v1/dbs/{DB}/txn"), Some(json!({"ops": ops}))).await;
    if let Ok(resp) = &r
        && resp.status == 200
    {
        return Ok(Done::Ok(fill(mops, &resp.json["results"])?));
    }
    failure(&r).ok_or_else(|| unexpected("one-shot", &r))
}

async fn interactive(c: &Client, mops: &[Mop]) -> Result<Done, String> {
    let base = format!("/v1/dbs/{DB}/transactions");
    let kv_appended: Vec<u64> = mops
        .iter()
        .filter_map(|m| match m {
            Mop::Append(k, _) if !is_stream(*k) => Some(*k),
            _ => None,
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let prefetch: Vec<Json> = kv_appended.iter().map(|k| read_op(*k)).collect();
    let r = c.call("POST", &base, Some(json!({"ops": prefetch}))).await;
    let begun = match &r {
        Ok(resp) if resp.status == 201 => resp.json.clone(),
        // A transaction that never began cannot commit.
        _ => return failure(&r).map(|_| Done::Fail).ok_or_else(|| unexpected("begin", &r)),
    };
    let id = begun["txn_id"].as_str().ok_or("missing txn_id")?.to_string();
    let empty = Json::Array(vec![]);
    let prefetched = begun["results"].as_array().unwrap_or(empty.as_array().expect("array"));
    let mut lists: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    for (k, res) in kv_appended.iter().zip(prefetched) {
        lists.insert(*k, parse_list(*k, res)?.unwrap_or_default());
    }
    let ops: Vec<Json> = mops
        .iter()
        .map(|m| match m {
            Mop::Read(k, _) => read_op(*k),
            Mop::Append(k, v) if is_stream(*k) => append_op(*k, *v),
            Mop::Append(k, v) => {
                let list = lists.entry(*k).or_default();
                list.push(*v);
                put_op(*k, list)
            }
        })
        .collect();
    let r = c.call("POST", &format!("{base}/{id}/ops"), Some(json!({"ops": ops}))).await;
    let filled = match &r {
        Ok(resp) if resp.status == 200 => fill(mops, &resp.json["results"])?,
        // The node aborts a transaction whose ops request fails, and an unanswered transaction
        // expires without committing.
        _ => {
            let _ = c.call("DELETE", &format!("{base}/{id}"), None).await;
            return failure(&r).map(|_| Done::Fail).ok_or_else(|| unexpected("ops", &r));
        }
    };
    let r = c.call("POST", &format!("{base}/{id}/commit"), Some(json!({}))).await;
    if let Ok(resp) = &r
        && resp.status == 200
    {
        return Ok(Done::Ok(filled));
    }
    failure(&r).ok_or_else(|| unexpected("commit", &r))
}

/// Creates the streams of `mops` that no client has created yet.
async fn create_streams(c: &Client, w: &Mutex<World>, mops: &[Mop]) -> Result<(), String> {
    let wanted: BTreeSet<u64> = mops.iter().map(Mop::key).filter(|k| is_stream(*k)).collect();
    for k in wanted {
        if world(w).created.contains(&k) {
            continue;
        }
        loop {
            let r = c.call("POST", &format!("/v1/dbs/{DB}/streams"), Some(json!({"name": format!("s{k}")}))).await;
            match &r {
                Ok(resp) if resp.status == 201 || resp.code() == "already_exists" => break,
                Ok(resp) if resp.status >= 500 || resp.status == 409 || resp.status == 429 => {}
                Err(_) => {}
                _ => return Err(unexpected("create stream", &r)),
            }
            c.pause(100).await;
        }
        world(w).created.insert(k);
    }
    Ok(())
}

/// Reads every key with strong one-shot transactions until each read succeeds.
async fn final_reads(sim: &Sim, c: &Client, w: &Mutex<World>) -> Result<BTreeMap<u64, Vec<u64>>, String> {
    let (keys, created) = {
        let w = world(w);
        (w.next_key, w.created.clone())
    };
    let mut out = BTreeMap::new();
    let keys: Vec<u64> = (0..keys).filter(|k| !is_stream(*k) || created.contains(k)).collect();
    for (i, chunk) in keys.chunks(64).enumerate() {
        let mops: Vec<Mop> = chunk.iter().map(|k| Mop::Read(*k, None)).collect();
        let process = FINAL_PROCESS + i as u64;
        loop {
            record(w, sim, Kind::Invoke, process, &mops);
            match one_shot(c, &mops).await? {
                Done::Ok(filled) => {
                    record(w, sim, Kind::Ok, process, &filled);
                    for m in filled {
                        if let Mop::Read(k, l) = m {
                            out.insert(k, l.unwrap_or_default());
                        }
                    }
                    break;
                }
                _ => {
                    record(w, sim, Kind::Fail, process, &mops);
                    c.pause(200).await;
                }
            }
        }
    }
    Ok(out)
}

/// Verifies the history against the final value of every key.
fn check(events: &[Event], finals: &BTreeMap<u64, Vec<u64>>) -> Result<(), String> {
    let mut appended: BTreeMap<u64, u64> = BTreeMap::new();
    let mut invoked: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    for e in events.iter().filter(|e| e.kind == Kind::Invoke) {
        for m in &e.mops {
            if let Mop::Append(k, v) = m {
                appended.insert(*v, *k);
                invoked.entry(*k).or_default().push(*v);
            }
        }
    }
    for (k, list) in finals {
        let mut seen = BTreeSet::new();
        for v in list {
            if !seen.insert(*v) {
                return Err(format!("key {k} holds {v} twice: {list:?}"));
            }
            if appended.get(v) != Some(k) {
                return Err(format!("key {k} holds {v}, which was never appended to it: {list:?}"));
            }
        }
    }
    let empty = Vec::new();
    for e in events {
        let fin = |k: &u64| finals.get(k).unwrap_or(&empty);
        for m in &e.mops {
            match (e.kind, m) {
                (Kind::Ok, Mop::Read(k, Some(l))) if !fin(k).starts_with(l) => {
                    return Err(format!(
                        "process {} read key {k} as {l:?}, which is not a prefix of its final value {:?}",
                        e.process,
                        fin(k)
                    ));
                }
                (Kind::Ok, Mop::Append(k, v)) if !fin(k).contains(v) => {
                    return Err(format!(
                        "process {} appended {v} to key {k} and committed, but the final value {:?} lacks it",
                        e.process,
                        fin(k)
                    ));
                }
                (Kind::Fail, Mop::Append(k, v)) if fin(k).contains(v) => {
                    return Err(format!(
                        "process {} appended {v} to key {k} and failed, but the final value {:?} holds it",
                        e.process,
                        fin(k)
                    ));
                }
                _ => {}
            }
        }
    }
    Ok(())
}

/// Formats `events` as an EDN history for Elle's list-append checker.
fn edn(events: &[Event]) -> String {
    let mut out = String::new();
    for (i, e) in events.iter().enumerate() {
        let mops: Vec<String> = e
            .mops
            .iter()
            .map(|m| match m {
                Mop::Read(k, None) => format!("[:r {k} nil]"),
                Mop::Read(k, Some(l)) => {
                    format!("[:r {k} [{}]]", l.iter().map(u64::to_string).collect::<Vec<_>>().join(" "))
                }
                Mop::Append(k, v) => format!("[:append {k} {v}]"),
            })
            .collect();
        let _ = writeln!(
            out,
            "{{:index {i}, :type {}, :process {}, :time {}, :f :txn, :value [{}]}}",
            e.kind.edn(),
            e.process,
            e.time_ms * 1_000_000 + i as u64,
            mops.join(" ")
        );
    }
    out
}

pub async fn run(sim: Arc<Sim>, shape: Shape, chaos: bool) -> Result<(), String> {
    let owner = Client::signup(&sim, "elle", "elle@elle.test", "password-123").await?;
    owner.create_db(DB).await?;
    let token = owner.token.clone().unwrap_or_default();
    let w = Arc::new(Mutex::new(World::new(shape.active_keys, shape.clients)));
    let phase = chaos.then(|| super::chaos_phase(&sim));
    let mut hs = Vec::new();
    for ci in 0..shape.clients {
        let c = Client::with_token(&sim, &format!("elle{ci}"), &token);
        let (w, sim) = (w.clone(), sim.clone());
        hs.push(tokio::spawn(async move {
            let mut rng = sim.fork_rng();
            let mut process = ci as u64;
            for _ in 0..shape.txns {
                c.pause(rng.below(shape.max_pause_ms + 1)).await;
                let interactive_txn = rng.chance(shape.interactive);
                let mops = world(&w).generate(&mut rng, &shape, interactive_txn);
                create_streams(&c, &w, &mops).await?;
                record(&w, &sim, Kind::Invoke, process, &mops);
                let done = match interactive_txn {
                    true => interactive(&c, &mops).await?,
                    false => one_shot(&c, &mops).await?,
                };
                match done {
                    Done::Ok(filled) => record(&w, &sim, Kind::Ok, process, &filled),
                    Done::Fail => record(&w, &sim, Kind::Fail, process, &mops),
                    Done::Info => {
                        record(&w, &sim, Kind::Info, process, &mops);
                        // The transaction may still be in flight, so this process cannot issue another.
                        let mut g = world(&w);
                        process = g.next_process;
                        g.next_process += 1;
                    }
                }
            }
            Ok::<(), String>(())
        }));
    }
    let mut err = None;
    for h in hs {
        if let Err(e) = h.await.map_err(|e| e.to_string()).and_then(|r| r) {
            err = err.or(Some(e));
        }
    }
    if let Some(e) = err {
        return Err(e);
    }
    super::settle(&sim, phase).await;
    let finals = final_reads(&sim, &owner, &w).await?;
    let events = world(&w).events.clone();
    if let Ok(dir) = std::env::var("SIM_ELLE_DIR") {
        let path = std::path::Path::new(&dir).join(format!("elle-{}.edn", sim.seed));
        std::fs::write(&path, edn(&events)).map_err(|e| format!("write {}: {e}", path.display()))?;
    }
    check(&events, &finals).map_err(|e| format!("list-append history of {} events: {e}", events.len()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(kind: Kind, mops: Vec<Mop>) -> Event {
        Event { kind, process: 0, time_ms: 0, mops }
    }

    #[test]
    fn rejects_lost_aborted_and_divergent_appends() {
        let finals: BTreeMap<u64, Vec<u64>> = [(1, vec![1, 2])].into();
        let invoke = ev(Kind::Invoke, vec![Mop::Append(1, 1), Mop::Append(1, 2), Mop::Append(1, 3)]);
        let ok = |m| vec![invoke.clone(), ev(Kind::Ok, m)];
        check(&ok(vec![Mop::Append(1, 1), Mop::Read(1, Some(vec![1]))]), &finals).unwrap();
        let lost = check(&ok(vec![Mop::Append(1, 3)]), &finals).unwrap_err();
        assert!(lost.contains("lacks it"), "{lost}");
        let divergent = check(&ok(vec![Mop::Read(1, Some(vec![2]))]), &finals).unwrap_err();
        assert!(divergent.contains("not a prefix"), "{divergent}");
        let aborted = check(&[invoke.clone(), ev(Kind::Fail, vec![Mop::Append(1, 2)])], &finals).unwrap_err();
        assert!(aborted.contains("failed"), "{aborted}");
        let twice: BTreeMap<u64, Vec<u64>> = [(1, vec![1, 1])].into();
        assert!(check(&[invoke.clone()], &twice).unwrap_err().contains("twice"));
        let garbage: BTreeMap<u64, Vec<u64>> = [(1, vec![9])].into();
        assert!(check(&[invoke], &garbage).unwrap_err().contains("never appended"));
    }

    #[test]
    fn writes_elle_histories() {
        let h = edn(&[
            ev(Kind::Invoke, vec![Mop::Read(2, None), Mop::Append(3, 4)]),
            ev(Kind::Ok, vec![Mop::Read(2, Some(vec![1, 5])), Mop::Append(3, 4)]),
        ]);
        assert_eq!(
            h,
            "{:index 0, :type :invoke, :process 0, :time 0, :f :txn, :value [[:r 2 nil] [:append 3 4]]}\n\
             {:index 1, :type :ok, :process 0, :time 1, :f :txn, :value [[:r 2 [1 5]] [:append 3 4]]}\n"
        );
    }
}
