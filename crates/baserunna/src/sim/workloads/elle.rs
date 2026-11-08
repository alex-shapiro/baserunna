//! Simulation workload and strict-serializability checker for list-append transactions over KV
//! keys and streams.
//!
//! Each key stores a list of unique integers. Even keys are KV entries that store
//! comma-separated integers and are updated by interactive read-modify-write transactions. Odd
//! keys are streams with one record per integer and are updated by `append` operations in
//! one-shot or interactive transactions. After a key reaches `max_appends`, the generator
//! replaces it with a new key to keep lists short.
//!
//! After fault injection ends, the workload reads the final list for every key and verifies:
//! - Every read returns a prefix of the key's final list.
//! - Every committed append appears once in the final list, and aborted appends are absent.
//! - Every read after a transaction's own KV appends ends with those appends.
//! - Every read includes all appends from transactions that completed before the reading
//!   transaction began.
//! - The write-write, write-read, read-write, and real-time dependency graph is acyclic.
//!
//! If `SIM_ELLE_DIR` is set, the workload also writes the history to `elle-<seed>.edn` for
//! Jepsen's Elle checker.

use super::super::client::Client;
use super::super::{NetError, Sim};
use crate::util::rng::Rng;
use serde_json::{Value as Json, json};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};

const DB_NAME: &str = "elle";
/// Minimum process ID reserved for final-state read transactions.
const FINAL_READ_PROCESS_BASE: u64 = 1_000_000;

/// A single list read or append operation within a transaction.
#[derive(Clone, Debug, PartialEq)]
enum ListOp {
    Read(u64, Option<Vec<u64>>),
    Append(u64, u64),
}

impl ListOp {
    fn key(&self) -> u64 {
        match self {
            ListOp::Read(k, _) | ListOp::Append(k, _) => *k,
        }
    }
}

fn is_stream_key(key: u64) -> bool {
    key % 2 == 1
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EventKind {
    Invoke,
    Ok,
    Fail,
    Info,
}

impl EventKind {
    fn edn_keyword(self) -> &'static str {
        match self {
            EventKind::Invoke => ":invoke",
            EventKind::Ok => ":ok",
            EventKind::Fail => ":fail",
            EventKind::Info => ":info",
        }
    }
}

#[derive(Clone, Debug)]
struct HistoryEvent {
    kind: EventKind,
    process: u64,
    time_ms: u64,
    ops: Vec<ListOp>,
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

/// Shared history and key-allocation state across workload clients.
struct WorkloadState {
    events: Vec<HistoryEvent>,
    active_keys: Vec<u64>,
    append_counts: BTreeMap<u64, u64>,
    next_key: u64,
    next_value: u64,
    next_process: u64,
    created_streams: BTreeSet<u64>,
}

impl WorkloadState {
    fn new(active_keys: usize, clients: usize) -> WorkloadState {
        WorkloadState {
            events: Vec::new(),
            active_keys: (0..active_keys as u64).collect(),
            append_counts: BTreeMap::new(),
            next_key: active_keys as u64,
            next_value: 1,
            next_process: clients as u64,
            created_streams: BTreeSet::new(),
        }
    }

    /// Generates a list-append transaction. KV appends require an interactive transaction, and
    /// stream reads after an append to the same stream in the same transaction are omitted because
    /// a transaction does not read its own uncommitted stream appends.
    fn generate_txn(&mut self, rng: &mut Rng, shape: &Shape, interactive: bool) -> Vec<ListOp> {
        let count = 1 + rng.usize(shape.max_mops);
        let mut ops = Vec::with_capacity(count);
        for _ in 0..count {
            let slot = if rng.chance(shape.hot) { 0 } else { rng.usize(self.active_keys.len()) };
            let key = self.active_keys[slot];
            if rng.chance(0.5) && (interactive || is_stream_key(key)) {
                let value = self.next_value;
                self.next_value += 1;
                ops.push(ListOp::Append(key, value));
                let appends = self.append_counts.entry(key).or_default();
                *appends += 1;
                if *appends >= shape.max_appends {
                    self.active_keys[slot] = self.next_key;
                    self.next_key += 1;
                }
            } else {
                ops.push(ListOp::Read(key, None));
            }
        }
        let mut appended_keys = BTreeSet::new();
        ops.retain(|op| match op {
            ListOp::Append(k, _) => {
                appended_keys.insert(*k);
                true
            }
            ListOp::Read(k, _) => !(is_stream_key(*k) && appended_keys.contains(k)),
        });
        ops
    }
}

fn lock_state(state: &Mutex<WorkloadState>) -> std::sync::MutexGuard<'_, WorkloadState> {
    state.lock().unwrap_or_else(|e| e.into_inner())
}

fn record_event(state: &Mutex<WorkloadState>, sim: &Sim, kind: EventKind, process: u64, ops: &[ListOp]) {
    let event = HistoryEvent { kind, process, time_ms: sim.now_ms(), ops: ops.to_vec() };
    lock_state(state).events.push(event);
}

fn read_op_json(key: u64) -> Json {
    match is_stream_key(key) {
        true => json!({"op": "read_stream", "stream": format!("s{key}"), "from_seq": 0, "limit": 1000}),
        false => json!({"op": "get", "key": format!("k{key}")}),
    }
}

fn append_op_json(key: u64, value: u64) -> Json {
    json!({"op": "append", "stream": format!("s{key}"), "records": [{"value": value.to_string()}]})
}

fn put_op_json(key: u64, list: &[u64]) -> Json {
    let value = list.iter().map(u64::to_string).collect::<Vec<_>>().join(",");
    json!({"op": "put", "key": format!("k{key}"), "value": value})
}

/// Parses the integer list from a `get` or `read_stream` operation result.
fn parse_list(key: u64, result: &Json) -> Result<Option<Vec<u64>>, String> {
    let invalid = || format!("unexpected result for key {key}: {result}");
    if is_stream_key(key) {
        let records = result["records"].as_array().ok_or_else(invalid)?;
        if result["done"] != true {
            return Err(invalid());
        }
        let list: Option<Vec<u64>> = records.iter().map(|x| x["value"].as_str()?.parse().ok()).collect();
        return list.map(Some).ok_or_else(invalid);
    }
    if result["found"] != true {
        return Ok(None);
    }
    let text = result["value"].as_str().ok_or_else(invalid)?;
    let list: Result<Vec<u64>, _> = text.split(',').map(str::parse).collect();
    list.map(Some).map_err(|_| invalid())
}

/// Client-observed outcome of a transaction attempt.
enum TxnOutcome {
    Ok(Vec<ListOp>),
    Failed,
    Unknown,
}

/// Classifies a non-2xx response or network error as [`TxnOutcome::Failed`] or
/// [`TxnOutcome::Unknown`].
fn classify_failed_response(resp: &Result<super::super::client::Resp, NetError>) -> Option<TxnOutcome> {
    match resp {
        Ok(r) if r.outcome_unknown() => Some(TxnOutcome::Unknown),
        Ok(r) if matches!(r.status, 404 | 409 | 412 | 429) || r.retryable_not_applied() => Some(TxnOutcome::Failed),
        Ok(_) => None,
        Err(NetError::Refused) => Some(TxnOutcome::Failed),
        Err(_) => Some(TxnOutcome::Unknown),
    }
}

fn format_unexpected_response(step: &str, resp: &Result<super::super::client::Resp, NetError>) -> String {
    match resp {
        Ok(r) => format!("{step}: unexpected {} {}", r.status, r.json),
        Err(e) => format!("{step}: {e:?}"),
    }
}

/// Populates the read results in `ops` from the transaction `results` array.
fn attach_read_results(ops: &[ListOp], results: &Json) -> Result<Vec<ListOp>, String> {
    let items = results.as_array().ok_or_else(|| format!("missing results: {results}"))?;
    if items.len() != ops.len() {
        return Err(format!("{} results for {} ops", items.len(), ops.len()));
    }
    ops.iter()
        .zip(items)
        .map(|(op, res)| match op {
            ListOp::Read(k, _) => Ok(ListOp::Read(*k, parse_list(*k, res)?)),
            append => Ok(append.clone()),
        })
        .collect()
}

async fn run_one_shot_txn(client: &Client, ops: &[ListOp]) -> Result<TxnOutcome, String> {
    let api_ops: Vec<Json> = ops
        .iter()
        .map(|op| match op {
            ListOp::Read(k, _) => read_op_json(*k),
            ListOp::Append(k, v) => append_op_json(*k, *v),
        })
        .collect();
    let resp = client.call("POST", &format!("/v1/dbs/{DB_NAME}/txn"), Some(json!({"ops": api_ops}))).await;
    if let Ok(ok) = &resp
        && ok.status == 200
    {
        return Ok(TxnOutcome::Ok(attach_read_results(ops, &ok.json["results"])?));
    }
    classify_failed_response(&resp).ok_or_else(|| format_unexpected_response("one-shot", &resp))
}

async fn run_interactive_txn(client: &Client, ops: &[ListOp]) -> Result<TxnOutcome, String> {
    let base = format!("/v1/dbs/{DB_NAME}/transactions");
    let kv_append_keys: Vec<u64> = ops
        .iter()
        .filter_map(|op| match op {
            ListOp::Append(k, _) if !is_stream_key(*k) => Some(*k),
            _ => None,
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let prefetch: Vec<Json> = kv_append_keys.iter().map(|k| read_op_json(*k)).collect();
    let resp = client.call("POST", &base, Some(json!({"ops": prefetch}))).await;
    let begun = match &resp {
        Ok(ok) if ok.status == 201 => ok.json.clone(),
        // A transaction that fails during `begin` cannot commit.
        _ => {
            return classify_failed_response(&resp)
                .map(|_| TxnOutcome::Failed)
                .ok_or_else(|| format_unexpected_response("begin", &resp));
        }
    };
    let txn_id = begun["txn_id"].as_str().ok_or("missing txn_id")?.to_string();
    let empty = Json::Array(vec![]);
    let prefetched = begun["results"].as_array().unwrap_or(empty.as_array().expect("array"));
    let mut current_lists: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
    for (k, res) in kv_append_keys.iter().zip(prefetched) {
        current_lists.insert(*k, parse_list(*k, res)?.unwrap_or_default());
    }
    let api_ops: Vec<Json> = ops
        .iter()
        .map(|op| match op {
            ListOp::Read(k, _) => read_op_json(*k),
            ListOp::Append(k, v) if is_stream_key(*k) => append_op_json(*k, *v),
            ListOp::Append(k, v) => {
                let list = current_lists.entry(*k).or_default();
                list.push(*v);
                put_op_json(*k, list)
            }
        })
        .collect();
    let resp = client.call("POST", &format!("{base}/{txn_id}/ops"), Some(json!({"ops": api_ops}))).await;
    let filled = match &resp {
        Ok(ok) if ok.status == 200 => attach_read_results(ops, &ok.json["results"])?,
        // If `ops` fails, the node aborts the transaction; if the request times out without
        // reaching `commit`, the transaction expires uncommitted.
        _ => {
            let _ = client.call("DELETE", &format!("{base}/{txn_id}"), None).await;
            return classify_failed_response(&resp)
                .map(|_| TxnOutcome::Failed)
                .ok_or_else(|| format_unexpected_response("ops", &resp));
        }
    };
    let resp = client.call("POST", &format!("{base}/{txn_id}/commit"), Some(json!({}))).await;
    if let Ok(ok) = &resp
        && ok.status == 200
    {
        return Ok(TxnOutcome::Ok(filled));
    }
    classify_failed_response(&resp).ok_or_else(|| format_unexpected_response("commit", &resp))
}

/// Creates any stream referenced in `ops` that has not been created yet.
async fn ensure_streams_exist(client: &Client, state: &Mutex<WorkloadState>, ops: &[ListOp]) -> Result<(), String> {
    let stream_keys: BTreeSet<u64> = ops.iter().map(ListOp::key).filter(|k| is_stream_key(*k)).collect();
    for k in stream_keys {
        if lock_state(state).created_streams.contains(&k) {
            continue;
        }
        loop {
            let resp = client
                .call("POST", &format!("/v1/dbs/{DB_NAME}/streams"), Some(json!({"name": format!("s{k}")})))
                .await;
            match &resp {
                Ok(ok) if ok.status == 201 || ok.code() == "already_exists" => break,
                Ok(ok) if ok.status >= 500 || ok.status == 409 || ok.status == 429 => {}
                Err(_) => {}
                _ => return Err(format_unexpected_response("create stream", &resp)),
            }
            client.pause(100).await;
        }
        lock_state(state).created_streams.insert(k);
    }
    Ok(())
}

/// Reads the final list of every key using strong one-shot transactions.
async fn read_final_lists(
    sim: &Sim,
    client: &Client,
    state: &Mutex<WorkloadState>,
) -> Result<BTreeMap<u64, Vec<u64>>, String> {
    let (next_key, created_streams) = {
        let s = lock_state(state);
        (s.next_key, s.created_streams.clone())
    };
    let mut final_lists = BTreeMap::new();
    let keys: Vec<u64> = (0..next_key).filter(|k| !is_stream_key(*k) || created_streams.contains(k)).collect();
    for (i, chunk) in keys.chunks(64).enumerate() {
        let ops: Vec<ListOp> = chunk.iter().map(|k| ListOp::Read(*k, None)).collect();
        let process = FINAL_READ_PROCESS_BASE + i as u64;
        loop {
            record_event(state, sim, EventKind::Invoke, process, &ops);
            match run_one_shot_txn(client, &ops).await? {
                TxnOutcome::Ok(filled) => {
                    record_event(state, sim, EventKind::Ok, process, &filled);
                    for op in filled {
                        if let ListOp::Read(k, list) = op {
                            final_lists.insert(k, list.unwrap_or_default());
                        }
                    }
                    break;
                }
                _ => {
                    record_event(state, sim, EventKind::Fail, process, &ops);
                    client.pause(200).await;
                }
            }
        }
    }
    Ok(final_lists)
}

/// Verifies per-key prefix consistency, durability, read-your-writes, and real-time read bounds.
fn verify_history_properties(events: &[HistoryEvent], final_lists: &BTreeMap<u64, Vec<u64>>) -> Result<(), String> {
    let mut appended_key_by_value: BTreeMap<u64, u64> = BTreeMap::new();
    for e in events.iter().filter(|e| e.kind == EventKind::Invoke) {
        for op in &e.ops {
            if let ListOp::Append(k, v) = op {
                appended_key_by_value.insert(*v, *k);
            }
        }
    }
    for (k, list) in final_lists {
        let mut seen = BTreeSet::new();
        for v in list {
            if !seen.insert(*v) {
                return Err(format!("key {k} holds {v} twice: {list:?}"));
            }
            if appended_key_by_value.get(v) != Some(k) {
                return Err(format!("key {k} holds {v}, which was never appended to it: {list:?}"));
            }
        }
    }
    let empty = Vec::new();
    for e in events {
        let final_list = |k: &u64| final_lists.get(k).unwrap_or(&empty);
        for op in &e.ops {
            match (e.kind, op) {
                (EventKind::Ok, ListOp::Read(k, Some(list))) if !final_list(k).starts_with(list) => {
                    return Err(format!(
                        "process {} read key {k} as {list:?}, which is not a prefix of its final value {:?}",
                        e.process,
                        final_list(k)
                    ));
                }
                (EventKind::Ok, ListOp::Append(k, v)) if !final_list(k).contains(v) => {
                    return Err(format!(
                        "process {} appended {v} to key {k} and committed, but the final value {:?} lacks it",
                        e.process,
                        final_list(k)
                    ));
                }
                (EventKind::Fail, ListOp::Append(k, v)) if final_list(k).contains(v) => {
                    return Err(format!(
                        "process {} appended {v} to key {k} and failed, but the final value {:?} holds it",
                        e.process,
                        final_list(k)
                    ));
                }
                _ => {}
            }
        }
    }
    // Verify that a committed transaction reads its own earlier KV appends.
    for e in events.iter().filter(|e| e.kind == EventKind::Ok) {
        let mut own_appends: BTreeMap<u64, Vec<u64>> = BTreeMap::new();
        for op in &e.ops {
            match op {
                ListOp::Append(k, v) => own_appends.entry(*k).or_default().push(*v),
                ListOp::Read(k, list) => {
                    let expected_suffix = own_appends.get(k).map_or(&[][..], Vec::as_slice);
                    if !list.as_deref().unwrap_or(&[]).ends_with(expected_suffix) {
                        return Err(format!(
                            "process {} read key {k} as {list:?} after appending {expected_suffix:?} to it",
                            e.process
                        ));
                    }
                }
            }
        }
    }
    // Every read is a prefix of the final list, so the prefix length identifies which appends the
    // read observed. A transaction must observe at least the prefix length of every transaction
    // that completed before it began.
    let mut max_completed_len: BTreeMap<u64, usize> = BTreeMap::new();
    let mut min_len_at_invoke: BTreeMap<u64, BTreeMap<u64, usize>> = BTreeMap::new();
    for e in events {
        if e.kind == EventKind::Invoke {
            min_len_at_invoke.insert(
                e.process,
                e.ops.iter().map(|op| (op.key(), max_completed_len.get(&op.key()).copied().unwrap_or(0))).collect(),
            );
            continue;
        }
        let required_len = min_len_at_invoke.remove(&e.process).unwrap_or_default();
        if e.kind != EventKind::Ok {
            continue;
        }
        for op in &e.ops {
            let observed_len = match op {
                ListOp::Read(k, list) => {
                    let len = list.as_ref().map_or(0, Vec::len);
                    let need = required_len.get(k).copied().unwrap_or(0);
                    if len < need {
                        return Err(format!(
                            "process {} read key {k} as {list:?}, but a transaction that completed before it began saw \
                             {need} elements",
                            e.process
                        ));
                    }
                    len
                }
                ListOp::Append(k, v) => final_list_position(final_lists, *k, *v).map_or(0, |i| i + 1),
            };
            let entry = max_completed_len.entry(op.key()).or_default();
            *entry = (*entry).max(observed_len);
        }
    }
    Ok(())
}

/// A committed or ambiguous transaction node in the dependency graph.
struct CommittedTxn<'a> {
    invoke_index: usize,
    /// Index of the completion event, or `None` if the outcome was `:info`.
    complete_index: Option<usize>,
    ops: &'a [ListOp],
}

/// Builds the write-write, write-read, read-write, and real-time dependency graph from `events`
/// and `final_lists`, and returns an error if the graph contains a cycle. Call after
/// [`verify_history_properties`], which checks that every read is a prefix of its key's final
/// list.
fn verify_dependency_graph_acyclic(
    events: &[HistoryEvent],
    final_lists: &BTreeMap<u64, Vec<u64>>,
) -> Result<(), String> {
    let mut txns: Vec<CommittedTxn> = Vec::new();
    let mut active_by_process: BTreeMap<u64, usize> = BTreeMap::new();
    for (i, e) in events.iter().enumerate() {
        match e.kind {
            EventKind::Invoke => {
                active_by_process.insert(e.process, i);
            }
            EventKind::Ok => {
                let invoke_index = active_by_process.remove(&e.process).ok_or("completion without invocation")?;
                txns.push(CommittedTxn { invoke_index, complete_index: Some(i), ops: &e.ops });
            }
            EventKind::Info => {
                let invoke_index = active_by_process.remove(&e.process).ok_or("completion without invocation")?;
                txns.push(CommittedTxn { invoke_index, complete_index: None, ops: &e.ops });
            }
            EventKind::Fail => {
                active_by_process.remove(&e.process);
            }
        }
    }
    let mut writer_by_value: BTreeMap<u64, usize> = BTreeMap::new();
    for (txn_idx, txn) in txns.iter().enumerate() {
        for op in txn.ops {
            if let ListOp::Append(_, v) = op {
                writer_by_value.insert(*v, txn_idx);
            }
        }
    }
    // Represent real-time order with a chain of time nodes after the transaction nodes. Each
    // completion advances to a new time node, and each invocation depends on the latest time node,
    // creating a path from transaction A to transaction B if A completed before B began.
    let completion_count = txns.iter().filter(|t| t.complete_index.is_some()).count();
    let mut edges: Vec<Vec<usize>> = vec![Vec::new(); txns.len() + completion_count];
    let mut add_edge = |from: usize, to: usize| {
        if from != to {
            edges[from].push(to);
        }
    };
    for list in final_lists.values() {
        for pair in list.windows(2) {
            if let (Some(a), Some(b)) = (writer_by_value.get(&pair[0]), writer_by_value.get(&pair[1])) {
                add_edge(*a, *b);
            }
        }
    }
    for (txn_idx, txn) in txns.iter().enumerate() {
        if txn.complete_index.is_none() {
            continue;
        }
        for op in txn.ops {
            // A completed read of an absent KV key observed the initial empty version.
            let ListOp::Read(k, list) = op else { continue };
            let read_slice = list.as_deref().unwrap_or(&[]);
            let final_slice = final_lists.get(k).map_or(&[][..], Vec::as_slice);
            if let Some(writer) = read_slice.last().and_then(|v| writer_by_value.get(v)) {
                add_edge(*writer, txn_idx);
            }
            if let Some(next_writer) = final_slice.get(read_slice.len()).and_then(|v| writer_by_value.get(v)) {
                add_edge(txn_idx, *next_writer);
            }
        }
    }
    let mut timeline: Vec<(usize, bool, usize)> = Vec::new();
    for (txn_idx, txn) in txns.iter().enumerate() {
        timeline.push((txn.invoke_index, false, txn_idx));
        if let Some(done) = txn.complete_index {
            timeline.push((done, true, txn_idx));
        }
    }
    timeline.sort();
    let mut latest_time_node: Option<usize> = None;
    let mut next_time_node = txns.len();
    for (_, is_completion, txn_idx) in timeline {
        match (is_completion, latest_time_node) {
            (true, prev) => {
                add_edge(txn_idx, next_time_node);
                if let Some(t) = prev {
                    add_edge(t, next_time_node);
                }
                latest_time_node = Some(next_time_node);
                next_time_node += 1;
            }
            (false, Some(t)) => add_edge(t, txn_idx),
            (false, None) => {}
        }
    }
    match find_cycle(&edges) {
        None => Ok(()),
        Some(cycle) => {
            let steps: Vec<String> =
                cycle.iter().filter(|n| **n < txns.len()).map(|n| format!("{:?}", txns[*n].ops)).collect();
            Err(format!("dependency cycle among committed transactions: {}", steps.join(" -> ")))
        }
    }
}

/// Returns the node indices of a cycle in the directed graph `edges`, or `None` if `edges` is
/// acyclic.
fn find_cycle(edges: &[Vec<usize>]) -> Option<Vec<usize>> {
    #[derive(Clone, Copy, PartialEq)]
    enum VisitState {
        Unvisited,
        OnPath,
        Visited,
    }
    let mut state = vec![VisitState::Unvisited; edges.len()];
    for start in 0..edges.len() {
        if state[start] != VisitState::Unvisited {
            continue;
        }
        let mut path: Vec<(usize, std::slice::Iter<usize>)> = vec![(start, edges[start].iter())];
        state[start] = VisitState::OnPath;
        while let Some((node, neighbors)) = path.last_mut() {
            match neighbors.next() {
                Some(&next) if state[next] == VisitState::OnPath => {
                    let cycle_start = path.iter().position(|(p, _)| *p == next).expect("node on path");
                    return Some(path[cycle_start..].iter().map(|(p, _)| *p).collect());
                }
                Some(&next) if state[next] == VisitState::Unvisited => {
                    state[next] = VisitState::OnPath;
                    path.push((next, edges[next].iter()));
                }
                Some(_) => {}
                None => {
                    state[*node] = VisitState::Visited;
                    path.pop();
                }
            }
        }
    }
    None
}

/// Returns the index of `value` in the final list of `key`.
fn final_list_position(final_lists: &BTreeMap<u64, Vec<u64>>, key: u64, value: u64) -> Option<usize> {
    final_lists.get(&key)?.iter().position(|x| *x == value)
}

/// Parses an EDN history formatted by [`format_edn_history`] and verifies it with
/// [`verify_history_properties`] and [`verify_dependency_graph_acyclic`]. Transactions with
/// process IDs at or above `1_000_000` supply the final lists.
pub fn check_edn(text: &str) -> Result<usize, String> {
    let events: Vec<HistoryEvent> =
        text.lines().filter(|l| !l.trim().is_empty()).map(parse_edn_event).collect::<Result<_, _>>()?;
    let mut final_lists = BTreeMap::new();
    for e in events.iter().filter(|e| e.kind == EventKind::Ok && e.process >= FINAL_READ_PROCESS_BASE) {
        for op in &e.ops {
            if let ListOp::Read(k, list) = op {
                final_lists.insert(*k, list.clone().unwrap_or_default());
            }
        }
    }
    verify_history_properties(&events, &final_lists)?;
    verify_dependency_graph_acyclic(&events, &final_lists)?;
    Ok(events.len())
}

fn parse_edn_event(line: &str) -> Result<HistoryEvent, String> {
    let malformed = || format!("malformed history line: {line}");
    let field = |name: &str| -> Option<&str> {
        let rest = &line[line.find(name)? + name.len()..];
        Some(rest[..rest.find([',', '}'])?].trim())
    };
    let kind = match field(":type").ok_or_else(malformed)? {
        ":invoke" => EventKind::Invoke,
        ":ok" => EventKind::Ok,
        ":fail" => EventKind::Fail,
        ":info" => EventKind::Info,
        _ => return Err(malformed()),
    };
    let process = field(":process").and_then(|p| p.parse().ok()).ok_or_else(malformed)?;
    let value = &line[line.find(":value [").ok_or_else(malformed)? + ":value [".len()..];
    let mut ops = Vec::new();
    for part in value.split("[:").skip(1) {
        let words: Vec<&str> = part.split([' ', '[', ']']).filter(|w| !w.is_empty() && *w != "}").collect();
        let parse_u64 = |w: &str| w.parse::<u64>().map_err(|_| malformed());
        ops.push(match words.as_slice() {
            ["append", k, v] => ListOp::Append(parse_u64(k)?, parse_u64(v)?),
            ["r", k, "nil"] => ListOp::Read(parse_u64(k)?, None),
            ["r", k, list @ ..] => {
                ListOp::Read(parse_u64(k)?, Some(list.iter().map(|w| parse_u64(w)).collect::<Result<_, _>>()?))
            }
            _ => return Err(malformed()),
        });
    }
    Ok(HistoryEvent { kind, process, time_ms: 0, ops })
}

/// Formats `events` as an EDN history for Elle's `list-append` model.
fn format_edn_history(events: &[HistoryEvent]) -> String {
    let mut out = String::new();
    for (i, e) in events.iter().enumerate() {
        let ops: Vec<String> = e
            .ops
            .iter()
            .map(|op| match op {
                ListOp::Read(k, None) => format!("[:r {k} nil]"),
                ListOp::Read(k, Some(list)) => {
                    format!("[:r {k} [{}]]", list.iter().map(u64::to_string).collect::<Vec<_>>().join(" "))
                }
                ListOp::Append(k, v) => format!("[:append {k} {v}]"),
            })
            .collect();
        let _ = writeln!(
            out,
            "{{:index {i}, :type {}, :process {}, :time {}, :f :txn, :value [{}]}}",
            e.kind.edn_keyword(),
            e.process,
            e.time_ms * 1_000_000 + i as u64,
            ops.join(" ")
        );
    }
    out
}

pub async fn run(sim: Arc<Sim>, shape: Shape, chaos: bool) -> Result<(), String> {
    let owner = Client::signup(&sim, "elle", "elle@elle.test", "password-123").await?;
    owner.create_db(DB_NAME).await?;
    let token = owner.token.clone().unwrap_or_default();
    let state = Arc::new(Mutex::new(WorkloadState::new(shape.active_keys, shape.clients)));
    let phase = chaos.then(|| super::chaos_phase(&sim));
    let mut tasks = Vec::new();
    for ci in 0..shape.clients {
        let client = Client::with_token(&sim, &format!("elle{ci}"), &token);
        let (state, sim) = (state.clone(), sim.clone());
        tasks.push(tokio::spawn(async move {
            let mut rng = sim.fork_rng();
            let mut process = ci as u64;
            for _ in 0..shape.txns {
                client.pause(rng.below(shape.max_pause_ms + 1)).await;
                let is_interactive = rng.chance(shape.interactive);
                let ops = lock_state(&state).generate_txn(&mut rng, &shape, is_interactive);
                ensure_streams_exist(&client, &state, &ops).await?;
                record_event(&state, &sim, EventKind::Invoke, process, &ops);
                let outcome = match is_interactive {
                    true => run_interactive_txn(&client, &ops).await?,
                    false => run_one_shot_txn(&client, &ops).await?,
                };
                match outcome {
                    TxnOutcome::Ok(filled) => record_event(&state, &sim, EventKind::Ok, process, &filled),
                    TxnOutcome::Failed => record_event(&state, &sim, EventKind::Fail, process, &ops),
                    TxnOutcome::Unknown => {
                        record_event(&state, &sim, EventKind::Info, process, &ops);
                        // An ambiguous transaction can remain in flight, so allocate a new process ID.
                        let mut guard = lock_state(&state);
                        process = guard.next_process;
                        guard.next_process += 1;
                    }
                }
            }
            Ok::<(), String>(())
        }));
    }
    let mut first_err = None;
    for task in tasks {
        if let Err(e) = task.await.map_err(|e| e.to_string()).and_then(|r| r) {
            first_err = first_err.or(Some(e));
        }
    }
    if let Some(e) = first_err {
        return Err(e);
    }
    super::settle(&sim, phase).await;
    let final_lists = read_final_lists(&sim, &owner, &state).await?;
    let events = lock_state(&state).events.clone();
    if let Ok(dir) = std::env::var("SIM_ELLE_DIR") {
        let path = std::path::Path::new(&dir).join(format!("elle-{}.edn", sim.seed));
        std::fs::write(&path, format_edn_history(&events)).map_err(|e| format!("write {}: {e}", path.display()))?;
    }
    verify_history_properties(&events, &final_lists)
        .and_then(|()| verify_dependency_graph_acyclic(&events, &final_lists))
        .map_err(|e| format!("list-append history of {} events: {e}", events.len()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event_for_process_zero(kind: EventKind, ops: Vec<ListOp>) -> HistoryEvent {
        HistoryEvent { kind, process: 0, time_ms: 0, ops }
    }

    #[test]
    fn rejects_lost_aborted_and_divergent_appends() {
        let final_lists: BTreeMap<u64, Vec<u64>> = [(1, vec![1, 2])].into();
        let invoke = event_for_process_zero(
            EventKind::Invoke,
            vec![ListOp::Append(1, 1), ListOp::Append(1, 2), ListOp::Append(1, 3)],
        );
        let committed = |ops| vec![invoke.clone(), event_for_process_zero(EventKind::Ok, ops)];
        verify_history_properties(&committed(vec![ListOp::Append(1, 1), ListOp::Read(1, Some(vec![1]))]), &final_lists)
            .unwrap();
        let lost = verify_history_properties(&committed(vec![ListOp::Append(1, 3)]), &final_lists).unwrap_err();
        assert!(lost.contains("lacks it"), "{lost}");
        let divergent =
            verify_history_properties(&committed(vec![ListOp::Read(1, Some(vec![2]))]), &final_lists).unwrap_err();
        assert!(divergent.contains("not a prefix"), "{divergent}");
        let aborted = verify_history_properties(
            &[invoke.clone(), event_for_process_zero(EventKind::Fail, vec![ListOp::Append(1, 2)])],
            &final_lists,
        )
        .unwrap_err();
        assert!(aborted.contains("failed"), "{aborted}");
        let duplicate: BTreeMap<u64, Vec<u64>> = [(1, vec![1, 1])].into();
        assert!(verify_history_properties(std::slice::from_ref(&invoke), &duplicate).unwrap_err().contains("twice"));
        let unappended: BTreeMap<u64, Vec<u64>> = [(1, vec![9])].into();
        assert!(verify_history_properties(&[invoke], &unappended).unwrap_err().contains("never appended"));
    }

    #[test]
    fn rejects_reads_that_miss_earlier_transactions() {
        let final_lists: BTreeMap<u64, Vec<u64>> = [(1, vec![1, 2])].into();
        let event = |kind, process, ops| HistoryEvent { kind, process, time_ms: 0, ops };
        let append = vec![ListOp::Append(1, 1), ListOp::Append(1, 2)];
        let read = |list: Option<Vec<u64>>| vec![ListOp::Read(1, list)];
        let history = |second_read: Option<Vec<u64>>| {
            vec![
                event(EventKind::Invoke, 0, append.clone()),
                event(EventKind::Ok, 0, append.clone()),
                event(EventKind::Invoke, 1, read(None)),
                event(EventKind::Ok, 1, read(second_read)),
            ]
        };
        verify_history_properties(&history(Some(vec![1, 2])), &final_lists).unwrap();
        let stale = verify_history_properties(&history(Some(vec![1])), &final_lists).unwrap_err();
        assert!(stale.contains("completed before it began"), "{stale}");
        let mut concurrent = history(Some(vec![1]));
        concurrent.swap(1, 2);
        verify_history_properties(&concurrent, &final_lists).unwrap();
    }

    #[test]
    fn rejects_dependency_cycles() {
        let event = |kind, process, ops| HistoryEvent { kind, process, time_ms: 0, ops };
        // Write skew: each transaction reads the key that the other transaction appends to.
        let t1 = vec![ListOp::Read(2, None), ListOp::Append(3, 1)];
        let t2 = vec![ListOp::Read(3, None), ListOp::Append(2, 2)];
        let skew_history = |r1: Option<Vec<u64>>, r2: Option<Vec<u64>>| {
            vec![
                event(EventKind::Invoke, 0, t1.clone()),
                event(EventKind::Invoke, 1, t2.clone()),
                event(EventKind::Ok, 0, vec![ListOp::Read(2, r1), ListOp::Append(3, 1)]),
                event(EventKind::Ok, 1, vec![ListOp::Read(3, r2), ListOp::Append(2, 2)]),
            ]
        };
        let final_lists: BTreeMap<u64, Vec<u64>> = [(2, vec![2]), (3, vec![1])].into();
        let skewed = skew_history(Some(vec![]), Some(vec![]));
        verify_history_properties(&skewed, &final_lists).unwrap();
        assert!(verify_dependency_graph_acyclic(&skewed, &final_lists).unwrap_err().contains("dependency cycle"));
        // A KV read of an absent key returns `None` and observes the initial empty version.
        let absent = skew_history(None, None);
        verify_history_properties(&absent, &final_lists).unwrap();
        assert!(verify_dependency_graph_acyclic(&absent, &final_lists).unwrap_err().contains("dependency cycle"));
        verify_dependency_graph_acyclic(&skew_history(Some(vec![]), Some(vec![1])), &final_lists).unwrap();
        // Real-time cycle: the second transaction's append precedes the first transaction's append.
        let ordered = vec![
            event(EventKind::Invoke, 0, vec![ListOp::Append(4, 1)]),
            event(EventKind::Ok, 0, vec![ListOp::Append(4, 1)]),
            event(EventKind::Invoke, 1, vec![ListOp::Append(4, 2)]),
            event(EventKind::Ok, 1, vec![ListOp::Append(4, 2)]),
        ];
        verify_dependency_graph_acyclic(&ordered, &[(4, vec![1, 2])].into()).unwrap();
        assert!(
            verify_dependency_graph_acyclic(&ordered, &[(4, vec![2, 1])].into())
                .unwrap_err()
                .contains("dependency cycle")
        );
    }

    #[test]
    fn rejects_reads_that_miss_their_own_appends() {
        let final_lists: BTreeMap<u64, Vec<u64>> = [(2, vec![1, 5])].into();
        let txn = |read: Vec<u64>| {
            vec![
                event_for_process_zero(
                    EventKind::Invoke,
                    vec![ListOp::Append(2, 1), ListOp::Append(2, 5), ListOp::Read(2, None)],
                ),
                event_for_process_zero(
                    EventKind::Ok,
                    vec![ListOp::Append(2, 1), ListOp::Append(2, 5), ListOp::Read(2, Some(read))],
                ),
            ]
        };
        verify_history_properties(&txn(vec![1, 5]), &final_lists).unwrap();
        assert!(verify_history_properties(&txn(vec![1]), &final_lists).unwrap_err().contains("after appending"));
    }

    #[test]
    fn checks_edn_histories() {
        let history = format_edn_history(&[
            event_for_process_zero(EventKind::Invoke, vec![ListOp::Append(3, 4)]),
            event_for_process_zero(EventKind::Ok, vec![ListOp::Append(3, 4)]),
            HistoryEvent {
                kind: EventKind::Invoke,
                process: FINAL_READ_PROCESS_BASE,
                time_ms: 0,
                ops: vec![ListOp::Read(3, None)],
            },
            HistoryEvent {
                kind: EventKind::Ok,
                process: FINAL_READ_PROCESS_BASE,
                time_ms: 0,
                ops: vec![ListOp::Read(3, Some(vec![4]))],
            },
        ]);
        assert_eq!(check_edn(&history), Ok(4));
        let lost = history.replace("[:r 3 [4]]", "[:r 3 []]");
        assert!(check_edn(&lost).unwrap_err().contains("lacks it"));
        assert!(check_edn("{:index 0, :type :ok}").unwrap_err().contains("malformed"));
    }

    #[test]
    fn writes_elle_histories() {
        let history = format_edn_history(&[
            event_for_process_zero(EventKind::Invoke, vec![ListOp::Read(2, None), ListOp::Append(3, 4)]),
            event_for_process_zero(EventKind::Ok, vec![ListOp::Read(2, Some(vec![1, 5])), ListOp::Append(3, 4)]),
        ]);
        assert_eq!(
            history,
            "{:index 0, :type :invoke, :process 0, :time 0, :f :txn, :value [[:r 2 nil] [:append 3 4]]}\n\
             {:index 1, :type :ok, :process 0, :time 1, :f :txn, :value [[:r 2 [1 5]] [:append 3 4]]}\n"
        );
    }
}
