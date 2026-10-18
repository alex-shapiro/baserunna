//! `register`: concurrent writers and strong readers on a few keys, with a
//! real-time linearizability check over the recorded history.

use super::super::Sim;
use super::super::client::Client;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
struct Op {
    key: usize,
    start: u64,
    end: u64,
    /// For writes: the committed version if known.
    version: Option<u64>,
    value: String,
    write: bool,
    client: String,
}

pub async fn run(
    sim: Arc<Sim>,
    writers: usize,
    readers: usize,
    ops: usize,
    max_pause_ms: u64,
    chaos: bool,
) -> Result<(), String> {
    let owner = Client::signup(&sim, "reg", "reg@reg.test", "password-123").await?;
    let db = "reg";
    owner.create_db(db).await?;
    let token = owner.token.clone().unwrap_or_default();
    let keys = 2;
    let hist: Arc<Mutex<Vec<Op>>> = Arc::new(Mutex::new(Vec::new()));
    let chaos_h = chaos.then(|| super::spawn_chaos(&sim));
    let mut hs = Vec::new();
    for w in 0..writers + readers {
        let c = Client::with_token(&sim, &format!("{}{w}", if w < writers { "writer" } else { "reader" }), &token);
        let hist = hist.clone();
        let sim2 = sim.clone();
        let write = w < writers;
        hs.push(tokio::spawn(async move {
            for n in 0..ops {
                let d = c.rng().below(max_pause_ms);
                c.pause(d).await;
                let key = c.rng().usize(keys);
                let path = format!("/v1/dbs/{db}/kv/r{key}");
                let start = sim2.now_ms();
                if write {
                    let value = format!("{}-{n}", c.name);
                    let r = c.send("PUT", &path, &[], bytes::Bytes::from(value.clone())).await;
                    let end = sim2.now_ms();
                    let version = match &r {
                        Ok(r) if r.status == 200 => serde_json::from_slice::<serde_json::Value>(&r.body)
                            .ok()
                            .and_then(|j| j["version"].as_u64()),
                        _ => None,
                    };
                    hist.lock().unwrap_or_else(|e| e.into_inner()).push(Op {
                        key,
                        start,
                        end,
                        version,
                        value,
                        write: true,
                        client: c.name.clone(),
                    });
                } else {
                    // Consecutive reads through different nodes must not observe stale state.
                    let kinds = match c.rng().below(8) {
                        0 => vec![ReadKind::Get],
                        1 => vec![ReadKind::LocalGet],
                        2 => vec![ReadKind::ReadOnlyTxn],
                        3 => vec![ReadKind::ReadWriteTxn],
                        4 => vec![ReadKind::LocalGet, ReadKind::Get],
                        5 => vec![ReadKind::Interactive { write: false }],
                        6 => vec![ReadKind::LocalGet, ReadKind::Interactive { write: true }],
                        _ => vec![ReadKind::LocalGet, ReadKind::ReadOnlyTxn],
                    };
                    let mut start = start;
                    for kind in kinds {
                        let read = read_once(&c, db, key, kind).await;
                        let end = sim2.now_ms();
                        if let Some((version, value)) = read {
                            hist.lock().unwrap_or_else(|e| e.into_inner()).push(Op {
                                key,
                                start,
                                end,
                                version: Some(version),
                                value,
                                write: false,
                                client: c.name.clone(),
                            });
                        }
                        start = sim2.now_ms();
                    }
                }
            }
        }));
    }
    for h in hs {
        h.await.map_err(|e| e.to_string())?;
    }
    if let Some(h) = chaos_h {
        h.abort();
    }
    super::recover(&sim).await;
    check(&hist.lock().unwrap_or_else(|e| e.into_inner()))
}

#[derive(Clone, Copy)]
enum ReadKind {
    Get,
    LocalGet,
    ReadOnlyTxn,
    ReadWriteTxn,
    /// Read in `begin`, then commit, with a write or read-only. The read
    /// counts only once the commit succeeds.
    Interactive {
        write: bool,
    },
}

/// Reads `r{key}` via `kind` and returns `(version, value)` (or `(0, "")` if not found).
async fn read_once(c: &Client, db: &str, key: usize, kind: ReadKind) -> Option<(u64, String)> {
    match kind {
        ReadKind::Get | ReadKind::LocalGet => {
            let headers = match kind {
                ReadKind::LocalGet => vec![("x-baserunna-hops", "2".to_string())],
                _ => vec![],
            };
            let r = c.send("GET", &format!("/v1/dbs/{db}/kv/r{key}"), &headers, bytes::Bytes::new()).await.ok()?;
            match r.status {
                200 => {
                    let version = r.headers.get("x-baserunna-version")?.to_str().ok()?.parse().ok()?;
                    Some((version, String::from_utf8_lossy(&r.body).into_owned()))
                }
                404 => Some((0, String::new())),
                _ => None,
            }
        }
        ReadKind::Interactive { write } => {
            let get = serde_json::json!({"op": "get", "key": format!("r{key}")});
            let r = c
                .call("POST", &format!("/v1/dbs/{db}/transactions"), Some(serde_json::json!({"ops": [get]})))
                .await
                .ok()?;
            if r.status != 201 {
                return None;
            }
            let id = r.json["txn_id"].as_str()?;
            let ops = if write {
                vec![serde_json::json!({"op": "put", "key": format!("scratch-{}", c.name), "value": "x"})]
            } else {
                vec![]
            };
            let path = format!("/v1/dbs/{db}/transactions/{id}/commit");
            let committed = c.call("POST", &path, Some(serde_json::json!({ "ops": ops }))).await.ok()?;
            if committed.status != 200 {
                return None;
            }
            let got = &r.json["results"][0];
            match got["found"].as_bool()? {
                true => Some((got["version"].as_u64()?, got["value"].as_str()?.to_string())),
                false => Some((0, String::new())),
            }
        }
        ReadKind::ReadOnlyTxn | ReadKind::ReadWriteTxn => {
            let mut ops = vec![serde_json::json!({"op": "get", "key": format!("r{key}")})];
            if matches!(kind, ReadKind::ReadWriteTxn) {
                ops.push(serde_json::json!({"op": "put", "key": format!("scratch-{}", c.name), "value": "x"}));
            }
            let r = c.call("POST", &format!("/v1/dbs/{db}/txn"), Some(serde_json::json!({ "ops": ops }))).await.ok()?;
            if r.status != 200 {
                return None;
            }
            let got = &r.json["results"][0];
            match got["found"].as_bool()? {
                true => Some((got["version"].as_u64()?, got["value"].as_str()?.to_string())),
                false => Some((0, String::new())),
            }
        }
    }
}

fn check(h: &[Op]) -> Result<(), String> {
    let mut by_version: BTreeMap<(usize, u64), &str> = BTreeMap::new();
    let mut written: BTreeMap<usize, Vec<&str>> = BTreeMap::new();
    for w in h.iter().filter(|o| o.write) {
        written.entry(w.key).or_default().push(&w.value);
        if let Some(v) = w.version
            && let Some(prev) = by_version.insert((w.key, v), &w.value)
        {
            return Err(format!("two writes acknowledged with version {v}: {prev} and {}", w.value));
        }
    }
    let reads: Vec<&Op> = h.iter().filter(|o| !o.write).collect();
    for r in &reads {
        let rv = r.version.unwrap_or(0);
        if rv > 0 {
            match by_version.get(&(r.key, rv)) {
                Some(v) if *v != r.value => {
                    return Err(format!(
                        "read of r{} at version {rv} returned {:?}, but that version wrote {v:?}",
                        r.key, r.value
                    ));
                }
                None if !written.get(&r.key).is_some_and(|ws| ws.contains(&r.value.as_str())) => {
                    return Err(format!("read returned a value never written: {:?}", r.value));
                }
                _ => {}
            }
        }
        // Writes completed before the read started must be visible.
        for w in h.iter().filter(|o| o.write && o.key == r.key) {
            if let Some(wv) = w.version
                && w.end < r.start
                && rv < wv
            {
                return Err(format!(
                    "stale read: {} read r{} version {rv} at [{}, {}] after {}'s write version {wv} completed at {}",
                    r.client, r.key, r.start, r.end, w.client, w.end
                ));
            }
        }
        for r2 in &reads {
            if r2.key == r.key && r2.end < r.start && r2.version.unwrap_or(0) > rv {
                return Err(format!(
                    "non-monotonic reads on r{}: version {} completed at {} then version {rv} started at {}",
                    r.key,
                    r2.version.unwrap_or(0),
                    r2.end,
                    r.start
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn w(key: usize, start: u64, end: u64, version: u64, value: &str) -> Op {
        Op { key, start, end, version: Some(version), value: value.into(), write: true, client: "w".into() }
    }

    fn r(key: usize, start: u64, end: u64, version: u64, value: &str) -> Op {
        Op { key, start, end, version: Some(version), value: value.into(), write: false, client: "r".into() }
    }

    #[test]
    fn accepts_linearizable_histories() {
        let h =
            [w(0, 0, 10, 1, "a"), r(0, 5, 8, 0, ""), r(0, 20, 30, 1, "a"), w(0, 25, 40, 2, "b"), r(0, 30, 35, 1, "a")];
        check(&h).unwrap();
        check(&[w(0, 0, 100, 1, "a"), r(0, 10, 20, 1, "a"), r(1, 30, 40, 0, "")]).unwrap();
        // Equal timestamps do not order operations: a read starting when a
        // write or another read ends is concurrent with it.
        check(&[w(0, 0, 10, 1, "a"), r(0, 10, 20, 0, "")]).unwrap();
        check(&[w(0, 0, 100, 1, "a"), r(0, 5, 10, 1, "a"), r(0, 10, 20, 0, "")]).unwrap();
    }

    #[test]
    fn rejects_violations() {
        let stale = [w(0, 0, 10, 1, "a"), w(0, 11, 15, 2, "b"), r(0, 20, 30, 1, "a")];
        assert!(check(&stale).unwrap_err().contains("stale read"));
        let missing = [w(0, 0, 10, 1, "a"), r(0, 20, 30, 0, "")];
        assert!(check(&missing).unwrap_err().contains("stale read"));
        let backwards = [w(0, 0, 5, 1, "a"), w(0, 0, 100, 2, "b"), r(0, 20, 30, 2, "b"), r(0, 40, 50, 1, "a")];
        assert!(check(&backwards).unwrap_err().contains("non-monotonic"));
        let duplicate = [w(0, 0, 10, 1, "a"), w(0, 0, 10, 1, "b")];
        assert!(check(&duplicate).unwrap_err().contains("two writes"));
        let wrong = [w(0, 0, 10, 1, "a"), r(0, 20, 30, 1, "b")];
        assert!(check(&wrong).unwrap_err().contains("that version wrote"));
        let invented = [w(0, 0, 10, 1, "a"), r(0, 20, 30, 7, "zzz")];
        assert!(check(&invented).unwrap_err().contains("never written"));
        let other_key = [w(1, 0, 10, 1, "a"), r(0, 20, 30, 0, "")];
        check(&other_key).unwrap();
    }
}
