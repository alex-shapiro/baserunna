//! `register`: concurrent writers and strong readers on a few keys, with a
//! real-time linearizability check over the recorded history (virtual
//! time is global, so "completed before started" is exact).

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

pub async fn run(sim: Arc<Sim>, writers: usize, readers: usize, ops: usize, chaos: bool) -> Result<(), String> {
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
                let d = c.rng().below(500);
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
                    let r = c.send("GET", &path, &[], bytes::Bytes::new()).await;
                    let end = sim2.now_ms();
                    if let Ok(r) = r {
                        if r.status == 200 {
                            let version = r
                                .headers
                                .get("x-baserunna-version")
                                .and_then(|v| v.to_str().ok())
                                .and_then(|v| v.parse().ok());
                            hist.lock().unwrap_or_else(|e| e.into_inner()).push(Op {
                                key,
                                start,
                                end,
                                version,
                                value: String::from_utf8_lossy(&r.body).into_owned(),
                                write: false,
                                client: c.name.clone(),
                            });
                        } else if r.status == 404 {
                            hist.lock().unwrap_or_else(|e| e.into_inner()).push(Op {
                                key,
                                start,
                                end,
                                version: Some(0),
                                value: String::new(),
                                write: false,
                                client: c.name.clone(),
                            });
                        }
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

fn check(h: &[Op]) -> Result<(), String> {
    // Value written at each known version, and all values ever written.
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
        // Real time: writes completed before the read started are visible.
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
        // Reads never go back in time.
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
        // Concurrent reads may see either side of a concurrent write.
        check(&[w(0, 0, 100, 1, "a"), r(0, 10, 20, 1, "a"), r(1, 30, 40, 0, "")]).unwrap();
        // Equal timestamps do not order operations: a read starting the
        // moment a write (or another read) ends is concurrent with it.
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
        // Keys are independent.
        let other_key = [w(1, 0, 10, 1, "a"), r(0, 20, 30, 0, "")];
        check(&other_key).unwrap();
    }
}
