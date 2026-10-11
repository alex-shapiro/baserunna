//! `blobs`: large values stored out-of-band through every path (raw PUT,
//! staged upload + transactional bind, blob table columns), verified with
//! full and ranged reads; then a full GC cycle must remove every
//! unreferenced object and keep every referenced one.

use super::super::Sim;
use super::super::client::{Client, ZOMBIE_WAIT_MS};
use crate::engine::manifest::{Manifest, ObjKind};
use crate::engine::{Value, segment};
use crate::util::{b64, sha256};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

type IdSets = (BTreeSet<String>, BTreeSet<String>, BTreeSet<String>);

/// Referenced blob ids, listed-obsolete ids, and live segment ids of a
/// database, read straight from the store.
pub fn db_objects(sim: &Sim, db_id: &str) -> Result<IdSets, String> {
    let g = sim.mem.get_sync(&crate::engine::manifest_key(db_id), &Default::default()).map_err(|e| e.to_string())?;
    let m = Manifest::decode(&g.data).map_err(|e| e.to_string())?;
    let mut entries = m.tail.clone();
    let mut segs = BTreeSet::new();
    for s in &m.segments {
        let d =
            sim.mem.get_sync(&crate::engine::seg_key(db_id, &s.id), &Default::default()).map_err(|e| e.to_string())?;
        entries.extend(segment::read_all(&d.data).map_err(|e| e.to_string())?);
        segs.insert(s.id.clone());
    }
    // Newest version per key wins.
    let mut newest: BTreeMap<bytes::Bytes, (u64, Value)> = BTreeMap::new();
    for e in entries {
        let keep = newest.get(&e.key).is_none_or(|(s, _)| e.seq > *s);
        if keep {
            newest.insert(e.key.clone(), (e.seq, e.value));
        }
    }
    let referenced = newest
        .values()
        .filter_map(|(_, v)| match v {
            Value::Blob(b) => Some(b.id.clone()),
            _ => None,
        })
        .collect();
    let listed = m
        .obsolete
        .iter()
        .map(|o| format!("{}:{}", if o.kind == ObjKind::Blob { "blob" } else { "seg" }, o.id))
        .collect();
    Ok((referenced, listed, segs))
}

fn stored_ids(sim: &Sim, db_id: &str, kind: &str) -> BTreeSet<String> {
    let prefix = format!("dbs/{db_id}/{kind}/");
    sim.mem
        .keys()
        .into_iter()
        .filter_map(|k| k.strip_prefix(&prefix).map(|r| r.split('/').next().unwrap_or("").to_string()))
        .collect()
}

pub async fn run(sim: Arc<Sim>, rounds: usize, chaos: bool) -> Result<(), String> {
    let c = Client::signup(&sim, "blobs", "b@blobs.test", "password-123").await?;
    c.create_db("media").await?;
    let base = "/v1/dbs/media";
    let r = c
        .call_retry(
            "POST",
            &format!("{base}/tables"),
            Some(json!({"name": "assets", "if_not_exists": true, "primary_key": ["id"], "columns": [
                {"name": "id", "type": "string", "nullable": false},
                {"name": "data", "type": "blob"}
            ]})),
        )
        .await;
    if r.status != 201 {
        return Err(format!("create table: {}", r.json));
    }
    let chaos_h = chaos.then(|| super::spawn_chaos(&sim));
    let mut kv: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    let mut rows: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    let mut rng = sim.fork_rng();
    for round in 0..rounds {
        let size = match rng.below(4) {
            0 => rng.usize(400),
            1 => 1024 * (1 + rng.usize(3)),
            _ => rng.usize(6000),
        };
        let data = rng.bytes(size);
        let key = format!("f{}", rng.below(6));
        match rng.below(5) {
            0 | 1 => {
                // Raw PUT: same content on retry, so retries are safe.
                let path = format!("{base}/kv/{key}");
                loop {
                    match c
                        .send("PUT", &path, &[("content-type", "image/png".into())], bytes::Bytes::from(data.clone()))
                        .await
                    {
                        Ok(r) if r.status == 200 => break,
                        Ok(r) if r.status == 503 && !String::from_utf8_lossy(&r.body).contains("\"unknown\"") => {}
                        Ok(r) if r.status != 503 => return Err(format!("put blob: {}", r.status)),
                        _ => c.pause(ZOMBIE_WAIT_MS).await,
                    }
                }
                kv.insert(key, data);
            }
            2 => {
                // Stage, then bind in a transaction.
                let staged = loop {
                    match c
                        .send(
                            "POST",
                            &format!("{base}/blobs"),
                            &[("content-type", "application/pdf".into())],
                            bytes::Bytes::from(data.clone()),
                        )
                        .await
                    {
                        Ok(r) if r.status == 201 => {
                            break serde_json::from_slice::<serde_json::Value>(&r.body).map_err(|e| e.to_string())?;
                        }
                        _ => c.pause(200).await,
                    }
                };
                if staged["size"] != json!(size) || staged["sha256"] != json!(b64::hex(&sha256(&data))) {
                    return Err(format!("staged metadata wrong: {staged}"));
                }
                let id = staged["blob_id"].as_str().unwrap_or_default();
                let body = json!({"ops": [{"op": "put", "key": key, "blob_id": id}], "idempotency_key": format!("bind-{round}")});
                let r = c.call_settled("POST", &format!("{base}/txn"), Some(body)).await;
                if r.status != 200 {
                    return Err(format!("bind: {} {}", r.status, r.json));
                }
                kv.insert(key, data);
            }
            3 => {
                let id = format!("asset{}", rng.below(4));
                let staged = loop {
                    match c.send("POST", &format!("{base}/blobs"), &[], bytes::Bytes::from(data.clone())).await {
                        Ok(r) if r.status == 201 => {
                            break serde_json::from_slice::<serde_json::Value>(&r.body).map_err(|e| e.to_string())?;
                        }
                        _ => c.pause(200).await,
                    }
                };
                let body = json!({"row": {"id": id, "data": staged["blob_id"]}, "upsert": true});
                let r = c.call_settled("POST", &format!("{base}/tables/assets/rows"), Some(body)).await;
                if r.status != 200 {
                    return Err(format!("row upsert: {} {}", r.status, r.json));
                }
                rows.insert(id, data);
            }
            _ => {
                if rng.chance(0.5) {
                    let r = c.call_settled("DELETE", &format!("{base}/kv/{key}"), None).await;
                    if r.status != 200 {
                        return Err(format!("delete: {}", r.json));
                    }
                    kv.remove(&key);
                } else {
                    let id = format!("asset{}", rng.below(4));
                    let r = c
                        .call_settled("POST", &format!("{base}/tables/assets/rows/delete"), Some(json!({"pk": id})))
                        .await;
                    if r.status != 200 {
                        return Err(format!("row delete: {}", r.json));
                    }
                    rows.remove(&id);
                }
            }
        }
        verify(&c, base, &kv, &rows, &mut rng).await?;
    }
    if let Some(h) = chaos_h {
        h.abort();
    }
    super::recover(&sim).await;
    verify(&c, base, &kv, &rows, &mut rng).await?;

    // Full GC cycle.
    let db_id = c.call_retry("GET", base, None).await.json["id"].as_str().unwrap_or_default().to_string();
    let e = &sim.cfg.engine;
    let touch = |n: u64| {
        let c = &c;
        async move {
            let body = json!({"ops": [{"op": "put", "key": "gc-touch", "value": n.to_string()}], "idempotency_key": format!("touch-{n}")});
            c.call_settled("POST", "/v1/dbs/media/txn", Some(body)).await;
        }
    };
    // GC completeness is checked with store faults off: under faults a sweep
    // may fail and is retried on a later cycle (safety, i.e. never deleting
    // referenced data, is checked under faults throughout).
    sim.store.set_faults_enabled(false);
    let (referenced0, _, _) = db_objects(&sim, &db_id)?;
    let garbage_before = stored_ids(&sim, &db_id, "blob").difference(&referenced0).count();
    sim.sleep(e.orphan_grace_ms.max(e.blob_stage_ttl_ms) + e.sweep_interval_ms + 1_000).await;
    touch(1).await; // triggers the orphan sweep
    sim.sleep(10_000).await;
    sim.sleep(e.gc_grace_ms + 1_000).await;
    touch(2).await; // deletes expired obsolete objects
    sim.sleep(10_000).await;
    let (referenced, listed, segs) = db_objects(&sim, &db_id)?;
    let stored_blobs = stored_ids(&sim, &db_id, "blob");
    for b in &stored_blobs {
        if !referenced.contains(b) && !listed.contains(&format!("blob:{b}")) {
            return Err(format!("garbage blob {b} survived a full GC cycle"));
        }
    }
    for b in &referenced {
        if !stored_blobs.contains(b) {
            return Err(format!("referenced blob {b} was deleted"));
        }
    }
    let garbage_after = stored_blobs.difference(&referenced).count();
    if std::env::var("SIM_VERBOSE").is_ok() {
        eprintln!(
            "blob gc: garbage blobs before={garbage_before} after={garbage_after} referenced={}",
            referenced.len()
        );
    }
    for s in stored_ids(&sim, &db_id, "seg") {
        if !segs.contains(&s) && !listed.contains(&format!("seg:{s}")) {
            return Err(format!("garbage segment {s} survived a full GC cycle"));
        }
    }
    verify(&c, base, &kv, &rows, &mut rng).await
}

async fn verify(
    c: &Client,
    base: &str,
    kv: &BTreeMap<String, Vec<u8>>,
    rows: &BTreeMap<String, Vec<u8>>,
    rng: &mut crate::util::rng::Rng,
) -> Result<(), String> {
    for (k, v) in kv {
        let path = format!("{base}/kv/{k}");
        let full = loop {
            match c.send("GET", &path, &[], bytes::Bytes::new()).await {
                Ok(r) if r.status == 200 => break r,
                Ok(r) if r.status != 503 => return Err(format!("get {k}: {}", r.status)),
                _ => c.pause(200).await,
            }
        };
        if full.body.as_ref() != v.as_slice() {
            return Err(format!("blob {k}: content mismatch ({} vs {} bytes)", full.body.len(), v.len()));
        }
        if !v.is_empty() {
            let a = rng.usize(v.len());
            let b = a + rng.usize(v.len() - a);
            let part = loop {
                match c.send("GET", &path, &[("range", format!("bytes={a}-{b}"))], bytes::Bytes::new()).await {
                    Ok(r) if r.status == 206 => break r,
                    Ok(r) if r.status != 503 => return Err(format!("range get {k}: {}", r.status)),
                    _ => c.pause(200).await,
                }
            };
            if part.body.as_ref() != &v[a..=b] {
                return Err(format!("blob {k}: range {a}-{b} mismatch"));
            }
        }
    }
    let r = c.call_retry("POST", &format!("{base}/tables/assets/rows/query"), Some(json!({"limit": 100}))).await;
    let got = r.json["rows"].as_array().cloned().unwrap_or_default();
    if got.len() != rows.len() {
        return Err(format!("asset rows: {} vs {}", got.len(), rows.len()));
    }
    for row in got {
        let id = row["id"].as_str().unwrap_or_default();
        let want = rows.get(id).ok_or("unknown row")?;
        if row["data"]["size"] != json!(want.len()) || row["data"]["sha256"] != json!(b64::hex(&sha256(want))) {
            return Err(format!("row {id} blob metadata mismatch: {}", row["data"]));
        }
        let blob_id = row["data"]["blob_id"].as_str().unwrap_or_default();
        let r = loop {
            match c.send("GET", &format!("{base}/blobs/{blob_id}"), &[], bytes::Bytes::new()).await {
                Ok(r) if r.status == 200 => break r,
                Ok(r) if r.status != 503 => return Err(format!("get blob {blob_id}: {}", r.status)),
                _ => c.pause(200).await,
            }
        };
        if r.body.as_ref() != want.as_slice() {
            return Err(format!("row {id} blob content mismatch"));
        }
    }
    Ok(())
}
