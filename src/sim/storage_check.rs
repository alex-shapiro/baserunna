//! Storage invariants checked after every simulation run.

use super::Sim;
use crate::engine::manifest::{Levels, Manifest};
use crate::engine::range_delete::RangeDeletes;
use crate::engine::{Value, segment};
use bytes::Bytes;
use std::collections::BTreeSet;

impl Sim {
    /// Verifies quiescence storage invariants: manifests decode, referenced levels
    /// and segments exist and decode, and all live blob chunks are present.
    pub async fn check_storage(&self) -> Result<StorageReport, String> {
        let keys: BTreeSet<String> = self.mem.keys().into_iter().collect();
        let mut rep = StorageReport::default();
        let chunk = self.cfg.engine.blob_chunk_size as u64;
        for k in keys.iter().filter(|k| k.ends_with("/manifest")) {
            let db = k
                .strip_prefix("dbs/")
                .and_then(|s| s.strip_suffix("/manifest"))
                .ok_or_else(|| format!("odd manifest key {k}"))?;
            let g = self.mem.get_sync(k, &Default::default()).map_err(|e| e.to_string())?;
            let mut m = Manifest::decode(&g.data).map_err(|e| format!("{k}: {e}"))?;
            rep.databases += 1;
            if m.is_deleted() {
                continue;
            }
            if let Some(id) = m.levels.clone() {
                let lk = crate::engine::levels_key(db, &id);
                let l = self
                    .mem
                    .get_sync(&lk, &Default::default())
                    .map_err(|e| format!("db {db}: levels object {id} missing: {e}"))?;
                let l = Levels::decode(&l.data).map_err(|e| format!("{lk}: {e}"))?;
                m.attach_levels(&l).map_err(|e| format!("{lk}: {e}"))?;
                rep.levels_objects += 1;
            }
            let mut entries = m.tail.clone();
            for s in &m.segments {
                let sk = crate::engine::seg_key(db, &s.id);
                let d = self
                    .mem
                    .get_sync(&sk, &Default::default())
                    .map_err(|e| format!("db {db}: referenced segment {} missing: {e}", s.id))?;
                let es = segment::read_all(&d.data).map_err(|e| format!("segment {sk}: {e}"))?;
                if es.len() as u64 != s.entries {
                    return Err(format!("segment {sk}: entry count mismatch"));
                }
                rep.segments += 1;
                entries.extend(es);
            }
            // Only the newest, non-deleted entry per key is live; shadowed or deleted
            // blob references may have been garbage collected.
            let deletes = RangeDeletes::new(&m.range_deletes);
            let mut newest: std::collections::BTreeMap<Bytes, &crate::engine::Entry> =
                std::collections::BTreeMap::new();
            for e in &entries {
                if newest.get(&e.key).is_none_or(|x| e.seq > x.seq) {
                    newest.insert(e.key.clone(), e);
                }
            }
            for e in newest.values().filter(|e| deletes.seq_at(&e.key) <= e.seq) {
                if let Value::Blob(b) = &e.value {
                    let n = b.size.div_ceil(chunk);
                    for c in 0..n {
                        let ck = crate::engine::blob_chunk_key(db, &b.id, c);
                        if !keys.contains(&ck) {
                            return Err(format!("db {db}: blob {} chunk {c} missing", b.id));
                        }
                    }
                    rep.blobs += 1;
                }
            }
        }
        rep.objects = keys.len();
        Ok(rep)
    }
}

#[derive(Debug, Default, Clone)]
pub struct StorageReport {
    pub databases: usize,
    pub segments: usize,
    pub levels_objects: usize,
    pub blobs: usize,
    pub objects: usize,
}
