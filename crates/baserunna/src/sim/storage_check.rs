//! Storage invariants checked after every simulation run.

use super::Sim;
use crate::engine::manifest::{Levels, Manifest};
use crate::engine::range_delete::RangeDeletes;
use crate::engine::stream::{self, Stored};
use crate::engine::{Value, segment};
use bytes::Bytes;
use std::collections::BTreeSet;

impl Sim {
    /// Verifies quiescence storage invariants across all databases.
    pub async fn check_storage(&self) -> Result<StorageReport, String> {
        let keys: BTreeSet<String> = self.mem.keys().into_iter().collect();
        let mut rep = StorageReport::default();
        let chunk = self.cfg.engine.blob_chunk_size as u64;
        for k in keys.iter().filter(|k| k.ends_with("/manifest")) {
            let db = k
                .strip_prefix("dbs/")
                .and_then(|s| s.strip_suffix("/manifest"))
                .ok_or_else(|| format!("invalid manifest key {k}"))?;
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
            // Shadowed or deleted blob references may be garbage-collected.
            let deletes = RangeDeletes::new(&m.range_deletes);
            let mut newest: std::collections::BTreeMap<Bytes, &crate::engine::Entry> =
                std::collections::BTreeMap::new();
            for e in &entries {
                if newest.get(&e.key).is_none_or(|x| e.seq > x.seq) {
                    newest.insert(e.key.clone(), e);
                }
            }
            let mut positions: std::collections::BTreeMap<(u32, crate::engine::events::Pos), String> =
                Default::default();
            let mut part_bytes = 0u64;
            let mut referenced_parts: BTreeSet<String> = BTreeSet::new();
            let live = || newest.values().filter(|e| deletes.seq_at(&e.key) <= e.seq && !e.value.is_tombstone());
            let schemas: std::collections::BTreeMap<u32, String> = live()
                .filter(|e| e.key.starts_with(crate::events::SCHEMAS))
                .filter_map(|e| match &e.value {
                    crate::engine::Value::Inline(b) => {
                        serde_json::from_slice::<crate::events::schema::EventSchema>(b).ok()
                    }
                    _ => None,
                })
                .map(|s| (s.id, s.name))
                .collect();
            let names: std::collections::BTreeMap<u32, String> = live()
                .filter_map(|e| {
                    let id = u32::from_be_bytes(e.key.strip_prefix(crate::events::NAMES)?.try_into().ok()?);
                    let crate::engine::Value::Inline(b) = &e.value else { return None };
                    Some((id, String::from_utf8_lossy(b).into_owned()))
                })
                .collect();
            if names != schemas {
                return Err(format!("db {db}: event table names by id {names:?} do not match schemas {schemas:?}"));
            }
            let tables: std::collections::BTreeSet<u32> = schemas.into_keys().collect();
            let mut states: std::collections::BTreeMap<u32, crate::engine::events::TableState> = Default::default();
            let mut counted: std::collections::BTreeMap<u32, crate::engine::events::TableState> = Default::default();
            for e in live() {
                let table = if let Some((t, _)) = crate::engine::events::parse_row_key(&e.key) {
                    let crate::engine::Value::Inline(row) = &e.value else {
                        return Err(format!("db {db}: unsealed row {:?} is not inline", e.key));
                    };
                    let c = counted.entry(t).or_default();
                    c.unsealed_rows += 1;
                    c.unsealed_bytes += (e.key.len() + row.len()) as u64;
                    t
                } else if let Some((t, st)) = crate::engine::events::state_of(e) {
                    states.insert(t, st);
                    t
                } else if let Some(r) = crate::engine::events::ref_of(e) {
                    counted.entry(r.table).or_default().part_bytes += r.stored_bytes();
                    r.table
                } else {
                    continue;
                };
                if !tables.contains(&table) {
                    return Err(format!(
                        "db {db}: {:?} belongs to event table {table}, which is missing from the catalog",
                        e.key
                    ));
                }
            }
            for t in &tables {
                let st = states.get(t).ok_or_else(|| format!("db {db}: event table {t} is missing state"))?;
                let want = counted.get(t).copied().unwrap_or_default();
                if *st != want {
                    return Err(format!("db {db}: event table {t} state {st:?} does not match counted state {want:?}"));
                }
            }
            let tables_part_bytes: u64 = states.values().map(|s| s.part_bytes).sum();
            if tables_part_bytes != m.part_bytes {
                return Err(format!(
                    "db {db}: event table part_bytes sum {tables_part_bytes} does not match manifest part_bytes {}",
                    m.part_bytes
                ));
            }
            for e in live() {
                let place = if let Some((table, pos)) = crate::engine::events::parse_row_key(&e.key) {
                    vec![((table, pos), "unsealed row".to_string())]
                } else if e.key.first() == Some(&crate::engine::events::PARTS) {
                    let r =
                        crate::engine::events::ref_of(e).ok_or_else(|| format!("db {db}: corrupt part reference"))?;
                    part_bytes += r.stored_bytes();
                    referenced_parts.insert(r.id.clone());
                    referenced_parts.extend(r.mask_object().map(str::to_string));
                    for id in std::iter::once(r.id.as_str()).chain(r.mask_object()) {
                        if m.obsolete.iter().any(|o| o.id == id) {
                            return Err(format!("db {db}: part {} references obsolete object {id}", r.id));
                        }
                    }
                    let live = check_part(&self.mem, db, &r)?;
                    rep.parts += 1;
                    live.into_iter().map(|p| ((r.table, p), format!("part {}", r.id))).collect()
                } else {
                    Vec::new()
                };
                for (k, where_) in place {
                    if let Some(other) = positions.insert(k, where_.clone()) {
                        return Err(format!(
                            "db {db}: event table {} row {:?} is in {other} and in {where_}",
                            k.0, k.1
                        ));
                    }
                }
            }
            let prefix = format!("dbs/{db}/part/");
            let mut stored: std::collections::BTreeMap<&str, u64> = Default::default();
            for k in keys.range(prefix.clone()..).take_while(|k| k.starts_with(&prefix)) {
                let id = k[prefix.len()..].split('/').next().unwrap_or_default();
                let g = self.mem.get_sync(k, &Default::default()).map_err(|e| format!("{k}: {e}"))?;
                let t = stored.entry(id).or_default();
                *t = (*t).max(g.last_modified_ms);
            }
            for (id, mtime) in stored {
                if referenced_parts.contains(id) {
                    continue;
                }
                match m.obsolete.iter().find(|o| o.id == id) {
                    Some(o) => rep.obsolete_parts.push(UnreferencedPart {
                        db: db.to_string(),
                        id: id.into(),
                        at_ms: o.since_ms,
                    }),
                    None => rep.orphan_parts.push(UnreferencedPart { db: db.to_string(), id: id.into(), at_ms: mtime }),
                }
            }
            if part_bytes != m.part_bytes {
                return Err(format!(
                    "db {db}: referenced part_bytes {part_bytes} does not match manifest part_bytes {}",
                    m.part_bytes
                ));
            }
            let mut covered: std::collections::BTreeMap<u64, u64> = std::collections::BTreeMap::new();
            for e in newest.values().filter(|e| deletes.seq_at(&e.key) <= e.seq && !e.value.is_tombstone()) {
                if let Some((id, s)) = stream::stored(e) {
                    let (lo, hi) = match &s {
                        Stored::Record(r) => (r.seq, r.seq),
                        Stored::Chunk(c) => {
                            check_chunk(&self.mem, db, id, c)?;
                            rep.chunks += 1;
                            (c.first_seq, c.last_seq)
                        }
                    };
                    if covered.get(&id).is_some_and(|end| *end >= lo) {
                        return Err(format!("db {db}: stream {id} stores seq {lo} twice"));
                    }
                    covered.insert(id, hi);
                }
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

fn check_part(
    mem: &crate::store::MemStore,
    db: &str,
    r: &crate::engine::events::PartRef,
) -> Result<Vec<crate::engine::events::Pos>, String> {
    let mut stream = Vec::with_capacity(r.size as usize);
    for i in 0..r.chunks() {
        let k = crate::engine::part_chunk_key(db, &r.id, i);
        let g = mem.get_sync(&k, &Default::default()).map_err(|e| format!("db {db}: part chunk {k} missing: {e}"))?;
        stream.extend_from_slice(&g.data);
    }
    let (_, pos) = crate::events::part::inspect(&Bytes::from(stream), r).map_err(|e| format!("db {db}: {e}"))?;
    let mask = match &r.mask {
        None => crate::engine::events::Runs::default(),
        Some(crate::engine::events::Mask::Inline(m)) => m.clone(),
        Some(crate::engine::events::Mask::Object { id, .. }) => {
            let k = crate::engine::part_chunk_key(db, id, 0);
            let g = mem.get_sync(&k, &Default::default()).map_err(|e| format!("db {db}: mask {k} missing: {e}"))?;
            crate::engine::events::decode_mask_object(&g.data).map_err(|e| format!("db {db}: mask {k}: {e}"))?
        }
    };
    if mask.count() != r.deleted {
        return Err(format!(
            "db {db}: part {} mask deletes {} rows, reference records {}",
            r.id,
            mask.count(),
            r.deleted
        ));
    }
    Ok(pos.into_iter().enumerate().filter(|(i, _)| !mask.contains(*i as u32)).map(|(_, p)| p).collect())
}

fn check_chunk(mem: &crate::store::MemStore, db: &str, id: u64, c: &stream::ChunkRef) -> Result<(), String> {
    let k = crate::engine::seg_key(db, &c.id);
    let g = mem.get_sync(&k, &Default::default()).map_err(|e| format!("db {db}: chunk {} missing: {e}", c.id))?;
    let entries = segment::read_all(&g.data).map_err(|e| format!("chunk {k}: {e}"))?;
    let mut seq = c.first_seq;
    let mut bytes = 0;
    for e in &entries {
        match stream::stored(e) {
            Some((i, Stored::Record(r))) if i == id && r.seq == seq => bytes += r.payload.len() as u64,
            _ => return Err(format!("chunk {k}: unexpected entry at {seq}")),
        }
        seq += 1;
    }
    if seq != c.last_seq + 1 || bytes != c.bytes || g.data.len() as u64 != c.size {
        return Err(format!("chunk {k} holds seqs {}..{seq} ({bytes} bytes), expected {c:?}", c.first_seq));
    }
    Ok(())
}

#[derive(Debug, Default, Clone)]
pub struct StorageReport {
    pub databases: usize,
    pub segments: usize,
    pub levels_objects: usize,
    pub blobs: usize,
    pub chunks: usize,
    pub parts: usize,
    pub objects: usize,
    /// Unreferenced part and mask objects missing from `Manifest::obsolete`.
    pub orphan_parts: Vec<UnreferencedPart>,
    /// Part and mask objects listed in `Manifest::obsolete`.
    pub obsolete_parts: Vec<UnreferencedPart>,
}

#[derive(Debug, Clone)]
pub struct UnreferencedPart {
    pub db: String,
    pub id: String,
    pub at_ms: u64,
}
