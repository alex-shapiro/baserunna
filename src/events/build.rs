//! Writing level 0 parts from rows held in memory: staged bulk inserts and
//! sealed unsealed rows.

use super::json::{self, JType};
use super::part::{self, ColPlan, ColRef, DICT_MAX, SharedPath, Sink, Source};
use super::schema::{EvType, EventSchema};
use super::vector::{Phys, Strs, Vector};
use crate::engine::Shared;
use crate::engine::events::{PartRef, Pos};
use crate::error::Result;
use crate::table::schema::{Datum, Row, encode_row};
use bytes::Bytes;
use serde_json::{Map, Value as Json};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::Arc;

/// A row to write: its position and values.
#[derive(Clone, Debug, PartialEq)]
pub struct PosRow {
    pub pos: Pos,
    pub row: Row,
}

enum Getter {
    User(u32),
    Present(u32),
    Version,
    Ordinal,
    Path { col: u32, key: String, ty: JType },
    Shared { col: u32, bucket: u8, chosen: Arc<BTreeSet<String>> },
}

/// Rows sorted by the sort key, then by position, as a part [`Source`].
pub struct RowsSource {
    rows: Vec<PosRow>,
    bytes: Vec<u64>,
    keys: Vec<Bytes>,
    plans: Vec<ColPlan>,
    getters: Vec<Getter>,
    /// Leaves of each JSON column's object in each row.
    leaves: BTreeMap<u32, Vec<BTreeMap<String, Json>>>,
    shared: BTreeMap<u32, Vec<SharedPath>>,
}

fn phys_of(t: EvType) -> Phys {
    match t {
        EvType::Time | EvType::Int => Phys::Int,
        EvType::Float => Phys::Float,
        EvType::Bool => Phys::Bool,
        EvType::String | EvType::Json => Phys::Str,
    }
}

/// A part-level dictionary for `vals`, if they have few enough distinct values.
pub fn dictionary<'a>(vals: impl Iterator<Item = &'a str>) -> Option<Arc<Vec<String>>> {
    let mut set = BTreeSet::new();
    for v in vals {
        set.insert(v);
        if set.len() > DICT_MAX {
            return None;
        }
    }
    Some(Arc::new(set.into_iter().map(str::to_string).collect()))
}

/// The defaults that parts store for rows written before their column was
/// added, so that every read of such a row sees the default.
pub fn stored_defaults(schema: &EventSchema) -> Vec<(u32, Datum)> {
    schema
        .columns
        .iter()
        .filter_map(|c| {
            let d = c.default.as_ref()?;
            match super::schema::datum(c, d) {
                Ok(v) if !v.is_null() && c.ty != EvType::Json => Some((c.id, v)),
                _ => None,
            }
        })
        .collect()
}

/// Chooses the `max` most common paths to store as columns, the most
/// common first and then by key.
pub fn choose_paths(stats: &BTreeMap<String, (u64, JType)>, max: usize) -> BTreeSet<String> {
    let mut v: Vec<(&String, u64)> = stats.iter().map(|(k, (n, _))| (k, *n)).collect();
    v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
    v.into_iter().take(max).map(|(k, _)| k.clone()).collect()
}

/// The value of a path in a column of type `ty`, as a vector element.
pub fn leaf_text(ty: JType, v: &Json) -> String {
    match (ty, v) {
        (JType::Str, Json::String(s)) => s.clone(),
        _ => v.to_string(),
    }
}

impl RowsSource {
    /// Sorts `rows` and plans their columns; `versions` stores each row's
    /// version, which binds of staged parts leave to their reference.
    pub fn new(schema: &EventSchema, mut rows: Vec<PosRow>, versions: bool, max_paths: usize) -> RowsSource {
        for (id, d) in stored_defaults(schema) {
            for r in rows.iter_mut() {
                r.row.entry(id).or_insert_with(|| d.clone());
            }
        }
        let mut keyed: Vec<(Vec<u8>, PosRow)> = rows.drain(..).map(|r| (schema.sort_key(&r.row), r)).collect();
        keyed.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.pos.cmp(&b.1.pos)));
        let keys: Vec<Bytes> = keyed.iter().map(|(k, _)| Bytes::from(k.clone())).collect();
        let rows: Vec<PosRow> = keyed.into_iter().map(|(_, r)| r).collect();
        let bytes = rows.iter().map(|r| encode_row(&r.row).len() as u64 + 12).collect();
        let blooms: BTreeSet<u32> = schema.bloom_ids().into_iter().collect();
        let mut plans = Vec::new();
        let mut getters = Vec::new();
        let mut leaves = BTreeMap::new();
        let mut shared = BTreeMap::new();
        for c in &schema.columns {
            if !rows.iter().any(|r| r.row.contains_key(&c.id)) {
                continue;
            }
            if c.ty != EvType::Json {
                let dict = match c.ty {
                    EvType::String => dictionary(rows.iter().filter_map(|r| match r.row.get(&c.id) {
                        Some(Datum::Str(s)) => Some(s.as_str()),
                        _ => None,
                    })),
                    _ => None,
                };
                plans.push(ColPlan {
                    cref: ColRef::User(c.id),
                    phys: phys_of(c.ty),
                    json: false,
                    dict,
                    bloom: blooms.contains(&c.id),
                });
                getters.push(Getter::User(c.id));
                continue;
            }
            plans.push(ColPlan { cref: ColRef::User(c.id), phys: Phys::Bool, json: false, dict: None, bloom: false });
            getters.push(Getter::Present(c.id));
            let per_row: Vec<BTreeMap<String, Json>> = rows
                .iter()
                .map(|r| match r.row.get(&c.id) {
                    Some(Datum::Json(Json::Object(o))) => {
                        json::flatten(o).into_iter().map(|(k, v)| (k, v.clone())).collect()
                    }
                    _ => BTreeMap::new(),
                })
                .collect();
            let mut stats: BTreeMap<String, (u64, JType)> = BTreeMap::new();
            for m in &per_row {
                for (k, v) in m {
                    let t = JType::of(v);
                    stats.entry(k.clone()).and_modify(|(n, ty)| (*n, *ty) = (*n + 1, ty.join(t))).or_insert((1, t));
                }
            }
            let chosen = Arc::new(choose_paths(&stats, max_paths));
            for key in chosen.iter() {
                let ty = stats[key].1;
                let (phys, js) = match ty {
                    JType::Int => (Phys::Int, false),
                    JType::Float => (Phys::Float, false),
                    JType::Bool => (Phys::Bool, false),
                    JType::Str => (Phys::Str, false),
                    JType::Dyn => (Phys::Str, true),
                };
                let dict = match phys {
                    Phys::Str => {
                        let texts: Vec<String> =
                            per_row.iter().filter_map(|m| m.get(key)).map(|v| leaf_text(ty, v)).collect();
                        dictionary(texts.iter().map(String::as_str))
                    }
                    _ => None,
                };
                plans.push(ColPlan {
                    cref: ColRef::Path { col: c.id, key: key.clone() },
                    phys,
                    json: js,
                    dict,
                    bloom: false,
                });
                getters.push(Getter::Path { col: c.id, key: key.clone(), ty });
            }
            let rest: Vec<SharedPath> = stats
                .iter()
                .filter(|(k, _)| !chosen.contains(*k))
                .map(|(k, (n, t))| SharedPath { key: k.clone(), count: *n, ty: *t })
                .collect();
            let buckets: BTreeSet<u8> = rest.iter().map(|p| json::bucket(&p.key)).collect();
            for b in buckets {
                plans.push(ColPlan {
                    cref: ColRef::Shared { col: c.id, bucket: b },
                    phys: Phys::Str,
                    json: true,
                    dict: None,
                    bloom: false,
                });
                getters.push(Getter::Shared { col: c.id, bucket: b, chosen: chosen.clone() });
            }
            if !rest.is_empty() {
                shared.insert(c.id, rest);
            }
            leaves.insert(c.id, per_row);
        }
        if versions {
            plans.push(ColPlan { cref: ColRef::Version, phys: Phys::Int, json: false, dict: None, bloom: false });
            getters.push(Getter::Version);
        }
        plans.push(ColPlan { cref: ColRef::Ordinal, phys: Phys::Int, json: false, dict: None, bloom: false });
        getters.push(Getter::Ordinal);
        RowsSource { rows, bytes, keys, plans, getters, leaves, shared }
    }

    pub fn sorted(&self) -> &[PosRow] {
        &self.rows
    }

    fn values<'a>(&'a self, g: &Getter, rows: Range<usize>) -> Vector {
        let rs = &self.rows[rows.clone()];
        match g {
            Getter::User(id) => {
                fn get<'r>(r: &'r PosRow, id: &u32) -> Option<&'r Datum> {
                    r.row.get(id)
                }
                let get = |r: &'a PosRow| get(r, id);
                match rs.iter().find_map(get) {
                    Some(Datum::Int(_)) | None => Vector::Int(
                        rs.iter()
                            .map(|r| match get(r) {
                                Some(Datum::Int(x)) => Some(*x),
                                _ => None,
                            })
                            .collect(),
                    ),
                    Some(Datum::Float(_)) => Vector::Float(
                        rs.iter()
                            .map(|r| match get(r) {
                                Some(Datum::Float(x)) => Some(*x),
                                _ => None,
                            })
                            .collect(),
                    ),
                    Some(Datum::Bool(_)) => Vector::Bool(
                        rs.iter()
                            .map(|r| match get(r) {
                                Some(Datum::Bool(x)) => Some(*x),
                                _ => None,
                            })
                            .collect(),
                    ),
                    Some(_) => Vector::Str(Strs::from_values(rs.iter().map(|r| match get(r) {
                        Some(Datum::Str(s)) => Some(s.as_str()),
                        _ => None,
                    }))),
                }
            }
            Getter::Present(id) => Vector::Bool(rs.iter().map(|r| r.row.contains_key(id).then_some(true)).collect()),
            Getter::Version => Vector::Int(rs.iter().map(|r| Some(r.pos.version as i64)).collect()),
            Getter::Ordinal => Vector::Int(rs.iter().map(|r| Some(r.pos.ordinal as i64)).collect()),
            Getter::Path { col, key, ty } => {
                let ls = &self.leaves[col][rows];
                let get = |i: usize| ls[i].get(key);
                match ty {
                    JType::Int => Vector::Int((0..ls.len()).map(|i| get(i).and_then(Json::as_i64)).collect()),
                    JType::Float => Vector::Float((0..ls.len()).map(|i| get(i).and_then(Json::as_f64)).collect()),
                    JType::Bool => Vector::Bool((0..ls.len()).map(|i| get(i).and_then(Json::as_bool)).collect()),
                    _ => {
                        let texts: Vec<Option<String>> =
                            (0..ls.len()).map(|i| get(i).map(|v| leaf_text(*ty, v))).collect();
                        Vector::Str(Strs::from_values(texts.iter().map(|t| t.as_deref())))
                    }
                }
            }
            Getter::Shared { col, bucket, chosen } => {
                let ls = &self.leaves[col][rows];
                let texts: Vec<Option<String>> = ls
                    .iter()
                    .map(|m| {
                        let o: Map<String, Json> = m
                            .iter()
                            .filter(|(k, _)| !chosen.contains(*k) && json::bucket(k) == *bucket)
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect();
                        (!o.is_empty()).then(|| Json::Object(o).to_string())
                    })
                    .collect();
                Vector::Str(Strs::from_values(texts.iter().map(|t| t.as_deref())))
            }
        }
    }
}

impl Source for RowsSource {
    fn rows(&self) -> u64 {
        self.rows.len() as u64
    }

    fn row_bytes(&self, i: u64) -> u64 {
        self.bytes[i as usize]
    }

    fn key_at(&self, i: u64) -> Bytes {
        self.keys[i as usize].clone()
    }

    fn plans(&self) -> &[ColPlan] {
        &self.plans
    }

    fn shared_paths(&self) -> BTreeMap<u32, Vec<SharedPath>> {
        self.shared.clone()
    }

    async fn column(&mut self, plan: usize, rows: Range<u64>) -> Result<Vector> {
        let v = self.values(&self.getters[plan], rows.start as usize..rows.end as usize);
        let phys = self.plans[plan].phys;
        Ok(if v.phys() == phys { v } else { phys.nulls(v.len()) })
    }
}

/// Writes `rows` of one partition as a level 0 part and returns its
/// reference (without a version when `versions` is false).
pub async fn write_rows(
    shared: &Shared,
    db: &str,
    schema: &EventSchema,
    partition: u32,
    rows: Vec<PosRow>,
    versions: bool,
) -> Result<(PartRef, RowsSource)> {
    let time_id = schema.time_id();
    let times = rows.iter().filter_map(|r| match r.row.get(&time_id) {
        Some(Datum::Int(t)) => Some(*t),
        _ => None,
    });
    let (min_time, max_time) = times.fold((i64::MAX, i64::MIN), |(lo, hi), t| (lo.min(t), hi.max(t)));
    let (min_version, max_version) =
        rows.iter().fold((u64::MAX, 0), |(lo, hi), r| (lo.min(r.pos.version), hi.max(r.pos.version)));
    let mut src = RowsSource::new(schema, rows, versions, shared.cfg.event_json_max_paths);
    let mut sink = Sink::new(shared, db, part::new_part_id(shared));
    let w = part::write(&mut sink, &mut src, 1, schema.sort_ids()).await?;
    let r = PartRef {
        id: sink.id.clone(),
        table: schema.id,
        partition,
        chunk_bytes: w.chunk_bytes,
        size: w.size,
        raw_size: w.raw_size,
        rows: w.rows,
        level: 0,
        meta_off: w.meta_off,
        meta_len: w.meta_len,
        min_time,
        max_time,
        min_key: w.min_key,
        max_key: w.max_key,
        version: None,
        min_version: if versions { min_version } else { 0 },
        max_version: if versions { max_version } else { 0 },
        deleted: 0,
        mask: None,
    };
    Ok((r, src))
}
