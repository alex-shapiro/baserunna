//! Writing level 0 parts from rows held in memory: staged bulk inserts and
//! sealed unsealed rows.

use super::json::{self, JType};
use super::part::{self, ColPlan, ColRef, DICT_MAX, DICT_MAX_BYTES, SharedPath, Sink, Source};
use super::schema::{EvType, EventSchema};
use super::vector::{Phys, Strs, Vector};
use crate::engine::Shared;
use crate::engine::events::{PartRef, Pos};
use crate::error::Result;
use crate::table::schema::{Datum, Row, encode_row};
use crate::table::tuple::{self, Key};
use bytes::Bytes;
use serde_json::{Map, Value as Json};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::Arc;

/// The bytes `row` takes in a part's columns, uncompressed: its encoding
/// plus its JSON objects flattened to paths, which repeat each leaf's
/// path. Granules are cut by it, so that no block outgrows what a reader
/// accepts.
pub fn stored_len(row: &Row) -> usize {
    let flat: usize = row
        .values()
        .map(|d| match d {
            Datum::Json(Json::Object(o)) => json::flattened_len(o),
            _ => 0,
        })
        .sum();
    encode_row(row).len() + 12 + flat
}

/// The most memory, as estimated by [`build_cost`], that the rows of one
/// part take decoded, which bounds the memory of seals and bulk inserts
/// whatever their events' sizes.
pub const PART_BUILD_BYTES: usize = 64 << 20;

/// The memory a row encoded in `len` bytes takes while its part is built.
pub fn build_cost(len: usize) -> usize {
    3 * len + 256
}

/// What rows gathered into a part take: the bytes the part stores for them
/// (their [`Defaults::stored_len`]), which `event_insert_part_max_bytes`
/// bounds, and the memory of building it (their [`build_cost`]), which
/// [`PART_BUILD_BYTES`] bounds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PartSize {
    pub stored: usize,
    pub cost: usize,
}

impl PartSize {
    /// The size of `row`, encoded in `encoded` bytes.
    pub fn of(row: &Row, encoded: usize, defaults: &Defaults) -> PartSize {
        PartSize { stored: defaults.stored_len(row), cost: build_cost(encoded) }
    }

    pub fn fits(self, max_bytes: usize) -> bool {
        self.stored <= max_bytes && self.cost <= PART_BUILD_BYTES
    }

    /// The most parts that [`next_part`] cuts rows of this size into: each
    /// holds at least half of one of the limits.
    pub fn parts(self, max_bytes: usize) -> usize {
        1 + self.stored / (max_bytes / 2).max(1) + self.cost / (PART_BUILD_BYTES / 2)
    }
}

impl std::ops::Add for PartSize {
    type Output = PartSize;

    fn add(self, o: PartSize) -> PartSize {
        PartSize { stored: self.stored + o.stored, cost: self.cost + o.cost }
    }
}

/// Takes from `rows` the next part's: rows of at most `max_bytes` stored
/// and [`PART_BUILD_BYTES`] of build cost, at least one row.
pub fn next_part<T>(rows: &mut std::iter::Peekable<impl Iterator<Item = (PartSize, T)>>, max_bytes: usize) -> Vec<T> {
    let (mut group, mut size) = (Vec::new(), PartSize::default());
    while let Some((n, _)) = rows.peek() {
        let grown = size + *n;
        if !group.is_empty() && !grown.fits(max_bytes) {
            break;
        }
        size = grown;
        group.extend(rows.next().map(|(_, r)| r));
    }
    group
}

/// Writes the encoded `rows` of one partition as level 0 parts, decoding
/// one part's rows at a time, and appends each part and the positions of
/// its rows, in its order, to `out`.
pub async fn write_encoded<'a>(
    shared: &Shared,
    db: &str,
    schema: &EventSchema,
    partition: u32,
    rows: impl Iterator<Item = (Pos, &'a [u8])> + Send,
    versions: bool,
    out: &mut Vec<(PartRef, Vec<Pos>)>,
) -> Result<()> {
    let defaults = Defaults::of(schema);
    let mut rows = Decoding { rows, defaults: &defaults }.peekable();
    while rows.peek().is_some() {
        let decoded =
            next_part(&mut rows, shared.cfg.event_insert_part_max_bytes).into_iter().collect::<Result<Vec<_>>>()?;
        let (part, src) = write_rows(shared, db, schema, partition, decoded, versions).await?;
        out.push((part, src.sorted().iter().map(|r| r.pos).collect()));
    }
    Ok(())
}

/// Encoded rows, decoded as they are taken, with their sizes.
struct Decoding<'d, I> {
    rows: I,
    defaults: &'d Defaults,
}

impl<'a, I: Iterator<Item = (Pos, &'a [u8])>> Iterator for Decoding<'_, I> {
    type Item = (PartSize, Result<PosRow>);

    fn next(&mut self) -> Option<Self::Item> {
        let (pos, r) = self.rows.next()?;
        Some(match crate::table::schema::decode_row(r) {
            Ok(row) => (PartSize::of(&row, r.len(), self.defaults), Ok(PosRow { pos, row })),
            Err(_) => (PartSize::default(), Err(crate::error::Error::internal("corrupt event row"))),
        })
    }
}

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
    RowBytes,
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
    partial: BTreeSet<u32>,
    defaults: Defaults,
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
    let mut bytes = 0;
    for v in vals {
        if set.insert(v) {
            bytes += v.len();
        }
        if set.len() > DICT_MAX || bytes > DICT_MAX_BYTES {
            return None;
        }
    }
    Some(Arc::new(set.into_iter().map(str::to_string).collect()))
}

/// The defaults of a table's columns that parts store for rows written
/// before the column was added, so that every read of such a row sees the
/// default. Rows are not filled with them: a part's builder writes a
/// column's default where a row lacks it, so that a 1 KiB default costs
/// nothing per row until it is encoded.
#[derive(Clone, Debug, Default)]
pub struct Defaults {
    /// Each default, and the bytes it adds to a row's [`stored_len`].
    cols: BTreeMap<u32, (Datum, usize)>,
}

impl Defaults {
    pub fn of(schema: &EventSchema) -> Defaults {
        let cols = schema
            .columns
            .iter()
            .filter_map(|c| {
                let d = c.default.as_ref()?;
                match super::schema::datum(c, d) {
                    Ok(v) if !v.is_null() && c.ty != EvType::Json => {
                        let len = encode_row(&Row::from([(c.id, v.clone())])).len() - encode_row(&Row::new()).len();
                        Some((c.id, (v, len)))
                    }
                    _ => None,
                }
            })
            .collect();
        Defaults { cols }
    }

    /// The default of column `id`, if parts store one.
    pub fn get(&self, id: u32) -> Option<&Datum> {
        self.cols.get(&id).map(|(d, _)| d)
    }

    /// The value of column `id` that a part stores for `row`.
    pub fn value<'r>(&'r self, row: &'r Row, id: u32) -> Option<&'r Datum> {
        row.get(&id).or_else(|| self.get(id))
    }

    /// The bytes that a row lacking column `id` grows by when the part
    /// stores its default.
    pub fn len(&self, id: u32) -> usize {
        self.cols.get(&id).map_or(0, |(_, n)| *n)
    }

    /// The [`stored_len`] of `row` with the defaults it lacks.
    pub fn stored_len(&self, row: &Row) -> usize {
        stored_len(row) + self.cols.iter().filter(|(id, _)| !row.contains_key(id)).map(|(_, (_, n))| n).sum::<usize>()
    }
}

/// Chooses the `max` most common paths to store as columns, the most
/// common first and then by key.
pub fn choose_paths(stats: &BTreeMap<String, (u64, JType)>, max: usize) -> BTreeSet<String> {
    let mut v: Vec<(&String, u64)> =
        stats.iter().filter(|(k, _)| k.len() <= PATH_COLUMN_KEY_MAX).map(|(k, (n, _))| (k, *n)).collect();
    v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
    v.into_iter().take(max).map(|(k, _)| k.clone()).collect()
}

/// Paths longer than this are never stored as columns.
pub const PATH_COLUMN_KEY_MAX: usize = 256;

/// How a part stores one JSON column's paths.
pub struct PathPlan {
    /// Paths stored as columns of their own.
    pub chosen: Arc<BTreeSet<String>>,
    /// The other paths the part's directory lists, by key.
    pub listed: Vec<SharedPath>,
    /// Whether the other paths include some that `listed` leaves out.
    pub partial: bool,
    /// The shared buckets holding the other paths.
    pub buckets: BTreeSet<u8>,
}

/// Plans a JSON column's paths from their counts and types: the most common
/// `event_json_max_paths` become columns, and the rest share buckets and
/// are listed, most common first, while they fit
/// `event_json_directory_bytes`. Both limits are divided among the table's
/// `json_columns`.
pub fn plan_paths(
    stats: &BTreeMap<String, (u64, JType)>,
    cfg: &crate::engine::EngineConfig,
    json_columns: usize,
    partial: bool,
) -> PathPlan {
    let share = json_columns.max(1);
    let chosen = Arc::new(choose_paths(stats, (cfg.event_json_max_paths / share).max(1)));
    let mut rest: Vec<(&String, &(u64, JType))> = stats.iter().filter(|(k, _)| !chosen.contains(*k)).collect();
    let buckets = rest.iter().map(|(k, _)| json::bucket(k)).collect();
    rest.sort_by(|a, b| b.1.0.cmp(&a.1.0).then_with(|| a.0.cmp(b.0)));
    let budget = cfg.event_json_directory_bytes / share;
    let mut bytes = 0;
    let mut listed: Vec<SharedPath> = Vec::new();
    for (k, (n, t)) in &rest {
        bytes += k.len();
        if bytes > budget {
            break;
        }
        listed.push(SharedPath { key: (*k).clone(), count: *n, ty: *t });
    }
    let partial = partial || listed.len() < rest.len();
    listed.sort_by(|a, b| a.key.cmp(&b.key));
    PathPlan { chosen, listed, partial, buckets }
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
    pub fn new(
        schema: &EventSchema,
        mut rows: Vec<PosRow>,
        versions: bool,
        cfg: &crate::engine::EngineConfig,
    ) -> RowsSource {
        let defaults = Defaults::of(schema);
        let sort_ids = schema.sort_ids();
        let sort_key = |row: &Row| {
            let keys: Vec<Key> = sort_ids
                .iter()
                .map(|id| defaults.value(row, *id).and_then(Datum::to_key).unwrap_or(Key::Null))
                .collect();
            tuple::encode(&keys)
        };
        let mut keyed: Vec<(Vec<u8>, PosRow)> = rows.drain(..).map(|r| (sort_key(&r.row), r)).collect();
        keyed.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.pos.cmp(&b.1.pos)));
        let keys: Vec<Bytes> = keyed.iter().map(|(k, _)| Bytes::from(k.clone())).collect();
        let rows: Vec<PosRow> = keyed.into_iter().map(|(_, r)| r).collect();
        let bytes = rows.iter().map(|r| defaults.stored_len(&r.row) as u64).collect();
        let blooms: BTreeSet<u32> = schema.bloom_ids().into_iter().collect();
        let mut plans = Vec::new();
        let mut getters = Vec::new();
        let mut leaves = BTreeMap::new();
        let mut shared = BTreeMap::new();
        let mut partial = BTreeSet::new();
        let json_columns = schema.columns.iter().filter(|c| c.ty == EvType::Json).count();
        for c in &schema.columns {
            if defaults.get(c.id).is_none() && !rows.iter().any(|r| r.row.contains_key(&c.id)) {
                continue;
            }
            if c.ty != EvType::Json {
                let dict = match c.ty {
                    EvType::String => dictionary(rows.iter().filter_map(|r| match defaults.value(&r.row, c.id) {
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
            let PathPlan { chosen, listed, partial: cut, buckets } = plan_paths(&stats, cfg, json_columns, false);
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
            if !listed.is_empty() {
                shared.insert(c.id, listed);
            }
            if cut {
                partial.insert(c.id);
            }
            leaves.insert(c.id, per_row);
        }
        if versions {
            plans.push(ColPlan { cref: ColRef::Version, phys: Phys::Int, json: false, dict: None, bloom: false });
            getters.push(Getter::Version);
        }
        plans.push(ColPlan { cref: ColRef::Ordinal, phys: Phys::Int, json: false, dict: None, bloom: false });
        getters.push(Getter::Ordinal);
        plans.push(ColPlan { cref: ColRef::RowBytes, phys: Phys::Int, json: false, dict: None, bloom: false });
        getters.push(Getter::RowBytes);
        RowsSource { rows, bytes, keys, plans, getters, leaves, shared, partial, defaults }
    }

    pub fn sorted(&self) -> &[PosRow] {
        &self.rows
    }

    fn values<'a>(&'a self, g: &Getter, rows: Range<usize>) -> Vector {
        let rs = &self.rows[rows.clone()];
        match g {
            Getter::User(id) => {
                let get = |r: &'a PosRow| self.defaults.value(&r.row, *id);
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
            Getter::RowBytes => Vector::Int(self.bytes[rows].iter().map(|b| Some(*b as i64)).collect()),
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

    fn partial_paths(&self) -> BTreeSet<u32> {
        self.partial.clone()
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
    let min_pos = rows.iter().map(|r| r.pos).min().unwrap_or_default();
    let max_pos = rows.iter().map(|r| r.pos).max().unwrap_or_default();
    let mut src = RowsSource::new(schema, rows, versions, &shared.cfg);
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
        arrived_ms: crate::engine::types::blob_id_time(&sink.id).unwrap_or(0),
        version: None,
        min_pos,
        max_pos,
        deleted: 0,
        masked_ms: 0,
        mask: None,
    };
    Ok((r, src))
}
