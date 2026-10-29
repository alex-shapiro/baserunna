//! Event table maintenance on the lease holder (SPEC §5.12): sealing
//! unsealed rows into parts, size-tiered merges within partitions,
//! dropping parts past retention, and deletes.
//!
//! Every change is an ordinary serializable transaction whose read set
//! holds what it replaced, so a seal or merge that raced with a delete (or
//! with another merge of the same parts) fails validation and is retried
//! from a new snapshot, and its outputs are deleted at once.

use super::build::{PosRow, choose_paths, dictionary, write_rows};
use super::json::{self, JType};
use super::part::{self, Caching, ColPlan, ColRef, PartMeta, ReadStats, Reading, SharedPath, Sink, Source};
use super::query::Filter;
use super::schema::{EvType, EventSchema};
use super::vector::{Phys, Strs, Vector};
use crate::engine::events::{self as ev, EventOp, Mask, PartRef, Pos, Runs, TableState};
use crate::engine::{Consistency, Db, Isolation, Shared, Txn, Value};
use crate::error::{Error, Result};
use crate::probe::{self, Probe};
use crate::table::schema::decode_row;
use crate::table::tuple::{self, Key};
use bytes::Bytes;
use futures_util::{StreamExt, stream};
use serde_json::{Map, Value as Json};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::{Arc, Mutex};

/// Finds the schema of table `id` at the transaction's snapshot without
/// recording the read: maintenance conflicts with drops through the rows
/// and references it reads instead.
pub async fn schema_by_id(txn: &mut Txn, id: u32) -> Result<Option<EventSchema>> {
    let mut prefix = vec![crate::table::SPACE_CATALOG];
    prefix.extend_from_slice(b"e/");
    let snap = txn.untracked_snapshot().clone();
    let items = snap.scan(&crate::engine::KeyRange::prefix(&prefix), false, 10_000, txn.read_time).await?;
    for e in items {
        if let Value::Inline(b) = &e.value
            && let Ok(s) = serde_json::from_slice::<EventSchema>(b)
            && s.id == id
        {
            return Ok(Some(s));
        }
    }
    Ok(None)
}

async fn delete_parts(shared: &Shared, db: &str, ids: impl IntoIterator<Item = String>) {
    for id in ids {
        if let Ok(objs) =
            crate::store::list_all(&*shared.store, &crate::engine::part_prefix(db, &id), &shared.retry).await
        {
            for o in objs {
                let _ = shared.retry.run(|| shared.store.delete(&o.key)).await;
            }
        }
    }
}

/// Seals up to `event_insert_part_max_bytes` of table `table`'s unsealed
/// rows into level 0 parts. Returns whether it sealed anything.
pub async fn seal(db: &Arc<Db>, table: u32) -> Result<bool> {
    let shared = db.shared().clone();
    let cfg = &shared.cfg;
    let mut txn = db.begin(Isolation::Serializable, Consistency::Strong).await?;
    let Some(s) = schema_by_id(&mut txn, table).await? else { return Ok(false) };
    let st = super::state(&mut txn, table).await?;
    if st.unsealed_rows == 0 {
        return Ok(false);
    }
    let avg = (st.unsealed_bytes / st.unsealed_rows).max(1) as usize;
    let limit = (cfg.event_insert_part_max_bytes / avg).max(1);
    let items = txn.scan(&ev::rows(table), false, limit).await?;
    let Some(last) = items.last().and_then(|it| ev::parse_row_key(&it.key)).map(|(_, p)| p) else { return Ok(false) };
    let mut bytes = 0u64;
    let mut by_partition: BTreeMap<u32, Vec<PosRow>> = BTreeMap::new();
    for it in &items {
        let (_, pos) = ev::parse_row_key(&it.key).ok_or_else(|| Error::internal("bad unsealed row key"))?;
        let Value::Inline(b) = &it.value else { return Err(Error::internal("unsealed row stored out of line")) };
        bytes += (it.key.len() + b.len()) as u64;
        let row = decode_row(b).map_err(|_| Error::internal("corrupt unsealed row"))?;
        by_partition.entry(ev::partition_key(s.bucket(s.time_of(&row)))).or_default().push(PosRow { pos, row });
    }
    let mut written = Vec::new();
    for (partition, rows) in by_partition {
        for chunk in split_rows(rows, cfg.event_insert_part_max_bytes) {
            let (r, _) = write_rows(&shared, &db.id, &s, partition, chunk, true).await?;
            written.push(r);
        }
    }
    shared.env.crash_point().await;
    txn.delete_range(ev::rows_through(table, last))?;
    for r in &written {
        txn.put(r.key(), Value::Inline(r.encode()))?;
        txn.event_op(EventOp::part_ref(None, Some(r.clone())))?;
    }
    txn.event_op(EventOp::Forget { table, rows: items.len() as u64, bytes, sealed_through: Some(last) })?;
    match db.commit(txn.into_request()).await {
        Ok(_) => {
            probe::hit(Probe::EventsSealed);
            Ok(true)
        }
        Err(e @ (Error::Conflict(_) | Error::NotFound(_))) => {
            probe::hit(Probe::SealConflicted);
            delete_parts(&shared, &db.id, written.into_iter().map(|r| r.id)).await;
            Err(e)
        }
        Err(e) => Err(e),
    }
}

/// Splits rows into groups of about `max` encoded bytes.
pub fn split_rows(rows: Vec<PosRow>, max: usize) -> Vec<Vec<PosRow>> {
    let mut out = vec![Vec::new()];
    let mut bytes = 0;
    for r in rows {
        let n = crate::table::schema::encode_row(&r.row).len() + 12;
        if bytes + n > max && out.last().is_some_and(|v: &Vec<PosRow>| !v.is_empty()) {
            out.push(Vec::new());
            bytes = 0;
        }
        bytes += n;
        if let Some(last) = out.last_mut() {
            last.push(r);
        }
    }
    out.retain(|v| !v.is_empty());
    out
}

/// Work the merge planner chose for one partition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Task {
    /// Parts past retention, dropped without reading them.
    Drop(Vec<PartRef>),
    /// Parts merged (or one part rewritten without its masked rows).
    Merge(Vec<PartRef>),
}

/// The merge tier of a part: `floor(log_k(raw_size / base))`.
pub fn tier(raw: u64, base: u64, k: u64) -> u32 {
    let mut t = 0;
    let mut cap = base.max(1).saturating_mul(k);
    while raw >= cap {
        t += 1;
        cap = cap.saturating_mul(k);
        if cap == u64::MAX {
            break;
        }
    }
    t
}

/// Chooses the most urgent maintenance for a table's references, by
/// priority: retention drops, rewrites of mostly deleted parts, size-tiered
/// merges, then merges that settle partitions whose time has passed and
/// that received no part for `event_settle_ms` (ClickHouse's
/// `min_age_to_force_merge_seconds`), so that backfilling old partitions
/// is not merged after every insert.
pub fn plan(s: &EventSchema, refs: &[PartRef], cfg: &crate::engine::EngineConfig, now_ms: i64) -> Option<Task> {
    if let Some(r) = s.retention_ms {
        let cutoff = now_ms.saturating_sub(r as i64);
        let expired: Vec<PartRef> = refs.iter().filter(|p| p.max_time < cutoff).cloned().collect();
        if !expired.is_empty() {
            return Some(Task::Drop(expired));
        }
    }
    if let Some(p) = refs.iter().find(|p| p.deleted * 2 >= p.rows) {
        return Some(Task::Merge(vec![p.clone()]));
    }
    let k = cfg.event_merge_fan_in.max(2);
    let mut by_partition: BTreeMap<u32, Vec<&PartRef>> = BTreeMap::new();
    for p in refs.iter().filter(|p| p.size < cfg.event_part_max_bytes) {
        by_partition.entry(p.partition).or_default().push(p);
    }
    for parts in by_partition.values() {
        let mut tiers: BTreeMap<u32, Vec<&PartRef>> = BTreeMap::new();
        for p in parts {
            tiers.entry(tier(p.raw_size, cfg.event_tier_base_bytes, k as u64)).or_default().push(p);
        }
        for t in tiers.values_mut() {
            if t.len() >= k {
                t.sort_by(|a, b| a.raw_size.cmp(&b.raw_size).then(a.id.cmp(&b.id)));
                return Some(Task::Merge(t.iter().take(k).map(|p| (*p).clone()).collect()));
            }
        }
    }
    for (partition, parts) in &by_partition {
        let end = s.bucket_span(ev::partition_bucket(*partition)).1;
        let newest = parts.iter().filter_map(|p| crate::engine::types::blob_id_time(&p.id)).max().unwrap_or(0) as i64;
        let quiet = newest.saturating_add(cfg.event_settle_ms as i64) < now_ms;
        if parts.len() >= 2 && end < now_ms && quiet {
            let mut ps = parts.clone();
            ps.sort_by(|a, b| a.raw_size.cmp(&b.raw_size).then(a.id.cmp(&b.id)));
            let mut chosen: Vec<PartRef> = Vec::new();
            let mut size = 0;
            for p in ps.into_iter().take(k) {
                if size + p.size > cfg.event_part_max_bytes && chosen.len() >= 2 {
                    break;
                }
                size += p.size;
                chosen.push(p.clone());
            }
            if chosen.len() >= 2 {
                return Some(Task::Merge(chosen));
            }
        }
    }
    None
}

/// An input part of a merge.
struct Input {
    r: PartRef,
    meta: Arc<PartMeta>,
    mask: Arc<Runs>,
}

/// How an output column of a merge gets its values from an input.
#[derive(Clone, Debug)]
enum Feed {
    /// The output column is this column of the input, converted as needed.
    Col(usize),
    /// The output path's values live in the input's shared bucket column.
    SharedPath(usize, String),
    /// The output shared bucket gathers these input path columns and this
    /// input shared bucket, leaving out paths the output stores as columns.
    Bucket {
        paths: Vec<(usize, String)>,
        shared: Option<usize>,
    },
    /// The input's version (from its reference) or ordinal.
    Version,
    Ordinal,
    None,
}

/// What an output column holds, for converting input values.
#[derive(Clone, Debug)]
enum OutKind {
    Plain,
    /// A shared bucket, leaving out the paths the output stores as columns.
    Bucket(Arc<BTreeSet<String>>),
}

/// A decoded run of one input column, kept while a merge reads it in order.
struct Cursor {
    granule: usize,
    start: u64,
    v: Option<Vector>,
}

/// The rows of several parts merged in sort key and position order, as a
/// part [`Source`]. Columns are produced one at a time from cursors over
/// the inputs (ClickHouse's vertical merge), so memory holds the sort keys
/// and one run of each input column.
struct MergeSource<'a> {
    rd: Reading<'a>,
    inputs: Vec<Input>,
    /// For each output row, its input and row offset.
    order: Vec<(u16, u32)>,
    keys: Vec<Bytes>,
    plans: Vec<ColPlan>,
    kinds: Vec<OutKind>,
    feeds: Vec<Vec<Feed>>,
    shared_out: BTreeMap<u32, Vec<SharedPath>>,
    row_bytes: u64,
    cursors: BTreeMap<(usize, usize, usize), Cursor>,
    cursor_plan: usize,
    /// The default each output column stores for rows that lack a value.
    defaults: Vec<Option<super::agg::Val>>,
}

impl MergeSource<'_> {
    /// The value of input column `ci` at `row` of input `i`, reading it in order.
    async fn input_val(&mut self, i: usize, ci: usize, row: u64, feed_key: usize) -> Result<super::agg::Val> {
        let inp = &self.inputs[i];
        let key = (feed_key, i, ci);
        let cur = self.cursors.entry(key).or_insert(Cursor { granule: 0, start: 0, v: None });
        let meta = &inp.meta;
        let end = |c: &Cursor| c.v.as_ref().map_or(0, |v| c.start + v.len() as u64);
        if cur.v.is_none() || row < cur.start || row >= end(cur) {
            let g = meta.starts.partition_point(|s| *s <= row) - 1;
            let g_end = (g + 8).min(meta.granule_count());
            let v = self.rd.column(&inp.r, meta, ci, g..g_end).await?;
            *cur = Cursor { granule: g, start: meta.starts[g], v: Some(v) };
        }
        let _ = cur.granule;
        let v = cur.v.as_ref().ok_or_else(|| Error::internal("merge cursor empty"))?;
        let at = (row - cur.start) as usize;
        let json = meta.cols[ci].json;
        Ok(match v {
            Vector::Int(x) => x[at].map_or(super::agg::Val::Null, super::agg::Val::Int),
            Vector::Float(x) => x[at].map_or(super::agg::Val::Null, super::agg::Val::Float),
            Vector::Bool(x) => x[at].map_or(super::agg::Val::Null, super::agg::Val::Bool),
            Vector::Str(s) => match s.get(at) {
                None => super::agg::Val::Null,
                Some(t) if json => serde_json::from_str::<Json>(t)
                    .map(|j| super::agg::Val::from_json(&j))
                    .unwrap_or(super::agg::Val::Null),
                Some(t) => super::agg::Val::Str(t.to_string()),
            },
        })
    }
}

fn to_vector(phys: Phys, json_text: bool, vals: Vec<Option<super::agg::Val>>) -> Vector {
    use super::agg::Val;
    match phys {
        Phys::Int => Vector::Int(
            vals.into_iter()
                .map(|v| match v {
                    Some(Val::Int(i)) => Some(i),
                    _ => None,
                })
                .collect(),
        ),
        Phys::Float => Vector::Float(
            vals.into_iter()
                .map(|v| match v {
                    Some(Val::Float(f)) => Some(f),
                    _ => None,
                })
                .collect(),
        ),
        Phys::Bool => Vector::Bool(
            vals.into_iter()
                .map(|v| match v {
                    Some(Val::Bool(b)) => Some(b),
                    _ => None,
                })
                .collect(),
        ),
        Phys::Str => {
            let texts: Vec<Option<String>> = vals
                .into_iter()
                .map(|v| match v {
                    None | Some(Val::Null) => None,
                    Some(Val::Str(s)) if !json_text => Some(s),
                    Some(v) if json_text => Some(v.to_json().to_string()),
                    Some(_) => None,
                })
                .collect();
            Vector::Str(Strs::from_values(texts.iter().map(|t| t.as_deref())))
        }
    }
}

impl Source for MergeSource<'_> {
    fn rows(&self) -> u64 {
        self.order.len() as u64
    }

    fn row_bytes(&self, _: u64) -> u64 {
        self.row_bytes
    }

    fn key_at(&self, i: u64) -> Bytes {
        self.keys[i as usize].clone()
    }

    fn plans(&self) -> &[ColPlan] {
        &self.plans
    }

    fn shared_paths(&self) -> BTreeMap<u32, Vec<SharedPath>> {
        self.shared_out.clone()
    }

    async fn column(&mut self, plan: usize, rows: Range<u64>) -> Result<Vector> {
        use super::agg::Val;
        if self.cursor_plan != plan {
            self.cursors.clear();
            self.cursor_plan = plan;
        }
        let phys = self.plans[plan].phys;
        let mut vals: Vec<Option<Val>> = Vec::with_capacity((rows.end - rows.start) as usize);
        for out in rows {
            let (i, row) = self.order[out as usize];
            let (i, row) = (i as usize, row as u64);
            let v = match self.feeds[plan][i].clone() {
                Feed::None => Val::Null,
                Feed::Version => match self.inputs[i].r.version {
                    Some(v) => Val::Int(v as i64),
                    None => match self.inputs[i].meta.find(&ColRef::Version) {
                        Some(ci) => self.input_val(i, ci, row, plan).await?,
                        None => Val::Int(0),
                    },
                },
                Feed::Ordinal => match self.inputs[i].meta.find(&ColRef::Ordinal) {
                    Some(ci) => self.input_val(i, ci, row, plan).await?,
                    None => Val::Int(0),
                },
                Feed::Col(ci) => self.input_val(i, ci, row, plan).await?,
                Feed::SharedPath(ci, key) => match self.input_val(i, ci, row, plan).await? {
                    Val::Json(Json::Object(o)) => o.get(&key).map_or(Val::Null, Val::from_json),
                    _ => Val::Null,
                },
                Feed::Bucket { paths, shared } => {
                    let OutKind::Bucket(chosen) = self.kinds[plan].clone() else {
                        return Err(Error::internal("bucket feed without a bucket"));
                    };
                    let mut o = Map::new();
                    for (ci, key) in paths {
                        let v = self.input_val(i, ci, row, plan).await?;
                        if !v.is_null() {
                            o.insert(key, v.to_json());
                        }
                    }
                    if let Some(ci) = shared
                        && let Val::Json(Json::Object(m)) = self.input_val(i, ci, row, plan).await?
                    {
                        for (k, v) in m {
                            if !chosen.contains(&k) {
                                o.insert(k, v);
                            }
                        }
                    }
                    if o.is_empty() { Val::Null } else { Val::Json(Json::Object(o)) }
                }
            };
            let v = match (&self.defaults[plan], v) {
                (Some(d), Val::Null) => d.clone(),
                (_, v) => v,
            };
            vals.push(Some(v));
        }
        Ok(to_vector(phys, self.plans[plan].json, vals))
    }
}

fn phys_of(t: EvType) -> Phys {
    match t {
        EvType::Time | EvType::Int => Phys::Int,
        EvType::Float => Phys::Float,
        EvType::Bool => Phys::Bool,
        EvType::String | EvType::Json => Phys::Str,
    }
}

fn input_type(meta: &PartMeta, ci: usize) -> JType {
    let c = &meta.cols[ci];
    match (c.phys, c.json) {
        (Phys::Int, _) => JType::Int,
        (Phys::Float, _) => JType::Float,
        (Phys::Bool, _) => JType::Bool,
        (Phys::Str, false) => JType::Str,
        (Phys::Str, true) => JType::Dyn,
    }
}

/// Writes the live rows of `inputs` as one part.
async fn merge_parts(
    shared: &Shared,
    db: &str,
    s: &EventSchema,
    inputs: &[PartRef],
    stats: &ReadStats,
) -> Result<Option<PartRef>> {
    let memo = part::Memo::default();
    let rd = Reading { shared, db, cache: Caching::Private(&memo), stats };
    let mut ins = Vec::with_capacity(inputs.len());
    for r in inputs {
        ins.push(Input { r: r.clone(), meta: rd.meta(r).await?, mask: rd.mask(r).await? });
    }
    let sort_ids = s.sort_ids();
    let mut entries: Vec<(Bytes, Pos, u16, u32)> = Vec::new();
    for (i, inp) in ins.iter().enumerate() {
        let g = 0..inp.meta.granule_count();
        let mut cols = Vec::new();
        for id in &sort_ids {
            cols.push(match inp.meta.find(&ColRef::User(*id)) {
                Some(ci) => Some(rd.column(&inp.r, &inp.meta, ci, g.clone()).await?),
                None => None,
            });
        }
        let ords = match inp.meta.find(&ColRef::Ordinal) {
            Some(ci) => Some(rd.column(&inp.r, &inp.meta, ci, g.clone()).await?),
            None => None,
        };
        let vers = match (inp.r.version, inp.meta.find(&ColRef::Version)) {
            (None, Some(ci)) => Some(rd.column(&inp.r, &inp.meta, ci, g.clone()).await?),
            _ => None,
        };
        for row in 0..inp.meta.rows as usize {
            if inp.mask.contains(row as u32) {
                continue;
            }
            let keys: Vec<Key> = cols
                .iter()
                .map(|c| match c {
                    Some(Vector::Int(x)) => x[row].map_or(Key::Null, Key::Int),
                    Some(Vector::Float(x)) => x[row].map_or(Key::Null, Key::Float),
                    Some(Vector::Bool(x)) => x[row].map_or(Key::Null, Key::Bool),
                    Some(Vector::Str(st)) => st.get(row).map_or(Key::Null, |v| Key::Str(v.to_string())),
                    None => Key::Null,
                })
                .collect();
            let int_at = |v: &Option<Vector>| match v {
                Some(Vector::Int(x)) => x[row].unwrap_or(0),
                _ => 0,
            };
            let version = inp.r.version.unwrap_or(int_at(&vers) as u64);
            let pos = Pos { version, ordinal: int_at(&ords) as u32 };
            entries.push((Bytes::from(tuple::encode(&keys)), pos, i as u16, row as u32));
        }
    }
    if entries.is_empty() {
        return Ok(None);
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    let live: u64 = entries.len() as u64;
    let raw: u64 = ins.iter().map(|i| i.r.raw_size * i.r.live_rows() / i.r.rows.max(1)).sum();
    let blooms: BTreeSet<u32> = s.bloom_ids().into_iter().collect();
    let mut plans = Vec::new();
    let mut kinds = Vec::new();
    let mut feeds: Vec<Vec<Feed>> = Vec::new();
    let mut shared_out = BTreeMap::new();
    let stored: BTreeMap<u32, super::agg::Val> =
        super::build::stored_defaults(s).into_iter().map(|(id, d)| (id, super::exec::datum_val(&d))).collect();
    for c in &s.columns {
        let srcs: Vec<Option<usize>> = ins.iter().map(|i| i.meta.find(&ColRef::User(c.id))).collect();
        if srcs.iter().all(Option::is_none) {
            continue;
        }
        if c.ty != EvType::Json {
            let dict = match c.ty {
                EvType::String => {
                    let fill = match stored.get(&c.id) {
                        Some(super::agg::Val::Str(f)) => Some(f.as_str()),
                        _ => None,
                    };
                    union_dicts(&rd, &ins, &srcs, fill).await?
                }
                _ => None,
            };
            plans.push(ColPlan {
                cref: ColRef::User(c.id),
                phys: phys_of(c.ty),
                json: false,
                dict,
                bloom: blooms.contains(&c.id),
            });
            kinds.push(OutKind::Plain);
            feeds.push(srcs.iter().map(|s| s.map_or(Feed::None, Feed::Col)).collect());
            continue;
        }
        plans.push(ColPlan { cref: ColRef::User(c.id), phys: Phys::Bool, json: false, dict: None, bloom: false });
        kinds.push(OutKind::Plain);
        feeds.push(srcs.iter().map(|s| s.map_or(Feed::None, Feed::Col)).collect());
        let mut stats: BTreeMap<String, (u64, JType)> = BTreeMap::new();
        for inp in &ins {
            for (ci, key) in inp.meta.paths(c.id) {
                let (n, t) = (inp.meta.cols[ci].nonnull, input_type(&inp.meta, ci));
                stats.entry(key.to_string()).and_modify(|(m, ty)| (*m, *ty) = (*m + n, ty.join(t))).or_insert((n, t));
            }
            for p in inp.meta.shared.get(&c.id).into_iter().flatten() {
                stats
                    .entry(p.key.clone())
                    .and_modify(|(m, ty)| (*m, *ty) = (*m + p.count, ty.join(p.ty)))
                    .or_insert((p.count, p.ty));
            }
        }
        let chosen = Arc::new(choose_paths(&stats, shared.cfg.event_json_max_paths));
        for key in chosen.iter() {
            let ty = stats[key].1;
            let (phys, js) = match ty {
                JType::Int => (Phys::Int, false),
                JType::Float => (Phys::Float, false),
                JType::Bool => (Phys::Bool, false),
                JType::Str => (Phys::Str, false),
                JType::Dyn => (Phys::Str, true),
            };
            let path_feeds: Vec<Feed> = ins
                .iter()
                .map(|inp| match inp.meta.find(&ColRef::Path { col: c.id, key: key.clone() }) {
                    Some(ci) => Feed::Col(ci),
                    None => match inp.meta.find(&ColRef::Shared { col: c.id, bucket: json::bucket(key) }) {
                        Some(ci) if inp.meta.shared.get(&c.id).is_some_and(|ps| ps.iter().any(|p| p.key == *key)) => {
                            Feed::SharedPath(ci, key.clone())
                        }
                        _ => Feed::None,
                    },
                })
                .collect();
            let dict = if phys == Phys::Str && !js {
                let srcs: Vec<Option<usize>> = path_feeds
                    .iter()
                    .map(|f| match f {
                        Feed::Col(ci) => Some(*ci),
                        _ => None,
                    })
                    .collect();
                if path_feeds.iter().all(|f| matches!(f, Feed::Col(_) | Feed::None)) {
                    union_dicts(&rd, &ins, &srcs, None).await?
                } else {
                    None
                }
            } else {
                None
            };
            plans.push(ColPlan {
                cref: ColRef::Path { col: c.id, key: key.clone() },
                phys,
                json: js,
                dict,
                bloom: false,
            });
            kinds.push(OutKind::Plain);
            feeds.push(path_feeds);
        }
        let rest: Vec<SharedPath> = stats
            .iter()
            .filter(|(k, _)| !chosen.contains(*k))
            .map(|(k, (n, t))| SharedPath { key: k.clone(), count: *n, ty: *t })
            .collect();
        let buckets: BTreeSet<u8> = rest.iter().map(|p| json::bucket(&p.key)).collect();
        for b in buckets {
            let bucket_feeds: Vec<Feed> = ins
                .iter()
                .map(|inp| {
                    let paths: Vec<(usize, String)> = inp
                        .meta
                        .paths(c.id)
                        .filter(|(_, k)| !chosen.contains(*k) && json::bucket(k) == b)
                        .map(|(ci, k)| (ci, k.to_string()))
                        .collect();
                    let shared = inp.meta.find(&ColRef::Shared { col: c.id, bucket: b });
                    Feed::Bucket { paths, shared }
                })
                .collect();
            plans.push(ColPlan {
                cref: ColRef::Shared { col: c.id, bucket: b },
                phys: Phys::Str,
                json: true,
                dict: None,
                bloom: false,
            });
            kinds.push(OutKind::Bucket(chosen.clone()));
            feeds.push(bucket_feeds);
        }
        if !rest.is_empty() {
            shared_out.insert(c.id, rest);
        }
    }
    plans.push(ColPlan { cref: ColRef::Version, phys: Phys::Int, json: false, dict: None, bloom: false });
    kinds.push(OutKind::Plain);
    feeds.push(vec![Feed::Version; ins.len()]);
    plans.push(ColPlan { cref: ColRef::Ordinal, phys: Phys::Int, json: false, dict: None, bloom: false });
    kinds.push(OutKind::Plain);
    feeds.push(vec![Feed::Ordinal; ins.len()]);
    let defaults: Vec<Option<super::agg::Val>> = plans
        .iter()
        .map(|p| match &p.cref {
            ColRef::User(id) => stored.get(id).cloned(),
            _ => None,
        })
        .collect();
    let (min_version, max_version) =
        entries.iter().fold((u64::MAX, 0), |(lo, hi), e| (lo.min(e.1.version), hi.max(e.1.version)));
    let level = ins.iter().map(|i| i.r.level).max().unwrap_or(0).saturating_add(1);
    let partition = inputs[0].partition;
    let keys: Vec<Bytes> = entries.iter().map(|e| e.0.clone()).collect();
    let order: Vec<(u16, u32)> = entries.iter().map(|e| (e.2, e.3)).collect();
    let (min_time, max_time) =
        inputs.iter().fold((i64::MAX, i64::MIN), |(lo, hi), r| (lo.min(r.min_time), hi.max(r.max_time)));
    let mut src = MergeSource {
        rd: Reading { shared, db, cache: Caching::Private(&memo), stats },
        inputs: ins,
        order,
        keys,
        plans,
        kinds,
        feeds,
        shared_out,
        row_bytes: (raw / live.max(1)).max(1),
        cursors: BTreeMap::new(),
        cursor_plan: usize::MAX,
        defaults,
    };
    let mut sink = Sink::new(shared, db, part::new_part_id(shared));
    let w = part::write(&mut sink, &mut src, 3, sort_ids).await?;
    Ok(Some(PartRef {
        id: sink.id.clone(),
        table: s.id,
        partition,
        chunk_bytes: w.chunk_bytes,
        size: w.size,
        raw_size: w.raw_size,
        rows: w.rows,
        level,
        meta_off: w.meta_off,
        meta_len: w.meta_len,
        min_time,
        max_time,
        min_key: w.min_key,
        max_key: w.max_key,
        version: None,
        min_version,
        max_version,
        deleted: 0,
        mask: None,
    }))
}

/// The union of the inputs' dictionaries of a string column, plus the
/// `fill` that rows of inputs lacking the column take, if every input that
/// has the column has a dictionary and the union stays small.
async fn union_dicts(
    rd: &Reading<'_>,
    ins: &[Input],
    srcs: &[Option<usize>],
    fill: Option<&str>,
) -> Result<Option<Arc<Vec<String>>>> {
    let mut all: Vec<Arc<Vec<String>>> = Vec::new();
    if let Some(f) = fill
        && srcs.iter().any(Option::is_none)
    {
        all.push(Arc::new(vec![f.to_string()]));
    }
    for (inp, ci) in ins.iter().zip(srcs) {
        let Some(ci) = ci else { continue };
        if inp.meta.cols[*ci].phys != Phys::Str {
            return Ok(None);
        }
        match rd.index(&inp.r, &inp.meta, *ci).await?.dict.clone() {
            Some(d) => all.push(d),
            None => return Ok(None),
        }
    }
    Ok(dictionary(all.iter().flat_map(|d| d.iter().map(String::as_str))))
}

/// Merges `inputs` (or rewrites one) and commits the replacement.
pub async fn merge(db: &Arc<Db>, s: &EventSchema, inputs: &[PartRef]) -> Result<()> {
    let shared = db.shared().clone();
    let started = shared.env.now_ms();
    let stats = ReadStats::default();
    let out = merge_parts(&shared, &db.id, s, inputs, &stats).await?;
    let cfg = &shared.cfg;
    let max_ms = cfg.event_merge_max_ms.min(cfg.blob_stage_ttl_ms.saturating_sub(4 * cfg.clock_sync_max_error_ms));
    if shared.env.now_ms().saturating_sub(started) > max_ms {
        if let Some(o) = out {
            delete_parts(&shared, &db.id, [o.id]).await;
        }
        return Err(Error::unavailable("the merge ran too long"));
    }
    shared.env.crash_point().await;
    let mut txn = db.begin(Isolation::Serializable, Consistency::Strong).await?;
    for r in inputs {
        let cur = txn.get(&r.key()).await?.map(|it| super::ref_of(&it)).transpose()?;
        if cur.as_ref() != Some(r) {
            if let Some(o) = out {
                delete_parts(&shared, &db.id, [o.id]).await;
            }
            probe::hit(Probe::MergeConflicted);
            return Err(Error::Conflict("a merge input changed".into()));
        }
        txn.delete(r.key())?;
        txn.event_op(EventOp::part_ref(Some(r.clone()), None))?;
    }
    if let Some(o) = &out {
        txn.put(o.key(), Value::Inline(o.encode()))?;
        txn.event_op(EventOp::part_ref(None, Some(o.clone())))?;
    }
    match db.commit(txn.into_request()).await {
        Ok(_) => {
            probe::hit(Probe::PartsMerged);
            Ok(())
        }
        Err(e @ Error::Conflict(_)) => {
            probe::hit(Probe::MergeConflicted);
            if let Some(o) = out {
                delete_parts(&shared, &db.id, [o.id]).await;
            }
            Err(e)
        }
        Err(e) => Err(e),
    }
}

/// Deletes parts past retention.
pub async fn drop_expired(db: &Arc<Db>, parts: &[PartRef]) -> Result<()> {
    let mut txn = db.begin(Isolation::Serializable, Consistency::Strong).await?;
    for r in parts {
        let cur = txn.get(&r.key()).await?.map(|it| super::ref_of(&it)).transpose()?;
        if let Some(c) = cur {
            txn.delete(c.key())?;
            txn.event_op(EventOp::part_ref(Some(c), None))?;
        }
    }
    db.commit(txn.into_request()).await?;
    probe::hit(Probe::EventPartsExpired);
    Ok(())
}

/// Runs maintenance on table `table` until nothing is due, at most `rounds` steps.
pub async fn maintain(db: &Arc<Db>, table: u32, rounds: usize) -> Result<()> {
    let shared = db.shared().clone();
    for _ in 0..rounds {
        let mut txn = db.begin(Isolation::Serializable, Consistency::Strong).await?;
        let Some(s) = schema_by_id(&mut txn, table).await? else { return Ok(()) };
        let st: TableState = super::state(&mut txn, table).await?;
        if st.unsealed_bytes >= shared.cfg.event_seal_bytes as u64 && st.unsealed_rows > 0 {
            match seal(db, table).await {
                Ok(_) | Err(Error::Conflict(_)) => continue,
                Err(e) => return Err(e),
            }
        }
        let snap = txn.untracked_snapshot().clone();
        let mut refs = Vec::new();
        for e in snap.scan(&ev::refs(table), false, usize::MAX >> 1, txn.read_time).await? {
            refs.push(ev::ref_of(&e).ok_or_else(|| Error::internal("corrupt part reference"))?);
        }
        let now = shared.time_ms() as i64;
        match plan(&s, &refs, &shared.cfg, now) {
            None => return Ok(()),
            Some(Task::Drop(parts)) => drop_expired(db, &parts).await?,
            Some(Task::Merge(parts)) => match merge(db, &s, &parts).await {
                Ok(()) | Err(Error::Conflict(_)) => {}
                Err(e) => return Err(e),
            },
        }
    }
    Ok(())
}

/// Schedules maintenance per table, running at most one task per table.
#[derive(Default)]
pub struct Maintainer {
    running: Mutex<BTreeMap<(String, u32), bool>>,
}

impl Maintainer {
    /// Runs maintenance for `tables` of `db` in the background.
    pub fn schedule(self: &Arc<Self>, db: &Arc<Db>, tables: BTreeSet<u32>) {
        for t in tables {
            let key = (db.id.clone(), t);
            {
                let mut g = self.running.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(again) = g.get_mut(&key) {
                    *again = true;
                    continue;
                }
                g.insert(key.clone(), false);
            }
            let (me, db) = (self.clone(), db.clone());
            db.shared().env.clone().spawn(async move {
                loop {
                    if let Err(e) = maintain(&db, t, 64).await {
                        crate::debug!("event maintenance of {} table {t} stopped: {e}", db.id);
                    }
                    let mut g = me.running.lock().unwrap_or_else(|e| e.into_inner());
                    match g.get_mut(&key) {
                        Some(again) if *again => *again = false,
                        _ => {
                            g.remove(&key);
                            return;
                        }
                    }
                }
            });
        }
    }

    pub fn active(&self) -> usize {
        self.running.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
}

/// Deletes the rows of table `name` matching `f` within `[from, to)`, in
/// the transaction, returning how many. Unsealed rows get tombstones and
/// parts get masks.
pub async fn delete(db: &Arc<Db>, txn: &mut Txn, name: &str, from: i64, to: i64, f: &Filter) -> Result<u64> {
    let s = super::get_schema(txn, name).await?;
    let input = super::exec::input(txn, &s, from, to).await?;
    let mut count = 0u64;
    let (mut rows, mut bytes) = (0u64, 0u64);
    for (pos, row) in &input.unsealed {
        if super::exec::row_matches(&s, row, f, from, to)? {
            let k = ev::row_key(s.id, *pos);
            bytes += (k.len() + crate::table::schema::encode_row(row).len()) as u64;
            txn.delete(k)?;
            rows += 1;
        }
    }
    if rows > 0 {
        txn.event_op(EventOp::Forget { table: s.id, rows, bytes, sealed_through: None })?;
        count += rows;
    }
    let shared = db.shared().clone();
    let stats = ReadStats::default();
    let rd = Reading { shared: &shared, db: &db.id, cache: Caching::Shared, stats: &stats };
    let reads: Vec<_> =
        input.refs.iter().map(|r| super::exec::matching_rows(&shared, &db.id, &s, r, f, from, to)).collect();
    let mut matches = stream::iter(reads).buffered(shared.cfg.event_read_concurrency);
    for r in &input.refs {
        let matched = matches.next().await.ok_or_else(|| Error::internal("delete reads ended early"))??;
        if matched.is_empty() {
            continue;
        }
        let old = rd.mask(r).await?;
        let runs = old.union(&Runs::from_sorted(matched.iter().copied()));
        count += runs.count() - old.count();
        let in_partition = |e: Error| match e {
            Error::TooLarge(m) => Error::TooLarge(format!(
                "{m}; the delete stopped in the partition starting at {}: delete partition by partition",
                s.bucket_span(ev::partition_bucket(r.partition)).0
            )),
            e => e,
        };
        if runs.count() >= r.rows {
            txn.delete(r.key()).map_err(in_partition)?;
            txn.event_op(EventOp::part_ref(Some(r.clone()), None)).map_err(in_partition)?;
            probe::hit(Probe::EventRowsMasked);
            continue;
        }
        let encoded = runs.to_bytes();
        let mask = if encoded.len() <= ev::MASK_INLINE_MAX {
            Mask::Inline(runs.clone())
        } else {
            probe::hit(Probe::MaskObjectWritten);
            let (id, size) = part::write_mask(&shared, &db.id, &runs).await?;
            Mask::Object { id, size }
        };
        let new = PartRef { deleted: runs.count(), mask: Some(mask), ..r.clone() };
        txn.put(new.key(), Value::Inline(new.encode())).map_err(in_partition)?;
        txn.event_op(EventOp::part_ref(Some(r.clone()), Some(new))).map_err(in_partition)?;
        probe::hit(Probe::EventRowsMasked);
    }
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::schema::Datum;

    #[test]
    fn tiers_grow_by_the_fan_in() {
        assert_eq!(tier(0, 4, 8), 0);
        assert_eq!(tier(31, 4, 8), 0);
        assert_eq!(tier(32, 4, 8), 1);
        assert_eq!(tier(255, 4, 8), 1);
        assert_eq!(tier(256, 4, 8), 2);
        assert_eq!(tier(u64::MAX, 4, 8), tier(u64::MAX - 1, 4, 8));
    }

    #[test]
    fn rows_split_by_size() {
        let row =
            |i: u32| PosRow { pos: Pos { version: 1, ordinal: i }, row: [(1, Datum::Str("x".repeat(100)))].into() };
        let groups = split_rows((0..10).map(row).collect(), 350);
        assert!(groups.len() >= 3 && groups.iter().all(|g| !g.is_empty()));
        assert_eq!(groups.iter().map(Vec::len).sum::<usize>(), 10);
        assert_eq!(split_rows(vec![row(0)], 1).len(), 1, "a row larger than the limit is a part of its own");
    }
}
