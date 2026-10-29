//! Event table maintenance on the lease holder (SPEC §5.13): sealing
//! unsealed rows into parts, size-tiered merges within partitions,
//! dropping parts past retention, and deletes.
//!
//! Every change is an ordinary serializable transaction whose read set
//! holds what it replaced, so a seal or merge that raced with a delete (or
//! with another merge of the same parts) fails validation and is retried
//! from a new snapshot, and its outputs are deleted at once.

use super::build::{PathPlan, dictionary, plan_paths, write_encoded};
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

/// Bytes a transaction is charged for each part reference it writes, at most.
const REF_WRITE_BYTES: usize = 512;

/// The most part references one maintenance transaction writes or removes.
pub fn refs_per_txn(cfg: &crate::engine::EngineConfig) -> usize {
    (cfg.txn_max_write_bytes / REF_WRITE_BYTES).max(1)
}

/// Seals up to `event_insert_part_max_bytes` of table `table`'s unsealed
/// rows, in position order, into level 0 parts, stopping before rows of
/// more partitions than one transaction can reference. Returns whether it
/// sealed anything.
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
    // Only the sealed rows join the read set: rows inserted after the
    // snapshot follow them, so they cannot conflict.
    let snap = txn.untracked_snapshot().clone();
    let items = snap.scan(&ev::rows(table), false, limit, txn.read_time).await?;
    let max_parts = refs_per_txn(cfg);
    let (mut bytes, mut rows, mut last) = (0u64, 0u64, None);
    let mut by_partition: BTreeMap<u32, Vec<(Pos, &[u8])>> = BTreeMap::new();
    for it in &items {
        let (_, pos) = ev::parse_row_key(&it.key).ok_or_else(|| Error::internal("bad unsealed row key"))?;
        let Value::Inline(b) = &it.value else { return Err(Error::internal("unsealed row stored out of line")) };
        let row = decode_row(b).map_err(|_| Error::internal("corrupt unsealed row"))?;
        let partition = ev::partition_key(s.bucket(s.time_of(&row)));
        if !by_partition.contains_key(&partition) && by_partition.len() >= max_parts {
            break;
        }
        bytes += (it.key.len() + b.len()) as u64;
        rows += 1;
        last = Some(pos);
        by_partition.entry(partition).or_default().push((pos, b));
    }
    let Some(last) = last else { return Ok(false) };
    txn.note_read(ev::rows_through(table, last));
    let mut parts = Vec::new();
    for (partition, rows) in by_partition {
        if let Err(e) = write_encoded(&shared, &db.id, &s, partition, rows.into_iter(), true, &mut parts).await {
            delete_parts(&shared, &db.id, parts.into_iter().map(|(r, _)| r.id)).await;
            return Err(e);
        }
    }
    let written: Vec<PartRef> = parts.into_iter().map(|(r, _)| r).collect();
    shared.env.crash_point().await;
    let committed = async {
        txn.delete_range(ev::rows_through(table, last))?;
        for r in &written {
            txn.put(r.key(), Value::Inline(r.encode()))?;
            txn.event_op(EventOp::part_ref(None, Some(r.clone())))?;
        }
        txn.event_op(EventOp::Forget { table, rows, bytes })?;
        db.commit(txn.into_request()).await
    };
    match committed.await {
        Ok(_) => {
            probe::hit(Probe::EventsSealed);
            Ok(true)
        }
        Err(e @ Error::Unavailable { outcome_unknown: true, .. }) => Err(e),
        Err(e) => {
            if matches!(e, Error::Conflict(_) | Error::NotFound(_)) {
                probe::hit(Probe::SealConflicted);
            }
            delete_parts(&shared, &db.id, written.into_iter().map(|r| r.id)).await;
            Err(e)
        }
    }
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
pub fn plan(
    s: &EventSchema,
    refs: &[PartRef],
    failed: &BTreeSet<String>,
    cfg: &crate::engine::EngineConfig,
    now_ms: i64,
) -> Option<Task> {
    if s.retention_ms.is_some() {
        let cutoff = s.retained_from(now_ms);
        let expired: Vec<PartRef> =
            refs.iter().filter(|p| p.max_time < cutoff).take(refs_per_txn(cfg)).cloned().collect();
        if !expired.is_empty() {
            return Some(Task::Drop(expired));
        }
    }
    // Parts of merges that failed are left out of merges, not of drops.
    let refs: Vec<&PartRef> = refs.iter().filter(|p| !failed.contains(&p.id)).collect();
    if let Some(p) = refs.iter().find(|p| p.deleted * 2 >= p.rows) {
        return Some(Task::Merge(vec![(*p).clone()]));
    }
    let k = cfg.event_merge_fan_in.max(2);
    let mut by_partition: BTreeMap<u32, Vec<&PartRef>> = BTreeMap::new();
    for p in refs.iter().copied().filter(|p| p.size < cfg.event_part_max_bytes) {
        by_partition.entry(p.partition).or_default().push(p);
    }
    for parts in by_partition.values() {
        let mut tiers: BTreeMap<u32, Vec<&PartRef>> = BTreeMap::new();
        for p in parts {
            tiers.entry(tier(p.raw_size, cfg.event_tier_base_bytes, k as u64)).or_default().push(p);
        }
        for t in tiers.values_mut() {
            if t.len() >= k
                && let Some(chosen) = smallest_under_cap(t, k, cfg.event_part_max_bytes)
            {
                return Some(Task::Merge(chosen));
            }
        }
    }
    for (partition, parts) in &by_partition {
        let end = s.bucket_span(ev::partition_bucket(*partition)).1;
        let newest = parts.iter().map(|p| p.arrived_ms).max().unwrap_or(0) as i64;
        let quiet = newest.saturating_add(cfg.event_settle_ms as i64) < now_ms;
        if parts.len() >= 2
            && end < now_ms
            && quiet
            && let Some(chosen) = smallest_under_cap(&mut parts.clone(), k, cfg.event_part_max_bytes)
        {
            return Some(Task::Merge(chosen));
        }
    }
    None
}

/// The smallest of `parts`, at most `k` and at least 2, whose sizes add up
/// to at most `cap`.
fn smallest_under_cap(parts: &mut [&PartRef], k: usize, cap: u64) -> Option<Vec<PartRef>> {
    parts.sort_by(|a, b| a.raw_size.cmp(&b.raw_size).then(a.id.cmp(&b.id)));
    let mut chosen: Vec<PartRef> = Vec::new();
    let mut size = 0;
    for p in parts.iter().take(k) {
        if size + p.size > cap {
            break;
        }
        size += p.size;
        chosen.push((*p).clone());
    }
    (chosen.len() >= 2).then_some(chosen)
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
    /// The row's stored bytes.
    RowBytes,
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
    partial_out: BTreeSet<u32>,
    /// Each output row's stored bytes.
    bytes: Vec<u64>,
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
        self.shared_out.clone()
    }

    fn partial_paths(&self) -> BTreeSet<u32> {
        self.partial_out.clone()
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
                Feed::RowBytes => Val::Int(self.bytes[out as usize] as i64),
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
    let mut entries: Vec<(Bytes, Pos, u16, u32, u64)> = Vec::new();
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
        let sizes = match inp.meta.find(&ColRef::RowBytes) {
            Some(ci) => Some(rd.column(&inp.r, &inp.meta, ci, g.clone()).await?),
            None => return Err(Error::internal(format!("part {} does not store its row sizes", inp.r.id))),
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
            entries.push((Bytes::from(tuple::encode(&keys)), pos, i as u16, row as u32, int_at(&sizes) as u64));
        }
    }
    if entries.is_empty() {
        return Ok(None);
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    let blooms: BTreeSet<u32> = s.bloom_ids().into_iter().collect();
    let mut plans = Vec::new();
    let mut kinds = Vec::new();
    let mut feeds: Vec<Vec<Feed>> = Vec::new();
    let mut shared_out = BTreeMap::new();
    let mut partial_out = BTreeSet::new();
    let json_columns = s.columns.iter().filter(|c| c.ty == EvType::Json).count();
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
        // An input whose directory is partial may hold any path, of any type,
        // in its buckets: paths it does not name are typed as JSON text.
        let partial_in = ins.iter().any(|i| i.meta.partial.contains(&c.id));
        for inp in ins.iter().filter(|i| i.meta.partial.contains(&c.id)) {
            let named: BTreeSet<&str> = inp
                .meta
                .paths(c.id)
                .map(|(_, k)| k)
                .chain(inp.meta.shared.get(&c.id).into_iter().flatten().map(|p| p.key.as_str()))
                .collect();
            for (key, (_, ty)) in stats.iter_mut() {
                if !named.contains(key.as_str()) {
                    *ty = ty.join(JType::Dyn);
                }
            }
        }
        let PathPlan { chosen, listed, partial, mut buckets } =
            plan_paths(&stats, &shared.cfg, json_columns, partial_in);
        for inp in ins.iter().filter(|i| i.meta.partial.contains(&c.id)) {
            for m in &inp.meta.cols {
                if let ColRef::Shared { col, bucket } = m.cref
                    && col == c.id
                {
                    buckets.insert(bucket);
                }
            }
        }
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
                        Some(ci) if inp.meta.may_share(c.id, key) => Feed::SharedPath(ci, key.clone()),
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
        if !listed.is_empty() {
            shared_out.insert(c.id, listed);
        }
        if partial {
            partial_out.insert(c.id);
        }
    }
    plans.push(ColPlan { cref: ColRef::Version, phys: Phys::Int, json: false, dict: None, bloom: false });
    kinds.push(OutKind::Plain);
    feeds.push(vec![Feed::Version; ins.len()]);
    plans.push(ColPlan { cref: ColRef::Ordinal, phys: Phys::Int, json: false, dict: None, bloom: false });
    kinds.push(OutKind::Plain);
    feeds.push(vec![Feed::Ordinal; ins.len()]);
    plans.push(ColPlan { cref: ColRef::RowBytes, phys: Phys::Int, json: false, dict: None, bloom: false });
    kinds.push(OutKind::Plain);
    feeds.push(vec![Feed::RowBytes; ins.len()]);
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
    // Rows of an input that lacks a column with a default grow by it.
    let filled: Vec<u64> = (0..ins.len())
        .map(|i| {
            plans
                .iter()
                .enumerate()
                .filter(|(p, _)| matches!(feeds[*p][i], Feed::None))
                .filter_map(|(p, _)| defaults[p].as_ref())
                .map(|d| d.mem_size() as u64 + 8)
                .sum()
        })
        .collect();
    let bytes: Vec<u64> = entries.iter().map(|e| e.4 + filled[e.2 as usize]).collect();
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
        partial_out,
        bytes,
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
        arrived_ms: inputs.iter().map(|r| r.arrived_ms).max().unwrap_or(0),
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
        return Err(Error::TooLarge(format!("the merge ran longer than {max_ms} ms")));
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
        Err(e @ Error::Unavailable { outcome_unknown: true, .. }) => Err(e),
        Err(e) => {
            if matches!(e, Error::Conflict(_)) {
                probe::hit(Probe::MergeConflicted);
            }
            if let Some(o) = out {
                delete_parts(&shared, &db.id, [o.id]).await;
            }
            Err(e)
        }
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

/// Runs maintenance on table `table` until nothing is due, at most `rounds`
/// steps. A merge too large to finish in time is not planned again in this
/// call, so it does not hold up the table's other merges, and its error is
/// returned at the end; other errors end maintenance at once.
pub async fn maintain(db: &Arc<Db>, table: u32, rounds: usize) -> Result<()> {
    maintain_skipping(db, table, rounds, &mut BTreeSet::new()).await
}

/// [`maintain`], leaving out parts whose merge failed for a reason a retry
/// would meet again, and adding to `failed` the parts of merges that fail.
pub async fn maintain_skipping(db: &Arc<Db>, table: u32, rounds: usize, failed: &mut BTreeSet<String>) -> Result<()> {
    let shared = db.shared().clone();
    let mut first_error = None;
    for _ in 0..rounds {
        let mut txn = db.begin(Isolation::Serializable, Consistency::Strong).await?;
        let Some(s) = schema_by_id(&mut txn, table).await? else { return Ok(()) };
        let st: TableState = super::state(&mut txn, table).await?;
        // A round that seals also plans from the snapshot before the seal, so
        // that steady inserts do not starve merges and retention.
        let sealing = st.unsealed_bytes >= shared.cfg.event_seal_bytes as u64 && st.unsealed_rows > 0;
        if sealing {
            match seal(db, table).await {
                Ok(_) | Err(Error::Conflict(_)) => {}
                Err(e) => return Err(e),
            }
        }
        let snap = txn.untracked_snapshot().clone();
        let mut refs = Vec::new();
        for e in snap.scan(&ev::refs(table), false, usize::MAX >> 1, txn.read_time).await? {
            refs.push(ev::ref_of(&e).ok_or_else(|| Error::internal("corrupt part reference"))?);
        }
        failed.retain(|id| refs.iter().any(|r| r.id == *id));
        let now = shared.time_ms() as i64;
        match plan(&s, &refs, failed, &shared.cfg, now) {
            None if sealing => continue,
            None => break,
            Some(Task::Drop(parts)) => drop_expired(db, &parts).await?,
            Some(Task::Merge(parts)) => match merge(db, &s, &parts).await {
                Ok(()) | Err(Error::Conflict(_)) => {}
                Err(e @ (Error::NotOwner { .. } | Error::Unavailable { .. })) => return Err(e),
                Err(e) => {
                    crate::debug!("event merge of {} table {table} failed: {e}", db.id);
                    failed.extend(parts.into_iter().map(|p| p.id));
                    first_error.get_or_insert(e);
                }
            },
        }
    }
    first_error.map_or(Ok(()), Err)
}

/// Parts of failed merges, and when the first of them failed.
type FailedMerges = (BTreeSet<String>, tokio::time::Instant);

/// How long maintenance leaves out the parts of a failed merge.
const FAILED_MERGE_RETRY: std::time::Duration = std::time::Duration::from_secs(3600);

/// Schedules maintenance per table, running at most one task per table.
#[derive(Default)]
pub struct Maintainer {
    running: Mutex<BTreeMap<(String, u32), bool>>,
    /// Parts of each table whose merge failed, which later runs leave out
    /// for [`FAILED_MERGE_RETRY`] after the first failure.
    failed: Mutex<BTreeMap<(String, u32), FailedMerges>>,
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
                    let now = tokio::time::Instant::now();
                    let (mut failed, since) = match me.failed.lock().unwrap_or_else(|e| e.into_inner()).remove(&key) {
                        Some((f, since)) if now.duration_since(since) < FAILED_MERGE_RETRY => (f, since),
                        _ => (BTreeSet::new(), now),
                    };
                    if let Err(e) = maintain_skipping(&db, t, 64, &mut failed).await {
                        crate::debug!("event maintenance of {} table {t} stopped: {e}", db.id);
                    }
                    if !failed.is_empty() {
                        me.failed.lock().unwrap_or_else(|e| e.into_inner()).insert(key.clone(), (failed, since));
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
    super::exec::check_filter(&s, f)?;
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
        txn.event_op(EventOp::Forget { table: s.id, rows, bytes })?;
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
        let mask = if encoded.len() <= shared.cfg.event_mask_inline_bytes {
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
    fn rows_split_by_size_and_count() {
        use super::super::build::{PART_BUILD_ROWS, next_part};
        let row = [b'x'; 100];
        let rows = |n: u32| (0..n).map(|i| (Pos { version: 1, ordinal: i }, &row[..]));
        let groups = |n: u32, max: usize| {
            let mut it = rows(n).peekable();
            let mut out = Vec::new();
            while it.peek().is_some() {
                out.push(next_part(&mut it, max).len());
            }
            out
        };
        assert_eq!(groups(10, 350), [3, 3, 3, 1]);
        assert_eq!(groups(1, 1), [1], "a row larger than the limit is a part of its own");
        assert_eq!(groups(PART_BUILD_ROWS as u32 * 2 + 1, usize::MAX), [PART_BUILD_ROWS, PART_BUILD_ROWS, 1]);
    }
}
