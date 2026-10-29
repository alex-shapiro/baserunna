//! Bulk inserts (SPEC §5.13): requests are buffered per node and table
//! (ClickHouse's asynchronous inserts with `wait_for_async_insert`),
//! staged as level 0 parts on the receiving node, and bound by one commit
//! on the lease holder, so row data never passes through it. Each request
//! is its own transaction with its own commit record.

use super::ValidRows;
use super::build::write_encoded;
use super::schema::EventSchema;
use crate::engine::events::{Bind, BindSlot, Pos, Runs, StagedPart};
use crate::engine::{Committed, Db};
use crate::error::{Error, Result};
use crate::store::BoxFut;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use tokio::time::Instant;

/// Binds staged parts on the database's lease holder.
pub type BindFn = Arc<dyn Fn(Arc<Db>, Bind) -> BoxFut<'static, Result<Vec<Result<Committed>>>> + Send + Sync>;

/// How long a buffer waits for more requests, and how much it holds.
#[derive(Clone, Copy, Debug)]
pub struct BufferConfig {
    pub wait_ms: u64,
    pub max_bytes: usize,
}

struct Slot {
    rows: ValidRows,
    commit_id: u64,
    deadline: Instant,
    tx: oneshot::Sender<Result<Committed>>,
}

struct Buffer {
    db: Arc<Db>,
    schema: EventSchema,
    slots: Vec<Slot>,
    bytes: usize,
    generation: u64,
}

/// The insert buffers of a node, one per database and table.
pub struct Buffers {
    map: Mutex<BTreeMap<(String, u32), Buffer>>,
    generations: std::sync::atomic::AtomicU64,
    bind: BindFn,
    cfg: BufferConfig,
}

impl Buffers {
    pub fn new(cfg: BufferConfig, bind: BindFn) -> Arc<Buffers> {
        Arc::new(Buffers { map: Mutex::new(BTreeMap::new()), generations: Default::default(), bind, cfg })
    }

    /// Inserts validated `rows` into table `schema` of `db` as one
    /// transaction with id `commit_id`, answered once its rows are bound.
    pub async fn submit(
        self: &Arc<Self>,
        db: Arc<Db>,
        schema: EventSchema,
        rows: ValidRows,
        commit_id: u64,
        deadline: Instant,
    ) -> Result<Committed> {
        let bytes = rows.bytes();
        let (tx, rx) = oneshot::channel();
        let slot = Slot { rows, commit_id, deadline, tx };
        if bytes >= self.cfg.max_bytes || self.cfg.wait_ms == 0 {
            self.spawn_flush(db, schema, vec![slot]);
        } else {
            let key = (db.id.clone(), schema.id);
            let mut full = None;
            let mut timer = None;
            {
                let mut g = self.map.lock().unwrap_or_else(|e| e.into_inner());
                let b = g.entry(key.clone()).or_insert_with(|| Buffer {
                    db: db.clone(),
                    schema: schema.clone(),
                    slots: Vec::new(),
                    bytes: 0,
                    generation: self.generations.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                });
                if schema.next_column_id > b.schema.next_column_id {
                    b.schema = schema;
                }
                b.slots.push(slot);
                b.bytes += bytes;
                if b.slots.len() == 1 {
                    timer = Some(b.generation);
                }
                if b.bytes >= self.cfg.max_bytes {
                    full = g.remove(&key);
                    timer = None;
                }
            }
            if let Some(generation) = timer {
                let me = self.clone();
                let wait = std::time::Duration::from_millis(self.cfg.wait_ms);
                db.shared().env.spawn(async move {
                    tokio::time::sleep(wait).await;
                    let b = {
                        let mut g = me.map.lock().unwrap_or_else(|e| e.into_inner());
                        match g.get(&key) {
                            Some(b) if b.generation == generation => g.remove(&key),
                            _ => None,
                        }
                    };
                    if let Some(b) = b {
                        me.spawn_flush(b.db, b.schema, b.slots);
                    }
                });
            }
            if let Some(b) = full {
                self.spawn_flush(b.db, b.schema, b.slots);
            }
        }
        rx.await.map_err(|_| Error::Unavailable {
            message: "the insert was interrupted".into(),
            retry_after_ms: Some(500),
            outcome_unknown: true,
        })?
    }

    /// Flushes `slots` in a task of its own, so that a cancelled request
    /// does not cancel the flush of the requests buffered with it.
    fn spawn_flush(self: &Arc<Self>, db: Arc<Db>, schema: EventSchema, slots: Vec<Slot>) {
        let me = self.clone();
        db.shared().env.clone().spawn(async move { me.flush(db, schema, slots).await });
    }

    /// Stages the slots' rows as parts and binds them, answering each slot.
    async fn flush(&self, db: Arc<Db>, schema: EventSchema, slots: Vec<Slot>) {
        let mut ranges = Vec::with_capacity(slots.len());
        // Each partition's rows as (slot, row) indices, in position order.
        let mut by_partition: BTreeMap<u32, Vec<(u32, u32)>> = BTreeMap::new();
        let mut ordinal = 0u32;
        let mut senders = Vec::with_capacity(slots.len());
        let mut bind_slots = Vec::with_capacity(slots.len());
        let mut rows = Vec::with_capacity(slots.len());
        for (si, s) in slots.into_iter().enumerate() {
            let start = ordinal;
            for (ri, (partition, _)) in s.rows.iter().enumerate() {
                by_partition.entry(partition).or_default().push((si as u32, ri as u32));
                ordinal += 1;
            }
            ranges.push(start..ordinal);
            senders.push(s.tx);
            bind_slots.push(BindSlot { commit_id: s.commit_id, deadline: Some(s.deadline) });
            rows.push(s.rows);
        }
        let shared = db.shared().clone();
        let mut written = Vec::new();
        let mut failed = None;
        for (partition, idx) in by_partition {
            let encoded = idx.into_iter().map(|(si, ri)| {
                let pos = Pos { version: 0, ordinal: ranges[si as usize].start + ri };
                (pos, rows[si as usize].get(ri as usize).1)
            });
            if let Err(e) = write_encoded(&shared, &db.id, &schema, partition, encoded, false, &mut written).await {
                failed = Some(e);
                break;
            }
        }
        drop(rows);
        let parts = written
            .into_iter()
            .map(|(part, positions)| {
                let mut offsets: Vec<Vec<u32>> = vec![Vec::new(); ranges.len()];
                for (i, p) in positions.iter().enumerate() {
                    let slot = ranges.partition_point(|rg| rg.end <= p.ordinal);
                    offsets[slot].push(i as u32);
                }
                StagedPart { part, slot_rows: offsets.into_iter().map(Runs::from_sorted).collect() }
            })
            .collect();
        shared.env.crash_point().await;
        let results = match failed {
            Some(e) => Err(e),
            None => (self.bind)(db, Bind { table: schema.id, slots: bind_slots, parts }).await,
        };
        match results {
            Ok(rs) => {
                for (tx, r) in senders.into_iter().zip(rs) {
                    let _ = tx.send(r);
                }
            }
            Err(e) => {
                for tx in senders {
                    let _ = tx.send(Err(e.clone()));
                }
            }
        }
    }

    /// Requests waiting in buffers.
    pub fn pending(&self) -> usize {
        self.map.lock().unwrap_or_else(|e| e.into_inner()).values().map(|b| b.slots.len()).sum()
    }
}
