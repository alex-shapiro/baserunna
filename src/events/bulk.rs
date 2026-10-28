//! Bulk inserts (SPEC §5.12): requests are buffered per node and table
//! (ClickHouse's asynchronous inserts with `wait_for_async_insert`),
//! staged as level 0 parts on the receiving node, and bound by one commit
//! on the lease holder, so row data never passes through it. Each request
//! is its own transaction with its own commit record.

use super::build::{PosRow, write_rows};
use super::maint::split_rows;
use super::schema::EventSchema;
use crate::engine::events::{Bind, BindSlot, Pos, Runs, StagedPart};
use crate::engine::{Committed, Db};
use crate::error::{Error, Result};
use crate::store::BoxFut;
use crate::table::schema::{Row, encode_row};
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
    rows: Vec<Row>,
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
        rows: Vec<Row>,
        commit_id: u64,
        deadline: Instant,
    ) -> Result<Committed> {
        let bytes: usize = rows.iter().map(|r| encode_row(r).len() + 12).sum();
        let (tx, rx) = oneshot::channel();
        let slot = Slot { rows, commit_id, deadline, tx };
        if bytes >= self.cfg.max_bytes || self.cfg.wait_ms == 0 {
            self.flush(db, schema, vec![slot]).await;
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
                        me.flush(b.db, b.schema, b.slots).await;
                    }
                });
            }
            if let Some(b) = full {
                self.flush(b.db, b.schema, b.slots).await;
            }
        }
        rx.await.map_err(|_| Error::Unavailable {
            message: "the insert was interrupted".into(),
            retry_after_ms: Some(500),
            outcome_unknown: true,
        })?
    }

    /// Stages the slots' rows as parts and binds them, answering each slot.
    async fn flush(&self, db: Arc<Db>, schema: EventSchema, slots: Vec<Slot>) {
        let mut ranges = Vec::with_capacity(slots.len());
        let mut by_partition: BTreeMap<u32, Vec<PosRow>> = BTreeMap::new();
        let mut ordinal = 0u32;
        let mut senders = Vec::with_capacity(slots.len());
        let mut bind_slots = Vec::with_capacity(slots.len());
        for s in slots {
            let start = ordinal;
            for row in s.rows {
                let partition = crate::engine::events::partition_key(schema.bucket(schema.time_of(&row)));
                by_partition.entry(partition).or_default().push(PosRow { pos: Pos { version: 0, ordinal }, row });
                ordinal += 1;
            }
            ranges.push(start..ordinal);
            senders.push(s.tx);
            bind_slots.push(BindSlot { commit_id: s.commit_id, deadline: Some(s.deadline) });
        }
        let shared = db.shared().clone();
        let mut parts = Vec::new();
        let mut failed = None;
        'write: for (partition, rows) in by_partition {
            for chunk in split_rows(rows, shared.cfg.event_insert_part_max_bytes) {
                match write_rows(&shared, &db.id, &schema, partition, chunk, false).await {
                    Ok((part, src)) => {
                        let mut offsets: Vec<Vec<u32>> = vec![Vec::new(); ranges.len()];
                        for (i, r) in src.sorted().iter().enumerate() {
                            let slot = ranges.partition_point(|rg| rg.end <= r.pos.ordinal);
                            offsets[slot].push(i as u32);
                        }
                        parts
                            .push(StagedPart { part, slot_rows: offsets.into_iter().map(Runs::from_sorted).collect() });
                    }
                    Err(e) => {
                        failed = Some(e);
                        break 'write;
                    }
                }
            }
        }
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
