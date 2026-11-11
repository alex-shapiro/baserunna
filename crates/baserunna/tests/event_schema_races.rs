//! Verifies that acknowledged event data is preserved across concurrent schema changes.
//! This file tests column drops during bulk inserts and retention changes during expired-part drops
//! (TODO.md items "Reject binds if a target column was dropped after validation" and
//! "Recheck table retention before committing expired-part drops").
//!
//! Each test drives a single node with production defaults through the HTTP API. The store adds
//! [`STORE_LATENCY_MS`] to each request so that concurrent requests can overlap in test-clock time.

use baserunna::sim::cost::{Resp, World, call_router, run_paused};
use bytes::Bytes;
use serde_json::{Value as Json, json};
use std::time::Duration;
use tokio::time::Instant;

/// Store latency for each request. S3 `PUT` latencies are typically 20 ms to 100 ms.
const STORE_LATENCY_MS: u64 = 20;
const QUERY_TO_MS: i64 = 9_999_999_999_999;
const HOUR_MS: u64 = 3_600_000;

fn excerpt(r: &Resp) -> String {
    String::from_utf8_lossy(&r.body[..r.body.len().min(300)]).into_owned()
}

async fn call(w: &World, method: &str, path: &str, token: &str, body: Json) -> Resp {
    let body = if body.is_null() { Bytes::new() } else { Bytes::from(body.to_string()) };
    call_router(&w.node(0).router().expect("router"), method, path, token, body).await
}

/// Sends a request from a separate client task and returns the response with its arrival time.
fn spawn_call(
    w: &World,
    method: &'static str,
    path: String,
    token: &str,
    body: Json,
) -> tokio::task::JoinHandle<(Resp, Instant)> {
    let (router, token) = (w.node(0).router().expect("router"), token.to_string());
    tokio::spawn(async move {
        let r = call_router(&router, method, &path, &token, Bytes::from(body.to_string())).await;
        (r, Instant::now())
    })
}

async fn setup(latency_ms: u64, table: Json) -> (World, String) {
    let w = World::new(1, latency_ms).await;
    let token = w.signup(0, "owner@races.test").await;
    w.create_db(0, &token, "d").await;
    let r = call(&w, "POST", "/v1/dbs/d/events", &token, table).await;
    assert_eq!(r.status, 201, "create table: {}", excerpt(&r));
    (w, token)
}

/// Returns the rows of a strong row query on table `ev` for `columns`, or the error response.
async fn strong_rows(w: &World, token: &str, columns: &[&str], from: i64, to: i64) -> Result<Vec<Json>, String> {
    let q = json!({"from": from, "to": to, "columns": columns, "consistency": "strong", "limit": 10_000});
    let r = call(w, "POST", "/v1/dbs/d/events/ev/rows/query", token, q).await;
    match r.status {
        200 => Ok(r.json()["rows"].as_array().cloned().unwrap_or_default()),
        s => Err(format!("{s} {}", excerpt(&r))),
    }
}

// ---------------------------------------------------------------------------------------------
// Bind after a column drop
// ---------------------------------------------------------------------------------------------

/// Schema change on column `x` while a bulk insert that includes `x` values is buffered.
#[derive(Clone, Copy, Debug)]
enum ColumnChange {
    None,
    Drop,
    DropAndReAdd,
}

const BULK_ROWS: i64 = 4;
const BULK_TIME_MS: i64 = 1_749_000_000_000;

/// Client-observed state after a bulk insert races with a column change.
#[derive(Debug)]
struct BindRace {
    insert_status: u16,
    insert_body: String,
    insert_version: Option<u64>,
    insert_answered_ms: u64,
    /// Arrival time and commit version of each column change response.
    change_answers: Vec<(u64, u64)>,
    /// `[time, x]` of each row, or `[time]` if `x` is dropped.
    rows: Result<Vec<Json>, String>,
}

impl BindRace {
    /// Returns true if the insert committed at a version after the last column change.
    fn committed_after_change(&self) -> bool {
        let last_change = self.change_answers.last().map_or(0, |(_, v)| *v);
        (200..300).contains(&self.insert_status) && self.insert_version.is_some_and(|v| v > last_change)
    }

    /// Returns true if the insert committed after the column change and a row returns null for `x`.
    fn lost_values(&self) -> bool {
        let rows = self.rows.as_deref().unwrap_or_default();
        self.committed_after_change() && rows.iter().any(|r| r.get(1).is_some_and(Json::is_null))
    }
}

/// Sends a bulk insert of [`BULK_ROWS`] rows with `x` values, then applies `change` after `change_after_ms`.
fn bind_race(latency_ms: u64, change: ColumnChange, change_after_ms: u64) -> BindRace {
    run_paused(async move {
        let table = json!({"name": "ev", "columns": [{"name": "time", "type": "time"}, {"name": "x", "type": "int"}],
            "time_column": "time", "order_by": ["time"]});
        let (w, token) = setup(latency_ms, table).await;
        let rows: Vec<Json> = (0..BULK_ROWS).map(|i| json!({"time": BULK_TIME_MS + i, "x": 100 + i})).collect();
        let start = Instant::now();
        let ms = |t: Instant| t.duration_since(start).as_millis() as u64;
        let insert = spawn_call(&w, "POST", "/v1/dbs/d/events/ev/rows".into(), &token, Json::Array(rows));
        tokio::time::sleep(Duration::from_millis(change_after_ms)).await;
        let mut change_answers = Vec::new();
        if matches!(change, ColumnChange::Drop | ColumnChange::DropAndReAdd) {
            let r = call(&w, "DELETE", "/v1/dbs/d/events/ev/columns/x", &token, Json::Null).await;
            assert_eq!(r.status, 200, "drop column: {}", excerpt(&r));
            change_answers.push((ms(Instant::now()), r.json()["version"].as_u64().expect("drop version")));
        }
        if matches!(change, ColumnChange::DropAndReAdd) {
            let r = call(&w, "POST", "/v1/dbs/d/events/ev/columns", &token, json!({"name": "x", "type": "int"})).await;
            assert_eq!(r.status, 200, "re-add column: {}", excerpt(&r));
            change_answers.push((ms(Instant::now()), r.json()["version"].as_u64().expect("re-add version")));
        }
        let (r, answered) = insert.await.expect("insert task");
        let columns: &[&str] = match change {
            ColumnChange::Drop => &["time"],
            _ => &["time", "x"],
        };
        let rows = strong_rows(&w, &token, columns, 0, QUERY_TO_MS).await;
        w.shutdown();
        BindRace {
            insert_status: r.status,
            insert_body: excerpt(&r),
            insert_version: r.json()["version"].as_u64(),
            insert_answered_ms: ms(answered),
            change_answers,
            rows,
        }
    })
}

/// Asserts that the insert either fails or preserves every inserted row and its `x` value.
fn assert_insert_kept_or_rejected(race: &BindRace, change: ColumnChange) {
    eprintln!("{change:?}: {race:?}");
    if !(200..300).contains(&race.insert_status) {
        return;
    }
    let rows = race.rows.as_ref().expect("the strong query succeeds");
    assert_eq!(rows.len() as i64, BULK_ROWS, "the acknowledged insert lost rows: {rows:?}");
    if race.committed_after_change() && !matches!(change, ColumnChange::Drop) {
        let xs: Vec<&Json> = rows.iter().filter_map(|r| r.get(1)).collect();
        let expected: Vec<Json> = (0..BULK_ROWS).map(|i| json!(100 + i)).collect();
        assert_eq!(
            xs,
            expected.iter().collect::<Vec<_>>(),
            "the insert committed after the column change {:?} and was acknowledged with {} {}, \
             but its x values were read back as {xs:?}",
            race.change_answers,
            race.insert_status,
            race.insert_body
        );
    }
}

#[test]
fn bulk_insert_keeps_x_values_without_a_column_change() {
    let race = bind_race(STORE_LATENCY_MS, ColumnChange::None, 10);
    assert_insert_kept_or_rejected(&race, ColumnChange::None);
}

#[test]
fn bulk_insert_keeps_its_rows_after_a_column_drop() {
    let race = bind_race(STORE_LATENCY_MS, ColumnChange::Drop, 10);
    assert_insert_kept_or_rejected(&race, ColumnChange::Drop);
}

#[test]
#[ignore = "TODO.md: Reject binds if a target column was dropped after validation"]
fn bulk_insert_keeps_x_values_after_a_column_drop_and_re_add() {
    let race = bind_race(STORE_LATENCY_MS, ColumnChange::DropAndReAdd, 10);
    assert!(race.committed_after_change(), "setup: the insert commits after the re-add: {race:?}");
    assert_insert_kept_or_rejected(&race, ColumnChange::DropAndReAdd);
}

/// Tests column drops and re-adds across a range of delays after a bulk insert starts.
#[test]
#[ignore = "TODO.md: Reject binds if a target column was dropped after validation"]
fn bulk_insert_keeps_x_values_at_every_drop_and_re_add_delay() {
    let mut lost = Vec::new();
    for latency_ms in [0, STORE_LATENCY_MS, 50] {
        for after_ms in (0..=400).step_by(10) {
            let race = bind_race(latency_ms, ColumnChange::DropAndReAdd, after_ms);
            eprintln!(
                "latency {latency_ms:>2} ms, change after {after_ms:>3} ms: insert {} v{:?} at {} ms, \
                 changes (ms, version) {:?}, committed after change: {}, lost x: {}",
                race.insert_status,
                race.insert_version,
                race.insert_answered_ms,
                race.change_answers,
                race.committed_after_change(),
                race.lost_values()
            );
            if race.lost_values() {
                lost.push((latency_ms, after_ms));
            }
        }
    }
    assert!(lost.is_empty(), "acknowledged x values were read as null at (latency ms, change delay ms) {lost:?}");
}

// ---------------------------------------------------------------------------------------------
// Retention recheck
// ---------------------------------------------------------------------------------------------

const OLD_ROWS: usize = 10;

/// Operation that triggers background maintenance in a retention race.
#[derive(Clone, Copy, Debug)]
enum MaintenanceTrigger {
    /// A transactional insert of 1 row.
    SmallInsert,
    /// A transactional insert exceeding `event_seal_bytes` (128 KiB), which causes maintenance to
    /// seal rows between planning and committing the expired-part drop.
    SealingInsert,
}

/// Result of a strong row query and a strong table read during a retention race.
#[derive(Debug)]
struct RetentionAnswer {
    sent_ms: u64,
    answered_ms: u64,
    old_rows: Result<usize, String>,
    /// Version of the table read and whether it includes the expired part.
    table_version: u64,
    has_old_part: bool,
}

#[derive(Debug)]
struct RetentionRace {
    trigger_answered_ms: u64,
    clear_answered_ms: u64,
    clear_version: u64,
    answers: Vec<RetentionAnswer>,
}

impl RetentionRace {
    /// Returns `(seen_ms, gone_ms)` if a strong query sent after the retention change returned
    /// the old rows and a subsequent query did not.
    fn visible_then_gone(&self) -> Option<(u64, u64)> {
        let mut seen = None;
        for a in self.answers.iter().filter(|a| a.sent_ms >= self.clear_answered_ms) {
            match (&a.old_rows, seen) {
                (Ok(n), None) if *n == OLD_ROWS => seen = Some(a.answered_ms),
                (Ok(n), Some(s)) if *n < OLD_ROWS => return Some((s, a.answered_ms)),
                _ => {}
            }
        }
        None
    }

    fn old_rows_kept(&self) -> bool {
        self.answers.last().is_some_and(|a| a.old_rows == Ok(OLD_ROWS))
    }

    /// Returns the last table version with the old part and the first table version without it.
    fn drop_bounds(&self) -> (Option<u64>, Option<u64>) {
        let last_with = self.answers.iter().filter(|a| a.has_old_part).map(|a| a.table_version).max();
        let first_without = self.answers.iter().filter(|a| !a.has_old_part).map(|a| a.table_version).min();
        (last_with, first_without)
    }

    /// Returns true if the drop committed at a version after the retention change.
    fn dropped_after_change(&self) -> bool {
        let (last_with, first_without) = self.drop_bounds();
        first_without.is_some() && last_with.is_some_and(|v| v >= self.clear_version)
    }

    /// Returns true if the drop committed at a version before the retention change.
    fn dropped_before_change(&self) -> bool {
        self.drop_bounds().1.is_some_and(|v| v <= self.clear_version)
    }

    fn verdict(&self) -> &'static str {
        match () {
            _ if self.old_rows_kept() => "kept",
            _ if self.visible_then_gone().is_some() => "visible after the change, then dropped",
            _ if self.dropped_after_change() => "dropped after the change, never visible",
            _ if self.dropped_before_change() => "dropped before the change",
            _ => "dropped, order unknown",
        }
    }
}

/// Returns the table version and whether the table lists a part that ends before `old_to`.
async fn table_has_old_part(w: &World, token: &str, old_to: i64) -> (u64, bool) {
    let r = call(w, "GET", "/v1/dbs/d/events/ev", token, Json::Null).await;
    let j = r.json();
    let parts = j["parts"].as_array().cloned().unwrap_or_default();
    let has_old = parts.iter().any(|p| p["max_time"].as_i64().is_some_and(|t| t < old_to));
    (j["version"].as_u64().unwrap_or(0), has_old)
}

/// Inserts [`OLD_ROWS`] rows with 1 h retention, waits for them to expire, triggers maintenance,
/// and clears retention after `clear_after_ms`. Polls strong queries every 1 ms for 2 s.
fn retention_race(latency_ms: u64, trigger: MaintenanceTrigger, clear_after_ms: u64) -> RetentionRace {
    run_paused(async move {
        let table = json!({"name": "ev", "columns": [{"name": "time", "type": "time"}, {"name": "pad", "type": "string"}],
            "time_column": "time", "order_by": ["time"], "partition": "hour", "retention_ms": HOUR_MS});
        let (w, token) = setup(latency_ms, table).await;
        let old_from = (w.node(0).env.now_ms() - 60_000) as i64;
        let old_to = old_from + OLD_ROWS as i64;
        let rows: Vec<Json> = (0..OLD_ROWS as i64).map(|i| json!({"time": old_from + i, "pad": "old"})).collect();
        let r = call(&w, "POST", "/v1/dbs/d/events/ev/rows", &token, Json::Array(rows)).await;
        assert_eq!(r.status, 201, "bulk insert: {}", excerpt(&r));
        assert_eq!(strong_rows(&w, &token, &["time"], old_from, old_to).await.map(|r| r.len()), Ok(OLD_ROWS));
        tokio::time::sleep(Duration::from_millis(2 * HOUR_MS)).await;
        let hidden = strong_rows(&w, &token, &["time"], old_from, old_to).await.map(|r| r.len());
        assert_eq!(hidden, Ok(0), "setup: retention hides the expired rows");
        assert!(table_has_old_part(&w, &token, old_to).await.1, "setup: the expired part is not dropped yet");

        let now = w.node(0).env.now_ms() as i64;
        let (count, pad) = match trigger {
            MaintenanceTrigger::SmallInsert => (1, 1),
            MaintenanceTrigger::SealingInsert => (1_500, 120),
        };
        let new_rows: Vec<Json> = (0..count).map(|i| json!({"time": now + i, "pad": "n".repeat(pad)})).collect();
        let body = json!({"ops": [{"op": "insert_events", "table": "ev", "rows": new_rows}]});
        let start = Instant::now();
        let ms = |t: Instant| t.duration_since(start).as_millis() as u64;
        let r = call(&w, "POST", "/v1/dbs/d/txn", &token, body).await;
        assert_eq!(r.status, 200, "trigger insert: {}", excerpt(&r));
        let trigger_answered_ms = ms(Instant::now());
        tokio::time::sleep(Duration::from_millis(clear_after_ms)).await;
        let mut clear = spawn_call(&w, "PATCH", "/v1/dbs/d/events/ev".into(), &token, json!({"retention_ms": null}));
        let mut answers = Vec::new();
        let mut clear_answer = None;
        let until = Instant::now() + Duration::from_secs(2);
        while Instant::now() < until {
            if clear_answer.is_none() && clear.is_finished() {
                clear_answer = Some((&mut clear).await.expect("clear task"));
            }
            let sent_ms = ms(Instant::now());
            let old_rows = strong_rows(&w, &token, &["time"], old_from, old_to).await.map(|r| r.len());
            let answered_ms = ms(Instant::now());
            let (table_version, has_old_part) = table_has_old_part(&w, &token, old_to).await;
            answers.push(RetentionAnswer { sent_ms, answered_ms, old_rows, table_version, has_old_part });
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let (clear_resp, clear_at) = clear_answer.expect("the retention change is answered within 2 s");
        assert_eq!(clear_resp.status, 200, "clear retention: {}", excerpt(&clear_resp));
        w.shutdown();
        RetentionRace {
            trigger_answered_ms,
            clear_answered_ms: ms(clear_at),
            clear_version: clear_resp.json()["version"].as_u64().expect("clear version"),
            answers,
        }
    })
}

fn summary(race: &RetentionRace) -> String {
    format!(
        "trigger answered at {} ms, clear answered at {} ms at v{}, old part last/first-missing version {:?}, \
         visible then gone {:?}: {}",
        race.trigger_answered_ms,
        race.clear_answered_ms,
        race.clear_version,
        race.drop_bounds(),
        race.visible_then_gone(),
        race.verdict()
    )
}

/// Asserts that the old rows are preserved unless the drop committed before the retention change.
fn assert_rows_kept_after_change(label: &str, race: &RetentionRace) {
    eprintln!("{label}: {}", summary(race));
    let mut last = None;
    for a in &race.answers {
        let now = (&a.old_rows, a.has_old_part);
        if last.as_ref() != Some(&now) {
            eprintln!(
                "  sent {:>4} ms answered {:>4} ms: old rows {:?}; table v{} lists the old part: {}",
                a.sent_ms, a.answered_ms, a.old_rows, a.table_version, a.has_old_part
            );
            last = Some(now);
        }
    }
    assert!(race.old_rows_kept() || race.dropped_before_change(), "{label}: {}", race.verdict());
}

#[test]
#[ignore = "TODO.md: Recheck table retention before committing expired-part drops"]
fn clearing_retention_during_a_sealing_maintenance_round_keeps_the_rows() {
    let race = retention_race(STORE_LATENCY_MS, MaintenanceTrigger::SealingInsert, 0);
    assert_rows_kept_after_change("sealing trigger, clear after 0 ms", &race);
}

#[test]
fn clearing_retention_during_a_small_maintenance_round_keeps_the_rows() {
    let race = retention_race(STORE_LATENCY_MS, MaintenanceTrigger::SmallInsert, 0);
    assert_rows_kept_after_change("small trigger, clear after 0 ms", &race);
}

/// Tests clearing retention across a range of delays after maintenance starts.
#[test]
#[ignore = "TODO.md: Recheck table retention before committing expired-part drops"]
fn clearing_retention_at_every_delay_keeps_the_rows() {
    let mut lost = Vec::new();
    for trigger in [MaintenanceTrigger::SmallInsert, MaintenanceTrigger::SealingInsert] {
        for latency_ms in [0, STORE_LATENCY_MS, 50] {
            for after_ms in (0..=300).step_by(10) {
                let race = retention_race(latency_ms, trigger, after_ms);
                eprintln!("{trigger:?} latency {latency_ms:>2} ms, clear after {after_ms:>3} ms: {}", summary(&race));
                if !race.old_rows_kept() && !race.dropped_before_change() {
                    lost.push((format!("{trigger:?}"), latency_ms, after_ms, race.verdict()));
                }
            }
        }
    }
    assert!(lost.is_empty(), "rows lost after the change at (trigger, latency ms, delay ms, verdict) {lost:?}");
}
