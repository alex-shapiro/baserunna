//! Simulation workload for `baserunna-client` under network, process, and storage faults.
//!
//! Counter tasks increment keys with a version precondition and an idempotency key per transaction
//! to verify that retries do not apply a write twice or report an applied write as `NotApplied`.
//! Mutator tasks write, copy, and delete binary keys while a watcher task reconstructs the key
//! space from the change feed and compares the final state against a full scan.
//! Ledger tasks move balances between table rows in interactive transactions that also count and
//! log each transfer, to verify that retried and resolved commits apply exactly once.

use super::super::Sim;
use super::super::client::{Client as SimClient, ZOMBIE_WAIT_MS};
use super::{chaos_phase, expect, settle};
use baserunna_api::{ColumnType, NewColumn, NewTable};
use baserunna_api::{ErrorCode, Outcome, Role};
use baserunna_client::{
    Change, ChangesRequest, Client, Db, Entry, Error, Expect, KeyRange, Op, OpResult, Row, Txn, TxnError, Value,
};
use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

const DB: &str = "client";
const ACCOUNTS: u64 = 4;
const OPENING_BALANCE: i64 = 100;
const TRANSFERS: &str = "transfers";
const TRANSFER_COUNT: &str = "transfer_count";
/// ID of the record that creates the transfer log.
const OPENING: &str = "opening";

/// Workload size.
pub struct Size {
    pub counters: usize,
    pub mutators: usize,
    pub ledgers: usize,
    pub steps: usize,
}

pub async fn run(sim: Arc<Sim>, size: Size, chaos: bool) -> Result<(), String> {
    let Size { counters, mutators, ledgers, steps } = size;
    let client = signed_up(&sim, "client-admin", "client@client.test").await?;
    let db = client.db(DB);
    retry(&sim, || db.ensure()).await?;
    open_accounts(&sim, &db).await?;
    let phase = chaos.then(|| chaos_phase(&sim));
    let watcher = tokio::spawn(watch(sim.clone(), client.db(DB), steps));
    let mut tasks = Vec::new();
    for i in 0..counters {
        let db = with_name(&sim, &client, &format!("counter{i}")).db(DB);
        tasks.push(tokio::spawn(count(sim.clone(), db, i, steps)));
    }
    for i in 0..mutators {
        let db = with_name(&sim, &client, &format!("mutator{i}")).db(DB);
        tasks.push(tokio::spawn(mutate(sim.clone(), db, i, steps)));
    }
    let tally = Arc::new(Tally::default());
    for i in 0..ledgers {
        let db = with_name(&sim, &client, &format!("ledger{i}")).db(DB);
        tasks.push(tokio::spawn(transfer(sim.clone(), db, i, steps, tally.clone())));
    }
    for t in tasks {
        t.await.map_err(|e| e.to_string())??;
    }
    settle(&sim, phase).await;
    audit(&sim, &client.db(DB), &tally).await?;
    let replica = watcher.await.map_err(|e| e.to_string())??;
    let db = client.db(DB);
    let replica = catch_up(&sim, &db, replica).await?;
    let state = scan_all(&sim, &db).await?;
    expect(replica.keys == state, format!("change feed replica {:?} differs from scan {state:?}", replica.keys))?;
    accounts(&sim, &client).await
}

fn client(sim: &Arc<Sim>, name: &str) -> Client {
    let mut t = SimClient::new(sim, name);
    t.retry_rate_limited = false;
    let rng = parking_lot::Mutex::new(sim.fork_rng());
    Client::new(t).with_entropy(move || rng.lock().next_u64())
}

async fn signed_up(sim: &Arc<Sim>, name: &str, email: &str) -> Result<Client, String> {
    let c = client(sim, name);
    let password = "client-password";
    at_least_once(sim, ErrorCode::AlreadyExists, || async { c.sign_up(email, password).await.map(drop) }).await?;
    let session = retry(sim, || c.log_in(email, password)).await?;
    Ok(c.with_token(&session.token))
}

fn with_name(sim: &Arc<Sim>, c: &Client, name: &str) -> Client {
    client(sim, name).with_token(c.token().unwrap_or_default())
}

async fn at_least_once<F: Future<Output = Result<(), Error>>>(
    sim: &Arc<Sim>,
    done: ErrorCode,
    mut f: impl FnMut() -> F,
) -> Result<(), String> {
    retry(sim, || {
        let r = f();
        async move {
            match r.await {
                Err(e) if e.code() == Some(done) => Ok(()),
                r => r,
            }
        }
    })
    .await
}

fn check(cond: bool, what: &str) -> Result<(), Error> {
    match cond {
        true => Ok(()),
        false => Err(Error::Malformed { path: what.into(), reason: "the check failed".into() }),
    }
}

async fn definitive<T: std::fmt::Debug, F: Future<Output = Result<T, Error>>>(
    sim: &Arc<Sim>,
    mut f: impl FnMut() -> F,
) -> Result<Option<ErrorCode>, String> {
    for _ in 0..400 {
        match f().await {
            Ok(v) => return Err(format!("unexpected success {v:?}")),
            Err(e) if e.is_transient(true) || e.restarts_txn() || matches!(e, Error::Transport(_)) => {
                sim.sleep(1_000).await
            }
            Err(e) => return Ok(e.code()),
        }
    }
    Err("a request never failed definitively".into())
}

async fn retry<T, F: Future<Output = Result<T, Error>>>(sim: &Arc<Sim>, mut f: impl FnMut() -> F) -> Result<T, String> {
    for _ in 0..400 {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) if e.is_transient(true) || e.restarts_txn() || matches!(e, Error::Transport(_)) => {
                sim.sleep(1_000).await
            }
            Err(e) => return Err(e.to_string()),
        }
    }
    Err("a request never succeeded".into())
}

async fn count(sim: Arc<Sim>, db: Db, i: usize, steps: usize) -> Result<(), String> {
    let key = Bytes::from(format!("ctr/{i}"));
    let (mut n, mut version) = (0u64, None);
    for step in 0..steps {
        sim.sleep(sim.rand_below(500)).await;
        let cond = version.map_or(Expect::Absent, Expect::Version);
        let ops = [
            Op::Check { key: key.clone(), expect: cond },
            Op::Put { key: key.clone(), value: (n + 1).to_string().into() },
        ];
        match db.txn(&ops, Some(&format!("ctr{i}-{step}"))).await {
            Ok(out) => (n, version) = (n + 1, Some(out.version)),
            Err(e) if e.outcome() == Outcome::NotApplied && e.code() != Some(ErrorCode::PreconditionFailed) => {}
            Err(e) if e.outcome() == Outcome::Unknown => {
                // Wait for any in-flight attempt to time out on the server before reading the counter.
                sim.sleep(ZOMBIE_WAIT_MS).await;
                let (got, v) = read_counter(&sim, &db, &key).await?;
                expect(got == n || got == n + 1, format!("counter {i} is {got} after {n} increments and a failure"))?;
                (n, version) = (got, v);
            }
            Err(e) => return Err(format!("counter {i} at {n}: {e}")),
        }
    }
    let (got, _) = read_counter(&sim, &db, &key).await?;
    expect(got == n, format!("counter {i} is {got}, expected {n}"))
}

#[derive(Debug, Serialize, Deserialize)]
struct Account {
    id: String,
    balance: i64,
}

impl Row for Account {
    const TABLE: &'static str = "accounts";
}

#[derive(Debug, Serialize, Deserialize)]
struct Transfer {
    id: String,
    from: String,
    to: String,
    amount: i64,
}

/// Outcomes of ledger transactions.
#[derive(Default)]
struct Tally {
    committed: Mutex<BTreeSet<String>>,
    unknown: Mutex<BTreeSet<String>>,
}

#[derive(Debug)]
enum LedgerError {
    Client(Error),
    Overdrawn,
}

impl From<Error> for LedgerError {
    fn from(e: Error) -> LedgerError {
        LedgerError::Client(e)
    }
}

impl TxnError for LedgerError {
    fn client_error(&self) -> Option<&Error> {
        match self {
            LedgerError::Client(e) => Some(e),
            LedgerError::Overdrawn => None,
        }
    }
}

async fn open_accounts(sim: &Arc<Sim>, db: &Db) -> Result<(), String> {
    let table = NewTable {
        name: Account::TABLE.into(),
        columns: vec![NewColumn::new("id", ColumnType::String), NewColumn::new("balance", ColumnType::Int)],
        primary_key: vec!["id".into()],
        indexes: vec![],
        if_not_exists: true,
    };
    retry(sim, || db.create_table(&table)).await?;
    let mut opened: Vec<Op> =
        (0..ACCOUNTS).map(|a| Op::upsert(&Account { id: format!("a{a}"), balance: OPENING_BALANCE })).collect();
    let open = Transfer { id: OPENING.into(), from: String::new(), to: String::new(), amount: 0 };
    opened.push(Op::append(TRANSFERS, &[open]));
    retry(sim, || db.write(&opened)).await.map(drop)
}

async fn transfer(sim: Arc<Sim>, db: Db, i: usize, steps: usize, tally: Arc<Tally>) -> Result<(), String> {
    for step in 0..steps {
        sim.sleep(sim.rand_below(500)).await;
        let (from, to) = (format!("a{}", sim.rand_below(ACCOUNTS)), format!("a{}", sim.rand_below(ACCOUNTS)));
        let amount = 1 + sim.rand_below(60) as i64;
        let t = Transfer { id: format!("t{i}-{step}"), from, to, amount };
        let mut retry = db.retry();
        let r = loop {
            let mut txn = db.begin();
            let r = move_funds(&mut txn, &t).await;
            match retry.finish(txn, r).await {
                Ok(None) => {}
                Ok(Some(())) => break Ok(()),
                Err(e) => break Err(e),
            }
        };
        match r {
            Ok(()) => {
                tally.committed.lock().insert(t.id.clone());
            }
            Err(LedgerError::Overdrawn) => {}
            Err(LedgerError::Client(e)) if e.outcome() == Outcome::Unknown => {
                tally.unknown.lock().insert(t.id.clone());
            }
            Err(LedgerError::Client(e)) if e.restarts_txn() => {}
            Err(LedgerError::Client(e)) => return Err(format!("ledger {i} transfer {}: {e}", t.id)),
        }
    }
    Ok(())
}

async fn move_funds(txn: &mut Txn, t: &Transfer) -> Result<(), LedgerError> {
    let missing = |id: &str| Error::Decode { what: id.into(), reason: "no account".into() };
    let from = txn.run_one(Op::get_row::<Account>(&t.from)).await?.row::<Account>()?.ok_or_else(|| missing(&t.from))?;
    if from.balance < t.amount {
        return Err(LedgerError::Overdrawn);
    }
    let to = txn.run(&[Op::get_row::<Account>(&t.to)]).await?.swap_remove(0).row::<Account>()?;
    let to = to.ok_or_else(|| missing(&t.to))?;
    let moved = |a: Account, by: i64| Account { balance: a.balance + by, ..a };
    match t.from == t.to {
        true => txn.write([Op::upsert(&from)]),
        false => txn.write([Op::upsert(&moved(from, -t.amount)), Op::upsert(&moved(to, t.amount))]),
    }
    txn.write([Op::increment(TRANSFER_COUNT, 1), Op::append(TRANSFERS, &[t])]);
    Ok(())
}

/// Checks that balances are conserved, that each acknowledged transfer applied exactly once, and
/// that no transfer applied unless it was acknowledged or its outcome was unknown.
async fn audit(sim: &Arc<Sim>, db: &Db, tally: &Tally) -> Result<(), String> {
    let (accounts, transfers, count) = retry(sim, || read_ledger(db)).await?;
    let total: i64 = accounts.iter().map(|a| a.balance).sum();
    expect(accounts.len() as u64 == ACCOUNTS, format!("{} of {ACCOUNTS} accounts", accounts.len()))?;
    expect(total == ACCOUNTS as i64 * OPENING_BALANCE, format!("balances sum to {total}"))?;
    expect(accounts.iter().all(|a| a.balance >= 0), format!("an account is overdrawn: {accounts:?}"))?;
    expect(count == transfers.len() as u64, format!("{count} counted transfers but {} logged", transfers.len()))?;
    let logged: Vec<&str> = transfers.iter().map(|t| t.id.as_str()).collect();
    check_transfers(&tally.committed.lock(), &tally.unknown.lock(), &logged)
}

/// Checks that the `logged` transfer IDs hold each `committed` ID once, and otherwise only IDs whose
/// outcome was `unknown`.
fn check_transfers(committed: &BTreeSet<String>, unknown: &BTreeSet<String>, logged: &[&str]) -> Result<(), String> {
    let ids: BTreeSet<&str> = logged.iter().copied().collect();
    expect(ids.len() == logged.len(), "a transfer was logged twice")?;
    let lost: Vec<&String> = committed.iter().filter(|id| !ids.contains(id.as_str())).collect();
    expect(lost.is_empty(), format!("acknowledged transfers {lost:?} were not applied"))?;
    let stray: Vec<&&str> = ids.iter().filter(|id| !committed.contains(**id) && !unknown.contains(**id)).collect();
    expect(stray.is_empty(), format!("transfers {stray:?} applied after they failed"))
}

/// Reads the accounts, the logged transfers after the opening record, and the transfer count.
async fn read_ledger(db: &Db) -> Result<(Vec<Account>, Vec<Transfer>, u64), Error> {
    let mut retry = db.retry();
    loop {
        let mut txn = db.begin();
        let r = ledger(&mut txn).await;
        if let Some(r) = retry.finish(txn, r).await? {
            return Ok(r);
        }
    }
}

async fn ledger(txn: &mut Txn) -> Result<(Vec<Account>, Vec<Transfer>, u64), Error> {
    let (mut accounts, mut transfers, mut count) = (Vec::new(), Vec::new(), 0);
    let mut pending = vec![Op::query_all::<Account>(), Op::read_stream(TRANSFERS, 0), Op::get(TRANSFER_COUNT)];
    while !pending.is_empty() {
        let results = txn.run(&pending).await?;
        let mut next = Vec::new();
        for (op, r) in pending.iter().zip(results) {
            next.extend(op.next_page(&r));
            match r {
                r @ OpResult::QueryRows(_) => accounts.extend(r.rows::<Account>()?),
                r @ OpResult::ReadStream(_) => transfers.extend(r.records::<Transfer>()?),
                r => count = counter(r.entry()?.as_ref())?,
            }
        }
        pending = next;
    }
    transfers.retain(|t: &Transfer| t.id != OPENING);
    Ok((accounts, transfers, count))
}

fn counter(e: Option<&Entry>) -> Result<u64, Error> {
    let Some(e) = e else { return Ok(0) };
    let text = e.value.data().and_then(|d| std::str::from_utf8(d).ok());
    text.and_then(|t| t.parse().ok())
        .ok_or_else(|| Error::Decode { what: TRANSFER_COUNT.into(), reason: format!("{e:?}") })
}

async fn read_counter(sim: &Arc<Sim>, db: &Db, key: &Bytes) -> Result<(u64, Option<u64>), String> {
    let get = [Op::Get { key: key.clone() }];
    let out = retry(sim, || db.txn(&get, None)).await?;
    match out.results.as_deref() {
        Some([OpResult::Get(None)]) => Ok((0, None)),
        Some([OpResult::Get(Some(e))]) => match &e.value {
            Value::Inline(b) => {
                let n = std::str::from_utf8(b).ok().and_then(|s| s.parse().ok()).ok_or("counter is not a number")?;
                Ok((n, Some(e.version.ok_or("counter has no committed version")?)))
            }
            v => Err(format!("counter holds {v:?}")),
        },
        r => Err(format!("unexpected get result {r:?}")),
    }
}

fn mutator_key(i: usize, k: u64) -> Bytes {
    let mut b = format!("m{i}/").into_bytes();
    b.extend_from_slice(&[[b'a', 0xff, 0x00, b'/'][k as usize % 4], b'0' + (k / 4) as u8]);
    Bytes::from(b)
}

async fn mutate(sim: Arc<Sim>, db: Db, i: usize, steps: usize) -> Result<(), String> {
    for step in 0..steps {
        sim.sleep(sim.rand_below(500)).await;
        let key = mutator_key(i, sim.rand_below(12));
        let op = match sim.rand_below(6) {
            0 => Op::Delete { key },
            1 => Op::DeleteRange { range: KeyRange::Prefix(key.slice(..key.len() - 1)) },
            2 => Op::Copy { from: mutator_key(i, sim.rand_below(12)), to: key },
            3 => {
                let data = Bytes::from(vec![step as u8; 1 + sim.rand_below(3000) as usize]);
                match db.stage_blob(data).await {
                    Ok(b) => Op::PutBlob { key, blob_id: b.blob_id },
                    Err(_) => continue,
                }
            }
            _ => Op::Put { key, value: Bytes::from(format!("{i}-{step}")) },
        };
        match db.txn(std::slice::from_ref(&op), Some(&format!("m{i}-{step}"))).await {
            Ok(out) => expect(out.results.is_none_or(|r| r == [OpResult::Done]), "write operations return Done")?,
            Err(e) if e.code() == Some(ErrorCode::PreconditionFailed) && matches!(op, Op::Copy { .. }) => {}
            Err(e) if e.code() == Some(ErrorCode::Aborted) => {}
            Err(e) if e.outcome() == Outcome::Unknown || e.is_transient(true) => {}
            Err(e) if e.code() == Some(ErrorCode::NotFound) && matches!(op, Op::PutBlob { .. }) => {}
            Err(e) => return Err(format!("mutator {i}: {op:?}: {e}")),
        }
    }
    Ok(())
}

struct Replica {
    keys: BTreeMap<Bytes, (u64, Bytes)>,
    cursor: String,
}

impl Replica {
    fn apply(&mut self, c: Change) -> Result<(), String> {
        match c {
            Change::Put { key, version, value } => {
                self.keys.insert(key, (version, data(&value)?));
            }
            Change::Delete { key, .. } => {
                self.keys.remove(&key);
            }
            Change::DeleteRange { start, end, .. } => {
                self.keys.retain(|k, _| *k < start || end.as_ref().is_some_and(|e| k >= e));
            }
        }
        Ok(())
    }
}

fn data(v: &Value) -> Result<Bytes, String> {
    match v {
        Value::Inline(b) | Value::Blob { data: Some(b), .. } => Ok(b.clone()),
        Value::Blob { meta, data: None } => Err(format!("blob {} is missing inline data", meta.blob_id)),
    }
}

async fn watch(sim: Arc<Sim>, db: Db, steps: usize) -> Result<Replica, String> {
    let mut r = Replica { keys: BTreeMap::new(), cursor: "0".into() };
    for _ in 0..steps {
        match read_changes(&db, &mut r, 10_000).await {
            Ok(_) => {}
            Err(e) if e.code() == Some(ErrorCode::CursorExpired) => r = rescan(&sim, &db).await?,
            Err(_) => sim.sleep(1_000).await,
        }
    }
    Ok(r)
}

async fn read_changes(db: &Db, r: &mut Replica, wait_ms: u64) -> Result<bool, Error> {
    let req = ChangesRequest {
        range: KeyRange::Prefix(Bytes::new()),
        after: r.cursor.clone(),
        limit: 50,
        wait_ms,
        max_staleness_ms: (wait_ms > 0).then_some(30_000),
    };
    let page = db.changes(&req).await?;
    let done = page.changes.is_empty() && !page.more;
    for c in page.changes {
        r.apply(c).map_err(|reason| Error::Malformed { path: "changes".into(), reason })?;
    }
    r.cursor = page.cursor;
    Ok(done)
}

async fn catch_up(sim: &Arc<Sim>, db: &Db, mut r: Replica) -> Result<Replica, String> {
    for _ in 0..400 {
        match read_changes(db, &mut r, 0).await {
            Ok(true) => return Ok(r),
            Ok(false) => {}
            Err(e) if e.code() == Some(ErrorCode::CursorExpired) => r = rescan(sim, db).await?,
            Err(e) if e.is_transient(true) => sim.sleep(1_000).await,
            Err(e) => return Err(e.to_string()),
        }
    }
    Err("the change feed never caught up".into())
}

async fn rescan(sim: &Arc<Sim>, db: &Db) -> Result<Replica, String> {
    let ops = [Op::Scan {
        range: KeyRange::Prefix(Bytes::new()),
        limit: 1000,
        reverse: false,
        keys_only: false,
        cursor: None,
    }];
    let out = retry(sim, || db.txn(&ops, None)).await?;
    let Some([OpResult::Scan(page)]) = out.results.as_deref() else {
        return Err("unexpected scan result".into());
    };
    expect(page.cursor.is_none(), "the key space fits in a single page")?;
    let keys = page
        .entries
        .iter()
        .map(|e| Ok((e.key.clone(), (e.version.ok_or("scan entry has no committed version")?, data(&e.value)?))))
        .collect::<Result<_, String>>()?;
    Ok(Replica { keys, cursor: out.version.to_string() })
}

async fn scan_all(sim: &Arc<Sim>, db: &Db) -> Result<BTreeMap<Bytes, (u64, Bytes)>, String> {
    Ok(rescan(sim, db).await?.keys)
}

async fn accounts(sim: &Arc<Sim>, owner: &Client) -> Result<(), String> {
    let member = signed_up(sim, "client-member", "member@client.test").await?;
    let me = retry(sim, || member.me()).await?;
    expect(me.user.email == "member@client.test" && me.credential == baserunna_api::Credential::Session, "me")?;
    let bad = member.with_token("not-a-token");
    let code = definitive(sim, || bad.me()).await?;
    expect(code == Some(ErrorCode::Unauthenticated), format!("a bad token fails with {code:?}"))?;
    let get = [Op::Get { key: "k".into() }];
    let missing = owner.db("no-such-db");
    let code = definitive(sim, || missing.txn(&get, None)).await?;
    expect(code == Some(ErrorCode::NotFound), format!("a missing database fails with {code:?}"))?;
    let code = definitive(sim, || owner.create_db(DB, None)).await?;
    expect(code == Some(ErrorCode::AlreadyExists), format!("a second create fails with {code:?}"))?;

    at_least_once(sim, ErrorCode::AlreadyExists, || async {
        let org = owner.create_org("clientorg").await?;
        check(org.role == Role::Owner, "the creator owns the organization")
    })
    .await?;
    at_least_once(sim, ErrorCode::AlreadyExists, || async {
        let m = owner.add_member("clientorg", "member@client.test", Role::Writer).await?;
        check(m.user_id == me.user.id && m.role == Role::Writer, "added member")
    })
    .await?;
    at_least_once(sim, ErrorCode::AlreadyExists, || async {
        let db = owner.create_db("shared", Some("clientorg")).await?;
        check(db.org.as_deref() == Some("clientorg"), "organization database")
    })
    .await?;
    let put = [Op::Put { key: "k".into(), value: "v".into() }];
    let shared = member.db("clientorg.shared");
    retry(sim, || shared.txn(&put, Some("member-put"))).await?;
    at_least_once(sim, ErrorCode::NotFound, || owner.remove_member("clientorg", &me.user.id)).await?;
    super::cache_expiry(sim).await;
    let code = definitive(sim, || shared.txn(&put, Some("member-put-2"))).await?;
    expect(
        matches!(code, Some(ErrorCode::PermissionDenied | ErrorCode::NotFound)),
        format!("a removed member's write fails with {code:?}"),
    )
}

#[cfg(test)]
mod tests {
    use super::check_transfers;
    use std::collections::BTreeSet;

    #[test]
    fn the_ledger_audit_finds_lost_stray_and_repeated_transfers() {
        let set = |ids: &[&str]| ids.iter().map(|s| s.to_string()).collect::<BTreeSet<_>>();
        let (committed, unknown) = (set(&["a"]), set(&["b"]));
        assert!(check_transfers(&committed, &unknown, &["a"]).is_ok());
        assert!(check_transfers(&committed, &unknown, &["a", "b"]).is_ok());
        let lost = check_transfers(&committed, &unknown, &["b"]).unwrap_err();
        assert!(lost.contains("[\"a\"] were not applied"), "{lost}");
        let stray = check_transfers(&committed, &unknown, &["a", "c"]).unwrap_err();
        assert!(stray.contains("[\"c\"] applied after they failed"), "{stray}");
        assert!(check_transfers(&committed, &unknown, &["a", "a"]).is_err());
    }
}
