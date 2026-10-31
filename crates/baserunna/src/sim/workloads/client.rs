//! Simulation workload for `baserunna-client` under network, process, and store faults.
//!
//! Counter clients increment their own keys with a version check and an idempotency key per
//! transaction, so a retry that applies a write twice, or reports a failure for an applied
//! write, breaks the count. Mutators write, copy, and delete binary keys while a watcher
//! rebuilds the key space from the change feed; at the end the rebuilt replica must equal a
//! scan. Every response also passes the OpenAPI check of the simulated client.

use super::super::Sim;
use super::super::client::{Client as SimClient, ZOMBIE_WAIT_MS};
use super::{chaos_phase, expect, settle};
use baserunna_api::{ErrorCode, Outcome, Role};
use baserunna_client::{Change, ChangesRequest, Client, Db, Error, Expect, KeyRange, Op, OpResult, Value};
use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::Arc;

const DB: &str = "client";

pub async fn run(sim: Arc<Sim>, counters: usize, mutators: usize, steps: usize, chaos: bool) -> Result<(), String> {
    let client = signed_up(&sim, "client-admin", "client@client.test").await?;
    at_least_once(&sim, ErrorCode::AlreadyExists, || async { client.create_db(DB, None).await.map(drop) }).await?;
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
    for t in tasks {
        t.await.map_err(|e| e.to_string())??;
    }
    settle(&sim, phase).await;
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
    Client::new(t)
}

/// Registers an account, or signs in if a lost response hid an earlier registration.
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

/// Calls a non-idempotent request until it succeeds, treating error `done` as the success of an
/// earlier attempt whose response was lost.
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

/// Fails with a non-transient error unless `cond` is true.
fn check(cond: bool, what: &str) -> Result<(), Error> {
    match cond {
        true => Ok(()),
        false => Err(Error::Malformed { path: what.into(), reason: "the check failed".into() }),
    }
}

/// Calls `f`, which must fail, until it fails with a non-transient error, and returns its code.
async fn definitive<T: std::fmt::Debug, F: Future<Output = Result<T, Error>>>(
    sim: &Arc<Sim>,
    mut f: impl FnMut() -> F,
) -> Result<Option<ErrorCode>, String> {
    for _ in 0..400 {
        match f().await {
            Ok(v) => return Err(format!("unexpected success {v:?}")),
            Err(e) if e.is_transient(true) || matches!(e, Error::Transport(_)) => sim.sleep(1_000).await,
            Err(e) => return Ok(e.code()),
        }
    }
    Err("a request never failed definitively".into())
}

/// Calls `f` until it succeeds or fails with a non-transient error, pausing between attempts.
async fn retry<T, F: Future<Output = Result<T, Error>>>(sim: &Arc<Sim>, mut f: impl FnMut() -> F) -> Result<T, String> {
    for _ in 0..400 {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) if e.is_transient(true) || matches!(e, Error::Transport(_)) => sim.sleep(1_000).await,
            Err(e) => return Err(e.to_string()),
        }
    }
    Err("a request never succeeded".into())
}

/// Increments key `ctr/{i}` `steps` times with a version check, resolving unknown outcomes by
/// reading the counter.
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
                // An earlier attempt can still apply until it times out on the server.
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

async fn read_counter(sim: &Arc<Sim>, db: &Db, key: &Bytes) -> Result<(u64, Option<u64>), String> {
    let get = [Op::Get { key: key.clone() }];
    let out = retry(sim, || db.txn(&get, None)).await?;
    match out.results.as_deref() {
        Some([OpResult::Get(None)]) => Ok((0, None)),
        Some([OpResult::Get(Some(e))]) => match &e.value {
            Value::Inline(b) => {
                let n = std::str::from_utf8(b).ok().and_then(|s| s.parse().ok()).ok_or("counter is not a number")?;
                Ok((n, Some(e.version)))
            }
            v => Err(format!("counter holds {v:?}")),
        },
        r => Err(format!("unexpected get result {r:?}")),
    }
}

/// Keys of mutator `i`, including bytes that are not valid UTF-8.
fn mutator_key(i: usize, k: u64) -> Bytes {
    let mut b = format!("m{i}/").into_bytes();
    b.extend_from_slice(&[[b'a', 0xff, 0x00, b'/'][k as usize % 4], b'0' + (k / 4) as u8]);
    Bytes::from(b)
}

/// Applies random writes, blob writes, copies, and deletes to the keys of mutator `i`.
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
            Ok(out) => expect(out.results.is_none_or(|r| r == [OpResult::Done]), "a write returns no data")?,
            Err(e) if e.code() == Some(ErrorCode::PreconditionFailed) && matches!(op, Op::Copy { .. }) => {}
            Err(e) if e.code() == Some(ErrorCode::Aborted) => {}
            Err(e) if e.outcome() == Outcome::Unknown || e.is_transient(true) => {}
            Err(e) if e.code() == Some(ErrorCode::NotFound) && matches!(op, Op::PutBlob { .. }) => {}
            Err(e) => return Err(format!("mutator {i}: {op:?}: {e}")),
        }
    }
    Ok(())
}

/// Keys and values rebuilt from the change feed, and the cursor of the next read.
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
        Value::Blob { meta, data: None } => Err(format!("blob {} has no inline data", meta.blob_id)),
    }
}

/// Follows the change feed with long polls, restarting from a scan if the cursor expires.
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

/// Applies one page of changes and returns whether the page was empty and the last one.
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

/// Reads the feed until no more changes are available.
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

/// Rebuilds a replica from a scan and continues the feed at the version of the scan.
async fn rescan(sim: &Arc<Sim>, db: &Db) -> Result<Replica, String> {
    let ops = [Op::Scan { range: KeyRange::Prefix(Bytes::new()), limit: 1000, reverse: false, keys_only: false }];
    let out = retry(sim, || db.txn(&ops, None)).await?;
    let Some([OpResult::Scan(page)]) = out.results.as_deref() else {
        return Err("unexpected scan result".into());
    };
    expect(page.cursor.is_none(), "the key space fits in one page")?;
    let keys = page
        .entries
        .iter()
        .map(|e| Ok((e.key.clone(), (e.version, data(&e.value)?))))
        .collect::<Result<_, String>>()?;
    Ok(Replica { keys, cursor: out.version.to_string() })
}

async fn scan_all(sim: &Arc<Sim>, db: &Db) -> Result<BTreeMap<Bytes, (u64, Bytes)>, String> {
    Ok(rescan(sim, db).await?.keys)
}

/// Exercises the account and organization endpoints and the mapping of their errors.
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
