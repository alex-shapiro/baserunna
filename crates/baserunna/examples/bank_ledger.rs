//! Ledger example with concurrent money transfers.
//!
//! Demonstrates interactive serializable transactions, automatic conflict
//! detection with client retries, idempotency keys, and balance invariant
//! verification.
//!
//! Run: `cargo run --example bank_ledger`

#[path = "common/mod.rs"]
mod common;

use baserunna::meta::SignupPolicy;
use baserunna_client::{Client, Db, Entry, Error, Expect, HttpTransport, Op, OpResult};
use common::step;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

const ACCOUNTS: usize = 8;

/// Decodes the decimal integer that `e` holds.
fn amount(e: &Entry) -> i64 {
    e.value
        .data()
        .and_then(|b| std::str::from_utf8(b).ok())
        .and_then(|s| s.parse().ok())
        .expect("amounts are decimal integers")
}

fn balance(r: OpResult) -> Result<i64, Error> {
    Ok(amount(&r.entry()?.expect("accounts exist")))
}

/// Moves `amount` from account `from` to `to` if `from` holds enough, and logs the transfer as
/// transfer `n` of `teller`.
async fn transfer(
    db: &Db,
    (teller, n): (u64, u32),
    from: usize,
    to: usize,
    amount: i64,
    retries: &AtomicU64,
) -> Result<bool, Error> {
    let (from_key, to_key) = (format!("acct/{from}"), format!("acct/{to}"));
    let mut retry = db.retry();
    loop {
        let mut txn = db.begin();
        let moved: Result<bool, Error> = async {
            let r = txn.run(&[Op::get(from_key.clone()), Op::get(to_key.clone())]).await?;
            let bal = r.into_iter().map(balance).collect::<Result<Vec<_>, _>>()?;
            let (a, b) = (bal[0], bal[1]);
            if a < amount {
                return Ok(false);
            }
            txn.write([
                Op::put(from_key.clone(), (a - amount).to_string()),
                Op::put(to_key.clone(), (b + amount).to_string()),
                Op::put(format!("log/{teller}/{n}"), format!("{from}->{to}:{amount}")),
            ]);
            Ok(true)
        }
        .await;
        // The commit fails with a conflict if another teller changed either balance after this
        // transaction read it; `finish` then backs off and the loop runs the transfer again.
        if let Some(moved) = retry.finish(txn, moved).await? {
            return Ok(moved);
        }
        retries.fetch_add(1, Ordering::Relaxed);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = common::start(common::shared_store(), "bank", SignupPolicy::FirstUserOnly).await;
    let anon = Client::new(HttpTransport::new(&server.url));
    let session = anon.sign_up("treasury@bank.example", "very-secret-1").await?;
    let teller = anon.with_token(&session.token);
    teller.create_db("ledger", None).await?;
    let db = teller.db("ledger");

    step(1, "Open accounts with 1000 each (one atomic transaction)");
    let ops: Vec<_> = (0..ACCOUNTS).map(|i| Op::put(format!("acct/{i}"), "1000")).collect();
    let c = db.write(&ops).await?.expect("puts commit");
    println!("    committed at version {}", c.version);

    step(2, "8 tellers run 25 random transfers each, concurrently");
    let retries = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::new();
    for t in 0..8u64 {
        let db = db.clone();
        let retries = retries.clone();
        tasks.push(tokio::spawn(async move {
            let mut done = 0;
            let mut x = t * 7919 + 1;
            for n in 0..25 {
                x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                let from = (x >> 33) as usize % ACCOUNTS;
                let to = (from + 1 + (x >> 40) as usize % (ACCOUNTS - 1)) % ACCOUNTS;
                let amount = 1 + (x >> 50) as i64 % 300;
                if transfer(&db, (t, n), from, to, amount, &retries).await? {
                    done += 1;
                }
            }
            Ok::<u32, Error>(done)
        }));
    }
    let mut total = 0;
    for t in tasks {
        total += t.await??;
    }
    println!("    {total} transfers committed, {} conflicts detected and retried", retries.load(Ordering::Relaxed));

    step(3, "Audit: a consistent snapshot of all balances sums to the initial total");
    let page = db.read_one(Op::scan("acct/")).await?.scan_page()?;
    let balances: Vec<i64> = page.entries.iter().map(amount).collect();
    let sum: i64 = balances.iter().sum();
    println!("    balances {balances:?}");
    println!("    total = {sum} (expected {})", 1000 * ACCOUNTS as i64);
    assert_eq!(sum, 1000 * ACCOUNTS as i64);
    assert!(balances.iter().all(|b| *b >= 0));

    step(4, "Idempotency keys: a retried request is applied once");
    let ops = [Op::increment("fees/collected", 25)];
    let first = db.txn(&ops, Some("invoice-2026-0042")).await?;
    let retry = db.txn(&ops, Some("invoice-2026-0042")).await?;
    let value = match first.results.as_deref() {
        Some([OpResult::Increment(v)]) => v.to_string(),
        other => format!("{other:?}"),
    };
    println!("    first: version {} value {value}", first.version);
    println!("    retry: version {} replayed={}", retry.version, retry.results.is_none());
    let fees = db.read_one(Op::get("fees/collected")).await?.entry()?.expect("fees were collected");
    println!("    fees/collected = {} (applied exactly once)", amount(&fees));

    step(5, "Conditional writes: check-and-set fails cleanly on a stale version");
    let acct = db.read_one(Op::get("acct/0")).await?.entry()?.expect("account 0 exists");
    let v = acct.version.expect("a read outside a transaction sees committed versions");
    let check = |v| Op::Check { key: "acct/0".into(), expect: Expect::Version(v) };
    let ok = db.write(&[check(v), Op::put("acct/0/frozen", "yes")]).await;
    let stale = db.write(&[check(v - 1), Op::put("acct/0/frozen", "no")]).await;
    println!("    with current version: {}", if ok.is_ok() { "applied" } else { "rejected" });
    println!("    with stale version:   {}", stale.err().map(|e| e.to_string()).unwrap_or_default());

    server.stop().await;
    println!("\ndone.");
    Ok(())
}
