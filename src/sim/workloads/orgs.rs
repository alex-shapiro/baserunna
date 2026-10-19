//! `orgs`: tests organization roles and membership changes while members read
//! and write the organization's databases across nodes under faults.
//!
//! - Verifies role permissions through sessions and API keys, and ensures
//!   outsiders receive 404.
//! - While an owner changes member roles, every write a member makes must
//!   match a role held during the request or within [`super::cache_lag`]
//!   before it. Rejected writes leave no trace; acknowledged writes persist.
//! - Concurrent owner demotions or departures, and organization deletions
//!   racing database creation, never leave an organization without an owner
//!   or a database without an organization.

use super::super::client::{Client, Resp, ZOMBIE_WAIT_MS};
use super::super::{NetError, Sim};
use super::steps::{add_member, create_key, create_org, members};
use super::{cache_expiry, cache_lag, expect};
use crate::meta::Role;
use serde_json::{Value as Json, json};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

struct User {
    c: Client,
    id: String,
    email: String,
}

async fn user(sim: &Arc<Sim>, name: &str) -> Result<User, String> {
    let email = format!("{name}@orgs.test");
    let c = Client::signup(sim, name, &email, &format!("{name}-password")).await?;
    let me = c.call_retry("GET", "/v1/me", None).await;
    let id = me.json["user"]["id"].as_str().ok_or(format!("me: {}", me.json))?.to_string();
    Ok(User { c, id, email })
}

fn wire(role: Role) -> Json {
    serde_json::to_value(role).unwrap_or(Json::Null)
}

fn tag(role: Option<Role>) -> String {
    role.map_or_else(|| "outsider".into(), |r| wire(r).as_str().unwrap_or("?").to_string())
}

/// Expected HTTP status for a request requiring `can` when made with `role`.
fn answer(role: Option<Role>, can: impl Fn(Role) -> bool, granted: u16) -> u16 {
    match role {
        None => 404,
        Some(r) if can(r) => granted,
        Some(_) => 403,
    }
}

/// Sets `u`'s role in `org` (`None` removes membership).
async fn set_role(by: &Client, org: &str, u: &User, role: Option<Role>) -> Result<(), String> {
    let current = members(by, org).await?.get(&u.id).copied();
    if current == role {
        return Ok(());
    }
    let (method, path, body) = match (current.is_some(), role) {
        (false, Some(r)) => {
            ("POST", format!("/v1/orgs/{org}/members"), Some(json!({"email": u.email, "role": wire(r)})))
        }
        (true, Some(r)) => ("POST", format!("/v1/orgs/{org}/members/{}", u.id), Some(json!({"role": wire(r)}))),
        (true, None) => ("DELETE", format!("/v1/orgs/{org}/members/{}", u.id), None),
        (false, None) => return Ok(()),
    };
    let (r, ambiguous) = by.settle(method, &path, body).await?;
    expect(r.ok() || ambiguous, format!("set role of {} to {role:?}: {} {}", u.email, r.status, r.json))?;
    let now = members(by, org).await?.get(&u.id).copied();
    expect(now == role, format!("role of {} is {now:?}, not {role:?}", u.email))
}

pub async fn run(sim: Arc<Sim>) -> Result<(), String> {
    // The first account is a site administrator; keep it out of the way.
    super::admin(&sim).await?;
    let owner = user(&sim, "owner").await?;
    let others = [user(&sim, "ann").await?, user(&sim, "bob").await?, user(&sim, "cat").await?];
    create_org(&owner.c, "acme").await?;
    owner.c.create_db_owned(Some("acme"), "main").await?;
    create_org(&owner.c, "beta").await?;
    add_member(&owner.c, "beta", &others[2].email, "reader").await?;
    let pairs = |orgs: Vec<Json>| {
        let mut v: Vec<(Json, Json)> = orgs.into_iter().map(|o| (o["name"].clone(), o["role"].clone())).collect();
        v.sort_by_key(|(n, _)| n.to_string());
        v
    };
    let listed = pairs(owner.c.paginate("/v1/orgs?limit=1", "orgs").await?);
    let want = vec![(json!("acme"), json!("owner")), (json!("beta"), json!("owner"))];
    expect(listed == want, format!("the owner's organizations are {listed:?}"))?;
    let listed = pairs(others[2].c.paginate("/v1/orgs", "orgs").await?);
    expect(listed == vec![(json!("beta"), json!("reader"))], format!("a reader's organizations are {listed:?}"))?;
    permissions(&sim, &owner, &others[0], &others[1]).await?;
    let phase = Some(super::chaos_phase(&sim));
    let r = async {
        role_changes(&sim, &owner, &others).await?;
        owner_races(&sim, &owner, &others[0]).await?;
        delete_races(&sim, &owner, &others[1]).await
    }
    .await;
    r?;
    super::settle(&sim, phase).await;
    super::check_integrity(&sim).await
}

/// Verifies what `u` can do in `acme` and `acme.main` across each role and as
/// an outsider, using both a session and API keys.
async fn permissions(sim: &Arc<Sim>, owner: &User, u: &User, other: &User) -> Result<(), String> {
    let add = |email: &str, role: &str| Some(json!({"email": email, "role": role}));
    let r = u.c.call_retry("POST", "/v1/orgs/acme/members", add(&other.email, "reader")).await;
    expect(r.status == 404, format!("an outsider added a member: {}", r.status))?;
    let r = owner.c.call_retry("POST", "/v1/orgs/acme/members", add("nobody@orgs.test", "reader")).await;
    expect(r.status == 404, format!("added an unknown account: {}", r.status))?;
    let r = owner.c.call_retry("POST", "/v1/orgs/acme/members", add(&u.email, "boss")).await;
    expect(r.status == 400, format!("accepted an unknown role: {}", r.status))?;
    let member = |o: &User| format!("/v1/orgs/acme/members/{}", o.id);
    let mut keys: Option<(Client, Client)> = None;
    let roles =
        [None, Some(Role::Reader), Some(Role::Writer), Some(Role::Admin), Some(Role::Owner), Some(Role::Reader), None];
    for role in roles {
        set_role(&owner.c, "acme", u, role).await?;
        cache_expiry(sim).await;
        let tag = tag(role);
        let info = u.c.call_retry("GET", "/v1/dbs/acme.main", None).await;
        match role {
            None => expect(info.status == 404, format!("{tag}: db info {}", info.status))?,
            Some(r) => {
                expect(info.status == 200 && info.json["role"] == wire(r), format!("{tag}: db info {}", info.json))?
            }
        }
        let org = u.c.call_retry("GET", "/v1/orgs/acme", None).await;
        expect(org.status == answer(role, |_| true, 200), format!("{tag}: org {}", org.status))?;
        let write = answer(role, Role::can_write, 200);
        let r = u.c.call_settled("PUT", &format!("/v1/dbs/acme.main/kv/by-{tag}"), None).await;
        expect(r.status == write, format!("{tag}: write {} (want {write})", r.status))?;
        let txn = json!({"ops": [{"op": "put", "key": format!("txn-by-{tag}"), "value": "v"}]});
        let r = u.c.call_settled("POST", "/v1/dbs/acme.main/txn", Some(txn)).await;
        expect(r.status == write, format!("{tag}: txn {}", r.status))?;
        let r = u.c.call_retry("GET", "/v1/dbs/acme.main/kv?limit=5", None).await;
        expect(r.status == answer(role, |_| true, 200), format!("{tag}: scan {}", r.status))?;
        let r = u.c.call_retry("GET", "/v1/dbs?org=acme", None).await;
        expect(r.status == answer(role, |_| true, 200), format!("{tag}: list dbs {}", r.status))?;
        if role.is_some() {
            expect(r.json["databases"][0]["org"] == "acme", format!("{tag}: listing {}", r.json))?;
        }
        let body = Some(json!({"name": "scratch", "org": "acme"}));
        let (r, ambiguous) = u.c.settle("POST", "/v1/dbs", body).await?;
        let want = answer(role, Role::can_administer, 201);
        expect(r.status == want || (ambiguous && r.status == 409), format!("{tag}: create db {}", r.status))?;
        owner.c.create_db_owned(Some("acme"), "scratch").await?;
        let (r, ambiguous) = u.c.settle("DELETE", "/v1/dbs/acme.scratch", None).await?;
        let want = answer(role, Role::can_administer, 200);
        expect(r.status == want || (ambiguous && r.status == 404), format!("{tag}: delete db {}", r.status))?;
        let r = owner.c.call_settled("DELETE", "/v1/dbs/acme.scratch", None).await;
        expect(r.status == 200 || r.status == 404, format!("{tag}: clean up the scratch database: {}", r.status))?;
        let (r, ambiguous) = u.c.settle("POST", "/v1/orgs/acme/members", add(&other.email, "reader")).await?;
        let want = answer(role, Role::can_administer, 201);
        expect(r.status == want || (ambiguous && r.status == 409), format!("{tag}: add a member {}", r.status))?;
        set_role(&owner.c, "acme", other, None).await?;
        if role.is_some_and(Role::can_administer) {
            let (r, ambiguous) = u.c.settle("POST", "/v1/orgs/acme/members", add(&other.email, "owner")).await?;
            let want = if role == Some(Role::Owner) { 201 } else { 403 };
            expect(r.status == want || (ambiguous && r.status == 409), format!("{tag}: add an owner {}", r.status))?;
            set_role(&owner.c, "acme", other, None).await?;
            let r = u.c.call_retry("POST", &member(owner), Some(json!({"role": "reader"}))).await;
            let want = if role == Some(Role::Owner) { 200 } else { 403 };
            expect(r.status == want, format!("{tag}: demote the owner {}", r.status))?;
            if role == Some(Role::Owner) {
                let r = owner.c.call_retry("GET", "/v1/orgs/acme", None).await;
                expect(r.json["role"] == "reader", format!("the demoted owner is {}", r.json))?;
                set_role(&u.c, "acme", owner, Some(Role::Owner)).await?;
            }
        }
        if role == Some(Role::Writer) {
            let (rw, info) = create_key(sim, &u.c, "key-rw", json!({"name": "rw", "db": "acme.main"})).await?;
            expect(info["db"] == "acme.main", format!("a key for an org database: {info}"))?;
            let body = json!({"name": "ro", "db": "acme.main", "read_only": true});
            let (ro, _) = create_key(sim, &u.c, "key-ro", body).await?;
            keys = Some((rw, ro));
        }
        if let Some((rw, ro)) = &keys {
            let r = rw.call_settled("PUT", "/v1/dbs/acme.main/kv/by-key", None).await;
            expect(r.status == write, format!("{tag}: key write {} (want {write})", r.status))?;
            let r = ro.call_settled("PUT", "/v1/dbs/acme.main/kv/by-read-only-key", None).await;
            let want = answer(role, |_| false, 200);
            expect(r.status == want, format!("{tag}: read-only key write {} (want {want})", r.status))?;
            let r = ro.call_retry("GET", "/v1/dbs/acme.main/kv?limit=1", None).await;
            expect(r.status == answer(role, |_| true, 200), format!("{tag}: read-only key read {}", r.status))?;
        }
    }
    set_role(&owner.c, "acme", u, Some(Role::Reader)).await?;
    let (r, ambiguous) = u.c.settle("DELETE", &member(u), None).await?;
    expect(r.status == 200 || (ambiguous && r.status == 404), format!("leave: {}", r.status))?;
    let r = owner.c.call_retry("DELETE", &member(owner), None).await;
    expect(r.status == 403, format!("the last owner left: {}", r.status))?;
    let r = owner.c.call_retry("DELETE", "/v1/orgs/acme", None).await;
    expect(r.status == 403, format!("deleted an organization with databases: {}", r.status))?;
    let items = owner.c.paginate("/v1/dbs/acme.main/kv?keys_only=true", "items").await?;
    let names: Vec<&str> = items.iter().filter_map(|i| i["key"].as_str()).collect();
    for role in [None, Some(Role::Reader), Some(Role::Writer), Some(Role::Admin), Some(Role::Owner)] {
        let allowed = role.is_some_and(Role::can_write);
        for k in [format!("by-{}", tag(role)), format!("txn-by-{}", tag(role))] {
            expect(names.contains(&k.as_str()) == allowed, format!("{k} stored: {} (keys {names:?})", !allowed))?;
        }
    }
    expect(!names.contains(&"by-read-only-key"), "a read-only key wrote")
}

/// Sequence of roles a user held, each with the timestamp the change was sent
/// and the timestamp its effect was confirmed.
#[derive(Default)]
struct History(Vec<(u64, u64, Option<Role>)>);

impl History {
    /// Returns true if at some instant in `[from, to]` the user held a role
    /// satisfying `ok`.
    fn some_role(&self, from: u64, to: u64, ok: impl Fn(Option<Role>) -> bool) -> bool {
        self.0.iter().enumerate().any(|(i, (sent, _, role))| {
            // A role is active from when its change was sent until the next change was confirmed.
            let until = self.0.get(i + 1).map_or(u64::MAX, |n| n.1);
            *sent <= to && until >= from && ok(*role)
        })
    }
}

struct Attempt {
    user: usize,
    key: String,
    start: u64,
    end: u64,
    /// 0 when the outcome is unknown.
    status: u16,
}

/// Randomly changes member roles while members write, then checks every
/// response against the role history.
async fn role_changes(sim: &Arc<Sim>, owner: &User, others: &[User; 3]) -> Result<(), String> {
    cache_expiry(sim).await;
    let history: Arc<Mutex<Vec<History>>> = Arc::default();
    let change = |i: usize, sent: u64, role: Option<Role>| {
        history.lock().unwrap_or_else(|e| e.into_inner())[i].0.push((sent, sim.now_ms(), role));
    };
    for (i, u) in others.iter().enumerate() {
        let current = members(&owner.c, "acme").await?.get(&u.id).copied();
        history.lock().unwrap_or_else(|e| e.into_inner()).push(History(vec![(0, 0, current)]));
        let sent = sim.now_ms();
        set_role(&owner.c, "acme", u, Some(Role::Writer)).await?;
        change(i, sent, Some(Role::Writer));
    }
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let attempts: Arc<Mutex<Vec<Attempt>>> = Arc::default();
    let mut writers = Vec::new();
    for (i, u) in others.iter().enumerate() {
        let c = Client::with_token(sim, &format!("writer-{i}"), u.c.token.as_deref().unwrap_or_default());
        let (sim, done, attempts) = (sim.clone(), done.clone(), attempts.clone());
        writers.push(tokio::spawn(async move {
            let mut n = 0;
            while !done.load(std::sync::atomic::Ordering::SeqCst) {
                n += 1;
                let key = format!("w{i}-{n}");
                let start = sim.now_ms();
                let status = match c.call("PUT", &format!("/v1/dbs/acme.main/kv/{key}"), Some(json!("v"))).await {
                    Ok(r) if r.outcome_unknown() => 0,
                    Ok(r) => r.status,
                    Err(_) => 0,
                };
                let a = Attempt { user: i, key, start, end: sim.now_ms(), status };
                attempts.lock().unwrap_or_else(|e| e.into_inner()).push(a);
                sim.sleep(50 + sim.rand_below(300)).await;
            }
        }));
    }
    for step in 0..12 {
        sim.sleep(200 + sim.rand_below(2_000)).await;
        // Start with a demotion and a removal so some writes are rejected.
        let (i, role) = match step {
            0 => (0, Some(Role::Reader)),
            1 => (1, None),
            _ => {
                let roles = [None, Some(Role::Reader), Some(Role::Writer), Some(Role::Admin)];
                (sim.rand_below(3) as usize, roles[sim.rand_below(4) as usize])
            }
        };
        let sent = sim.now_ms();
        set_role(&owner.c, "acme", &others[i], role).await?;
        change(i, sent, role);
    }
    done.store(true, std::sync::atomic::Ordering::SeqCst);
    for w in writers {
        w.await.map_err(|e| format!("writer panicked: {e}"))?;
    }
    sim.sleep(ZOMBIE_WAIT_MS).await;
    let history = std::mem::take(&mut *history.lock().unwrap_or_else(|e| e.into_inner()));
    let attempts = std::mem::take(&mut *attempts.lock().unwrap_or_else(|e| e.into_inner()));
    check_attempts(&history, &attempts, cache_lag(sim))?;
    let items = owner.c.paginate("/v1/dbs/acme.main/kv?prefix=w&keys_only=true&limit=1000", "items").await?;
    let stored: std::collections::BTreeSet<&str> = items.iter().filter_map(|i| i["key"].as_str()).collect();
    for a in &attempts {
        match a.status {
            200 => expect(stored.contains(a.key.as_str()), format!("acknowledged write {} lost", a.key))?,
            403 | 404 => expect(!stored.contains(a.key.as_str()), format!("refused write {} applied", a.key))?,
            _ => {}
        }
    }
    let granted = attempts.iter().filter(|a| a.status == 200).count();
    let refused = attempts.iter().filter(|a| a.status == 403 || a.status == 404).count();
    expect(granted > 0 && refused > 0, format!("too few decisions to check: {granted} granted, {refused} refused"))
}

/// Verifies that each write response matches a role held within `lag` before
/// or during the request: 200 requires writer or above, 403 requires a member
/// below writer, and 404 requires non-membership.
fn check_attempts(history: &[History], attempts: &[Attempt], lag: u64) -> Result<(), String> {
    for a in attempts {
        let h = &history[a.user];
        let from = a.start.saturating_sub(lag);
        let ok = match a.status {
            200 => h.some_role(from, a.end, |r| r.is_some_and(Role::can_write)),
            403 => h.some_role(from, a.end, |r| r.is_some_and(|r| !r.can_write())),
            404 => h.some_role(from, a.end, |r| r.is_none()),
            0 | 429 | 503 => true,
            _ => false,
        };
        if !ok {
            return Err(format!(
                "user {} got {} for a write during [{from}, {}] but held {:?}",
                a.user, a.status, a.end, h.0
            ));
        }
    }
    Ok(())
}

/// Returns `Some(true)` if the request certainly took effect, `Some(false)` if
/// it certainly did not, or `None` if ambiguous.
fn effect(r: &Result<Resp, NetError>) -> Option<bool> {
    match r {
        Ok(r) if r.ok() => Some(true),
        Ok(r) if r.status == 503 => r.retryable_not_applied().then_some(false),
        Ok(_) => Some(false),
        Err(_) => None,
    }
}

/// Races two owners demoting each other or leaving; at most one may succeed.
async fn owner_races(sim: &Arc<Sim>, owner: &User, ann: &User) -> Result<(), String> {
    create_org(&owner.c, "duo").await?;
    let member = |o: &User| format!("/v1/orgs/duo/members/{}", o.id);
    let (ann_path, owner_path) = (member(ann), member(owner));
    for round in 0..4 {
        set_role(&owner.c, "duo", ann, Some(Role::Owner)).await?;
        let (a, b) = if round % 2 == 1 {
            tokio::join!(ann.c.call("DELETE", &ann_path, None), owner.c.call("DELETE", &owner_path, None))
        } else {
            let demote = Some(json!({"role": "writer"}));
            tokio::join!(ann.c.call("POST", &owner_path, demote.clone()), owner.c.call("POST", &ann_path, demote))
        };
        let both = effect(&a) == Some(true) && effect(&b) == Some(true);
        expect(!both, format!("round {round}: both owners prevailed"))?;
        sim.sleep(ZOMBIE_WAIT_MS).await;
        let m = members_any(sim, "duo", [owner, ann]).await?;
        expect(m.values().any(|r| *r == Role::Owner), format!("round {round}: no owner left: {m:?}"))?;
        if m.get(&owner.id) != Some(&Role::Owner) {
            set_role(&ann.c, "duo", owner, Some(Role::Owner)).await?;
        }
    }
    Ok(())
}

async fn members_any(sim: &Arc<Sim>, org: &str, users: [&User; 2]) -> Result<BTreeMap<String, Role>, String> {
    for _ in 0..50 {
        for u in users {
            if let Ok(m) = members(&u.c, org).await {
                return Ok(m);
            }
        }
        sim.sleep(500).await;
    }
    Err(format!("nobody can list {org}"))
}

/// Races organization deletion against an admin creating a database in it.
async fn delete_races(sim: &Arc<Sim>, owner: &User, bob: &User) -> Result<(), String> {
    for round in 0..4 {
        let org = format!("tmp{round}");
        create_org(&owner.c, &org).await?;
        set_role(&owner.c, &org, bob, Some(Role::Admin)).await?;
        let org_path = format!("/v1/orgs/{org}");
        let (c, d) = tokio::join!(
            bob.c.call("POST", "/v1/dbs", Some(json!({"name": "x", "org": org}))),
            owner.c.call("DELETE", &org_path, None)
        );
        let both = effect(&c) == Some(true) && effect(&d) == Some(true);
        expect(!both, format!("round {round}: created a database in a deleted organization"))?;
        sim.sleep(ZOMBIE_WAIT_MS).await;
        let info = owner.c.call_retry("GET", &org_path, None).await;
        if info.status == 200 {
            let r = owner.c.call_settled("DELETE", &format!("/v1/dbs/{org}.x"), None).await;
            expect(r.status == 200 || r.status == 404, format!("delete {org}.x: {}", r.status))?;
            let r = owner.c.call_settled("DELETE", &org_path, None).await;
            expect(r.status == 200 || r.status == 404, format!("delete {org}: {} {}", r.status, r.json))?;
        }
        let gone = bob.c.call_retry("GET", &format!("/v1/dbs/{org}.x"), None).await;
        expect(gone.status == 404, format!("{org}.x outlived its organization: {}", gone.status))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checker_rejects_unexplained_answers() {
        let (w, r) = (Some(Role::Writer), Some(Role::Reader));
        let h = vec![History(vec![(0, 0, None), (0, 100, w), (1_000, 1_100, r), (5_000, 5_050, None)])];
        let at = |start, end, status| Attempt { user: 0, key: "k".into(), start, end, status };
        let lag = 200;
        assert!(check_attempts(&h, &[at(500, 600, 200)], lag).is_ok());
        assert!(check_attempts(&h, &[at(500, 600, 403)], lag).is_err(), "a writer refused");
        assert!(check_attempts(&h, &[at(1_050, 1_060, 200)], lag).is_ok(), "the demotion was in flight");
        assert!(check_attempts(&h, &[at(1_200, 1_250, 200)], lag).is_ok(), "within the cache lag");
        assert!(check_attempts(&h, &[at(2_000, 2_100, 200)], lag).is_err(), "a reader wrote");
        assert!(check_attempts(&h, &[at(2_000, 2_100, 404)], lag).is_err(), "a member treated as an outsider");
        assert!(check_attempts(&h, &[at(6_000, 6_100, 403)], lag).is_err(), "an outsider learned of the database");
        assert!(check_attempts(&h, &[at(6_000, 6_100, 404)], lag).is_ok());
        assert!(check_attempts(&h, &[at(6_000, 6_100, 0)], lag).is_ok(), "unknown outcomes prove nothing");
        assert!(check_attempts(&h, &[at(500, 600, 500)], lag).is_err(), "an internal error");
        assert!(check_attempts(&h, &[at(500, 600, 401)], lag).is_err(), "a member's session refused");
    }
}
