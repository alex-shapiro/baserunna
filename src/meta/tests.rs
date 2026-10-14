use super::*;
use crate::engine::tests::{engine, rt};
use crate::engine::{Db, EngineConfig, Isolation};
use crate::util::prop;
use crate::util::rng::Rng;
use serde::de::DeserializeOwned;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

async fn run(db: &Arc<Db>, cmd: &MetaCmd) -> Result<Json> {
    let mut t = db.begin(Isolation::Serializable, crate::engine::Consistency::Strong).await?;
    let r = apply(&mut t, &BlobResolver { db }, cmd, 1000).await;
    if r.is_ok() {
        db.commit(t.into_request()).await?;
    }
    r
}

async fn run_as<T: DeserializeOwned>(db: &Arc<Db>, cmd: &MetaCmd) -> Result<T> {
    Ok(serde_json::from_value(run(db, cmd).await?).unwrap())
}

async fn read(db: &Arc<Db>) -> Txn {
    db.begin(Isolation::Serializable, crate::engine::Consistency::Strong).await.unwrap()
}

fn signup(id: &str, email: &str, policy: SignupPolicy) -> MetaCmd {
    MetaCmd::Signup {
        id: id.into(),
        email: email.into(),
        password_hash: Some("h".into()),
        policy,
        bootstrap_admin: false,
    }
}

fn session(hash: &str, user: &str, expires_at: i64) -> MetaCmd {
    MetaCmd::CreateSession {
        token_hash: hash.into(),
        id: format!("id-{hash}"),
        user_id: user.into(),
        expires_at,
        client: None,
        method: SignInMethod::Password,
    }
}

#[test]
fn migrate_signup_and_dbs() {
    rt().block_on(async {
        let (eng, _s, _e) = engine(3, EngineConfig::small_for_tests());
        let db = eng.db(SYS_DB);
        db.open(true).await.unwrap();
        assert!(run_as::<bool>(&db, &MetaCmd::Migrate).await.unwrap());
        assert!(!run_as::<bool>(&db, &MetaCmd::Migrate).await.unwrap());
        let r: SignedUp = run_as(&db, &signup("u1", "a@x.io", SignupPolicy::FirstUserOnly)).await.unwrap();
        assert!(r.created && r.user.is_admin);
        assert!(matches!(
            run(&db, &signup("u2", "b@x.io", SignupPolicy::FirstUserOnly)).await,
            Err(Error::PermissionDenied(_))
        ));
        assert!(matches!(run(&db, &signup("u2", "a@x.io", SignupPolicy::Open)).await, Err(Error::AlreadyExists(_))));
        let r: SignedUp = run_as(&db, &signup("u2", "b@x.io", SignupPolicy::Open)).await.unwrap();
        assert!(!r.user.is_admin);
        let c = |id: &str, name: &str| MetaCmd::CreateDb {
            id: id.into(),
            owner_id: "u2".into(),
            actor: "u2".into(),
            name: name.into(),
            max_dbs: 2,
        };
        run(&db, &c("d1", "one")).await.unwrap();
        assert!(matches!(run(&db, &c("d2", "one")).await, Err(Error::AlreadyExists(_))));
        assert!(matches!(run(&db, &c("d2", "Bad Name")).await, Err(Error::InvalidArgument(_))));
        run(&db, &c("d2", "two")).await.unwrap();
        assert!(matches!(run(&db, &c("d3", "three")).await, Err(Error::PermissionDenied(_))));
        let foreign = MetaCmd::CreateDb {
            id: "d9".into(),
            owner_id: "u2".into(),
            actor: "u1".into(),
            name: "mine".into(),
            max_dbs: 9,
        };
        assert!(matches!(run(&db, &foreign).await, Err(Error::PermissionDenied(_))), "only for yourself");
        run(&db, &MetaCmd::DeleteDb { id: "d1".into() }).await.unwrap();
        run(&db, &c("d3", "one")).await.unwrap();
        run(&db, &MetaCmd::DeleteDb { id: "d2".into() }).await.unwrap();
        run(&db, &MetaCmd::DeleteDb { id: "d3".into() }).await.unwrap();
        let mut t = read(&db).await;
        check_integrity(&mut t).await.unwrap();
        assert!(table::get(&mut t, "counts", &json!(["dbs", "u2"])).await.unwrap().is_none(), "no row at zero");
        run(&db, &c("d2", "two")).await.unwrap();
        run(&db, &c("d3", "one")).await.unwrap();
        let mut t = read(&db).await;
        check_integrity(&mut t).await.unwrap();
        assert_eq!(resolve_db(&mut t, "u2", "one").await.unwrap().unwrap().id, "d3");
        assert_eq!(list_dbs(&mut t, "u2").await.unwrap().len(), 2);
        assert_eq!(list_deleted_dbs(&mut t).await.unwrap().len(), 3);
        assert_eq!(user_by_email(&mut t, "b@x.io").await.unwrap().unwrap().id, "u2");
        let p = dbs_page(&mut t, "u2", 1, None).await.unwrap();
        assert_eq!((p.items.len(), p.items[0].name.as_str()), (1, "one"));
        let p2 = dbs_page(&mut t, "u2", 1, p.next_cursor).await.unwrap();
        assert_eq!(p2.items[0].name, "two");
        assert_eq!(p2.next_cursor, None, "the listing ends");
        assert!(
            dbs_page(&mut t, "u1", 5, Some(p2.items[0].id.clone())).await.is_err(),
            "a cursor from another listing is refused"
        );
        for name in ["users", "databases", "api_keys"] {
            table::check_indexes(&mut t, name).await.unwrap();
        }
        for (h, exp) in [("s1", 100), ("s2", 5000), ("s3", 200)] {
            run(&db, &session(h, "u1", exp)).await.unwrap();
        }
        assert!(matches!(run(&db, &session("s4", "nobody", 9)).await, Err(Error::NotFound(_))));
        let mut t = read(&db).await;
        assert!(!any_session_expired(&mut t, 100).await.unwrap());
        assert!(any_session_expired(&mut t, 101).await.unwrap());
        let r: Batch = run_as(&db, &MetaCmd::ExpireSessions { before_ms: 1000, limit: 10 }).await.unwrap();
        assert_eq!(r, Batch { deleted: 2, more: false });
        let mut t = read(&db).await;
        assert!(get_session(&mut t, "s1").await.unwrap().is_none());
        assert!(get_session(&mut t, "s2").await.unwrap().is_some());
        table::check_indexes(&mut t, "sessions").await.unwrap();
    });
}

#[test]
fn sessions_and_passwords() {
    rt().block_on(async {
        let (eng, _s, _e) = engine(5, EngineConfig::small_for_tests());
        let db = eng.db(SYS_DB);
        db.open(true).await.unwrap();
        run(&db, &MetaCmd::Migrate).await.unwrap();
        run(&db, &signup("u1", "a@x.io", SignupPolicy::Open)).await.unwrap();
        run(&db, &signup("u2", "b@x.io", SignupPolicy::Open)).await.unwrap();
        for i in 0..7 {
            run(&db, &session(&format!("h{i}"), "u1", 9_000)).await.unwrap();
        }
        run(&db, &session("other", "u2", 9_000)).await.unwrap();
        let revoke = |user: &str, id: &str| MetaCmd::RevokeSession { user_id: user.into(), id: id.into() };
        assert!(matches!(run(&db, &revoke("u2", "id-h0")).await, Err(Error::NotFound(_))), "not yours");
        assert_eq!(run_as::<String>(&db, &revoke("u1", "id-h0")).await.unwrap(), "h0");
        let all_but = |limit| MetaCmd::RevokeSessions { user_id: "u1".into(), except: Some("h3".into()), limit };
        assert_eq!(run_as::<Batch>(&db, &all_but(2)).await.unwrap(), Batch { deleted: 2, more: true });
        assert_eq!(run_as::<Batch>(&db, &all_but(3)).await.unwrap(), Batch { deleted: 3, more: false });
        let mut t = read(&db).await;
        let left: Vec<String> =
            sessions_page(&mut t, "u1", 10, None).await.unwrap().items.into_iter().map(|s| s.token_hash).collect();
        assert_eq!(left, ["h3"]);
        assert!(get_session(&mut t, "other").await.unwrap().is_some(), "other users keep theirs");
        run(&db, &session("h4", "u1", 9_000)).await.unwrap();
        let set = |expected: Option<&str>| MetaCmd::SetPassword {
            user_id: "u1".into(),
            password_hash: "new".into(),
            expected: expected.map(|e| e.to_string()),
            keep_session: Some("h3".into()),
            verify_email: true,
        };
        assert!(matches!(run(&db, &set(Some("stale"))).await, Err(Error::PreconditionFailed { .. })));
        run(&db, &set(Some("h"))).await.unwrap();
        let mut t = read(&db).await;
        let u = get_user(&mut t, "u1").await.unwrap().unwrap();
        assert_eq!((u.password_hash.as_deref(), u.email_verified), (Some("new"), true));
        let valid = |s: Option<Session>| s.is_some_and(|s| s.password_fp == password_fp(&u));
        assert!(valid(get_session(&mut t, "h3").await.unwrap()), "the changing session carries over");
        assert!(!valid(get_session(&mut t, "h4").await.unwrap()), "other sessions end with the change");
        let verify = |email: &str| MetaCmd::VerifyEmail { user_id: "u2".into(), email: email.into() };
        assert!(run(&db, &verify("old@x.io")).await.is_err(), "bound to the current address");
        run(&db, &verify("b@x.io")).await.unwrap();
        let mut t = read(&db).await;
        check_integrity(&mut t).await.unwrap();
    });
}

#[test]
fn google_links_only_verified_accounts() {
    rt().block_on(async {
        let (eng, _s, _e) = engine(6, EngineConfig::small_for_tests());
        let db = eng.db(SYS_DB);
        db.open(true).await.unwrap();
        run(&db, &MetaCmd::Migrate).await.unwrap();
        run(&db, &signup("u1", "victim@x.io", SignupPolicy::Open)).await.unwrap();
        let google = |sub: &str| MetaCmd::GoogleLogin {
            new_id: "g1".into(),
            sub: sub.into(),
            email: "victim@x.io".into(),
            email_verified: true,
            policy: SignupPolicy::Open,
        };
        assert!(matches!(run(&db, &google("sub-1")).await, Err(Error::PermissionDenied(_))));
        run(&db, &MetaCmd::VerifyEmail { user_id: "u1".into(), email: "victim@x.io".into() }).await.unwrap();
        assert_eq!(run_as::<SignedUp>(&db, &google("sub-1")).await.unwrap().user.id, "u1");
        let mut t = read(&db).await;
        assert_eq!(user_by_google(&mut t, "sub-1").await.unwrap().map(|u| u.id).as_deref(), Some("u1"));
    });
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Ok,
    Invalid,
    Denied,
    Missing,
    Exists,
}

fn outcome(r: &Result<Json>) -> Outcome {
    match r {
        Ok(_) => Outcome::Ok,
        Err(Error::InvalidArgument(_)) => Outcome::Invalid,
        Err(Error::PermissionDenied(_)) => Outcome::Denied,
        Err(Error::NotFound(_)) => Outcome::Missing,
        Err(Error::AlreadyExists(_)) => Outcome::Exists,
        Err(e) => panic!("unexpected error {e:?}"),
    }
}

/// Test operations indexed into the model's fixed user, organization, and database pools.
#[derive(Clone, Debug)]
enum Step {
    CreateOrg { actor: usize, name: usize },
    DeleteOrg { actor: usize, name: usize },
    AddMember { actor: usize, name: usize, user: usize, role: Role },
    SetRole { actor: usize, name: usize, user: usize, role: Role },
    RemoveMember { actor: usize, name: usize, user: usize },
    CreateDb { actor: usize, org: Option<usize>, db: usize },
    DeleteDb { pick: usize },
    DeleteUser { user: usize },
    SetDisabled { actor: usize, user: usize, disabled: bool },
    CreateSession { user: usize },
}

const USERS: usize = 6;
const ORG_NAMES: [&str; 3] = ["a", "b", "c"];
const DB_NAMES: [&str; 2] = ["x", "y"];
const MAX_ORGS: usize = 2;
const MAX_MEMBERS: usize = 3;
const MAX_DBS: usize = 2;

fn gen_step(r: &mut Rng) -> Step {
    let u = |r: &mut Rng| r.usize(USERS);
    let o = |r: &mut Rng| r.usize(ORG_NAMES.len());
    let role = |r: &mut Rng| Role::ALL[r.usize(4)];
    match r.below(20) {
        0..=2 => Step::CreateOrg { actor: u(r), name: o(r) },
        3 => Step::DeleteOrg { actor: u(r), name: o(r) },
        4..=7 => Step::AddMember { actor: u(r), name: o(r), user: u(r), role: role(r) },
        8..=10 => Step::SetRole { actor: u(r), name: o(r), user: u(r), role: role(r) },
        11..=12 => Step::RemoveMember { actor: u(r), name: o(r), user: u(r) },
        13..=15 => Step::CreateDb { actor: u(r), org: r.chance(0.7).then(|| o(r)), db: r.usize(DB_NAMES.len()) },
        16 => Step::DeleteDb { pick: r.usize(8) },
        17 => Step::DeleteUser { user: u(r) },
        18 => Step::SetDisabled { actor: u(r), user: u(r), disabled: r.chance(0.6) },
        _ => Step::CreateSession { user: u(r) },
    }
}

/// Reference implementation of role transition permissions, independent of [`Role::may_change`].
fn may(actor: Role, from: Option<Role>, to: Option<Role>) -> bool {
    match actor {
        Role::Owner => true,
        Role::Admin => from != Some(Role::Owner) && to != Some(Role::Owner),
        Role::Writer | Role::Reader => false,
    }
}

#[derive(Clone, Debug)]
struct MUser {
    id: String,
    email: String,
    admin: bool,
    disabled: bool,
}

/// Reference model of organization and account rules from SPEC §9.
#[derive(Default)]
struct Model {
    users: Vec<Option<MUser>>,
    orgs: BTreeMap<String, String>,
    members: BTreeMap<(String, String), Role>,
    dbs: BTreeMap<String, (String, String)>,
    next: usize,
}

impl Model {
    fn user(&self, i: usize) -> Option<&MUser> {
        self.users[i].as_ref()
    }

    fn active(&self, i: usize) -> Result<&MUser, Outcome> {
        match self.user(i) {
            None => Err(Outcome::Missing),
            Some(u) if u.disabled => Err(Outcome::Denied),
            Some(u) => Ok(u),
        }
    }

    fn uid(&self, i: usize) -> String {
        self.user(i).map_or_else(|| format!("gone-{i}"), |u| u.id.clone())
    }

    fn org_id(&self, n: usize) -> String {
        self.orgs.get(ORG_NAMES[n]).cloned().unwrap_or_else(|| "org-missing".into())
    }

    fn role(&self, org: &str, user: &str) -> Option<Role> {
        self.members.get(&(org.to_string(), user.to_string())).copied()
    }

    fn acting(&self, org: &str, u: &MUser) -> Option<Role> {
        if u.admin && !u.disabled { Some(Role::Owner) } else { self.role(org, &u.id) }
    }

    fn require(&self, org: &str, u: &MUser, min: Role) -> Result<Role, Outcome> {
        if !self.orgs.values().any(|o| o == org) {
            return Err(Outcome::Missing);
        }
        match self.acting(org, u) {
            None => Err(Outcome::Missing),
            Some(r) if r < min => Err(Outcome::Denied),
            Some(r) => Ok(r),
        }
    }

    fn count(&self, f: impl Fn(&(String, String), &Role) -> bool) -> usize {
        self.members.iter().filter(|(k, r)| f(k, r)).count()
    }

    fn owners(&self, org: &str) -> usize {
        self.count(|(o, _), r| o == org && *r == Role::Owner)
    }

    fn orgs_of(&self, user: &str) -> usize {
        self.count(|(_, u), _| u == user)
    }

    fn step(&mut self, step: &Step) -> (MetaCmd, Outcome) {
        let (cmd, r) = self.predict(step);
        (cmd, r.err().unwrap_or(Outcome::Ok))
    }

    fn predict(&mut self, step: &Step) -> (MetaCmd, Result<(), Outcome>) {
        match *step {
            Step::CreateOrg { actor, name } => {
                self.next += 1;
                let id = format!("{ORG_ID_PREFIX}{}", self.next);
                let cmd = MetaCmd::CreateOrg {
                    id: id.clone(),
                    name: ORG_NAMES[name].into(),
                    actor: self.uid(actor),
                    max_orgs: MAX_ORGS,
                };
                let r = (|| {
                    let me = self.active(actor)?.id.clone();
                    if self.orgs_of(&me) >= MAX_ORGS {
                        return Err(Outcome::Denied);
                    }
                    if self.orgs.contains_key(ORG_NAMES[name]) {
                        return Err(Outcome::Exists);
                    }
                    self.orgs.insert(ORG_NAMES[name].into(), id.clone());
                    self.members.insert((id.clone(), me), Role::Owner);
                    Ok(())
                })();
                (cmd, r)
            }
            Step::DeleteOrg { actor, name } => {
                let org = self.org_id(name);
                let cmd = MetaCmd::DeleteOrg { org_id: org.clone(), actor: self.uid(actor) };
                let r = (|| {
                    let me = self.active(actor)?.clone();
                    self.require(&org, &me, Role::Owner)?;
                    if self.dbs.values().any(|(o, _)| *o == org) {
                        return Err(Outcome::Denied);
                    }
                    self.members.retain(|(o, _), _| *o != org);
                    self.orgs.retain(|_, id| *id != org);
                    Ok(())
                })();
                (cmd, r)
            }
            Step::AddMember { actor, name, user, role } => {
                let org = self.org_id(name);
                let email = self.user(user).map_or_else(|| format!("gone{user}@x.io"), |u| u.email.clone());
                let cmd = MetaCmd::AddMember {
                    org_id: org.clone(),
                    actor: self.uid(actor),
                    email,
                    role,
                    limits: MemberLimits { max_members: MAX_MEMBERS, max_orgs: MAX_ORGS },
                };
                let r = (|| {
                    let me = self.active(actor)?.clone();
                    let mine = self.require(&org, &me, Role::Admin)?;
                    if !may(mine, None, Some(role)) {
                        return Err(Outcome::Denied);
                    }
                    let target = self.user(user).ok_or(Outcome::Missing)?.id.clone();
                    if self.role(&org, &target).is_some() {
                        return Err(Outcome::Exists);
                    }
                    if self.count(|(o, _), _| *o == org) >= MAX_MEMBERS || self.orgs_of(&target) >= MAX_ORGS {
                        return Err(Outcome::Denied);
                    }
                    self.members.insert((org.clone(), target), role);
                    Ok(())
                })();
                (cmd, r)
            }
            Step::SetRole { actor, name, user, role } => {
                let org = self.org_id(name);
                let cmd =
                    MetaCmd::SetRole { org_id: org.clone(), actor: self.uid(actor), user_id: self.uid(user), role };
                let r = (|| {
                    let me = self.active(actor)?.clone();
                    let mine = self.require(&org, &me, Role::Admin)?;
                    let target = self.uid(user);
                    let cur = self.role(&org, &target).ok_or(Outcome::Missing)?;
                    if !may(mine, Some(cur), Some(role)) {
                        return Err(Outcome::Denied);
                    }
                    if cur == Role::Owner && role != Role::Owner && self.owners(&org) <= 1 {
                        return Err(Outcome::Denied);
                    }
                    self.members.insert((org.clone(), target), role);
                    Ok(())
                })();
                (cmd, r)
            }
            Step::RemoveMember { actor, name, user } => {
                let org = self.org_id(name);
                let cmd =
                    MetaCmd::RemoveMember { org_id: org.clone(), actor: self.uid(actor), user_id: self.uid(user) };
                let r = (|| {
                    let me = self.active(actor)?.clone();
                    let mine = self.require(&org, &me, Role::Reader)?;
                    let target = self.uid(user);
                    let cur = self.role(&org, &target).ok_or(Outcome::Missing)?;
                    if me.id != target && !may(mine, Some(cur), None) {
                        return Err(Outcome::Denied);
                    }
                    if cur == Role::Owner && self.owners(&org) <= 1 {
                        return Err(Outcome::Denied);
                    }
                    self.members.remove(&(org.clone(), target));
                    Ok(())
                })();
                (cmd, r)
            }
            Step::CreateDb { actor, org, db } => {
                self.next += 1;
                let id = format!("d{}", self.next);
                let owner = match org {
                    Some(n) => self.org_id(n),
                    None => self.uid(actor),
                };
                let name = DB_NAMES[db].to_string();
                let cmd = MetaCmd::CreateDb {
                    id: id.clone(),
                    owner_id: owner.clone(),
                    actor: self.uid(actor),
                    name: name.clone(),
                    max_dbs: MAX_DBS,
                };
                let r = (|| {
                    let me = self.active(actor)?.clone();
                    if org.is_some() {
                        self.require(&owner, &me, Role::Admin)?;
                    }
                    if self.dbs.values().any(|(o, n)| *o == owner && *n == name) {
                        return Err(Outcome::Exists);
                    }
                    if self.dbs.values().filter(|(o, _)| *o == owner).count() >= MAX_DBS {
                        return Err(Outcome::Denied);
                    }
                    self.dbs.insert(id.clone(), (owner.clone(), name.clone()));
                    Ok(())
                })();
                (cmd, r)
            }
            Step::DeleteDb { pick } => {
                let id = self.dbs.keys().nth(pick).cloned().unwrap_or_else(|| "d-missing".into());
                let r = self.dbs.remove(&id).map(|_| ()).ok_or(Outcome::Missing);
                (MetaCmd::DeleteDb { id }, r)
            }
            Step::DeleteUser { user } => {
                let cmd = MetaCmd::DeleteUser { user_id: self.uid(user) };
                let r = (|| {
                    let u = self.user(user).ok_or(Outcome::Missing)?.clone();
                    let other_admin = self.users.iter().flatten().any(|o| o.id != u.id && o.admin && !o.disabled);
                    if u.admin && !u.disabled && !other_admin {
                        return Err(Outcome::Denied);
                    }
                    let sole =
                        self.members.iter().any(|((o, m), r)| *m == u.id && *r == Role::Owner && self.owners(o) <= 1);
                    if sole {
                        return Err(Outcome::Denied);
                    }
                    self.members.retain(|(_, m), _| *m != u.id);
                    self.dbs.retain(|_, (o, _)| *o != u.id);
                    self.users[user] = None;
                    Ok(())
                })();
                (cmd, r)
            }
            Step::SetDisabled { actor, user, disabled } => {
                let cmd = MetaCmd::SetDisabled { actor: self.uid(actor), user_id: self.uid(user), disabled };
                let r = (|| {
                    let me = self.active(actor)?.clone();
                    if !me.admin {
                        return Err(Outcome::Denied);
                    }
                    if me.id == self.uid(user) && disabled {
                        return Err(Outcome::Invalid);
                    }
                    let u = self.users[user].as_mut().ok_or(Outcome::Missing)?;
                    u.disabled = disabled;
                    Ok(())
                })();
                (cmd, r)
            }
            Step::CreateSession { user } => {
                self.next += 1;
                let cmd = session(&format!("t{}", self.next), &self.uid(user), 1_000_000);
                (cmd, self.active(user).map(|_| ()))
            }
        }
    }
}

async fn stored_state(
    db: &Arc<Db>,
) -> (
    BTreeMap<String, String>,
    BTreeMap<(String, String), Role>,
    BTreeMap<String, (String, String)>,
    BTreeSet<(String, bool)>,
) {
    let mut t = read(db).await;
    check_integrity(&mut t).await.unwrap();
    let orgs: Vec<Org> = query_all(&mut t, "orgs", Query::default()).await.unwrap();
    let members: Vec<Member> = query_all(&mut t, "org_members", Query::default()).await.unwrap();
    let dbs = list_all_dbs(&mut t).await.unwrap();
    let users = list_users(&mut t).await.unwrap();
    for name in ["users", "orgs", "org_members", "databases", "sessions"] {
        table::check_indexes(&mut t, name).await.unwrap();
    }
    (
        orgs.into_iter().map(|o| (o.name, o.id)).collect(),
        members.into_iter().map(|m| ((m.org_id, m.user_id), m.role)).collect(),
        dbs.into_iter().map(|d| (d.id, (d.owner_id, d.name))).collect(),
        users.into_iter().map(|u| (u.id, u.disabled)).collect(),
    )
}

#[test]
fn organizations_follow_the_role_rules() {
    prop::check(
        "organization role rules",
        150,
        |r| (0..10 + r.usize(50)).map(|_| gen_step(r)).collect::<Vec<_>>(),
        |v| prop::shrink_vec(v),
        |steps| {
            rt().block_on(async {
                let (eng, _s, _e) = engine(7, EngineConfig::small_for_tests());
                let db = eng.db(SYS_DB);
                db.open(true).await.unwrap();
                run(&db, &MetaCmd::Migrate).await.unwrap();
                let mut m = Model::default();
                for i in 0..USERS {
                    let (id, email) = (format!("u{i}"), format!("u{i}@x.io"));
                    let cmd = MetaCmd::Signup {
                        id: id.clone(),
                        email: email.clone(),
                        password_hash: None,
                        policy: SignupPolicy::Open,
                        bootstrap_admin: i == USERS - 1,
                    };
                    run(&db, &cmd).await.unwrap();
                    m.users.push(Some(MUser { id, email, admin: i == 0 || i == USERS - 1, disabled: false }));
                }
                for (i, s) in steps.iter().enumerate() {
                    let (cmd, want) = m.step(s);
                    let got = outcome(&run(&db, &cmd).await);
                    if got != want {
                        return Err(format!("step {i} {s:?}: got {got:?}, the rules say {want:?}"));
                    }
                    let (orgs, members, dbs, users) = stored_state(&db).await;
                    let want_users: BTreeSet<(String, bool)> =
                        m.users.iter().flatten().map(|u| (u.id.clone(), u.disabled)).collect();
                    if orgs != m.orgs || members != m.members || dbs != m.dbs || users != want_users {
                        return Err(format!(
                            "after step {i} {s:?}: stored {orgs:?} {members:?} {dbs:?} {users:?}\n\
                             model {:?} {:?} {:?} {want_users:?}",
                            m.orgs, m.members, m.dbs
                        ));
                    }
                }
                Ok(())
            })
        },
    );
}

#[test]
fn role_lattice() {
    for a in Role::ALL {
        for b in Role::ALL {
            if a <= b {
                assert!(!a.can_write() || b.can_write(), "writing is monotone in the role");
                assert!(!a.can_administer() || b.can_administer());
                for from in [None, Some(Role::Reader), Some(Role::Owner)] {
                    for to in [None, Some(Role::Writer), Some(Role::Owner)] {
                        assert!(!a.may_change(from, to) || b.may_change(from, to), "{a:?} {b:?} {from:?} {to:?}");
                    }
                }
            }
        }
        assert_eq!(serde_json::to_value(a).unwrap(), json!(a.name()), "name() is the serialized name");
        assert!(!Role::Admin.may_change(Some(Role::Owner), Some(Role::Reader)), "admins cannot demote owners");
        assert!(!Role::Admin.may_change(None, Some(Role::Owner)), "nor make owners");
        assert!(Role::Admin.may_change(Some(Role::Writer), Some(Role::Admin)));
        assert!(!Role::Writer.may_change(Some(Role::Reader), None));
    }
}

#[test]
fn db_refs() {
    let p = |s: &str| DbRef::parse(s);
    assert_eq!(p("app"), Some(DbRef::Personal("app".into())));
    assert_eq!(p("acme.billing"), Some(DbRef::Org { org: "acme".into(), name: "billing".into() }));
    assert_eq!(p("id:abc"), Some(DbRef::Id("abc".into())));
    for bad in
        ["", "A", "a.b.c", ".b", "a.", "id:", "-a", "a/b", "a..b", &"x".repeat(64), &format!("{}.x", "o".repeat(40))]
    {
        assert_eq!(p(bad), None, "{bad}");
    }
    assert_eq!(DbRef::format(Some("acme"), "x"), "acme.x");
    let mut r = Rng::new(3);
    let pieces = ["", ".", "id:", "a", "-", "_", "Z", "é", "%2E", "0", ":", "/", &"o".repeat(39)];
    for _ in 0..20_000 {
        let s: String = (0..r.usize(6)).map(|_| pieces[r.usize(pieces.len())]).collect();
        match p(&s) {
            Some(DbRef::Personal(n)) => assert_eq!(DbRef::format(None, &n), s),
            Some(DbRef::Org { org, name }) => assert_eq!(DbRef::format(Some(&org), &name), s),
            Some(DbRef::Id(id)) => assert_eq!(format!("id:{id}"), s),
            None => {}
        }
    }
    prop::check(
        "db refs round trip",
        500,
        |r| {
            let s = |r: &mut Rng, n: usize| {
                (0..1 + r.usize(n)).map(|_| (b'a' + r.below(26) as u8) as char).collect::<String>()
            };
            (r.chance(0.5).then(|| s(r, 39)), s(r, 63))
        },
        prop::no_shrink,
        |(org, name)| {
            let want = match org {
                Some(o) => DbRef::Org { org: o.clone(), name: name.clone() },
                None => DbRef::Personal(name.clone()),
            };
            match DbRef::parse(&DbRef::format(org.as_deref(), name)) {
                Some(got) if got == want => Ok(()),
                other => Err(format!("{other:?} != {want:?}")),
            }
        },
    );
}
