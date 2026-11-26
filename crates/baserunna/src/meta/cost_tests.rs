//! Tests metadata command read and write costs and batched deletion.

use super::tests::{engine, rt, session};
use super::*;
use crate::node::{META_BATCH, NodeConfig};
use baserunna_api::PAGE_MAX;
use baserunna_storage::engine::{Consistency, Db, EngineConfig, Isolation, TxnCost};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

/// Maximum email address length from SPEC §8.
const EMAIL_MAX: usize = 254;
/// Maximum API key name length from SPEC §8.
const API_KEY_NAME_MAX: usize = 100;
const NOW_MS: i64 = 1_000;
/// Maximum entries a command can read and write.
///
/// Allows 16 store entries per row across `PAGE_MAX` rows.
const COMMAND_ENTRIES_MAX: usize = 16 * PAGE_MAX;
const LIVE_SESSION_EXPIRES_MS: i64 = 1 << 50;

fn member_limits(cfg: &NodeConfig) -> MemberLimits {
    MemberLimits { max_members: cfg.max_members_per_org, max_orgs: cfg.max_orgs_per_user }
}

async fn begin_txn(db: &Arc<Db>) -> Txn {
    db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap()
}

/// Commits `cmds` in batches of 200 commands per transaction.
async fn commit_cmds(db: &Arc<Db>, cmds: Vec<MetaCmd>) {
    for chunk in cmds.chunks(200) {
        let mut txn = begin_txn(db).await;
        for cmd in chunk {
            apply(&mut txn, &BlobResolver { db }, cmd, NOW_MS).await.unwrap_or_else(|e| panic!("{cmd:?}: {e}"));
        }
        db.commit(txn.into_request()).await.unwrap();
    }
}

/// Runs `cmd` without committing and returns its response and [`TxnCost`].
async fn measure_cmd(db: &Arc<Db>, cmd: &MetaCmd) -> (Json, TxnCost) {
    let mut txn = begin_txn(db).await;
    let answer = apply(&mut txn, &BlobResolver { db }, cmd, NOW_MS).await.unwrap_or_else(|e| panic!("{cmd:?}: {e}"));
    (answer, txn.cost())
}

/// Returns a maximum-length email address for `user_id`.
fn max_length_email(user_id: &str) -> String {
    let domain = "x".repeat(EMAIL_MAX - user_id.len() - 4);
    format!("{user_id}@{domain}.io")
}

fn padded_name(prefix: &str, i: usize, len: usize) -> String {
    let head = format!("{prefix}{i}-");
    format!("{head}{}", "a".repeat(len - head.len()))
}

fn signup_cmd(env: &baserunna_util::env::Env, user_id: &str) -> MetaCmd {
    MetaCmd::Signup {
        id: user_id.into(),
        email: max_length_email(user_id),
        password_hash: Some(crate::auth::hash_password(env, "password", 1)),
        policy: SignupPolicy::Open,
        bootstrap_admin: false,
    }
}

fn token_hash(user_id: &str, i: usize) -> String {
    crate::auth::fingerprint(&format!("{user_id}/{i}")).repeat(4)
}

fn create_session_cmds(user_id: &str, count: usize, expires_at: i64) -> Vec<MetaCmd> {
    (0..count)
        .map(|i| {
            let mut cmd = session(&token_hash(user_id, i), user_id, expires_at);
            if let MetaCmd::CreateSession { client, .. } = &mut cmd {
                *client = Some("2001:db8:ffff:ffff:ffff:ffff:ffff:ffff".into());
            }
            cmd
        })
        .collect()
}

fn api_key(user_id: &str, i: usize) -> ApiKey {
    ApiKey {
        id: format!("key-{user_id}-{i}"),
        user_id: user_id.into(),
        name: padded_name("key", i, API_KEY_NAME_MAX),
        secret_hash: token_hash("secret", i),
        db_id: None,
        read_only: false,
        rate_limit: None,
        created_at: NOW_MS,
    }
}

fn node_record(id: &str) -> NodeRecord {
    NodeRecord { id: id.into(), addr: format!("http://{id}.example:8080"), started_at: 1, heartbeat_at: NOW_MS }
}

/// Fills every per-owner limit and paginated listing to test worst-case command costs.
///
/// - User `victim` has `PAGE_MAX - 1` sessions, `PAGE_MAX - 1` API keys, and the maximum number of memberships.
/// - User `partner` owns all of `victim`'s organizations.
/// - User `dbowner` owns the maximum number of personal databases.
/// - Organization `org-big` has the maximum number of members. Site administrator `admin` is not a member.
/// - User `m0` has more than `PAGE_MAX` active sessions.
/// - Other users have more than `PAGE_MAX` expired sessions.
async fn fill_limits(db: &Arc<Db>, env: &baserunna_util::env::Env, cfg: &NodeConfig) {
    let member_ids: Vec<String> = (0..cfg.max_members_per_org - 2).map(|i| format!("m{i}")).collect();
    let mut cmds = vec![MetaCmd::Migrate];
    for id in ["admin", "victim", "partner", "dbowner"].into_iter().chain(member_ids.iter().map(|s| s.as_str())) {
        cmds.push(signup_cmd(env, id));
    }
    cmds.push(MetaCmd::CreateOrg {
        id: "org-big".into(),
        name: padded_name("big", 0, ORG_NAME_MAX),
        actor: "partner".into(),
        max_orgs: cfg.max_orgs_per_user,
    });
    let add_member = |org_id: &str, actor: &str, user_id: &str, role: Role| MetaCmd::AddMember {
        org_id: org_id.into(),
        actor: actor.into(),
        email: max_length_email(user_id),
        role,
        limits: member_limits(cfg),
    };
    cmds.push(add_member("org-big", "admin", "victim", Role::Owner));
    for id in &member_ids {
        cmds.push(add_member("org-big", "admin", id, Role::Reader));
    }
    for i in 0..cfg.max_orgs_per_user - 1 {
        let id = format!("org-p{i}");
        let name = padded_name("p", i, ORG_NAME_MAX);
        cmds.push(MetaCmd::CreateOrg {
            id: id.clone(),
            name,
            actor: "partner".into(),
            max_orgs: cfg.max_orgs_per_user,
        });
        cmds.push(add_member(&id, "partner", "victim", Role::Owner));
    }
    cmds.extend(create_session_cmds("victim", PAGE_MAX - 1, LIVE_SESSION_EXPIRES_MS));
    cmds.extend(create_session_cmds("m0", PAGE_MAX + 1, LIVE_SESSION_EXPIRES_MS));
    for id in member_ids.iter().skip(1).take(3) {
        cmds.extend(create_session_cmds(id, PAGE_MAX / 2, 10));
    }
    cmds.extend((0..PAGE_MAX - 1).map(|i| MetaCmd::CreateApiKey { key: api_key("victim", i) }));
    let create_db = |i: usize| MetaCmd::CreateDb {
        id: format!("db-dbowner-{i}"),
        owner_id: "dbowner".into(),
        actor: "dbowner".into(),
        name: padded_name("db", i, 63),
        max_dbs: cfg.max_dbs_per_owner,
    };
    cmds.push(create_db(cfg.max_dbs_per_owner));
    cmds.push(MetaCmd::DeleteDb { id: format!("db-dbowner-{}", cfg.max_dbs_per_owner) });
    cmds.extend((0..cfg.max_dbs_per_owner).map(create_db));
    cmds.extend(
        ["node-a", "node-b", "node-c"].map(|id| MetaCmd::Heartbeat { node: node_record(id), dead_after_ms: 3_600_000 }),
    );
    commit_cmds(db, cmds).await;
    let mut txn = begin_txn(db).await;
    check_integrity(&mut txn).await.unwrap();
}

/// Returns a worst-case instance of every [`MetaCmd`] variant against the state from [`fill_limits`].
fn worst_case_commands(
    env: &baserunna_util::env::Env,
    cfg: &NodeConfig,
    victim_password_hash: Option<String>,
) -> Vec<MetaCmd> {
    let victim_session = token_hash("victim", 0);
    vec![
        MetaCmd::Migrate,
        MetaCmd::EnsureConfig { key: "cluster_key".into(), value: token_hash("cluster", 0) },
        signup_cmd(env, "newcomer"),
        MetaCmd::GoogleLogin {
            new_id: "googler".into(),
            sub: "1".repeat(255),
            email: max_length_email("googler"),
            email_verified: true,
            policy: SignupPolicy::Open,
        },
        MetaCmd::SetPassword {
            user_id: "victim".into(),
            password_hash: crate::auth::hash_password(env, "new password", 1),
            expected: victim_password_hash,
            keep_session: Some(victim_session.clone()),
            verify_email: true,
        },
        MetaCmd::VerifyEmail { user_id: "victim".into(), email: max_length_email("victim") },
        MetaCmd::SetDisabled { actor: "admin".into(), user_id: "victim".into(), disabled: true },
        create_session_cmds("victim", PAGE_MAX + 1, LIVE_SESSION_EXPIRES_MS).pop().unwrap(),
        MetaCmd::DeleteSession { token_hash: victim_session.clone() },
        MetaCmd::RevokeSession { user_id: "victim".into(), id: format!("id-{victim_session}") },
        MetaCmd::RevokeSessions { user_id: "m0".into(), except: Some(token_hash("m0", 0)), limit: META_BATCH },
        MetaCmd::CreateApiKey { key: api_key("victim", PAGE_MAX) },
        MetaCmd::DeleteApiKey { id: api_key("victim", 0).id, user_id: "victim".into() },
        MetaCmd::CreateDb {
            id: "db-shared".into(),
            owner_id: "org-p0".into(),
            actor: "victim".into(),
            name: padded_name("shared", 0, 63),
            max_dbs: cfg.max_dbs_per_owner,
        },
        MetaCmd::DeleteDb { id: "db-dbowner-0".into() },
        MetaCmd::DeleteUser { user_id: "victim".into() },
        MetaCmd::DeleteUser { user_id: "m0".into() },
        MetaCmd::DeleteUser { user_id: "dbowner".into() },
        MetaCmd::DeleteUser { user_id: "partner".into() },
        MetaCmd::PurgePass { id: format!("db-dbowner-{}", cfg.max_dbs_per_owner), passes: 0 },
        MetaCmd::ExpireSessions { before_ms: NOW_MS, limit: META_BATCH },
        MetaCmd::Heartbeat { node: node_record("node-d"), dead_after_ms: 3_600_000 },
        MetaCmd::Leave { id: "node-a".into() },
        MetaCmd::CreateOrg {
            id: "org-new".into(),
            name: padded_name("new", 0, ORG_NAME_MAX),
            actor: "admin".into(),
            max_orgs: cfg.max_orgs_per_user,
        },
        MetaCmd::DeleteOrg { org_id: "org-big".into(), actor: "admin".into() },
        MetaCmd::AddMember {
            org_id: "org-p0".into(),
            actor: "partner".into(),
            email: max_length_email("m1"),
            role: Role::Admin,
            limits: member_limits(cfg),
        },
        MetaCmd::SetRole {
            org_id: "org-p0".into(),
            actor: "partner".into(),
            user_id: "victim".into(),
            role: Role::Admin,
        },
        MetaCmd::RemoveMember { org_id: "org-big".into(), actor: "admin".into(), user_id: "m1".into() },
    ]
}

/// Adds unlimited tenant data while keeping every limited count from [`fill_limits`] unchanged.
async fn add_unlimited_data(db: &Arc<Db>, env: &baserunna_util::env::Env, cfg: &NodeConfig) {
    let mut cmds = create_session_cmds("m0", 2 * PAGE_MAX, LIVE_SESSION_EXPIRES_MS).split_off(PAGE_MAX + 1);
    for i in 0..cfg.max_members_per_org {
        let id = format!("o{i}");
        cmds.push(signup_cmd(env, &id));
        cmds.extend(create_session_cmds(&id, 1, LIVE_SESSION_EXPIRES_MS));
        cmds.extend(create_session_cmds(&format!("{id}-expired"), 1, 10).into_iter().map(|mut cmd| {
            if let MetaCmd::CreateSession { user_id, .. } = &mut cmd {
                *user_id = id.clone();
            }
            cmd
        }));
        cmds.push(MetaCmd::CreateApiKey { key: api_key(&id, 0) });
        for name in ["kept", "gone"] {
            cmds.push(MetaCmd::CreateDb {
                id: format!("db-{id}-{name}"),
                owner_id: id.clone(),
                actor: id.clone(),
                name: padded_name(name, i, 63),
                max_dbs: cfg.max_dbs_per_owner,
            });
        }
        cmds.push(MetaCmd::DeleteDb { id: format!("db-{id}-gone") });
        cmds.push(MetaCmd::CreateOrg {
            id: format!("org-{id}"),
            name: padded_name("o", i, ORG_NAME_MAX),
            actor: id.clone(),
            max_orgs: cfg.max_orgs_per_user,
        });
    }
    commit_cmds(db, cmds).await;
    let mut txn = begin_txn(db).await;
    check_integrity(&mut txn).await.unwrap();
}

async fn table_row_counts(db: &Arc<Db>) -> BTreeMap<String, usize> {
    let mut txn = begin_txn(db).await;
    let mut rows = BTreeMap::new();
    for def in tables() {
        let name = def["name"].as_str().unwrap();
        rows.insert(name.to_string(), query_all::<Json>(&mut txn, name, Query::default()).await.unwrap().len());
    }
    rows
}

/// Measures [`TxnCost`] for every command from [`worst_case_commands`] without committing.
async fn measure_all_commands(
    db: &Arc<Db>,
    env: &baserunna_util::env::Env,
    cfg: &NodeConfig,
) -> Vec<(&'static str, TxnCost)> {
    let victim = get_user(&mut begin_txn(db).await, "victim").await.unwrap().unwrap();
    let mut costs = Vec::new();
    for cmd in worst_case_commands(env, cfg, victim.password_hash) {
        costs.push((cmd.name(), measure_cmd(db, &cmd).await.1));
    }
    costs
}

/// Compares entry counts before and after adding unlimited data. Field length rules limit bytes per entry.
#[test]
fn metadata_command_cost_stays_constant_with_stored_data() {
    rt().block_on(async {
        let (eng, _store, env) = engine(5, EngineConfig::small_for_tests());
        let db = eng.db(SYS_DB);
        db.open(true).await.unwrap();
        let cfg = NodeConfig::default();
        fill_limits(&db, &env, &cfg).await;
        let (rows_before, costs_before) = (table_row_counts(&db).await, measure_all_commands(&db, &env, &cfg).await);
        add_unlimited_data(&db, &env, &cfg).await;
        let (rows_after, costs_after) = (table_row_counts(&db).await, measure_all_commands(&db, &env, &cfg).await);
        for (table, before) in &rows_before {
            let should_grow = !["nodes", "config"].contains(&table.as_str());
            assert_eq!(rows_after[table] > *before, should_grow, "{table}: {before} rows, then {}", rows_after[table]);
        }
        let measured: BTreeSet<&str> = costs_before.iter().map(|(name, _)| *name).collect();
        assert_eq!(
            measured,
            BTreeSet::from_iter(MetaCmd::NAMES.iter().copied()),
            "missing metadata commands from worst_case_commands"
        );
        let mut report = format!("rows before: {rows_before:?}\nrows after: {rows_after:?}\n");
        let mut failures = Vec::new();
        for ((name, before), (_, after)) in costs_before.iter().zip(&costs_after) {
            report.push_str(&format!("{name:<16} {before:?}\n"));
            if (before.entries_read, before.entries_written) != (after.entries_read, after.entries_written) {
                failures.push(format!("{name}: {before:?}, then {after:?}"));
            }
            if after.entries_read + after.entries_written > COMMAND_ENTRIES_MAX {
                failures.push(format!("{name}: {after:?} exceeds {COMMAND_ENTRIES_MAX} entries"));
            }
        }
        eprintln!("{report}");
        assert!(failures.is_empty(), "{failures:#?}");
    });
}

/// Repeats `cmd` until `more` is `false` and returns the batch count.
///
/// Checks metadata integrity after each batch to simulate a node stopping between batches.
async fn run_batches_until_done(db: &Arc<Db>, cmd: &MetaCmd) -> usize {
    for batch_count in 1.. {
        let mut txn = begin_txn(db).await;
        let answer =
            apply(&mut txn, &BlobResolver { db }, cmd, NOW_MS).await.unwrap_or_else(|e| panic!("{cmd:?}: {e}"));
        db.commit(txn.into_request()).await.unwrap();
        let mut txn = begin_txn(db).await;
        check_integrity(&mut txn).await.unwrap_or_else(|e| panic!("after batch {batch_count} of {cmd:?}: {e}"));
        if answer["more"] == json!(false) {
            return batch_count;
        }
    }
    unreachable!()
}

#[test]
fn batched_deletes_remove_owners_at_their_limits() {
    rt().block_on(async {
        let (eng, _store, env) = engine(6, EngineConfig::small_for_tests());
        let db = eng.db(SYS_DB);
        db.open(true).await.unwrap();
        let cfg = NodeConfig::default();
        fill_limits(&db, &env, &cfg).await;
        let delete_org = MetaCmd::DeleteOrg { org_id: "org-big".into(), actor: "admin".into() };
        assert_eq!(
            run_batches_until_done(&db, &delete_org).await,
            2,
            "a site administrator deletes 1,000 members in two batches"
        );
        for (user_id, batches) in [("victim", 3), ("dbowner", 2)] {
            let cmd = MetaCmd::DeleteUser { user_id: user_id.into() };
            assert_eq!(run_batches_until_done(&db, &cmd).await, batches, "{user_id}");
        }
        let mut txn = begin_txn(&db).await;
        let delete_partner = MetaCmd::DeleteUser { user_id: "partner".into() };
        let partner_delete_result = apply(&mut txn, &BlobResolver { db: &db }, &delete_partner, NOW_MS).await;
        assert!(
            matches!(partner_delete_result, Err(Error::PermissionDenied(_))),
            "partner is now the only owner: {partner_delete_result:?}"
        );
        let mut txn = begin_txn(&db).await;
        check_integrity(&mut txn).await.unwrap();
        for user_id in ["victim", "dbowner"] {
            assert_eq!(get_user(&mut txn, user_id).await.unwrap(), None, "{user_id}");
        }
        assert_eq!(org::get_org(&mut txn, "org-big").await.unwrap(), None);
        assert_eq!(dbs_page(&mut txn, "dbowner", 10, None).await.unwrap().items, vec![]);
        let deleted = list_deleted_dbs(&mut txn).await.unwrap();
        assert_eq!(deleted.len(), cfg.max_dbs_per_owner + 1, "every deleted database is recorded for storage purge");
    });
}
