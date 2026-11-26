//! Tests that each metadata command reads and writes at most a fixed number of rows and bytes.

use super::tests::{engine, rt, session};
use super::*;
use crate::node::{META_BATCH, NodeConfig};
use baserunna_api::PAGE_MAX;
use baserunna_storage::engine::{Consistency, Db, EngineConfig, Isolation, TxnCost};
use std::collections::BTreeSet;
use std::sync::Arc;

/// Length limits from SPEC §8 for emails and API key names.
const EMAIL_MAX: usize = 254;
const API_KEY_NAME_MAX: usize = 100;
const NOW_MS: i64 = 1_000;
const LIVE_UNTIL_MS: i64 = 1 << 50;

/// Store entries that one metadata row costs: the row, its index entries, and the table schema.
const ENTRIES_PER_ROW: usize = 8;
/// Bytes that one metadata row costs with every field at its length limit.
const BYTES_PER_ROW: usize = 2048;
/// Rows that a command without a multi-row step can touch.
const SINGLE_ROWS: usize = 16;

fn member_limits(cfg: &NodeConfig) -> MemberLimits {
    MemberLimits { max_members: cfg.max_members_per_org, max_orgs: cfg.max_orgs_per_user }
}

/// Returns the most metadata rows that `cmd` can touch.
fn row_limit(cmd: &MetaCmd, cfg: &NodeConfig) -> usize {
    match cmd {
        MetaCmd::Migrate
        | MetaCmd::EnsureConfig { .. }
        | MetaCmd::Signup { .. }
        | MetaCmd::GoogleLogin { .. }
        | MetaCmd::SetPassword { .. }
        | MetaCmd::VerifyEmail { .. }
        | MetaCmd::SetDisabled { .. }
        | MetaCmd::CreateSession { .. }
        | MetaCmd::DeleteSession { .. }
        | MetaCmd::RevokeSession { .. }
        | MetaCmd::CreateApiKey { .. }
        | MetaCmd::DeleteApiKey { .. }
        | MetaCmd::CreateDb { .. }
        | MetaCmd::DeleteDb { .. }
        | MetaCmd::PurgePass { .. }
        | MetaCmd::Leave { .. }
        | MetaCmd::CreateOrg { .. }
        | MetaCmd::AddMember { .. }
        | MetaCmd::SetRole { .. }
        | MetaCmd::RemoveMember { .. } => SINGLE_ROWS,
        MetaCmd::RevokeSessions { .. } | MetaCmd::DeleteApiKeys { .. } | MetaCmd::ExpireSessions { .. } => {
            SINGLE_ROWS + PAGE_MAX
        }
        // Each database has a row and a deleted row. Each membership has a row and up to two counter rows.
        MetaCmd::DeleteUser { .. } => {
            SINGLE_ROWS + 2 * PAGE_MAX + 2 * cfg.max_dbs_per_owner + 3 * cfg.max_orgs_per_user
        }
        // Each membership has a row and a counter row for its user.
        MetaCmd::DeleteOrg { .. } => SINGLE_ROWS + 2 * cfg.max_members_per_org,
        // Heartbeat reads every node row. Operators control the node count, and tenants cannot change it.
        MetaCmd::Heartbeat { .. } => SINGLE_ROWS,
    }
}

const COMMAND_NAMES: [&str; 26] = [
    "migrate",
    "ensure_config",
    "signup",
    "google_login",
    "set_password",
    "verify_email",
    "set_disabled",
    "create_session",
    "delete_session",
    "revoke_session",
    "revoke_sessions",
    "create_api_key",
    "delete_api_key",
    "delete_api_keys",
    "create_db",
    "delete_db",
    "delete_user",
    "purge_pass",
    "expire_sessions",
    "heartbeat",
    "leave",
    "create_org",
    "delete_org",
    "add_member",
    "set_role",
    "remove_member",
];

async fn begin(db: &Arc<Db>) -> Txn {
    db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap()
}

/// Commits `cmds` in transactions of a few hundred commands each.
async fn apply_all(db: &Arc<Db>, cmds: Vec<MetaCmd>) {
    for chunk in cmds.chunks(200) {
        let mut t = begin(db).await;
        for cmd in chunk {
            apply(&mut t, &BlobResolver { db }, cmd, NOW_MS).await.unwrap_or_else(|e| panic!("{cmd:?}: {e}"));
        }
        db.commit(t.into_request()).await.unwrap();
    }
}

/// Runs `cmd` in a transaction that does not commit, and returns its answer and cost.
async fn measure(db: &Arc<Db>, cmd: &MetaCmd) -> (Json, TxnCost) {
    let mut t = begin(db).await;
    let answer = apply(&mut t, &BlobResolver { db }, cmd, NOW_MS).await.unwrap_or_else(|e| panic!("{cmd:?}: {e}"));
    (answer, t.cost())
}

/// Returns an email address of the maximum length for `user_id`.
fn long_email(user_id: &str) -> String {
    let domain = "x".repeat(EMAIL_MAX - user_id.len() - 4);
    format!("{user_id}@{domain}.io")
}

fn long_name(prefix: &str, i: usize, len: usize) -> String {
    let head = format!("{prefix}{i}-");
    format!("{head}{}", "a".repeat(len - head.len()))
}

fn signup_cmd(env: &baserunna_util::env::Env, id: &str) -> MetaCmd {
    MetaCmd::Signup {
        id: id.into(),
        email: long_email(id),
        password_hash: Some(crate::auth::hash_password(env, "password", 1)),
        policy: SignupPolicy::Open,
        bootstrap_admin: false,
    }
}

fn token_hash(user_id: &str, i: usize) -> String {
    crate::auth::fingerprint(&format!("{user_id}/{i}")).repeat(4)
}

fn sessions(user_id: &str, n: usize, expires_at: i64) -> Vec<MetaCmd> {
    (0..n)
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
        name: long_name("key", i, API_KEY_NAME_MAX),
        secret_hash: token_hash("secret", i),
        db_id: None,
        read_only: false,
        rate_limit: None,
        created_at: NOW_MS,
    }
}

fn node(id: &str) -> NodeRecord {
    NodeRecord { id: id.into(), addr: format!("http://{id}.example:8080"), started_at: 1, heartbeat_at: NOW_MS }
}

/// Fills every per-owner limit and every listing that a command can scan.
///
/// - The user `victim` has the most sessions and API keys that `delete_user` deletes in one transaction.
///   This user also has the maximum number of personal databases and organization memberships.
/// - The organization `org-big` has the maximum number of members.
/// - The user `m0` has more than `PAGE_MAX` sessions.
/// - Other users have more than `PAGE_MAX` expired sessions.
async fn fill_limits(db: &Arc<Db>, env: &baserunna_util::env::Env, cfg: &NodeConfig) {
    let member_ids: Vec<String> = (0..cfg.max_members_per_org - 2).map(|i| format!("m{i}")).collect();
    let mut cmds = vec![MetaCmd::Migrate];
    for id in ["admin", "victim", "partner"].into_iter().chain(member_ids.iter().map(|s| s.as_str())) {
        cmds.push(signup_cmd(env, id));
    }
    cmds.push(MetaCmd::CreateOrg {
        id: "org-big".into(),
        name: long_name("big", 0, ORG_NAME_MAX),
        actor: "admin".into(),
        max_orgs: cfg.max_orgs_per_user,
    });
    let add = |org_id: &str, actor: &str, user_id: &str, role: Role| MetaCmd::AddMember {
        org_id: org_id.into(),
        actor: actor.into(),
        email: long_email(user_id),
        role,
        limits: member_limits(cfg),
    };
    cmds.push(add("org-big", "admin", "victim", Role::Owner));
    for id in &member_ids {
        cmds.push(add("org-big", "admin", id, Role::Reader));
    }
    for i in 0..cfg.max_orgs_per_user - 1 {
        let id = format!("org-p{i}");
        let name = long_name("p", i, ORG_NAME_MAX);
        cmds.push(MetaCmd::CreateOrg {
            id: id.clone(),
            name,
            actor: "partner".into(),
            max_orgs: cfg.max_orgs_per_user,
        });
        cmds.push(add(&id, "partner", "victim", Role::Owner));
    }
    cmds.extend(sessions("victim", PAGE_MAX - 1, LIVE_UNTIL_MS));
    cmds.extend(sessions("m0", PAGE_MAX + 1, LIVE_UNTIL_MS));
    for id in member_ids.iter().skip(1).take(3) {
        cmds.extend(sessions(id, PAGE_MAX / 2, 10));
    }
    cmds.extend((0..PAGE_MAX - 1).map(|i| MetaCmd::CreateApiKey { key: api_key("victim", i) }));
    let create_db = |i: usize| MetaCmd::CreateDb {
        id: format!("db-victim-{i}"),
        owner_id: "victim".into(),
        actor: "victim".into(),
        name: long_name("db", i, 63),
        max_dbs: cfg.max_dbs_per_owner,
    };
    cmds.push(create_db(cfg.max_dbs_per_owner));
    cmds.push(MetaCmd::DeleteDb { id: format!("db-victim-{}", cfg.max_dbs_per_owner) });
    cmds.extend((0..cfg.max_dbs_per_owner).map(create_db));
    cmds.extend(
        ["node-a", "node-b", "node-c"].map(|id| MetaCmd::Heartbeat { node: node(id), dead_after_ms: 3_600_000 }),
    );
    apply_all(db, cmds).await;
    let mut t = begin(db).await;
    check_integrity(&mut t).await.unwrap();
}

/// Returns one command of each kind, each aimed at the largest state that `fill_limits` creates.
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
            email: long_email("googler"),
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
        MetaCmd::VerifyEmail { user_id: "victim".into(), email: long_email("victim") },
        MetaCmd::SetDisabled { actor: "admin".into(), user_id: "victim".into(), disabled: true },
        sessions("victim", PAGE_MAX + 1, LIVE_UNTIL_MS).pop().unwrap(),
        MetaCmd::DeleteSession { token_hash: victim_session.clone() },
        MetaCmd::RevokeSession { user_id: "victim".into(), id: format!("id-{victim_session}") },
        MetaCmd::RevokeSessions { user_id: "m0".into(), except: Some(token_hash("m0", 0)), limit: META_BATCH },
        MetaCmd::CreateApiKey { key: api_key("victim", PAGE_MAX) },
        MetaCmd::DeleteApiKey { id: api_key("victim", 0).id, user_id: "victim".into() },
        MetaCmd::DeleteApiKeys { user_id: "victim".into(), limit: META_BATCH },
        MetaCmd::CreateDb {
            id: "db-shared".into(),
            owner_id: "org-p0".into(),
            actor: "victim".into(),
            name: long_name("shared", 0, 63),
            max_dbs: cfg.max_dbs_per_owner,
        },
        MetaCmd::DeleteDb { id: "db-victim-0".into() },
        MetaCmd::DeleteUser { user_id: "victim".into() },
        MetaCmd::DeleteUser { user_id: "m0".into() },
        MetaCmd::PurgePass { id: format!("db-victim-{}", cfg.max_dbs_per_owner), passes: 0 },
        MetaCmd::ExpireSessions { before_ms: NOW_MS, limit: META_BATCH },
        MetaCmd::Heartbeat { node: node("node-d"), dead_after_ms: 3_600_000 },
        MetaCmd::Leave { id: "node-a".into() },
        MetaCmd::CreateOrg {
            id: "org-new".into(),
            name: long_name("new", 0, ORG_NAME_MAX),
            actor: "admin".into(),
            max_orgs: cfg.max_orgs_per_user,
        },
        MetaCmd::DeleteOrg { org_id: "org-big".into(), actor: "admin".into() },
        MetaCmd::AddMember {
            org_id: "org-p0".into(),
            actor: "partner".into(),
            email: long_email("m1"),
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

#[test]
fn every_metadata_command_has_a_fixed_cost() {
    rt().block_on(async {
        let (eng, _store, env) = engine(5, EngineConfig::small_for_tests());
        let db = eng.db(SYS_DB);
        db.open(true).await.unwrap();
        let cfg = NodeConfig::default();
        fill_limits(&db, &env, &cfg).await;
        let mut measured = BTreeSet::new();
        let mut report = String::new();
        let mut failures = Vec::new();
        let victim = get_user(&mut begin(&db).await, "victim").await.unwrap().unwrap();
        for cmd in worst_case_commands(&env, &cfg, victim.password_hash) {
            let (answer, cost) = measure(&db, &cmd).await;
            let rows = row_limit(&cmd, &cfg);
            let (entries_max, bytes_max) = (ENTRIES_PER_ROW * rows, BYTES_PER_ROW * rows);
            report.push_str(&format!("{:<16} {cost:?} for {rows} rows\n", cmd.name()));
            if cost.entries_read.max(cost.entries_written) > entries_max
                || cost.bytes_read.max(cost.bytes_written) > bytes_max
            {
                failures.push(format!("{}: {cost:?} exceeds {entries_max} entries or {bytes_max} bytes", cmd.name()));
            }
            if let MetaCmd::DeleteUser { user_id } = &cmd {
                let deletion: UserDeletion = serde_json::from_value(answer).unwrap();
                let full = user_id == "victim";
                assert_eq!(matches!(deletion, UserDeletion::Deleted { .. }), full, "{user_id}: {deletion:?}");
            }
            measured.insert(cmd.name());
        }
        eprintln!("{report}");
        assert_eq!(measured, BTreeSet::from(COMMAND_NAMES), "every metadata command is measured");
        assert!(failures.is_empty(), "{failures:#?}");
    });
}
