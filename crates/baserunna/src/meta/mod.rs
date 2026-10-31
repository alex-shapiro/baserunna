//! Service metadata stored in the system database (`sys`). See SPEC §8.
//!
//! Writes are serializable [`MetaCmd`]s executed by the system database's
//! lease holder, checking user permissions within the same transaction
//! (except [`MetaCmd::DeleteDb`], which the caller authorizes before marking
//! storage deleted). Reads are snapshot reads that any node can serve.

mod org;
#[cfg(test)]
mod tests;

pub use org::{
    Access, Member, MemberInfo, MemberLimits, ORG_NAME_MAX, Org, Role, get_org, member_role, members_page,
    memberships_page, org_by_name, valid_org_name,
};

use crate::engine::Txn;
use crate::error::{Error, Result};
use crate::table::{self, BlobResolver, CreateTable, InsertMode, Query};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};
use std::collections::BTreeMap;

pub const SYS_DB: &str = "sys";
pub const SCHEMA_VERSION: i64 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SignupPolicy {
    Open,
    #[default]
    FirstUserOnly,
    Closed,
}

impl SignupPolicy {
    pub fn parse(s: &str) -> Option<SignupPolicy> {
        match s {
            "open" => Some(SignupPolicy::Open),
            "first_user_only" => Some(SignupPolicy::FirstUserOnly),
            "closed" => Some(SignupPolicy::Closed),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct User {
    pub id: String,
    pub email: String,
    pub password_hash: Option<String>,
    pub google_sub: Option<String>,
    pub is_admin: bool,
    pub disabled: bool,
    pub email_verified: bool,
    pub created_at: i64,
}

impl User {
    pub fn public(&self) -> baserunna_api::User {
        baserunna_api::User {
            id: self.id.clone(),
            email: self.email.clone(),
            email_verified: self.email_verified,
            is_admin: self.is_admin,
            disabled: self.disabled,
            has_password: self.password_hash.is_some(),
            google_linked: self.google_sub.is_some(),
            created_at: self.created_at,
        }
    }

    pub fn public_json(&self) -> Json {
        serde_json::to_value(self.public()).expect("users serialize")
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Session {
    /// SHA-256 hash of the session token, used as the primary key.
    pub token_hash: String,
    /// Public identifier used to list and revoke the session.
    pub id: String,
    pub user_id: String,
    pub created_at: i64,
    pub expires_at: i64,
    pub client: Option<String>,
    pub method: SignInMethod,
    /// [`crate::auth::fingerprint`] of the user's password hash when the
    /// session was created. A session is valid only while this matches the
    /// user's current password hash, invalidating other sessions on password
    /// change.
    pub password_fp: String,
}

pub fn password_fp(user: &User) -> String {
    password_hash_fp(user.password_hash.as_deref())
}

fn password_hash_fp(hash: Option<&str>) -> String {
    crate::auth::fingerprint(hash.unwrap_or(""))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignInMethod {
    Signup,
    Password,
    Google,
    Reset,
}

impl Session {
    /// Returns whether the session is valid for `user` at `now_ms`.
    pub fn is_live(&self, user: &User, now_ms: i64) -> bool {
        self.user_id == user.id && self.expires_at >= now_ms && self.password_fp == password_fp(user)
    }

    pub fn public_json(&self, current: bool) -> Json {
        json!({
            "id": self.id,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "client": self.client,
            "method": self.method,
            "current": current,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiKey {
    pub id: String,
    pub user_id: String,
    pub name: String,
    pub secret_hash: String,
    pub db_id: Option<String>,
    pub read_only: bool,
    /// Per-node rate limit in requests per second, or `None` for the node default.
    pub rate_limit: Option<u32>,
    pub created_at: i64,
}

impl ApiKey {
    pub fn public_json(&self, db_ref: Option<&str>) -> Json {
        json!({
            "id": self.id,
            "name": self.name,
            "db": db_ref,
            "read_only": self.read_only,
            "rate_limit": self.rate_limit,
            "created_at": self.created_at,
        })
    }
}

/// Metadata record for a database. `owner_id` is either a user ID or an
/// organization ID; `name` is unique per owner.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DbRecord {
    pub id: String,
    pub owner_id: String,
    pub name: String,
    pub created_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeletedDb {
    pub id: String,
    pub owner_id: String,
    pub name: String,
    pub deleted_at: i64,
    pub purge_passes: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeRecord {
    pub id: String,
    pub addr: String,
    /// Monotonic start timestamp among processes at `addr`.
    pub started_at: i64,
    pub heartbeat_at: i64,
}

pub const ORG_ID_PREFIX: &str = "org-";

pub fn is_org_id(owner_id: &str) -> bool {
    owner_id.starts_with(ORG_ID_PREFIX)
}

pub fn valid_db_name(s: &str) -> bool {
    valid_slug(s, 63)
}

/// Returns whether `s` is 1..=`max` characters of `[a-z0-9_-]` starting with an alphanumeric character.
pub fn valid_slug(s: &str, max: usize) -> bool {
    !s.is_empty()
        && s.len() <= max
        && s.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
        && s.as_bytes()[0].is_ascii_alphanumeric()
}

/// Database identifier in a request: `name`, `org.name`, or `id:<db_id>`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DbRef {
    Personal(String),
    Org { org: String, name: String },
    Id(String),
}

impl DbRef {
    pub fn parse(s: &str) -> Option<DbRef> {
        if let Some(id) = s.strip_prefix("id:") {
            return (!id.is_empty() && id.len() <= 64).then(|| DbRef::Id(id.to_string()));
        }
        match s.split_once('.') {
            Some((org, name)) => (org::valid_org_name(org) && valid_db_name(name))
                .then(|| DbRef::Org { org: org.to_string(), name: name.to_string() }),
            None => valid_db_name(s).then(|| DbRef::Personal(s.to_string())),
        }
    }

    pub fn format(org: Option<&str>, name: &str) -> String {
        match org {
            Some(o) => format!("{o}.{name}"),
            None => name.to_string(),
        }
    }
}

fn tables() -> Vec<Json> {
    vec![
        json!({"name": "users", "primary_key": ["id"], "columns": [
            {"name": "id", "type": "string", "nullable": false},
            {"name": "email", "type": "string", "nullable": false},
            {"name": "password_hash", "type": "string"},
            {"name": "google_sub", "type": "string"},
            {"name": "is_admin", "type": "bool", "nullable": false, "default": false},
            {"name": "disabled", "type": "bool", "nullable": false, "default": false},
            {"name": "email_verified", "type": "bool", "nullable": false, "default": false},
            {"name": "created_at", "type": "int", "nullable": false}
        ], "indexes": [
            {"name": "by_email", "columns": ["email"], "unique": true},
            {"name": "by_google", "columns": ["google_sub"], "unique": true},
            {"name": "by_admin", "columns": ["is_admin"]}
        ]}),
        json!({"name": "sessions", "primary_key": ["token_hash"], "columns": [
            {"name": "token_hash", "type": "string", "nullable": false},
            {"name": "id", "type": "string", "nullable": false},
            {"name": "user_id", "type": "string", "nullable": false},
            {"name": "created_at", "type": "int", "nullable": false},
            {"name": "expires_at", "type": "int", "nullable": false},
            {"name": "client", "type": "string"},
            {"name": "method", "type": "string", "nullable": false},
            {"name": "password_fp", "type": "string", "nullable": false}
        ], "indexes": [
            {"name": "by_id", "columns": ["id"], "unique": true},
            {"name": "by_user", "columns": ["user_id"]},
            {"name": "by_expiry", "columns": ["expires_at"]}
        ]}),
        json!({"name": "api_keys", "primary_key": ["id"], "columns": [
            {"name": "id", "type": "string", "nullable": false},
            {"name": "user_id", "type": "string", "nullable": false},
            {"name": "name", "type": "string", "nullable": false},
            {"name": "secret_hash", "type": "string", "nullable": false},
            {"name": "db_id", "type": "string"},
            {"name": "read_only", "type": "bool", "nullable": false, "default": false},
            {"name": "rate_limit", "type": "int"},
            {"name": "created_at", "type": "int", "nullable": false}
        ], "indexes": [{"name": "by_user", "columns": ["user_id"]}]}),
        json!({"name": "orgs", "primary_key": ["id"], "columns": [
            {"name": "id", "type": "string", "nullable": false},
            {"name": "name", "type": "string", "nullable": false},
            {"name": "created_at", "type": "int", "nullable": false}
        ], "indexes": [{"name": "by_name", "columns": ["name"], "unique": true}]}),
        json!({"name": "org_members", "primary_key": ["org_id", "user_id"], "columns": [
            {"name": "org_id", "type": "string", "nullable": false},
            {"name": "user_id", "type": "string", "nullable": false},
            {"name": "role", "type": "string", "nullable": false},
            {"name": "added_at", "type": "int", "nullable": false}
        ], "indexes": [{"name": "by_user", "columns": ["user_id"]}]}),
        json!({"name": "databases", "primary_key": ["id"], "columns": [
            {"name": "id", "type": "string", "nullable": false},
            {"name": "owner_id", "type": "string", "nullable": false},
            {"name": "name", "type": "string", "nullable": false},
            {"name": "created_at", "type": "int", "nullable": false}
        ], "indexes": [{"name": "by_owner_name", "columns": ["owner_id", "name"], "unique": true}]}),
        json!({"name": "counts", "primary_key": ["kind", "key"], "columns": [
            {"name": "kind", "type": "string", "nullable": false},
            {"name": "key", "type": "string", "nullable": false},
            {"name": "count", "type": "int", "nullable": false}
        ]}),
        json!({"name": "deleted_databases", "primary_key": ["id"], "columns": [
            {"name": "id", "type": "string", "nullable": false},
            {"name": "owner_id", "type": "string", "nullable": false},
            {"name": "name", "type": "string", "nullable": false},
            {"name": "deleted_at", "type": "int", "nullable": false},
            {"name": "purge_passes", "type": "int", "nullable": false, "default": 0}
        ]}),
        json!({"name": "nodes", "primary_key": ["id"], "columns": [
            {"name": "id", "type": "string", "nullable": false},
            {"name": "addr", "type": "string", "nullable": false},
            {"name": "started_at", "type": "int", "nullable": false},
            {"name": "heartbeat_at", "type": "int", "nullable": false}
        ]}),
        json!({"name": "config", "primary_key": ["key"], "columns": [
            {"name": "key", "type": "string", "nullable": false},
            {"name": "value", "type": "string", "nullable": false}
        ]}),
    ]
}

/// Metadata write commands executed on the system database's lease holder.
/// Variants return `()` unless noted otherwise.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum MetaCmd {
    /// Returns `true` if the schema was newly created.
    Migrate,
    /// Returns the existing value for `key`, or inserts and returns `value`.
    EnsureConfig {
        key: String,
        value: String,
    },
    /// Returns [`SignedUp`].
    Signup {
        id: String,
        email: String,
        password_hash: Option<String>,
        policy: SignupPolicy,
        /// Whether this is a configured bootstrap admin, bypassing `policy`.
        bootstrap_admin: bool,
    },
    /// Returns [`SignedUp`].
    GoogleLogin {
        new_id: String,
        sub: String,
        email: String,
        email_verified: bool,
        policy: SignupPolicy,
    },
    /// Updates a user's password if the current hash matches `expected`,
    /// invalidating all sessions except `keep_session`.
    SetPassword {
        user_id: String,
        password_hash: String,
        expected: Option<String>,
        keep_session: Option<String>,
        verify_email: bool,
    },
    /// Marks `email` verified if it still matches the user's current address.
    VerifyEmail {
        user_id: String,
        email: String,
    },
    SetDisabled {
        actor: String,
        user_id: String,
        disabled: bool,
    },
    CreateSession {
        token_hash: String,
        id: String,
        user_id: String,
        expires_at: i64,
        client: Option<String>,
        method: SignInMethod,
    },
    DeleteSession {
        token_hash: String,
    },
    /// Revokes a session by its public ID and returns its token hash.
    RevokeSession {
        user_id: String,
        id: String,
    },
    /// Revokes up to `limit` sessions for `user_id`, excluding `except`. Returns [`Batch`].
    RevokeSessions {
        user_id: String,
        except: Option<String>,
        limit: usize,
    },
    CreateApiKey {
        key: ApiKey,
    },
    DeleteApiKey {
        id: String,
        user_id: String,
    },
    /// Deletes up to `limit` API keys for `user_id`. Returns [`Batch`].
    DeleteApiKeys {
        user_id: String,
        limit: usize,
    },
    /// Creates database `name` under `owner_id` and returns its [`DbRecord`].
    CreateDb {
        id: String,
        owner_id: String,
        actor: String,
        name: String,
        max_dbs: usize,
    },
    /// Deletes a database's metadata after the caller has authorized the
    /// deletion and marked its storage deleted.
    DeleteDb {
        id: String,
    },
    /// Deletes a user account along with its sessions, API keys, memberships,
    /// and personal databases. Rejects deletion of the last enabled
    /// administrator or the sole owner of an organization. Returns [`UserDeletion`].
    DeleteUser {
        user_id: String,
    },
    /// Records a completed storage purge pass if `purge_passes` still equals `passes`.
    PurgePass {
        id: String,
        passes: i64,
    },
    /// Deletes up to `limit` sessions that expired before `before_ms`. Returns [`Batch`].
    ExpireSessions {
        before_ms: i64,
        limit: usize,
    },
    Heartbeat {
        node: NodeRecord,
        dead_after_ms: i64,
    },
    Leave {
        id: String,
    },
    /// Returns [`Org`].
    CreateOrg {
        id: String,
        name: String,
        actor: String,
        max_orgs: usize,
    },
    /// Deletes an organization that has no remaining databases.
    DeleteOrg {
        org_id: String,
        actor: String,
    },
    /// Returns [`MemberInfo`].
    AddMember {
        org_id: String,
        actor: String,
        email: String,
        role: Role,
        limits: MemberLimits,
    },
    /// Returns [`MemberInfo`].
    SetRole {
        org_id: String,
        actor: String,
        user_id: String,
        role: Role,
    },
    RemoveMember {
        org_id: String,
        actor: String,
        user_id: String,
    },
}

impl MetaCmd {
    pub fn name(&self) -> &'static str {
        match self {
            MetaCmd::Migrate => "migrate",
            MetaCmd::EnsureConfig { .. } => "ensure_config",
            MetaCmd::Signup { .. } => "signup",
            MetaCmd::GoogleLogin { .. } => "google_login",
            MetaCmd::SetPassword { .. } => "set_password",
            MetaCmd::VerifyEmail { .. } => "verify_email",
            MetaCmd::SetDisabled { .. } => "set_disabled",
            MetaCmd::CreateSession { .. } => "create_session",
            MetaCmd::DeleteSession { .. } => "delete_session",
            MetaCmd::RevokeSession { .. } => "revoke_session",
            MetaCmd::RevokeSessions { .. } => "revoke_sessions",
            MetaCmd::CreateApiKey { .. } => "create_api_key",
            MetaCmd::DeleteApiKey { .. } => "delete_api_key",
            MetaCmd::DeleteApiKeys { .. } => "delete_api_keys",
            MetaCmd::CreateDb { .. } => "create_db",
            MetaCmd::DeleteDb { .. } => "delete_db",
            MetaCmd::DeleteUser { .. } => "delete_user",
            MetaCmd::PurgePass { .. } => "purge_pass",
            MetaCmd::ExpireSessions { .. } => "expire_sessions",
            MetaCmd::Heartbeat { .. } => "heartbeat",
            MetaCmd::Leave { .. } => "leave",
            MetaCmd::CreateOrg { .. } => "create_org",
            MetaCmd::DeleteOrg { .. } => "delete_org",
            MetaCmd::AddMember { .. } => "add_member",
            MetaCmd::SetRole { .. } => "set_role",
            MetaCmd::RemoveMember { .. } => "remove_member",
        }
    }
}

fn from_row<T: DeserializeOwned>(v: Json) -> Result<T> {
    serde_json::from_value(v).map_err(|e| Error::internal(format!("bad metadata row: {e}")))
}

fn to_row<T: Serialize>(v: &T) -> Json {
    serde_json::to_value(v).unwrap_or(Json::Null)
}

async fn query_all<T: DeserializeOwned>(txn: &mut Txn, t: &str, mut q: Query) -> Result<Vec<T>> {
    let mut out = Vec::new();
    q.limit = Some(PAGE_MAX);
    loop {
        let r = table::query(txn, t, &q).await?;
        for row in r.rows {
            out.push(from_row(row)?);
        }
        match r.next_cursor {
            Some(c) => q.cursor = Some(c),
            None => return Ok(out),
        }
    }
}

fn q_eq(index: Option<&str>, eq: Vec<Json>) -> Query {
    Query { index: index.map(|i| i.to_string()), eq, ..Default::default() }
}

/// Result of [`MetaCmd::Signup`] and [`MetaCmd::GoogleLogin`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SignedUp {
    pub user: User,
    pub created: bool,
}

/// Result of a batched deletion command.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Batch {
    pub deleted: usize,
    pub more: bool,
}

/// Result of [`MetaCmd::DeleteUser`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum UserDeletion {
    /// The user was deleted; `dbs` lists personal databases whose storage must
    /// be marked deleted.
    Deleted { dbs: Vec<String> },
    /// The user has too many sessions or API keys to delete in a single
    /// transaction; the caller must delete them in batches first.
    TooManyCredentials,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

pub const PAGE_DEFAULT: usize = 100;
pub const PAGE_MAX: usize = 1000;

async fn page<T: DeserializeOwned>(
    txn: &mut Txn,
    t: &str,
    mut q: Query,
    limit: usize,
    cursor: Option<String>,
) -> Result<Page<T>> {
    if !(1..=PAGE_MAX).contains(&limit) {
        return Err(Error::invalid(format!("limit must be in 1..={PAGE_MAX}")));
    }
    q.limit = Some(limit);
    q.cursor = cursor;
    let r = table::query(txn, t, &q).await?;
    let items = r.rows.into_iter().map(from_row).collect::<Result<Vec<T>>>()?;
    Ok(Page { items, next_cursor: r.next_cursor })
}

pub async fn get_user(txn: &mut Txn, id: &str) -> Result<Option<User>> {
    table::get(txn, "users", &json!(id)).await?.map(from_row).transpose()
}

async fn first<T: DeserializeOwned>(txn: &mut Txn, t: &str, index: &str, eq: Vec<Json>) -> Result<Option<T>> {
    let mut q = q_eq(Some(index), eq);
    q.limit = Some(1);
    table::query(txn, t, &q).await?.rows.into_iter().next().map(from_row).transpose()
}

pub async fn user_by_email(txn: &mut Txn, email: &str) -> Result<Option<User>> {
    first(txn, "users", "by_email", vec![json!(email)]).await
}

pub async fn user_by_google(txn: &mut Txn, sub: &str) -> Result<Option<User>> {
    first(txn, "users", "by_google", vec![json!(sub)]).await
}

async fn list_users(txn: &mut Txn) -> Result<Vec<User>> {
    query_all(txn, "users", Query::default()).await
}

pub async fn users_page(txn: &mut Txn, limit: usize, cursor: Option<String>) -> Result<Page<User>> {
    page(txn, "users", Query::default(), limit, cursor).await
}

async fn other_enabled_admin(txn: &mut Txn, user_id: &str) -> Result<bool> {
    let admins: Vec<User> = query_all(txn, "users", q_eq(Some("by_admin"), vec![json!(true)])).await?;
    Ok(admins.iter().any(|u| u.id != user_id && !u.disabled))
}

fn expired_sessions(before_ms: i64) -> Query {
    let range = crate::table::RangeSpec { lt: Some(json!(before_ms)), ..Default::default() };
    Query { index: Some("by_expiry".into()), range: Some(range), ..Default::default() }
}

pub async fn any_session_expired(txn: &mut Txn, before_ms: i64) -> Result<bool> {
    Ok(first_rows::<Session>(txn, "sessions", expired_sessions(before_ms), 1).await?.0.len() == 1)
}

pub async fn get_session(txn: &mut Txn, token_hash: &str) -> Result<Option<Session>> {
    table::get(txn, "sessions", &json!(token_hash)).await?.map(from_row).transpose()
}

pub async fn sessions_page(
    txn: &mut Txn,
    user_id: &str,
    limit: usize,
    cursor: Option<String>,
) -> Result<Page<Session>> {
    page(txn, "sessions", q_eq(Some("by_user"), vec![json!(user_id)]), limit, cursor).await
}

pub async fn get_api_key(txn: &mut Txn, id: &str) -> Result<Option<ApiKey>> {
    table::get(txn, "api_keys", &json!(id)).await?.map(from_row).transpose()
}

pub async fn api_keys_page(txn: &mut Txn, user_id: &str, limit: usize, cursor: Option<String>) -> Result<Page<ApiKey>> {
    page(txn, "api_keys", q_eq(Some("by_user"), vec![json!(user_id)]), limit, cursor).await
}

pub async fn get_db(txn: &mut Txn, id: &str) -> Result<Option<DbRecord>> {
    table::get(txn, "databases", &json!(id)).await?.map(from_row).transpose()
}

pub async fn resolve_db(txn: &mut Txn, owner_id: &str, name: &str) -> Result<Option<DbRecord>> {
    first(txn, "databases", "by_owner_name", vec![json!(owner_id), json!(name)]).await
}

async fn list_dbs(txn: &mut Txn, owner_id: &str) -> Result<Vec<DbRecord>> {
    query_all(txn, "databases", q_eq(Some("by_owner_name"), vec![json!(owner_id)])).await
}

pub async fn dbs_page(txn: &mut Txn, owner_id: &str, limit: usize, cursor: Option<String>) -> Result<Page<DbRecord>> {
    page(txn, "databases", q_eq(Some("by_owner_name"), vec![json!(owner_id)]), limit, cursor).await
}

pub async fn list_all_dbs(txn: &mut Txn) -> Result<Vec<DbRecord>> {
    query_all(txn, "databases", Query::default()).await
}

pub async fn list_deleted_dbs(txn: &mut Txn) -> Result<Vec<DeletedDb>> {
    query_all(txn, "deleted_databases", Query::default()).await
}

pub async fn list_nodes(txn: &mut Txn) -> Result<Vec<NodeRecord>> {
    query_all(txn, "nodes", Query::default()).await
}

pub async fn get_config(txn: &mut Txn, key: &str) -> Result<Option<String>> {
    Ok(table::get(txn, "config", &json!(key)).await?.and_then(|r| r["value"].as_str().map(|s| s.to_string())))
}

pub async fn is_migrated(txn: &mut Txn) -> Result<bool> {
    match table::get_schema(txn, "config").await {
        Ok(_) => Ok(get_config(txn, "schema_version").await?.is_some()),
        Err(Error::NotFound(_)) => Ok(false),
        Err(e) => Err(e),
    }
}

/// Materialized row counts used for quota and invariant checks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Count {
    Dbs,
    Members,
    Owners,
    Orgs,
}

impl Count {
    fn name(self) -> &'static str {
        match self {
            Count::Dbs => "dbs",
            Count::Members => "members",
            Count::Owners => "owners",
            Count::Orgs => "orgs",
        }
    }
}

pub(crate) async fn count(txn: &mut Txn, c: Count, key: &str) -> Result<i64> {
    Ok(table::get(txn, "counts", &json!([c.name(), key])).await?.and_then(|r| r["count"].as_i64()).unwrap_or(0))
}

/// Adds `delta` to a counter, deleting the row when the count reaches zero.
pub(crate) async fn add_count(txn: &mut Txn, blobs: &BlobResolver<'_>, c: Count, key: &str, delta: i64) -> Result<i64> {
    let n = count(txn, c, key).await? + delta;
    if n <= 0 {
        table::delete(txn, "counts", &json!([c.name(), key])).await?;
    } else {
        let row = json!({"kind": c.name(), "key": key, "count": n});
        table::insert(txn, blobs, "counts", &row, InsertMode::Upsert).await?;
    }
    Ok(n)
}

/// Verifies referential integrity and materialized counters across all metadata tables.
pub async fn check_integrity(txn: &mut Txn) -> Result<()> {
    let bad = |m: String| Err(Error::internal(m));
    let users: BTreeMap<String, User> = list_users(txn).await?.into_iter().map(|u| (u.id.clone(), u)).collect();
    let orgs: BTreeMap<String, Org> =
        query_all::<Org>(txn, "orgs", Query::default()).await?.into_iter().map(|o| (o.id.clone(), o)).collect();
    let members: Vec<Member> = query_all(txn, "org_members", Query::default()).await?;
    let mut want: BTreeMap<(String, String), i64> = BTreeMap::new();
    let mut bump = |c: Count, k: &str| *want.entry((c.name().to_string(), k.to_string())).or_default() += 1;
    for d in list_all_dbs(txn).await? {
        if !users.contains_key(&d.owner_id) && !orgs.contains_key(&d.owner_id) {
            return bad(format!("database {} belongs to missing owner {}", d.id, d.owner_id));
        }
        bump(Count::Dbs, &d.owner_id);
    }
    for m in &members {
        if !users.contains_key(&m.user_id) || !orgs.contains_key(&m.org_id) {
            return bad(format!("membership {m:?} names a missing user or organization"));
        }
        bump(Count::Members, &m.org_id);
        bump(Count::Orgs, &m.user_id);
        if m.role == Role::Owner {
            bump(Count::Owners, &m.org_id);
        }
    }
    for o in orgs.keys() {
        if !want.contains_key(&(Count::Owners.name().to_string(), o.clone())) {
            return bad(format!("organization {o} has no owner"));
        }
    }
    let mut got = BTreeMap::new();
    for r in query_all::<Json>(txn, "counts", Query::default()).await? {
        let k = (r["kind"].as_str().unwrap_or_default().to_string(), r["key"].as_str().unwrap_or_default().to_string());
        got.insert(k, r["count"].as_i64().unwrap_or(-1));
    }
    if got != want {
        return bad(format!("counters {got:?} do not match the metadata {want:?}"));
    }
    for s in query_all::<Session>(txn, "sessions", Query::default()).await? {
        if !users.contains_key(&s.user_id) {
            return bad(format!("session {} belongs to missing user {}", s.id, s.user_id));
        }
    }
    for k in query_all::<ApiKey>(txn, "api_keys", Query::default()).await? {
        if !users.contains_key(&k.user_id) {
            return bad(format!("API key {} belongs to missing user {}", k.id, k.user_id));
        }
    }
    Ok(())
}

pub(crate) struct Write<'a, 'b> {
    pub(crate) txn: &'a mut Txn,
    pub(crate) blobs: &'a BlobResolver<'b>,
    pub(crate) now_ms: i64,
}

async fn any_user(txn: &mut Txn) -> Result<bool> {
    let r = table::query(txn, "users", &Query { limit: Some(1), ..Default::default() }).await?;
    Ok(!r.rows.is_empty())
}

fn signup_allowed(policy: SignupPolicy, first: bool) -> Result<()> {
    match policy {
        SignupPolicy::Open => Ok(()),
        SignupPolicy::FirstUserOnly if first => Ok(()),
        _ => Err(Error::PermissionDenied("sign-up is disabled on this server".into())),
    }
}

async fn active_user(txn: &mut Txn, id: &str) -> Result<User> {
    match get_user(txn, id).await? {
        Some(u) if u.disabled => Err(Error::PermissionDenied("this account is disabled".into())),
        Some(u) => Ok(u),
        None => Err(Error::not_found("user not found")),
    }
}

async fn first_rows<T: DeserializeOwned>(txn: &mut Txn, t: &str, q: Query, limit: usize) -> Result<(Vec<T>, bool)> {
    let p = page(txn, t, q, limit.clamp(1, PAGE_MAX), None).await?;
    Ok((p.items, p.next_cursor.is_some()))
}

fn by_user(user_id: &str) -> Query {
    q_eq(Some("by_user"), vec![json!(user_id)])
}

async fn delete_db(w: &mut Write<'_, '_>, d: DbRecord) -> Result<()> {
    table::delete(w.txn, "databases", &json!(d.id)).await?;
    add_count(w.txn, w.blobs, Count::Dbs, &d.owner_id, -1).await?;
    let del = DeletedDb { id: d.id, owner_id: d.owner_id, name: d.name, deleted_at: w.now_ms, purge_passes: 0 };
    table::insert(w.txn, w.blobs, "deleted_databases", &to_row(&del), InsertMode::Upsert).await
}

async fn migrate(w: &mut Write<'_, '_>) -> Result<bool> {
    if is_migrated(w.txn).await? {
        return Ok(false);
    }
    for t in tables() {
        let mut def: CreateTable = serde_json::from_value(t).map_err(|e| Error::internal(e.to_string()))?;
        def.if_not_exists = true;
        table::create_table(w.txn, &def).await?;
    }
    let row = json!({"key": "schema_version", "value": SCHEMA_VERSION.to_string()});
    table::insert(w.txn, w.blobs, "config", &row, InsertMode::Upsert).await?;
    Ok(true)
}

async fn ensure_config(w: &mut Write<'_, '_>, key: &str, value: &str) -> Result<String> {
    if let Some(v) = get_config(w.txn, key).await? {
        return Ok(v);
    }
    table::insert(w.txn, w.blobs, "config", &json!({"key": key, "value": value}), InsertMode::Insert).await?;
    Ok(value.to_string())
}

async fn signup(
    w: &mut Write<'_, '_>,
    id: &str,
    email: &str,
    password_hash: &Option<String>,
    policy: SignupPolicy,
    bootstrap_admin: bool,
) -> Result<SignedUp> {
    if let Some(user) = user_by_email(w.txn, email).await? {
        if bootstrap_admin {
            return Ok(SignedUp { user, created: false });
        }
        return Err(Error::AlreadyExists("an account with this email already exists".into()));
    }
    let first = !any_user(w.txn).await?;
    if !bootstrap_admin {
        signup_allowed(policy, first)?;
    }
    let user = User {
        id: id.into(),
        email: email.into(),
        password_hash: password_hash.clone(),
        google_sub: None,
        is_admin: first || bootstrap_admin,
        disabled: false,
        email_verified: bootstrap_admin,
        created_at: w.now_ms,
    };
    table::insert(w.txn, w.blobs, "users", &to_row(&user), InsertMode::Insert).await?;
    Ok(SignedUp { user, created: true })
}

async fn google_login(
    w: &mut Write<'_, '_>,
    new_id: &str,
    sub: &str,
    email: &str,
    email_verified: bool,
    policy: SignupPolicy,
) -> Result<SignedUp> {
    if let Some(user) = user_by_google(w.txn, sub).await? {
        return Ok(SignedUp { user, created: false });
    }
    if !email_verified {
        return Err(Error::PermissionDenied("Google account email is not verified".into()));
    }
    if let Some(mut user) = user_by_email(w.txn, email).await? {
        // Require email verification before linking a password-bearing account
        // so an attacker cannot pre-register a victim's email with a known password.
        if user.password_hash.is_some() && !user.email_verified {
            return Err(Error::PermissionDenied(
                "an account with this email exists: sign in with its password and verify the email address \
                 before signing in with Google"
                    .into(),
            ));
        }
        let set = json!({"google_sub": sub, "email_verified": true});
        table::update(w.txn, w.blobs, "users", &json!(user.id), &set).await?;
        (user.google_sub, user.email_verified) = (Some(sub.into()), true);
        return Ok(SignedUp { user, created: false });
    }
    let first = !any_user(w.txn).await?;
    signup_allowed(policy, first)?;
    let user = User {
        id: new_id.into(),
        email: email.into(),
        password_hash: None,
        google_sub: Some(sub.into()),
        is_admin: first,
        disabled: false,
        email_verified: true,
        created_at: w.now_ms,
    };
    table::insert(w.txn, w.blobs, "users", &to_row(&user), InsertMode::Insert).await?;
    Ok(SignedUp { user, created: true })
}

async fn set_password(
    w: &mut Write<'_, '_>,
    user_id: &str,
    password_hash: &str,
    expected: &Option<String>,
    keep_session: &Option<String>,
    verify_email: bool,
) -> Result<()> {
    let u = get_user(w.txn, user_id).await?.ok_or_else(|| Error::not_found("user not found"))?;
    if u.password_hash != *expected {
        return Err(Error::PreconditionFailed {
            message: "the password changed concurrently; try again".into(),
            op_index: None,
        });
    }
    let mut set = json!({"password_hash": password_hash});
    if verify_email {
        set["email_verified"] = json!(true);
    }
    table::update(w.txn, w.blobs, "users", &json!(user_id), &set).await?;
    if let Some(h) = keep_session
        && get_session(w.txn, h).await?.is_some_and(|s| s.user_id == user_id)
    {
        let fp = password_hash_fp(Some(password_hash));
        table::update(w.txn, w.blobs, "sessions", &json!(h), &json!({"password_fp": fp})).await?;
    }
    Ok(())
}

async fn verify_email(w: &mut Write<'_, '_>, user_id: &str, email: &str) -> Result<()> {
    match get_user(w.txn, user_id).await? {
        Some(u) if u.email == email => {
            if !u.email_verified {
                table::update(w.txn, w.blobs, "users", &json!(user_id), &json!({"email_verified": true})).await?;
            }
            Ok(())
        }
        _ => Err(Error::Unauthenticated("this verification link is no longer valid".into())),
    }
}

async fn set_disabled(w: &mut Write<'_, '_>, actor: &str, user_id: &str, disabled: bool) -> Result<()> {
    if !active_user(w.txn, actor).await?.is_admin {
        return Err(Error::PermissionDenied("administrator access required".into()));
    }
    if actor == user_id && disabled {
        return Err(Error::invalid("you cannot disable your own account"));
    }
    if !table::update(w.txn, w.blobs, "users", &json!(user_id), &json!({"disabled": disabled})).await? {
        return Err(Error::not_found("user not found"));
    }
    Ok(())
}

async fn revoke_session(txn: &mut Txn, user_id: &str, id: &str) -> Result<String> {
    match first::<Session>(txn, "sessions", "by_id", vec![json!(id)]).await? {
        Some(s) if s.user_id == user_id => {
            table::delete(txn, "sessions", &json!(s.token_hash)).await?;
            Ok(s.token_hash)
        }
        _ => Err(Error::not_found("session not found")),
    }
}

async fn revoke_sessions(txn: &mut Txn, user_id: &str, except: &Option<String>, limit: usize) -> Result<Batch> {
    let (sessions, more) = first_rows::<Session>(txn, "sessions", by_user(user_id), limit + 1).await?;
    let doomed: Vec<Session> = sessions.into_iter().filter(|s| Some(&s.token_hash) != except.as_ref()).collect();
    for s in doomed.iter().take(limit) {
        table::delete(txn, "sessions", &json!(s.token_hash)).await?;
    }
    Ok(Batch { deleted: doomed.len().min(limit), more: more || doomed.len() > limit })
}

async fn delete_api_keys(txn: &mut Txn, user_id: &str, limit: usize) -> Result<Batch> {
    let (keys, more) = first_rows::<ApiKey>(txn, "api_keys", by_user(user_id), limit).await?;
    for k in &keys {
        table::delete(txn, "api_keys", &json!(k.id)).await?;
    }
    Ok(Batch { deleted: keys.len(), more })
}

async fn create_db(
    w: &mut Write<'_, '_>,
    id: &str,
    owner_id: &str,
    actor: &str,
    name: &str,
    max_dbs: usize,
) -> Result<DbRecord> {
    if !valid_db_name(name) {
        return Err(Error::invalid("database names are 1-63 chars of [a-z0-9_-], starting alphanumeric"));
    }
    let me = active_user(w.txn, actor).await?;
    if is_org_id(owner_id) {
        org::require_role(w.txn, owner_id, &me, Role::Admin).await?;
    } else if owner_id != actor {
        return Err(Error::PermissionDenied("databases are created for yourself or an organization".into()));
    }
    let d = DbRecord { id: id.into(), owner_id: owner_id.into(), name: name.into(), created_at: w.now_ms };
    table::insert(w.txn, w.blobs, "databases", &to_row(&d), InsertMode::Insert).await.map_err(|e| match e {
        Error::AlreadyExists(_) => Error::AlreadyExists(format!("database {name:?} already exists")),
        other => other,
    })?;
    if add_count(w.txn, w.blobs, Count::Dbs, owner_id, 1).await? > max_dbs as i64 {
        return Err(Error::PermissionDenied(format!("database limit of {max_dbs} reached")));
    }
    Ok(d)
}

async fn delete_user(w: &mut Write<'_, '_>, user_id: &str) -> Result<UserDeletion> {
    let u = get_user(w.txn, user_id).await?.ok_or_else(|| Error::not_found("user not found"))?;
    if u.is_admin && !u.disabled && !other_enabled_admin(w.txn, user_id).await? {
        return Err(Error::PermissionDenied("the last administrator cannot delete their account".into()));
    }
    let memberships = org::memberships(w.txn, user_id).await?;
    for m in memberships.iter().filter(|m| m.role == Role::Owner) {
        if count(w.txn, Count::Owners, &m.org_id).await? <= 1 {
            let name = org::get_org(w.txn, &m.org_id).await?.map(|o| o.name).unwrap_or_default();
            return Err(Error::PermissionDenied(format!(
                "you are the only owner of organization {name:?}: add another owner or delete it first"
            )));
        }
    }
    let (sessions, more_sessions) = first_rows::<Session>(w.txn, "sessions", by_user(user_id), PAGE_MAX).await?;
    let (keys, more_keys) = first_rows::<ApiKey>(w.txn, "api_keys", by_user(user_id), PAGE_MAX).await?;
    if more_sessions || more_keys {
        return Ok(UserDeletion::TooManyCredentials);
    }
    for s in sessions {
        table::delete(w.txn, "sessions", &json!(s.token_hash)).await?;
    }
    for k in keys {
        table::delete(w.txn, "api_keys", &json!(k.id)).await?;
    }
    for m in memberships {
        org::change_membership(w, &m.org_id, user_id, Some(m.role), None).await?;
    }
    let mut dbs = Vec::new();
    for d in list_dbs(w.txn, user_id).await? {
        dbs.push(d.id.clone());
        delete_db(w, d).await?;
    }
    table::delete(w.txn, "users", &json!(user_id)).await?;
    Ok(UserDeletion::Deleted { dbs })
}

async fn purge_pass(w: &mut Write<'_, '_>, id: &str, passes: i64) -> Result<()> {
    let Some(row) = table::get(w.txn, "deleted_databases", &json!(id)).await? else { return Ok(()) };
    let d: DeletedDb = from_row(row)?;
    if d.purge_passes != passes {
        return Ok(());
    }
    if d.purge_passes >= 1 {
        table::delete(w.txn, "deleted_databases", &json!(id)).await?;
        return Ok(());
    }
    let set = json!({"purge_passes": d.purge_passes + 1, "deleted_at": w.now_ms});
    table::update(w.txn, w.blobs, "deleted_databases", &json!(id), &set).await?;
    Ok(())
}

async fn expire_sessions(txn: &mut Txn, before_ms: i64, limit: usize) -> Result<Batch> {
    let (sessions, more) = first_rows::<Session>(txn, "sessions", expired_sessions(before_ms), limit).await?;
    for s in &sessions {
        table::delete(txn, "sessions", &json!(s.token_hash)).await?;
    }
    Ok(Batch { deleted: sessions.len(), more })
}

async fn heartbeat(w: &mut Write<'_, '_>, node: &NodeRecord, dead_after_ms: i64) -> Result<()> {
    let nodes = list_nodes(w.txn).await?;
    // Ensure `started_at` increases monotonically across restarts at the same address, regardless of clock skew.
    let started_at = match nodes.iter().find(|n| n.id == node.id) {
        Some(n) => n.started_at,
        None => nodes.iter().filter(|n| n.addr == node.addr).map(|n| n.started_at + 1).fold(node.started_at, i64::max),
    };
    let rec = NodeRecord { started_at, ..node.clone() };
    table::insert(w.txn, w.blobs, "nodes", &to_row(&rec), InsertMode::Upsert).await?;
    for n in nodes {
        if n.id != node.id && n.heartbeat_at + dead_after_ms < w.now_ms {
            table::delete(w.txn, "nodes", &json!(n.id)).await?;
        }
    }
    Ok(())
}

fn answer<T: Serialize>(v: T) -> Result<Json> {
    serde_json::to_value(v).map_err(|e| Error::internal(format!("encoding an answer: {e}")))
}

/// Executes `cmd` within `txn` and returns its serialized result.
pub async fn apply(txn: &mut Txn, blobs: &BlobResolver<'_>, cmd: &MetaCmd, now_ms: i64) -> Result<Json> {
    let w = &mut Write { txn, blobs, now_ms };
    match cmd {
        MetaCmd::Migrate => answer(migrate(w).await?),
        MetaCmd::EnsureConfig { key, value } => answer(ensure_config(w, key, value).await?),
        MetaCmd::Signup { id, email, password_hash, policy, bootstrap_admin } => {
            answer(signup(w, id, email, password_hash, *policy, *bootstrap_admin).await?)
        }
        MetaCmd::GoogleLogin { new_id, sub, email, email_verified, policy } => {
            answer(google_login(w, new_id, sub, email, *email_verified, *policy).await?)
        }
        MetaCmd::SetPassword { user_id, password_hash, expected, keep_session, verify_email } => {
            answer(set_password(w, user_id, password_hash, expected, keep_session, *verify_email).await?)
        }
        MetaCmd::VerifyEmail { user_id, email } => answer(verify_email(w, user_id, email).await?),
        MetaCmd::SetDisabled { actor, user_id, disabled } => answer(set_disabled(w, actor, user_id, *disabled).await?),
        MetaCmd::CreateSession { token_hash, id, user_id, expires_at, client, method } => {
            let user = active_user(w.txn, user_id).await?;
            let s = Session {
                token_hash: token_hash.clone(),
                id: id.clone(),
                user_id: user_id.clone(),
                created_at: w.now_ms,
                expires_at: *expires_at,
                client: client.clone(),
                method: *method,
                password_fp: password_fp(&user),
            };
            answer(table::insert(w.txn, w.blobs, "sessions", &to_row(&s), InsertMode::Insert).await?)
        }
        MetaCmd::DeleteSession { token_hash } => answer(table::delete(w.txn, "sessions", &json!(token_hash)).await?),
        MetaCmd::RevokeSession { user_id, id } => answer(revoke_session(w.txn, user_id, id).await?),
        MetaCmd::RevokeSessions { user_id, except, limit } => {
            answer(revoke_sessions(w.txn, user_id, except, *limit).await?)
        }
        MetaCmd::CreateApiKey { key } => {
            active_user(w.txn, &key.user_id).await?;
            answer(table::insert(w.txn, w.blobs, "api_keys", &to_row(key), InsertMode::Insert).await?)
        }
        MetaCmd::DeleteApiKey { id, user_id } => match get_api_key(w.txn, id).await? {
            Some(k) if k.user_id == *user_id => answer(table::delete(w.txn, "api_keys", &json!(id)).await?),
            _ => Err(Error::not_found("API key not found")),
        },
        MetaCmd::DeleteApiKeys { user_id, limit } => answer(delete_api_keys(w.txn, user_id, *limit).await?),
        MetaCmd::CreateDb { id, owner_id, actor, name, max_dbs } => {
            answer(create_db(w, id, owner_id, actor, name, *max_dbs).await?)
        }
        MetaCmd::DeleteDb { id } => {
            let d = get_db(w.txn, id).await?.ok_or_else(|| Error::not_found("database not found"))?;
            answer(delete_db(w, d).await?)
        }
        MetaCmd::DeleteUser { user_id } => answer(delete_user(w, user_id).await?),
        MetaCmd::PurgePass { id, passes } => answer(purge_pass(w, id, *passes).await?),
        MetaCmd::ExpireSessions { before_ms, limit } => answer(expire_sessions(w.txn, *before_ms, *limit).await?),
        MetaCmd::Heartbeat { node, dead_after_ms } => answer(heartbeat(w, node, *dead_after_ms).await?),
        MetaCmd::Leave { id } => answer(table::delete(w.txn, "nodes", &json!(id)).await?),
        MetaCmd::CreateOrg { id, name, actor, max_orgs } => {
            answer(org::create_org(w, id, name, actor, *max_orgs).await?)
        }
        MetaCmd::DeleteOrg { org_id, actor } => answer(org::delete_org(w, org_id, actor).await?),
        MetaCmd::AddMember { org_id, actor, email, role, limits } => {
            answer(org::add_member(w, org_id, actor, email, *role, *limits).await?)
        }
        MetaCmd::SetRole { org_id, actor, user_id, role } => {
            answer(org::set_role(w, org_id, actor, user_id, *role).await?)
        }
        MetaCmd::RemoveMember { org_id, actor, user_id } => {
            answer(org::remove_member(w, org_id, actor, user_id).await?)
        }
    }
}
