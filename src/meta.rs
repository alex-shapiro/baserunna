//! Service metadata stored in the system database (`sys`) using the table
//! layer: users, sessions, API keys, databases, cluster nodes and config.
//!
//! Writes are expressed as serializable [`MetaCmd`]s so that any node can
//! ask the system database's lease holder to run them. Reads are snapshot
//! reads that any node can serve.

use crate::engine::Txn;
use crate::error::{Error, Result};
use crate::table::{self, BlobResolver, CreateTable, InsertMode, Query};
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};

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
    pub created_at: i64,
}

impl User {
    pub fn public_json(&self) -> Json {
        json!({
            "id": self.id,
            "email": self.email,
            "is_admin": self.is_admin,
            "disabled": self.disabled,
            "has_password": self.password_hash.is_some(),
            "google_linked": self.google_sub.is_some(),
            "created_at": self.created_at,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Session {
    pub token_hash: String,
    pub user_id: String,
    pub created_at: i64,
    pub expires_at: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiKey {
    pub id: String,
    pub user_id: String,
    pub name: String,
    pub secret_hash: String,
    pub db_id: Option<String>,
    pub read_only: bool,
    /// Optional request rate limit in requests per second. Defaults to the node rate limit if None.
    pub rate_limit: Option<u32>,
    pub created_at: i64,
}

impl ApiKey {
    pub fn public_json(&self, db_name: Option<&str>) -> Json {
        json!({
            "id": self.id,
            "name": self.name,
            "db": db_name,
            "read_only": self.read_only,
            "rate_limit": self.rate_limit,
            "created_at": self.created_at,
        })
    }
}

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
    pub started_at: i64,
    pub heartbeat_at: i64,
}

pub fn valid_db_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 63
        && s.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-' || b == b'_')
        && s.as_bytes()[0].is_ascii_alphanumeric()
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
            {"name": "created_at", "type": "int", "nullable": false}
        ], "indexes": [
            {"name": "by_email", "columns": ["email"], "unique": true},
            {"name": "by_google", "columns": ["google_sub"], "unique": true}
        ]}),
        json!({"name": "sessions", "primary_key": ["token_hash"], "columns": [
            {"name": "token_hash", "type": "string", "nullable": false},
            {"name": "user_id", "type": "string", "nullable": false},
            {"name": "created_at", "type": "int", "nullable": false},
            {"name": "expires_at", "type": "int", "nullable": false}
        ], "indexes": [
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
        json!({"name": "databases", "primary_key": ["id"], "columns": [
            {"name": "id", "type": "string", "nullable": false},
            {"name": "owner_id", "type": "string", "nullable": false},
            {"name": "name", "type": "string", "nullable": false},
            {"name": "created_at", "type": "int", "nullable": false}
        ], "indexes": [{"name": "by_owner_name", "columns": ["owner_id", "name"], "unique": true}]}),
        json!({"name": "db_counts", "primary_key": ["owner_id"], "columns": [
            {"name": "owner_id", "type": "string", "nullable": false},
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

/// Metadata writes. Executed on the system database's lease holder.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum MetaCmd {
    Migrate,
    EnsureConfig {
        key: String,
        value: String,
    },
    Signup {
        id: String,
        email: String,
        password_hash: Option<String>,
        policy: SignupPolicy,
        /// Bootstrap admin from configuration (bypasses the policy).
        bootstrap_admin: bool,
    },
    GoogleLogin {
        new_id: String,
        sub: String,
        email: String,
        email_verified: bool,
        policy: SignupPolicy,
    },
    SetPassword {
        user_id: String,
        password_hash: String,
    },
    SetDisabled {
        user_id: String,
        disabled: bool,
    },
    CreateSession {
        token_hash: String,
        user_id: String,
        expires_at: i64,
    },
    DeleteSession {
        token_hash: String,
    },
    CreateApiKey {
        key: ApiKey,
    },
    DeleteApiKey {
        id: String,
        user_id: String,
    },
    CreateDb {
        id: String,
        owner_id: String,
        name: String,
        max_dbs: usize,
    },
    DeleteDb {
        id: String,
    },
    /// Record a purge of a deleted database's objects. `passes` is the
    /// count the purge was made at, so concurrent reapers count it once.
    PurgePass {
        id: String,
        passes: i64,
    },
    /// Delete up to `limit` sessions that expired before `before_ms`.
    ExpireSessions {
        before_ms: i64,
        limit: usize,
    },
    Heartbeat {
        node: NodeRecord,
        dead_after_ms: i64,
    },
    /// Remove a node from the membership table (graceful shutdown).
    Leave {
        id: String,
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
            MetaCmd::SetDisabled { .. } => "set_disabled",
            MetaCmd::CreateSession { .. } => "create_session",
            MetaCmd::DeleteSession { .. } => "delete_session",
            MetaCmd::CreateApiKey { .. } => "create_api_key",
            MetaCmd::DeleteApiKey { .. } => "delete_api_key",
            MetaCmd::CreateDb { .. } => "create_db",
            MetaCmd::DeleteDb { .. } => "delete_db",
            MetaCmd::PurgePass { .. } => "purge_pass",
            MetaCmd::ExpireSessions { .. } => "expire_sessions",
            MetaCmd::Heartbeat { .. } => "heartbeat",
            MetaCmd::Leave { .. } => "leave",
        }
    }
}

fn from_row<T: serde::de::DeserializeOwned>(v: Json) -> Result<T> {
    serde_json::from_value(v).map_err(|e| Error::internal(format!("bad metadata row: {e}")))
}

fn to_row<T: Serialize>(v: &T) -> Json {
    serde_json::to_value(v).unwrap_or(Json::Null)
}

async fn query_all(txn: &mut Txn, t: &str, mut q: Query) -> Result<Vec<Json>> {
    let mut out = Vec::new();
    q.limit = Some(1000);
    loop {
        let r = table::query(txn, t, &q).await?;
        out.extend(r.rows);
        match r.next_cursor {
            Some(c) => q.cursor = Some(c),
            None => return Ok(out),
        }
    }
}

fn q_index(index: &str, eq: Vec<Json>, limit: usize) -> Query {
    Query { index: Some(index.into()), eq, limit: Some(limit), ..Default::default() }
}

// ------------------------------------------------------------------ reads

pub async fn get_user(txn: &mut Txn, id: &str) -> Result<Option<User>> {
    table::get(txn, "users", &json!(id)).await?.map(from_row).transpose()
}

pub async fn user_by_email(txn: &mut Txn, email: &str) -> Result<Option<User>> {
    let r = table::query(txn, "users", &q_index("by_email", vec![json!(email)], 1)).await?;
    r.rows.into_iter().next().map(from_row).transpose()
}

pub async fn user_by_google(txn: &mut Txn, sub: &str) -> Result<Option<User>> {
    let r = table::query(txn, "users", &q_index("by_google", vec![json!(sub)], 1)).await?;
    r.rows.into_iter().next().map(from_row).transpose()
}

pub async fn list_users(txn: &mut Txn) -> Result<Vec<User>> {
    query_all(txn, "users", Query::default()).await?.into_iter().map(from_row).collect()
}

/// Whether some session expired before `before_ms`.
pub async fn any_session_expired(txn: &mut Txn, before_ms: i64) -> Result<bool> {
    let q = Query {
        index: Some("by_expiry".into()),
        range: Some(crate::table::RangeSpec { lt: Some(json!(before_ms)), ..Default::default() }),
        limit: Some(1),
        ..Default::default()
    };
    Ok(!table::query(txn, "sessions", &q).await?.rows.is_empty())
}

pub async fn get_session(txn: &mut Txn, token_hash: &str) -> Result<Option<Session>> {
    table::get(txn, "sessions", &json!(token_hash)).await?.map(from_row).transpose()
}

pub async fn get_api_key(txn: &mut Txn, id: &str) -> Result<Option<ApiKey>> {
    table::get(txn, "api_keys", &json!(id)).await?.map(from_row).transpose()
}

pub async fn list_api_keys(txn: &mut Txn, user_id: &str) -> Result<Vec<ApiKey>> {
    query_all(txn, "api_keys", q_index("by_user", vec![json!(user_id)], 1000))
        .await?
        .into_iter()
        .map(from_row)
        .collect()
}

pub async fn get_db(txn: &mut Txn, id: &str) -> Result<Option<DbRecord>> {
    table::get(txn, "databases", &json!(id)).await?.map(from_row).transpose()
}

pub async fn resolve_db(txn: &mut Txn, owner_id: &str, name: &str) -> Result<Option<DbRecord>> {
    let r = table::query(txn, "databases", &q_index("by_owner_name", vec![json!(owner_id), json!(name)], 1)).await?;
    r.rows.into_iter().next().map(from_row).transpose()
}

pub async fn list_dbs(txn: &mut Txn, owner_id: &str) -> Result<Vec<DbRecord>> {
    query_all(txn, "databases", q_index("by_owner_name", vec![json!(owner_id)], 1000))
        .await?
        .into_iter()
        .map(from_row)
        .collect()
}

pub async fn list_all_dbs(txn: &mut Txn) -> Result<Vec<DbRecord>> {
    query_all(txn, "databases", Query::default()).await?.into_iter().map(from_row).collect()
}

pub async fn list_deleted_dbs(txn: &mut Txn) -> Result<Vec<DeletedDb>> {
    query_all(txn, "deleted_databases", Query::default()).await?.into_iter().map(from_row).collect()
}

pub async fn list_nodes(txn: &mut Txn) -> Result<Vec<NodeRecord>> {
    query_all(txn, "nodes", Query::default()).await?.into_iter().map(from_row).collect()
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

/// How many databases `owner_id` has (kept in `db_counts`, so creating a
/// database reads one row however many the owner has).
async fn db_count(txn: &mut Txn, owner_id: &str) -> Result<i64> {
    Ok(table::get(txn, "db_counts", &json!(owner_id)).await?.and_then(|r| r["count"].as_i64()).unwrap_or(0))
}

async fn set_db_count(txn: &mut Txn, blobs: &BlobResolver<'_>, owner_id: &str, count: i64) -> Result<()> {
    if count <= 0 {
        table::delete(txn, "db_counts", &json!(owner_id)).await?;
    } else {
        table::insert(txn, blobs, "db_counts", &json!({"owner_id": owner_id, "count": count}), InsertMode::Upsert)
            .await?;
    }
    Ok(())
}

/// Every owner's `db_counts` row must equal its number of databases, and
/// owners without databases have no row (consistency checker for tests and
/// the simulator).
pub async fn check_db_counts(txn: &mut Txn) -> Result<()> {
    let mut want: std::collections::BTreeMap<String, i64> = std::collections::BTreeMap::new();
    for d in list_all_dbs(txn).await? {
        *want.entry(d.owner_id).or_default() += 1;
    }
    let mut got = std::collections::BTreeMap::new();
    for r in query_all(txn, "db_counts", Query::default()).await? {
        let owner = r["owner_id"].as_str().unwrap_or_default().to_string();
        got.insert(owner, r["count"].as_i64().unwrap_or(-1));
    }
    if got != want {
        return Err(Error::internal(format!("database counts {got:?} do not match the databases {want:?}")));
    }
    Ok(())
}

// ----------------------------------------------------------------- writes

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

/// Apply a metadata command inside a system-database transaction.
pub async fn apply(txn: &mut Txn, blobs: &BlobResolver<'_>, cmd: &MetaCmd, now_ms: i64) -> Result<Json> {
    match cmd {
        MetaCmd::Migrate => {
            if is_migrated(txn).await? {
                return Ok(json!({"migrated": false}));
            }
            for t in tables() {
                let mut def: CreateTable = serde_json::from_value(t).map_err(|e| Error::internal(e.to_string()))?;
                def.if_not_exists = true;
                table::create_table(txn, &def).await?;
            }
            table::insert(
                txn,
                blobs,
                "config",
                &json!({"key": "schema_version", "value": SCHEMA_VERSION.to_string()}),
                InsertMode::Upsert,
            )
            .await?;
            Ok(json!({"migrated": true}))
        }
        MetaCmd::EnsureConfig { key, value } => {
            if let Some(v) = get_config(txn, key).await? {
                return Ok(json!({"value": v}));
            }
            table::insert(txn, blobs, "config", &json!({"key": key, "value": value}), InsertMode::Insert).await?;
            Ok(json!({"value": value}))
        }
        MetaCmd::Signup { id, email, password_hash, policy, bootstrap_admin } => {
            if let Some(u) = user_by_email(txn, email).await? {
                if *bootstrap_admin {
                    return Ok(json!({"user_id": u.id, "is_admin": u.is_admin, "created": false}));
                }
                return Err(Error::AlreadyExists("an account with this email already exists".into()));
            }
            let first = !any_user(txn).await?;
            if !bootstrap_admin {
                signup_allowed(*policy, first)?;
            }
            let u = User {
                id: id.clone(),
                email: email.clone(),
                password_hash: password_hash.clone(),
                google_sub: None,
                is_admin: first || *bootstrap_admin,
                disabled: false,
                created_at: now_ms,
            };
            table::insert(txn, blobs, "users", &to_row(&u), InsertMode::Insert).await?;
            Ok(json!({"user_id": u.id, "is_admin": u.is_admin, "created": true}))
        }
        MetaCmd::GoogleLogin { new_id, sub, email, email_verified, policy } => {
            if let Some(u) = user_by_google(txn, sub).await? {
                return Ok(json!({"user_id": u.id, "created": false}));
            }
            if let Some(u) = user_by_email(txn, email).await? {
                if !email_verified {
                    return Err(Error::PermissionDenied("Google account email is not verified".into()));
                }
                table::update(txn, blobs, "users", &json!(u.id), &json!({"google_sub": sub})).await?;
                return Ok(json!({"user_id": u.id, "created": false}));
            }
            if !email_verified {
                return Err(Error::PermissionDenied("Google account email is not verified".into()));
            }
            let first = !any_user(txn).await?;
            signup_allowed(*policy, first)?;
            let u = User {
                id: new_id.clone(),
                email: email.clone(),
                password_hash: None,
                google_sub: Some(sub.clone()),
                is_admin: first,
                disabled: false,
                created_at: now_ms,
            };
            table::insert(txn, blobs, "users", &to_row(&u), InsertMode::Insert).await?;
            Ok(json!({"user_id": u.id, "created": true}))
        }
        MetaCmd::SetPassword { user_id, password_hash } => {
            let found =
                table::update(txn, blobs, "users", &json!(user_id), &json!({"password_hash": password_hash})).await?;
            if !found {
                return Err(Error::not_found("user not found"));
            }
            Ok(json!({}))
        }
        MetaCmd::SetDisabled { user_id, disabled } => {
            let found = table::update(txn, blobs, "users", &json!(user_id), &json!({"disabled": disabled})).await?;
            if !found {
                return Err(Error::not_found("user not found"));
            }
            Ok(json!({}))
        }
        MetaCmd::CreateSession { token_hash, user_id, expires_at } => {
            let s = Session {
                token_hash: token_hash.clone(),
                user_id: user_id.clone(),
                created_at: now_ms,
                expires_at: *expires_at,
            };
            table::insert(txn, blobs, "sessions", &to_row(&s), InsertMode::Insert).await?;
            Ok(json!({}))
        }
        MetaCmd::DeleteSession { token_hash } => {
            table::delete(txn, "sessions", &json!(token_hash)).await?;
            Ok(json!({}))
        }
        MetaCmd::CreateApiKey { key } => {
            table::insert(txn, blobs, "api_keys", &to_row(key), InsertMode::Insert).await?;
            Ok(json!({}))
        }
        MetaCmd::DeleteApiKey { id, user_id } => match get_api_key(txn, id).await? {
            Some(k) if k.user_id == *user_id => {
                table::delete(txn, "api_keys", &json!(id)).await?;
                Ok(json!({}))
            }
            _ => Err(Error::not_found("API key not found")),
        },
        MetaCmd::CreateDb { id, owner_id, name, max_dbs } => {
            if !valid_db_name(name) {
                return Err(Error::invalid("database names are 1-63 chars of [a-z0-9_-], starting alphanumeric"));
            }
            let count = db_count(txn, owner_id).await?;
            let d = DbRecord { id: id.clone(), owner_id: owner_id.clone(), name: name.clone(), created_at: now_ms };
            table::insert(txn, blobs, "databases", &to_row(&d), InsertMode::Insert).await.map_err(|e| match e {
                Error::AlreadyExists(_) => Error::AlreadyExists(format!("database {name:?} already exists")),
                other => other,
            })?;
            if count >= *max_dbs as i64 {
                return Err(Error::PermissionDenied(format!("database limit of {max_dbs} reached")));
            }
            set_db_count(txn, blobs, owner_id, count + 1).await?;
            Ok(json!({"id": id, "name": name, "created_at": now_ms}))
        }
        MetaCmd::DeleteDb { id } => {
            let Some(d) = get_db(txn, id).await? else {
                return Err(Error::not_found("database not found"));
            };
            table::delete(txn, "databases", &json!(id)).await?;
            let count = db_count(txn, &d.owner_id).await?;
            set_db_count(txn, blobs, &d.owner_id, count - 1).await?;
            let del =
                DeletedDb { id: d.id.clone(), owner_id: d.owner_id, name: d.name, deleted_at: now_ms, purge_passes: 0 };
            table::insert(txn, blobs, "deleted_databases", &to_row(&del), InsertMode::Upsert).await?;
            Ok(json!({}))
        }
        MetaCmd::PurgePass { id, passes } => {
            let Some(row) = table::get(txn, "deleted_databases", &json!(id)).await? else {
                return Ok(json!({"done": true}));
            };
            let d: DeletedDb = from_row(row)?;
            if d.purge_passes != *passes {
                return Ok(json!({"done": false}));
            }
            if d.purge_passes >= 1 {
                table::delete(txn, "deleted_databases", &json!(id)).await?;
                return Ok(json!({"done": true}));
            }
            table::update(
                txn,
                blobs,
                "deleted_databases",
                &json!(id),
                &json!({"purge_passes": d.purge_passes + 1, "deleted_at": now_ms}),
            )
            .await?;
            Ok(json!({"done": false}))
        }
        MetaCmd::ExpireSessions { before_ms, limit } => {
            let q = Query {
                index: Some("by_expiry".into()),
                range: Some(crate::table::RangeSpec { lt: Some(json!(before_ms)), ..Default::default() }),
                limit: Some((*limit).clamp(1, 1000)),
                ..Default::default()
            };
            let rows = table::query(txn, "sessions", &q).await?.rows;
            for r in &rows {
                table::delete(txn, "sessions", &r["token_hash"]).await?;
            }
            Ok(json!({"deleted": rows.len()}))
        }
        MetaCmd::Heartbeat { node, dead_after_ms } => {
            table::insert(txn, blobs, "nodes", &to_row(node), InsertMode::Upsert).await?;
            // Forget long-dead nodes.
            for n in list_nodes(txn).await? {
                if n.id != node.id && n.heartbeat_at + dead_after_ms < now_ms {
                    table::delete(txn, "nodes", &json!(n.id)).await?;
                }
            }
            Ok(json!({}))
        }
        MetaCmd::Leave { id } => {
            table::delete(txn, "nodes", &json!(id)).await?;
            Ok(json!({}))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::tests::{engine, rt};
    use crate::engine::{Consistency, EngineConfig, Isolation};

    #[test]
    fn migrate_signup_and_dbs() {
        rt().block_on(async {
            let (eng, _s, _e) = engine(3, EngineConfig::small_for_tests());
            let db = eng.db(SYS_DB);
            db.open(true).await.unwrap();
            let blobs = BlobResolver { db: &db };
            let run = |cmd: MetaCmd| {
                let db = db.clone();
                async move {
                    let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
                    let r = apply(&mut t, &BlobResolver { db: &db }, &cmd, 1000).await;
                    if r.is_ok() {
                        db.commit(t.into_request()).await.unwrap();
                    }
                    r
                }
            };
            let _ = &blobs;
            assert_eq!(run(MetaCmd::Migrate).await.unwrap()["migrated"], json!(true));
            assert_eq!(run(MetaCmd::Migrate).await.unwrap()["migrated"], json!(false));
            let s = |id: &str, email: &str, policy| MetaCmd::Signup {
                id: id.into(),
                email: email.into(),
                password_hash: Some("h".into()),
                policy,
                bootstrap_admin: false,
            };
            let r = run(s("u1", "a@x.io", SignupPolicy::FirstUserOnly)).await.unwrap();
            assert_eq!(r["is_admin"], json!(true));
            assert!(matches!(
                run(s("u2", "b@x.io", SignupPolicy::FirstUserOnly)).await,
                Err(Error::PermissionDenied(_))
            ));
            assert!(matches!(run(s("u2", "a@x.io", SignupPolicy::Open)).await, Err(Error::AlreadyExists(_))));
            assert_eq!(run(s("u2", "b@x.io", SignupPolicy::Open)).await.unwrap()["is_admin"], json!(false));
            let c = |id: &str, name: &str| MetaCmd::CreateDb {
                id: id.into(),
                owner_id: "u2".into(),
                name: name.into(),
                max_dbs: 2,
            };
            run(c("d1", "one")).await.unwrap();
            assert!(matches!(run(c("d2", "one")).await, Err(Error::AlreadyExists(_))));
            assert!(matches!(run(c("d2", "Bad Name")).await, Err(Error::InvalidArgument(_))));
            run(c("d2", "two")).await.unwrap();
            assert!(matches!(run(c("d3", "three")).await, Err(Error::PermissionDenied(_))));
            run(MetaCmd::DeleteDb { id: "d1".into() }).await.unwrap();
            run(c("d3", "one")).await.unwrap();
            run(MetaCmd::DeleteDb { id: "d2".into() }).await.unwrap();
            run(MetaCmd::DeleteDb { id: "d3".into() }).await.unwrap();
            let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
            check_db_counts(&mut t).await.unwrap();
            assert!(table::get(&mut t, "db_counts", &json!("u2")).await.unwrap().is_none(), "no row at zero");
            run(c("d2", "two")).await.unwrap();
            run(c("d3", "one")).await.unwrap();
            let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
            check_db_counts(&mut t).await.unwrap();
            assert_eq!(resolve_db(&mut t, "u2", "one").await.unwrap().unwrap().id, "d3");
            assert_eq!(list_dbs(&mut t, "u2").await.unwrap().len(), 2);
            assert_eq!(list_deleted_dbs(&mut t).await.unwrap().len(), 3);
            assert_eq!(user_by_email(&mut t, "b@x.io").await.unwrap().unwrap().id, "u2");
            for name in ["users", "databases", "api_keys"] {
                table::check_indexes(&mut t, name).await.unwrap();
            }
            for (h, exp) in [("s1", 100), ("s2", 5000), ("s3", 200)] {
                run(MetaCmd::CreateSession { token_hash: h.into(), user_id: "u1".into(), expires_at: exp })
                    .await
                    .unwrap();
            }
            let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
            assert!(!any_session_expired(&mut t, 100).await.unwrap());
            assert!(any_session_expired(&mut t, 101).await.unwrap());
            let r = run(MetaCmd::ExpireSessions { before_ms: 1000, limit: 10 }).await.unwrap();
            assert_eq!(r["deleted"], json!(2));
            let mut t = db.begin(Isolation::Serializable, Consistency::Strong).await.unwrap();
            assert!(get_session(&mut t, "s1").await.unwrap().is_none());
            assert!(get_session(&mut t, "s2").await.unwrap().is_some());
            table::check_indexes(&mut t, "sessions").await.unwrap();
        });
    }
}
