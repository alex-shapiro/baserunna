//! Database management endpoints and types.

use crate::{Empty, Endpoint, NoBody, Role, fill_route, json_body, with_query};
use http::{Method, StatusCode};
use serde::{Deserialize, Serialize};

/// `POST /v1/dbs`.
#[derive(Clone, Copy, Debug, Default)]
pub struct CreateDb;

impl Endpoint for CreateDb {
    const METHOD: Method = Method::POST;
    const ROUTE: &'static str = "/v1/dbs";
    const STATUS: StatusCode = StatusCode::CREATED;
    type Body = NewDb;
    type Response = Db;

    fn is_idempotent(&self, _: &Self::Body) -> bool {
        false
    }

    fn path(&self) -> String {
        Self::ROUTE.into()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NewDb {
    pub name: String,
    /// Owning organization, or `None` for a personal database.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub org: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Db {
    pub id: String,
    pub name: String,
    /// Owning organization, or `None` for a personal database.
    pub org: Option<String>,
    pub created_at: i64,
}

json_body!(NewDb, Db);

/// `GET /v1/dbs`.
#[derive(Clone, Debug, Default)]
pub struct ListDbs {
    pub query: ListDbsQuery,
}

impl Endpoint for ListDbs {
    const METHOD: Method = Method::GET;
    const ROUTE: &'static str = "/v1/dbs";
    const STATUS: StatusCode = StatusCode::OK;
    type Body = NoBody;
    type Response = DbPage;

    fn is_idempotent(&self, _: &Self::Body) -> bool {
        true
    }

    fn path(&self) -> String {
        with_query(Self::ROUTE.into(), &self.query)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListDbsQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub org: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DbPage {
    pub databases: Vec<Db>,
    pub next_cursor: Option<String>,
}

/// `GET /v1/dbs/{db}`.
#[derive(Clone, Debug)]
pub struct GetDb {
    pub db: String,
}

impl Endpoint for GetDb {
    const METHOD: Method = Method::GET;
    const ROUTE: &'static str = "/v1/dbs/{db}";
    const STATUS: StatusCode = StatusCode::OK;
    type Body = NoBody;
    type Response = DbInfo;

    fn is_idempotent(&self, _: &Self::Body) -> bool {
        true
    }

    fn path(&self) -> String {
        fill_route(Self::ROUTE, &[&self.db])
    }
}

/// `DELETE /v1/dbs/{db}`.
#[derive(Clone, Debug)]
pub struct DeleteDb {
    pub db: String,
}

impl Endpoint for DeleteDb {
    const METHOD: Method = Method::DELETE;
    const ROUTE: &'static str = "/v1/dbs/{db}";
    const STATUS: StatusCode = StatusCode::OK;
    type Body = NoBody;
    type Response = Empty;

    fn is_idempotent(&self, _: &Self::Body) -> bool {
        false
    }

    fn path(&self) -> String {
        fill_route(Self::ROUTE, &[&self.db])
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DbInfo {
    pub id: String,
    pub name: String,
    pub org: Option<String>,
    pub role: Role,
    pub created_at: i64,
    pub stats: DbStats,
}

/// Storage statistics for a database snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DbStats {
    pub version: u64,
    /// Non-empty segment levels, shallowest first.
    pub levels: Vec<LevelStats>,
    pub tail_entries: usize,
    pub tail_bytes: usize,
    pub range_tombstones: usize,
    /// Referenced blob bytes.
    pub blob_bytes: u64,
    /// Unreferenced segments and blobs awaiting garbage collection.
    pub pending_gc_objects: usize,
    /// Node that holds the write lease.
    pub lease_holder: Option<String>,
    pub lease_expires_at: Option<u64>,
    /// Store time of the last commit or maintenance write.
    pub updated_at: u64,
    /// Total stored bytes counted against the database quota.
    pub size_bytes: u64,
    pub quota_bytes: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LevelStats {
    pub level: u8,
    pub segments: usize,
    pub bytes: u64,
    pub entries: u64,
}

json_body!(DbPage, DbInfo);
