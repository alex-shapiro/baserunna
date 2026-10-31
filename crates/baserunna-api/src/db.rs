//! Databases.

use crate::{Endpoint, json_body};
use http::{Method, StatusCode};
use serde::{Deserialize, Serialize};

/// `POST /v1/dbs`: creates a database owned by the user or by an organization.
#[derive(Clone, Copy, Debug, Default)]
pub struct CreateDb;

impl Endpoint for CreateDb {
    const METHOD: Method = Method::POST;
    const ROUTE: &'static str = "/v1/dbs";
    const STATUS: StatusCode = StatusCode::CREATED;
    const IDEMPOTENT: bool = false;
    type Body = NewDb;
    type Response = Db;

    fn path(&self) -> String {
        Self::ROUTE.into()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NewDb {
    pub name: String,
    /// Organization that owns the database, or `None` for a personal database.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub org: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Db {
    pub id: String,
    pub name: String,
    /// Organization that owns the database, or `None` for a personal database.
    pub org: Option<String>,
    pub created_at: i64,
}

json_body!(NewDb, Db);
