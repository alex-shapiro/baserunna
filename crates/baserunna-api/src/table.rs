//! Table schemas.

use crate::{Consistency, Endpoint, NoBody, fill_route, is_false, json_body, with_query};
use http::{Method, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::Value as Json;

/// `POST /v1/dbs/{db}/tables`: creates a table.
///
/// Fails with `already_exists` if the table exists, unless `if_not_exists` is set.
#[derive(Clone, Debug)]
pub struct CreateTable {
    pub db: String,
}

impl Endpoint for CreateTable {
    const METHOD: Method = Method::POST;
    const ROUTE: &'static str = "/v1/dbs/{db}/tables";
    const STATUS: StatusCode = StatusCode::CREATED;
    type Body = NewTable;
    type Response = CreatedTable;

    fn is_idempotent(body: &NewTable) -> bool {
        body.if_not_exists
    }

    fn path(&self) -> String {
        fill_route(Self::ROUTE, &[&self.db])
    }
}

/// `GET /v1/dbs/{db}/tables`: lists tables in name order.
#[derive(Clone, Debug)]
pub struct ListTables {
    pub db: String,
    pub query: ListTablesQuery,
}

impl Endpoint for ListTables {
    const METHOD: Method = Method::GET;
    const ROUTE: &'static str = "/v1/dbs/{db}/tables";
    const STATUS: StatusCode = StatusCode::OK;
    type Body = NoBody;
    type Response = TablePage;

    fn is_idempotent(_: &NoBody) -> bool {
        true
    }

    fn path(&self) -> String {
        with_query(fill_route(Self::ROUTE, &[&self.db]), &self.query)
    }
}

/// `GET /v1/dbs/{db}/tables/{table}`: reads a table schema.
#[derive(Clone, Debug)]
pub struct GetTable {
    pub db: String,
    pub table: String,
}

impl Endpoint for GetTable {
    const METHOD: Method = Method::GET;
    const ROUTE: &'static str = "/v1/dbs/{db}/tables/{t}";
    const STATUS: StatusCode = StatusCode::OK;
    type Body = NoBody;
    type Response = Table;

    fn is_idempotent(_: &NoBody) -> bool {
        true
    }

    fn path(&self) -> String {
        fill_route(Self::ROUTE, &[&self.db, &self.table])
    }
}

/// Column value type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ColumnType {
    String,
    Int,
    Float,
    Bool,
    /// Binary value, as base64 in JSON.
    Bytes,
    Json,
    /// Binary value stored out of band, written as a staged `blob_id` and read as blob metadata.
    Blob,
}

impl ColumnType {
    pub const ALL: [ColumnType; 7] = [
        ColumnType::String,
        ColumnType::Int,
        ColumnType::Float,
        ColumnType::Bool,
        ColumnType::Bytes,
        ColumnType::Json,
        ColumnType::Blob,
    ];

    pub fn name(self) -> &'static str {
        match self {
            ColumnType::String => "string",
            ColumnType::Int => "int",
            ColumnType::Float => "float",
            ColumnType::Bool => "bool",
            ColumnType::Bytes => "bytes",
            ColumnType::Json => "json",
            ColumnType::Blob => "blob",
        }
    }

    /// Returns true if the column can be part of a primary key or index.
    pub fn indexable(self) -> bool {
        !matches!(self, ColumnType::Json | ColumnType::Blob)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: ColumnType,
    #[serde(default = "yes")]
    pub nullable: bool,
    /// Value of the column in rows written before the column was added.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<Json>,
}

impl Column {
    /// Returns a non-nullable column.
    pub fn new(name: &str, ty: ColumnType) -> Column {
        Column { name: name.into(), ty, nullable: false, default: None }
    }

    /// Returns a nullable column.
    pub fn nullable(name: &str, ty: ColumnType) -> Column {
        Column { nullable: true, ..Column::new(name, ty) }
    }
}

fn yes() -> bool {
    true
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NewIndex {
    pub name: String,
    pub columns: Vec<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub unique: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NewTable {
    pub name: String,
    pub columns: Vec<Column>,
    pub primary_key: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub indexes: Vec<NewIndex>,
    /// If true, returns the existing table instead of failing with `already_exists`.
    #[serde(default, skip_serializing_if = "is_false")]
    pub if_not_exists: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub primary_key: Vec<String>,
    pub indexes: Vec<Index>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Index {
    pub name: String,
    pub columns: Vec<String>,
    pub unique: bool,
    pub state: IndexState,
}

/// Every row write maintains an index in either state, but only a `Ready` index serves queries.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IndexState {
    #[default]
    Ready,
    /// Being backfilled from the table's rows.
    Building,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CreatedTable {
    #[serde(flatten)]
    pub table: Table,
    /// Version that committed the table, or the version read if the table already existed.
    pub version: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListTablesQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Maximum number of tables, defaulting to 100.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consistency: Option<Consistency>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_staleness_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TablePage {
    pub tables: Vec<Table>,
    pub next_cursor: Option<String>,
    pub version: u64,
}

json_body!(NewTable, Table, CreatedTable, TablePage);
