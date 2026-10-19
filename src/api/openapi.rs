//! OpenAPI 3.1 specification for the HTTP API.

use serde_json::{Map, Value, json};
use std::sync::OnceLock;

fn r(name: &str) -> Value {
    json!({"$ref": format!("#/components/schemas/{name}")})
}

fn obj(props: Value, required: &[&str]) -> Value {
    json!({"type": "object", "properties": props, "required": required, "additionalProperties": false})
}

fn obj_desc(props: Value, required: &[&str], desc: &str) -> Value {
    let mut o = obj(props, required);
    o["description"] = json!(desc);
    o
}

fn nullable(t: &str) -> Value {
    json!({"type": [t, "null"]})
}

fn arr(items: Value) -> Value {
    json!({"type": "array", "items": items})
}

fn s() -> Value {
    json!({"type": "string"})
}

fn i() -> Value {
    json!({"type": "integer"})
}

fn b() -> Value {
    json!({"type": "boolean"})
}

fn any() -> Value {
    json!({})
}

fn json_body(schema: Value) -> Value {
    json!({"required": true, "content": {"application/json": {"schema": schema}}})
}

fn octet() -> Value {
    json!({"application/octet-stream": {"schema": {"type": "string", "format": "binary"}}})
}

fn octet_body() -> Value {
    json!({"required": true, "content": octet()})
}

fn resp(desc: &str, schema: Value) -> Value {
    json!({"description": desc, "content": {"application/json": {"schema": schema}}})
}

fn resp_plain(desc: &str) -> Value {
    json!({"description": desc})
}

fn bytes_resp(desc: &str) -> Value {
    json!({"description": desc, "content": octet()})
}

fn path_param(name: &str, desc: &str) -> Value {
    json!({"name": name, "in": "path", "required": true, "schema": s(), "description": desc})
}

fn query(name: &str, schema: Value, desc: &str) -> Value {
    json!({"name": name, "in": "query", "required": false, "schema": schema, "description": desc})
}

fn query_required(name: &str, schema: Value, desc: &str) -> Value {
    json!({"name": name, "in": "query", "required": true, "schema": schema, "description": desc})
}

fn header(name: &str, desc: &str) -> Value {
    json!({"name": name, "in": "header", "required": false, "schema": s(), "description": desc})
}

fn ttl() -> Value {
    json!({"type": "integer", "minimum": 1, "maximum": crate::ops::MAX_TTL_MS})
}

fn enum_of(values: &[&str]) -> Value {
    json!({"type": "string", "enum": values})
}

/// Builds a recursive filter schema named `name` with boolean combinators and `comparisons`.
fn filter_schema(name: &str, comparisons: Vec<Value>, desc: &str) -> Value {
    let mut nodes = vec![
        obj(json!({"and": arr(r(name))}), &["and"]),
        obj(json!({"or": arr(r(name))}), &["or"]),
        obj(json!({"not": r(name)}), &["not"]),
    ];
    nodes.extend(comparisons);
    let semantics = "A missing or null operand evaluates to false for all comparison operators, including `ne`; \
        use `exists` to test for presence. Numeric values are compared by numeric value.";
    json!({"oneOf": nodes, "description": format!("{desc} {semantics}")})
}

/// Builds a comparison leaf schema for the operand field `key`.
fn comparison(key: &str, operand: Value) -> Value {
    let value = json!({"description": "Omitted for `exists`, an array of at most 1000 elements for `in`, or a non-null scalar."});
    obj(json!({(key): operand, "op": enum_of(&crate::filter::OPS), "value": value}), &[key, "op"])
}

#[derive(Default)]
struct Op<'a> {
    summary: &'a str,
    tag: &'a str,
    responses: Vec<(&'a str, Value)>,
    body: Option<Value>,
    params: Vec<Value>,
    desc: Option<&'a str>,
    public: bool,
}

impl Op<'_> {
    fn build(self) -> Value {
        let mut responses = Map::new();
        for (code, r) in self.responses {
            responses.insert(code.to_string(), r);
        }
        responses.insert("default".into(), json!({"$ref": "#/components/responses/Error"}));
        let mut o = json!({"summary": self.summary, "tags": [self.tag], "responses": responses});
        if let Some(d) = self.desc {
            o["description"] = json!(d);
        }
        if let Some(b) = self.body {
            o["requestBody"] = b;
        }
        if !self.params.is_empty() {
            o["parameters"] = Value::Array(self.params);
        }
        if self.public {
            o["security"] = json!([]);
        }
        o
    }
}

fn op<'a>(summary: &'a str, tag: &'a str, responses: Vec<(&'a str, Value)>) -> Op<'a> {
    Op { summary, tag, responses, ..Default::default() }
}

fn db() -> Value {
    path_param(
        "db",
        "Database reference: `name` (personal database), `org.name` (organization database), or `id:<db_id>` \
         (administrators only).",
    )
}

fn org() -> Value {
    path_param("org", "Organization name.")
}

fn paging() -> Vec<Value> {
    vec![
        query(
            "limit",
            json!({"type": "integer", "minimum": 1, "maximum": crate::meta::PAGE_MAX}),
            &format!("Page size (default {})", crate::meta::PAGE_DEFAULT),
        ),
        query("cursor", s(), "From a previous next_cursor"),
    ]
}

fn page_of(field: &str, items: Value) -> Value {
    let mut props = Map::new();
    props.insert(field.to_string(), arr(items));
    props.insert("next_cursor".into(), nullable("string"));
    obj(Value::Object(props), &[field, "next_cursor"])
}

fn table() -> Value {
    path_param("t", "Table name.")
}

fn consistency() -> Vec<Value> {
    vec![
        query(
            "consistency",
            enum_of(&["strong", "snapshot"]),
            "`strong` (default, linearizable) or `snapshot` (bounded staleness).",
        ),
        query("max_staleness_ms", i(), "Staleness bound for snapshot reads (default 5000)."),
    ]
}

fn schemas() -> Value {
    json!({
        "Error": obj(json!({
            "error": obj(json!({
                "code": enum_of(&["invalid_argument", "unauthenticated", "permission_denied", "not_found",
                    "txn_not_found", "conflict", "already_exists", "precondition_failed", "too_large",
                    "cursor_expired", "quota_exceeded", "rate_limited", "unavailable", "internal"]),
                "message": s(),
                "retryable": b(),
                "details": obj(json!({
                    "op_index": i(),
                    "retry_after_ms": i(),
                    "lease_holder": s(),
                    "outcome": enum_of(&["unknown", "not_applied"]),
                }), &[]),
            }), &["code", "message", "retryable"]),
        }), &["error"]),
        "UserPublic": obj(json!({
            "id": s(), "email": s(), "email_verified": b(), "is_admin": b(), "disabled": b(), "has_password": b(),
            "google_linked": b(), "created_at": i(),
        }), &["id", "email", "email_verified", "is_admin", "disabled", "has_password", "google_linked", "created_at"]),
        "SessionInfo": obj_desc(json!({
            "id": s(), "created_at": i(), "expires_at": i(), "client": nullable("string"),
            "method": enum_of(&["signup", "password", "google", "reset"]), "current": b(),
        }), &["id", "created_at", "expires_at", "client", "method", "current"],
            "Authenticated session: `client` is the client address at sign-in, and `current` is true for the calling session."),
        "PasswordChange": obj_desc(json!({"current_password": s(), "new_password": {"type": "string", "minLength": 8}}),
            &["new_password"], "`current_password` is required for accounts that have a password."),
        "Role": enum_of(&["reader", "writer", "admin", "owner"]),
        "Org": obj(json!({"id": s(), "name": s(), "role": r("Role"), "created_at": i()}),
            &["id", "name", "role", "created_at"]),
        "Member": obj(json!({"user_id": s(), "email": s(), "role": r("Role"), "added_at": i()}),
            &["user_id", "email", "role", "added_at"]),
        "Session": obj(json!({"token": s(), "user": r("UserPublic")}), &["token", "user"]),
        "Credentials": obj(json!({"email": s(), "password": {"type": "string", "minLength": 8}}), &["email", "password"]),
        "Me": obj(json!({
            "user": r("UserPublic"),
            "credential": obj(json!({
                "type": enum_of(&["session", "api_key"]), "id": s(), "db": nullable("string"), "read_only": b(),
                "rate_limit": nullable("integer"),
            }), &["type"]),
        }), &["user", "credential"]),
        "ApiKey": obj(json!({
            "id": s(), "name": s(), "db": nullable("string"), "read_only": b(), "rate_limit": nullable("integer"),
            "created_at": i(),
        }), &["id", "name", "db", "read_only", "created_at"]),
        "CreateApiKey": obj_desc(json!({
            "name": s(), "db": nullable("string"), "read_only": b(),
            "rate_limit": {"type": ["integer", "null"], "minimum": 1, "maximum": 1_000_000},
        }), &["name"], "`rate_limit`: maximum requests per second allowed for this key on each node (defaults to the \
            node per-key limit)."),
        "Node": obj(json!({"id": s(), "addr": s(), "started_at": i(), "heartbeat_at": i(), "live": b()}),
            &["id", "addr", "started_at", "heartbeat_at", "live"]),
        "DbSummary": obj_desc(json!({"id": s(), "name": s(), "org": nullable("string"), "created_at": i()}),
            &["id", "name", "org", "created_at"], "`org` is the owning organization, or `null` for personal databases."),
        "LevelStats": obj(json!({"level": i(), "segments": i(), "bytes": i(), "entries": i()}),
            &["level", "segments", "bytes", "entries"]),
        "DbStats": obj_desc(json!({
            "version": i(), "levels": arr(r("LevelStats")), "tail_entries": i(), "tail_bytes": i(),
            "range_tombstones": i(), "blob_bytes": i(), "pending_gc_objects": i(), "lease_holder": nullable("string"),
            "lease_expires_at": nullable("integer"), "updated_at": i(), "size_bytes": i(),
            "quota_bytes": nullable("integer"),
        }), &["version", "levels", "tail_entries", "tail_bytes", "range_tombstones", "blob_bytes", "pending_gc_objects",
            "lease_holder", "lease_expires_at", "updated_at", "size_bytes", "quota_bytes"],
            "Database statistics at the read version (at most 5 s stale). `levels` lists segments by level from shallowest \
            to deepest; `size_bytes` (including blobs) is counted against `quota_bytes`."),
        "DbInfo": obj_desc(json!({
            "id": s(), "name": s(), "org": nullable("string"), "role": r("Role"), "created_at": i(), "stats": r("DbStats"),
        }), &["id", "name", "org", "role", "created_at", "stats"], "`role` specifies the caller's permissions: `reader` allows reads, \
            `writer` adds writes, and `admin` or `owner` adds database deletion."),
        "BlobMeta": obj_desc(json!({"blob_id": s(), "size": i(), "sha256": s(), "content_type": s()}),
            &["blob_id", "size", "sha256", "content_type"], "Out-of-band blob metadata."),
        "KvItem": obj_desc(json!({
            "key": s(), "key_b64": s(), "value": s(), "value_b64": s(), "blob": r("BlobMeta"),
            "version": nullable("integer"), "expires_at": i(),
        }), &["version"], "Key-value entry. Keys and values that are not valid UTF-8 in `utf8` mode are \
            returned in `key_b64` and `value_b64`. Values stored as blobs larger than the inline JSON limit \
            include only `blob`. An entry with a TTL includes `expires_at` (cluster time in ms) once committed."),
        "ScanResult": obj(json!({"items": arr(r("KvItem")), "next_cursor": nullable("string"), "version": i()}),
            &["items", "next_cursor"]),
        "OpResult": obj_desc(json!({
            "found": b(), "value": {"type": ["string", "integer"]}, "value_b64": s(),
            "blob": r("BlobMeta"), "version": nullable("integer"),
            "items": arr(r("KvItem")), "next_cursor": nullable("string"),
            "row": r("Row"), "rows": arr(r("Row")), "expires_at": i(), "count": i(),
            "records": arr(r("StreamRecord")), "first_seq": i(), "next_seq": i(), "done": b(), "seq": i(),
            "bytes": i(), "first_time": i(), "last_time": i(),
        }), &[], "Result of a transaction operation; populated fields depend on the operation type (see `Op`). \
            An `append` operation reports `first_seq` when the transaction is committed."),
        "Op": {
            "oneOf": [
                obj(json!({"op": {"const": "get"}, "key": s()}), &["op", "key"]),
                obj(json!({"op": {"const": "scan"}, "prefix": s(), "start": s(), "end": s(),
                    "limit": {"type": "integer", "minimum": 1, "maximum": 1000},
                    "reverse": b(), "keys_only": b(), "cursor": s()}), &["op"]),
                obj(json!({"op": {"const": "put"}, "key": s(), "value": s(), "blob_id": s(), "ttl_ms": ttl()}), &["op", "key"]),
                obj(json!({"op": {"const": "delete"}, "key": s()}), &["op", "key"]),
                obj(json!({"op": {"const": "delete_range"}, "prefix": s(), "start": s(), "end": s()}), &["op"]),
                obj(json!({"op": {"const": "check"}, "key": s(), "exists": b(), "version": i(), "value": s()}), &["op", "key"]),
                obj(json!({"op": {"const": "increment"}, "key": s(), "by": i(), "ttl_ms": ttl()}), &["op", "key"]),
                obj(json!({"op": {"const": "get_row"}, "table": s(), "pk": any()}), &["op", "table", "pk"]),
                obj(json!({"op": {"const": "insert_row"}, "table": s(), "row": r("Row"), "ttl_ms": ttl()}),
                    &["op", "table", "row"]),
                obj(json!({"op": {"const": "upsert_row"}, "table": s(), "row": r("Row"), "ttl_ms": ttl()}),
                    &["op", "table", "row"]),
                obj(json!({"op": {"const": "update_row"}, "table": s(), "pk": any(), "set": r("Row"), "ttl_ms": ttl()}),
                    &["op", "table", "pk", "set"]),
                obj(json!({"op": {"const": "delete_row"}, "table": s(), "pk": any()}), &["op", "table", "pk"]),
                obj(json!({"op": {"const": "update_rows"}, "table": s(), "where": r("Selection"), "set": r("Row"),
                    "ttl_ms": ttl(), "limit": {"type": "integer", "minimum": 1}}), &["op", "table", "where", "set"]),
                obj(json!({"op": {"const": "delete_rows"}, "table": s(), "where": r("Selection"),
                    "limit": {"type": "integer", "minimum": 1}}), &["op", "table", "where"]),
                {"type": "object", "properties": {"op": {"const": "query_rows"}, "table": s()},
                 "required": ["op", "table"], "allOf": [r("Query")], "additionalProperties": true},
                obj(json!({"op": {"const": "append"}, "stream": s(), "records": arr(r("NewRecord")),
                    "expect_seq": i(), "create": b()}), &["op", "stream", "records"]),
                {"type": "object", "properties": {"op": {"const": "read_stream"}, "stream": s()},
                 "required": ["op", "stream"], "allOf": [r("StreamRead")], "additionalProperties": true},
                obj(json!({"op": {"const": "stream_info"}, "stream": s()}), &["op", "stream"]),
                obj(json!({"op": {"const": "trim_stream"}, "stream": s(), "before_seq": i(), "before_time": i()}),
                    &["op", "stream"]),
                obj(json!({"op": {"const": "get_consumer"}, "stream": s(), "consumer": s()}),
                    &["op", "stream", "consumer"]),
                obj(json!({"op": {"const": "set_consumer"}, "stream": s(), "consumer": s(), "seq": i()}),
                    &["op", "stream", "consumer", "seq"]),
            ],
            "description": "Transaction operation. Keys and values are UTF-8 strings unless `encoding: base64` is set. \
                `ttl_ms` sets the entry or row expiration relative to the commit timestamp; row updates preserve \
                existing expiration unless `ttl_ms` is specified. `update_rows` and `delete_rows` modify all rows \
                matching `where` (up to `limit`) within the transaction write limit and return `count`. For stream \
                operations: `append` assigns sequence numbers at commit (`expect_seq` aborts the transaction with \
                412 if the stream's next sequence number does not match); `read_stream` conflicts with concurrent \
                appends only when the read reaches the end of the stream, and observes earlier trims in the same \
                transaction but not uncommitted appends; `stream_info` conflicts with concurrent appends; \
                `set_consumer` requires `seq` to be at most the stream's `next_seq`.",
        },
        "TxnRequest": obj(json!({
            "ops": arr(r("Op")),
            "encoding": enum_of(&["utf8", "base64"]),
            "isolation": enum_of(&["serializable", "snapshot"]),
            "idempotency_key": s(),
            "consistency": enum_of(&["strong", "snapshot"]),
            "max_staleness_ms": i(),
        }), &["ops"]),
        "TxnResult": obj_desc(json!({
            "version": i(), "committed": b(), "replayed": b(), "commit_id": s(),
            "results": {"oneOf": [arr(r("OpResult")), {"type": "null"}]},
            "appends": arr(i()),
        }), &["version", "committed", "results"], "`appends` lists the starting sequence number assigned to each `append` operation in order."),
        "BeginRequest": obj_desc(json!({"isolation": enum_of(&["serializable", "snapshot"]),
            "encoding": enum_of(&["utf8", "base64"]), "ops": arr(r("Op"))}), &[],
            "Optional operations to execute in the new transaction."),
        "Begin": obj(json!({"txn_id": s(), "commit_id": s(), "read_version": i(), "expires_at": i(),
            "idle_timeout_ms": i(), "results": arr(r("OpResult"))}),
            &["txn_id", "commit_id", "read_version", "expires_at", "idle_timeout_ms", "results"]),
        "CommitRequest": obj_desc(json!({"ops": arr(r("Op"))}), &[], "Optional operations to execute before the transaction is committed."),
        "OpsRequest": obj(json!({"ops": arr(r("Op"))}), &["ops"]),
        "OpsResult": obj(json!({"results": arr(r("OpResult"))}), &["results"]),
        "Committed": obj(json!({"version": i(), "commit_id": s(), "appends": arr(i()), "results": arr(r("OpResult"))}),
            &["version", "commit_id"]),
        "Metrics": obj(json!({"node": s(), "store": {"type": "object"}, "commits": {"type": "object"},
            "routes": {"type": "object"}, "lag": {"type": "object"}, "cpu": {"type": "object"}}),
            &["node", "store", "commits", "routes", "lag", "cpu"]),
        "CommitStatus": obj(json!({"status": enum_of(&["committed", "not_committed"]), "version": i(),
            "commit_id": s(), "appends": arr(i()), "retention_ms": i()}), &["status", "commit_id"]),
        "Version": obj(json!({"version": i()}), &["version"]),
        "DeleteResult": obj(json!({"version": i(), "existed": b()}), &["version", "existed"]),
        "Empty": obj(json!({}), &[]),
        "ColumnType": enum_of(&["string", "int", "float", "bool", "bytes", "json", "blob"]),
        "Column": obj(json!({"name": s(), "type": r("ColumnType"), "nullable": b(), "default": any()}), &["name", "type"]),
        "Index": obj(json!({"name": s(), "columns": arr(s()), "unique": b()}), &["name", "columns"]),
        "IndexInfo": obj_desc(json!({"name": s(), "columns": arr(s()), "unique": b(),
            "state": enum_of(&["ready", "building"])}), &["name", "columns", "unique", "state"],
            "A `building` index is maintained by writes but cannot be queried until its backfill finishes."),
        "CreateTable": obj(json!({"name": s(), "columns": arr(r("Column")), "primary_key": arr(s()),
            "indexes": arr(r("Index")), "if_not_exists": b()}), &["name", "columns", "primary_key"]),
        "TableSchema": obj(json!({"name": s(), "columns": arr(r("Column")), "primary_key": arr(s()),
            "indexes": arr(r("IndexInfo")), "version": i()}), &["name", "columns", "primary_key", "indexes"]),
        "Row": {"type": "object", "additionalProperties": true,
            "description": "Map of column names to values. `bytes` columns use base64; `blob` columns accept a staged \
                `blob_id` on write and return `BlobMeta` on read."},
        "Filter": filter_schema("Filter", vec![comparison("col", s())],
            "Row filter expression composed of `and`, `or`, `not`, or a comparison on column `col`."),
        "Query": obj_desc(json!({
            "index": s(), "eq": arr(any()),
            "range": obj(json!({"gt": any(), "gte": any(), "lt": any(), "lte": any()}), &[]),
            "filter": r("Filter"),
            "limit": {"type": "integer", "minimum": 1, "maximum": 1000},
            "reverse": b(), "cursor": s(),
            "consistency": enum_of(&["strong", "snapshot"]),
            "max_staleness_ms": i(),
        }), &[], "Scans the primary key (default) or a secondary index (`index`) using an equality prefix (`eq`), \
            an optional range bound on the next column, a residual `filter`, and pagination via `cursor`."),
        "Selection": obj_desc(json!({
            "index": s(), "eq": arr(any()),
            "range": obj(json!({"gt": any(), "gte": any(), "lt": any(), "lte": any()}), &[]),
            "filter": r("Filter"),
        }), &[], "Selects rows to modify using the primary key or a secondary index (`index`), an equality prefix \
            (`eq`), a range bound on the next column, and a residual `filter`. `{}` selects all rows."),
        "InsertRows": obj(json!({"row": r("Row"), "rows": arr(r("Row")), "upsert": b(), "ttl_ms": ttl()}), &[]),
        "RowCount": obj(json!({"count": i(), "version": i()}), &["count", "version"]),
        "PkRequest": obj(json!({"pk": any(), "consistency": s(), "max_staleness_ms": i()}), &["pk"]),
        "UpdateRows": obj_desc(json!({"pk": any(), "where": r("Selection"), "limit": {"type": "integer", "minimum": 1},
            "set": r("Row"), "ttl_ms": ttl()}), &["set"],
            "Specify either `pk` (updates a single row and returns `found`) or `where` (updates matching rows up to \
            `limit` and returns `count`). Existing row expiration is preserved unless `ttl_ms` is set."),
        "DeleteRows": obj_desc(json!({"pk": any(), "where": r("Selection"), "limit": {"type": "integer", "minimum": 1}}),
            &[], "Specify either `pk` (deletes a single row and returns `found`) or `where` (deletes matching rows up to \
            `limit` and returns `count`)."),
        "RowResult": obj(json!({"found": b(), "row": r("Row"), "expires_at": i(), "version": i()}), &["found", "version"]),
        "Found": obj(json!({"found": b(), "version": i()}), &["found", "version"]),
        "FoundOrCount": {"oneOf": [r("Found"), r("RowCount")]},
        "KvChange": obj_desc(json!({
            "version": i(), "key": s(), "key_b64": s(), "value": s(), "value_b64": s(), "blob": r("BlobMeta"),
            "expires_at": i(), "deleted": {"const": true},
            "deleted_range": obj(json!({"start": s(), "start_b64": s(), "end": nullable("string"), "end_b64": s()}), &[]),
        }), &["version"], "Key change entry as of the feed snapshot: either a live key-value entry (with `expires_at` if set), \
            a point deletion (`deleted: true`, including expired keys), or a range deletion over `[start, end)` \
            (`deleted_range`, where `end: null` denotes the end of the keyspace)."),
        "KvChanges": obj(json!({"changes": arr(r("KvChange")), "more": b(), "next_cursor": s(), "version": i()}),
            &["changes", "more", "next_cursor", "version"]),
        "RowChange": obj_desc(json!({
            "version": i(), "pk": arr(any()), "row": r("Row"), "expires_at": i(), "deleted": {"const": true},
            "deleted_range": {"const": true},
        }), &["version"], "Row change entry as of the feed snapshot: either a live row, a deleted primary key (`deleted: true`), \
            or a table truncation (`deleted_range: true`)."),
        "RowChanges": obj(json!({"changes": arr(r("RowChange")), "more": b(), "next_cursor": s(), "version": i()}),
            &["changes", "more", "next_cursor", "version"]),
        "QueryResult": obj(json!({"rows": arr(r("Row")), "next_cursor": nullable("string"), "version": i()}),
            &["rows", "next_cursor", "version"]),
        "Headers": {"type": "object", "additionalProperties": s(),
            "description": "Map of header names (1-128 bytes) to values (at most 4 KiB), with at most 64 entries."},
        "NewRecord": obj_desc(json!({"value": s(), "json": any(), "headers": r("Headers")}), &[],
            "Specify either `value` (encoded according to `encoding`) or `json` (any JSON value). Maximum record size is 1 MiB."),
        "StreamRecord": obj_desc(json!({
            "seq": i(), "time": i(), "headers": r("Headers"), "value": s(), "value_b64": s(), "json": any(),
        }), &["seq", "time"], "`seq` is the record sequence number; `time` (cluster time in ms) is the commit timestamp \
            of the append. The payload is returned in `json`, `value`, or `value_b64`."),
        "Consumer": obj_desc(json!({"name": s(), "seq": i(), "lag": i(), "updated_at": i(), "version": i()}),
            &["name", "seq", "lag", "updated_at"], "Consumer cursor state: `seq` is the next sequence number to read, and `lag` \
            is the number of unread records (`next_seq - seq`)."),
        "Stream": obj_desc(json!({
            "name": s(), "created_at": i(), "retention_ms": nullable("integer"), "max_bytes": nullable("integer"),
            "first_seq": i(), "next_seq": i(), "bytes": i(), "first_time": i(), "last_time": i(),
            "consumers": arr(r("Consumer")), "consumers_next_cursor": nullable("string"), "version": i(),
        }), &["name", "created_at", "retention_ms", "max_bytes", "first_seq", "next_seq", "bytes"],
            "Stream metadata and state. Retained records span `[first_seq, next_seq)` with total payload and header size \
            `bytes` and timestamps from `first_time` to `last_time`. `consumers` includes up to 100 consumers, continued via \
            `consumers_next_cursor`."),
        "CreateStream": obj_desc(json!({
            "name": s(), "retention_ms": {"type": "integer", "minimum": 1}, "max_bytes": {"type": "integer", "minimum": 1},
        }), &["name"], "Stream names must be 1-128 characters matching `[A-Za-z0-9_.:-]`. Records older than `retention_ms` \
            or exceeding `max_bytes` are eligible for automatic trimming."),
        "StreamConfig": obj_desc(json!({"retention_ms": nullable("integer"), "max_bytes": nullable("integer")}), &[],
            "Omitted fields keep their current setting; `null` removes a retention limit."),
        "AppendRequest": obj(json!({
            "records": arr(r("NewRecord")), "expect_seq": i(), "create": b(), "idempotency_key": s(),
            "encoding": enum_of(&["utf8", "base64"]),
        }), &["records"]),
        "Appended": obj_desc(json!({
            "first_seq": i(), "next_seq": i(), "count": i(), "time": i(), "version": i(), "commit_id": s(),
            "replayed": b(),
        }), &["first_seq", "next_seq", "count", "time", "version", "commit_id", "replayed"],
            "Appended records were assigned sequence numbers `[first_seq, next_seq)` at commit timestamp `time`. \
            When `replayed` is true, the response returns the result of an earlier append with the same `idempotency_key`."),
        "StreamFilter": filter_schema("StreamFilter", vec![
            comparison("field", json!({"type": "string", "minLength": 1, "maxLength": 256})),
            comparison("header", json!({"type": "string", "minLength": 1, "maxLength": 128})),
            obj(json!({"op": {"const": "contains"}, "value": s()}), &["op", "value"]),
        ], "Stream record filter composed of `and`, `or`, `not`, a comparison on a JSON body `field` (dot-separated \
            path where numeric segments index arrays) or `header`, or a `{ \"op\": \"contains\", \"value\": \"...\" }` \
            substring match on the raw record body."),
        "StreamRead": obj_desc(json!({
            "from_seq": i(), "from_time": i(), "tail": i(), "consumer": s(), "until_seq": i(), "until_time": i(),
            "reverse": b(), "limit": {"type": "integer", "minimum": 1, "maximum": 1000},
            "max_bytes": {"type": "integer", "minimum": 1, "maximum": 16 << 20}, "filter": r("StreamFilter"),
            "scan_limit": {"type": "integer", "minimum": 1, "maximum": 100_000},
            "encoding": enum_of(&["utf8", "base64"]), "consistency": enum_of(&["strong", "snapshot"]),
            "max_staleness_ms": i(),
        }), &[], "Reads records starting from at most one of `from_seq`, `from_time` (first record at or after the timestamp), \
            `tail` (last N records), or `consumer` (consumer cursor, starting at `first_seq` if unset), up to \
            `until_seq` or `until_time` (exclusive). Returns records oldest-first (or newest-first when `reverse` is true), \
            bounded by `limit` (default 100) and `max_bytes` (default 4 MiB, max 16 MiB). When `filter` is set, at most \
            `scan_limit` records (default 10,000) are scanned."),
        "StreamRecords": obj_desc(json!({
            "records": arr(r("StreamRecord")), "next_seq": i(), "first_seq": i(), "tail_seq": i(), "done": b(),
            "version": i(),
        }), &["records", "next_seq", "first_seq", "tail_seq", "done", "version"],
            "`next_seq` is the cursor for the next page (`from_seq` for forward reads, or `until_seq` for reverse reads). \
            `done` is true when the read reached the end of the requested range. `first_seq` and `tail_seq` are the stream's \
            current `first_seq` and `next_seq`."),
        "Trim": obj(json!({"before_seq": i(), "before_time": i()}), &[]),
        "SetConsumer": obj_desc(json!({"seq": i()}), &["seq"], "Must be at most the stream's `next_seq`."),
    })
}

fn paths() -> Value {
    let mut p = Map::new();
    let mut add = |path: &str, method: &str, o: Op| {
        let e = p.entry(path.to_string()).or_insert_with(|| json!({}));
        e[method] = o.build();
    };
    let txn_id = || path_param("id", "Transaction id");

    add(
        "/healthz",
        "get",
        Op {
            public: true,
            desc: Some("503 once the node is shutting down, so load balancers stop sending it requests."),
            ..op(
                "Liveness and node status",
                "ops",
                vec![
                    (
                        "200",
                        resp(
                            "OK",
                            json!({"type": "object", "properties": {"ok": b(), "node": {"type": "object"}}, "required": ["ok"]}),
                        ),
                    ),
                    (
                        "503",
                        resp(
                            "Draining before shutdown",
                            json!({"type": "object", "properties": {"ok": b(), "draining": b()}, "required": ["ok"]}),
                        ),
                    ),
                ],
            )
        },
    );
    add(
        "/v1/openapi.json",
        "get",
        Op { public: true, ..op("This document", "ops", vec![("200", resp("OpenAPI document", json!({})))]) },
    );
    add(
        "/v1/auth/signup",
        "post",
        Op {
            public: true,
            body: Some(json_body(r("Credentials"))),
            desc: Some("Subject to the server's sign-up policy. The first account becomes an administrator."),
            ..op(
                "Create an account (password)",
                "auth",
                vec![("201", resp("Signed up; a session is returned and set as a cookie", r("Session")))],
            )
        },
    );
    add(
        "/v1/auth/login",
        "post",
        Op {
            public: true,
            body: Some(json_body(r("Credentials"))),
            ..op("Sign in with email and password", "auth", vec![("200", resp("Session", r("Session")))])
        },
    );
    add(
        "/v1/auth/logout",
        "post",
        op("End the current session", "auth", vec![("200", resp("Signed out", r("Empty")))]),
    );
    add(
        "/v1/auth/google/start",
        "get",
        Op {
            public: true,
            params: vec![query("return_to", s(), "Relative path to return to")],
            ..op("Begin Google sign-in", "auth", vec![("302", resp_plain("Redirect to Google"))])
        },
    );
    add(
        "/v1/auth/google/callback",
        "get",
        Op {
            public: true,
            params: vec![query("code", s(), "Authorization code"), query("state", s(), "State")],
            ..op("Google OAuth callback", "auth", vec![("302", resp_plain("Signed in; redirect to return_to"))])
        },
    );
    add(
        "/v1/auth/verify-email",
        "post",
        Op {
            public: true,
            body: Some(json_body(obj(json!({"token": s()}), &["token"]))),
            desc: Some("The token comes from the link mailed by `POST /v1/me/verify-email` or at sign-up."),
            ..op("Verify an email address", "auth", vec![("200", resp("Verified", r("Empty")))])
        },
    );
    add(
        "/v1/auth/password-reset",
        "post",
        Op {
            public: true,
            body: Some(json_body(obj(json!({"email": s()}), &["email"]))),
            desc: Some(
                "Mails a reset link valid for an hour if an account has this email. The response does not indicate \
                 whether an account exists. Returns 404 if the server cannot send mail.",
            ),
            ..op("Request a password reset", "auth", vec![("200", resp("Requested", r("Empty")))])
        },
    );
    add(
        "/v1/auth/password-reset/confirm",
        "post",
        Op {
            public: true,
            body: Some(json_body(obj(
                json!({"token": s(), "password": {"type": "string", "minLength": 8}}),
                &["token", "password"],
            ))),
            desc: Some(
                "Sets the password, verifies the email address, and revokes all existing sessions. Repeating the request with the same password succeeds idempotently; any other password is rejected.",
            ),
            ..op("Reset a password", "auth", vec![("200", resp("A new session", r("Session")))])
        },
    );
    add("/v1/me", "get", op("The authenticated principal", "auth", vec![("200", resp("Me", r("Me")))]));
    add(
        "/v1/me",
        "delete",
        Op {
            body: Some(json!({"required": false, "content": {"application/json": {"schema":
                obj(json!({"password": s()}), &[])}}})),
            desc: Some(
                "Deletes the account along with its sessions, API keys, memberships, and databases (purged after a grace \
                 period). Password-authenticated accounts must confirm their password. Rejected for the last administrator \
                 or the sole owner of an organization. Requires a session.",
            ),
            ..op("Delete your account", "auth", vec![("200", resp("Deleted", r("Empty")))])
        },
    );
    add(
        "/v1/me/password",
        "post",
        Op {
            body: Some(json_body(r("PasswordChange"))),
            desc: Some("Revokes all other sessions. Requires a session."),
            ..op("Change your password", "auth", vec![("200", resp("Changed", r("Empty")))])
        },
    );
    add(
        "/v1/me/verify-email",
        "post",
        Op {
            desc: Some("Mails a verification link (valid 7 days). 404 if the server cannot send mail."),
            ..op("Send an email verification link", "auth", vec![("200", resp("Sent", r("Empty")))])
        },
    );
    add(
        "/v1/me/sessions",
        "get",
        Op {
            params: paging(),
            desc: Some("Requires a session."),
            ..op("List your sessions", "auth", vec![("200", resp("Sessions", page_of("sessions", r("SessionInfo"))))])
        },
    );
    add(
        "/v1/me/sessions",
        "delete",
        Op {
            desc: Some("Requires a session."),
            ..op(
                "Sign out everywhere else",
                "auth",
                vec![("200", resp("Revoked", obj(json!({"revoked": i()}), &["revoked"])))],
            )
        },
    );
    add(
        "/v1/me/sessions/{id}",
        "delete",
        Op {
            params: vec![path_param("id", "Session id")],
            desc: Some(
                "`current` indicates whether the revoked session was the caller's session (which clears its cookie). \
                Requires a session.",
            ),
            ..op("Revoke a session", "auth", vec![("200", resp("Revoked", obj(json!({"current": b()}), &["current"])))])
        },
    );
    add(
        "/v1/api-keys",
        "get",
        Op {
            params: paging(),
            ..op("List API keys", "auth", vec![("200", resp("Keys", page_of("api_keys", r("ApiKey"))))])
        },
    );
    add(
        "/v1/api-keys",
        "post",
        Op {
            body: Some(json_body(r("CreateApiKey"))),
            desc: Some("Keys may be scoped to a single database (`db`) and/or marked read-only. Requires a session."),
            ..op(
                "Create an API key",
                "auth",
                vec![(
                    "201",
                    resp(
                        "The full key is shown only once",
                        obj(json!({"key": s(), "api_key": r("ApiKey")}), &["key", "api_key"]),
                    ),
                )],
            )
        },
    );
    add(
        "/v1/api-keys/{id}",
        "delete",
        Op {
            params: vec![path_param("id", "Key id")],
            ..op("Revoke an API key", "auth", vec![("200", resp("Revoked", r("Empty")))])
        },
    );
    add(
        "/v1/admin/users",
        "get",
        Op {
            params: paging(),
            ..op("List users (admin)", "admin", vec![("200", resp("Users", page_of("users", r("UserPublic"))))])
        },
    );
    add(
        "/v1/admin/users/{id}/disable",
        "post",
        Op {
            body: Some(json_body(obj(json!({"disabled": b()}), &[]))),
            params: vec![path_param("id", "User id")],
            ..op("Disable or re-enable a user (admin)", "admin", vec![("200", resp("Done", r("Empty")))])
        },
    );
    add(
        "/v1/admin/metrics",
        "get",
        Op {
            desc: Some(
                "Returns latency histograms for object-store operations, commit batches, HTTP routes, and \
                 scheduler lag, along with cumulative process CPU time and host steal time.",
            ),
            ..op("Read node latency metrics (admin)", "admin", vec![("200", resp("Metrics", r("Metrics")))])
        },
    );
    add(
        "/v1/admin/metrics",
        "delete",
        op("Read and reset node latency metrics (admin)", "admin", vec![("200", resp("Metrics", r("Metrics")))]),
    );
    add(
        "/v1/admin/nodes",
        "get",
        op(
            "Cluster membership (admin)",
            "admin",
            vec![(
                "200",
                resp("Nodes", obj(json!({"nodes": arr(r("Node")), "this_node": s()}), &["nodes", "this_node"])),
            )],
        ),
    );
    add(
        "/v1/orgs",
        "get",
        Op {
            params: paging(),
            ..op("List your organizations", "orgs", vec![("200", resp("Organizations", page_of("orgs", r("Org"))))])
        },
    );
    add(
        "/v1/orgs",
        "post",
        Op {
            body: Some(json_body(obj(
                json!({"name": {"type": "string",
                    "pattern": format!("^[a-z0-9][a-z0-9_-]{{0,{}}}$", crate::meta::ORG_NAME_MAX - 1)}}),
                &["name"],
            ))),
            desc: Some("You become its owner. Requires a session."),
            ..op("Create an organization", "orgs", vec![("201", resp("Created", r("Org")))])
        },
    );
    add(
        "/v1/orgs/{org}",
        "get",
        Op { params: vec![org()], ..op("An organization and your role", "orgs", vec![("200", resp("Org", r("Org")))]) },
    );
    add(
        "/v1/orgs/{org}",
        "delete",
        Op {
            params: vec![org()],
            desc: Some("Requires the owner role and an organization with zero databases."),
            ..op("Delete an organization", "orgs", vec![("200", resp("Deleted", r("Empty")))])
        },
    );
    let mut member_list = vec![org()];
    member_list.extend(paging());
    add(
        "/v1/orgs/{org}/members",
        "get",
        Op {
            params: member_list,
            ..op("List members", "orgs", vec![("200", resp("Members", page_of("members", r("Member"))))])
        },
    );
    add(
        "/v1/orgs/{org}/members",
        "post",
        Op {
            params: vec![org()],
            body: Some(json_body(obj(json!({"email": s(), "role": r("Role")}), &["email", "role"]))),
            desc: Some("Admins add readers, writers and admins; owners also add owners. The account must exist."),
            ..op("Add a member", "orgs", vec![("201", resp("Added", r("Member")))])
        },
    );
    let member = || vec![org(), path_param("user", "The member's user id")];
    add(
        "/v1/orgs/{org}/members/{user}",
        "post",
        Op {
            params: member(),
            body: Some(json_body(obj(json!({"role": r("Role")}), &["role"]))),
            desc: Some("Changes that involve the owner role require an owner. The last owner cannot be demoted."),
            ..op("Change a member's role", "orgs", vec![("200", resp("Changed", r("Member")))])
        },
    );
    add(
        "/v1/orgs/{org}/members/{user}",
        "delete",
        Op {
            params: member(),
            desc: Some(
                "Admins remove non-owners, owners remove any member, and members can remove themselves. The last \
                owner cannot leave.",
            ),
            ..op("Remove a member", "orgs", vec![("200", resp("Removed", r("Empty")))])
        },
    );
    let mut list_params = vec![query("org", s(), "An organization you belong to (default: your own databases)")];
    list_params.extend(paging());
    add(
        "/v1/dbs",
        "get",
        Op {
            params: list_params,
            desc: Some(
                "An API key scoped to a single database lists only that database and cannot list organization databases.",
            ),
            ..op(
                "List your or an organization's databases",
                "databases",
                vec![("200", resp("Databases", page_of("databases", r("DbSummary"))))],
            )
        },
    );
    add(
        "/v1/dbs",
        "post",
        Op {
            body: Some(json_body(obj(
                json!({"name": {"type": "string", "pattern": "^[a-z0-9][a-z0-9_-]{0,62}$"}, "org": s()}),
                &["name"],
            ))),
            desc: Some(
                "With `org`, the database belongs to that organization (you must be one of its admins); address \
                 it as `org.name`.",
            ),
            ..op("Create a database", "databases", vec![("201", resp("Created", r("DbSummary")))])
        },
    );
    add(
        "/v1/dbs/{db}",
        "get",
        Op {
            params: vec![db()],
            ..op("Database info and stats", "databases", vec![("200", resp("Info", r("DbInfo")))])
        },
    );
    add(
        "/v1/dbs/{db}",
        "delete",
        Op {
            params: vec![db()],
            desc: Some(
                "Immediately invisible everywhere; storage is purged after a grace period. The name can be reused at \
                 once. In an organization, admins and owners may delete databases.",
            ),
            ..op("Delete a database", "databases", vec![("200", resp("Deleted", r("Empty")))])
        },
    );
    let mut scan_params = vec![
        db(),
        query("prefix", s(), "Key prefix"),
        query("start", s(), "Inclusive start"),
        query("end", s(), "Exclusive end"),
        query("limit", i(), "1..=1000 (default 100)"),
        query("reverse", b(), "Descending order"),
        query("keys_only", b(), "Omit values"),
        query("cursor", s(), "From a previous next_cursor"),
        query("encoding", enum_of(&["utf8", "base64"]), "Key/value encoding"),
    ];
    scan_params.extend(consistency());
    add(
        "/v1/dbs/{db}/kv",
        "get",
        Op { params: scan_params, ..op("Scan keys", "kv", vec![("200", resp("A page of items", r("ScanResult")))]) },
    );
    let key = || path_param("key", "The key (percent-encoded bytes; `/` must be encoded as %2F)");
    let mut get_params = vec![db(), key(), header("Range", "bytes=a-b")];
    get_params.extend(consistency());
    add(
        "/v1/dbs/{db}/kv/{key}",
        "get",
        Op {
            params: get_params,
            ..op(
                "Read a value as raw bytes",
                "kv",
                vec![
                    (
                        "200",
                        json!({"description": "The value", "content": octet(), "headers": {
            "ETag": {"schema": s(), "description": "\"<version>\""}, "x-baserunna-version": {"schema": i()},
            "x-baserunna-expires-at": {"schema": i(), "description": "Expiration timestamp (cluster time in ms), if set"}}}),
                    ),
                    ("206", bytes_resp("Partial content (Range)")),
                ],
            )
        },
    );
    add(
        "/v1/dbs/{db}/kv/{key}",
        "put",
        Op {
            body: Some(octet_body()),
            params: vec![
                db(),
                key(),
                header("If-Match", "\"<version>\": write only if unchanged"),
                header("If-None-Match", "*: create only"),
                query("ttl_ms", ttl(), "Expire the value after this duration once the write is committed"),
            ],
            desc: Some("Large bodies are streamed into an out-of-band blob. Content-Type is preserved for blobs."),
            ..op("Write a value from raw bytes", "kv", vec![("200", resp("Written", r("Version")))])
        },
    );
    add(
        "/v1/dbs/{db}/kv/{key}",
        "delete",
        Op {
            params: vec![db(), key(), header("If-Match", "\"<version>\"")],
            ..op("Delete a key", "kv", vec![("200", resp("Deleted", r("DeleteResult")))])
        },
    );
    add(
        "/v1/dbs/{db}/blobs",
        "post",
        Op {
            body: Some(octet_body()),
            params: vec![db()],
            ..op(
                "Stage a blob (streamed upload)",
                "kv",
                vec![(
                    "201",
                    resp("Staged; bind it with a put op (blob_id) or a blob column within an hour", r("BlobMeta")),
                )],
            )
        },
    );
    add(
        "/v1/dbs/{db}/blobs/{blob_id}",
        "get",
        Op {
            params: vec![db(), path_param("blob_id", "Blob id")],
            ..op(
                "Download a blob by id",
                "kv",
                vec![("200", bytes_resp("Content")), ("206", bytes_resp("Partial content"))],
            )
        },
    );
    add(
        "/v1/dbs/{db}/txn",
        "post",
        Op {
            body: Some(json_body(r("TxnRequest"))),
            params: vec![db()],
            desc: Some(
                "Operations execute in order with read-your-writes, atomically. Serializable by default; conflicts are \
            retried server-side. With `idempotency_key`, retries are never applied twice (a replay returns \
            `replayed: true` and null results).",
            ),
            ..op(
                "Run a one-shot transaction",
                "transactions",
                vec![("200", resp("Results (committed if it wrote)", r("TxnResult")))],
            )
        },
    );
    add(
        "/v1/dbs/{db}/transactions",
        "post",
        Op {
            body: Some(json_body(r("BeginRequest"))),
            params: vec![db()],
            ..op("Begin an interactive transaction", "transactions", vec![("201", resp("Begun", r("Begin")))])
        },
    );
    add(
        "/v1/dbs/{db}/transactions/{id}",
        "delete",
        Op { params: vec![db(), txn_id()], ..op("Abort", "transactions", vec![("200", resp("Aborted", r("Empty")))]) },
    );
    add(
        "/v1/dbs/{db}/transactions/{id}/ops",
        "post",
        Op {
            body: Some(json_body(r("OpsRequest"))),
            params: vec![db(), txn_id()],
            desc: Some("Any error aborts the transaction."),
            ..op("Run ops in a transaction", "transactions", vec![("200", resp("Results", r("OpsResult")))])
        },
    );
    add(
        "/v1/dbs/{db}/transactions/{id}/commit",
        "post",
        Op {
            body: Some(json_body(r("CommitRequest"))),
            params: vec![db(), txn_id()],
            desc: Some("If any operation in `ops` fails, the transaction is aborted."),
            ..op("Commit", "transactions", vec![("200", resp("Committed", r("Committed")))])
        },
    );
    add(
        "/v1/dbs/{db}/commits/{id}",
        "get",
        Op {
            params: vec![db(), path_param("id", "commit_id (16 hex characters) or key:<idempotency key>")],
            desc: Some(
                "Fences in-flight attempts first, so the status is definitive for commits whose request has finished \
            (wait at least the commit deadline, about 15 s, after an ambiguous failure). Commit records are \
            retained for 10 minutes.",
            ),
            ..op(
                "Resolve an ambiguous commit",
                "transactions",
                vec![("200", resp("Definitive status", r("CommitStatus")))],
            )
        },
    );
    let mut lt = vec![db()];
    lt.extend(consistency());
    add(
        "/v1/dbs/{db}/tables",
        "get",
        Op {
            params: lt,
            ..op(
                "List tables",
                "tables",
                vec![(
                    "200",
                    resp(
                        "Tables",
                        obj(json!({"tables": arr(r("TableSchema")), "version": i()}), &["tables", "version"]),
                    ),
                )],
            )
        },
    );
    add(
        "/v1/dbs/{db}/tables",
        "post",
        Op {
            body: Some(json_body(r("CreateTable"))),
            params: vec![db()],
            ..op("Create a table", "tables", vec![("201", resp("Created", r("TableSchema")))])
        },
    );
    let mut gt = vec![db(), table()];
    gt.extend(consistency());
    add(
        "/v1/dbs/{db}/tables/{t}",
        "get",
        Op { params: gt, ..op("Describe a table", "tables", vec![("200", resp("Schema", r("TableSchema")))]) },
    );
    add(
        "/v1/dbs/{db}/tables/{t}",
        "delete",
        Op {
            params: vec![db(), table()],
            ..op("Drop a table", "tables", vec![("200", resp("Dropped", r("Version")))])
        },
    );
    add(
        "/v1/dbs/{db}/tables/{t}/columns",
        "post",
        Op {
            body: Some(json_body(r("Column"))),
            params: vec![db(), table()],
            ..op("Add a column", "tables", vec![("200", resp("Schema", r("TableSchema")))])
        },
    );
    add(
        "/v1/dbs/{db}/tables/{t}/columns/{c}",
        "delete",
        Op {
            params: vec![db(), table(), path_param("c", "Column")],
            ..op("Drop a column", "tables", vec![("200", resp("Schema", r("TableSchema")))])
        },
    );
    add(
        "/v1/dbs/{db}/tables/{t}/indexes",
        "post",
        Op {
            body: Some(json_body(r("Index"))),
            params: vec![db(), table()],
            desc: Some(
                "Adds the index in the `building` state, backfills it from the table's rows in batches of \
                 separate transactions while writes continue, and returns once it is `ready`. If the request \
                 fails midway, the index stays `building`; repeating the request resumes the backfill. A unique \
                 index over duplicate values is removed again and the request fails with `409 already_exists`.",
            ),
            ..op("Add an index (backfilled online)", "tables", vec![("200", resp("Schema", r("TableSchema")))])
        },
    );
    add(
        "/v1/dbs/{db}/tables/{t}/indexes/{i}",
        "delete",
        Op {
            params: vec![db(), table(), path_param("i", "Index")],
            ..op("Drop an index", "tables", vec![("200", resp("Schema", r("TableSchema")))])
        },
    );
    add(
        "/v1/dbs/{db}/tables/{t}/rows",
        "post",
        Op {
            body: Some(json_body(r("InsertRows"))),
            params: vec![db(), table()],
            ..op(
                "Insert (or upsert) rows",
                "tables",
                vec![("201", resp("Inserted", r("RowCount"))), ("200", resp("Upserted", r("RowCount")))],
            )
        },
    );
    add(
        "/v1/dbs/{db}/tables/{t}/rows/get",
        "post",
        Op {
            body: Some(json_body(r("PkRequest"))),
            params: vec![db(), table()],
            ..op("Get a row by primary key", "tables", vec![("200", resp("Row", r("RowResult")))])
        },
    );
    add(
        "/v1/dbs/{db}/tables/{t}/rows/update",
        "post",
        Op {
            body: Some(json_body(r("UpdateRows"))),
            params: vec![db(), table()],
            ..op("Update columns of one or more rows", "tables", vec![("200", resp("Updated", r("FoundOrCount")))])
        },
    );
    add(
        "/v1/dbs/{db}/tables/{t}/rows/delete",
        "post",
        Op {
            body: Some(json_body(r("DeleteRows"))),
            params: vec![db(), table()],
            ..op("Delete one or more rows", "tables", vec![("200", resp("Deleted", r("FoundOrCount")))])
        },
    );
    add(
        "/v1/dbs/{db}/tables/{t}/rows/query",
        "post",
        Op {
            body: Some(json_body(r("Query"))),
            params: vec![db(), table()],
            ..op("Query rows", "tables", vec![("200", resp("Rows", r("QueryResult")))])
        },
    );
    let feed = |extra: Vec<Value>| {
        let mut v = vec![
            query_required("after", s(), "Report changes after this version or `next_cursor`"),
            query("limit", i(), "1..=1000 (default 100)"),
            query("wait_ms", i(), "0..=20000: wait up to this duration for a change"),
        ];
        v.extend(extra);
        v.extend(consistency());
        v
    };
    let feed_desc = "Returns the latest state as of the feed's snapshot for each key modified after `after` \
        (intermediate states are omitted), ordered by version. Continue from `next_cursor` while `more` is true. \
        If `wait_ms` is set, waits up to `wait_ms` for new changes. Tombstones are \
        retained for at least the transaction lifetime (5 minutes); cursors older than the tombstone horizon \
        fail with `410 cursor_expired`.";
    let mut kv_feed = vec![db()];
    kv_feed.extend(feed(vec![
        query("prefix", s(), "Key prefix"),
        query("start", s(), "Inclusive start"),
        query("end", s(), "Exclusive end"),
        query("keys_only", b(), "Omit values"),
        query("encoding", enum_of(&["utf8", "base64"]), "Key/value encoding"),
    ]));
    add(
        "/v1/dbs/{db}/changes",
        "get",
        Op {
            params: kv_feed,
            desc: Some(feed_desc),
            ..op("Follow changes to keys", "kv", vec![("200", resp("A page of changes", r("KvChanges")))])
        },
    );
    let mut row_feed = vec![db(), table()];
    row_feed.extend(feed(vec![]));
    add(
        "/v1/dbs/{db}/tables/{t}/changes",
        "get",
        Op {
            params: row_feed,
            desc: Some(feed_desc),
            ..op("Follow changes to rows", "tables", vec![("200", resp("A page of changes", r("RowChanges")))])
        },
    );
    let stream = || path_param("s", "Stream name.");
    let mut ls = vec![db(), query("prefix", s(), "Names starting with this")];
    ls.extend(paging());
    ls.extend(consistency());
    add(
        "/v1/dbs/{db}/streams",
        "get",
        Op {
            params: ls,
            ..op(
                "List streams",
                "streams",
                vec![(
                    "200",
                    resp(
                        "Streams",
                        obj(
                            json!({"streams": arr(r("Stream")), "next_cursor": nullable("string"), "version": i()}),
                            &["streams", "next_cursor", "version"],
                        ),
                    ),
                )],
            )
        },
    );
    add(
        "/v1/dbs/{db}/streams",
        "post",
        Op {
            body: Some(json_body(r("CreateStream"))),
            params: vec![db()],
            ..op("Create a stream", "streams", vec![("201", resp("Created", r("Stream")))])
        },
    );
    let mut gs = vec![db(), stream()];
    gs.extend(consistency());
    add(
        "/v1/dbs/{db}/streams/{s}",
        "get",
        Op {
            params: gs.clone(),
            ..op("Describe a stream and its consumers", "streams", vec![("200", resp("Stream", r("Stream")))])
        },
    );
    add(
        "/v1/dbs/{db}/streams/{s}",
        "patch",
        Op {
            body: Some(json_body(r("StreamConfig"))),
            params: vec![db(), stream()],
            ..op("Change a stream's retention", "streams", vec![("200", resp("Stream", r("Stream")))])
        },
    );
    add(
        "/v1/dbs/{db}/streams/{s}",
        "delete",
        Op {
            params: vec![db(), stream()],
            ..op("Delete a stream and its records", "streams", vec![("200", resp("Deleted", r("Version")))])
        },
    );
    add(
        "/v1/dbs/{db}/streams/{s}/records",
        "post",
        Op {
            body: Some(json!({"required": true, "content": {
                "application/json": {"schema": r("AppendRequest")},
                "application/x-ndjson": {"schema": {"type": "string", "description": "A JSON record per line"}},
                "text/plain": {"schema": {"type": "string", "description": "A text record per line"}},
            }})),
            params: vec![
                db(),
                stream(),
                query("expect_seq", i(), "Append only if the stream's next seq is this (else 412)"),
                query("create", b(), "Create the stream if it does not exist"),
                query("encoding", enum_of(&["utf8", "base64"]), "Encoding of JSON `value`s"),
                header("Idempotency-Key", "Retries with the same key are applied once (within 10 minutes)"),
            ],
            desc: Some(
                "Appends records atomically at the stream's next sequence numbers; concurrent appends do not conflict. \
                 Empty lines in NDJSON and text bodies and any leading UTF-8 BOM are skipped; an empty body \
                 appends zero records (`count` 0). Send NDJSON and text requests with their matching `Content-Type`.",
            ),
            ..op("Append records", "streams", vec![("200", resp("Appended", r("Appended")))])
        },
    );
    let mut rd = vec![
        db(),
        stream(),
        query("from_seq", i(), "Start at this seq"),
        query("from_time", i(), "Start at the first record at or after this time (ms)"),
        query("tail", i(), "Start at the last N records"),
        query("consumer", s(), "Start at this consumer's cursor"),
        query("until_seq", i(), "Stop before this seq"),
        query("until_time", i(), "Stop before the first record at or after this time"),
        query("reverse", b(), "Newest first"),
        query("limit", i(), "1..=1000 records (default 100)"),
        query("max_bytes", i(), "Stop after about this many body bytes (default 4 MiB, at most 16 MiB)"),
        query(
            "wait_ms",
            i(),
            "Wait this long for an append if the read is caught up: 0..=20000, or up to 300000 for events",
        ),
        query("format", enum_of(&["json", "ndjson", "sse"]), "Response format (default: from Accept, else json)"),
        query("encoding", enum_of(&["utf8", "base64"]), "Encoding of `value`s"),
        header("Last-Event-ID", "Server-sent events: resume after this seq"),
    ];
    rd.extend(consistency());
    add(
        "/v1/dbs/{db}/streams/{s}/records",
        "get",
        Op {
            params: rd,
            desc: Some(
                "Returns a page of records as JSON, one record per line as NDJSON (with `next_seq`, `first_seq`, \
                 `tail_seq`, and `done` in `x-baserunna-*` headers), or a server-sent event stream (forward reads \
                 only) emitting `id: <seq>` and `data: <record>` for up to `wait_ms` with a keepalive comment \
                 every 15 s. SSE streams end at `limit`, `until_seq`, `until_time`, `wait_ms`, or node shutdown, \
                 and resume after `Last-Event-ID`.",
            ),
            ..op(
                "Read records",
                "streams",
                vec![(
                    "200",
                    json!({"description": "Records", "content": {
                        "application/json": {"schema": r("StreamRecords")},
                        "application/x-ndjson": {"schema": {"type": "string"}},
                        "text/event-stream": {"schema": {"type": "string"}},
                    }}),
                )],
            )
        },
    );
    add(
        "/v1/dbs/{db}/streams/{s}/records/query",
        "post",
        Op {
            body: Some(json_body(r("StreamRead"))),
            params: vec![db(), stream()],
            desc: Some("Reads records matching a filter; continue from `next_seq` until `done` is true."),
            ..op("Query records", "streams", vec![("200", resp("Records", r("StreamRecords")))])
        },
    );
    add(
        "/v1/dbs/{db}/streams/{s}/trim",
        "post",
        Op {
            body: Some(json_body(r("Trim"))),
            params: vec![db(), stream()],
            desc: Some(
                "Removes the records before `before_seq`, or before the first record at or after `before_time`.",
            ),
            ..op("Trim a stream", "streams", vec![("200", resp("Trimmed", r("Version")))])
        },
    );
    let mut lc = vec![db(), stream()];
    lc.extend(paging());
    lc.extend(consistency());
    add(
        "/v1/dbs/{db}/streams/{s}/consumers",
        "get",
        Op {
            params: lc,
            ..op(
                "List a stream's consumers",
                "streams",
                vec![(
                    "200",
                    resp(
                        "Consumers",
                        obj(
                            json!({"consumers": arr(r("Consumer")), "next_cursor": nullable("string"), "version": i()}),
                            &["consumers", "next_cursor", "version"],
                        ),
                    ),
                )],
            )
        },
    );
    let consumer = || path_param("c", "Consumer name.");
    let mut gc = vec![db(), stream(), consumer()];
    gc.extend(consistency());
    add(
        "/v1/dbs/{db}/streams/{s}/consumers/{c}",
        "get",
        Op { params: gc, ..op("Get a consumer's cursor", "streams", vec![("200", resp("Consumer", r("Consumer")))]) },
    );
    add(
        "/v1/dbs/{db}/streams/{s}/consumers/{c}",
        "put",
        Op {
            body: Some(json_body(r("SetConsumer"))),
            params: vec![db(), stream(), consumer()],
            ..op("Set a consumer's cursor", "streams", vec![("200", resp("Set", r("Version")))])
        },
    );
    add(
        "/v1/dbs/{db}/streams/{s}/consumers/{c}",
        "delete",
        Op {
            params: vec![db(), stream(), consumer()],
            ..op("Delete a consumer", "streams", vec![("200", resp("Deleted", r("DeleteResult")))])
        },
    );
    Value::Object(p)
}

/// Returns the OpenAPI specification as a JSON value.
pub fn document() -> &'static Value {
    static DOC: OnceLock<Value> = OnceLock::new();
    DOC.get_or_init(|| {
        let tags: Vec<Value> = ["auth", "orgs", "databases", "kv", "transactions", "tables", "streams", "admin", "ops"]
            .iter()
            .map(|n| json!({"name": n}))
            .collect();
        json!({
            "openapi": "3.1.0",
            "info": {
                "title": "Baserunna",
                "version": env!("CARGO_PKG_VERSION"),
                "description": "Transactional key-value, table, and stream databases on object storage. Authenticate with \
                    `Authorization: Bearer <session token or API key>` (or the `brs_session` cookie). Errors use the \
                    Error schema with a stable `code`; `retryable` errors may be retried (use idempotency keys for \
                    writes). Requests over a rate limit return 429 `rate_limited` with `Retry-After` and are not \
                    applied; writes that would exceed a database's size quota return 507 `quota_exceeded`. Every response \
                    includes an `x-request-id` header containing the client-supplied value (1-64 characters of \
                    `[A-Za-z0-9._:-]`) or a generated ID recorded in server logs.",
            },
            "servers": [{"url": "/"}],
            "security": [{"bearer": []}],
            "tags": tags,
            "paths": paths(),
            "components": {
                "securitySchemes": {"bearer": {"type": "http", "scheme": "bearer"}},
                "responses": {"Error": resp("Error", r("Error"))},
                "schemas": schemas(),
            },
        })
    })
}

/// Returns the OpenAPI specification as pretty-printed JSON.
pub fn document_json() -> &'static str {
    static TEXT: OnceLock<String> = OnceLock::new();
    TEXT.get_or_init(|| {
        let mut t = serde_json::to_string_pretty(document()).unwrap_or_default();
        t.push('\n');
        t
    })
}
