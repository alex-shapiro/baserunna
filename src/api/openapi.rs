//! The OpenAPI 3.1 document for the HTTP API, served at
//! `GET /v1/openapi.json` and checked in as `openapi.json`.

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

fn header(name: &str, desc: &str) -> Value {
    json!({"name": name, "in": "header", "required": false, "schema": s(), "description": desc})
}

fn enum_of(values: &[&str]) -> Value {
    json!({"type": "string", "enum": values})
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
    path_param("db", "Database name (or `id:<db_id>` for administrators).")
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
                    "unavailable", "internal"]),
                "message": s(),
                "retryable": b(),
                "details": obj(json!({
                    "op_index": i(),
                    "lease_holder": s(),
                    "outcome": enum_of(&["unknown", "not_applied"]),
                }), &[]),
            }), &["code", "message", "retryable"]),
        }), &["error"]),
        "UserPublic": obj(json!({
            "id": s(), "email": s(), "is_admin": b(), "disabled": b(), "has_password": b(),
            "google_linked": b(), "created_at": i(),
        }), &["id", "email", "is_admin"]),
        "Session": obj(json!({"token": s(), "user": r("UserPublic")}), &["token", "user"]),
        "Credentials": obj(json!({"email": s(), "password": {"type": "string", "minLength": 8}}), &["email", "password"]),
        "Me": obj(json!({
            "user": r("UserPublic"),
            "credential": obj(json!({
                "type": enum_of(&["session", "api_key"]), "id": s(), "db": nullable("string"), "read_only": b(),
            }), &["type"]),
        }), &["user", "credential"]),
        "ApiKey": obj(json!({"id": s(), "name": s(), "db": nullable("string"), "read_only": b(), "created_at": i()}),
            &["id", "name", "db", "read_only", "created_at"]),
        "CreateApiKey": obj(json!({"name": s(), "db": nullable("string"), "read_only": b()}), &["name"]),
        "Node": obj(json!({"id": s(), "addr": s(), "started_at": i(), "heartbeat_at": i(), "live": b()}),
            &["id", "addr", "started_at", "heartbeat_at", "live"]),
        "DbSummary": obj(json!({"id": s(), "name": s(), "created_at": i()}), &["id", "name", "created_at"]),
        "DbInfo": obj(json!({
            "id": s(), "name": s(), "created_at": i(),
            "stats": obj(json!({
                "version": i(), "segments": nullable("integer"), "segment_bytes": nullable("integer"),
                "tail_bytes": nullable("integer"), "lease_holder": nullable("string"),
            }), &["version"]),
        }), &["id", "name", "created_at", "stats"]),
        "BlobMeta": obj_desc(json!({"blob_id": s(), "size": i(), "sha256": s(), "content_type": s()}),
            &["blob_id", "size", "sha256", "content_type"], "An out-of-band blob."),
        "KvItem": obj_desc(json!({
            "key": s(), "key_b64": s(), "value": s(), "value_b64": s(), "blob": r("BlobMeta"),
            "version": nullable("integer"),
        }), &["version"], "A key and its value. Keys/values that are not valid UTF-8 in utf8 mode are \
            returned in `key_b64` / `value_b64`. Values stored as blobs larger than the inline JSON limit \
            only carry `blob`."),
        "ScanResult": obj(json!({"items": arr(r("KvItem")), "next_cursor": nullable("string"), "version": i()}),
            &["items", "next_cursor"]),
        "OpResult": obj_desc(json!({
            "found": b(), "value": {"type": ["string", "integer"]}, "value_b64": s(),
            "blob": r("BlobMeta"), "version": nullable("integer"),
            "items": arr(r("KvItem")), "next_cursor": nullable("string"),
            "row": r("Row"), "rows": arr(r("Row")),
        }), &[], "Result of one op; the fields present depend on the op (see Op)."),
        "Op": {
            "oneOf": [
                obj(json!({"op": {"const": "get"}, "key": s()}), &["op", "key"]),
                obj(json!({"op": {"const": "scan"}, "prefix": s(), "start": s(), "end": s(),
                    "limit": {"type": "integer", "minimum": 1, "maximum": 1000},
                    "reverse": b(), "keys_only": b(), "cursor": s()}), &["op"]),
                obj(json!({"op": {"const": "put"}, "key": s(), "value": s(), "blob_id": s()}), &["op", "key"]),
                obj(json!({"op": {"const": "delete"}, "key": s()}), &["op", "key"]),
                obj(json!({"op": {"const": "delete_range"}, "prefix": s(), "start": s(), "end": s()}), &["op"]),
                obj(json!({"op": {"const": "check"}, "key": s(), "exists": b(), "version": i(), "value": s()}), &["op", "key"]),
                obj(json!({"op": {"const": "increment"}, "key": s(), "by": i()}), &["op", "key"]),
                obj(json!({"op": {"const": "get_row"}, "table": s(), "pk": any()}), &["op", "table", "pk"]),
                obj(json!({"op": {"const": "insert_row"}, "table": s(), "row": r("Row")}), &["op", "table", "row"]),
                obj(json!({"op": {"const": "upsert_row"}, "table": s(), "row": r("Row")}), &["op", "table", "row"]),
                obj(json!({"op": {"const": "update_row"}, "table": s(), "pk": any(), "set": r("Row")}),
                    &["op", "table", "pk", "set"]),
                obj(json!({"op": {"const": "delete_row"}, "table": s(), "pk": any()}), &["op", "table", "pk"]),
                {"type": "object", "properties": {"op": {"const": "query_rows"}, "table": s()},
                 "required": ["op", "table"], "allOf": [r("Query")], "additionalProperties": true},
            ],
            "description": "One operation. Keys and values are UTF-8 strings unless the request sets `encoding: base64`.",
        },
        "TxnRequest": obj(json!({
            "ops": arr(r("Op")),
            "encoding": enum_of(&["utf8", "base64"]),
            "isolation": enum_of(&["serializable", "snapshot"]),
            "idempotency_key": s(),
            "consistency": enum_of(&["strong", "snapshot"]),
            "max_staleness_ms": i(),
        }), &["ops"]),
        "TxnResult": obj(json!({
            "version": i(), "committed": b(), "replayed": b(), "commit_id": s(),
            "results": {"oneOf": [arr(r("OpResult")), {"type": "null"}]},
        }), &["version", "committed", "results"]),
        "BeginRequest": obj(json!({"isolation": enum_of(&["serializable", "snapshot"]),
            "encoding": enum_of(&["utf8", "base64"])}), &[]),
        "Begin": obj(json!({"txn_id": s(), "commit_id": s(), "read_version": i(), "expires_at": i(),
            "idle_timeout_ms": i()}), &["txn_id", "commit_id", "read_version", "expires_at", "idle_timeout_ms"]),
        "OpsRequest": obj(json!({"ops": arr(r("Op"))}), &["ops"]),
        "OpsResult": obj(json!({"results": arr(r("OpResult"))}), &["results"]),
        "Committed": obj(json!({"version": i(), "commit_id": s()}), &["version", "commit_id"]),
        "CommitStatus": obj(json!({"status": enum_of(&["committed", "not_committed"]), "version": i(),
            "commit_id": s()}), &["status", "commit_id"]),
        "Version": obj(json!({"version": i()}), &["version"]),
        "DeleteResult": obj(json!({"version": i(), "existed": b()}), &["version", "existed"]),
        "Empty": obj(json!({}), &[]),
        "ColumnType": enum_of(&["string", "int", "float", "bool", "bytes", "json", "blob"]),
        "Column": obj(json!({"name": s(), "type": r("ColumnType"), "nullable": b(), "default": any()}), &["name", "type"]),
        "Index": obj(json!({"name": s(), "columns": arr(s()), "unique": b()}), &["name", "columns"]),
        "CreateTable": obj(json!({"name": s(), "columns": arr(r("Column")), "primary_key": arr(s()),
            "indexes": arr(r("Index")), "if_not_exists": b()}), &["name", "columns", "primary_key"]),
        "TableSchema": obj(json!({"name": s(), "columns": arr(r("Column")), "primary_key": arr(s()),
            "indexes": arr(r("Index")), "version": i()}), &["name", "columns", "primary_key", "indexes"]),
        "Row": {"type": "object", "additionalProperties": true,
            "description": "Column name to value. `bytes` are base64; `blob` columns take a staged `blob_id` \
                on write and return BlobMeta on read."},
        "Filter": {"type": "object", "additionalProperties": true,
            "description": "`{\"and\": [...]}`, `{\"or\": [...]}`, `{\"not\": f}` or \
                `{\"col\": name, \"op\": eq|ne|lt|le|gt|ge|in|prefix|is_null, \"value\": v}`."},
        "Query": obj_desc(json!({
            "index": s(), "eq": arr(any()),
            "range": obj(json!({"gt": any(), "gte": any(), "lt": any(), "lte": any()}), &[]),
            "filter": r("Filter"),
            "limit": {"type": "integer", "minimum": 1, "maximum": 1000},
            "reverse": b(), "cursor": s(),
            "consistency": enum_of(&["strong", "snapshot"]),
            "max_staleness_ms": i(),
        }), &[], "Scan the primary key (default) or a secondary index: equality on a prefix of its columns \
            (`eq`), an optional range on the next column, a residual filter, and pagination via `cursor`."),
        "InsertRows": obj(json!({"row": r("Row"), "rows": arr(r("Row")), "upsert": b()}), &[]),
        "RowCount": obj(json!({"count": i(), "version": i()}), &["count", "version"]),
        "PkRequest": obj(json!({"pk": any(), "consistency": s(), "max_staleness_ms": i()}), &["pk"]),
        "UpdateRow": obj(json!({"pk": any(), "set": r("Row")}), &["pk", "set"]),
        "RowResult": obj(json!({"found": b(), "row": r("Row"), "version": i()}), &["found", "version"]),
        "Found": obj(json!({"found": b(), "version": i()}), &["found", "version"]),
        "QueryResult": obj(json!({"rows": arr(r("Row")), "next_cursor": nullable("string"), "version": i()}),
            &["rows", "next_cursor", "version"]),
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
            ..op(
                "Liveness and node status",
                "ops",
                vec![(
                    "200",
                    resp(
                        "OK",
                        json!({"type": "object", "properties": {"ok": b(), "node": {"type": "object"}}, "required": ["ok"]}),
                    ),
                )],
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
    add("/v1/me", "get", op("The authenticated principal", "auth", vec![("200", resp("Me", r("Me")))]));
    add(
        "/v1/api-keys",
        "get",
        op(
            "List API keys",
            "auth",
            vec![("200", resp("Keys", obj(json!({"api_keys": arr(r("ApiKey"))}), &["api_keys"])))],
        ),
    );
    add(
        "/v1/api-keys",
        "post",
        Op {
            body: Some(json_body(r("CreateApiKey"))),
            desc: Some("Keys may be limited to one database (`db`) and/or be read-only. Requires a session."),
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
        op(
            "List users (admin)",
            "admin",
            vec![("200", resp("Users", obj(json!({"users": arr(r("UserPublic"))}), &["users"])))],
        ),
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
        "/v1/dbs",
        "get",
        op(
            "List your databases",
            "databases",
            vec![("200", resp("Databases", obj(json!({"databases": arr(r("DbSummary"))}), &["databases"])))],
        ),
    );
    add(
        "/v1/dbs",
        "post",
        Op {
            body: Some(json_body(obj(
                json!({"name": {"type": "string", "pattern": "^[a-z0-9][a-z0-9_-]{0,62}$"}}),
                &["name"],
            ))),
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
                "Immediately invisible everywhere; storage is purged after a grace period. The name can be reused at once.",
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
            "ETag": {"schema": s(), "description": "\"<version>\""}, "x-baserunna-version": {"schema": i()}}}),
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
                "Ops execute in order with read-your-writes, atomically. Serializable by default; conflicts are \
            retried server-side. With `idempotency_key`, retries never apply twice (a replay returns \
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
            params: vec![db(), txn_id()],
            ..op("Commit", "transactions", vec![("200", resp("Committed", r("Committed")))])
        },
    );
    add(
        "/v1/dbs/{db}/commits/{id}",
        "get",
        Op {
            params: vec![db(), path_param("id", "commit_id (hex) or key:<idempotency key>")],
            desc: Some(
                "Fences in-flight attempts first, so the answer is final for commits whose request has ended \
            (wait at least the commit deadline, about 15 s, after an ambiguous failure).",
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
            ..op("Add an index (backfilled transactionally)", "tables", vec![("200", resp("Schema", r("TableSchema")))])
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
            body: Some(json_body(r("UpdateRow"))),
            params: vec![db(), table()],
            ..op("Update columns of a row", "tables", vec![("200", resp("Updated", r("Found")))])
        },
    );
    add(
        "/v1/dbs/{db}/tables/{t}/rows/delete",
        "post",
        Op {
            body: Some(json_body(r("PkRequest"))),
            params: vec![db(), table()],
            ..op("Delete a row", "tables", vec![("200", resp("Deleted", r("Found")))])
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
    Value::Object(p)
}

/// The OpenAPI document.
pub fn document() -> &'static Value {
    static DOC: OnceLock<Value> = OnceLock::new();
    DOC.get_or_init(|| {
        let tags: Vec<Value> = ["auth", "databases", "kv", "transactions", "tables", "admin", "ops"]
            .iter()
            .map(|n| json!({"name": n}))
            .collect();
        json!({
            "openapi": "3.1.0",
            "info": {
                "title": "Baserunna",
                "version": env!("CARGO_PKG_VERSION"),
                "description": "Transactional key-value and table databases on object storage. Authenticate with \
                    `Authorization: Bearer <session token or API key>` (or the `brs_session` cookie). Errors use the \
                    Error schema with a stable `code`; `retryable` errors may be retried (use idempotency keys for \
                    writes).",
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

/// The document as pretty-printed JSON (what `openapi.json` contains).
pub fn document_json() -> &'static str {
    static TEXT: OnceLock<String> = OnceLock::new();
    TEXT.get_or_init(|| {
        let mut t = serde_json::to_string_pretty(document()).unwrap_or_default();
        t.push('\n');
        t
    })
}
