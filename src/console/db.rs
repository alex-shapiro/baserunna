//! Database console pages.

use super::{
    Fields, Flash, Page, Pager, Params, Posted, Reply, Session, back, esc, field, format_bytes, multipart, page, stay,
    table, url_bytes, url_seg, when,
};
use crate::engine::DbStats;
use crate::meta::{DbRef, Role};
use crate::node::Node;
use crate::table::schema::{ColType, IndexState};
use crate::util::b64;
use axum::Router;
use axum::body::Bytes;
use axum::extract::Path;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use http::{HeaderMap, HeaderValue, StatusCode};
use serde::Deserialize;
use serde_json::{Map, Value as Json, json};
use std::sync::Arc;

pub fn routes() -> Router<Arc<Node>> {
    Router::new()
        .route("/console/dbs", get(dbs_page).post(create_db))
        .route("/console/dbs/{db}", get(db_page))
        .route("/console/dbs/{db}/delete", post(delete_db))
        .route("/console/dbs/{db}/kv", post(put_kv))
        .route("/console/dbs/{db}/kv/delete", post(delete_kv))
        .route("/console/dbs/{db}/kv/upload", post(upload))
        .route("/console/dbs/{db}/kv/download", get(download_kv))
        .route("/console/dbs/{db}/blobs/{blob}", get(download_blob))
        .route("/console/dbs/{db}/txn", post(run_txn))
        .route("/console/dbs/{db}/tables", post(create_table))
        .route("/console/dbs/{db}/tables/{t}", get(table_page))
        .route("/console/dbs/{db}/tables/{t}/drop", post(drop_table))
        .route("/console/dbs/{db}/tables/{t}/columns", post(add_column))
        .route("/console/dbs/{db}/tables/{t}/columns/{c}/drop", post(drop_column))
        .route("/console/dbs/{db}/tables/{t}/indexes", post(add_index))
        .route("/console/dbs/{db}/tables/{t}/indexes/{i}/drop", post(drop_index))
}

pub(super) fn db_path(db: &str) -> String {
    format!("/console/dbs/{}", url_seg(db))
}

pub(super) fn api_db(db: &str) -> String {
    format!("/v1/dbs/{}", url_seg(db))
}

fn table_path(db: &str, t: &str) -> String {
    format!("{}/tables/{}", db_path(db), url_seg(t))
}

fn api_table(db: &str, t: &str) -> String {
    format!("{}/tables/{}", api_db(db), url_seg(t))
}

#[derive(Deserialize)]
struct DbInfo {
    id: String,
    org: Option<String>,
    role: Role,
    stats: DbStats,
}

#[derive(Deserialize)]
pub(super) struct DbSummary {
    id: String,
    name: String,
    created_at: u64,
}

#[derive(Deserialize)]
pub(super) struct DbList {
    pub databases: Vec<DbSummary>,
    pub next_cursor: Option<String>,
}

#[derive(Deserialize)]
struct BlobMeta {
    blob_id: String,
    size: u64,
    content_type: String,
}

#[derive(Deserialize)]
struct KvItem {
    key: Option<String>,
    key_b64: Option<String>,
    value: Option<String>,
    blob: Option<BlobMeta>,
    version: Option<u64>,
}

#[derive(Deserialize)]
struct Scan {
    items: Vec<KvItem>,
    next_cursor: Option<String>,
}

#[derive(Deserialize)]
struct Column {
    name: String,
    #[serde(rename = "type")]
    ty: ColType,
    nullable: bool,
    default: Option<Json>,
}

#[derive(Deserialize)]
struct Index {
    name: String,
    columns: Vec<String>,
    unique: bool,
    state: IndexState,
}

#[derive(Deserialize)]
struct Schema {
    name: String,
    columns: Vec<Column>,
    primary_key: Vec<String>,
    indexes: Vec<Index>,
}

#[derive(Deserialize)]
struct Tables {
    tables: Vec<Schema>,
}

#[derive(Deserialize)]
struct Rows {
    rows: Vec<Map<String, Json>>,
    next_cursor: Option<String>,
}

fn cell(db: &str, v: &Json) -> String {
    match v {
        Json::Null => r#"<span class="muted">null</span>"#.into(),
        Json::String(s) => esc(s),
        v => match BlobMeta::deserialize(v) {
            Ok(b) => blob_cell(db, &b),
            Err(_) => format!("<code>{}</code>", esc(&v.to_string())),
        },
    }
}

fn blob_cell(db: &str, b: &BlobMeta) -> String {
    format!(
        r#"<a href="{}/blobs/{}">blob</a> {} · {}"#,
        db_path(db),
        url_seg(&b.blob_id),
        esc(&b.content_type),
        format_bytes(b.size)
    )
}

pub(super) fn db_rows(dbs: &[DbSummary], org: Option<&str>) -> String {
    let rows: Vec<String> = dbs
        .iter()
        .map(|d| {
            let r = DbRef::format(org, &d.name);
            format!(
                r#"<tr><td><a href="{p}">{r}</a></td><td><code>{id}</code></td><td>{c}</td></tr>"#,
                p = db_path(&r),
                r = esc(&r),
                id = esc(&d.id),
                c = when(d.created_at)
            )
        })
        .collect();
    table(&["Database", "Id", "Created"], &rows, "No databases yet.")
}

async fn dbs_page(s: Session, q: Params) -> Page {
    let p = Pager::of(&q, "cursor");
    let list: DbList = s.get(&format!("/v1/dbs?{}", p.api_query())).await.map_err(|e| s.fail("Databases", &e))?;
    Ok(page(
        "Databases",
        Some(&s),
        Flash::of(&q).as_ref(),
        &format!(
            r#"<h1>Your databases</h1><div class="card">{rows}{pager}</div>
<p class="muted">Organizations' databases are on each <a href="/console/orgs">organization's page</a>.</p>
<div class="card"><form class="row" method="post" action="/console/dbs">{csrf}<input name="name" placeholder="new-database" pattern="[a-z0-9][a-z0-9_-]*" required><button>Create database</button></form></div>"#,
            rows = db_rows(&list.databases, None),
            pager = p.links("/console/dbs", list.next_cursor.as_deref()),
            csrf = s.csrf_input()
        ),
    ))
}

async fn create_db(Posted { s, f }: Posted) -> Page {
    let body = json!({"name": field(&f, "name")});
    s.act("POST", "/v1/dbs", Some(body), "/console/dbs", |j| {
        (db_path(j["name"].as_str().unwrap_or_default()), "Database created.".into())
    })
    .await
}

async fn delete_db(Path(db): Path<String>, Posted { s, f }: Posted) -> Page {
    if field(&f, "confirm") != db {
        return Ok(back(&db_path(&db), "Type the database name to confirm deletion.", false));
    }
    let home = match DbRef::parse(&db) {
        Some(DbRef::Org { org, .. }) => super::orgs::org_path(&org),
        _ => "/console/dbs".into(),
    };
    s.act("DELETE", &api_db(&db), None, &db_path(&db), stay(&home, "Database deleted.")).await
}

fn metrics(info: &DbInfo) -> String {
    let st = &info.stats;
    let quota = match st.quota_bytes {
        Some(q) if q > 0 => {
            let pct = (st.size_bytes as f64 * 100.0 / q as f64).min(100.0);
            format!(
                r#"{} of {} ({pct:.1}%)<div class="bar"><i style="width:{pct:.1}%"></i></div>"#,
                format_bytes(st.size_bytes),
                format_bytes(q)
            )
        }
        _ => format!("{} (no quota)", format_bytes(st.size_bytes)),
    };
    let levels: Vec<String> = st
        .levels
        .iter()
        .map(|l| {
            format!(
                "<tr><td>L{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                l.level,
                l.segments,
                format_bytes(l.bytes),
                l.entries
            )
        })
        .collect();
    let lease = match &st.lease_holder {
        Some(h) => format!("{} until {}", esc(h), st.lease_expires_ms.map_or_else(|| "—".into(), when)),
        None => "none".into(),
    };
    format!(
        r#"<div class="card"><div class="stats">
<div><span>Size</span>{quota}</div>
<div><span>Version</span>{ver}</div>
<div><span>Your role</span>{role}</div>
<div><span>Organization</span>{org}</div>
<div><span>Blob bytes</span>{blobs}</div>
<div><span>Tail</span>{te} entries · {tb}</div>
<div><span>Range tombstones</span>{rt}</div>
<div><span>Objects awaiting GC</span>{gc}</div>
<div><span>Lease</span>{lease}</div>
<div><span>Updated</span>{upd}</div>
</div><h2>Segments</h2>{levels}</div>"#,
        ver = st.version,
        role = info.role.name(),
        org = esc(info.org.as_deref().unwrap_or("—")),
        blobs = format_bytes(st.blob_bytes),
        te = st.tail_entries,
        tb = format_bytes(st.tail_bytes as u64),
        rt = st.range_tombstones,
        gc = st.pending_gc_objects,
        upd = when(st.updated_ms),
        levels = table(
            &["Level", "Segments", "Bytes", "Entries"],
            &levels,
            "No segments yet: everything is in the manifest's tail."
        ),
    )
}

/// Key encoding for console forms and query parameters: UTF-8 text when valid,
/// or base64 otherwise.
#[derive(Clone, Debug, PartialEq, Eq)]
enum ItemKey {
    Utf8(String),
    B64(String),
}

impl ItemKey {
    fn of(it: &KvItem) -> ItemKey {
        match (&it.key, &it.key_b64) {
            (Some(k), _) => ItemKey::Utf8(k.clone()),
            (None, b) => ItemKey::B64(b.clone().unwrap_or_default()),
        }
    }

    fn from_fields(f: &Fields) -> Option<ItemKey> {
        match (f.get("key_b64"), f.get("key")) {
            (Some(b), _) => Some(ItemKey::B64(b.clone())),
            (None, Some(k)) if !k.is_empty() => Some(ItemKey::Utf8(k.clone())),
            _ => None,
        }
    }

    fn bytes(&self) -> Option<Vec<u8>> {
        match self {
            ItemKey::Utf8(k) => Some(k.as_bytes().to_vec()),
            ItemKey::B64(b) => b64::decode(b),
        }
    }

    fn label(&self) -> String {
        match self {
            ItemKey::Utf8(k) => esc(k),
            ItemKey::B64(b) => format!("(base64) {}", esc(b)),
        }
    }

    fn query(&self) -> String {
        match self {
            ItemKey::Utf8(k) => format!("key={}", url_seg(k)),
            ItemKey::B64(b) => format!("key_b64={}", url_seg(b)),
        }
    }

    fn hidden_input(&self) -> String {
        let (name, v) = match self {
            ItemKey::Utf8(k) => ("key", k),
            ItemKey::B64(b) => ("key_b64", b),
        };
        format!(r#"<input type="hidden" name="{name}" value="{}">"#, esc(v))
    }
}

/// Form state preserved when re-rendering the database page after a POST.
#[derive(Default)]
struct Forms {
    txn: Option<String>,
    result: Option<String>,
    table_def: Option<String>,
}

async fn db_page(Path(db): Path<String>, s: Session, q: Params) -> Page {
    render_db(&s, &db, &q.0, Forms::default(), Flash::of(&q)).await
}

async fn render_db(s: &Session, db: &str, q: &Fields, forms: Forms, flash: Option<Flash>) -> Page {
    let info: DbInfo = s.get(&api_db(db)).await.map_err(|e| back("/console/dbs", &e, false))?;
    let keys = keys_section(s, db, &info, q).await.map_err(|e| s.fail(db, &e))?;
    let tables = tables_section(s, db, &info, forms.table_def.as_deref()).await.map_err(|e| s.fail(db, &e))?;
    let streams = super::streams::streams_section(s, db, info.role.can_write(), q).await.map_err(|e| s.fail(db, &e))?;
    let events = super::events::events_section(s, db, info.role.can_write()).await.map_err(|e| s.fail(db, &e))?;
    let txn = txn_section(s, db, forms.txn.as_deref(), forms.result.as_deref());
    let danger = if info.role.can_administer() {
        format!(
            r#"<h2>Danger zone</h2><div class="card"><form class="row" method="post" action="{dp}/delete">{csrf}<input name="confirm" placeholder="type {dbn} to confirm"><button class="danger">Delete database</button></form></div>"#,
            dp = db_path(db),
            csrf = s.csrf_input(),
            dbn = esc(db)
        )
    } else {
        String::new()
    };
    Ok(page(
        db,
        Some(s),
        flash.as_ref(),
        &format!(
            r#"<h1>{dbn}</h1><p class="muted">id <code>{id}</code></p>{metrics}{keys}{tables}{streams}{events}{txn}{danger}"#,
            dbn = esc(db),
            id = esc(&info.id),
            metrics = metrics(&info),
        ),
    ))
}

async fn keys_section(s: &Session, db: &str, info: &DbInfo, q: &Fields) -> Result<String, String> {
    let prefix = field(q, "prefix");
    let p = Pager::of(q, "cursor");
    let scan: Scan = s.get(&format!("{}/kv?prefix={}&{}", api_db(db), url_seg(prefix), p.api_query())).await?;
    let can_write = info.role.can_write();
    let dp = db_path(db);
    let rows: Vec<String> = scan
        .items
        .iter()
        .map(|it| {
            let key = ItemKey::of(it);
            let val = match (&it.value, &it.blob) {
                (Some(v), _) => {
                    let short: String = v.chars().take(200).collect();
                    format!("<code>{}</code>{}", esc(&short), if v.chars().count() > 200 { "…" } else { "" })
                }
                (None, Some(b)) => blob_cell(db, b),
                (None, None) => r#"<span class="muted">(binary)</span>"#.into(),
            };
            let delete = if can_write {
                format!(
                    r#"<form class="inline" method="post" action="{dp}/kv/delete">{csrf}{hidden}<button class="link danger">delete</button></form>"#,
                    csrf = s.csrf_input(),
                    hidden = key.hidden_input()
                )
            } else {
                String::new()
            };
            format!(
                r#"<tr><td><code>{k}</code></td><td>{val}</td><td>{v}</td><td><a href="{dp}/kv/download?{param}">download</a>{delete}</td></tr>"#,
                k = key.label(),
                v = it.version.map_or_else(String::new, |v| v.to_string()),
                param = esc(&key.query()),
            )
        })
        .collect();
    let writes = if can_write {
        format!(
            r#"<form class="row" method="post" action="{dp}/kv">{csrf}<input name="key" placeholder="key" required><input name="value" placeholder="value"><button>Put</button></form>
<form class="row" method="post" action="{dp}/kv/upload" enctype="multipart/form-data">{csrf}<input name="key" placeholder="key (default: file name)"><input type="file" name="file" required><button>Upload file</button></form>"#,
            csrf = s.csrf_input()
        )
    } else {
        r#"<p class="muted">You have read-only access to this database.</p>"#.to_string()
    };
    Ok(format!(
        r#"<h2>Keys</h2><div class="card"><form class="row" method="get" action="{dp}"><input name="prefix" value="{pf}" placeholder="prefix"><button>Filter</button></form>
{table}{more}{writes}</div>"#,
        pf = esc(prefix),
        table = table(&["Key", "Value", "Version", ""], &rows, "No keys."),
        more = p.links(&format!("{dp}?prefix={}", url_seg(prefix)), scan.next_cursor.as_deref()),
    ))
}

async fn tables_section(s: &Session, db: &str, info: &DbInfo, def: Option<&str>) -> Result<String, String> {
    let tables: Tables = s.get(&format!("{}/tables", api_db(db))).await?;
    let rows: Vec<String> = tables
        .tables
        .iter()
        .map(|t| {
            format!(
                r#"<tr><td><a href="{p}">{n}</a></td><td>{c}</td><td>{i}</td></tr>"#,
                p = table_path(db, &t.name),
                n = esc(&t.name),
                c = t.columns.len(),
                i = t.indexes.len(),
            )
        })
        .collect();
    let create = if info.role.can_write() {
        format!(
            r#"<form method="post" action="{dp}/tables">{csrf}<textarea name="def">{d}</textarea><p><button>Create table</button></p></form>"#,
            dp = db_path(db),
            csrf = s.csrf_input(),
            d = esc(def.unwrap_or(
                r#"{"name": "items", "primary_key": ["id"], "columns": [{"name": "id", "type": "int", "nullable": false}, {"name": "label", "type": "string"}]}"#
            ))
        )
    } else {
        String::new()
    };
    Ok(format!(
        r#"<h2>Tables</h2><div class="card">{t}{create}</div>"#,
        t = table(&["Table", "Columns", "Indexes"], &rows, "No tables.")
    ))
}

fn txn_section(s: &Session, db: &str, txn: Option<&str>, result: Option<&str>) -> String {
    let txn = txn.unwrap_or(r#"{"ops": [{"op": "scan", "prefix": "", "limit": 10}]}"#);
    let result = result.map(|r| format!(r#"<h2>Result</h2><pre>{}</pre>"#, esc(r))).unwrap_or_default();
    format!(
        r#"<h2>Transaction</h2><div class="card"><form method="post" action="{dp}/txn">{csrf}<textarea name="body">{t}</textarea><p><button>Run transaction</button></p></form>{result}</div>"#,
        dp = db_path(db),
        csrf = s.csrf_input(),
        t = esc(txn),
    )
}

async fn put_kv(Path(db): Path<String>, Posted { s, f }: Posted) -> Page {
    let body = json!({"ops": [{"op": "put", "key": field(&f, "key"), "value": field(&f, "value")}]});
    let dp = db_path(&db);
    s.act("POST", &format!("{}/txn", api_db(&db)), Some(body), &dp, |j| {
        (dp.clone(), format!("Saved at version {}.", j["version"]))
    })
    .await
}

async fn delete_kv(Path(db): Path<String>, Posted { s, f }: Posted) -> Page {
    let dp = db_path(&db);
    let Some(key) = ItemKey::from_fields(&f).and_then(|k| k.bytes()) else {
        return Ok(back(&dp, "No key given.", false));
    };
    s.act("DELETE", &format!("{}/kv/{}", api_db(&db), url_bytes(&key)), None, &dp, stay(&dp, "Deleted.")).await
}

async fn upload(Path(db): Path<String>, s: Session, headers: HeaderMap, body: Bytes) -> Page {
    let ct = headers.get(http::header::CONTENT_TYPE).and_then(|v| v.to_str().ok()).unwrap_or("");
    let mut parts = multipart::parse(ct, &body)
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("malformed upload: {e}")).into_response())?;
    let text = |name: &str| {
        parts.iter().find(|p| p.name == name).and_then(|p| std::str::from_utf8(&p.data).ok()).unwrap_or("").to_string()
    };
    s.check_csrf(Some(&text("csrf")))?;
    let key = text("key");
    let dp = db_path(&db);
    let Some(i) = parts.iter().position(|p| p.name == "file") else {
        return Ok(back(&dp, "Choose a file to upload.", false));
    };
    let file = parts.swap_remove(i);
    let key = if key.is_empty() { file.filename.clone().unwrap_or_default() } else { key };
    if key.is_empty() {
        return Ok(back(&dp, "Name the key to upload to.", false));
    }
    let ct = file.content_type.as_deref().filter(|c| !c.is_empty()).unwrap_or("application/octet-stream");
    let size = file.data.len() as u64;
    let path = format!("{}/kv/{}", api_db(&db), url_seg(&key));
    let reply = Reply::of(s.raw("PUT", &path, Some(ct), file.data).await).await;
    Ok(match reply.into_result() {
        Ok(j) => back(&dp, &format!("Uploaded {} to {key} at version {}.", format_bytes(size), j["version"]), true),
        Err(e) => back(&dp, &e, false),
    })
}

/// Extracts a sanitized download filename from the last path segment of `key`.
fn file_name(key: &[u8]) -> String {
    let last = key.rsplit(|b| *b == b'/').next().unwrap_or(key);
    let name: String =
        last.iter().filter(|b| b.is_ascii_alphanumeric() || b"._-".contains(b)).take(100).map(|b| *b as char).collect();
    match name.trim_start_matches('.') {
        "" => "download".into(),
        n => n.to_string(),
    }
}

/// Streams `resp` as a file download with headers that prevent browser execution.
async fn attachment(resp: Response, name: &str, from: &str) -> Response {
    if resp.status() != StatusCode::OK {
        let e = Reply::of(resp).await.into_result().err().unwrap_or_else(|| "the download failed".into());
        return back(from, &e, false);
    }
    let (parts, body) = resp.into_parts();
    let mut out = Response::new(body);
    let h = out.headers_mut();
    for k in [http::header::CONTENT_TYPE, http::header::CONTENT_LENGTH] {
        if let Some(v) = parts.headers.get(&k) {
            h.insert(k, v.clone());
        }
    }
    if let Ok(v) = HeaderValue::from_str(&format!("attachment; filename=\"{name}\"")) {
        h.insert(http::header::CONTENT_DISPOSITION, v);
    }
    h.insert(http::header::X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    h.insert(http::header::CONTENT_SECURITY_POLICY, HeaderValue::from_static("sandbox; default-src 'none'"));
    out
}

async fn download_kv(Path(db): Path<String>, s: Session, q: Params) -> Page {
    let dp = db_path(&db);
    let Some(key) = ItemKey::from_fields(&q.0).and_then(|k| k.bytes()) else {
        return Ok(back(&dp, "No key given.", false));
    };
    let resp = s.raw("GET", &format!("{}/kv/{}", api_db(&db), url_bytes(&key)), None, Bytes::new()).await;
    Ok(attachment(resp, &file_name(&key), &dp).await)
}

async fn download_blob(Path((db, blob)): Path<(String, String)>, s: Session) -> Page {
    let resp = s.raw("GET", &format!("{}/blobs/{}", api_db(&db), url_seg(&blob)), None, Bytes::new()).await;
    Ok(attachment(resp, &file_name(blob.as_bytes()), &db_path(&db)).await)
}

async fn run_txn(Path(db): Path<String>, Posted { s, f }: Posted) -> Page {
    let text = field(&f, "body").to_string();
    let result = match crate::api::parse_json::<Json>(text.as_bytes()) {
        Ok(body) => {
            let reply = s.call("POST", &format!("{}/txn", api_db(&db)), Some(body)).await;
            serde_json::to_string_pretty(&reply.body).unwrap_or_default()
        }
        Err(e) => format!("Invalid JSON: {e}"),
    };
    let forms = Forms { txn: Some(text), result: Some(result), ..Forms::default() };
    render_db(&s, &db, &Fields::new(), forms, None).await
}

async fn create_table(Path(db): Path<String>, Posted { s, f }: Posted) -> Page {
    let text = field(&f, "def").to_string();
    let outcome = match crate::api::parse_json::<Json>(text.as_bytes()) {
        Ok(def) => s.api("POST", &format!("{}/tables", api_db(&db)), Some(def)).await,
        Err(e) => Err(format!("Invalid JSON: {e}")),
    };
    match outcome {
        Ok(j) => Ok(back(&table_path(&db, j["name"].as_str().unwrap_or_default()), "Table created.", true)),
        Err(e) => {
            let forms = Forms { table_def: Some(text), ..Forms::default() };
            render_db(&s, &db, &Fields::new(), forms, Some(Flash::err(&e))).await
        }
    }
}

async fn table_page(Path((db, t)): Path<(String, String)>, s: Session, q: Params) -> Page {
    let schema: Schema = s.get(&api_table(&db, &t)).await.map_err(|e| back(&db_path(&db), &e, false))?;
    let info: DbInfo = s.get(&api_db(&db)).await.map_err(|e| s.fail(&t, &e))?;
    let p = Pager::of(&q, "cursor");
    let query = json!({"limit": p.limit, "cursor": p.cursor});
    let rows = s.api("POST", &format!("{}/rows/query", api_table(&db, &t)), Some(query)).await;
    let rows: Rows = rows
        .and_then(|j| serde_json::from_value(j).map_err(|e| format!("unexpected rows: {e}")))
        .map_err(|e| s.fail(&t, &e))?;
    let tp = table_path(&db, &t);
    let can_write = info.role.can_write();
    let drop = |what: &str, name: &str| {
        if can_write { s.button(&format!("{tp}/{what}/{}/drop", url_seg(name)), "drop", true) } else { String::new() }
    };
    let columns: Vec<String> = schema
        .columns
        .iter()
        .map(|c| {
            let action = if schema.primary_key.contains(&c.name) { String::new() } else { drop("columns", &c.name) };
            format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{action}</td></tr>",
                esc(&c.name),
                c.ty.name(),
                if c.nullable { "yes" } else { "no" },
                c.default.as_ref().map(|d| cell(&db, d)).unwrap_or_default()
            )
        })
        .collect();
    let indexes: Vec<String> = schema
        .indexes
        .iter()
        .map(|i| {
            format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                esc(&i.name),
                esc(&i.columns.join(", ")),
                if i.unique { "yes" } else { "no" },
                match i.state {
                    IndexState::Ready => "ready",
                    IndexState::Building => "building",
                },
                drop("indexes", &i.name)
            )
        })
        .collect();
    let head: Vec<String> = schema.columns.iter().map(|c| esc(&c.name)).collect();
    let head: Vec<&str> = head.iter().map(|c| c.as_str()).collect();
    let body: Vec<String> = rows
        .rows
        .iter()
        .map(|r| {
            let cells: String = schema
                .columns
                .iter()
                .map(|c| format!("<td>{}</td>", cell(&db, r.get(&c.name).unwrap_or(&Json::Null))))
                .collect();
            format!("<tr>{cells}</tr>")
        })
        .collect();
    let edit = if can_write {
        {
            let types: String = ColType::ALL.iter().map(|t| format!("<option>{}</option>", t.name())).collect();
            format!(
                r#"<h2>Change the schema</h2><div class="card">
<form class="row" method="post" action="{tp}/columns">{csrf}<input name="name" placeholder="column" required><select name="type">{types}</select>
<label><input type="checkbox" name="nullable" value="1" checked> nullable</label><input name="default" placeholder="default (JSON)"><button>Add column</button></form>
<form class="row" method="post" action="{tp}/indexes">{csrf}<input name="name" placeholder="index" required><input name="columns" placeholder="columns, comma separated" required>
<label><input type="checkbox" name="unique" value="1"> unique</label><button>Add index</button></form>
<form class="row" method="post" action="{tp}/drop">{csrf}<input name="confirm" placeholder="type {tn} to confirm"><button class="danger">Drop table</button></form></div>"#,
                csrf = s.csrf_input(),
                tn = esc(&t),
            )
        }
    } else {
        String::new()
    };
    Ok(page(
        &t,
        Some(&s),
        Flash::of(&q).as_ref(),
        &format!(
            r#"<p><a href="{dp}">← {dbn}</a></p><h1>{tn}</h1>
<div class="card">{columns}<p class="muted">Primary key: {pk}</p>{indexes}</div>{edit}
<h2>Rows</h2><div class="card">{rows}{more}</div>"#,
            dp = db_path(&db),
            dbn = esc(&db),
            tn = esc(&schema.name),
            pk = esc(&schema.primary_key.join(", ")),
            columns = table(&["Column", "Type", "Nullable", "Default", ""], &columns, "No columns."),
            indexes = table(&["Index", "Columns", "Unique", "State", ""], &indexes, "No secondary indexes."),
            rows = table(&head, &body, "No rows."),
            more = p.links(&tp, rows.next_cursor.as_deref()),
        ),
    ))
}

async fn drop_table(Path((db, t)): Path<(String, String)>, Posted { s, f }: Posted) -> Page {
    let tp = table_path(&db, &t);
    if field(&f, "confirm") != t {
        return Ok(back(&tp, "Type the table name to confirm.", false));
    }
    s.act("DELETE", &api_table(&db, &t), None, &tp, stay(&db_path(&db), "Table dropped.")).await
}

async fn add_column(Path((db, t)): Path<(String, String)>, Posted { s, f }: Posted) -> Page {
    let tp = table_path(&db, &t);
    let default = match field(&f, "default").trim() {
        "" => None,
        d => match crate::api::parse_json::<Json>(d.as_bytes()) {
            Ok(v) => Some(v),
            Err(e) => return Ok(back(&tp, &format!("The default is not JSON: {e}"), false)),
        },
    };
    let mut col = json!({"name": field(&f, "name"), "type": field(&f, "type"), "nullable": f.contains_key("nullable")});
    if let Some(d) = default {
        col["default"] = d;
    }
    s.act("POST", &format!("{}/columns", api_table(&db, &t)), Some(col), &tp, stay(&tp, "Column added.")).await
}

async fn drop_column(Path((db, t, c)): Path<(String, String, String)>, Posted { s, .. }: Posted) -> Page {
    let tp = table_path(&db, &t);
    let path = format!("{}/columns/{}", api_table(&db, &t), url_seg(&c));
    s.act("DELETE", &path, None, &tp, stay(&tp, "Column dropped.")).await
}

async fn add_index(Path((db, t)): Path<(String, String)>, Posted { s, f }: Posted) -> Page {
    let tp = table_path(&db, &t);
    let columns: Vec<&str> = field(&f, "columns").split(',').map(str::trim).filter(|c| !c.is_empty()).collect();
    let body = json!({"name": field(&f, "name"), "columns": columns, "unique": f.contains_key("unique")});
    s.act("POST", &format!("{}/indexes", api_table(&db, &t)), Some(body), &tp, stay(&tp, "Index added.")).await
}

async fn drop_index(Path((db, t, i)): Path<(String, String, String)>, Posted { s, .. }: Posted) -> Page {
    let tp = table_path(&db, &t);
    let path = format!("{}/indexes/{}", api_table(&db, &t), url_seg(&i));
    s.act("DELETE", &path, None, &tp, stay(&tp, "Index dropped.")).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn download_names_are_safe() {
        assert_eq!(file_name(b"photos/2024/cat.png"), "cat.png");
        assert_eq!(file_name(b"a\"; filename=evil.html"), "afilenameevil.html");
        assert_eq!(file_name(b"../.."), "download");
        assert_eq!(file_name(b"\xff\x00"), "download");
        assert_eq!(file_name(b"dir/"), "download");
    }

    #[test]
    fn item_keys_round_trip_through_forms() {
        for key in [ItemKey::Utf8("a/b c".into()), ItemKey::B64(b64::encode(&[0xff, 0]))] {
            let q = key.query();
            let (name, value) = q.split_once('=').unwrap();
            let decoded = String::from_utf8(crate::api::percent_decode(value)).unwrap();
            assert_eq!(ItemKey::from_fields(&[(name.to_string(), decoded)].into()), Some(key.clone()));
        }
        assert_eq!(ItemKey::B64(b64::encode(&[0xff])).bytes(), Some(vec![0xff]));
        assert_eq!(ItemKey::from_fields(&[("key".to_string(), String::new())].into()), None);
        assert!(ItemKey::Utf8("<k>".into()).hidden_input().contains(r#"name="key" value="&lt;k&gt;""#));
    }
}
