//! Event table console pages: a database's event tables, and each table's
//! partitions and parts, with a query runner and inserts.

use super::db::{api_db, db_path};
use super::{Flash, Page, Params, Posted, Session, back, esc, field, format_bytes, page, stay, table, url_seg, when};
use crate::meta::Role;
use axum::Router;
use axum::extract::Path;
use axum::routing::{get, post};
use serde::Deserialize;
use serde_json::{Value as Json, json};
use std::sync::Arc;

/// Parts listed on a table's page.
const PARTS_SHOWN: usize = 100;

pub fn routes() -> Router<Arc<crate::node::Node>> {
    Router::new()
        .route("/console/dbs/{db}/events", post(create))
        .route("/console/dbs/{db}/events/{t}", get(table_page))
        .route("/console/dbs/{db}/events/{t}/query", post(query))
        .route("/console/dbs/{db}/events/{t}/rows", post(insert))
        .route("/console/dbs/{db}/events/{t}/delete", post(drop_table))
}

fn table_path(db: &str, t: &str) -> String {
    format!("{}/events/{}", db_path(db), url_seg(t))
}

fn api_table(db: &str, t: &str) -> String {
    format!("{}/events/{}", api_db(db), url_seg(t))
}

#[derive(Deserialize)]
struct TableList {
    tables: Vec<Json>,
}

#[derive(Deserialize)]
struct Info {
    role: Role,
}

const DEFAULT_TABLE: &str = r#"{"name": "events", "columns": [{"name": "time", "type": "time", "default": "now"}, {"name": "event", "type": "string"}, {"name": "user", "type": "string"}, {"name": "properties", "type": "json"}], "time_column": "time", "order_by": ["event", "time"], "partition": "day"}"#;

/// The event tables section of a database's page.
pub(super) async fn events_section(s: &Session, db: &str, can_write: bool) -> Result<String, String> {
    let list: TableList = s.get(&format!("{}/events", api_db(db))).await?;
    let rows: Vec<String> = list
        .tables
        .iter()
        .map(|t| {
            let name = t["name"].as_str().unwrap_or_default();
            format!(
                r#"<tr><td><a href="{p}">{n}</a></td><td>{c}</td><td><code>{o}</code></td><td>{pt}</td></tr>"#,
                p = table_path(db, name),
                n = esc(name),
                c = t["columns"].as_array().map_or(0, Vec::len),
                o = esc(&t["order_by"].to_string()),
                pt = esc(t["partition"].as_str().unwrap_or_default()),
            )
        })
        .collect();
    let create = if can_write {
        format!(
            r#"<form method="post" action="{dp}/events">{csrf}<textarea name="def">{d}</textarea><p><button>Create event table</button></p></form>"#,
            dp = db_path(db),
            csrf = s.csrf_input(),
            d = esc(DEFAULT_TABLE),
        )
    } else {
        String::new()
    };
    Ok(format!(
        r#"<h2>Event tables</h2><div class="card">{t}{create}</div>"#,
        t = table(&["Event table", "Columns", "Sort key", "Partition"], &rows, "No event tables."),
    ))
}

fn cell(v: &Json) -> String {
    match v {
        Json::Null => r#"<span class="muted">null</span>"#.into(),
        Json::String(s) => esc(s),
        Json::Array(_) | Json::Object(_) => format!("<code>{}</code>", esc(&v.to_string())),
        other => esc(&other.to_string()),
    }
}

/// Output columns holding times: time buckets, and time columns read as is.
fn time_outputs(t: &Json, q: &Json) -> Vec<String> {
    let mut out: Vec<String> = t["columns"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|c| c["type"] == "time")
        .filter_map(|c| c["name"].as_str().map(str::to_string))
        .collect();
    let stages = q["group_by"].as_array().into_iter().chain(q["then"]["group_by"].as_array());
    for g in stages.flatten() {
        if g.get("bucket").is_some() {
            out.push(g["as"].as_str().unwrap_or("bucket").to_string());
        }
    }
    out
}

/// The answer to a query as a table, with its statistics.
fn answer_html(a: &Json, times: &[String]) -> String {
    let names: Vec<&str> = a["columns"].as_array().into_iter().flatten().map(|c| c.as_str().unwrap_or("")).collect();
    let cols: Vec<String> = names.iter().map(|c| esc(c)).collect();
    let heads: Vec<&str> = cols.iter().map(String::as_str).collect();
    let is_time: Vec<bool> = names.iter().map(|n| times.iter().any(|t| t == n)).collect();
    let rows: Vec<String> = a["rows"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|r| {
            let cells =
                r.as_array().into_iter().flatten().enumerate().map(|(i, v)| match (is_time.get(i), v.as_i64()) {
                    (Some(true), Some(ms)) if ms >= 0 => format!("<td>{}</td>", when(ms as u64)),
                    _ => format!("<td>{}</td>", cell(v)),
                });
            format!("<tr>{}</tr>", cells.collect::<String>())
        })
        .collect();
    let st = &a["stats"];
    format!(
        r#"{t}<p class="muted">version {v} · {pr} of {p} parts, {gr} of {g} granules, {rr} rows ({u} unsealed), {b} read in {gets} requests{more}</p>"#,
        t = table(&heads, &rows, "No rows."),
        v = a["version"],
        pr = st["parts_read"],
        p = st["parts"],
        gr = st["granules_read"],
        g = st["granules"],
        rr = st["rows_read"],
        u = st["unsealed_rows"],
        b = format_bytes(st["bytes_read"].as_u64().unwrap_or(0)),
        gets = st["store_gets"],
        more = match a["next_cursor"].as_str() {
            Some(c) => format!(" · more rows after cursor <code>{}</code>", esc(c)),
            None => String::new(),
        },
    )
}

/// Events per day and by the leading sort key, named so that no output
/// collides with the key.
pub(crate) fn default_query(t: &Json) -> String {
    let time = t["time_column"].as_str().unwrap_or("time");
    let key = t["order_by"][0].as_str().unwrap_or(time);
    let day = if key == "day" { "date" } else { "day" };
    let count = if key == "count" { "events" } else { "count" };
    let mut group_by = vec![json!({"bucket": {"col": time}, "interval": "1d", "as": day})];
    if key != time {
        group_by.push(json!(key));
    }
    // The 30 days before the newest partition ends, and any newer events.
    let newest = t["partitions"].as_array().into_iter().flatten().filter_map(|p| p["end"].as_i64()).max();
    let from = newest.map_or(json!("2000-01-01"), |end| json!(end - 30 * 86_400_000));
    json!({"from": from, "to": crate::events::schema::TIME_RANGE.end() + 1, "group_by": group_by,
        "aggregates": {count: {"fn": "count"}}, "order_by": [{"col": count, "desc": true}], "limit": 50})
    .to_string()
}

async fn render(s: &Session, db: &str, name: &str, t: &Json, q: &str, answer: String, flash: Option<Flash>) -> Page {
    let info: Info = s.get(&api_db(db)).await.map_err(|e| s.fail(name, &e))?;
    let tp = table_path(db, name);
    let csrf = s.csrf_input();
    let partitions: Vec<String> = t["partitions"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|p| {
            format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                when(p["start"].as_u64().unwrap_or(0)),
                p["parts"],
                p["rows"],
                format_bytes(p["bytes"].as_u64().unwrap_or(0))
            )
        })
        .collect();
    let parts: Vec<String> = t["parts"]
        .as_array()
        .into_iter()
        .flatten()
        .take(PARTS_SHOWN)
        .map(|p| {
            format!(
                "<tr><td><code>{}</code></td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                esc(p["id"].as_str().unwrap_or_default()),
                when(p["partition"].as_u64().unwrap_or(0)),
                p["level"],
                p["rows"],
                p["deleted"],
                format_bytes(p["bytes"].as_u64().unwrap_or(0))
            )
        })
        .collect();
    let columns: Vec<String> = t["columns"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|c| {
            format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                esc(c["name"].as_str().unwrap_or_default()),
                esc(c["type"].as_str().unwrap_or_default()),
                if c["nullable"].as_bool().unwrap_or(true) { "yes" } else { "no" },
                c.get("default").map(cell).unwrap_or_default()
            )
        })
        .collect();
    let edit = if info.role.can_write() {
        format!(
            r#"<h2>Insert</h2><div class="card"><form method="post" action="{tp}/rows">{csrf}<textarea name="rows" placeholder='[{{"event": "signup"}}]'></textarea><p><button>Insert events</button></p></form></div>
<h2>Danger zone</h2><div class="card"><form class="row" method="post" action="{tp}/delete">{csrf}<input name="confirm" placeholder="type {n} to confirm"><button class="danger">Drop event table</button></form></div>"#,
            n = esc(name),
        )
    } else {
        String::new()
    };
    Ok(page(
        name,
        Some(s),
        flash.as_ref(),
        &format!(
            r#"<p><a href="{dp}">← {dbn}</a></p><h1>{n}</h1>
<div class="card"><div class="stats"><div><span>Rows</span>{rows}</div><div><span>Stored</span>{bytes}</div><div><span>Unsealed</span>{un}</div><div><span>Retention</span>{ret}</div></div>
<p class="muted">sorted by <code>{order}</code>, partitioned by {part}</p></div>
<h2>Query</h2><div class="card"><form method="post" action="{tp}/query">{csrf}<textarea name="query">{q}</textarea><p><button>Run query</button></p></form>{answer}</div>
<h2>Columns</h2><div class="card">{columns}</div>
<h2>Partitions</h2><div class="card">{partitions}</div>
<h2>Parts</h2><div class="card">{parts}</div>{edit}"#,
            dp = db_path(db),
            dbn = esc(db),
            n = esc(name),
            rows = t["rows"],
            bytes = format_bytes(t["bytes"].as_u64().unwrap_or(0)),
            un = t["unsealed_rows"],
            ret = match t["retention_ms"].as_u64() {
                Some(ms) => super::span(ms),
                None => "keeps everything".into(),
            },
            order = esc(&t["order_by"].to_string()),
            part = esc(t["partition"].as_str().unwrap_or_default()),
            q = esc(q),
            columns = table(&["Column", "Type", "Nullable", "Default"], &columns, "No columns."),
            partitions = table(&["Partition", "Parts", "Rows", "Bytes"], &partitions, "No parts yet."),
            parts = table(&["Part", "Partition", "Level", "Rows", "Deleted", "Bytes"], &parts, "No parts yet."),
        ),
    ))
}

async fn table_page(Path((db, name)): Path<(String, String)>, s: Session, q: Params) -> Page {
    let t: Json = s.get(&api_table(&db, &name)).await.map_err(|e| back(&db_path(&db), &e, false))?;
    let text = default_query(&t);
    let answer = run_query(&s, &db, &t, &text).await;
    render(&s, &db, &name, &t, &text, answer, Flash::of(&q)).await
}

/// Runs a query typed into the console, rendering its answer or why it failed.
async fn run_query(s: &Session, db: &str, t: &Json, text: &str) -> String {
    match serde_json::from_str::<Json>(text) {
        Ok(mut q) => {
            if let Some(o) = q.as_object_mut() {
                o.entry("consistency").or_insert(json!("strong"));
            }
            let times = time_outputs(t, &q);
            let name = t["name"].as_str().unwrap_or_default();
            let reply = s.call("POST", &format!("{}/rows/query", api_table(db, name)), Some(q)).await;
            match reply.status.is_success() {
                true => answer_html(&reply.body, &times),
                false => format!(
                    r#"<p class="error">{}</p>"#,
                    esc(reply.body["error"]["message"].as_str().unwrap_or("query failed"))
                ),
            }
        }
        Err(e) => format!(r#"<p class="error">Invalid JSON: {}</p>"#, esc(&e.to_string())),
    }
}

async fn create(Path(db): Path<String>, Posted { s, f }: Posted) -> Page {
    let dp = db_path(&db);
    let def = match serde_json::from_str::<Json>(field(&f, "def")) {
        Ok(d) => d,
        Err(e) => return Ok(back(&dp, &format!("Invalid JSON: {e}"), false)),
    };
    s.act("POST", &format!("{}/events", api_db(&db)), Some(def), &dp, |j| {
        (table_path(&db, j["name"].as_str().unwrap_or_default()), "Event table created.".into())
    })
    .await
}

async fn query(Path((db, name)): Path<(String, String)>, Posted { s, f }: Posted) -> Page {
    let t: Json = s.get(&api_table(&db, &name)).await.map_err(|e| back(&db_path(&db), &e, false))?;
    let text = field(&f, "query");
    let answer = run_query(&s, &db, &t, text).await;
    render(&s, &db, &name, &t, text, answer, None).await
}

async fn insert(Path((db, name)): Path<(String, String)>, Posted { s, f }: Posted) -> Page {
    let tp = table_path(&db, &name);
    let rows = match serde_json::from_str::<Json>(field(&f, "rows")) {
        Ok(Json::Array(r)) => r,
        Ok(o @ Json::Object(_)) => vec![o],
        Ok(_) => return Ok(back(&tp, "Give an event object or an array of them.", false)),
        Err(e) => return Ok(back(&tp, &format!("Invalid JSON: {e}"), false)),
    };
    s.act("POST", &format!("{}/rows", api_table(&db, &name)), Some(json!({"rows": rows})), &tp, |j| {
        (tp.clone(), format!("Inserted {} events at version {}.", j["count"], j["version"]))
    })
    .await
}

async fn drop_table(Path((db, name)): Path<(String, String)>, Posted { s, f }: Posted) -> Page {
    let tp = table_path(&db, &name);
    if field(&f, "confirm") != name {
        return Ok(back(&tp, "Type the event table's name to confirm.", false));
    }
    s.act("DELETE", &api_table(&db, &name), None, &tp, stay(&db_path(&db), "Event table dropped.")).await
}
