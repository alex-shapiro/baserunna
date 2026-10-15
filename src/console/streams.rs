//! Stream console pages: a database's streams, and each stream's records,
//! newest first, with appends, retention, trims and consumers.

use super::db::{api_db, db_path};
use super::{
    Fields, Flash, Page, Pager, Params, Posted, Session, back, esc, field, format_bytes, page, stay, table, url_seg,
    when,
};
use crate::meta::Role;
use axum::Router;
use axum::extract::Path;
use axum::routing::{get, post};
use serde::Deserialize;
use serde_json::{Value as Json, json};
use std::sync::Arc;

const RECORDS_PAGE: u64 = 50;

pub fn routes() -> Router<Arc<crate::node::Node>> {
    Router::new()
        .route("/console/dbs/{db}/streams", post(create_stream))
        .route("/console/dbs/{db}/streams/{s}", get(stream_page))
        .route("/console/dbs/{db}/streams/{s}/records", post(append))
        .route("/console/dbs/{db}/streams/{s}/retention", post(retention))
        .route("/console/dbs/{db}/streams/{s}/trim", post(trim))
        .route("/console/dbs/{db}/streams/{s}/delete", post(delete))
}

fn stream_path(db: &str, s: &str) -> String {
    format!("{}/streams/{}", db_path(db), url_seg(s))
}

fn api_stream(db: &str, s: &str) -> String {
    format!("{}/streams/{}", api_db(db), url_seg(s))
}

#[derive(Deserialize)]
struct Consumer {
    name: String,
    seq: u64,
    lag: u64,
    updated_at: u64,
}

#[derive(Deserialize)]
struct Stream {
    name: String,
    retention_ms: Option<u64>,
    max_bytes: Option<u64>,
    first_seq: u64,
    next_seq: u64,
    bytes: u64,
    last_time: Option<u64>,
    #[serde(default)]
    consumers: Vec<Consumer>,
}

#[derive(Deserialize)]
struct StreamList {
    streams: Vec<Stream>,
    next_cursor: Option<String>,
}

#[derive(Deserialize)]
struct Records {
    records: Vec<Json>,
    next_seq: u64,
    done: bool,
}

#[derive(Deserialize)]
struct Info {
    role: Role,
}

fn limits(s: &Stream) -> String {
    let mut out = Vec::new();
    if let Some(ms) = s.retention_ms {
        out.push(format!("{} s", ms / 1000));
    }
    if let Some(b) = s.max_bytes {
        out.push(format_bytes(b));
    }
    if out.is_empty() { "keeps everything".into() } else { format!("keeps {}", out.join(", at most ")) }
}

/// The streams section of a database's page.
pub(super) async fn streams_section(s: &Session, db: &str, can_write: bool, q: &Fields) -> Result<String, String> {
    let p = Pager::of(q, "streams");
    let list: StreamList = s.get(&format!("{}/streams?{}", api_db(db), p.api_query())).await?;
    let rows: Vec<String> = list
        .streams
        .iter()
        .map(|st| {
            format!(
                r#"<tr><td><a href="{p}">{n}</a></td><td>{r}</td><td>{b}</td><td>{l}</td><td class="muted">{k}</td></tr>"#,
                p = stream_path(db, &st.name),
                n = esc(&st.name),
                r = st.next_seq - st.first_seq,
                b = format_bytes(st.bytes),
                l = st.last_time.map(when).unwrap_or_default(),
                k = esc(&limits(st)),
            )
        })
        .collect();
    let create = if can_write {
        format!(
            r#"<form class="row" method="post" action="{dp}/streams">{csrf}<input name="name" placeholder="stream" pattern="[A-Za-z0-9_.:-]+" required><input name="retention_ms" placeholder="retention (ms)" inputmode="numeric"><input name="max_bytes" placeholder="max bytes" inputmode="numeric"><button>Create stream</button></form>"#,
            dp = db_path(db),
            csrf = s.csrf_input()
        )
    } else {
        String::new()
    };
    Ok(format!(
        r#"<h2>Streams</h2><div class="card">{t}{more}{create}</div>"#,
        t = table(&["Stream", "Records", "Bytes", "Last append", "Retention"], &rows, "No streams."),
        more = p.links(&db_path(db), list.next_cursor.as_deref()),
    ))
}

fn record_cell(r: &Json) -> String {
    let body = match (&r["json"], &r["value"], &r["value_b64"]) {
        (Json::Null, Json::String(v), _) => esc(v),
        (Json::Null, _, Json::String(b)) => format!(r#"<span class="muted">base64</span> {}"#, esc(b)),
        (j, _, _) if r.get("json").is_some() => format!("<code>{}</code>", esc(&j.to_string())),
        _ => String::new(),
    };
    let headers = match r["headers"].as_object() {
        Some(h) => {
            let hs: Vec<String> =
                h.iter().map(|(k, v)| format!("{}: {}", esc(k), esc(v.as_str().unwrap_or("")))).collect();
            format!(r#"<div class="muted">{}</div>"#, hs.join(" · "))
        }
        None => String::new(),
    };
    format!("{body}{headers}")
}

async fn stream_page(Path((db, name)): Path<(String, String)>, s: Session, q: Params) -> Page {
    let st: Stream = s.get(&api_stream(&db, &name)).await.map_err(|e| back(&db_path(&db), &e, false))?;
    let info: Info = s.get(&api_db(&db)).await.map_err(|e| s.fail(&name, &e))?;
    let until = q.get("until").and_then(|u| u.parse::<u64>().ok());
    let read = match until {
        Some(u) => format!("reverse=true&limit={RECORDS_PAGE}&until_seq={u}"),
        None => format!("reverse=true&limit={RECORDS_PAGE}"),
    };
    let records: Records =
        s.get(&format!("{}/records?{read}", api_stream(&db, &name))).await.map_err(|e| s.fail(&name, &e))?;
    let sp = stream_path(&db, &name);
    let rows: Vec<String> = records
        .records
        .iter()
        .map(|r| {
            format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td></tr>",
                r["seq"],
                when(r["time"].as_u64().unwrap_or(0)),
                record_cell(r)
            )
        })
        .collect();
    let mut pager = Vec::new();
    if until.is_some() {
        pager.push(format!(r#"<a href="{}">Newest</a>"#, esc(&sp)));
    }
    if !records.done {
        pager.push(format!(r#"<a href="{}?until={}">Older →</a>"#, esc(&sp), records.next_seq));
    }
    let pager =
        if pager.is_empty() { String::new() } else { format!(r#"<div class="pager">{}</div>"#, pager.concat()) };
    let consumers: Vec<String> = st
        .consumers
        .iter()
        .map(|c| {
            format!(
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                esc(&c.name),
                c.seq,
                c.lag,
                when(c.updated_at)
            )
        })
        .collect();
    let edit = if info.role.can_write() {
        let csrf = s.csrf_input();
        format!(
            r#"<h2>Append</h2><div class="card"><form class="row" method="post" action="{sp}/records">{csrf}<input name="value" placeholder="record" required size="60"><label><input type="checkbox" name="json" value="1"> JSON</label><button>Append</button></form></div>
<h2>Retention and trims</h2><div class="card">
<form class="row" method="post" action="{sp}/retention">{csrf}<input name="retention_ms" value="{rm}" placeholder="retention (ms)" inputmode="numeric"><input name="max_bytes" value="{mb}" placeholder="max bytes" inputmode="numeric"><button>Set retention</button></form>
<form class="row" method="post" action="{sp}/trim">{csrf}<input name="before_seq" placeholder="trim records before seq" inputmode="numeric" required><button class="danger">Trim</button></form>
<form class="row" method="post" action="{sp}/delete">{csrf}<input name="confirm" placeholder="type {sn} to confirm"><button class="danger">Delete stream</button></form></div>"#,
            rm = st.retention_ms.map(|v| v.to_string()).unwrap_or_default(),
            mb = st.max_bytes.map(|v| v.to_string()).unwrap_or_default(),
            sn = esc(&name),
        )
    } else {
        String::new()
    };
    Ok(page(
        &name,
        Some(&s),
        Flash::of(&q).as_ref(),
        &format!(
            r#"<p><a href="{dp}">← {dbn}</a></p><h1>{sn}</h1>
<div class="card"><div class="stats"><div><span>Records</span>{range}</div><div><span>Bytes</span>{bytes}</div><div><span>Retention</span>{lim}</div></div></div>
<h2>Records, newest first</h2><div class="card">{rows}{pager}</div>
<h2>Consumers</h2><div class="card">{consumers}</div>{edit}"#,
            dp = db_path(&db),
            dbn = esc(&db),
            sn = esc(&st.name),
            range = match st.next_seq > st.first_seq {
                true => format!("{}–{}", st.first_seq, st.next_seq - 1),
                false => format!("none (next {})", st.next_seq),
            },
            bytes = format_bytes(st.bytes),
            lim = esc(&limits(&st)),
            rows = table(&["Seq", "Time", "Record"], &rows, "No records."),
            consumers = table(&["Consumer", "Next seq", "Unread", "Updated"], &consumers, "No consumers."),
        ),
    ))
}

/// Reads an optional number from a form field.
fn number(f: &Fields, k: &str) -> Result<Option<u64>, String> {
    match field(f, k).trim() {
        "" => Ok(None),
        v => v.parse().map(Some).map_err(|_| format!("{k} must be a number")),
    }
}

async fn create_stream(Path(db): Path<String>, Posted { s, f }: Posted) -> Page {
    let dp = db_path(&db);
    let (retention_ms, max_bytes) = match (number(&f, "retention_ms"), number(&f, "max_bytes")) {
        (Ok(r), Ok(m)) => (r, m),
        (Err(e), _) | (_, Err(e)) => return Ok(back(&dp, &e, false)),
    };
    let mut body = json!({"name": field(&f, "name")});
    if let Some(r) = retention_ms {
        body["retention_ms"] = json!(r);
    }
    if let Some(m) = max_bytes {
        body["max_bytes"] = json!(m);
    }
    s.act("POST", &format!("{}/streams", api_db(&db)), Some(body), &dp, |j| {
        (stream_path(&db, j["name"].as_str().unwrap_or_default()), "Stream created.".into())
    })
    .await
}

async fn append(Path((db, name)): Path<(String, String)>, Posted { s, f }: Posted) -> Page {
    let sp = stream_path(&db, &name);
    let value = field(&f, "value");
    let record = match f.contains_key("json") {
        true => match serde_json::from_str::<Json>(value) {
            Ok(j) => json!({"json": j}),
            Err(e) => return Ok(back(&sp, &format!("The record is not JSON: {e}"), false)),
        },
        false => json!({"value": value}),
    };
    let body = json!({"records": [record]});
    s.act("POST", &format!("{}/records", api_stream(&db, &name)), Some(body), &sp, |j| {
        (sp.clone(), format!("Appended as record {}.", j["first_seq"]))
    })
    .await
}

async fn retention(Path((db, name)): Path<(String, String)>, Posted { s, f }: Posted) -> Page {
    let sp = stream_path(&db, &name);
    let (retention_ms, max_bytes) = match (number(&f, "retention_ms"), number(&f, "max_bytes")) {
        (Ok(r), Ok(m)) => (r, m),
        (Err(e), _) | (_, Err(e)) => return Ok(back(&sp, &e, false)),
    };
    let body = json!({"retention_ms": retention_ms, "max_bytes": max_bytes});
    s.act("PATCH", &api_stream(&db, &name), Some(body), &sp, stay(&sp, "Retention set.")).await
}

async fn trim(Path((db, name)): Path<(String, String)>, Posted { s, f }: Posted) -> Page {
    let sp = stream_path(&db, &name);
    let before = match number(&f, "before_seq") {
        Ok(Some(b)) => b,
        Ok(None) => return Ok(back(&sp, "Give the seq to trim before.", false)),
        Err(e) => return Ok(back(&sp, &e, false)),
    };
    let body = json!({"before_seq": before});
    s.act("POST", &format!("{}/trim", api_stream(&db, &name)), Some(body), &sp, stay(&sp, "Trimmed.")).await
}

async fn delete(Path((db, name)): Path<(String, String)>, Posted { s, f }: Posted) -> Page {
    let sp = stream_path(&db, &name);
    if field(&f, "confirm") != name {
        return Ok(back(&sp, "Type the stream name to confirm.", false));
    }
    s.act("DELETE", &api_stream(&db, &name), None, &sp, stay(&db_path(&db), "Stream deleted.")).await
}
