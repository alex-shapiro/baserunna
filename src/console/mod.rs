//! Admin console: server-rendered HTML (no JavaScript framework, no external
//! assets). Every action calls the public API through the node's own
//! router with the signed-in user's session, so the console has exactly
//! the API's semantics (routing, authorization, validation). See SPEC §11.

use crate::api::{ClientAddr, SESSION_COOKIE, cookie};
use crate::auth;
use crate::node::Node;
use crate::util::{b64, hmac_sha256};
use axum::Router;
use axum::body::Body;
use axum::extract::{Form, Path, Query, State};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use http::{HeaderMap, Request, StatusCode};
use serde_json::{Value as Json, json};
use std::collections::BTreeMap;
use std::sync::Arc;
use tower::ServiceExt;

type F = Form<BTreeMap<String, String>>;
type Q = Query<BTreeMap<String, String>>;

pub fn routes() -> Router<Arc<Node>> {
    Router::new()
        .route("/console", get(|| async { Redirect::to("/console/dbs") }))
        .route("/console/login", get(login_page).post(login_post))
        .route("/console/signup", post(signup_post))
        .route("/console/logout", post(logout_post))
        .route("/console/dbs", get(dbs_page).post(create_db))
        .route("/console/dbs/{db}", get(db_page))
        .route("/console/dbs/{db}/delete", post(delete_db))
        .route("/console/dbs/{db}/kv", post(put_kv))
        .route("/console/dbs/{db}/kv/delete", post(delete_kv))
        .route("/console/dbs/{db}/txn", post(run_txn))
        .route("/console/dbs/{db}/tables/{t}", get(table_page))
        .route("/console/keys", get(keys_page).post(create_key))
        .route("/console/keys/{id}/revoke", post(revoke_key))
        .route("/console/admin", get(admin_page))
        .route("/console/admin/users/{id}", post(admin_user))
}

/// HTML-escape text for element content and attribute values.
pub fn esc(s: &str) -> String {
    let mut o = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => o.push_str("&amp;"),
            '<' => o.push_str("&lt;"),
            '>' => o.push_str("&gt;"),
            '"' => o.push_str("&quot;"),
            '\'' => o.push_str("&#39;"),
            c => o.push(c),
        }
    }
    o
}

fn url_seg(s: &str) -> String {
    s.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

const CSS: &str = r#"
:root{--bg:#f7f7f5;--fg:#1d1d1b;--muted:#6b6b66;--card:#fff;--line:#e3e3de;--accent:#2f5bd3;--danger:#b4232c;--ok:#1f7a3d}
@media (prefers-color-scheme:dark){:root{--bg:#141413;--fg:#ececea;--muted:#9a9a94;--card:#1e1e1c;--line:#33332f;--accent:#7c9cf5;--danger:#f0707a;--ok:#5fc787}}
*{box-sizing:border-box}body{margin:0;font:15px/1.5 system-ui,-apple-system,Segoe UI,sans-serif;background:var(--bg);color:var(--fg)}
header{display:flex;gap:18px;align-items:center;padding:12px 20px;border-bottom:1px solid var(--line);background:var(--card);flex-wrap:wrap}
header b{margin-right:12px}header a{color:var(--fg);text-decoration:none}header a:hover{color:var(--accent)}
header form{margin-left:auto}main{max-width:1100px;margin:24px auto;padding:0 16px}
h1{font-size:22px;margin:0 0 16px}h2{font-size:17px;margin:24px 0 8px}
.card{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:16px;margin-bottom:16px;overflow-x:auto}
table{border-collapse:collapse;width:100%}th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--line);vertical-align:top}
th{color:var(--muted);font-weight:600;font-size:13px}td code,pre{font:13px ui-monospace,SFMono-Regular,Menlo,monospace;white-space:pre-wrap;word-break:break-all}
input,textarea,select{font:inherit;padding:6px 8px;border:1px solid var(--line);border-radius:6px;background:var(--bg);color:var(--fg)}
textarea{width:100%;min-height:140px;font-family:ui-monospace,Menlo,monospace}
button{font:inherit;padding:6px 12px;border-radius:6px;border:1px solid var(--accent);background:var(--accent);color:#fff;cursor:pointer}
button.danger{background:transparent;color:var(--danger);border-color:var(--danger)}button.link{background:none;border:none;color:var(--accent);padding:0}
.row{display:flex;gap:8px;flex-wrap:wrap;align-items:center}.muted{color:var(--muted)}.flash{padding:10px 12px;border-radius:6px;margin-bottom:16px;border:1px solid var(--line)}
.flash.err{border-color:var(--danger);color:var(--danger)}.flash.ok{border-color:var(--ok);color:var(--ok)}
.login{max-width:380px;margin:60px auto}.login input{width:100%;margin-bottom:10px}
"#;

fn page(title: &str, nav: Option<&Nav>, flash: Option<(&str, bool)>, body: &str) -> Html<String> {
    let header = match nav {
        Some(n) => format!(
            r#"<header><b>Baserunna</b><a href="/console/dbs">Databases</a><a href="/console/keys">API keys</a>{admin}
<span class="muted">{email}</span><form method="post" action="/console/logout"><input type="hidden" name="csrf" value="{csrf}"><button class="link">Sign out</button></form></header>"#,
            admin = if n.is_admin { r#"<a href="/console/admin">Admin</a>"# } else { "" },
            email = esc(&n.email),
            csrf = esc(&n.csrf),
        ),
        None => "<header><b>Baserunna</b></header>".to_string(),
    };
    let flash = match flash {
        Some((m, ok)) => format!(r#"<div class="flash {}">{}</div>"#, if ok { "ok" } else { "err" }, esc(m)),
        None => String::new(),
    };
    Html(format!(
        r#"<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{t} · Baserunna</title><style>{CSS}</style></head><body>{header}<main>{flash}{body}</main></body></html>"#,
        t = esc(title)
    ))
}

struct Nav {
    token: String,
    email: String,
    is_admin: bool,
    csrf: String,
}

fn csrf_for(node: &Node, token: &str) -> String {
    b64::encode_url(&hmac_sha256(&node.cookie_key(), format!("csrf:{}", auth::secret_hash(token)).as_bytes())[..16])
}

/// The signed-in user (from the session cookie), or a redirect to login.
#[allow(clippy::result_large_err)]
async fn nav(node: &Arc<Node>, headers: &HeaderMap) -> Result<Nav, Response> {
    let Some(token) = cookie(headers, SESSION_COOKIE).filter(|t| !t.is_empty()) else {
        return Err(Redirect::to("/console/login").into_response());
    };
    match node.authenticate(&token).await {
        Ok(p) if p.is_session() => {
            Ok(Nav { csrf: csrf_for(node, &token), email: p.user.email.clone(), is_admin: p.user.is_admin, token })
        }
        // Propagate retryable errors (e.g. rate limits) rather than redirecting to login.
        Err(e) if e.retryable() => Err(e.into_response()),
        _ => Err(Redirect::to("/console/login").into_response()),
    }
}

#[allow(clippy::result_large_err)]
fn check_csrf(node: &Node, n: &Nav, f: &BTreeMap<String, String>) -> Result<(), Response> {
    let ok = f.get("csrf").is_some_and(|c| crate::util::ct_eq(c.as_bytes(), csrf_for(node, &n.token).as_bytes()));
    if ok { Ok(()) } else { Err((StatusCode::FORBIDDEN, "invalid CSRF token").into_response()) }
}

/// Call the public API as the signed-in user.
async fn api(node: &Arc<Node>, token: &str, method: &str, path: &str, body: Option<Json>) -> (u16, Json) {
    let Some(router) = node.router() else {
        return (500, json!({"error": {"message": "router unavailable"}}));
    };
    let mut b = Request::builder().method(method).uri(path).header("authorization", format!("Bearer {token}"));
    let body = match body {
        Some(j) => {
            b = b.header("content-type", "application/json");
            Body::from(j.to_string())
        }
        None => Body::empty(),
    };
    let Ok(req) = b.body(body) else {
        return (400, json!({"error": {"message": "bad request"}}));
    };
    let resp = match router.oneshot(req).await {
        Ok(r) => r,
        Err(e) => match e {},
    };
    let status = resp.status().as_u16();
    let bytes = http_body_util::BodyExt::collect(resp.into_body()).await.map(|b| b.to_bytes()).unwrap_or_default();
    (status, serde_json::from_slice(&bytes).unwrap_or(Json::Null))
}

fn err_msg(j: &Json) -> String {
    j["error"]["message"].as_str().unwrap_or("request failed").to_string()
}

fn hidden_csrf(n: &Nav) -> String {
    format!(r#"<input type="hidden" name="csrf" value="{}">"#, esc(&n.csrf))
}

fn back(path: &str, msg: &str, ok: bool) -> Response {
    let sep = if path.contains('?') { '&' } else { '?' };
    let key = if ok { "ok" } else { "err" };
    Redirect::to(&format!("{path}{sep}{key}={}", url_seg(msg))).into_response()
}

fn flash_of(q: &BTreeMap<String, String>) -> Option<(String, bool)> {
    q.get("err").map(|m| (m.clone(), false)).or_else(|| q.get("ok").map(|m| (m.clone(), true)))
}

fn cell(v: &Json) -> String {
    match v {
        Json::Null => r#"<span class="muted">null</span>"#.into(),
        Json::String(s) => esc(s),
        Json::Object(o) if o.contains_key("blob_id") => {
            format!("blob {} · {} bytes", esc(o["content_type"].as_str().unwrap_or("")), o["size"])
        }
        other => format!("<code>{}</code>", esc(&other.to_string())),
    }
}

// ------------------------------------------------------------- sign in

async fn login_page(State(node): State<Arc<Node>>, q: Q) -> Html<String> {
    let google = if node.oidc.is_some() {
        r#"<p><a href="/v1/auth/google/start?return_to=/console/dbs">Sign in with Google</a></p>"#
    } else {
        ""
    };
    let flash = flash_of(&q);
    page(
        "Sign in",
        None,
        flash.as_ref().map(|(m, ok)| (m.as_str(), *ok)),
        &format!(
            r#"<div class="login card"><h1>Sign in</h1>
<form method="post" action="/console/login"><input name="email" type="email" placeholder="Email" required autocomplete="username">
<input name="password" type="password" placeholder="Password" required autocomplete="current-password"><button>Sign in</button></form>
{google}<h2>New here?</h2><form method="post" action="/console/signup"><input name="email" type="email" placeholder="Email" required>
<input name="password" type="password" placeholder="Password (8+ characters)" required minlength="8"><button>Create account</button></form></div>"#
        ),
    )
}

fn session_redirect(node: &Node, token: &str) -> Response {
    let secure = if node.cfg.secure_cookies { "; Secure" } else { "" };
    let mut r = Redirect::to("/console/dbs").into_response();
    if let Ok(v) = format!(
        "{SESSION_COOKIE}={token}; Path=/; HttpOnly; SameSite=Lax; Max-Age={}{secure}",
        node.cfg.session_ttl_ms / 1000
    )
    .parse()
    {
        r.headers_mut().append(http::header::SET_COOKIE, v);
    }
    r
}

fn field<'a>(f: &'a BTreeMap<String, String>, k: &str) -> &'a str {
    f.get(k).map_or("", |v| v.as_str())
}

async fn login_post(State(node): State<Arc<Node>>, client: ClientAddr, Form(f): F) -> Response {
    let session = async {
        let user = node.password_login(&client.0, field(&f, "email"), field(&f, "password")).await?;
        node.create_session(&user.id).await
    };
    match session.await {
        Ok(t) => session_redirect(&node, &t),
        Err(e) => back("/console/login", &e.message(), false),
    }
}

async fn signup_post(State(node): State<Arc<Node>>, client: ClientAddr, Form(f): F) -> Response {
    let session = async {
        let (_, r) = node.password_signup(&client.0, field(&f, "email"), field(&f, "password")).await?;
        node.create_session(r["user_id"].as_str().unwrap_or_default()).await
    };
    match session.await {
        Ok(t) => session_redirect(&node, &t),
        Err(e) => back("/console/login", &e.message(), false),
    }
}

async fn logout_post(State(node): State<Arc<Node>>, headers: HeaderMap, Form(f): F) -> Response {
    if let Ok(n) = nav(&node, &headers).await
        && check_csrf(&node, &n, &f).is_ok()
    {
        api(&node, &n.token, "POST", "/v1/auth/logout", Some(json!({}))).await;
    }
    let mut r = Redirect::to("/console/login").into_response();
    if let Ok(v) = format!("{SESSION_COOKIE}=; Path=/; HttpOnly; Max-Age=0").parse() {
        r.headers_mut().append(http::header::SET_COOKIE, v);
    }
    r
}

// ------------------------------------------------------------ databases

async fn dbs_page(State(node): State<Arc<Node>>, headers: HeaderMap, q: Q) -> Response {
    let n = match nav(&node, &headers).await {
        Ok(n) => n,
        Err(r) => return r,
    };
    let (_, j) = api(&node, &n.token, "GET", "/v1/dbs", None).await;
    let mut rows = String::new();
    for d in j["databases"].as_array().into_iter().flatten() {
        let name = d["name"].as_str().unwrap_or("");
        rows.push_str(&format!(
            r#"<tr><td><a href="/console/dbs/{u}">{n}</a></td><td><code>{id}</code></td><td>{c}</td></tr>"#,
            u = url_seg(name),
            n = esc(name),
            id = esc(d["id"].as_str().unwrap_or("")),
            c = d["created_at"]
        ));
    }
    if rows.is_empty() {
        rows = r#"<tr><td colspan="3" class="muted">No databases yet.</td></tr>"#.into();
    }
    let flash = flash_of(&q);
    page(
        "Databases",
        Some(&n),
        flash.as_ref().map(|(m, ok)| (m.as_str(), *ok)),
        &format!(
            r#"<h1>Databases</h1><div class="card"><table><tr><th>Name</th><th>Id</th><th>Created (ms)</th></tr>{rows}</table></div>
<div class="card"><form class="row" method="post" action="/console/dbs">{csrf}<input name="name" placeholder="new-database" pattern="[a-z0-9][a-z0-9_-]*" required><button>Create database</button></form></div>"#,
            csrf = hidden_csrf(&n)
        ),
    )
    .into_response()
}

async fn create_db(State(node): State<Arc<Node>>, headers: HeaderMap, Form(f): F) -> Response {
    let n = match nav(&node, &headers).await {
        Ok(n) => n,
        Err(r) => return r,
    };
    if let Err(r) = check_csrf(&node, &n, &f) {
        return r;
    }
    let (st, j) = api(&node, &n.token, "POST", "/v1/dbs", Some(json!({"name": f.get("name")}))).await;
    if st == 201 { back("/console/dbs", "Database created.", true) } else { back("/console/dbs", &err_msg(&j), false) }
}

async fn delete_db(State(node): State<Arc<Node>>, headers: HeaderMap, Path(db): Path<String>, Form(f): F) -> Response {
    let n = match nav(&node, &headers).await {
        Ok(n) => n,
        Err(r) => return r,
    };
    if let Err(r) = check_csrf(&node, &n, &f) {
        return r;
    }
    if f.get("confirm").map(|s| s.as_str()) != Some(db.as_str()) {
        return back(&format!("/console/dbs/{}", url_seg(&db)), "Type the database name to confirm deletion.", false);
    }
    let (st, j) = api(&node, &n.token, "DELETE", &format!("/v1/dbs/{}", url_seg(&db)), None).await;
    if st == 200 {
        back("/console/dbs", "Database deleted.", true)
    } else {
        back(&format!("/console/dbs/{}", url_seg(&db)), &err_msg(&j), false)
    }
}

async fn db_page(State(node): State<Arc<Node>>, headers: HeaderMap, Path(db): Path<String>, q: Q) -> Response {
    let n = match nav(&node, &headers).await {
        Ok(n) => n,
        Err(r) => return r,
    };
    let base = format!("/v1/dbs/{}", url_seg(&db));
    let (st, info) = api(&node, &n.token, "GET", &base, None).await;
    if st != 200 {
        return back("/console/dbs", &err_msg(&info), false);
    }
    let prefix = q.get("prefix").cloned().unwrap_or_default();
    let mut scan = format!("{base}/kv?limit=50&prefix={}", url_seg(&prefix));
    if let Some(c) = q.get("cursor") {
        scan.push_str(&format!("&cursor={}", url_seg(c)));
    }
    let (_, kv) = api(&node, &n.token, "GET", &scan, None).await;
    let mut kv_rows = String::new();
    for it in kv["items"].as_array().into_iter().flatten() {
        let key = it["key"]
            .as_str()
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("(base64) {}", it["key_b64"].as_str().unwrap_or("")));
        let val = if let Some(v) = it["value"].as_str() {
            let short: String = v.chars().take(200).collect();
            format!("<code>{}</code>{}", esc(&short), if v.chars().count() > 200 { "…" } else { "" })
        } else if it["blob"].is_object() {
            cell(&it["blob"])
        } else {
            r#"<span class="muted">(binary)</span>"#.into()
        };
        kv_rows.push_str(&format!(
            r#"<tr><td><code>{k}</code></td><td>{val}</td><td>{v}</td><td><form method="post" action="/console/dbs/{d}/kv/delete">{csrf}<input type="hidden" name="key" value="{k}"><button class="link">delete</button></form></td></tr>"#,
            k = esc(&key),
            v = it["version"],
            d = url_seg(&db),
            csrf = hidden_csrf(&n),
        ));
    }
    if kv_rows.is_empty() {
        kv_rows = r#"<tr><td colspan="4" class="muted">No keys.</td></tr>"#.into();
    }
    let more = kv["next_cursor"]
        .as_str()
        .map(|c| {
            format!(
                r#"<p><a href="/console/dbs/{}?prefix={}&cursor={}">Next page →</a></p>"#,
                url_seg(&db),
                url_seg(&prefix),
                url_seg(c)
            )
        })
        .unwrap_or_default();
    let (_, tables) = api(&node, &n.token, "GET", &format!("{base}/tables"), None).await;
    let mut trows = String::new();
    for t in tables["tables"].as_array().into_iter().flatten() {
        let name = t["name"].as_str().unwrap_or("");
        trows.push_str(&format!(
            r#"<tr><td><a href="/console/dbs/{d}/tables/{u}">{n}</a></td><td>{c}</td><td>{i}</td></tr>"#,
            d = url_seg(&db),
            u = url_seg(name),
            n = esc(name),
            c = t["columns"].as_array().map(|a| a.len()).unwrap_or(0),
            i = t["indexes"].as_array().map(|a| a.len()).unwrap_or(0),
        ));
    }
    if trows.is_empty() {
        trows =
            r#"<tr><td colspan="3" class="muted">No tables. Create one with the API or a transaction below.</td></tr>"#
                .into();
    }
    let txn_default =
        q.get("txn").cloned().unwrap_or_else(|| r#"{"ops": [{"op": "scan", "prefix": "", "limit": 10}]}"#.to_string());
    let txn_result = q.get("result").map(|r| format!(r#"<h2>Result</h2><pre>{}</pre>"#, esc(r))).unwrap_or_default();
    let s = &info["stats"];
    let flash = flash_of(&q);
    page(
        &db,
        Some(&n),
        flash.as_ref().map(|(m, ok)| (m.as_str(), *ok)),
        &format!(
            r#"<h1>{dbn}</h1>
<div class="card row"><span>id <code>{id}</code></span><span>version {ver}</span><span>segments {segs}</span><span>segment bytes {sb}</span><span>tail bytes {tb}</span><span class="muted">lease {lease}</span></div>
<h2>Keys</h2><div class="card"><form class="row" method="get"><input name="prefix" value="{p}" placeholder="prefix"><button>Filter</button></form>
<table><tr><th>Key</th><th>Value</th><th>Version</th><th></th></tr>{kv_rows}</table>{more}
<form class="row" method="post" action="/console/dbs/{du}/kv">{csrf}<input name="key" placeholder="key" required><input name="value" placeholder="value"><button>Put</button></form></div>
<h2>Tables</h2><div class="card"><table><tr><th>Table</th><th>Columns</th><th>Indexes</th></tr>{trows}</table></div>
<h2>Transaction</h2><div class="card"><form method="post" action="/console/dbs/{du}/txn">{csrf}<textarea name="body">{txn}</textarea><p><button>Run transaction</button></p></form>{txn_result}</div>
<h2>Danger zone</h2><div class="card"><form class="row" method="post" action="/console/dbs/{du}/delete">{csrf}<input name="confirm" placeholder="type {dbn} to confirm"><button class="danger">Delete database</button></form></div>"#,
            dbn = esc(&db),
            du = url_seg(&db),
            id = esc(info["id"].as_str().unwrap_or("")),
            ver = s["version"],
            segs = s["segments"],
            sb = s["segment_bytes"],
            tb = s["tail_bytes"],
            lease = esc(s["lease_holder"].as_str().unwrap_or("none")),
            p = esc(&prefix),
            csrf = hidden_csrf(&n),
            txn = esc(&txn_default),
        ),
    )
    .into_response()
}

async fn put_kv(State(node): State<Arc<Node>>, headers: HeaderMap, Path(db): Path<String>, Form(f): F) -> Response {
    let n = match nav(&node, &headers).await {
        Ok(n) => n,
        Err(r) => return r,
    };
    if let Err(r) = check_csrf(&node, &n, &f) {
        return r;
    }
    let body =
        json!({"ops": [{"op": "put", "key": f.get("key"), "value": f.get("value").cloned().unwrap_or_default()}]});
    let (st, j) = api(&node, &n.token, "POST", &format!("/v1/dbs/{}/txn", url_seg(&db)), Some(body)).await;
    let path = format!("/console/dbs/{}", url_seg(&db));
    if st == 200 {
        back(&path, &format!("Saved at version {}.", j["version"]), true)
    } else {
        back(&path, &err_msg(&j), false)
    }
}

async fn delete_kv(State(node): State<Arc<Node>>, headers: HeaderMap, Path(db): Path<String>, Form(f): F) -> Response {
    let n = match nav(&node, &headers).await {
        Ok(n) => n,
        Err(r) => return r,
    };
    if let Err(r) = check_csrf(&node, &n, &f) {
        return r;
    }
    let body = json!({"ops": [{"op": "delete", "key": f.get("key")}]});
    let (st, j) = api(&node, &n.token, "POST", &format!("/v1/dbs/{}/txn", url_seg(&db)), Some(body)).await;
    let path = format!("/console/dbs/{}", url_seg(&db));
    if st == 200 { back(&path, "Deleted.", true) } else { back(&path, &err_msg(&j), false) }
}

async fn run_txn(State(node): State<Arc<Node>>, headers: HeaderMap, Path(db): Path<String>, Form(f): F) -> Response {
    let n = match nav(&node, &headers).await {
        Ok(n) => n,
        Err(r) => return r,
    };
    if let Err(r) = check_csrf(&node, &n, &f) {
        return r;
    }
    let text = f.get("body").cloned().unwrap_or_default();
    let path = format!("/console/dbs/{}", url_seg(&db));
    let parsed: Json = match serde_json::from_str(&text) {
        Ok(j) => j,
        Err(e) => return back(&format!("{path}?txn={}", url_seg(&text)), &format!("Invalid JSON: {e}"), false),
    };
    let (_, j) = api(&node, &n.token, "POST", &format!("/v1/dbs/{}/txn", url_seg(&db)), Some(parsed)).await;
    let pretty = serde_json::to_string_pretty(&j).unwrap_or_default();
    let shown: String = pretty.chars().take(6000).collect();
    Redirect::to(&format!("{path}?txn={}&result={}", url_seg(&text), url_seg(&shown))).into_response()
}

async fn table_page(
    State(node): State<Arc<Node>>,
    headers: HeaderMap,
    Path((db, t)): Path<(String, String)>,
    q: Q,
) -> Response {
    let n = match nav(&node, &headers).await {
        Ok(n) => n,
        Err(r) => return r,
    };
    let base = format!("/v1/dbs/{}/tables/{}", url_seg(&db), url_seg(&t));
    let (st, schema) = api(&node, &n.token, "GET", &base, None).await;
    if st != 200 {
        return back(&format!("/console/dbs/{}", url_seg(&db)), &err_msg(&schema), false);
    }
    let mut query = json!({"limit": 50});
    if let Some(c) = q.get("cursor") {
        query["cursor"] = json!(c);
    }
    let (_, rows) = api(&node, &n.token, "POST", &format!("{base}/rows/query"), Some(query)).await;
    let cols: Vec<String> = schema["columns"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|c| c["name"].as_str().map(|s| s.to_string()))
        .collect();
    let head: String = cols.iter().map(|c| format!("<th>{}</th>", esc(c))).collect();
    let mut body = String::new();
    for r in rows["rows"].as_array().into_iter().flatten() {
        body.push_str("<tr>");
        for c in &cols {
            body.push_str(&format!("<td>{}</td>", cell(&r[c])));
        }
        body.push_str("</tr>");
    }
    let more = rows["next_cursor"]
        .as_str()
        .map(|c| format!(r#"<p><a href="?cursor={}">Next page →</a></p>"#, url_seg(c)))
        .unwrap_or_default();
    let mut sch = String::new();
    for c in schema["columns"].as_array().into_iter().flatten() {
        sch.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            esc(c["name"].as_str().unwrap_or("")),
            esc(c["type"].as_str().unwrap_or("")),
            if c["nullable"] == true { "yes" } else { "no" },
            c.get("default").map(cell).unwrap_or_default()
        ));
    }
    let idx: String = schema["indexes"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|i| {
            format!(
                "<li>{} ({}){}{}</li>",
                esc(i["name"].as_str().unwrap_or("")),
                esc(&i["columns"].to_string()),
                if i["unique"] == true { " unique" } else { "" },
                if i["state"] == "building" { " (building)" } else { "" }
            )
        })
        .collect();
    page(
        &t,
        Some(&n),
        None,
        &format!(
            r#"<p><a href="/console/dbs/{du}">← {dbn}</a></p><h1>{tn}</h1>
<div class="card"><table><tr><th>Column</th><th>Type</th><th>Nullable</th><th>Default</th></tr>{sch}</table>
<p class="muted">Primary key: {pk}</p><ul>{idx}</ul></div>
<h2>Rows</h2><div class="card"><table><tr>{head}</tr>{body}</table>{more}</div>"#,
            du = url_seg(&db),
            dbn = esc(&db),
            tn = esc(&t),
            pk = esc(&schema["primary_key"].to_string()),
        ),
    )
    .into_response()
}

// -------------------------------------------------------------- keys

async fn keys_page(State(node): State<Arc<Node>>, headers: HeaderMap, q: Q) -> Response {
    let n = match nav(&node, &headers).await {
        Ok(n) => n,
        Err(r) => return r,
    };
    let (_, j) = api(&node, &n.token, "GET", "/v1/api-keys", None).await;
    let mut rows = String::new();
    for k in j["api_keys"].as_array().into_iter().flatten() {
        rows.push_str(&format!(
            r#"<tr><td>{name}</td><td><code>{id}</code></td><td>{db}</td><td>{ro}</td><td><form method="post" action="/console/keys/{id}/revoke">{csrf}<button class="link">revoke</button></form></td></tr>"#,
            name = esc(k["name"].as_str().unwrap_or("")),
            id = esc(k["id"].as_str().unwrap_or("")),
            db = esc(k["db"].as_str().unwrap_or("all databases")),
            ro = if k["read_only"] == true { "read-only" } else { "read-write" },
            csrf = hidden_csrf(&n),
        ));
    }
    if rows.is_empty() {
        rows = r#"<tr><td colspan="5" class="muted">No API keys.</td></tr>"#.into();
    }
    let created = q
        .get("key")
        .map(|k| format!(r#"<div class="flash ok">New key (shown once): <code>{}</code></div>"#, esc(k)))
        .unwrap_or_default();
    let flash = flash_of(&q);
    page(
        "API keys",
        Some(&n),
        flash.as_ref().map(|(m, ok)| (m.as_str(), *ok)),
        &format!(
            r#"<h1>API keys</h1>{created}<div class="card"><table><tr><th>Name</th><th>Id</th><th>Database</th><th>Access</th><th></th></tr>{rows}</table></div>
<div class="card"><form class="row" method="post" action="/console/keys">{csrf}<input name="name" placeholder="name" required><input name="db" placeholder="database (optional)">
<label><input type="checkbox" name="read_only" value="1"> read-only</label><button>Create key</button></form></div>"#,
            csrf = hidden_csrf(&n)
        ),
    )
    .into_response()
}

async fn create_key(State(node): State<Arc<Node>>, headers: HeaderMap, Form(f): F) -> Response {
    let n = match nav(&node, &headers).await {
        Ok(n) => n,
        Err(r) => return r,
    };
    if let Err(r) = check_csrf(&node, &n, &f) {
        return r;
    }
    let db = f.get("db").filter(|d| !d.trim().is_empty()).cloned();
    let body = json!({"name": f.get("name"), "db": db, "read_only": f.contains_key("read_only")});
    let (st, j) = api(&node, &n.token, "POST", "/v1/api-keys", Some(body)).await;
    if st == 201 {
        // The secret is shown once; it is not stored anywhere.
        Redirect::to(&format!("/console/keys?key={}", url_seg(j["key"].as_str().unwrap_or("")))).into_response()
    } else {
        back("/console/keys", &err_msg(&j), false)
    }
}

async fn revoke_key(State(node): State<Arc<Node>>, headers: HeaderMap, Path(id): Path<String>, Form(f): F) -> Response {
    let n = match nav(&node, &headers).await {
        Ok(n) => n,
        Err(r) => return r,
    };
    if let Err(r) = check_csrf(&node, &n, &f) {
        return r;
    }
    let (st, j) = api(&node, &n.token, "DELETE", &format!("/v1/api-keys/{}", url_seg(&id)), None).await;
    if st == 200 { back("/console/keys", "Key revoked.", true) } else { back("/console/keys", &err_msg(&j), false) }
}

// ------------------------------------------------------------- admin

async fn admin_page(State(node): State<Arc<Node>>, headers: HeaderMap, q: Q) -> Response {
    let n = match nav(&node, &headers).await {
        Ok(n) => n,
        Err(r) => return r,
    };
    let (st, users) = api(&node, &n.token, "GET", "/v1/admin/users", None).await;
    if st != 200 {
        return back("/console/dbs", &err_msg(&users), false);
    }
    let (_, nodes) = api(&node, &n.token, "GET", "/v1/admin/nodes", None).await;
    let mut urows = String::new();
    for u in users["users"].as_array().into_iter().flatten() {
        let disabled = u["disabled"] == true;
        urows.push_str(&format!(
            r#"<tr><td>{e}</td><td><code>{id}</code></td><td>{a}</td><td>{g}</td><td>{d}</td><td><form method="post" action="/console/admin/users/{id}">{csrf}<input type="hidden" name="disabled" value="{nd}"><button class="link">{act}</button></form></td></tr>"#,
            e = esc(u["email"].as_str().unwrap_or("")),
            id = esc(u["id"].as_str().unwrap_or("")),
            a = if u["is_admin"] == true { "admin" } else { "" },
            g = if u["google_linked"] == true { "Google" } else { "" },
            d = if disabled { "disabled" } else { "active" },
            nd = !disabled,
            act = if disabled { "enable" } else { "disable" },
            csrf = hidden_csrf(&n),
        ));
    }
    let mut nrows = String::new();
    for x in nodes["nodes"].as_array().into_iter().flatten() {
        nrows.push_str(&format!(
            "<tr><td><code>{}</code></td><td>{}</td><td>{}</td><td>{}</td></tr>",
            esc(x["id"].as_str().unwrap_or("")),
            esc(x["addr"].as_str().unwrap_or("")),
            if x["live"] == true { "live" } else { "down" },
            x["heartbeat_at"]
        ));
    }
    let flash = flash_of(&q);
    page(
        "Admin",
        Some(&n),
        flash.as_ref().map(|(m, ok)| (m.as_str(), *ok)),
        &format!(
            r#"<h1>Administration</h1><h2>Users</h2><div class="card"><table><tr><th>Email</th><th>Id</th><th>Role</th><th>Sign-in</th><th>Status</th><th></th></tr>{urows}</table></div>
<h2>Cluster</h2><div class="card"><p class="muted">This node: <code>{this}</code></p><table><tr><th>Node</th><th>Address</th><th>State</th><th>Last heartbeat (ms)</th></tr>{nrows}</table></div>"#,
            this = esc(nodes["this_node"].as_str().unwrap_or(""))
        ),
    )
    .into_response()
}

async fn admin_user(State(node): State<Arc<Node>>, headers: HeaderMap, Path(id): Path<String>, Form(f): F) -> Response {
    let n = match nav(&node, &headers).await {
        Ok(n) => n,
        Err(r) => return r,
    };
    if let Err(r) = check_csrf(&node, &n, &f) {
        return r;
    }
    let disabled = f.get("disabled").map(|s| s == "true").unwrap_or(true);
    let (st, j) = api(
        &node,
        &n.token,
        "POST",
        &format!("/v1/admin/users/{}/disable", url_seg(&id)),
        Some(json!({"disabled": disabled})),
    )
    .await;
    if st == 200 {
        back("/console/admin", if disabled { "User disabled." } else { "User enabled." }, true)
    } else {
        back("/console/admin", &err_msg(&j), false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escaping() {
        assert_eq!(esc(r#"<a href="x">'&'</a>"#), "&lt;a href=&quot;x&quot;&gt;&#39;&amp;&#39;&lt;/a&gt;");
        assert_eq!(url_seg("a b/c"), "a%20b%2Fc");
    }
}
