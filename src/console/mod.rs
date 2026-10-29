//! Admin console: server-rendered HTML that calls the public API through the
//! node's router with the signed-in user's session. See SPEC §11.

#![allow(clippy::result_large_err)]

mod account;
mod db;
pub(crate) mod events;
pub mod multipart;
mod orgs;
mod streams;

use crate::api::{ClientAddr, SESSION_COOKIE, cookie};
use crate::auth;
use crate::meta::SignInMethod;
use crate::node::Node;
use crate::util::{b64, hmac_sha256};
use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{Form, FromRequest, FromRequestParts, Query, Request, State};
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use http::StatusCode;
use serde::de::DeserializeOwned;
use serde_json::{Value as Json, json};
use std::collections::BTreeMap;
use std::sync::Arc;
use tower::ServiceExt;

type Fields = BTreeMap<String, String>;
type Params = Query<Fields>;
type Page = Result<Response, Response>;

pub fn routes() -> Router<Arc<Node>> {
    Router::new()
        .route("/console", get(|| async { Redirect::to("/console/dbs") }))
        .route("/console/login", get(login_page).post(login_post))
        .route("/console/signup", post(signup_post))
        .route("/console/logout", post(logout_post))
        .route("/console/forgot", get(forgot_page).post(forgot_post))
        .route("/console/reset", get(reset_page).post(reset_post))
        .route("/console/verify", get(verify_page).post(verify_post))
        .merge(db::routes())
        .merge(streams::routes())
        .merge(events::routes())
        .merge(orgs::routes())
        .merge(account::routes())
}

/// HTML-escapes text for element content and attribute values.
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

/// Percent-encodes bytes for a path segment or query value.
fn url_bytes(b: &[u8]) -> String {
    b.iter()
        .map(|&b| {
            if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

fn url_seg(s: &str) -> String {
    url_bytes(s.as_bytes())
}

const CSS: &str = r#"
:root{--bg:#f7f7f5;--fg:#1d1d1b;--muted:#6b6b66;--card:#fff;--line:#e3e3de;--accent:#2f5bd3;--danger:#b4232c;--ok:#1f7a3d}
@media (prefers-color-scheme:dark){:root{--bg:#141413;--fg:#ececea;--muted:#9a9a94;--card:#1e1e1c;--line:#33332f;--accent:#7c9cf5;--danger:#f0707a;--ok:#5fc787}}
*{box-sizing:border-box}body{margin:0;font:15px/1.5 system-ui,-apple-system,Segoe UI,sans-serif;background:var(--bg);color:var(--fg)}
header{display:flex;gap:18px;align-items:center;padding:12px 20px;border-bottom:1px solid var(--line);background:var(--card);flex-wrap:wrap}
header b{margin-right:12px}header a{color:var(--fg);text-decoration:none}header a:hover{color:var(--accent)}
header form{margin-left:auto}main{max-width:1100px;margin:24px auto;padding:0 16px}
h1{font-size:22px;margin:0 0 16px}h2{font-size:17px;margin:24px 0 8px}
a{color:var(--accent);text-decoration:none}a:hover{text-decoration:underline}
.card{background:var(--card);border:1px solid var(--line);border-radius:8px;padding:16px;margin-bottom:16px;overflow-x:auto}
.card>*{margin-top:0;margin-bottom:0}.card>*+*{margin-top:14px}.card form+form{margin-top:10px}
table{border-collapse:collapse;width:100%}th,td{text-align:left;padding:8px;border-bottom:1px solid var(--line);vertical-align:middle}
th{color:var(--muted);font-weight:600;font-size:13px}td code,pre{font:13px ui-monospace,SFMono-Regular,Menlo,monospace;white-space:pre-wrap;word-break:break-all}
input,textarea,select{font:inherit;padding:6px 8px;border:1px solid var(--line);border-radius:6px;background:var(--bg);color:var(--fg)}
textarea{width:100%;min-height:140px;font-family:ui-monospace,Menlo,monospace}
button{font:inherit;padding:6px 12px;border-radius:6px;border:1px solid var(--accent);background:var(--accent);color:#fff;cursor:pointer}
button.danger{background:transparent;color:var(--danger);border-color:var(--danger)}button.link{background:none;border:none;color:var(--accent);padding:0}
button.link:hover{text-decoration:underline}button.link.danger{color:var(--danger)}
.row{display:flex;gap:8px;flex-wrap:wrap;align-items:center}.muted{color:var(--muted)}.flash{padding:10px 12px;border-radius:6px;margin-bottom:16px;border:1px solid var(--line)}
.flash.err{border-color:var(--danger);color:var(--danger)}.flash.ok{border-color:var(--ok);color:var(--ok)}
.login{max-width:380px;margin:60px auto}.login input{width:100%;margin-bottom:10px}
.stats{display:grid;grid-template-columns:repeat(auto-fill,minmax(170px,1fr));gap:12px}.stats div span{display:block;color:var(--muted);font-size:13px}
.bar{height:8px;border-radius:4px;background:var(--line);overflow:hidden;margin-top:4px}.bar i{display:block;height:100%;background:var(--accent)}
.pager{display:flex;gap:16px}form.inline{display:inline-flex;gap:6px;align-items:center;margin-right:14px}
td a+form.inline,td form.inline+a{margin-left:14px}td:last-child{white-space:nowrap}
"#;

/// Flash message passed via `?ok=` or `?err=`.
struct Flash {
    msg: String,
    ok: bool,
}

impl Flash {
    fn of(q: &Fields) -> Option<Flash> {
        let err = q.get("err").map(|m| Flash { msg: m.clone(), ok: false });
        err.or_else(|| q.get("ok").map(|m| Flash { msg: m.clone(), ok: true }))
    }

    fn err(msg: &str) -> Flash {
        Flash { msg: msg.to_string(), ok: false }
    }
}

fn page(title: &str, s: Option<&Session>, flash: Option<&Flash>, body: &str) -> Response {
    let header = match s {
        Some(s) => format!(
            r#"<header><b>Baserunna</b><a href="/console/dbs">Databases</a><a href="/console/orgs">Organizations</a><a href="/console/keys">API keys</a><a href="/console/account">Account</a>{admin}
<span class="muted">{email}</span><form method="post" action="/console/logout">{csrf}<button class="link">Sign out</button></form></header>"#,
            admin = if s.is_admin { r#"<a href="/console/admin">Admin</a>"# } else { "" },
            email = esc(&s.email),
            csrf = s.csrf_input(),
        ),
        None => "<header><b>Baserunna</b></header>".to_string(),
    };
    let flash = match flash {
        Some(f) => format!(r#"<div class="flash {}">{}</div>"#, if f.ok { "ok" } else { "err" }, esc(&f.msg)),
        None => String::new(),
    };
    Html(format!(
        r#"<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{t} · Baserunna</title><style>{CSS}</style></head><body>{header}<main>{flash}{body}</main></body></html>"#,
        t = esc(title)
    ))
    .into_response()
}

fn back(path: &str, msg: &str, ok: bool) -> Response {
    let sep = if path.contains('?') { '&' } else { '?' };
    let key = if ok { "ok" } else { "err" };
    Redirect::to(&format!("{path}{sep}{key}={}", url_seg(msg))).into_response()
}

fn stay(path: &str, msg: &str) -> impl FnOnce(&Json) -> (String, String) {
    let (path, msg) = (path.to_string(), msg.to_string());
    move |_| (path, msg)
}

fn field<'a>(f: &'a Fields, k: &str) -> &'a str {
    f.get(k).map_or("", |v| v.as_str())
}

fn table(headers: &[&str], rows: &[String], empty: &str) -> String {
    let head: String = headers.iter().map(|h| format!("<th>{h}</th>")).collect();
    let body = if rows.is_empty() {
        format!(r#"<tr><td colspan="{}" class="muted">{empty}</td></tr>"#, headers.len())
    } else {
        rows.concat()
    };
    format!("<table><tr>{head}</tr>{body}</table>")
}

struct Reply {
    status: StatusCode,
    body: Json,
}

impl Reply {
    async fn of(resp: Response) -> Reply {
        let status = resp.status();
        let bytes = http_body_util::BodyExt::collect(resp.into_body()).await.map(|b| b.to_bytes()).unwrap_or_default();
        Reply { status, body: serde_json::from_slice(&bytes).unwrap_or(Json::Null) }
    }

    fn into_result(self) -> Result<Json, String> {
        if self.status.is_success() {
            Ok(self.body)
        } else {
            Err(self.body["error"]["message"]
                .as_str()
                .map_or_else(|| format!("request failed ({})", self.status), |m| m.to_string()))
        }
    }
}

/// Authenticated console session extracted from the session cookie; redirects
/// unauthenticated requests to `/console/login`.
struct Session {
    node: Arc<Node>,
    token: String,
    user_id: String,
    email: String,
    is_admin: bool,
    csrf: String,
}

impl FromRequestParts<Arc<Node>> for Session {
    type Rejection = Response;

    async fn from_request_parts(parts: &mut http::request::Parts, node: &Arc<Node>) -> Result<Session, Response> {
        let sign_in = || Redirect::to("/console/login").into_response();
        let token = cookie(&parts.headers, SESSION_COOKIE).filter(|t| !t.is_empty()).ok_or_else(sign_in)?;
        match node.authenticate(&token).await {
            Ok(p) if p.is_session() => Ok(Session {
                csrf: csrf_for(node, &token),
                node: node.clone(),
                user_id: p.user.id.clone(),
                email: p.user.email.clone(),
                is_admin: p.user.is_admin,
                token,
            }),
            Err(e) if e.retryable() => Err(e.into_response()),
            _ => Err(sign_in()),
        }
    }
}

/// Form submission authenticated with a valid session and CSRF token.
struct Posted {
    s: Session,
    f: Fields,
}

impl FromRequest<Arc<Node>> for Posted {
    type Rejection = Response;

    async fn from_request(req: Request, node: &Arc<Node>) -> Result<Posted, Response> {
        let (mut parts, body) = req.into_parts();
        let s = Session::from_request_parts(&mut parts, node).await?;
        let Form(f) = Form::<Fields>::from_request(Request::from_parts(parts, body), node)
            .await
            .map_err(IntoResponse::into_response)?;
        s.check_csrf(f.get("csrf").map(|c| c.as_str()))?;
        Ok(Posted { s, f })
    }
}

fn csrf_for(node: &Node, token: &str) -> String {
    b64::encode_url(&hmac_sha256(&node.cookie_key(), format!("csrf:{}", auth::secret_hash(token)).as_bytes())[..16])
}

impl Session {
    fn csrf_input(&self) -> String {
        format!(r#"<input type="hidden" name="csrf" value="{}">"#, esc(&self.csrf))
    }

    fn check_csrf(&self, token: Option<&str>) -> Result<(), Response> {
        if token.is_some_and(|c| crate::util::ct_eq(c.as_bytes(), self.csrf.as_bytes())) {
            Ok(())
        } else {
            Err((StatusCode::FORBIDDEN, "invalid CSRF token").into_response())
        }
    }

    fn button(&self, action: &str, label: &str, danger: bool) -> String {
        format!(
            r#"<form class="inline" method="post" action="{a}">{csrf}<button class="link{d}">{label}</button></form>"#,
            a = esc(action),
            csrf = self.csrf_input(),
            d = if danger { " danger" } else { "" },
        )
    }

    /// Calls the public API as this user, retrying transient errors that have
    /// not taken effect.
    async fn raw(&self, method: &str, path: &str, content_type: Option<&str>, body: Bytes) -> Response {
        let Some(router) = self.node.router() else {
            return (StatusCode::INTERNAL_SERVER_ERROR, "router unavailable").into_response();
        };
        let mut attempt = 0;
        loop {
            let auth = format!("Bearer {}", self.token);
            let mut b = http::Request::builder().method(method).uri(path).header("authorization", auth);
            if let Some(ct) = content_type {
                b = b.header("content-type", ct);
            }
            if let Some(rid) = crate::log::request_id() {
                b = b.header(crate::log::REQUEST_ID_HEADER, rid.as_ref());
            }
            let Ok(req) = b.body(Body::from(body.clone())) else {
                return (StatusCode::BAD_REQUEST, "bad request").into_response();
            };
            let resp = match router.clone().oneshot(req).await {
                Ok(r) => r,
                Err(e) => match e {},
            };
            let transient = matches!(resp.status(), StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE);
            if !transient || attempt == RETRIES {
                return resp;
            }
            let (parts, b) = resp.into_parts();
            let bytes = http_body_util::BodyExt::collect(b).await.map(|b| b.to_bytes()).unwrap_or_default();
            let err: Json = serde_json::from_slice(&bytes).unwrap_or(Json::Null);
            if err["error"]["details"]["outcome"] == "unknown" {
                return Response::from_parts(parts, Body::from(bytes));
            }
            attempt += 1;
            let wait = err["error"]["details"]["retry_after_ms"].as_u64().unwrap_or(100 << attempt);
            tokio::time::sleep(std::time::Duration::from_millis(wait.min(2_000))).await;
        }
    }

    async fn call(&self, method: &str, path: &str, body: Option<Json>) -> Reply {
        let (ct, body) = match body {
            Some(j) => (Some("application/json"), Bytes::from(j.to_string())),
            None => (None, Bytes::new()),
        };
        Reply::of(self.raw(method, path, ct, body).await).await
    }

    async fn api(&self, method: &str, path: &str, body: Option<Json>) -> Result<Json, String> {
        self.call(method, path, body).await.into_result()
    }

    async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, String> {
        let j = self.api("GET", path, None).await?;
        serde_json::from_value(j).map_err(|e| format!("unexpected answer from {path}: {e}"))
    }

    fn fail(&self, title: &str, msg: &str) -> Response {
        page(title, Some(self), Some(&Flash::err(msg)), "")
    }

    /// Executes an API request and redirects using `done` on success or back to
    /// `from` with the error on failure.
    async fn act(
        &self,
        method: &str,
        api_path: &str,
        body: Option<Json>,
        from: &str,
        done: impl FnOnce(&Json) -> (String, String),
    ) -> Page {
        Ok(match self.api(method, api_path, body).await {
            Ok(j) => {
                let (to, msg) = done(&j);
                back(&to, &msg, true)
            }
            Err(e) => back(from, &e, false),
        })
    }
}

const RETRIES: u32 = 4;
const PAGE: usize = 50;

struct Pager {
    limit: usize,
    /// Query parameter name for this listing's cursor.
    param: &'static str,
    cursor: Option<String>,
}

impl Pager {
    fn of(q: &Fields, param: &'static str) -> Pager {
        let max = crate::meta::PAGE_MAX;
        let limit = q.get("limit").and_then(|l| l.parse().ok()).filter(|l| (1..=max).contains(l)).unwrap_or(PAGE);
        Pager { limit, param, cursor: q.get(param).filter(|c| !c.is_empty()).cloned() }
    }

    fn api_query(&self) -> String {
        match &self.cursor {
            Some(c) => format!("limit={}&cursor={}", self.limit, url_seg(c)),
            None => format!("limit={}", self.limit),
        }
    }

    fn links(&self, base: &str, next: Option<&str>) -> String {
        let sep = if base.contains('?') { "&amp;" } else { "?" };
        let mut out = String::new();
        if self.cursor.is_some() {
            out.push_str(&format!(r#"<a href="{}{sep}limit={}">First page</a>"#, esc(base), self.limit));
        }
        if let Some(c) = next {
            out.push_str(&format!(
                r#"<a href="{}{sep}limit={}&amp;{}={}">Next page →</a>"#,
                esc(base),
                self.limit,
                self.param,
                url_seg(c)
            ));
        }
        if out.is_empty() { out } else { format!(r#"<div class="pager">{out}</div>"#) }
    }
}

fn format_bytes(n: u64) -> String {
    match n {
        n if n < 1024 => format!("{n} B"),
        n if n < 1 << 20 => format!("{:.1} KiB", n as f64 / 1024.0),
        n if n < 1 << 30 => format!("{:.1} MiB", n as f64 / (1u64 << 20) as f64),
        n => format!("{:.1} GiB", n as f64 / (1u64 << 30) as f64),
    }
}

/// A duration in the largest unit that states it exactly, such as `30 min`.
fn span(ms: u64) -> String {
    let units = [(86_400_000, "d"), (3_600_000, "h"), (60_000, "min"), (1000, "s")];
    match units.iter().find(|(u, _)| ms > 0 && ms.is_multiple_of(*u)) {
        Some((u, name)) => format!("{} {name}", ms / u),
        None => format!("{ms} ms"),
    }
}

fn when(ms: impl TryInto<i64>) -> String {
    crate::log::rfc3339(ms.try_into().unwrap_or(i64::MAX))[..19].replace('T', " ")
}

async fn login_page(State(node): State<Arc<Node>>, q: Params) -> Response {
    let google = if node.services.oidc.is_some() {
        r#"<p><a href="/v1/auth/google/start?return_to=/console/dbs">Sign in with Google</a></p>"#
    } else {
        ""
    };
    let forgot = if node.services.mailer.is_some() {
        r#"<p><a href="/console/forgot">Forgot your password?</a></p>"#
    } else {
        ""
    };
    page(
        "Sign in",
        None,
        Flash::of(&q).as_ref(),
        &format!(
            r#"<div class="login card"><h1>Sign in</h1>
<form method="post" action="/console/login"><input name="email" type="email" placeholder="Email" required autocomplete="username">
<input name="password" type="password" placeholder="Password" required autocomplete="current-password"><button>Sign in</button></form>
{forgot}{google}<h2>New here?</h2><form method="post" action="/console/signup"><input name="email" type="email" placeholder="Email" required>
<input name="password" type="password" placeholder="Password (8+ characters)" required minlength="8"><button>Create account</button></form></div>"#
        ),
    )
}

fn with_session(node: &Node, token: Option<&str>, to: &str) -> Response {
    let cookie = match token {
        Some(t) => crate::api::session_cookie(node, t, node.cfg.session_ttl_ms / 1000),
        None => crate::api::session_cookie(node, "", 0),
    };
    let mut r = Redirect::to(to).into_response();
    r.headers_mut().append(http::header::SET_COOKIE, cookie);
    r
}

async fn login_post(State(node): State<Arc<Node>>, client: ClientAddr, Form(f): Form<Fields>) -> Response {
    let session = async {
        let user = node.password_login(&client.0, field(&f, "email"), field(&f, "password")).await?;
        node.create_session(&user.id, Some(&client.0), SignInMethod::Password).await
    };
    match session.await {
        Ok(t) => with_session(&node, Some(&t), "/console/dbs"),
        Err(e) => back("/console/login", &e.message(), false),
    }
}

async fn signup_post(State(node): State<Arc<Node>>, client: ClientAddr, Form(f): Form<Fields>) -> Response {
    let session = async {
        let user = node.password_signup(&client.0, field(&f, "email"), field(&f, "password")).await?;
        node.create_session(&user.id, Some(&client.0), SignInMethod::Signup).await
    };
    match session.await {
        Ok(t) => with_session(&node, Some(&t), "/console/dbs"),
        Err(e) => back("/console/login", &e.message(), false),
    }
}

async fn logout_post(Posted { s, .. }: Posted) -> Page {
    Ok(match s.api("POST", "/v1/auth/logout", Some(json!({}))).await {
        Ok(_) => with_session(&s.node, None, "/console/login"),
        Err(e) => back("/console/account", &format!("Could not sign out: {e}"), false),
    })
}

async fn forgot_page(q: Params) -> Response {
    page(
        "Reset your password",
        None,
        Flash::of(&q).as_ref(),
        r#"<div class="login card"><h1>Reset your password</h1><p class="muted">We will email you a link to choose a new password.</p>
<form method="post" action="/console/forgot"><input name="email" type="email" placeholder="Email" required><button>Send link</button></form>
<p><a href="/console/login">Back to sign in</a></p></div>"#,
    )
}

async fn forgot_post(State(node): State<Arc<Node>>, client: ClientAddr, Form(f): Form<Fields>) -> Response {
    match node.request_password_reset(&client.0, field(&f, "email")).await {
        Ok(()) => back("/console/login", "If an account has this email, a reset link is on its way.", true),
        Err(e) => back("/console/forgot", &e.message(), false),
    }
}

async fn reset_page(q: Params) -> Response {
    page(
        "Choose a new password",
        None,
        Flash::of(&q).as_ref(),
        &format!(
            r#"<div class="login card"><h1>Choose a new password</h1><p class="muted">Every other session of your account will be signed out.</p>
<form method="post" action="/console/reset"><input type="hidden" name="token" value="{t}">
<input name="password" type="password" placeholder="New password (8+ characters)" required minlength="8" autocomplete="new-password"><button>Set password</button></form></div>"#,
            t = esc(field(&q, "token"))
        ),
    )
}

async fn reset_post(State(node): State<Arc<Node>>, client: ClientAddr, Form(f): Form<Fields>) -> Response {
    let token = field(&f, "token");
    match node.reset_password(&client.0, token, field(&f, "password")).await {
        Ok((_, session)) => with_session(&node, Some(&session), "/console/dbs"),
        Err(e) => back(&format!("/console/reset?token={}", url_seg(token)), &e.message(), false),
    }
}

async fn verify_page(q: Params) -> Response {
    page(
        "Verify your email",
        None,
        Flash::of(&q).as_ref(),
        &format!(
            r#"<div class="login card"><h1>Verify your email address</h1>
<form method="post" action="/console/verify"><input type="hidden" name="token" value="{t}"><button>Verify my address</button></form></div>"#,
            t = esc(field(&q, "token"))
        ),
    )
}

async fn verify_post(
    State(node): State<Arc<Node>>,
    client: ClientAddr,
    signed_in: Result<Session, Response>,
    Form(f): Form<Fields>,
) -> Response {
    let token = field(&f, "token");
    if let Err(e) = node.verify_email(&client.0, token).await {
        return back(&format!("/console/verify?token={}", url_seg(token)), &e.message(), false);
    }
    let to = if signed_in.is_ok() { "/console/account" } else { "/console/login" };
    back(to, "Your email address is verified.", true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escaping() {
        assert_eq!(esc(r#"<a href="x">'&'</a>"#), "&lt;a href=&quot;x&quot;&gt;&#39;&amp;&#39;&lt;/a&gt;");
        assert_eq!(url_seg("a b/c"), "a%20b%2Fc");
        assert_eq!(url_bytes(&[0xff, b'.']), "%FF.");
    }

    #[test]
    fn pagers_keep_their_size() {
        let q: Fields = [("limit", "2"), ("c", "abc")].map(|(k, v)| (k.into(), v.into())).into();
        let p = Pager::of(&q, "c");
        assert_eq!(p.api_query(), "limit=2&cursor=abc");
        let l = p.links("/console/x?a=1", Some("n+1"));
        assert!(l.contains(r#"href="/console/x?a=1&amp;limit=2""#) && l.contains("limit=2&amp;c=n%2B1"), "{l}");
        assert_eq!(Pager::of(&Fields::new(), "c").links("/x", None), "");
        assert_eq!(Pager::of(&[("limit".to_string(), "0".to_string())].into(), "c").limit, PAGE);
    }

    #[test]
    fn units() {
        assert_eq!(format_bytes(1023), "1023 B");
        assert_eq!(format_bytes(1536), "1.5 KiB");
        assert_eq!(format_bytes(3 << 20), "3.0 MiB");
        assert_eq!(when(0), "1970-01-01 00:00:00");
    }

    #[test]
    fn tables_fill_empty_listings() {
        let empty = table(&["A", "B"], &[], "none");
        assert_eq!(
            empty,
            r#"<table><tr><th>A</th><th>B</th></tr><tr><td colspan="2" class="muted">none</td></tr></table>"#
        );
        assert!(table(&["A"], &["<tr><td>x</td></tr>".into()], "none").ends_with("<tr><td>x</td></tr></table>"));
    }

    #[test]
    fn replies() {
        let ok = Reply { status: StatusCode::CREATED, body: json!({"a": 1}) };
        assert_eq!(ok.into_result(), Ok(json!({"a": 1})));
        let err = Reply { status: StatusCode::FORBIDDEN, body: json!({"error": {"message": "no"}}) };
        assert_eq!(err.into_result(), Err("no".to_string()));
        let bare = Reply { status: StatusCode::BAD_GATEWAY, body: Json::Null };
        assert!(bare.into_result().unwrap_err().contains("502"));
    }
}
