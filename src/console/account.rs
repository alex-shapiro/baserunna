//! Account, API key, and administration console pages.

use super::{
    Flash, Page, Pager, Params, Posted, Session, back, esc, field, page, stay, table, url_seg, when, with_session,
};
use crate::node::Node;
use axum::Router;
use axum::extract::Path;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use http::{HeaderValue, header};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

pub fn routes() -> Router<Arc<Node>> {
    Router::new()
        .route("/console/account", get(account_page))
        .route("/console/account/password", post(change_password))
        .route("/console/account/verify", post(send_verification))
        .route("/console/account/sessions/others/revoke", post(revoke_others))
        .route("/console/account/sessions/{id}/revoke", post(revoke_session))
        .route("/console/account/delete", post(delete_account))
        .route("/console/keys", get(keys_page).post(create_key))
        .route("/console/keys/{id}/revoke", post(revoke_key))
        .route("/console/admin", get(admin_page))
        .route("/console/admin/users/{id}/disable", post(disable_user))
        .route("/console/admin/users/{id}/enable", post(enable_user))
}

#[derive(Deserialize)]
struct User {
    id: String,
    email: String,
    email_verified: bool,
    is_admin: bool,
    disabled: bool,
    has_password: bool,
    google_linked: bool,
    created_at: u64,
}

#[derive(Deserialize)]
struct Me {
    user: User,
}

#[derive(Deserialize)]
struct SignedIn {
    id: String,
    created_at: u64,
    expires_at: u64,
    client: Option<String>,
    method: String,
    current: bool,
}

#[derive(Deserialize)]
struct Sessions {
    sessions: Vec<SignedIn>,
    next_cursor: Option<String>,
}

#[derive(Deserialize)]
struct Key {
    id: String,
    name: String,
    db: Option<String>,
    read_only: bool,
}

#[derive(Deserialize)]
struct Keys {
    api_keys: Vec<Key>,
    next_cursor: Option<String>,
}

#[derive(Deserialize)]
struct Users {
    users: Vec<User>,
    next_cursor: Option<String>,
}

#[derive(Deserialize)]
struct ClusterNode {
    id: String,
    addr: String,
    heartbeat_at: u64,
    live: bool,
}

#[derive(Deserialize)]
struct Nodes {
    nodes: Vec<ClusterNode>,
    this_node: String,
}

async fn account_page(s: Session, q: Params) -> Page {
    let me: Me = s.get("/v1/me").await.map_err(|e| s.fail("Account", &e))?;
    let u = me.user;
    let p = Pager::of(&q, "cursor");
    let sessions: Sessions =
        s.get(&format!("/v1/me/sessions?{}", p.api_query())).await.map_err(|e| s.fail("Account", &e))?;
    let rows: Vec<String> = sessions
        .sessions
        .iter()
        .map(|x| {
            let action = s.button(
                &format!("/console/account/sessions/{}/revoke", url_seg(&x.id)),
                if x.current { "sign out" } else { "revoke" },
                false,
            );
            format!(
                r#"<tr><td><code>{id}</code>{cur}</td><td>{m}</td><td>{c}</td><td>{at}</td><td>{exp}</td><td>{action}</td></tr>"#,
                id = esc(&x.id),
                cur = if x.current { r#" <span class="muted">(this one)</span>"# } else { "" },
                m = esc(&x.method),
                c = esc(x.client.as_deref().unwrap_or("—")),
                at = when(x.created_at),
                exp = when(x.expires_at),
            )
        })
        .collect();
    let verified = if u.email_verified {
        r#"<span class="muted">verified</span>"#.to_string()
    } else {
        format!(
            r#"<span class="muted">not verified</span> {}"#,
            s.button("/console/account/verify", "send a verification link", false)
        )
    };
    let (current, confirm) = if u.has_password {
        (
            r#"<input name="current_password" type="password" placeholder="Current password" required autocomplete="current-password">"#,
            r#"<input name="password" type="password" placeholder="Password" required autocomplete="current-password">"#,
        )
    } else {
        (r#"<span class="muted">Your account signs in with Google; you may add a password.</span>"#, "")
    };
    let csrf = s.csrf_input();
    Ok(page(
        "Account",
        Some(&s),
        Flash::of(&q).as_ref(),
        &format!(
            r#"<h1>Account</h1><div class="card"><p>{email} {verified}</p><p class="muted">Member since {since}</p></div>
<h2>Password</h2><div class="card"><form class="row" method="post" action="/console/account/password">{csrf}{current}
<input name="new_password" type="password" placeholder="New password (8+ characters)" required minlength="8" autocomplete="new-password"><button>Change password</button></form>
<p class="muted">Changing your password signs out every other session.</p></div>
<h2>Sessions</h2><div class="card">{table}{pager}
<form class="row" method="post" action="/console/account/sessions/others/revoke">{csrf}<button>Sign out everywhere else</button></form></div>
<h2>Delete account</h2><div class="card"><p class="muted">Deletes your account, sessions, API keys, memberships and databases.</p>
<form class="row" method="post" action="/console/account/delete">{csrf}<input name="confirm" placeholder="type your email to confirm">{confirm}<button class="danger">Delete account</button></form></div>"#,
            email = esc(&u.email),
            since = when(u.created_at),
            table = table(&["Session", "Signed in with", "From", "Since", "Expires", ""], &rows, "No sessions."),
            pager = p.links("/console/account", sessions.next_cursor.as_deref()),
        ),
    ))
}

async fn change_password(Posted { s, f }: Posted) -> Page {
    let body = json!({"current_password": f.get("current_password"), "new_password": field(&f, "new_password")});
    let done = "Password changed. Your other sessions are signed out.";
    s.act("POST", "/v1/me/password", Some(body), "/console/account", stay("/console/account", done)).await
}

async fn send_verification(Posted { s, .. }: Posted) -> Page {
    let done = stay("/console/account", "A verification link is on its way.");
    s.act("POST", "/v1/me/verify-email", None, "/console/account", done).await
}

async fn revoke_others(Posted { s, .. }: Posted) -> Page {
    s.act("DELETE", "/v1/me/sessions", None, "/console/account", |j| {
        ("/console/account".into(), format!("Signed out {} other sessions.", j["revoked"]))
    })
    .await
}

async fn revoke_session(Path(id): Path<String>, Posted { s, .. }: Posted) -> Page {
    Ok(match s.api("DELETE", &format!("/v1/me/sessions/{}", url_seg(&id)), None).await {
        Ok(j) if j["current"] == true => with_session(&s.node, None, "/console/login"),
        Ok(_) => back("/console/account", "Session revoked.", true),
        Err(e) => back("/console/account", &e, false),
    })
}

async fn delete_account(Posted { s, f }: Posted) -> Page {
    if field(&f, "confirm") != s.email {
        return Ok(back("/console/account", "Type your email address to confirm.", false));
    }
    Ok(match s.api("DELETE", "/v1/me", Some(json!({"password": f.get("password")}))).await {
        Ok(_) => with_session(&s.node, None, "/console/login?ok=Your%20account%20is%20deleted."),
        Err(e) => back("/console/account", &e, false),
    })
}

async fn keys_page(s: Session, q: Params) -> Page {
    render_keys(&s, &Pager::of(&q, "cursor"), None, Flash::of(&q)).await
}

async fn render_keys(s: &Session, p: &Pager, created: Option<&str>, flash: Option<Flash>) -> Page {
    let keys: Keys = s.get(&format!("/v1/api-keys?{}", p.api_query())).await.map_err(|e| s.fail("API keys", &e))?;
    let rows: Vec<String> = keys
        .api_keys
        .iter()
        .map(|k| {
            format!(
                r#"<tr><td>{name}</td><td><code>{id}</code></td><td>{db}</td><td>{ro}</td><td>{revoke}</td></tr>"#,
                name = esc(&k.name),
                id = esc(&k.id),
                db = esc(k.db.as_deref().unwrap_or("all databases")),
                ro = if k.read_only { "read-only" } else { "read-write" },
                revoke = s.button(&format!("/console/keys/{}/revoke", url_seg(&k.id)), "revoke", true),
            )
        })
        .collect();
    let created = created
        .map(|k| format!(r#"<div class="flash ok">New key (shown once): <code>{}</code></div>"#, esc(k)))
        .unwrap_or_default();
    Ok(page(
        "API keys",
        Some(s),
        flash.as_ref(),
        &format!(
            r#"<h1>API keys</h1>{created}<div class="card">{table}{pager}</div>
<div class="card"><form class="row" method="post" action="/console/keys">{csrf}<input name="name" placeholder="name" required><input name="db" placeholder="database (optional: name or org.name)">
<label><input type="checkbox" name="read_only" value="1"> read-only</label><button>Create key</button></form></div>"#,
            table = table(&["Name", "Id", "Database", "Access", ""], &rows, "No API keys."),
            pager = p.links("/console/keys", keys.next_cursor.as_deref()),
            csrf = s.csrf_input()
        ),
    ))
}

/// Renders the new key's secret directly in an uncached response so it never
/// appears in a redirect URL.
async fn create_key(Posted { s, f }: Posted) -> Page {
    let db = f.get("db").filter(|d| !d.trim().is_empty());
    let body = json!({"name": field(&f, "name"), "db": db, "read_only": f.contains_key("read_only")});
    let key = match s.api("POST", "/v1/api-keys", Some(body)).await {
        Ok(j) => j["key"].as_str().unwrap_or_default().to_string(),
        Err(e) => return Ok(back("/console/keys", &e, false)),
    };
    let mut r = render_keys(&s, &Pager::of(&Default::default(), "cursor"), Some(&key), None).await?;
    r.headers_mut().insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    Ok(r.into_response())
}

async fn revoke_key(Path(id): Path<String>, Posted { s, .. }: Posted) -> Page {
    let done = stay("/console/keys", "Key revoked.");
    s.act("DELETE", &format!("/v1/api-keys/{}", url_seg(&id)), None, "/console/keys", done).await
}

async fn admin_page(s: Session, q: Params) -> Page {
    let p = Pager::of(&q, "cursor");
    let users: Users =
        s.get(&format!("/v1/admin/users?{}", p.api_query())).await.map_err(|e| back("/console/dbs", &e, false))?;
    let nodes: Nodes = s.get("/v1/admin/nodes").await.map_err(|e| s.fail("Admin", &e))?;
    let urows: Vec<String> = users
        .users
        .iter()
        .map(|u| {
            let action = if u.disabled {
                s.button(&format!("/console/admin/users/{}/enable", url_seg(&u.id)), "re-enable", false)
            } else {
                s.button(&format!("/console/admin/users/{}/disable", url_seg(&u.id)), "disable", true)
            };
            format!(
                r#"<tr><td>{e}</td><td><code>{id}</code></td><td>{a}</td><td>{g}</td><td>{d}</td><td>{action}</td></tr>"#,
                e = esc(&u.email),
                id = esc(&u.id),
                a = if u.is_admin { "admin" } else { "" },
                g = if u.google_linked { "Google" } else { "" },
                d = if u.disabled { "disabled" } else { "active" },
            )
        })
        .collect();
    let nrows: Vec<String> = nodes
        .nodes
        .iter()
        .map(|x| {
            format!(
                "<tr><td><code>{}</code></td><td>{}</td><td>{}</td><td>{}</td></tr>",
                esc(&x.id),
                esc(&x.addr),
                if x.live { "live" } else { "down" },
                when(x.heartbeat_at)
            )
        })
        .collect();
    Ok(page(
        "Admin",
        Some(&s),
        Flash::of(&q).as_ref(),
        &format!(
            r#"<h1>Administration</h1><h2>Users</h2><div class="card">{users}{pager}</div>
<h2>Cluster</h2><div class="card"><p class="muted">This node: <code>{this}</code></p>{nodes}</div>"#,
            users = table(&["Email", "Id", "Role", "Sign-in", "Status", ""], &urows, "No users."),
            pager = p.links("/console/admin", users.next_cursor.as_deref()),
            this = esc(&nodes.this_node),
            nodes = table(&["Node", "Address", "State", "Last heartbeat"], &nrows, "No nodes."),
        ),
    ))
}

async fn set_disabled(s: Session, id: &str, disabled: bool) -> Page {
    let done = stay("/console/admin", if disabled { "User disabled." } else { "User re-enabled." });
    let path = format!("/v1/admin/users/{}/disable", url_seg(id));
    s.act("POST", &path, Some(json!({"disabled": disabled})), "/console/admin", done).await
}

async fn disable_user(Path(id): Path<String>, Posted { s, .. }: Posted) -> Page {
    set_disabled(s, &id, true).await
}

async fn enable_user(Path(id): Path<String>, Posted { s, .. }: Posted) -> Page {
    set_disabled(s, &id, false).await
}
