//! Authentication, account, API key, admin and internal routes.

use super::{Auth, ClientAddr, JsonBody, Params, SESSION_COOKIE, cookie, credential};
use crate::auth;
use crate::error::{Error, Result};
use crate::meta::{self, ApiKey, MetaCmd};
use crate::node::{Credential, Node};
use axum::Router;
use axum::extract::{Path, State};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use http::{HeaderMap, HeaderValue, StatusCode};
use serde::Deserialize;
use serde_json::{Value as Json, json};
use std::sync::Arc;

pub fn routes() -> Router<Arc<Node>> {
    Router::new()
        .route("/v1/auth/signup", post(signup))
        .route("/v1/auth/login", post(login))
        .route("/v1/auth/logout", post(logout))
        .route("/v1/auth/google/start", get(google_start))
        .route("/v1/auth/google/callback", get(google_callback))
        .route("/v1/me", get(me))
        .route("/v1/api-keys", get(list_keys).post(create_key))
        .route("/v1/api-keys/{id}", delete(delete_key))
        .route("/v1/admin/users", get(admin_users))
        .route("/v1/admin/users/{id}/disable", post(admin_disable))
        .route("/v1/admin/nodes", get(admin_nodes))
}

fn session_cookie(node: &Node, token: &str, max_age_s: u64) -> HeaderValue {
    let secure = if node.cfg.secure_cookies { "; Secure" } else { "" };
    HeaderValue::from_str(&format!(
        "{SESSION_COOKIE}={token}; Path=/; HttpOnly; SameSite=Lax; Max-Age={max_age_s}{secure}"
    ))
    .unwrap_or_else(|_| HeaderValue::from_static(""))
}

fn with_cookie(mut r: Response, c: HeaderValue) -> Response {
    r.headers_mut().append(http::header::SET_COOKIE, c);
    r
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credentials {
    email: String,
    password: String,
}

async fn signup(
    State(node): State<Arc<Node>>,
    client: ClientAddr,
    JsonBody(c): JsonBody<Credentials>,
) -> Result<Response> {
    let (email, r) = node.password_signup(&client.0, &c.email, &c.password).await?;
    let user_id = r["user_id"].as_str().unwrap_or_default().to_string();
    let token = node.create_session(&user_id).await?;
    let body = json!({
        "token": token,
        "user": {"id": user_id, "email": email, "is_admin": r["is_admin"]},
    });
    let ttl = node.cfg.session_ttl_ms / 1000;
    Ok(with_cookie((StatusCode::CREATED, axum::Json(body)).into_response(), session_cookie(&node, &token, ttl)))
}

async fn login(
    State(node): State<Arc<Node>>,
    client: ClientAddr,
    JsonBody(c): JsonBody<Credentials>,
) -> Result<Response> {
    let user = node.password_login(&client.0, &c.email, &c.password).await?;
    let token = node.create_session(&user.id).await?;
    let ttl = node.cfg.session_ttl_ms / 1000;
    Ok(with_cookie(
        axum::Json(json!({"token": token, "user": user.public_json()})).into_response(),
        session_cookie(&node, &token, ttl),
    ))
}

async fn logout(State(node): State<Arc<Node>>, headers: HeaderMap) -> Result<Response> {
    let token = credential(&headers).ok_or_else(|| Error::Unauthenticated("missing credentials".into()))?;
    let p = node.authenticate(&token).await?;
    let Credential::Session { token_hash } = &p.cred else {
        return Err(Error::invalid("API keys cannot log out; revoke the key instead"));
    };
    node.run_meta(MetaCmd::DeleteSession { token_hash: token_hash.clone() }).await?;
    node.forget_credential(&token);
    Ok(with_cookie(axum::Json(json!({})).into_response(), session_cookie(&node, "", 0)))
}

fn safe_return_to(s: Option<&String>) -> String {
    match s {
        Some(r) if r.starts_with('/') && !r.starts_with("//") && !r.contains('\\') => r.clone(),
        _ => "/console".into(),
    }
}

const OAUTH_COOKIE: &str = "brs_oauth";

fn redirect_uri(node: &Node) -> String {
    format!("{}/v1/auth/google/callback", node.cfg.public_url.trim_end_matches('/'))
}

async fn google_start(State(node): State<Arc<Node>>, client: ClientAddr, q: Params) -> Result<Response> {
    let oidc = node.oidc.clone().ok_or_else(|| Error::not_found("Google sign-in is not configured"))?;
    let auth_key = format!("auth:{}", client.0);
    node.limiter.take(&[(&auth_key, node.cfg.limits.auth_per_client)], "sign-in attempts")?;
    let state = node.env.random_hex(16);
    let ret = safe_return_to(q.get("return_to"));
    let signed = auth::sign_value(&node.cookie_key(), &format!("{state}|{ret}"), node.now_ms() + 600_000);
    let url = oidc.authorize_url(&state, &redirect_uri(&node));
    let mut r = (StatusCode::FOUND, [(http::header::LOCATION, url)]).into_response();
    let secure = if node.cfg.secure_cookies { "; Secure" } else { "" };
    if let Ok(v) = HeaderValue::from_str(&format!(
        "{OAUTH_COOKIE}={signed}; Path=/v1/auth/google; HttpOnly; SameSite=Lax; Max-Age=600{secure}"
    )) {
        r.headers_mut().append(http::header::SET_COOKIE, v);
    }
    Ok(r)
}

async fn google_callback(
    State(node): State<Arc<Node>>,
    client: ClientAddr,
    headers: HeaderMap,
    q: Params,
) -> Result<Response> {
    let oidc = node.oidc.clone().ok_or_else(|| Error::not_found("Google sign-in is not configured"))?;
    let auth_key = format!("auth:{}", client.0);
    node.limiter.take(&[(&auth_key, node.cfg.limits.auth_per_client)], "sign-in attempts")?;
    let bad = |m: &str| Error::Unauthenticated(format!("Google sign-in failed: {m}"));
    if let Some(e) = q.get("error") {
        return Err(bad(e));
    }
    let signed = cookie(&headers, OAUTH_COOKIE).ok_or_else(|| bad("missing state cookie"))?;
    let payload = auth::verify_signed(&node.cookie_key(), &signed, node.now_ms()).ok_or_else(|| bad("bad state"))?;
    let (state, ret) = payload.split_once('|').ok_or_else(|| bad("bad state"))?;
    if q.get("state").map(|s| s.as_str()) != Some(state) {
        return Err(bad("state mismatch"));
    }
    let code = q.get("code").ok_or_else(|| bad("missing code"))?;
    let id_token = oidc.exchange_code(code, &redirect_uri(&node)).await?;
    let jwks = oidc.jwks().await?;
    let claims = auth::verify_id_token(&id_token, &jwks, oidc.client_id(), oidc.issuers(), node.now_ms())?;
    let r = node
        .run_meta(MetaCmd::GoogleLogin {
            new_id: node.env.random_hex(8),
            sub: claims.sub,
            email: claims.email,
            email_verified: claims.email_verified,
            policy: node.cfg.signup,
        })
        .await?;
    let user_id = r["user_id"].as_str().unwrap_or_default().to_string();
    let mut t = node.sys_read_strong().await?;
    if let Some(u) = meta::get_user(&mut t, &user_id).await?
        && u.disabled
    {
        return Err(Error::PermissionDenied("this account is disabled".into()));
    }
    let token = node.create_session(&user_id).await?;
    let ret = safe_return_to(Some(&ret.to_string()));
    let r = (StatusCode::FOUND, [(http::header::LOCATION, ret)]).into_response();
    let r = with_cookie(r, session_cookie(&node, &token, node.cfg.session_ttl_ms / 1000));
    let clear = HeaderValue::from_static("brs_oauth=; Path=/v1/auth/google; HttpOnly; Max-Age=0");
    Ok(with_cookie(r, clear))
}

async fn me(State(node): State<Arc<Node>>, Auth(p): Auth) -> Result<axum::Json<Json>> {
    let cred = match &p.cred {
        Credential::Session { .. } => json!({"type": "session"}),
        Credential::ApiKey { id, db_id, read_only, rate_limit } => {
            let db = match db_id {
                Some(d) => {
                    let mut t = node.sys_read().await?;
                    meta::get_db(&mut t, d).await?.map(|r| r.name)
                }
                None => None,
            };
            json!({"type": "api_key", "id": id, "db": db, "read_only": read_only, "rate_limit": rate_limit})
        }
    };
    Ok(axum::Json(json!({"user": p.user.public_json(), "credential": cred})))
}

async fn list_keys(State(node): State<Arc<Node>>, Auth(p): Auth) -> Result<axum::Json<Json>> {
    p.require_session()?;
    let mut t = node.sys_read_strong().await?;
    let keys = meta::list_api_keys(&mut t, &p.user.id).await?;
    let mut out = Vec::new();
    for k in keys {
        let name = match &k.db_id {
            Some(d) => meta::get_db(&mut t, d).await?.map(|r| r.name),
            None => None,
        };
        out.push(k.public_json(name.as_deref()));
    }
    Ok(axum::Json(json!({"api_keys": out})))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateKey {
    name: String,
    #[serde(default)]
    db: Option<String>,
    #[serde(default)]
    read_only: bool,
    #[serde(default)]
    rate_limit: Option<u32>,
}

async fn create_key(
    State(node): State<Arc<Node>>,
    Auth(p): Auth,
    JsonBody(c): JsonBody<CreateKey>,
) -> Result<Response> {
    p.require_session()?;
    if c.name.trim().is_empty() || c.name.len() > 100 {
        return Err(Error::invalid("key names are 1-100 characters"));
    }
    if c.rate_limit.is_some_and(|r| !(1..=1_000_000).contains(&r)) {
        return Err(Error::invalid("rate_limit is 1 to 1000000 requests per second"));
    }
    let db_id = match &c.db {
        Some(name) => Some(node.resolve_db(&p, name).await?.id),
        None => None,
    };
    let (full, id, secret_hash) = auth::new_api_key(&node.env);
    let key = ApiKey {
        id: id.clone(),
        user_id: p.user.id.clone(),
        name: c.name.clone(),
        secret_hash,
        db_id,
        read_only: c.read_only,
        rate_limit: c.rate_limit,
        created_at: node.now_ms() as i64,
    };
    node.run_meta(MetaCmd::CreateApiKey { key: key.clone() }).await?;
    Ok((StatusCode::CREATED, axum::Json(json!({"key": full, "api_key": key.public_json(c.db.as_deref())})))
        .into_response())
}

async fn delete_key(State(node): State<Arc<Node>>, Auth(p): Auth, Path(id): Path<String>) -> Result<axum::Json<Json>> {
    p.require_session()?;
    node.run_meta(MetaCmd::DeleteApiKey { id: id.clone(), user_id: p.user.id.clone() }).await?;
    node.forget_api_key(&id);
    Ok(axum::Json(json!({})))
}

async fn admin_users(State(node): State<Arc<Node>>, Auth(p): Auth) -> Result<axum::Json<Json>> {
    p.require_admin()?;
    p.require_unscoped()?;
    let mut t = node.sys_read_strong().await?;
    let users: Vec<Json> = meta::list_users(&mut t).await?.iter().map(|u| u.public_json()).collect();
    Ok(axum::Json(json!({"users": users})))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Disable {
    #[serde(default = "yes")]
    disabled: bool,
}

fn yes() -> bool {
    true
}

async fn admin_disable(
    State(node): State<Arc<Node>>,
    Auth(p): Auth,
    Path(id): Path<String>,
    JsonBody(d): JsonBody<Disable>,
) -> Result<axum::Json<Json>> {
    p.require_admin()?;
    p.require_writable()?;
    p.require_unscoped()?;
    if id == p.user.id && d.disabled {
        return Err(Error::invalid("you cannot disable your own account"));
    }
    node.run_meta(MetaCmd::SetDisabled { user_id: id.clone(), disabled: d.disabled }).await?;
    node.forget_user(&id);
    Ok(axum::Json(json!({})))
}

async fn admin_nodes(State(node): State<Arc<Node>>, Auth(p): Auth) -> Result<axum::Json<Json>> {
    p.require_admin()?;
    p.require_unscoped()?;
    node.refresh_members().await?;
    let live: Vec<String> = node.live_members().into_iter().map(|n| n.id).collect();
    let nodes: Vec<Json> = node
        .members()
        .into_iter()
        .map(|n| {
            json!({
                "id": n.id, "addr": n.addr, "started_at": n.started_at,
                "heartbeat_at": n.heartbeat_at, "live": live.contains(&n.id),
            })
        })
        .collect();
    Ok(axum::Json(json!({"nodes": nodes, "this_node": node.id})))
}

pub async fn internal_meta(
    State(node): State<Arc<Node>>,
    JsonBody(cmd): JsonBody<MetaCmd>,
) -> Result<axum::Json<Json>> {
    // Execute here; never forward again (avoids loops).
    let mut last = Error::unavailable("system database busy");
    for _ in 0..5 {
        match node.run_meta_local(&cmd).await {
            Ok(r) => return Ok(axum::Json(r)),
            Err(e @ Error::Conflict(_)) => last = e,
            Err(e) => return Err(e),
        }
    }
    Err(last)
}
