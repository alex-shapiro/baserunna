//! Shared API helper steps for simulation workloads. Non-idempotent requests
//! tolerate responses explained by an earlier applied attempt.

use super::super::Sim;
use super::super::client::Client;
use super::{cache_expiry, expect};
use crate::meta::Role;
use serde_json::{Value as Json, json};
use std::collections::BTreeMap;
use std::sync::Arc;

pub const MAIL_WAIT_MS: u64 = 10_000;

pub async fn create_org(c: &Client, name: &str) -> Result<(), String> {
    let (r, _) = c.settle("POST", "/v1/orgs", Some(json!({"name": name}))).await?;
    match r.status {
        201 => Ok(()),
        409 => {
            let r = c.call_retry("GET", &format!("/v1/orgs/{name}"), None).await;
            expect(r.json["role"] == "owner", format!("{name} exists but is not ours: {}", r.json))
        }
        s => Err(format!("create org {name}: {s} {}", r.json)),
    }
}

pub async fn add_member(c: &Client, org: &str, email: &str, role: &str) -> Result<(), String> {
    let body = json!({"email": email, "role": role});
    let (r, _) = c.settle("POST", &format!("/v1/orgs/{org}/members"), Some(body)).await?;
    expect(r.status == 201 || r.status == 409, format!("add {email} to {org}: {} {}", r.status, r.json))
}

/// Lists all members of `org` by user ID and role.
pub async fn members(c: &Client, org: &str) -> Result<BTreeMap<String, Role>, String> {
    let all = c.paginate(&format!("/v1/orgs/{org}/members?limit=2"), "members").await?;
    all.iter()
        .map(|m| {
            let id = m["user_id"].as_str().ok_or(format!("member without an id: {m}"))?;
            let role = serde_json::from_value(m["role"].clone()).map_err(|e| format!("member {m}: {e}"))?;
            Ok((id.to_string(), role))
        })
        .collect()
}

/// Creates an API key with `body` and returns a client authenticated with it.
pub async fn create_key(sim: &Arc<Sim>, c: &Client, name: &str, body: Json) -> Result<(Client, Json), String> {
    let (r, _) = c.settle("POST", "/v1/api-keys", Some(body)).await?;
    expect(r.status == 201, format!("create key {name}: {} {}", r.status, r.json))?;
    let key = r.json["key"].as_str().ok_or(format!("missing key: {}", r.json))?;
    Ok((Client::with_token(sim, name, key), r.json["api_key"].clone()))
}

pub async fn status(c: &Client, path: &str) -> u16 {
    c.call_retry("GET", path, None).await.status
}

/// Waits for a background email to `to` after the first `seen` messages and
/// returns the token from the newest link to `path`.
pub async fn mailed_token(sim: &Arc<Sim>, to: &str, path: &str, seen: usize) -> Option<String> {
    for _ in 0..MAIL_WAIT_MS / 200 {
        if let Some(t) = sim.mailbox.token_after(to, path, seen) {
            return Some(t);
        }
        sim.sleep(200).await;
    }
    None
}

/// Sends `req` repeatedly until an email with a link to `link` arrives for `to`.
pub async fn request_mail(c: &Client, req: (&str, &str, Option<Json>), to: &str, link: &str) -> Result<String, String> {
    for _ in 0..20 {
        let seen = c.sim.mailbox.count(to);
        let r = c.call_settled(req.0, req.1, req.2.clone()).await;
        expect(r.status == 200, format!("{} {}: {} {}", req.0, req.1, r.status, r.json))?;
        if let Some(t) = mailed_token(&c.sim, to, link, seen).await {
            return Ok(t);
        }
    }
    Err(format!("missing mail to {to} with link {link}"))
}

pub async fn verify_email(sim: &Arc<Sim>, c: &Client, email: &str) -> Result<(), String> {
    let token = request_mail(c, ("POST", "/v1/me/verify-email", None), email, "/console/verify").await?;
    let anon = Client::new(sim, "mail-reader");
    let r = anon.call_settled("POST", "/v1/auth/verify-email", Some(json!({"token": token}))).await;
    expect(r.status == 200, format!("verify email: {} {}", r.status, r.json))?;
    cache_expiry(sim).await;
    let me = c.call_retry("GET", "/v1/me", None).await;
    expect(me.json["user"]["email_verified"] == true, format!("email still unverified: {}", me.json))
}
