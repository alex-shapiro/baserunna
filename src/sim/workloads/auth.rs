//! `auth`: sign-up, login, sessions, logout, API keys (scoped, read-only),
//! revocation, admin powers and Google sign-in, across nodes, including
//! negative cases (tampered ID tokens, wrong state, unverified email).

use super::super::Sim;
use super::super::client::Client;
use super::super::google::{Identity, Tamper};
use serde_json::json;
use std::sync::Arc;

fn expect(cond: bool, msg: impl Into<String>) -> Result<(), String> {
    if cond { Ok(()) } else { Err(msg.into()) }
}

fn cookie_value(r: &super::super::SimResponse, name: &str) -> Option<String> {
    r.headers.get_all("set-cookie").iter().find_map(|v| {
        let s = v.to_str().ok()?;
        let (kv, _) = s.split_once(';').unwrap_or((s, ""));
        let (k, v) = kv.split_once('=')?;
        (k == name).then(|| v.to_string())
    })
}

/// Wait until every node's credential cache has expired.
async fn cache_expiry(sim: &Arc<Sim>) {
    sim.sleep(sim.cfg.node.auth_cache_ms + sim.cfg.node.sys_staleness_ms + 500).await;
}

pub async fn run(sim: Arc<Sim>) -> Result<(), String> {
    let anon = Client::new(&sim, "anon");
    // The very first account becomes an administrator.
    let admin = Client::signup(&sim, "admin", "admin@auth.test", "admin-password").await?;
    let me = admin.call_retry("GET", "/v1/me", None).await;
    expect(me.json["user"]["is_admin"] == true, format!("first user should be admin: {}", me.json))?;
    let alice = Client::signup(&sim, "alice", "Alice@Auth.test", "alice-password").await?;
    let me = alice.call_retry("GET", "/v1/me", None).await;
    expect(
        me.json["user"]["is_admin"] == false && me.json["user"]["email"] == "alice@auth.test",
        format!("alice: {}", me.json),
    )?;

    // Internal endpoints are closed to clients (no cluster key).
    let forged = json!({"cmd": "signup", "id": "evil", "email": "evil@x.io", "password_hash": null,
                        "policy": "open", "bootstrap_admin": true});
    for _ in 0..3 {
        if let Ok(r) = anon.post("/internal/v1/meta", forged.clone()).await {
            expect(r.status == 403, format!("internal endpoint open to clients: {} {}", r.status, r.json))?;
        }
    }

    // Duplicate sign-up and bad input.
    let r = anon
        .call_retry("POST", "/v1/auth/signup", Some(json!({"email": "alice@auth.test", "password": "whatever-123"})))
        .await;
    expect(r.status == 409, format!("duplicate signup: {}", r.status))?;
    let r =
        anon.call_retry("POST", "/v1/auth/signup", Some(json!({"email": "nope", "password": "whatever-123"}))).await;
    expect(r.status == 400, "bad email accepted")?;
    let r = anon.call_retry("POST", "/v1/auth/signup", Some(json!({"email": "x@y.io", "password": "short"}))).await;
    expect(r.status == 400, "short password accepted")?;

    // Login.
    let r = anon
        .call_retry("POST", "/v1/auth/login", Some(json!({"email": "alice@auth.test", "password": "wrong-password"})))
        .await;
    expect(r.status == 401, format!("wrong password: {}", r.status))?;
    let r = anon
        .call_retry("POST", "/v1/auth/login", Some(json!({"email": "ghost@auth.test", "password": "wrong-password"})))
        .await;
    expect(r.status == 401, "unknown user login")?;
    let alice2 = Client::login(&sim, "alice2", "alice@auth.test", "alice-password").await?;
    expect(alice2.token != alice.token, "sessions must differ")?;
    let r = anon.call_retry("GET", "/v1/me", None).await;
    expect(r.status == 401, "anonymous /me")?;
    let bogus = Client::with_token(&sim, "bogus", "brs_not-a-real-token");
    expect(bogus.call_retry("GET", "/v1/me", None).await.status == 401, "bogus token accepted")?;

    // Databases are private to their owner.
    alice.create_db("private").await?;
    alice.create_db("shared").await?;
    let r = admin.call_retry("GET", "/v1/dbs/private/kv/x", None).await;
    expect(r.status == 404, format!("admin resolves names in its own namespace: {}", r.status))?;
    let alice_id = me.json["user"]["id"].as_str().unwrap_or_default().to_string();
    let r = alice.call_settled("PUT", "/v1/dbs/private/kv/secret", None).await;
    expect(r.status == 200, format!("put: {}", r.status))?;
    let bob = Client::signup(&sim, "bob", "bob@auth.test", "bob-password").await?;
    expect(bob.call_retry("GET", "/v1/dbs/private/kv/secret", None).await.status == 404, "bob sees alice's db")?;
    expect(
        bob.call_retry("GET", "/v1/dbs", None).await.json["databases"].as_array().is_some_and(|a| a.is_empty()),
        "bob lists alice's dbs",
    )?;
    // Admins can address any database by id.
    let info = alice.call_retry("GET", "/v1/dbs/private", None).await;
    let db_id = info.json["id"].as_str().unwrap_or_default().to_string();
    let r = admin.call_retry("GET", &format!("/v1/dbs/id:{db_id}/kv/secret"), None).await;
    expect(r.status == 200, format!("admin by id: {}", r.status))?;
    expect(
        bob.call_retry("GET", &format!("/v1/dbs/id:{db_id}/kv/secret"), None).await.status == 403,
        "non-admin by id",
    )?;

    // API keys: read-only, scoped to one database.
    let r =
        alice.call_retry("POST", "/v1/api-keys", Some(json!({"name": "ro", "db": "private", "read_only": true}))).await;
    expect(r.status == 201, format!("create key: {}", r.json))?;
    let ro = Client::with_token(&sim, "ro-key", r.json["key"].as_str().unwrap_or_default());
    let ro_id = r.json["api_key"]["id"].as_str().unwrap_or_default().to_string();
    expect(ro.call_retry("GET", "/v1/dbs/private/kv/secret", None).await.status == 200, "ro key read")?;
    expect(ro.call_retry("PUT", "/v1/dbs/private/kv/x", None).await.status == 403, "ro key wrote")?;
    let r = ro
        .call_retry("POST", "/v1/dbs/private/txn", Some(json!({"ops": [{"op": "put", "key": "k", "value": "v"}]})))
        .await;
    expect(r.status == 403, "ro key txn write")?;
    expect(ro.call_retry("GET", "/v1/dbs/shared/kv/x", None).await.status == 403, "scoped key reached other db")?;
    let l = ro.call_retry("GET", "/v1/dbs", None).await;
    expect(l.json["databases"].as_array().map(|a| a.len()) == Some(1), format!("scoped list: {}", l.json))?;
    expect(
        ro.call_retry("POST", "/v1/dbs", Some(json!({"name": "nope"}))).await.status == 403,
        "scoped key created db",
    )?;
    expect(ro.call_retry("POST", "/v1/api-keys", Some(json!({"name": "x"}))).await.status == 403, "key minted a key")?;
    let me = ro.call_retry("GET", "/v1/me", None).await;
    expect(
        me.json["credential"]["type"] == "api_key" && me.json["credential"]["db"] == "private",
        format!("me as key: {}", me.json),
    )?;
    // Unscoped read-write key.
    let r = alice.call_retry("POST", "/v1/api-keys", Some(json!({"name": "rw"}))).await;
    expect(r.status == 201, format!("create rw key: {} {}", r.status, r.json))?;
    let rw = Client::with_token(&sim, "rw-key", r.json["key"].as_str().unwrap_or_default());
    expect(rw.call_settled("PUT", "/v1/dbs/shared/kv/k", None).await.status == 200, "rw key write")?;
    let keys = alice.call_retry("GET", "/v1/api-keys", None).await;
    // Key creation is not idempotent: a retried request after a lost
    // response may create a second key with the same name.
    let names: std::collections::BTreeSet<String> = keys.json["api_keys"]
        .as_array()
        .map(|a| a.iter().filter_map(|k| k["name"].as_str().map(|s| s.to_string())).collect())
        .unwrap_or_default();
    expect(names == ["ro".to_string(), "rw".to_string()].into(), format!("list keys: {}", keys.json))?;
    expect(!keys.json.to_string().contains("secret_hash"), "key listing leaks hashes")?;

    // Revocation reaches every node within the cache TTL.
    let r = alice.call_settled("DELETE", &format!("/v1/api-keys/{ro_id}"), None).await;
    // A retried revoke may find the key already gone.
    expect(r.status == 200 || r.status == 404, format!("revoke: {}", r.json))?;
    cache_expiry(&sim).await;
    for _ in 0..4 {
        expect(ro.call_retry("GET", "/v1/dbs/private/kv/secret", None).await.status == 401, "revoked key still works")?;
    }

    // Logout.
    let r = alice2.call_retry("POST", "/v1/auth/logout", Some(json!({}))).await;
    // A retried logout finds the session already gone.
    expect(r.status == 200 || r.status == 401, format!("logout: {} {}", r.status, r.json))?;
    cache_expiry(&sim).await;
    expect(alice2.call_retry("GET", "/v1/me", None).await.status == 401, "logged-out session works")?;
    expect(alice.call_retry("GET", "/v1/me", None).await.status == 200, "other session revoked")?;

    // Admin powers.
    expect(alice.call_retry("GET", "/v1/admin/users", None).await.status == 403, "non-admin listed users")?;
    let users = admin.call_retry("GET", "/v1/admin/users", None).await;
    expect(users.json["users"].as_array().map(|a| a.len()) == Some(3), format!("users: {}", users.json))?;
    expect(!users.json.to_string().contains("pbkdf2"), "user listing leaks password hashes")?;
    let nodes = admin.call_retry("GET", "/v1/admin/nodes", None).await;
    expect(nodes.status == 200 && nodes.json["nodes"].as_array().is_some_and(|a| !a.is_empty()), "nodes")?;
    let r = admin
        .call_settled("POST", &format!("/v1/admin/users/{alice_id}/disable"), Some(json!({"disabled": true})))
        .await;
    expect(r.status == 200, format!("disable: {}", r.json))?;
    cache_expiry(&sim).await;
    expect(alice.call_retry("GET", "/v1/me", None).await.status == 403, "disabled user active")?;
    expect(rw.call_retry("GET", "/v1/dbs/shared/kv/k", None).await.status == 403, "disabled user's key active")?;
    let r = anon
        .call_retry("POST", "/v1/auth/login", Some(json!({"email": "alice@auth.test", "password": "alice-password"})))
        .await;
    expect(r.status == 403, "disabled login")?;
    admin.call_settled("POST", &format!("/v1/admin/users/{alice_id}/disable"), Some(json!({"disabled": false}))).await;
    cache_expiry(&sim).await;
    expect(alice.call_retry("GET", "/v1/me", None).await.status == 200, "re-enabled user")?;

    google(&sim).await
}

async fn google_flow(
    sim: &Arc<Sim>,
    id: Identity,
    state_override: Option<&str>,
) -> Result<(u16, Option<String>), String> {
    let c = Client::new(sim, "browser");
    let r = loop {
        match c.send("GET", "/v1/auth/google/start?return_to=/console/dbs", &[], bytes::Bytes::new()).await {
            Ok(r) if r.status == 302 => break r,
            Ok(r) => return Err(format!("start: {}", r.status)),
            Err(_) => {}
        }
    };
    let loc = r.headers.get("location").and_then(|v| v.to_str().ok()).unwrap_or_default().to_string();
    let state = loc.split("state=").nth(1).and_then(|s| s.split('&').next()).unwrap_or_default().to_string();
    let oauth = cookie_value(&r, "brs_oauth").ok_or("no state cookie")?;
    let code = sim.google.issue_code(id);
    let st = state_override.unwrap_or(&state);
    let path = format!("/v1/auth/google/callback?code={code}&state={st}");
    match c.send("GET", &path, &[("cookie", format!("brs_oauth={oauth}"))], bytes::Bytes::new()).await {
        // The single-use code may have been consumed: restart the flow.
        Ok(r) if r.status == 503 => Ok((0, None)),
        Ok(r) => {
            let loc = r.headers.get("location").and_then(|v| v.to_str().ok()).map(|s| s.to_string());
            if r.status == 302 {
                if loc.as_deref() != Some("/console/dbs") {
                    return Err(format!("bad redirect {loc:?}"));
                }
                return Ok((302, cookie_value(&r, "brs_session")));
            }
            Ok((r.status, None))
        }
        // A lost callback consumed the single-use code; the browser
        // would restart the flow. Report as a failure to sign in.
        Err(_) => Ok((0, None)),
    }
}

async fn google(sim: &Arc<Sim>) -> Result<(), String> {
    let ident = |sub: &str, email: &str, verified: bool| Identity {
        sub: sub.into(),
        email: email.into(),
        email_verified: verified,
        tamper: Tamper::None,
    };
    // New user via Google.
    let mut tries = 0;
    let session = loop {
        tries += 1;
        match google_flow(sim, ident("g-1", "Gina@Gmail.test", true), None).await? {
            (302, Some(s)) => break s,
            (0, _) if tries < 50 => continue,
            other => return Err(format!("google sign-in failed: {other:?}")),
        }
    };
    let g = Client::with_token(sim, "gina", &session);
    let me = g.call_retry("GET", "/v1/me", None).await;
    expect(
        me.json["user"]["email"] == "gina@gmail.test" && me.json["user"]["google_linked"] == true,
        format!("google me: {}", me.json),
    )?;
    // Same Google account again: same user.
    let s2 = loop {
        match google_flow(sim, ident("g-1", "gina@gmail.test", true), None).await? {
            (302, Some(s)) => break s,
            (0, _) => continue,
            other => return Err(format!("second google sign-in: {other:?}")),
        }
    };
    let me2 = Client::with_token(sim, "gina2", &s2).call_retry("GET", "/v1/me", None).await;
    expect(me2.json["user"]["id"] == me.json["user"]["id"], "second google login made a new user")?;
    // Verified email of a password account links it.
    let s3 = loop {
        match google_flow(sim, ident("g-bob", "bob@auth.test", true), None).await? {
            (302, Some(s)) => break s,
            (0, _) => continue,
            other => return Err(format!("linking google sign-in: {other:?}")),
        }
    };
    let me3 = Client::with_token(sim, "bob-g", &s3).call_retry("GET", "/v1/me", None).await;
    expect(
        me3.json["user"]["email"] == "bob@auth.test" && me3.json["user"]["has_password"] == true,
        format!("link: {}", me3.json),
    )?;
    // Negative cases.
    for t in [Tamper::WrongAudience, Tamper::Expired, Tamper::BadSignature, Tamper::WrongIssuer] {
        let (st, s) = google_flow(sim, Identity { tamper: t, ..ident("g-evil", "evil@x.test", true) }, None).await?;
        expect(s.is_none() && (st == 401 || st == 0), format!("tampered token {t:?} accepted: {st}"))?;
    }
    let (st, s) = google_flow(sim, ident("g-2", "new@x.test", true), Some("forged-state")).await?;
    expect(s.is_none() && (st == 401 || st == 0), format!("forged state accepted: {st}"))?;
    let (st, s) = google_flow(sim, ident("g-3", "unverified@x.test", false), None).await?;
    expect(s.is_none() && (st == 403 || st == 0), format!("unverified email accepted: {st}"))?;
    Ok(())
}
