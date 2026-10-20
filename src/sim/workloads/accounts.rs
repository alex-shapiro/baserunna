//! Account self-service under faults: password changes, session listing and
//! revocation, email verification, password reset, and account deletion.

use super::super::Sim;
use super::super::client::{Client, ZOMBIE_WAIT_MS};
use super::steps::{MAIL_WAIT_MS, add_member, create_key, create_org, mailed_token, members, request_mail, status};
use super::{cache_expiry, expect};
use serde_json::{Value as Json, json};
use std::collections::BTreeSet;
use std::sync::Arc;

pub async fn run(sim: Arc<Sim>) -> Result<(), String> {
    let admin = super::admin(&sim).await?;
    let phase = Some(super::chaos_phase(&sim));
    let r = async {
        password_change(&sim).await?;
        sessions(&sim).await?;
        reset(&sim, &admin).await?;
        verification(&sim).await?;
        deletion(&sim, &admin).await
    }
    .await;
    let deleted = r?;
    super::settle(&sim, phase).await;
    super::check_integrity(&sim).await?;
    super::check_purged(&sim, &deleted).await
}

async fn password_change(sim: &Arc<Sim>) -> Result<(), String> {
    let (email, old, new) = ("pw@acct.test", "first-password", "second-password");
    let a = Client::signup(sim, "pw-a", email, old).await?;
    let b = Client::login(sim, "pw-b", email, old).await?;
    let r = a
        .call_retry("POST", "/v1/me/password", Some(json!({"current_password": "wrong-password", "new_password": new})))
        .await;
    expect(r.status == 403, format!("wrong current password: {} {}", r.status, r.json))?;
    let r =
        a.call_retry("POST", "/v1/me/password", Some(json!({"current_password": old, "new_password": "short"}))).await;
    expect(r.status == 400, format!("short password: {}", r.status))?;
    let r = a.call_retry("POST", "/v1/me/password", Some(json!({"new_password": new}))).await;
    expect(r.status == 400, format!("change without the current password: {}", r.status))?;
    let body = json!({"current_password": old, "new_password": new});
    let (r, ambiguous) = a.settle("POST", "/v1/me/password", Some(body)).await?;
    expect(r.status == 200 || (ambiguous && r.status == 403), format!("change password: {} {}", r.status, r.json))?;
    cache_expiry(sim).await;
    expect(status(&a, "/v1/me").await == 200, "the password change ended its own session")?;
    expect(status(&b, "/v1/me").await == 401, "another session outlived a password change")?;
    let anon = Client::new(sim, "pw-anon");
    let r = anon.call_retry("POST", "/v1/auth/login", Some(json!({"email": email, "password": old}))).await;
    expect(r.status == 401, format!("old password still signs in: {}", r.status))?;
    Client::login(sim, "pw-c", email, new).await.map(|_| ())
}

async fn session_ids(c: &Client, page: usize) -> Result<Vec<Json>, String> {
    c.paginate(&format!("/v1/me/sessions?limit={page}"), "sessions").await
}

async fn sessions(sim: &Arc<Sim>) -> Result<(), String> {
    let email = "sess@acct.test";
    let a = Client::signup(sim, "sess-a", email, "sess-password").await?;
    let b = Client::login(sim, "sess-b", email, "sess-password").await?;
    let c = Client::login(sim, "sess-c", email, "sess-password").await?;
    // A login whose response was lost may still create a session until its
    // commit deadline passes, as between two listings or after a revocation.
    sim.sleep(ZOMBIE_WAIT_MS).await;
    let all = session_ids(&a, 100).await?;
    let paged = session_ids(&a, 1).await?;
    let ids = |v: &[Json]| v.iter().filter_map(|s| s["id"].as_str().map(|s| s.to_string())).collect::<BTreeSet<_>>();
    expect(ids(&all) == ids(&paged) && all.len() == paged.len(), "paginated session list mismatch")?;
    expect(all.len() >= 3, format!("sessions: {all:?}"))?;
    expect(all.iter().filter(|s| s["current"] == true).count() == 1, format!("expected 1 current session: {all:?}"))?;
    expect(
        all.iter().any(|s| s["client"] == b.addr.as_str() && s["method"] == "password"),
        format!("missing client address for session b: {all:?}"),
    )?;
    let b_id = session_ids(&b, 100).await?.iter().find(|s| s["current"] == true).map(|s| s["id"].clone());
    let b_id = b_id.and_then(|s| s.as_str().map(|s| s.to_string())).ok_or("missing current session for b")?;
    let r = c.call_settled("DELETE", "/v1/me/sessions/no-such-session", None).await;
    expect(r.status == 404, format!("revoke unknown session: {}", r.status))?;
    let (r, ambiguous) = a.settle("DELETE", &format!("/v1/me/sessions/{b_id}"), None).await?;
    let revoked = (r.status == 200 && r.json["current"] == false) || (ambiguous && r.status == 404);
    expect(revoked, format!("revoke b: {} {}", r.status, r.json))?;
    cache_expiry(sim).await;
    expect(status(&b, "/v1/me").await == 401, "revoked session remained valid")?;
    expect(status(&c, "/v1/me").await == 200, "revoking session b terminated session c")?;
    let r = a.call_settled("DELETE", "/v1/me/sessions", None).await;
    expect(r.status == 200, format!("revoke other sessions: {} {}", r.status, r.json))?;
    cache_expiry(sim).await;
    expect(status(&c, "/v1/me").await == 401, "other session remained active")?;
    expect(status(&a, "/v1/me").await == 200, "revoking other sessions terminated the current session")?;
    let left = session_ids(&a, 100).await?;
    expect(left.len() == 1 && left[0]["current"] == true, format!("unexpected remaining sessions: {left:?}"))?;
    let (key, _) = create_key(sim, &a, "sess-key", json!({"name": "k"})).await?;
    expect(status(&key, "/v1/me/sessions").await == 403, "API key was allowed to list sessions")?;
    let own = left[0]["id"].as_str().ok_or(format!("session missing id: {left:?}"))?.to_string();
    let (r, ambiguous) = a.settle("DELETE", &format!("/v1/me/sessions/{own}"), None).await?;
    let revoked = (r.status == 200 && r.json["current"] == true) || (ambiguous && r.status == 401);
    expect(revoked, format!("revoke own: {} {}", r.status, r.json))?;
    cache_expiry(sim).await;
    expect(status(&a, "/v1/me").await == 401, "revoked current session remained valid")
}

async fn reset(sim: &Arc<Sim>, admin: &Client) -> Result<(), String> {
    let (email, old, new) = ("reset@acct.test", "forgotten-1", "remembered-2");
    let u = Client::signup(sim, "reset-u", email, old).await?;
    let anon = Client::new(sim, "reset-anon");
    let req = ("POST", "/v1/auth/password-reset", Some(json!({"email": "Reset@Acct.test"})));
    let token = request_mail(&anon, req, email, "/console/reset").await?;
    let r = anon.call_settled("POST", "/v1/auth/password-reset", Some(json!({"email": "nobody@acct.test"}))).await;
    expect(r.status == 200, format!("password reset for unknown account: {} {}", r.status, r.json))?;
    sim.sleep(MAIL_WAIT_MS).await;
    expect(sim.mailbox.count("nobody@acct.test") == 0, "mail sent to unknown account")?;
    let confirm = |t: &str, p: &str| json!({"token": t, "password": p});
    let r = anon.call_retry("POST", "/v1/auth/password-reset/confirm", Some(confirm(&format!("{token}x"), new))).await;
    expect(r.status == 401, format!("tampered reset token: {}", r.status))?;
    let r = anon.call_retry("POST", "/v1/auth/password-reset/confirm", Some(confirm(&token, "short"))).await;
    expect(r.status == 400, format!("short password: {}", r.status))?;
    let (r, _) = anon.settle("POST", "/v1/auth/password-reset/confirm", Some(confirm(&token, new))).await?;
    expect(r.status == 200, format!("reset: {} {}", r.status, r.json))?;
    expect(r.json["user"]["email_verified"] == true, "password reset did not mark email verified")?;
    let r = anon.call_retry("POST", "/v1/auth/password-reset/confirm", Some(confirm(&token, new))).await;
    expect(r.status == 200, format!("idempotent reset confirm: {} {}", r.status, r.json))?;
    let r = anon.call_retry("POST", "/v1/auth/password-reset/confirm", Some(confirm(&token, "third-password"))).await;
    expect(r.status == 401, format!("reset token accepted with different password: {}", r.status))?;
    cache_expiry(sim).await;
    expect(status(&u, "/v1/me").await == 401, "existing session remained active after password reset")?;
    let fresh = Client::login(sim, "reset-v", email, new).await?;
    let me = fresh.call_retry("GET", "/v1/me", None).await;
    expect(me.json["user"]["email_verified"] == true, format!("unverified after reset: {}", me.json))?;
    let id = me.json["user"]["id"].as_str().ok_or(format!("me: {}", me.json))?.to_string();
    let disable = |d: bool| Some(json!({"disabled": d}));
    let r = admin.call_settled("POST", &format!("/v1/admin/users/{id}/disable"), disable(true)).await;
    expect(r.status == 200, format!("disable: {}", r.status))?;
    let sent = sim.mailbox.count(email);
    let r = anon.call_settled("POST", "/v1/auth/password-reset", Some(json!({"email": email}))).await;
    expect(r.status == 200, format!("disabled account password reset status: {} {}", r.status, r.json))?;
    sim.sleep(MAIL_WAIT_MS).await;
    expect(sim.mailbox.count(email) == sent, "reset email sent to disabled account")?;
    let r = admin.call_settled("POST", &format!("/v1/admin/users/{id}/disable"), disable(false)).await;
    expect(r.status == 200, format!("re-enable: {}", r.status))?;
    cache_expiry(sim).await;
    expect(status(&fresh, "/v1/me").await == 200, "re-enabled account session failed")
}

async fn verification(sim: &Arc<Sim>) -> Result<(), String> {
    let email = "verify@acct.test";
    let before = sim.mailbox.count(email);
    let v = Client::signup(sim, "verify-v", email, "verify-password").await?;
    let token = match mailed_token(sim, email, "/console/verify", before).await {
        Some(t) => t,
        None => request_mail(&v, ("POST", "/v1/me/verify-email", None), email, "/console/verify").await?,
    };
    let anon = Client::new(sim, "verify-anon");
    let mut forged = token.clone();
    forged.insert(3, 'A');
    let r = anon.call_retry("POST", "/v1/auth/verify-email", Some(json!({"token": forged}))).await;
    expect(r.status == 401, format!("forged verification token: {}", r.status))?;
    let r = anon.call_settled("POST", "/v1/auth/verify-email", Some(json!({"token": token}))).await;
    expect(r.status == 200, format!("verify: {} {}", r.status, r.json))?;
    cache_expiry(sim).await;
    let me = v.call_retry("GET", "/v1/me", None).await;
    expect(me.json["user"]["email_verified"] == true, format!("email remained unverified: {}", me.json))?;
    let r = anon.call_settled("POST", "/v1/auth/verify-email", Some(json!({"token": token}))).await;
    expect(r.status == 200, format!("reused verification link: {} {}", r.status, r.json))?;
    let r = v.call_retry("POST", "/v1/me/verify-email", None).await;
    expect(r.status == 400, format!("re-verifying verified email: {}", r.status))
}

/// Returns the database IDs of the deleted account.
async fn deletion(sim: &Arc<Sim>, admin: &Client) -> Result<Vec<String>, String> {
    let (email, password) = ("deleted@acct.test", "deleted-password");
    let d = Client::signup(sim, "del-d", email, password).await?;
    let owner = Client::signup(sim, "del-owner", "keeper@acct.test", "keeper-password").await?;
    let me = d.call_retry("GET", "/v1/me", None).await;
    let d_id = me.json["user"]["id"].as_str().ok_or(format!("me: {}", me.json))?.to_string();
    for name in ["d1", "d2"] {
        d.create_db(name).await?;
        let r = d.call_settled("PUT", &format!("/v1/dbs/{name}/kv/k"), None).await;
        expect(r.status == 200, format!("write {name}: {}", r.status))?;
    }
    let mut db_ids = Vec::new();
    for name in ["d1", "d2"] {
        let info = d.call_retry("GET", &format!("/v1/dbs/{name}"), None).await;
        db_ids.push(info.json["id"].as_str().ok_or(format!("info: {}", info.json))?.to_string());
    }
    let (key, _) = create_key(sim, &d, "del-key", json!({"name": "doomed"})).await?;
    expect(status(&key, "/v1/dbs/d2/kv/k").await == 200, "API key failed before account deletion")?;
    create_org(&owner, "keep-org").await?;
    add_member(&owner, "keep-org", email, "writer").await?;
    create_org(&d, "solo-org").await?;
    let r = d.call_retry("DELETE", "/v1/me", Some(json!({"password": password}))).await;
    expect(r.status == 403, format!("sole organization owner deleted account: {}", r.status))?;
    let r = admin.call_retry("DELETE", "/v1/me", Some(json!({"password": super::ADMIN_PASSWORD}))).await;
    expect(r.status == 403, format!("last administrator deleted account: {}", r.status))?;
    let r = d.call_retry("DELETE", "/v1/me", Some(json!({"password": "wrong-password"}))).await;
    expect(r.status == 403, format!("deletion with wrong password: {}", r.status))?;
    let r = d.call_settled("DELETE", "/v1/orgs/solo-org", None).await;
    expect(r.status == 200 || r.status == 404, format!("delete org: {} {}", r.status, r.json))?;
    let (r, ambiguous) = d.settle("DELETE", "/v1/me", Some(json!({"password": password}))).await?;
    expect(r.status == 200 || (ambiguous && r.status == 401), format!("delete account: {} {}", r.status, r.json))?;
    cache_expiry(sim).await;
    expect(status(&d, "/v1/me").await == 401, "deleted account session remained valid")?;
    expect(status(&key, "/v1/me").await == 401, "deleted account API key remained valid")?;
    let anon = Client::new(sim, "del-anon");
    let r = anon.call_retry("POST", "/v1/auth/login", Some(json!({"email": email, "password": password}))).await;
    expect(r.status == 401, format!("deleted account logged in: {}", r.status))?;
    let left = members(&owner, "keep-org").await?;
    expect(!left.contains_key(&d_id), format!("deleted account remained in organization: {left:?}"))?;
    let again = Client::signup(sim, "del-again", email, "another-password").await?;
    let dbs = again.paginate("/v1/dbs", "databases").await?;
    expect(dbs.is_empty(), format!("recreated account inherited databases: {dbs:?}"))?;
    expect(status(&again, "/v1/dbs/d1/kv/k").await == 404, "recreated account accessed deleted database")?;
    Ok(db_ids)
}
