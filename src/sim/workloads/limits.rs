//! Simulation workload for request rate limiting and authentication failure backoff.

use super::super::client::{Client, Resp};
use super::super::{Sim, SimConfig};
use super::expect;
use crate::limits::{LimitConfig, Rate};
use serde_json::{Value as Json, json};
use std::sync::Arc;

pub fn config() -> SimConfig {
    let mut c = SimConfig::basic();
    c.nodes = 2;
    c.node.limits = LimitConfig {
        user: Rate { per_sec: 10.0, burst: 20.0 },
        api_key: Rate::per_sec(100.0),
        auth_per_client: Rate { per_sec: 1.0, burst: 6.0 },
        signup_per_client: Rate::per_hour(60.0, 3.0),
        account_free_failures: 3,
        client_free_failures: 6,
        failure_backoff_ms: 1_000,
        failure_backoff_max_ms: 8_000,
        ..c.node.limits
    };
    c
}

/// Creates a client pinned to node 0 with automatic 429 retry disabled.
fn raw(sim: &Arc<Sim>, name: &str, token: Option<&str>) -> Client {
    let mut c = Client::new(sim, name);
    c.token = token.map(str::to_string);
    c.pin = Some(0);
    c.retry_rate_limited = false;
    c
}

async fn call(c: &Client, method: &str, path: &str, body: Option<Json>) -> Result<Resp, String> {
    let r = c.call(method, path, body).await.map_err(|e| format!("{method} {path}: {e:?}"))?;
    if r.status == 429 {
        let after = r.raw.headers.get("retry-after").and_then(|v| v.to_str().ok()?.parse::<u64>().ok());
        expect(
            r.code() == "rate_limited" && r.json["error"]["retryable"] == true && after.is_some_and(|s| s >= 1),
            format!("bad 429: {:?} {}", r.raw.headers, r.json),
        )?;
    }
    Ok(r)
}

fn login(email: &str, password: &str) -> Option<Json> {
    Some(json!({"email": email, "password": password}))
}

pub async fn run(sim: Arc<Sim>) -> Result<(), String> {
    let mut owner = Client::signup(&sim, "owner", "owner@limits.test", "owner-password").await?;
    owner.pin = Some(0);
    owner.create_db("app").await?;

    let r = owner.call_retry("POST", "/v1/api-keys", Some(json!({"name": "slow", "rate_limit": 2}))).await;
    expect(r.status == 201 && r.json["api_key"]["rate_limit"] == 2, format!("create key: {}", r.json))?;
    let key = raw(&sim, "slow-key", r.json["key"].as_str());
    let started = sim.now_ms();
    let mut acked = Vec::new();
    for i in 0..20 {
        let path = format!("/v1/dbs/app/kv/k{i:02}");
        let r = key.send("PUT", &path, &[], bytes::Bytes::from_static(b"v")).await.map_err(|e| format!("{e:?}"))?;
        match r.status {
            200 => acked.push(format!("k{i:02}")),
            429 => {}
            s => return Err(format!("PUT {path}: {s}")),
        }
    }
    let secs = (sim.now_ms() - started) as f64 / 1000.0;
    expect(
        acked.len() >= 4 && acked.len() as f64 <= 4.0 + 2.0 * secs + 1.0,
        format!("{} of 20 writes admitted in {secs:.2} s at 2/s with a burst of 4", acked.len()),
    )?;
    let r = owner.call_retry("GET", "/v1/dbs/app/kv?limit=100", None).await;
    let stored: Vec<&str> =
        r.json["items"].as_array().into_iter().flatten().filter_map(|i| i["key"].as_str()).collect();
    expect(stored == acked, format!("stored {stored:?}, acknowledged {acked:?}"))?;
    let mut elsewhere = raw(&sim, "slow-key-elsewhere", key.token.as_deref());
    elsewhere.pin = Some(1);
    expect(call(&elsewhere, "GET", "/v1/me", None).await?.status == 200, "the key is limited on another node")?;

    let mut busy = Client::signup(&sim, "busy", "busy@limits.test", "busy-password").await?;
    busy.pin = Some(0);
    busy.retry_rate_limited = false;
    let mut limited = 0;
    for _ in 0..40 {
        limited += (call(&busy, "GET", "/v1/me", None).await?.status == 429) as usize;
    }
    expect((15..=25).contains(&limited), format!("{limited} of 40 requests refused, burst 20"))?;
    expect(call(&raw(&sim, "o2", owner.token.as_deref()), "GET", "/v1/me", None).await?.status == 200, "owner")?;

    Client::signup(&sim, "bob", "bob@limits.test", "bob-password").await?;
    let bob = raw(&sim, "bob-login", None);
    let pace = || sim.sleep(1_100);
    for i in 0..4 {
        pace().await;
        let r = call(&bob, "POST", "/v1/auth/login", login("bob@limits.test", "wrong-password")).await?;
        expect(r.status == 401, format!("wrong password {i}: {}", r.status))?;
    }
    let r = call(&bob, "POST", "/v1/auth/login", login("bob@limits.test", "bob-password")).await?;
    expect(r.status == 429, format!("signed in during backoff: {}", r.status))?;
    let r =
        call(&raw(&sim, "bob-phone", None), "POST", "/v1/auth/login", login("bob@limits.test", "bob-password")).await?;
    expect(r.status == 429, format!("backoff skipped from another address: {}", r.status))?;
    sim.sleep(1_000).await;
    let r = call(&bob, "POST", "/v1/auth/login", login("bob@limits.test", "bob-password")).await?;
    expect(r.status == 200, format!("signing in after the backoff: {} {}", r.status, r.json))?;
    pace().await;
    let r = call(&bob, "POST", "/v1/auth/login", login("bob@limits.test", "wrong-password")).await?;
    expect(r.status == 401, "a success starts the count again")?;

    let spray = raw(&sim, "sprayer", None);
    for i in 0..7 {
        pace().await;
        let r =
            call(&spray, "POST", "/v1/auth/login", login(&format!("guess{i}@limits.test"), "password-guess")).await?;
        expect(r.status == 401, format!("guess {i}: {}", r.status))?;
    }
    let r = call(&spray, "POST", "/v1/auth/login", login("owner@limits.test", "owner-password")).await?;
    expect(r.status == 429, format!("sprayer not held back: {}", r.status))?;
    let r = call(&raw(&sim, "owner-2", None), "POST", "/v1/auth/login", login("owner@limits.test", "owner-password"))
        .await?;
    expect(r.status == 200, format!("another address held back: {}", r.status))?;

    let eager = raw(&sim, "eager", None);
    let mut refused = 0;
    for _ in 0..10 {
        let r = call(&eager, "POST", "/v1/auth/login", login("nobody@limits.test", "x")).await?;
        refused += (r.status == 429) as usize;
    }
    expect(refused >= 3, format!("{refused} of 10 quick sign-ins refused"))?;

    let signups = raw(&sim, "farm", None);
    let mut created = 0;
    for i in 0..5 {
        let r =
            call(&signups, "POST", "/v1/auth/signup", login(&format!("farm{i}@limits.test"), "farm-password")).await?;
        match r.status {
            201 => created += 1,
            429 => {}
            s => return Err(format!("signup {i}: {s}")),
        }
    }
    expect(created == 3, format!("{created} sign-ups admitted, burst 3"))?;
    let admin = owner.call_retry("GET", "/v1/admin/users", None).await;
    let farmed = admin.json["users"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|u| u["email"].as_str().is_some_and(|e| e.starts_with("farm")))
        .count();
    expect(farmed == 3, format!("{farmed} accounts exist for 3 admitted sign-ups"))?;
    Ok(())
}
