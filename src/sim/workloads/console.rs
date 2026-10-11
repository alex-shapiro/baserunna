//! `console`: drive the admin console like a browser (cookies, forms,
//! CSRF tokens) and check escaping of hostile input.

use super::super::client::Client;
use super::super::{Sim, SimResponse};
use std::sync::Arc;

struct Browser {
    c: Client,
    cookie: Option<String>,
}

fn form(pairs: &[(&str, &str)]) -> String {
    pairs
        .iter()
        .map(|(k, v)| {
            format!(
                "{}={}",
                super::super::client::url_escape(k.as_bytes()),
                super::super::client::url_escape(v.as_bytes())
            )
        })
        .collect::<Vec<_>>()
        .join("&")
}

impl Browser {
    async fn req(&mut self, method: &str, path: &str, body: Option<String>) -> Result<SimResponse, String> {
        for _ in 0..50 {
            let mut h: Vec<(&str, String)> = Vec::new();
            if let Some(c) = &self.cookie {
                h.push(("cookie", format!("brs_session={c}")));
            }
            if body.is_some() {
                h.push(("content-type", "application/x-www-form-urlencoded".into()));
            }
            let b = bytes::Bytes::from(body.clone().unwrap_or_default());
            match self.c.send(method, path, &h, b).await {
                Ok(r) if r.status == 503 => {}
                Ok(r) => {
                    for v in r.headers.get_all("set-cookie") {
                        let s = v.to_str().unwrap_or("");
                        if let Some(rest) = s.strip_prefix("brs_session=") {
                            let val = rest.split(';').next().unwrap_or("");
                            self.cookie = if val.is_empty() { None } else { Some(val.to_string()) };
                        }
                    }
                    return Ok(r);
                }
                Err(_) if method == "GET" => {}
                Err(e) => return Err(format!("{method} {path}: {e:?}")),
            }
            self.c.pause(200).await;
        }
        Err(format!("{method} {path}: never answered"))
    }

    async fn page(&mut self, path: &str) -> Result<String, String> {
        let r = self.req("GET", path, None).await?;
        if r.status != 200 {
            return Err(format!("GET {path}: {}", r.status));
        }
        Ok(String::from_utf8_lossy(&r.body).into_owned())
    }
}

fn csrf(html: &str) -> Result<String, String> {
    let i = html.find("name=\"csrf\" value=\"").ok_or("no csrf token on page")?;
    let rest = &html[i + 19..];
    Ok(rest[..rest.find('"').ok_or("bad csrf")?].to_string())
}

fn location(r: &SimResponse) -> String {
    r.headers.get("location").and_then(|v| v.to_str().ok()).unwrap_or("").to_string()
}

pub async fn run(sim: Arc<Sim>) -> Result<(), String> {
    let mut b = Browser { c: Client::new(&sim, "browser"), cookie: None };
    let r = b.req("GET", "/console/dbs", None).await?;
    if !(300..400).contains(&r.status) || location(&r) != "/console/login" {
        return Err(format!("anonymous console access not redirected: {}", r.status));
    }
    let login = b.page("/console/login").await?;
    if !login.contains("Sign in with Google") {
        return Err("google sign-in link missing".into());
    }
    let r = b
        .req("POST", "/console/signup", Some(form(&[("email", "ui@console.test"), ("password", "console-pass")])))
        .await?;
    if b.cookie.is_none() || location(&r) != "/console/dbs" {
        return Err(format!("signup did not sign in: {} {}", r.status, location(&r)));
    }
    let home = b.page("/console/dbs").await?;
    let token = csrf(&home)?;
    // Missing CSRF token is rejected.
    let r = b.req("POST", "/console/dbs", Some(form(&[("name", "nope")]))).await?;
    if r.status != 403 {
        return Err(format!("POST without CSRF accepted: {}", r.status));
    }
    let r = b.req("POST", "/console/dbs", Some(form(&[("csrf", &token), ("name", "webdb")]))).await?;
    if !location(&r).contains("ok=") {
        return Err(format!("create db via console failed: {}", location(&r)));
    }
    let evil = "<script>alert(1)</script>";
    b.req("POST", "/console/dbs/webdb/kv", Some(form(&[("csrf", &token), ("key", evil), ("value", evil)]))).await?;
    let db = b.page("/console/dbs/webdb").await?;
    if db.contains(evil) || !db.contains("&lt;script&gt;") {
        return Err("console does not escape stored values".into());
    }
    let txn = r#"{"ops":[{"op":"increment","key":"hits","by":3}]}"#;
    let r = b.req("POST", "/console/dbs/webdb/txn", Some(form(&[("csrf", &token), ("body", txn)]))).await?;
    let page = b.page(&location(&r)).await?;
    if !page.contains("Result") || !page.contains("&quot;value&quot;: 3") {
        let i = page.find("Result").unwrap_or(0);
        return Err(format!("transaction result not shown: {}", &page[i..(i + 600).min(page.len())]));
    }
    let r = b
        .req(
            "POST",
            "/console/keys",
            Some(form(&[("csrf", &token), ("name", evil), ("db", "webdb"), ("read_only", "1")])),
        )
        .await?;
    let keys = b.page(&location(&r)).await?;
    if !keys.contains("brk_") || keys.contains(evil) {
        return Err("key creation page wrong".into());
    }
    let admin = b.page("/console/admin").await?;
    if !admin.contains("ui@console.test") || !admin.contains("Cluster") {
        return Err("admin page missing content".into());
    }
    let r = b.req("POST", "/console/logout", Some(form(&[("csrf", &token)]))).await?;
    if b.cookie.is_some() || location(&r) != "/console/login" {
        return Err("logout did not clear the session".into());
    }
    Ok(())
}
