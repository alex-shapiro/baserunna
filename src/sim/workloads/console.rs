//! `console`: exercises all web console pages and verifies HTML escaping,
//! attachment download headers, pagination, account self-service,
//! organization management, and admin actions.

use super::super::client::{Client, url_escape};
use super::super::{Sim, SimResponse};
use super::{cache_expiry, expect};
use crate::api::SESSION_COOKIE;
use crate::console::multipart::{self, Part};
use bytes::Bytes;
use serde_json::json;
use std::sync::Arc;

struct Browser {
    c: Client,
    cookie: Option<String>,
}

fn form(pairs: &[(&str, &str)]) -> String {
    pairs
        .iter()
        .map(|(k, v)| format!("{}={}", url_escape(k.as_bytes()), url_escape(v.as_bytes())))
        .collect::<Vec<_>>()
        .join("&")
}

impl Browser {
    fn new(sim: &Arc<Sim>, name: &str) -> Browser {
        Browser { c: Client::new(sim, name), cookie: None }
    }

    /// Returns an API client authenticated with this browser's session.
    fn api(&self, name: &str) -> Client {
        Client::with_token(&self.c.sim, name, self.cookie.as_deref().unwrap_or_default())
    }

    async fn send(&mut self, method: &str, path: &str, ct: Option<&str>, body: Vec<u8>) -> Result<SimResponse, String> {
        for _ in 0..50 {
            let mut h: Vec<(&str, String)> = Vec::new();
            if let Some(c) = &self.cookie {
                h.push(("cookie", format!("{SESSION_COOKIE}={c}")));
            }
            if let Some(ct) = ct {
                h.push(("content-type", ct.to_string()));
            }
            match self.c.send(method, path, &h, Bytes::from(body.clone())).await {
                Ok(r) if r.status == 503 => {}
                Ok(r) => {
                    let prefix = format!("{SESSION_COOKIE}=");
                    for v in r.headers.get_all("set-cookie") {
                        if let Some(rest) = v.to_str().unwrap_or("").strip_prefix(&prefix) {
                            let val = rest.split(';').next().unwrap_or("");
                            self.cookie = (!val.is_empty()).then(|| val.to_string());
                        }
                    }
                    return Ok(r);
                }
                Err(_) if method == "GET" => {}
                Err(e) => return Err(format!("{method} {path}: {e:?}")),
            }
            self.c.pause(200).await;
        }
        Err(format!("{method} {path}: request timed out"))
    }

    async fn req(&mut self, method: &str, path: &str, body: Option<String>) -> Result<SimResponse, String> {
        let ct = body.is_some().then_some("application/x-www-form-urlencoded");
        self.send(method, path, ct, body.unwrap_or_default().into_bytes()).await
    }

    async fn page(&mut self, path: &str) -> Result<String, String> {
        let r = self.req("GET", path, None).await?;
        expect(r.status == 200, format!("GET {path} returned {} (location {})", r.status, header(&r, "location")))?;
        Ok(String::from_utf8_lossy(&r.body).into_owned())
    }

    /// Posts a form with the CSRF token from `from`.
    async fn post(&mut self, from: &str, action: &str, pairs: &[(&str, &str)]) -> Result<SimResponse, String> {
        let token = csrf(&self.page(from).await?)?;
        let mut all = vec![("csrf", token.as_str())];
        all.extend_from_slice(pairs);
        self.req("POST", action, Some(form(&all))).await
    }

    /// Posts a form and returns the redirect `Location` header.
    async fn submit(&mut self, from: &str, action: &str, pairs: &[(&str, &str)]) -> Result<String, String> {
        let r = self.post(from, action, pairs).await?;
        expect(
            (300..400).contains(&r.status),
            format!("POST {action} returned {} instead of a redirect: {}", r.status, String::from_utf8_lossy(&r.body)),
        )?;
        Ok(header(&r, "location"))
    }

    /// Submits a form and asserts it redirects with `ok=`.
    async fn submit_ok(&mut self, from: &str, action: &str, pairs: &[(&str, &str)]) -> Result<String, String> {
        let to = self.submit(from, action, pairs).await?;
        expect(to.contains("ok="), format!("POST {action} failed: {}", unescape_url(&to)))?;
        Ok(to)
    }

    /// Submits a form and asserts it redirects with `err=`.
    async fn submit_err(&mut self, from: &str, action: &str, pairs: &[(&str, &str)]) -> Result<String, String> {
        let to = self.submit(from, action, pairs).await?;
        expect(to.contains("err="), format!("POST {action} was accepted: {}", unescape_url(&to)))?;
        Ok(unescape_url(&to))
    }

    async fn signed_out(&mut self, path: &str) -> Result<bool, String> {
        let r = self.req("GET", path, None).await?;
        Ok((300..400).contains(&r.status) && header(&r, "location") == "/console/login")
    }
}

fn after<'a>(html: &'a str, needle: &str) -> Option<&'a str> {
    html.find(needle).map(|i| &html[i + needle.len()..])
}

fn csrf(html: &str) -> Result<String, String> {
    let rest = after(html, "name=\"csrf\" value=\"").ok_or("missing csrf token on the page")?;
    Ok(rest[..rest.find('"').ok_or("unterminated csrf token")?].to_string())
}

/// Returns the `action` attribute of the first form after `marker`.
fn form_after(html: &str, marker: &str) -> Result<String, String> {
    let rest = after(html, marker).ok_or(format!("{marker:?} is not on the page"))?;
    let action = after(rest, "action=\"").ok_or(format!("missing form after {marker:?}"))?;
    Ok(action[..action.find('"').unwrap_or(0)].replace("&amp;", "&"))
}

fn header(r: &SimResponse, name: &str) -> String {
    r.headers.get(name).and_then(|v| v.to_str().ok()).unwrap_or("").to_string()
}

fn unescape_url(s: &str) -> String {
    String::from_utf8_lossy(&crate::api::percent_decode(s)).into_owned()
}

/// Returns the `href` attribute of the link labelled `text`.
fn link(html: &str, text: &str) -> Option<String> {
    let end = html.find(&format!(">{text}</a>"))?;
    let needle = "href=\"";
    let href = &html[html[..end].rfind(needle)? + needle.len()..end];
    Some(href[..href.find('"')?].replace("&amp;", "&"))
}

async fn signup(sim: &Arc<Sim>, name: &str, email: &str, password: &str) -> Result<Browser, String> {
    let mut b = Browser::new(sim, name);
    let r = b.req("POST", "/console/signup", Some(form(&[("email", email), ("password", password)]))).await?;
    let to = header(&r, "location");
    expect(b.cookie.is_some() && to == "/console/dbs", format!("signing up {email} went to {}", unescape_url(&to)))?;
    Ok(b)
}

async fn login(sim: &Arc<Sim>, name: &str, email: &str, password: &str) -> Result<Browser, String> {
    let mut b = Browser::new(sim, name);
    let r = b.req("POST", "/console/login", Some(form(&[("email", email), ("password", password)]))).await?;
    let to = header(&r, "location");
    expect(b.cookie.is_some() && to == "/console/dbs", format!("signing in {email} went to {}", unescape_url(&to)))?;
    Ok(b)
}

/// Asserts that signing in with `password` is rejected.
async fn cannot_login(sim: &Arc<Sim>, email: &str, password: &str, why: &str) -> Result<(), String> {
    let mut b = Browser::new(sim, "refused");
    let r = b.req("POST", "/console/login", Some(form(&[("email", email), ("password", password)]))).await?;
    expect(b.cookie.is_none() && header(&r, "location").contains("err="), why)
}

pub async fn run(sim: Arc<Sim>) -> Result<(), String> {
    let mut anon = Browser::new(&sim, "anonymous");
    expect(anon.signed_out("/console/dbs").await?, "anonymous console access was not sent to sign in")?;
    let login_page = anon.page("/console/login").await?;
    expect(login_page.contains("/v1/auth/google/start"), "the Google sign-in link is missing")?;
    expect(login_page.contains("/console/forgot"), "the password reset link is missing")?;
    let mut b = signup(&sim, "browser", "ui@console.test", "console-pass").await?;
    for (action, field) in [
        ("/console/dbs", "name"),
        ("/console/orgs", "name"),
        ("/console/account/password", "new_password"),
        ("/console/account/delete", "confirm"),
    ] {
        let r = b.req("POST", action, Some(form(&[(field, "nope-nope")]))).await?;
        expect(r.status == 403, format!("POST {action} without a CSRF token returned {}", r.status))?;
    }
    b.submit_ok("/console/dbs", "/console/dbs", &[("name", "webdb")]).await?;
    let evil = "<script>alert(1)</script>";
    b.submit_ok("/console/dbs/webdb", "/console/dbs/webdb/kv", &[("key", evil), ("value", evil)]).await?;
    let db = b.page("/console/dbs/webdb").await?;
    expect(!db.contains(evil) && db.contains("&lt;script&gt;"), "the console does not escape stored values")?;
    transactions(&mut b).await?;
    keys(&mut b, evil).await?;
    let admin = b.page("/console/admin").await?;
    let this_node = admin.contains("This node: <code>n");
    expect(admin.contains("ui@console.test") && this_node, "the admin page lacks users or this node")?;

    pagination(&mut b).await?;
    streams(&mut b, evil).await?;
    files(&sim, &mut b).await?;
    schema(&mut b).await?;
    organizations(&sim, &mut b).await?;
    account(&sim).await?;
    mailed_links(&sim).await?;
    administration(&sim, &mut b).await?;

    let r = b.post("/console/dbs", "/console/logout", &[]).await?;
    expect(b.cookie.is_none() && header(&r, "location") == "/console/login", "signing out kept the session")?;
    expect(b.signed_out("/console/dbs").await?, "a signed-out session still works")
}

/// Verifies that the transaction runner renders results directly on the page
/// rather than redirecting with the transaction or its result in the URL.
async fn transactions(b: &mut Browser) -> Result<(), String> {
    let txn = r#"{"ops":[{"op":"increment","key":"hits","by":3}]}"#;
    let r = b.post("/console/dbs/webdb", "/console/dbs/webdb/txn", &[("body", txn)]).await?;
    let page = String::from_utf8_lossy(&r.body);
    expect(r.status == 200, format!("running a transaction returned {}", r.status))?;
    let result = after(&page, "<h2>Result</h2><pre>").unwrap_or("");
    expect(result.contains("&quot;value&quot;: 3"), format!("the transaction's result is not shown: {result:.300}"))?;
    let r = b.post("/console/dbs/webdb", "/console/dbs/webdb/txn", &[("body", "{not json")]).await?;
    let page = String::from_utf8_lossy(&r.body);
    expect(
        page.contains("Invalid JSON") && page.contains("{not json</textarea>"),
        "invalid JSON is not reported next to the text to fix",
    )
}

/// Verifies that a new key's secret is shown once in the form response.
async fn keys(b: &mut Browser, evil: &str) -> Result<(), String> {
    let r = b.post("/console/keys", "/console/keys", &[("name", evil), ("db", "webdb"), ("read_only", "1")]).await?;
    let page = String::from_utf8_lossy(&r.body);
    expect(
        r.status == 200 && header(&r, "location").is_empty(),
        "creating a key redirected (the secret would be exposed in the URL)",
    )?;
    expect(header(&r, "cache-control") == "no-store", "the page with a key's secret may be cached")?;
    let secret = after(&page, "New key (shown once): <code>").and_then(|s| s.split('<').next()).unwrap_or("");
    expect(secret.starts_with("brk_") && !page.contains(evil), "the new key is not shown, or its name is unescaped")?;
    let keys = b.page("/console/keys").await?;
    expect(!keys.contains(secret), "a key's secret is shown again")?;
    let me = Client::with_token(&b.c.sim, "new-key", secret).call_retry("GET", "/v1/me", None).await;
    expect(me.json["credential"]["read_only"] == true, format!("the shown secret does not work: {}", me.json))
}

/// Verifies pagination across default and custom page sizes.
async fn pagination(b: &mut Browser) -> Result<(), String> {
    let ops: Vec<_> = (0..55).map(|i| json!({"op": "put", "key": format!("p/{i:03}"), "value": "v"})).collect();
    let body = json!({"ops": ops, "idempotency_key": "pagination"});
    let r = b.api("pager").call_settled("POST", "/v1/dbs/webdb/txn", Some(body)).await;
    expect(r.status == 200, format!("writing 55 keys: {} {}", r.status, r.json))?;
    let first = b.page("/console/dbs/webdb?prefix=p%2F").await?;
    let shown = first.matches("<code>p/").count();
    expect(
        shown == 50 && first.contains("p/049") && !first.contains("p/050"),
        format!("the first page shows {shown} keys, not p/000 to p/049"),
    )?;
    let next = link(&first, "Next page →").ok_or("missing next page link on the first page")?;
    let second = b.page(&next).await?;
    expect(second.contains("p/054") && !second.contains("p/049"), format!("the second page ({next}) is wrong"))?;
    expect(
        link(&second, "First page").is_some() && link(&second, "Next page →").is_none(),
        "the last page's pager is wrong",
    )?;
    let existing = b.page("/console/keys?limit=1000").await?.matches("/revoke\"").count();
    for i in 0..3 {
        b.post("/console/keys", "/console/keys", &[("name", &format!("pk{i}"))]).await?;
    }
    let mut seen = 0;
    let mut path = "/console/keys?limit=2".to_string();
    for _ in 0..10 {
        let p = b.page(&path).await?;
        seen += p.matches("/revoke\"").count();
        match link(&p, "Next page →") {
            Some(n) => path = n,
            None => break,
        }
    }
    expect(seen == existing + 3, format!("paging API keys two at a time found {seen} of {}", existing + 3))
}

/// Verifies stream pages: creation, appends of text and JSON, escaping,
/// newest-first paging, retention, trims and deletion.
async fn streams(b: &mut Browser, evil: &str) -> Result<(), String> {
    let r = b.req("POST", "/console/dbs/webdb/streams", Some(form(&[("name", "x")]))).await?;
    expect(r.status == 403, format!("creating a stream without a CSRF token returned {}", r.status))?;
    let sp = "/console/dbs/webdb/streams/events";
    let to = b
        .submit_ok("/console/dbs/webdb", "/console/dbs/webdb/streams", &[("name", "events"), ("max_bytes", "100000")])
        .await?;
    expect(to.starts_with(sp), format!("creating a stream led to {to}"))?;
    b.submit_err("/console/dbs/webdb", "/console/dbs/webdb/streams", &[("name", "events")]).await?;
    b.submit_ok(sp, &format!("{sp}/records"), &[("value", evil)]).await?;
    b.submit_ok(sp, &format!("{sp}/records"), &[("value", r#"{"tag": "<b>bold</b>"}"#), ("json", "1")]).await?;
    b.submit_err(sp, &format!("{sp}/records"), &[("value", "{nope"), ("json", "1")]).await?;
    let page = b.page(sp).await?;
    expect(!page.contains(evil) && page.contains("&lt;script&gt;"), "the console does not escape stream records")?;
    expect(!page.contains("<b>bold") && page.contains("&lt;b&gt;bold"), "the console does not escape JSON records")?;
    let records: Vec<_> = (0..60).map(|i| json!({"value": format!("r{i:02}")})).collect();
    let body = json!({"records": records, "idempotency_key": "console-streams"});
    let r = b.api("streams").call_settled("POST", "/v1/dbs/webdb/streams/events/records", Some(body)).await;
    expect(r.status == 200, format!("appending 60 records: {} {}", r.status, r.json))?;
    let newest = b.page(sp).await?;
    // Match whole cells: a token elsewhere on the page may contain "r10".
    let cell = |v: &str| format!("<td>{v}</td>");
    // The first two records precede r00, at seq 2.
    let (r59, r10) = (newest.find(&cell("r59")), newest.find(&cell("r10")));
    expect(r59.is_some() && r10.is_some() && r59 < r10, "the newest records are not shown first")?;
    expect(!newest.contains(&cell("r09")), "the first page shows more than 50 records")?;
    let older = link(&newest, "Older →").ok_or("missing link to older records")?;
    let page = b.page(&older).await?;
    expect(
        page.contains(&cell("r09")) && page.contains("&lt;script&gt;") && !page.contains(&cell("r10")),
        format!("{older} is wrong"),
    )?;
    expect(link(&page, "Newest").is_some() && link(&page, "Older →").is_none(), "the oldest page's pager is wrong")?;
    b.submit_ok(sp, &format!("{sp}/retention"), &[("retention_ms", "3600000"), ("max_bytes", "")]).await?;
    expect(b.page(sp).await?.contains("keeps 3600 s"), "the retention is not shown")?;
    b.submit_err(sp, &format!("{sp}/retention"), &[("retention_ms", "soon")]).await?;
    b.submit_ok(sp, &format!("{sp}/trim"), &[("before_seq", "10")]).await?;
    expect(b.page(sp).await?.contains("<span>Records</span>10–61"), "the trim is not shown")?;
    b.submit_err(sp, &format!("{sp}/delete"), &[("confirm", "nope")]).await?;
    b.submit_ok(sp, &format!("{sp}/delete"), &[("confirm", "events")]).await?;
    let db = b.page("/console/dbs/webdb").await?;
    expect(!db.contains("streams/events"), "a deleted stream is still listed")
}

/// Verifies multipart file uploads and attachment download headers.
async fn files(sim: &Arc<Sim>, b: &mut Browser) -> Result<(), String> {
    let data: Vec<u8> = (0..3000u32).map(|i| (i * 7 % 251) as u8).collect();
    expect(data.len() > sim.cfg.engine.value_inline_max, "the upload is too small to become a blob")?;
    let boundary = "----sim0boundary";
    let ct = format!("multipart/form-data; boundary={boundary}");
    let text = |name: &str, v: &str| Part {
        name: name.into(),
        filename: None,
        content_type: None,
        data: Bytes::copy_from_slice(v.as_bytes()),
    };
    let body = |csrf: &str, key: &str, data: &[u8], file_ct: &str| {
        multipart::encode(
            boundary,
            &[
                text("csrf", csrf),
                text("key", key),
                Part {
                    name: "file".into(),
                    filename: Some("page.html".into()),
                    content_type: Some(file_ct.into()),
                    data: Bytes::copy_from_slice(data),
                },
            ],
        )
    };
    let upload = "/console/dbs/webdb/kv/upload";
    let r = b.send("POST", upload, Some(&ct), body("forged", "x", b"x", "text/plain")).await?;
    expect(r.status == 403, format!("an upload with a forged CSRF token returned {}", r.status))?;
    let r = b.send("POST", upload, Some(&ct), b"not multipart".to_vec()).await?;
    expect(r.status == 400, format!("a malformed upload returned {}", r.status))?;
    let token = csrf(&b.page("/console/dbs/webdb").await?)?;
    for (key, data) in [("files/big.html", &data[..]), ("files/small.txt", &b"<b>hi</b>"[..])] {
        let r = b.send("POST", upload, Some(&ct), body(&token, key, data, "text/html")).await?;
        let to = unescape_url(&header(&r, "location"));
        expect(to.contains(&format!("to {key} at version")), format!("uploading {key} went to {to}"))?;
        let r =
            b.req("GET", &format!("/console/dbs/webdb/kv/download?key={}", url_escape(key.as_bytes())), None).await?;
        expect(
            r.status == 200 && r.body[..] == data[..],
            format!("downloading {key}: {} {}B", r.status, r.body.len()),
        )?;
        let name = key.rsplit('/').next().unwrap_or("");
        expect(
            header(&r, "content-disposition") == format!("attachment; filename=\"{name}\"")
                && header(&r, "x-content-type-options") == "nosniff"
                && header(&r, "content-security-policy").starts_with("sandbox"),
            format!("downloading {key} could render as a page: {:?}", r.headers),
        )?;
    }
    let page = b.page("/console/dbs/webdb?prefix=files").await?;
    expect(page.contains(">blob</a> text/html"), "a large upload is not listed as a blob")?;
    let api = b.api("browser-api");
    let r = api
        .call_retry(
            "POST",
            "/v1/dbs/webdb/tables",
            Some(json!({"name": "media", "primary_key": ["id"], "columns": [
                {"name": "id", "type": "int", "nullable": false}, {"name": "file", "type": "blob"}]})),
        )
        .await;
    expect(r.status == 201, format!("creating the media table: {}", r.json))?;
    let r = api.send("POST", "/v1/dbs/webdb/blobs", &[], Bytes::from(data.clone())).await;
    let staged: serde_json::Value = serde_json::from_slice(&r.map_err(|e| format!("{e:?}"))?.body).unwrap_or_default();
    let id = staged["blob_id"].as_str().ok_or(format!("staging a blob: {staged}"))?.to_string();
    let r =
        api.call_retry("POST", "/v1/dbs/webdb/tables/media/rows", Some(json!({"row": {"id": 1, "file": id}}))).await;
    expect(r.status == 201, format!("inserting the media row: {}", r.json))?;
    let t = b.page("/console/dbs/webdb/tables/media").await?;
    let href = link(&t, "blob").ok_or("missing link in the blob cell")?;
    let r = b.req("GET", &href, None).await?;
    expect(r.status == 200 && r.body[..] == data[..], format!("downloading the blob returned {}", r.status))?;
    expect(header(&r, "content-disposition").starts_with("attachment"), "the blob download is not an attachment")
}

async fn schema(b: &mut Browser) -> Result<(), String> {
    let evil = "<img src=x onerror=alert(1)>";
    let r = b.post("/console/dbs/webdb", "/console/dbs/webdb/tables", &[("def", "{not json")]).await?;
    let page = String::from_utf8_lossy(&r.body);
    expect(
        page.contains("Invalid JSON") && page.contains(">{not json</textarea>"),
        "a bad table definition is not returned for fixing",
    )?;
    let def = r#"{"name": "notes", "primary_key": ["id"], "columns": [{"name": "id", "type": "int", "nullable": false}, {"name": "label", "type": "string"}]}"#;
    b.submit_ok("/console/dbs/webdb", "/console/dbs/webdb/tables", &[("def", def)]).await?;
    let t = "/console/dbs/webdb/tables/notes";
    let columns = format!("{t}/columns");
    b.submit_ok(t, &columns, &[("name", "memo"), ("type", "string"), ("nullable", "1"), ("default", "\"n/a\"")])
        .await?;
    let e = b.submit_err(t, &columns, &[("name", "bad"), ("type", "int"), ("default", "{")]).await?;
    expect(e.contains("not JSON"), format!("a bad default was refused for another reason: {e}"))?;
    b.submit_ok(t, &format!("{t}/indexes"), &[("name", "by_label"), ("columns", "label, memo"), ("unique", "1")])
        .await?;
    let api = b.api("schema-api");
    let r =
        api.call_retry("POST", "/v1/dbs/webdb/tables/notes/rows", Some(json!({"row": {"id": 1, "label": evil}}))).await;
    expect(r.status == 201, format!("inserting a note: {}", r.json))?;
    let p = b.page(t).await?;
    expect(
        p.contains("<td>memo</td><td>string</td><td>yes</td><td>n/a</td>")
            && p.contains("<td>by_label</td><td>label, memo</td><td>yes</td><td>ready</td>"),
        "the added column or index is not shown",
    )?;
    expect(!p.contains(evil) && p.contains("&lt;img"), "rows are shown unescaped")?;
    b.submit_ok(t, &format!("{t}/indexes/by_label/drop"), &[]).await?;
    b.submit_ok(t, &format!("{t}/columns/memo/drop"), &[]).await?;
    let p = b.page(t).await?;
    expect(!p.contains("by_label") && !p.contains("<td>memo</td>"), "a dropped index or column is still shown")?;
    b.submit_err(t, &format!("{t}/drop"), &[("confirm", "wrong")]).await?;
    b.submit_ok(t, &format!("{t}/drop"), &[("confirm", "notes")]).await?;
    let p = b.page("/console/dbs/webdb").await?;
    expect(!p.contains("tables/notes"), "a dropped table is still listed")?;
    let blobs = after(&p, "<span>Blob bytes</span>").unwrap_or("");
    expect(
        p.contains("<span>Your role</span>owner") && !blobs.starts_with("0 B") && p.contains("<span>Lease</span>n"),
        "the metrics panel does not show the role, blob bytes or lease",
    )
}

async fn organizations(sim: &Arc<Sim>, owner: &mut Browser) -> Result<(), String> {
    let mut m = signup(sim, "member", "member@console.test", "member-pass").await?;
    owner.submit_ok("/console/orgs", "/console/orgs", &[("name", "acme")]).await?;
    let o = "/console/orgs/acme";
    owner.submit_ok(o, &format!("{o}/dbs"), &[("name", "shared")]).await?;
    owner.submit_ok(o, &format!("{o}/members"), &[("email", "member@console.test"), ("role", "reader")]).await?;
    let e =
        owner.submit_err(o, &format!("{o}/members"), &[("email", "nobody@console.test"), ("role", "reader")]).await?;
    expect(e.contains("no account"), format!("adding an unknown account was refused for another reason: {e}"))?;
    let orgs = m.page("/console/orgs").await?;
    expect(orgs.contains(">acme</a></td><td>reader</td>"), "the member does not see the organization and role")?;
    let p = m.page(o).await?;
    expect(p.contains("acme.shared") && !p.contains("Add member"), "a reader sees member management")?;
    let db = m.page("/console/dbs/acme.shared").await?;
    expect(db.contains("read-only access") && !db.contains("Upload file"), "a reader gets write forms")?;
    let shared = "/console/dbs/acme.shared";
    let e = m.submit_err(shared, &format!("{shared}/kv"), &[("key", "k"), ("value", "v")]).await?;
    expect(e.contains("cannot write"), format!("a reader's write was refused for another reason: {e}"))?;
    let member_id = m.api("member-api").call_retry("GET", "/v1/me", None).await.json["user"]["id"]
        .as_str()
        .ok_or("missing member id")?
        .to_string();
    owner.submit_ok(o, &format!("{o}/members/{member_id}"), &[("role", "writer")]).await?;
    cache_expiry(sim).await;
    m.submit_ok(shared, &format!("{shared}/kv"), &[("key", "k"), ("value", "v")]).await?;
    owner.submit_ok(o, &format!("{o}/members/{member_id}/remove"), &[]).await?;
    cache_expiry(sim).await;
    let r = m.req("GET", o, None).await?;
    expect(header(&r, "location").starts_with("/console/orgs?err="), "a removed member still sees the organization")?;
    owner.submit_ok(o, &format!("{o}/members"), &[("email", "member@console.test"), ("role", "admin")]).await?;
    m.submit_ok(o, &format!("{o}/leave"), &[]).await?;
    expect(!m.page("/console/orgs").await?.contains("/console/orgs/acme"), "leaving kept the membership")?;
    let e = owner.submit_err(o, &format!("{o}/delete"), &[("confirm", "acme")]).await?;
    expect(e.contains("databases first"), format!("deleting an organization with databases: {e}"))?;
    owner.submit_ok(shared, &format!("{shared}/delete"), &[("confirm", "acme.shared")]).await?;
    owner.submit_err(o, &format!("{o}/delete"), &[("confirm", "wrong")]).await?;
    owner.submit_ok(o, &format!("{o}/delete"), &[("confirm", "acme")]).await?;
    expect(!owner.page("/console/orgs").await?.contains("/console/orgs/acme"), "a deleted organization is listed")
}

async fn account(sim: &Arc<Sim>) -> Result<(), String> {
    let (email, pass) = ("self@console.test", "self-pass-1");
    let mut a = signup(sim, "self-a", email, pass).await?;
    let mut other = login(sim, "self-b", email, pass).await?;
    let mut third = login(sim, "self-c", email, pass).await?;
    let p = a.page("/console/account").await?;
    expect(p.contains(email) && p.contains("not verified") && p.contains("(this one)"), "the account page is wrong")?;
    let one = a.page("/console/account?limit=1").await?;
    expect(link(&one, "Next page →").is_some(), "sessions do not paginate")?;
    let acct = "/console/account";
    let pw = "/console/account/password";
    let e = a.submit_err(acct, pw, &[("current_password", "wrong-pass"), ("new_password", "self-pass-2")]).await?;
    expect(e.contains("incorrect"), format!("a wrong current password was refused for another reason: {e}"))?;
    a.submit_ok(acct, pw, &[("current_password", pass), ("new_password", "self-pass-2")]).await?;
    cache_expiry(sim).await;
    expect(
        other.signed_out("/console/dbs").await? && third.signed_out("/console/dbs").await?,
        "other sessions survive a password change",
    )?;
    expect(a.page("/console/dbs").await.is_ok(), "the session that changed the password ended")?;
    cannot_login(sim, email, pass, "the old password still works").await?;
    let mut b2 = login(sim, "self-e", email, "self-pass-2").await?;
    let s = b2.api("self-api").call_retry("GET", "/v1/me/sessions", None).await;
    let b2_id = s.json["sessions"]
        .as_array()
        .and_then(|a| a.iter().find(|s| s["current"] == true))
        .and_then(|s| s["id"].as_str())
        .ok_or(format!("missing current session in {}", s.json))?
        .to_string();
    expect(a.page(acct).await?.contains(&b2_id), "another session is not listed")?;
    a.submit_ok(acct, &format!("/console/account/sessions/{b2_id}/revoke"), &[]).await?;
    cache_expiry(sim).await;
    expect(b2.signed_out("/console/dbs").await?, "a revoked session still works")?;
    let mut b3 = login(sim, "self-f", email, "self-pass-2").await?;
    let to = a.submit_ok(acct, "/console/account/sessions/others/revoke", &[]).await?;
    expect(unescape_url(&to).contains("Signed out 1 other"), format!("signing out elsewhere: {}", unescape_url(&to)))?;
    cache_expiry(sim).await;
    expect(b3.signed_out("/console/dbs").await?, "signing out everywhere else left a session")?;
    let p = a.page(acct).await?;
    let action = form_after(&p, "(this one)")?;
    let r = a.post(acct, &action, &[]).await?;
    expect(
        a.cookie.is_none() && header(&r, "location") == "/console/login",
        "revoking the current session did not sign out",
    )?;
    let mut d = signup(sim, "doomed", "doomed@console.test", "doomed-pass").await?;
    d.submit_ok("/console/dbs", "/console/dbs", &[("name", "doomed-db")]).await?;
    let del = "/console/account/delete";
    d.submit_err(acct, del, &[("confirm", "someone@else.test"), ("password", "doomed-pass")]).await?;
    let e = d.submit_err(acct, del, &[("confirm", "doomed@console.test"), ("password", "nope-nope")]).await?;
    expect(e.contains("incorrect"), format!("a wrong password was refused for another reason: {e}"))?;
    let to = d.submit(acct, del, &[("confirm", "doomed@console.test"), ("password", "doomed-pass")]).await?;
    expect(
        to.starts_with("/console/login") && to.contains("ok=") && d.cookie.is_none(),
        format!("deleting the account went to {}", unescape_url(&to)),
    )?;
    cannot_login(sim, "doomed@console.test", "doomed-pass", "a deleted account signs in").await
}

async fn mailed_links(sim: &Arc<Sim>) -> Result<(), String> {
    let (email, pass) = ("mail@console.test", "mail-pass-1");
    let mut u = signup(sim, "mail", email, pass).await?;
    let mut old = login(sim, "mail-old", email, pass).await?;
    u.submit_ok("/console/account", "/console/account/verify", &[]).await?;
    sim.sleep(500).await;
    let token = sim.mailbox.latest_token(email, "/console/verify").ok_or("missing verification mail")?;
    let p = u.page(&format!("/console/verify?token={token}")).await?;
    expect(
        p.contains("Verify my address") && p.contains(&format!("value=\"{token}\"")),
        "the verification page lacks its form",
    )?;
    expect(u.page("/console/account").await?.contains("not verified"), "opening the link verified the address")?;
    let r = u.req("POST", "/console/verify", Some(form(&[("token", &token)]))).await?;
    let to = unescape_url(&header(&r, "location"));
    expect(to.starts_with("/console/account?ok="), format!("verifying went to {to}"))?;
    cache_expiry(sim).await;
    expect(!u.page("/console/account").await?.contains("not verified"), "the email is still not verified")?;
    let r = u.req("POST", "/console/verify", Some(form(&[("token", "forged")]))).await?;
    expect(header(&r, "location").contains("err="), "a forged verification token was accepted")?;
    let mut anon = Browser::new(sim, "forgetful");
    expect(anon.page("/console/forgot").await?.contains("Send link"), "the forgot page lacks its form")?;
    let before = sim.mailbox.count(email);
    let r = anon.req("POST", "/console/forgot", Some(form(&[("email", email)]))).await?;
    expect(header(&r, "location").starts_with("/console/login?ok="), "requesting a reset was refused")?;
    let r = anon.req("POST", "/console/forgot", Some(form(&[("email", "ghost@console.test")]))).await?;
    expect(header(&r, "location").starts_with("/console/login?ok="), "unknown accounts return a different response")?;
    sim.sleep(500).await;
    expect(sim.mailbox.count(email) == before + 1, "not exactly 1 reset mail was sent")?;
    let token = sim.mailbox.latest_token(email, "/console/reset").ok_or("missing reset mail")?;
    expect(anon.page(&format!("/console/reset?token={token}")).await?.contains("new password"), "missing reset form")?;
    let r = anon.req("POST", "/console/reset", Some(form(&[("token", &token), ("password", "mail-pass-2")]))).await?;
    let to = unescape_url(&header(&r, "location"));
    expect(anon.cookie.is_some() && to == "/console/dbs", format!("resetting went to {to}"))?;
    let mut replay = Browser::new(sim, "replayer");
    let r = replay.req("POST", "/console/reset", Some(form(&[("token", &token), ("password", "mail-pass-3")]))).await?;
    expect(replay.cookie.is_none() && header(&r, "location").contains("err="), "a reset link worked twice")?;
    cache_expiry(sim).await;
    expect(old.signed_out("/console/dbs").await? && u.signed_out("/console/dbs").await?, "a reset left a session")?;
    login(sim, "mail-new", email, "mail-pass-2").await.map(|_| ())
}

async fn administration(sim: &Arc<Sim>, admin: &mut Browser) -> Result<(), String> {
    let mut v = signup(sim, "victim", "victim@console.test", "victim-pass").await?;
    let p = admin.page("/console/admin?limit=1").await?;
    expect(link(&p, "Next page →").is_some(), "users do not paginate")?;
    let mut path = "/console/admin?limit=2".to_string();
    let disable = loop {
        let p = admin.page(&path).await?;
        if p.contains("victim@console.test") {
            break form_after(&p, "victim@console.test")?;
        }
        path = link(&p, "Next page →").ok_or("the victim is not listed")?;
    };
    expect(disable.ends_with("/disable"), format!("an active user's action is {disable}"))?;
    admin.submit_ok("/console/admin", &disable, &[]).await?;
    cache_expiry(sim).await;
    expect(v.signed_out("/console/dbs").await?, "a disabled user's session works")?;
    cannot_login(sim, "victim@console.test", "victim-pass", "a disabled user signs in").await?;
    let p = admin.page("/console/admin?limit=1000").await?;
    let enable = form_after(&p, "victim@console.test")?;
    expect(enable.ends_with("/enable"), format!("a disabled user's action is {enable}"))?;
    admin.submit_ok("/console/admin", &enable, &[]).await?;
    // Sign-in reads a metadata snapshot up to `sys_staleness_ms` old.
    cache_expiry(sim).await;
    login(sim, "victim-again", "victim@console.test", "victim-pass").await.map(|_| ())
}
