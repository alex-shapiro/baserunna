//! Exercises less common endpoints such as schema drops, transaction aborts,
//! commit status by idempotency key, and metadata endpoints so every documented
//! operation runs under simulation.

use super::super::Sim;
use super::super::client::Client;
use super::expect;
use serde_json::json;
use std::sync::Arc;

pub async fn run(sim: Arc<Sim>) -> Result<(), String> {
    let anon = Client::new(&sim, "tourist");
    let h = anon.call_retry("GET", "/healthz", None).await;
    expect(h.json["ok"] == true, "healthz")?;
    let doc = anon.call_retry("GET", "/v1/openapi.json", None).await;
    expect(doc.json["openapi"] == "3.1.0", "openapi document")?;
    let r = anon.call_retry("GET", "/v1/auth/google/start", None).await;
    expect(r.status == 302, format!("google start: {}", r.status))?;
    let r = anon.call_retry("GET", "/v1/auth/google/callback?code=x&state=y", None).await;
    expect(r.status == 401, format!("google callback without state cookie: {}", r.status))?;

    let c = Client::signup(&sim, "tour", "tour@api.test", "tour-password").await?;
    let me = c.call_retry("GET", "/v1/me", None).await;
    expect(me.json["user"]["email"] == "tour@api.test", "me")?;
    c.create_db("tour").await?;
    let base = "/v1/dbs/tour";

    let r = c
        .call_settled(
            "POST",
            &format!("{base}/tables"),
            Some(json!({"name": "items", "if_not_exists": true, "primary_key": ["id"], "columns": [
                {"name": "id", "type": "int", "nullable": false},
                {"name": "label", "type": "string"},
                {"name": "extra", "type": "bytes"}
            ], "indexes": [{"name": "by_label", "columns": ["label"]}]})),
        )
        .await;
    expect(r.status == 201, format!("create table: {}", r.json))?;
    let r = c.call_retry("GET", &format!("{base}/tables/items"), None).await;
    expect(r.json["indexes"][0]["name"] == "by_label", format!("describe: {}", r.json))?;
    let r = c.call_retry("GET", &format!("{base}/tables"), None).await;
    expect(r.json["tables"].as_array().is_some_and(|t| t.len() == 1), "list tables")?;
    let r = c
        .call_settled(
            "POST",
            &format!("{base}/tables/items/rows"),
            Some(json!({"row": {"id": 1, "label": "a", "extra": "AAE="}, "upsert": true})),
        )
        .await;
    expect(r.status == 200, format!("upsert row: {}", r.json))?;
    let r = c.call_settled("DELETE", &format!("{base}/tables/items/indexes/by_label"), None).await;
    expect(r.status == 200 || r.status == 404, format!("drop index: {}", r.json))?;
    let r = c.call_settled("DELETE", &format!("{base}/tables/items/columns/extra"), None).await;
    expect(r.status == 200 || r.status == 404, format!("drop column: {}", r.json))?;
    let r = c.call_retry("POST", &format!("{base}/tables/items/rows/get"), Some(json!({"pk": 1}))).await;
    expect(
        r.json["row"].get("extra").is_none() && r.json["row"]["label"] == "a",
        format!("row after drops: {}", r.json),
    )?;
    let r = c.call_settled("DELETE", &format!("{base}/tables/items"), None).await;
    expect(r.status == 200 || r.status == 404, format!("drop table: {}", r.json))?;
    let r = c.call_retry("GET", &format!("{base}/tables/items"), None).await;
    expect(r.status == 404, "dropped table still described")?;

    let t = c.call_retry("POST", &format!("{base}/transactions"), Some(json!({"isolation": "snapshot"}))).await;
    expect(t.status == 201, format!("begin: {}", t.json))?;
    let id = t.json["txn_id"].as_str().unwrap_or_default().to_string();
    let r = c
        .call_retry(
            "POST",
            &format!("{base}/transactions/{id}/ops"),
            Some(json!({"ops": [{"op": "put", "key": "aborted", "value": "x"}]})),
        )
        .await;
    if r.status == 200 {
        let r = c.call_retry("DELETE", &format!("{base}/transactions/{id}"), None).await;
        expect(r.status == 200 || r.status == 404, format!("abort: {}", r.json))?;
    }
    let r = c.call_retry("POST", &format!("{base}/txn"), Some(json!({"ops": [{"op": "get", "key": "aborted"}]}))).await;
    expect(r.json["results"][0]["found"] == false, "aborted write visible")?;

    let body = json!({"ops": [{"op": "increment", "key": "n", "by": 7}], "idempotency_key": "tour-1"});
    let r = c.call_settled("POST", &format!("{base}/txn"), Some(body)).await;
    expect(r.status == 200, format!("idempotent txn: {}", r.json))?;
    let st = c.call_retry("GET", &format!("{base}/commits/key:tour-1"), None).await;
    expect(st.json["status"] == "committed", format!("status by key: {}", st.json))?;
    let st = c.call_retry("GET", &format!("{base}/commits/key:never-used"), None).await;
    expect(st.json["status"] == "not_committed", format!("status of unknown key: {}", st.json))?;

    let r = c.call_retry("GET", "/v1/admin/nodes", None).await;
    expect(r.status == 200 || r.status == 403, "admin nodes")?;
    let r = c.call_retry("GET", "/v1/admin/users", None).await;
    expect(r.status == 200 || r.status == 403, "admin users")?;
    Ok(())
}
