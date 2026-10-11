//! Real-world example: a SaaS backend with one database per tenant.
//!
//! The operator account creates a database per customer and hands each
//! customer's service an API key scoped to its own database. A separate
//! read-only key powers an analytics job. Isolation is enforced by the
//! service, not by application code.
//!
//! Run: `cargo run --example saas_tenants`

#[path = "common/mod.rs"]
mod common;

use baserunna::client::Client;
use baserunna::meta::SignupPolicy;
use common::step;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = common::start(common::shared_store(), "saas", SignupPolicy::FirstUserOnly).await;
    let ops = Client::new(&server.url).signup("ops@saas.example", "operator-pass-1").await?;

    step(1, "Provision a database and a scoped API key per tenant");
    let tenants = ["acme", "globex", "initech"];
    let mut keys = Vec::new();
    for t in tenants {
        ops.create_db(&format!("tenant-{t}")).await?;
        let k = ops.post("/v1/api-keys", json!({"name": format!("{t}-service"), "db": format!("tenant-{t}")})).await?;
        println!("    tenant-{t}: key {}…", &k["key"].as_str().unwrap()[..12]);
        keys.push(ops.with_token(k["key"].as_str().unwrap()));
    }

    step(2, "Each tenant's service writes its own data with its own key");
    for (t, svc) in tenants.iter().zip(&keys) {
        let db = format!("tenant-{t}");
        svc.txn(
            &db,
            json!([
                {"op": "put", "key": "settings/plan", "value": if *t == "acme" { "enterprise" } else { "starter" }},
                {"op": "increment", "key": "usage/api_calls", "by": 1200},
                {"op": "put", "key": "users/1", "value": format!("{{\"name\":\"admin@{t}.example\"}}")}
            ]),
        )
        .await?;
    }

    step(3, "Isolation: a tenant key cannot reach another tenant's database");
    let err = keys[0].txn("tenant-globex", json!([{"op": "get", "key": "settings/plan"}])).await.unwrap_err();
    println!("    acme key -> tenant-globex: {err}");
    let listed = keys[0].get("/v1/dbs").await?;
    println!(
        "    acme key lists databases: {:?}",
        listed["databases"].as_array().unwrap().iter().filter_map(|d| d["name"].as_str()).collect::<Vec<_>>()
    );
    let err = keys[0].post("/v1/dbs", json!({"name": "sneaky"})).await.unwrap_err();
    println!("    acme key creating a database: {err}");

    step(4, "A read-only analytics key over one tenant");
    let ro = ops.post("/v1/api-keys", json!({"name": "analytics", "db": "tenant-acme", "read_only": true})).await?;
    let analytics = ops.with_token(ro["key"].as_str().unwrap());
    let r = analytics.txn("tenant-acme", json!([{"op": "get", "key": "usage/api_calls"}])).await?;
    println!("    analytics reads usage: {}", r["results"][0]["value"]);
    let err =
        analytics.txn("tenant-acme", json!([{"op": "put", "key": "usage/api_calls", "value": "0"}])).await.unwrap_err();
    println!("    analytics tries to write: {err}");

    step(5, "Offboard a tenant: revoke its key and delete its database");
    let list = ops.get("/v1/api-keys").await?;
    let initech_key =
        list["api_keys"].as_array().unwrap().iter().find(|k| k["name"] == "initech-service").unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();
    ops.delete(&format!("/v1/api-keys/{initech_key}")).await?;
    ops.delete("/v1/dbs/tenant-initech").await?;
    // Revocation is immediate on the node that handled it and reaches every
    // other node within the credential cache TTL (30 s by default).
    let err = keys[2].txn("tenant-initech", json!([{"op": "get", "key": "settings/plan"}])).await.unwrap_err();
    println!("    offboarded tenant's service: {err}");
    let dbs = ops.get("/v1/dbs").await?;
    println!(
        "    remaining: {:?}",
        dbs["databases"].as_array().unwrap().iter().filter_map(|d| d["name"].as_str()).collect::<Vec<_>>()
    );

    server.stop().await;
    println!("\ndone.");
    Ok(())
}
