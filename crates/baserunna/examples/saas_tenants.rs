//! Multi-tenant SaaS backend example with one database per tenant.
//!
//! The operator account provisions a database per customer with a database-scoped
//! API key and a read-only analytics key.
//!
//! Run: `cargo run --example saas_tenants`

#[path = "common/mod.rs"]
mod common;

use baserunna::meta::SignupPolicy;
use baserunna_client::{Client, HttpTransport, Op, api};
use common::step;

/// Returns the names of the databases that `client` can list.
async fn db_names(client: &Client) -> Result<Vec<String>, baserunna_client::Error> {
    let page = client.list_dbs(api::ListDbsQuery::default()).await?;
    Ok(page.databases.into_iter().map(|d| d.name).collect())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = common::start(common::shared_store(), "saas", SignupPolicy::FirstUserOnly).await;
    let anon = Client::new(HttpTransport::new(&server.url));
    let ops = anon.with_token(&anon.sign_up("ops@saas.example", "operator-pass-1").await?.token);

    step(1, "Provision a database and a scoped API key per tenant");
    let tenants = ["acme", "globex", "initech"];
    let mut keys = Vec::new();
    for t in tenants {
        ops.create_db(&format!("tenant-{t}"), None).await?;
        let new =
            api::NewApiKey { name: format!("{t}-service"), db: Some(format!("tenant-{t}")), ..Default::default() };
        let k = ops.create_api_key(&new).await?;
        println!("    tenant-{t}: key {}…", &k.key[..12]);
        keys.push(ops.with_token(&k.key));
    }

    step(2, "Each tenant's service writes its own data with its own key");
    for (t, svc) in tenants.iter().zip(&keys) {
        svc.db(&format!("tenant-{t}"))
            .write(&[
                Op::put("settings/plan", if *t == "acme" { "enterprise" } else { "starter" }),
                Op::increment("usage/api_calls", 1200),
                Op::put("users/1", format!("{{\"name\":\"admin@{t}.example\"}}")),
            ])
            .await?;
    }

    step(3, "Isolation: a tenant key cannot reach another tenant's database");
    let err = keys[0].db("tenant-globex").read_one(Op::get("settings/plan")).await.unwrap_err();
    println!("    acme key -> tenant-globex: {err}");
    println!("    acme key lists databases: {:?}", db_names(&keys[0]).await?);
    let err = keys[0].create_db("sneaky", None).await.unwrap_err();
    println!("    acme key creating a database: {err}");

    step(4, "A read-only analytics key over one tenant");
    let new = api::NewApiKey {
        name: "analytics".into(),
        db: Some("tenant-acme".into()),
        read_only: true,
        ..Default::default()
    };
    let analytics = ops.with_token(&ops.create_api_key(&new).await?.key).db("tenant-acme");
    let usage = analytics.read_one(Op::get("usage/api_calls")).await?.entry()?;
    let usage = usage.and_then(|e| Some(String::from_utf8_lossy(e.value.data()?).into_owned())).unwrap_or_default();
    println!("    analytics reads usage: {usage}");
    let err = analytics.write(&[Op::put("usage/api_calls", "0")]).await.unwrap_err();
    println!("    analytics tries to write: {err}");

    step(5, "Offboard a tenant: revoke its key and delete its database");
    let list = ops.list_api_keys(api::PageQuery::default()).await?;
    let initech_key = list.api_keys.iter().find(|k| k.name == "initech-service").unwrap();
    ops.delete_api_key(&initech_key.id).await?;
    ops.db("tenant-initech").delete().await?;
    // Revocation is immediate on the handling node and propagates to other
    // nodes within the credential cache TTL.
    let err = keys[2].db("tenant-initech").read_one(Op::get("settings/plan")).await.unwrap_err();
    println!("    offboarded tenant's service: {err}");
    println!("    remaining: {:?}", db_names(&ops).await?);

    server.stop().await;
    println!("\ndone.");
    Ok(())
}
