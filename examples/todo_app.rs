//! Real-world example: a multi-user todo service.
//!
//! Every user gets their own database with a `todos` table, a secondary
//! index for "open tasks by due date", JSON tags, filters and pagination,
//! and updates and deletes of many tasks at once. Users are isolated from
//! each other by construction.
//!
//! Run: `cargo run --example todo_app`

#[path = "common/mod.rs"]
mod common;

use baserunna::client::Client;
use baserunna::meta::SignupPolicy;
use common::step;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = common::start(common::shared_store(), "todo", SignupPolicy::Open).await;
    let anon = Client::new(&server.url);

    step(1, "Two users sign up; each gets a private `todos` database");
    let ada = anon.signup("ada@example.com", "correct-horse-1").await?;
    let bob = anon.signup("bob@example.com", "battery-staple-2").await?;
    for u in [&ada, &bob] {
        u.create_db("todos").await?;
        u.post(
            "/v1/dbs/todos/tables",
            json!({
                "name": "todos",
                "columns": [
                    {"name": "id", "type": "int", "nullable": false},
                    {"name": "title", "type": "string", "nullable": false},
                    {"name": "done", "type": "bool", "nullable": false, "default": false},
                    {"name": "due", "type": "int"},
                    {"name": "tags", "type": "json"}
                ],
                "primary_key": ["id"],
                "indexes": [{"name": "open_by_due", "columns": ["done", "due"]}]
            }),
        )
        .await?;
    }

    step(2, "Ada adds a batch of todos in one atomic request");
    let todos: Vec<_> = (1..=12)
        .map(|i| {
            json!({"id": i, "title": format!("Task #{i}"), "due": 20261001 + (i * 37 % 30),
                   "tags": if i % 3 == 0 { json!(["work"]) } else { json!(["home"]) }})
        })
        .collect();
    let r = ada.post("/v1/dbs/todos/tables/todos/rows", json!({"rows": todos})).await?;
    println!("    inserted {} rows at version {}", r["count"], r["version"]);
    bob.post("/v1/dbs/todos/tables/todos/rows", json!({"row": {"id": 1, "title": "Bob's only task"}})).await?;

    step(3, "Complete a few tasks (partial updates)");
    for id in [2, 5, 9] {
        ada.post("/v1/dbs/todos/tables/todos/rows/update", json!({"pk": id, "set": {"done": true}})).await?;
    }

    step(4, "Open tasks ordered by due date, 4 per page, via the secondary index");
    let mut q = json!({"index": "open_by_due", "eq": [false], "limit": 4});
    let mut page = 1;
    loop {
        let r = ada.post("/v1/dbs/todos/tables/todos/rows/query", q.clone()).await?;
        let titles: Vec<String> = r["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| format!("{} (due {})", t["title"].as_str().unwrap(), t["due"]))
            .collect();
        println!("    page {page}: {}", titles.join(", "));
        match r["next_cursor"].as_str() {
            Some(c) => q["cursor"] = json!(c),
            None => break,
        }
        page += 1;
    }

    step(5, "Due before the 15th, with a server-side filter on tags");
    let r = ada
        .post(
            "/v1/dbs/todos/tables/todos/rows/query",
            json!({"index": "open_by_due", "eq": [false], "range": {"lt": 20261015},
                   "filter": {"col": "tags", "op": "eq", "value": ["home"]}}),
        )
        .await?;
    for t in r["rows"].as_array().unwrap() {
        println!("    {} due {} tags {}", t["title"], t["due"], t["tags"]);
    }

    step(6, "Users are isolated: Bob sees only his own data");
    let r = bob.post("/v1/dbs/todos/tables/todos/rows/query", json!({})).await?;
    println!("    Bob has {} todo(s): {}", r["rows"].as_array().unwrap().len(), r["rows"][0]["title"]);

    step(7, "Schema evolution: add a priority column with a default, index it");
    ada.post(
        "/v1/dbs/todos/tables/todos/columns",
        json!({"name": "priority", "type": "int", "nullable": false, "default": 2}),
    )
    .await?;
    ada.post("/v1/dbs/todos/tables/todos/indexes", json!({"name": "by_priority", "columns": ["priority", "id"]}))
        .await?;
    ada.post("/v1/dbs/todos/tables/todos/rows/update", json!({"pk": 7, "set": {"priority": 1}})).await?;
    let r = ada.post("/v1/dbs/todos/tables/todos/rows/query", json!({"index": "by_priority", "eq": [1]})).await?;
    println!("    high priority: {}", r["rows"][0]["title"]);

    step(8, "Finish every open home task due before the 20th, in one transaction");
    let r = ada
        .post(
            "/v1/dbs/todos/tables/todos/rows/update",
            json!({"where": {"index": "open_by_due", "eq": [false], "range": {"lt": 20261020},
                             "filter": {"col": "tags", "op": "eq", "value": ["home"]}},
                   "set": {"done": true}}),
        )
        .await?;
    println!("    {} task(s) done at version {}", r["count"], r["version"]);

    step(9, "Clear out finished tasks");
    let r = ada
        .post("/v1/dbs/todos/tables/todos/rows/delete", json!({"where": {"index": "open_by_due", "eq": [true]}}))
        .await?;
    println!("    deleted {} finished task(s)", r["count"]);

    server.stop().await;
    println!("\ndone.");
    Ok(())
}
