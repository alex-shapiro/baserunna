//! Multi-user todo service example.
//!
//! Each user gets an isolated database with a `todos` table, a secondary index
//! for open tasks by due date, JSON tags, filters, pagination, and bulk row
//! updates and deletes.
//!
//! Run: `cargo run --example todo_app`

#[path = "common/mod.rs"]
mod common;

use baserunna::meta::SignupPolicy;
use baserunna_client::api::{ColumnType, NewColumn, NewIndex, NewTable, RangeSpec, Selection};
use baserunna_client::{Client, Db, HttpTransport, Op, OpResult, Row};
use common::step;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Serialize, Deserialize)]
struct Todo {
    id: i64,
    title: String,
    done: bool,
    due: Option<i64>,
    tags: Option<Vec<String>>,
}

impl Row for Todo {
    const TABLE: &str = "todos";
}

/// Selects todos with the given `done` status using the `open_by_due` index.
fn by_due(done: bool) -> Selection {
    Selection { index: Some("open_by_due".into()), eq: vec![json!(done)], ..Selection::default() }
}

/// Selects open todos tagged `home` that are due before `due`.
fn open_home_before(due: i64) -> Selection {
    Selection {
        range: Some(RangeSpec { lt: Some(json!(due)), ..RangeSpec::default() }),
        filter: Some(json!({"col": "tags", "op": "contains", "value": "home"})),
        ..by_due(false)
    }
}

/// Runs `op` in a transaction.
/// Returns the modified row count and the commit version.
async fn count_rows(db: &Db, op: Op) -> Result<(u64, Option<u64>), baserunna_client::Error> {
    let mut txn = db.begin();
    let OpResult::Count(n) = txn.run_one(op).await? else { unreachable!("bulk row writes return a count") };
    Ok((n, txn.commit().await?.map(|c| c.version)))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = common::start(common::shared_store(), "todo", SignupPolicy::Open).await;
    let anon = Client::new(HttpTransport::new(&server.url));

    step(1, "Two users sign up; each gets a private `todos` database");
    let ada = anon.with_token(&anon.sign_up("ada@example.com", "correct-horse-1").await?.token).db("todos");
    let bob = anon.with_token(&anon.sign_up("bob@example.com", "battery-staple-2").await?.token).db("todos");
    for db in [&ada, &bob] {
        db.client().create_db("todos", None).await?;
        db.create_table(&NewTable {
            name: Todo::TABLE.into(),
            columns: vec![
                NewColumn::new("id", ColumnType::Int),
                NewColumn::new("title", ColumnType::String),
                NewColumn { default: Some(json!(false)), ..NewColumn::new("done", ColumnType::Bool) },
                NewColumn::nullable("due", ColumnType::Int),
                NewColumn::nullable("tags", ColumnType::Json),
            ],
            primary_key: vec!["id".into()],
            indexes: vec![NewIndex {
                name: "open_by_due".into(),
                columns: vec!["done".into(), "due".into()],
                unique: false,
            }],
            if_not_exists: false,
        })
        .await?;
    }

    step(2, "Ada adds a batch of todos in one atomic request");
    let inserts: Vec<Op> = (1..=12)
        .map(|i| {
            let tag = if i % 3 == 0 { "work" } else { "home" };
            Op::insert(&Todo {
                id: i,
                title: format!("Task #{i}"),
                done: false,
                due: Some(20261001 + (i * 37 % 30)),
                tags: Some(vec![tag.into()]),
            })
        })
        .collect();
    let c = ada.write(&inserts).await?.expect("inserts write rows");
    println!("    inserted {} rows at version {}", inserts.len(), c.version);
    bob.write(&[Op::insert(&Todo { id: 1, title: "Bob's only task".into(), done: false, due: None, tags: None })])
        .await?;

    step(3, "Complete a few tasks (partial updates)");
    let updates: Vec<Op> = [2, 5, 9].into_iter().map(|id| Op::update::<Todo>(id, &json!({"done": true}))).collect();
    ada.write(&updates).await?;

    step(4, "Open tasks ordered by due date, 4 per page, via the secondary index");
    let mut q = Some(Op::QueryRows {
        table: Todo::TABLE.into(),
        selection: by_due(false),
        limit: 4,
        reverse: false,
        cursor: None,
    });
    let mut page = 1;
    while let Some(op) = q {
        let r = ada.read_one(op.clone()).await?;
        q = op.next_page(&r);
        let titles: Vec<String> =
            r.rows::<Todo>()?.iter().map(|t| format!("{} (due {})", t.title, t.due.unwrap_or_default())).collect();
        println!("    page {page}: {}", titles.join(", "));
        page += 1;
    }

    step(5, "Due before the 15th, with a server-side filter on tags");
    for t in ada.read_one(Op::query_where::<Todo>(open_home_before(20261015))).await?.rows::<Todo>()? {
        println!("    {:?} due {} tags {:?}", t.title, t.due.unwrap_or_default(), t.tags.unwrap_or_default());
    }

    step(6, "Users are isolated: Bob sees only his own data");
    let rows = bob.read_one(Op::query_all::<Todo>()).await?.rows::<Todo>()?;
    println!("    Bob has {} todo(s): {:?}", rows.len(), rows[0].title);

    step(7, "Schema evolution: add a priority column with a default, index it");
    ada.add_column(Todo::TABLE, &NewColumn { default: Some(json!(2)), ..NewColumn::new("priority", ColumnType::Int) })
        .await?;
    let index = NewIndex { name: "by_priority".into(), columns: vec!["priority".into(), "id".into()], unique: false };
    ada.add_index(Todo::TABLE, &index).await?;
    ada.write(&[Op::update::<Todo>(7, &json!({"priority": 1}))]).await?;
    let high = Selection { index: Some("by_priority".into()), eq: vec![json!(1)], ..Selection::default() };
    let rows = ada.read_one(Op::query_where::<Todo>(high)).await?.rows::<Todo>()?;
    println!("    high priority: {}", rows[0].title);

    step(8, "Finish every open home task due before the 20th, in one transaction");
    let op = Op::update_where::<Todo>(open_home_before(20261020), &json!({"done": true}));
    let (n, version) = count_rows(&ada, op).await?;
    println!("    {n} task(s) done at version {}", version.map_or("-".into(), |v| v.to_string()));

    step(9, "Clear out finished tasks");
    let (n, _) = count_rows(&ada, Op::delete_where::<Todo>(by_due(true))).await?;
    println!("    deleted {n} finished task(s)");

    server.stop().await;
    println!("\ndone.");
    Ok(())
}
