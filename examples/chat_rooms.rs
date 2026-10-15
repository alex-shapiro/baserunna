//! Chat rooms on streams.
//!
//! Each room is a stream, created by its first message. Posting a message
//! appends to the room and bumps the poster's message count in one
//! transaction; members receive new messages by waiting on the room;
//! `expect_seq` lets a client post only if it has seen everything before;
//! and each member's read position is a consumer cursor, so unread counts
//! come from the stream itself.
//!
//! Run: `cargo run --example chat_rooms`

#[path = "common/mod.rs"]
mod common;

use baserunna::client::Client;
use baserunna::meta::SignupPolicy;
use common::step;
use serde_json::json;

async fn post(c: &Client, room: &str, from: &str, text: &str) -> Result<u64, Box<dyn std::error::Error>> {
    let r = c
        .txn(
            "chat",
            json!([
                {"op": "append", "stream": room, "create": true, "records": [{"json": {"from": from, "text": text}, "headers": {"from": from}}]},
                {"op": "increment", "key": format!("posts/{from}"), "by": 1},
            ]),
        )
        .await?;
    Ok(r["results"][0]["first_seq"].as_u64().unwrap_or(0))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = common::start(common::shared_store(), "chat", SignupPolicy::FirstUserOnly).await;
    let app = Client::new(&server.url).signup("chat@example.com", "chat-admin-1").await?;
    app.create_db("chat").await?;

    step(1, "Rooms are streams, created by their first message");
    for (room, from, text) in [
        ("room:general", "ann", "hello everyone"),
        ("room:general", "bob", "hi ann!"),
        ("room:rust", "ann", "anyone tried the new borrow checker?"),
        ("room:general", "cyd", "morning"),
    ] {
        let seq = post(&app, room, from, text).await?;
        println!("    {room} #{seq} {from}: {text}");
    }
    let rooms = app.get("/v1/dbs/chat/streams?prefix=room:").await?;
    for r in rooms["streams"].as_array().unwrap() {
        println!("    {} holds {} messages", r["name"].as_str().unwrap_or(""), r["next_seq"]);
    }

    step(2, "Bob waits in #general; Ann posts");
    let waiter = {
        let c = app.clone();
        tokio::spawn(async move { c.read_stream("chat", "room:general", "from_seq=3&wait_ms=10000").await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    post(&app, "room:general", "ann", "standup in 5").await?;
    let page = waiter.await??;
    for m in page["records"].as_array().unwrap() {
        println!(
            "    bob receives #{} {}: {}",
            m["seq"],
            m["json"]["from"].as_str().unwrap_or(""),
            m["json"]["text"].as_str().unwrap_or("")
        );
    }

    step(3, "Post only if you have seen the latest message (expect_seq)");
    let r =
        app.append("chat", "room:general", json!([{"json": {"from": "cyd", "text": "replying to #2"}}]), None).await?;
    println!("    cyd posted #{}", r["first_seq"]);
    let stale = app
        .post(
            "/v1/dbs/chat/streams/room:general/records",
            json!({"records": [{"json": {"from": "bob", "text": "replying to #3"}}], "expect_seq": 4}),
        )
        .await;
    match stale {
        Err(e) => println!("    bob's reply expected #4 to be next: HTTP {} {}", e.status, e.message),
        Ok(v) => println!("    unexpected: {v}"),
    }

    step(4, "Read positions and unread counts");
    app.call("PUT", "/v1/dbs/chat/streams/room:general/consumers/bob", Some(json!({"seq": 5}))).await?;
    app.call("PUT", "/v1/dbs/chat/streams/room:general/consumers/ann", Some(json!({"seq": 2}))).await?;
    let info = app.get("/v1/dbs/chat/streams/room:general").await?;
    for c in info["consumers"].as_array().unwrap() {
        println!("    {} reads from #{} next and has {} unread", c["name"].as_str().unwrap_or(""), c["seq"], c["lag"]);
    }
    let unread = app.read_stream("chat", "room:general", "consumer=ann").await?;
    let texts: Vec<&str> =
        unread["records"].as_array().unwrap().iter().filter_map(|m| m["json"]["text"].as_str()).collect();
    println!("    ann's unread: {texts:?}");

    step(5, "History: the last three messages, newest first, and messages from Ann");
    let last = app.read_stream("chat", "room:general", "tail=3&reverse=true").await?;
    for m in last["records"].as_array().unwrap() {
        println!(
            "    #{} {}: {}",
            m["seq"],
            m["json"]["from"].as_str().unwrap_or(""),
            m["json"]["text"].as_str().unwrap_or("")
        );
    }
    let anns = app
        .query_stream("chat", "room:general", json!({"filter": {"header": "from", "op": "eq", "value": "ann"}}))
        .await?;
    println!("    ann wrote {} messages here", anns["records"].as_array().map_or(0, Vec::len));
    let counts = app.txn("chat", json!([{"op": "get", "key": "posts/ann"}, {"op": "get", "key": "posts/bob"}])).await?;
    println!(
        "    posts counted with the messages: ann {}, bob {}",
        counts["results"][0]["value"].as_str().unwrap_or("0"),
        counts["results"][1]["value"].as_str().unwrap_or("0")
    );

    server.stop().await;
    println!("\ndone.");
    Ok(())
}
