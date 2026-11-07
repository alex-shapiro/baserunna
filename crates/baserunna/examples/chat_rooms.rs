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

use baserunna::meta::SignupPolicy;
use baserunna_client::{Client, Db, HttpTransport, NewRecord, Op, api};
use common::step;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Serialize, Deserialize)]
struct Message {
    from: String,
    text: String,
}

impl Message {
    fn new(from: &str, text: &str) -> Message {
        Message { from: from.into(), text: text.into() }
    }
}

fn message(r: &api::StreamRecord) -> Result<Message, serde_json::Error> {
    serde_json::from_value(r.json.clone().unwrap_or_default())
}

async fn post(db: &Db, room: &str, from: &str, text: &str) -> Result<u64, Box<dyn std::error::Error>> {
    let record = NewRecord::json(&Message::new(from, text)).header("from", from);
    let ops = [Op::append_records(room, vec![record]), Op::increment(format!("posts/{from}"), 1)];
    let c = db.write(&ops).await?.expect("appends commit");
    Ok(c.appends[0])
}

fn print_messages(prefix: &str, page: &api::RecordPage) -> Result<(), serde_json::Error> {
    for r in &page.records {
        let m = message(r)?;
        println!("    {prefix}#{} {}: {}", r.seq, m.from, m.text);
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let server = common::start(common::shared_store(), "chat", SignupPolicy::FirstUserOnly).await;
    let anon = Client::new(HttpTransport::new(&server.url));
    let session = anon.sign_up("chat@example.com", "chat-admin-1").await?;
    let app = anon.with_token(&session.token);
    app.create_db("chat", None).await?;
    let db = app.db("chat");

    step(1, "Rooms are streams, created by their first message");
    for (room, from, text) in [
        ("room:general", "ann", "hello everyone"),
        ("room:general", "bob", "hi ann!"),
        ("room:rust", "ann", "anyone tried the new borrow checker?"),
        ("room:general", "cyd", "morning"),
    ] {
        let seq = post(&db, room, from, text).await?;
        println!("    {room} #{seq} {from}: {text}");
    }
    let rooms = db.list_streams(api::ListStreamsQuery { prefix: Some("room:".into()), ..Default::default() }).await?;
    for r in &rooms.streams {
        println!("    {} holds {} messages", r.name, r.state.next_seq);
    }

    step(2, "Bob waits in #general; Ann posts");
    let waiter = {
        let db = db.clone();
        let q = api::ReadRecordsQuery { from_seq: Some(3), wait_ms: Some(10_000), ..Default::default() };
        tokio::spawn(async move { db.read_records("room:general", q).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    post(&db, "room:general", "ann", "standup in 5").await?;
    print_messages("bob receives ", &waiter.await??)?;

    step(3, "Post only if you have seen the latest message (expect_seq)");
    let record = |m: Message| api::NewRecord { json: Some(json!(m)), ..Default::default() };
    let reply =
        api::AppendRequest { records: vec![record(Message::new("cyd", "replying to #2"))], ..Default::default() };
    let r = db.append_records("room:general", &reply).await?;
    println!("    cyd posted #{}", r.first_seq);
    let stale = api::AppendRequest {
        records: vec![record(Message::new("bob", "replying to #3"))],
        expect_seq: Some(4),
        ..Default::default()
    };
    match db.append_records("room:general", &stale).await {
        Err(e) => println!("    bob's reply expected #4 to be next: {e}"),
        Ok(a) => println!("    unexpected: {a:?}"),
    }

    step(4, "Read positions and unread counts");
    db.write(&[Op::set_consumer("room:general", "bob", 5), Op::set_consumer("room:general", "ann", 2)]).await?;
    let info = db.stream_info("room:general").await?;
    for c in &info.consumers {
        println!("    {} reads from #{} next and has {} unread", c.name, c.seq, c.lag);
    }
    let unread = db
        .read_records("room:general", api::ReadRecordsQuery { consumer: Some("ann".into()), ..Default::default() })
        .await?;
    let texts = unread.records.iter().map(|r| message(r).map(|m| m.text)).collect::<Result<Vec<_>, _>>()?;
    println!("    ann's unread: {texts:?}");

    step(5, "History: the last three messages, newest first, and messages from Ann");
    let last = db
        .read_records("room:general", api::ReadRecordsQuery { tail: Some(3), reverse: true, ..Default::default() })
        .await?;
    print_messages("", &last)?;
    let from_ann =
        api::RecordQuery { filter: Some(json!({"header": "from", "op": "eq", "value": "ann"})), ..Default::default() };
    let anns = db.query_records("room:general", &from_ann).await?;
    println!("    ann wrote {} messages here", anns.records.len());
    let counts = db.read(&[Op::get("posts/ann"), Op::get("posts/bob")]).await?;
    let mut posts = Vec::new();
    for r in counts {
        let n = r.entry()?.and_then(|e| e.value.data().map(|b| String::from_utf8_lossy(b).into_owned()));
        posts.push(n.unwrap_or_else(|| "0".into()));
    }
    println!("    posts counted with the messages: ann {}, bob {}", posts[0], posts[1]);

    server.stop().await;
    println!("\ndone.");
    Ok(())
}
