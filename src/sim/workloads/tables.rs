//! `tables`: schema changes and row operations through the HTTP API against
//! a model, plus concurrent unique-index contention. Updates and deletes of
//! many rows by selection must change exactly the rows the model selects,
//! and a follower of the table's change feed must reproduce the model's
//! rows. At quiescence every index is recomputed from the rows and must
//! match exactly.

use super::super::Sim;
use super::super::client::Client;
use crate::engine::{Consistency, Isolation};
use serde_json::{Value as Json, json};
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
struct Row {
    email: Option<String>,
    team: String,
    score: i64,
}

fn row_json(id: i64, r: &Row) -> Json {
    json!({"id": id, "email": r.email, "team": r.team, "score": r.score})
}

fn strip(v: &Json) -> Json {
    json!({"id": v["id"], "email": v["email"], "team": v["team"], "score": v["score"]})
}

async fn retry(c: &Client, path: &str, body: Json) -> Result<super::super::client::Resp, String> {
    Ok(c.call_settled("POST", path, Some(body)).await)
}

/// Returns up to `limit` row IDs in `model` matching `team` and `score >= min`,
/// ordered by `(score, id)` to match `by_team_score`.
fn select(model: &BTreeMap<i64, Row>, team: &str, min: i64, limit: Option<usize>) -> Vec<i64> {
    let mut hits: Vec<(i64, i64)> =
        model.iter().filter(|(_, m)| m.team == team && m.score >= min).map(|(i, m)| (m.score, *i)).collect();
    hits.sort();
    hits.into_iter().map(|(_, i)| i).take(limit.unwrap_or(usize::MAX)).collect()
}

/// Runs one-shot `ops` with an idempotency key until definitive, returning
/// `None` if replayed.
async fn once(c: &Client, ops: Json, key: &str) -> Result<Option<Json>, String> {
    let body = json!({"ops": ops, "idempotency_key": key});
    loop {
        match c.call("POST", "/v1/dbs/tdb/txn", Some(body.clone())).await {
            Ok(r) if r.status == 200 => return Ok((r.json["replayed"] != true).then(|| r.json["results"].clone())),
            Ok(r) if r.code() == "conflict" || r.retryable_not_applied() => c.pause(50).await,
            Ok(r) if r.status != 503 && r.status != 429 => return Err(format!("{key}: {} {}", r.status, r.json)),
            _ => c.pause(super::super::client::ZOMBIE_WAIT_MS).await,
        }
    }
}

struct Follower {
    cursor: String,
    rows: BTreeMap<i64, Json>,
}

impl Follower {
    /// Reads the change feed until caught up and verifies rows with ID < 1000
    /// against `model`. Resets from a full table query if the cursor expires.
    async fn follow(&mut self, c: &Client, model: &BTreeMap<i64, Row>) -> Result<(), String> {
        loop {
            let p = format!("/v1/dbs/tdb/tables/players/changes?after={}&limit={}", self.cursor, 1 + c.rng().below(6));
            let r = c.call_retry("GET", &p, None).await;
            match r.status {
                200 => {}
                410 => {
                    let q = c
                        .call_retry("POST", "/v1/dbs/tdb/tables/players/rows/query", Some(json!({"limit": 1000})))
                        .await;
                    self.cursor = q.json["version"].as_u64().ok_or("no version")?.to_string();
                    let rows = q.json["rows"].as_array().cloned().unwrap_or_default();
                    self.rows = rows.iter().filter_map(|r| Some((r["id"].as_i64()?, strip(r)))).collect();
                    continue;
                }
                _ => return Err(format!("row feed: {} {}", r.status, r.json)),
            }
            for ch in r.json["changes"].as_array().into_iter().flatten() {
                let id = ch["pk"][0].as_i64().ok_or("change without a pk")?;
                match ch["deleted"] == true {
                    true => self.rows.remove(&id),
                    false => self.rows.insert(id, strip(&ch["row"])),
                };
            }
            self.cursor = r.json["next_cursor"].as_str().ok_or("no next_cursor")?.to_string();
            if r.json["more"] != true {
                break;
            }
        }
        let got: Vec<&Json> = self.rows.range(..1000).map(|(_, r)| r).collect();
        let want: Vec<Json> = model.iter().map(|(i, m)| row_json(*i, m)).collect();
        if got.len() != want.len() || got.iter().zip(&want).any(|(g, w)| *g != w) {
            return Err(format!("the row feed's rows differ:\n got {got:?}\nwant {want:?}"));
        }
        Ok(())
    }
}

/// Verifies all table indexes and checks that no orphaned data remains from
/// dropped tables or indexes.
pub async fn check_all_indexes(sim: &Arc<Sim>, db_id: &str) -> Result<(), String> {
    let n = sim.up_nodes().first().copied().ok_or("no node up")?;
    let node = sim.node(n).ok_or("node gone")?.node.clone();
    let db = node.engine.db(db_id);
    let mut t = crate::node::retry_unavailable(|| db.begin(Isolation::Serializable, Consistency::Strong))
        .await
        .map_err(|e| e.to_string())?;
    let tables = crate::table::list_tables(&mut t).await.map_err(|e| e.to_string())?;
    for s in tables {
        crate::table::check_indexes(&mut t, &s.name).await.map_err(|e| format!("table {}: {e}", s.name))?;
    }
    crate::table::check_no_orphans(&mut t).await.map_err(|e| e.to_string())
}

pub async fn run(sim: Arc<Sim>, steps: usize, contenders: usize, chaos: bool) -> Result<(), String> {
    let c = Client::signup(&sim, "tables", "t@tables.test", "password-123").await?;
    c.create_db("tdb").await?;
    let base = "/v1/dbs/tdb";
    let r = c
        .call_retry(
            "POST",
            &format!("{base}/tables"),
            Some(json!({
                "name": "players", "if_not_exists": true,
                "columns": [
                    {"name": "id", "type": "int", "nullable": false},
                    {"name": "email", "type": "string"},
                    {"name": "team", "type": "string", "nullable": false},
                    {"name": "score", "type": "int", "nullable": false, "default": 0}
                ],
                "primary_key": ["id"],
                "indexes": [
                    {"name": "by_email", "columns": ["email"], "unique": true},
                    {"name": "by_team_score", "columns": ["team", "score"]}
                ]
            })),
        )
        .await;
    if r.status != 201 {
        return Err(format!("create table: {} {}", r.status, r.json));
    }
    let chaos_h = chaos.then(|| super::spawn_chaos(&sim));
    // Contenders race to claim the same unique emails.
    let mut hs = Vec::new();
    for k in 0..contenders {
        let cc = Client::with_token(&sim, &format!("contender{k}"), c.token.as_deref().unwrap_or_default());
        hs.push(tokio::spawn(async move {
            let mut won = Vec::new();
            for e in 0..6 {
                let id = 1000 + (k as i64) * 100 + e;
                let body = json!({"row": {"id": id, "email": format!("claim{e}@x.io"), "team": "contest", "score": 1}});
                loop {
                    match cc.post("/v1/dbs/tdb/tables/players/rows", body.clone()).await {
                        Ok(r) if r.status == 201 => {
                            won.push(id);
                            break;
                        }
                        Ok(r) if r.status == 409 && r.code() == "already_exists" => break,
                        Ok(r) if r.status == 409 || r.retryable_not_applied() => {}
                        Ok(r) if r.status != 503 => return Err(format!("contender: {} {}", r.status, r.json)),
                        // Ambiguous: check if our row exists after waiting out zombies.
                        _ => {
                            cc.pause(super::super::client::ZOMBIE_WAIT_MS).await;
                            let g = cc
                                .call_retry("POST", "/v1/dbs/tdb/tables/players/rows/get", Some(json!({"pk": id})))
                                .await;
                            if g.json["found"] == true {
                                won.push(id);
                                break;
                            }
                        }
                    }
                    cc.pause(50).await;
                }
            }
            Ok::<Vec<i64>, String>(won)
        }));
    }
    let mut model: BTreeMap<i64, Row> = BTreeMap::new();
    let mut rng = sim.fork_rng();
    let mut follower = Follower { cursor: "0".into(), rows: BTreeMap::new() };
    for step in 0..steps {
        let id = rng.below(40) as i64;
        let email = if rng.chance(0.3) { None } else { Some(format!("u{}@m.io", rng.below(15))) };
        let team = ["red", "blue"][rng.usize(2)].to_string();
        let score = rng.below(10) as i64;
        match rng.below(9) {
            6 => {
                let limit = rng.chance(0.4).then(|| 1 + rng.usize(3));
                let (to_team, to_score) = (["red", "blue"][rng.usize(2)], rng.below(10) as i64);
                let mut op = json!({"op": "update_rows", "table": "players",
                    "where": {"index": "by_team_score", "eq": [team], "range": {"gte": score}},
                    "set": {"team": to_team, "score": to_score}});
                if let Some(l) = limit {
                    op["limit"] = json!(l);
                }
                let ids = select(&model, &team, score, limit);
                if let Some(res) = once(&c, json!([op]), &format!("upd-{step}")).await?
                    && res[0]["count"] != ids.len()
                {
                    return Err(format!("step {step}: update_rows changed {}, want {}", res[0]["count"], ids.len()));
                }
                for i in ids {
                    let m = model.get_mut(&i).ok_or("selected a missing row")?;
                    (m.team, m.score) = (to_team.to_string(), to_score);
                }
            }
            7 => {
                let limit = rng.chance(0.4).then(|| 1 + rng.usize(3));
                let mut op = json!({"op": "delete_rows", "table": "players",
                    "where": {"range": {"lt": 1000}, "filter": {"col": "score", "op": "lt", "value": score / 2}}});
                if let Some(l) = limit {
                    op["limit"] = json!(l);
                }
                let ids: Vec<i64> = model
                    .iter()
                    .filter(|(_, m)| m.score < score / 2)
                    .map(|(i, _)| *i)
                    .take(limit.unwrap_or(usize::MAX))
                    .collect();
                if let Some(res) = once(&c, json!([op]), &format!("del-{step}")).await?
                    && res[0]["count"] != ids.len()
                {
                    return Err(format!("step {step}: delete_rows removed {}, want {}", res[0]["count"], ids.len()));
                }
                for i in ids {
                    model.remove(&i);
                }
            }
            8 => {
                // Setting a constant score is idempotent, so retries without an idempotency key are safe.
                let r = retry(
                    &c,
                    &format!("{base}/tables/players/rows/update"),
                    json!({"where": {"index": "by_team_score", "eq": [team], "range": {"gte": score}}, "set": {"score": 9}}),
                )
                .await?;
                if r.status != 200 || r.json["count"].as_u64().is_none() {
                    return Err(format!("step {step}: update by selection: {} {}", r.status, r.json));
                }
                for i in select(&model, &team, score, None) {
                    model.get_mut(&i).ok_or("selected a missing row")?.score = 9;
                }
                follower.follow(&c, &model).await?;
            }
            0 | 1 => {
                let dup = email.is_some() && model.iter().any(|(k, v)| *k != id && v.email == email);
                let row = Row { email: email.clone(), team: team.clone(), score };
                let r = retry(
                    &c,
                    &format!("{base}/tables/players/rows"),
                    json!({"row": row_json(id, &row), "upsert": true}),
                )
                .await?;
                match (r.status, dup) {
                    (200, false) => {
                        model.insert(id, row);
                    }
                    (409, true) if r.code() == "already_exists" => {}
                    _ => return Err(format!("step {step} upsert {id}: {} {} (dup={dup})", r.status, r.json)),
                }
            }
            2 => {
                let r = retry(
                    &c,
                    &format!("{base}/tables/players/rows/update"),
                    json!({"pk": id, "set": {"score": score}}),
                )
                .await?;
                if r.json["found"] != model.contains_key(&id) {
                    return Err(format!("update found mismatch: {}", r.json));
                }
                if let Some(m) = model.get_mut(&id) {
                    m.score = score;
                }
            }
            3 => {
                let r = retry(&c, &format!("{base}/tables/players/rows/delete"), json!({"pk": id})).await?;
                // A retried delete may report found=false for a row an earlier attempt removed.
                if r.json["found"] == true && !model.contains_key(&id) {
                    return Err(format!("delete found a row the model lacks: {id}"));
                }
                model.remove(&id);
            }
            4 => {
                let r = retry(&c, &format!("{base}/tables/players/rows/get"), json!({"pk": id})).await?;
                let got = (r.json["found"] == true).then(|| strip(&r.json["row"]));
                let want = model.get(&id).map(|m| row_json(id, m));
                if got != want {
                    return Err(format!("get {id}: {got:?} vs {want:?}"));
                }
            }
            _ => {
                let lim = 1 + rng.usize(4);
                let mut q = json!({"index": "by_team_score", "eq": [team], "range": {"gte": score}, "limit": lim});
                let mut got = Vec::new();
                loop {
                    let r = retry(&c, &format!("{base}/tables/players/rows/query"), q.clone()).await?;
                    if r.status != 200 {
                        return Err(format!("query: {}", r.json));
                    }
                    got.extend(r.json["rows"].as_array().cloned().unwrap_or_default().iter().map(strip));
                    match r.json["next_cursor"].as_str() {
                        Some(cur) => q["cursor"] = json!(cur),
                        None => break,
                    }
                }
                got.retain(|r| r["id"].as_i64().is_some_and(|i| i < 1000));
                let mut want: Vec<(i64, i64)> = model
                    .iter()
                    .filter(|(_, m)| m.team == team && m.score >= score)
                    .map(|(i, m)| (m.score, *i))
                    .collect();
                want.sort();
                let want: Vec<Json> = want.iter().map(|(_, i)| row_json(*i, &model[i])).collect();
                if got != want {
                    return Err(format!("query team={team} score>={score}: {got:?} vs {want:?}"));
                }
            }
        }
    }
    let mut winners: BTreeMap<String, i64> = BTreeMap::new();

    for h in hs {
        for id in h.await.map_err(|e| e.to_string())?? {
            let e = (id - 1000) % 100;
            if let Some(prev) = winners.insert(format!("claim{e}@x.io"), id) {
                return Err(format!("unique email claim{e}@x.io won by {prev} and {id}"));
            }
        }
    }
    if let Some(h) = chaos_h {
        h.abort();
    }
    super::recover(&sim).await;
    let r = c
        .call_retry(
            "POST",
            &format!("{base}/tables/players/rows/query"),
            Some(json!({"index": "by_team_score", "eq": ["contest"], "limit": 1000})),
        )
        .await;
    let rows = r.json["rows"].as_array().cloned().unwrap_or_default();
    if rows.len() != 6.min(contenders * 6) && contenders > 0 {
        return Err(format!("expected 6 contested rows, found {}", rows.len()));
    }
    for row in &rows {
        let email = row["email"].as_str().unwrap_or_default().to_string();
        if winners.get(&email) != row["id"].as_i64().as_ref() {
            return Err(format!("contested row {row} is not the acknowledged winner"));
        }
    }
    let r = c
        .call_retry(
            "POST",
            &format!("{base}/tables/players/columns"),
            Some(json!({"name": "level", "type": "int", "nullable": false, "default": 3})),
        )
        .await;
    if r.status != 200 && !(r.status == 409 && r.code() == "already_exists") {
        return Err(format!("add column: {} {}", r.status, r.json));
    }
    let r = c
        .call_retry(
            "POST",
            &format!("{base}/tables/players/indexes"),
            Some(json!({"name": "by_level", "columns": ["level", "id"]})),
        )
        .await;
    if r.status != 200 && !(r.status == 409 && r.code() == "already_exists") {
        return Err(format!("add index: {} {}", r.status, r.json));
    }
    let info = c.call_retry("GET", base, None).await;
    let db_id = info.json["id"].as_str().ok_or("no db id")?.to_string();
    check_all_indexes(&sim, &db_id).await?;
    follower.follow(&c, &model).await?;
    let contested: BTreeMap<String, i64> =
        follower.rows.range(1000..).map(|(i, r)| (r["email"].as_str().unwrap_or_default().to_string(), *i)).collect();
    if contested != winners {
        return Err(format!("the row feed's contested rows {contested:?} are not the winners {winners:?}"));
    }
    let r = c
        .call_retry(
            "POST",
            &format!("{base}/tables/players/rows/query"),
            Some(json!({"range": {"lt": 1000}, "limit": 1000})),
        )
        .await;
    let got: Vec<Json> = r.json["rows"].as_array().cloned().unwrap_or_default().iter().map(strip).collect();
    let want: Vec<Json> = model.iter().map(|(i, m)| row_json(*i, m)).collect();
    if got != want {
        return Err(format!("final rows differ:\n got {got:?}\nwant {want:?}"));
    }
    drop_a_large_table(&sim, &c, base, &db_id).await
}

/// Tests large table and index drops that trigger range tombstones.
async fn drop_a_large_table(sim: &Arc<Sim>, c: &Client, base: &str, db_id: &str) -> Result<(), String> {
    let t = format!("{base}/tables/archive");
    let create = json!({
        "name": "archive", "if_not_exists": true,
        "columns": [
            {"name": "id", "type": "int", "nullable": false},
            {"name": "tag", "type": "string", "nullable": false}
        ],
        "primary_key": ["id"],
        "indexes": [{"name": "by_tag", "columns": ["tag"]}]
    });
    let r = c.call_settled("POST", &format!("{base}/tables"), Some(create.clone())).await;
    if r.status != 201 {
        return Err(format!("create archive: {} {}", r.status, r.json));
    }
    for b in 0..4 {
        let rows: Vec<Json> = (0..40).map(|i| json!({"id": b * 40 + i, "tag": format!("tag-{}", i % 7)})).collect();
        let r = c.call_settled("POST", &format!("{t}/rows"), Some(json!({"rows": rows, "upsert": true}))).await;
        if r.status != 200 {
            return Err(format!("fill archive: {} {}", r.status, r.json));
        }
    }
    let count = |r: &super::super::client::Resp| r.json["rows"].as_array().map_or(0, |a| a.len());
    let r = c.call_settled("POST", &format!("{t}/rows/query"), Some(json!({"limit": 1000}))).await;
    if count(&r) != 160 {
        return Err(format!("archive holds {} rows, want 160", count(&r)));
    }
    for path in [format!("{t}/indexes/by_tag"), t.clone()] {
        let r = c.call_settled("DELETE", &path, None).await;
        // Ignore 404 if a previous attempt dropped it under fault injection.
        if r.status != 200 && r.status != 404 {
            return Err(format!("DELETE {path}: {} {}", r.status, r.json));
        }
        check_all_indexes(sim, db_id).await?;
    }
    let r = c.call_settled("GET", &t, None).await;
    if r.status != 404 {
        return Err(format!("dropped table still there: {} {}", r.status, r.json));
    }
    let r = c.call_settled("POST", &format!("{base}/tables"), Some(create)).await;
    if r.status != 201 {
        return Err(format!("recreate archive: {} {}", r.status, r.json));
    }
    let r = c.call_settled("POST", &format!("{t}/rows/query"), Some(json!({"index": "by_tag", "limit": 1000}))).await;
    if r.status != 200 || count(&r) != 0 {
        return Err(format!("recreated table not empty: {} {}", r.status, r.json));
    }
    check_all_indexes(sim, db_id).await
}
