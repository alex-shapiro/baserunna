//! `schema_race`: row writes on every path and at both isolation levels
//! race the creation and dropping of indexes, a column and a table. At
//! quiescence every index must match its rows, dropped tables and indexes
//! must leave nothing behind, and each writer's rows must match its model.

use super::super::Sim;
use super::super::client::{Client, Resp, ZOMBIE_WAIT_MS};
use crate::util::rng::Rng;
use serde_json::{Value as Json, json};
use std::collections::BTreeMap;
use std::sync::Arc;

const BASE: &str = "/v1/dbs/race";
const EMAILS: usize = 5;

#[derive(Clone, Debug, PartialEq)]
struct Row {
    email: Option<String>,
    n: i64,
}

fn row_json(id: i64, r: &Row) -> Json {
    json!({"id": id, "email": r.email, "n": r.n, "tag": format!("t{}", r.n % 3)})
}

/// Outcome of a write whose ambiguity has been resolved.
enum Applied {
    Yes,
    No,
}

/// Settle a request: `Yes` if it took effect, `No` if it definitively did
/// not (a 4xx the workload expects, given in `rejected`).
async fn settle(c: &Client, method: &str, path: &str, body: Json, rejected: &[(u16, &str)]) -> Result<Applied, String> {
    let r = c.call_settled(method, path, Some(body)).await;
    judge(&r, rejected)
}

fn judge(r: &Resp, rejected: &[(u16, &str)]) -> Result<Applied, String> {
    if r.ok() {
        return Ok(Applied::Yes);
    }
    if rejected.iter().any(|(s, code)| r.status == *s && r.code() == *code) {
        return Ok(Applied::No);
    }
    Err(format!("unexpected response {} {}", r.status, r.json))
}

/// A one-shot transaction with an idempotency key, retried until its
/// outcome is known.
async fn one_shot(c: &Client, body: Json, rejected: &[(u16, &str)]) -> Result<Applied, String> {
    let path = format!("{BASE}/txn");
    loop {
        match c.call("POST", &path, Some(body.clone())).await {
            Ok(r) if r.outcome_unknown() => c.pause(ZOMBIE_WAIT_MS).await,
            Ok(r) if r.status == 503 || (r.status == 409 && r.code() == "conflict") => c.pause(100).await,
            Ok(r) => return judge(&r, rejected),
            Err(_) => c.pause(ZOMBIE_WAIT_MS).await,
        }
    }
}

/// An interactive transaction of `ops`; ambiguity is resolved with a fenced
/// commit-status query.
async fn interactive(c: &Client, isolation: &str, ops: Json, rejected: &[(u16, &str)]) -> Result<Applied, String> {
    let r = c.call("POST", &format!("{BASE}/transactions"), Some(json!({"isolation": isolation}))).await;
    let Ok(r) = r else { return Ok(Applied::No) };
    if r.status != 201 {
        return judge(&r, &[(503, "unavailable")]).map(|_| Applied::No);
    }
    let id = r.json["txn_id"].as_str().unwrap_or_default().to_string();
    let cid = r.json["commit_id"].as_str().unwrap_or_default().to_string();
    match c.call("POST", &format!("{BASE}/transactions/{id}/ops"), Some(json!({"ops": ops}))).await {
        Ok(r) if r.ok() => {}
        // The transaction is gone (its node changed or an op failed):
        // nothing was committed.
        Ok(r) if r.status == 404 || r.status == 503 => return Ok(Applied::No),
        Ok(r) => return judge(&r, rejected).map(|_| Applied::No),
        Err(_) => return Ok(Applied::No),
    }
    match c.call("POST", &format!("{BASE}/transactions/{id}/commit"), Some(json!({}))).await {
        Ok(r) if r.ok() => Ok(Applied::Yes),
        Ok(r) if r.status == 404 || r.retryable_not_applied() || (r.status == 409 && r.code() == "conflict") => {
            Ok(Applied::No)
        }
        Ok(r) if !r.outcome_unknown() => judge(&r, rejected),
        _ => {
            c.pause(ZOMBIE_WAIT_MS).await;
            let st = c.call_retry("GET", &format!("{BASE}/commits/{cid}"), None).await;
            match st.json["status"].as_str() {
                Some("committed") => Ok(Applied::Yes),
                Some("not_committed") => Ok(Applied::No),
                _ => Err(format!("commit status: {}", st.json)),
            }
        }
    }
}

/// Rejections a row write may legitimately get while the schema changes:
/// a unique index rejects a duplicate email, the scratch table may not
/// exist, and the `extra` column may not exist.
const ROW_REJECTIONS: &[(u16, &str)] = &[(409, "already_exists"), (404, "not_found"), (400, "invalid_argument")];

async fn writer(c: Client, w: i64, steps: usize) -> Result<BTreeMap<i64, Row>, String> {
    let mut model: BTreeMap<i64, Row> = BTreeMap::new();
    let mut rng: Rng = c.sim.fork_rng();
    for step in 0..steps {
        c.pause(rng.below(300)).await;
        let id = w * 100 + rng.below(12) as i64;
        let row = Row {
            email: (!rng.chance(0.3)).then(|| format!("e{}@race.io", rng.below(EMAILS as u64))),
            n: rng.below(9) as i64,
        };
        let isolation = if rng.chance(0.5) { "snapshot" } else { "serializable" };
        let ctx = format!("writer {w} step {step}");
        match rng.below(6) {
            0 => {
                let body = json!({"row": row_json(id, &row), "upsert": true});
                let path = format!("{BASE}/tables/items/rows");
                if let Applied::Yes =
                    settle(&c, "POST", &path, body, ROW_REJECTIONS).await.map_err(|e| format!("{ctx}: {e}"))?
                {
                    model.insert(id, row);
                }
            }
            1 | 2 => {
                // The scratch insert may fail (table dropped), aborting the
                // whole transaction.
                let mut ops = vec![json!({"op": "upsert_row", "table": "items", "row": row_json(id, &row)})];
                if rng.chance(0.4) {
                    ops.push(json!({"op": "upsert_row", "table": "scratch", "row": {"k": id, "v": step as i64}}));
                }
                let body = json!({"ops": ops, "isolation": isolation, "idempotency_key": format!("w{w}-{step}")});
                if let Applied::Yes = one_shot(&c, body, ROW_REJECTIONS).await.map_err(|e| format!("{ctx}: {e}"))? {
                    model.insert(id, row);
                }
            }
            3 => {
                let ops = json!([{"op": "upsert_row", "table": "items", "row": row_json(id, &row)}]);
                if let Applied::Yes =
                    interactive(&c, isolation, ops, ROW_REJECTIONS).await.map_err(|e| format!("{ctx}: {e}"))?
                {
                    model.insert(id, row);
                }
            }
            4 => {
                // Partial update, sometimes of the column that comes and goes.
                let set = if rng.chance(0.3) { json!({"extra": 1}) } else { json!({"n": row.n}) };
                let body = json!({"pk": id, "set": set.clone()});
                let r = c.call_settled("POST", &format!("{BASE}/tables/items/rows/update"), Some(body)).await;
                if let Applied::Yes = judge(&r, ROW_REJECTIONS).map_err(|e| format!("{ctx}: {e}"))? {
                    if r.json["found"] != model.contains_key(&id) {
                        return Err(format!(
                            "{ctx}: update of {id} found={} model={}",
                            r.json["found"],
                            model.contains_key(&id)
                        ));
                    }
                    if let (Some(m), Some(n)) = (model.get_mut(&id), set["n"].as_i64()) {
                        m.n = n;
                    }
                }
            }
            _ => {
                let body = json!({"pk": id});
                let path = format!("{BASE}/tables/items/rows/delete");
                if let Applied::Yes = settle(&c, "POST", &path, body, &[]).await.map_err(|e| format!("{ctx}: {e}"))? {
                    model.remove(&id);
                }
            }
        }
    }
    Ok(model)
}

async fn schema_changer(c: Client, steps: usize) -> Result<(), String> {
    let mut rng: Rng = c.sim.fork_rng();
    let indexes = [
        json!({"name": "by_n", "columns": ["n"]}),
        json!({"name": "by_tag_n", "columns": ["tag", "n"]}),
        json!({"name": "uniq_email", "columns": ["email"], "unique": true}),
        json!({"name": "by_extra", "columns": ["extra", "id"]}),
    ];
    let ok_codes: &[(u16, &str)] = &[(409, "already_exists"), (404, "not_found"), (400, "invalid_argument")];
    for _ in 0..steps {
        c.pause(200 + rng.below(800)).await;
        let (method, path, body) = match rng.below(8) {
            0 | 1 => ("POST", format!("{BASE}/tables/items/indexes"), Some(indexes[rng.usize(indexes.len())].clone())),
            2 | 3 => {
                let name = indexes[rng.usize(indexes.len())]["name"].as_str().unwrap_or_default().to_string();
                ("DELETE", format!("{BASE}/tables/items/indexes/{name}"), None)
            }
            4 => (
                "POST",
                format!("{BASE}/tables/items/columns"),
                Some(json!({"name": "extra", "type": "int", "nullable": false, "default": 7})),
            ),
            5 => ("DELETE", format!("{BASE}/tables/items/columns/extra"), None),
            6 => (
                "POST",
                format!("{BASE}/tables"),
                Some(json!({"name": "scratch", "if_not_exists": true, "primary_key": ["k"], "columns": [
                    {"name": "k", "type": "int", "nullable": false}, {"name": "v", "type": "int"}],
                    "indexes": [{"name": "by_v", "columns": ["v"]}]})),
            ),
            _ => ("DELETE", format!("{BASE}/tables/scratch"), None),
        };
        let r = c.call_settled(method, &path, body).await;
        judge(&r, ok_codes).map_err(|e| format!("schema change {method} {path}: {e}"))?;
    }
    Ok(())
}

pub async fn run(sim: Arc<Sim>, writers: usize, steps: usize, chaos: bool) -> Result<(), String> {
    let owner = Client::signup(&sim, "racer", "racer@race.io", "password-123").await?;
    owner.create_db("race").await?;
    let r = owner
        .call_settled(
            "POST",
            &format!("{BASE}/tables"),
            Some(json!({"name": "items", "if_not_exists": true, "primary_key": ["id"], "columns": [
                {"name": "id", "type": "int", "nullable": false},
                {"name": "email", "type": "string"},
                {"name": "n", "type": "int", "nullable": false, "default": 0},
                {"name": "tag", "type": "string"}]})),
        )
        .await;
    if r.status != 201 {
        return Err(format!("create table: {} {}", r.status, r.json));
    }
    let token = owner.token.clone().unwrap_or_default();
    let chaos_h = chaos.then(|| super::spawn_chaos(&sim));
    let changer = tokio::spawn(schema_changer(Client::with_token(&sim, "schema", &token), steps / 2));
    let hs: Vec<_> = (0..writers)
        .map(|w| tokio::spawn(writer(Client::with_token(&sim, &format!("writer{w}"), &token), w as i64, steps)))
        .collect();
    let mut models = Vec::new();
    let mut err = changer.await.map_err(|e| e.to_string()).and_then(|r| r).err();
    for h in hs {
        match h.await.map_err(|e| e.to_string()).and_then(|r| r) {
            Ok(m) => models.push(m),
            Err(e) => err = err.or(Some(e)),
        }
    }
    if let Some(h) = chaos_h {
        h.abort();
    }
    if let Some(e) = err {
        return Err(e);
    }
    super::recover(&sim).await;
    let info = owner.call_retry("GET", BASE, None).await;
    let db_id = info.json["id"].as_str().ok_or("no db id")?.to_string();
    super::tables::check_all_indexes(&sim, &db_id).await?;
    let r = owner.call_retry("POST", &format!("{BASE}/tables/items/rows/query"), Some(json!({"limit": 1000}))).await;
    let rows = r.json["rows"].as_array().cloned().unwrap_or_default();
    for (w, model) in models.iter().enumerate() {
        let lo = w as i64 * 100;
        let got: BTreeMap<i64, Row> = rows
            .iter()
            .filter_map(|row| {
                let id = row["id"].as_i64()?;
                (lo..lo + 100).contains(&id).then(|| {
                    (
                        id,
                        Row { email: row["email"].as_str().map(|s| s.to_string()), n: row["n"].as_i64().unwrap_or(-1) },
                    )
                })
            })
            .collect();
        if got != *model {
            return Err(format!("writer {w}: rows differ from the model\n got {got:?}\nwant {model:?}"));
        }
    }
    Ok(())
}
