//! Deterministic simulation tests (SPEC §13). Each scenario runs a few seeds
//! by default; scale up with `SIM_SEEDS=500 cargo test --release --test sim`,
//! reproduce with `SIM_SEED=<seed> cargo test --test sim <name>`, and debug
//! with `cargo run --release --example sim_tool -- trace <scenario> <seed>`.

use baserunna::sim::workloads::{chaos_mix, scenario};
use baserunna::sim::{SimConfig, run, run_seeds};

fn run_scenario(name: &str, base: u64, seeds: u64) {
    let s = scenario(name).expect("scenario exists");
    run_seeds(name, base, seeds, s.config, s.run);
}

#[test]
fn sim_kv_model_single_node() {
    run_scenario("kv_single_node", 1, 2);
}

#[test]
fn sim_kv_model_faulty_cluster() {
    run_scenario("kv_faulty_cluster", 2, 2);
}

#[test]
fn sim_bank_serializability() {
    run_scenario("bank", 3, 2);
}

#[test]
fn sim_register_linearizability() {
    run_scenario("register", 4, 2);
}

/// Writers and readers with short pauses, so requests keep arriving while a
/// commit is in flight: write transactions begin without waiting for it.
#[test]
fn sim_register_linearizability_under_contention() {
    run_scenario("register_contended", 22, 4);
}

#[test]
fn sim_tables_and_indexes() {
    run_scenario("tables", 5, 2);
}

#[test]
fn sim_blobs_and_gc() {
    run_scenario("blobs", 6, 2);
}

#[test]
fn sim_auth() {
    run_scenario("auth", 7, 2);
}

#[test]
fn sim_lifecycle() {
    run_scenario("lifecycle", 8, 2);
}

#[test]
fn sim_console() {
    run_scenario("console", 10, 2);
}

#[test]
fn sim_rate_limits() {
    run_scenario("limits", 17, 2);
}

#[test]
fn sim_chaos_mix() {
    run_scenario("chaos", 9, 1);
}

#[test]
fn sim_schema_changes_race_row_writes() {
    run_scenario("schema_race", 12, 2);
}

#[test]
fn sim_abandoned_interactive_transactions() {
    run_scenario("abandoned", 13, 2);
}

#[test]
fn sim_blob_reads_race_gc() {
    run_scenario("blob_gc_race", 14, 2);
}

#[test]
fn sim_many_db_churn() {
    run_scenario("db_churn", 15, 1);
}

#[test]
fn sim_ttl_expiry() {
    run_scenario("ttl", 18, 2);
}

#[test]
fn sim_watch_change_feeds() {
    run_scenario("watch", 19, 2);
}

#[test]
fn sim_streams() {
    run_scenario("streams", 22, 2);
}

#[test]
fn sim_organization_roles() {
    run_scenario("orgs", 20, 2);
}

#[test]
fn sim_account_self_service() {
    run_scenario("accounts", 21, 2);
}

/// Every workload again under [`SimConfig::hostile`].
#[test]
fn sim_hostile() {
    for name in [
        "kv_hostile",
        "bank_hostile",
        "register_hostile",
        "register_contended_hostile",
        "tables_hostile",
        "blobs_hostile",
        "lifecycle_hostile",
        "schema_race_hostile",
        "abandoned_hostile",
        "blob_gc_race_hostile",
        "db_churn_hostile",
        "chaos_hostile",
        "ttl_hostile",
        "watch_hostile",
        "streams_hostile",
        "orgs_hostile",
        "accounts_hostile",
    ] {
        run_scenario(name, 16, 1);
    }
}

#[test]
fn sim_hostile_is_deterministic() {
    let s = scenario("chaos_hostile").expect("scenario exists");
    let a = run(4243, (s.config)(), s.run);
    let b = run(4243, (s.config)(), s.run);
    assert!(a.result.is_ok(), "{:?}", a.result);
    assert_eq!((a.trace_hash, a.events), (b.trace_hash, b.events), "same seed, different trace");
}

#[test]
fn sim_is_deterministic() {
    let seed = baserunna::util::prop::env_u64("SIM_SEED").unwrap_or(4242);
    let a = run(seed, SimConfig::faulty(3), |sim| chaos_mix(sim, 1));
    let b = run(seed, SimConfig::faulty(3), |sim| chaos_mix(sim, 1));
    assert!(a.result.is_ok(), "{:?}", a.result);
    assert_eq!((a.trace_hash, a.events), (b.trace_hash, b.events), "same seed, different trace");
    let c = run(seed + 1, SimConfig::faulty(3), |sim| chaos_mix(sim, 1));
    assert_ne!(a.trace_hash, c.trace_hash, "different seeds should diverge");
}

#[test]
fn sim_api_tour() {
    run_scenario("api_tour", 11, 2);
}

/// Verifies that the end-of-run storage invariant check detects missing objects.
#[test]
fn storage_check_detects_missing_objects() {
    use baserunna::sim::client::Client;
    let out = run(5, SimConfig::basic(), |sim| async move {
        let c = Client::signup(&sim, "u", "u@chk.test", "password-123").await?;
        c.create_db("chk").await?;
        // Write enough out-of-band values to flush the tail to a segment.
        for i in 0..40 {
            let r = c.send("PUT", &format!("/v1/dbs/chk/kv/k{i}"), &[], vec![b'x'; 600].into()).await;
            assert_eq!(r.map(|r| r.status).ok(), Some(200));
        }
        let rep = sim.check_storage().await?;
        assert!(rep.databases >= 1 && rep.segments >= 1 && rep.blobs >= 40 && rep.objects > 40, "{rep:?}");
        let keys = sim.mem.keys();
        let r = c.get("/v1/dbs/chk/kv?prefix=k0&keys_only=true").await.map_err(|e| format!("{e:?}"))?;
        let old_id = r.json["items"][0]["blob"]["blob_id"].as_str().ok_or("no blob id")?.to_string();
        let db = keys.iter().find(|k| k.contains(&format!("/blob/{old_id}/"))).ok_or("no blob")?;
        let db = &db[..db.find("/blob/").ok_or("odd key")?];
        let seg = keys.iter().find(|k| k.starts_with(&format!("{db}/seg/"))).ok_or("no segment written")?;
        // Overwrite k0 so its old blob in the segment is shadowed and eligible
        // for collection while the new blob must remain intact.
        let r = c.send("PUT", "/v1/dbs/chk/kv/k0", &[], vec![b'y'; 700].into()).await;
        assert_eq!(r.map(|r| r.status).ok(), Some(200));
        let after = sim.mem.keys();
        for k in after.iter().filter(|k| k.contains(&format!("/blob/{old_id}/"))) {
            sim.mem.delete_sync(k);
        }
        sim.check_storage().await.map_err(|e| format!("a collected shadowed blob is not data loss: {e}"))?;
        let chunk = after
            .iter()
            .find(|k| k.contains("/blob/") && !k.ends_with("/meta") && !keys.contains(k))
            .ok_or("no new blob")?;
        sim.mem.delete_sync(chunk);
        let e = sim.check_storage().await.expect_err("missing chunk of the newest blob not detected");
        assert!(e.contains("missing"), "{e}");
        sim.mem.delete_sync(seg);
        let e = sim.check_storage().await.expect_err("missing segment not detected");
        assert!(e.contains("missing"), "{e}");
        Ok(())
    });
    let e = out.result.expect_err("the final storage check must fail too");
    assert!(e.starts_with("storage invariant"), "{e}");
}

/// Verifies that a duplicate forwarded write delivered after its target node
/// restarted is rejected rather than executed twice.
#[test]
fn forwards_to_a_restarted_node_are_refused() {
    use baserunna::cluster::{FORWARD_ID_HEADER, FORWARD_TO_HEADER};
    use baserunna::sim::client::Client;
    let out = run(7, SimConfig::basic(), |sim| async move {
        let c = Client::signup(&sim, "u", "u@fwd.test", "password-123").await?;
        c.create_db("fwd").await?;
        let token = c.token.clone().ok_or("no token")?;
        let put = |to: &str, id: &str, value: &'static str| {
            axum::http::Request::builder()
                .method("PUT")
                .uri("/v1/dbs/fwd/kv/k")
                .header("authorization", format!("Bearer {token}"))
                .header(FORWARD_ID_HEADER, id)
                .header(FORWARD_TO_HEADER, to)
                .body(axum::body::Body::from(value))
                .unwrap()
        };
        let first = sim.node(0).ok_or("node 0 down")?.node.id.clone();
        let r = sim.call_node(0, put(&first, "a", "old")).await.map_err(|e| format!("{e:?}"))?;
        assert_eq!(r.status, 200);
        sim.restart(0).await;
        // Wait for the restarted node's previous lease to expire.
        let second = sim.node(0).ok_or("node 0 down")?.node.id.clone();
        for attempt in 0.. {
            let r =
                sim.call_node(0, put(&second, &format!("b{attempt}"), "new")).await.map_err(|e| format!("{e:?}"))?;
            if r.status == 200 {
                break;
            }
            assert!(r.status == 503 && attempt < 20, "{} {}", r.status, String::from_utf8_lossy(&r.body));
            sim.sleep(500).await;
        }
        let r = sim.call_node(0, put(&first, "a", "old")).await.map_err(|e| format!("{e:?}"))?;
        assert_eq!(r.status, 503, "a copy addressed to the previous run was executed");
        let r = c.get("/v1/dbs/fwd/kv/k").await.map_err(|e| format!("{e:?}"))?;
        assert_eq!(&r.raw.body[..], b"new");
        Ok(())
    });
    out.result.unwrap();
}

/// While the leaseholder's manifest write has applied but not yet replied,
/// another node can serve the write from the store. Answers the leaseholder
/// gives after that, from a read-only transaction or a failed precondition,
/// must include it, though its write transactions begin without waiting for
/// the write (SPEC §5.5).
#[test]
fn leaseholder_answers_include_a_write_another_node_served() {
    use baserunna::sim::client::Client;
    use serde_json::json;
    let mut cfg = SimConfig::basic();
    cfg.nodes = 2;
    let out = run(23, cfg, |sim| async move {
        let owner = Client::signup(&sim, "u", "u@held.test", "password-123").await?;
        owner.create_db("t").await?;
        let token = owner.token.clone().unwrap_or_default();
        let at = |node: usize, name: &str| {
            let mut c = Client::with_token(&sim, name, &token);
            c.pin = Some(node);
            c
        };
        let net = |e| format!("{e:?}");
        // Requests that have been forwarded twice are answered where they land.
        let here = || vec![("x-baserunna-hops", "2".to_string())];
        let first = owner.send("PUT", "/v1/dbs/t/kv/k", &[], "a".into()).await.map_err(net)?;
        if first.status != 200 {
            return Err(format!("first write failed: {}", first.status));
        }
        let holder_id = owner.call_settled("GET", "/v1/dbs/t", None).await.json["stats"]["lease_holder"].clone();
        let holder = (0..2).find(|&i| sim.node(i).is_some_and(|n| holder_id == n.node.id.as_str()));
        let holder = holder.ok_or(format!("no lease holder: {holder_id}"))?;
        let other = 1 - holder;

        sim.store.hold_replies("/manifest");
        let writer = at(holder, "writer");
        let write = tokio::spawn(async move { writer.send("PUT", "/v1/dbs/t/kv/k", &here(), "b".into()).await });
        sim.sleep(500).await;
        let seen = at(other, "other").send("GET", "/v1/dbs/t/kv/k", &here(), Default::default()).await.map_err(net)?;
        if &seen.body[..] != b"b" {
            return Err(format!("the other node should serve the applied write, got {:?}", seen.body));
        }
        let version: String = seen.headers["x-baserunna-version"].to_str().map_err(|e| e.to_string())?.into();

        let reader = at(holder, "reader");
        let read = tokio::spawn(async move {
            reader.call("POST", "/v1/dbs/t/txn", Some(json!({"ops": [{"op": "get", "key": "k"}]}))).await
        });
        let conditional = at(holder, "conditional");
        let put = tokio::spawn(async move {
            let mut h = here();
            h.push(("if-match", format!("\"{version}\"")));
            conditional.send("PUT", "/v1/dbs/t/kv/k", &h, "c".into()).await
        });
        sim.sleep(500).await;
        sim.store.release_replies();

        let w = write.await.map_err(|e| e.to_string())?.map_err(net)?;
        if w.status != 200 {
            return Err(format!("held write failed: {}", w.status));
        }
        let r = read.await.map_err(|e| e.to_string())?.map_err(net)?;
        if r.json["results"][0]["value"] != "b" {
            return Err(format!("read-only transaction missed the served write: {}", r.json));
        }
        let p = put.await.map_err(|e| e.to_string())?.map_err(net)?;
        if p.status != 200 {
            return Err(format!(
                "If-Match on the served version failed: {} {}",
                p.status,
                String::from_utf8_lossy(&p.body)
            ));
        }
        Ok(())
    });
    out.result.unwrap();
}

#[test]
fn request_and_commit_logs_account_for_time() {
    use baserunna::log::{Level, REQUEST_ID_HEADER, capture};
    use baserunna::sim::client::Client;
    let cap = capture(Level::Info);
    let out = run(22, SimConfig::basic(), |sim| async move {
        let c = Client::signup(&sim, "u", "u@timing.test", "password-123").await?;
        c.create_db("t").await?;
        let rid = [(REQUEST_ID_HEADER, "put-1".to_string())];
        c.send("PUT", "/v1/dbs/t/kv/k", &rid, "v".into()).await.map_err(|e| format!("{e:?}"))?;
        Ok(())
    });
    out.result.unwrap();
    let records = cap.records();
    let put = records.iter().find(|r| r["msg"] == "request" && r["request_id"] == "put-1").expect("request record");
    assert_eq!(put["commits"], 1, "{put}");
    assert!(put["commit_ms"].is_u64(), "{put}");
    let commit = records
        .iter()
        .find(|r| {
            r["msg"] == "commit" && r["request_ids"].as_array().is_some_and(|ids| ids.iter().any(|i| i == "put-1"))
        })
        .expect("commit record naming the request");
    assert!(commit["txns"].as_u64() >= Some(1), "{commit}");
    assert!(commit["store"]["manifest.put"]["n"].as_u64() >= Some(1), "{commit}");
    assert!(commit["store"]["manifest.put"]["bytes"].as_u64() > Some(0), "{commit}");
    for field in ["queued_ms", "build_ms", "ms"] {
        assert!(commit[field].is_u64(), "{field} missing: {commit}");
    }
}

#[test]
fn request_ids_follow_forwarded_requests() {
    use baserunna::log::{Level, REQUEST_ID_HEADER, capture};
    use baserunna::sim::client::Client;
    let cap = capture(Level::Info);
    let mut cfg = SimConfig::basic();
    cfg.nodes = 2;
    let out = run(21, cfg, |sim| async move {
        let c = Client::signup(&sim, "u", "u@rid.test", "password-123").await?;
        c.create_db("rid").await?;
        let mut sent = Vec::new();
        for i in 0..12 {
            let rid = format!("client-{i}");
            let r = c
                .send("PUT", &format!("/v1/dbs/rid/kv/k{i}"), &[(REQUEST_ID_HEADER, rid.clone())], "v".into())
                .await
                .map_err(|e| format!("{e:?}"))?;
            let echoed = r.headers.get(REQUEST_ID_HEADER).and_then(|v| v.to_str().ok()).unwrap_or_default();
            if echoed != rid {
                return Err(format!("sent {rid}, echoed {echoed:?}"));
            }
            sent.push(rid);
        }
        let too_long = "x".repeat(baserunna::log::MAX_REQUEST_ID_LEN + 1);
        for bad in ["has space", "é", &too_long] {
            let r = c.send("GET", "/v1/me", &[(REQUEST_ID_HEADER, bad.to_string())], "".into()).await;
            let r = r.map_err(|e| format!("{e:?}"))?;
            let echoed = r.headers.get(REQUEST_ID_HEADER).and_then(|v| v.to_str().ok()).unwrap_or_default();
            if echoed == bad || !baserunna::log::valid_request_id(echoed) {
                return Err(format!("invalid id {bad:?} echoed as {echoed:?}"));
            }
        }
        for i in 0..6 {
            c.create_db(&format!("meta{i}")).await?;
        }
        Ok(())
    });
    out.result.unwrap();
    let records = cap.records();
    let requests: Vec<_> = records.iter().filter(|r| r["msg"] == "request").collect();
    let ids = |rid: &str| requests.iter().filter(|r| r["request_id"] == rid).collect::<Vec<_>>();
    let mut forwarded = 0;
    for i in 0..12 {
        let rid = format!("client-{i}");
        let seen = ids(&rid);
        assert!(!seen.is_empty(), "no log record for {rid}");
        for r in seen.iter().filter(|r| r["forwarded_to"].is_string()) {
            forwarded += 1;
            assert!(
                seen.iter().any(|t| t["node"] == r["forwarded_to"] && t["hops"] == 1 && t["status"] == r["status"]),
                "the forward of {rid} was not logged under its id: {seen:?}"
            );
            assert_eq!(r["route"], "/v1/dbs/{db}/kv/{*key}");
        }
    }
    assert!(forwarded > 0, "no request was forwarded: {requests:?}");
    // Forwarded metadata commands on the system database's lease holder are logged under the caller's request ID.
    let creates: Vec<_> = requests.iter().filter(|r| r["route"] == "/v1/dbs" && r["method"] == "POST").collect();
    let relayed = creates
        .iter()
        .filter(|c| {
            requests.iter().any(|r| {
                r["route"] == "/internal/v1/meta" && r["request_id"] == c["request_id"] && r["node"] != c["node"]
            })
        })
        .count();
    assert!(relayed > 0, "no forwarded metadata command carried its request's id: {creates:?}");
}
