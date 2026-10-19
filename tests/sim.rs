//! Deterministic simulation tests (SPEC §13). Each scenario runs a few seeds
//! by default; scale up with `SIM_SEEDS=500 cargo test --release --test sim`,
//! reproduce with `SIM_SEED=<seed> cargo test --test sim <name>`, and debug
//! with `cargo run --release --example sim_tool -- trace <scenario> <seed>`.

use baserunna::probe::{self, Probe};
use baserunna::sim::workloads::scenario;
use baserunna::sim::{SimConfig, run, run_seeds};

/// One test per scenario, each running a few seeds of its swarm.
macro_rules! scenario_tests {
    ($($test:ident: $name:literal, $base:literal, $seeds:literal;)*) => {
        $(
            #[test]
            fn $test() {
                run_seeds(&scenario($name).expect("scenario exists"), $base, $seeds);
            }
        )*

        #[test]
        fn every_scenario_has_a_test() {
            let tested = [$($name),*];
            for s in baserunna::sim::workloads::scenarios() {
                assert!(tested.contains(&s.name), "scenario {} has no test", s.name);
            }
        }
    };
}

scenario_tests! {
    sim_kv_model: "kv_model", 1, 4;
    sim_history: "history", 23, 4;
    sim_bank: "bank", 3, 4;
    sim_register: "register", 4, 4;
    sim_tables: "tables", 5, 3;
    sim_blobs: "blobs", 6, 3;
    sim_auth: "auth", 7, 2;
    sim_lifecycle: "lifecycle", 8, 3;
    sim_console: "console", 10, 2;
    sim_chaos: "chaos", 9, 2;
    sim_limits: "limits", 17, 2;
    sim_api_tour: "api_tour", 11, 2;
    sim_schema_race: "schema_race", 12, 3;
    sim_abandoned: "abandoned", 13, 3;
    sim_blob_gc_race: "blob_gc_race", 14, 2;
    sim_db_churn: "db_churn", 15, 2;
    sim_ttl: "ttl", 18, 3;
    sim_watch: "watch", 19, 3;
    sim_streams: "streams", 22, 3;
    sim_orgs: "orgs", 20, 2;
    sim_accounts: "accounts", 21, 2;
}

#[test]
fn sim_is_deterministic() {
    let s = scenario("chaos").expect("scenario exists");
    let seed = baserunna::util::prop::env_u64("SIM_SEED").unwrap_or(4242);
    let a = run(seed, s.config_for(seed), s.run);
    let b = run(seed, s.config_for(seed), s.run);
    assert!(a.result.is_ok(), "{:?}", a.result);
    assert_eq!((a.trace_hash, a.events), (b.trace_hash, b.events), "same seed, different trace");
    let c = run(seed + 1, s.config_for(seed + 1), s.run);
    assert_ne!(a.trace_hash, c.trace_hash, "different seeds should diverge");
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

/// If a second node serves a write before the leaseholder receives its
/// manifest `PUT` reply, subsequent transactions on the leaseholder observe
/// that write (SPEC §5.5).
#[test]
fn optimistic_txn_revalidation() {
    use baserunna::sim::client::Client;
    use serde_json::json;
    let mut cfg = SimConfig::basic();
    cfg.nodes = 2;
    let out = run(23, cfg, |sim| async move {
        let owner = Client::signup(&sim, "u", "u@held.test", "password-123").await?;
        owner.create_db("t").await?;
        let token = owner.token.clone().unwrap_or_default();
        let client_on = |node: usize, name: &str| {
            let mut c = Client::with_token(&sim, name, &token);
            c.pin = Some(node);
            c
        };
        // Setting x-baserunna-hops to 2 prevents the receiving node from forwarding.
        let no_forward = || vec![("x-baserunna-hops", "2".to_string())];
        let first = owner.send("PUT", "/v1/dbs/t/kv/k", &[], "a".into()).await.unwrap();
        assert_eq!(first.status, 200);
        let holder_id = owner.call_settled("GET", "/v1/dbs/t", None).await.json["stats"]["lease_holder"].clone();
        let holder = (0..2).find(|&i| sim.node(i).is_some_and(|n| holder_id == n.node.id.as_str())).unwrap();
        let other = 1 - holder;

        sim.store.hold_replies("/manifest");
        let writer = client_on(holder, "writer");
        let write_task =
            tokio::spawn(async move { writer.send("PUT", "/v1/dbs/t/kv/k", &no_forward(), "b".into()).await });
        sim.sleep(500).await;
        let seen =
            client_on(other, "other").send("GET", "/v1/dbs/t/kv/k", &no_forward(), Default::default()).await.unwrap();
        assert_eq!(&seen.body[..], b"b");
        let version = seen.headers["x-baserunna-version"].to_str().unwrap().to_string();

        let reader = client_on(holder, "reader");
        let read_task = tokio::spawn(async move {
            reader.call("POST", "/v1/dbs/t/txn", Some(json!({"ops": [{"op": "get", "key": "k"}]}))).await
        });
        let cas_client = client_on(holder, "cas");
        let cas_task = tokio::spawn(async move {
            let mut headers = no_forward();
            headers.push(("if-match", format!("\"{version}\"")));
            cas_client.send("PUT", "/v1/dbs/t/kv/k", &headers, "c".into()).await
        });
        sim.sleep(500).await;
        sim.store.release_replies();

        let write_res = write_task.await.unwrap().unwrap();
        assert_eq!(write_res.status, 200);
        let read_res = read_task.await.unwrap().unwrap();
        assert_eq!(read_res.json["results"][0]["value"], "b", "{}", read_res.json);
        let cas_res = cas_task.await.unwrap().unwrap();
        assert_eq!(cas_res.status, 200, "{}", String::from_utf8_lossy(&cas_res.body));
        Ok(())
    });
    out.result.unwrap();
}

/// If a second node serves a stream append before the leaseholder receives its
/// manifest `PUT` reply, stream operations on the leaseholder that read outside
/// the key-value read set still observe that append (SPEC §5.5).
#[test]
fn optimistic_stream_revalidation() {
    use baserunna::sim::client::Client;
    use serde_json::json;
    let mut cfg = SimConfig::basic();
    cfg.nodes = 2;
    let out = run(25, cfg, |sim| async move {
        let owner = Client::signup(&sim, "u", "u@held-stream.test", "password-123").await?;
        owner.create_db("t").await?;
        let token = owner.token.clone().unwrap_or_default();
        let client_on = |node: usize, name: &str| {
            let mut c = Client::with_token(&sim, name, &token);
            c.pin = Some(node);
            c
        };
        let records = json!({"records": [{"json": {"n": 1}}]});
        let created = owner.call_settled("POST", "/v1/dbs/t/streams", Some(json!({"name": "s"}))).await;
        assert!(created.status == 200 || created.status == 201, "{} {}", created.status, created.json);
        let first = owner.call_settled("POST", "/v1/dbs/t/streams/s/records", Some(records.clone())).await;
        assert_eq!(first.status, 200, "{}", first.json);
        let holder_id = owner.call_settled("GET", "/v1/dbs/t", None).await.json["stats"]["lease_holder"].clone();
        let holder = (0..2).find(|&i| sim.node(i).is_some_and(|n| holder_id == n.node.id.as_str())).unwrap();
        let other = 1 - holder;

        sim.store.hold_replies("/manifest");
        let appender = client_on(holder, "appender");
        let append_task =
            tokio::spawn(async move { appender.call("POST", "/v1/dbs/t/streams/s/records", Some(records)).await });
        sim.sleep(500).await;
        // Setting x-baserunna-hops to 2 prevents the receiving node from forwarding.
        let no_forward = [("x-baserunna-hops", "2".to_string())];
        let seen = client_on(other, "other")
            .send("GET", "/v1/dbs/t/streams/s", &no_forward, Default::default())
            .await
            .unwrap();
        let seen: serde_json::Value = serde_json::from_slice(&seen.body).unwrap();
        assert_eq!(seen["next_seq"], 2, "{seen}");

        let consumer = client_on(holder, "consumer");
        let consumer_task = tokio::spawn(async move {
            consumer.call("PUT", "/v1/dbs/t/streams/s/consumers/c", Some(json!({"seq": 2}))).await
        });
        let patcher = client_on(holder, "patcher");
        let patch_task = tokio::spawn(async move {
            patcher.call("PATCH", "/v1/dbs/t/streams/s", Some(json!({"retention_ms": 7_200_000}))).await
        });
        sim.sleep(500).await;
        sim.store.release_replies();

        let append_res = append_task.await.unwrap().unwrap();
        assert_eq!(append_res.status, 200, "{}", append_res.json);
        let consumer_res = consumer_task.await.unwrap().unwrap();
        assert_eq!(consumer_res.status, 200, "{}", consumer_res.json);
        let patch_res = patch_task.await.unwrap().unwrap();
        assert_eq!(patch_res.status, 200, "{}", patch_res.json);
        assert_eq!(patch_res.json["next_seq"], 2, "{}", patch_res.json);
        Ok(())
    });
    out.result.unwrap();
}

/// Leases expired according to the object store clock can be taken over
/// immediately, whereas recently written leases still require waiting (SPEC §5.7).
#[test]
fn store_clock_lease_expiry() {
    use baserunna::sim::client::Client;
    let mut cfg = SimConfig::basic();
    cfg.nodes = 2;
    let out = run(24, cfg, |sim| async move {
        let owner = Client::signup(&sim, "u", "u@expired.test", "password-123").await?;
        let token = owner.token.clone().unwrap_or_default();
        let client_on = |node: usize, name: &str| {
            let mut c = Client::with_token(&sim, name, &token);
            c.pin = Some(node);
            c
        };
        // Setting x-baserunna-hops to 2 prevents the receiving node from forwarding.
        let no_forward = [("x-baserunna-hops", "2".to_string())];
        let put = |c: Client, db: &'static str| {
            let no_forward = no_forward.clone();
            async move { c.send("PUT", &format!("/v1/dbs/{db}/kv/k"), &no_forward, "v".into()).await.unwrap().status }
        };
        for db in ["old", "recent"] {
            owner.create_db(db).await?;
        }
        let expired_before = probe::count(Probe::ExpiredLeaseByStoreClock);
        assert_eq!(put(client_on(0, "a"), "old").await, 200);
        let lease_ms = sim.cfg.engine.lease_ms;
        sim.sleep(3 * lease_ms + 2_000).await;
        assert_eq!(put(client_on(0, "b"), "recent").await, 200);
        sim.crash(0);
        assert_eq!(put(client_on(1, "c"), "recent").await, 503);
        assert_eq!(put(client_on(1, "d"), "old").await, 200);
        assert!(probe::count(Probe::ExpiredLeaseByStoreClock) > expired_before);
        Ok(())
    });
    out.result.unwrap();
}

#[test]
fn request_and_commit_log_timing() {
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
