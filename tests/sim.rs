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

/// Every workload again under [`SimConfig::hostile`].
#[test]
fn sim_hostile() {
    for name in [
        "kv_hostile",
        "bank_hostile",
        "register_hostile",
        "tables_hostile",
        "blobs_hostile",
        "lifecycle_hostile",
        "schema_race_hostile",
        "abandoned_hostile",
        "blob_gc_race_hostile",
        "db_churn_hostile",
        "chaos_hostile",
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

/// The end-of-run storage invariants must notice lost data.
#[test]
fn storage_check_detects_missing_objects() {
    use baserunna::sim::client::Client;
    let out = run(5, SimConfig::basic(), |sim| async move {
        let c = Client::signup(&sim, "u", "u@chk.test", "password-123").await?;
        c.create_db("chk").await?;
        // Enough out-of-band values to flush the tail to a segment.
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
        // Overwrite k0, whose old version is in a segment: the old blob is
        // now shadowed and may be collected, but the new one must exist.
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

/// A copy of a forwarded write delivered after its target restarted is
/// refused rather than run a second time.
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
        // The restarted node first waits out its previous run's lease.
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
