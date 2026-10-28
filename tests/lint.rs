//! Source lints that protect simulation determinism and safety. Service code
//! must route time, randomness, and task spawning through `Env`, and must
//! avoid tokio primitives with internal randomness.

use std::path::Path;

fn rust_files(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    for e in std::fs::read_dir(dir).unwrap() {
        let p = e.unwrap().path();
        if p.is_dir() {
            rust_files(&p, out);
        } else if p.extension().is_some_and(|x| x == "rs") {
            out.push(p);
        }
    }
}

const RULES: &[(&str, &[&str], &str)] = &[
    ("unsafe ", &[], "unsafe code is forbidden"),
    ("tokio::sync::watch", &[], "watch channels wake waiters in non-deterministic order"),
    ("select!", &[], "select! polls branches in non-deterministic order"),
    (
        "tokio::spawn(",
        &[
            "src/sim/",
            "src/engine/tests.rs",
            "src/engine/memory_tests.rs",
            "src/stream/tests.rs",
            "src/store/conformance.rs",
        ],
        "spawn tasks through Env so node crashes abort them",
    ),
    ("SystemTime", &["src/env.rs", "src/store/", "src/main.rs", "src/log.rs"], "read wall time from Env::now_ms"),
    (
        "std::time::Instant",
        &["src/auth.rs", "src/sim/campaign.rs", "src/sim/explore.rs"],
        "use tokio::time::Instant for virtual time",
    ),
    ("aws_lc_rs::rand", &["src/env.rs", "src/store/fs.rs", "src/sim/google.rs"], "obtain randomness from Env"),
    ("thread_rng", &[], "obtain randomness from Env"),
];

#[test]
fn determinism_and_safety_lints() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    rust_files(&root.join("src"), &mut files);
    let mut violations = Vec::new();
    for f in files {
        let rel = f.strip_prefix(root).unwrap().to_string_lossy().replace('\\', "/");
        let text = std::fs::read_to_string(&f).unwrap();
        for (n, line) in text.lines().enumerate() {
            let code = line.split("//").next().unwrap_or("");
            for (pat, allowed, why) in RULES {
                if code.contains(pat) && !allowed.iter().any(|a| rel.starts_with(a)) {
                    violations.push(format!("{rel}:{}: `{pat}`: {why}", n + 1));
                }
            }
        }
    }
    assert!(violations.is_empty(), "lint violations:\n{}", violations.join("\n"));
}

#[test]
fn forbid_unsafe_is_declared() {
    let lib = std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/lib.rs")).unwrap();
    assert!(lib.contains("#![forbid(unsafe_code)]"));
}
