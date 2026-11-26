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
            "baserunna/src/sim/",
            "baserunna-storage/src/engine/tests.rs",
            "baserunna-storage/src/engine/memory_tests.rs",
            "baserunna-storage/src/queries_tests.rs",
            "baserunna-storage/src/stream/tests.rs",
            "baserunna-storage/src/store/conformance.rs",
            "baserunna-client/src/",
        ],
        "spawn tasks through Env so node crashes abort them",
    ),
    (
        "SystemTime",
        &[
            "baserunna-util/src/env.rs",
            "baserunna-storage/src/store/",
            "baserunna/src/main.rs",
            "baserunna-util/src/log.rs",
        ],
        "read wall time from Env::now_ms",
    ),
    (
        "std::time::Instant",
        &["baserunna/src/auth.rs", "baserunna/src/sim/campaign.rs", "baserunna/src/sim/explore.rs"],
        "use tokio::time::Instant for virtual time",
    ),
    (
        "aws_lc_rs::rand",
        &["baserunna-util/src/env.rs", "baserunna-storage/src/store/fs.rs", "baserunna/src/sim/google.rs"],
        "obtain randomness from Env",
    ),
    ("thread_rng", &[], "obtain randomness from Env"),
];

/// Workspace library crates included in the source lints.
///
/// `baserunna-client` runs inside simulations, so it follows the same rules.
const LINTED_CRATES: &[&str] =
    &["baserunna", "baserunna-api", "baserunna-client", "baserunna-storage", "baserunna-util"];

#[test]
fn determinism_and_safety_lints() {
    let crates_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let mut files = Vec::new();
    for name in LINTED_CRATES {
        rust_files(&crates_dir.join(name).join("src"), &mut files);
    }
    let mut violations = Vec::new();
    for f in files {
        let rel = f.strip_prefix(&crates_dir).unwrap().to_string_lossy().replace('\\', "/");
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
    let crates_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    for name in LINTED_CRATES {
        let text = std::fs::read_to_string(crates_dir.join(name).join("src/lib.rs")).unwrap();
        assert!(text.contains("#![forbid(unsafe_code)]"), "{name} does not forbid unsafe code");
    }
}
