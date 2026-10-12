//! Shared helpers for the examples: start real nodes on localhost.
#![allow(dead_code)]

use baserunna::cluster::HttpTransport;
use baserunna::env::Env;
use baserunna::meta::SignupPolicy;
use baserunna::node::{Node, NodeConfig};
use baserunna::store::{DynStore, MemStore};
use std::sync::Arc;

pub struct Server {
    pub url: String,
    pub node: Arc<Node>,
    task: tokio::task::JoinHandle<()>,
}

impl Server {
    /// Stop serving and release leases (graceful).
    pub async fn stop(self) {
        self.node.shutdown().await;
        self.task.abort();
    }

    /// Simulate a crash: stop without releasing anything.
    pub fn kill(self) {
        self.node.env.shutdown();
        self.task.abort();
    }
}

pub fn shared_store() -> DynStore {
    Arc::new(MemStore::with_system_clock())
}

/// Start a node on a random localhost port, sharing `store` with any
/// other nodes started from it.
pub async fn start(store: DynStore, name: &str, signup: SignupPolicy) -> Server {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let url = format!("http://{}", listener.local_addr().expect("addr"));
    let cfg = NodeConfig {
        name: name.into(),
        addr: url.clone(),
        public_url: url.clone(),
        signup,
        // Fast hashing for demos; production uses 600k iterations.
        pbkdf2_iterations: 10_000,
        heartbeat_ms: 5_000,
        members_refresh_ms: 1_000,
        ..NodeConfig::default()
    };
    let node =
        Node::start(cfg, Arc::new(Env::real()), store, Arc::new(HttpTransport::new()), None).await.expect("node start");
    let app = baserunna::api::router(node.clone());
    let task = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    Server { url, node, task }
}

/// A node running in its own runtime on its own thread, so that `kill`
/// is a faithful crash: every task and socket of the node disappears.
pub struct Isolated {
    pub url: String,
    pub id: String,
    stop: Option<std::sync::mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl Isolated {
    pub fn kill(mut self) {
        if let Some(s) = self.stop.take() {
            let _ = s.send(());
        }
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

pub async fn start_isolated(store: DynStore, name: &str) -> Isolated {
    let (tx, rx) = tokio::sync::oneshot::channel();
    let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
    let name = name.to_string();
    let thread = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build().expect("runtime");
        rt.spawn(async move {
            let s = start(store, &name, SignupPolicy::FirstUserOnly).await;
            let _ = tx.send((s.url.clone(), s.node.id.clone()));
            std::future::pending::<()>().await;
            drop(s);
        });
        let _ = stop_rx.recv();
        rt.shutdown_background();
    });
    let (url, id) = rx.await.expect("node started");
    Isolated { url, id, stop: Some(stop_tx), thread: Some(thread) }
}

pub fn step(n: u32, s: &str) {
    println!("\n[{n}] {s}");
}
