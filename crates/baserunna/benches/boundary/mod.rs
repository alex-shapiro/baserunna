//! Isolation boundaries that each benchmark request crosses twice: once with
//! the request bytes before the router runs, and once with the response body
//! after. The far side does no work except to return the bytes, so the cost
//! is a floor for any design that runs tenant request work behind the boundary.

use std::io::{BufRead, BufReader, ErrorKind, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream as StdUnixStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use parking_lot::Mutex;
use tokio::net::UnixStream;
use wasmtime::{Config, Engine, Instance, Memory, Module, Store, StoreLimits, StoreLimitsBuilder, TypedFunc};

/// Env var that makes the bench binary run as one owner's worker process on this socket path.
pub const WORKER_SOCKET_VAR: &str = "BENCH_WORKER_SOCKET";

/// Isolation boundary that `BENCH_BOUNDARY` selects.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BoundaryMode {
    /// Requests go straight to the router.
    None,
    /// Requests cross a Unix socket to a warm worker process of their owner.
    Pipe,
    /// Requests cross into a warm WebAssembly instance of their owner.
    Wasm,
}

impl BoundaryMode {
    pub fn from_env() -> BoundaryMode {
        match std::env::var("BENCH_BOUNDARY").as_deref() {
            Err(_) | Ok("") | Ok("none") => BoundaryMode::None,
            Ok("pipe") => BoundaryMode::Pipe,
            Ok("wasm") => BoundaryMode::Wasm,
            Ok(other) => panic!("BENCH_BOUNDARY={other}: expected none, pipe, or wasm"),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            BoundaryMode::None => "none",
            BoundaryMode::Pipe => "pipe",
            BoundaryMode::Wasm => "wasm",
        }
    }
}

/// Serves one owner's echo worker on `socket` until the parent closes stdin.
///
/// Each line on stdin asks for the worker's CPU seconds, which it writes to stdout.
pub fn run_worker(socket: &str) -> ! {
    let _ = std::fs::remove_file(socket);
    let listener = UnixListener::bind(socket).expect("worker binds its socket");
    std::thread::spawn(move || {
        for conn in listener.incoming() {
            let conn = conn.expect("worker accepts");
            std::thread::spawn(move || serve_echo(conn));
        }
    });
    let mut out = std::io::stdout().lock();
    writeln!(out, "ready").and_then(|()| out.flush()).expect("worker reports ready");
    for line in std::io::stdin().lock().lines() {
        if line.is_err() {
            break;
        }
        writeln!(out, "{}", crate::cpu_secs()).and_then(|()| out.flush()).expect("worker reports cpu");
    }
    std::process::exit(0)
}

/// Returns each length-prefixed frame on `conn` unchanged.
fn serve_echo(conn: StdUnixStream) {
    let mut reader = BufReader::with_capacity(64 << 10, conn.try_clone().expect("clone worker socket"));
    let mut writer = conn;
    let mut frame = Vec::new();
    loop {
        let mut header = [0u8; 4];
        if reader.read_exact(&mut header).is_err() {
            return;
        }
        let len = u32::from_le_bytes(header) as usize;
        frame.clear();
        frame.extend_from_slice(&header);
        frame.resize(4 + len, 0);
        if reader.read_exact(&mut frame[4..]).is_err() || writer.write_all(&frame).is_err() {
            return;
        }
    }
}

/// Warm worker processes, one per owner, that run the bench binary itself.
pub struct OwnerWorkers {
    workers: Vec<OwnerWorker>,
}

struct OwnerWorker {
    child: Child,
    control: Mutex<(ChildStdin, BufReader<ChildStdout>)>,
    socket: PathBuf,
}

impl OwnerWorkers {
    pub fn start(owners: usize) -> OwnerWorkers {
        let exe = std::env::current_exe().expect("bench binary path");
        let workers = (0..owners)
            .map(|owner| {
                let socket = std::env::temp_dir().join(format!("baserunna-bench-{}-{owner}.sock", std::process::id()));
                let mut child = Command::new(&exe)
                    .env(WORKER_SOCKET_VAR, &socket)
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .spawn()
                    .expect("spawn worker");
                let control_in = child.stdin.take().expect("worker stdin");
                let mut control_out = BufReader::new(child.stdout.take().expect("worker stdout"));
                let mut line = String::new();
                control_out.read_line(&mut line).expect("worker ready line");
                assert_eq!(line.trim(), "ready", "worker {owner} did not start");
                OwnerWorker { child, control: Mutex::new((control_in, control_out)), socket }
            })
            .collect();
        OwnerWorkers { workers }
    }

    /// CPU seconds that all workers have used.
    pub fn cpu_secs(&self) -> f64 {
        self.workers
            .iter()
            .map(|w| {
                let mut control = w.control.lock();
                let (control_in, control_out) = &mut *control;
                writeln!(control_in).and_then(|()| control_in.flush()).expect("ask worker cpu");
                let mut line = String::new();
                control_out.read_line(&mut line).expect("worker cpu line");
                line.trim().parse::<f64>().expect("worker cpu value")
            })
            .sum()
    }

    async fn connect(&self, owner: usize) -> PipeCrossing {
        let stream = UnixStream::connect(&self.workers[owner].socket).await.expect("connect worker");
        PipeCrossing { stream, frame: Vec::new(), reply: Vec::new() }
    }
}

impl Drop for OwnerWorkers {
    fn drop(&mut self) {
        for w in &mut self.workers {
            let _ = w.child.kill();
            let _ = w.child.wait();
            let _ = std::fs::remove_file(&w.socket);
        }
    }
}

/// One client task's warm connection to one owner's worker.
pub struct PipeCrossing {
    stream: UnixStream,
    frame: Vec<u8>,
    reply: Vec<u8>,
}

impl PipeCrossing {
    async fn round_trip(&mut self, pieces: &[&[u8]]) {
        let len: usize = pieces.iter().map(|p| p.len()).sum();
        self.frame.clear();
        self.frame.extend_from_slice(&(len as u32).to_le_bytes());
        for piece in pieces {
            self.frame.extend_from_slice(piece);
        }
        let mut sent = 0;
        while sent < self.frame.len() {
            match self.stream.try_write(&self.frame[sent..]) {
                Ok(n) => sent += n,
                Err(e) if e.kind() == ErrorKind::WouldBlock => self.stream.writable().await.expect("worker writable"),
                Err(e) => panic!("write to worker: {e}"),
            }
        }
        self.reply.resize(self.frame.len(), 0);
        let mut got = 0;
        while got < self.reply.len() {
            match self.stream.try_read(&mut self.reply[got..]) {
                Ok(0) => panic!("worker closed its socket"),
                Ok(n) => got += n,
                Err(e) if e.kind() == ErrorKind::WouldBlock => self.stream.readable().await.expect("worker readable"),
                Err(e) => panic!("read from worker: {e}"),
            }
        }
        assert!(self.reply == self.frame, "worker echo differs");
    }
}

/// Guest module that exports its memory and `echo(ptr, len) -> out_ptr`.
///
/// `ECHO_WASM` is the binary encoding of this text, since the `wat` feature of
/// wasmtime is off in this crate.
#[allow(dead_code)]
const ECHO_WAT: &str = r#"
(module
  (memory (export "memory") 1)
  (func (export "echo") (param $ptr i32) (param $len i32) (result i32)
    (memory.copy
      (i32.add (local.get $ptr) (local.get $len))
      (local.get $ptr)
      (local.get $len))
    (i32.add (local.get $ptr) (local.get $len))))
"#;

#[rustfmt::skip]
const ECHO_WASM: &[u8] = &[
    0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00,
    // type section: (i32, i32) -> i32
    0x01, 0x07, 0x01, 0x60, 0x02, 0x7f, 0x7f, 0x01, 0x7f,
    // function section: one function of type 0
    0x03, 0x02, 0x01, 0x00,
    // memory section: one memory of 1 page
    0x05, 0x03, 0x01, 0x00, 0x01,
    // export section: "memory" and "echo"
    0x07, 0x11, 0x02,
    0x06, b'm', b'e', b'm', b'o', b'r', b'y', 0x02, 0x00,
    0x04, b'e', b'c', b'h', b'o', 0x00, 0x00,
    // code section
    0x0a, 0x16, 0x01, 0x14, 0x00,
    0x20, 0x00, 0x20, 0x01, 0x6a,
    0x20, 0x00,
    0x20, 0x01,
    0xfc, 0x0a, 0x00, 0x00,
    0x20, 0x00, 0x20, 0x01, 0x6a,
    0x0b,
];

/// Linear memory cap of one guest instance.
const GUEST_MEMORY_BOUND: usize = 16 << 20;
/// Fuel that one guest call may burn before the runtime stops it.
const GUEST_CALL_FUEL: u64 = 1 << 20;
const WASM_PAGE: usize = 64 << 10;

/// Compiled echo module with fuel metering on.
pub struct EchoModule {
    engine: Engine,
    module: Module,
}

impl EchoModule {
    pub fn compile() -> EchoModule {
        let mut config = Config::new();
        config.consume_fuel(true);
        let engine = Engine::new(&config).expect("wasm engine");
        let module = Module::new(&engine, ECHO_WASM).expect("compile echo module");
        EchoModule { engine, module }
    }

    fn instantiate(&self) -> WasmCrossing {
        let limits = StoreLimitsBuilder::new().memory_size(GUEST_MEMORY_BOUND).instances(1).build();
        let mut store = Store::new(&self.engine, limits);
        store.limiter(|limits| limits);
        let instance = Instance::new(&mut store, &self.module, &[]).expect("instantiate echo module");
        let memory = instance.get_memory(&mut store, "memory").expect("echo memory");
        let echo = instance.get_typed_func::<(u32, u32), u32>(&mut store, "echo").expect("echo export");
        WasmCrossing { store, memory, echo, reply: Vec::new() }
    }
}

/// One client task's warm instance of one owner's echo module.
pub struct WasmCrossing {
    store: Store<StoreLimits>,
    memory: Memory,
    echo: TypedFunc<(u32, u32), u32>,
    reply: Vec<u8>,
}

impl WasmCrossing {
    fn round_trip(&mut self, pieces: &[&[u8]]) {
        let len: usize = pieces.iter().map(|p| p.len()).sum();
        let have = self.memory.data_size(&self.store);
        if 2 * len > have {
            let pages = (2 * len).div_ceil(WASM_PAGE) - have / WASM_PAGE;
            self.memory.grow(&mut self.store, pages as u64).expect("grow guest memory");
        }
        let mut at = 0;
        for piece in pieces {
            self.memory.write(&mut self.store, at, piece).expect("write guest memory");
            at += piece.len();
        }
        self.store.set_fuel(GUEST_CALL_FUEL).expect("set guest fuel");
        let out = self.echo.call(&mut self.store, (0, len as u32)).expect("guest echo") as usize;
        self.reply.clear();
        self.reply.extend_from_slice(&self.memory.data(&self.store)[out..out + len]);
        assert!(self.reply[..] == self.memory.data(&self.store)[..len], "guest echo differs");
    }
}

/// Shared state that each crossing starts from.
pub enum BoundaryHost {
    None,
    Pipe(OwnerWorkers),
    Wasm(EchoModule),
}

impl BoundaryHost {
    pub fn start(mode: BoundaryMode, owners: usize) -> BoundaryHost {
        match mode {
            BoundaryMode::None => BoundaryHost::None,
            BoundaryMode::Pipe => BoundaryHost::Pipe(OwnerWorkers::start(owners)),
            BoundaryMode::Wasm => BoundaryHost::Wasm(EchoModule::compile()),
        }
    }

    /// CPU seconds spent outside this process on the far side of the boundary.
    pub fn far_cpu_secs(&self) -> f64 {
        match self {
            BoundaryHost::Pipe(workers) => workers.cpu_secs(),
            BoundaryHost::None | BoundaryHost::Wasm(_) => 0.0,
        }
    }

    /// Opens a warm crossing to `owner`'s far side, or none when the mode has no boundary.
    async fn open_crossing(&self, owner: usize) -> Option<OwnerCrossing> {
        match self {
            BoundaryHost::None => None,
            BoundaryHost::Pipe(workers) => Some(OwnerCrossing::Pipe(workers.connect(owner).await)),
            BoundaryHost::Wasm(module) => Some(OwnerCrossing::Wasm(module.instantiate())),
        }
    }

    /// Opens one client task's warm crossing to each of `owners` owners.
    pub async fn client_crossings(&self, owners: usize) -> ClientCrossings {
        let mut crossings = Vec::with_capacity(owners);
        for owner in 0..owners {
            crossings.extend(self.open_crossing(owner).await);
        }
        ClientCrossings(crossings)
    }
}

/// Warm crossing to one owner's far side.
pub enum OwnerCrossing {
    Pipe(PipeCrossing),
    Wasm(WasmCrossing),
}

impl OwnerCrossing {
    async fn round_trip(&mut self, pieces: &[&[u8]]) {
        match self {
            OwnerCrossing::Pipe(conn) => conn.round_trip(pieces).await,
            OwnerCrossing::Wasm(guest) => guest.round_trip(pieces),
        }
    }
}

/// One client task's warm crossings, indexed by owner, and empty without a boundary.
pub struct ClientCrossings(Vec<OwnerCrossing>);

impl ClientCrossings {
    /// Sends the concatenation of `pieces` to `owner`'s far side and reads back the echo.
    pub async fn round_trip(&mut self, owner: usize, pieces: &[&[u8]]) {
        if let Some(crossing) = self.0.get_mut(owner) {
            crossing.round_trip(pieces).await;
        }
    }
}

/// Idle warm crossings per owner that server request handlers share.
pub struct PooledCrossings {
    host: Arc<BoundaryHost>,
    idle: Vec<Mutex<Vec<OwnerCrossing>>>,
}

impl PooledCrossings {
    pub fn new(host: Arc<BoundaryHost>, owners: usize) -> PooledCrossings {
        PooledCrossings { host, idle: (0..owners).map(|_| Mutex::default()).collect() }
    }

    /// Sends the concatenation of `pieces` to `owner`'s far side on an idle
    /// crossing, or on a new one when all are busy, and reads back the echo.
    pub async fn round_trip(&self, owner: usize, pieces: &[&[u8]]) {
        let idle = self.idle[owner].lock().pop();
        let Some(mut crossing) = (match idle {
            Some(crossing) => Some(crossing),
            None => self.host.open_crossing(owner).await,
        }) else {
            return;
        };
        crossing.round_trip(pieces).await;
        self.idle[owner].lock().push(crossing);
    }
}
