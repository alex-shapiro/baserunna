//! Structured JSON and text logging with task-local request context.

use serde_json::Value as Json;
use std::cell::RefCell;
use std::future::Future;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError};
use std::sync::{Arc, OnceLock};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Level {
    Error = 1,
    Warn = 2,
    Info = 3,
    Debug = 4,
}

impl Level {
    pub fn name(self) -> &'static str {
        match self {
            Level::Error => "error",
            Level::Warn => "warn",
            Level::Info => "info",
            Level::Debug => "debug",
        }
    }

    /// Parses a log level name, returning `Ok(None)` for `"off"`.
    pub fn parse(s: &str) -> Result<Option<Level>, String> {
        match s.to_ascii_lowercase().as_str() {
            "error" => Ok(Some(Level::Error)),
            "warn" => Ok(Some(Level::Warn)),
            "info" => Ok(Some(Level::Info)),
            "debug" => Ok(Some(Level::Debug)),
            "off" => Ok(None),
            o => Err(format!("unknown log level {o:?}")),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
    Json,
    Text,
}

static LEVEL: AtomicU8 = AtomicU8::new(Level::Info as u8);
static TEXT: AtomicBool = AtomicBool::new(false);
static PROCESS_NODE: OnceLock<String> = OnceLock::new();
static WRITER: OnceLock<Writer> = OnceLock::new();

/// Maximum queued log lines before new lines are dropped.
const QUEUE_CAPACITY: usize = 64 * 1024;

enum LogMsg {
    Line(String),
    Flush(SyncSender<()>),
}

struct Writer {
    tx: SyncSender<LogMsg>,
    dropped: AtomicU64,
}

/// Spawns a background thread that buffers and writes log lines to stderr,
/// dropping new lines if the queue is full. Call [`flush`] before exiting.
pub fn start_background_writer() {
    WRITER.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::sync_channel(QUEUE_CAPACITY);
        std::thread::Builder::new()
            .name("log-writer".into())
            .spawn(move || write_lines(rx))
            .expect("failed to spawn log writer thread");
        Writer { tx, dropped: AtomicU64::new(0) }
    });
}

fn write_lines(rx: Receiver<LogMsg>) {
    let mut out = std::io::BufWriter::new(std::io::stderr());
    while let Ok(first) = rx.recv() {
        let mut acks = Vec::new();
        for msg in std::iter::once(first).chain(rx.try_iter()) {
            match msg {
                LogMsg::Line(line) => {
                    let _ = writeln!(out, "{line}");
                }
                LogMsg::Flush(ack) => acks.push(ack),
            }
        }
        if let Some(n) = WRITER.get().map(|w| w.dropped.swap(0, Ordering::Relaxed)).filter(|n| *n > 0) {
            let node = PROCESS_NODE.get().map(|n| n.as_str());
            let msg = format!("dropped {n} log lines (log queue full)");
            let line = match TEXT.load(Ordering::Relaxed) {
                false => json_line(now_ms(), Level::Warn, node, None, &msg, &[]),
                true => text_line(Level::Warn, node, None, &msg, &[]),
            };
            let _ = writeln!(out, "{line}");
        }
        let _ = out.flush();
        for ack in acks {
            let _ = ack.send(());
        }
    }
}

/// Blocks until all queued log lines have been flushed to stderr.
pub fn flush() {
    if let Some(w) = WRITER.get() {
        let (ack, done) = std::sync::mpsc::sync_channel(1);
        if w.tx.send(LogMsg::Flush(ack)).is_ok() {
            let _ = done.recv();
        }
    }
}

fn write_line(line: String) {
    let Some(w) = WRITER.get() else {
        eprintln!("{line}");
        return;
    };
    match w.tx.try_send(LogMsg::Line(line)) {
        Ok(()) => {}
        Err(TrySendError::Full(_)) => {
            w.dropped.fetch_add(1, Ordering::Relaxed);
        }
        Err(TrySendError::Disconnected(LogMsg::Line(line))) => eprintln!("{line}"),
        Err(TrySendError::Disconnected(_)) => {}
    }
}

pub fn set_level(l: Option<Level>) {
    LEVEL.store(l.map_or(0, |l| l as u8), Ordering::Relaxed);
}

pub fn level() -> Option<Level> {
    match LEVEL.load(Ordering::Relaxed) {
        1 => Some(Level::Error),
        2 => Some(Level::Warn),
        3 => Some(Level::Info),
        4 => Some(Level::Debug),
        _ => None,
    }
}

pub fn set_format(f: Format) {
    TEXT.store(f == Format::Text, Ordering::Relaxed);
}

/// Sets the default node identifier included in log records outside a request scope.
pub fn set_process_node(id: &str) {
    let _ = PROCESS_NODE.set(id.to_string());
}

/// Configures log level and format from `BASERUNNA_LOG` and `BASERUNNA_LOG_FORMAT`.
pub fn init_from_env() -> Result<(), String> {
    if let Ok(v) = std::env::var("BASERUNNA_LOG") {
        set_level(Level::parse(&v)?);
    }
    match std::env::var("BASERUNNA_LOG_FORMAT").as_deref() {
        Err(_) | Ok("json") => set_format(Format::Json),
        Ok("text") => set_format(Format::Text),
        Ok(o) => return Err(format!("unknown log format {o:?} (json or text)")),
    }
    Ok(())
}

/// Task-local logging context for an active request.
#[derive(Clone, Debug)]
pub struct Context {
    pub node: Arc<str>,
    pub request_id: Arc<str>,
}

tokio::task_local! {
    static CONTEXT: Context;
}

/// Runs `f` with `ctx` attached to all log records emitted on the task.
pub async fn scoped<F: Future>(ctx: Context, f: F) -> F::Output {
    CONTEXT.scope(ctx, f).await
}

/// Returns the current task's request ID, if any.
pub fn request_id() -> Option<Arc<str>> {
    CONTEXT.try_with(|c| c.request_id.clone()).ok()
}

/// HTTP header name for request correlation IDs.
pub const REQUEST_ID_HEADER: &str = "x-request-id";

pub const MAX_REQUEST_ID_LEN: usize = 64;

/// Returns whether `s` is a valid client-supplied request ID.
pub fn valid_request_id(s: &str) -> bool {
    (1..=MAX_REQUEST_ID_LEN).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_alphanumeric() || b"._:-".contains(&b))
}

thread_local! {
    static CAPTURE: RefCell<Option<(Level, Vec<Json>)>> = const { RefCell::new(None) };
}

/// Guard that captures log records emitted on the current thread at or above `level`.
pub struct Capture(());

pub fn capture(level: Level) -> Capture {
    CAPTURE.with(|c| *c.borrow_mut() = Some((level, Vec::new())));
    Capture(())
}

impl Capture {
    /// Returns captured log records.
    pub fn records(&self) -> Vec<Json> {
        CAPTURE.with(|c| c.borrow().as_ref().map(|(_, v)| v.clone()).unwrap_or_default())
    }
}

impl Drop for Capture {
    fn drop(&mut self) {
        CAPTURE.with(|c| *c.borrow_mut() = None);
    }
}

fn captured(l: Level) -> bool {
    CAPTURE.with(|c| c.borrow().as_ref().is_some_and(|(max, _)| l <= *max))
}

pub fn enabled(l: Level) -> bool {
    l as u8 <= LEVEL.load(Ordering::Relaxed) || captured(l)
}

/// Emits a structured log record.
pub fn event(l: Level, msg: &str, fields: &[(&str, Json)]) {
    if !enabled(l) {
        return;
    }
    let ctx = CONTEXT.try_with(|c| c.clone()).ok();
    let node = ctx.as_ref().map(|c| &*c.node).or_else(|| PROCESS_NODE.get().map(|n| n.as_str()));
    let rid = ctx.as_ref().map(|c| &*c.request_id);
    if captured(l) {
        let mut m = serde_json::Map::new();
        m.insert("level".into(), l.name().into());
        if let Some(n) = node {
            m.insert("node".into(), n.into());
        }
        if let Some(r) = rid {
            m.insert("request_id".into(), r.into());
        }
        m.insert("msg".into(), msg.into());
        for (k, v) in fields {
            m.insert((*k).into(), v.clone());
        }
        CAPTURE.with(|c| {
            if let Some((_, v)) = c.borrow_mut().as_mut() {
                v.push(Json::Object(m));
            }
        });
    }
    if l as u8 <= LEVEL.load(Ordering::Relaxed) {
        let line = match TEXT.load(Ordering::Relaxed) {
            false => json_line(now_ms(), l, node, rid, msg, fields),
            true => text_line(l, node, rid, msg, fields),
        };
        write_line(line);
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

fn push_str(out: &mut String, s: &str) {
    out.push_str(&serde_json::to_string(s).unwrap_or_else(|_| "\"\"".into()));
}

/// Formats a single-line JSON log record.
pub fn json_line(
    ts_ms: u64,
    l: Level,
    node: Option<&str>,
    rid: Option<&str>,
    msg: &str,
    fields: &[(&str, Json)],
) -> String {
    let mut out = String::with_capacity(128);
    out.push_str("{\"ts\":");
    push_str(&mut out, &rfc3339(ts_ms));
    out.push_str(",\"level\":");
    push_str(&mut out, l.name());
    for (k, v) in [("node", node), ("request_id", rid), ("msg", Some(msg))] {
        if let Some(v) = v {
            out.push(',');
            push_str(&mut out, k);
            out.push(':');
            push_str(&mut out, v);
        }
    }
    for (k, v) in fields {
        out.push(',');
        push_str(&mut out, k);
        out.push(':');
        out.push_str(&v.to_string());
    }
    out.push('}');
    out
}

fn text_line(l: Level, node: Option<&str>, rid: Option<&str>, msg: &str, fields: &[(&str, Json)]) -> String {
    let mut out = format!("[{}]", l.name());
    if let Some(n) = node {
        out.push(' ');
        out.push_str(n);
    }
    if let Some(r) = rid {
        out.push_str(" req=");
        out.push_str(r);
    }
    out.push(' ');
    out.push_str(msg);
    for (k, v) in fields {
        out.push_str(&format!(" {k}={v}"));
    }
    out
}

/// Formats Unix epoch milliseconds as an RFC 3339 UTC timestamp.
pub fn rfc3339(ms: u64) -> String {
    let days = (ms / 86_400_000) as i64;
    let rem = ms % 86_400_000;
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{:03}Z",
        rem / 3_600_000,
        rem / 60_000 % 60,
        rem / 1000 % 60,
        rem % 1000
    )
}

#[macro_export]
macro_rules! log_at {
    ($lvl:expr, $($arg:tt)*) => {
        if $crate::log::enabled($lvl) {
            $crate::log::event($lvl, &format!($($arg)*), &[]);
        }
    };
}

#[macro_export]
macro_rules! error {
    ($($arg:tt)*) => { $crate::log_at!($crate::log::Level::Error, $($arg)*) };
}

#[macro_export]
macro_rules! warn {
    ($($arg:tt)*) => { $crate::log_at!($crate::log::Level::Warn, $($arg)*) };
}

#[macro_export]
macro_rules! info {
    ($($arg:tt)*) => { $crate::log_at!($crate::log::Level::Info, $($arg)*) };
}

#[macro_export]
macro_rules! debug {
    ($($arg:tt)*) => { $crate::log_at!($crate::log::Level::Debug, $($arg)*) };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::prop;
    use serde_json::json;

    #[test]
    fn dates() {
        assert_eq!(rfc3339(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(rfc3339(951_782_400_000), "2000-02-29T00:00:00.000Z");
        assert_eq!(rfc3339(1_791_979_200_123), "2026-10-14T12:00:00.123Z");
        assert_eq!(rfc3339(4_107_542_399_999), "2100-02-28T23:59:59.999Z");
    }

    #[test]
    fn json_lines_round_trip() {
        prop::check(
            "log json lines",
            300,
            |r| {
                let s = |r: &mut crate::util::rng::Rng| {
                    String::from_utf8_lossy(&prop::small_key(r, 12)).replace('a', "\"\\\n\u{2028}é")
                };
                (r.next_u64() % 8_000_000_000_000, s(r), s(r), r.chance(0.5).then(|| s(r)), r.next_u64() as i64)
            },
            prop::no_shrink,
            |(ts, msg, node, rid, n)| {
                let fields = [("n", json!(n)), ("s", json!(msg)), ("o", json!({"k": [1, null]}))];
                let line = json_line(*ts, Level::Info, Some(node), rid.as_deref(), msg, &fields);
                if line.contains('\n') {
                    return Err("a record spans lines".into());
                }
                let v: Json = serde_json::from_str(&line).map_err(|e| format!("{e}: {line}"))?;
                let want = json!({"ts": rfc3339(*ts), "level": "info", "node": node, "msg": msg,
                    "n": n, "s": msg, "o": {"k": [1, null]}});
                let mut want = want.as_object().cloned().unwrap_or_default();
                if let Some(r) = rid {
                    want.insert("request_id".into(), json!(r));
                }
                if v != Json::Object(want.clone()) {
                    return Err(format!("{v} != {}", Json::Object(want)));
                }
                Ok(())
            },
        );
    }

    #[test]
    fn capture_takes_context_and_levels() {
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        rt.block_on(async {
            let cap = capture(Level::Info);
            debug!("hidden");
            let ctx = Context { node: "n1".into(), request_id: "r-1".into() };
            scoped(ctx, async {
                assert_eq!(request_id().as_deref(), Some("r-1"));
                event(Level::Info, "request", &[("status", json!(200))]);
            })
            .await;
            assert!(request_id().is_none());
            warn!("plain {}", 7);
            let r = cap.records();
            assert_eq!(r.len(), 2, "{r:?}");
            assert_eq!(
                r[0],
                json!({"level": "info", "node": "n1", "request_id": "r-1", "msg": "request", "status": 200})
            );
            assert_eq!(r[1]["msg"], "plain 7");
            assert!(r[1].get("request_id").is_none());
        });
        assert!(!captured(Level::Error), "dropping the capture stops it");
    }

    #[test]
    fn request_ids() {
        for ok in ["a", "0123456789abcdef", "req-1.2:3_x", &"x".repeat(64)] {
            assert!(valid_request_id(ok), "{ok}");
        }
        for bad in ["", "a b", "x\n", "é", "<script>", &"x".repeat(65)] {
            assert!(!valid_request_id(bad), "{bad}");
        }
    }
}
