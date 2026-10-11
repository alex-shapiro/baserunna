//! Minimal leveled logging to stderr (no external dependencies).
//! Level is set from `BASERUNNA_LOG` (`error`, `warn`, `info`, `debug`) or
//! programmatically; the simulator keeps it at `error` unless asked.

use std::sync::atomic::{AtomicU8, Ordering};

pub const ERROR: u8 = 1;
pub const WARN: u8 = 2;
pub const INFO: u8 = 3;
pub const DEBUG: u8 = 4;

static LEVEL: AtomicU8 = AtomicU8::new(WARN);

pub fn set_level(l: u8) {
    LEVEL.store(l, Ordering::Relaxed);
}

pub fn level() -> u8 {
    LEVEL.load(Ordering::Relaxed)
}

pub fn init_from_env() {
    if let Ok(v) = std::env::var("BASERUNNA_LOG") {
        set_level(match v.to_ascii_lowercase().as_str() {
            "error" => ERROR,
            "warn" => WARN,
            "info" => INFO,
            "debug" => DEBUG,
            "off" => 0,
            _ => INFO,
        });
    }
}

pub fn enabled(l: u8) -> bool {
    l <= level()
}

#[macro_export]
macro_rules! log_at {
    ($lvl:expr, $tag:expr, $($arg:tt)*) => {
        if $crate::log::enabled($lvl) {
            eprintln!("[{}] {}", $tag, format!($($arg)*));
        }
    };
}

#[macro_export]
macro_rules! warn {
    ($($arg:tt)*) => { $crate::log_at!($crate::log::WARN, "warn", $($arg)*) };
}

#[macro_export]
macro_rules! info {
    ($($arg:tt)*) => { $crate::log_at!($crate::log::INFO, "info", $($arg)*) };
}

#[macro_export]
macro_rules! debug {
    ($($arg:tt)*) => { $crate::log_at!($crate::log::DEBUG, "debug", $($arg)*) };
}
