//! Request correlation IDs, carried between nodes in the `x-request-id` header.

use std::future::Future;
use std::sync::Arc;

/// HTTP header carrying a request's correlation ID.
pub const HEADER: &str = "x-request-id";

/// Longest client-supplied request ID a node accepts.
pub const MAX_LEN: usize = 64;

/// Returns whether `s` is a valid client-supplied request ID.
pub fn valid(s: &str) -> bool {
    (1..=MAX_LEN).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_alphanumeric() || b"._:-".contains(&b))
}

tokio::task_local! {
    static CURRENT: Arc<str>;
}

/// Runs `f` with `id` as the current task's request ID, so that internal
/// requests and commits made on behalf of `f` can be correlated with it.
pub async fn scoped<F: Future>(id: Arc<str>, f: F) -> F::Output {
    CURRENT.scope(id, f).await
}

/// Returns the current task's request ID, if it runs within [`scoped`].
pub fn current() -> Option<Arc<str>> {
    CURRENT.try_with(Arc::clone).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_ids() {
        for ok in ["a", "0123456789abcdef", "req-1.2:3_x", &"x".repeat(64)] {
            assert!(valid(ok), "{ok}");
        }
        for bad in ["", "a b", "x\n", "é", "<script>", &"x".repeat(65)] {
            assert!(!valid(bad), "{bad}");
        }
    }

    #[test]
    fn scopes_follow_the_task() {
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        rt.block_on(async {
            assert!(current().is_none());
            let seen = scoped("r-1".into(), async { current() }).await;
            assert_eq!(seen.as_deref(), Some("r-1"));
            assert!(current().is_none());
        });
    }
}
