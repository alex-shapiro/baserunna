//! In-memory [`Mailer`] that captures outbound emails in simulation.

use crate::error::Result;
use crate::mail::{Mail, Mailer};
use crate::store::BoxFut;
use parking_lot::Mutex;

#[derive(Default)]
pub struct Mailbox {
    sent: Mutex<Vec<Mail>>,
}

impl Mailbox {
    pub fn sent(&self) -> Vec<Mail> {
        self.sent.lock().clone()
    }

    pub fn count(&self, to: &str) -> usize {
        self.sent().iter().filter(|m| m.to == to).count()
    }

    /// Extracts the `token` query parameter from the newest link to `path` sent to `to`.
    pub fn latest_token(&self, to: &str, path: &str) -> Option<String> {
        self.token_after(to, path, 0)
    }

    /// Like [`Mailbox::latest_token`], skipping the first `skip` messages to `to`.
    pub fn token_after(&self, to: &str, path: &str, skip: usize) -> Option<String> {
        let mine: Vec<Mail> = self.sent().into_iter().filter(|m| m.to == to).collect();
        mine.iter().skip(skip).rev().find_map(|m| link_token(&m.text, path))
    }
}

/// Extracts the `token` query parameter from the first link to `path` in `text`.
pub fn link_token(text: &str, path: &str) -> Option<String> {
    let needle = format!("{path}?token=");
    let at = text.find(&needle)?;
    let rest = &text[at + needle.len()..];
    let end = rest.find(|c: char| c.is_whitespace() || c == '&').unwrap_or(rest.len());
    Some(rest[..end].to_string())
}

impl Mailer for Mailbox {
    fn send(&self, mail: &Mail) -> BoxFut<'static, Result<()>> {
        self.sent.lock().push(mail.clone());
        Box::pin(async { Ok(()) })
    }
}
