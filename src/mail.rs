//! Outbound email delivery.

use crate::error::Result;
use crate::store::BoxFut;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Mail {
    pub to: String,
    pub subject: String,
    pub text: String,
}

/// Outbound email transport.
pub trait Mailer: Send + Sync + 'static {
    fn send(&self, mail: &Mail) -> BoxFut<'static, Result<()>>;
}

/// [`Mailer`] that POSTs messages as JSON (`{"from", "to", "subject", "text"}`) to an HTTP endpoint.
#[cfg(feature = "cloud")]
pub struct WebhookMailer {
    url: String,
    token: Option<String>,
    from: String,
    http: reqwest::Client,
}

#[cfg(feature = "cloud")]
impl WebhookMailer {
    pub fn new(url: String, token: Option<String>, from: String) -> WebhookMailer {
        WebhookMailer { url, token, from, http: reqwest::Client::new() }
    }
}

#[cfg(feature = "cloud")]
impl Mailer for WebhookMailer {
    fn send(&self, mail: &Mail) -> BoxFut<'static, Result<()>> {
        let body = serde_json::json!({"from": self.from, "to": mail.to, "subject": mail.subject, "text": mail.text});
        let mut req = self.http.post(&self.url).json(&body);
        if let Some(t) = &self.token {
            req = req.bearer_auth(t);
        }
        Box::pin(async move {
            let resp = req.send().await.map_err(|e| crate::Error::unavailable(format!("mail webhook: {e}")))?;
            if !resp.status().is_success() {
                return Err(crate::Error::unavailable(format!("mail webhook answered {}", resp.status())));
            }
            Ok(())
        })
    }
}
