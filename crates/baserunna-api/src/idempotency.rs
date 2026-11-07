use serde::{Deserialize, Serialize};
use std::fmt;

/// The header that makes a write apply once per key.
pub const IDEMPOTENCY_KEY: &str = "idempotency-key";

/// Most bytes of an [`IdempotencyKey`].
pub const MAX_IDEMPOTENCY_KEY: usize = 200;

/// The key that makes a write apply once: 1 to [`MAX_IDEMPOTENCY_KEY`] visible ASCII characters,
/// so that any key is a valid [`IDEMPOTENCY_KEY`] header value. Only a valid key parses or
/// deserializes.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for IdempotencyKey {
    type Error = String;

    fn try_from(key: String) -> Result<IdempotencyKey, String> {
        if key.is_empty() || key.len() > MAX_IDEMPOTENCY_KEY || !key.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(format!(
                "an idempotency key has 1 to {MAX_IDEMPOTENCY_KEY} visible ASCII characters, without spaces"
            ));
        }
        Ok(IdempotencyKey(key))
    }
}

impl std::str::FromStr for IdempotencyKey {
    type Err = String;

    fn from_str(key: &str) -> Result<IdempotencyKey, String> {
        IdempotencyKey::try_from(key.to_string())
    }
}

impl From<IdempotencyKey> for String {
    fn from(key: IdempotencyKey) -> String {
        key.0
    }
}

impl std::ops::Deref for IdempotencyKey {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for IdempotencyKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(&self.0)
    }
}
