//! Password hashing, opaque tokens, and Google OpenID Connect ID-token verification.

use crate::env::Env;
use crate::error::{Error, Result};
use crate::store::BoxFut;
use crate::util::{ct_eq, hmac_sha256, sha256};
use baserunna_api::b64;
use serde_json::Value as Json;
use std::num::NonZeroU32;

pub const SESSION_PREFIX: &str = "brs_";
pub const API_KEY_PREFIX: &str = "brk_";

/// Hashes `password` as `pbkdf2-sha256$<iters>$<salt b64>$<hash b64>`.
pub fn hash_password(env: &Env, password: &str, iterations: u32) -> String {
    let salt = env.random_bytes(16);
    let iters = NonZeroU32::new(iterations.max(1)).unwrap_or(NonZeroU32::MIN);
    let mut out = [0u8; 32];
    aws_lc_rs::pbkdf2::derive(aws_lc_rs::pbkdf2::PBKDF2_HMAC_SHA256, iters, &salt, password.as_bytes(), &mut out);
    format!("pbkdf2-sha256${}${}${}", iters, b64::encode(&salt), b64::encode(&out))
}

pub fn verify_password(password: &str, stored: &str) -> bool {
    let parts: Vec<&str> = stored.split('$').collect();
    if parts.len() != 4 || parts[0] != "pbkdf2-sha256" {
        return false;
    }
    let (Ok(iters), Some(salt), Some(hash)) = (parts[1].parse::<u32>(), b64::decode(parts[2]), b64::decode(parts[3]))
    else {
        return false;
    };
    let Some(iters) = NonZeroU32::new(iters) else { return false };
    aws_lc_rs::pbkdf2::verify(aws_lc_rs::pbkdf2::PBKDF2_HMAC_SHA256, iters, &salt, password.as_bytes(), &hash).is_ok()
}

pub fn validate_password(p: &str) -> Result<()> {
    if p.chars().count() < 8 || p.len() > 256 {
        return Err(Error::invalid("passwords must be 8 to 256 characters"));
    }
    Ok(())
}

pub fn normalize_email(e: &str) -> Result<String> {
    let e = e.trim().to_ascii_lowercase();
    let ok = e.len() <= 254
        && e.split_once('@')
            .is_some_and(|(l, d)| !l.is_empty() && d.contains('.') && !d.starts_with('.') && !d.ends_with('.'))
        && !e.chars().any(|c| c.is_whitespace() || c.is_control() || c == '<' || c == '>' || c == '"');
    if !ok {
        return Err(Error::invalid("invalid email address"));
    }
    Ok(e)
}

/// Hex-encoded SHA-256 digest of `secret`.
pub fn secret_hash(secret: &str) -> String {
    b64::hex(&sha256(secret.as_bytes()))
}

/// Generates a session token and its hex-encoded SHA-256 hash.
pub fn new_session_token(env: &Env) -> (String, String) {
    let t = format!("{SESSION_PREFIX}{}", b64::encode_url(&env.random_bytes(32)));
    let h = secret_hash(&t);
    (t, h)
}

/// Generates an API key, returning `(full_key, id, secret_hash)`.
pub fn new_api_key(env: &Env) -> (String, String, String) {
    let id = env.random_hex(8);
    let secret = b64::encode_url(&env.random_bytes(32));
    let full = format!("{API_KEY_PREFIX}{id}_{secret}");
    (full, id, secret_hash(&secret))
}

/// Splits an API key of the form `brk_<id>_<secret>` into `(id, secret)`.
pub fn parse_api_key(k: &str) -> Option<(&str, &str)> {
    let rest = k.strip_prefix(API_KEY_PREFIX)?;
    let (id, secret) = rest.split_once('_')?;
    if id.len() != 16 || secret.is_empty() {
        return None;
    }
    Some((id, secret))
}

pub fn verify_api_secret(secret: &str, stored_hash: &str) -> bool {
    ct_eq(secret_hash(secret).as_bytes(), stored_hash.as_bytes())
}

/// Signs `payload` with an expiration timestamp as `<payload>.<expiry ms>.<hmac b64url>`.
pub fn sign_value(key: &[u8], payload: &str, expires_ms: u64) -> String {
    let msg = format!("{payload}.{expires_ms}");
    let mac = hmac_sha256(key, msg.as_bytes());
    format!("{msg}.{}", b64::encode_url(&mac))
}

pub fn verify_signed(key: &[u8], signed: &str, now_ms: u64) -> Option<String> {
    let (msg, mac) = signed.rsplit_once('.')?;
    let (payload, exp) = msg.rsplit_once('.')?;
    let exp: u64 = exp.parse().ok()?;
    let want = hmac_sha256(key, msg.as_bytes());
    let got = b64::decode_url(mac)?;
    if !ct_eq(&want, &got) || now_ms > exp {
        return None;
    }
    Some(payload.to_string())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TokenPurpose {
    Verify,
    Reset,
}

impl TokenPurpose {
    fn tag(self) -> &'static str {
        match self {
            TokenPurpose::Verify => "verify",
            TokenPurpose::Reset => "reset",
        }
    }
}

/// Computes a short digest binding a token to a mutable value such as an email or password hash.
pub fn fingerprint(s: &str) -> String {
    b64::hex(&sha256(format!("fp:{s}").as_bytes())[..8])
}

/// Creates a signed, URL-safe token for `purpose` bound to `subject` and `fp`.
pub fn sign_token(key: &[u8], purpose: TokenPurpose, subject: &str, fp: &str, expires_ms: u64) -> String {
    b64::encode_url(sign_value(key, &format!("{}:{subject}:{fp}", purpose.tag()), expires_ms).as_bytes())
}

/// Verifies `token` for `purpose` at `now_ms`, returning `(subject, fingerprint)`.
pub fn open_token(key: &[u8], purpose: TokenPurpose, token: &str, now_ms: u64) -> Option<(String, String)> {
    let signed = String::from_utf8(b64::decode_url(token)?).ok()?;
    let payload = verify_signed(key, &signed, now_ms)?;
    let mut parts = payload.splitn(3, ':');
    if parts.next()? != purpose.tag() {
        return None;
    }
    let (subject, fp) = (parts.next()?, parts.next()?);
    (!subject.is_empty()).then(|| (subject.to_string(), fp.to_string()))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdClaims {
    pub sub: String,
    pub email: String,
    pub email_verified: bool,
}

/// OpenID Connect provider interface.
pub trait OidcProvider: Send + Sync + 'static {
    fn client_id(&self) -> &str;
    fn issuers(&self) -> &[String];
    fn authorize_url(&self, state: &str, redirect_uri: &str) -> String;
    /// Exchanges an authorization code for a signed ID token.
    fn exchange_code<'a>(&'a self, code: &'a str, redirect_uri: &'a str) -> BoxFut<'a, Result<String>>;
    fn jwks(&self) -> BoxFut<'_, Result<Json>>;
}

fn jwt_part(s: &str) -> Result<Json> {
    let b = b64::decode_url(s).ok_or_else(|| Error::Unauthenticated("malformed ID token".into()))?;
    serde_json::from_slice(&b).map_err(|_| Error::Unauthenticated("malformed ID token".into()))
}

/// Verifies an RS256 ID token against `jwks`.
pub fn verify_id_token(token: &str, jwks: &Json, client_id: &str, issuers: &[String], now_ms: u64) -> Result<IdClaims> {
    let unauth = |m: &str| Error::Unauthenticated(format!("ID token rejected: {m}"));
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(unauth("not a JWT"));
    }
    let header = jwt_part(parts[0])?;
    let claims = jwt_part(parts[1])?;
    if header["alg"] != "RS256" {
        return Err(unauth("unsupported algorithm"));
    }
    let kid = header["kid"].as_str().ok_or_else(|| unauth("missing kid"))?;
    let key = jwks["keys"]
        .as_array()
        .and_then(|ks| ks.iter().find(|k| k["kid"] == kid && k["kty"] == "RSA"))
        .ok_or_else(|| unauth("unknown signing key"))?;
    let n = key["n"].as_str().and_then(b64::decode_url).ok_or_else(|| unauth("bad jwk"))?;
    let e = key["e"].as_str().and_then(b64::decode_url).ok_or_else(|| unauth("bad jwk"))?;
    let sig = b64::decode_url(parts[2]).ok_or_else(|| unauth("bad signature encoding"))?;
    let signed = format!("{}.{}", parts[0], parts[1]);
    let pk = aws_lc_rs::signature::RsaPublicKeyComponents { n: &n[..], e: &e[..] };
    pk.verify(&aws_lc_rs::signature::RSA_PKCS1_2048_8192_SHA256, signed.as_bytes(), &sig)
        .map_err(|_| unauth("bad signature"))?;
    let iss = claims["iss"].as_str().unwrap_or("");
    if !issuers.iter().any(|i| i == iss) {
        return Err(unauth("wrong issuer"));
    }
    let aud_ok = match &claims["aud"] {
        Json::String(a) => a == client_id,
        Json::Array(a) => a.iter().any(|x| x == client_id),
        _ => false,
    };
    if !aud_ok {
        return Err(unauth("wrong audience"));
    }
    let exp = claims["exp"].as_u64().ok_or_else(|| unauth("missing exp"))?;
    if exp * 1000 + 60_000 < now_ms {
        return Err(unauth("expired"));
    }
    if let Some(iat) = claims["iat"].as_u64()
        && iat * 1000 > now_ms + 300_000
    {
        return Err(unauth("issued in the future"));
    }
    let sub = claims["sub"].as_str().ok_or_else(|| unauth("missing sub"))?.to_string();
    let email = claims["email"].as_str().ok_or_else(|| unauth("missing email"))?;
    let email_verified = match &claims["email_verified"] {
        Json::Bool(b) => *b,
        Json::String(s) => s == "true",
        _ => false,
    };
    Ok(IdClaims { sub, email: normalize_email(email)?, email_verified })
}

/// Google OpenID Connect provider.
#[cfg(feature = "cloud")]
pub struct GoogleOidc {
    pub client_id: String,
    pub client_secret: String,
    issuers: Vec<String>,
    http: reqwest::Client,
    jwks_cache: parking_lot::Mutex<Option<(std::time::Instant, Json)>>,
}

#[cfg(feature = "cloud")]
impl GoogleOidc {
    pub fn new(client_id: String, client_secret: String) -> GoogleOidc {
        GoogleOidc {
            client_id,
            client_secret,
            issuers: vec!["https://accounts.google.com".into(), "accounts.google.com".into()],
            http: reqwest::Client::new(),
            jwks_cache: parking_lot::Mutex::new(None),
        }
    }
}

#[cfg(feature = "cloud")]
fn form_encode(pairs: &[(&str, &str)]) -> String {
    fn enc(s: &str) -> String {
        let mut o = String::new();
        for b in s.bytes() {
            match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => o.push(b as char),
                _ => o.push_str(&format!("%{b:02X}")),
            }
        }
        o
    }
    pairs.iter().map(|(k, v)| format!("{}={}", enc(k), enc(v))).collect::<Vec<_>>().join("&")
}

#[cfg(feature = "cloud")]
impl OidcProvider for GoogleOidc {
    fn client_id(&self) -> &str {
        &self.client_id
    }

    fn issuers(&self) -> &[String] {
        &self.issuers
    }

    fn authorize_url(&self, state: &str, redirect_uri: &str) -> String {
        format!(
            "https://accounts.google.com/o/oauth2/v2/auth?{}",
            form_encode(&[
                ("client_id", &self.client_id),
                ("redirect_uri", redirect_uri),
                ("response_type", "code"),
                ("scope", "openid email"),
                ("state", state),
                ("prompt", "select_account"),
            ])
        )
    }

    fn exchange_code<'a>(&'a self, code: &'a str, redirect_uri: &'a str) -> BoxFut<'a, Result<String>> {
        Box::pin(async move {
            let body = form_encode(&[
                ("code", code),
                ("client_id", &self.client_id),
                ("client_secret", &self.client_secret),
                ("redirect_uri", redirect_uri),
                ("grant_type", "authorization_code"),
            ]);
            let resp = self
                .http
                .post("https://oauth2.googleapis.com/token")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(body)
                .send()
                .await
                .map_err(|e| Error::unavailable(format!("google token endpoint: {e}")))?;
            let j: Json = resp.json().await.map_err(|e| Error::unavailable(format!("google token endpoint: {e}")))?;
            j["id_token"]
                .as_str()
                .map(|s| s.to_string())
                .ok_or_else(|| Error::Unauthenticated("google did not return an ID token".into()))
        })
    }

    fn jwks(&self) -> BoxFut<'_, Result<Json>> {
        Box::pin(async move {
            if let Some((t, j)) = self.jwks_cache.lock().as_ref()
                && t.elapsed() < std::time::Duration::from_secs(3600)
            {
                return Ok(j.clone());
            }
            let j: Json = self
                .http
                .get("https://www.googleapis.com/oauth2/v3/certs")
                .send()
                .await
                .map_err(|e| Error::unavailable(format!("google jwks: {e}")))?
                .json()
                .await
                .map_err(|e| Error::unavailable(format!("google jwks: {e}")))?;
            *self.jwks_cache.lock() = Some((std::time::Instant::now(), j.clone()));
            Ok(j)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passwords() {
        let env = Env::sim(1, 0, tokio::time::Instant::now(), 0);
        let h = hash_password(&env, "correct horse", 1000);
        assert!(verify_password("correct horse", &h));
        assert!(!verify_password("wrong horse", &h));
        assert!(!verify_password("correct horse", "garbage"));
        assert!(!verify_password("x", "pbkdf2-sha256$0$AA==$AA=="));
        assert!(validate_password("short").is_err());
    }

    #[test]
    fn emails() {
        assert_eq!(normalize_email(" A@Example.com ").unwrap(), "a@example.com");
        for bad in ["", "a", "a@b", "@b.com", "a b@c.com", "a@.com", "a@com."] {
            assert!(normalize_email(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn tokens_and_signing() {
        let env = Env::sim(2, 0, tokio::time::Instant::now(), 0);
        let (k, id, h) = new_api_key(&env);
        let (pid, secret) = parse_api_key(&k).unwrap();
        assert_eq!(pid, id);
        assert!(verify_api_secret(secret, &h));
        assert!(!verify_api_secret("nope", &h));
        let s = sign_value(b"key", "nonce123", 1000);
        assert_eq!(verify_signed(b"key", &s, 999).as_deref(), Some("nonce123"));
        assert!(verify_signed(b"key", &s, 1001).is_none());
        assert!(verify_signed(b"other", &s, 0).is_none());
        assert!(verify_signed(b"key", "a.b", 0).is_none());
    }

    #[test]
    fn mailed_link_tokens() {
        let fp = fingerprint("pbkdf2-sha256$1$x$y");
        let t = sign_token(b"key", TokenPurpose::Reset, "u1", &fp, 1000);
        assert!(t.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'), "URL-safe: {t}");
        assert_eq!(open_token(b"key", TokenPurpose::Reset, &t, 1000), Some(("u1".into(), fp.clone())));
        assert_eq!(open_token(b"key", TokenPurpose::Verify, &t, 0), None, "a reset token does not verify email");
        assert_eq!(open_token(b"key", TokenPurpose::Reset, &t, 1001), None, "expired");
        assert_eq!(open_token(b"other", TokenPurpose::Reset, &t, 0), None, "another server's key");
        let mut forged = b64::decode_url(&t).unwrap();
        forged[6] ^= 1;
        assert_eq!(open_token(b"key", TokenPurpose::Reset, &b64::encode_url(&forged), 0), None, "tampered");
        assert_ne!(fingerprint("a@b.co"), fingerprint("a@b.com"));
    }

    #[test]
    fn fuzz_tokens() {
        let mut r = crate::util::rng::Rng::new(11);
        let valid: Vec<(String, (String, String))> = (0..8)
            .map(|i| {
                let (subject, fp) = (format!("user-{i}"), fingerprint(&format!("v{i}")));
                let purpose = if i % 2 == 0 { TokenPurpose::Reset } else { TokenPurpose::Verify };
                (sign_token(b"key", purpose, &subject, &fp, 10_000), (subject, fp))
            })
            .collect();
        let pieces = ["", ".", ":", "reset", "verify", "AA", "_-", "=", "é", "\u{0}", "999999999999999999999"];
        for _ in 0..20_000 {
            let (input, want) = if r.chance(0.5) {
                let (t, want) = &valid[r.usize(valid.len())];
                let mut b = t.clone().into_bytes();
                for _ in 0..1 + r.usize(3) {
                    let i = r.usize(b.len());
                    b[i] = b"ABCxyz019-_.:"[r.usize(13)];
                }
                (String::from_utf8_lossy(&b).into_owned(), Some(want.clone()))
            } else {
                ((0..r.usize(8)).map(|_| pieces[r.usize(pieces.len())]).collect(), None)
            };
            for purpose in [TokenPurpose::Reset, TokenPurpose::Verify] {
                if let Some(opened) = open_token(b"key", purpose, &input, 0) {
                    assert_eq!(Some(&opened), want.as_ref(), "{input:?} opened as {opened:?}");
                }
            }
            let _ = verify_signed(b"key", &input, 0);
        }
    }
}
