//! A fake Google OpenID provider for the simulator. It signs real RS256 ID
//! tokens with a fixed test key so the production verification path runs
//! unchanged.

use crate::auth::OidcProvider;
use crate::error::{Error, Result};
use crate::store::BoxFut;
use crate::util::b64;
use aws_lc_rs::rsa::KeyPair;
use aws_lc_rs::signature::{KeyPair as _, RSA_PKCS1_SHA256, RsaPublicKeyComponents};
use serde_json::{Value as Json, json};
use std::collections::BTreeMap;
use std::sync::Mutex;

const KEY_DER: &[u8] = include_bytes!("test_rsa_key.der");
pub const CLIENT_ID: &str = "sim-client.apps.googleusercontent.com";
pub const ISSUER: &str = "https://accounts.google.com";
const KID: &str = "sim-key-1";

#[derive(Clone, Debug)]
pub struct Identity {
    pub sub: String,
    pub email: String,
    pub email_verified: bool,
    /// Test hooks to produce invalid tokens.
    pub tamper: Tamper,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Tamper {
    #[default]
    None,
    WrongAudience,
    Expired,
    BadSignature,
    WrongIssuer,
}

pub struct FakeGoogle {
    key: KeyPair,
    codes: Mutex<BTreeMap<String, Identity>>,
    next: Mutex<u64>,
    now_ms: Box<dyn Fn() -> u64 + Send + Sync>,
    issuers: Vec<String>,
}

impl FakeGoogle {
    pub fn new(now_ms: Box<dyn Fn() -> u64 + Send + Sync>) -> FakeGoogle {
        FakeGoogle {
            key: KeyPair::from_pkcs8(KEY_DER).expect("valid test key"),
            codes: Mutex::new(BTreeMap::new()),
            next: Mutex::new(1),
            now_ms,
            issuers: vec![ISSUER.into(), "accounts.google.com".into()],
        }
    }

    /// The user "consents": returns an authorization code for `id`.
    pub fn issue_code(&self, id: Identity) -> String {
        let mut n = self.next.lock().unwrap_or_else(|e| e.into_inner());
        *n += 1;
        let code = format!("code-{}", *n);
        self.codes.lock().unwrap_or_else(|e| e.into_inner()).insert(code.clone(), id);
        code
    }

    fn sign(&self, id: &Identity) -> String {
        let now = (self.now_ms)() / 1000;
        let header = json!({"alg": "RS256", "kid": KID, "typ": "JWT"});
        let claims = json!({
            "iss": if id.tamper == Tamper::WrongIssuer { "https://evil.example" } else { ISSUER },
            "aud": if id.tamper == Tamper::WrongAudience { "someone-else" } else { CLIENT_ID },
            "sub": id.sub,
            "email": id.email,
            "email_verified": id.email_verified,
            "iat": now,
            "exp": if id.tamper == Tamper::Expired { now.saturating_sub(7200) } else { now + 3600 },
        });
        let msg = format!(
            "{}.{}",
            b64::encode_url(header.to_string().as_bytes()),
            b64::encode_url(claims.to_string().as_bytes())
        );
        let mut sig = vec![0u8; self.key.public_modulus_len()];
        let rng = aws_lc_rs::rand::SystemRandom::new();
        self.key.sign(&RSA_PKCS1_SHA256, &rng, msg.as_bytes(), &mut sig).expect("rsa sign");
        if id.tamper == Tamper::BadSignature {
            sig[10] ^= 0x55;
        }
        format!("{msg}.{}", b64::encode_url(&sig))
    }
}

impl OidcProvider for FakeGoogle {
    fn client_id(&self) -> &str {
        CLIENT_ID
    }

    fn issuers(&self) -> &[String] {
        &self.issuers
    }

    fn authorize_url(&self, state: &str, redirect_uri: &str) -> String {
        format!("https://accounts.google.test/auth?state={state}&redirect_uri={redirect_uri}")
    }

    fn exchange_code<'a>(&'a self, code: &'a str, _redirect_uri: &'a str) -> BoxFut<'a, Result<String>> {
        Box::pin(async move {
            let id = self
                .codes
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(code)
                .ok_or_else(|| Error::Unauthenticated("invalid authorization code".into()))?;
            Ok(self.sign(&id))
        })
    }

    fn jwks(&self) -> BoxFut<'_, Result<Json>> {
        Box::pin(async move {
            let pk = RsaPublicKeyComponents::<Vec<u8>>::from(self.key.public_key());
            Ok(json!({"keys": [{
                "kty": "RSA", "alg": "RS256", "use": "sig", "kid": KID,
                "n": b64::encode_url(&pk.n), "e": b64::encode_url(&pk.e),
            }]}))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::verify_id_token;

    #[tokio::test]
    async fn tokens_verify_and_tampering_is_caught() {
        let g = FakeGoogle::new(Box::new(|| 1_700_000_000_000));
        let jwks = g.jwks().await.unwrap();
        let ok = Identity { sub: "s1".into(), email: "U@x.io".into(), email_verified: true, tamper: Tamper::None };
        let code = g.issue_code(ok.clone());
        let tok = g.exchange_code(&code, "").await.unwrap();
        let c = verify_id_token(&tok, &jwks, CLIENT_ID, g.issuers(), 1_700_000_000_000).unwrap();
        assert_eq!(c.email, "u@x.io");
        assert!(g.exchange_code(&code, "").await.is_err(), "codes are single use");
        for t in [Tamper::WrongAudience, Tamper::Expired, Tamper::BadSignature, Tamper::WrongIssuer] {
            let code = g.issue_code(Identity { tamper: t, ..ok.clone() });
            let tok = g.exchange_code(&code, "").await.unwrap();
            assert!(verify_id_token(&tok, &jwks, CLIENT_ID, g.issuers(), 1_700_000_000_000).is_err(), "{t:?}");
        }
    }
}
