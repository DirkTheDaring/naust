use base64::Engine as _;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum TokenError {
    #[error("invalid token format")]
    InvalidFormat,
    #[error("invalid token signature")]
    InvalidSignature,
    #[error("invalid token payload")]
    InvalidPayload,
    #[error("token expired")]
    Expired,
    #[error("token signing key invalid")]
    InvalidSigningKey,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct TokenScope {
    #[serde(rename = "type")]
    pub typ: String,
    pub name: String,
    pub actions: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TokenClaims {
    pub exp: u64,
    #[serde(default)]
    pub scopes: Vec<TokenScope>,

    // Present in tokens we mint, but not required for verification.
    #[serde(default)]
    pub iss: Option<String>,
    #[serde(default)]
    pub sub: Option<String>,
    #[serde(default)]
    pub iat: Option<u64>,

    // Audience binding (service name). We mint this and enforce it for auth.
    #[serde(default)]
    pub aud: Option<String>,

    // Unique ID (useful for log correlation; not persisted).
    #[serde(default)]
    pub jti: Option<String>,
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RepoAction {
    Pull,
    Push,
}

impl RepoAction {
    pub fn as_str(self) -> &'static str {
        match self {
            RepoAction::Pull => "pull",
            RepoAction::Push => "push",
        }
    }
}

pub fn verify_bearer_token(signing_key: &str, token: &str) -> Result<TokenClaims, TokenError> {
    const MAX_TOKEN_LEN: usize = 8192;
    if token.is_empty() || token.len() > MAX_TOKEN_LEN {
        return Err(TokenError::InvalidFormat);
    }

    let (payload_b64, sig_b64) = token.split_once('.').ok_or(TokenError::InvalidFormat)?;
    if payload_b64.is_empty() || sig_b64.is_empty() {
        return Err(TokenError::InvalidFormat);
    }

    let sig = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(sig_b64.as_bytes())
        .map_err(|_| TokenError::InvalidSignature)?;

    let mut mac = Hmac::<Sha256>::new_from_slice(signing_key.as_bytes())
        .map_err(|_| TokenError::InvalidSigningKey)?;
    mac.update(payload_b64.as_bytes());
    mac.verify_slice(&sig)
        .map_err(|_| TokenError::InvalidSignature)?;

    let payload_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload_b64.as_bytes())
        .map_err(|_| TokenError::InvalidPayload)?;
    let claims: TokenClaims =
        serde_json::from_slice(&payload_bytes).map_err(|_| TokenError::InvalidPayload)?;

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| TokenError::InvalidPayload)?
        .as_secs();
    if now > claims.exp {
        return Err(TokenError::Expired);
    }

    Ok(claims)
}

pub fn verify_bearer_token_bound(
    signing_key: &str,
    token: &str,
    expected_aud: &str,
    max_ttl_secs: u64,
) -> Result<TokenClaims, TokenError> {
    let claims = verify_bearer_token(signing_key, token)?;

    if claims.aud.as_deref() != Some(expected_aud) {
        return Err(TokenError::InvalidPayload);
    }

    let Some(iat) = claims.iat else {
        return Err(TokenError::InvalidPayload);
    };
    if claims.exp < iat {
        return Err(TokenError::InvalidPayload);
    }
    if max_ttl_secs > 0 {
        let ttl = claims.exp.saturating_sub(iat);
        if ttl > max_ttl_secs {
            return Err(TokenError::InvalidPayload);
        }
    }

    Ok(claims)
}

pub fn issue_bearer_token(
    signing_key: &str,
    aud: &str,
    subject: Option<&str>,
    scopes: &[TokenScope],
    iat: u64,
    exp: u64,
) -> Result<String, TokenError> {
    let claims = TokenClaims {
        iss: Some("registry-rust".to_string()),
        sub: subject.map(|s| s.to_string()),
        iat: Some(iat),
        exp,
        scopes: scopes.to_vec(),
        aud: Some(aud.to_string()),
        jti: Some(Uuid::new_v4().to_string()),
    };

    let payload_bytes = serde_json::to_vec(&claims).map_err(|_| TokenError::InvalidPayload)?;
    let payload_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&payload_bytes);

    let mut mac = Hmac::<Sha256>::new_from_slice(signing_key.as_bytes())
        .map_err(|_| TokenError::InvalidSigningKey)?;
    mac.update(payload_b64.as_bytes());
    let sig = mac.finalize().into_bytes();
    let sig_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(sig);

    Ok(format!("{payload_b64}.{sig_b64}"))
}

pub fn token_allows_repo_action(claims: &TokenClaims, repo: &str, action: RepoAction) -> bool {
    let action = action.as_str();
    claims
        .scopes
        .iter()
        .any(|s| s.typ == "repository" && s.name == repo && s.actions.iter().any(|a| a == action))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_secs()
    }

    #[test]
    fn bearer_round_trip_ok() {
        let signing_key = "test-signing-key";
        let aud = "registry";
        let now = now_secs();
        let scopes = vec![TokenScope {
            typ: "repository".to_string(),
            name: "org/repo".to_string(),
            actions: vec!["pull".to_string(), "push".to_string()],
        }];

        let token = issue_bearer_token(signing_key, aud, Some("user"), &scopes, now, now + 3600)
            .expect("issue token");
        let claims =
            verify_bearer_token_bound(signing_key, &token, aud, 3600).expect("verify token");

        assert_eq!(claims.iss.as_deref(), Some("registry-rust"));
        assert_eq!(claims.sub.as_deref(), Some("user"));
        assert_eq!(claims.aud.as_deref(), Some(aud));
        assert!(claims.jti.as_deref().is_some_and(|s| !s.is_empty()));
        assert!(token_allows_repo_action(
            &claims,
            "org/repo",
            RepoAction::Push
        ));
        assert!(token_allows_repo_action(
            &claims,
            "org/repo",
            RepoAction::Pull
        ));
        assert!(!token_allows_repo_action(
            &claims,
            "org/other",
            RepoAction::Pull
        ));
    }

    #[test]
    fn bearer_expired_rejected() {
        let signing_key = "test-signing-key";
        let aud = "registry";
        let now = now_secs();
        let scopes: Vec<TokenScope> = Vec::new();

        let token = issue_bearer_token(signing_key, aud, None, &scopes, now, now.saturating_sub(1))
            .expect("issue token");
        let err = verify_bearer_token_bound(signing_key, &token, aud, 3600)
            .expect_err("should be expired");

        match err {
            TokenError::Expired => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn bearer_invalid_format_rejected() {
        let err = verify_bearer_token("k", "not-a-token").expect_err("invalid format");
        match err {
            TokenError::InvalidFormat => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn bearer_tampered_signature_rejected() {
        let signing_key = "test-signing-key";
        let aud = "registry";
        let now = now_secs();
        let scopes: Vec<TokenScope> = Vec::new();
        let token = issue_bearer_token(signing_key, aud, None, &scopes, now, now + 3600)
            .expect("issue token");

        let (payload, sig) = token.split_once('.').expect("token format");
        let mut sig_bytes = sig.as_bytes().to_vec();
        // Flip a base64url character in a minimal way.
        if let Some(b) = sig_bytes.get_mut(0) {
            *b = if *b == b'A' { b'B' } else { b'A' };
        }
        let tampered = format!("{payload}.{}", String::from_utf8(sig_bytes).expect("utf8"));

        let err = verify_bearer_token(signing_key, &tampered).expect_err("invalid signature");
        match err {
            TokenError::InvalidSignature => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn bearer_wrong_key_rejected() {
        let now = now_secs();
        let scopes: Vec<TokenScope> = Vec::new();
        let token =
            issue_bearer_token("key-a", "registry", None, &scopes, now, now + 3600).expect("issue");

        let err = verify_bearer_token("key-b", &token).expect_err("wrong key");
        match err {
            TokenError::InvalidSignature => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn bearer_empty_or_oversized_rejected_as_invalid_format() {
        let err = verify_bearer_token("k", "").expect_err("empty");
        assert!(matches!(err, TokenError::InvalidFormat));

        let huge = "a".repeat(9000);
        let err = verify_bearer_token("k", &huge).expect_err("oversized");
        assert!(matches!(err, TokenError::InvalidFormat));
    }

    #[test]
    fn bearer_empty_parts_rejected_as_invalid_format() {
        let err = verify_bearer_token("k", ".sig").expect_err("empty payload");
        assert!(matches!(err, TokenError::InvalidFormat));

        let err = verify_bearer_token("k", "payload.").expect_err("empty sig");
        assert!(matches!(err, TokenError::InvalidFormat));
    }

    #[test]
    fn bearer_invalid_signature_vs_format_classification() {
        // Wrong separator -> format error.
        let err = verify_bearer_token("k", "payload+sig").expect_err("format");
        assert!(matches!(err, TokenError::InvalidFormat));

        // Proper structure but invalid base64/signature should not look like a format issue.
        let err = verify_bearer_token("k", "cGF5bG9hZA.sig").expect_err("signature");
        assert!(matches!(err, TokenError::InvalidSignature));
    }

    #[test]
    fn bearer_wrong_audience_rejected() {
        let signing_key = "test-signing-key";
        let now = now_secs();
        let scopes: Vec<TokenScope> = Vec::new();

        let token = issue_bearer_token(signing_key, "aud-a", None, &scopes, now, now + 3600)
            .expect("issue");

        let err =
            verify_bearer_token_bound(signing_key, &token, "aud-b", 3600).expect_err("wrong aud");
        assert!(matches!(err, TokenError::InvalidPayload));
    }

    #[test]
    fn bearer_ttl_over_max_rejected() {
        let signing_key = "test-signing-key";
        let now = now_secs();
        let scopes: Vec<TokenScope> = Vec::new();

        let token = issue_bearer_token(signing_key, "registry", None, &scopes, now, now + 7200)
            .expect("issue");

        let err = verify_bearer_token_bound(signing_key, &token, "registry", 3600)
            .expect_err("ttl too large");
        assert!(matches!(err, TokenError::InvalidPayload));
    }
}
