use base64::Engine as _;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};
use subtle::ConstantTimeEq;
use uuid::Uuid;

/// Equal-length byte compare. Different lengths return false immediately.
pub fn constant_time_eq(left: &str, right: &str) -> bool {
    let left = left.as_bytes();
    let right = right.as_bytes();
    if left.len() != right.len() {
        return false;
    }
    left.ct_eq(right).into()
}

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

fn deserialize_actions<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct ActionsVisitor;
    impl<'de> serde::de::Visitor<'de> for ActionsVisitor {
        type Value = Vec<String>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a string, list of strings, or null")
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(Vec::new())
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(Vec::new())
        }

        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            deserializer.deserialize_any(self)
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(v.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect())
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut list = Vec::new();
            while let Some(item) = seq.next_element::<String>()? {
                if !item.is_empty() {
                    list.push(item);
                }
            }
            Ok(list)
        }
    }

    deserializer.deserialize_any(ActionsVisitor)
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct TokenScope {
    #[serde(rename = "type", alias = "typ")]
    pub typ: String,
    pub name: String,
    #[serde(default, alias = "action", deserialize_with = "deserialize_actions")]
    pub actions: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TokenClaims {
    pub exp: u64,
    #[serde(default, rename = "access", alias = "scopes")]
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

    // Key id used for signing (helps key rotation with overlap).
    #[serde(default)]
    pub kid: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TokenSigningKey {
    pub kid: String,
    pub key: String,
}

pub fn decode_token_parts(token: &str) -> Result<(&str, Vec<u8>, Vec<u8>), TokenError> {
    const MAX_TOKEN_LEN: usize = 65536;
    if token.is_empty() || token.len() > MAX_TOKEN_LEN {
        return Err(TokenError::InvalidFormat);
    }

    let parts: Vec<&str> = token.split('.').collect();
    // Bearers are three-part only (ADR-016). A two-part token is the upload-state
    // construction and must not verify here.
    let (signed_input, payload_b64, sig_b64) = match parts.len() {
        3 => {
            let signed_len = parts[0].len() + 1 + parts[1].len();
            (&token[..signed_len], parts[1], parts[2])
        }
        _ => return Err(TokenError::InvalidFormat),
    };

    if payload_b64.is_empty() || sig_b64.is_empty() {
        return Err(TokenError::InvalidFormat);
    }

    let sig = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(sig_b64.as_bytes())
        .or_else(|_| base64::engine::general_purpose::STANDARD_NO_PAD.decode(sig_b64.as_bytes()))
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(sig_b64.as_bytes()))
        .or_else(|_| base64::engine::general_purpose::STANDARD.decode(sig_b64.as_bytes()))
        .map_err(|_| TokenError::InvalidSignature)?;

    let payload_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload_b64.as_bytes())
        .or_else(|_| {
            base64::engine::general_purpose::STANDARD_NO_PAD.decode(payload_b64.as_bytes())
        })
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(payload_b64.as_bytes()))
        .or_else(|_| base64::engine::general_purpose::STANDARD.decode(payload_b64.as_bytes()))
        .map_err(|_| TokenError::InvalidPayload)?;

    Ok((signed_input, sig, payload_bytes))
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RepoAction {
    Pull,
    Push,
    Delete,
}

impl RepoAction {
    pub fn as_str(self) -> &'static str {
        match self {
            RepoAction::Pull => "pull",
            RepoAction::Push => "push",
            RepoAction::Delete => "delete",
        }
    }
}

pub fn verify_bearer_token(signing_key: &str, token: &str) -> Result<TokenClaims, TokenError> {
    let key = TokenSigningKey {
        kid: "default".to_string(),
        key: signing_key.to_string(),
    };
    verify_bearer_token_with_keys(&[key], token)
}

pub fn verify_bearer_token_with_keys(
    signing_keys: &[TokenSigningKey],
    token: &str,
) -> Result<TokenClaims, TokenError> {
    if signing_keys.is_empty() {
        return Err(TokenError::InvalidSigningKey);
    }

    let (signed_input, sig, payload_bytes) = decode_token_parts(token)?;

    let mut verified = false;
    for k in signing_keys {
        let mut mac = Hmac::<Sha256>::new_from_slice(k.key.as_bytes())
            .map_err(|_| TokenError::InvalidSigningKey)?;
        mac.update(signed_input.as_bytes());
        if mac.verify_slice(&sig).is_ok() {
            verified = true;
            break;
        }
    }

    if !verified {
        return Err(TokenError::InvalidSignature);
    }

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
    let key = TokenSigningKey {
        kid: "default".to_string(),
        key: signing_key.to_string(),
    };
    verify_bearer_token_bound_with_keys(&[key], token, expected_aud, max_ttl_secs)
}

pub fn verify_bearer_token_bound_with_keys(
    signing_keys: &[TokenSigningKey],
    token: &str,
    expected_aud: &str,
    max_ttl_secs: u64,
) -> Result<TokenClaims, TokenError> {
    let claims = verify_bearer_token_with_keys(signing_keys, token)?;

    if !expected_aud.is_empty() {
        match claims.aud.as_deref() {
            Some(aud) if !aud.is_empty() && aud == expected_aud => {}
            _ => return Err(TokenError::InvalidPayload),
        }
    }

    if let Some(iat) = claims.iat {
        if claims.exp < iat {
            return Err(TokenError::InvalidPayload);
        }
        if max_ttl_secs > 0 {
            let ttl = claims.exp.saturating_sub(iat);
            if ttl > max_ttl_secs {
                return Err(TokenError::InvalidPayload);
            }
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
    let header = serde_json::json!({
        "typ": "JWT",
        "alg": "HS256"
    });
    let header_bytes = serde_json::to_vec(&header).map_err(|_| TokenError::InvalidPayload)?;
    let header_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&header_bytes);

    let claims = TokenClaims {
        iss: Some("naust".to_string()),
        sub: subject.map(|s| s.to_string()),
        iat: Some(iat),
        exp,
        scopes: scopes.to_vec(),
        aud: Some(aud.to_string()),
        jti: Some(Uuid::new_v4().to_string()),
        kid: None,
    };

    let payload_bytes = serde_json::to_vec(&claims).map_err(|_| TokenError::InvalidPayload)?;
    let payload_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&payload_bytes);
    let signing_input = format!("{header_b64}.{payload_b64}");

    let mut mac = Hmac::<Sha256>::new_from_slice(signing_key.as_bytes())
        .map_err(|_| TokenError::InvalidSigningKey)?;
    mac.update(signing_input.as_bytes());
    let sig = mac.finalize().into_bytes();
    let sig_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(sig);

    Ok(format!("{signing_input}.{sig_b64}"))
}

pub fn issue_bearer_token_with_key(
    signing_key: &TokenSigningKey,
    aud: &str,
    subject: Option<&str>,
    scopes: &[TokenScope],
    iat: u64,
    exp: u64,
) -> Result<String, TokenError> {
    let header = serde_json::json!({
        "typ": "JWT",
        "alg": "HS256",
        "kid": signing_key.kid
    });
    let header_bytes = serde_json::to_vec(&header).map_err(|_| TokenError::InvalidPayload)?;
    let header_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&header_bytes);

    let claims = TokenClaims {
        iss: Some("naust".to_string()),
        sub: subject.map(|s| s.to_string()),
        iat: Some(iat),
        exp,
        scopes: scopes.to_vec(),
        aud: Some(aud.to_string()),
        jti: Some(Uuid::new_v4().to_string()),
        kid: Some(signing_key.kid.clone()),
    };

    let payload_bytes = serde_json::to_vec(&claims).map_err(|_| TokenError::InvalidPayload)?;
    let payload_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&payload_bytes);
    let signing_input = format!("{header_b64}.{payload_b64}");

    let mut mac = Hmac::<Sha256>::new_from_slice(signing_key.key.as_bytes())
        .map_err(|_| TokenError::InvalidSigningKey)?;
    mac.update(signing_input.as_bytes());
    let sig = mac.finalize().into_bytes();
    let sig_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(sig);

    Ok(format!("{signing_input}.{sig_b64}"))
}

fn matches_repo_name(scope_name: &str, target_name: &str) -> bool {
    let s = scope_name.trim().trim_start_matches('/');
    let t = target_name.trim().trim_start_matches('/');
    // The scope names the stored repository (ADR-016). `library/name` and `name`
    // are different repositories. `*` and `…/*` authorize nothing.
    if s.is_empty() || t.is_empty() || s.contains('*') {
        return false;
    }
    s == t
}

/// Key for upload-state tokens. Distinct from the bearer signing key so an
/// upload-state MAC cannot verify as a bearer (ADR-016).
pub fn upload_state_signing_key(token_key: &[u8]) -> Vec<u8> {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(token_key).expect("HMAC-SHA256 accepts a key of any length");
    mac.update(b"naust.upload-state.v1");
    mac.finalize().into_bytes().to_vec()
}

pub fn token_allows_repo_action(claims: &TokenClaims, repo: &str, action: RepoAction) -> bool {
    let action_str = action.as_str();
    claims.scopes.iter().any(|s| {
        (s.typ == "repository" || s.typ == "repo" || s.typ == "image")
            && matches_repo_name(&s.name, repo)
            && s.actions.iter().any(|a| a == action_str || a == "*")
    })
}

pub fn token_allows_catalog_action(claims: &TokenClaims) -> bool {
    claims.scopes.iter().any(|s| {
        s.typ == "registry"
            && (s.name == "catalog" || s.name == "*")
            && s.actions
                .iter()
                .any(|a| a == "*" || a == "pull" || a == "push" || a == "read")
    })
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

        assert_eq!(claims.iss.as_deref(), Some("naust"));
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
    fn bearer_round_trip_with_key_includes_kid_and_verifies_with_keyring() {
        let signing_key_primary = TokenSigningKey {
            kid: "k2026_01".to_string(),
            key: "key-primary".to_string(),
        };
        let signing_key_secondary = TokenSigningKey {
            kid: "k2025_12".to_string(),
            key: "key-secondary".to_string(),
        };
        let aud = "registry";
        let now = now_secs();
        let scopes: Vec<TokenScope> = Vec::new();

        let token = issue_bearer_token_with_key(
            &signing_key_primary,
            aud,
            Some("user"),
            &scopes,
            now,
            now + 60,
        )
        .expect("issue token");

        let claims = verify_bearer_token_bound_with_keys(
            &[signing_key_primary.clone(), signing_key_secondary],
            &token,
            aud,
            60,
        )
        .expect("verify");

        assert_eq!(claims.kid.as_deref(), Some("k2026_01"));
    }

    #[test]
    fn bearer_overlap_verification_accepts_token_signed_with_secondary_key() {
        let signing_key_primary = TokenSigningKey {
            kid: "k_new".to_string(),
            key: "new-key".to_string(),
        };
        let signing_key_old = TokenSigningKey {
            kid: "k_old".to_string(),
            key: "old-key".to_string(),
        };
        let aud = "registry";
        let now = now_secs();
        let scopes: Vec<TokenScope> = Vec::new();

        // Simulate an in-flight token minted before rotation.
        let token =
            issue_bearer_token_with_key(&signing_key_old, aud, None, &scopes, now, now + 60)
                .expect("issue token");

        // After rotation, verify against both keys.
        let claims = verify_bearer_token_bound_with_keys(
            &[signing_key_primary.clone(), signing_key_old.clone()],
            &token,
            aud,
            60,
        )
        .expect("verify with overlap");
        assert_eq!(claims.kid.as_deref(), Some("k_old"));

        // Without the old key, signature verification should fail.
        let err = verify_bearer_token_bound_with_keys(&[signing_key_primary], &token, aud, 60)
            .expect_err("should fail without old key");
        assert!(matches!(err, TokenError::InvalidSignature));
    }

    #[test]
    fn bearer_legacy_token_without_kid_verifies_with_default_keyring_entry() {
        let aud = "registry";
        let now = now_secs();
        let scopes: Vec<TokenScope> = Vec::new();

        let token = issue_bearer_token("legacy-key", aud, None, &scopes, now, now + 60)
            .expect("issue legacy token");

        let keyring = vec![TokenSigningKey {
            kid: "default".to_string(),
            key: "legacy-key".to_string(),
        }];

        let claims = verify_bearer_token_bound_with_keys(&keyring, &token, aud, 60)
            .expect("verify legacy token");
        assert!(claims.kid.is_none());
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

        let (prefix, sig) = token.rsplit_once('.').expect("token format");
        let mut sig_bytes = sig.as_bytes().to_vec();
        // Flip a base64url character in a minimal way.
        if let Some(b) = sig_bytes.get_mut(0) {
            *b = if *b == b'A' { b'B' } else { b'A' };
        }
        let tampered = format!("{prefix}.{}", String::from_utf8(sig_bytes).expect("utf8"));

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

        // Two-part tokens are the upload-state construction, not bearers.
        let err = verify_bearer_token("k", "cGF5bG9hZA.sig").expect_err("two-part");
        assert!(matches!(err, TokenError::InvalidFormat));

        // Three-part structure with a bad signature is an invalid signature.
        let err =
            verify_bearer_token("k", "eyJhbGciOiJIUzI1NiJ9.cGF5bG9hZA.sig").expect_err("signature");
        assert!(matches!(err, TokenError::InvalidSignature));
    }

    #[test]
    fn test_prod_verifier_1_correct_audience_accepted() {
        let key = TokenSigningKey {
            kid: "k1".to_string(),
            key: "secret".to_string(),
        };
        let now = now_secs();
        let token =
            issue_bearer_token_with_key(&key, "registry.example.com", None, &[], now, now + 3600)
                .unwrap();
        let claims =
            verify_bearer_token_bound_with_keys(&[key], &token, "registry.example.com", 3600)
                .unwrap();
        assert_eq!(claims.aud.as_deref(), Some("registry.example.com"));
    }

    #[test]
    fn test_prod_verifier_2_incorrect_audience_rejected() {
        let key = TokenSigningKey {
            kid: "k1".to_string(),
            key: "secret".to_string(),
        };
        let now = now_secs();
        let token =
            issue_bearer_token_with_key(&key, "auth.example.com", None, &[], now, now + 3600)
                .unwrap();
        let err = verify_bearer_token_bound_with_keys(&[key], &token, "registry.example.com", 3600)
            .unwrap_err();
        assert!(matches!(err, TokenError::InvalidPayload));
    }

    #[test]
    fn test_prod_verifier_3_missing_audience_rejected_when_expected_aud_configured() {
        let key = TokenSigningKey {
            kid: "k1".to_string(),
            key: "secret".to_string(),
        };
        let now = now_secs();
        // Token minted without audience ("")
        let token = issue_bearer_token_with_key(&key, "", None, &[], now, now + 3600).unwrap();
        let err = verify_bearer_token_bound_with_keys(&[key], &token, "registry.example.com", 3600)
            .unwrap_err();
        assert!(matches!(err, TokenError::InvalidPayload));
    }

    #[test]
    fn test_prod_verifier_4_expired_tokens_rejected() {
        let key = TokenSigningKey {
            kid: "k1".to_string(),
            key: "secret".to_string(),
        };
        let now = now_secs();
        let token = issue_bearer_token_with_key(
            &key,
            "registry",
            None,
            &[],
            now.saturating_sub(10),
            now.saturating_sub(1),
        )
        .unwrap();
        let err =
            verify_bearer_token_bound_with_keys(&[key], &token, "registry", 3600).unwrap_err();
        assert!(matches!(err, TokenError::Expired));
    }

    #[test]
    fn test_prod_verifier_5_unknown_key_rejected() {
        let key_a = TokenSigningKey {
            kid: "ka".to_string(),
            key: "secret_a".to_string(),
        };
        let key_b = TokenSigningKey {
            kid: "kb".to_string(),
            key: "secret_b".to_string(),
        };
        let now = now_secs();
        let token =
            issue_bearer_token_with_key(&key_a, "registry", None, &[], now, now + 3600).unwrap();
        let err =
            verify_bearer_token_bound_with_keys(&[key_b], &token, "registry", 3600).unwrap_err();
        assert!(matches!(err, TokenError::InvalidSignature));
    }

    #[test]
    fn test_prod_verifier_6_key_rotation_supported() {
        let key_old = TokenSigningKey {
            kid: "k_old".to_string(),
            key: "old_secret".to_string(),
        };
        let key_new = TokenSigningKey {
            kid: "k_new".to_string(),
            key: "new_secret".to_string(),
        };
        let now = now_secs();
        let token =
            issue_bearer_token_with_key(&key_old, "registry", None, &[], now, now + 3600).unwrap();
        let claims =
            verify_bearer_token_bound_with_keys(&[key_new, key_old], &token, "registry", 3600)
                .unwrap();
        assert_eq!(claims.kid.as_deref(), Some("k_old"));
    }

    #[test]
    fn test_prod_verifier_7_issuer_and_verifier_agree_on_audience() {
        let key = TokenSigningKey {
            kid: "k1".to_string(),
            key: "secret".to_string(),
        };
        let aud = "production-registry-service";
        let now = now_secs();
        let token =
            issue_bearer_token_with_key(&key, aud, Some("user1"), &[], now, now + 3600).unwrap();
        let claims = verify_bearer_token_bound_with_keys(&[key], &token, aud, 3600).unwrap();
        assert_eq!(claims.aud.as_deref(), Some(aud));
        assert_eq!(claims.sub.as_deref(), Some("user1"));
    }

    #[test]
    fn test_token_scope_repository_authorization_matrix() {
        let claims = TokenClaims {
            sub: Some("user1".to_string()),
            aud: Some("service".to_string()),
            exp: now_secs() + 3600,
            iat: Some(now_secs()),
            jti: None,
            iss: None,
            kid: None,
            scopes: vec![
                TokenScope {
                    typ: "repository".to_string(),
                    name: "org/app".to_string(),
                    actions: vec!["pull".to_string()],
                },
                TokenScope {
                    typ: "repository".to_string(),
                    name: "teams/backend/*".to_string(),
                    actions: vec!["pull".to_string(), "push".to_string()],
                },
                TokenScope {
                    typ: "repository".to_string(),
                    name: "library/ubuntu".to_string(),
                    actions: vec!["pull".to_string()],
                },
            ],
        };

        // 1. Exact repository scope
        assert!(token_allows_repo_action(
            &claims,
            "org/app",
            RepoAction::Pull
        ));
        // Action restriction: pull allowed, push denied
        assert!(!token_allows_repo_action(
            &claims,
            "org/app",
            RepoAction::Push
        ));
        assert!(!token_allows_repo_action(
            &claims,
            "org/app",
            RepoAction::Delete
        ));
        // Exact prefix must not match extended name or sub-path
        assert!(!token_allows_repo_action(
            &claims,
            "org/application",
            RepoAction::Pull
        ));
        assert!(!token_allows_repo_action(
            &claims,
            "org/app/sub",
            RepoAction::Pull
        ));

        // 2. A `…/*` scope name authorizes nothing (ADR-015). Grants keep the wildcard.
        assert!(!token_allows_repo_action(
            &claims,
            "teams/backend/service",
            RepoAction::Pull
        ));
        assert!(!token_allows_repo_action(
            &claims,
            "teams/backend/service",
            RepoAction::Push
        ));
        assert!(!token_allows_repo_action(
            &claims,
            "teams/backend/service",
            RepoAction::Delete
        ));
        // Cross boundary rejection
        assert!(!token_allows_repo_action(
            &claims,
            "teams/backend-other/service",
            RepoAction::Pull
        ));
        assert!(!token_allows_repo_action(
            &claims,
            "teams/other/service",
            RepoAction::Pull
        ));

        // 3. library/ubuntu is the stored name. ubuntu is a different repository.
        assert!(token_allows_repo_action(
            &claims,
            "library/ubuntu",
            RepoAction::Pull
        ));
        assert!(!token_allows_repo_action(
            &claims,
            "ubuntu",
            RepoAction::Pull
        ));
        assert!(!token_allows_repo_action(
            &claims,
            "library2/ubuntu",
            RepoAction::Pull
        ));
        assert!(!token_allows_repo_action(
            &claims,
            "library/ubuntu-extra",
            RepoAction::Pull
        ));
        assert!(!token_allows_repo_action(
            &claims,
            "ubuntu-extra",
            RepoAction::Pull
        ));

        // 4. Empty and malformed repository names fail closed
        assert!(!token_allows_repo_action(&claims, "", RepoAction::Pull));
        assert!(!token_allows_repo_action(&claims, "   ", RepoAction::Pull));
        assert!(!token_allows_repo_action(&claims, "/", RepoAction::Pull));
    }

    #[test]
    fn expansive_repository_scope_does_not_authorize_repo_or_catalog() {
        let claims = TokenClaims {
            sub: None,
            aud: Some("service".to_string()),
            exp: now_secs() + 3600,
            iat: Some(now_secs()),
            jti: None,
            iss: None,
            kid: None,
            scopes: vec![
                TokenScope {
                    typ: "repository".to_string(),
                    name: "*".to_string(),
                    actions: vec!["*".to_string()],
                },
                TokenScope {
                    typ: "repository".to_string(),
                    name: "teams/backend/*".to_string(),
                    actions: vec!["pull".to_string()],
                },
            ],
        };
        assert!(!token_allows_repo_action(
            &claims,
            "any/repo",
            RepoAction::Pull
        ));
        assert!(!token_allows_repo_action(
            &claims,
            "teams/backend/service",
            RepoAction::Pull
        ));
        assert!(!token_allows_catalog_action(&claims));

        let catalog = TokenClaims {
            scopes: vec![TokenScope {
                typ: "registry".to_string(),
                name: "catalog".to_string(),
                actions: vec!["*".to_string()],
            }],
            ..claims
        };
        assert!(token_allows_catalog_action(&catalog));
    }

    #[test]
    fn bearer_rejects_two_part_and_payload_only_macs() {
        let signing_key = "test-signing-key";
        let aud = "registry";
        let now = now_secs();
        let token =
            issue_bearer_token(signing_key, aud, Some("user"), &[], now, now + 60).expect("issue");
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);

        let two_part = format!("{}.{}", parts[1], parts[2]);
        assert!(matches!(
            verify_bearer_token(signing_key, &two_part),
            Err(TokenError::InvalidFormat)
        ));

        let mut mac = Hmac::<Sha256>::new_from_slice(signing_key.as_bytes()).expect("hmac");
        mac.update(parts[1].as_bytes());
        let payload_sig =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        let payload_only = format!("{}.{}.{}", parts[0], parts[1], payload_sig);
        assert!(matches!(
            verify_bearer_token(signing_key, &payload_only),
            Err(TokenError::InvalidSignature)
        ));
    }

    #[test]
    fn upload_state_mac_is_not_a_bearer() {
        let token_key = b"test-signing-key";
        let derived = upload_state_signing_key(token_key);
        assert_ne!(derived.as_slice(), token_key);

        // A MAC computed with the derived upload-state key must fail verification against token_key
        let payload = b"org/app:01234567-89ab-cdef-0123-456789abcdef:0";
        let mut derived_mac = Hmac::<Sha256>::new_from_slice(&derived).expect("valid HMAC key");
        derived_mac.update(payload);
        let derived_tag = derived_mac.finalize().into_bytes();

        let mut token_mac = Hmac::<Sha256>::new_from_slice(token_key).expect("valid HMAC key");
        token_mac.update(payload);
        assert!(token_mac.verify_slice(&derived_tag).is_err());

        // Upload state token string is not a valid bearer token
        let dummy_upload_state = format!(
            "org/app:uuid:0.{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(derived_tag)
        );
        assert!(verify_bearer_token("test-signing-key", &dummy_upload_state).is_err());
    }
}
