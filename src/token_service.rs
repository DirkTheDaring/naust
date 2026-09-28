//! Server-side token issuance service (remediation R4/A1, KI-26): the `/token`
//! handler delegates here instead of orchestrating decision, allowlist,
//! signing, metrics, and events inline.

use crate::app_state::AuthMetrics;
use crate::http_api::auth_token::{
    TokenRejection, parse_scopes, sanitize_token_scopes, service_param_is_valid,
    token_scope_requests_repo_action, wants_push_from_token_scopes,
};
use crate::security;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub enum TokenOutcome {
    Issued { body: serde_json::Value },
    Unauthorized,
    Denied(&'static str),
    NameInvalid,
    Internal,
}

pub struct TokenService {
    auth: Arc<naust_auth::AuthConfig>,
    metrics: Arc<AuthMetrics>,
}

impl TokenService {
    pub fn new(auth: Arc<naust_auth::AuthConfig>, metrics: Arc<AuthMetrics>) -> Self {
        Self { auth, metrics }
    }

    fn deny(&self, reason: &'static str, detail: &str) {
        self.metrics.inc_token_denied();
        tracing::info!(
            event = "token_denied",
            reason,
            detail,
            "token request denied"
        );
    }

    pub fn issue(
        &self,
        service_param: Option<&str>,
        scopes_raw: &[String],
        basic: Option<(String, String)>,
    ) -> TokenOutcome {
        if !service_param_is_valid(service_param, &self.auth.token_service) {
            self.deny("invalid_service", service_param.unwrap_or(""));
            return TokenOutcome::Denied("invalid token service");
        }

        let scopes = scopes_raw
            .iter()
            .flat_map(|s| parse_scopes(s))
            .collect::<Vec<_>>();
        let token_scopes = sanitize_token_scopes(&scopes);

        let decision = match naust_auth::policy::decide_token_scopes_for_request(
            &self.auth,
            &token_scopes,
            basic,
        ) {
            Ok(d) => d,
            Err(TokenRejection::Unauthorized) => {
                self.deny("unauthorized", "");
                return TokenOutcome::Unauthorized;
            }
            Err(TokenRejection::Denied(msg)) => {
                self.deny("policy", msg);
                return TokenOutcome::Denied(msg);
            }
        };

        if (wants_push_from_token_scopes(&decision.scopes)
            || decision
                .scopes
                .iter()
                .any(|s| token_scope_requests_repo_action(s, security::RepoAction::Delete)))
            && let Some(allowlist) = self.auth.push_allow_repos.as_deref()
        {
            for scope in &decision.scopes {
                if scope.typ == "repository" {
                    let Ok(canonical_repo) = crate::registry::CanonicalRepoName::parse(&scope.name)
                    else {
                        return TokenOutcome::NameInvalid;
                    };
                    if !crate::auth::push_repository_allowed(allowlist, &canonical_repo) {
                        self.deny("push_allowlist", &scope.name);
                        return TokenOutcome::Denied("push not allowed for this repository");
                    }
                }
            }
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_secs();
        let exp = now.saturating_add(decision.ttl_secs);

        let signing_key = self
            .auth
            .token_signing_keys
            .first()
            .cloned()
            .unwrap_or_else(|| security::TokenSigningKey {
                kid: "default".to_string(),
                key: String::new(),
            });
        let token = match security::issue_bearer_token_with_key(
            &signing_key,
            &self.auth.token_service,
            decision.subject.as_deref(),
            &decision.scopes,
            now,
            exp,
        ) {
            Ok(t) => t,
            Err(_) => {
                self.metrics.inc_token_internal_error();
                tracing::error!(event = "token_error", "token signing failed");
                return TokenOutcome::Internal;
            }
        };

        let scopes_json: Vec<serde_json::Value> = decision
            .scopes
            .iter()
            .map(|s| {
                serde_json::json!({
                    "type": s.typ,
                    "name": s.name,
                    "actions": s.actions,
                })
            })
            .collect();

        self.metrics.inc_token_issued();
        tracing::info!(
            event = "token_issued",
            subject = ?decision.subject,
            scopes = decision.scopes.len(),
            ttl_secs = decision.ttl_secs,
            "token issued"
        );

        TokenOutcome::Issued {
            body: serde_json::json!({
                "token": token,
                "access_token": token,
                "expires_in": decision.ttl_secs,
                "issued_at": crate::http_api::handlers::format_rfc3339(now),
                "access": scopes_json,
                "scopes": scopes_json,
            }),
        }
    }
}
