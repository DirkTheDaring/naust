use crate::{AppState, http_api::errors, http_api::handlers::registry_headers, security};
use axum::{
    body::Body,
    extract::{RawQuery, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use headers::{Authorization, HeaderMapExt, authorization::Basic};
use std::collections::HashSet;
use url::form_urlencoded;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TokenRejection {
    Unauthorized,
    Denied(&'static str),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TokenDecision {
    pub subject: Option<String>,
    pub scopes: Vec<security::TokenScope>,
    pub ttl_secs: u64,
}

pub fn parse_scopes(raw: &str) -> Vec<security::TokenScope> {
    let mut out = Vec::new();
    for token in raw.split_whitespace() {
        if token.is_empty() {
            continue;
        }
        let parts: Vec<&str> = token.split(':').collect();
        if parts.len() >= 3 {
            let typ = parts[0].trim().to_ascii_lowercase();
            let name = parts[1].to_string();
            let actions = parts[2]
                .split(',')
                .map(|a| a.trim().to_ascii_lowercase())
                .filter(|a| !a.is_empty())
                .collect::<Vec<_>>();
            let mut unique_actions = Vec::new();
            let mut seen = HashSet::new();
            for a in actions {
                if seen.insert(a.clone()) {
                    unique_actions.push(a);
                }
            }
            out.push(security::TokenScope {
                typ,
                name,
                actions: unique_actions,
            });
        } else if parts.len() == 2 {
            let typ = parts[0].trim().to_ascii_lowercase();
            let name = parts[1].to_string();
            out.push(security::TokenScope {
                typ,
                name,
                actions: vec!["pull".to_string()],
            });
        }
    }
    out
}

pub fn sanitize_token_scopes(scopes: &[security::TokenScope]) -> Vec<security::TokenScope> {
    let mut out = Vec::new();
    for s in scopes {
        let typ_lower = s.typ.to_ascii_lowercase();
        if typ_lower != "repository" && typ_lower != "registry" && typ_lower != "repo" {
            continue;
        }
        let canonical_typ = if typ_lower == "repo" {
            "repository".to_string()
        } else {
            typ_lower
        };

        if s.name.trim().is_empty() {
            continue;
        }

        let mut valid_actions = Vec::new();
        for a in &s.actions {
            let a_lower = a.to_ascii_lowercase();
            if a_lower == "pull"
                || a_lower == "push"
                || a_lower == "delete"
                || a_lower == "*"
                || a_lower == "read"
            {
                valid_actions.push(a_lower);
            }
        }

        if valid_actions.is_empty() {
            continue;
        }

        out.push(security::TokenScope {
            typ: canonical_typ,
            name: s.name.clone(),
            actions: valid_actions,
        });
    }
    out
}

pub fn token_scope_requests_repo_action(
    scope: &security::TokenScope,
    action: security::RepoAction,
) -> bool {
    let action_str = action.as_str();
    if scope.typ != "repository" && scope.typ != "repo" {
        return false;
    }
    scope.actions.iter().any(|a| a == action_str || a == "*")
}

pub fn wants_push_from_token_scopes(scopes: &[security::TokenScope]) -> bool {
    scopes
        .iter()
        .any(|s| token_scope_requests_repo_action(s, security::RepoAction::Push))
}

pub fn wants_auth_from_token_scopes(
    cfg: &crate::config::Config,
    token_scopes: &[security::TokenScope],
) -> bool {
    let wants_push = wants_push_from_token_scopes(token_scopes);
    let wants_delete = token_scopes
        .iter()
        .any(|s| token_scope_requests_repo_action(s, security::RepoAction::Delete));
    let wants_catalog = token_scopes.iter().any(scope_is_registry_catalog);
    let wants_private = token_scopes.iter().any(|s| cfg.is_repo_private(&s.name));
    let wants_expansive = token_scopes
        .iter()
        .any(|s| scope_name_is_expansive(&s.name));
    wants_push
        || wants_delete
        || wants_private
        || wants_expansive
        || (wants_catalog && crate::auth::catalog_auth_required(cfg))
}

fn scope_name_is_expansive(name: &str) -> bool {
    name.trim().trim_start_matches('/').contains('*')
}

fn scope_is_registry_catalog(scope: &security::TokenScope) -> bool {
    scope.typ == "registry" && (scope.name == "catalog" || scope.name == "*")
}

/// Anonymous tokens stay exact and public (ADR-014, amended by ADR-015).
/// A scope name containing `*`, a registry catalog scope, or a private
/// repository name is not issued without a subject.
fn anonymous_scope_may_be_issued(
    cfg: &crate::config::Config,
    scope: &security::TokenScope,
) -> bool {
    if scope_name_is_expansive(&scope.name) || scope_is_registry_catalog(scope) {
        return false;
    }
    let is_repo = scope.typ == "repository" || scope.typ == "repo" || scope.typ == "image";
    !(is_repo && cfg.is_repo_private(&scope.name))
}

pub fn decide_token_scopes_for_request(
    cfg: &crate::config::Config,
    token_scopes: &[security::TokenScope],
    basic: Option<(String, String)>,
) -> Result<TokenDecision, TokenRejection> {
    let wants_push = wants_push_from_token_scopes(token_scopes);
    let wants_auth = wants_auth_from_token_scopes(cfg, token_scopes);
    let requires_auth = wants_auth || !cfg.anonymous_pull;

    if !requires_auth {
        let scopes: Vec<security::TokenScope> = token_scopes
            .iter()
            .filter(|s| anonymous_scope_may_be_issued(cfg, s))
            .cloned()
            .collect();
        if scopes.len() != token_scopes.len() {
            return Err(TokenRejection::Unauthorized);
        }
        return Ok(TokenDecision {
            subject: None,
            scopes,
            ttl_secs: cfg.token_ttl_secs,
        });
    }

    if cfg.robots.enabled
        && let Some((user, pass)) = basic.as_ref()
            && let Some(account) = cfg.robots.accounts.iter().find(|a| a.name == *user)
                && crate::robot_secrets::verify_robot_secret(pass, &account.secret_hash) {
                    let granted = if token_scopes.is_empty() {
                        Vec::new()
                    } else {
                        crate::rbac::grant_scopes_by_prefix_with_options(
                            token_scopes,
                            &account.grants,
                            cfg.star_grants_catalog,
                        )
                    };
                    if !token_scopes.is_empty() && granted.is_empty() {
                        return Err(TokenRejection::Denied("action not allowed by robot policy"));
                    }

                    let granted_wants_push = wants_push_from_token_scopes(&granted);
                    if wants_push && !granted_wants_push {
                        return Err(TokenRejection::Denied("push not allowed by robot policy"));
                    }

                    let ttl_secs = match account.max_ttl_secs {
                        Some(max) if max > 0 => cfg.token_ttl_secs.min(max),
                        _ => cfg.token_ttl_secs,
                    };

                    return Ok(TokenDecision {
                        subject: Some(format!("robot:{}", account.name)),
                        scopes: granted,
                        ttl_secs,
                    });
                }

    if cfg.users.enabled
        && let Some((user, pass)) = basic.as_ref()
            && let Some(account) = cfg.users.accounts.iter().find(|a| a.name == *user)
                && crate::robot_secrets::verify_robot_secret(pass, &account.secret_hash) {
                    let mut union_grants: Vec<crate::rbac::Grant> = Vec::new();
                    for group_name in &account.groups {
                        if let Some(group) = cfg.users.groups.iter().find(|g| g.name == *group_name)
                        {
                            union_grants.extend(group.grants.clone());
                        }
                    }

                    let granted = if token_scopes.is_empty() {
                        Vec::new()
                    } else {
                        crate::rbac::grant_scopes_by_prefix_with_options(
                            token_scopes,
                            &union_grants,
                            cfg.star_grants_catalog,
                        )
                    };
                    if !token_scopes.is_empty() && granted.is_empty() {
                        return Err(TokenRejection::Denied("action not allowed by user policy"));
                    }

                    let granted_wants_push = wants_push_from_token_scopes(&granted);
                    if wants_push && !granted_wants_push {
                        return Err(TokenRejection::Denied("push not allowed by user policy"));
                    }

                    let ttl_secs = match account.max_ttl_secs {
                        Some(max) if max > 0 => cfg.token_ttl_secs.min(max),
                        _ => cfg.token_ttl_secs,
                    };

                    return Ok(TokenDecision {
                        subject: Some(format!("user:{}", account.name)),
                        scopes: granted,
                        ttl_secs,
                    });
                }

    // Legacy global push auth
    let Some(expected_user) = cfg.push_username.as_deref() else {
        return Err(TokenRejection::Unauthorized);
    };
    let Some(expected_pass) = cfg.push_password.as_deref() else {
        return Err(TokenRejection::Unauthorized);
    };
    let Some((user, pass)) = basic else {
        return Err(TokenRejection::Unauthorized);
    };
    if !crate::auth::configured_secrets_match(&user, &pass, expected_user, expected_pass) {
        return Err(TokenRejection::Unauthorized);
    }

    let allowed_actions: Vec<String> = if cfg.push_implies_delete {
        vec!["pull".to_string(), "push".to_string(), "delete".to_string()]
    } else {
        cfg.push_actions.clone()
    };

    let mut granted_scopes: Vec<security::TokenScope> = Vec::new();
    for req in token_scopes {
        if req.typ == "registry" && (req.name == "catalog" || req.name == "*") {
            if allowed_actions
                .iter()
                .any(|a| a == "*" || a == "pull" || a == "push")
            {
                granted_scopes.push(req.clone());
            }
            continue;
        }
        if req.typ != "repository" {
            continue;
        }
        let Ok(canonical_repo) = crate::registry::CanonicalRepoName::parse(req.name.trim()) else {
            continue;
        };
        let repo_allowed = match cfg.push_allow_repos.as_deref() {
            Some(allowlist) => crate::auth::push_repository_allowed(allowlist, &canonical_repo),
            None => true,
        };

        let mut granted_actions: Vec<String> = Vec::new();
        for a in &req.actions {
            let a_norm = a.trim().to_ascii_lowercase();
            if a_norm == "pull" {
                if repo_allowed || cfg.anonymous_pull {
                    granted_actions.push(a_norm);
                }
            } else if repo_allowed && (allowed_actions.iter().any(|x| x == "*" || x == &a_norm)) {
                granted_actions.push(a_norm);
            }
        }

        if !granted_actions.is_empty() {
            granted_scopes.push(security::TokenScope {
                typ: req.typ.clone(),
                name: canonical_repo.to_string(),
                actions: granted_actions,
            });
        }
    }

    if !token_scopes.is_empty() && granted_scopes.is_empty() {
        return Err(TokenRejection::Denied("action not allowed by policy"));
    }

    if wants_push && !wants_push_from_token_scopes(&granted_scopes) {
        return Err(TokenRejection::Denied("push not allowed by policy"));
    }

    let wants_delete = token_scopes
        .iter()
        .any(|s| token_scope_requests_repo_action(s, security::RepoAction::Delete));
    let granted_wants_delete = granted_scopes
        .iter()
        .any(|s| token_scope_requests_repo_action(s, security::RepoAction::Delete));
    if wants_delete && !granted_wants_delete {
        return Err(TokenRejection::Denied("delete not allowed by policy"));
    }

    Ok(TokenDecision {
        subject: Some(user),
        scopes: granted_scopes,
        ttl_secs: cfg.token_ttl_secs,
    })
}

pub fn service_param_is_valid(param: Option<&str>, configured: &str) -> bool {
    match param {
        None => true,
        Some(p) => p == configured,
    }
}

pub fn token_unauthorized() -> Response {
    let mut resp = errors::unauthorized("authentication required");
    let basic = "Basic realm=\"registry\"";
    if let Ok(v) = http::HeaderValue::from_str(basic) {
        resp.headers_mut().insert(http::header::WWW_AUTHENTICATE, v);
    }
    resp
}

pub fn issue_token(
    state: &AppState,
    subject: Option<&str>,
    scopes: &[security::TokenScope],
    iat: u64,
    exp: u64,
) -> Result<String, security::TokenError> {
    let signing_key = state
        .config
        .token_signing_keys
        .first()
        .cloned()
        .unwrap_or_else(|| security::TokenSigningKey {
            kid: "default".to_string(),
            key: state.config.token_signing_key.clone(),
        });

    security::issue_bearer_token_with_key(
        &signing_key,
        &state.config.token_service,
        subject,
        scopes,
        iat,
        exp,
    )
}

pub async fn token(
    State(state): State<AppState>,
    raw_query: RawQuery,
    headers: HeaderMap,
) -> Response {
    // Parse (transport concern) …
    let mut service_param: Option<String> = None;
    let mut scopes_raw: Vec<String> = Vec::new();
    let raw = raw_query.0.unwrap_or_default();
    for (k, v) in form_urlencoded::parse(raw.as_bytes()) {
        if k == "service" {
            service_param = Some(v.into_owned());
        } else if k == "scope" {
            scopes_raw.push(v.into_owned());
        }
    }
    let basic = headers
        .typed_get::<Authorization<Basic>>()
        .map(|Authorization(b)| (b.username().to_string(), b.password().to_string()));

    // … delegate (R4/KI-26: the token bypass is closed — issuance lives in
    // TokenService) …
    let outcome = state
        .token_svc
        .issue(service_param.as_deref(), &scopes_raw, basic);

    // … format.
    match outcome {
        crate::token_service::TokenOutcome::Issued { body } => {
            let bytes = match serde_json::to_vec(&body) {
                Ok(b) => b,
                Err(_) => return errors::internal_error().into_response(),
            };
            let mut resp_headers = registry_headers();
            resp_headers.insert("Content-Type", "application/json".parse().unwrap());
            resp_headers.insert("Content-Length", bytes.len().to_string().parse().unwrap());
            (StatusCode::OK, resp_headers, Body::from(bytes)).into_response()
        }
        crate::token_service::TokenOutcome::Unauthorized => token_unauthorized(),
        crate::token_service::TokenOutcome::Denied(msg) => errors::denied(msg).into_response(),
        crate::token_service::TokenOutcome::NameInvalid => errors::name_invalid(),
        crate::token_service::TokenOutcome::Internal => errors::internal_error().into_response(),
    }
}
