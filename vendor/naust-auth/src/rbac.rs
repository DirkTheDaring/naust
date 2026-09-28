use std::fmt;
use std::str::FromStr;

use naust_types::canonical_name::{CanonicalRepoName, RepoNameError};
use crate::security;

/// Dedicated closed domain model for RBAC repository grants.
///
/// Invariants:
/// - `All`: wildcard `*` authorizes any valid repository.
/// - `Namespace(prefix)`: namespace pattern `prefix/` (e.g. `org/`, `team/sub/`).
///   Matches only repository names strictly under `{prefix}/` (e.g. `org/app`, `org/sub/app`).
///   Never matches sibling names (e.g. `org-secret`), never matches bare `org`.
/// - `Exact(repo)`: exact pattern `repo` (e.g. `org/app`).
///   Matches iff `repo == candidate`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum RbacRepoPattern {
    All,
    Namespace(CanonicalRepoName),
    Exact(CanonicalRepoName),
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RbacPatternError {
    #[error("empty repository grant pattern")]
    Empty,
    #[error("invalid repository name in grant pattern: {0}")]
    InvalidRepoName(#[from] RepoNameError),
    #[error("unsupported wildcard or separator syntax in grant pattern: {0}")]
    InvalidSyntax(String),
}

impl RbacRepoPattern {
    pub fn parse(s: &str) -> Result<Self, RbacPatternError> {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            return Err(RbacPatternError::Empty);
        }
        if trimmed == "*" {
            return Ok(Self::All);
        }
        if trimmed.contains('*') {
            return Err(RbacPatternError::InvalidSyntax(format!(
                "arbitrary wildcards not supported in grant: '{trimmed}'"
            )));
        }
        if trimmed.ends_with('/') {
            let base = trimmed.trim_end_matches('/');
            if base.is_empty() {
                return Err(RbacPatternError::Empty);
            }
            let canonical = CanonicalRepoName::parse(base)?;
            Ok(Self::Namespace(canonical))
        } else {
            let canonical = CanonicalRepoName::parse(trimmed)?;
            Ok(Self::Exact(canonical))
        }
    }

    /// Evaluates whether this grant pattern authorizes the candidate canonical repository name.
    ///
    /// Security Invariants:
    /// - `All` -> returns `true`.
    /// - `Exact(exact)` -> returns `true` iff `exact == candidate`.
    /// - `Namespace(prefix)` -> returns `true` iff `candidate` starts with `{prefix}/` (strictly segment-delimited).
    ///   Does NOT match bare `prefix` and does NOT match `prefix-secret` or `prefix_secret`.
    pub fn matches(&self, candidate: &CanonicalRepoName) -> bool {
        match self {
            Self::All => true,
            Self::Exact(exact) => exact == candidate,
            Self::Namespace(prefix) => {
                let cand_str = candidate.as_str();
                let prefix_str = prefix.as_str();
                if cand_str.starts_with(prefix_str) {
                    let next_byte = cand_str.as_bytes().get(prefix_str.len());
                    next_byte == Some(&b'/')
                } else {
                    false
                }
            }
        }
    }
}

impl fmt::Display for RbacRepoPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::All => write!(f, "*"),
            Self::Namespace(ns) => write!(f, "{}/", ns.as_str()),
            Self::Exact(exact) => write!(f, "{}", exact.as_str()),
        }
    }
}

impl FromStr for RbacRepoPattern {
    type Err = RbacPatternError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Grant {
    pub repo_pattern: RbacRepoPattern,
    pub actions: Vec<String>,
}

impl Grant {
    pub fn new(repo_pattern: RbacRepoPattern, actions: Vec<String>) -> Self {
        Self {
            repo_pattern,
            actions,
        }
    }

    pub fn try_new(repo_prefix: &str, actions: Vec<String>) -> Result<Self, PolicyError> {
        let repo_pattern = RbacRepoPattern::parse(repo_prefix)?;
        Ok(Self {
            repo_pattern,
            actions,
        })
    }

    pub fn allows(&self, repo: &CanonicalRepoName, action: &str) -> bool {
        if !self.repo_pattern.matches(repo) {
            return false;
        }
        self.actions
            .iter()
            .any(|a| a == "*" || a.eq_ignore_ascii_case(action))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum PolicyError {
    #[error("invalid repository grant pattern: {0}")]
    InvalidPattern(#[from] RbacPatternError),
    #[error("invalid action in grant: {0}")]
    InvalidAction(String),
}

fn normalize_action(action: &str) -> Option<&'static str> {
    match action.trim().to_ascii_lowercase().as_str() {
        "pull" => Some("pull"),
        "push" => Some("push"),
        "delete" => Some("delete"),
        "*" => Some("*"),
        _ => None,
    }
}

pub fn validate_grants(grants: &[Grant]) -> Result<Vec<Grant>, PolicyError> {
    let mut out: Vec<Grant> = Vec::with_capacity(grants.len());
    for g in grants {
        let mut actions: Vec<String> = Vec::new();
        for a in &g.actions {
            let Some(norm) = normalize_action(a) else {
                return Err(PolicyError::InvalidAction(a.to_string()));
            };
            if !actions.iter().any(|x| x == norm) {
                actions.push(norm.to_string());
            }
        }

        out.push(Grant {
            repo_pattern: g.repo_pattern.clone(),
            actions,
        });
    }
    Ok(out)
}

/// Evaluates whether a repository grant pattern authorizes access to a repository.
///
/// Security Invariants:
/// - Evaluates via typed `RbacRepoPattern` and `CanonicalRepoName`.
/// - Empty repositories or invalid grant syntax fails closed (returns `false`).
pub fn matches_repo_grant(grant_prefix: &str, repo: &str) -> bool {
    let Ok(pattern) = RbacRepoPattern::parse(grant_prefix) else {
        return false;
    };
    let Ok(candidate) = CanonicalRepoName::parse(repo) else {
        return false;
    };
    pattern.matches(&candidate)
}

/// Compute granted token scopes as the intersection of:
/// - requested scopes (already sanitized)
/// - allowed actions derived from prefix grants
///
/// Security invariants:
/// - output is always a subset of requested
/// - output is always a subset of allowed policy
/// - deterministic: preserves request ordering and requested action ordering
pub fn grant_scopes_by_prefix(
    requested: &[security::TokenScope],
    grants: &[Grant],
) -> Vec<security::TokenScope> {
    grant_scopes_by_prefix_with_options(requested, grants, true)
}

/// KI-18 (resolved): whether an explicit `*` grant confers registry catalog
/// scope is a policy option (`auth.star_grants_catalog`, default preserves the
/// historical behavior).
pub fn grant_scopes_by_prefix_with_options(
    requested: &[security::TokenScope],
    grants: &[Grant],
    star_grants_catalog: bool,
) -> Vec<security::TokenScope> {
    if requested.is_empty() || grants.is_empty() {
        return Vec::new();
    }

    let Ok(grants) = validate_grants(grants) else {
        // When policy is invalid, deny by default.
        return Vec::new();
    };

    let mut out: Vec<security::TokenScope> = Vec::new();

    for req in requested {
        if req.typ == "registry" && (req.name == "catalog" || req.name == "*") {
            let has_catalog = star_grants_catalog
                && grants
                    .iter()
                    .any(|g| matches!(g.repo_pattern, RbacRepoPattern::All));
            if has_catalog {
                out.push(req.clone());
            }
            continue;
        }
        if req.typ != "repository" {
            continue;
        }
        let Ok(canonical_repo) = CanonicalRepoName::parse(req.name.trim()) else {
            // Cannot grant access to invalid repository name
            continue;
        };

        let mut allowed: Vec<&str> = Vec::new();
        for g in &grants {
            if g.repo_pattern.matches(&canonical_repo) {
                for a in &g.actions {
                    if !allowed.iter().any(|x| x == a) {
                        allowed.push(a);
                    }
                }
            }
        }

        if allowed.is_empty() {
            continue;
        }

        let mut granted_actions: Vec<String> = Vec::new();
        for a in &req.actions {
            let a_norm = a.trim().to_ascii_lowercase();
            if (allowed.iter().any(|x| *x == "*" || *x == a_norm))
                && !granted_actions.iter().any(|x| x == &a_norm)
            {
                granted_actions.push(a_norm);
            }
        }

        if !granted_actions.is_empty() {
            out.push(security::TokenScope {
                typ: req.typ.clone(),
                name: canonical_repo.to_string(),
                actions: granted_actions,
            });
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_grants_rejects_invalid_syntax() {
        let err = Grant::try_new("org/*", vec!["pull".to_string()])
            .expect_err("should reject arbitrary star in grant");
        match err {
            PolicyError::InvalidPattern(RbacPatternError::InvalidSyntax(p)) => {
                assert!(p.contains("org/*"))
            }
            other => panic!("unexpected error: {other:?}"),
        }

        let err_empty =
            Grant::try_new("", vec!["pull".to_string()]).expect_err("should reject empty");
        assert!(matches!(
            err_empty,
            PolicyError::InvalidPattern(RbacPatternError::Empty)
        ));
    }

    #[test]
    fn grant_scopes_is_subset_of_requested_and_policy() {
        let requested = vec![security::TokenScope {
            typ: "repository".to_string(),
            name: "org/repo".to_string(),
            actions: vec!["pull".to_string(), "push".to_string()],
        }];

        let grants = vec![Grant::try_new("org/", vec!["pull".to_string()]).unwrap()];

        let granted = grant_scopes_by_prefix(&requested, &grants);
        assert_eq!(
            granted,
            vec![security::TokenScope {
                typ: "repository".to_string(),
                name: "org/repo".to_string(),
                actions: vec!["pull".to_string()],
            }]
        );
    }

    #[test]
    fn grant_scopes_denies_by_default_when_no_grants_match() {
        let requested = vec![security::TokenScope {
            typ: "repository".to_string(),
            name: "org2/repo".to_string(),
            actions: vec!["pull".to_string()],
        }];

        let grants = vec![Grant::try_new("org/", vec!["pull".to_string()]).unwrap()];

        let granted = grant_scopes_by_prefix(&requested, &grants);
        assert!(granted.is_empty());
    }

    #[test]
    fn prefix_boundary_does_not_match_similar_prefixes_or_bare_org() {
        let grants =
            vec![Grant::try_new("org/", vec!["pull".to_string(), "push".to_string()]).unwrap()];

        let requested = vec![
            security::TokenScope {
                typ: "repository".to_string(),
                name: "org".to_string(),
                actions: vec!["pull".to_string()],
            },
            security::TokenScope {
                typ: "repository".to_string(),
                name: "org2/repo".to_string(),
                actions: vec!["pull".to_string()],
            },
        ];

        let granted = grant_scopes_by_prefix(&requested, &grants);
        assert!(granted.is_empty());
    }

    #[test]
    fn grant_scopes_preserves_requested_action_order() {
        let requested = vec![security::TokenScope {
            typ: "repository".to_string(),
            name: "org/repo".to_string(),
            actions: vec!["push".to_string(), "pull".to_string()],
        }];

        let grants =
            vec![Grant::try_new("org/", vec!["pull".to_string(), "push".to_string()]).unwrap()];

        let granted = grant_scopes_by_prefix(&requested, &grants);
        assert_eq!(
            granted[0].actions,
            vec!["push".to_string(), "pull".to_string()]
        );
    }

    #[test]
    fn validate_grants_allows_star_wildcard() {
        let ok =
            validate_grants(&[
                Grant::try_new("*", vec!["pull".to_string(), "push".to_string()]).unwrap(),
            ])
            .expect("should accept");

        assert_eq!(ok[0].repo_pattern, RbacRepoPattern::All);
    }

    #[test]
    fn wildcard_grant_matches_any_repository() {
        let requested = vec![security::TokenScope {
            typ: "repository".to_string(),
            name: "anyorg/anyrepo".to_string(),
            actions: vec!["pull".to_string(), "push".to_string()],
        }];

        let grants =
            vec![Grant::try_new("*", vec!["pull".to_string(), "push".to_string()]).unwrap()];

        let granted = grant_scopes_by_prefix(&requested, &grants);
        assert_eq!(granted, requested);
    }

    #[test]
    fn test_authorization_regression_boundaries() {
        let grants = vec![
            Grant::try_new("teams/core/", vec!["pull".to_string(), "push".to_string()]).unwrap(),
            Grant::try_new("teams/read-only/", vec!["pull".to_string()]).unwrap(),
        ];

        // 1. Valid nested matches
        let req_valid = vec![
            security::TokenScope {
                typ: "repository".to_string(),
                name: "teams/core/service-a".to_string(),
                actions: vec!["push".to_string()],
            },
            security::TokenScope {
                typ: "repository".to_string(),
                name: "teams/read-only/docs".to_string(),
                actions: vec!["pull".to_string(), "push".to_string()],
            },
        ];
        let granted = grant_scopes_by_prefix(&req_valid, &grants);
        assert_eq!(granted.len(), 2);
        assert_eq!(granted[0].name, "teams/core/service-a");
        assert_eq!(granted[0].actions, vec!["push".to_string()]);
        // push denied on read-only prefix
        assert_eq!(granted[1].name, "teams/read-only/docs");
        assert_eq!(granted[1].actions, vec!["pull".to_string()]);

        // 2. Prefix collision attempts fail closed
        let req_collisions = vec![
            security::TokenScope {
                typ: "repository".to_string(),
                name: "teams/core-extra/service".to_string(),
                actions: vec!["pull".to_string()],
            },
            security::TokenScope {
                typ: "repository".to_string(),
                name: "teams/core".to_string(),
                actions: vec!["pull".to_string()],
            },
        ];
        let granted_collisions = grant_scopes_by_prefix(&req_collisions, &grants);
        assert!(granted_collisions.is_empty());
    }
}
