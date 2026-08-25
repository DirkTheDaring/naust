use crate::security;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Grant {
    pub repo_prefix: String,
    pub actions: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PolicyError {
    EmptyRepoPrefix,
    RepoPrefixMustEndWithSlash(String),
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
        let prefix = g.repo_prefix.trim().to_string();
        if prefix.is_empty() {
            return Err(PolicyError::EmptyRepoPrefix);
        }
        // Special-case: allow '*' as an explicit "match all repositories" grant.
        if prefix != "*" && !prefix.ends_with('/') {
            return Err(PolicyError::RepoPrefixMustEndWithSlash(prefix));
        }

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
            repo_prefix: prefix,
            actions,
        });
    }
    Ok(out)
}

/// Evaluates whether a validated repository grant prefix authorizes access to a repository.
///
/// Security Invariants:
/// - `grant_prefix == "*"` authorizes any valid non-empty repository name.
/// - If `grant_prefix` ends with `/` (e.g. `"org/"`), it matches `"org/app"` and `"org/sub/app"`,
///   but NEVER matches `"org2/app"` (prefix boundary leakage) or bare `"org"`.
/// - Empty repositories or empty grants fail closed (return `false`).
/// - Exact repository grants match `grant_prefix == repo`.
pub fn matches_repo_grant(grant_prefix: &str, repo: &str) -> bool {
    let repo = repo.trim();
    let grant = grant_prefix.trim();
    if repo.is_empty() || grant.is_empty() {
        return false;
    }
    if grant == "*" {
        return true;
    }
    if grant.ends_with('/') {
        return repo.starts_with(grant);
    }
    grant == repo
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
            let has_catalog = grants.iter().any(|g| g.repo_prefix == "*");
            if has_catalog {
                out.push(req.clone());
            }
            continue;
        }
        if req.typ != "repository" {
            continue;
        }
        let repo = req.name.trim();
        if repo.is_empty() {
            continue;
        }

        let mut allowed: Vec<&str> = Vec::new();
        for g in &grants {
            if matches_repo_grant(&g.repo_prefix, repo) {
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
                name: repo.to_string(),
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
    fn validate_grants_requires_trailing_slash() {
        let err = validate_grants(&[Grant {
            repo_prefix: "org".to_string(),
            actions: vec!["pull".to_string()],
        }])
        .expect_err("should reject");

        match err {
            PolicyError::RepoPrefixMustEndWithSlash(p) => assert_eq!(p, "org"),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn grant_scopes_is_subset_of_requested_and_policy() {
        let requested = vec![security::TokenScope {
            typ: "repository".to_string(),
            name: "org/repo".to_string(),
            actions: vec!["pull".to_string(), "push".to_string()],
        }];

        let grants = vec![Grant {
            repo_prefix: "org/".to_string(),
            actions: vec!["pull".to_string()],
        }];

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

        let grants = vec![Grant {
            repo_prefix: "org/".to_string(),
            actions: vec!["pull".to_string()],
        }];

        let granted = grant_scopes_by_prefix(&requested, &grants);
        assert!(granted.is_empty());
    }

    #[test]
    fn prefix_boundary_does_not_match_similar_prefixes_or_bare_org() {
        let grants = vec![Grant {
            repo_prefix: "org/".to_string(),
            actions: vec!["pull".to_string(), "push".to_string()],
        }];

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

        let grants = vec![Grant {
            repo_prefix: "org/".to_string(),
            actions: vec!["pull".to_string(), "push".to_string()],
        }];

        let granted = grant_scopes_by_prefix(&requested, &grants);
        assert_eq!(
            granted[0].actions,
            vec!["push".to_string(), "pull".to_string()]
        );
    }

    #[test]
    fn validate_grants_allows_star_wildcard() {
        let ok = validate_grants(&[Grant {
            repo_prefix: "*".to_string(),
            actions: vec!["pull".to_string(), "push".to_string()],
        }])
        .expect("should accept");

        assert_eq!(ok[0].repo_prefix, "*");
    }

    #[test]
    fn wildcard_grant_matches_any_repository() {
        let requested = vec![security::TokenScope {
            typ: "repository".to_string(),
            name: "anyorg/anyrepo".to_string(),
            actions: vec!["pull".to_string(), "push".to_string()],
        }];

        let grants = vec![Grant {
            repo_prefix: "*".to_string(),
            actions: vec!["pull".to_string(), "push".to_string()],
        }];

        let granted = grant_scopes_by_prefix(&requested, &grants);
        assert_eq!(granted, requested);
    }

    #[test]
    fn test_matcher_truth_table_differential_analysis() {
        use crate::auth::legacy_repo_allowed;
        use crate::glob::wildcard_match;

        struct TruthRow {
            pattern: &'static str,
            value: &'static str,
            expected_glob: bool,
            expected_rbac: bool,
            expected_legacy: bool,
        }

        let rows = [
            // Exact match
            TruthRow {
                pattern: "org/app",
                value: "org/app",
                expected_glob: true,
                expected_rbac: true, // exact repo match
                expected_legacy: true,
            },
            TruthRow {
                pattern: "org/app",
                value: "org/other",
                expected_glob: false,
                expected_rbac: false,
                expected_legacy: false,
            },
            // Global '*'
            TruthRow {
                pattern: "*",
                value: "org/app",
                expected_glob: true,
                expected_rbac: true,
                expected_legacy: true,
            },
            TruthRow {
                pattern: "*",
                value: "",
                expected_glob: true,
                expected_rbac: false,   // empty repo fails closed
                expected_legacy: false, // empty repo fails closed for authorization
            },
            // Prefix boundary 'org/'
            TruthRow {
                pattern: "org/",
                value: "org/app",
                expected_glob: false,   // glob is exact
                expected_rbac: true,    // RBAC prefix match
                expected_legacy: false, // legacy uses 'org/*'
            },
            TruthRow {
                pattern: "org/",
                value: "org/sub/app",
                expected_glob: false,
                expected_rbac: true,
                expected_legacy: false,
            },
            TruthRow {
                pattern: "org/",
                value: "org",
                expected_glob: false,
                expected_rbac: false, // bare base repo not matched by prefix
                expected_legacy: false,
            },
            TruthRow {
                pattern: "org/",
                value: "org2/app",
                expected_glob: false,
                expected_rbac: false, // prefix boundary enforced
                expected_legacy: false,
            },
            // Legacy wildcard 'org/*'
            TruthRow {
                pattern: "org/*",
                value: "org/app",
                expected_glob: true,
                expected_rbac: false, // RBAC grants use 'org/', not 'org/*'
                expected_legacy: true,
            },
            TruthRow {
                pattern: "org/*",
                value: "org/sub/app",
                expected_glob: true,
                expected_rbac: false,
                expected_legacy: true,
            },
            TruthRow {
                pattern: "org/*",
                value: "org",
                expected_glob: false,
                expected_rbac: false,
                expected_legacy: true, // legacy compatibility feature: org/* covers org
            },
            TruthRow {
                pattern: "org/*",
                value: "org2/app",
                expected_glob: false,
                expected_rbac: false,
                expected_legacy: false,
            },
            // Suffix and arbitrary glob
            TruthRow {
                pattern: "*app",
                value: "my-app",
                expected_glob: true,
                expected_rbac: false,
                expected_legacy: false,
            },
            TruthRow {
                pattern: "a*b*c",
                value: "a1b2c",
                expected_glob: true,
                expected_rbac: false,
                expected_legacy: false,
            },
            // Empty pattern
            TruthRow {
                pattern: "",
                value: "",
                expected_glob: true,
                expected_rbac: false,
                expected_legacy: false,
            },
            TruthRow {
                pattern: "",
                value: "org/app",
                expected_glob: false,
                expected_rbac: false,
                expected_legacy: false,
            },
        ];

        for row in rows {
            let actual_glob = wildcard_match(row.pattern, row.value);
            let actual_rbac = matches_repo_grant(row.pattern, row.value);
            let actual_legacy = legacy_repo_allowed(&[row.pattern.to_string()], row.value);

            assert_eq!(
                actual_glob, row.expected_glob,
                "Glob mismatch for pattern='{}' value='{}'",
                row.pattern, row.value
            );
            assert_eq!(
                actual_rbac, row.expected_rbac,
                "RBAC mismatch for pattern='{}' value='{}'",
                row.pattern, row.value
            );
            assert_eq!(
                actual_legacy, row.expected_legacy,
                "Legacy mismatch for pattern='{}' value='{}'",
                row.pattern, row.value
            );
        }
    }

    #[test]
    fn test_authorization_regression_boundaries() {
        let grants = vec![
            Grant {
                repo_prefix: "teams/core/".to_string(),
                actions: vec!["pull".to_string(), "push".to_string()],
            },
            Grant {
                repo_prefix: "teams/read-only/".to_string(),
                actions: vec!["pull".to_string()],
            },
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
