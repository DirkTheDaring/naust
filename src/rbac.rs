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
        // Reserved for future use. Not currently supported by the registry handlers.
        // Keeping it out of the allowlist ensures we never mint unexpected permissions.
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
        if !prefix.ends_with('/') {
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
        if req.typ != "repository" {
            continue;
        }
        let repo = req.name.trim();
        if repo.is_empty() {
            continue;
        }

        let mut allowed: Vec<&str> = Vec::new();
        for g in &grants {
            if repo.starts_with(&g.repo_prefix) {
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
            if allowed.iter().any(|x| *x == a_norm) && !granted_actions.iter().any(|x| x == &a_norm)
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
}
