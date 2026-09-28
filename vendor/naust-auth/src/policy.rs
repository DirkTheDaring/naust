use crate::rbac::{self, Grant};
use crate::robot_secrets;
use crate::security::{self, TokenSigningKey};
use naust_types::access_pattern::push_repository_allowed;
use naust_types::canonical_name::CanonicalRepoName;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum AuthStrategy {
    #[default]
    Token,
    Basic,
    Both,
}

#[derive(Clone, Debug, Default)]
pub struct RobotsConfig {
    pub enabled: bool,
    pub accounts: Vec<RobotAccountConfig>,
}

#[derive(Clone, Debug, Default)]
pub struct UsersConfig {
    pub enabled: bool,
    pub accounts: Vec<UserAccountConfig>,
    pub groups: Vec<GroupConfig>,
}

#[derive(Clone, Debug)]
pub struct UserAccountConfig {
    pub name: String,
    pub secret_hash: String,
    pub groups: Vec<String>,
    pub max_ttl_secs: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct GroupConfig {
    pub name: String,
    pub grants: Vec<Grant>,
}

#[derive(Clone, Debug)]
pub struct RobotAccountConfig {
    pub name: String,
    pub secret_hash: String,
    pub grants: Vec<Grant>,
    pub max_ttl_secs: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct AuthConfig {
    pub auth_strategy: AuthStrategy,
    pub anonymous_pull: bool,
    pub catalog_requires_auth: bool,
    pub star_grants_catalog: bool,
    pub push_username: Option<String>,
    pub push_password: Option<String>,
    pub push_actions: Vec<String>,
    pub push_implies_delete: bool,
    pub push_allow_repos: Option<Vec<naust_types::RepositoryAccessPattern>>,
    pub robots: RobotsConfig,
    pub users: UsersConfig,
    pub token_service: String,
    pub token_signing_keys: Vec<TokenSigningKey>,
    pub token_ttl_secs: u64,
    pub private_name_prefixes: Vec<String>,
}

impl AuthConfig {
    pub fn is_repo_private_with_prefixes(
        anonymous_pull: bool,
        private_name_prefixes: &[String],
        repo: &str,
    ) -> bool {
        if !anonymous_pull {
            return true;
        }
        let raw = repo.trim_start_matches('/');
        let norm = raw.to_ascii_lowercase();
        let r = norm.strip_prefix("library/").unwrap_or(&norm);
        private_name_prefixes
            .iter()
            .any(|p| repo_matches_private_prefix(r, p))
            || r.contains('<')
            || r.contains('>')
            || r.contains("%3c")
            || r.contains("%3e")
    }

    pub fn is_repo_private(&self, repo: &str) -> bool {
        Self::is_repo_private_with_prefixes(
            self.anonymous_pull,
            &self.private_name_prefixes,
            repo,
        )
    }

    pub fn catalog_auth_required(&self) -> bool {
        self.catalog_requires_auth
            || !self.anonymous_pull
            || self.push_username.is_some()
            || self.users.enabled
            || self.robots.enabled
    }
}

pub fn repo_matches_private_prefix(repo: &str, prefix: &str) -> bool {
    let prefix = prefix.trim().trim_matches('/').to_ascii_lowercase();
    if prefix.is_empty() {
        return false;
    }
    repo == prefix || repo.starts_with(&format!("{prefix}/"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CatalogAccess {
    Full,
    PublicOnly,
    Denied,
}

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

pub fn configured_secrets_match(
    user: &str,
    pass: &str,
    expected_user: &str,
    expected_pass: &str,
) -> bool {
    let user_ok = security::constant_time_eq(user, expected_user);
    let pass_ok = security::constant_time_eq(pass, expected_pass);
    user_ok && pass_ok
}

pub fn verify_any_basic_credentials(cfg: &AuthConfig, user: &str, pass: &str) -> bool {
    if cfg.robots.enabled
        && let Some(account) = cfg.robots.accounts.iter().find(|a| a.name == user)
    {
        return robot_secrets::verify_robot_secret(pass, &account.secret_hash);
    }
    if cfg.users.enabled
        && let Some(account) = cfg.users.accounts.iter().find(|a| a.name == user)
    {
        return robot_secrets::verify_robot_secret(pass, &account.secret_hash);
    }
    if let (Some(expected_user), Some(expected_pass)) =
        (cfg.push_username.as_deref(), cfg.push_password.as_deref())
        && configured_secrets_match(user, pass, expected_user, expected_pass)
    {
        return true;
    }
    if cfg.robots.enabled || cfg.users.enabled {
        let _ = robot_secrets::verify_robot_secret(
            pass,
            robot_secrets::DUMMY_SENTINEL_HASH,
        );
    }
    false
}

pub fn basic_allows_catalog(cfg: &AuthConfig, user: &str, pass: &str) -> bool {
    let requested = [security::TokenScope {
        typ: "registry".to_string(),
        name: "catalog".to_string(),
        actions: vec!["*".to_string()],
    }];

    if cfg.robots.enabled
        && let Some(account) = cfg.robots.accounts.iter().find(|a| a.name == user)
    {
        if robot_secrets::verify_robot_secret(pass, &account.secret_hash) {
            return !rbac::grant_scopes_by_prefix_with_options(
                &requested,
                &account.grants,
                cfg.star_grants_catalog,
            )
            .is_empty();
        }
        return false;
    }

    if cfg.users.enabled
        && let Some(account) = cfg.users.accounts.iter().find(|a| a.name == user)
    {
        if robot_secrets::verify_robot_secret(pass, &account.secret_hash) {
            let mut union_grants: Vec<Grant> = Vec::new();
            for group_name in &account.groups {
                if let Some(group) = cfg.users.groups.iter().find(|g| g.name == *group_name) {
                    union_grants.extend(group.grants.clone());
                }
            }
            return !rbac::grant_scopes_by_prefix_with_options(
                &requested,
                &union_grants,
                cfg.star_grants_catalog,
            )
            .is_empty();
        }
        return false;
    }

    if let (Some(expected_user), Some(expected_pass)) =
        (cfg.push_username.as_deref(), cfg.push_password.as_deref())
        && configured_secrets_match(user, pass, expected_user, expected_pass)
    {
        return cfg.push_implies_delete
            || cfg
                .push_actions
                .iter()
                .any(|a| a == "*" || a == "pull" || a == "push");
    }

    if cfg.robots.enabled || cfg.users.enabled {
        let _ = robot_secrets::verify_robot_secret(
            pass,
            robot_secrets::DUMMY_SENTINEL_HASH,
        );
    }
    false
}

pub fn verify_direct_basic_access(
    cfg: &AuthConfig,
    user: &str,
    pass: &str,
    repo: &CanonicalRepoName,
    action: &str,
) -> bool {
    let token_scopes = [security::TokenScope {
        typ: "repository".to_string(),
        name: repo.as_str().to_string(),
        actions: vec![action.to_string()],
    }];

    // 1. Try Robots
    if cfg.robots.enabled
        && let Some(account) = cfg.robots.accounts.iter().find(|a| a.name == user)
    {
        if robot_secrets::verify_robot_secret(pass, &account.secret_hash) {
            let granted = rbac::grant_scopes_by_prefix(&token_scopes, &account.grants);
            return !granted.is_empty();
        }
        return false;
    }

    // 2. Try Users
    if cfg.users.enabled
        && let Some(account) = cfg.users.accounts.iter().find(|a| a.name == user)
    {
        if robot_secrets::verify_robot_secret(pass, &account.secret_hash) {
            let mut union_grants: Vec<Grant> = Vec::new();
            for group_name in &account.groups {
                if let Some(group) = cfg.users.groups.iter().find(|g| g.name == *group_name) {
                    union_grants.extend(group.grants.clone());
                }
            }
            let granted = rbac::grant_scopes_by_prefix(&token_scopes, &union_grants);
            return !granted.is_empty();
        }
        return false;
    }

    // 3. Fallback to global basic auth
    if let (Some(expected_user), Some(expected_pass)) =
        (cfg.push_username.as_deref(), cfg.push_password.as_deref())
        && configured_secrets_match(user, pass, expected_user, expected_pass)
    {
        let action_norm = action.to_ascii_lowercase();
        let action_allowed = if cfg.push_implies_delete {
            action_norm == "pull" || action_norm == "push" || action_norm == "delete"
        } else {
            cfg.push_actions
                .iter()
                .any(|a| a == "*" || a.eq_ignore_ascii_case(&action_norm))
        };
        if !action_allowed {
            return false;
        }
        if let Some(allowlist) = cfg.push_allow_repos.as_deref() {
            return push_repository_allowed(allowlist, repo);
        }
        return true;
    }

    if cfg.robots.enabled || cfg.users.enabled {
        let _ = robot_secrets::verify_robot_secret(
            pass,
            robot_secrets::DUMMY_SENTINEL_HASH,
        );
    }
    false
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
            let mut seen = std::collections::HashSet::new();
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
    cfg: &AuthConfig,
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
        || (wants_catalog && cfg.catalog_auth_required())
}

fn scope_name_is_expansive(name: &str) -> bool {
    name.trim().trim_start_matches('/').contains('*')
}

fn scope_is_registry_catalog(scope: &security::TokenScope) -> bool {
    scope.typ == "registry" && (scope.name == "catalog" || scope.name == "*")
}

fn anonymous_scope_may_be_issued(
    cfg: &AuthConfig,
    scope: &security::TokenScope,
) -> bool {
    if scope_name_is_expansive(&scope.name) || scope_is_registry_catalog(scope) {
        return false;
    }
    let is_repo = scope.typ == "repository" || scope.typ == "repo" || scope.typ == "image";
    !(is_repo && cfg.is_repo_private(&scope.name))
}

pub fn decide_token_scopes_for_request(
    cfg: &AuthConfig,
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
        && robot_secrets::verify_robot_secret(pass, &account.secret_hash)
    {
        let granted = if token_scopes.is_empty() {
            Vec::new()
        } else {
            rbac::grant_scopes_by_prefix_with_options(
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
        && robot_secrets::verify_robot_secret(pass, &account.secret_hash)
    {
        let mut union_grants: Vec<Grant> = Vec::new();
        for group_name in &account.groups {
            if let Some(group) = cfg.users.groups.iter().find(|g| g.name == *group_name) {
                union_grants.extend(group.grants.clone());
            }
        }

        let granted = if token_scopes.is_empty() {
            Vec::new()
        } else {
            rbac::grant_scopes_by_prefix_with_options(
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
    if !configured_secrets_match(&user, &pass, expected_user, expected_pass) {
        return Err(TokenRejection::Unauthorized);
    }

    if token_scopes.is_empty() {
        return Ok(TokenDecision {
            subject: Some(expected_user.to_string()),
            scopes: Vec::new(),
            ttl_secs: cfg.token_ttl_secs,
        });
    }

    let mut granted: Vec<security::TokenScope> = Vec::new();
    for scope in token_scopes {
        if scope.typ == "registry" && (scope.name == "catalog" || scope.name == "*") {
            if cfg.push_implies_delete
                || cfg
                    .push_actions
                    .iter()
                    .any(|a| a == "*" || a == "pull" || a == "push")
            {
                granted.push(scope.clone());
            }
            continue;
        }

        if scope.typ != "repository" {
            continue;
        }

        let Ok(canonical_repo) = CanonicalRepoName::parse(scope.name.trim()) else {
            continue;
        };

        if let Some(allowlist) = cfg.push_allow_repos.as_deref()
            && !push_repository_allowed(allowlist, &canonical_repo)
        {
            return Err(TokenRejection::Denied(
                "push not allowed for requested repository",
            ));
        }

        let mut allowed_actions: Vec<String> = Vec::new();
        for action in &scope.actions {
            let action_norm = action.to_ascii_lowercase();
            let action_allowed = if cfg.push_implies_delete {
                action_norm == "pull" || action_norm == "push" || action_norm == "delete"
            } else {
                cfg.push_actions
                    .iter()
                    .any(|a| a == "*" || a.eq_ignore_ascii_case(&action_norm))
            };
            if action_allowed && !allowed_actions.contains(&action_norm) {
                allowed_actions.push(action_norm);
            }
        }

        if allowed_actions.is_empty() {
            return Err(TokenRejection::Denied("action not allowed by push policy"));
        }

        granted.push(security::TokenScope {
            typ: scope.typ.clone(),
            name: canonical_repo.to_string(),
            actions: allowed_actions,
        });
    }

    Ok(TokenDecision {
        subject: Some(expected_user.to_string()),
        scopes: granted,
        ttl_secs: cfg.token_ttl_secs,
    })
}

pub fn service_param_is_valid(param: Option<&str>, configured: &str) -> bool {
    match param {
        None => true,
        Some(p) => p == configured,
    }
}

