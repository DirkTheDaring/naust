use crate::{config, rbac};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Default)]
pub struct AuditReport {
    pub warnings: Vec<String>,
}

fn merge_grants(grants: &[rbac::Grant]) -> BTreeMap<String, BTreeSet<String>> {
    let mut out: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for g in grants {
        let prefix = g.repo_prefix.trim().to_string();
        if prefix.is_empty() {
            continue;
        }
        let entry = out.entry(prefix).or_default();
        for a in &g.actions {
            let a = a.trim().to_ascii_lowercase();
            if !a.is_empty() {
                entry.insert(a);
            }
        }
    }
    out
}

fn format_merged_grants(grants: &BTreeMap<String, BTreeSet<String>>) -> Vec<String> {
    grants
        .iter()
        .map(|(prefix, actions)| {
            let actions = actions.iter().cloned().collect::<Vec<_>>().join(",");
            format!("{prefix} -> [{actions}]")
        })
        .collect()
}

fn union_group_grants(
    users: &config::UsersConfig,
    user: &config::UserAccountConfig,
) -> Vec<rbac::Grant> {
    let mut out: Vec<rbac::Grant> = Vec::new();
    for group_name in &user.groups {
        if let Some(g) = users.groups.iter().find(|g| g.name == *group_name) {
            out.extend(g.grants.clone());
        }
    }
    out
}

pub fn print_audit(cfg: &config::Config) -> AuditReport {
    let mut report = AuditReport::default();

    println!("Access audit / effective permissions");
    println!("service: {}", cfg.token_service);
    println!("token_ttl_secs: {}", cfg.token_ttl_secs);
    println!();

    // Name collisions: robots are checked before users.
    if cfg.robots.enabled && cfg.users.enabled {
        let mut collisions: Vec<String> = Vec::new();
        for r in &cfg.robots.accounts {
            if cfg.users.accounts.iter().any(|u| u.name == r.name) {
                collisions.push(r.name.clone());
            }
        }
        collisions.sort();
        collisions.dedup();
        if !collisions.is_empty() {
            report.warnings.push(format!(
                "name collision(s) between robots and users: {} (robots take precedence on /token)",
                collisions.join(", ")
            ));
        }
    }

    // Robots.
    if cfg.robots.enabled {
        let mut robots = cfg.robots.accounts.clone();
        robots.sort_by(|a, b| a.name.cmp(&b.name));

        println!("robots:");
        if robots.is_empty() {
            println!("  (none)");
            report
                .warnings
                .push("robots.enabled=true but no robot accounts configured".to_string());
        }

        for r in robots {
            let ttl = r
                .max_ttl_secs
                .map(|v| v.to_string())
                .unwrap_or_else(|| "(default)".to_string());
            println!("  - robot:{} (max_ttl_secs: {})", r.name, ttl);

            match rbac::validate_grants(&r.grants) {
                Ok(valid) => {
                    let merged = merge_grants(&valid);
                    if merged.is_empty() {
                        println!("      grants: (none)");
                        report
                            .warnings
                            .push(format!("robot:{} has no effective grants", r.name));
                    } else {
                        println!("      grants:");
                        for line in format_merged_grants(&merged) {
                            println!("        - {line}");
                        }
                    }
                }
                Err(e) => {
                    println!("      grants: (invalid)");
                    report
                        .warnings
                        .push(format!("robot:{} has invalid grants: {:?}", r.name, e));
                }
            }
        }
        println!();
    } else {
        println!("robots: (disabled)");
        println!();
    }

    // Users + groups.
    if cfg.users.enabled {
        let mut group_names: Vec<String> =
            cfg.users.groups.iter().map(|g| g.name.clone()).collect();
        group_names.sort();
        group_names.dedup();

        println!("users/groups:");
        if cfg.users.accounts.is_empty() {
            println!("  users: (none)");
            report
                .warnings
                .push("auth.users.enabled=true but no user accounts configured".to_string());
        }

        if cfg.users.groups.is_empty() {
            println!("  groups: (none)");
            report
                .warnings
                .push("auth.users.enabled=true but no groups configured".to_string());
        }

        // Unused groups.
        if !cfg.users.groups.is_empty() {
            let mut referenced: BTreeSet<String> = BTreeSet::new();
            for u in &cfg.users.accounts {
                for g in &u.groups {
                    referenced.insert(g.clone());
                }
            }
            for g in &group_names {
                if !referenced.contains(g) {
                    report.warnings.push(format!(
                        "group '{g}' is defined but not referenced by any user"
                    ));
                }
            }
        }

        // Print groups.
        if !cfg.users.groups.is_empty() {
            println!("  groups:");
            let mut groups = cfg.users.groups.clone();
            groups.sort_by(|a, b| a.name.cmp(&b.name));
            for g in groups {
                println!("    - {}", g.name);
                let merged = merge_grants(&g.grants);
                if merged.is_empty() {
                    println!("        grants: (none)");
                    report
                        .warnings
                        .push(format!("group '{}' has no effective grants", g.name));
                } else {
                    println!("        grants:");
                    for line in format_merged_grants(&merged) {
                        println!("          - {line}");
                    }
                }
            }
        }

        // Print users and effective grants.
        println!("  users:");
        let mut users = cfg.users.accounts.clone();
        users.sort_by(|a, b| a.name.cmp(&b.name));
        for u in users {
            let ttl = u
                .max_ttl_secs
                .map(|v| v.to_string())
                .unwrap_or_else(|| "(default)".to_string());
            println!("    - user:{} (max_ttl_secs: {})", u.name, ttl);
            if u.groups.is_empty() {
                println!("        groups: (none)");
                report.warnings.push(format!(
                    "user:{} has no groups; will be denied for push",
                    u.name
                ));
            } else {
                println!("        groups: {}", u.groups.join(", "));
            }

            let effective = union_group_grants(&cfg.users, &u);
            let merged = merge_grants(&effective);
            if merged.is_empty() {
                println!("        effective_grants: (none)");
                report
                    .warnings
                    .push(format!("user:{} has no effective grants", u.name));
            } else {
                println!("        effective_grants:");
                for line in format_merged_grants(&merged) {
                    println!("          - {line}");
                }
            }
        }

        println!();
    } else {
        println!("users/groups: (disabled)");
        println!();
    }

    if !report.warnings.is_empty() {
        println!("warnings:");
        for w in &report.warnings {
            println!("  - {w}");
        }
    }

    report
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_grants_unions_actions_per_prefix() {
        let grants = vec![
            rbac::Grant {
                repo_prefix: "org/".to_string(),
                actions: vec!["pull".to_string()],
            },
            rbac::Grant {
                repo_prefix: "org/".to_string(),
                actions: vec!["push".to_string(), "pull".to_string()],
            },
        ];

        let merged = merge_grants(&grants);
        assert_eq!(
            merged
                .get("org/")
                .expect("prefix")
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
            vec!["pull".to_string(), "push".to_string()]
        );
    }
}
