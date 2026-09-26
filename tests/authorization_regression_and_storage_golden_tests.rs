//! Comprehensive authorization regression, proxy routing, and storage golden addressing tests.
//!
//! Verifies:
//! 1. Byte-for-byte storage addressing equality for FS and S3 across legacy and complex canonical repository names.
//! 2. Storage codec error classification (`RepoKeyDecodeError`) distinguishing malformed encoding, non-UTF8, invalid grammar, and schema version.
//! 3. Segment-aware authorization with `RepositoryAccessPattern` and `RbacRepoPattern` (preventing prefix confusion).
//! 4. Typed Proxy routing patterns (`ProxyHostPattern`, `ProxyRepoPattern`, `ProxyAllowedPrefix`) and rule precedence.
//! 5. Push-allowlist parity across Basic Auth, Token Issuance, and Middleware.
//! 6. Fail-closed rejection of invalid patterns at configuration startup.

use std::path::Path;

use naust::config::{Config, ConfigError};
use naust::proxy::{ProxyAllowedPrefix, ProxyHostPattern, ProxyRepoPattern};
use naust::rbac::{Grant, RbacRepoPattern};
use naust::registry::access_pattern::RepositoryAccessPattern;
use naust::registry::canonical_name::CanonicalRepoName;
use naust::registry::digest::Digest;
use naust::storage::repo_membership::RepoKeyDecodeError;
use naust::test_support::{
    canonical_all_memberships_prefix, canonical_repo_membership_prefix,
    canonical_repo_membership_relpath, decode_canonical_repo_key, encode_canonical_repo_key,
    fs_repo_dir, push_repository_allowed, s3_repo_prefix,
};

// ================================================================================================
// 1. STORAGE ADDRESSING GOLDEN TESTS
// ================================================================================================

#[test]
fn test_golden_fs_storage_addressing() {
    let base_root = Path::new("/var/lib/registry");

    let cases = [
        // Standard legacy repos
        ("alpine", "/var/lib/registry/repos/alpine"),
        ("library/ubuntu", "/var/lib/registry/repos/library/ubuntu"),
        (
            "team/project/app",
            "/var/lib/registry/repos/team/project/app",
        ),
        // Newly valid OCI grammar repos with double underscore, double dash, dots
        (
            "team/image__cache",
            "/var/lib/registry/repos/team/image__cache",
        ),
        (
            "team/image--service",
            "/var/lib/registry/repos/team/image--service",
        ),
        (
            "org/app---backend",
            "/var/lib/registry/repos/org/app---backend",
        ),
        (
            "my.domain.com/org/sub.service",
            "/var/lib/registry/repos/my.domain.com/org/sub.service",
        ),
    ];

    for (name, expected_fs_dir) in cases {
        let repo = CanonicalRepoName::parse(name).expect("valid canonical repo name");
        let computed = fs_repo_dir(base_root, &repo).expect("fs_repo_dir succeeds");
        assert_eq!(
            computed.to_str().unwrap(),
            expected_fs_dir,
            "FS repo dir addressing mismatch for '{name}'"
        );

        // Verify subpath address constructions
        let manifest_path = computed.join("manifests").join("sha256").join("abcd1234");
        let tag_path = computed.join("tags").join("v1.0.0");
        let journal_path = computed.join("meta").join("lifecycle_journal.json");
        let lock_path = computed.join(".repo_lock");
        let referrers_path = computed
            .join("referrers")
            .join("sha256")
            .join("abcd1234.json");

        assert!(manifest_path.starts_with(base_root));
        assert!(tag_path.starts_with(base_root));
        assert!(journal_path.starts_with(base_root));
        assert!(lock_path.starts_with(base_root));
        assert!(referrers_path.starts_with(base_root));
    }
}

#[test]
fn test_golden_s3_storage_addressing() {
    let root_prefixes = ["", "root", "live-test/prefix"];

    let cases = [
        ("alpine", "repos/alpine/"),
        ("library/ubuntu", "repos/library/ubuntu/"),
        ("team/project/app", "repos/team/project/app/"),
        ("team/image__cache", "repos/team/image__cache/"),
        ("team/image--service", "repos/team/image--service/"),
        ("org/app---backend", "repos/org/app---backend/"),
    ];

    for root in root_prefixes {
        for (name, expected_rel) in cases {
            let repo = CanonicalRepoName::parse(name).expect("valid canonical repo name");
            let computed = s3_repo_prefix(root, &repo);

            let expected = if root.is_empty() {
                expected_rel.to_string()
            } else {
                format!("{root}/{expected_rel}")
            };

            assert_eq!(
                computed, expected,
                "S3 repo prefix mismatch for root='{root}', repo='{name}'"
            );

            assert!(!computed.contains("//"));
            assert!(!computed.contains(".."));
            assert!(computed.ends_with('/'));
        }
    }
}

#[test]
fn test_golden_repo_membership_keys() {
    let digest =
        Digest::parse("sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
            .unwrap();

    let cases = [
        "alpine",
        "library/ubuntu",
        "team/project/app",
        "team/image__cache",
        "team/image--service",
        "org/app---backend",
    ];

    for name in cases {
        let repo = CanonicalRepoName::parse(name).unwrap();
        let encoded_key = encode_canonical_repo_key(&repo);
        let decoded = decode_canonical_repo_key(&encoded_key).expect("decode succeeds");
        assert_eq!(decoded.as_str(), name);

        let relpath = canonical_repo_membership_relpath(&repo, &digest);
        assert!(relpath.starts_with("repo-memberships/by-repo/"));
        assert!(relpath.contains(&encoded_key));

        let prefix = canonical_repo_membership_prefix(&repo);
        assert!(prefix.starts_with("repo-memberships/by-repo/"));
        assert!(prefix.ends_with('/'));
    }

    assert_eq!(
        canonical_all_memberships_prefix(),
        "repo-memberships/by-repo/"
    );
}

// ================================================================================================
// 2. STORAGE CODEC & FAIL-CLOSED CORRUPTION TESTS
// ================================================================================================

#[test]
fn test_storage_codec_typed_error_classification() {
    // 1. Valid roundtrip
    let valid_repo = CanonicalRepoName::parse("library/ubuntu").unwrap();
    let encoded = encode_canonical_repo_key(&valid_repo);
    assert_eq!(decode_canonical_repo_key(&encoded).unwrap(), valid_repo);

    // 2. Malformed Base64 / non-URL-safe
    let malformed_err = decode_canonical_repo_key("not-valid-base64!@#$%^").unwrap_err();
    assert!(matches!(
        malformed_err,
        RepoKeyDecodeError::MalformedEncoding(_)
    ));

    // 3. Non-UTF8 payload
    use base64::Engine;
    let non_utf8_bytes = vec![0xFF, 0xFE, 0xFD];
    let non_utf8_encoded = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&non_utf8_bytes);
    let non_utf8_err = decode_canonical_repo_key(&non_utf8_encoded).unwrap_err();
    assert!(matches!(non_utf8_err, RepoKeyDecodeError::NonUtf8));

    // 4. Grammar violation in decoded string (e.g. UPPERCASE, path traversal, empty)
    let invalid_grammar_encoded =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"INVALID/UPPERCASE");
    let grammar_err = decode_canonical_repo_key(&invalid_grammar_encoded).unwrap_err();
    assert!(matches!(
        grammar_err,
        RepoKeyDecodeError::InvalidRepoName(_)
    ));

    let traversal_encoded =
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b"../../etc/passwd");
    let traversal_err = decode_canonical_repo_key(&traversal_encoded).unwrap_err();
    assert!(matches!(
        traversal_err,
        RepoKeyDecodeError::InvalidRepoName(_)
    ));
}

// ================================================================================================
// 3. RBAC GRANT PATTERN TESTS
// ================================================================================================

#[test]
fn test_rbac_repo_pattern_segment_matching_and_isolation() {
    // Pattern: Namespace("team") from wire "team/"
    let ns_pat = RbacRepoPattern::parse("team/").unwrap();
    assert_eq!(ns_pat.to_string(), "team/");

    // Must match subtrees
    assert!(ns_pat.matches(&CanonicalRepoName::parse("team/app").unwrap()));
    assert!(ns_pat.matches(&CanonicalRepoName::parse("team/sub/app").unwrap()));

    // MUST NOT match bare "team" (since wire was "team/")
    assert!(!ns_pat.matches(&CanonicalRepoName::parse("team").unwrap()));

    // MUST NOT match prefix siblings (preventing prefix confusion)
    assert!(!ns_pat.matches(&CanonicalRepoName::parse("team-secret").unwrap()));
    assert!(!ns_pat.matches(&CanonicalRepoName::parse("team-secret/app").unwrap()));
    assert!(!ns_pat.matches(&CanonicalRepoName::parse("team_secret/app").unwrap()));
    assert!(!ns_pat.matches(&CanonicalRepoName::parse("team.secret/app").unwrap()));

    // Pattern: Exact("team/app")
    let exact_pat = RbacRepoPattern::parse("team/app").unwrap();
    assert!(exact_pat.matches(&CanonicalRepoName::parse("team/app").unwrap()));
    assert!(!exact_pat.matches(&CanonicalRepoName::parse("team/app/nested").unwrap()));
    assert!(!exact_pat.matches(&CanonicalRepoName::parse("team/other").unwrap()));

    // Pattern: All ("*")
    let all_pat = RbacRepoPattern::parse("*").unwrap();
    assert!(all_pat.matches(&CanonicalRepoName::parse("anything").unwrap()));
    assert!(all_pat.matches(&CanonicalRepoName::parse("team/app").unwrap()));

    // Grant evaluation
    let grant = Grant::try_new("team/", vec!["pull".to_string()]).unwrap();
    assert!(grant.allows(&CanonicalRepoName::parse("team/app").unwrap(), "pull"));
    assert!(!grant.allows(&CanonicalRepoName::parse("team/app").unwrap(), "push"));
    assert!(!grant.allows(&CanonicalRepoName::parse("team-secret").unwrap(), "pull"));
}

// ================================================================================================
// 4. PROXY ROUTING PATTERNS & PRECEDENCE TESTS
// ================================================================================================

#[test]
fn test_proxy_host_pattern_matching() {
    let exact = ProxyHostPattern::parse("docker.io").unwrap();
    assert!(exact.matches("docker.io"));
    assert!(exact.matches("DOCKER.IO")); // Case-insensitive ASCII
    assert!(!exact.matches("sub.docker.io"));
    assert!(!exact.matches("other.com"));

    let wildcard = ProxyHostPattern::parse("*.docker.io").unwrap();
    assert!(wildcard.matches("registry-1.docker.io"));
    assert!(wildcard.matches("auth.docker.io"));
    assert!(wildcard.matches("a.b.docker.io"));
    // Wildcard MUST NOT match bare base domain
    assert!(!wildcard.matches("docker.io"));
    assert!(!wildcard.matches("evil-docker.io"));

    let all = ProxyHostPattern::parse("*").unwrap();
    assert!(all.matches("anything.local"));

    // Invalid syntax rejected
    assert!(ProxyHostPattern::parse("docker*.io").is_err());
    assert!(ProxyHostPattern::parse("*docker.io").is_err());
    assert!(ProxyHostPattern::parse("").is_err());
}

#[test]
fn test_proxy_repo_pattern_precedence_and_safety() {
    let exact_pat = ProxyRepoPattern::parse("library/ubuntu").unwrap();
    let subtree_pat = ProxyRepoPattern::parse("library/*").unwrap();
    let all_pat = ProxyRepoPattern::parse("*").unwrap();

    let target = CanonicalRepoName::parse("library/ubuntu").unwrap();
    assert!(exact_pat.matches(&target));
    assert!(subtree_pat.matches(&target));
    assert!(all_pat.matches(&target));

    // Prefix safety check
    let safety = ProxyAllowedPrefix::parse("library").unwrap();
    assert!(safety.matches(&CanonicalRepoName::parse("library/ubuntu").unwrap()));
    assert!(safety.matches(&CanonicalRepoName::parse("library").unwrap()));
    // Prevents sibling prefix leakage
    assert!(!safety.matches(&CanonicalRepoName::parse("library-secret/app").unwrap()));
    assert!(!safety.matches(&CanonicalRepoName::parse("library_secret").unwrap()));
}

// ================================================================================================
// 5. PUSH ALLOWLIST PARITY TEST
// ================================================================================================

#[test]
fn test_push_allowlist_parity_across_auth_evaluation_paths() {
    // Configured policy
    let patterns = vec![
        RepositoryAccessPattern::parse("org/app").unwrap(),
        RepositoryAccessPattern::parse("team/*").unwrap(),
    ];

    let candidates = [
        ("org/app", true),
        ("org/app/sub", false),
        ("org/other", false),
        ("team", true),
        ("team/service", true),
        ("team/nested/service", true),
        ("team-secret", false),
        ("team-secret/app", false),
        ("other/repo", false),
    ];

    for (cand_str, expected) in candidates {
        let canonical_repo = CanonicalRepoName::parse(cand_str).unwrap();

        // 1. Evaluator used by basic auth, token issuance, and middleware
        let decision = push_repository_allowed(&patterns, &canonical_repo);
        assert_eq!(
            decision, expected,
            "Push allowlist parity failed for candidate '{cand_str}'"
        );
    }
}

// ================================================================================================
// 6. ACCESS PATTERN DOMAIN MODEL TESTS
// ================================================================================================

#[test]
fn test_access_pattern_closed_model_truth_table() {
    struct Case {
        pattern_str: &'static str,
        candidate_str: &'static str,
        expected_match: bool,
    }

    let cases = [
        // Exact matching
        Case {
            pattern_str: "team/app",
            candidate_str: "team/app",
            expected_match: true,
        },
        Case {
            pattern_str: "team/app",
            candidate_str: "team/app/sub",
            expected_match: false,
        },
        Case {
            pattern_str: "team/app",
            candidate_str: "team/other",
            expected_match: false,
        },
        Case {
            pattern_str: "team/app",
            candidate_str: "team/app-other",
            expected_match: false,
        },
        // Subtree wildcard matching (segment-delimited)
        Case {
            pattern_str: "team/*",
            candidate_str: "team",
            expected_match: true,
        }, // legacy compatibility: base is allowed
        Case {
            pattern_str: "team/*",
            candidate_str: "team/app",
            expected_match: true,
        },
        Case {
            pattern_str: "team/*",
            candidate_str: "team/sub/app",
            expected_match: true,
        },
        Case {
            pattern_str: "team/nested/*",
            candidate_str: "team/nested",
            expected_match: true,
        },
        Case {
            pattern_str: "team/nested/*",
            candidate_str: "team/nested/svc",
            expected_match: true,
        },
        Case {
            pattern_str: "team/nested/*",
            candidate_str: "team/other",
            expected_match: false,
        },
        // PREFIX CONFUSION PREVENTION (Critical Security Rule)
        Case {
            pattern_str: "team/*",
            candidate_str: "team-secret",
            expected_match: false,
        },
        Case {
            pattern_str: "team/*",
            candidate_str: "team-secret/app",
            expected_match: false,
        },
        Case {
            pattern_str: "team/*",
            candidate_str: "team_secret/app",
            expected_match: false,
        },
        Case {
            pattern_str: "team/*",
            candidate_str: "team.secret/app",
            expected_match: false,
        },
        Case {
            pattern_str: "team/*",
            candidate_str: "team__extra/app",
            expected_match: false,
        },
        // All wildcard matching
        Case {
            pattern_str: "*",
            candidate_str: "alpine",
            expected_match: true,
        },
        Case {
            pattern_str: "*",
            candidate_str: "team/image__cache",
            expected_match: true,
        },
        Case {
            pattern_str: "*",
            candidate_str: "org/nested/deep/service",
            expected_match: true,
        },
    ];

    for case in cases {
        let pat = RepositoryAccessPattern::parse(case.pattern_str).expect("valid pattern");
        let cand = CanonicalRepoName::parse(case.candidate_str).expect("valid candidate repo");
        let result = pat.matches(&cand);
        assert_eq!(
            result, case.expected_match,
            "Mismatch for pattern='{}' candidate='{}'",
            case.pattern_str, case.candidate_str
        );
    }
}

#[test]
fn test_access_pattern_rejection_of_arbitrary_wildcards() {
    let invalid_patterns = [
        "team*",       // arbitrary trailing wildcard without slash
        "*team",       // leading wildcard
        "team/*/app",  // middle wildcard
        "team/*/*",    // multiple wildcards
        "team/app*",   // arbitrary suffix
        "a*b",         // inline wildcard
        "",            // empty
        "   ",         // whitespace only
        "/team/*",     // leading slash
        "team/*/",     // trailing slash after star
        "TEAM/*",      // uppercase in repository name
        "team//app/*", // empty component
    ];

    for invalid in invalid_patterns {
        let res = RepositoryAccessPattern::parse(invalid);
        assert!(
            res.is_err(),
            "Expected pattern '{invalid}' to fail parsing, but succeeded: {res:?}"
        );
    }
}

// ================================================================================================
// 7. CONFIGURATION STARTUP VALIDATION TESTS
// ================================================================================================

#[test]
fn test_config_push_allow_repos_startup_validation() {
    let temp_dir = tempfile::tempdir().unwrap();

    // Valid configuration with exact, subtree, and global wildcard
    let valid_toml = r#"
[server]
listen_addr = "127.0.0.1:8080"

[auth.push]
allow_repos = ["team/image__cache", "team/services/*", "*"]
"#;
    let valid_file = temp_dir.path().join("valid_config.toml");
    std::fs::write(&valid_file, valid_toml).unwrap();

    let cfg = Config::from_env_with_files(&[valid_file]).expect("valid config should parse");
    let allowlist = cfg.push_allow_repos.expect("push_allow_repos is some");
    assert_eq!(allowlist.len(), 3);
    assert!(matches!(allowlist[0], RepositoryAccessPattern::Exact(_)));
    assert!(matches!(allowlist[1], RepositoryAccessPattern::Subtree(_)));
    assert!(matches!(allowlist[2], RepositoryAccessPattern::All));

    // Invalid configuration with arbitrary wildcard
    let invalid_toml = r#"
[server]
listen_addr = "127.0.0.1:8080"

[auth.push]
allow_repos = ["team*"]
"#;
    let invalid_file = temp_dir.path().join("invalid_config.toml");
    std::fs::write(&invalid_file, invalid_toml).unwrap();

    let err = Config::from_env_with_files(&[invalid_file])
        .expect_err("invalid pattern should fail config loading");
    match err {
        ConfigError::InvalidValue { field, message } => {
            assert_eq!(field, "auth.push.allow_repos");
            assert!(message.contains("team*"));
        }
        other => panic!("expected ConfigError::InvalidValue, got {other:?}"),
    }
}
