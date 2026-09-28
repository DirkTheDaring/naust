use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf};
use toml::Value;
use url::Url;

use crate::registry::CanonicalRepoName;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, serde::Serialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SlowConnectionPolicy {
    #[default]
    Enforce,
    AuditOnly,
    Disabled,
}

fn sanitize_for_path_component(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        let ch = ch.to_ascii_lowercase();
        if ch.is_ascii_alphanumeric() || ch == '.' || ch == '-' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    let out = out.trim_matches('_').to_string();
    if out.is_empty() {
        "upstream".to_string()
    } else {
        out
    }
}

fn cache_key_from_base_url(base_url: &str) -> String {
    let base_url = base_url.trim();
    if let Ok(u) = Url::parse(base_url)
        && let Some(host) = u.host_str() {
            // Include port if present to avoid collisions.
            if let Some(port) = u.port() {
                return sanitize_for_path_component(&format!("{host}_{port}"));
            }
            return sanitize_for_path_component(host);
        }
    sanitize_for_path_component(base_url)
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file {path:?}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error(
        "failed to parse TOML in {}: {source}",
        .path.as_deref().map(|p| p.to_string_lossy()).unwrap_or_else(|| "<string>".into())
    )]
    Toml {
        path: Option<PathBuf>,
        #[source]
        source: toml::de::Error,
    },

    #[error("failed to deserialize merged configuration: {source}")]
    Deserialize {
        #[source]
        source: toml::de::Error,
    },

    #[error("unknown TOML configuration keys (strict mode):\n  - {}", .keys.join("\n  - "))]
    UnknownKeys { keys: Vec<String> },

    #[error("invalid environment variable '{key}': expected {expected}")]
    InvalidEnvValue {
        key: &'static str,
        expected: &'static str,
    },

    #[error("missing required configuration: {field}")]
    MissingRequired { field: &'static str },

    #[error("invalid configuration value for '{field}': {message}")]
    InvalidValue {
        field: &'static str,
        message: String,
    },

    #[error("conflicting configuration: {message}")]
    Conflict { message: String },
}

#[derive(Clone, Debug)]
pub struct Config {
    pub listen_addr: SocketAddr,

    pub tls_cert_path: Option<PathBuf>,
    pub tls_key_path: Option<PathBuf>,
    // Poll interval for detecting externally renewed cert/key files (hot reload).
    pub tls_reload_poll_secs: u64,

    pub tls_acme: Option<AcmeConfig>,

    pub push_username: Option<String>,
    pub push_password: Option<String>,
    pub push_allow_repos: Option<Vec<crate::registry::RepositoryAccessPattern>>,
    pub push_actions: Vec<String>,
    pub push_implies_delete: bool,

    pub auth_strategy: AuthStrategy,
    pub anonymous_pull: bool,
    /// Repository names that require auth even when `anonymous_pull` is on (KI-17, ADR-014).
    /// A prefix matches the whole name or a path-boundary prefix (`private` matches
    /// `private` and `private/app`, not `private-repo`).
    pub private_name_prefixes: Vec<String>,
    /// Whether an explicit `*` RBAC grant also confers registry catalog scope (KI-18).
    pub star_grants_catalog: bool,

    pub storage_backend: StorageBackend,

    pub fs_root: PathBuf,
    pub fs_manifest_listing_max_entries: usize,
    pub fs_manifest_listing_max_name_bytes: usize,
    pub fs_tag_listing_max_entries: usize,
    pub fs_tag_listing_max_name_bytes: usize,
    pub fs_tag_listing_repo_probe_max_entries: usize,
    pub fs_tag_listing_repo_probe_max_name_bytes: usize,
    pub fs_tag_listing_max_payload_bytes: u64,

    pub fs_gc_discovery_max_depth: usize,

    pub fs_gc_discovery_max_dir_enumerations: usize,
    pub fs_gc_discovery_max_total_discovery_entries: usize,
    pub fs_gc_discovery_max_manifest_dirs: usize,
    pub fs_gc_discovery_max_discovery_retained_path_bytes: usize,
    pub fs_gc_discovery_intermediate_dir_max_entries: usize,
    pub fs_gc_discovery_intermediate_dir_max_name_bytes: usize,
    pub fs_gc_discovery_max_terminal_dir_enumerations: usize,
    pub fs_gc_discovery_terminal_dir_max_entries: usize,
    pub fs_gc_discovery_terminal_dir_max_name_bytes: usize,
    pub fs_gc_discovery_max_total_manifest_entries: usize,
    pub fs_gc_discovery_max_manifests_read: usize,
    pub fs_gc_discovery_max_total_references: usize,
    pub fs_gc_discovery_max_retained_logical_bytes: usize,
    pub fs_gc_discovery_max_manifest_payload_bytes: Option<u64>,

    pub s3_endpoint: Option<String>,
    pub s3_region: Option<String>,
    pub s3_bucket: Option<String>,
    pub s3_prefix: String,
    // Explicit operator assertion for single-instance S3 deployment (allows online destructive GC with local Sled index).
    pub s3_single_instance_mode: bool,
    pub s3_lease_duration_secs: u64,
    pub s3_lease_renewal_interval_secs: u64,
    pub s3_max_retry_attempts: u32,
    pub s3_legacy_multipart_cleanup_policy: LegacyMultipartCleanupPolicy,
    pub upload_receipt_lifetime_secs: u64,
    pub gc_pin_duration_secs: u64,

    // Persistent blob reference index (used to make DELETE blob safe without full scans).
    pub ref_index: RefIndexConfig,

    pub allow_tag_overwrite: bool,

    // When true, allow cross-mounting blobs without a `from` repository.
    pub automatic_crossmount: bool,

    // Filesystem backend maintenance: clean up stale upload temp files.
    pub upload_gc_enabled: bool,
    pub upload_gc_interval_secs: u64,
    pub upload_gc_max_age_secs: u64,

    // Online blob GC safety: keep newly finalized blobs pinned for at least this long.
    // This covers the common "finalized blob exists but tag/manifest not yet written" window.
    pub blob_gc_finalize_grace_secs: u64,

    // Online blob GC operational safety toggles + defaults.
    // - enabled gates quarantine/delete (plan remains available).
    // - enable_delete gates the delete phase specifically.
    // - defaults are used by admin endpoints when request fields are omitted.
    pub blob_gc_enabled: bool,
    pub blob_gc_enable_delete: bool,
    pub blob_gc_default_min_age_secs: u64,
    pub blob_gc_default_quarantine_delay_secs: u64,
    pub blob_gc_default_max_blobs: usize,
    pub blob_gc_default_max_bytes: u64,
    pub blob_gc_default_max_seconds: u64,

    // Optional background scheduling for online blob GC (server only).
    // Disabled by default.
    pub blob_gc_schedule_enabled: bool,
    pub blob_gc_schedule_interval_secs: u64,

    // Admin-only HTTP endpoints (e.g. online blob GC triggers). Disabled by default.
    pub admin_api: AdminApiConfig,

    pub max_upload_bytes: u64,
    pub max_request_body_bytes: usize,
    // Optional minimum chunk size for chunked blob uploads (OCI-Chunk-Min-Length).
    pub upload_chunk_min_bytes: Option<usize>,
    // Concurrency guard for endpoints that buffer full bodies into memory (e.g. manifest PUT,
    // proxy manifest GET when caching). Caps worst-case RAM to ~N * max_request_body_bytes.
    pub max_concurrent_buffered_requests: usize,
    // Concurrency guard for total in-flight non-upload /v2 requests (caps tasks, open files, sockets).
    pub max_concurrent_requests: usize,
    // Concurrency guard for upload endpoints (/v2/.../blobs/uploads...).
    // Upload requests can be long-lived and numerous during buildx/push; keep this bounded to
    // avoid exhausting file descriptors under retry storms or slow clients.
    pub max_concurrent_upload_requests: usize,
    pub request_timeout_secs: u64,

    // Longer timeout for upload endpoints (PATCH/PUT/POST blobs/uploads).
    pub upload_request_timeout_secs: u64,

    // Maximum silence between data chunks during streaming uploads (seconds).
    pub upload_chunk_idle_timeout_secs: u64,
    // Window duration for rolling throughput calculation (seconds).
    pub upload_rate_window_secs: u64,
    // Grace period before minimum throughput is enforced (seconds).
    pub upload_rate_grace_period_secs: u64,
    // Minimum required throughput during sliding window (bytes/second).
    pub min_upload_bytes_per_sec: u64,
    // Timeout for receiving complete HTTP request headers (seconds).
    pub header_read_timeout_secs: u64,
    // Slow connection enforcement mode ("enforce", "audit_only", "disabled").
    pub slow_connection_policy: SlowConnectionPolicy,

    // Max concurrent TCP/TLS connections per normalized client IP (/32 IPv4, /64 IPv6).
    pub max_connections_per_ip: usize,
    // CIDR subnets exempt from per-IP connection limits (e.g. CI/CD runners).
    pub trusted_bypass_cidrs: Vec<ipnet::IpNet>,
    // Upstream reverse proxy subnets trusted to provide client IP in X-Forwarded-For.
    pub trusted_proxies: Vec<ipnet::IpNet>,

    // If true, reject monolithic blob uploads (body on POST ?digest or PUT finalize).
    // This forces clients to use PATCH-based chunked uploads.
    pub disallow_monolithic_uploads: bool,

    // Upload cleanup policy: controls whether failed uploads are aborted immediately,
    // trading disk cleanup vs the ability to resume.
    pub upload_policy: UploadPolicyConfig,

    // If true, repository/org listing endpoints require authentication.
    pub catalog_requires_auth: bool,

    pub public_url: Option<String>,
    pub token_service: String,
    pub token_signing_key: String,
    pub token_signing_keys: Vec<crate::security::TokenSigningKey>,
    /// True when the key was generated because none was configured (ADR-014).
    pub token_signing_key_ephemeral: bool,
    /// Dev opt-in: allow `serve` / `check-config` to run with an ephemeral key.
    pub allow_ephemeral_token_signing_key: bool,
    pub token_ttl_secs: u64,
    /// /token fixed-window rate limit: max requests per window (0 disables).
    pub token_rate_limit_rpm: u32,
    pub token_rate_limit_window_secs: u64,

    // Robot accounts + scoped grants for token minting.
    pub robots: RobotsConfig,

    // Harbor-lite Phase 2: human users + groups for token minting.
    pub users: UsersConfig,

    pub proxy: ProxyConfig,
}

#[derive(Clone, Debug, Default)]
pub struct AdminApiConfig {
    pub enabled: bool,
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Clone, Debug)]
pub struct RefIndexConfig {
    pub enabled: bool,
    pub path: PathBuf,
    pub rebuild_on_start: bool,
    pub auto_rebuild_on_corruption: bool,
}

pub use naust_auth::policy::{
    AuthConfig, AuthStrategy, GroupConfig, RobotAccountConfig, RobotsConfig, UserAccountConfig,
    UsersConfig,
};

// Moved to the core policy module (ADR-010 §2.3); re-exported here so server-side
// consumers keep compiling until the Phase 2 crate split.
pub use crate::policy::LegacyMultipartCleanupPolicy;

/// Composition-time mapping into the core GC policy (ADR-010 §2.3).
impl From<&Config> for crate::policy::GcPolicy {
    fn from(cfg: &Config) -> Self {
        crate::policy::GcPolicy {
            enabled: cfg.blob_gc_enabled,
            enable_delete: cfg.blob_gc_enable_delete,
            default_min_age_secs: cfg.blob_gc_default_min_age_secs,
            default_quarantine_delay_secs: cfg.blob_gc_default_quarantine_delay_secs,
            default_max_blobs: cfg.blob_gc_default_max_blobs,
            default_max_bytes: cfg.blob_gc_default_max_bytes,
            default_max_seconds: cfg.blob_gc_default_max_seconds,
            auto_rebuild_ref_index_on_corruption: cfg.ref_index.auto_rebuild_on_corruption,
            fs_root: cfg.fs_root.clone(),
        }
    }
}

/// Composition-time mapping into the auth policy (ADR-019).
impl From<&Config> for naust_auth::AuthConfig {
    fn from(cfg: &Config) -> Self {
        naust_auth::AuthConfig {
            auth_strategy: cfg.auth_strategy,
            anonymous_pull: cfg.anonymous_pull,
            catalog_requires_auth: cfg.catalog_requires_auth,
            star_grants_catalog: cfg.star_grants_catalog,
            push_username: cfg.push_username.clone(),
            push_password: cfg.push_password.clone(),
            push_actions: cfg.push_actions.clone(),
            push_implies_delete: cfg.push_implies_delete,
            push_allow_repos: cfg.push_allow_repos.clone(),
            robots: cfg.robots.clone(),
            users: cfg.users.clone(),
            token_service: cfg.token_service.clone(),
            token_signing_keys: cfg.token_signing_keys.clone(),
            token_ttl_secs: cfg.token_ttl_secs,
            private_name_prefixes: cfg.private_name_prefixes.clone(),
        }
    }
}

impl Config {
    pub fn token_primary_signing_key(&self) -> &crate::security::TokenSigningKey {
        self.token_signing_keys.first().unwrap_or_else(|| {
            panic!(
                "token_signing_keys is empty (misconfiguration): set TOKEN_SIGNING_KEY, token.signing_key, or token.signing_keys"
            )
        })
    }
}

#[derive(Clone, Debug)]
pub struct UploadPolicyConfig {
    pub abort_on_error: bool,
    pub abort_on_digest_mismatch: bool,
    // TOML only (for now): repo-specific overrides.
    pub repo_rules: Vec<UploadRepoPolicyRule>,
}

#[derive(Clone, Debug)]
pub struct UploadRepoPolicyRule {
    pub match_pattern: String,
    pub abort_on_error: Option<bool>,
    pub abort_on_digest_mismatch: Option<bool>,
}

#[derive(Clone, Copy, Debug)]
pub struct ResolvedUploadPolicy {
    pub abort_on_error: bool,
    pub abort_on_digest_mismatch: bool,
}

#[derive(Clone, Debug)]
pub struct ProxyConfig {
    pub enabled: bool,

    // "allowlist" (default) or "any".
    pub mode: ProxyMode,

    // Upstream base URL, e.g. https://registry-1.docker.io
    pub upstream_base_url: Option<String>,

    // Optional upstream credentials (used for Bearer token exchange, e.g. Docker Hub).
    pub upstream_username: Option<String>,
    pub upstream_password: Option<String>,

    // Safety net: explicit allowlist of upstream hosts.
    pub allowed_upstream_hosts: Vec<String>,

    /// Hosts, besides the upstream host, that may receive upstream Basic
    /// credentials during token exchange (ADR-014). Not inferred from DNS labels.
    pub token_realm_hosts: Vec<String>,

    // Safety net: even in proxy-any mode, only allow these prefixes.
    // Examples: ["library/", "myorg/"]
    pub allowed_repo_prefixes: Vec<crate::proxy::ProxyAllowedPrefix>,

    // SSRF guard: block loopback/link-local/private IPs.
    pub block_private_networks: bool,

    // Redirect policy for upstream responses (notably Docker Hub blob CDN redirects).
    pub redirect_policy: RedirectPolicy,

    // Concurrency guard: cap upstream requests.
    pub max_concurrent_upstream: usize,

    // Cache/metadata index path (local disk).
    pub index_path: PathBuf,

    // Separate cache storage location to avoid mixing push content with pull-through content.
    // - Filesystem: root directory for cached registry storage layout.
    // - S3: prefix under the configured bucket for cached content.
    pub cache_fs_root: Option<PathBuf>,
    pub cache_s3_prefix: Option<String>,

    // How often to run cache GC/eviction (seconds).
    pub gc_interval_secs: u64,

    // Optional cache scrub: scan cached manifests/tags and delete corrupt entries.
    // Useful after crashes or unclean shutdowns, and to clean legacy/corrupt files.
    pub scrub_enabled: bool,
    pub scrub_interval_secs: u64,
    pub scrub_max_files_per_run: usize,

    // Required when enabled: upper bound for cached content (best-effort enforcement).
    pub max_cache_bytes: Option<u64>,

    // Repo-specific policies (TOML only, for now).
    pub repo_rules: Vec<ProxyRepoRule>,

    // Multi-upstream routing (TOML only): route by Host (or X-Forwarded-Host) to select an
    // upstream + isolated cache. If non-empty, this instance can serve multiple upstream registries
    // without cache collisions.
    pub upstreams: Vec<ProxyUpstreamRoute>,

    // Request routing: select proxy behavior based on Host (or X-Forwarded-Host).
    pub routing_proxy_hosts: Vec<crate::proxy::ProxyHostPattern>,
    pub routing_trust_x_forwarded_host: bool,
}

#[derive(Clone, Debug)]
pub struct ProxyUpstreamRoute {
    // Host patterns (minimal '*' glob). If the effective request host matches, this upstream is
    // selected and the request runs in proxy-only mode.
    pub hosts: Vec<crate::proxy::ProxyHostPattern>,

    // Whether to use X-Forwarded-Host when matching hosts for this upstream route.
    // Only enable this if the registry is reachable only via a trusted reverse proxy.
    pub trust_x_forwarded_host: bool,

    pub upstream_base_url: String,
    pub upstream_username: Option<String>,
    pub upstream_password: Option<String>,

    // Safety settings (can be different per upstream).
    pub allowed_upstream_hosts: Vec<String>,
    pub token_realm_hosts: Vec<String>,
    pub allowed_repo_prefixes: Vec<crate::proxy::ProxyAllowedPrefix>,
    pub block_private_networks: bool,
    pub redirect_policy: RedirectPolicy,
    pub max_concurrent_upstream: usize,

    // Cache settings (must be unique per upstream to avoid collisions).
    pub index_path: PathBuf,
    pub cache_fs_root: Option<PathBuf>,
    pub cache_s3_prefix: Option<String>,
    pub max_cache_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProxyMode {
    Allowlist,
    Any,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum RedirectPolicy {
    Disabled,
    #[default]
    SameHost,
    AnyPublic,
}


impl std::str::FromStr for RedirectPolicy {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "disabled" => Ok(Self::Disabled),
            "same_host" => Ok(Self::SameHost),
            "any_public" => Ok(Self::AnyPublic),
            _ => Err(()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ProxyRepoRule {
    pub match_pattern: crate::proxy::ProxyRepoPattern,
    pub upstream_repo: Option<crate::registry::canonical_name::CanonicalRepoName>,
    pub tag_policy: TagPolicy,
    pub eviction_policy: EvictionPolicy,
}

// Moved to the core policy module (ADR-010 §2.3); re-exported here so server-side
// consumers keep compiling until the Phase 2 crate split.
pub use crate::policy::TagPolicy;

// Moved to the core policy module (ADR-010 §2.3); re-exported here so server-side
// consumers keep compiling until the Phase 2 crate split.
pub use crate::policy::EvictionPolicy;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageBackend {
    Filesystem,
    S3,
}

#[derive(Clone, Debug)]
pub struct AcmeConfig {
    pub email: String,
    pub names: Vec<String>,
    pub output_dir: PathBuf,
    pub allow_first_wildcard: bool,
    pub renewal_window_secs: u64,
    // Interval between runtime renewal attempts (the ACME call itself renews only
    // inside `renewal_window`).
    pub renew_check_interval_secs: u64,
    // Break-glass: serve despite a SAN/name mismatch at startup.
    pub allow_san_mismatch: bool,
    pub proxy: Option<String>,
    pub debug: bool,
    pub propagation_check_disabled: bool,
    pub propagation_check_strict: bool,
    pub provider: AcmeProvider,
}

#[derive(Clone, Debug)]
pub enum AcmeProvider {
    Ispone {
        base_url: String,
        authorization: String,
    },
    ExecPath {
        exec_path: PathBuf,
    },
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileConfig {
    #[serde(default)]
    config: FileConfigMeta,
    #[serde(default)]
    profile: FileProfile,
    #[serde(default)]
    server: FileServer,
    #[serde(default)]
    auth: FileAuth,
    #[serde(default)]
    token: FileToken,
    #[serde(default)]
    storage: FileStorage,
    #[serde(default)]
    features: FileFeatures,
    #[serde(default)]
    uploads: FileUploads,
    #[serde(default)]
    limits: FileLimits,
    #[serde(default)]
    timeouts: FileTimeouts,
    #[serde(default)]
    catalog: FileCatalog,

    #[serde(default)]
    blob_gc: FileBlobGc,

    #[serde(default)]
    admin_api: FileAdminApi,

    #[serde(default)]
    proxy: FileProxy,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileBlobGc {
    #[serde(default)]
    finalize_grace_secs: Option<u64>,

    #[serde(default)]
    enabled: Option<bool>,

    #[serde(default)]
    enable_delete: Option<bool>,

    #[serde(default)]
    default_min_age_secs: Option<u64>,

    #[serde(default)]
    default_quarantine_delay_secs: Option<u64>,

    #[serde(default)]
    default_max_blobs: Option<usize>,

    #[serde(default)]
    default_max_bytes: Option<u64>,

    #[serde(default)]
    default_max_seconds: Option<u64>,

    #[serde(default)]
    schedule_enabled: Option<bool>,

    #[serde(default)]
    schedule_interval_secs: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileAdminApi {
    #[serde(default)]
    enabled: Option<bool>,

    #[serde(default)]
    username: Option<String>,

    #[serde(default)]
    password: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileConfigMeta {
    // When enabled, fail fast if the TOML contains unknown keys (helps catch typos).
    #[serde(default)]
    strict: Option<bool>,
}

#[derive(Clone, Debug, Default)]
struct LoadedFileConfig {
    cfg: FileConfig,
    ignored_paths: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileProxy {
    #[serde(default)]
    enabled: Option<bool>,

    #[serde(default)]
    mode: Option<String>,

    #[serde(default)]
    upstream: FileProxyUpstream,

    #[serde(default)]
    safety: FileProxySafety,

    #[serde(default)]
    cache: FileProxyCache,

    #[serde(default)]
    routing: FileProxyRouting,

    #[serde(default)]
    upstreams: Vec<FileProxyUpstreamRoute>,

    #[serde(default)]
    repos: Vec<FileProxyRepoRule>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileProxyUpstreamRoute {
    // Host patterns (minimal '*' glob).
    #[serde(default)]
    hosts: Vec<String>,

    // Shorthand form (optional): allow flat keys in [[proxy.upstreams]] entries.
    // These are equivalent to the nested [proxy.upstreams.routing]/[proxy.upstreams.upstream]/[proxy.upstreams.cache] blocks.
    #[serde(default)]
    trust_x_forwarded_host: Option<bool>,
    #[serde(default)]
    base_url: Option<String>,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    max_cache_bytes: Option<u64>,

    #[serde(default)]
    routing: FileProxyUpstreamRouting,

    #[serde(default)]
    upstream: FileProxyUpstream,

    #[serde(default)]
    safety: FileProxySafety,

    #[serde(default)]
    cache: FileProxyRouteCache,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileProxyUpstreamRouting {
    #[serde(default)]
    hosts: Option<Vec<String>>,
    #[serde(default)]
    trust_x_forwarded_host: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileProxyRouteCache {
    #[serde(default)]
    index_path: Option<String>,
    #[serde(default)]
    fs_root: Option<String>,
    #[serde(default)]
    s3_prefix: Option<String>,
    #[serde(default)]
    max_cache_bytes: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileProxyUpstream {
    #[serde(default)]
    base_url: Option<String>,

    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileProxyRouting {
    #[serde(default)]
    proxy_hosts: Option<Vec<String>>,
    #[serde(default)]
    trust_x_forwarded_host: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileProxySafety {
    #[serde(default)]
    allowed_upstream_hosts: Option<Vec<String>>,
    #[serde(default)]
    token_realm_hosts: Option<Vec<String>>,
    #[serde(default)]
    allowed_repo_prefixes: Option<Vec<String>>,
    #[serde(default)]
    block_private_networks: Option<bool>,
    #[serde(default)]
    redirect_policy: Option<RedirectPolicy>,
    #[serde(default)]
    max_concurrent_upstream: Option<usize>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileProxyCache {
    #[serde(default)]
    index_path: Option<String>,
    #[serde(default)]
    fs_root: Option<String>,
    #[serde(default)]
    s3_prefix: Option<String>,
    #[serde(default)]
    max_cache_bytes: Option<u64>,
    #[serde(default)]
    gc_interval_secs: Option<u64>,

    #[serde(default)]
    scrub_enabled: Option<bool>,
    #[serde(default)]
    scrub_interval_secs: Option<u64>,
    #[serde(default)]
    scrub_max_files_per_run: Option<usize>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileProxyRepoRule {
    #[serde(rename = "match")]
    match_pattern: String,

    #[serde(default)]
    upstream_repo: Option<String>,

    #[serde(default)]
    tag_policy: FileTagPolicy,

    #[serde(default)]
    eviction: FileEvictionPolicy,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FileTagPolicy {
    #[default]
    DigestOnly,
    TtlSeconds(u64),
    AlwaysRevalidate,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum FileEvictionPolicy {
    #[default]
    #[serde(rename = "default")]
    Default,
    #[serde(rename = "keep_tags")]
    KeepTags { tags: Vec<String> },
    #[serde(rename = "keep_latest_cached_semver")]
    KeepLatestCachedSemver {
        #[serde(default)]
        tag_regex: Option<String>,
        #[serde(default)]
        allow_prerelease: Option<bool>,
    },
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileProfile {
    #[serde(default)]
    name: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileServer {
    #[serde(default)]
    listen_addr: Option<SocketAddr>,
    #[serde(default)]
    public_url: Option<String>,
    #[serde(default)]
    tls: FileTls,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileTls {
    #[serde(default)]
    cert_path: Option<String>,
    #[serde(default)]
    key_path: Option<String>,
    #[serde(default)]
    reload_poll_secs: Option<u64>,

    #[serde(default)]
    acme: FileTlsAcme,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileTlsAcme {
    #[serde(default)]
    enabled: Option<bool>,

    // "ispone" or "exec_path".
    #[serde(default)]
    provider: Option<String>,

    #[serde(default)]
    email: Option<String>,

    #[serde(default)]
    names: Vec<String>,

    // Directory where acmecert-core writes cert.pem + key.pem.
    #[serde(default)]
    output_dir: Option<String>,

    #[serde(default)]
    allow_first_wildcard: Option<bool>,

    #[serde(default)]
    renewal_window_secs: Option<u64>,

    #[serde(default)]
    renew_check_interval_secs: Option<u64>,

    #[serde(default)]
    allow_san_mismatch: Option<bool>,

    // Optional HTTP proxy for ACME + hook calls.
    #[serde(default)]
    proxy: Option<String>,

    #[serde(default)]
    debug: Option<bool>,

    // DNS TXT propagation pre-checks.
    #[serde(default)]
    propagation_check_disabled: Option<bool>,
    #[serde(default)]
    propagation_check_strict: Option<bool>,

    #[serde(default)]
    ispone: FileTlsAcmeIspone,

    #[serde(default)]
    exec_path: FileTlsAcmeExec,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileTlsAcmeIspone {
    #[serde(default)]
    base_url: Option<String>,
    #[serde(default)]
    authorization: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileTlsAcmeExec {
    #[serde(default)]
    exec_path: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileAuth {
    #[serde(default)]
    strategy: Option<String>,

    #[serde(default)]
    anonymous_pull: Option<bool>,

    #[serde(default)]
    private_name_prefixes: Option<Vec<String>>,

    #[serde(default)]
    star_grants_catalog: Option<bool>,

    #[serde(default)]
    push: FilePushAuth,

    #[serde(default)]
    robots: FileRobots,

    #[serde(default)]
    users: FileUsers,

    #[serde(default)]
    groups: Vec<FileAuthGroup>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileUsers {
    #[serde(default)]
    enabled: Option<bool>,

    #[serde(default)]
    accounts: Vec<FileUserAccount>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileUserAccount {
    name: String,
    secret_hash: String,

    #[serde(default)]
    groups: Vec<String>,

    #[serde(default)]
    max_ttl_secs: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileAuthGroup {
    name: String,

    #[serde(default)]
    grants: Vec<FileRobotGrant>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileRobots {
    #[serde(default)]
    enabled: Option<bool>,

    #[serde(default)]
    accounts: Vec<FileRobotAccount>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileRobotAccount {
    name: String,
    secret_hash: String,

    #[serde(default)]
    grants: Vec<FileRobotGrant>,

    #[serde(default)]
    max_ttl_secs: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileRobotGrant {
    repo_prefix: String,

    #[serde(default)]
    actions: Vec<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FilePushAuth {
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    allow_repos: Option<Vec<String>>,
    #[serde(default)]
    actions: Option<Vec<String>>,
    #[serde(default)]
    implies_delete: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileToken {
    #[serde(default)]
    service: Option<String>,
    #[serde(default)]
    signing_key: Option<String>,
    #[serde(default)]
    allow_ephemeral_signing_key: Option<bool>,
    #[serde(default)]
    signing_keys: Vec<FileTokenSigningKey>,
    #[serde(default)]
    ttl_secs: Option<u64>,
    #[serde(default)]
    rate_limit_rpm: Option<u32>,
    #[serde(default)]
    rate_limit_window_secs: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileTokenSigningKey {
    kid: String,
    key: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileStorage {
    #[serde(default)]
    backend: Option<String>,
    #[serde(default)]
    fs: FileStorageFs,
    #[serde(default)]
    s3: FileStorageS3,

    #[serde(default)]
    ref_index: FileStorageRefIndex,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileStorageRefIndex {
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    rebuild_on_start: Option<bool>,
    #[serde(default)]
    auto_rebuild_on_corruption: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileStorageFs {
    #[serde(default)]
    root: Option<String>,
    #[serde(default)]
    manifest_listing_max_entries: Option<usize>,
    #[serde(default)]
    manifest_listing_max_name_bytes: Option<usize>,
    #[serde(default)]
    tag_listing_max_entries: Option<usize>,
    #[serde(default)]
    tag_listing_max_name_bytes: Option<usize>,
    #[serde(default)]
    tag_listing_repo_probe_max_entries: Option<usize>,
    #[serde(default)]
    tag_listing_repo_probe_max_name_bytes: Option<usize>,
    #[serde(default)]
    tag_listing_max_payload_bytes: Option<u64>,
    #[serde(default)]
    gc: FileStorageFsGc,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileStorageFsGc {
    #[serde(default)]
    discovery: FileStorageFsGcDiscovery,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileStorageFsGcDiscovery {
    #[serde(default)]
    max_depth: Option<usize>,
    #[serde(default)]
    max_dir_enumerations: Option<usize>,
    #[serde(default)]
    max_total_discovery_entries: Option<usize>,
    #[serde(default)]
    max_manifest_dirs: Option<usize>,
    #[serde(default)]
    max_discovery_retained_path_bytes: Option<usize>,
    #[serde(default)]
    intermediate_dir_max_entries: Option<usize>,
    #[serde(default)]
    intermediate_dir_max_name_bytes: Option<usize>,
    #[serde(default)]
    max_terminal_dir_enumerations: Option<usize>,
    #[serde(default)]
    terminal_dir_max_entries: Option<usize>,
    #[serde(default)]
    terminal_dir_max_name_bytes: Option<usize>,
    #[serde(default)]
    max_total_manifest_entries: Option<usize>,
    #[serde(default)]
    max_manifests_read: Option<usize>,
    #[serde(default)]
    max_total_references: Option<usize>,
    #[serde(default)]
    max_retained_logical_bytes: Option<usize>,
    #[serde(default)]
    max_manifest_payload_bytes: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileStorageS3 {
    #[serde(default)]
    endpoint: Option<String>,
    #[serde(default)]
    region: Option<String>,
    #[serde(default)]
    bucket: Option<String>,
    #[serde(default)]
    prefix: Option<String>,
    #[serde(default)]
    single_instance_mode: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileFeatures {
    #[serde(default)]
    allow_tag_overwrite: Option<bool>,
    #[serde(default)]
    automatic_crossmount: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileUploadsS3 {
    #[serde(default)]
    lease_duration_secs: Option<u64>,
    #[serde(default)]
    lease_renewal_interval_secs: Option<u64>,
    #[serde(default)]
    max_retry_attempts: Option<u32>,
    #[serde(default)]
    legacy_multipart_cleanup_policy: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileUploads {
    #[serde(default)]
    disallow_monolithic_uploads: Option<bool>,

    #[serde(default)]
    abort_on_error: Option<bool>,
    #[serde(default)]
    abort_on_digest_mismatch: Option<bool>,

    #[serde(default)]
    gc_enabled: Option<bool>,
    #[serde(default)]
    gc_interval_secs: Option<u64>,
    #[serde(default)]
    gc_max_age_secs: Option<u64>,

    #[serde(default)]
    s3: FileUploadsS3,

    #[serde(default)]
    receipt_lifetime_secs: Option<u64>,
    #[serde(default)]
    gc_pin_duration_secs: Option<u64>,

    #[serde(default)]
    repos: Vec<FileUploadRepoPolicy>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileUploadRepoPolicy {
    #[serde(rename = "match")]
    match_pattern: String,
    #[serde(default)]
    abort_on_error: Option<bool>,
    #[serde(default)]
    abort_on_digest_mismatch: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileLimits {
    #[serde(default)]
    max_upload_bytes: Option<u64>,
    #[serde(default)]
    max_request_body_bytes: Option<usize>,
    #[serde(default)]
    upload_chunk_min_bytes: Option<usize>,
    #[serde(default)]
    max_concurrent_buffered_requests: Option<usize>,
    #[serde(default)]
    max_concurrent_requests: Option<usize>,
    #[serde(default)]
    max_concurrent_upload_requests: Option<usize>,
    #[serde(default)]
    max_connections_per_ip: Option<usize>,
    #[serde(default)]
    trusted_bypass_cidrs: Option<Vec<String>>,
    #[serde(default)]
    trusted_proxies: Option<Vec<String>>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileTimeouts {
    #[serde(default)]
    request_timeout_secs: Option<u64>,
    #[serde(default)]
    upload_request_timeout_secs: Option<u64>,
    #[serde(default)]
    upload_chunk_idle_timeout_secs: Option<u64>,
    #[serde(default)]
    upload_rate_window_secs: Option<u64>,
    #[serde(default)]
    upload_rate_grace_period_secs: Option<u64>,
    #[serde(default)]
    min_upload_bytes_per_sec: Option<u64>,
    #[serde(default)]
    header_read_timeout_secs: Option<u64>,
    #[serde(default)]
    slow_connection_policy: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileCatalog {
    requires_auth: Option<bool>,
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_env_with_files(&[])
    }

    pub fn from_env_with_files(config_paths: &[PathBuf]) -> Result<Self, ConfigError> {
        // Precedence:
        //   defaults < config file(s) < env vars
        let loaded = if config_paths.is_empty() {
            load_config_file()?
        } else {
            load_config_files(config_paths)?
        };
        let file_cfg = loaded.cfg;

        let best_practice = env_bool_opt(&["BEST_PRACTICE"])?.unwrap_or(false)
            || file_cfg
                .profile
                .name
                .as_deref()
                .map(|s| s.eq_ignore_ascii_case("best_practice"))
                .unwrap_or(false);

        // Optional strict config parsing: fail fast on unknown keys/typos.
        // - enabled by env vars, TOML [config].strict, or best_practice profile.
        let strict_config = env_bool_opt(&["REGISTRY__CONFIG__STRICT", "STRICT_CONFIG"])?
            .or(file_cfg.config.strict)
            .unwrap_or(best_practice);
        if !loaded.ignored_paths.is_empty() {
            if strict_config {
                let mut paths = loaded.ignored_paths;
                paths.sort();
                paths.dedup();
                return Err(ConfigError::UnknownKeys { keys: paths });
            } else {
                let mut paths = loaded.ignored_paths;
                paths.sort();
                paths.dedup();
                eprintln!(
                    "Warning: unknown TOML keys ignored (set STRICT_CONFIG=1 to fail fast):\n  - {}",
                    paths.join("\n  - ")
                );
            }
        }

        let listen_addr = env_socket_addr_opt(&["REGISTRY__SERVER__LISTEN_ADDR", "LISTEN_ADDR"])?
            .unwrap_or_else(|| {
                file_cfg
                    .server
                    .listen_addr
                    .unwrap_or_else(|| ([127, 0, 0, 1], 5000).into())
            });

        let mut tls_cert_path = env_str_opt(&["REGISTRY__SERVER__TLS__CERT_PATH", "TLS_CERT_PATH"])
            .or_else(|| file_cfg.server.tls.cert_path.clone())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .map(PathBuf::from);
        let mut tls_key_path = env_str_opt(&["REGISTRY__SERVER__TLS__KEY_PATH", "TLS_KEY_PATH"])
            .or_else(|| file_cfg.server.tls.key_path.clone())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .map(PathBuf::from);

        // ACME TLS provisioning (DNS-01 via acmecert-core).
        let acme_enabled =
            env_bool_opt(&["REGISTRY__SERVER__TLS__ACME__ENABLED", "TLS_ACME_ENABLED"])?
                .or(file_cfg.server.tls.acme.enabled)
                .unwrap_or(false);

        let tls_acme = if acme_enabled {
            let provider_raw =
                env_str_opt(&["REGISTRY__SERVER__TLS__ACME__PROVIDER", "TLS_ACME_PROVIDER"])
                    .or_else(|| file_cfg.server.tls.acme.provider.clone())
                    .map(|s| s.trim().to_ascii_lowercase())
                    .unwrap_or_else(|| "ispone".to_string());

            let email = env_str_opt(&["REGISTRY__SERVER__TLS__ACME__EMAIL", "TLS_ACME_EMAIL"])
                .or_else(|| file_cfg.server.tls.acme.email.clone())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .ok_or(ConfigError::MissingRequired {
                    field: "server.tls.acme.email (or TLS_ACME_EMAIL)",
                })?;

            let names = {
                let mut out =
                    env_str_opt(&["REGISTRY__SERVER__TLS__ACME__NAMES", "TLS_ACME_NAMES"])
                        .map(|s| {
                            s.split(',')
                                .map(|p| p.trim().to_string())
                                .filter(|p| !p.is_empty())
                                .collect::<Vec<_>>()
                        })
                        .filter(|v| !v.is_empty())
                        .unwrap_or_else(|| file_cfg.server.tls.acme.names.clone());
                out.retain(|s| !s.trim().is_empty());
                if out.is_empty() {
                    return Err(ConfigError::MissingRequired {
                        field: "server.tls.acme.names (or TLS_ACME_NAMES)",
                    });
                }
                out
            };

            let output_dir = env_str_opt(&[
                "REGISTRY__SERVER__TLS__ACME__OUTPUT_DIR",
                "TLS_ACME_OUTPUT_DIR",
            ])
            .or_else(|| file_cfg.server.tls.acme.output_dir.clone())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .ok_or(ConfigError::MissingRequired {
                field: "server.tls.acme.output_dir (or TLS_ACME_OUTPUT_DIR)",
            })?;

            let allow_first_wildcard = env_bool_opt(&[
                "REGISTRY__SERVER__TLS__ACME__ALLOW_FIRST_WILDCARD",
                "TLS_ACME_ALLOW_FIRST_WILDCARD",
            ])?
            .or(file_cfg.server.tls.acme.allow_first_wildcard)
            .unwrap_or(false);

            let renewal_window_secs = env_u64_opt(&[
                "REGISTRY__SERVER__TLS__ACME__RENEWAL_WINDOW_SECS",
                "TLS_ACME_RENEWAL_WINDOW_SECS",
            ])?
            .or(file_cfg.server.tls.acme.renewal_window_secs)
            .unwrap_or(30 * 24 * 60 * 60);

            let renew_check_interval_secs = env_u64_opt(&[
                "REGISTRY__SERVER__TLS__ACME__RENEW_CHECK_INTERVAL_SECS",
                "TLS_ACME_RENEW_CHECK_INTERVAL_SECS",
            ])?
            .or(file_cfg.server.tls.acme.renew_check_interval_secs)
            .unwrap_or(12 * 60 * 60);

            let allow_san_mismatch = env_bool_opt(&[
                "REGISTRY__SERVER__TLS__ACME__ALLOW_SAN_MISMATCH",
                "TLS_ACME_ALLOW_SAN_MISMATCH",
            ])?
            .or(file_cfg.server.tls.acme.allow_san_mismatch)
            .unwrap_or(false);

            let proxy = env_str_opt(&["REGISTRY__SERVER__TLS__ACME__PROXY", "TLS_ACME_PROXY"])
                .or_else(|| file_cfg.server.tls.acme.proxy.clone())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty());

            let debug = env_bool_opt(&["REGISTRY__SERVER__TLS__ACME__DEBUG", "TLS_ACME_DEBUG"])?
                .or(file_cfg.server.tls.acme.debug)
                .unwrap_or(false);

            let propagation_check_disabled = env_bool_opt(&[
                "REGISTRY__SERVER__TLS__ACME__PROPAGATION_CHECK_DISABLED",
                "TLS_ACME_PROPAGATION_CHECK_DISABLED",
            ])?
            .or(file_cfg.server.tls.acme.propagation_check_disabled)
            .unwrap_or(false);

            let propagation_check_strict = env_bool_opt(&[
                "REGISTRY__SERVER__TLS__ACME__PROPAGATION_CHECK_STRICT",
                "TLS_ACME_PROPAGATION_CHECK_STRICT",
            ])?
            .or(file_cfg.server.tls.acme.propagation_check_strict)
            .unwrap_or(false);

            let provider = match provider_raw.as_str() {
                "ispone" => {
                    let base_url = env_str_opt(&[
                        "REGISTRY__SERVER__TLS__ACME__ISPONE__BASE_URL",
                        "TLS_ACME_ISPONE_BASE_URL",
                    ])
                    .or_else(|| file_cfg.server.tls.acme.ispone.base_url.clone())
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .ok_or(ConfigError::MissingRequired {
                        field: "server.tls.acme.ispone.base_url (or TLS_ACME_ISPONE_BASE_URL)",
                    })?;

                    let authorization = env_str_opt(&[
                        "REGISTRY__SERVER__TLS__ACME__ISPONE__AUTHORIZATION",
                        "TLS_ACME_ISPONE_AUTHORIZATION",
                    ])
                    .or_else(|| file_cfg.server.tls.acme.ispone.authorization.clone())
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .ok_or(ConfigError::MissingRequired {
                        field: "server.tls.acme.ispone.authorization (or TLS_ACME_ISPONE_AUTHORIZATION)",
                    })?;

                    AcmeProvider::Ispone {
                        base_url,
                        authorization,
                    }
                }
                "exec_path" | "exec" => {
                    let exec_path = env_str_opt(&[
                        "REGISTRY__SERVER__TLS__ACME__EXEC_PATH__EXEC_PATH",
                        "TLS_ACME_EXEC_PATH",
                    ])
                    .or_else(|| file_cfg.server.tls.acme.exec_path.exec_path.clone())
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .map(PathBuf::from)
                    .ok_or(ConfigError::MissingRequired {
                        field: "server.tls.acme.exec_path.exec_path (or TLS_ACME_EXEC_PATH)",
                    })?;
                    AcmeProvider::ExecPath { exec_path }
                }
                _ => {
                    return Err(ConfigError::InvalidValue {
                        field: "server.tls.acme.provider",
                        message: "unknown provider: expected 'ispone' or 'exec_path'".into(),
                    });
                }
            };

            // When ACME is enabled, default TLS cert/key paths to the generated output.
            let generated_cert = output_dir.join("cert.pem");
            let generated_key = output_dir.join("key.pem");

            match (&tls_cert_path, &tls_key_path) {
                (None, None) => {
                    tls_cert_path = Some(generated_cert.clone());
                    tls_key_path = Some(generated_key.clone());
                }
                (Some(cert), Some(key)) => {
                    if cert != &generated_cert || key != &generated_key {
                        return Err(ConfigError::Conflict {
                            message: format!(
                                "ACME is enabled but TLS cert/key paths do not match ACME output_dir. Expected cert_path={generated_cert:?} key_path={generated_key:?}"
                            ),
                        });
                    }
                }
                _ => {
                    return Err(ConfigError::Conflict {
                        message: "ACME is enabled but only one of TLS cert/key paths is set; set both or neither".into(),
                    });
                }
            }

            Some(AcmeConfig {
                email,
                names,
                output_dir,
                allow_first_wildcard,
                renewal_window_secs,
                renew_check_interval_secs,
                allow_san_mismatch,
                proxy,
                debug,
                propagation_check_disabled,
                propagation_check_strict,
                provider,
            })
        } else {
            None
        };

        let push_username = env_str_opt(&["REGISTRY__AUTH__PUSH__USERNAME", "REGISTRY_USERNAME"])
            .or_else(|| file_cfg.auth.push.username.clone());
        let push_password = env_str_opt(&["REGISTRY__AUTH__PUSH__PASSWORD", "REGISTRY_PASSWORD"])
            .or_else(|| file_cfg.auth.push.password.clone());

        let auth_strategy_raw = env_str_opt(&[
            "REGISTRY__AUTH__STRATEGY",
            "REGISTRY_AUTH_STRATEGY",
            "AUTH_STRATEGY",
        ])
        .or_else(|| file_cfg.auth.strategy.clone())
        .map(|s| s.trim().to_ascii_lowercase())
        .unwrap_or_else(|| "token".to_string());
        let auth_strategy = match auth_strategy_raw.as_str() {
            "bearer" | "token" => AuthStrategy::Token,
            "basic" => AuthStrategy::Basic,
            "both" | "basic_and_token" => AuthStrategy::Both,
            _ => {
                return Err(ConfigError::InvalidValue {
                    field: "auth.strategy",
                    message: "unknown strategy: expected 'token', 'basic', or 'both'".into(),
                });
            }
        };

        let anonymous_pull = env_bool_opt(&[
            "REGISTRY__AUTH__ANONYMOUS_PULL",
            "REGISTRY_AUTH_ANONYMOUS_PULL",
            "REGISTRY_ANONYMOUS_PULL",
            "AUTH_ANONYMOUS_PULL",
        ])?
        .or(file_cfg.auth.anonymous_pull)
        .unwrap_or(true);

        let private_name_prefixes = env_str_opt(&["REGISTRY__AUTH__PRIVATE_NAME_PREFIXES"])
            .map(|v| {
                v.split(',')
                    .map(|p| p.trim().to_string())
                    .filter(|p| !p.is_empty())
                    .collect::<Vec<_>>()
            })
            .or_else(|| file_cfg.auth.private_name_prefixes.clone())
            .unwrap_or_else(default_private_name_prefixes);

        let star_grants_catalog = env_bool_opt(&["REGISTRY__AUTH__STAR_GRANTS_CATALOG"])?
            .or(file_cfg.auth.star_grants_catalog)
            .unwrap_or(true);

        let push_allow_repos_raw = env_str_opt(&[
            "REGISTRY__AUTH__PUSH__ALLOW_REPOS",
            "REGISTRY_PUSH_ALLOW_REPOS",
        ])
        .map(|s| {
            s.split(',')
                .map(|p| p.trim().to_string())
                .filter(|p| !p.is_empty())
                .collect::<Vec<_>>()
        })
        .filter(|v| !v.is_empty())
        .or_else(|| file_cfg.auth.push.allow_repos.clone())
        .filter(|v| !v.is_empty());

        let push_actions = env_str_opt(&[
            "REGISTRY__AUTH__PUSH__ACTIONS",
            "REGISTRY_AUTH_ACTIONS",
            "REGISTRY_PUSH_ACTIONS",
        ])
        .map(|s| {
            s.split(',')
                .map(|p| p.trim().to_ascii_lowercase())
                .filter(|p| !p.is_empty())
                .collect::<Vec<_>>()
        })
        .filter(|v| !v.is_empty())
        .or_else(|| file_cfg.auth.push.actions.clone())
        .unwrap_or_else(|| vec!["pull".to_string(), "push".to_string()]);

        let push_implies_delete = env_bool_opt(&[
            "REGISTRY__AUTH__PUSH_IMPLIES_DELETE",
            "REGISTRY_PUSH_IMPLIES_DELETE",
        ])?
        .or(file_cfg.auth.push.implies_delete)
        .unwrap_or(false);

        let push_allow_repos = match push_allow_repos_raw {
            Some(raw_list) => {
                let mut parsed_list = Vec::with_capacity(raw_list.len());
                for entry in raw_list {
                    let pat =
                        crate::registry::RepositoryAccessPattern::parse(&entry).map_err(|err| {
                            ConfigError::InvalidValue {
                                field: "auth.push.allow_repos",
                                message: format!(
                                    "invalid repository access pattern '{entry}': {err}"
                                ),
                            }
                        })?;
                    parsed_list.push(pat);
                }
                Some(parsed_list)
            }
            None => None,
        };

        let storage_backend_raw = env_str_opt(&["REGISTRY__STORAGE__BACKEND", "STORAGE_BACKEND"])
            .or_else(|| file_cfg.storage.backend.clone())
            .unwrap_or_else(|| "fs".to_string());
        let storage_backend = match storage_backend_raw.trim().to_ascii_lowercase().as_str() {
            "fs" | "filesystem" => StorageBackend::Filesystem,
            "s3" => StorageBackend::S3,
            other => {
                eprintln!("Unknown STORAGE_BACKEND='{other}', defaulting to fs");
                StorageBackend::Filesystem
            }
        };

        let fs_root = env_str_opt(&["REGISTRY__STORAGE__FS__ROOT", "STORAGE_FS_ROOT"])
            .or_else(|| file_cfg.storage.fs.root.clone())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("./data"));

        let fs_manifest_listing_max_entries = env_usize_opt(&[
            "REGISTRY__STORAGE__FS__MANIFEST_LISTING_MAX_ENTRIES",
            "STORAGE_FS_MANIFEST_LISTING_MAX_ENTRIES",
        ])?
        .or(file_cfg.storage.fs.manifest_listing_max_entries)
        .unwrap_or(crate::storage::fs::manifest_listing::DEFAULT_MANIFEST_LISTING_MAX_ENTRIES);

        let fs_manifest_listing_max_name_bytes = env_usize_opt(&[
            "REGISTRY__STORAGE__FS__MANIFEST_LISTING_MAX_NAME_BYTES",
            "STORAGE_FS_MANIFEST_LISTING_MAX_NAME_BYTES",
        ])?
        .or(file_cfg.storage.fs.manifest_listing_max_name_bytes)
        .unwrap_or(crate::storage::fs::manifest_listing::DEFAULT_MANIFEST_LISTING_MAX_NAME_BYTES);

        let fs_tag_listing_max_entries = env_usize_opt(&[
            "REGISTRY__STORAGE__FS__TAG_LISTING_MAX_ENTRIES",
            "STORAGE_FS_TAG_LISTING_MAX_ENTRIES",
        ])?
        .or(file_cfg.storage.fs.tag_listing_max_entries)
        .unwrap_or(crate::storage::fs::tag_listing::DEFAULT_TAG_LISTING_MAX_ENTRIES);

        let fs_tag_listing_max_name_bytes = env_usize_opt(&[
            "REGISTRY__STORAGE__FS__TAG_LISTING_MAX_NAME_BYTES",
            "STORAGE_FS_TAG_LISTING_MAX_NAME_BYTES",
        ])?
        .or(file_cfg.storage.fs.tag_listing_max_name_bytes)
        .unwrap_or(crate::storage::fs::tag_listing::DEFAULT_TAG_LISTING_MAX_NAME_BYTES);

        let fs_tag_listing_repo_probe_max_entries = env_usize_opt(&[
            "REGISTRY__STORAGE__FS__TAG_LISTING_REPO_PROBE_MAX_ENTRIES",
            "STORAGE_FS_TAG_LISTING_REPO_PROBE_MAX_ENTRIES",
        ])?
        .or(file_cfg.storage.fs.tag_listing_repo_probe_max_entries)
        .unwrap_or(crate::storage::fs::tag_listing::DEFAULT_TAG_LISTING_REPO_PROBE_MAX_ENTRIES);

        let fs_tag_listing_repo_probe_max_name_bytes = env_usize_opt(&[
            "REGISTRY__STORAGE__FS__TAG_LISTING_REPO_PROBE_MAX_NAME_BYTES",
            "STORAGE_FS_TAG_LISTING_REPO_PROBE_MAX_NAME_BYTES",
        ])?
        .or(file_cfg.storage.fs.tag_listing_repo_probe_max_name_bytes)
        .unwrap_or(crate::storage::fs::tag_listing::DEFAULT_TAG_LISTING_REPO_PROBE_MAX_NAME_BYTES);

        let fs_tag_listing_max_payload_bytes = env_u64_opt(&[
            "REGISTRY__STORAGE__FS__TAG_LISTING_MAX_PAYLOAD_BYTES",
            "STORAGE_FS_TAG_LISTING_MAX_PAYLOAD_BYTES",
        ])?
        .or(file_cfg.storage.fs.tag_listing_max_payload_bytes)
        .unwrap_or(crate::storage::fs::tag_listing::DEFAULT_TAG_LISTING_MAX_PAYLOAD_BYTES);

        let fs_gc_discovery_max_depth = env_usize_opt(&[
            "REGISTRY__STORAGE__FS__GC__DISCOVERY__MAX_DEPTH",
            "REGISTRY_BLOB_GC_DISCOVERY_MAX_DEPTH",
        ])?
        .or(file_cfg.storage.fs.gc.discovery.max_depth)
        .unwrap_or(32);

        let fs_gc_discovery_max_dir_enumerations = env_usize_opt(&[
            "REGISTRY__STORAGE__FS__GC__DISCOVERY__MAX_DIR_ENUMERATIONS",
            "REGISTRY_BLOB_GC_DISCOVERY_MAX_DIR_ENUMERATIONS",
        ])?
        .or(file_cfg.storage.fs.gc.discovery.max_dir_enumerations)
        .unwrap_or(10_000);

        let fs_gc_discovery_max_total_discovery_entries = env_usize_opt(&[
            "REGISTRY__STORAGE__FS__GC__DISCOVERY__MAX_TOTAL_DISCOVERY_ENTRIES",
            "REGISTRY_BLOB_GC_DISCOVERY_MAX_TOTAL_DISCOVERY_ENTRIES",
        ])?
        .or(file_cfg.storage.fs.gc.discovery.max_total_discovery_entries)
        .unwrap_or(250_000);

        let fs_gc_discovery_max_manifest_dirs = env_usize_opt(&[
            "REGISTRY__STORAGE__FS__GC__DISCOVERY__MAX_MANIFEST_DIRS",
            "REGISTRY_BLOB_GC_DISCOVERY_MAX_MANIFEST_DIRS",
        ])?
        .or(file_cfg.storage.fs.gc.discovery.max_manifest_dirs)
        .unwrap_or(10_000);

        let fs_gc_discovery_max_discovery_retained_path_bytes = env_usize_opt(&[
            "REGISTRY__STORAGE__FS__GC__DISCOVERY__MAX_DISCOVERY_RETAINED_PATH_BYTES",
            "REGISTRY_BLOB_GC_DISCOVERY_MAX_DISCOVERY_RETAINED_PATH_BYTES",
        ])?
        .or(file_cfg
            .storage
            .fs
            .gc
            .discovery
            .max_discovery_retained_path_bytes)
        .unwrap_or(10_485_760);

        let fs_gc_discovery_intermediate_dir_max_entries = env_usize_opt(&[
            "REGISTRY__STORAGE__FS__GC__DISCOVERY__INTERMEDIATE_DIR_MAX_ENTRIES",
            "REGISTRY_BLOB_GC_DISCOVERY_INTERMEDIATE_DIR_MAX_ENTRIES",
        ])?
        .or(file_cfg
            .storage
            .fs
            .gc
            .discovery
            .intermediate_dir_max_entries)
        .unwrap_or(1_000);

        let fs_gc_discovery_intermediate_dir_max_name_bytes = env_usize_opt(&[
            "REGISTRY__STORAGE__FS__GC__DISCOVERY__INTERMEDIATE_DIR_MAX_NAME_BYTES",
            "REGISTRY_BLOB_GC_DISCOVERY_INTERMEDIATE_DIR_MAX_NAME_BYTES",
        ])?
        .or(file_cfg
            .storage
            .fs
            .gc
            .discovery
            .intermediate_dir_max_name_bytes)
        .unwrap_or(100_000);

        let fs_gc_discovery_max_terminal_dir_enumerations = env_usize_opt(&[
            "REGISTRY__STORAGE__FS__GC__DISCOVERY__MAX_TERMINAL_DIR_ENUMERATIONS",
            "REGISTRY_BLOB_GC_DISCOVERY_MAX_TERMINAL_DIR_ENUMERATIONS",
        ])?
        .or(file_cfg
            .storage
            .fs
            .gc
            .discovery
            .max_terminal_dir_enumerations)
        .unwrap_or(10_000);

        let fs_gc_discovery_terminal_dir_max_entries = env_usize_opt(&[
            "REGISTRY__STORAGE__FS__GC__DISCOVERY__TERMINAL_DIR_MAX_ENTRIES",
            "REGISTRY_BLOB_GC_DISCOVERY_TERMINAL_DIR_MAX_ENTRIES",
        ])?
        .or(file_cfg.storage.fs.gc.discovery.terminal_dir_max_entries)
        .unwrap_or(10_000);

        let fs_gc_discovery_terminal_dir_max_name_bytes = env_usize_opt(&[
            "REGISTRY__STORAGE__FS__GC__DISCOVERY__TERMINAL_DIR_MAX_NAME_BYTES",
            "REGISTRY_BLOB_GC_DISCOVERY_TERMINAL_DIR_MAX_NAME_BYTES",
        ])?
        .or(file_cfg.storage.fs.gc.discovery.terminal_dir_max_name_bytes)
        .unwrap_or(1_500_000);

        let fs_gc_discovery_max_total_manifest_entries = env_usize_opt(&[
            "REGISTRY__STORAGE__FS__GC__DISCOVERY__MAX_TOTAL_MANIFEST_ENTRIES",
            "REGISTRY_BLOB_GC_DISCOVERY_MAX_TOTAL_MANIFEST_ENTRIES",
        ])?
        .or(file_cfg.storage.fs.gc.discovery.max_total_manifest_entries)
        .unwrap_or(250_000);

        let fs_gc_discovery_max_manifests_read = env_usize_opt(&[
            "REGISTRY__STORAGE__FS__GC__DISCOVERY__MAX_MANIFESTS_READ",
            "REGISTRY_BLOB_GC_DISCOVERY_MAX_MANIFESTS_READ",
        ])?
        .or(file_cfg.storage.fs.gc.discovery.max_manifests_read)
        .unwrap_or(50_000);

        let fs_gc_discovery_max_total_references = env_usize_opt(&[
            "REGISTRY__STORAGE__FS__GC__DISCOVERY__MAX_TOTAL_REFERENCES",
            "REGISTRY_BLOB_GC_DISCOVERY_MAX_TOTAL_REFERENCES",
        ])?
        .or(file_cfg.storage.fs.gc.discovery.max_total_references)
        .unwrap_or(250_000);

        let fs_gc_discovery_max_retained_logical_bytes = env_usize_opt(&[
            "REGISTRY__STORAGE__FS__GC__DISCOVERY__MAX_RETAINED_LOGICAL_BYTES",
            "REGISTRY_BLOB_GC_DISCOVERY_MAX_RETAINED_LOGICAL_BYTES",
        ])?
        .or(file_cfg.storage.fs.gc.discovery.max_retained_logical_bytes)
        .unwrap_or(33_554_432);

        let fs_gc_discovery_max_manifest_payload_bytes = env_u64_opt(&[
            "REGISTRY__STORAGE__FS__GC__DISCOVERY__MAX_MANIFEST_PAYLOAD_BYTES",
            "REGISTRY_BLOB_GC_DISCOVERY_MAX_MANIFEST_PAYLOAD_BYTES",
        ])?
        .or(file_cfg.storage.fs.gc.discovery.max_manifest_payload_bytes);

        if storage_backend == StorageBackend::Filesystem {
            if fs_manifest_listing_max_entries
                < crate::storage::fs::manifest_listing::MIN_MANIFEST_LISTING_ENTRIES
            {
                return Err(ConfigError::InvalidValue {
                    field: "manifest_listing_max_entries",
                    message: "must be at least 1".to_string(),
                });
            }
            if fs_manifest_listing_max_name_bytes
                < crate::storage::fs::manifest_listing::MIN_MANIFEST_LISTING_NAME_BYTES
            {
                return Err(ConfigError::InvalidValue {
                    field: "manifest_listing_max_name_bytes",
                    message: "must be at least 128".to_string(),
                });
            }

            if fs_tag_listing_max_entries < crate::storage::fs::tag_listing::MIN_TAG_LISTING_ENTRIES
            {
                return Err(ConfigError::InvalidValue {
                    field: "tag_listing_max_entries",
                    message: "must be at least 1".to_string(),
                });
            }
            if fs_tag_listing_max_name_bytes
                < crate::storage::fs::tag_listing::MIN_TAG_LISTING_NAME_BYTES
            {
                return Err(ConfigError::InvalidValue {
                    field: "tag_listing_max_name_bytes",
                    message: "must be at least 128".to_string(),
                });
            }
            if fs_tag_listing_repo_probe_max_entries
                < crate::storage::fs::tag_listing::MIN_TAG_LISTING_REPO_PROBE_ENTRIES
            {
                return Err(ConfigError::InvalidValue {
                    field: "tag_listing_repo_probe_max_entries",
                    message: "must be at least 1".to_string(),
                });
            }
            if fs_tag_listing_repo_probe_max_name_bytes
                < crate::storage::fs::tag_listing::MIN_TAG_LISTING_REPO_PROBE_NAME_BYTES
            {
                return Err(ConfigError::InvalidValue {
                    field: "tag_listing_repo_probe_max_name_bytes",
                    message: "must be at least 64".to_string(),
                });
            }
            if fs_tag_listing_max_payload_bytes
                < crate::storage::fs::tag_listing::MIN_TAG_LISTING_PAYLOAD_BYTES
                || fs_tag_listing_max_payload_bytes == u64::MAX
            {
                return Err(ConfigError::InvalidValue {
                    field: "tag_listing_max_payload_bytes",
                    message: "must be at least 256 and less than u64::MAX".to_string(),
                });
            }

            if fs_gc_discovery_max_depth < 1 {
                return Err(ConfigError::InvalidValue {
                    field: "storage.fs.gc.discovery.max_depth",
                    message: "must be at least 1".to_string(),
                });
            }
            if fs_gc_discovery_max_dir_enumerations < 1 {
                return Err(ConfigError::InvalidValue {
                    field: "storage.fs.gc.discovery.max_dir_enumerations",
                    message: "must be at least 1".to_string(),
                });
            }
            if fs_gc_discovery_max_total_discovery_entries < 1 {
                return Err(ConfigError::InvalidValue {
                    field: "storage.fs.gc.discovery.max_total_discovery_entries",
                    message: "must be at least 1".to_string(),
                });
            }
            if fs_gc_discovery_max_manifest_dirs < 1 {
                return Err(ConfigError::InvalidValue {
                    field: "storage.fs.gc.discovery.max_manifest_dirs",
                    message: "must be at least 1".to_string(),
                });
            }
            if fs_gc_discovery_max_discovery_retained_path_bytes < 65536 {
                return Err(ConfigError::InvalidValue {
                    field: "storage.fs.gc.discovery.max_discovery_retained_path_bytes",
                    message: "must be at least 65536 bytes".to_string(),
                });
            }
            if fs_gc_discovery_intermediate_dir_max_entries < 1 {
                return Err(ConfigError::InvalidValue {
                    field: "storage.fs.gc.discovery.intermediate_dir_max_entries",
                    message: "must be at least 1".to_string(),
                });
            }
            if fs_gc_discovery_intermediate_dir_max_name_bytes < 128 {
                return Err(ConfigError::InvalidValue {
                    field: "storage.fs.gc.discovery.intermediate_dir_max_name_bytes",
                    message: "must be at least 128 bytes".to_string(),
                });
            }
            if fs_gc_discovery_max_terminal_dir_enumerations < 1 {
                return Err(ConfigError::InvalidValue {
                    field: "storage.fs.gc.discovery.max_terminal_dir_enumerations",
                    message: "must be at least 1".to_string(),
                });
            }
            if fs_gc_discovery_terminal_dir_max_entries < 1 {
                return Err(ConfigError::InvalidValue {
                    field: "storage.fs.gc.discovery.terminal_dir_max_entries",
                    message: "must be at least 1".to_string(),
                });
            }
            if fs_gc_discovery_terminal_dir_max_name_bytes < 128 {
                return Err(ConfigError::InvalidValue {
                    field: "storage.fs.gc.discovery.terminal_dir_max_name_bytes",
                    message: "must be at least 128 bytes".to_string(),
                });
            }
            if fs_gc_discovery_max_total_manifest_entries < 1 {
                return Err(ConfigError::InvalidValue {
                    field: "storage.fs.gc.discovery.max_total_manifest_entries",
                    message: "must be at least 1".to_string(),
                });
            }
            if fs_gc_discovery_max_manifests_read < 1 {
                return Err(ConfigError::InvalidValue {
                    field: "storage.fs.gc.discovery.max_manifests_read",
                    message: "must be at least 1".to_string(),
                });
            }
            if fs_gc_discovery_max_total_references < 1 {
                return Err(ConfigError::InvalidValue {
                    field: "storage.fs.gc.discovery.max_total_references",
                    message: "must be at least 1".to_string(),
                });
            }
            if fs_gc_discovery_max_retained_logical_bytes < 131072 {
                return Err(ConfigError::InvalidValue {
                    field: "storage.fs.gc.discovery.max_retained_logical_bytes",
                    message: "must be at least 131072 bytes".to_string(),
                });
            }
            if let Some(ceiling) = fs_gc_discovery_max_manifest_payload_bytes
                && (ceiling < 1024 || ceiling == u64::MAX) {
                    return Err(ConfigError::InvalidValue {
                        field: "storage.fs.gc.discovery.max_manifest_payload_bytes",
                        message: "must be at least 1024 and less than u64::MAX".to_string(),
                    });
                }
        }

        let admin_api_enabled =
            env_bool_opt(&["REGISTRY__ADMIN_API__ENABLED", "ADMIN_API_ENABLED"])?
                .or(file_cfg.admin_api.enabled)
                .unwrap_or(false);
        let admin_api_username =
            env_str_opt(&["REGISTRY__ADMIN_API__USERNAME", "ADMIN_API_USERNAME"])
                .or_else(|| file_cfg.admin_api.username.clone());
        let admin_api_password =
            env_str_opt(&["REGISTRY__ADMIN_API__PASSWORD", "ADMIN_API_PASSWORD"])
                .or_else(|| file_cfg.admin_api.password.clone());

        let s3_endpoint = env_str_opt(&["REGISTRY__STORAGE__S3__ENDPOINT", "STORAGE_S3_ENDPOINT"])
            .or_else(|| file_cfg.storage.s3.endpoint.clone());
        let s3_region = env_str_opt(&["REGISTRY__STORAGE__S3__REGION", "STORAGE_S3_REGION"])
            .or_else(|| file_cfg.storage.s3.region.clone());
        let s3_bucket = env_str_opt(&["REGISTRY__STORAGE__S3__BUCKET", "STORAGE_S3_BUCKET"])
            .or_else(|| file_cfg.storage.s3.bucket.clone());
        let s3_prefix = env_str_opt(&["REGISTRY__STORAGE__S3__PREFIX", "STORAGE_S3_PREFIX"])
            .or_else(|| file_cfg.storage.s3.prefix.clone())
            .unwrap_or_else(|| "registry".to_string());
        let s3_single_instance_mode = env_bool_opt(&[
            "REGISTRY__STORAGE__S3__SINGLE_INSTANCE_MODE",
            "S3_SINGLE_INSTANCE_MODE",
        ])?
        .or(file_cfg.storage.s3.single_instance_mode)
        .unwrap_or(false);

        let s3_lease_duration_secs = env_u64_opt(&[
            "REGISTRY__UPLOADS__S3__LEASE_DURATION_SECS",
            "S3_LEASE_DURATION_SECS",
        ])?
        .or(file_cfg.uploads.s3.lease_duration_secs)
        .unwrap_or(300);

        let s3_lease_renewal_interval_secs = env_u64_opt(&[
            "REGISTRY__UPLOADS__S3__LEASE_RENEWAL_INTERVAL_SECS",
            "S3_LEASE_RENEWAL_INTERVAL_SECS",
        ])?
        .or(file_cfg.uploads.s3.lease_renewal_interval_secs)
        .unwrap_or(60);

        let s3_max_retry_attempts = env_usize_opt(&[
            "REGISTRY__UPLOADS__S3__MAX_RETRY_ATTEMPTS",
            "S3_MAX_RETRY_ATTEMPTS",
        ])?
        .map(|v| v as u32)
        .or(file_cfg.uploads.s3.max_retry_attempts)
        .unwrap_or(3);

        let s3_legacy_multipart_cleanup_policy_str = env_str_opt(&[
            "REGISTRY__UPLOADS__S3__LEGACY_MULTIPART_CLEANUP_POLICY",
            "S3_LEGACY_MULTIPART_CLEANUP_POLICY",
        ])
        .or_else(|| file_cfg.uploads.s3.legacy_multipart_cleanup_policy.clone());

        let s3_legacy_multipart_cleanup_policy = match s3_legacy_multipart_cleanup_policy_str
            .as_deref()
        {
            None | Some("disabled") | Some("") => LegacyMultipartCleanupPolicy::Disabled,
            Some("current_format_only") => LegacyMultipartCleanupPolicy::CurrentFormatOnly,
            Some("operator_confirmed_all_unknown") => {
                LegacyMultipartCleanupPolicy::OperatorConfirmedAllUnknown
            }
            Some(other) => {
                return Err(ConfigError::InvalidValue {
                    field: "uploads.s3.legacy_multipart_cleanup_policy",
                    message: format!(
                        "invalid policy '{other}': expected 'disabled', 'current_format_only', or 'operator_confirmed_all_unknown'"
                    ),
                });
            }
        };

        let upload_receipt_lifetime_secs = env_u64_opt(&[
            "REGISTRY__UPLOADS__RECEIPT_LIFETIME_SECS",
            "UPLOAD_RECEIPT_LIFETIME_SECS",
        ])?
        .or(file_cfg.uploads.receipt_lifetime_secs)
        .unwrap_or(72 * 3600);

        let gc_pin_duration_secs = env_u64_opt(&[
            "REGISTRY__UPLOADS__GC_PIN_DURATION_SECS",
            "GC_PIN_DURATION_SECS",
        ])?
        .or(file_cfg.uploads.gc_pin_duration_secs)
        .unwrap_or(3600);

        let ref_index_enabled = env_bool_opt(&[
            "REGISTRY__STORAGE__REF_INDEX__ENABLED",
            "STORAGE_REF_INDEX_ENABLED",
        ])?
        .or(file_cfg.storage.ref_index.enabled)
        .unwrap_or(true);

        let ref_index_path = env_str_opt(&[
            "REGISTRY__STORAGE__REF_INDEX__PATH",
            "STORAGE_REF_INDEX_PATH",
        ])
        .or_else(|| file_cfg.storage.ref_index.path.clone())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| fs_root.join("ref-index"));

        let ref_index_rebuild_on_start = env_bool_opt(&[
            "REGISTRY__STORAGE__REF_INDEX__REBUILD_ON_START",
            "STORAGE_REF_INDEX_REBUILD_ON_START",
        ])?
        .or(file_cfg.storage.ref_index.rebuild_on_start)
        .unwrap_or(false);

        let ref_index_auto_rebuild_on_corruption = env_bool_opt(&[
            "REGISTRY__STORAGE__REF_INDEX__AUTO_REBUILD_ON_CORRUPTION",
            "STORAGE_REF_INDEX_AUTO_REBUILD_ON_CORRUPTION",
        ])?
        .or(file_cfg.storage.ref_index.auto_rebuild_on_corruption)
        .unwrap_or(true);

        let allow_tag_overwrite = env_bool_opt(&[
            "REGISTRY__FEATURES__ALLOW_TAG_OVERWRITE",
            "ALLOW_TAG_OVERWRITE",
        ])?
        .or(file_cfg.features.allow_tag_overwrite)
        .unwrap_or(!best_practice);

        let automatic_crossmount = env_bool_opt(&[
            "REGISTRY__FEATURES__AUTOMATIC_CROSSMOUNT",
            "REGISTRY_AUTOMATIC_CROSSMOUNT",
        ])?
        .or(file_cfg.features.automatic_crossmount)
        .unwrap_or(false);

        let upload_gc_enabled =
            env_bool_opt(&["REGISTRY__UPLOADS__GC_ENABLED", "UPLOAD_GC_ENABLED"])?
                .or(file_cfg.uploads.gc_enabled)
                .unwrap_or(true);

        let upload_gc_interval_secs = env_u64_opt(&[
            "REGISTRY__UPLOADS__GC_INTERVAL_SECS",
            "UPLOAD_GC_INTERVAL_SECS",
        ])?
        .or(file_cfg.uploads.gc_interval_secs)
        .unwrap_or(3600);

        let upload_gc_max_age_secs = env_u64_opt(&[
            "REGISTRY__UPLOADS__GC_MAX_AGE_SECS",
            "UPLOAD_GC_MAX_AGE_SECS",
        ])?
        .or(file_cfg.uploads.gc_max_age_secs)
        .unwrap_or(24 * 3600);

        let blob_gc_finalize_grace_secs = env_u64_opt(&[
            "REGISTRY__BLOB_GC__FINALIZE_GRACE_SECS",
            "BLOB_GC_FINALIZE_GRACE_SECS",
        ])?
        .or(file_cfg.blob_gc.finalize_grace_secs)
        .unwrap_or(72 * 3600);

        // Online blob GC safety toggles + defaults.
        // Defaults are conservative and match docs/blob-gc-online.md.
        let blob_gc_enabled = env_bool_opt(&["REGISTRY__BLOB_GC__ENABLED", "BLOB_GC_ENABLED"])?
            .or(file_cfg.blob_gc.enabled)
            .unwrap_or(false);
        let blob_gc_enable_delete =
            env_bool_opt(&["REGISTRY__BLOB_GC__ENABLE_DELETE", "BLOB_GC_ENABLE_DELETE"])?
                .or(file_cfg.blob_gc.enable_delete)
                .unwrap_or(false);
        let blob_gc_default_min_age_secs = env_u64_opt(&[
            "REGISTRY__BLOB_GC__DEFAULT_MIN_AGE_SECS",
            "BLOB_GC_DEFAULT_MIN_AGE_SECS",
        ])?
        .or(file_cfg.blob_gc.default_min_age_secs)
        .unwrap_or(7 * 24 * 3600);
        let blob_gc_default_quarantine_delay_secs = env_u64_opt(&[
            "REGISTRY__BLOB_GC__DEFAULT_QUARANTINE_DELAY_SECS",
            "BLOB_GC_DEFAULT_QUARANTINE_DELAY_SECS",
        ])?
        .or(file_cfg.blob_gc.default_quarantine_delay_secs)
        .unwrap_or(24 * 3600);

        let blob_gc_default_max_blobs = env_usize_opt(&[
            "REGISTRY__BLOB_GC__DEFAULT_MAX_BLOBS",
            "BLOB_GC_DEFAULT_MAX_BLOBS",
        ])?
        .or(file_cfg.blob_gc.default_max_blobs)
        .unwrap_or(1000);
        let blob_gc_default_max_bytes = env_u64_opt(&[
            "REGISTRY__BLOB_GC__DEFAULT_MAX_BYTES",
            "BLOB_GC_DEFAULT_MAX_BYTES",
        ])?
        .or(file_cfg.blob_gc.default_max_bytes)
        .unwrap_or(u64::MAX);
        let blob_gc_default_max_seconds = env_u64_opt(&[
            "REGISTRY__BLOB_GC__DEFAULT_MAX_SECONDS",
            "BLOB_GC_DEFAULT_MAX_SECONDS",
        ])?
        .or(file_cfg.blob_gc.default_max_seconds)
        .unwrap_or(60);

        let blob_gc_schedule_enabled = env_bool_opt(&[
            "REGISTRY__BLOB_GC__SCHEDULE_ENABLED",
            "BLOB_GC_SCHEDULE_ENABLED",
        ])?
        .or(file_cfg.blob_gc.schedule_enabled)
        .unwrap_or(false);

        let blob_gc_schedule_interval_secs = env_u64_opt(&[
            "REGISTRY__BLOB_GC__SCHEDULE_INTERVAL_SECS",
            "BLOB_GC_SCHEDULE_INTERVAL_SECS",
        ])?
        .or(file_cfg.blob_gc.schedule_interval_secs)
        .unwrap_or(7 * 24 * 3600);

        let max_upload_bytes =
            env_u64_opt(&["REGISTRY__LIMITS__MAX_UPLOAD_BYTES", "MAX_UPLOAD_BYTES"])?
                .or(file_cfg.limits.max_upload_bytes)
                .unwrap_or(5 * 1024 * 1024 * 1024);

        let max_request_body_bytes = env_usize_opt(&[
            "REGISTRY__LIMITS__MAX_REQUEST_BODY_BYTES",
            "MAX_REQUEST_BODY_BYTES",
        ])?
        .or(file_cfg.limits.max_request_body_bytes)
        .unwrap_or(32 * 1024 * 1024);

        let upload_chunk_min_bytes = env_usize_opt(&[
            "REGISTRY__LIMITS__UPLOAD_CHUNK_MIN_BYTES",
            "UPLOAD_CHUNK_MIN_BYTES",
        ])?
        .or(file_cfg.limits.upload_chunk_min_bytes);

        let max_concurrent_buffered_requests = env_usize_opt(&[
            "REGISTRY__LIMITS__MAX_CONCURRENT_BUFFERED_REQUESTS",
            "MAX_CONCURRENT_BUFFERED_REQUESTS",
        ])?
        .or(file_cfg.limits.max_concurrent_buffered_requests)
        .unwrap_or(if best_practice { 4 } else { 8 })
        .max(1);

        let max_concurrent_requests = env_usize_opt(&[
            "REGISTRY__LIMITS__MAX_CONCURRENT_REQUESTS",
            "MAX_CONCURRENT_REQUESTS",
        ])?
        .or(file_cfg.limits.max_concurrent_requests)
        .unwrap_or(if best_practice { 64 } else { 256 })
        .max(1);

        let max_concurrent_upload_requests = env_usize_opt(&[
            "REGISTRY__LIMITS__MAX_CONCURRENT_UPLOAD_REQUESTS",
            "MAX_CONCURRENT_UPLOAD_REQUESTS",
        ])?
        .or(file_cfg.limits.max_concurrent_upload_requests)
        // Uploads are long-lived and can easily exhaust file descriptors when clients retry
        // behind proxies; keep the default smaller than the general request limit.
        .unwrap_or(max_concurrent_requests.min(32))
        .max(1);

        let request_timeout_secs = env_u64_opt(&[
            "REGISTRY__TIMEOUTS__REQUEST_TIMEOUT_SECS",
            "REQUEST_TIMEOUT_SECS",
        ])?
        .or(file_cfg.timeouts.request_timeout_secs)
        .unwrap_or(if best_practice { 60 } else { 300 });

        let upload_request_timeout_secs = env_u64_opt(&[
            "REGISTRY__TIMEOUTS__UPLOAD_REQUEST_TIMEOUT_SECS",
            "UPLOAD_REQUEST_TIMEOUT_SECS",
        ])?
        .or(file_cfg.timeouts.upload_request_timeout_secs)
        .unwrap_or(if best_practice { 7200 } else { 3600 });

        let upload_chunk_idle_timeout_secs = env_u64_opt(&[
            "REGISTRY__TIMEOUTS__UPLOAD_CHUNK_IDLE_TIMEOUT_SECS",
            "UPLOAD_CHUNK_IDLE_TIMEOUT_SECS",
        ])?
        .or(file_cfg.timeouts.upload_chunk_idle_timeout_secs)
        .unwrap_or(20);

        let upload_rate_window_secs = env_u64_opt(&[
            "REGISTRY__TIMEOUTS__UPLOAD_RATE_WINDOW_SECS",
            "UPLOAD_RATE_WINDOW_SECS",
        ])?
        .or(file_cfg.timeouts.upload_rate_window_secs)
        .unwrap_or(10);

        let upload_rate_grace_period_secs = env_u64_opt(&[
            "REGISTRY__TIMEOUTS__UPLOAD_RATE_GRACE_PERIOD_SECS",
            "UPLOAD_RATE_GRACE_PERIOD_SECS",
        ])?
        .or(file_cfg.timeouts.upload_rate_grace_period_secs)
        .unwrap_or(15);

        let min_upload_bytes_per_sec = env_u64_opt(&[
            "REGISTRY__TIMEOUTS__MIN_UPLOAD_BYTES_PER_SEC",
            "MIN_UPLOAD_BYTES_PER_SEC",
        ])?
        .or(file_cfg.timeouts.min_upload_bytes_per_sec)
        .unwrap_or(32768);

        let header_read_timeout_secs = env_u64_opt(&[
            "REGISTRY__TIMEOUTS__HEADER_READ_TIMEOUT_SECS",
            "HEADER_READ_TIMEOUT_SECS",
        ])?
        .or(file_cfg.timeouts.header_read_timeout_secs)
        .unwrap_or(10);

        let slow_connection_policy_str = env_str_opt(&[
            "REGISTRY__TIMEOUTS__SLOW_CONNECTION_POLICY",
            "SLOW_CONNECTION_POLICY",
        ])
        .or_else(|| file_cfg.timeouts.slow_connection_policy.clone());

        let slow_connection_policy = match slow_connection_policy_str
            .as_deref()
            .map(str::trim)
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("audit_only") | Some("audit") => SlowConnectionPolicy::AuditOnly,
            Some("disabled") | Some("disable") | Some("off") => SlowConnectionPolicy::Disabled,
            _ => SlowConnectionPolicy::Enforce,
        };

        let max_connections_per_ip = env_u64_opt(&[
            "REGISTRY__LIMITS__MAX_CONNECTIONS_PER_IP",
            "MAX_CONNECTIONS_PER_IP",
        ])?
        .map(|v| v as usize)
        .or(file_cfg.limits.max_connections_per_ip)
        .unwrap_or(50);

        let trusted_bypass_cidrs = parse_cidrs_opt(
            env_str_any(&[
                "REGISTRY__LIMITS__TRUSTED_BYPASS_CIDRS",
                "TRUSTED_BYPASS_CIDRS",
            ]),
            file_cfg.limits.trusted_bypass_cidrs.clone(),
        )?;

        let trusted_proxies = parse_cidrs_opt(
            env_str_any(&["REGISTRY__LIMITS__TRUSTED_PROXIES", "TRUSTED_PROXIES"]),
            file_cfg.limits.trusted_proxies.clone(),
        )?;

        let disallow_monolithic_uploads = env_bool_opt(&[
            "REGISTRY__UPLOADS__DISALLOW_MONOLITHIC_UPLOADS",
            "DISALLOW_MONOLITHIC_UPLOADS",
        ])?
        .or(file_cfg.uploads.disallow_monolithic_uploads)
        .unwrap_or(best_practice);

        let uploads_abort_on_error =
            env_bool_opt(&["REGISTRY__UPLOADS__ABORT_ON_ERROR", "UPLOAD_ABORT_ON_ERROR"])?
                .or(file_cfg.uploads.abort_on_error)
                .unwrap_or(false);
        let uploads_abort_on_digest_mismatch = env_bool_opt(&[
            "REGISTRY__UPLOADS__ABORT_ON_DIGEST_MISMATCH",
            "UPLOAD_ABORT_ON_DIGEST_MISMATCH",
        ])?
        .or(file_cfg.uploads.abort_on_digest_mismatch)
        .unwrap_or(false);

        let upload_policy = UploadPolicyConfig {
            abort_on_error: uploads_abort_on_error,
            abort_on_digest_mismatch: uploads_abort_on_digest_mismatch,
            repo_rules: file_cfg
                .uploads
                .repos
                .iter()
                .map(|r| UploadRepoPolicyRule {
                    match_pattern: r.match_pattern.clone(),
                    abort_on_error: r.abort_on_error,
                    abort_on_digest_mismatch: r.abort_on_digest_mismatch,
                })
                .collect(),
        };

        let catalog_requires_auth =
            env_bool_opt(&["REGISTRY__CATALOG__REQUIRES_AUTH", "CATALOG_REQUIRES_AUTH"])?
                .or(file_cfg.catalog.requires_auth)
                .unwrap_or(best_practice);

        let public_url = env_str_opt(&["REGISTRY__SERVER__PUBLIC_URL", "PUBLIC_URL"])
            .or_else(|| file_cfg.server.public_url.clone())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        let token_service = env_str_opt(&["REGISTRY__TOKEN__SERVICE", "TOKEN_SERVICE"])
            .or_else(|| file_cfg.token.service.clone())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "naust".to_string());

        let env_token_signing_key =
            env_str_opt(&["REGISTRY__TOKEN__SIGNING_KEY", "TOKEN_SIGNING_KEY"])
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty());

        let raw_file_signing_keys_count = file_cfg.token.signing_keys.len();

        let file_signing_keys = file_cfg
            .token
            .signing_keys
            .iter()
            .map(|k| crate::security::TokenSigningKey {
                kid: k.kid.trim().to_string(),
                key: k.key.trim().to_string(),
            })
            .filter(|k| !k.kid.is_empty() && !k.key.is_empty())
            .collect::<Vec<_>>();

        if raw_file_signing_keys_count > 0 && file_signing_keys.is_empty() {
            return Err(ConfigError::InvalidValue {
                field: "token.signing_keys",
                message: "present but contains no valid entries (each entry requires non-empty kid and key)".into(),
            });
        }
        if raw_file_signing_keys_count > file_signing_keys.len() {
            eprintln!(
                "Warning: token.signing_keys contains {} invalid entries (missing/empty kid or key); ignoring them",
                raw_file_signing_keys_count - file_signing_keys.len()
            );
        }

        let allow_ephemeral_token_signing_key = env_bool_opt(&[
            "REGISTRY__TOKEN__ALLOW_EPHEMERAL_SIGNING_KEY",
            "TOKEN_ALLOW_EPHEMERAL_SIGNING_KEY",
        ])?
        .or(file_cfg.token.allow_ephemeral_signing_key)
        .unwrap_or(false);

        let mut token_signing_key_ephemeral = false;
        let (token_signing_key, token_signing_keys) = if !file_signing_keys.is_empty() {
            if env_token_signing_key.is_some() {
                eprintln!(
                    "Warning: TOKEN_SIGNING_KEY is set but token.signing_keys is present; ignoring env TOKEN_SIGNING_KEY"
                );
            }

            let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
            for k in &file_signing_keys {
                if !seen.insert(k.kid.clone()) {
                    return Err(ConfigError::InvalidValue {
                        field: "token.signing_keys",
                        message: format!("contains duplicate kid='{}'", k.kid),
                    });
                }
            }

            (file_signing_keys[0].key.clone(), file_signing_keys)
        } else {
            let token_signing_key = match env_token_signing_key
                .or_else(|| file_cfg.token.signing_key.clone())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
            {
                Some(k) => k,
                None => {
                    if best_practice {
                        return Err(ConfigError::MissingRequired {
                            field: "token.signing_key (required in best_practice profile)",
                        });
                    }
                    token_signing_key_ephemeral = true;
                    eprintln!(
                        "Warning: no token signing key configured; generated an ephemeral key. serve and check-config refuse it unless token.allow_ephemeral_signing_key is true"
                    );
                    uuid::Uuid::new_v4().to_string()
                }
            };

            let token_signing_keys = vec![crate::security::TokenSigningKey {
                kid: "default".to_string(),
                key: token_signing_key.clone(),
            }];

            (token_signing_key, token_signing_keys)
        };

        let token_rate_limit_rpm =
            env_u64_opt(&["REGISTRY__TOKEN__RATE_LIMIT_RPM", "TOKEN_RATE_LIMIT_RPM"])?
                .map(|v| v.min(u32::MAX as u64) as u32)
                .or(file_cfg.token.rate_limit_rpm)
                .unwrap_or(1200);
        let token_rate_limit_window_secs = env_u64_opt(&[
            "REGISTRY__TOKEN__RATE_LIMIT_WINDOW_SECS",
            "TOKEN_RATE_LIMIT_WINDOW_SECS",
        ])?
        .or(file_cfg.token.rate_limit_window_secs)
        .unwrap_or(60);

        let token_ttl_secs = env_u64_opt(&["REGISTRY__TOKEN__TTL_SECS", "TOKEN_TTL_SECS"])?
            .or(file_cfg.token.ttl_secs)
            .unwrap_or(600);

        let robots = resolve_robots_config(&file_cfg)?;

        let users = resolve_users_config(&file_cfg)?;

        let proxy_enabled = env_bool_opt(&["REGISTRY__PROXY__ENABLED", "PROXY_ENABLED"])?
            .or(file_cfg.proxy.enabled)
            .unwrap_or(false);

        let proxy_mode_raw =
            env_str_opt(&["REGISTRY__PROXY__MODE", "PROXY_MODE"]).or(file_cfg.proxy.mode.clone());
        let proxy_mode = match proxy_mode_raw
            .as_deref()
            .map(str::trim)
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("any") => ProxyMode::Any,
            _ => ProxyMode::Allowlist,
        };

        let upstream_base_url = env_str_opt(&[
            "REGISTRY__PROXY__UPSTREAM__BASE_URL",
            "PROXY_UPSTREAM_BASE_URL",
        ])
        .or_else(|| file_cfg.proxy.upstream.base_url.clone())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

        let upstream_username = env_str_opt(&[
            "REGISTRY__PROXY__UPSTREAM__USERNAME",
            "PROXY_UPSTREAM_USERNAME",
        ])
        .or_else(|| file_cfg.proxy.upstream.username.clone())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
        let upstream_password = env_str_opt(&[
            "REGISTRY__PROXY__UPSTREAM__PASSWORD",
            "PROXY_UPSTREAM_PASSWORD",
        ])
        .or_else(|| file_cfg.proxy.upstream.password.clone())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

        let allowed_upstream_hosts = env_str_opt(&[
            "REGISTRY__PROXY__SAFETY__ALLOWED_UPSTREAM_HOSTS",
            "PROXY_ALLOWED_UPSTREAM_HOSTS",
        ])
        .map(|s| {
            s.split(',')
                .map(|p| p.trim().to_string())
                .filter(|p| !p.is_empty())
                .collect::<Vec<_>>()
        })
        .filter(|v| !v.is_empty())
        .or_else(|| file_cfg.proxy.safety.allowed_upstream_hosts.clone())
        .unwrap_or_default();

        let token_realm_hosts = normalize_host_list(
            env_str_opt(&[
                "REGISTRY__PROXY__SAFETY__TOKEN_REALM_HOSTS",
                "PROXY_TOKEN_REALM_HOSTS",
            ])
            .map(|s| {
                s.split(',')
                    .map(|p| p.trim().to_string())
                    .filter(|p| !p.is_empty())
                    .collect::<Vec<_>>()
            })
            .filter(|v| !v.is_empty())
            .or_else(|| file_cfg.proxy.safety.token_realm_hosts.clone())
            .unwrap_or_default(),
        );

        let allowed_repo_prefixes_raw = env_str_opt(&[
            "REGISTRY__PROXY__SAFETY__ALLOWED_REPO_PREFIXES",
            "PROXY_ALLOWED_REPO_PREFIXES",
        ])
        .map(|s| {
            s.split(',')
                .map(|p| p.trim().to_string())
                .filter(|p| !p.is_empty())
                .collect::<Vec<_>>()
        })
        .filter(|v| !v.is_empty())
        .or_else(|| file_cfg.proxy.safety.allowed_repo_prefixes.clone())
        .unwrap_or_default();

        let mut allowed_repo_prefixes = Vec::new();
        for p in allowed_repo_prefixes_raw {
            let prefix = crate::proxy::ProxyAllowedPrefix::parse(&p).map_err(|e| {
                ConfigError::InvalidValue {
                    field: "proxy.safety.allowed_repo_prefixes",
                    message: format!("invalid allowed_repo_prefix '{p}': {e}"),
                }
            })?;
            allowed_repo_prefixes.push(prefix);
        }

        let block_private_networks = env_bool_opt(&[
            "REGISTRY__PROXY__SAFETY__BLOCK_PRIVATE_NETWORKS",
            "PROXY_BLOCK_PRIVATE_NETWORKS",
        ])?
        .or(file_cfg.proxy.safety.block_private_networks)
        .unwrap_or(true);

        let redirect_policy = env_str_opt(&[
            "REGISTRY__PROXY__SAFETY__REDIRECT_POLICY",
            "PROXY_REDIRECT_POLICY",
        ])
        .and_then(|s| s.parse::<RedirectPolicy>().ok())
        .or(file_cfg.proxy.safety.redirect_policy)
        .unwrap_or_default();

        let max_concurrent_upstream = env_usize_opt(&[
            "REGISTRY__PROXY__SAFETY__MAX_CONCURRENT_UPSTREAM",
            "PROXY_MAX_CONCURRENT_UPSTREAM",
        ])?
        .or(file_cfg.proxy.safety.max_concurrent_upstream)
        .unwrap_or(16);

        let cache_fs_root =
            env_str_opt(&["REGISTRY__PROXY__CACHE__FS_ROOT", "PROXY_CACHE_FS_ROOT"])
                .or_else(|| file_cfg.proxy.cache.fs_root.clone())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .map(PathBuf::from)
                .or_else(|| {
                    if storage_backend == StorageBackend::Filesystem {
                        Some(fs_root.join("cache"))
                    } else {
                        None
                    }
                });

        let cache_s3_prefix =
            env_str_opt(&["REGISTRY__PROXY__CACHE__S3_PREFIX", "PROXY_CACHE_S3_PREFIX"])
                .or_else(|| file_cfg.proxy.cache.s3_prefix.clone())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .or_else(|| {
                    if storage_backend == StorageBackend::S3 {
                        Some(format!("{}/cache", s3_prefix.trim_end_matches('/')))
                    } else {
                        None
                    }
                });

        let index_path = env_str_opt(&["REGISTRY__PROXY__CACHE__INDEX_PATH", "PROXY_INDEX_PATH"])
            .or_else(|| file_cfg.proxy.cache.index_path.clone())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .or_else(|| cache_fs_root.as_ref().map(|p| p.join("proxy-index")))
            .unwrap_or_else(|| PathBuf::from("./data/cache/proxy-index"));

        let max_cache_bytes = env_u64_opt(&[
            "REGISTRY__PROXY__CACHE__MAX_CACHE_BYTES",
            "PROXY_MAX_CACHE_BYTES",
        ])?
        .or(file_cfg.proxy.cache.max_cache_bytes);

        let gc_interval_secs = env_u64_opt(&[
            "REGISTRY__PROXY__CACHE__GC_INTERVAL_SECS",
            "PROXY_GC_INTERVAL_SECS",
        ])?
        .or(file_cfg.proxy.cache.gc_interval_secs)
        .unwrap_or(3600);

        let scrub_enabled = env_bool_opt(&[
            "REGISTRY__PROXY__CACHE__SCRUB_ENABLED",
            "PROXY_SCRUB_ENABLED",
        ])?
        .or(file_cfg.proxy.cache.scrub_enabled)
        .unwrap_or(false);

        let scrub_interval_secs = env_u64_opt(&[
            "REGISTRY__PROXY__CACHE__SCRUB_INTERVAL_SECS",
            "PROXY_SCRUB_INTERVAL_SECS",
        ])?
        .or(file_cfg.proxy.cache.scrub_interval_secs)
        .unwrap_or(3600)
        .max(1);

        let scrub_max_files_per_run = env_usize_opt(&[
            "REGISTRY__PROXY__CACHE__SCRUB_MAX_FILES_PER_RUN",
            "PROXY_SCRUB_MAX_FILES_PER_RUN",
        ])?
        .or(file_cfg.proxy.cache.scrub_max_files_per_run)
        .unwrap_or(2000)
        .max(1);

        let mut repo_rules = Vec::new();
        for r in &file_cfg.proxy.repos {
            let tag_policy = match r.tag_policy {
                FileTagPolicy::DigestOnly => TagPolicy::DigestOnly,
                FileTagPolicy::TtlSeconds(s) => TagPolicy::TtlSeconds(s),
                FileTagPolicy::AlwaysRevalidate => TagPolicy::AlwaysRevalidate,
            };
            let eviction_policy = match &r.eviction {
                FileEvictionPolicy::Default => EvictionPolicy::Default,
                FileEvictionPolicy::KeepTags { tags } => EvictionPolicy::KeepTags(tags.clone()),
                FileEvictionPolicy::KeepLatestCachedSemver {
                    tag_regex,
                    allow_prerelease,
                } => EvictionPolicy::KeepLatestCachedSemver {
                    tag_regex: tag_regex.clone(),
                    allow_prerelease: allow_prerelease.unwrap_or(false),
                },
            };
            let match_pattern =
                crate::proxy::ProxyRepoPattern::parse(&r.match_pattern).map_err(|e| {
                    ConfigError::InvalidValue {
                        field: "proxy.repos.match_pattern",
                        message: format!(
                            "invalid proxy repo match_pattern '{}': {e}",
                            r.match_pattern
                        ),
                    }
                })?;
            let upstream_repo = match &r.upstream_repo {
                Some(s) if !s.trim().is_empty() => {
                    let canon = CanonicalRepoName::parse(s.trim()).map_err(|e| {
                        ConfigError::InvalidValue {
                            field: "proxy.repos.upstream_repo",
                            message: format!("invalid upstream_repo '{s}': {e}"),
                        }
                    })?;
                    Some(canon)
                }
                _ => None,
            };
            repo_rules.push(ProxyRepoRule {
                match_pattern,
                upstream_repo,
                tag_policy,
                eviction_policy,
            });
        }

        let routing_proxy_hosts_raw = env_str_opt(&[
            "REGISTRY__PROXY__ROUTING__PROXY_HOSTS",
            "PROXY_ROUTING_PROXY_HOSTS",
        ])
        .map(|s| {
            s.split(',')
                .map(|p| p.trim().to_string())
                .filter(|p| !p.is_empty())
                .collect::<Vec<_>>()
        })
        .filter(|v| !v.is_empty())
        .or_else(|| file_cfg.proxy.routing.proxy_hosts.clone())
        .unwrap_or_default();

        let mut routing_proxy_hosts = Vec::new();
        for h in routing_proxy_hosts_raw {
            let pat = crate::proxy::ProxyHostPattern::parse(&h).map_err(|e| {
                ConfigError::InvalidValue {
                    field: "proxy.routing.proxy_hosts",
                    message: format!("invalid host pattern '{h}': {e}"),
                }
            })?;
            routing_proxy_hosts.push(pat);
        }

        let routing_trust_x_forwarded_host = env_bool_opt(&[
            "REGISTRY__PROXY__ROUTING__TRUST_X_FORWARDED_HOST",
            "PROXY_ROUTING_TRUST_X_FORWARDED_HOST",
        ])?
        .or(file_cfg.proxy.routing.trust_x_forwarded_host)
        .unwrap_or(false);

        let upstreams = resolve_proxy_upstreams(
            &storage_backend,
            &fs_root,
            &s3_prefix,
            routing_trust_x_forwarded_host,
            &file_cfg.proxy.safety,
            &file_cfg.proxy.upstreams,
        )?;

        if proxy_enabled {
            // Either single-upstream mode (proxy.upstream.*) or multi-upstream mode (proxy.upstreams).
            if upstream_base_url.is_none() && upstreams.is_empty() {
                return Err(ConfigError::MissingRequired {
                    field: "proxy.upstream.base_url (or PROXY_UPSTREAM_BASE_URL) OR proxy.upstreams[]",
                });
            }
            // In single-upstream mode, keep the existing requirement.
            if upstreams.is_empty() && max_cache_bytes.is_none() {
                return Err(ConfigError::MissingRequired {
                    field: "proxy.cache.max_cache_bytes (or PROXY_MAX_CACHE_BYTES)",
                });
            }
        }

        let proxy = ProxyConfig {
            enabled: proxy_enabled,
            mode: proxy_mode,
            upstream_base_url,
            upstream_username,
            upstream_password,
            allowed_upstream_hosts,
            token_realm_hosts,
            allowed_repo_prefixes,
            block_private_networks,
            redirect_policy,
            max_concurrent_upstream,
            index_path,
            cache_fs_root,
            cache_s3_prefix,
            gc_interval_secs,
            scrub_enabled,
            scrub_interval_secs,
            scrub_max_files_per_run,
            max_cache_bytes,
            repo_rules,
            upstreams,
            routing_proxy_hosts,
            routing_trust_x_forwarded_host,
        };

        if storage_backend == StorageBackend::S3 {
            if s3_lease_renewal_interval_secs >= s3_lease_duration_secs {
                return Err(ConfigError::InvalidValue {
                    field: "uploads.s3.lease_renewal_interval_secs",
                    message: format!(
                        "lease_renewal_interval_secs ({s3_lease_renewal_interval_secs}) must be strictly less than lease_duration_secs ({s3_lease_duration_secs})"
                    ),
                });
            }
            if s3_max_retry_attempts == 0 {
                return Err(ConfigError::InvalidValue {
                    field: "uploads.s3.max_retry_attempts",
                    message: "max_retry_attempts must be at least 1".into(),
                });
            }
            if blob_gc_enabled
                && blob_gc_enable_delete
                && ref_index_enabled
                && !s3_single_instance_mode
            {
                return Err(ConfigError::Conflict {
                    message: "Unsafe S3 online GC topology: destructive online GC (blob_gc.enable_delete = true) with S3 storage backend and a process-local Sled index is unsafe in multi-instance topologies. You must explicitly configure 'storage.s3.single_instance_mode = true' (or env 'S3_SINGLE_INSTANCE_MODE=1') to affirm that only a single registry instance operates against this S3 namespace, or disable destructive online GC.".into(),
                });
            }
        }

        if upload_receipt_lifetime_secs < upload_gc_max_age_secs {
            return Err(ConfigError::InvalidValue {
                field: "uploads.receipt_lifetime_secs",
                message: format!(
                    "receipt_lifetime_secs ({upload_receipt_lifetime_secs}) must be >= upload_gc_max_age_secs ({upload_gc_max_age_secs})"
                ),
            });
        }

        let tls_reload_poll_secs = env_u64_opt(&[
            "REGISTRY__SERVER__TLS__RELOAD_POLL_SECS",
            "TLS_RELOAD_POLL_SECS",
        ])?
        .or(file_cfg.server.tls.reload_poll_secs)
        .unwrap_or(300);

        Ok(Self {
            listen_addr,
            tls_cert_path,
            tls_key_path,
            tls_reload_poll_secs,
            tls_acme,
            push_username,
            push_password,
            push_allow_repos,
            push_actions,
            push_implies_delete,
            auth_strategy,
            anonymous_pull,
            private_name_prefixes,
            star_grants_catalog,
            storage_backend,
            fs_root,
            fs_manifest_listing_max_entries,
            fs_manifest_listing_max_name_bytes,
            fs_tag_listing_max_entries,
            fs_tag_listing_max_name_bytes,
            fs_tag_listing_repo_probe_max_entries,
            fs_tag_listing_repo_probe_max_name_bytes,
            fs_tag_listing_max_payload_bytes,
            fs_gc_discovery_max_depth,

            fs_gc_discovery_max_dir_enumerations,
            fs_gc_discovery_max_total_discovery_entries,
            fs_gc_discovery_max_manifest_dirs,
            fs_gc_discovery_max_discovery_retained_path_bytes,
            fs_gc_discovery_intermediate_dir_max_entries,
            fs_gc_discovery_intermediate_dir_max_name_bytes,
            fs_gc_discovery_max_terminal_dir_enumerations,
            fs_gc_discovery_terminal_dir_max_entries,
            fs_gc_discovery_terminal_dir_max_name_bytes,
            fs_gc_discovery_max_total_manifest_entries,
            fs_gc_discovery_max_manifests_read,
            fs_gc_discovery_max_total_references,
            fs_gc_discovery_max_retained_logical_bytes,
            fs_gc_discovery_max_manifest_payload_bytes,
            s3_endpoint,
            s3_region,
            s3_bucket,
            s3_prefix,
            s3_single_instance_mode,
            s3_lease_duration_secs,
            s3_lease_renewal_interval_secs,
            s3_max_retry_attempts,
            s3_legacy_multipart_cleanup_policy,
            upload_receipt_lifetime_secs,
            gc_pin_duration_secs,

            ref_index: RefIndexConfig {
                enabled: ref_index_enabled,
                path: ref_index_path,
                rebuild_on_start: ref_index_rebuild_on_start,
                auto_rebuild_on_corruption: ref_index_auto_rebuild_on_corruption,
            },
            allow_tag_overwrite,
            automatic_crossmount,
            upload_gc_enabled,
            upload_gc_interval_secs,
            upload_gc_max_age_secs,
            blob_gc_finalize_grace_secs,
            blob_gc_enabled,
            blob_gc_enable_delete,
            blob_gc_default_min_age_secs,
            blob_gc_default_quarantine_delay_secs,
            blob_gc_default_max_blobs,
            blob_gc_default_max_bytes,
            blob_gc_default_max_seconds,
            blob_gc_schedule_enabled,
            blob_gc_schedule_interval_secs,
            admin_api: AdminApiConfig {
                enabled: admin_api_enabled,
                username: admin_api_username,
                password: admin_api_password,
            },
            max_upload_bytes,
            max_request_body_bytes,
            upload_chunk_min_bytes,
            max_concurrent_buffered_requests,
            max_concurrent_requests,
            max_concurrent_upload_requests,
            request_timeout_secs,
            upload_request_timeout_secs,
            upload_chunk_idle_timeout_secs,
            upload_rate_window_secs,
            upload_rate_grace_period_secs,
            min_upload_bytes_per_sec,
            header_read_timeout_secs,
            slow_connection_policy,

            max_connections_per_ip,
            trusted_bypass_cidrs,
            trusted_proxies,

            disallow_monolithic_uploads,
            upload_policy,
            catalog_requires_auth,
            public_url,
            token_service,
            token_signing_key,
            token_signing_keys,
            token_signing_key_ephemeral,
            allow_ephemeral_token_signing_key,
            token_ttl_secs,
            token_rate_limit_rpm,
            token_rate_limit_window_secs,

            robots,
            users,

            proxy,
        })
    }

    pub fn resolved_upload_policy_for_repo(&self, repo: &str) -> ResolvedUploadPolicy {
        for rule in &self.upload_policy.repo_rules {
            if crate::glob::wildcard_match(&rule.match_pattern, repo) {
                return ResolvedUploadPolicy {
                    abort_on_error: rule
                        .abort_on_error
                        .unwrap_or(self.upload_policy.abort_on_error),
                    abort_on_digest_mismatch: rule
                        .abort_on_digest_mismatch
                        .unwrap_or(self.upload_policy.abort_on_digest_mismatch),
                };
            }
        }
        ResolvedUploadPolicy {
            abort_on_error: self.upload_policy.abort_on_error,
            abort_on_digest_mismatch: self.upload_policy.abort_on_digest_mismatch,
        }
    }
    /// `serve` and `check-config` refuse a generated signing key unless the
    /// operator opted in (ADR-014).
    pub fn ensure_server_token_key(&self) -> Result<(), ConfigError> {
        if self.token_signing_key_ephemeral && !self.allow_ephemeral_token_signing_key {
            Err(ConfigError::MissingRequired {
                field: "token.signing_key (set token.signing_key or token.signing_keys; token.allow_ephemeral_signing_key is for local development only)",
            })
        } else if self.token_signing_keys.is_empty() || self.token_signing_key.is_empty() {
            Err(ConfigError::MissingRequired {
                field: "token.signing_key",
            })
        } else {
            Ok(())
        }
    }

    pub fn auth_configured(&self) -> bool {
        !self.anonymous_pull
            || (self.push_username.is_some() && self.push_password.is_some())
            || (self.robots.enabled && !self.robots.accounts.is_empty())
            || (self.users.enabled && !self.users.accounts.is_empty())
    }

    /// KI-17 (resolved) / ADR-014: the private-name override is explicit
    /// configuration (`auth.private_name_prefixes`), matched on a path boundary.
    /// Angle-bracket/encoding smells stay hardcoded — they indicate injection
    /// attempts, not naming policy.
    pub fn is_repo_private(&self, repo: &str) -> bool {
        naust_auth::AuthConfig::is_repo_private_with_prefixes(
            self.anonymous_pull,
            &self.private_name_prefixes,
            repo,
        )
    }
}

fn normalize_host_list(hosts: Vec<String>) -> Vec<String> {
    hosts
        .into_iter()
        .map(|h| h.trim().trim_end_matches('.').to_ascii_lowercase())
        .filter(|h| !h.is_empty())
        .collect()
}

pub fn default_private_name_prefixes() -> Vec<String> {
    ["private", "secret", "protected", "restricted"]
        .into_iter()
        .map(str::to_string)
        .collect()
}

fn merge_toml_value(into: &mut Value, overlay: Value) {
    match (into, overlay) {
        (Value::Table(into_tbl), Value::Table(overlay_tbl)) => {
            for (k, v) in overlay_tbl {
                match into_tbl.get_mut(&k) {
                    Some(existing) => merge_toml_value(existing, v),
                    None => {
                        into_tbl.insert(k, v);
                    }
                }
            }
        }
        // Arrays/lists are replaced wholesale.
        (into_any, overlay_any) => {
            *into_any = overlay_any;
        }
    }
}

fn load_config_files(paths: &[PathBuf]) -> Result<LoadedFileConfig, ConfigError> {
    let mut merged = Value::Table(toml::map::Map::new());
    let mut ignored_paths = Vec::<String>::new();

    for path in paths {
        let contents = std::fs::read_to_string(path).map_err(|err| ConfigError::Io {
            path: path.clone(),
            source: err,
        })?;

        // Validate unknown keys per file.
        let (_cfg, ignored) = parse_toml_config(&contents).map_err(|err| ConfigError::Toml {
            path: Some(path.clone()),
            source: err,
        })?;
        for p in ignored {
            ignored_paths.push(format!("{}: {p}", path.display()));
        }

        let value: Value = contents.parse::<Value>().map_err(|err| ConfigError::Toml {
            path: Some(path.clone()),
            source: err,
        })?;
        merge_toml_value(&mut merged, value);
    }

    let cfg: FileConfig = merged
        .try_into()
        .map_err(|err| ConfigError::Deserialize { source: err })?;
    Ok(LoadedFileConfig { cfg, ignored_paths })
}

fn resolve_users_config(file_cfg: &FileConfig) -> Result<UsersConfig, ConfigError> {
    let groups = file_cfg
        .auth
        .groups
        .iter()
        .map(|g| {
            let mut grants = Vec::with_capacity(g.grants.len());
            for gr in &g.grants {
                let repo_pattern = crate::rbac::RbacRepoPattern::parse(&gr.repo_prefix)
                    .unwrap_or_else(|_| {
                        crate::rbac::RbacRepoPattern::Exact(
                            CanonicalRepoName::parse("invalid-grant").unwrap(),
                        )
                    });
                grants.push(crate::rbac::Grant {
                    repo_pattern,
                    actions: gr
                        .actions
                        .iter()
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect(),
                });
            }
            GroupConfig {
                name: g.name.trim().to_string(),
                grants,
            }
        })
        .filter(|g| !g.name.is_empty())
        .collect::<Vec<_>>();

    // Validate group uniqueness + grants when users are enabled.
    let enabled = file_cfg.auth.users.enabled.unwrap_or(false);
    if enabled {
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for g in &groups {
            if !seen.insert(g.name.clone()) {
                return Err(ConfigError::InvalidValue {
                    field: "auth.groups",
                    message: format!("contains duplicate name='{}'", g.name),
                });
            }
            if let Some(fg) = file_cfg.auth.groups.iter().find(|fg| fg.name == g.name) {
                for gr in &fg.grants {
                    crate::rbac::RbacRepoPattern::parse(&gr.repo_prefix).map_err(|e| {
                        ConfigError::InvalidValue {
                            field: "auth.groups.grants",
                            message: format!(
                                "invalid grant repo_prefix '{}' in group '{}': {e}",
                                gr.repo_prefix, g.name
                            ),
                        }
                    })?;
                }
            }
            crate::rbac::validate_grants(&g.grants).map_err(|e| ConfigError::InvalidValue {
                field: "auth.groups.grants",
                message: format!("invalid grants in group '{}': {:?}", g.name, e),
            })?;
        }
    }

    let users = UsersConfig {
        enabled,
        groups: groups.clone(),
        accounts: file_cfg
            .auth
            .users
            .accounts
            .iter()
            .map(|a| UserAccountConfig {
                name: a.name.trim().to_string(),
                secret_hash: a.secret_hash.trim().to_string(),
                groups: a
                    .groups
                    .iter()
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect(),
                max_ttl_secs: a.max_ttl_secs,
            })
            .filter(|a| !a.name.is_empty())
            .collect(),
    };

    if users.enabled {
        for a in &users.accounts {
            if a.secret_hash.is_empty() {
                return Err(ConfigError::InvalidValue {
                    field: "auth.users.accounts",
                    message: format!("entry '{}' has empty secret_hash", a.name),
                });
            }
            for grp in &a.groups {
                if !groups.iter().any(|g| g.name == *grp) {
                    return Err(ConfigError::InvalidValue {
                        field: "auth.users.accounts",
                        message: format!("account '{}' references unknown group '{}'", a.name, grp),
                    });
                }
            }
        }
    }

    Ok(users)
}

fn resolve_robots_config(file_cfg: &FileConfig) -> Result<RobotsConfig, ConfigError> {
    let enabled = file_cfg.auth.robots.enabled.unwrap_or(false);
    let robots = RobotsConfig {
        enabled,
        accounts: file_cfg
            .auth
            .robots
            .accounts
            .iter()
            .map(|a| RobotAccountConfig {
                name: a.name.trim().to_string(),
                secret_hash: a.secret_hash.trim().to_string(),
                grants: a
                    .grants
                    .iter()
                    .map(|g| {
                        let repo_pattern = crate::rbac::RbacRepoPattern::parse(&g.repo_prefix)
                            .unwrap_or_else(|_| {
                                crate::rbac::RbacRepoPattern::Exact(
                                    CanonicalRepoName::parse("invalid-grant").unwrap(),
                                )
                            });
                        crate::rbac::Grant {
                            repo_pattern,
                            actions: g
                                .actions
                                .iter()
                                .map(|s| s.trim().to_string())
                                .filter(|s| !s.is_empty())
                                .collect(),
                        }
                    })
                    .collect(),
                max_ttl_secs: a.max_ttl_secs,
            })
            .filter(|a| !a.name.is_empty())
            .collect(),
    };

    if robots.enabled {
        let mut seen = std::collections::HashSet::new();
        for a in &robots.accounts {
            if !seen.insert(a.name.clone()) {
                return Err(ConfigError::InvalidValue {
                    field: "auth.robots.accounts",
                    message: format!("contains duplicate robot account name='{}'", a.name),
                });
            }
            if a.secret_hash.is_empty() {
                return Err(ConfigError::InvalidValue {
                    field: "auth.robots.accounts",
                    message: format!("entry '{}' has empty secret_hash", a.name),
                });
            }
            if let Some(fa) = file_cfg
                .auth
                .robots
                .accounts
                .iter()
                .find(|fa| fa.name == a.name)
            {
                for gr in &fa.grants {
                    crate::rbac::RbacRepoPattern::parse(&gr.repo_prefix).map_err(|e| {
                        ConfigError::InvalidValue {
                            field: "auth.robots.accounts.grants",
                            message: format!(
                                "invalid grant repo_prefix '{}' for robot '{}': {e}",
                                gr.repo_prefix, a.name
                            ),
                        }
                    })?;
                }
            }
            crate::rbac::validate_grants(&a.grants).map_err(|e| ConfigError::InvalidValue {
                field: "auth.robots.accounts.grants",
                message: format!("invalid grants for robot '{}': {:?}", a.name, e),
            })?;
        }
    }

    Ok(robots)
}

fn resolve_proxy_upstreams(
    storage_backend: &StorageBackend,
    fs_root: &std::path::Path,
    s3_prefix: &str,
    routing_trust_x_forwarded_host: bool,
    global_safety: &FileProxySafety,
    file_upstreams: &[FileProxyUpstreamRoute],
) -> Result<Vec<ProxyUpstreamRoute>, ConfigError> {
    let mut upstreams = Vec::new();
    for r in file_upstreams.iter() {
        let mut raw_hosts = Vec::new();
        raw_hosts.extend(
            r.hosts
                .iter()
                .map(|h| h.trim().to_string())
                .filter(|h| !h.is_empty()),
        );
        if let Some(extra) = r.routing.hosts.as_ref() {
            raw_hosts.extend(
                extra
                    .iter()
                    .map(|h| h.trim().to_string())
                    .filter(|h| !h.is_empty()),
            );
        }

        if raw_hosts.is_empty() {
            continue;
        }

        let mut hosts = Vec::new();
        for h in raw_hosts {
            let pat = crate::proxy::ProxyHostPattern::parse(&h).map_err(|e| {
                ConfigError::InvalidValue {
                    field: "proxy.upstreams.hosts",
                    message: format!("invalid upstream host pattern '{h}': {e}"),
                }
            })?;
            if !hosts.contains(&pat) {
                hosts.push(pat);
            }
        }

        let upstream_base_url = r
            .upstream
            .base_url
            .as_deref()
            .or(r.base_url.as_deref())
            .unwrap_or("")
            .trim()
            .to_string();

        if upstream_base_url.is_empty() {
            return Err(ConfigError::MissingRequired {
                field: "proxy.upstreams[].upstream.base_url",
            });
        }

        let max_cache_bytes = r.cache.max_cache_bytes.or(r.max_cache_bytes).unwrap_or(0);
        if max_cache_bytes == 0 {
            return Err(ConfigError::MissingRequired {
                field: "proxy.upstreams[].cache.max_cache_bytes",
            });
        }

        let cache_key = cache_key_from_base_url(&upstream_base_url);

        let trust_x_forwarded_host = r
            .routing
            .trust_x_forwarded_host
            .or(r.trust_x_forwarded_host)
            .unwrap_or(routing_trust_x_forwarded_host);

        let derived_cache_fs_root = fs_root.join("cache").join(&cache_key);
        let derived_index_path = derived_cache_fs_root.join("proxy-index");
        let derived_cache_s3_prefix =
            format!("{}/cache/{}", s3_prefix.trim_end_matches('/'), cache_key);

        let upstream_username = r
            .upstream
            .username
            .clone()
            .or_else(|| r.username.clone())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        let upstream_password = r
            .upstream
            .password
            .clone()
            .or_else(|| r.password.clone())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        let cache_fs_root = r
            .cache
            .fs_root
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(PathBuf::from);
        let cache_s3_prefix = r
            .cache
            .s3_prefix
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());

        let allowed_upstream_hosts = r
            .safety
            .allowed_upstream_hosts
            .clone()
            .or_else(|| global_safety.allowed_upstream_hosts.clone())
            .unwrap_or_default();
        let token_realm_hosts = normalize_host_list(
            r.safety
                .token_realm_hosts
                .clone()
                .or_else(|| global_safety.token_realm_hosts.clone())
                .unwrap_or_default(),
        );

        let raw_prefixes = r
            .safety
            .allowed_repo_prefixes
            .clone()
            .or_else(|| global_safety.allowed_repo_prefixes.clone())
            .unwrap_or_default();
        let mut allowed_repo_prefixes = Vec::new();
        for p in raw_prefixes {
            let prefix = crate::proxy::ProxyAllowedPrefix::parse(&p).map_err(|e| {
                ConfigError::InvalidValue {
                    field: "proxy.upstreams.safety.allowed_repo_prefixes",
                    message: format!("invalid allowed_repo_prefix '{p}': {e}"),
                }
            })?;
            allowed_repo_prefixes.push(prefix);
        }

        let block_private_networks = r
            .safety
            .block_private_networks
            .or(global_safety.block_private_networks)
            .unwrap_or(true);

        let redirect_policy = r
            .safety
            .redirect_policy
            .or(global_safety.redirect_policy)
            .unwrap_or_default();
        let max_concurrent_upstream = r
            .safety
            .max_concurrent_upstream
            .or(global_safety.max_concurrent_upstream)
            .unwrap_or(16);

        let cache_fs_root = match storage_backend {
            StorageBackend::Filesystem => {
                Some(cache_fs_root.unwrap_or_else(|| derived_cache_fs_root.clone()))
            }
            StorageBackend::S3 => None,
        };

        let cache_s3_prefix = match storage_backend {
            StorageBackend::Filesystem => None,
            StorageBackend::S3 => Some(
                cache_s3_prefix
                    .clone()
                    .unwrap_or_else(|| derived_cache_s3_prefix.clone()),
            ),
        };

        let index_path = r
            .cache
            .index_path
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| match storage_backend {
                StorageBackend::Filesystem => match &cache_fs_root {
                    Some(root) => root.join("proxy-index"),
                    None => derived_cache_fs_root.join("proxy-index"),
                },
                StorageBackend::S3 => derived_index_path.clone(),
            });

        upstreams.push(ProxyUpstreamRoute {
            hosts,
            trust_x_forwarded_host,
            upstream_base_url,
            upstream_username,
            upstream_password,
            allowed_upstream_hosts,
            token_realm_hosts,
            allowed_repo_prefixes,
            block_private_networks,
            redirect_policy,
            max_concurrent_upstream,
            index_path,
            cache_fs_root,
            cache_s3_prefix,
            max_cache_bytes,
        });
    }

    // Validate: upstream host patterns must be unique across routes.
    let mut seen: std::collections::HashMap<crate::proxy::ProxyHostPattern, usize> =
        std::collections::HashMap::new();
    for (idx, up) in upstreams.iter().enumerate() {
        for host_pat in &up.hosts {
            if let Some(prev) = seen.insert(host_pat.clone(), idx) {
                return Err(ConfigError::InvalidValue {
                    field: "proxy.upstreams.hosts",
                    message: format!(
                        "duplicate host pattern '{host_pat}' across routes (indices {prev} and {idx})"
                    ),
                });
            }
        }
    }

    Ok(upstreams)
}

fn parse_toml_config(contents: &str) -> Result<(FileConfig, Vec<String>), toml::de::Error> {
    let mut ignored_paths = Vec::<String>::new();
    let deser = toml::de::Deserializer::new(contents);
    let cfg = serde_ignored::deserialize(deser, |path| {
        ignored_paths.push(path.to_string());
    })?;
    Ok((cfg, ignored_paths))
}

fn load_config_file() -> Result<LoadedFileConfig, ConfigError> {
    let Some((_key, path)) =
        env_str_any(&["CONFIG_PATH", "REGISTRY__CONFIG_PATH", "REGISTRY_TOML_PATH"])
    else {
        return Ok(LoadedFileConfig::default());
    };
    let path = path.trim();
    if path.is_empty() {
        return Ok(LoadedFileConfig::default());
    }
    let p = PathBuf::from(path);
    let contents = std::fs::read_to_string(&p).map_err(|err| ConfigError::Io {
        path: p.clone(),
        source: err,
    })?;
    let (cfg, ignored_paths) = parse_toml_config(&contents).map_err(|err| ConfigError::Toml {
        path: Some(p),
        source: err,
    })?;
    Ok(LoadedFileConfig { cfg, ignored_paths })
}

fn env_str_any(keys: &[&'static str]) -> Option<(&'static str, String)> {
    for k in keys {
        if let Ok(v) = std::env::var(k) {
            let v = v.trim().to_string();
            if !v.is_empty() {
                return Some((*k, v));
            }
        }
    }
    None
}

fn env_str_opt(keys: &[&'static str]) -> Option<String> {
    env_str_any(keys).map(|(_, v)| v)
}

fn env_bool_opt(keys: &[&'static str]) -> Result<Option<bool>, ConfigError> {
    let Some((key, v)) = env_str_any(keys) else {
        return Ok(None);
    };
    let norm = v.trim().to_ascii_lowercase();
    match norm.as_str() {
        "1" | "true" | "yes" | "on" => Ok(Some(true)),
        "0" | "false" | "no" | "off" => Ok(Some(false)),
        _ => Err(ConfigError::InvalidEnvValue {
            key,
            expected: "boolean (true/false, 1/0, yes/no, on/off)",
        }),
    }
}

fn env_u64_opt(keys: &[&'static str]) -> Result<Option<u64>, ConfigError> {
    let Some((key, v)) = env_str_any(keys) else {
        return Ok(None);
    };
    v.trim()
        .parse::<u64>()
        .map(Some)
        .map_err(|_| ConfigError::InvalidEnvValue {
            key,
            expected: "unsigned 64-bit integer",
        })
}

fn env_usize_opt(keys: &[&'static str]) -> Result<Option<usize>, ConfigError> {
    let Some((key, v)) = env_str_any(keys) else {
        return Ok(None);
    };
    v.trim()
        .parse::<usize>()
        .map(Some)
        .map_err(|_| ConfigError::InvalidEnvValue {
            key,
            expected: "unsigned integer",
        })
}

fn env_socket_addr_opt(keys: &[&'static str]) -> Result<Option<SocketAddr>, ConfigError> {
    let Some((key, v)) = env_str_any(keys) else {
        return Ok(None);
    };
    v.parse::<SocketAddr>()
        .map(Some)
        .map_err(|_| ConfigError::InvalidEnvValue {
            key,
            expected: "valid socket address (e.g. '127.0.0.1:5000' or '[::1]:5000')",
        })
}

fn parse_cidrs_opt(
    env_val: Option<(&'static str, String)>,
    file_val: Option<Vec<String>>,
) -> Result<Vec<ipnet::IpNet>, ConfigError> {
    let mut out = Vec::new();
    if let Some((key, s)) = env_val {
        for token in s.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            if let Ok(net) = token.parse::<ipnet::IpNet>() {
                out.push(net);
            } else if let Ok(ip) = token.parse::<std::net::IpAddr>() {
                out.push(ipnet::IpNet::from(ip));
            } else {
                return Err(ConfigError::InvalidEnvValue {
                    key,
                    expected: "valid IP address or CIDR network",
                });
            }
        }
    } else if let Some(list) = file_val {
        for token in list.iter().map(|s| s.trim()).filter(|s| !s.is_empty()) {
            if let Ok(net) = token.parse::<ipnet::IpNet>() {
                out.push(net);
            } else if let Ok(ip) = token.parse::<std::net::IpAddr>() {
                out.push(ipnet::IpNet::from(ip));
            } else {
                return Err(ConfigError::InvalidValue {
                    field: "trusted_bypass_cidrs / trusted_proxies",
                    message: "contains invalid IP address or CIDR network entry".into(),
                });
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_upstreams_shorthand_parses_and_resolves() {
        let cfg = r#"
[storage]
backend = "fs"
[storage.fs]
root = "./data"

[proxy]
enabled = true
mode = "any"

[[proxy.upstreams]]
hosts = ["dockerhub-cache.local"]
base_url = "https://registry-1.docker.io"
max_cache_bytes = 123

[[proxy.upstreams]]
hosts = ["ghcr-cache.local"]
base_url = "https://ghcr.io"
max_cache_bytes = 456
"#;

        let file_cfg: FileConfig = toml::from_str(cfg).expect("parse toml");

        let upstreams = resolve_proxy_upstreams(
            &StorageBackend::Filesystem,
            &PathBuf::from("./data"),
            "registry",
            false,
            &file_cfg.proxy.safety,
            &file_cfg.proxy.upstreams,
        )
        .expect("resolve upstreams");

        assert_eq!(upstreams.len(), 2);

        let dockerhub = upstreams
            .iter()
            .find(|u| u.upstream_base_url == "https://registry-1.docker.io")
            .expect("dockerhub upstream");
        assert_eq!(
            dockerhub.hosts,
            vec![crate::proxy::ProxyHostPattern::parse("dockerhub-cache.local").unwrap()]
        );
        assert_eq!(dockerhub.max_cache_bytes, 123);
        assert_eq!(
            dockerhub
                .cache_fs_root
                .as_ref()
                .expect("fs cache root")
                .to_string_lossy(),
            "./data/cache/registry-1.docker.io"
        );
        assert_eq!(
            dockerhub.index_path.to_string_lossy(),
            "./data/cache/registry-1.docker.io/proxy-index"
        );

        let ghcr = upstreams
            .iter()
            .find(|u| u.upstream_base_url == "https://ghcr.io")
            .expect("ghcr upstream");
        assert_eq!(
            ghcr.hosts,
            vec![crate::proxy::ProxyHostPattern::parse("ghcr-cache.local").unwrap()]
        );
        assert_eq!(ghcr.max_cache_bytes, 456);
        assert_eq!(
            ghcr.cache_fs_root
                .as_ref()
                .expect("fs cache root")
                .to_string_lossy(),
            "./data/cache/ghcr.io"
        );
        assert_eq!(
            ghcr.index_path.to_string_lossy(),
            "./data/cache/ghcr.io/proxy-index"
        );
    }

    #[test]
    fn toml_unknown_keys_are_detected() {
        let cfg = r#"
[server]
listen_addr = "127.0.0.1:5000"
lisen_addr = "127.0.0.1:5001" # typo (unknown)

[storage]
backend = "fs"
[storage.fs]
root = "./data"
"#;

        let (_cfg, ignored_paths) = parse_toml_config(cfg).expect("parse toml");

        assert!(
            ignored_paths.iter().any(|p| p == "server.lisen_addr"),
            "expected typo key to be reported"
        );
        assert!(
            !ignored_paths.iter().any(|p| p == "server.listen_addr"),
            "known key should not be reported"
        );
    }

    #[test]
    fn users_groups_toml_parses_into_config_when_enabled() {
        let cfg = r#"
[auth.users]
enabled = true

[[auth.groups]]
name = "devs"
grants = [
  { repo_prefix = "org/", actions = ["pull", "push"] },
]

[[auth.users.accounts]]
name = "alice"
secret_hash = "$argon2id$v=19$m=19456,t=2,p=1$example$example"
groups = ["devs"]
max_ttl_secs = 123
"#;

        let file_cfg: FileConfig = toml::from_str(cfg).expect("parse toml");
        let cfg = resolve_users_config(&file_cfg).expect("resolve users config");

        assert!(cfg.enabled);
        assert_eq!(cfg.groups.len(), 1);
        assert_eq!(cfg.groups[0].name, "devs");
        assert_eq!(cfg.accounts.len(), 1);
        assert_eq!(cfg.accounts[0].name, "alice");
        assert_eq!(cfg.accounts[0].groups, vec!["devs".to_string()]);
        assert_eq!(cfg.accounts[0].max_ttl_secs, Some(123));
    }

    #[test]
    fn users_groups_rejects_unknown_group_reference() {
        let cfg = r#"
[auth.users]
enabled = true

[[auth.users.accounts]]
name = "alice"
secret_hash = "not-empty"
groups = ["missing"]
"#;

        let file_cfg: FileConfig = toml::from_str(cfg).expect("parse toml");
        let err = resolve_users_config(&file_cfg).expect_err("should return ConfigError");
        match err {
            ConfigError::InvalidValue { field, message } => {
                assert_eq!(field, "auth.users.accounts");
                assert!(message.contains("references unknown group 'missing'"));
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[test]
    fn users_groups_rejects_duplicate_group_name() {
        let cfg = r#"
[auth.users]
enabled = true

[[auth.groups]]
name = "devs"
grants = [{ repo_prefix = "org/", actions = ["pull"] }]

[[auth.groups]]
name = "devs"
grants = [{ repo_prefix = "other/", actions = ["push"] }]
"#;

        let file_cfg: FileConfig = toml::from_str(cfg).expect("parse toml");
        let err = resolve_users_config(&file_cfg).expect_err("should return ConfigError");
        match err {
            ConfigError::InvalidValue { field, message } => {
                assert_eq!(field, "auth.groups");
                assert!(message.contains("contains duplicate name='devs'"));
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[test]
    fn users_groups_rejects_empty_secret_hash() {
        let cfg = r#"
[auth.users]
enabled = true

[[auth.groups]]
name = "devs"
grants = [{ repo_prefix = "org/", actions = ["pull"] }]

[[auth.users.accounts]]
name = "alice"
secret_hash = "   "
groups = ["devs"]
"#;

        let file_cfg: FileConfig = toml::from_str(cfg).expect("parse toml");
        let err = resolve_users_config(&file_cfg).expect_err("should return ConfigError");
        match err {
            ConfigError::InvalidValue { field, message } => {
                assert_eq!(field, "auth.users.accounts");
                assert!(message.contains("has empty secret_hash"));
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[test]
    fn default_configuration_loads_successfully() {
        let cfg = Config::from_env_with_files(&[]).expect("default config must load");
        assert_eq!(cfg.listen_addr, ([127, 0, 0, 1], 5000).into());
        assert_eq!(cfg.storage_backend, StorageBackend::Filesystem);
        assert!(!cfg.proxy.enabled);
        assert!(!cfg.robots.enabled);
        assert!(!cfg.users.enabled);
    }

    #[test]
    fn load_single_config_file_succeeds() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(
            &path,
            r#"
[server]
listen_addr = "127.0.0.1:8080"
"#,
        )
        .unwrap();

        let cfg = Config::from_env_with_files(&[path]).expect("load config file");
        assert_eq!(cfg.listen_addr, ([127, 0, 0, 1], 8080).into());
    }

    #[test]
    fn load_multiple_config_files_merges_tables_and_replaces_arrays() {
        let dir = tempfile::tempdir().unwrap();
        let path1 = dir.path().join("base.toml");
        let path2 = dir.path().join("overlay.toml");

        std::fs::write(
            &path1,
            r#"
[server]
listen_addr = "127.0.0.1:8080"

[auth.push]
allow_repos = ["org/repo1", "org/repo2"]
"#,
        )
        .unwrap();

        std::fs::write(
            &path2,
            r#"
[auth.push]
allow_repos = ["org/repo3"]
"#,
        )
        .unwrap();

        let cfg = Config::from_env_with_files(&[path1, path2]).expect("load merged configs");
        assert_eq!(cfg.listen_addr, ([127, 0, 0, 1], 8080).into());
        let expected_pattern =
            crate::registry::RepositoryAccessPattern::parse("org/repo3").unwrap();
        assert_eq!(cfg.push_allow_repos, Some(vec![expected_pattern]));
    }

    #[test]
    fn missing_config_file_returns_io_error() {
        let path = PathBuf::from("/non/existent/path/for/config.toml");
        let err = Config::from_env_with_files(std::slice::from_ref(&path))
            .expect_err("should fail with Io");
        match err {
            ConfigError::Io { path: p, source: _ } => {
                assert_eq!(p, path);
            }
            other => panic!("expected ConfigError::Io, got: {other:?}"),
        }
    }

    #[test]
    fn invalid_toml_syntax_returns_toml_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("invalid.toml");
        std::fs::write(&path, "this is not valid [ toml = {").unwrap();

        let err = Config::from_env_with_files(std::slice::from_ref(&path))
            .expect_err("should fail with Toml");
        match err {
            ConfigError::Toml { path: p, source: _ } => {
                assert_eq!(p, Some(path));
            }
            other => panic!("expected ConfigError::Toml, got: {other:?}"),
        }
    }

    #[test]
    fn wrong_toml_type_returns_deserialize_or_toml_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wrong_type.toml");
        std::fs::write(
            &path,
            r#"
[server]
listen_addr = 12345
"#,
        )
        .unwrap();

        let err = Config::from_env_with_files(&[path]).expect_err("should fail with wrong type");
        match err {
            ConfigError::Toml { .. } | ConfigError::Deserialize { .. } => {}
            other => panic!("expected Toml or Deserialize error, got: {other:?}"),
        }
    }

    #[test]
    fn strict_mode_rejects_unknown_keys() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("typo.toml");
        std::fs::write(
            &path,
            r#"
[config]
strict = true

[server]
listen_addr = "127.0.0.1:5000"
lisn_addr = "127.0.0.1:5001"
"#,
        )
        .unwrap();

        let err = Config::from_env_with_files(&[path]).expect_err("should fail with UnknownKeys");
        match err {
            ConfigError::UnknownKeys { keys } => {
                assert!(keys.iter().any(|k| k.contains("server.lisn_addr")));
            }
            other => panic!("expected UnknownKeys error, got: {other:?}"),
        }
    }

    #[test]
    fn missing_required_acme_fields_return_missing_required_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("acme.toml");
        std::fs::write(
            &path,
            r#"
[server.tls.acme]
enabled = true
# missing email, names, output_dir
"#,
        )
        .unwrap();

        let err =
            Config::from_env_with_files(&[path]).expect_err("should fail with MissingRequired");
        match err {
            ConfigError::MissingRequired { field } => {
                assert!(field.contains("server.tls.acme.email"));
            }
            other => panic!("expected MissingRequired error, got: {other:?}"),
        }
    }

    #[test]
    fn missing_required_proxy_fields_return_missing_required_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proxy.toml");
        std::fs::write(
            &path,
            r#"
[proxy]
enabled = true
# missing upstream base_url or upstreams
"#,
        )
        .unwrap();

        let err =
            Config::from_env_with_files(&[path]).expect_err("should fail with MissingRequired");
        match err {
            ConfigError::MissingRequired { field } => {
                assert!(field.contains("proxy.upstream.base_url"));
            }
            other => panic!("expected MissingRequired error, got: {other:?}"),
        }
    }

    #[test]
    fn duplicate_proxy_upstream_host_patterns_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("proxy_dup.toml");
        std::fs::write(
            &path,
            r#"
[proxy]
enabled = true

[[proxy.upstreams]]
hosts = ["shared-host.local"]
base_url = "https://registry-1.docker.io"
max_cache_bytes = 1000

[[proxy.upstreams]]
hosts = ["shared-host.local"]
base_url = "https://ghcr.io"
max_cache_bytes = 1000
"#,
        )
        .unwrap();

        let err = Config::from_env_with_files(&[path]).expect_err("should fail on duplicate hosts");
        match err {
            ConfigError::InvalidValue { field, message } => {
                assert_eq!(field, "proxy.upstreams.hosts");
                assert!(message.contains("duplicate host pattern 'shared-host.local'"));
            }
            other => panic!("expected InvalidValue error, got: {other:?}"),
        }
    }

    #[test]
    fn duplicate_token_signing_key_kids_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("keys.toml");
        std::fs::write(
            &path,
            r#"
[[token.signing_keys]]
kid = "key-1"
key = "super-secret-key-1"

[[token.signing_keys]]
kid = "key-1"
key = "super-secret-key-2"
"#,
        )
        .unwrap();

        let err = Config::from_env_with_files(&[path]).expect_err("should fail on duplicate kid");
        match err {
            ConfigError::InvalidValue { field, message } => {
                assert_eq!(field, "token.signing_keys");
                assert!(message.contains("contains duplicate kid='key-1'"));
                // Invariant: secret key material must NOT appear in error output
                assert!(!message.contains("super-secret-key"));
            }
            other => panic!("expected InvalidValue error, got: {other:?}"),
        }
    }

    #[test]
    fn error_display_and_debug_do_not_leak_secrets() {
        let secret = "VERY_CONFIDENTIAL_SIGNING_KEY_12345";
        let err = ConfigError::InvalidValue {
            field: "token.signing_keys",
            message: "contains duplicate kid='k1'".to_string(),
        };

        let disp = err.to_string();
        let dbg = format!("{err:?}");

        assert!(!disp.contains(secret));
        assert!(!dbg.contains(secret));
    }

    #[test]
    fn adversarial_redaction_invalid_env_value_password() {
        let sentinel = "SUPER_SECRET_SENTINEL_PASSWORD_998877";
        let err = ConfigError::InvalidEnvValue {
            key: "REGISTRY_PORT",
            expected: "unsigned 16-bit integer",
        };

        let disp = err.to_string();
        let dbg = format!("{err:?}");

        assert!(!disp.contains(sentinel));
        assert!(!dbg.contains(sentinel));
        assert!(!disp.contains("SUPER_SECRET"));
        assert!(!dbg.contains("SUPER_SECRET"));
    }

    #[test]
    fn adversarial_redaction_invalid_env_value_bearer_token() {
        let sentinel =
            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.SENTINEL_BEARER_PAYLOAD_443322.SIGNATURE";
        let err = ConfigError::InvalidEnvValue {
            key: "REGISTRY_LISTEN_ADDR",
            expected: "valid socket address",
        };

        let disp = err.to_string();
        let dbg = format!("{err:?}");

        assert!(!disp.contains(sentinel));
        assert!(!dbg.contains(sentinel));
        assert!(!disp.contains("SENTINEL_BEARER"));
        assert!(!dbg.contains("SENTINEL_BEARER"));
    }

    #[test]
    fn adversarial_redaction_invalid_signing_key_configuration() {
        let sentinel_key1 = "TOP_SECRET_SIGNING_KEY_XYZ_112233";
        let sentinel_key2 = "ANOTHER_TOP_SECRET_KEY_XYZ_445566";
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dup_keys.toml");
        std::fs::write(
            &path,
            format!(
                r#"
[[token.signing_keys]]
kid = "shared-kid"
key = "{sentinel_key1}"

[[token.signing_keys]]
kid = "shared-kid"
key = "{sentinel_key2}"
"#
            ),
        )
        .unwrap();

        let err = Config::from_env_with_files(std::slice::from_ref(&path))
            .expect_err("must fail on duplicate kid");
        let disp = err.to_string();
        let dbg = format!("{err:?}");

        assert!(!disp.contains(sentinel_key1));
        assert!(!dbg.contains(sentinel_key1));
        assert!(!disp.contains(sentinel_key2));
        assert!(!dbg.contains(sentinel_key2));
        assert!(!disp.contains("TOP_SECRET"));
        assert!(!dbg.contains("TOP_SECRET"));
        assert!(!disp.contains("112233"));
        assert!(!dbg.contains("445566"));
    }

    #[test]
    fn adversarial_redaction_account_validation_errors() {
        let sentinel_account = "alice";
        let sentinel_hash = "$argon2id$v=19$m=19456,t=2,p=1$SENTINEL_SECRET_HASH_ABC123";
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("users_unknown_grp.toml");
        std::fs::write(
            &path,
            format!(
                r#"
[auth.users]
enabled = true

[[auth.groups]]
name = "devs"
grants = [{{ repo_prefix = "org/", actions = ["pull"] }}]

[[auth.users.accounts]]
name = "{sentinel_account}"
secret_hash = "{sentinel_hash}"
groups = ["nonexistent_group"]
"#
            ),
        )
        .unwrap();

        let err = Config::from_env_with_files(std::slice::from_ref(&path))
            .expect_err("must fail on unknown group");
        let disp = err.to_string();
        let dbg = format!("{err:?}");

        assert!(!disp.contains(sentinel_hash));
        assert!(!dbg.contains(sentinel_hash));
        assert!(!disp.contains("SENTINEL_SECRET_HASH"));
        assert!(!dbg.contains("SENTINEL_SECRET_HASH"));
    }

    #[test]
    fn env_parsing_semantics_missing_empty_whitespace_valid_invalid() {
        // 1. Boolean parser
        // Absent
        assert_eq!(env_bool_opt(&["NON_EXISTENT_TEST_KEY_1"]).unwrap(), None);

        // 2. u64 parser
        assert_eq!(env_u64_opt(&["NON_EXISTENT_TEST_KEY_2"]).unwrap(), None);

        // 3. usize parser
        assert_eq!(env_usize_opt(&["NON_EXISTENT_TEST_KEY_3"]).unwrap(), None);

        // 4. SocketAddr parser
        assert_eq!(
            env_socket_addr_opt(&["NON_EXISTENT_TEST_KEY_4"]).unwrap(),
            None
        );

        // 5. CIDR parser
        assert_eq!(
            parse_cidrs_opt(None, None).unwrap(),
            Vec::<ipnet::IpNet>::new()
        );
        assert_eq!(
            parse_cidrs_opt(None, Some(vec![])).unwrap(),
            Vec::<ipnet::IpNet>::new()
        );
    }

    fn run_process_isolated<F>(test_name: &str, extra_env: &[(&str, &str)], test_fn: F)
    where
        F: FnOnce(),
    {
        if std::env::var("REGISTRY_TEST_ISOLATED_CHILD").as_deref() == Ok("1") {
            test_fn();
            return;
        }

        let current_exe = std::env::current_exe().expect("current test executable");
        let mut cmd = std::process::Command::new(current_exe);
        cmd.env_clear();
        for var in &["PATH", "TMPDIR", "TEMP", "TMP", "HOME"] {
            if let Ok(val) = std::env::var(var) {
                cmd.env(var, val);
            }
        }
        cmd.env("REGISTRY_TEST_ISOLATED_CHILD", "1");
        for (k, v) in extra_env {
            cmd.env(k, v);
        }
        cmd.arg("--exact").arg(test_name).arg("--nocapture");

        let output = cmd.output().expect("failed to spawn child test process");
        if !output.status.success() {
            panic!(
                "Process-isolated test '{}' failed in child process:\nSTDOUT:\n{}\nSTDERR:\n{}",
                test_name,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }

    #[test]
    fn test_config_storage_fs_manifest_listing_defaults() {
        run_process_isolated(
            "config::tests::test_config_storage_fs_manifest_listing_defaults",
            &[],
            || {
                let cfg = Config::from_env_with_files(&[]).expect("default config must load");
                assert_eq!(cfg.fs_manifest_listing_max_entries, 10_000);
                assert_eq!(cfg.fs_manifest_listing_max_name_bytes, 1_500_000);
            },
        );
    }

    #[test]
    fn test_config_storage_fs_manifest_listing_toml_parsing() {
        run_process_isolated(
            "config::tests::test_config_storage_fs_manifest_listing_toml_parsing",
            &[],
            || {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("config.toml");
                std::fs::write(
                    &path,
                    r#"
[storage.fs]
manifest_listing_max_entries = 5000
manifest_listing_max_name_bytes = 200000
"#,
                )
                .unwrap();

                let cfg = Config::from_env_with_files(&[path]).expect("load config file");
                assert_eq!(cfg.fs_manifest_listing_max_entries, 5000);
                assert_eq!(cfg.fs_manifest_listing_max_name_bytes, 200000);
            },
        );
    }

    #[test]
    fn test_config_storage_fs_manifest_listing_env_hierarchical_precedence() {
        run_process_isolated(
            "config::tests::test_config_storage_fs_manifest_listing_env_hierarchical_precedence",
            &[
                (
                    "REGISTRY__STORAGE__FS__MANIFEST_LISTING_MAX_ENTRIES",
                    "7000",
                ),
                ("STORAGE_FS_MANIFEST_LISTING_MAX_ENTRIES", "6000"),
                (
                    "REGISTRY__STORAGE__FS__MANIFEST_LISTING_MAX_NAME_BYTES",
                    "400000",
                ),
                ("STORAGE_FS_MANIFEST_LISTING_MAX_NAME_BYTES", "300000"),
            ],
            || {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("config.toml");
                std::fs::write(
                    &path,
                    r#"
[storage.fs]
manifest_listing_max_entries = 5000
manifest_listing_max_name_bytes = 200000
"#,
                )
                .unwrap();

                let res = Config::from_env_with_files(&[path]);
                let cfg = res.expect("load config with env override");
                assert_eq!(cfg.fs_manifest_listing_max_entries, 7000);
                assert_eq!(cfg.fs_manifest_listing_max_name_bytes, 400000);
            },
        );
    }

    #[test]
    fn test_config_storage_fs_manifest_listing_env_flat_precedence() {
        run_process_isolated(
            "config::tests::test_config_storage_fs_manifest_listing_env_flat_precedence",
            &[
                ("STORAGE_FS_MANIFEST_LISTING_MAX_ENTRIES", "6000"),
                ("STORAGE_FS_MANIFEST_LISTING_MAX_NAME_BYTES", "300000"),
            ],
            || {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("config.toml");
                std::fs::write(
                    &path,
                    r#"
[storage.fs]
manifest_listing_max_entries = 5000
manifest_listing_max_name_bytes = 200000
"#,
                )
                .unwrap();

                let res = Config::from_env_with_files(&[path]);
                let cfg = res.expect("load config with flat env override");
                assert_eq!(cfg.fs_manifest_listing_max_entries, 6000);
                assert_eq!(cfg.fs_manifest_listing_max_name_bytes, 300000);
            },
        );
    }

    #[test]
    fn test_config_storage_fs_manifest_listing_invalid_lower_bounds_entries() {
        run_process_isolated(
            "config::tests::test_config_storage_fs_manifest_listing_invalid_lower_bounds_entries",
            &[],
            || {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("config.toml");
                std::fs::write(
                    &path,
                    r#"
[storage.fs]
manifest_listing_max_entries = 0
"#,
                )
                .unwrap();

                let err =
                    Config::from_env_with_files(&[path]).expect_err("should reject entries = 0");
                match err {
                    ConfigError::InvalidValue { field, message } => {
                        assert_eq!(field, "manifest_listing_max_entries");
                        assert!(message.contains("must be at least 1"));
                    }
                    other => panic!("expected InvalidValue, got: {other:?}"),
                }
            },
        );
    }

    #[test]
    fn test_config_storage_fs_manifest_listing_invalid_lower_bounds_bytes() {
        run_process_isolated(
            "config::tests::test_config_storage_fs_manifest_listing_invalid_lower_bounds_bytes",
            &[],
            || {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("config.toml");
                std::fs::write(
                    &path,
                    r#"
[storage.fs]
manifest_listing_max_name_bytes = 127
"#,
                )
                .unwrap();

                let err = Config::from_env_with_files(&[path])
                    .expect_err("should reject name_bytes = 127");
                match err {
                    ConfigError::InvalidValue { field, message } => {
                        assert_eq!(field, "manifest_listing_max_name_bytes");
                        assert!(message.contains("must be at least 128"));
                    }
                    other => panic!("expected InvalidValue, got: {other:?}"),
                }
            },
        );
    }

    #[test]
    fn test_config_storage_fs_manifest_listing_malformed_entries() {
        run_process_isolated(
            "config::tests::test_config_storage_fs_manifest_listing_malformed_entries",
            &[("REGISTRY__STORAGE__FS__MANIFEST_LISTING_MAX_ENTRIES", "abc")],
            || {
                let res = Config::from_env_with_files(&[]);
                match res.expect_err("should fail on malformed entries") {
                    ConfigError::InvalidEnvValue { key, expected } => {
                        assert_eq!(key, "REGISTRY__STORAGE__FS__MANIFEST_LISTING_MAX_ENTRIES");
                        assert_eq!(expected, "unsigned integer");
                    }
                    other => panic!("expected InvalidEnvValue, got: {other:?}"),
                }
            },
        );
    }

    #[test]
    fn test_config_storage_fs_manifest_listing_malformed_name_bytes() {
        run_process_isolated(
            "config::tests::test_config_storage_fs_manifest_listing_malformed_name_bytes",
            &[("STORAGE_FS_MANIFEST_LISTING_MAX_NAME_BYTES", "not_a_number")],
            || {
                let res = Config::from_env_with_files(&[]);
                match res.expect_err("should fail on malformed name bytes") {
                    ConfigError::InvalidEnvValue { key, expected } => {
                        assert_eq!(key, "STORAGE_FS_MANIFEST_LISTING_MAX_NAME_BYTES");
                        assert_eq!(expected, "unsigned integer");
                    }
                    other => panic!("expected InvalidEnvValue, got: {other:?}"),
                }
            },
        );
    }

    #[test]
    fn test_config_storage_fs_manifest_listing_numeric_overflow() {
        run_process_isolated(
            "config::tests::test_config_storage_fs_manifest_listing_numeric_overflow",
            &[(
                "REGISTRY__STORAGE__FS__MANIFEST_LISTING_MAX_ENTRIES",
                "999999999999999999999999999999999999999999999999999999999999",
            )],
            || {
                let res = Config::from_env_with_files(&[]);
                match res.expect_err("should fail on numeric overflow") {
                    ConfigError::InvalidEnvValue { key, expected } => {
                        assert_eq!(key, "REGISTRY__STORAGE__FS__MANIFEST_LISTING_MAX_ENTRIES");
                        assert_eq!(expected, "unsigned integer");
                    }
                    other => panic!("expected InvalidEnvValue, got: {other:?}"),
                }
            },
        );
    }

    #[test]
    fn test_config_storage_fs_manifest_listing_s3_ignores_fs_bounds() {
        run_process_isolated(
            "config::tests::test_config_storage_fs_manifest_listing_s3_ignores_fs_bounds",
            &[],
            || {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("config.toml");
                std::fs::write(
                    &path,
                    r#"
[storage]
backend = "s3"

[storage.fs]
manifest_listing_max_entries = 0
manifest_listing_max_name_bytes = 50
"#,
                )
                .unwrap();

                let cfg = Config::from_env_with_files(&[path])
                    .expect("s3 backend should ignore fs bounds");
                assert_eq!(cfg.storage_backend, StorageBackend::S3);
            },
        );
    }

    #[test]
    fn test_config_storage_fs_tag_listing_defaults() {
        run_process_isolated(
            "config::tests::test_config_storage_fs_tag_listing_defaults",
            &[],
            || {
                let cfg = Config::from_env_with_files(&[]).expect("default config must load");
                assert_eq!(cfg.fs_tag_listing_max_entries, 10_000);
                assert_eq!(cfg.fs_tag_listing_max_name_bytes, 1_500_000);
                assert_eq!(cfg.fs_tag_listing_repo_probe_max_entries, 64);
                assert_eq!(cfg.fs_tag_listing_repo_probe_max_name_bytes, 4_096);
                assert_eq!(cfg.fs_tag_listing_max_payload_bytes, 1_024);
            },
        );
    }

    #[test]
    fn test_config_storage_fs_tag_listing_toml_parsing() {
        run_process_isolated(
            "config::tests::test_config_storage_fs_tag_listing_toml_parsing",
            &[],
            || {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("config.toml");
                std::fs::write(
                    &path,
                    r#"
[storage.fs]
tag_listing_max_entries = 5000
tag_listing_max_name_bytes = 200000
tag_listing_repo_probe_max_entries = 32
tag_listing_repo_probe_max_name_bytes = 2048
tag_listing_max_payload_bytes = 2048
"#,
                )
                .unwrap();

                let cfg = Config::from_env_with_files(&[path]).expect("load config file");
                assert_eq!(cfg.fs_tag_listing_max_entries, 5000);
                assert_eq!(cfg.fs_tag_listing_max_name_bytes, 200000);
                assert_eq!(cfg.fs_tag_listing_repo_probe_max_entries, 32);
                assert_eq!(cfg.fs_tag_listing_repo_probe_max_name_bytes, 2048);
                assert_eq!(cfg.fs_tag_listing_max_payload_bytes, 2048);
            },
        );
    }

    #[test]
    fn test_config_storage_fs_tag_listing_env_hierarchical_precedence() {
        run_process_isolated(
            "config::tests::test_config_storage_fs_tag_listing_env_hierarchical_precedence",
            &[
                ("REGISTRY__STORAGE__FS__TAG_LISTING_MAX_ENTRIES", "7000"),
                ("STORAGE_FS_TAG_LISTING_MAX_ENTRIES", "6000"),
                (
                    "REGISTRY__STORAGE__FS__TAG_LISTING_MAX_NAME_BYTES",
                    "400000",
                ),
                ("STORAGE_FS_TAG_LISTING_MAX_NAME_BYTES", "300000"),
                (
                    "REGISTRY__STORAGE__FS__TAG_LISTING_REPO_PROBE_MAX_ENTRIES",
                    "128",
                ),
                ("STORAGE_FS_TAG_LISTING_REPO_PROBE_MAX_ENTRIES", "64"),
                (
                    "REGISTRY__STORAGE__FS__TAG_LISTING_REPO_PROBE_MAX_NAME_BYTES",
                    "8192",
                ),
                ("STORAGE_FS_TAG_LISTING_REPO_PROBE_MAX_NAME_BYTES", "4096"),
                (
                    "REGISTRY__STORAGE__FS__TAG_LISTING_MAX_PAYLOAD_BYTES",
                    "4096",
                ),
                ("STORAGE_FS_TAG_LISTING_MAX_PAYLOAD_BYTES", "2048"),
            ],
            || {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("config.toml");
                std::fs::write(
                    &path,
                    r#"
[storage.fs]
tag_listing_max_entries = 5000
tag_listing_max_name_bytes = 200000
tag_listing_repo_probe_max_entries = 32
tag_listing_repo_probe_max_name_bytes = 2048
tag_listing_max_payload_bytes = 1024
"#,
                )
                .unwrap();

                let res = Config::from_env_with_files(&[path]);
                let cfg = res.expect("load config with env override");
                assert_eq!(cfg.fs_tag_listing_max_entries, 7000);
                assert_eq!(cfg.fs_tag_listing_max_name_bytes, 400000);
                assert_eq!(cfg.fs_tag_listing_repo_probe_max_entries, 128);
                assert_eq!(cfg.fs_tag_listing_repo_probe_max_name_bytes, 8192);
                assert_eq!(cfg.fs_tag_listing_max_payload_bytes, 4096);
            },
        );
    }

    #[test]
    fn test_config_storage_fs_tag_listing_env_flat_precedence() {
        run_process_isolated(
            "config::tests::test_config_storage_fs_tag_listing_env_flat_precedence",
            &[
                ("STORAGE_FS_TAG_LISTING_MAX_ENTRIES", "6000"),
                ("STORAGE_FS_TAG_LISTING_MAX_NAME_BYTES", "300000"),
                ("STORAGE_FS_TAG_LISTING_REPO_PROBE_MAX_ENTRIES", "50"),
                ("STORAGE_FS_TAG_LISTING_REPO_PROBE_MAX_NAME_BYTES", "3000"),
                ("STORAGE_FS_TAG_LISTING_MAX_PAYLOAD_BYTES", "3000"),
            ],
            || {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("config.toml");
                std::fs::write(
                    &path,
                    r#"
[storage.fs]
tag_listing_max_entries = 5000
tag_listing_max_name_bytes = 200000
tag_listing_repo_probe_max_entries = 32
tag_listing_repo_probe_max_name_bytes = 2048
tag_listing_max_payload_bytes = 1024
"#,
                )
                .unwrap();

                let res = Config::from_env_with_files(&[path]);
                let cfg = res.expect("load config with flat env override");
                assert_eq!(cfg.fs_tag_listing_max_entries, 6000);
                assert_eq!(cfg.fs_tag_listing_max_name_bytes, 300000);
                assert_eq!(cfg.fs_tag_listing_repo_probe_max_entries, 50);
                assert_eq!(cfg.fs_tag_listing_repo_probe_max_name_bytes, 3000);
                assert_eq!(cfg.fs_tag_listing_max_payload_bytes, 3000);
            },
        );
    }

    #[test]
    fn test_config_storage_fs_tag_listing_invalid_lower_bounds_entries() {
        run_process_isolated(
            "config::tests::test_config_storage_fs_tag_listing_invalid_lower_bounds_entries",
            &[("STORAGE_FS_TAG_LISTING_MAX_ENTRIES", "0")],
            || {
                let err = Config::from_env_with_files(&[]).expect_err("should reject entries = 0");
                match err {
                    ConfigError::InvalidValue { field, message } => {
                        assert_eq!(field, "tag_listing_max_entries");
                        assert!(message.contains("must be at least 1"));
                    }
                    other => panic!("unexpected error: {other:?}"),
                }
            },
        );
    }

    #[test]
    fn test_config_storage_fs_tag_listing_invalid_lower_bounds_name_bytes() {
        run_process_isolated(
            "config::tests::test_config_storage_fs_tag_listing_invalid_lower_bounds_name_bytes",
            &[("STORAGE_FS_TAG_LISTING_MAX_NAME_BYTES", "127")],
            || {
                let err =
                    Config::from_env_with_files(&[]).expect_err("should reject name bytes = 127");
                match err {
                    ConfigError::InvalidValue { field, message } => {
                        assert_eq!(field, "tag_listing_max_name_bytes");
                        assert!(message.contains("must be at least 128"));
                    }
                    other => panic!("unexpected error: {other:?}"),
                }
            },
        );
    }

    #[test]
    fn test_config_storage_fs_tag_listing_invalid_lower_bounds_probe_entries() {
        run_process_isolated(
            "config::tests::test_config_storage_fs_tag_listing_invalid_lower_bounds_probe_entries",
            &[("STORAGE_FS_TAG_LISTING_REPO_PROBE_MAX_ENTRIES", "0")],
            || {
                let err =
                    Config::from_env_with_files(&[]).expect_err("should reject probe entries = 0");
                match err {
                    ConfigError::InvalidValue { field, message } => {
                        assert_eq!(field, "tag_listing_repo_probe_max_entries");
                        assert!(message.contains("must be at least 1"));
                    }
                    other => panic!("unexpected error: {other:?}"),
                }
            },
        );
    }

    #[test]
    fn test_config_storage_fs_tag_listing_invalid_lower_bounds_probe_name_bytes() {
        run_process_isolated(
            "config::tests::test_config_storage_fs_tag_listing_invalid_lower_bounds_probe_name_bytes",
            &[("STORAGE_FS_TAG_LISTING_REPO_PROBE_MAX_NAME_BYTES", "63")],
            || {
                let err =
                    Config::from_env_with_files(&[]).expect_err("should reject probe bytes = 63");
                match err {
                    ConfigError::InvalidValue { field, message } => {
                        assert_eq!(field, "tag_listing_repo_probe_max_name_bytes");
                        assert!(message.contains("must be at least 64"));
                    }
                    other => panic!("unexpected error: {other:?}"),
                }
            },
        );
    }

    #[test]
    fn test_config_storage_fs_tag_listing_invalid_lower_bounds_payload_bytes() {
        run_process_isolated(
            "config::tests::test_config_storage_fs_tag_listing_invalid_lower_bounds_payload_bytes",
            &[("STORAGE_FS_TAG_LISTING_MAX_PAYLOAD_BYTES", "255")],
            || {
                let err = Config::from_env_with_files(&[])
                    .expect_err("should reject payload bytes = 255");
                match err {
                    ConfigError::InvalidValue { field, message } => {
                        assert_eq!(field, "tag_listing_max_payload_bytes");
                        assert!(message.contains("must be at least 256"));
                    }
                    other => panic!("unexpected error: {other:?}"),
                }
            },
        );
    }

    #[test]
    fn test_config_storage_fs_tag_listing_invalid_upper_bounds_payload_bytes() {
        run_process_isolated(
            "config::tests::test_config_storage_fs_tag_listing_invalid_upper_bounds_payload_bytes",
            &[(
                "STORAGE_FS_TAG_LISTING_MAX_PAYLOAD_BYTES",
                &u64::MAX.to_string(),
            )],
            || {
                let err = Config::from_env_with_files(&[])
                    .expect_err("should reject payload bytes = u64::MAX");
                match err {
                    ConfigError::InvalidValue { field, message } => {
                        assert_eq!(field, "tag_listing_max_payload_bytes");
                        assert!(message.contains("less than u64::MAX"));
                    }
                    other => panic!("unexpected error: {other:?}"),
                }
            },
        );
    }

    #[test]
    fn test_config_storage_fs_tag_listing_s3_ignores_fs_bounds() {
        run_process_isolated(
            "config::tests::test_config_storage_fs_tag_listing_s3_ignores_fs_bounds",
            &[],
            || {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("config.toml");
                std::fs::write(
                    &path,
                    r#"
[storage]
backend = "s3"

[storage.fs]
tag_listing_max_entries = 0
tag_listing_max_name_bytes = 10
tag_listing_repo_probe_max_entries = 0
tag_listing_repo_probe_max_name_bytes = 10
tag_listing_max_payload_bytes = 10
"#,
                )
                .unwrap();

                let cfg = Config::from_env_with_files(&[path])
                    .expect("s3 backend should ignore fs tag listing bounds");
                assert_eq!(cfg.storage_backend, StorageBackend::S3);
            },
        );
    }

    #[test]
    fn gc_policy_mapping_covers_every_field() {
        let mut cfg = Config::from_env().unwrap();
        cfg.blob_gc_enabled = true;
        cfg.blob_gc_enable_delete = true;
        cfg.blob_gc_default_min_age_secs = 11;
        cfg.blob_gc_default_quarantine_delay_secs = 22;
        cfg.blob_gc_default_max_blobs = 33;
        cfg.blob_gc_default_max_bytes = 44;
        cfg.blob_gc_default_max_seconds = 55;
        cfg.ref_index.auto_rebuild_on_corruption = false;
        cfg.fs_root = std::path::PathBuf::from("/tmp/gc-map-root");

        // Exhaustive destructuring: adding a GcPolicy field breaks this test
        // until the Config mapping is extended (plan risk R3).
        let crate::policy::GcPolicy {
            enabled,
            enable_delete,
            default_min_age_secs,
            default_quarantine_delay_secs,
            default_max_blobs,
            default_max_bytes,
            default_max_seconds,
            auto_rebuild_ref_index_on_corruption,
            fs_root,
        } = crate::policy::GcPolicy::from(&cfg);

        assert!(enabled);
        assert!(enable_delete);
        assert_eq!(default_min_age_secs, 11);
        assert_eq!(default_quarantine_delay_secs, 22);
        assert_eq!(default_max_blobs, 33);
        assert_eq!(default_max_bytes, 44);
        assert_eq!(default_max_seconds, 55);
        assert!(!auto_rebuild_ref_index_on_corruption);
        assert_eq!(fs_root, std::path::PathBuf::from("/tmp/gc-map-root"));
    }
}
