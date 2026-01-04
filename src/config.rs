use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf};
use url::Url;

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
    if let Ok(u) = Url::parse(base_url) {
        if let Some(host) = u.host_str() {
            // Include port if present to avoid collisions.
            if let Some(port) = u.port() {
                return sanitize_for_path_component(&format!("{host}_{port}"));
            }
            return sanitize_for_path_component(host);
        }
    }
    sanitize_for_path_component(base_url)
}

#[derive(Clone, Debug)]
pub struct Config {
    pub listen_addr: SocketAddr,

    pub tls_cert_path: Option<PathBuf>,
    pub tls_key_path: Option<PathBuf>,

    pub push_username: Option<String>,
    pub push_password: Option<String>,
    pub push_allow_repos: Option<Vec<String>>,

    pub storage_backend: StorageBackend,

    pub fs_root: PathBuf,

    pub s3_endpoint: Option<String>,
    pub s3_region: Option<String>,
    pub s3_bucket: Option<String>,
    pub s3_prefix: String,

    pub allow_tag_overwrite: bool,

    // When true, allow cross-mounting blobs without a `from` repository.
    pub automatic_crossmount: bool,

    // Filesystem backend maintenance: clean up stale upload temp files.
    pub upload_gc_enabled: bool,
    pub upload_gc_interval_secs: u64,
    pub upload_gc_max_age_secs: u64,

    pub max_upload_bytes: u64,
    pub max_request_body_bytes: usize,
    // Concurrency guard for endpoints that buffer full bodies into memory (e.g. manifest PUT,
    // proxy manifest GET when caching). Caps worst-case RAM to ~N * max_request_body_bytes.
    pub max_concurrent_buffered_requests: usize,
    // Concurrency guard for total in-flight /v2 requests (caps tasks, open files, sockets).
    // Upload endpoints are long-lived and streaming; we typically do not want to count them here.
    pub max_concurrent_requests: usize,
    pub request_timeout_secs: u64,

    // Longer timeout for upload endpoints (PATCH/PUT/POST blobs/uploads).
    pub upload_request_timeout_secs: u64,

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
    pub token_ttl_secs: u64,

    // Robot accounts + scoped grants for token minting.
    pub robots: RobotsConfig,

    pub proxy: ProxyConfig,
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

#[derive(Clone, Debug, Default)]
pub struct RobotsConfig {
    pub enabled: bool,
    pub accounts: Vec<RobotAccountConfig>,
}

#[derive(Clone, Debug)]
pub struct RobotAccountConfig {
    pub name: String,
    pub secret_hash: String,
    pub grants: Vec<crate::rbac::Grant>,
    pub max_ttl_secs: Option<u64>,
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

    // Safety net: even in proxy-any mode, only allow these prefixes.
    // Examples: ["library/", "myorg/"]
    pub allowed_repo_prefixes: Vec<String>,

    // SSRF guard: block loopback/link-local/private IPs.
    pub block_private_networks: bool,

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
    pub routing_proxy_hosts: Vec<String>,
    pub routing_trust_x_forwarded_host: bool,
}

#[derive(Clone, Debug)]
pub struct ProxyUpstreamRoute {
    // Host patterns (minimal '*' glob). If the effective request host matches, this upstream is
    // selected and the request runs in proxy-only mode.
    pub hosts: Vec<String>,

    // Whether to use X-Forwarded-Host when matching hosts for this upstream route.
    // Only enable this if the registry is reachable only via a trusted reverse proxy.
    pub trust_x_forwarded_host: bool,

    pub upstream_base_url: String,
    pub upstream_username: Option<String>,
    pub upstream_password: Option<String>,

    // Safety settings (can be different per upstream).
    pub allowed_upstream_hosts: Vec<String>,
    pub allowed_repo_prefixes: Vec<String>,
    pub block_private_networks: bool,
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

#[derive(Clone, Debug)]
pub struct ProxyRepoRule {
    pub match_pattern: String,
    pub upstream_repo: Option<String>,
    pub tag_policy: TagPolicy,
    pub eviction_policy: EvictionPolicy,
}

#[derive(Clone, Debug)]
pub enum TagPolicy {
    DigestOnly,
    TtlSeconds(u64),
    AlwaysRevalidate,
}

#[derive(Clone, Debug)]
pub enum EvictionPolicy {
    Default,
    KeepTags(Vec<String>),
    // Keep the highest SemVer tag among *cached tags* (optionally filtered by regex).
    KeepLatestCachedSemver {
        tag_regex: Option<String>,
        allow_prerelease: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageBackend {
    Filesystem,
    S3,
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
    proxy: FileProxy,
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
    allowed_repo_prefixes: Option<Vec<String>>,
    #[serde(default)]
    block_private_networks: Option<bool>,
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
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileAuth {
    #[serde(default)]
    push: FilePushAuth,

    #[serde(default)]
    robots: FileRobots,
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
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileToken {
    #[serde(default)]
    service: Option<String>,
    #[serde(default)]
    signing_key: Option<String>,
    #[serde(default)]
    signing_keys: Vec<FileTokenSigningKey>,
    #[serde(default)]
    ttl_secs: Option<u64>,
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
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileStorageFs {
    #[serde(default)]
    root: Option<String>,
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
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileFeatures {
    #[serde(default)]
    allow_tag_overwrite: Option<bool>,
    #[serde(default)]
    automatic_crossmount: Option<bool>,
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
    max_concurrent_buffered_requests: Option<usize>,
    #[serde(default)]
    max_concurrent_requests: Option<usize>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileTimeouts {
    #[serde(default)]
    request_timeout_secs: Option<u64>,
    #[serde(default)]
    upload_request_timeout_secs: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileCatalog {
    #[serde(default)]
    requires_auth: Option<bool>,
}

impl Config {
    pub fn from_env() -> Self {
        // Precedence:
        //   defaults < config file (CONFIG_PATH) < env vars
        let loaded = load_config_file();
        let file_cfg = loaded.cfg;

        let best_practice = env_bool_opt(&["BEST_PRACTICE"]).unwrap_or(false)
            || file_cfg
                .profile
                .name
                .as_deref()
                .map(|s| s.eq_ignore_ascii_case("best_practice"))
                .unwrap_or(false);

        // Optional strict config parsing: fail fast on unknown keys/typos.
        // - enabled by env vars, TOML [config].strict, or best_practice profile.
        let strict_config = env_bool_opt(&["REGISTRY__CONFIG__STRICT", "STRICT_CONFIG"])
            .or(file_cfg.config.strict)
            .unwrap_or(best_practice);
        if !loaded.ignored_paths.is_empty() {
            if strict_config {
                let mut paths = loaded.ignored_paths;
                paths.sort();
                paths.dedup();
                panic!(
                    "Unknown TOML keys found (enable fix or remove typos):\n  - {}",
                    paths.join("\n  - ")
                );
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

        let listen_addr = env_socket_addr(&["REGISTRY__SERVER__LISTEN_ADDR", "LISTEN_ADDR"])
            .unwrap_or_else(|| {
                file_cfg
                    .server
                    .listen_addr
                    .unwrap_or_else(|| ([127, 0, 0, 1], 5000).into())
            });

        let tls_cert_path = env_str_any(&["REGISTRY__SERVER__TLS__CERT_PATH", "TLS_CERT_PATH"])
            .or_else(|| file_cfg.server.tls.cert_path.clone())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .map(PathBuf::from);
        let tls_key_path = env_str_any(&["REGISTRY__SERVER__TLS__KEY_PATH", "TLS_KEY_PATH"])
            .or_else(|| file_cfg.server.tls.key_path.clone())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .map(PathBuf::from);

        let push_username = env_str_any(&["REGISTRY__AUTH__PUSH__USERNAME", "REGISTRY_USERNAME"])
            .or_else(|| file_cfg.auth.push.username.clone());
        let push_password = env_str_any(&["REGISTRY__AUTH__PUSH__PASSWORD", "REGISTRY_PASSWORD"])
            .or_else(|| file_cfg.auth.push.password.clone());

        let push_allow_repos = env_str_any(&[
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

        let storage_backend_raw = env_str_any(&["REGISTRY__STORAGE__BACKEND", "STORAGE_BACKEND"])
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

        let fs_root = env_str_any(&["REGISTRY__STORAGE__FS__ROOT", "STORAGE_FS_ROOT"])
            .or_else(|| file_cfg.storage.fs.root.clone())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("./data"));

        let s3_endpoint = env_str_any(&["REGISTRY__STORAGE__S3__ENDPOINT", "STORAGE_S3_ENDPOINT"])
            .or_else(|| file_cfg.storage.s3.endpoint.clone());
        let s3_region = env_str_any(&["REGISTRY__STORAGE__S3__REGION", "STORAGE_S3_REGION"])
            .or_else(|| file_cfg.storage.s3.region.clone());
        let s3_bucket = env_str_any(&["REGISTRY__STORAGE__S3__BUCKET", "STORAGE_S3_BUCKET"])
            .or_else(|| file_cfg.storage.s3.bucket.clone());
        let s3_prefix = env_str_any(&["REGISTRY__STORAGE__S3__PREFIX", "STORAGE_S3_PREFIX"])
            .or_else(|| file_cfg.storage.s3.prefix.clone())
            .unwrap_or_else(|| "registry".to_string());

        let allow_tag_overwrite = env_bool_opt(&[
            "REGISTRY__FEATURES__ALLOW_TAG_OVERWRITE",
            "ALLOW_TAG_OVERWRITE",
        ])
        .or(file_cfg.features.allow_tag_overwrite)
        .unwrap_or_else(|| !best_practice);

        let automatic_crossmount = env_bool_opt(&[
            "REGISTRY__FEATURES__AUTOMATIC_CROSSMOUNT",
            "REGISTRY_AUTOMATIC_CROSSMOUNT",
        ])
        .or(file_cfg.features.automatic_crossmount)
        .unwrap_or(false);

        let upload_gc_enabled =
            env_bool_opt(&["REGISTRY__UPLOADS__GC_ENABLED", "UPLOAD_GC_ENABLED"])
                .or(file_cfg.uploads.gc_enabled)
                .unwrap_or(true);

        let upload_gc_interval_secs = env_u64_any(&[
            "REGISTRY__UPLOADS__GC_INTERVAL_SECS",
            "UPLOAD_GC_INTERVAL_SECS",
        ])
        .or(file_cfg.uploads.gc_interval_secs)
        .unwrap_or(3600);

        let upload_gc_max_age_secs = env_u64_any(&[
            "REGISTRY__UPLOADS__GC_MAX_AGE_SECS",
            "UPLOAD_GC_MAX_AGE_SECS",
        ])
        .or(file_cfg.uploads.gc_max_age_secs)
        .unwrap_or(24 * 3600);

        let max_upload_bytes =
            env_u64_any(&["REGISTRY__LIMITS__MAX_UPLOAD_BYTES", "MAX_UPLOAD_BYTES"])
                .or(file_cfg.limits.max_upload_bytes)
                .unwrap_or(5 * 1024 * 1024 * 1024);

        let max_request_body_bytes = env_usize_any(&[
            "REGISTRY__LIMITS__MAX_REQUEST_BODY_BYTES",
            "MAX_REQUEST_BODY_BYTES",
        ])
        .or(file_cfg.limits.max_request_body_bytes)
        .unwrap_or(32 * 1024 * 1024);

        let max_concurrent_buffered_requests = env_usize_any(&[
            "REGISTRY__LIMITS__MAX_CONCURRENT_BUFFERED_REQUESTS",
            "MAX_CONCURRENT_BUFFERED_REQUESTS",
        ])
        .or(file_cfg.limits.max_concurrent_buffered_requests)
        .unwrap_or(if best_practice { 4 } else { 8 })
        .max(1);

        let max_concurrent_requests = env_usize_any(&[
            "REGISTRY__LIMITS__MAX_CONCURRENT_REQUESTS",
            "MAX_CONCURRENT_REQUESTS",
        ])
        .or(file_cfg.limits.max_concurrent_requests)
        .unwrap_or(if best_practice { 64 } else { 256 })
        .max(1);

        let request_timeout_secs = env_u64_any(&[
            "REGISTRY__TIMEOUTS__REQUEST_TIMEOUT_SECS",
            "REQUEST_TIMEOUT_SECS",
        ])
        .or(file_cfg.timeouts.request_timeout_secs)
        .unwrap_or(if best_practice { 60 } else { 300 });

        let upload_request_timeout_secs = env_u64_any(&[
            "REGISTRY__TIMEOUTS__UPLOAD_REQUEST_TIMEOUT_SECS",
            "UPLOAD_REQUEST_TIMEOUT_SECS",
        ])
        .or(file_cfg.timeouts.upload_request_timeout_secs)
        .unwrap_or(if best_practice { 7200 } else { 3600 });

        let disallow_monolithic_uploads = env_bool_opt(&[
            "REGISTRY__UPLOADS__DISALLOW_MONOLITHIC_UPLOADS",
            "DISALLOW_MONOLITHIC_UPLOADS",
        ])
        .or(file_cfg.uploads.disallow_monolithic_uploads)
        .unwrap_or(best_practice);

        let uploads_abort_on_error =
            env_bool_opt(&["REGISTRY__UPLOADS__ABORT_ON_ERROR", "UPLOAD_ABORT_ON_ERROR"])
                .or(file_cfg.uploads.abort_on_error)
                .unwrap_or(false);
        let uploads_abort_on_digest_mismatch = env_bool_opt(&[
            "REGISTRY__UPLOADS__ABORT_ON_DIGEST_MISMATCH",
            "UPLOAD_ABORT_ON_DIGEST_MISMATCH",
        ])
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
            env_bool_opt(&["REGISTRY__CATALOG__REQUIRES_AUTH", "CATALOG_REQUIRES_AUTH"])
                .or(file_cfg.catalog.requires_auth)
                .unwrap_or(best_practice);

        let public_url = env_str_any(&["REGISTRY__SERVER__PUBLIC_URL", "PUBLIC_URL"])
            .or_else(|| file_cfg.server.public_url.clone())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        let token_service = env_str_any(&["REGISTRY__TOKEN__SERVICE", "TOKEN_SERVICE"])
            .or_else(|| file_cfg.token.service.clone())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "registry-rust".to_string());

        let env_token_signing_key =
            env_str_any(&["REGISTRY__TOKEN__SIGNING_KEY", "TOKEN_SIGNING_KEY"])
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
            panic!(
                "token.signing_keys is present but contains no valid entries (each entry requires non-empty kid and key)"
            );
        }
        if raw_file_signing_keys_count > file_signing_keys.len() {
            eprintln!(
                "Warning: token.signing_keys contains {} invalid entries (missing/empty kid or key); ignoring them",
                raw_file_signing_keys_count - file_signing_keys.len()
            );
        }

        let (token_signing_key, token_signing_keys) = if !file_signing_keys.is_empty() {
            if env_token_signing_key.is_some() {
                eprintln!(
                    "Warning: TOKEN_SIGNING_KEY is set but token.signing_keys is present; ignoring env TOKEN_SIGNING_KEY"
                );
            }

            let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
            for k in &file_signing_keys {
                if !seen.insert(k.kid.clone()) {
                    panic!("token.signing_keys contains duplicate kid='{}'", k.kid);
                }
            }

            (file_signing_keys[0].key.clone(), file_signing_keys)
        } else {
            let token_signing_key = env_token_signing_key
                .or_else(|| file_cfg.token.signing_key.clone())
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| {
                    if best_practice {
                        panic!(
                            "best_practice requires TOKEN_SIGNING_KEY (or config token.signing_key or token.signing_keys) to be set"
                        );
                    }
                    uuid::Uuid::new_v4().to_string()
                });

            let token_signing_keys = vec![crate::security::TokenSigningKey {
                kid: "default".to_string(),
                key: token_signing_key.clone(),
            }];

            (token_signing_key, token_signing_keys)
        };

        let token_ttl_secs = env_u64_any(&["REGISTRY__TOKEN__TTL_SECS", "TOKEN_TTL_SECS"])
            .or(file_cfg.token.ttl_secs)
            .unwrap_or(600);

        let robots = RobotsConfig {
            enabled: file_cfg.auth.robots.enabled.unwrap_or(false),
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
                        .map(|g| crate::rbac::Grant {
                            repo_prefix: g.repo_prefix.trim().to_string(),
                            actions: g
                                .actions
                                .iter()
                                .map(|s| s.trim().to_string())
                                .filter(|s| !s.is_empty())
                                .collect(),
                        })
                        .collect(),
                    max_ttl_secs: a.max_ttl_secs,
                })
                .collect(),
        };

        let proxy_enabled = env_bool_opt(&["REGISTRY__PROXY__ENABLED", "PROXY_ENABLED"])
            .or(file_cfg.proxy.enabled)
            .unwrap_or(false);

        let proxy_mode_raw =
            env_str_any(&["REGISTRY__PROXY__MODE", "PROXY_MODE"]).or(file_cfg.proxy.mode.clone());
        let proxy_mode = match proxy_mode_raw
            .as_deref()
            .unwrap_or("allowlist")
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "allowlist" => ProxyMode::Allowlist,
            "any" => ProxyMode::Any,
            other => {
                eprintln!("Unknown PROXY_MODE='{other}', defaulting to allowlist");
                ProxyMode::Allowlist
            }
        };

        let upstream_base_url = env_str_any(&[
            "REGISTRY__PROXY__UPSTREAM__BASE_URL",
            "PROXY_UPSTREAM_BASE_URL",
        ])
        .or_else(|| file_cfg.proxy.upstream.base_url.clone())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

        let upstream_username = env_str_any(&[
            "REGISTRY__PROXY__UPSTREAM__USERNAME",
            "PROXY_UPSTREAM_USERNAME",
        ])
        .or_else(|| file_cfg.proxy.upstream.username.clone())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

        let upstream_password = env_str_any(&[
            "REGISTRY__PROXY__UPSTREAM__PASSWORD",
            "PROXY_UPSTREAM_PASSWORD",
        ])
        .or_else(|| file_cfg.proxy.upstream.password.clone())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

        let allowed_upstream_hosts = env_str_any(&[
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

        let allowed_repo_prefixes = env_str_any(&[
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

        let block_private_networks = env_bool_opt(&[
            "REGISTRY__PROXY__SAFETY__BLOCK_PRIVATE_NETWORKS",
            "PROXY_BLOCK_PRIVATE_NETWORKS",
        ])
        .or(file_cfg.proxy.safety.block_private_networks)
        .unwrap_or(true);

        let max_concurrent_upstream = env_usize_any(&[
            "REGISTRY__PROXY__SAFETY__MAX_CONCURRENT_UPSTREAM",
            "PROXY_MAX_CONCURRENT_UPSTREAM",
        ])
        .or(file_cfg.proxy.safety.max_concurrent_upstream)
        .unwrap_or(16);

        let cache_fs_root =
            env_str_any(&["REGISTRY__PROXY__CACHE__FS_ROOT", "PROXY_CACHE_FS_ROOT"])
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
            env_str_any(&["REGISTRY__PROXY__CACHE__S3_PREFIX", "PROXY_CACHE_S3_PREFIX"])
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

        let index_path = env_str_any(&["REGISTRY__PROXY__CACHE__INDEX_PATH", "PROXY_INDEX_PATH"])
            .or_else(|| file_cfg.proxy.cache.index_path.clone())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .map(PathBuf::from)
            .or_else(|| cache_fs_root.as_ref().map(|p| p.join("proxy-index")))
            .unwrap_or_else(|| PathBuf::from("./data/cache/proxy-index"));

        let max_cache_bytes = env_u64_any(&[
            "REGISTRY__PROXY__CACHE__MAX_CACHE_BYTES",
            "PROXY_MAX_CACHE_BYTES",
        ])
        .or(file_cfg.proxy.cache.max_cache_bytes);

        let gc_interval_secs = env_u64_any(&[
            "REGISTRY__PROXY__CACHE__GC_INTERVAL_SECS",
            "PROXY_GC_INTERVAL_SECS",
        ])
        .or(file_cfg.proxy.cache.gc_interval_secs)
        .unwrap_or(3600);

        let scrub_enabled = env_bool_opt(&[
            "REGISTRY__PROXY__CACHE__SCRUB_ENABLED",
            "PROXY_SCRUB_ENABLED",
        ])
        .or(file_cfg.proxy.cache.scrub_enabled)
        .unwrap_or(false);

        let scrub_interval_secs = env_u64_any(&[
            "REGISTRY__PROXY__CACHE__SCRUB_INTERVAL_SECS",
            "PROXY_SCRUB_INTERVAL_SECS",
        ])
        .or(file_cfg.proxy.cache.scrub_interval_secs)
        .unwrap_or(3600)
        .max(1);

        let scrub_max_files_per_run = env_usize_any(&[
            "REGISTRY__PROXY__CACHE__SCRUB_MAX_FILES_PER_RUN",
            "PROXY_SCRUB_MAX_FILES_PER_RUN",
        ])
        .or(file_cfg.proxy.cache.scrub_max_files_per_run)
        .unwrap_or(2000)
        .max(1);

        let repo_rules = file_cfg
            .proxy
            .repos
            .iter()
            .map(|r| {
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
                ProxyRepoRule {
                    match_pattern: r.match_pattern.trim().to_string(),
                    upstream_repo: r.upstream_repo.clone().map(|s| s.trim().to_string()),
                    tag_policy,
                    eviction_policy,
                }
            })
            .collect::<Vec<_>>();

        let routing_proxy_hosts = env_str_any(&[
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

        let routing_trust_x_forwarded_host = env_bool_opt(&[
            "REGISTRY__PROXY__ROUTING__TRUST_X_FORWARDED_HOST",
            "PROXY_ROUTING_TRUST_X_FORWARDED_HOST",
        ])
        .or(file_cfg.proxy.routing.trust_x_forwarded_host)
        .unwrap_or(false);

        let upstreams = resolve_proxy_upstreams(
            &storage_backend,
            &fs_root,
            &s3_prefix,
            routing_trust_x_forwarded_host,
            &file_cfg.proxy.safety,
            &file_cfg.proxy.upstreams,
        );

        if proxy_enabled {
            // Either single-upstream mode (proxy.upstream.*) or multi-upstream mode (proxy.upstreams).
            if upstream_base_url.is_none() && upstreams.is_empty() {
                panic!(
                    "proxy.enabled requires proxy.upstream.base_url (or PROXY_UPSTREAM_BASE_URL) OR proxy.upstreams[]"
                );
            }
            // In single-upstream mode, keep the existing requirement.
            if upstreams.is_empty() && max_cache_bytes.is_none() {
                panic!(
                    "proxy.enabled requires proxy.cache.max_cache_bytes (or PROXY_MAX_CACHE_BYTES) to be set"
                );
            }
        }

        let proxy = ProxyConfig {
            enabled: proxy_enabled,
            mode: proxy_mode,
            upstream_base_url,
            upstream_username,
            upstream_password,
            allowed_upstream_hosts,
            allowed_repo_prefixes,
            block_private_networks,
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

        Self {
            listen_addr,
            tls_cert_path,
            tls_key_path,
            push_username,
            push_password,
            push_allow_repos,
            storage_backend,
            fs_root,
            s3_endpoint,
            s3_region,
            s3_bucket,
            s3_prefix,
            allow_tag_overwrite,
            automatic_crossmount,
            upload_gc_enabled,
            upload_gc_interval_secs,
            upload_gc_max_age_secs,
            max_upload_bytes,
            max_request_body_bytes,
            max_concurrent_buffered_requests,
            max_concurrent_requests,
            request_timeout_secs,
            upload_request_timeout_secs,
            disallow_monolithic_uploads,
            upload_policy,
            catalog_requires_auth,
            public_url,
            token_service,
            token_signing_key,
            token_signing_keys,
            token_ttl_secs,

            robots,

            proxy,
        }
    }

    pub fn resolved_upload_policy_for_repo(&self, repo: &str) -> ResolvedUploadPolicy {
        for rule in &self.upload_policy.repo_rules {
            if wildcard_match(&rule.match_pattern, repo) {
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
    pub fn push_auth_configured(&self) -> bool {
        (self.push_username.is_some() && self.push_password.is_some())
            || (self.robots.enabled && !self.robots.accounts.is_empty())
    }
}

fn resolve_proxy_upstreams(
    storage_backend: &StorageBackend,
    fs_root: &PathBuf,
    s3_prefix: &str,
    routing_trust_x_forwarded_host: bool,
    global_safety: &FileProxySafety,
    file_upstreams: &[FileProxyUpstreamRoute],
) -> Vec<ProxyUpstreamRoute> {
    let upstreams = file_upstreams
        .iter()
        .enumerate()
        .map(|(i, r)| {
            let mut hosts = Vec::new();
            hosts.extend(
                r.hosts
                    .iter()
                    .map(|h| h.trim().to_string())
                    .filter(|h| !h.is_empty()),
            );
            if let Some(extra) = r.routing.hosts.as_ref() {
                hosts.extend(
                    extra
                        .iter()
                        .map(|h| h.trim().to_string())
                        .filter(|h| !h.is_empty()),
                );
            }
            hosts.sort();
            hosts.dedup();

            let upstream_base_url = r
                .upstream
                .base_url
                .as_deref()
                .or(r.base_url.as_deref())
                .unwrap_or("")
                .trim()
                .to_string();

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

            let max_cache_bytes = r.cache.max_cache_bytes.or(r.max_cache_bytes).unwrap_or(0);

            // Per-upstream safety: if fields are not set, fall back to global proxy.safety.
            let allowed_upstream_hosts = r
                .safety
                .allowed_upstream_hosts
                .clone()
                .or_else(|| global_safety.allowed_upstream_hosts.clone())
                .unwrap_or_default();
            let allowed_repo_prefixes = r
                .safety
                .allowed_repo_prefixes
                .clone()
                .or_else(|| global_safety.allowed_repo_prefixes.clone())
                .unwrap_or_default();
            let block_private_networks = r
                .safety
                .block_private_networks
                .or(global_safety.block_private_networks)
                .unwrap_or(true);
            let max_concurrent_upstream = r
                .safety
                .max_concurrent_upstream
                .or(global_safety.max_concurrent_upstream)
                .unwrap_or(16);

            // Validate (fail fast) when the feature is used.
            if !hosts.is_empty() {
                if upstream_base_url.is_empty() {
                    panic!("proxy.upstreams[{i}] requires [proxy.upstreams.upstream].base_url");
                }
                if max_cache_bytes == 0 {
                    panic!("proxy.upstreams[{i}] requires [proxy.upstreams.cache].max_cache_bytes");
                }

                match storage_backend {
                    StorageBackend::Filesystem => {
                        // cache fs_root is optional; derived from base_url when omitted.
                    }
                    StorageBackend::S3 => {
                        // cache s3_prefix and index_path are optional; derived from base_url when omitted.
                    }
                }
            }

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
                    StorageBackend::Filesystem => cache_fs_root
                        .as_ref()
                        .expect("filesystem cache fs_root")
                        .join("proxy-index"),
                    StorageBackend::S3 => derived_index_path.clone(),
                });

            ProxyUpstreamRoute {
                hosts,
                trust_x_forwarded_host,
                upstream_base_url,
                upstream_username,
                upstream_password,
                allowed_upstream_hosts,
                allowed_repo_prefixes,
                block_private_networks,
                max_concurrent_upstream,
                index_path,
                cache_fs_root,
                cache_s3_prefix,
                max_cache_bytes,
            }
        })
        .filter(|r| !r.hosts.is_empty())
        .collect::<Vec<_>>();

    // Validate: upstream host patterns must be unique across routes.
    // (If the same host pattern appears in more than one upstream route, routing would be
    // ambiguous because we select the first match.)
    {
        let mut seen: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        for (idx, up) in upstreams.iter().enumerate() {
            for host_pat in &up.hosts {
                let key = host_pat.trim().to_ascii_lowercase();
                if key.is_empty() {
                    continue;
                }
                if let Some(prev) = seen.insert(key.clone(), idx) {
                    panic!(
                        "proxy.upstreams has duplicate host pattern '{}' (indices {} and {})",
                        host_pat, prev, idx
                    );
                }
            }
        }
    }

    upstreams
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
        );

        assert_eq!(upstreams.len(), 2);

        let dockerhub = upstreams
            .iter()
            .find(|u| u.upstream_base_url == "https://registry-1.docker.io")
            .expect("dockerhub upstream");
        assert_eq!(dockerhub.hosts, vec!["dockerhub-cache.local".to_string()]);
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
        assert_eq!(ghcr.hosts, vec!["ghcr-cache.local".to_string()]);
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
}

fn wildcard_match(pattern: &str, value: &str) -> bool {
    // Minimal glob: '*' matches any substring.
    if pattern == "*" {
        return true;
    }
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == value;
    }

    let mut rest = value;
    let mut first = true;
    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        if first && !pattern.starts_with('*') {
            if !rest.starts_with(part) {
                return false;
            }
            rest = &rest[part.len()..];
            first = false;
            continue;
        }

        if let Some(pos) = rest.find(part) {
            rest = &rest[pos + part.len()..];
        } else {
            return false;
        }

        if i == parts.len() - 1 && !pattern.ends_with('*') {
            return rest.is_empty();
        }

        first = false;
    }
    true
}

fn parse_toml_config(contents: &str) -> Result<(FileConfig, Vec<String>), toml::de::Error> {
    let mut ignored_paths = Vec::<String>::new();
    let deser = toml::de::Deserializer::new(contents);
    let cfg = serde_ignored::deserialize(deser, |path| {
        ignored_paths.push(path.to_string());
    })?;
    Ok((cfg, ignored_paths))
}

fn load_config_file() -> LoadedFileConfig {
    let Some(path) = env_str_any(&["CONFIG_PATH", "REGISTRY__CONFIG_PATH"]) else {
        return LoadedFileConfig::default();
    };
    let path = path.trim();
    if path.is_empty() {
        return LoadedFileConfig::default();
    }
    let contents = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(err) => {
            eprintln!("Failed to read CONFIG_PATH='{path}': {err}");
            return LoadedFileConfig::default();
        }
    };
    match parse_toml_config(&contents) {
        Ok((cfg, ignored_paths)) => LoadedFileConfig { cfg, ignored_paths },
        Err(err) => {
            eprintln!("Failed to parse config file '{path}' as TOML: {err}");
            LoadedFileConfig::default()
        }
    }
}

fn env_str_any(keys: &[&str]) -> Option<String> {
    for k in keys {
        if let Ok(v) = std::env::var(k) {
            let v = v.trim().to_string();
            if !v.is_empty() {
                return Some(v);
            }
        }
    }
    None
}

fn env_bool_opt(keys: &[&str]) -> Option<bool> {
    let Some(v) = env_str_any(keys) else {
        return None;
    };
    let v = v.trim().to_ascii_lowercase();
    match v.as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn env_u64_any(keys: &[&str]) -> Option<u64> {
    env_str_any(keys).and_then(|s| s.trim().parse::<u64>().ok())
}

fn env_usize_any(keys: &[&str]) -> Option<usize> {
    env_str_any(keys).and_then(|s| s.trim().parse::<usize>().ok())
}

fn env_socket_addr(keys: &[&str]) -> Option<SocketAddr> {
    env_str_any(keys).and_then(|s| s.parse::<SocketAddr>().ok())
}
