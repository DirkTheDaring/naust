use serde::Deserialize;
use std::{net::SocketAddr, path::PathBuf};

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
    pub request_timeout_secs: u64,

    // Longer timeout for upload endpoints (PATCH/PUT/POST blobs/uploads).
    pub upload_request_timeout_secs: u64,

    // If true, reject monolithic blob uploads (body on POST ?digest or PUT finalize).
    // This forces clients to use PATCH-based chunked uploads.
    pub disallow_monolithic_uploads: bool,

    // If true, repository/org listing endpoints require authentication.
    pub catalog_requires_auth: bool,

    pub public_url: Option<String>,
    pub token_service: String,
    pub token_signing_key: String,
    pub token_ttl_secs: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StorageBackend {
    Filesystem,
    S3,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileConfig {
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
    ttl_secs: Option<u64>,
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
    gc_enabled: Option<bool>,
    #[serde(default)]
    gc_interval_secs: Option<u64>,
    #[serde(default)]
    gc_max_age_secs: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct FileLimits {
    #[serde(default)]
    max_upload_bytes: Option<u64>,
    #[serde(default)]
    max_request_body_bytes: Option<usize>,
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
        let file_cfg = load_config_file();

        let best_practice = env_bool_opt(&["BEST_PRACTICE"]).unwrap_or(false)
            || file_cfg
                .profile
                .name
                .as_deref()
                .map(|s| s.eq_ignore_ascii_case("best_practice"))
                .unwrap_or(false);

        let listen_addr = env_socket_addr(&["REGISTRY__SERVER__LISTEN_ADDR", "LISTEN_ADDR"]).unwrap_or_else(|| {
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

        let allow_tag_overwrite = env_bool_opt(&["REGISTRY__FEATURES__ALLOW_TAG_OVERWRITE", "ALLOW_TAG_OVERWRITE"])
            .or(file_cfg.features.allow_tag_overwrite)
            .unwrap_or_else(|| !best_practice);

        let automatic_crossmount = env_bool_opt(&["REGISTRY__FEATURES__AUTOMATIC_CROSSMOUNT", "REGISTRY_AUTOMATIC_CROSSMOUNT"])
            .or(file_cfg.features.automatic_crossmount)
            .unwrap_or(false);

        let upload_gc_enabled = env_bool_opt(&["REGISTRY__UPLOADS__GC_ENABLED", "UPLOAD_GC_ENABLED"])
            .or(file_cfg.uploads.gc_enabled)
            .unwrap_or(true);

        let upload_gc_interval_secs = env_u64_any(&["REGISTRY__UPLOADS__GC_INTERVAL_SECS", "UPLOAD_GC_INTERVAL_SECS"])
            .or(file_cfg.uploads.gc_interval_secs)
            .unwrap_or(3600);

        let upload_gc_max_age_secs = env_u64_any(&["REGISTRY__UPLOADS__GC_MAX_AGE_SECS", "UPLOAD_GC_MAX_AGE_SECS"])
            .or(file_cfg.uploads.gc_max_age_secs)
            .unwrap_or(24 * 3600);

        let max_upload_bytes = env_u64_any(&["REGISTRY__LIMITS__MAX_UPLOAD_BYTES", "MAX_UPLOAD_BYTES"])
            .or(file_cfg.limits.max_upload_bytes)
            .unwrap_or(5 * 1024 * 1024 * 1024);

        let max_request_body_bytes = env_usize_any(&["REGISTRY__LIMITS__MAX_REQUEST_BODY_BYTES", "MAX_REQUEST_BODY_BYTES"])
            .or(file_cfg.limits.max_request_body_bytes)
            .unwrap_or(32 * 1024 * 1024);

        let request_timeout_secs = env_u64_any(&["REGISTRY__TIMEOUTS__REQUEST_TIMEOUT_SECS", "REQUEST_TIMEOUT_SECS"])
            .or(file_cfg.timeouts.request_timeout_secs)
            .unwrap_or(if best_practice { 60 } else { 300 });

        let upload_request_timeout_secs =
            env_u64_any(&["REGISTRY__TIMEOUTS__UPLOAD_REQUEST_TIMEOUT_SECS", "UPLOAD_REQUEST_TIMEOUT_SECS"])
                .or(file_cfg.timeouts.upload_request_timeout_secs)
                .unwrap_or(if best_practice { 7200 } else { 3600 });

        let disallow_monolithic_uploads =
            env_bool_opt(&["REGISTRY__UPLOADS__DISALLOW_MONOLITHIC_UPLOADS", "DISALLOW_MONOLITHIC_UPLOADS"])
                .or(file_cfg.uploads.disallow_monolithic_uploads)
                .unwrap_or(best_practice);

        let catalog_requires_auth = env_bool_opt(&["REGISTRY__CATALOG__REQUIRES_AUTH", "CATALOG_REQUIRES_AUTH"])
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

        let token_signing_key = env_str_any(&["REGISTRY__TOKEN__SIGNING_KEY", "TOKEN_SIGNING_KEY"])
            .or_else(|| file_cfg.token.signing_key.clone())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| {
                if best_practice {
                    panic!(
                        "best_practice requires TOKEN_SIGNING_KEY (or config token.signing_key) to be set"
                    );
                }
                uuid::Uuid::new_v4().to_string()
            });

        let token_ttl_secs = env_u64_any(&["REGISTRY__TOKEN__TTL_SECS", "TOKEN_TTL_SECS"])
            .or(file_cfg.token.ttl_secs)
            .unwrap_or(600);

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
            request_timeout_secs,
            upload_request_timeout_secs,
            disallow_monolithic_uploads,
            catalog_requires_auth,
            public_url,
            token_service,
            token_signing_key,
            token_ttl_secs,
        }
    }

    pub fn push_auth_configured(&self) -> bool {
        self.push_username.is_some() && self.push_password.is_some()
    }
}

fn load_config_file() -> FileConfig {
    let Some(path) = env_str_any(&["CONFIG_PATH", "REGISTRY__CONFIG_PATH"]) else {
        return FileConfig::default();
    };
    let path = path.trim();
    if path.is_empty() {
        return FileConfig::default();
    }
    let contents = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(err) => {
            eprintln!("Failed to read CONFIG_PATH='{path}': {err}");
            return FileConfig::default();
        }
    };
    match toml::from_str::<FileConfig>(&contents) {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("Failed to parse config file '{path}' as TOML: {err}");
            FileConfig::default()
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
