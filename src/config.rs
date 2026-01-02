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

impl Config {
    pub fn from_env() -> Self {
        let listen_addr = std::env::var("LISTEN_ADDR")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| ([127, 0, 0, 1], 5000).into());

        let tls_cert_path = std::env::var("TLS_CERT_PATH")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .map(PathBuf::from);
        let tls_key_path = std::env::var("TLS_KEY_PATH")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .map(PathBuf::from);

        let push_username = std::env::var("REGISTRY_USERNAME").ok();
        let push_password = std::env::var("REGISTRY_PASSWORD").ok();

        let push_allow_repos = std::env::var("REGISTRY_PUSH_ALLOW_REPOS")
            .ok()
            .map(|s| {
                s.split(',')
                    .map(|p| p.trim().to_string())
                    .filter(|p| !p.is_empty())
                    .collect::<Vec<_>>()
            })
            .filter(|v| !v.is_empty());

        let storage_backend = match std::env::var("STORAGE_BACKEND")
            .ok()
            .as_deref()
            .unwrap_or("fs")
            .to_ascii_lowercase()
            .as_str()
        {
            "fs" | "filesystem" => StorageBackend::Filesystem,
            "s3" => StorageBackend::S3,
            other => {
                eprintln!("Unknown STORAGE_BACKEND='{other}', defaulting to fs");
                StorageBackend::Filesystem
            }
        };

        let fs_root = std::env::var("STORAGE_FS_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("./data"));

        let s3_endpoint = std::env::var("STORAGE_S3_ENDPOINT").ok();
        let s3_region = std::env::var("STORAGE_S3_REGION").ok();
        let s3_bucket = std::env::var("STORAGE_S3_BUCKET").ok();
        let s3_prefix = std::env::var("STORAGE_S3_PREFIX").unwrap_or_else(|_| "registry".to_string());

        let allow_tag_overwrite = std::env::var("ALLOW_TAG_OVERWRITE")
            .ok()
            .as_deref()
            .unwrap_or("1")
            .trim()
            .to_ascii_lowercase();
        let allow_tag_overwrite = !(allow_tag_overwrite == "0" || allow_tag_overwrite == "false" || allow_tag_overwrite == "no");

        let automatic_crossmount = std::env::var("REGISTRY_AUTOMATIC_CROSSMOUNT")
            .ok()
            .as_deref()
            .unwrap_or("0")
            .trim()
            .to_ascii_lowercase();
        let automatic_crossmount =
            automatic_crossmount == "1" || automatic_crossmount == "true" || automatic_crossmount == "yes";

        let upload_gc_enabled = std::env::var("UPLOAD_GC_ENABLED")
            .ok()
            .as_deref()
            .unwrap_or("1")
            .trim()
            .to_ascii_lowercase();
        let upload_gc_enabled = !(upload_gc_enabled == "0" || upload_gc_enabled == "false" || upload_gc_enabled == "no");

        let upload_gc_interval_secs = std::env::var("UPLOAD_GC_INTERVAL_SECS")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(3600);

        let upload_gc_max_age_secs = std::env::var("UPLOAD_GC_MAX_AGE_SECS")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(24 * 3600);

        let max_upload_bytes = std::env::var("MAX_UPLOAD_BYTES")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(5 * 1024 * 1024 * 1024);

        let max_request_body_bytes = std::env::var("MAX_REQUEST_BODY_BYTES")
            .ok()
            .and_then(|s| s.trim().parse::<usize>().ok())
            .unwrap_or(32 * 1024 * 1024);

        let request_timeout_secs = std::env::var("REQUEST_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(300);

        let upload_request_timeout_secs = std::env::var("UPLOAD_REQUEST_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(3600);

        let disallow_monolithic_uploads = std::env::var("DISALLOW_MONOLITHIC_UPLOADS")
            .ok()
            .as_deref()
            .unwrap_or("0")
            .trim()
            .to_ascii_lowercase();
        let disallow_monolithic_uploads =
            disallow_monolithic_uploads == "1" || disallow_monolithic_uploads == "true" || disallow_monolithic_uploads == "yes";

        let catalog_requires_auth = std::env::var("CATALOG_REQUIRES_AUTH")
            .ok()
            .as_deref()
            .unwrap_or("0")
            .trim()
            .to_ascii_lowercase();
        let catalog_requires_auth =
            catalog_requires_auth == "1" || catalog_requires_auth == "true" || catalog_requires_auth == "yes";

        let public_url = std::env::var("PUBLIC_URL")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        let token_service = std::env::var("TOKEN_SERVICE")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "registry-rust".to_string());

        // NOTE: for real deployments set TOKEN_SIGNING_KEY to a random secret.
        // We fall back to a process-local random-ish value so dev works out-of-the-box.
        let token_signing_key = std::env::var("TOKEN_SIGNING_KEY")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        let token_ttl_secs = std::env::var("TOKEN_TTL_SECS")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
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
