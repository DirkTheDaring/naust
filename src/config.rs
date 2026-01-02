use std::{net::SocketAddr, path::PathBuf};

#[derive(Clone, Debug)]
pub struct Config {
    pub listen_addr: SocketAddr,

    pub push_username: Option<String>,
    pub push_password: Option<String>,

    pub storage_backend: StorageBackend,

    pub fs_root: PathBuf,

    pub s3_endpoint: Option<String>,
    pub s3_region: Option<String>,
    pub s3_bucket: Option<String>,
    pub s3_prefix: String,
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

        let push_username = std::env::var("REGISTRY_USERNAME").ok();
        let push_password = std::env::var("REGISTRY_PASSWORD").ok();

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

        Self {
            listen_addr,
            push_username,
            push_password,
            storage_backend,
            fs_root,
            s3_endpoint,
            s3_region,
            s3_bucket,
            s3_prefix,
        }
    }

    pub fn push_auth_configured(&self) -> bool {
        self.push_username.is_some() && self.push_password.is_some()
    }
}
