use std::path::PathBuf;
use thiserror::Error;

use crate::blob_ref_index::RefIndexError;
use crate::config::ConfigError;
use crate::gc_service::GcServiceError;
use crate::storage::StorageError;

#[derive(Debug, Error)]
pub enum CliError {
    #[error("failed to load config: {0}")]
    Config(#[from] ConfigError),

    #[error("ref-index is disabled (storage.ref_index.enabled=false)")]
    RefIndexDisabled,

    #[error("refusing to run while registry is active ({0}); stop the server first")]
    ServerActive(String),

    #[error("failed to acquire exclusive deployment writer authority: {0}")]
    LockContention(StorageError),

    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("ref-index error: {0}")]
    Index(#[from] RefIndexError),

    #[error("ref-index not found at {path}; run `ref-index rebuild` or `ensure` to initialize it")]
    IndexMissing { path: PathBuf },

    #[error(
        "ref-index at {path} is unhealthy: {reason}; run `ref-index rebuild` or `ensure` to repair it"
    )]
    IndexUnhealthy { path: PathBuf, reason: String },

    #[error("blob-gc error: {0}")]
    Gc(#[from] GcServiceError),

    #[error(
        "repository blob memberships require migration; run `migrate-membership apply` before proceeding"
    )]
    MembershipBackfillRequired,

    #[error(
        "blob-gc: destructive offline GC on S3 storage requires explicit confirmation that all writers using bucket '{bucket}' (prefix '{prefix}') are stopped.\nRe-run with --confirm-all-writers-stopped to proceed, or use 'plan' for dry-run."
    )]
    S3GcConfirmationRequired { bucket: String, prefix: String },

    #[error("empty secret provided")]
    EmptySecret,

    #[error("failed to read from stdin: {0}")]
    StdinRead(#[source] std::io::Error),

    #[error("hashing failed: {0}")]
    Hashing(String),

    #[error(
        "verification failed: one or more referenced blobs lack membership records. Run `migrate-membership apply`."
    )]
    MembershipVerificationFailed,

    #[error("flush failed: {0}")]
    Flush(String),

    #[error("authority release failed: {0}")]
    AuthorityRelease(StorageError),

    #[error("{source}; authority release also failed: {release_error}")]
    ExecutionAndTeardownFailed {
        source: Box<CliError>,
        release_error: StorageError,
    },

    #[error("server error: {0}")]
    Server(String),

    #[error("admin-clear-lock failed: {0}")]
    AdminClearLock(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("unsupported operation on storage backend: {0}")]
    Unsupported(String),
}

impl CliError {
    pub fn exit_code(&self) -> i32 {
        match self {
            CliError::Config(_)
            | CliError::RefIndexDisabled
            | CliError::ServerActive(_)
            | CliError::S3GcConfirmationRequired { .. } => 2,

            CliError::ExecutionAndTeardownFailed { source, .. } => source.exit_code(),

            _ => 1,
        }
    }
}
