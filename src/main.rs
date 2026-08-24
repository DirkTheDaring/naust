mod audit;
mod auth;
mod blob_delete_safety;
mod blob_gc;
mod blob_ref_index;
mod gc_service;
mod config;
mod fs_root_lock;
mod http_api;
mod ip_concurrency;
mod manifest_refs;
mod proxy;
mod rbac;
mod registry;
mod request_routing;
mod robot_secrets;
mod security;
mod storage;
mod token_rate_limit;

use axum::{
    Router,
    extract::DefaultBodyLimit,
    http::HeaderMap,
    http::Request,
    middleware::Next,
    response::IntoResponse,
    routing::{any, get, post},
};
use clap::{Parser, Subcommand};
use config::{Config, StorageBackend};
use http_api::handlers;
use semver::Version;
use sha2::Digest as _;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::token_rate_limit::{TokenRateLimiter, limit_token_requests};

fn install_rustls_crypto_provider() {
    // rustls 0.23 requires selecting a process-wide CryptoProvider when multiple
    // providers are enabled via crate features (e.g. both aws-lc-rs and ring).
    // We prefer aws-lc-rs here.
    let provider = rustls::crypto::aws_lc_rs::default_provider();
    // Ignore "already installed" errors (e.g. in tests or if a dependency installed it first).
    let _ = rustls::crypto::CryptoProvider::install_default(provider);
}

async fn maybe_generate_tls_certs(cfg: &Config) {
    let Some(acme) = cfg.tls_acme.as_ref() else {
        return;
    };

    use acmecert_core::prefab::{ExecHook, IsponeHttpHook};
    use acmecert_core::types::{AuthorizationHeader, ProxyUrl};

    tracing::info!(
        output_dir = %acme.output_dir.display(),
        names = ?acme.names,
        provider = %match &acme.provider {
            config::AcmeProvider::Ispone { .. } => "ispone",
            config::AcmeProvider::ExecPath { .. } => "exec_path",
        },
        "acme: ensuring TLS certificate"
    );

    let proxy = match acme.proxy.as_deref() {
        Some(s) => match s.parse::<ProxyUrl>() {
            Ok(p) => Some(p),
            Err(_) => {
                tracing::error!(
                    proxy = %s,
                    output_dir = %acme.output_dir.display(),
                    names = ?acme.names,
                    "acme.proxy is not a valid URL"
                );
                std::process::exit(2);
            }
        },
        None => None,
    };

    let propagation_check = acmecert_core::api::PropagationCheck::from_flags(
        acme.propagation_check_disabled,
        acme.propagation_check_strict,
    );

    let req = acmecert_core::simple::PemDirRequest::new(
        acme.email.clone(),
        acme.names.clone(),
        acme.output_dir.clone(),
    );
    let opts = acmecert_core::simple::PemDirOptions {
        allow_first_wildcard: acme.allow_first_wildcard,
        proxy: acme.proxy.clone(),
        propagation_check,
        renewal_window: Duration::from_secs(acme.renewal_window_secs.max(1)),
    };

    match &acme.provider {
        config::AcmeProvider::Ispone {
            base_url,
            authorization,
        } => {
            let authorization = match AuthorizationHeader::from_token_or_header_value(authorization)
            {
                Ok(a) => a,
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        output_dir = %acme.output_dir.display(),
                        names = ?acme.names,
                        "acme.ispone.authorization is invalid"
                    );
                    std::process::exit(2);
                }
            };

            let hook = match IsponeHttpHook::new(base_url.clone(), authorization, proxy, acme.debug)
            {
                Ok(h) => h,
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        base_url = %base_url,
                        output_dir = %acme.output_dir.display(),
                        names = ?acme.names,
                        "failed to initialize ispone hook"
                    );
                    std::process::exit(1);
                }
            };

            if let Err(e) = acmecert_core::simple::generate_pem_dir(req, opts, &hook).await {
                // If we already have a cert/key on disk, keep the service available.
                // If not, fail fast as before.
                let cert_path = acme.output_dir.join("cert.pem");
                let key_path = acme.output_dir.join("key.pem");
                if cert_path.exists() && key_path.exists() {
                    tracing::warn!(
                        error = %e,
                        cert_path = %cert_path.display(),
                        key_path = %key_path.display(),
                        "acme: provisioning failed; using existing certificate"
                    );
                } else {
                    tracing::error!(
                        error = ?e,
                        cert_path = %cert_path.display(),
                        key_path = %key_path.display(),
                        output_dir = %acme.output_dir.display(),
                        names = ?acme.names,
                        "acme: provisioning failed and no existing certificate is present"
                    );
                    std::process::exit(1);
                }
            }
        }
        config::AcmeProvider::ExecPath { exec_path } => {
            let hook = ExecHook {
                path: exec_path.clone(),
                debug: acme.debug,
            };

            if let Err(e) = acmecert_core::simple::generate_pem_dir(req, opts, &hook).await {
                let cert_path = acme.output_dir.join("cert.pem");
                let key_path = acme.output_dir.join("key.pem");
                if cert_path.exists() && key_path.exists() {
                    tracing::warn!(
                        error = %e,
                        cert_path = %cert_path.display(),
                        key_path = %key_path.display(),
                        "acme: provisioning failed; using existing certificate"
                    );
                } else {
                    tracing::error!(
                        error = ?e,
                        cert_path = %cert_path.display(),
                        key_path = %key_path.display(),
                        output_dir = %acme.output_dir.display(),
                        names = ?acme.names,
                        "acme: provisioning failed and no existing certificate is present"
                    );
                    std::process::exit(1);
                }
            }
        }
    }

    tracing::info!("acme: TLS certificate ready");
}

#[derive(Debug, Parser)]
#[command(
    name = "registry-rust",
    about = "Minimal Docker/OCI registry (Distribution v2-ish)",
    version,
    arg_required_else_help = true,
    disable_help_subcommand = true
)]
struct Cli {
    /// Path to a TOML config file.
    ///
    /// Can be provided multiple times; later files override earlier ones.
    ///
    /// Merge behavior:
    /// - tables deep-merge
    /// - arrays/lists are replaced wholesale
    /// - scalar values are overridden
    #[arg(
        short = 'c',
        long = "config",
        value_name = "PATH",
        global = true,
        action = clap::ArgAction::Append
    )]
    config: Vec<std::path::PathBuf>,

    #[command(subcommand)]
    command: CliCommand,
}

#[derive(Debug, Subcommand)]
enum CliCommand {
    /// Run the registry server.
    #[command(name = "server")]
    Server,

    /// Parse and validate configuration, then exit.
    #[command(name = "check-config")]
    CheckConfig,

    /// Read a secret from stdin and print an Argon2id hash (for robots/users).
    #[command(name = "hash-secret")]
    HashSecret,

    /// Print effective RBAC permissions (robots + users/groups) and exit.
    #[command(name = "audit-permissions")]
    AuditPermissions,

    /// Inspect / rebuild the persistent blob reference index.
    #[command(name = "ref-index")]
    RefIndex {
        #[command(subcommand)]
        command: RefIndexCommand,
    },

    /// Reclaim storage by quarantining/deleting unreferenced blobs (filesystem backend only).
    #[command(name = "blob-gc")]
    BlobGc {
        #[command(subcommand)]
        command: BlobGcCommand,
    },
}

#[derive(Debug, Subcommand)]
enum RefIndexCommand {
    /// Verify the index is healthy (schema + ready state). Exit 0 if OK, 1 if corrupt.
    #[command(name = "check")]
    Check,

    /// Rebuild the index from the registry storage.
    #[command(name = "rebuild")]
    Rebuild,

    /// Check and rebuild if corrupt (respects auto-rebuild config).
    #[command(name = "ensure")]
    Ensure,
}

#[derive(Debug, Subcommand)]
enum BlobGcCommand {
    /// Print what would be quarantined (dry-run).
    #[command(name = "plan")]
    Plan {
        /// Reference policy to decide whether a blob is considered in use.
        #[arg(long, value_enum, default_value_t = crate::blob_gc::BlobGcPolicy::ManifestRooted)]
        policy: crate::blob_gc::BlobGcPolicy,

        /// Only consider blobs older than this age.
        #[arg(long, default_value_t = 7 * 24 * 3600)]
        min_age_secs: u64,

        /// Maximum number of blobs to report.
        #[arg(long, default_value_t = 10_000)]
        max_per_run: usize,
    },

    /// Move eligible blobs into quarantine (reversible).
    #[command(name = "quarantine")]
    Quarantine {
        #[arg(long, value_enum, default_value_t = crate::blob_gc::BlobGcPolicy::ManifestRooted)]
        policy: crate::blob_gc::BlobGcPolicy,

        #[arg(long, default_value_t = 7 * 24 * 3600)]
        min_age_secs: u64,

        #[arg(long, default_value_t = 10_000)]
        max_per_run: usize,
    },

    /// Permanently delete blobs from quarantine after a delay (re-checks reachability).
    #[command(name = "delete")]
    Delete {
        #[arg(long, value_enum, default_value_t = crate::blob_gc::BlobGcPolicy::ManifestRooted)]
        policy: crate::blob_gc::BlobGcPolicy,

        /// A quarantined blob must be at least this old before it can be deleted.
        #[arg(long, default_value_t = 24 * 3600)]
        quarantine_delay_secs: u64,

        #[arg(long, default_value_t = 10_000)]
        max_per_run: usize,
    },
}

#[derive(Debug, Default)]
pub struct AuthMetrics {
    token_issued_total: AtomicU64,
    token_denied_total: AtomicU64,
    token_internal_error_total: AtomicU64,
}

impl AuthMetrics {
    pub fn inc_token_issued(&self) -> u64 {
        self.token_issued_total.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn inc_token_denied(&self) -> u64 {
        self.token_denied_total.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn inc_token_internal_error(&self) -> u64 {
        self.token_internal_error_total
            .fetch_add(1, Ordering::Relaxed)
            + 1
    }
}

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub auth_metrics: Arc<AuthMetrics>,
    pub storage: Arc<dyn storage::Storage>,
    pub ref_index: Option<Arc<blob_ref_index::BlobRefIndex>>,
    pub gc_service: Option<Arc<gc_service::GcService>>,
    pub gc_run_seq: Arc<AtomicU64>,
    pub proxy: Option<Arc<proxy::Proxy>>,
    pub proxy_cache: Option<Arc<dyn storage::Storage>>,
    // Multi-upstream: proxy/cache selected per request host.
    pub proxy_upstreams: Vec<ProxyContext>,
    pub buffered_body_sem: Arc<Semaphore>,
    pub request_sem: Arc<Semaphore>,
    pub upload_request_sem: Arc<Semaphore>,

    // Diagnostics: in-flight request counters and rate-limited saturation logs.
    pub active_non_upload_requests: Arc<AtomicU64>,
    pub active_upload_requests: Arc<AtomicU64>,
    pub last_sem_saturation_log_unix_secs: Arc<AtomicU64>,

    // Security & Anti-Slowloris:
    pub ip_limiter: Arc<ip_concurrency::IpConcurrencyLimiter>,
    pub is_high_pressure: Arc<std::sync::atomic::AtomicBool>,
}

#[derive(Clone)]
pub struct ProxyContext {
    pub proxy: Arc<proxy::Proxy>,
    pub cache: Arc<dyn storage::Storage>,
}

impl AppState {
    pub fn proxy_context_for_request(&self, headers: &HeaderMap) -> Option<ProxyContext> {
        // Multi-upstream mode: choose the upstream by request host.
        if !self.config.proxy.upstreams.is_empty() {
            let idx = crate::request_routing::proxy_upstream_index_for_request(
                &self.config.proxy,
                headers,
            )?;
            return self.proxy_upstreams.get(idx).cloned();
        }

        // Single-upstream mode.
        match (self.proxy.as_ref(), self.proxy_cache.as_ref()) {
            (Some(proxy), Some(cache)) => Some(ProxyContext {
                proxy: proxy.clone(),
                cache: cache.clone(),
            }),
            _ => None,
        }
    }

    pub fn current_stream_guard_params(&self) -> (Duration, u64) {
        let total_permits = self.config.max_concurrent_upload_requests.max(1) as f64;
        let active = self.active_upload_requests.load(Ordering::Relaxed) as f64;
        let load_ratio = active / total_permits;

        let currently_high = self.is_high_pressure.load(Ordering::Relaxed);
        let new_high = if currently_high {
            load_ratio >= 0.70
        } else {
            load_ratio >= 0.85
        };

        if new_high != currently_high {
            self.is_high_pressure.store(new_high, Ordering::Relaxed);
            tracing::info!(
                high_pressure = new_high,
                load_ratio = %format!("{:.1}%", load_ratio * 100.0),
                "adaptive slowloris defense mode transitioned"
            );
        }

        if new_high {
            (
                Duration::from_secs(self.config.upload_chunk_idle_timeout_secs.min(8)),
                self.config.min_upload_bytes_per_sec.saturating_mul(2),
            )
        } else {
            (
                Duration::from_secs(self.config.upload_chunk_idle_timeout_secs),
                self.config.min_upload_bytes_per_sec,
            )
        }
    }
}

#[tokio::main]
async fn main() {
    install_rustls_crypto_provider();

    let cli = Cli::parse();

    let config_paths = cli.config;

    match cli.command {
        CliCommand::CheckConfig => {
            let _cfg = match std::panic::catch_unwind(|| Config::from_env_with_files(&config_paths)) {
                Ok(c) => c,
                Err(err) => {
                    let msg = if let Some(s) = err.downcast_ref::<String>() {
                        s.clone()
                    } else if let Some(s) = err.downcast_ref::<&str>() {
                        (*s).to_string()
                    } else {
                        "<non-string panic>".to_string()
                    };
                    eprintln!("check-config: failed to load config: {msg}");
                    std::process::exit(2);
                }
            };

            println!("OK");
            return;
        }
        CliCommand::AuditPermissions => {
            // Access audit: print effective permissions and exit.
            let cfg = match std::panic::catch_unwind(|| Config::from_env_with_files(&config_paths)) {
                Ok(c) => c,
                Err(err) => {
                    let msg = if let Some(s) = err.downcast_ref::<String>() {
                        s.clone()
                    } else if let Some(s) = err.downcast_ref::<&str>() {
                        (*s).to_string()
                    } else {
                        "<non-string panic>".to_string()
                    };
                    eprintln!("audit-permissions: failed to load config: {msg}");
                    std::process::exit(2);
                }
            };

            let _report = crate::audit::print_audit(&cfg);
            return;
        }
        CliCommand::RefIndex { command } => {
            // CLI utilities should not panic; keep errors user-friendly.
            let cfg = match std::panic::catch_unwind(|| {
                if config_paths.is_empty() {
                    Config::from_env()
                } else {
                    Config::from_env_with_files(&config_paths)
                }
            }) {
                Ok(c) => c,
                Err(err) => {
                    let msg = if let Some(s) = err.downcast_ref::<String>() {
                        s.clone()
                    } else if let Some(s) = err.downcast_ref::<&str>() {
                        (*s).to_string()
                    } else {
                        "<non-string panic>".to_string()
                    };
                    eprintln!("ref-index: failed to load config: {msg}");
                    std::process::exit(2);
                }
            };

            if !cfg.ref_index.enabled {
                eprintln!("ref-index is disabled (storage.ref_index.enabled=false)");
                std::process::exit(2);
            }

            let _fs_root_lock = if cfg.storage_backend == StorageBackend::Filesystem {
                match crate::fs_root_lock::FsRootLock::try_acquire(&cfg.fs_root) {
                    Ok(l) => Some(l),
                    Err(e) => {
                        eprintln!(
                            "ref-index: refusing to run while registry is active ({e}); stop the server first"
                        );
                        std::process::exit(2);
                    }
                }
            } else {
                None
            };

            let idx = match blob_ref_index::BlobRefIndex::open(cfg.ref_index.path.clone()) {
                Ok(i) => i,
                Err(e) => {
                    eprintln!("ref-index: failed to open {}: {e}", cfg.ref_index.path.display());
                    std::process::exit(1);
                }
            };

            match command {
                RefIndexCommand::Check => match idx.check_health() {
                    Ok(()) => {
                        println!("OK");
                        return;
                    }
                    Err(e) => {
                        eprintln!("ref-index: {e}");
                        std::process::exit(1);
                    }
                },
                RefIndexCommand::Rebuild => {
                    let storage = storage::from_config(&cfg);
                    if let Err(e) = idx.rebuild(&storage).await {
                        eprintln!("ref-index: rebuild failed: {e}");
                        std::process::exit(1);
                    }
                    println!("OK");
                    return;
                }
                RefIndexCommand::Ensure => {
                    let storage = storage::from_config(&cfg);
                    if let Err(e) = idx
                        .ensure_healthy_or_rebuild(
                            &storage,
                            cfg.ref_index.auto_rebuild_on_corruption,
                            cfg.ref_index.rebuild_on_start,
                        )
                        .await
                    {
                        eprintln!("ref-index: ensure failed: {e}");
                        std::process::exit(1);
                    }
                    println!("OK");
                    return;
                }
            }
        }
        CliCommand::BlobGc { command } => {
            let cfg = match std::panic::catch_unwind(|| {
                if config_paths.is_empty() {
                    Config::from_env()
                } else {
                    Config::from_env_with_files(&config_paths)
                }
            }) {
                Ok(c) => c,
                Err(err) => {
                    let msg = if let Some(s) = err.downcast_ref::<String>() {
                        s.clone()
                    } else if let Some(s) = err.downcast_ref::<&str>() {
                        (*s).to_string()
                    } else {
                        "<non-string panic>".to_string()
                    };
                    eprintln!("blob-gc: failed to load config: {msg}");
                    std::process::exit(2);
                }
            };

            if cfg.storage_backend != StorageBackend::Filesystem {
                eprintln!("blob-gc: only filesystem backend is supported");
                std::process::exit(2);
            }

            if !cfg.ref_index.enabled {
                eprintln!("blob-gc: ref-index is disabled (storage.ref_index.enabled=false)");
                std::process::exit(2);
            }

            let _fs_root_lock = match crate::fs_root_lock::FsRootLock::try_acquire(&cfg.fs_root) {
                Ok(l) => l,
                Err(e) => {
                    eprintln!(
                        "blob-gc: refusing to run while registry is active ({e}); stop the server first"
                    );
                    std::process::exit(2);
                }
            };

            let idx = match blob_ref_index::BlobRefIndex::open(cfg.ref_index.path.clone()) {
                Ok(i) => i,
                Err(e) => {
                    eprintln!("blob-gc: failed to open ref-index {}: {e}", cfg.ref_index.path.display());
                    std::process::exit(1);
                }
            };

            let storage = storage::from_config(&cfg);
            if let Err(e) = idx
                .ensure_healthy_or_rebuild(
                    &storage,
                    cfg.ref_index.auto_rebuild_on_corruption,
                    cfg.ref_index.rebuild_on_start,
                )
                .await
            {
                eprintln!("blob-gc: ref-index ensure failed: {e}");
                std::process::exit(1);
            }

            match command {
                BlobGcCommand::Plan {
                    policy,
                    min_age_secs,
                    max_per_run,
                } => {
                    let stats = match crate::blob_gc::blob_gc_plan(
                        &cfg,
                        &storage,
                        &idx,
                        policy,
                        Duration::from_secs(min_age_secs),
                        crate::blob_gc::BlobGcLimits::unlimited(max_per_run),
                    )
                    .await
                    {
                        Ok(s) => s,
                        Err(e) => {
                            eprintln!("blob-gc: plan failed: {e}");
                            std::process::exit(1);
                        }
                    };

                    println!(
                        "scanned_blobs={} scanned_bytes={} eligible_blobs={} eligible_bytes={}",
                        stats.scanned_blobs, stats.scanned_bytes, stats.eligible_blobs, stats.eligible_bytes
                    );
                    return;
                }
                BlobGcCommand::Quarantine {
                    policy,
                    min_age_secs,
                    max_per_run,
                } => {
                    let stats = match crate::blob_gc::blob_gc_quarantine(
                        &cfg,
                        &storage,
                        &idx,
                        policy,
                        Duration::from_secs(min_age_secs),
                        crate::blob_gc::BlobGcLimits::unlimited(max_per_run),
                    )
                    .await
                    {
                        Ok(s) => s,
                        Err(e) => {
                            eprintln!("blob-gc: quarantine failed: {e}");
                            std::process::exit(1);
                        }
                    };

                    println!(
                        "scanned_blobs={} scanned_bytes={} quarantined_blobs={} quarantined_bytes={}",
                        stats.scanned_blobs, stats.scanned_bytes, stats.quarantined_blobs, stats.quarantined_bytes
                    );
                    return;
                }
                BlobGcCommand::Delete {
                    policy,
                    quarantine_delay_secs,
                    max_per_run,
                } => {
                    let stats = match crate::blob_gc::blob_gc_delete(
                        &cfg,
                        &storage,
                        &idx,
                        policy,
                        Duration::from_secs(quarantine_delay_secs),
                        crate::blob_gc::BlobGcLimits::unlimited(max_per_run),
                    )
                    .await
                    {
                        Ok(s) => s,
                        Err(e) => {
                            eprintln!("blob-gc: delete failed: {e}");
                            std::process::exit(1);
                        }
                    };

                    println!(
                        "restored_blobs={} restored_bytes={} deleted_blobs={} deleted_bytes={}",
                        stats.restored_blobs, stats.restored_bytes, stats.deleted_blobs, stats.deleted_bytes
                    );
                    return;
                }
            }
        }
        CliCommand::HashSecret => {
            use std::io::Read as _;

            let mut secret = String::new();
            std::io::stdin()
                .read_to_string(&mut secret)
                .expect("read stdin");

            match crate::robot_secrets::hash_robot_secret(&secret) {
                Ok(hash) => {
                    println!("{hash}");
                    return;
                }
                Err(err) => {
                    eprintln!("hash-secret failed: {err}");
                    std::process::exit(2);
                }
            }
        }
        CliCommand::Server => {}
    }

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .init();

    tracing::info!(version = env!("CARGO_PKG_VERSION"), "registry-rust starting");

    let config = match std::panic::catch_unwind(|| {
        if config_paths.is_empty() {
            Config::from_env()
        } else {
            Config::from_env_with_files(&config_paths)
        }
    }) {
        Ok(c) => Arc::new(c),
        Err(err) => {
            let msg = if let Some(s) = err.downcast_ref::<String>() {
                s.clone()
            } else if let Some(s) = err.downcast_ref::<&str>() {
                (*s).to_string()
            } else {
                "<non-string panic>".to_string()
            };
            if config_paths.is_empty() {
                eprintln!("server: failed to load config: {msg}");
            } else {
                eprintln!("server: failed to load layered config: {msg}");
            }
            std::process::exit(2);
        }
    };

    // Start phase: generate/renew TLS certs before we attempt to load them.
    // This runs only when [server.tls.acme] is enabled.
    maybe_generate_tls_certs(config.as_ref()).await;
    let addr = config.listen_addr;

    let _fs_root_lock = if config.storage_backend == StorageBackend::Filesystem {
        match crate::fs_root_lock::FsRootLock::try_acquire(&config.fs_root) {
            Ok(l) => Some(l),
            Err(e) => {
                eprintln!(
                    "server: failed to acquire exclusive filesystem lock ({e}); is another registry or blob-gc running?"
                );
                std::process::exit(1);
            }
        }
    } else {
        None
    };

    let storage = storage::from_config(config.as_ref());

    let ref_index: Option<Arc<blob_ref_index::BlobRefIndex>> = if config.ref_index.enabled {
        match blob_ref_index::BlobRefIndex::open(config.ref_index.path.clone()) {
            Ok(idx) => {
                if let Err(err) = idx
                    .ensure_healthy_or_rebuild(
                        &storage,
                        config.ref_index.auto_rebuild_on_corruption,
                        config.ref_index.rebuild_on_start,
                    )
                    .await
                {
                    tracing::error!(
                        error = %err,
                        path = %config.ref_index.path.display(),
                        "ref-index init failed; falling back to scan-based safe delete"
                    );
                    None
                } else {
                    Some(Arc::new(idx))
                }
            }
            Err(err) => {
                tracing::error!(
                    error = %err,
                    path = %config.ref_index.path.display(),
                    "ref-index open failed; falling back to scan-based safe delete"
                );
                None
            }
        }
    } else {
        None
    };

    let tls_enabled = config.tls_cert_path.is_some() && config.tls_key_path.is_some();
    let proxy_mode = if !config.proxy.enabled {
        "disabled"
    } else if config.proxy.upstreams.is_empty() {
        "single"
    } else {
        "multi"
    };

    tracing::info!(
        version = env!("CARGO_PKG_VERSION"),
        listen_addr = %addr,
        tls = tls_enabled,
        public_url = config.public_url.as_deref().unwrap_or(""),
        storage_backend = ?config.storage_backend,
        proxy_enabled = config.proxy.enabled,
        proxy_mode,
        proxy_upstreams = config.proxy.upstreams.len(),
        proxy_routing_hosts = config.proxy.routing_proxy_hosts.len(),
        "registry starting"
    );

    let mut proxy_upstreams: Vec<ProxyContext> = Vec::new();

    // Multi-upstream mode: create one Proxy + cache Storage per configured upstream.
    if config.proxy.enabled && !config.proxy.upstreams.is_empty() {
        for (i, up) in config.proxy.upstreams.iter().enumerate() {
            let mut per = config.proxy.clone();
            per.upstreams = vec![];
            per.upstream_base_url = Some(up.upstream_base_url.clone());
            per.upstream_username = up.upstream_username.clone();
            per.upstream_password = up.upstream_password.clone();
            per.allowed_upstream_hosts = up.allowed_upstream_hosts.clone();
            per.allowed_repo_prefixes = up.allowed_repo_prefixes.clone();
            per.block_private_networks = up.block_private_networks;
            per.redirect_policy = up.redirect_policy;
            per.max_concurrent_upstream = up.max_concurrent_upstream;
            per.index_path = up.index_path.clone();
            per.cache_fs_root = up.cache_fs_root.clone();
            per.cache_s3_prefix = up.cache_s3_prefix.clone();
            per.max_cache_bytes = Some(up.max_cache_bytes);

            let proxy = match proxy::Proxy::new(&per) {
                Ok(p) => p.map(Arc::new).expect("proxy enabled"),
                Err(err) => {
                    tracing::error!(
                        error = %err,
                        upstream_index = i,
                        index_path = %per.index_path.display(),
                        cache_fs_root = ?per.cache_fs_root.as_deref().map(|p| p.display().to_string()),
                        "proxy upstream init failed"
                    );
                    std::process::exit(1);
                }
            };

            let cache: Arc<dyn storage::Storage> = match config.storage_backend {
                StorageBackend::Filesystem => {
                    let root = up
                        .cache_fs_root
                        .clone()
                        .expect("validated: filesystem cache fs_root");
                    Arc::new(storage::fs::FsStorage::new(root, config.max_upload_bytes))
                }
                StorageBackend::S3 => {
                    let endpoint = config
                        .s3_endpoint
                        .clone()
                        .expect("proxy enabled: S3 cache requires STORAGE_S3_ENDPOINT");
                    let region = config
                        .s3_region
                        .clone()
                        .expect("proxy enabled: S3 cache requires STORAGE_S3_REGION");
                    let bucket = config
                        .s3_bucket
                        .clone()
                        .expect("proxy enabled: S3 cache requires STORAGE_S3_BUCKET");
                    let prefix = up
                        .cache_s3_prefix
                        .clone()
                        .expect("validated: S3 cache s3_prefix");
                    Arc::new(storage::s3::S3Storage::new(
                        Some(endpoint),
                        Some(region),
                        Some(bucket),
                        prefix,
                        config.max_upload_bytes,
                    ))
                }
            };

            proxy_upstreams.push(ProxyContext { proxy, cache });
        }
    }

    // Single-upstream mode (legacy).
    let proxy = if config.proxy.enabled && config.proxy.upstreams.is_empty() {
        match proxy::Proxy::new(&config.proxy) {
            Ok(p) => p.map(Arc::new),
            Err(err) => {
                tracing::error!(
                    error = %err,
                    index_path = %config.proxy.index_path.display(),
                    cache_fs_root = ?config.proxy.cache_fs_root.as_deref().map(|p| p.display().to_string()),
                    "proxy init failed"
                );
                std::process::exit(1);
            }
        }
    } else {
        None
    };

    let proxy_cache: Option<Arc<dyn storage::Storage>> =
        if config.proxy.enabled && config.proxy.upstreams.is_empty() {
            match config.storage_backend {
                StorageBackend::Filesystem => {
                    let root = config
                        .proxy
                        .cache_fs_root
                        .clone()
                        .unwrap_or_else(|| config.fs_root.join("cache"));
                    Some(Arc::new(storage::fs::FsStorage::new(
                        root,
                        config.max_upload_bytes,
                    )))
                }
                StorageBackend::S3 => {
                    let endpoint = config
                        .s3_endpoint
                        .clone()
                        .expect("proxy enabled: S3 cache requires STORAGE_S3_ENDPOINT");
                    let region = config
                        .s3_region
                        .clone()
                        .expect("proxy enabled: S3 cache requires STORAGE_S3_REGION");
                    let bucket = config
                        .s3_bucket
                        .clone()
                        .expect("proxy enabled: S3 cache requires STORAGE_S3_BUCKET");
                    let prefix = config.proxy.cache_s3_prefix.clone().unwrap_or_else(|| {
                        format!("{}/cache", config.s3_prefix.trim_end_matches('/'))
                    });
                    Some(Arc::new(storage::s3::S3Storage::new(
                        Some(endpoint),
                        Some(region),
                        Some(bucket),
                        prefix,
                        config.max_upload_bytes,
                    )))
                }
            }
        } else {
            None
        };
    let buffered_body_sem = Arc::new(Semaphore::new(
        config.max_concurrent_buffered_requests.max(1),
    ));
    let request_sem = Arc::new(Semaphore::new(config.max_concurrent_requests.max(1)));
    let upload_request_sem = Arc::new(Semaphore::new(
        config.max_concurrent_upload_requests.max(1),
    ));

    let gc_service = match (&ref_index, &config.storage_backend) {
        (Some(idx), StorageBackend::Filesystem) => Some(Arc::new(gc_service::GcService::new(
            config.clone(),
            storage.clone(),
            idx.clone(),
        ))),
        _ => None,
    };

    let ip_limiter = Arc::new(ip_concurrency::IpConcurrencyLimiter::new(
        config.max_connections_per_ip,
        config.trusted_bypass_cidrs.clone(),
    ));
    let is_high_pressure = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let state = AppState {
        config,
        auth_metrics: Arc::new(AuthMetrics::default()),
        storage,
        ref_index,
        gc_service,
        gc_run_seq: Arc::new(AtomicU64::new(0)),
        proxy,
        proxy_cache,
        proxy_upstreams,
        buffered_body_sem,
        request_sem,
        upload_request_sem,
        active_non_upload_requests: Arc::new(AtomicU64::new(0)),
        active_upload_requests: Arc::new(AtomicU64::new(0)),
        last_sem_saturation_log_unix_secs: Arc::new(AtomicU64::new(0)),
        ip_limiter,
        is_high_pressure,
    };

    // For large blobs we stream request bodies; enforce blob size via MAX_UPLOAD_BYTES and
    // enforce manifest size in-handler (read_body_limited). So we disable the default body
    // limit on the registry API router.
    let v2_body_limit = DefaultBodyLimit::disable();

    // `/v2/*rest` owns all registry API subpaths (repo names can contain `/`).
    // We gate write methods (push) via middleware; GET/HEAD stay anonymous.
    let v2 = Router::new()
        .route("/v2", get(handlers::v2_redirect))
        .route("/v2/", get(handlers::ping))
        .route("/v2/*rest", any(handlers::v2_dispatch))
        .layer(v2_body_limit)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::require_auth_middleware,
        ))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            concurrency_limit_v2_non_upload,
        ));

    // Operational metadata / inventory endpoints (non-standard).
    let meta = Router::new()
        .route("/_meta/catalog", get(handlers::meta_catalog))
        .route("/_meta/orgs", get(handlers::meta_orgs))
        .route("/_meta/orgs/:org/repos", get(handlers::meta_org_repos))
        .route("/_meta/repos/*name", get(handlers::meta_repo));

    let tls_cert_path = state.config.tls_cert_path.clone();
    let tls_key_path = state.config.tls_key_path.clone();

    spawn_upload_gc(state.clone());
    spawn_blob_gc_scheduler(state.clone());
    spawn_proxy_gc(state.clone());
    spawn_proxy_scrub(state.clone());
    spawn_fd_diagnostics_logger(state.clone());

    // Token endpoint hardening: rate limit expensive credential checks.
    // Defaults are conservative and should not impact normal clients.
    // Set TOKEN_RATE_LIMIT_RPM=0 to disable.
    let token_rate_limit_rpm = std::env::var("REGISTRY__TOKEN__RATE_LIMIT_RPM")
        .ok()
        .or_else(|| std::env::var("TOKEN_RATE_LIMIT_RPM").ok())
        .and_then(|s| s.trim().parse::<u32>().ok())
        .unwrap_or(1200);
    let token_rate_limit_window_secs = std::env::var("REGISTRY__TOKEN__RATE_LIMIT_WINDOW_SECS")
        .ok()
        .or_else(|| std::env::var("TOKEN_RATE_LIMIT_WINDOW_SECS").ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(60);

    let token_rate_limiter = if token_rate_limit_rpm == 0 {
        TokenRateLimiter::disabled()
    } else {
        TokenRateLimiter::new(
            token_rate_limit_rpm,
            Duration::from_secs(token_rate_limit_window_secs.max(1)),
        )
    };

    let token =
        Router::new()
            .route("/token", get(handlers::token))
            .layer(axum::middleware::from_fn(move |req, next| {
                let limiter = token_rate_limiter.clone();
                async move { limit_token_requests(limiter, req, next).await }
            }));

    // Admin-only endpoints (disabled by default).
    let admin = if state.config.admin_api.enabled {
        Router::new()
            .route("/_admin/gc/health", get(handlers::admin_gc_health))
            .route("/_admin/gc/plan", post(handlers::admin_gc_plan))
            .route("/_admin/gc/quarantine", post(handlers::admin_gc_quarantine))
            .route("/_admin/gc/delete", post(handlers::admin_gc_delete))
    } else {
        Router::new()
    };

    let app = Router::new()
        .merge(token)
        .merge(admin)
        .merge(meta)
        .merge(v2)
        .with_state(state.clone())
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            ip_concurrency_middleware,
        ))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            request_timeout_by_path,
        ))
        .layer(CatchPanicLayer::new())
        .layer(TraceLayer::new_for_http())
        .layer(axum::middleware::from_fn(log_server_errors));

    tracing::info!(%addr, tls = tls_enabled, "registry listening");

    if let (Some(cert), Some(key)) = (tls_cert_path, tls_key_path) {
        let tls = axum_server::tls_rustls::RustlsConfig::from_pem_file(cert, key)
            .await
            .expect("load TLS cert/key");
        let handle = axum_server::Handle::new();
        let handle_for_shutdown = handle.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            handle_for_shutdown.graceful_shutdown(Some(Duration::from_secs(10)));
        });

        axum_server::bind_rustls(addr, tls)
            .handle(handle)
            .serve(app.into_make_service_with_connect_info::<std::net::SocketAddr>())
            .await
            .expect("serve https");
    } else {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .expect("bind listen addr");
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("serve http");
    }
}

async fn log_server_errors(req: Request<axum::body::Body>, next: Next) -> impl IntoResponse {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let start = Instant::now();

    let response = next.run(req).await;
    let status = response.status();

    if status.is_server_error() {
        tracing::error!(
            method = %method,
            path = %path,
            status = %status,
            latency_ms = start.elapsed().as_millis(),
            "request returned 5xx"
        );
    }

    response
}

fn spawn_proxy_gc(state: AppState) {
    if !state.config.proxy.enabled {
        return;
    }

    // Multi-upstream mode: run GC per upstream cache.
    if !state.config.proxy.upstreams.is_empty() {
        if state.config.storage_backend != StorageBackend::Filesystem {
            tracing::warn!("proxy gc: only filesystem backend is supported for eviction currently");
            return;
        }

        let interval = Duration::from_secs(state.config.proxy.gc_interval_secs.max(1));
        let repo_rules = state.config.proxy.repo_rules.clone();

        for (i, up) in state.config.proxy.upstreams.iter().enumerate() {
            let Some(ctx) = state.proxy_upstreams.get(i).cloned() else {
                continue;
            };
            let fs_root = up
                .cache_fs_root
                .clone()
                .unwrap_or_else(|| state.config.fs_root.join(format!("cache-upstream-{i}")));
            let max_cache_bytes = up.max_cache_bytes;
            let storage = ctx.cache;
            let proxy_for_gc = ctx.proxy;
            let repo_rules_for_gc = repo_rules.clone();

            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                loop {
                    ticker.tick().await;
                    if let Err(err) = proxy_gc_once(
                        &storage,
                        &fs_root,
                        max_cache_bytes,
                        &repo_rules_for_gc,
                        &proxy_for_gc,
                    )
                    .await
                    {
                        tracing::warn!(upstream_index = i, error = %err, "proxy gc failed");
                    }
                }
            });
        }
        return;
    }
    if state.config.storage_backend != StorageBackend::Filesystem {
        tracing::warn!("proxy gc: only filesystem backend is supported for eviction currently");
        return;
    }
    let Some(cache_storage) = state.proxy_cache.clone() else {
        tracing::warn!("proxy gc: cache storage not configured");
        return;
    };
    let Some(proxy) = state.proxy.clone() else {
        tracing::warn!("proxy gc: proxy instance not configured");
        return;
    };
    let Some(max_cache_bytes) = state.config.proxy.max_cache_bytes else {
        return;
    };

    let interval = Duration::from_secs(state.config.proxy.gc_interval_secs.max(1));
    let fs_root = state
        .config
        .proxy
        .cache_fs_root
        .clone()
        .unwrap_or_else(|| state.config.fs_root.join("cache"));
    let repo_rules = state.config.proxy.repo_rules.clone();
    let storage = cache_storage;

    let proxy_for_gc = proxy;

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            if let Err(err) = proxy_gc_once(
                &storage,
                &fs_root,
                max_cache_bytes,
                &repo_rules,
                &proxy_for_gc,
            )
            .await
            {
                tracing::warn!(error = %err, "proxy gc: run failed");
            }
        }
    });
}

fn spawn_proxy_scrub(state: AppState) {
    if !state.config.proxy.enabled {
        return;
    }
    if !state.config.proxy.scrub_enabled {
        return;
    }

    // Multi-upstream mode: scrub per upstream cache.
    if !state.config.proxy.upstreams.is_empty() {
        if state.config.storage_backend != StorageBackend::Filesystem {
            tracing::warn!("proxy scrub: only filesystem backend is supported currently");
            return;
        }

        let interval = Duration::from_secs(state.config.proxy.scrub_interval_secs.max(1));
        let max_files = state.config.proxy.scrub_max_files_per_run.max(1);

        for (i, up) in state.config.proxy.upstreams.iter().enumerate() {
            let fs_root = up
                .cache_fs_root
                .clone()
                .unwrap_or_else(|| state.config.fs_root.join(format!("cache-upstream-{i}")));

            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                loop {
                    ticker.tick().await;
                    match proxy_scrub_once(&fs_root, max_files).await {
                        Ok((scanned, removed)) => {
                            if removed > 0 {
                                tracing::info!(
                                    upstream_index = i,
                                    scanned,
                                    removed,
                                    "proxy scrub: removed corrupt cache files"
                                );
                            }
                        }
                        Err(err) => {
                            tracing::warn!(upstream_index = i, error = %err, "proxy scrub failed");
                        }
                    }
                }
            });
        }
        return;
    }
    if state.config.storage_backend != StorageBackend::Filesystem {
        tracing::warn!("proxy scrub: only filesystem backend is supported currently");
        return;
    }

    let interval = Duration::from_secs(state.config.proxy.scrub_interval_secs.max(1));
    let max_files = state.config.proxy.scrub_max_files_per_run.max(1);
    let fs_root = state
        .config
        .proxy
        .cache_fs_root
        .clone()
        .unwrap_or_else(|| state.config.fs_root.join("cache"));

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            match proxy_scrub_once(&fs_root, max_files).await {
                Ok((scanned, removed)) => {
                    if removed > 0 {
                        tracing::info!(
                            scanned,
                            removed,
                            "proxy scrub: removed corrupted cache entries"
                        );
                    } else {
                        tracing::debug!(scanned, removed, "proxy scrub: ok");
                    }
                }
                Err(err) => {
                    tracing::warn!(error = %err, "proxy scrub: run failed");
                }
            }
        }
    });
}

async fn proxy_scrub_once(
    fs_root: &std::path::PathBuf,
    max_files: usize,
) -> Result<(u64, u64), String> {
    let repos_root = fs_root.join("repos");
    let mut stack: Vec<std::path::PathBuf> = vec![repos_root];
    let mut scanned: u64 = 0;
    let mut removed: u64 = 0;

    while let Some(dir) = stack.pop() {
        if (scanned as usize) >= max_files {
            break;
        }

        let mut rd = match tokio::fs::read_dir(&dir).await {
            Ok(d) => d,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(err.to_string()),
        };

        while let Ok(Some(ent)) = rd.next_entry().await {
            if (scanned as usize) >= max_files {
                break;
            }

            let path = ent.path();
            let ft = match ent.file_type().await {
                Ok(t) => t,
                Err(_) => continue,
            };

            if ft.is_dir() {
                // Skip blob store; scrub focuses on repo metadata-like structures.
                let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
                if name == "blobs" || name == "uploads" {
                    continue;
                }
                stack.push(path);
                continue;
            }

            if !ft.is_file() {
                continue;
            }

            scanned += 1;

            let parent_name = path
                .parent()
                .and_then(|p| p.file_name())
                .and_then(|s| s.to_str())
                .unwrap_or("");

            if parent_name == "manifests" {
                let file_hex = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
                if file_hex.len() != 64 || !file_hex.chars().all(|c| c.is_ascii_hexdigit()) {
                    continue;
                }

                let bytes = match tokio::fs::read(&path).await {
                    Ok(b) => b,
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(err) => {
                        tracing::debug!(error = %err, path = %path.display(), "proxy scrub: read manifest failed");
                        continue;
                    }
                };
                if bytes.is_empty() {
                    let _ = tokio::fs::remove_file(&path).await;
                    removed += 1;
                    continue;
                }

                // Must be valid JSON.
                if serde_json::from_slice::<serde_json::Value>(&bytes).is_err() {
                    let _ = tokio::fs::remove_file(&path).await;
                    removed += 1;
                    continue;
                }

                // Must match filename digest.
                let mut hasher = sha2::Sha256::new();
                hasher.update(&bytes);
                let computed = hex::encode(hasher.finalize());
                if computed != file_hex {
                    let _ = tokio::fs::remove_file(&path).await;
                    removed += 1;
                }
            } else if parent_name == "tags" {
                // Tag pointers should parse as a digest; invalid pointers are removed.
                let content = match tokio::fs::read_to_string(&path).await {
                    Ok(s) => s,
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(_) => continue,
                };
                if crate::registry::digest::Digest::parse(content.trim()).is_err() {
                    let _ = tokio::fs::remove_file(&path).await;
                    removed += 1;
                }
            }
        }
    }

    Ok((scanned, removed))
}

async fn proxy_gc_once(
    storage: &Arc<dyn storage::Storage>,
    fs_root: &std::path::PathBuf,
    max_cache_bytes: u64,
    repo_rules: &[config::ProxyRepoRule],
    proxy: &proxy::Proxy,
) -> Result<(), String> {
    let protected = compute_protected_blobs(storage, repo_rules, proxy).await;

    let blobs_root = fs_root.join("blobs").join("sha256");
    let mut entries: Vec<(
        registry::digest::Digest,
        u64,
        Option<u64>,
        std::time::SystemTime,
    )> = Vec::new();
    let mut total: u64 = 0;

    let mut prefixes = match tokio::fs::read_dir(&blobs_root).await {
        Ok(d) => d,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(err) => return Err(err.to_string()),
    };

    while let Ok(Some(prefix_ent)) = prefixes.next_entry().await {
        let prefix_path = prefix_ent.path();
        if !prefix_ent
            .file_type()
            .await
            .map_err(|e| e.to_string())?
            .is_dir()
        {
            continue;
        }
        let mut dir = match tokio::fs::read_dir(&prefix_path).await {
            Ok(d) => d,
            Err(_) => continue,
        };
        while let Ok(Some(ent)) = dir.next_entry().await {
            let path = ent.path();
            let ft = match ent.file_type().await {
                Ok(t) => t,
                Err(_) => continue,
            };
            if !ft.is_file() {
                continue;
            }
            let file_name = match path.file_name().and_then(|s| s.to_str()) {
                Some(s) => s,
                None => continue,
            };
            let digest = match registry::digest::Digest::parse(&format!("sha256:{file_name}")) {
                Ok(d) => d,
                Err(_) => continue,
            };
            if protected.contains(digest.hex()) {
                continue;
            }
            let meta = match ent.metadata().await {
                Ok(m) => m,
                Err(_) => continue,
            };
            let size = meta.len();
            let modified = meta.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH);
            let last_access = proxy.get_blob_last_access(&digest);
            total = total.saturating_add(size);
            entries.push((digest, size, last_access, modified));
        }
    }

    if total <= max_cache_bytes {
        return Ok(());
    }

    // Evict oldest-first by last access (fallback to mtime).
    entries.sort_by_key(|(_, _, last, mtime)| (*last, *mtime));

    let mut removed_blobs: u64 = 0;
    let mut removed_bytes: u64 = 0;
    for (digest, size, _, _) in entries {
        if total.saturating_sub(removed_bytes) <= max_cache_bytes {
            break;
        }
        match storage.delete_blob(&digest).await {
            Ok(()) => {
                removed_blobs += 1;
                removed_bytes = removed_bytes.saturating_add(size);
            }
            Err(storage::StorageError::NotFound) => {}
            Err(err) => {
                tracing::warn!(error = %err, digest = digest.as_str(), "proxy gc: delete_blob failed");
            }
        }
    }

    if removed_blobs > 0 {
        tracing::info!(
            removed_blobs,
            removed_bytes,
            max_cache_bytes,
            "proxy gc: evicted cached blobs"
        );
    }
    Ok(())
}

async fn compute_protected_blobs(
    storage: &Arc<dyn storage::Storage>,
    repo_rules: &[config::ProxyRepoRule],
    proxy: &proxy::Proxy,
) -> HashSet<String> {
    let repos = storage.list_repositories().await.unwrap_or_default();
    let mut protected_blobs: HashSet<String> = HashSet::new();
    let mut seen_manifests: HashSet<(String, String)> = HashSet::new();

    for repo in repos {
        for rule in repo_rules {
            if !wildcard_match(&rule.match_pattern, &repo) {
                continue;
            }

            let mut pinned_tags: Vec<String> = Vec::new();
            match &rule.eviction_policy {
                config::EvictionPolicy::KeepTags(tags) => {
                    pinned_tags.extend(tags.iter().cloned());
                }
                config::EvictionPolicy::KeepLatestCachedSemver {
                    tag_regex,
                    allow_prerelease,
                } => {
                    if let Ok(tags) = storage.list_tags(&repo).await {
                        if let Some(latest) =
                            pick_latest_semver_tag(tags, tag_regex.as_deref(), *allow_prerelease)
                        {
                            pinned_tags.push(latest);
                        }
                    }
                }
                _ => {}
            }

            for tag in pinned_tags {
                if let Ok(digest) = storage.resolve_tag(&repo, &tag).await {
                    collect_protected_blobs_for_manifest(
                        storage,
                        &repo,
                        &digest,
                        &mut protected_blobs,
                        &mut seen_manifests,
                        proxy,
                        0,
                    )
                    .await;
                }
            }
        }
    }

    protected_blobs
}

async fn collect_protected_blobs_for_manifest(
    storage: &Arc<dyn storage::Storage>,
    repo: &str,
    digest: &registry::digest::Digest,
    protected_blobs: &mut HashSet<String>,
    seen_manifests: &mut HashSet<(String, String)>,
    proxy: &proxy::Proxy,
    depth: usize,
) {
    let mut stack: Vec<(registry::digest::Digest, usize)> = vec![(digest.clone(), depth)];
    while let Some((digest, depth)) = stack.pop() {
        if depth >= 5 {
            continue;
        }
        let key = (repo.to_string(), digest.hex().to_string());
        if !seen_manifests.insert(key) {
            continue;
        }

        if let Some(refs) = proxy.get_manifest_refs(repo, &digest) {
            if !refs.manifests.is_empty() {
                for child in &refs.manifests {
                    if let Ok(child) = registry::digest::Digest::parse(child) {
                        stack.push((child, depth + 1));
                    }
                }
                // Index/list: only traverse to children.
                continue;
            }

            for blob in refs.blobs {
                if let Ok(d) = registry::digest::Digest::parse(&blob) {
                    protected_blobs.insert(d.hex().to_string());
                }
            }
            continue;
        }

        let Ok((_meta, bytes)) = storage.get_manifest(repo, &digest).await else {
            continue;
        };
        let Ok(v) = serde_json::from_slice::<serde_json::Value>(&bytes) else {
            continue;
        };

        // Best-effort: populate DB index so next GC pass is faster.
        proxy.index_manifest(repo, &digest, &bytes);

        // Index/list: manifests[].digest
        if let Some(manifests) = v.get("manifests").and_then(|m| m.as_array()) {
            for m in manifests {
                if let Some(d) = m.get("digest").and_then(|d| d.as_str()) {
                    if let Ok(child) = registry::digest::Digest::parse(d) {
                        stack.push((child, depth + 1));
                    }
                }
            }
            continue;
        }

        // Manifest: config.digest + layers[].digest
        if let Some(cfg_digest) = v
            .get("config")
            .and_then(|c| c.get("digest"))
            .and_then(|d| d.as_str())
        {
            if let Ok(d) = registry::digest::Digest::parse(cfg_digest) {
                protected_blobs.insert(d.hex().to_string());
            }
        }
        if let Some(layers) = v.get("layers").and_then(|l| l.as_array()) {
            for layer in layers {
                if let Some(d) = layer.get("digest").and_then(|d| d.as_str()) {
                    if let Ok(d) = registry::digest::Digest::parse(d) {
                        protected_blobs.insert(d.hex().to_string());
                    }
                }
            }
        }
    }
}

fn wildcard_match(pattern: &str, value: &str) -> bool {
    if !pattern.contains('*') {
        return pattern == value;
    }
    let mut parts = pattern.split('*');
    let first = parts.next().unwrap_or("");
    if !value.starts_with(first) {
        return false;
    }
    let mut remainder = &value[first.len()..];
    let mut last_part = first;
    for part in parts {
        if part.is_empty() {
            last_part = part;
            continue;
        }
        if let Some(idx) = remainder.find(part) {
            remainder = &remainder[idx + part.len()..];
            last_part = part;
        } else {
            return false;
        }
    }
    if !pattern.ends_with('*') {
        if !value.ends_with(last_part) {
            return false;
        }
    }
    true
}

fn pick_latest_semver_tag(
    tags: Vec<String>,
    tag_regex: Option<&str>,
    allow_prerelease: bool,
) -> Option<String> {
    let re = tag_regex.and_then(|r| regex::Regex::new(r).ok());
    let mut best: Option<(Version, String)> = None;

    for tag in tags {
        if let Some(re) = &re {
            if !re.is_match(&tag) {
                continue;
            }
        }

        let parsed = Version::parse(tag.strip_prefix('v').unwrap_or(&tag)).ok();
        let Some(v) = parsed else { continue };
        if !allow_prerelease && !v.pre.is_empty() {
            continue;
        }
        match &best {
            Some((best_v, _)) if &v <= best_v => {}
            _ => best = Some((v, tag)),
        }
    }
    best.map(|(_, t)| t)
}

async fn request_timeout_by_path(
    axum::extract::State(state): axum::extract::State<AppState>,
    req: Request<axum::body::Body>,
    next: Next,
) -> axum::response::Response {
    let path = req.uri().path().to_string();
    let method = req.method().clone();
    let timeout_secs = if is_upload_path(&path) {
        state.config.upload_request_timeout_secs
    } else {
        state.config.request_timeout_secs
    };

    match tokio::time::timeout(Duration::from_secs(timeout_secs), next.run(req)).await {
        Ok(resp) => resp,
        Err(_) => {
            tracing::warn!(%method, path = %path, timeout_secs, "request timed out");
            axum::http::StatusCode::REQUEST_TIMEOUT.into_response()
        }
    }
}

async fn ip_concurrency_middleware(
    axum::extract::State(state): axum::extract::State<AppState>,
    req: Request<axum::body::Body>,
    next: Next,
) -> axum::response::Response {
    let path = req.uri().path();
    if path == "/v2"
        || path == "/v2/"
        || path.starts_with("/_meta/")
        || path.starts_with("/_admin/gc/health")
    {
        return next.run(req).await;
    }

    let peer_addr = req
        .extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
        .map(|ci| ci.0.ip())
        .unwrap_or_else(|| std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)));

    let effective_ip = crate::request_routing::resolve_trusted_client_ip(
        peer_addr,
        req.headers(),
        &state.config.trusted_proxies,
    );

    match state.ip_limiter.acquire(effective_ip) {
        Ok(guard) => {
            let resp = next.run(req).await;
            drop(guard);
            resp
        }
        Err(()) => {
            tracing::warn!(
                client_ip = %effective_ip,
                peer_ip = %peer_addr,
                max = state.config.max_connections_per_ip,
                "connection limit per IP exceeded"
            );
            (
                axum::http::StatusCode::TOO_MANY_REQUESTS,
                [("Retry-After", "5")],
                "Too many concurrent requests from your IP",
            )
                .into_response()
        }
    }
}


#[cfg(target_os = "linux")]
fn open_fd_count_linux() -> Option<u64> {
    // Best-effort diagnostic only.
    let rd = std::fs::read_dir("/proc/self/fd").ok()?;
    Some(rd.count() as u64)
}

#[cfg(not(target_os = "linux"))]
fn open_fd_count_linux() -> Option<u64> {
    None
}

fn unix_seconds_now() -> u64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => d.as_secs(),
        Err(_) => 0,
    }
}

async fn concurrency_limit_v2_non_upload(
    axum::extract::State(state): axum::extract::State<AppState>,
    req: Request<axum::body::Body>,
    next: Next,
) -> axum::response::Response {
    let path = req.uri().path();
    let (is_upload, sem, active_counter, sem_name, sem_limit) = if is_upload_path(path) {
        (
            true,
            state.upload_request_sem.clone(),
            state.active_upload_requests.clone(),
            "upload",
            state.config.max_concurrent_upload_requests,
        )
    } else {
        (
            false,
            state.request_sem.clone(),
            state.active_non_upload_requests.clone(),
            "non_upload",
            state.config.max_concurrent_requests,
        )
    };

    // Fast path: if permits are available, avoid any extra logging overhead.
    let _permit: OwnedSemaphorePermit = match sem.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            // Semaphore is saturated; rate-limit warnings to avoid log spam.
            let now = unix_seconds_now();
            let last = state
                .last_sem_saturation_log_unix_secs
                .load(Ordering::Relaxed);
            if now.saturating_sub(last) >= 10 {
                state
                    .last_sem_saturation_log_unix_secs
                    .store(now, Ordering::Relaxed);
                tracing::warn!(
                    sem = sem_name,
                    is_upload,
                    sem_limit,
                    available_permits = sem.available_permits(),
                    active_non_upload = state.active_non_upload_requests.load(Ordering::Relaxed),
                    active_upload = state.active_upload_requests.load(Ordering::Relaxed),
                    open_fds = open_fd_count_linux(),
                    "concurrency limit reached; waiting for permit"
                );
            }

            sem.acquire_owned()
                .await
                .expect("request semaphore unexpectedly closed")
        }
    };

    active_counter.fetch_add(1, Ordering::Relaxed);
    let response = next.run(req).await;
    active_counter.fetch_sub(1, Ordering::Relaxed);

    response

}

fn is_upload_path(path: &str) -> bool {
    path.starts_with("/v2/") && path.contains("/blobs/uploads")
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("install Ctrl-C handler");
    };

    #[cfg(unix)]
    let term = async {
        use tokio::signal::unix::{SignalKind, signal};
        let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        sigterm.recv().await;
    };

    #[cfg(not(unix))]
    let term = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = term => {},
    }

    tracing::info!("shutdown signal received");
}

fn spawn_upload_gc(state: AppState) {
    if state.config.storage_backend != StorageBackend::Filesystem {
        return;
    }
    if !state.config.upload_gc_enabled {
        return;
    }

    let uploads_dir = state.config.fs_root.join("uploads");
    let interval = Duration::from_secs(state.config.upload_gc_interval_secs.max(1));
    let max_age = Duration::from_secs(state.config.upload_gc_max_age_secs);

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            let now = std::time::SystemTime::now();

            let mut dir = match tokio::fs::read_dir(&uploads_dir).await {
                Ok(d) => d,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => {
                    tracing::warn!(error = %err, path = %uploads_dir.display(), "upload gc: read_dir failed");
                    continue;
                }
            };

            let mut removed = 0u64;
            let mut scanned = 0u64;
            while let Ok(Some(entry)) = dir.next_entry().await {
                scanned += 1;
                let path = entry.path();

                let meta = match entry.metadata().await {
                    Ok(m) => m,
                    Err(_) => continue,
                };
                let modified = match meta.modified() {
                    Ok(t) => t,
                    Err(_) => continue,
                };
                let age = match now.duration_since(modified) {
                    Ok(d) => d,
                    Err(_) => Duration::from_secs(0),
                };

                if age >= max_age {
                    let removed_this = tokio::fs::remove_file(&path).await.is_ok();

                    // If we removed a partial upload file, also remove its stored hash state.
                    // And vice versa, to avoid leaving orphan sidecars around.
                    if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                        if let Some(uuid) = name.strip_suffix(".data") {
                            let sidecar = uploads_dir.join(format!("{uuid}.sha256state"));
                            let _ = tokio::fs::remove_file(&sidecar).await;
                        } else if let Some(uuid) = name.strip_suffix(".sha256state") {
                            let data = uploads_dir.join(format!("{uuid}.data"));
                            let _ = tokio::fs::remove_file(&data).await;
                        }
                    }

                    if removed_this {
                        removed += 1;
                    }
                }
            }

            if removed > 0 {
                tracing::info!(scanned, removed, path = %uploads_dir.display(), "upload gc: removed stale temp files");
            }
        }
    });
}

fn spawn_fd_diagnostics_logger(state: AppState) {
    let interval_secs = std::env::var("REGISTRY_DIAG_FD_LOG_INTERVAL_SECS")
        .ok()
        .or_else(|| std::env::var("FD_LOG_INTERVAL_SECS").ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(0);

    if interval_secs == 0 {
        return;
    }

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs.max(1)));
        loop {
            ticker.tick().await;
            tracing::info!(
                open_fds = open_fd_count_linux(),
                active_non_upload = state.active_non_upload_requests.load(Ordering::Relaxed),
                active_upload = state.active_upload_requests.load(Ordering::Relaxed),
                non_upload_available_permits = state.request_sem.available_permits(),
                upload_available_permits = state.upload_request_sem.available_permits(),
                "fd diagnostics"
            );
        }
    });
}

fn spawn_blob_gc_scheduler(state: AppState) {
    if state.config.storage_backend != StorageBackend::Filesystem {
        return;
    }
    if !state.config.blob_gc_schedule_enabled {
        return;
    }

    let Some(service) = state.gc_service.clone() else {
        tracing::warn!("blob gc scheduler enabled but gc service unavailable");
        return;
    };

    let interval = Duration::from_secs(state.config.blob_gc_schedule_interval_secs.max(1));

    tokio::spawn(async move {
        // Delay first run until after one full interval.
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ticker.tick().await; // consume immediate tick
        loop {
            ticker.tick().await;
            tracing::info!(event = "blob_gc", action = "scheduled_cleanup", "starting scheduled blob gc cleanup");

            match service.scheduled_cleanup_once().await {
                Ok(stats) => {
                    tracing::info!(
                        event = "blob_gc",
                        action = "scheduled_cleanup",
                        quarantined_blobs = stats.quarantine.quarantined_blobs,
                        quarantined_bytes = stats.quarantine.quarantined_bytes,
                        restored_blobs = stats.quarantine.restored_blobs,
                        restored_bytes = stats.quarantine.restored_bytes,
                        deleted_blobs = stats
                            .delete
                            .as_ref()
                            .map(|s| s.deleted_blobs)
                            .unwrap_or(0),
                        deleted_bytes = stats
                            .delete
                            .as_ref()
                            .map(|s| s.deleted_bytes)
                            .unwrap_or(0),
                        "scheduled blob gc cleanup finished"
                    );
                }
                Err(crate::gc_service::GcServiceError::AlreadyRunning) => {
                    tracing::info!(event = "blob_gc", action = "scheduled_cleanup", "scheduled blob gc skipped (already running)");
                }
                Err(crate::gc_service::GcServiceError::Disabled) => {
                    tracing::warn!(event = "blob_gc", action = "scheduled_cleanup", "blob gc scheduler enabled but blob_gc.enabled=false; stopping scheduler");
                    return;
                }
                Err(err) => {
                    tracing::warn!(event = "blob_gc", action = "scheduled_cleanup", error = %err, "scheduled blob gc cleanup failed");
                }
            }
        }
    });
}
