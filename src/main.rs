mod auth;
mod config;
mod http_api;
mod proxy;
mod request_routing;
mod registry;
mod security;
mod storage;

use axum::{
    Router,
    extract::DefaultBodyLimit,
    http::Request,
    middleware::Next,
    response::IntoResponse,
    routing::{any, get},
};
use config::{Config, StorageBackend};
use http_api::handlers;
use semver::Version;
use sha2::Digest as _;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub storage: Arc<dyn storage::Storage>,
    pub proxy: Option<Arc<proxy::Proxy>>,
    pub proxy_cache: Option<Arc<dyn storage::Storage>>,
    pub buffered_body_sem: Arc<Semaphore>,
    pub request_sem: Arc<Semaphore>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = Arc::new(Config::from_env());
    let addr = config.listen_addr;
    let storage = storage::from_config(config.as_ref());
    let proxy = match proxy::Proxy::new(&config.proxy) {
        Ok(p) => p.map(Arc::new),
        Err(err) => {
            // Fail fast: proxy config errors should not start the server in a surprising state.
            panic!("proxy init failed: {err}");
        }
    };

    let proxy_cache: Option<Arc<dyn storage::Storage>> = if config.proxy.enabled {
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
                let prefix =
                    config.proxy.cache_s3_prefix.clone().unwrap_or_else(|| {
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

    let state = AppState {
        config,
        storage,
        proxy,
        proxy_cache,
        buffered_body_sem,
        request_sem,
    };

    // For large blobs we stream request bodies; enforce blob size via MAX_UPLOAD_BYTES and
    // enforce manifest size in-handler (read_body_limited). So we disable the default body
    // limit on the registry API router.
    let v2_body_limit = DefaultBodyLimit::disable();

    // `/v2/*rest` owns all registry API subpaths (repo names can contain `/`).
    // We gate write methods (push) via middleware; GET/HEAD stay anonymous.
    let v2 = Router::new()
        .route("/v2", get(handlers::ping))
        .route("/v2/", get(handlers::ping))
        .route("/v2/*rest", any(handlers::v2_dispatch))
        .layer(v2_body_limit)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::require_push_basic_auth,
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
    spawn_proxy_gc(state.clone());
    spawn_proxy_scrub(state.clone());

    let app = Router::new()
        .route("/token", get(handlers::token))
        .merge(meta)
        .merge(v2)
        .with_state(state.clone())
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            request_timeout_by_path,
        ))
        .layer(CatchPanicLayer::new())
        .layer(TraceLayer::new_for_http());

    tracing::info!(%addr, "registry listening");

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
            .serve(app.into_make_service())
            .await
            .expect("serve https");
    } else {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .expect("bind listen addr");
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .expect("serve http");
    }
}

fn spawn_proxy_gc(state: AppState) {
    if !state.config.proxy.enabled {
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
    let path = req.uri().path();
    let timeout_secs = if is_upload_path(path) {
        state.config.upload_request_timeout_secs
    } else {
        state.config.request_timeout_secs
    };

    match tokio::time::timeout(Duration::from_secs(timeout_secs), next.run(req)).await {
        Ok(resp) => resp,
        Err(_) => axum::http::StatusCode::REQUEST_TIMEOUT.into_response(),
    }
}

async fn concurrency_limit_v2_non_upload(
    axum::extract::State(state): axum::extract::State<AppState>,
    req: Request<axum::body::Body>,
    next: Next,
) -> axum::response::Response {
    let path = req.uri().path();
    let _permit: Option<OwnedSemaphorePermit> = if is_upload_path(path) {
        None
    } else {
        Some(
            state
                .request_sem
                .clone()
                .acquire_owned()
                .await
                .expect("request semaphore unexpectedly closed"),
        )
    };

    next.run(req).await
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
                    if tokio::fs::remove_file(&path).await.is_ok() {
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
