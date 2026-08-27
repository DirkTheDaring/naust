use bytes::Bytes;
use futures_util::StreamExt;
use reqwest::redirect::Policy;
use sha2::Digest as _;
use std::{
    collections::HashMap,
    fmt,
    net::IpAddr,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, Semaphore};
use url::Url;

use crate::{
    config::{EvictionPolicy, ProxyConfig, ProxyMode, ProxyRepoRule, RedirectPolicy, TagPolicy},
    registry::canonical_name::{CanonicalRepoName, RepoNameError},
    registry::digest::Digest,
};

/// Host pattern for proxy upstream routing.
///
/// Invariants:
/// - `All`: wildcard `*` matches all hosts.
/// - `DomainWildcard(suffix)`: domain wildcard pattern starting with `*.` (e.g. `*.docker.io`).
///   Matches subdomains (e.g. `sub.docker.io`, `a.b.docker.io`). Does NOT match bare `docker.io`.
/// - `Exact(host)`: exact host matching (e.g. `docker.io`, `registry.company.com`).
///   Case-insensitive ASCII matching.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ProxyHostPattern {
    All,
    DomainWildcard(String),
    Exact(String),
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ProxyHostPatternError {
    #[error("empty proxy host pattern")]
    Empty,
    #[error("invalid wildcard in proxy host pattern: '{0}'")]
    InvalidWildcard(String),
}

impl ProxyHostPattern {
    pub fn parse(s: &str) -> Result<Self, ProxyHostPatternError> {
        let trimmed = s.trim().to_ascii_lowercase();
        if trimmed.is_empty() {
            return Err(ProxyHostPatternError::Empty);
        }
        if trimmed == "*" {
            return Ok(Self::All);
        }
        if trimmed.starts_with("*.") {
            let suffix = &trimmed[2..];
            if suffix.is_empty() || suffix.contains('*') {
                return Err(ProxyHostPatternError::InvalidWildcard(trimmed));
            }
            return Ok(Self::DomainWildcard(suffix.to_string()));
        }
        if trimmed.contains('*') {
            return Err(ProxyHostPatternError::InvalidWildcard(trimmed));
        }
        Ok(Self::Exact(trimmed))
    }

    pub fn matches(&self, host: &str) -> bool {
        let host_norm = host.trim().to_ascii_lowercase();
        if host_norm.is_empty() {
            return false;
        }
        match self {
            Self::All => true,
            Self::Exact(exact) => exact == &host_norm,
            Self::DomainWildcard(suffix) => {
                if host_norm.ends_with(suffix) && host_norm.len() > suffix.len() {
                    let prefix_part = &host_norm[..host_norm.len() - suffix.len()];
                    prefix_part.ends_with('.')
                } else {
                    false
                }
            }
        }
    }
}

impl fmt::Display for ProxyHostPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::All => write!(f, "*"),
            Self::DomainWildcard(s) => write!(f, "*.{s}"),
            Self::Exact(s) => write!(f, "{s}"),
        }
    }
}

impl FromStr for ProxyHostPattern {
    type Err = ProxyHostPatternError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// Repository pattern for proxy routing and caching rules.
///
/// Invariants:
/// - `All`: wildcard `*` matches any repository.
/// - `Subtree(prefix)`: pattern `prefix/*` (e.g. `library/*`, `org/sub/*`).
///   Matches repository names strictly under `{prefix}/` (e.g. `library/ubuntu`) and `{prefix}`.
///   Never matches sibling names (e.g. `library-secret`).
/// - `Exact(repo)`: pattern `repo` (e.g. `library/ubuntu`).
///   Matches iff `repo == candidate`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ProxyRepoPattern {
    All,
    Subtree(CanonicalRepoName),
    Exact(CanonicalRepoName),
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ProxyRepoPatternError {
    #[error("empty proxy repository pattern")]
    Empty,
    #[error("invalid repository name in proxy pattern: {0}")]
    InvalidRepoName(#[from] RepoNameError),
    #[error("invalid wildcard syntax in proxy repository pattern: '{0}'")]
    InvalidSyntax(String),
}

impl ProxyRepoPattern {
    pub fn parse(s: &str) -> Result<Self, ProxyRepoPatternError> {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            return Err(ProxyRepoPatternError::Empty);
        }
        if trimmed == "*" {
            return Ok(Self::All);
        }
        if trimmed.ends_with("/*") {
            let base = &trimmed[..trimmed.len() - 2];
            if base.is_empty() {
                return Err(ProxyRepoPatternError::Empty);
            }
            let canonical = CanonicalRepoName::parse(base)?;
            Ok(Self::Subtree(canonical))
        } else if trimmed.contains('*') {
            Err(ProxyRepoPatternError::InvalidSyntax(trimmed.to_string()))
        } else {
            let canonical = CanonicalRepoName::parse(trimmed)?;
            Ok(Self::Exact(canonical))
        }
    }

    pub fn matches(&self, candidate: &CanonicalRepoName) -> bool {
        match self {
            Self::All => true,
            Self::Exact(exact) => exact == candidate,
            Self::Subtree(prefix) => {
                if prefix == candidate {
                    return true;
                }
                let cand_str = candidate.as_str();
                let prefix_str = prefix.as_str();
                if cand_str.starts_with(prefix_str) {
                    let next_byte = cand_str.as_bytes().get(prefix_str.len());
                    next_byte == Some(&b'/')
                } else {
                    false
                }
            }
        }
    }
}

impl fmt::Display for ProxyRepoPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::All => write!(f, "*"),
            Self::Subtree(ns) => write!(f, "{}/*", ns.as_str()),
            Self::Exact(exact) => write!(f, "{}", exact.as_str()),
        }
    }
}

impl FromStr for ProxyRepoPattern {
    type Err = ProxyRepoPatternError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// Allowed repository namespace prefix for proxy safety checks.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProxyAllowedPrefix(pub CanonicalRepoName);

impl ProxyAllowedPrefix {
    pub fn parse(s: &str) -> Result<Self, RepoNameError> {
        let trimmed = s.trim().trim_end_matches('/');
        let canonical = CanonicalRepoName::parse(trimmed)?;
        Ok(Self(canonical))
    }

    pub fn matches(&self, candidate: &CanonicalRepoName) -> bool {
        let cand_str = candidate.as_str();
        let prefix_str = self.0.as_str();
        if cand_str == prefix_str {
            return true;
        }
        if cand_str.starts_with(prefix_str) {
            let next_byte = cand_str.as_bytes().get(prefix_str.len());
            next_byte == Some(&b'/')
        } else {
            false
        }
    }
}

impl fmt::Display for ProxyAllowedPrefix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/", self.0.as_str())
    }
}

#[derive(Clone)]
pub struct Proxy {
    cfg: ProxyConfig,
    client: reqwest::Client,
    upstream_host_allow: Vec<String>,
    upstream_base_domain: Option<String>,
    upstream_sem: Arc<Semaphore>,
    token_cache: Arc<Mutex<HashMap<String, CachedToken>>>,
    db: sled::Db,
    singleflight: Arc<SingleFlight>,
}

const MAX_TOKEN_CACHE_ENTRIES: usize = 1024;

#[derive(Clone, Debug)]
pub struct RepoDecision {
    pub local_repo: CanonicalRepoName,
    pub upstream_repo: CanonicalRepoName,
    pub tag_policy: TagPolicy,
    pub eviction_policy: EvictionPolicy,
}

#[derive(Clone, Debug)]
struct CachedToken {
    token: String,
    expires_at_unix: u64,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct TagMeta {
    pub digest: String,
    pub expires_at_unix: u64,
    pub etag: Option<String>,
}

pub use crate::manifest_refs::ManifestRefs;

#[derive(thiserror::Error, Debug)]
pub enum ProxyError {
    #[error("proxy disabled")]
    Disabled,
    #[error("repo not allowed")]
    RepoNotAllowed,
    #[error("upstream not configured")]
    UpstreamNotConfigured,
    #[error("invalid upstream url")]
    InvalidUpstreamUrl,
    #[error("upstream host not allowed: {0}")]
    UpstreamHostNotAllowed(String),
    #[error("blocked upstream egress to private network: {0}")]
    BlockedEgress(String),
    #[error("upstream request failed: {0}")]
    Upstream(String),
    #[error("digest mismatch")]
    DigestMismatch,
    #[error("not found")]
    NotFound,
    #[error("too large")]
    TooLarge,
    #[error("internal: {0}")]
    Internal(String),
}

impl Proxy {
    pub fn new(cfg: &ProxyConfig) -> Result<Option<Self>, ProxyError> {
        if !cfg.enabled {
            return Ok(None);
        }

        let upstream_base = cfg
            .upstream_base_url
            .as_deref()
            .ok_or(ProxyError::UpstreamNotConfigured)?;
        let upstream_url = Url::parse(upstream_base).map_err(|_| ProxyError::InvalidUpstreamUrl)?;
        let upstream_host = upstream_url
            .host_str()
            .ok_or(ProxyError::InvalidUpstreamUrl)?
            .to_string();

        // If no explicit allowlist is provided, default to allowing only the configured upstream host.
        let upstream_host_allow = if cfg.allowed_upstream_hosts.is_empty() {
            vec![upstream_host.clone()]
        } else {
            cfg.allowed_upstream_hosts.clone()
        };

        let upstream_base_domain = base_domain(&upstream_host);

        let client = reqwest::Client::builder()
            .redirect(Policy::none())
            .timeout(Duration::from_secs(60))
            .build()
            .map_err(|e| ProxyError::Internal(e.to_string()))?;

        let db = sled::open(&cfg.index_path).map_err(|e| ProxyError::Internal(e.to_string()))?;

        Ok(Some(Self {
            cfg: cfg.clone(),
            client,
            upstream_host_allow,
            upstream_base_domain,
            upstream_sem: Arc::new(Semaphore::new(cfg.max_concurrent_upstream.max(1))),
            token_cache: Arc::new(Mutex::new(HashMap::new())),
            db,
            singleflight: Arc::new(SingleFlight::default()),
        }))
    }

    pub fn upstream_base_url_for_log(&self) -> Option<&str> {
        self.cfg.upstream_base_url.as_deref()
    }

    fn is_token_realm_host_allowed(&self, host: &str) -> bool {
        if self
            .upstream_host_allow
            .iter()
            .any(|h| h.eq_ignore_ascii_case(host))
        {
            return true;
        }

        // Generic safe-ish default for Bearer token exchange without per-registry hardcoding:
        // allow token realm hosts under the same base domain as the configured upstream host.
        // Example: registry-1.docker.io -> auth.docker.io (base domain docker.io).
        let Some(up) = self.upstream_base_domain.as_deref() else {
            return false;
        };
        let Some(h) = base_domain(host) else {
            return false;
        };
        h.as_str().eq_ignore_ascii_case(up)
    }

    pub fn decision_for_repo(&self, repo: &str) -> Result<RepoDecision, ProxyError> {
        if !self.cfg.enabled {
            return Err(ProxyError::Disabled);
        }

        let canonical_repo =
            CanonicalRepoName::parse(repo).map_err(|_| ProxyError::RepoNotAllowed)?;

        if !self.allowed_by_prefix_safety(&canonical_repo) {
            return Err(ProxyError::RepoNotAllowed);
        }

        let matched_rule = self.match_rule(&canonical_repo);
        let (tag_policy, eviction_policy, upstream_repo) = match (&self.cfg.mode, matched_rule) {
            (ProxyMode::Allowlist, None) => return Err(ProxyError::RepoNotAllowed),
            (_, Some(rule)) => (
                rule.tag_policy.clone(),
                rule.eviction_policy.clone(),
                rule.upstream_repo
                    .clone()
                    .unwrap_or_else(|| canonical_repo.clone()),
            ),
            (ProxyMode::Any, None) => (
                TagPolicy::DigestOnly,
                EvictionPolicy::Default,
                canonical_repo.clone(),
            ),
        };

        Ok(RepoDecision {
            local_repo: canonical_repo,
            upstream_repo,
            tag_policy,
            eviction_policy,
        })
    }

    fn allowed_by_prefix_safety(&self, repo: &CanonicalRepoName) -> bool {
        if self.cfg.allowed_repo_prefixes.is_empty() {
            return true;
        }
        self.cfg
            .allowed_repo_prefixes
            .iter()
            .any(|p| p.matches(repo))
    }

    fn match_rule(&self, repo: &CanonicalRepoName) -> Option<&ProxyRepoRule> {
        // Precedence: Exact > Subtree (longer prefix first) > All
        let mut best_match: Option<(usize, usize, &ProxyRepoRule)> = None;
        for rule in &self.cfg.repo_rules {
            if rule.match_pattern.matches(repo) {
                let rank_info = match &rule.match_pattern {
                    ProxyRepoPattern::Exact(_) => (3, repo.as_str().len()),
                    ProxyRepoPattern::Subtree(ns) => (2, ns.as_str().len()),
                    ProxyRepoPattern::All => (1, 0),
                };
                match best_match {
                    None => {
                        best_match = Some((rank_info.0, rank_info.1, rule));
                    }
                    Some((best_rank, best_len, _)) => {
                        if rank_info.0 > best_rank
                            || (rank_info.0 == best_rank && rank_info.1 > best_len)
                        {
                            best_match = Some((rank_info.0, rank_info.1, rule));
                        }
                    }
                }
            }
        }
        best_match.map(|(_, _, rule)| rule)
    }

    fn upstream_base_url(&self) -> Result<Url, ProxyError> {
        let upstream_base = self
            .cfg
            .upstream_base_url
            .as_deref()
            .ok_or(ProxyError::UpstreamNotConfigured)?;
        Url::parse(upstream_base).map_err(|_| ProxyError::InvalidUpstreamUrl)
    }

    async fn ensure_upstream_allowed(&self, url: &Url) -> Result<(), ProxyError> {
        let host = url
            .host_str()
            .ok_or(ProxyError::InvalidUpstreamUrl)?
            .to_string();
        if !self
            .upstream_host_allow
            .iter()
            .any(|h| h.eq_ignore_ascii_case(&host))
        {
            return Err(ProxyError::UpstreamHostNotAllowed(host));
        }

        if self.cfg.block_private_networks {
            let port = url.port_or_known_default().unwrap_or(443);
            let addrs = tokio::net::lookup_host((host.as_str(), port))
                .await
                .map_err(|e| ProxyError::Upstream(e.to_string()))?;
            for addr in addrs {
                if is_blocked_ip(addr.ip()) {
                    return Err(ProxyError::BlockedEgress(addr.to_string()));
                }
            }
        }
        Ok(())
    }

    async fn ensure_redirect_allowed(&self, url: &Url) -> Result<(), ProxyError> {
        let host = url
            .host_str()
            .ok_or(ProxyError::InvalidUpstreamUrl)?
            .to_string();

        // Only follow HTTPS redirects.
        if url.scheme() != "https" {
            return Err(ProxyError::UpstreamHostNotAllowed(host));
        }

        // Only follow redirects to the standard HTTPS port.
        if let Some(port) = url.port() {
            if port != 443 {
                return Err(ProxyError::UpstreamHostNotAllowed(host));
            }
        }

        if self.cfg.block_private_networks {
            let port = url.port_or_known_default().unwrap_or(443);
            let addrs = tokio::net::lookup_host((host.as_str(), port))
                .await
                .map_err(|e| ProxyError::Upstream(e.to_string()))?;
            for addr in addrs {
                if is_blocked_ip(addr.ip()) {
                    return Err(ProxyError::BlockedEgress(addr.to_string()));
                }
            }
        }

        Ok(())
    }

    async fn ensure_token_realm_allowed(&self, url: &Url) -> Result<(), ProxyError> {
        let host = url
            .host_str()
            .ok_or(ProxyError::InvalidUpstreamUrl)?
            .to_string();
        if url.scheme() != "https" {
            return Err(ProxyError::UpstreamHostNotAllowed(host));
        }
        if !self.is_token_realm_host_allowed(&host) {
            return Err(ProxyError::UpstreamHostNotAllowed(host));
        }

        if self.cfg.block_private_networks {
            let port = url.port_or_known_default().unwrap_or(443);
            let addrs = tokio::net::lookup_host((host.as_str(), port))
                .await
                .map_err(|e| ProxyError::Upstream(e.to_string()))?;
            for addr in addrs {
                if is_blocked_ip(addr.ip()) {
                    return Err(ProxyError::BlockedEgress(addr.to_string()));
                }
            }
        }
        Ok(())
    }
    fn accept_manifest_header_value() -> &'static str {
        // Include both OCI and Docker media types, including manifest lists.
        "application/vnd.oci.image.manifest.v1+json, application/vnd.oci.artifact.manifest.v1+json, application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.v2+json, application/vnd.docker.distribution.manifest.list.v2+json"
    }

    pub async fn head_blob_upstream(
        &self,
        decision: &RepoDecision,
        digest: &Digest,
    ) -> Result<u64, ProxyError> {
        let _permit = self
            .upstream_sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| ProxyError::Internal(e.to_string()))?;

        let mut url = self.upstream_base_url()?;
        url.set_path(&format!(
            "/v2/{}/blobs/{}",
            decision.upstream_repo,
            digest.as_str()
        ));
        self.ensure_upstream_allowed(&url).await?;

        let resp = self
            .send_with_bearer(reqwest::Method::HEAD, url, None, Some(decision))
            .await?;

        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(ProxyError::NotFound);
        }
        if !resp.status().is_success() {
            return Err(ProxyError::Upstream(format!(
                "HEAD blob status {}",
                resp.status()
            )));
        }

        let len = resp
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        Ok(len)
    }

    pub async fn fetch_blob_into_storage(
        &self,
        decision: &RepoDecision,
        digest: &Digest,
        coordinator: &crate::upload_coordinator::BlobUploadCoordinator,
    ) -> Result<(), ProxyError> {
        // Singleflight per digest to avoid thundering herd.
        let key = format!("blob:{}", digest.as_str());
        let (sf_key, sf_arc, sf_guard) = self.singleflight.lock_key(&key).await;

        let result = async {
            if coordinator.storage().head_blob(digest).await.is_ok()
                && coordinator
                    .storage()
                    .get_repo_blob_membership(decision.local_repo.as_str(), digest)
                    .await
                    .ok()
                    .flatten()
                    .is_some()
            {
                return Ok(());
            }

            let _permit = self
                .upstream_sem
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| ProxyError::Internal(e.to_string()))?;

            let mut url = self.upstream_base_url()?;
            url.set_path(&format!(
                "/v2/{}/blobs/{}",
                decision.upstream_repo,
                digest.as_str()
            ));
            self.ensure_upstream_allowed(&url).await?;

            let resp = self
                .send_with_bearer(reqwest::Method::GET, url, None, Some(decision))
                .await?;
            if resp.status() == reqwest::StatusCode::NOT_FOUND {
                return Err(ProxyError::NotFound);
            }
            if !resp.status().is_success() {
                return Err(ProxyError::Upstream(format!(
                    "GET blob status {}",
                    resp.status()
                )));
            }

            let stream = resp.bytes_stream().map(|item| {
                item.map_err(|e| {
                    crate::storage::upload_session::UploadStreamError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        e.to_string(),
                    ))
                })
            });

            let pinned_stream: crate::storage::upload_session::UploadByteStream = Box::pin(stream);

            coordinator
                .publish_proxy_blob(&decision.local_repo, digest, pinned_stream)
                .await
                .map_err(|e| ProxyError::Internal(e.to_string()))?;

            self.note_blob_access(digest);
            Ok(())
        };

        let out = result.await;
        drop(sf_guard);
        self.singleflight.unlock_key(sf_key, sf_arc).await;
        out
    }

    pub async fn fetch_manifest_and_cache(
        &self,
        decision: &RepoDecision,
        reference: &str,
        _storage: &Arc<dyn crate::storage::Storage>,
        max_bytes: usize,
        revalidate_only: bool,
        if_none_match: Option<String>,
        lifecycle: &crate::manifest_lifecycle::ManifestLifecycleService,
    ) -> Result<FetchManifestResult, ProxyError> {
        // Singleflight per repo+reference.
        let key = format!("manifest:{}:{}", decision.local_repo.as_str(), reference);
        let (sf_key, sf_arc, sf_guard) = self.singleflight.lock_key(&key).await;

        let result = async {
            let mut url = self.upstream_base_url()?;
            url.set_path(&format!(
                "/v2/{}/manifests/{}",
                decision.upstream_repo.as_str(),
                reference
            ));
            self.ensure_upstream_allowed(&url).await?;

            let mut extra_headers = vec![(
                reqwest::header::ACCEPT,
                Self::accept_manifest_header_value().to_string(),
            )];
            if let Some(etag) = if_none_match {
                extra_headers.push((reqwest::header::IF_NONE_MATCH, etag));
            }

            let method = if revalidate_only {
                reqwest::Method::HEAD
            } else {
                reqwest::Method::GET
            };

            let resp = self
                .send_with_bearer(method, url, Some(extra_headers), Some(decision))
                .await?;

            if resp.status() == reqwest::StatusCode::NOT_FOUND {
                return Err(ProxyError::NotFound);
            }

            if resp.status() == reqwest::StatusCode::NOT_MODIFIED {
                let etag = resp
                    .headers()
                    .get(reqwest::header::ETAG)
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string());
                let digest = resp
                    .headers()
                    .get("docker-content-digest")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| Digest::parse(s).ok());
                return Ok(FetchManifestResult::NotModified { etag, digest });
            }

            if !resp.status().is_success() {
                return Err(ProxyError::Upstream(format!(
                    "{} manifest status {}",
                    if revalidate_only { "HEAD" } else { "GET" },
                    resp.status()
                )));
            }

            let media_type = resp
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
                .unwrap_or_else(|| "application/vnd.oci.image.manifest.v1+json".to_string());
            let etag = resp
                .headers()
                .get(reqwest::header::ETAG)
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());

            let upstream_digest = resp
                .headers()
                .get("docker-content-digest")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| Digest::parse(s).ok());

            if revalidate_only {
                return Ok(FetchManifestResult::NotModified {
                    etag,
                    digest: upstream_digest,
                });
            }

            let bytes = resp
                .bytes()
                .await
                .map_err(|e| ProxyError::Upstream(e.to_string()))?;
            if bytes.len() > max_bytes {
                return Err(ProxyError::TooLarge);
            }

            // Digest check.
            let mut hasher = sha2::Sha256::new();
            sha2::Digest::update(&mut hasher, &bytes);
            let hex = hex::encode(sha2::Digest::finalize(hasher));
            let computed = Digest::parse(&format!("sha256:{hex}"))
                .map_err(|_| ProxyError::Internal("failed to parse computed digest".to_string()))?;

            if let Ok(ref_digest) = Digest::parse(reference) {
                if ref_digest.hex() != computed.hex() {
                    return Err(ProxyError::DigestMismatch);
                }
            }
            if let Some(up) = upstream_digest {
                if up.hex() != computed.hex() {
                    // Should not happen with a correct upstream.
                    return Err(ProxyError::DigestMismatch);
                }
            }

            // Validate manifest descriptor structure before storing in local cache
            if let Err(e) = crate::manifest_refs::parse_manifest_refs(&bytes) {
                return Err(ProxyError::Upstream(format!(
                    "upstream manifest has invalid reference structure: {e}"
                )));
            }

            let evidence = crate::manifest_lifecycle::ProxyPublicationEvidence::new(
                decision.local_repo.as_str(),
                reference,
                bytes.clone(),
                Some(media_type.clone()),
                true,
                computed.clone(),
            );

            let published = lifecycle
                .publish_proxy_cached_manifest(evidence)
                .await
                .map_err(|e| ProxyError::Internal(format!("lifecycle publication failed: {e}")))?;

            self.index_manifest(decision.local_repo.as_str(), &published.digest, &bytes);
            self.note_manifest_access(decision.local_repo.as_str(), &published.digest);

            Ok(FetchManifestResult::Fetched {
                digest: published.digest,
                media_type: published.media_type,
                etag,
                bytes,
            })
        };

        let out = result.await;
        drop(sf_guard);
        self.singleflight.unlock_key(sf_key, sf_arc).await;
        out
    }

    pub fn get_tag_meta(&self, repo: &str, tag: &str) -> Option<TagMeta> {
        let key = format!("tagmeta::{repo}::{tag}");
        self.db
            .get(key)
            .ok()
            .flatten()
            .and_then(|v| serde_json::from_slice::<TagMeta>(&v).ok())
    }

    pub fn put_tag_meta(&self, repo: &str, tag: &str, meta: &TagMeta) {
        let key = format!("tagmeta::{repo}::{tag}");
        if let Ok(v) = serde_json::to_vec(meta) {
            let _ = self.db.insert(key, v);
            let _ = self.db.flush();
        }
    }

    pub fn note_blob_access(&self, digest: &Digest) {
        let key = format!("blobaccess::{}", digest.hex());
        let ts = Self::now_unix();
        let _ = self.db.insert(key, ts.to_be_bytes().to_vec());
    }

    pub fn note_manifest_access(&self, repo: &str, digest: &Digest) {
        let key = format!("manifestaccess::{repo}::{}", digest.hex());
        let ts = Self::now_unix();
        let _ = self.db.insert(key, ts.to_be_bytes().to_vec());
    }

    pub fn note_tag_access(&self, repo: &str, tag: &str) {
        let key = format!("tagaccess::{repo}::{tag}");
        let ts = Self::now_unix();
        let _ = self.db.insert(key, ts.to_be_bytes().to_vec());
    }

    pub fn get_tag_last_access(&self, repo: &str, tag: &str) -> Option<u64> {
        let key = format!("tagaccess::{repo}::{tag}");
        let v = self.db.get(key).ok().flatten()?;
        if v.len() != 8 {
            return None;
        }
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&v);
        Some(u64::from_be_bytes(buf))
    }

    pub fn index_manifest(&self, repo: &str, digest: &Digest, bytes: &[u8]) {
        let Ok(refs) = crate::manifest_refs::parse_manifest_refs(bytes) else {
            return;
        };

        let key = format!("manifestrefs::{repo}::{}", digest.hex());
        if let Ok(encoded) = serde_json::to_vec(&refs) {
            let _ = self.db.insert(key, encoded);
        }
    }

    pub fn get_manifest_refs(&self, repo: &str, digest: &Digest) -> Option<ManifestRefs> {
        let key = format!("manifestrefs::{repo}::{}", digest.hex());
        self.db
            .get(key)
            .ok()
            .flatten()
            .and_then(|v| serde_json::from_slice::<ManifestRefs>(&v).ok())
    }

    pub fn get_blob_last_access(&self, digest: &Digest) -> Option<u64> {
        let key = format!("blobaccess::{}", digest.hex());
        let v = self.db.get(key).ok().flatten()?;
        if v.len() != 8 {
            return None;
        }
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&v);
        Some(u64::from_be_bytes(buf))
    }

    pub fn now_unix() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_secs()
    }

    pub fn ttl_expires_at(ttl_secs: u64) -> u64 {
        Self::now_unix().saturating_add(ttl_secs)
    }

    async fn send_with_bearer(
        &self,
        method: reqwest::Method,
        url: Url,
        extra_headers: Option<Vec<(reqwest::header::HeaderName, String)>>,
        decision: Option<&RepoDecision>,
    ) -> Result<reqwest::Response, ProxyError> {
        const MAX_REDIRECTS: usize = 5;

        // First request must be to the configured upstream host; redirects are checked separately.
        self.ensure_upstream_allowed(&url).await?;

        let mut current_url = url;
        let mut current_headers = extra_headers;
        let mut bearer_token: Option<String> = None;
        let mut redirects = 0usize;

        loop {
            if redirects > 0 {
                self.ensure_redirect_allowed(&current_url).await?;
            }

            let mut req = self.client.request(method.clone(), current_url.clone());

            if let Some(hs) = current_headers.as_ref() {
                for (name, value) in hs {
                    if let Ok(v) = reqwest::header::HeaderValue::from_str(value) {
                        req = req.header(name, v);
                    }
                }
            }

            if let Some(tok) = bearer_token.as_ref() {
                req = req.bearer_auth(tok);
            }

            let resp = req
                .send()
                .await
                .map_err(|e| ProxyError::Upstream(e.to_string()))?;

            // Handle bearer challenge only for the original upstream host.
            if resp.status() == reqwest::StatusCode::UNAUTHORIZED && bearer_token.is_none() {
                let www = resp
                    .headers()
                    .get(reqwest::header::WWW_AUTHENTICATE)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string();
                let Some(challenge) = parse_bearer_challenge(&www) else {
                    return Err(ProxyError::Upstream("missing bearer challenge".to_string()));
                };

                // Compute scope if not present (Docker Hub usually includes it).
                let scope = challenge.scope.or_else(|| {
                    decision.map(|d| format!("repository:{}:pull", d.upstream_repo.as_str()))
                });
                let token = self
                    .get_token(&challenge.realm, &challenge.service, scope.as_deref())
                    .await?;
                bearer_token = Some(token);
                continue;
            }

            // Upstreams (notably Docker Hub) may redirect blob downloads to a CDN.
            if resp.status().is_redirection() {
                // Never follow redirects for non-idempotent methods.
                if method != reqwest::Method::GET && method != reqwest::Method::HEAD {
                    return Err(ProxyError::Upstream(format!(
                        "unexpected redirect for method {}",
                        method
                    )));
                }

                let Some(loc) = resp
                    .headers()
                    .get(reqwest::header::LOCATION)
                    .and_then(|v| v.to_str().ok())
                else {
                    return Err(ProxyError::Upstream(
                        "redirect without Location".to_string(),
                    ));
                };

                if redirects >= MAX_REDIRECTS {
                    return Err(ProxyError::Upstream(
                        "too many upstream redirects".to_string(),
                    ));
                }
                redirects += 1;

                let next_url = Url::parse(loc)
                    .or_else(|_| current_url.join(loc))
                    .map_err(|_| ProxyError::Upstream("invalid redirect location".to_string()))?;

                let host_changed = next_url.host_str() != current_url.host_str();

                match self.cfg.redirect_policy {
                    RedirectPolicy::Disabled => {
                        return Err(ProxyError::Upstream(
                            "upstream redirect blocked".to_string(),
                        ));
                    }
                    RedirectPolicy::SameHost => {
                        if host_changed {
                            return Err(ProxyError::Upstream(
                                "upstream cross-host redirect blocked".to_string(),
                            ));
                        }
                    }
                    RedirectPolicy::AnyPublic => {}
                }

                current_url = next_url;

                if host_changed {
                    // Never forward bearer auth across hosts.
                    bearer_token = None;

                    // Drop any potentially sensitive headers if we ever add them.
                    if let Some(hs) = current_headers.as_ref() {
                        let filtered: Vec<(reqwest::header::HeaderName, String)> = hs
                            .iter()
                            .filter(|(n, _)| {
                                *n != reqwest::header::AUTHORIZATION
                                    && *n != reqwest::header::COOKIE
                                    && *n != reqwest::header::PROXY_AUTHORIZATION
                            })
                            .cloned()
                            .collect();
                        current_headers = Some(filtered);
                    }
                }

                continue;
            }

            return Ok(resp);
        }
    }

    async fn get_token(
        &self,
        realm: &str,
        service: &str,
        scope: Option<&str>,
    ) -> Result<String, ProxyError> {
        let cache_key = format!("{realm}|{service}|{}", scope.unwrap_or(""));
        let now = Self::now_unix();
        {
            let cache = self.token_cache.lock().await;
            if let Some(t) = cache.get(&cache_key) {
                if t.expires_at_unix > now.saturating_add(5) {
                    return Ok(t.token.clone());
                }
            }
        }

        let realm_url = Url::parse(realm)
            .map_err(|_| ProxyError::Upstream("invalid token realm".to_string()))?;
        self.ensure_token_realm_allowed(&realm_url).await?;

        let mut base_req = self.client.get(realm_url.clone());
        base_req = base_req.query(&[("service", service)]);
        if let Some(scope) = scope {
            base_req = base_req.query(&[("scope", scope)]);
        }

        // Optional upstream credentials (e.g. Docker Hub requires authentication to raise rate limits).
        // Treat empty strings as "not set" to avoid spurious 401s.
        let mut auth_user: Option<&str> = None;
        let mut auth_pass: Option<&str> = None;
        if let (Some(user), Some(pass)) = (
            self.cfg.upstream_username.as_deref(),
            self.cfg.upstream_password.as_deref(),
        ) {
            let user = user.trim();
            let pass = pass.trim();
            if !user.is_empty() && !pass.is_empty() {
                auth_user = Some(user);
                auth_pass = Some(pass);
            }
        }

        let mut resp = {
            let mut req = base_req;
            if let (Some(user), Some(pass)) = (auth_user, auth_pass) {
                req = req.basic_auth(user, Some(pass));
            }
            req.send()
                .await
                .map_err(|e| ProxyError::Upstream(e.to_string()))?
        };

        // If credentials were configured but rejected, fall back to anonymous token fetch.
        // This keeps public pulls working even if upstream creds are wrong.
        if resp.status() == reqwest::StatusCode::UNAUTHORIZED && auth_user.is_some() {
            tracing::warn!(
                token_realm = realm,
                token_service = service,
                token_scope = scope.unwrap_or(""),
                "proxy: upstream token basic auth rejected, retrying anonymously"
            );
            resp = self
                .client
                .get(realm_url.clone())
                .query(&[("service", service)])
                .query(&scope.map(|s| ("scope", s)).into_iter().collect::<Vec<_>>())
                .send()
                .await
                .map_err(|e| ProxyError::Upstream(e.to_string()))?;
        }

        if !resp.status().is_success() {
            return Err(ProxyError::Upstream(format!(
                "token endpoint status {} (realm={realm} service={service} scope={} auth_used={})",
                resp.status(),
                scope.unwrap_or(""),
                auth_user.is_some()
            )));
        }
        let v: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| ProxyError::Upstream(e.to_string()))?;
        let token = v
            .get("token")
            .or_else(|| v.get("access_token"))
            .and_then(|t| t.as_str())
            .ok_or_else(|| ProxyError::Upstream("missing token in response".to_string()))?
            .to_string();
        let expires_in = v.get("expires_in").and_then(|e| e.as_u64()).unwrap_or(300);

        let mut cache = self.token_cache.lock().await;
        if cache.len() >= MAX_TOKEN_CACHE_ENTRIES {
            cache.retain(|_, t| t.expires_at_unix > now.saturating_add(5));
            if cache.len() >= MAX_TOKEN_CACHE_ENTRIES {
                cache.clear();
            }
        }
        cache.insert(
            cache_key,
            CachedToken {
                token: token.clone(),
                expires_at_unix: now.saturating_add(expires_in),
            },
        );
        Ok(token)
    }
}

pub enum FetchManifestResult {
    Fetched {
        digest: Digest,
        media_type: String,
        etag: Option<String>,
        bytes: Bytes,
    },
    HeadOk {
        media_type: String,
        etag: Option<String>,
        digest: Option<Digest>,
    },
    NotModified {
        etag: Option<String>,
        digest: Option<Digest>,
    },
}

#[derive(Default)]
struct SingleFlight {
    locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl SingleFlight {
    async fn lock_key(
        &self,
        key: &str,
    ) -> (
        String,
        Arc<tokio::sync::Mutex<()>>,
        tokio::sync::OwnedMutexGuard<()>,
    ) {
        let key_string = key.to_string();
        let arc = {
            let mut map = self.locks.lock().await;
            map.entry(key_string.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        let guard = arc.clone().lock_owned().await;
        (key_string, arc, guard)
    }

    async fn unlock_key(&self, key: String, arc: Arc<tokio::sync::Mutex<()>>) {
        // If no other tasks are waiting/running for this key, drop it from the map.
        // We remove when the only remaining strong refs should be: map entry + this call.
        if Arc::strong_count(&arc) == 2 {
            let mut map = self.locks.lock().await;
            if let Some(existing) = map.get(&key) {
                if Arc::ptr_eq(existing, &arc) {
                    map.remove(&key);
                }
            }
        }
    }
}

fn base_domain(host: &str) -> Option<String> {
    let host = host.trim().trim_end_matches('.');
    if host.is_empty() {
        return None;
    }

    // Never treat IPs as having a "base domain".
    if host.parse::<IpAddr>().is_ok() {
        return None;
    }

    let labels: Vec<&str> = host.split('.').filter(|l| !l.is_empty()).collect();
    if labels.len() < 2 {
        return None;
    }

    Some(format!("{}.{}", labels[labels.len() - 2], labels[labels.len() - 1]).to_ascii_lowercase())
}

fn is_blocked_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_loopback()
                || v4.is_private()
                || v4.is_link_local()
                || v4.is_broadcast()
                || v4.is_unspecified()
                || v4.is_multicast()
        }
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unique_local()
                || v6.is_unicast_link_local()
                || v6.is_unspecified()
                || v6.is_multicast()
        }
    }
}

#[allow(dead_code)]
async fn read_response_limited(resp: reqwest::Response, limit: usize) -> Result<Bytes, ProxyError> {
    if let Some(len) = resp.content_length() {
        if len > limit as u64 {
            return Err(ProxyError::TooLarge);
        }
    }

    let initial_capacity = resp
        .content_length()
        .unwrap_or(0)
        .min(limit as u64)
        .min(1024 * 1024) as usize;
    let mut buf: Vec<u8> = Vec::with_capacity(initial_capacity);
    let mut stream = resp.bytes_stream();
    while let Some(next) = stream.next().await {
        let chunk = next.map_err(|e| ProxyError::Upstream(e.to_string()))?;
        if chunk.is_empty() {
            continue;
        }
        if buf.len().saturating_add(chunk.len()) > limit {
            return Err(ProxyError::TooLarge);
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(Bytes::from(buf))
}

#[derive(Debug)]
struct BearerChallenge {
    realm: String,
    service: String,
    scope: Option<String>,
}

fn parse_bearer_challenge(header: &str) -> Option<BearerChallenge> {
    // Example: Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:library/alpine:pull"
    let header = header.trim();
    if !header.to_ascii_lowercase().starts_with("bearer ") {
        return None;
    }
    let rest = header[7..].trim();
    let mut realm = None;
    let mut service = None;
    let mut scope = None;
    for part in rest.split(',') {
        let part = part.trim();
        let (k, v) = part.split_once('=')?;
        let k = k.trim().to_ascii_lowercase();
        let mut v = v.trim().to_string();
        if v.starts_with('"') && v.ends_with('"') && v.len() >= 2 {
            v = v[1..v.len() - 1].to_string();
        }
        match k.as_str() {
            "realm" => realm = Some(v),
            "service" => service = Some(v),
            "scope" => scope = Some(v),
            _ => {}
        }
    }
    Some(BearerChallenge {
        realm: realm?,
        service: service?,
        scope,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_proxy() -> (Proxy, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("proxy.db");
        let cfg = ProxyConfig {
            enabled: true,
            mode: ProxyMode::Allowlist,
            upstream_base_url: Some("http://localhost:5000".to_string()),
            upstream_username: None,
            upstream_password: None,
            allowed_upstream_hosts: vec!["localhost".to_string()],
            allowed_repo_prefixes: vec![],
            block_private_networks: false,
            redirect_policy: RedirectPolicy::AnyPublic,
            max_concurrent_upstream: 10,
            index_path: db_path,
            cache_fs_root: None,
            cache_s3_prefix: None,
            gc_interval_secs: 0,
            scrub_enabled: false,
            scrub_interval_secs: 0,
            scrub_max_files_per_run: 0,
            max_cache_bytes: None,
            repo_rules: vec![],
            upstreams: vec![],
            routing_proxy_hosts: vec![],
            routing_trust_x_forwarded_host: false,
        };
        let proxy = Proxy::new(&cfg).unwrap().unwrap();
        (proxy, temp_dir)
    }

    #[test]
    fn test_index_manifest_handles_oci_artifact_blobs_and_subject() {
        let (proxy, _temp) = create_test_proxy();
        let repo = "library/artifact";
        let digest = Digest::parse(
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap();
        let blob_digest = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let subject_digest =
            "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

        let artifact_json = serde_json::json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.artifact.manifest.v1+json",
            "blobs": [{ "digest": blob_digest }],
            "subject": { "digest": subject_digest }
        });
        let bytes = serde_json::to_vec(&artifact_json).unwrap();

        proxy.index_manifest(repo, &digest, &bytes);
        let refs = proxy
            .get_manifest_refs(repo, &digest)
            .expect("indexed refs");

        let blob_refs: Vec<String> = refs.blob_references().map(|d| d.as_str()).collect();
        assert_eq!(blob_refs, vec![blob_digest]);

        let manifest_refs: Vec<String> = refs.manifest_references().map(|d| d.as_str()).collect();
        assert_eq!(manifest_refs, vec![subject_digest]);
    }

    #[test]
    fn test_index_manifest_handles_image_manifest_and_index() {
        let (proxy, _temp) = create_test_proxy();
        let repo = "library/image";
        let img_digest = Digest::parse(
            "sha256:1111111111111111111111111111111111111111111111111111111111111111",
        )
        .unwrap();
        let cfg_digest = "sha256:2222222222222222222222222222222222222222222222222222222222222222";
        let layer_digest =
            "sha256:3333333333333333333333333333333333333333333333333333333333333333";

        let img_json = serde_json::json!({
            "schemaVersion": 2,
            "config": { "digest": cfg_digest },
            "layers": [{ "digest": layer_digest }]
        });
        proxy.index_manifest(repo, &img_digest, &serde_json::to_vec(&img_json).unwrap());

        let refs = proxy
            .get_manifest_refs(repo, &img_digest)
            .expect("indexed image refs");
        let blob_refs: Vec<String> = refs.blob_references().map(|d| d.as_str()).collect();
        assert_eq!(blob_refs, vec![cfg_digest, layer_digest]);
    }

    #[test]
    fn test_index_manifest_rejects_malformed_and_does_not_index_partial_graph() {
        let (proxy, _temp) = create_test_proxy();
        let repo = "library/malformed";
        let digest = Digest::parse(
            "sha256:1111111111111111111111111111111111111111111111111111111111111111",
        )
        .unwrap();

        // Mixed valid and invalid layer digest
        let malformed_json = serde_json::json!({
            "schemaVersion": 2,
            "config": { "digest": "sha256:2222222222222222222222222222222222222222222222222222222222222222" },
            "layers": [
                { "digest": "sha256:invalid-hex" }
            ]
        });
        proxy.index_manifest(repo, &digest, &serde_json::to_vec(&malformed_json).unwrap());

        assert!(
            proxy.get_manifest_refs(repo, &digest).is_none(),
            "malformed manifest must not be indexed in proxy db"
        );
    }
}
