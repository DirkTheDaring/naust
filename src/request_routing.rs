use axum::http::HeaderMap;

use crate::config::ProxyConfig;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum V2RouteMode {
    /// Default behavior: serve from local storage (and may fall back to proxy cache/upstream).
    Default,
    /// Proxy-only behavior: never consult local storage for reads; use cache/upstream only.
    ProxyOnly,
}

fn strip_port(host: &str) -> &str {
    // Host header can be: "example.com" or "example.com:5000".
    // IPv6 literals are usually bracketed; we keep it simple and only strip ":port" for non-IPv6.
    if host.starts_with('[') {
        return host;
    }
    host.split_once(':').map(|(h, _)| h).unwrap_or(host)
}

pub fn effective_host_for_request(
    headers: &HeaderMap,
    trust_x_forwarded_host: bool,
) -> Option<String> {
    let raw = if trust_x_forwarded_host {
        headers
            .get("x-forwarded-host")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.split(',').next())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .or_else(|| {
                headers
                    .get(axum::http::header::HOST)
                    .and_then(|v| v.to_str().ok())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string())
            })
    } else {
        headers
            .get(axum::http::header::HOST)
            .and_then(|v| v.to_str().ok())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
    }?;

    let host = strip_port(&raw).trim().to_ascii_lowercase();
    if host.is_empty() { None } else { Some(host) }
}

fn host_matches_any(patterns: &[crate::proxy::ProxyHostPattern], host: &str) -> bool {
    patterns.iter().any(|p| p.matches(host))
}

pub fn proxy_upstream_index_for_request(proxy: &ProxyConfig, headers: &HeaderMap) -> Option<usize> {
    if proxy.upstreams.is_empty() {
        return None;
    }

    // We compute both variants once and then select per-route.
    let host_direct = effective_host_for_request(headers, false);
    let host_forwarded = effective_host_for_request(headers, true);

    for (idx, r) in proxy.upstreams.iter().enumerate() {
        let host = if r.trust_x_forwarded_host {
            host_forwarded.as_deref()
        } else {
            host_direct.as_deref()
        };

        if let Some(host) = host {
            if host_matches_any(&r.hosts, host) {
                return Some(idx);
            }
        }
    }

    None
}

pub fn v2_route_mode_for_request(proxy: &ProxyConfig, headers: &HeaderMap) -> V2RouteMode {
    // Multi-upstream mode: if a request matches an upstream route by host, it is proxy-only.
    if !proxy.upstreams.is_empty() {
        return if proxy_upstream_index_for_request(proxy, headers).is_some() {
            V2RouteMode::ProxyOnly
        } else {
            V2RouteMode::Default
        };
    }

    if proxy.routing_proxy_hosts.is_empty() {
        return V2RouteMode::Default;
    }

    let Some(host) = effective_host_for_request(headers, proxy.routing_trust_x_forwarded_host)
    else {
        return V2RouteMode::Default;
    };

    if host_matches_any(&proxy.routing_proxy_hosts, &host) {
        V2RouteMode::ProxyOnly
    } else {
        V2RouteMode::Default
    }
}

pub fn resolve_trusted_client_ip(
    peer_addr: std::net::IpAddr,
    headers: &HeaderMap,
    trusted_proxies: &[ipnet::IpNet],
) -> std::net::IpAddr {
    let is_peer_trusted = trusted_proxies.iter().any(|net| net.contains(&peer_addr));
    if !is_peer_trusted {
        return peer_addr;
    }

    if let Some(xff) = headers.get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
        for ip_str in xff.split(',').map(str::trim).rev() {
            if let Ok(ip) = ip_str.parse::<std::net::IpAddr>() {
                if !trusted_proxies.iter().any(|net| net.contains(&ip)) {
                    return ip;
                }
            }
        }
    }
    peer_addr
}

#[cfg(test)]
mod tests {
    use super::{
        V2RouteMode, effective_host_for_request, proxy_upstream_index_for_request,
        resolve_trusted_client_ip, v2_route_mode_for_request,
    };
    use crate::config::{ProxyConfig, ProxyMode, RedirectPolicy};
    use axum::http::HeaderMap;
    use std::path::PathBuf;

    fn proxy_cfg(proxy_hosts: Vec<&str>, trust_xfh: bool) -> ProxyConfig {
        let routing_proxy_hosts = proxy_hosts
            .into_iter()
            .map(|s| crate::proxy::ProxyHostPattern::parse(s).unwrap())
            .collect();
        let allowed_repo_prefixes =
            vec![crate::proxy::ProxyAllowedPrefix::parse("library").unwrap()];
        ProxyConfig {
            enabled: true,
            mode: ProxyMode::Allowlist,
            upstream_base_url: Some("https://registry-1.docker.io".to_string()),
            upstream_username: None,
            upstream_password: None,
            allowed_upstream_hosts: vec![],
            token_realm_hosts: vec![],
            allowed_repo_prefixes,
            block_private_networks: true,
            redirect_policy: RedirectPolicy::AnyPublic,
            max_concurrent_upstream: 1,
            index_path: PathBuf::from("/tmp/naust-test-proxy-index"),
            cache_fs_root: None,
            cache_s3_prefix: None,
            gc_interval_secs: 3600,
            scrub_enabled: false,
            scrub_interval_secs: 3600,
            scrub_max_files_per_run: 1,
            max_cache_bytes: None,
            repo_rules: vec![],
            upstreams: vec![],
            routing_proxy_hosts,
            routing_trust_x_forwarded_host: trust_xfh,
        }
    }

    #[test]
    fn host_wildcard_matching() {
        use crate::glob::wildcard_match;
        assert!(wildcard_match("cache.example.com", "cache.example.com"));
        assert!(!wildcard_match("cache.example.com", "other.example.com"));
        assert!(wildcard_match("*.example.com", "cache.example.com"));
        assert!(wildcard_match("cache.*", "cache.example.com"));
        assert!(wildcard_match("*", "anything"));
    }

    #[test]
    fn effective_host_uses_host_and_strips_port() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::HOST,
            "Cache.Example.Com:5000".parse().unwrap(),
        );
        assert_eq!(
            effective_host_for_request(&headers, false).as_deref(),
            Some("cache.example.com")
        );
    }

    #[test]
    fn effective_host_can_use_x_forwarded_host_when_trusted() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::HOST,
            "local.example.com".parse().unwrap(),
        );
        headers.insert("x-forwarded-host", "cache.example.com".parse().unwrap());
        assert_eq!(
            effective_host_for_request(&headers, true).as_deref(),
            Some("cache.example.com")
        );
        assert_eq!(
            effective_host_for_request(&headers, false).as_deref(),
            Some("local.example.com")
        );
    }

    #[test]
    fn proxy_upstream_index_selects_matching_route() {
        let mut cfg = proxy_cfg(vec![], false);
        cfg.upstreams = vec![
            crate::config::ProxyUpstreamRoute {
                hosts: vec![
                    crate::proxy::ProxyHostPattern::parse("dockerhub-cache.example.com").unwrap(),
                ],
                trust_x_forwarded_host: false,
                upstream_base_url: "https://registry-1.docker.io".to_string(),
                upstream_username: None,
                upstream_password: None,
                allowed_upstream_hosts: vec![],
                token_realm_hosts: vec![],
                allowed_repo_prefixes: vec![
                    crate::proxy::ProxyAllowedPrefix::parse("library").unwrap(),
                ],
                block_private_networks: true,
                redirect_policy: RedirectPolicy::AnyPublic,
                max_concurrent_upstream: 1,
                index_path: PathBuf::from("/tmp/a"),
                cache_fs_root: Some(PathBuf::from("/tmp/cache-a")),
                cache_s3_prefix: None,
                max_cache_bytes: 1,
            },
            crate::config::ProxyUpstreamRoute {
                hosts: vec![
                    crate::proxy::ProxyHostPattern::parse("ghcr-cache.example.com").unwrap(),
                ],
                trust_x_forwarded_host: false,
                upstream_base_url: "https://ghcr.io".to_string(),
                upstream_username: None,
                upstream_password: None,
                allowed_upstream_hosts: vec![],
                token_realm_hosts: vec![],
                allowed_repo_prefixes: vec![],
                block_private_networks: true,
                redirect_policy: RedirectPolicy::AnyPublic,
                max_concurrent_upstream: 1,
                index_path: PathBuf::from("/tmp/b"),
                cache_fs_root: Some(PathBuf::from("/tmp/cache-b")),
                cache_s3_prefix: None,
                max_cache_bytes: 1,
            },
        ];

        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::HOST,
            "ghcr-cache.example.com".parse().unwrap(),
        );
        assert_eq!(proxy_upstream_index_for_request(&cfg, &headers), Some(1));
        assert_eq!(
            v2_route_mode_for_request(&cfg, &headers),
            V2RouteMode::ProxyOnly
        );
    }

    #[test]
    fn route_mode_defaults_when_no_proxy_hosts_configured() {
        let cfg = proxy_cfg(vec![], false);
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::HOST,
            "cache.example.com".parse().unwrap(),
        );
        assert_eq!(
            v2_route_mode_for_request(&cfg, &headers),
            V2RouteMode::Default
        );
    }

    #[test]
    fn route_mode_proxy_only_when_host_matches() {
        let cfg = proxy_cfg(vec!["cache.example.com", "*.proxy.local"], false);
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::HOST,
            "cache.example.com".parse().unwrap(),
        );
        assert_eq!(
            v2_route_mode_for_request(&cfg, &headers),
            V2RouteMode::ProxyOnly
        );

        headers.insert(axum::http::header::HOST, "foo.proxy.local".parse().unwrap());
        assert_eq!(
            v2_route_mode_for_request(&cfg, &headers),
            V2RouteMode::ProxyOnly
        );
    }

    #[test]
    fn trusted_client_ip_resolution_and_anti_spoofing() {
        use std::str::FromStr;
        let trusted_proxies = vec![
            ipnet::IpNet::from_str("10.0.0.0/8").unwrap(),
            ipnet::IpNet::from_str("127.0.0.1/32").unwrap(),
        ];

        // 1. Untrusted peer sending spoofed X-Forwarded-For: must return peer IP
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "1.1.1.1, 10.0.0.5".parse().unwrap());
        let untrusted_peer: std::net::IpAddr = "198.51.100.1".parse().unwrap();
        assert_eq!(
            resolve_trusted_client_ip(untrusted_peer, &headers, &trusted_proxies),
            untrusted_peer
        );

        // 2. Trusted peer sending valid chain: must return last untrusted client IP
        let trusted_peer: std::net::IpAddr = "10.0.0.1".parse().unwrap();
        let expected_client: std::net::IpAddr = "203.0.113.50".parse().unwrap();
        headers.insert("x-forwarded-for", "203.0.113.50, 10.0.0.2".parse().unwrap());
        assert_eq!(
            resolve_trusted_client_ip(trusted_peer, &headers, &trusted_proxies),
            expected_client
        );
    }
}
