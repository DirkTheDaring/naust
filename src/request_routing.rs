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

fn effective_host(headers: &HeaderMap, trust_x_forwarded_host: bool) -> Option<String> {
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

	// First part must match at start.
	if !parts[0].is_empty() {
		if let Some(r) = rest.strip_prefix(parts[0]) {
			rest = r;
		} else {
			return false;
		}
	}

	// Middle parts must occur in order.
	for part in parts.iter().skip(1).take(parts.len().saturating_sub(2)) {
		if part.is_empty() {
			continue;
		}
		if let Some(idx) = rest.find(part) {
			rest = &rest[idx + part.len()..];
		} else {
			return false;
		}
	}

	// Last part must match at end.
	if let Some(last) = parts.last() {
		if last.is_empty() {
			return true;
		}
		return rest.ends_with(last);
	}
	true
}

pub fn v2_route_mode_for_request(proxy: &ProxyConfig, headers: &HeaderMap) -> V2RouteMode {
	if proxy.routing_proxy_hosts.is_empty() {
		return V2RouteMode::Default;
	}

	let Some(host) = effective_host(headers, proxy.routing_trust_x_forwarded_host) else {
		return V2RouteMode::Default;
	};

	if proxy
		.routing_proxy_hosts
		.iter()
		.any(|p| wildcard_match(&p.to_ascii_lowercase(), &host))
	{
		V2RouteMode::ProxyOnly
	} else {
		V2RouteMode::Default
	}
}

#[cfg(test)]
mod tests {
	use super::{effective_host, v2_route_mode_for_request, wildcard_match, V2RouteMode};
	use crate::config::{ProxyConfig, ProxyMode};
	use axum::http::HeaderMap;
	use std::path::PathBuf;

	fn proxy_cfg(proxy_hosts: Vec<&str>, trust_xfh: bool) -> ProxyConfig {
		ProxyConfig {
			enabled: true,
			mode: ProxyMode::Allowlist,
			upstream_base_url: Some("https://registry-1.docker.io".to_string()),
			upstream_username: None,
			upstream_password: None,
			allowed_upstream_hosts: vec![],
			allowed_repo_prefixes: vec!["library/".to_string()],
			block_private_networks: true,
			max_concurrent_upstream: 1,
			index_path: PathBuf::from("/tmp/registry-rust-test-proxy-index"),
			cache_fs_root: None,
			cache_s3_prefix: None,
			gc_interval_secs: 3600,
			scrub_enabled: false,
			scrub_interval_secs: 3600,
			scrub_max_files_per_run: 1,
			max_cache_bytes: None,
			repo_rules: vec![],
			routing_proxy_hosts: proxy_hosts.into_iter().map(|s| s.to_string()).collect(),
			routing_trust_x_forwarded_host: trust_xfh,
		}
	}

	#[test]
	fn wildcard_match_minimal_glob() {
		assert!(wildcard_match("cache.example.com", "cache.example.com"));
		assert!(!wildcard_match("cache.example.com", "other.example.com"));
		assert!(wildcard_match("*.example.com", "cache.example.com"));
		assert!(wildcard_match("cache.*", "cache.example.com"));
		assert!(wildcard_match("*", "anything"));
	}

	#[test]
	fn effective_host_uses_host_and_strips_port() {
		let mut headers = HeaderMap::new();
		headers.insert(axum::http::header::HOST, "Cache.Example.Com:5000".parse().unwrap());
		assert_eq!(
			effective_host(&headers, false).as_deref(),
			Some("cache.example.com")
		);
	}

	#[test]
	fn effective_host_can_use_x_forwarded_host_when_trusted() {
		let mut headers = HeaderMap::new();
		headers.insert(axum::http::header::HOST, "local.example.com".parse().unwrap());
		headers.insert("x-forwarded-host", "cache.example.com".parse().unwrap());
		assert_eq!(
			effective_host(&headers, true).as_deref(),
			Some("cache.example.com")
		);
		assert_eq!(
			effective_host(&headers, false).as_deref(),
			Some("local.example.com")
		);
	}

	#[test]
	fn route_mode_defaults_when_no_proxy_hosts_configured() {
		let cfg = proxy_cfg(vec![], false);
		let mut headers = HeaderMap::new();
		headers.insert(axum::http::header::HOST, "cache.example.com".parse().unwrap());
		assert_eq!(v2_route_mode_for_request(&cfg, &headers), V2RouteMode::Default);
	}

	#[test]
	fn route_mode_proxy_only_when_host_matches() {
		let cfg = proxy_cfg(vec!["cache.example.com", "*.proxy.local"], false);
		let mut headers = HeaderMap::new();
		headers.insert(axum::http::header::HOST, "cache.example.com".parse().unwrap());
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
}
