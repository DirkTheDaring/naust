use crate::error::AnyError;
use crate::types::ProxyUrl;
use std::time::Duration;

pub(super) const DEFAULT_HTTP_TIMEOUT: Duration = Duration::from_secs(30);
pub(super) const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Build a reqwest client for prefab hooks.
///
/// This is meant to be called once per hook instance (so connections can be reused).
pub(super) fn build_http_client(
    proxy: Option<&ProxyUrl>,
    timeout: Duration,
    connect_timeout: Duration,
) -> Result<reqwest::Client, AnyError> {
    let mut builder = reqwest::Client::builder()
        .timeout(timeout)
        .connect_timeout(connect_timeout);

    if let Some(proxy) = proxy {
        let px = reqwest::Proxy::all(proxy.as_str()).map_err(|e| Box::new(e) as AnyError)?;
        builder = builder.proxy(px);
    }

    builder.build().map_err(|e| Box::new(e) as AnyError)
}

pub(super) fn json_string(input: &str) -> String {
    let mut out = String::with_capacity(input.len() + 2);
    out.push('"');
    for ch in input.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => {
                use std::fmt::Write as _;
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}
