use std::fmt;
use std::str::FromStr;

use std::path::PathBuf;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DnsName(String);

impl DnsName {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn is_wildcard(&self) -> bool {
        self.0.starts_with("*.")
    }

    pub fn base_domain(&self) -> &str {
        self.0.strip_prefix("*.").unwrap_or(&self.0)
    }
}

impl fmt::Display for DnsName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for DnsName {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        parse_dns_name(input).map(DnsName)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EmailAddress(String);

impl EmailAddress {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for EmailAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for EmailAddress {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let input = input.trim();
        if input.is_empty() {
            return Err("email must not be empty".to_string());
        }
        // Lightweight validation: enough to catch common mistakes without rejecting valid addresses.
        if !input.contains('@') || input.starts_with('@') || input.ends_with('@') {
            return Err("email must contain a single '@' separator".to_string());
        }
        Ok(EmailAddress(input.to_string()))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AuthorizationHeader(String);

impl AuthorizationHeader {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Helper: accept a raw token and convert it into `Bearer <token>`.
    pub fn bearer_token(token: &str) -> Result<Self, String> {
        let token = token.trim();
        if token.is_empty() {
            return Err("token must not be empty".to_string());
        }
        Ok(Self(format!("Bearer {token}")))
    }

    /// Helper: Accept either a raw bearer token or a full Authorization header value.
    ///
    /// If the string contains a space, it is treated as a full header value.
    /// Otherwise it is treated as a raw token and converted into `Bearer <token>`.
    pub fn from_token_or_header_value(input: &str) -> Result<Self, String> {
        let input = input.trim();
        if input.is_empty() {
            return Err("authorization must not be empty".to_string());
        }

        if input.contains(' ') {
            input.parse::<AuthorizationHeader>()
        } else {
            AuthorizationHeader::bearer_token(input)
        }
    }
}

impl fmt::Display for AuthorizationHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for AuthorizationHeader {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let input = input.trim();
        if input.is_empty() {
            return Err("authorization header must not be empty".to_string());
        }
        Ok(Self(input.to_string()))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ProxyUrl(url::Url);

impl ProxyUrl {
    pub fn as_url(&self) -> &url::Url {
        &self.0
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl fmt::Display for ProxyUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.as_str().fmt(f)
    }
}

impl FromStr for ProxyUrl {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let url = url::Url::parse(input).map_err(|e| format!("invalid proxy URL: {e}"))?;
        match url.scheme() {
            "http" | "https" | "socks5" | "socks5h" => Ok(ProxyUrl(url)),
            other => Err(format!(
                "unsupported proxy URL scheme '{other}' (use http/https/socks5/socks5h)"
            )),
        }
    }
}

fn parse_dns_name(input: &str) -> Result<String, String> {
    if input.is_empty() {
        return Err("DNS name must not be empty".to_string());
    }

    if !input.is_ascii() {
        return Err("DNS name must be ASCII (use punycode for IDNs)".to_string());
    }

    if let Some(remainder) = input.strip_prefix("*.") {
        validate_domain_name(remainder)?;

        if !remainder.contains('.') {
            return Err(
                "Wildcard DNS name must be like '*.example.com' (at least 2 labels after '*.')"
                    .to_string(),
            );
        }

        return Ok(input.to_string());
    }

    if input.contains('*') {
        return Err("Wildcard '*' is only allowed as a leading '*.'".to_string());
    }

    validate_domain_name(input)?;
    Ok(input.to_string())
}

#[derive(Debug, Clone)]
pub struct OutputPaths {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
}

impl OutputPaths {
    pub fn in_dir(dir: PathBuf) -> Self {
        Self {
            cert_path: dir.join("cert.pem"),
            key_path: dir.join("key.pem"),
        }
    }
}

fn validate_domain_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("DNS name must not be empty".to_string());
    }

    if name.len() > 253 {
        return Err("DNS name is too long (max 253 characters)".to_string());
    }

    if name.starts_with('.') || name.ends_with('.') {
        return Err("DNS name must not start or end with '.'".to_string());
    }

    if name.contains("..") {
        return Err("DNS name must not contain empty labels ('..')".to_string());
    }

    for label in name.split('.') {
        validate_dns_label(label)?;
    }

    Ok(())
}

fn validate_dns_label(label: &str) -> Result<(), String> {
    if label.is_empty() {
        return Err("DNS label must not be empty".to_string());
    }

    if label.len() > 63 {
        return Err(format!(
            "DNS label '{label}' is too long (max 63 characters)"
        ));
    }

    if label.starts_with('-') || label.ends_with('-') {
        return Err(format!(
            "DNS label '{label}' must not start or end with '-'"
        ));
    }

    for ch in label.chars() {
        let ok = ch.is_ascii_alphanumeric() || ch == '-';
        if !ok {
            return Err(format!(
                "DNS label '{label}' contains invalid character '{ch}' (allowed: a-z, 0-9, '-')"
            ));
        }
    }

    Ok(())
}
