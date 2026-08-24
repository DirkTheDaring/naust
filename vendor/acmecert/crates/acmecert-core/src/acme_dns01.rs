use crate::error::AnyError;
use crate::hook::DnsHook;
use crate::types::OutputPaths;
use bytes::Bytes;
use http::Request;
use http_body_util::{BodyExt, Full};
use instant_acme::{
    Account, AuthorizationStatus, ChallengeType, Identifier, LetsEncrypt, NewAccount, NewOrder,
    OrderStatus,
};
use std::error::Error;
use std::fs;
use std::future::Future;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::info;
use x509_parser::prelude::*;

fn box_error<E>(e: E) -> AnyError
where
    E: Error + Send + Sync + 'static,
{
    Box::new(e)
}

fn msg_error(message: impl Into<String>) -> AnyError {
    box_error(std::io::Error::other(message.into()))
}

#[derive(Debug, Clone)]
pub struct AcmeProvisioningSettings {
    pub email: Option<String>,

    /// DNS names to include in the order / CSR.
    pub domain_names: Vec<String>,

    /// ACME directory URL. If unset, defaults to Let's Encrypt Production.
    pub directory_url: Option<String>,

    /// Public DNS propagation verification via Cloudflare DoH.
    pub propagation_check: PropagationCheck,

    /// Optional explicit HTTP proxy URL to use for all outbound HTTP(S).
    /// If unset, standard proxy environment variables / system proxy config apply.
    pub proxy: Option<String>,

    /// How long before expiry a certificate should be treated as needing renewal.
    pub renewal_window: Duration,
}

impl Default for AcmeProvisioningSettings {
    fn default() -> Self {
        Self {
            email: None,
            domain_names: Vec::new(),
            directory_url: None,
            propagation_check: PropagationCheck::default(),
            proxy: None,
            renewal_window: Duration::from_secs(30 * 24 * 60 * 60),
        }
    }
}

#[derive(Debug, Clone)]
pub enum PropagationCheck {
    #[allow(dead_code)]
    Disabled,
    CloudflareDoh {
        attempts: u32,
        sleep: Duration,
        /// If true, fail the issuance flow when propagation cannot be verified.
        ///
        /// If false, only logs a warning and proceeds (validation may still fail).
        strict: bool,
    },
}

impl Default for PropagationCheck {
    fn default() -> Self {
        Self::CloudflareDoh {
            attempts: 30,
            sleep: Duration::from_secs(10),
            strict: false,
        }
    }
}

impl PropagationCheck {
    pub fn disabled() -> Self {
        Self::Disabled
    }

    /// Helper for frontends: map a "disable propagation check" switch and a "strict" switch.
    ///
    /// If `disable_check` is true, strict mode is ignored.
    pub fn from_flags(disable_check: bool, strict: bool) -> Self {
        if disable_check {
            Self::Disabled
        } else {
            Self::default().with_strict(strict)
        }
    }

    pub fn cloudflare_doh(attempts: u32, sleep: Duration) -> Self {
        Self::CloudflareDoh {
            attempts,
            sleep,
            strict: false,
        }
    }

    pub fn cloudflare_doh_strict(attempts: u32, sleep: Duration) -> Self {
        Self::CloudflareDoh {
            attempts,
            sleep,
            strict: true,
        }
    }

    /// Enable/disable strict mode where supported.
    pub fn with_strict(self, strict: bool) -> Self {
        match self {
            Self::CloudflareDoh {
                attempts, sleep, ..
            } => Self::CloudflareDoh {
                attempts,
                sleep,
                strict,
            },
            other => other,
        }
    }
}

/// DNS-01 material required by the caller to create the `_acme-challenge` TXT record.
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
    any(feature = "format-json", feature = "format-yaml"),
    derive(serde::Serialize)
)]
pub struct AcmeDns01Challenge {
    pub record_fqdn: String,
    pub txt_value: String,
}

#[derive(Debug, Clone)]
pub struct ProvisionedCertificate {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    /// Challenges used during provisioning.
    ///
    /// Cleanup semantics:
    /// - If you call the higher-level APIs (`api::run_gen` / `api::run_issue` or the `simple` facade),
    ///   the library will attempt best-effort cleanup automatically.
    /// - This field still carries the challenges for audit/debugging and for callers that want to
    ///   implement their own cleanup or retries.
    pub challenges: Vec<AcmeDns01Challenge>,
}

/// Result of a successful ACME issuance.
///
/// This is output-neutral: it contains the issued certificate chain and private key
/// in PEM form, and can be persisted/serialized by the caller in any format.
#[derive(Debug, Clone)]
#[cfg_attr(
    any(feature = "format-json", feature = "format-yaml"),
    derive(serde::Serialize)
)]
pub struct IssuedCertificate {
    /// PEM-encoded certificate chain.
    pub cert_chain_pem: String,
    /// PEM-encoded private key.
    pub private_key_pem: String,
    /// Challenges used during issuance.
    ///
    /// Cleanup semantics:
    /// - High-level APIs attempt best-effort cleanup automatically.
    /// - Challenges are still returned for audit/debugging and custom retry/cleanup flows.
    pub challenges: Vec<AcmeDns01Challenge>,
}

#[derive(Debug, thiserror::Error)]
pub enum CertificateProvisionError {
    #[error("missing required email")]
    MissingEmail,

    #[error("no domain names provided")]
    MissingDomainNames,

    #[error("DNS-01 challenge setup/verification failed")]
    Dns01NotReady {
        challenges: Vec<AcmeDns01Challenge>,
        #[source]
        source: AnyError,
    },

    #[error("ACME certificate provisioning failed")]
    AcmeFailed {
        challenges: Vec<AcmeDns01Challenge>,
        #[source]
        source: AnyError,
    },
}

impl CertificateProvisionError {
    pub fn challenges(&self) -> &[AcmeDns01Challenge] {
        match self {
            CertificateProvisionError::MissingEmail
            | CertificateProvisionError::MissingDomainNames => &[],
            CertificateProvisionError::Dns01NotReady { challenges, .. }
            | CertificateProvisionError::AcmeFailed { challenges, .. } => challenges,
        }
    }
}

pub type Result<T> = std::result::Result<T, CertificateProvisionError>;

fn acme_failed(challenges: &[AcmeDns01Challenge], source: AnyError) -> CertificateProvisionError {
    CertificateProvisionError::AcmeFailed {
        challenges: challenges.to_vec(),
        source,
    }
}

fn dns01_not_ready(
    challenges: Vec<AcmeDns01Challenge>,
    source: AnyError,
) -> CertificateProvisionError {
    CertificateProvisionError::Dns01NotReady { challenges, source }
}

#[derive(Clone)]
struct ReqwestHttpClient {
    client: reqwest::Client,
}

impl instant_acme::HttpClient for ReqwestHttpClient {
    fn request(
        &self,
        req: Request<Full<Bytes>>,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = std::result::Result<instant_acme::BytesResponse, instant_acme::Error>,
                > + Send,
        >,
    > {
        let client = self.client.clone();

        Box::pin(async move {
            let (parts, body) = req.into_parts();

            let url = reqwest::Url::parse(&parts.uri.to_string())
                .map_err(|e| instant_acme::Error::Other(Box::new(e)))?;

            let collected = body
                .collect()
                .await
                .map_err(|e| instant_acme::Error::Other(Box::new(e)))?;
            let body_bytes = collected.to_bytes();

            let mut req_builder = client.request(parts.method, url);
            req_builder = req_builder.headers(parts.headers);
            let rsp = req_builder
                .body(body_bytes)
                .send()
                .await
                .map_err(|e| instant_acme::Error::Other(Box::new(e)))?;

            let status = rsp.status();
            let headers = rsp.headers().clone();
            let rsp_bytes = rsp
                .bytes()
                .await
                .map_err(|e| instant_acme::Error::Other(Box::new(e)))?;

            let mut http_rsp = http::Response::new(Full::new(rsp_bytes));
            *http_rsp.status_mut() = status;
            *http_rsp.headers_mut() = headers;

            Ok(instant_acme::BytesResponse::from(http_rsp))
        })
    }
}

/// Provision (or renew) a certificate using ACME DNS-01.
///
/// Boundary:
/// - This function computes the DNS-01 challenge(s) and waits until the caller confirms they are
///   "present and ready" via the `dns_ready` callback.
/// - It never creates or deletes DNS records.
/// - It returns the used challenge(s) so higher layers can attempt cleanup and callers can audit/debug.
pub async fn provision_certificate_with_dns01(
    settings: &AcmeProvisioningSettings,
    output: &OutputPaths,
    hook: &dyn DnsHook,
) -> Result<ProvisionedCertificate> {
    let cert_path = output.cert_path.clone();
    let key_path = output.key_path.clone();

    // 1) Reuse valid certs if present
    if cert_path.exists() && key_path.exists() {
        match check_cert_validity(&cert_path, settings.renewal_window) {
            Ok((true, expiry)) => {
                info!(
                    "TLS certificate found in data directory and is valid (expires on {}).",
                    expiry
                );
                return Ok(ProvisionedCertificate {
                    cert_path,
                    key_path,
                    challenges: Vec::new(),
                });
            }
            Ok((false, expiry)) => {
                info!(
                    "TLS certificate exists but is expired or expiring soon (expires on {}). Renewing...",
                    expiry
                );
            }
            Err(e) => {
                tracing::warn!(
                    "Failed to check existing certificate validity: {}. Renewing...",
                    e
                );
            }
        }
    }

    // 2) Issue a new certificate (output-neutral)
    let issued = issue_certificate_with_dns01(settings, hook).await?;

    // 3) Persist as PEM files (legacy behavior)
    write_atomic(&cert_path, issued.cert_chain_pem.as_bytes(), None)
        .map_err(|e| acme_failed(&issued.challenges, e))?;
    write_atomic(&key_path, issued.private_key_pem.as_bytes(), Some(0o600))
        .map_err(|e| acme_failed(&issued.challenges, e))?;

    info!(
        "Certificate provisioned successfully to cert={:?} key={:?}",
        cert_path, key_path
    );
    Ok(ProvisionedCertificate {
        cert_path,
        key_path,
        challenges: issued.challenges,
    })
}

/// Issue a certificate using ACME DNS-01.
///
/// This performs the full ACME flow but does not write any files.
///
/// The returned [`IssuedCertificate`] can be persisted/serialized by the caller.
pub async fn issue_certificate_with_dns01(
    settings: &AcmeProvisioningSettings,
    hook: &dyn DnsHook,
) -> Result<IssuedCertificate> {
    // Validate provisioning settings
    let email = match settings.email.as_deref() {
        Some(e) => e,
        None => return Err(CertificateProvisionError::MissingEmail),
    };
    if settings.domain_names.is_empty() {
        return Err(CertificateProvisionError::MissingDomainNames);
    }

    info!(
        "Issuing new ACME certificate for {:?} (email: {})...",
        settings.domain_names, email
    );

    // ACME Flow
    // Reqwest respects proxy env vars / system proxy config when built with the
    // `system-proxy` feature. If an explicit proxy is provided, use it.
    let mut http_builder = reqwest::Client::builder();
    if let Some(proxy_url) = settings.proxy.as_deref() {
        let proxy = reqwest::Proxy::all(proxy_url).map_err(|e| acme_failed(&[], box_error(e)))?;
        http_builder = http_builder.proxy(proxy);
    }

    let http_client = http_builder
        .build()
        .map_err(|e| acme_failed(&[], box_error(e)))?;

    let doh_client = http_client.clone();
    let mut used_challenges: Vec<AcmeDns01Challenge> = Vec::new();

    let directory_url = settings
        .directory_url
        .clone()
        .unwrap_or_else(|| LetsEncrypt::Production.url().to_owned());

    let (account, _credentials) = Account::create_with_http(
        &NewAccount {
            contact: &[&format!("mailto:{}", email)],
            terms_of_service_agreed: true,
            only_return_existing: false,
        },
        &directory_url,
        None,
        Box::new(ReqwestHttpClient {
            client: http_client.clone(),
        }),
    )
    .await
    .map_err(|e| acme_failed(&used_challenges, box_error(e)))?;

    let identifiers: Vec<Identifier> = settings
        .domain_names
        .iter()
        .cloned()
        .map(Identifier::Dns)
        .collect();

    let mut order = account
        .new_order(&NewOrder {
            identifiers: identifiers.as_slice(),
        })
        .await
        .map_err(|e| acme_failed(&used_challenges, box_error(e)))?;

    // Prepare & validate authorizations
    let authzs = order
        .authorizations()
        .await
        .map_err(|e| acme_failed(&used_challenges, box_error(e)))?;

    for authz in authzs {
        if matches!(authz.status, AuthorizationStatus::Valid) {
            continue;
        }

        let challenge = authz
            .challenges
            .into_iter()
            .find(|c| matches!(c.r#type, ChallengeType::Dns01))
            .ok_or_else(|| acme_failed(&used_challenges, msg_error("No DNS-01 challenge found")))?;

        let record_domain = dns01_record_domain_from_identifier(&authz.identifier)
            .map_err(|e| acme_failed(&used_challenges, e))?;
        let record_fqdn = format!("_acme-challenge.{record_domain}");
        let txt_value = order.key_authorization(&challenge).dns_value();

        let challenge_info = AcmeDns01Challenge {
            record_fqdn: record_fqdn.clone(),
            txt_value: txt_value.clone(),
        };
        used_challenges.push(challenge_info.clone());

        if let Err(e) = hook.present(&challenge_info).await {
            return Err(dns01_not_ready(used_challenges, e));
        }

        match settings.propagation_check {
            PropagationCheck::Disabled => {}
            PropagationCheck::CloudflareDoh {
                attempts,
                sleep,
                strict,
            } => {
                let ok = verify_propagation(&doh_client, &record_fqdn, &txt_value, attempts, sleep)
                    .await;
                if strict && !ok {
                    return Err(dns01_not_ready(
                        used_challenges,
                        msg_error(format!(
                            "DNS propagation not verified for '{record_fqdn}' after {attempts} attempts"
                        )),
                    ));
                }
            }
        }

        order
            .set_challenge_ready(&challenge.url)
            .await
            .map_err(|e| acme_failed(&used_challenges, box_error(e)))?;
    }

    wait_for_order_ready(&mut order, Duration::from_secs(2), 60)
        .await
        .map_err(|e| acme_failed(&used_challenges, e))?;

    let (csr_der, key_pem) = make_csr_and_private_key(&settings.domain_names)
        .map_err(|e| acme_failed(&used_challenges, e))?;

    order
        .finalize(&csr_der)
        .await
        .map_err(|e| acme_failed(&used_challenges, box_error(e)))?;

    let cert_chain_pem = wait_for_certificate_pem(&mut order, Duration::from_secs(2), 120)
        .await
        .map_err(|e| acme_failed(&used_challenges, e))?;

    Ok(IssuedCertificate {
        cert_chain_pem,
        private_key_pem: key_pem,
        challenges: used_challenges,
    })
}

pub fn issued_to_json(issued: &IssuedCertificate) -> std::result::Result<String, AnyError> {
    #[cfg(feature = "format-json")]
    {
        serde_json::to_string_pretty(issued).map_err(box_error)
    }

    #[cfg(not(feature = "format-json"))]
    {
        Ok(render_json_fallback(issued))
    }
}

pub fn issued_to_yaml(issued: &IssuedCertificate) -> std::result::Result<String, AnyError> {
    #[cfg(feature = "format-yaml")]
    {
        serde_yaml::to_string(issued).map_err(box_error)
    }

    #[cfg(not(feature = "format-yaml"))]
    {
        Ok(render_yaml_fallback(issued))
    }
}

#[cfg(not(feature = "format-json"))]
fn render_json_fallback(issued: &IssuedCertificate) -> String {
    let mut out = String::new();
    out.push_str("{\n");
    out.push_str("  \"cert_chain_pem\": ");
    push_json_string(&mut out, &issued.cert_chain_pem);
    out.push_str(",\n");
    out.push_str("  \"private_key_pem\": ");
    push_json_string(&mut out, &issued.private_key_pem);
    out.push_str(",\n");
    out.push_str("  \"challenges\": [\n");

    for (i, ch) in issued.challenges.iter().enumerate() {
        out.push_str("    { \"record_fqdn\": ");
        push_json_string(&mut out, &ch.record_fqdn);
        out.push_str(", \"txt_value\": ");
        push_json_string(&mut out, &ch.txt_value);
        out.push_str(" }");
        if i + 1 != issued.challenges.len() {
            out.push(',');
        }
        out.push('\n');
    }

    out.push_str("  ]\n");
    out.push_str("}\n");
    out
}

#[cfg(not(feature = "format-json"))]
fn push_json_string(out: &mut String, s: &str) {
    out.push('"');
    for ch in s.chars() {
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
}

#[cfg(not(feature = "format-yaml"))]
fn render_yaml_fallback(issued: &IssuedCertificate) -> String {
    let mut out = String::new();

    out.push_str("cert_chain_pem: |-\n");
    push_yaml_block(&mut out, &issued.cert_chain_pem);

    out.push_str("private_key_pem: |-\n");
    push_yaml_block(&mut out, &issued.private_key_pem);

    out.push_str("challenges:\n");
    if issued.challenges.is_empty() {
        out.push_str("  []\n");
        return out;
    }

    for ch in &issued.challenges {
        out.push_str("  - record_fqdn: ");
        push_yaml_quoted(&mut out, &ch.record_fqdn);
        out.push('\n');
        out.push_str("    txt_value: ");
        push_yaml_quoted(&mut out, &ch.txt_value);
        out.push('\n');
    }

    out
}

#[cfg(not(feature = "format-yaml"))]
fn push_yaml_block(out: &mut String, s: &str) {
    if s.is_empty() {
        out.push_str("  \n");
        return;
    }

    for line in s.split_inclusive('\n') {
        out.push_str("  ");
        out.push_str(line);
        if !line.ends_with('\n') {
            out.push('\n');
        }
    }
}

#[cfg(not(feature = "format-yaml"))]
fn push_yaml_quoted(out: &mut String, s: &str) {
    out.push('\'');
    for ch in s.chars() {
        if ch == '\'' {
            out.push_str("''");
        } else {
            out.push(ch);
        }
    }
    out.push('\'');
}

fn write_atomic(
    path: &Path,
    contents: &[u8],
    unix_mode: Option<u32>,
) -> std::result::Result<(), AnyError> {
    let parent = path
        .parent()
        .ok_or_else(|| msg_error(format!("output path has no parent directory: {path:?}")))?;

    fs::create_dir_all(parent)?;

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(box_error)?
        .as_nanos();

    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("output");

    let tmp_path = parent.join(format!(".{file_name}.tmp-{nanos}-{}", std::process::id()));

    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp_path)?;
    file.write_all(contents)?;
    let _ = file.sync_all();

    #[cfg(unix)]
    if let Some(mode) = unix_mode {
        use std::os::unix::fs::PermissionsExt;
        let perms = fs::Permissions::from_mode(mode);
        fs::set_permissions(&tmp_path, perms)?;
    }

    if let Err(e) = fs::rename(&tmp_path, path) {
        let _ = fs::remove_file(&tmp_path);
        return Err(box_error(e));
    }

    Ok(())
}

fn dns01_record_domain_from_identifier(
    identifier: &Identifier,
) -> std::result::Result<String, AnyError> {
    match identifier {
        Identifier::Dns(name) => {
            let base = name.strip_prefix("*.").unwrap_or(name);
            Ok(base.to_string())
        }
    }
}

async fn wait_for_order_ready(
    order: &mut instant_acme::Order,
    sleep: Duration,
    max_attempts: u32,
) -> std::result::Result<(), AnyError> {
    for _ in 0..max_attempts {
        order.refresh().await.map_err(box_error)?;
        let status = order.state().status;
        match status {
            OrderStatus::Ready => return Ok(()),
            OrderStatus::Invalid => return Err(msg_error("order became invalid")),
            OrderStatus::Pending | OrderStatus::Processing | OrderStatus::Valid => {
                tokio::time::sleep(sleep).await;
            }
        }
    }

    Err(msg_error("timed out waiting for order to become ready"))
}

async fn wait_for_certificate_pem(
    order: &mut instant_acme::Order,
    sleep: Duration,
    max_attempts: u32,
) -> std::result::Result<String, AnyError> {
    for _ in 0..max_attempts {
        match order.certificate().await.map_err(box_error)? {
            Some(pem) => return Ok(pem),
            None => tokio::time::sleep(sleep).await,
        }
    }

    Err(msg_error("timed out waiting for certificate"))
}

fn make_csr_and_private_key(
    domain_names: &[String],
) -> std::result::Result<(Vec<u8>, String), AnyError> {
    use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair};

    let mut params = CertificateParams::new(domain_names.to_vec()).map_err(box_error)?;

    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, domain_names[0].clone());
    params.distinguished_name = dn;

    let key_pair = KeyPair::generate().map_err(box_error)?;
    let key_pem = key_pair.serialize_pem();
    let csr = params.serialize_request(&key_pair).map_err(box_error)?;
    let csr_der = csr.der().to_vec();

    Ok((csr_der, key_pem))
}

async fn verify_propagation(
    client: &reqwest::Client,
    fqdn: &str,
    expected_value: &str,
    attempts: u32,
    sleep: Duration,
) -> bool {
    info!("Verifying propagation for {} via Cloudflare DoH...", fqdn);
    let url = format!(
        "https://cloudflare-dns.com/dns-query?name={}&type=TXT",
        fqdn
    );

    for i in 1..=attempts {
        match client
            .get(&url)
            .header("Accept", "application/dns-json")
            .send()
            .await
        {
            Ok(resp) => match resp.text().await {
                Ok(text) => {
                    let answers = extract_cloudflare_txt_answers(&text);
                    if answers.iter().any(|a| a.contains(expected_value)) {
                        info!(
                            "Propagation verified on attempt {}/{}: Record found.",
                            i, attempts
                        );
                        return true;
                    }
                }
                Err(e) => tracing::warn!("DoH response read failed: {}", e),
            },
            Err(e) => tracing::warn!("DoH query failed: {}", e),
        }

        info!(
            "Propagation check {}/{}: Record not yet visible. Waiting {}s...",
            i,
            attempts,
            sleep.as_secs()
        );
        tokio::time::sleep(sleep).await;
    }

    tracing::warn!(
        "Propagation verification timed out. Proceeding with validation likely to fail."
    );

    false
}

fn extract_cloudflare_txt_answers(json: &str) -> Vec<String> {
    // Cloudflare DoH JSON format includes TXT answers like:
    // {"Answer":[{"data":"\"<txt>\""}, ...]}
    // We avoid a full JSON parser to keep dependencies minimal.
    let mut out = Vec::new();
    let mut rest = json;

    while let Some(idx) = rest.find("\"data\"") {
        rest = &rest[idx + "\"data\"".len()..];

        let Some(colon) = rest.find(':') else {
            break;
        };
        rest = &rest[colon + 1..];

        // Skip whitespace
        rest = rest.trim_start();

        // We only care about string values.
        if !rest.starts_with('"') {
            continue;
        }

        let Some((raw, next)) = parse_json_string(rest) else {
            break;
        };
        rest = next;

        // TXT answers are often quoted like "<value>".
        // Unwrap one layer of quotes if present.
        let unwrapped = raw
            .strip_prefix('"')
            .and_then(|s| s.strip_suffix('"'))
            .unwrap_or(&raw)
            .to_string();

        out.push(unwrapped);
    }

    out
}

fn parse_json_string(input: &str) -> Option<(String, &str)> {
    // Parses a JSON string starting at the opening quote.
    // This is intentionally minimal (enough for Cloudflare DoH responses).
    let bytes = input.as_bytes();
    if bytes.first().copied()? != b'"' {
        return None;
    }

    let mut out = String::new();
    let mut i = 1;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => return Some((out, &input[i + 1..])),
            b'\\' => {
                i += 1;
                if i >= bytes.len() {
                    return None;
                }
                match bytes[i] {
                    b'"' => out.push('"'),
                    b'\\' => out.push('\\'),
                    b'/' => out.push('/'),
                    b'b' => out.push('\u{0008}'),
                    b'f' => out.push('\u{000C}'),
                    b'n' => out.push('\n'),
                    b'r' => out.push('\r'),
                    b't' => out.push('\t'),
                    b'u' => {
                        if i + 4 >= bytes.len() {
                            return None;
                        }
                        let hex = &input[i + 1..i + 5];
                        let code = u32::from_str_radix(hex, 16).ok()?;
                        out.push(char::from_u32(code)?);
                        i += 4;
                    }
                    other => out.push(other as char),
                }
            }
            other => out.push(other as char),
        }

        i += 1;
    }

    None
}

fn check_cert_validity(
    cert_path: &Path,
    renewal_window: Duration,
) -> std::result::Result<(bool, String), AnyError> {
    let info = cert_validity_info(cert_path)?;
    let window_secs: i64 = renewal_window.as_secs().min(i64::MAX as u64) as i64;
    let valid = info.not_after_ts > (info.now_ts + window_secs);
    Ok((valid, info.expiry_str))
}

#[derive(Debug, Clone)]
pub struct CertValidityInfo {
    pub not_after_ts: i64,
    pub now_ts: i64,
    pub days_remaining: i64,
    pub expiry_str: String,
}

pub fn cert_validity_info(cert_path: &Path) -> std::result::Result<CertValidityInfo, AnyError> {
    let cert_pem = fs::read_to_string(cert_path)?;
    let (_, pem) = parse_x509_pem(cert_pem.as_bytes())
        .map_err(|e| msg_error(format!("Failed to parse PEM: {:?}", e)))?;

    let cert = pem
        .parse_x509()
        .map_err(|_| msg_error("Failed to parse X.509 certificate"))?;

    let not_after = cert.validity().not_after;
    let not_after_ts = not_after.timestamp();
    let now_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(box_error)?
        .as_secs() as i64;

    let days_remaining = (not_after_ts - now_ts) / 86400;
    let expiry_str = format!("{} (in {} days)", not_after, days_remaining);

    Ok(CertValidityInfo {
        not_after_ts,
        now_ts,
        days_remaining,
        expiry_str,
    })
}
