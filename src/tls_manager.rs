//! TLS lifecycle manager (technical-debt remediation A2/R1, resolves KI-01).
//!
//! Two responsibilities the startup path never had:
//! - **Runtime renewal** for ACME-managed certificates: ACME provisioning used
//!   to run only at startup, so a long-lived server never picked up a renewed
//!   certificate. The manager re-invokes provisioning periodically (the ACME
//!   call itself renews only inside `renewal_window`).
//! - **Hot reload** for both certificate sources via
//!   `axum_server::tls_rustls::RustlsConfig::reload_from_pem_file` (shared
//!   handle; no rebind). Externally managed certs are watched by content
//!   fingerprint on a poll interval.
//!
//! Safety posture: a bad certificate NEVER degrades a running server — the
//! reload path validates first (parse + SAN preflight) and keeps serving the
//! old certificate on any failure. Fail-closed behavior exists only at
//! startup (`preflight_startup`), with a break-glass
//! `tls.acme.allow_san_mismatch` escape hatch.

use sha2::Digest as _;
use std::path::{Path, PathBuf};
use x509_parser::prelude::{FromDer, GeneralName, ParsedExtension, X509Certificate};

#[derive(Clone, Debug)]
pub struct CertSummary {
    pub sans: Vec<String>,
    pub not_after: String,
}

/// Parses the first certificate in a PEM file and extracts DNS SANs + expiry.
pub fn inspect_cert_pem(path: &Path) -> Result<CertSummary, String> {
    let pem_bytes =
        std::fs::read(path).map_err(|e| format!("read cert {}: {e}", path.display()))?;
    let (_, pem) = x509_parser::pem::parse_x509_pem(&pem_bytes)
        .map_err(|e| format!("parse PEM {}: {e}", path.display()))?;
    let (_, cert) = X509Certificate::from_der(&pem.contents)
        .map_err(|e| format!("parse certificate DER {}: {e}", path.display()))?;

    let mut sans = Vec::new();
    for ext in cert.extensions() {
        if let ParsedExtension::SubjectAlternativeName(san) = ext.parsed_extension() {
            for name in &san.general_names {
                match name {
                    GeneralName::DNSName(d) => sans.push(d.to_string()),
                    GeneralName::IPAddress(_) => {}
                    _ => {}
                }
            }
        }
    }
    Ok(CertSummary {
        sans,
        not_after: cert.validity().not_after.to_string(),
    })
}

/// Returns the configured names NOT covered by the certificate's DNS SANs.
/// Wildcard SANs (`*.example.com`) cover exactly one additional label.
pub fn uncovered_names(sans: &[String], names: &[String]) -> Vec<String> {
    names
        .iter()
        .filter(|name| {
            let name = name.to_ascii_lowercase();
            !sans.iter().any(|san| {
                let san = san.to_ascii_lowercase();
                if let Some(suffix) = san.strip_prefix("*.") {
                    match name.strip_suffix(suffix) {
                        Some(head) => {
                            head.ends_with('.') && head.matches('.').count() == 1 && head.len() > 1
                        }
                        None => false,
                    }
                } else {
                    san == name
                }
            })
        })
        .cloned()
        .collect()
}

/// Startup preflight for ACME-managed certificates: fail closed on SAN
/// mismatch unless the break-glass flag is set. Always logs SANs + expiry.
pub fn preflight_startup(
    cert_path: &Path,
    expected_names: &[String],
    allow_san_mismatch: bool,
) -> Result<(), String> {
    let summary = inspect_cert_pem(cert_path)?;
    tracing::info!(
        cert = %cert_path.display(),
        sans = ?summary.sans,
        not_after = %summary.not_after,
        "tls: certificate loaded"
    );
    let missing = uncovered_names(&summary.sans, expected_names);
    if missing.is_empty() {
        return Ok(());
    }
    if allow_san_mismatch {
        tracing::warn!(
            missing = ?missing,
            "tls: certificate SANs do not cover all configured names (allow_san_mismatch set — serving anyway)"
        );
        return Ok(());
    }
    Err(format!(
        "certificate {} does not cover configured names {:?} (SANs: {:?}); set tls.acme.allow_san_mismatch=true to override",
        cert_path.display(),
        missing,
        summary.sans
    ))
}

fn fingerprint(path: &Path) -> Option<[u8; 32]> {
    let bytes = std::fs::read(path).ok()?;
    Some(sha2::Sha256::digest(&bytes).into())
}

/// One watcher/renewal tick, factored for testability. `renew` runs first (ACME
/// mode; `None` for externally managed certs); a cert-content change then
/// triggers validation and, only on success, `reload`.
///
/// Returns `Ok(true)` when a reload happened.
pub async fn tick<Renew, RenewFut, Reload, ReloadFut>(
    cert_path: &Path,
    expected_names: Option<&[String]>,
    last_fingerprint: &mut Option<[u8; 32]>,
    renew: Option<Renew>,
    reload: Reload,
) -> Result<bool, String>
where
    Renew: FnOnce() -> RenewFut,
    RenewFut: std::future::Future<Output = Result<(), String>>,
    Reload: FnOnce() -> ReloadFut,
    ReloadFut: std::future::Future<Output = Result<(), String>>,
{
    if let Some(renew) = renew {
        if let Err(e) = renew().await {
            // Renewal failure is log-and-retry; the current cert keeps serving.
            tracing::warn!(error = %e, "tls: renewal attempt failed; will retry");
        }
    }

    let current = fingerprint(cert_path);
    if current.is_none() || current == *last_fingerprint {
        return Ok(false);
    }

    // Validate BEFORE swapping; on any failure keep serving the old cert.
    let summary = inspect_cert_pem(cert_path)?;
    if let Some(names) = expected_names {
        let missing = uncovered_names(&summary.sans, names);
        if !missing.is_empty() {
            return Err(format!(
                "refusing reload: renewed certificate does not cover {missing:?} (SANs: {:?})",
                summary.sans
            ));
        }
    }

    reload().await?;
    *last_fingerprint = current;
    tracing::info!(
        cert = %cert_path.display(),
        sans = ?summary.sans,
        not_after = %summary.not_after,
        "tls: certificate reloaded without restart"
    );
    Ok(true)
}

/// Watcher state shared across `spawn_loop` ticks.
pub struct TlsWatcher {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    /// `Some(names)` enforces SAN coverage on reload (ACME mode); `None`
    /// reloads with log-only inspection (externally managed certs).
    pub expected_names: Option<Vec<String>>,
    /// `Some(acme)` makes each tick attempt a renewal first (ACME mode).
    pub acme: Option<crate::config::AcmeConfig>,
    pub tls: axum_server::tls_rustls::RustlsConfig,
    last: tokio::sync::Mutex<Option<[u8; 32]>>,
}

impl TlsWatcher {
    pub fn new(
        cert_path: PathBuf,
        key_path: PathBuf,
        expected_names: Option<Vec<String>>,
        acme: Option<crate::config::AcmeConfig>,
        tls: axum_server::tls_rustls::RustlsConfig,
    ) -> Self {
        let last = tokio::sync::Mutex::new(fingerprint(&cert_path));
        Self {
            cert_path,
            key_path,
            expected_names,
            acme,
            tls,
            last,
        }
    }

    /// One renewal/watch/reload iteration; safe to call from a supervised loop.
    /// Never returns Err for "keep serving the old cert" situations — those are
    /// logged and swallowed so the supervised task does not count them as
    /// worker failures.
    pub async fn tick_once(&self) -> Result<(), String> {
        let mut last = self.last.lock().await;
        let renew = self.acme.as_ref().map(|acme| {
            let acme = acme.clone();
            move || async move {
                crate::supervisor::try_acme_renewal(&acme)
                    .await
                    .map_err(|e| e.to_string())
            }
        });
        let tls = self.tls.clone();
        let cert = self.cert_path.clone();
        let key = self.key_path.clone();
        let reload = move || async move {
            tls.reload_from_pem_file(cert, key)
                .await
                .map_err(|e| format!("reload_from_pem_file: {e}"))
        };
        match tick(
            &self.cert_path,
            self.expected_names.as_deref(),
            &mut last,
            renew,
            reload,
        )
        .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                tracing::warn!(error = %e, "tls: reload refused; continuing with current certificate");
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests;
