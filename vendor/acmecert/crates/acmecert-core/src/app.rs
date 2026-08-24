use crate::acme_dns01;
use crate::error::AnyError;
use crate::hook::DnsHook;
use crate::types::{DnsName, EmailAddress, OutputPaths, ProxyUrl};
use std::fs;
use std::io;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(transparent)]
    Provision(#[from] acme_dns01::CertificateProvisionError),

    /// A non-I/O error outside of ACME provisioning.
    ///
    /// This variant is intentionally generic: it is used for plumbing errors in helper APIs
    /// (e.g. parsing/formatting helpers) and should generally be logged and bubbled up.
    #[error("internal error")]
    Other(#[source] AnyError),
}

impl From<AnyError> for AppError {
    fn from(value: AnyError) -> Self {
        AppError::Other(value)
    }
}

fn parent_dir(output: &OutputPaths) -> Option<&Path> {
    output.cert_path.parent()
}

#[derive(Debug, Clone)]
pub struct GenOptions {
    allow_first_wildcard: bool,
    email: EmailAddress,
    names: Vec<DnsName>,
    output: OutputPaths,
    proxy: Option<ProxyUrl>,
    propagation_check: acme_dns01::PropagationCheck,
    renewal_window: Duration,
}

/// Advanced, output-neutral issuance options.
///
/// This drives the ACME flow and returns an in-memory certificate result.
/// Persistence/serialization is left to the caller.
#[derive(Debug, Clone)]
pub struct IssueOptions {
    allow_first_wildcard: bool,
    email: EmailAddress,
    names: Vec<DnsName>,
    proxy: Option<ProxyUrl>,
    propagation_check: acme_dns01::PropagationCheck,
}

impl GenOptions {
    pub fn new(email: EmailAddress, names: Vec<DnsName>, output: OutputPaths) -> Self {
        Self {
            allow_first_wildcard: false,
            email,
            names,
            output,
            proxy: None,
            propagation_check: acme_dns01::PropagationCheck::default(),
            renewal_window: Duration::from_secs(30 * 24 * 60 * 60),
        }
    }

    pub fn allow_first_wildcard(mut self, allow: bool) -> Self {
        self.allow_first_wildcard = allow;
        self
    }

    pub fn proxy(mut self, proxy: Option<ProxyUrl>) -> Self {
        self.proxy = proxy;
        self
    }

    pub fn propagation_check(mut self, propagation_check: acme_dns01::PropagationCheck) -> Self {
        self.propagation_check = propagation_check;
        self
    }

    pub fn renewal_window(mut self, renewal_window: Duration) -> Self {
        self.renewal_window = renewal_window;
        self
    }
}

impl IssueOptions {
    pub fn new(email: EmailAddress, names: Vec<DnsName>) -> Self {
        Self {
            allow_first_wildcard: false,
            email,
            names,
            proxy: None,
            propagation_check: acme_dns01::PropagationCheck::default(),
        }
    }

    pub fn allow_first_wildcard(mut self, allow: bool) -> Self {
        self.allow_first_wildcard = allow;
        self
    }

    pub fn proxy(mut self, proxy: Option<ProxyUrl>) -> Self {
        self.proxy = proxy;
        self
    }

    pub fn propagation_check(mut self, propagation_check: acme_dns01::PropagationCheck) -> Self {
        self.propagation_check = propagation_check;
        self
    }
}

pub async fn run_gen(
    opts: GenOptions,
    hook: &dyn DnsHook,
) -> Result<acme_dns01::ProvisionedCertificate, AppError> {
    validate_names(opts.allow_first_wildcard, &opts.names)?;

    if let Some(dir) = parent_dir(&opts.output) {
        fs::create_dir_all(dir).map_err(|e| {
            io::Error::other(format!("failed to create output directory {dir:?}: {e}"))
        })?;
        ensure_dir_writable(dir).map_err(io::Error::other)?;
    }

    let settings = build_settings(
        &opts.email,
        &opts.names,
        opts.proxy.as_ref(),
        opts.propagation_check.clone(),
        Some(opts.renewal_window),
    );

    let cert =
        match acme_dns01::provision_certificate_with_dns01(&settings, &opts.output, hook).await {
            Ok(v) => v,
            Err(e) => {
                cleanup_best_effort(hook, e.challenges()).await;
                return Err(AppError::Provision(e));
            }
        };

    cleanup_best_effort(hook, &cert.challenges).await;
    Ok(cert)
}

/// Issue a certificate (output-neutral) using the ACME DNS-01 flow.
///
/// This does not read or write any files.
pub async fn run_issue(
    opts: IssueOptions,
    hook: &dyn DnsHook,
) -> Result<acme_dns01::IssuedCertificate, AppError> {
    validate_names(opts.allow_first_wildcard, &opts.names)?;

    let settings = build_settings(
        &opts.email,
        &opts.names,
        opts.proxy.as_ref(),
        opts.propagation_check.clone(),
        None,
    );

    let issued = match acme_dns01::issue_certificate_with_dns01(&settings, hook).await {
        Ok(v) => v,
        Err(e) => {
            cleanup_best_effort(hook, e.challenges()).await;
            return Err(AppError::Provision(e));
        }
    };

    cleanup_best_effort(hook, &issued.challenges).await;
    Ok(issued)
}

async fn cleanup_best_effort(hook: &dyn DnsHook, challenges: &[acme_dns01::AcmeDns01Challenge]) {
    for ch in challenges {
        if let Err(e) = hook.cleanup(ch).await {
            tracing::warn!("DNS hook cleanup failed: {e}");
        }
    }
}

fn build_settings(
    email: &EmailAddress,
    names: &[DnsName],
    proxy: Option<&ProxyUrl>,
    propagation_check: acme_dns01::PropagationCheck,
    renewal_window: Option<Duration>,
) -> acme_dns01::AcmeProvisioningSettings {
    let mut settings = acme_dns01::AcmeProvisioningSettings {
        email: Some(email.as_str().to_string()),
        domain_names: names.iter().map(|n| n.to_string()).collect(),
        proxy: proxy.map(|p| p.as_str().to_string()),
        propagation_check,
        ..Default::default()
    };

    if let Some(window) = renewal_window {
        settings.renewal_window = window;
    }

    settings
}

pub fn validity_days(cert_path: &Path) -> Result<i64, AppError> {
    if !cert_path.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("certificate not found at {cert_path:?}"),
        )
        .into());
    }

    let info = acme_dns01::cert_validity_info(cert_path).map_err(AppError::Other)?;
    Ok(info.days_remaining)
}

fn validate_names(allow_first_wildcard: bool, names: &[DnsName]) -> Result<(), AppError> {
    if names.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "at least one DNS name is required",
        )
        .into());
    }

    if !allow_first_wildcard {
        if let Some(first) = names.first() {
            if first.is_wildcard() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "first DNS name must not be a wildcard",
                )
                .into());
            }
        }
    }

    Ok(())
}

fn ensure_dir_writable(dir: &Path) -> Result<(), String> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| format!("failed to read system time: {e}"))?
        .as_nanos();

    let filename = format!(".acmecert-write-test-{nanos}-{}", std::process::id());
    let probe_path = dir.join(filename);

    match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe_path)
    {
        Ok(_) => {
            let _ = fs::remove_file(&probe_path);
            Ok(())
        }
        Err(e) => Err(format!("output directory is not writable: {e}")),
    }
}
