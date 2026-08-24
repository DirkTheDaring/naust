//! Simple, third-party-friendly API.
//!
//! This module is intentionally “boring”: it accepts plain strings and paths,
//! performs validation using the stronger internal types, and then delegates to
//! the advanced API.
//!
//! # Example
//! ```no_run
//! use acmecert_core::simple::issue_async;
//! use std::path::PathBuf;
//!
//! # async fn run() -> Result<(), acmecert_core::api::AppError> {
//! let issued = issue_async(
//!     "admin@example.com",
//!     ["example.com"],
//!     |ch| async move {
//!         // Create TXT record:
//!         //   name  = ch.record_fqdn
//!         //   value = ch.txt_value
//!         Ok(())
//!     },
//!     |_ch| async move {
//!         // Remove TXT record.
//!         Ok(())
//!     },
//! ).await?;
//! let _paths = acmecert_core::simple::write_pem_dir(PathBuf::from("./acmecert-data/example.com"), &issued)?;
//! # Ok(()) }
//! ```

use crate::acme_dns01::{AcmeDns01Challenge, IssuedCertificate, ProvisionedCertificate};
use crate::app::{run_gen, run_issue, AppError};
use crate::error::AnyError;
use crate::hook::DnsHook;
use crate::types::{DnsName, EmailAddress, OutputPaths, ProxyUrl};
use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::time::Duration;

/// The future type used by hook callbacks in the simple API.
pub type HookFuture = Pin<Box<dyn Future<Output = Result<(), AnyError>> + Send + 'static>>;

/// Box a future into the `HookFuture` type.
pub fn boxed<F>(future: F) -> HookFuture
where
    F: Future<Output = Result<(), AnyError>> + Send + 'static,
{
    Box::pin(future)
}

/// Hook adapter for async closures.
///
/// This keeps call sites minimal by avoiding explicit boxing (`boxed(async move { ... })`).
pub struct AsyncFnHook<P, C> {
    present: P,
    cleanup: C,
}

/// Create an `AsyncFnHook`.
pub fn async_hook<P, PFut, C, CFut>(present: P, cleanup: C) -> AsyncFnHook<P, C>
where
    P: Fn(AcmeDns01Challenge) -> PFut + Send + Sync,
    PFut: Future<Output = Result<(), AnyError>> + Send + 'static,
    C: Fn(AcmeDns01Challenge) -> CFut + Send + Sync,
    CFut: Future<Output = Result<(), AnyError>> + Send + 'static,
{
    AsyncFnHook { present, cleanup }
}

impl<P, PFut, C, CFut> DnsHook for AsyncFnHook<P, C>
where
    P: Fn(AcmeDns01Challenge) -> PFut + Send + Sync,
    PFut: Future<Output = Result<(), AnyError>> + Send + 'static,
    C: Fn(AcmeDns01Challenge) -> CFut + Send + Sync,
    CFut: Future<Output = Result<(), AnyError>> + Send + 'static,
{
    fn present<'a>(
        &'a self,
        challenge: &'a AcmeDns01Challenge,
    ) -> Pin<Box<dyn Future<Output = Result<(), AnyError>> + Send + 'a>> {
        let owned = challenge.clone();
        Box::pin((self.present)(owned))
    }

    fn cleanup<'a>(
        &'a self,
        challenge: &'a AcmeDns01Challenge,
    ) -> Pin<Box<dyn Future<Output = Result<(), AnyError>> + Send + 'a>> {
        let owned = challenge.clone();
        Box::pin((self.cleanup)(owned))
    }
}

/// Convenience adapter to build a `DnsHook` from two closures.
///
/// This is meant to keep consumers from having to write a custom struct + impl.
pub struct FnHook<P, C> {
    present: P,
    cleanup: C,
}

/// Create a `FnHook`.
pub fn fn_hook<P, C>(present: P, cleanup: C) -> FnHook<P, C> {
    FnHook { present, cleanup }
}

impl<P, C> DnsHook for FnHook<P, C>
where
    P: Fn(AcmeDns01Challenge) -> HookFuture + Send + Sync,
    C: Fn(AcmeDns01Challenge) -> HookFuture + Send + Sync,
{
    fn present<'a>(
        &'a self,
        challenge: &'a AcmeDns01Challenge,
    ) -> Pin<Box<dyn Future<Output = Result<(), AnyError>> + Send + 'a>> {
        let owned = challenge.clone();
        (self.present)(owned)
    }

    fn cleanup<'a>(
        &'a self,
        challenge: &'a AcmeDns01Challenge,
    ) -> Pin<Box<dyn Future<Output = Result<(), AnyError>> + Send + 'a>> {
        let owned = challenge.clone();
        (self.cleanup)(owned)
    }
}

/// Required inputs for output-neutral issuance.
#[derive(Debug, Clone)]
pub struct IssueRequest {
    pub email: String,
    pub names: Vec<String>,
}

impl IssueRequest {
    pub fn new<E, N, S>(email: E, names: N) -> Self
    where
        E: Into<String>,
        N: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            email: email.into(),
            names: names.into_iter().map(Into::into).collect(),
        }
    }
}

/// Optional knobs for output-neutral issuance.
#[derive(Debug, Clone, Default)]
pub struct IssueOptions {
    pub allow_first_wildcard: bool,
    pub proxy: Option<String>,
    pub propagation_check: crate::acme_dns01::PropagationCheck,
}

/// Issue a certificate and return it in-memory.
///
/// This is the recommended KISS entry point for third-party usage.
pub async fn issue_with(
    req: IssueRequest,
    opts: IssueOptions,
    hook: &dyn DnsHook,
) -> Result<IssuedCertificate, AppError> {
    let IssueOptions {
        allow_first_wildcard,
        proxy,
        propagation_check,
    } = opts;

    let (email, names, proxy) = parse_common(&req.email, req.names, proxy)?;

    run_issue(
        crate::app::IssueOptions::new(email, names)
            .allow_first_wildcard(allow_first_wildcard)
            .proxy(proxy)
            .propagation_check(propagation_check),
        hook,
    )
    .await
}

/// Convenience wrapper: `issue_with(...)`, but accept async closures directly.
pub async fn issue_with_async<P, PFut, C, CFut>(
    req: IssueRequest,
    opts: IssueOptions,
    present: P,
    cleanup: C,
) -> Result<IssuedCertificate, AppError>
where
    P: Fn(AcmeDns01Challenge) -> PFut + Send + Sync,
    PFut: Future<Output = Result<(), AnyError>> + Send + 'static,
    C: Fn(AcmeDns01Challenge) -> CFut + Send + Sync,
    CFut: Future<Output = Result<(), AnyError>> + Send + 'static,
{
    let hook = async_hook(present, cleanup);
    issue_with(req, opts, &hook).await
}

/// KISS entry point: issue a certificate with default options.
pub async fn issue<E, N, S>(
    email: E,
    names: N,
    hook: &dyn DnsHook,
) -> Result<IssuedCertificate, AppError>
where
    E: Into<String>,
    N: IntoIterator<Item = S>,
    S: Into<String>,
{
    issue_with(
        IssueRequest::new(email, names),
        IssueOptions::default(),
        hook,
    )
    .await
}

/// Ultra-KISS entry point: issue a certificate using async closures directly.
///
/// This avoids the extra `let hook = async_hook(...);` binding at the call site.
pub async fn issue_async<E, N, S, P, PFut, C, CFut>(
    email: E,
    names: N,
    present: P,
    cleanup: C,
) -> Result<IssuedCertificate, AppError>
where
    E: Into<String>,
    N: IntoIterator<Item = S>,
    S: Into<String>,
    P: Fn(AcmeDns01Challenge) -> PFut + Send + Sync,
    PFut: Future<Output = Result<(), AnyError>> + Send + 'static,
    C: Fn(AcmeDns01Challenge) -> CFut + Send + Sync,
    CFut: Future<Output = Result<(), AnyError>> + Send + 'static,
{
    let hook = async_hook(present, cleanup);
    issue(email, names, &hook).await
}

/// Required inputs for generating PEM files into an output directory.
///
/// This is a convenience wrapper around the advanced `run_gen` API.
///
/// Behavior:
/// - If `cert.pem` + `key.pem` already exist and the certificate is still valid beyond
///   `renewal_window`, no new certificate is created.
/// - Otherwise, a new certificate is issued and written to the directory.
#[derive(Debug, Clone)]
pub struct PemDirRequest {
    pub email: String,
    pub names: Vec<String>,
    pub output_dir: PathBuf,
}

impl PemDirRequest {
    pub fn new<E, N, S>(email: E, names: N, output_dir: impl Into<PathBuf>) -> Self
    where
        E: Into<String>,
        N: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            email: email.into(),
            names: names.into_iter().map(Into::into).collect(),
            output_dir: output_dir.into(),
        }
    }
}

/// Optional knobs for directory-based PEM generation.
#[derive(Debug, Clone)]
pub struct PemDirOptions {
    pub allow_first_wildcard: bool,
    pub proxy: Option<String>,
    pub propagation_check: crate::acme_dns01::PropagationCheck,
    pub renewal_window: Duration,
}

impl Default for PemDirOptions {
    fn default() -> Self {
        Self {
            allow_first_wildcard: false,
            proxy: None,
            propagation_check: crate::acme_dns01::PropagationCheck::default(),
            renewal_window: Duration::from_secs(30 * 24 * 60 * 60),
        }
    }
}

/// Generate `cert.pem` + `key.pem` into an output directory (with reuse/renewal).
pub async fn generate_pem_dir(
    req: PemDirRequest,
    opts: PemDirOptions,
    hook: &dyn DnsHook,
) -> Result<ProvisionedCertificate, AppError> {
    let PemDirOptions {
        allow_first_wildcard,
        proxy,
        propagation_check,
        renewal_window,
    } = opts;

    let (email, names, proxy) = parse_common(&req.email, req.names, proxy)?;

    run_gen(
        crate::app::GenOptions::new(email, names, OutputPaths::in_dir(req.output_dir))
            .allow_first_wildcard(allow_first_wildcard)
            .proxy(proxy)
            .propagation_check(propagation_check)
            .renewal_window(renewal_window),
        hook,
    )
    .await
}

/// Convenience wrapper: `generate_pem_dir(...)`, but accept async closures directly.
pub async fn generate_pem_dir_async<P, PFut, C, CFut>(
    req: PemDirRequest,
    opts: PemDirOptions,
    present: P,
    cleanup: C,
) -> Result<ProvisionedCertificate, AppError>
where
    P: Fn(AcmeDns01Challenge) -> PFut + Send + Sync,
    PFut: Future<Output = Result<(), AnyError>> + Send + 'static,
    C: Fn(AcmeDns01Challenge) -> CFut + Send + Sync,
    CFut: Future<Output = Result<(), AnyError>> + Send + 'static,
{
    let hook = async_hook(present, cleanup);
    generate_pem_dir(req, opts, &hook).await
}

/// KISS entry point: generate `cert.pem` + `key.pem` into a directory using default options.
pub async fn generate_pem_dir_simple<E, N, S>(
    email: E,
    names: N,
    output_dir: impl Into<PathBuf>,
    hook: &dyn DnsHook,
) -> Result<ProvisionedCertificate, AppError>
where
    E: Into<String>,
    N: IntoIterator<Item = S>,
    S: Into<String>,
{
    generate_pem_dir(
        PemDirRequest::new(email, names, output_dir),
        PemDirOptions::default(),
        hook,
    )
    .await
}

/// Ultra-KISS entry point: generate PEM files using async closures directly.
///
/// This avoids the extra `let hook = async_hook(...);` binding at the call site.
pub async fn generate_pem_dir_simple_async<E, N, S, P, PFut, C, CFut>(
    email: E,
    names: N,
    output_dir: impl Into<PathBuf>,
    present: P,
    cleanup: C,
) -> Result<ProvisionedCertificate, AppError>
where
    E: Into<String>,
    N: IntoIterator<Item = S>,
    S: Into<String>,
    P: Fn(AcmeDns01Challenge) -> PFut + Send + Sync,
    PFut: Future<Output = Result<(), AnyError>> + Send + 'static,
    C: Fn(AcmeDns01Challenge) -> CFut + Send + Sync,
    CFut: Future<Output = Result<(), AnyError>> + Send + 'static,
{
    let hook = async_hook(present, cleanup);
    generate_pem_dir_simple(email, names, output_dir, &hook).await
}

/// Persist an issued certificate as `cert.pem` + `key.pem` inside `dir`.
///
/// Returns the written paths.
pub fn write_pem_dir(
    dir: impl Into<PathBuf>,
    issued: &IssuedCertificate,
) -> Result<OutputPaths, AppError> {
    let output = OutputPaths::in_dir(dir.into());
    write_pem_files(&output, issued)?;
    Ok(output)
}

/// Persist an issued certificate to explicit output paths.
pub fn write_pem_files(output: &OutputPaths, issued: &IssuedCertificate) -> Result<(), AppError> {
    // Mirror the behavior of the existing atomic writer.
    write_atomic(&output.cert_path, issued.cert_chain_pem.as_bytes(), None)?;
    write_atomic(
        &output.key_path,
        issued.private_key_pem.as_bytes(),
        Some(0o600),
    )?;
    Ok(())
}

pub fn to_json(issued: &IssuedCertificate) -> Result<String, AppError> {
    crate::acme_dns01::issued_to_json(issued).map_err(AppError::Other)
}

pub fn to_yaml(issued: &IssuedCertificate) -> Result<String, AppError> {
    crate::acme_dns01::issued_to_yaml(issued).map_err(AppError::Other)
}

fn write_atomic(
    path: &std::path::Path,
    contents: &[u8],
    unix_mode: Option<u32>,
) -> Result<(), AppError> {
    // Keep persistence helpers local to the simple API.
    let parent = path.parent().ok_or_else(|| {
        io::Error::other(format!("output path has no parent directory: {path:?}"))
    })?;

    std::fs::create_dir_all(parent)?;

    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| io::Error::other(format!("failed to read system time: {e}")))?
        .as_nanos();

    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("output");

    let tmp_path = parent.join(format!(".{file_name}.tmp-{nanos}-{}", std::process::id()));

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp_path)?;
    use std::io::Write;
    file.write_all(contents)?;
    let _ = file.sync_all();

    #[cfg(unix)]
    if let Some(mode) = unix_mode {
        use std::os::unix::fs::PermissionsExt;
        let perms = std::fs::Permissions::from_mode(mode);
        std::fs::set_permissions(&tmp_path, perms)?;
    }

    if let Err(e) = std::fs::rename(&tmp_path, path) {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(io::Error::other(e.to_string()).into());
    }

    Ok(())
}

fn parse_or_invalid<T>(label: &'static str, input: &str) -> Result<T, AppError>
where
    T: std::str::FromStr<Err = String>,
{
    input.parse::<T>().map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid {label}: {e}"),
        )
        .into()
    })
}

fn parse_common(
    email: &str,
    names: Vec<String>,
    proxy: Option<String>,
) -> Result<(EmailAddress, Vec<DnsName>, Option<ProxyUrl>), AppError> {
    let email = parse_or_invalid::<EmailAddress>("email", email)?;

    let mut parsed_names: Vec<DnsName> = Vec::with_capacity(names.len());
    for raw in names {
        parsed_names.push(parse_or_invalid::<DnsName>("DNS name", &raw)?);
    }

    let proxy: Option<ProxyUrl> = match proxy {
        Some(p) => Some(parse_or_invalid::<ProxyUrl>("proxy URL", &p)?),
        None => None,
    };

    Ok((email, parsed_names, proxy))
}
