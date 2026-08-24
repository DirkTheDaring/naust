//! Stable public API surface.
//!
//! This module groups the items intended for downstream use and keeps them
//! decoupled from the internal module/file layout.

/// High-level entrypoints + primary error.
pub use crate::app::{run_gen, run_issue, validity_days, AppError, GenOptions, IssueOptions};

/// Error type produced by the ACME/DNS-01 engine (wrapped by `AppError::Provision`).
pub use crate::acme_dns01::CertificateProvisionError;

/// Hook integration point.
pub use crate::hook::DnsHook;

/// Strongly-typed inputs and helper types.
pub use crate::types::{AuthorizationHeader, DnsName, EmailAddress, OutputPaths, ProxyUrl};

/// Main domain model objects returned by issuance.
pub use crate::acme_dns01::{
    cert_validity_info, AcmeDns01Challenge, CertValidityInfo, IssuedCertificate,
    ProvisionedCertificate,
};

/// Propagation tuning.
pub use crate::acme_dns01::PropagationCheck;

/// Formatting helpers (feature-aware; fallbacks exist).
pub use crate::acme_dns01::{issued_to_json, issued_to_yaml};

/// “Plumbing” error type used by hooks/formatting glue.
pub use crate::error::AnyError;
