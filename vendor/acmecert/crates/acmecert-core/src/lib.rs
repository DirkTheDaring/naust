//! `acmecert-core` provides the reusable core for the `acmecert` CLI.
//!
//! Goals:
//! - no dependency on environment variables
//! - no stdin/stdout interactions
//! - integration points via traits (e.g. DNS hooks)
//!
//! # API layers
//! - **Recommended (third-party use):** [`simple`] – accepts strings/paths, validates internally, and provides closure-based hook adapters.
//! - **Advanced:** [`api::run_issue`] / [`api::run_gen`] – typed options and output-neutral issuance/generation.
//! - **Integration point:** [`api::DnsHook`] – implement this trait (or use `simple::issue_async` / `simple::async_hook`).
//!
//! The crate root is intentionally kept lean; the curated, stable public surface for advanced usage lives in [`api`].
//!
//! # Stability
//! - The API in [`api`] is the **stable, curated surface** intended for downstream use.
//! - Other public modules (like [`types`], [`hook`], and [`prefab`]) are public primarily for documentation
//!   and power-users, but are allowed to evolve more quickly.
//!
//! # Features
//! - `format-json` / `format-yaml`: enable serde-based JSON/YAML formatting.
//!   Formatting helpers are still available without these features (fallback renderers).
//!
//! # Errors
//! - Most entry points return [`api::AppError`]. This is the primary error type to bubble up and log.
//! - [`api::CertificateProvisionError`] is the ACME/DNS-01 provisioning error wrapped by `AppError::Provision`.
//!
//! # Example
//! Prefer the simple facade for third-party use:
//! ```no_run
//! use acmecert_core::simple::issue_async;
//!
//! # async fn run() -> Result<(), acmecert_core::api::AppError> {
//! let issued = issue_async(
//!     "admin@example.com",
//!     ["example.com"],
//!     |ch| async move {
//!         // Create TXT record ch.record_fqdn with value ch.txt_value.
//!         Ok(())
//!     },
//!     |_ch| async move { Ok(()) },
//! ).await?;
//! // Persist however you like (files, stdout JSON/YAML, secrets manager, ...).
//! // Example: write PEM files into a directory:
//! let _paths = acmecert_core::simple::write_pem_dir("./acmecert-data/example.com", &issued)?;
//! # Ok(()) }
//! ```

mod acme_dns01;
pub mod api;
mod app;
pub mod error;
pub mod hook;
pub mod prefab;
pub mod simple;
pub mod types;
