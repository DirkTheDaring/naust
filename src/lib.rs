#![allow(clippy::all)]

pub mod app_state;
pub use app_state::{AppState, AuthMetrics, ProxyContext};

pub use registry_core::application;
pub mod audit;
pub mod auth;
pub use registry_core::blob_delete_safety;
pub use registry_core::blob_gc;
pub use registry_core::blob_ref_index;
pub mod cli;
pub mod config;
pub use registry_core::consistency;
pub use registry_core::fs_root_lock;
pub use registry_core::gc_service;
pub use registry_core::{ConsistencyCoordinator, GcRevalidationGuard, MutationGuard};
pub mod glob;
pub mod http_api;
pub mod ip_concurrency;
pub use registry_core::manifest_lifecycle;
pub use registry_core::manifest_refs;
pub use registry_core::membership_migration;
pub use registry_core::policy;
pub mod proxy;
pub mod rbac;
pub use registry_core::registry;
pub use registry_core::repository_membership_ledger;
pub mod request_routing;
pub mod robot_secrets;
pub(crate) mod runtime;
pub mod security;
pub use registry_core::storage;
pub mod storage_wiring;
pub mod supervisor;
pub mod task_supervisor;
pub mod token_rate_limit;
pub use registry_core::upload_coordinator;
pub use registry_core::upload_lifecycle;
pub use registry_core::upstream;

#[doc(hidden)]
pub use registry_core::test_support;

pub use registry_core::{impl_gc_storage_port, impl_storage_ports};

pub fn install_rustls_crypto_provider() {
    let provider = rustls::crypto::aws_lc_rs::default_provider();
    let _ = rustls::crypto::CryptoProvider::install_default(provider);
}
