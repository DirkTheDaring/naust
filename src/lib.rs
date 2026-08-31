#![allow(clippy::all)]

pub mod app_state;
pub use app_state::{AppState, AuthMetrics, ProxyContext};

pub mod application;
pub mod audit;
pub mod auth;
pub mod blob_delete_safety;
pub mod blob_gc;
pub mod blob_ref_index;
pub mod cli;
pub mod config;
pub mod consistency;
pub use consistency::{ConsistencyCoordinator, GcRevalidationGuard, MutationGuard};
pub mod fs_root_lock;
pub mod gc_service;
pub mod glob;
pub mod http_api;
pub mod ip_concurrency;
pub mod manifest_lifecycle;
pub mod manifest_publication;
pub mod manifest_refs;
pub mod membership_migration;
pub mod proxy;
pub mod rbac;
pub mod registry;
pub mod repository_membership_ledger;
pub mod request_routing;
pub mod robot_secrets;
pub mod security;
pub mod storage;
pub mod supervisor;
pub mod task_supervisor;
pub mod token_rate_limit;
pub mod upload_coordinator;

#[doc(hidden)]
pub mod test_support;

pub fn install_rustls_crypto_provider() {
    let provider = rustls::crypto::aws_lc_rs::default_provider();
    let _ = rustls::crypto::CryptoProvider::install_default(provider);
}
