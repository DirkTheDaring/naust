#![allow(clippy::result_large_err, clippy::too_many_arguments)]

pub mod app_state;
pub use app_state::{AppState, AuthMetrics, ProxyContext};

pub use naust_core::application;
pub mod audit;
pub mod auth;
pub use naust_core::blob_delete_safety;
pub use naust_core::blob_gc;
pub use naust_core::blob_ref_index;
pub use naust_core::cache_eviction;
pub mod cli;
pub mod config;
pub use naust_core::consistency;
pub use naust_core::fs_root_lock;
pub mod gc_admin;
pub use naust_core::gc_service;
pub use naust_core::{ConsistencyCoordinator, GcRevalidationGuard, MutationGuard};
pub mod glob;
pub mod http_api;
pub mod ip_concurrency;
pub use naust_core::manifest_lifecycle;
pub use naust_core::manifest_refs;
pub use naust_core::membership_migration;
pub use naust_core::policy;
pub mod proxy;
pub use naust_auth::rbac;
pub use naust_core::registry;
pub use naust_core::repository_membership_ledger;
pub mod request_routing;
pub use naust_auth::robot_secrets;
pub(crate) mod runtime;
pub use naust_auth::security;
pub use naust_core::storage;
pub mod storage_wiring;
pub mod supervisor;
pub mod task_supervisor;
pub mod tls_manager;
pub mod token_rate_limit;
pub mod token_service;
pub use naust_core::upload_coordinator;
pub use naust_core::upload_lifecycle;
pub use naust_core::upstream;

#[doc(hidden)]
pub use naust_core::test_support;

pub use naust_core::{impl_cache_eviction_port, impl_gc_storage_port, impl_storage_ports};

pub fn install_rustls_crypto_provider() {
    let provider = rustls::crypto::aws_lc_rs::default_provider();
    let _ = rustls::crypto::CryptoProvider::install_default(provider);
}
