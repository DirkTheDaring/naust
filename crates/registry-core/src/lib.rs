//! Registry-object primitives (ADR-010).
//!
//! Everything needed to implement an OCI registry on top of pluggable storage:
//! value types, capability ports, domain engines, and transport-neutral
//! application services. Deliberately free of HTTP, auth, and server
//! configuration; composition roots map their config into `policy` types and
//! implement `upstream::UpstreamFetcher` for pull-through setups.
#![allow(clippy::all)]

pub mod application;
pub mod blob_delete_safety;
pub mod blob_gc;
pub mod blob_ref_index;
pub mod consistency;
pub use consistency::{ConsistencyCoordinator, GcRevalidationGuard, MutationGuard};
pub mod fs_root_lock;
pub mod gc_service;
pub mod manifest_lifecycle;
pub mod manifest_refs;
pub mod membership_migration;
pub mod policy;
pub mod registry;
pub mod repository_membership_ledger;
pub mod storage;
pub mod upload_coordinator;
pub mod upload_lifecycle;
pub mod upstream;

#[doc(hidden)]
pub mod test_support;
