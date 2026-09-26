pub mod admin;
pub mod auth_token;
pub mod catalog;
pub mod errors;
pub mod handlers;
pub mod referrers;
pub mod routing;
pub mod stream_guard;
pub mod tags;
// Temporary compatibility shim (removed with the Phase 2 crate split, ADR-010):
// the module moved to `crate::upload_lifecycle::state`.
pub use crate::upload_lifecycle::state as upload_state;
