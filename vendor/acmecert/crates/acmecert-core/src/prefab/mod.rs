//! Prefabricated, ready-to-use `DnsHook` implementations.
//!
//! These helpers are **optional conveniences** for consumers of `acmecert-core`.
//!
//! Notes:
//! - `ManualHook` performs stdout/stderr + stdin I/O.
//! - `ExecHook` runs a local process.
//! - `GandiLiveDnsHook` performs HTTP calls via `reqwest`.
//! - `IsponeHttpHook` performs HTTP calls via `reqwest`.
//!
//! The core library does not use these internally.

mod exec;
mod gandi;
mod http;
mod ispone;
mod manual;

pub use exec::ExecHook;
pub use gandi::GandiLiveDnsHook;
pub use ispone::IsponeHttpHook;
pub use manual::ManualHook;
