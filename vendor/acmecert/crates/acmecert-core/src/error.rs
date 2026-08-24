use std::error::Error;

/// A generic error type for glue code and hook implementations.
///
/// This is intentionally broad: callers should treat it as an opaque error payload.
pub type AnyError = Box<dyn Error + Send + Sync + 'static>;
