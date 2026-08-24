use crate::acme_dns01::AcmeDns01Challenge;
use crate::error::AnyError;
use std::future::Future;
use std::pin::Pin;

/// Integration point for creating and removing DNS TXT records for the ACME DNS-01 flow.
///
/// Implementations live outside `acmecert-core` (e.g. the CLI can offer interactive, exec, or
/// HTTP-bridge based implementations).
pub trait DnsHook: Send + Sync {
    /// Create (or ensure) the TXT record exists and wait until it is ready.
    fn present<'a>(
        &'a self,
        challenge: &'a AcmeDns01Challenge,
    ) -> Pin<Box<dyn Future<Output = Result<(), AnyError>> + Send + 'a>>;

    /// Remove the TXT record again.
    ///
    /// Called after a successful certificate issuance.
    fn cleanup<'a>(
        &'a self,
        challenge: &'a AcmeDns01Challenge,
    ) -> Pin<Box<dyn Future<Output = Result<(), AnyError>> + Send + 'a>>;
}
