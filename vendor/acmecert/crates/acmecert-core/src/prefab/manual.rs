use crate::acme_dns01::AcmeDns01Challenge;
use crate::error::AnyError;
use crate::hook::DnsHook;
use std::future::Future;
use std::io;
use std::io::Write as _;
use std::pin::Pin;

/// Manual (interactive) DNS hook.
///
/// `present()` prints the DNS TXT record details and waits for the user to press Enter.
/// `cleanup()` is a no-op.
#[derive(Debug, Default, Clone, Copy)]
pub struct ManualHook;

impl DnsHook for ManualHook {
    fn present<'a>(
        &'a self,
        challenge: &'a AcmeDns01Challenge,
    ) -> Pin<Box<dyn Future<Output = Result<(), AnyError>> + Send + 'a>> {
        Box::pin(async move {
            println!("Create DNS TXT record:");
            println!("  Name : {}", challenge.record_fqdn);
            println!("  Value: {}", challenge.txt_value);
            println!("Then press Enter once the record is in place.");
            let _ = io::stdout().flush();

            tokio::task::spawn_blocking(|| {
                let mut line = String::new();
                let _ = io::stdin().read_line(&mut line);
            })
            .await
            .map_err(|e| Box::new(e) as AnyError)?;

            Ok(())
        })
    }

    fn cleanup<'a>(
        &'a self,
        _challenge: &'a AcmeDns01Challenge,
    ) -> Pin<Box<dyn Future<Output = Result<(), AnyError>> + Send + 'a>> {
        Box::pin(async move { Ok(()) })
    }
}
