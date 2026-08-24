use super::http::{build_http_client, json_string, DEFAULT_CONNECT_TIMEOUT, DEFAULT_HTTP_TIMEOUT};
use crate::acme_dns01::AcmeDns01Challenge;
use crate::error::AnyError;
use crate::hook::DnsHook;
use crate::types::ProxyUrl;
use std::future::Future;
use std::io;
use std::pin::Pin;

/// Gandi LiveDNS API hook.
///
/// This implements the *shape* of a Gandi integration, modeled after the documentation example.
/// It expects `zone` to be provided explicitly.
#[derive(Debug, Clone)]
pub struct GandiLiveDnsHook {
    client: reqwest::Client,
    api_key: String,
    zone: String,
    ttl: u32,
}

impl GandiLiveDnsHook {
    pub fn new(
        api_key: String,
        zone: String,
        ttl: u32,
        proxy: Option<ProxyUrl>,
    ) -> Result<Self, AnyError> {
        let client = build_http_client(
            proxy.as_ref(),
            DEFAULT_HTTP_TIMEOUT,
            DEFAULT_CONNECT_TIMEOUT,
        )?;

        Ok(Self {
            client,
            api_key,
            zone,
            ttl,
        })
    }
}

impl DnsHook for GandiLiveDnsHook {
    fn present<'a>(
        &'a self,
        challenge: &'a AcmeDns01Challenge,
    ) -> Pin<Box<dyn Future<Output = Result<(), AnyError>> + Send + 'a>> {
        Box::pin(async move {
            let record_name = record_name_relative_to_zone(&challenge.record_fqdn, &self.zone)
                .ok_or_else(|| {
                    Box::new(io::Error::other(format!(
                        "record_fqdn '{}' is not within zone '{}'",
                        challenge.record_fqdn, self.zone
                    ))) as AnyError
                })?;

            let url = format!(
                "https://api.gandi.net/v5/livedns/domains/{}/records/{}/TXT",
                self.zone, record_name
            );

            let body = format!(
                "{{\"rrset_ttl\":{},\"rrset_values\":[{}]}}",
                self.ttl,
                json_string(&challenge.txt_value)
            );

            let rsp = self
                .client
                .put(&url)
                .bearer_auth(&self.api_key)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body)
                .send()
                .await
                .map_err(|e| Box::new(e) as AnyError)?;

            if !rsp.status().is_success() {
                let status = rsp.status();
                let body = rsp.text().await.unwrap_or_default();
                return Err(Box::new(io::Error::other(format!(
                    "Gandi present failed: HTTP {status}: {body}"
                ))) as AnyError);
            }

            Ok(())
        })
    }

    fn cleanup<'a>(
        &'a self,
        challenge: &'a AcmeDns01Challenge,
    ) -> Pin<Box<dyn Future<Output = Result<(), AnyError>> + Send + 'a>> {
        Box::pin(async move {
            let Some(record_name) =
                record_name_relative_to_zone(&challenge.record_fqdn, &self.zone)
            else {
                return Ok(());
            };

            let url = format!(
                "https://api.gandi.net/v5/livedns/domains/{}/records/{}/TXT",
                self.zone, record_name
            );

            let rsp = self
                .client
                .delete(&url)
                .bearer_auth(&self.api_key)
                .send()
                .await
                .map_err(|e| Box::new(e) as AnyError)?;

            // Cleanup is often best-effort; treat 404 as success.
            if !(rsp.status().is_success() || rsp.status() == reqwest::StatusCode::NOT_FOUND) {
                let status = rsp.status();
                let body = rsp.text().await.unwrap_or_default();
                return Err(Box::new(io::Error::other(format!(
                    "Gandi cleanup failed: HTTP {status}: {body}"
                ))) as AnyError);
            }

            Ok(())
        })
    }
}

fn record_name_relative_to_zone(record_fqdn: &str, zone: &str) -> Option<String> {
    let record_fqdn = record_fqdn.trim_end_matches('.');
    let zone = zone.trim_end_matches('.');

    let suffix = format!(".{zone}");
    if record_fqdn == zone {
        return Some("@".to_string());
    }

    record_fqdn
        .strip_suffix(&suffix)
        .map(|name| name.to_string())
}
