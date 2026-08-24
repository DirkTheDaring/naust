use super::http::{build_http_client, json_string, DEFAULT_CONNECT_TIMEOUT, DEFAULT_HTTP_TIMEOUT};
use crate::acme_dns01::AcmeDns01Challenge;
use crate::error::AnyError;
use crate::hook::DnsHook;
use crate::types::{AuthorizationHeader, ProxyUrl};
use std::future::Future;
use std::io;
use std::pin::Pin;

/// ispone-bridge HTTP API hook.
///
/// This is the prefab version of the CLI's `ACME_URL`/`ACME_TOKEN` hook.
#[derive(Debug, Clone)]
pub struct IsponeHttpHook {
    client: reqwest::Client,
    base_url: String,
    authorization: AuthorizationHeader,
    debug: bool,
}

impl IsponeHttpHook {
    pub fn new(
        base_url: String,
        authorization: AuthorizationHeader,
        proxy: Option<ProxyUrl>,
        debug: bool,
    ) -> Result<Self, AnyError> {
        let client = build_http_client(
            proxy.as_ref(),
            DEFAULT_HTTP_TIMEOUT,
            DEFAULT_CONNECT_TIMEOUT,
        )?;
        Ok(Self {
            client,
            base_url,
            authorization,
            debug,
        })
    }
}

impl DnsHook for IsponeHttpHook {
    fn present<'a>(
        &'a self,
        challenge: &'a AcmeDns01Challenge,
    ) -> Pin<Box<dyn Future<Output = Result<(), AnyError>> + Send + 'a>> {
        Box::pin(async move {
            acme_http_hook(
                &self.client,
                &self.base_url,
                &self.authorization,
                "present",
                &challenge.record_fqdn,
                &challenge.txt_value,
                self.debug,
            )
            .await
        })
    }

    fn cleanup<'a>(
        &'a self,
        challenge: &'a AcmeDns01Challenge,
    ) -> Pin<Box<dyn Future<Output = Result<(), AnyError>> + Send + 'a>> {
        Box::pin(async move {
            acme_http_hook(
                &self.client,
                &self.base_url,
                &self.authorization,
                "cleanup",
                &challenge.record_fqdn,
                &challenge.txt_value,
                self.debug,
            )
            .await
        })
    }
}

async fn acme_http_hook(
    client: &reqwest::Client,
    base_url: &str,
    authorization: &AuthorizationHeader,
    action: &str,
    record_fqdn: &str,
    txt_value: &str,
    hook_debug: bool,
) -> Result<(), AnyError> {
    let base_url = base_url.trim_end_matches('/');
    let url = format!("{base_url}/acme/{action}");

    if hook_debug {
        eprintln!("ispone request: POST {url}");
    }

    let body = format!(
        "{{\"name\":{},\"value\":{}}}",
        json_string(record_fqdn),
        json_string(txt_value)
    );

    let rsp = client
        .post(&url)
        .header(reqwest::header::AUTHORIZATION, authorization.as_str())
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await
        .map_err(|e| Box::new(e) as AnyError)?;

    let status = rsp.status();
    let body = rsp.text().await.unwrap_or_default();

    if hook_debug {
        if !body.is_empty() {
            eprintln!("ispone response ({status}):\n{body}");
        } else {
            eprintln!("ispone response ({status})");
        }
    }

    if !status.is_success() {
        return Err(Box::new(io::Error::other(format!(
            "ispone {url} failed with HTTP {status}: {body}"
        ))) as AnyError);
    }

    Ok(())
}
