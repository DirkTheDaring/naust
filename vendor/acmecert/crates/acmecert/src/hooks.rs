use crate::cli::{GenArgs, HookKind};
use crate::env_helpers::env_non_empty;
use crate::error::CliError;
use acmecert_core::api::{AuthorizationHeader, DnsHook};
use std::env;
use std::path::PathBuf;

pub struct ResolvedHook {
    pub is_interactive: bool,
    pub hook: Box<dyn DnsHook>,
}

fn select_hook_kind(args: &GenArgs) -> HookKind {
    if args.hook != HookKind::Auto {
        return args.hook;
    }

    if args.acme_url.is_some() || env::var("ACME_URL").ok().is_some() {
        HookKind::Ispone
    } else if args.exec_path.is_some() || env::var("EXEC_PATH").ok().is_some() {
        HookKind::Exec
    } else {
        HookKind::Manual
    }
}

pub fn resolve_hook(args: &GenArgs, hook_debug: bool) -> Result<ResolvedHook, CliError> {
    let selected_hook = select_hook_kind(args);

    let (is_interactive, hook): (bool, Box<dyn DnsHook>) = match selected_hook {
        HookKind::Manual => (true, Box::new(acmecert_core::prefab::ManualHook)),
        HookKind::Exec => {
            let path = args
                .exec_path
                .clone()
                .or_else(|| env_non_empty("EXEC_PATH").map(PathBuf::from));

            match path {
                Some(p) if p.is_file() => (
                    false,
                    Box::new(acmecert_core::prefab::ExecHook {
                        path: p,
                        debug: hook_debug,
                    }),
                ),
                Some(p) => {
                    return Err(CliError::Message(format!(
                        "exec hook path is not a file: {p:?}"
                    )))
                }
                None => {
                    return Err(CliError::Message(
                        "exec hook selected but no path was provided (use --exec-path or set EXEC_PATH)"
                            .to_string(),
                    ));
                }
            }
        }
        HookKind::Gandi => {
            let api_key = args
                .gandi_api_key
                .clone()
                .or_else(|| env_non_empty("GANDI_API_KEY"))
                .filter(|v| !v.is_empty())
                .ok_or_else(|| {
                    CliError::Message(
                        "gandi hook selected but no API key was provided (use --gandi-api-key or set GANDI_API_KEY)"
                            .to_string(),
                    )
                })?;

            let zone = args
                .gandi_zone
                .clone()
                .or_else(|| env_non_empty("GANDI_ZONE"))
                .filter(|v| !v.is_empty())
                .ok_or_else(|| {
                    CliError::Message(
                        "gandi hook selected but no zone was provided (use --gandi-zone or set GANDI_ZONE)"
                            .to_string(),
                    )
                })?;

            let hook = match acmecert_core::prefab::GandiLiveDnsHook::new(
                api_key,
                zone,
                args.gandi_ttl,
                args.proxy.clone(),
            ) {
                Ok(v) => v,
                Err(e) => {
                    return Err(CliError::Message(format!(
                        "failed to build gandi hook: {e}"
                    )))
                }
            };

            (false, Box::new(hook))
        }
        HookKind::Ispone => {
            let base_url = args
                .acme_url
                .clone()
                .or_else(|| env_non_empty("ACME_URL"))
                .ok_or_else(|| {
                    CliError::Message(
                        "ispone hook selected but no base URL was provided (use --acme-url or set ACME_URL)"
                            .to_string(),
                    )
                })?;

            let token = args
                .acme_token
                .clone()
                .or_else(|| env_non_empty("ACME_TOKEN"))
                .or_else(|| env_non_empty("BRIDGE_API_KEY"));

            let token = token.ok_or_else(|| {
                CliError::Message(
                    "ACME_URL is set but no token was provided (use --acme-token or set ACME_TOKEN/BRIDGE_API_KEY)"
                        .to_string(),
                )
            })?;

            let authorization = AuthorizationHeader::from_token_or_header_value(&token)?;

            let hook = match acmecert_core::prefab::IsponeHttpHook::new(
                base_url,
                authorization,
                args.proxy.clone(),
                hook_debug,
            ) {
                Ok(v) => v,
                Err(e) => {
                    return Err(CliError::Message(format!(
                        "failed to build ispone hook: {e}"
                    )))
                }
            };

            (false, Box::new(hook))
        }
        HookKind::Auto => unreachable!("auto resolved above"),
    };

    Ok(ResolvedHook {
        is_interactive,
        hook,
    })
}
