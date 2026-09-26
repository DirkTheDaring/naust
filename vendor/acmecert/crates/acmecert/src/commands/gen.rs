use crate::cli::GenArgs;
use crate::error::CliError;
use crate::hooks::ResolvedHook;
use crate::{output, paths};
use acmecert_core::api::{OutputPaths, PropagationCheck};
use std::time::Duration;

const DEFAULT_RENEWAL_WINDOW: Duration = Duration::from_secs(30 * 24 * 60 * 60);

pub async fn run(args: GenArgs, resolved: ResolvedHook) -> Result<(), CliError> {
    let propagation_check =
        PropagationCheck::from_flags(args.no_propagation_check, args.strict_propagation);

    // Default behavior (no output format selected): keep the existing
    // renewal/reuse logic and directory persistence.
    if args.output_format.is_none() {
        let out_dir = match args.target_dir.clone() {
            Some(dir) => dir,
            None => paths::default_target_dir(&args.names),
        };

        let opts = acmecert_core::api::GenOptions::new(
            args.email,
            args.names,
            OutputPaths::in_dir(out_dir),
        )
        .allow_first_wildcard(args.allow_first_wildcard)
        .proxy(args.proxy)
        .propagation_check(propagation_check)
        .renewal_window(DEFAULT_RENEWAL_WINDOW);

        let cert = acmecert_core::api::run_gen(opts, resolved.hook.as_ref()).await?;

        println!("Certificate written to: {:?}", cert.cert_path);
        println!("Private key written to: {:?}", cert.key_path);
        if resolved.is_interactive && !cert.challenges.is_empty() {
            println!("Remember to delete the DNS TXT records you created.");
        }

        return Ok(());
    }

    // Output-neutral issuance for JSON/YAML.
    let opts = acmecert_core::api::IssueOptions::new(args.email, args.names)
        .allow_first_wildcard(args.allow_first_wildcard)
        .proxy(args.proxy)
        .propagation_check(propagation_check);

    let issued = acmecert_core::api::run_issue(opts, resolved.hook.as_ref()).await?;

    let format = args.output_format.expect("checked above");
    let target = args.output.as_deref().unwrap_or("-");

    let rendered = match format {
        crate::cli::OutputFormat::Json => {
            acmecert_core::api::issued_to_json(&issued).map_err(|e| e.to_string())?
        }
        crate::cli::OutputFormat::Yaml => {
            acmecert_core::api::issued_to_yaml(&issued).map_err(|e| e.to_string())?
        }
    };
    output::write_to_target(&rendered, target)?;

    Ok(())
}
