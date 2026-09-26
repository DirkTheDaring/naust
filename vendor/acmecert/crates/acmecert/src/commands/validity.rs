use crate::cli::ValidityArgs;
use crate::error::CliError;
use crate::paths;
use acmecert_core::api::DnsName;
use std::path::PathBuf;

pub fn run(args: ValidityArgs) -> Result<(), CliError> {
    let input_path = PathBuf::from(&args.input);
    let cert_path = if input_path.is_file() {
        input_path
    } else {
        let name = args.input.parse::<DnsName>().map_err(|msg| {
            CliError::Message(format!(
                "invalid DNS name or cert path '{}': {msg}",
                args.input
            ))
        })?;

        let dir = match args.target_dir {
            Some(d) => d,
            None => paths::default_target_dir(&[name]),
        };
        dir.join("cert.pem")
    };

    let days = acmecert_core::api::validity_days(&cert_path)?;
    println!("{days}");

    Ok(())
}
