mod cli;
mod commands;
mod env_helpers;
mod error;
mod hooks;
mod output;
mod paths;

use clap::{error::ErrorKind, CommandFactory, Parser};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    if let Err(e) = run().await {
        fatal(&e);
    }
}

async fn run() -> Result<(), crate::error::CliError> {
    let cli = crate::cli::Cli::parse();

    match cli.command {
        Some(crate::cli::Command::Gen(args)) => {
            if !args.allow_first_wildcard {
                if let Some(first) = args.names.first() {
                    if first.is_wildcard() {
                        crate::cli::Cli::command()
                            .error(
                                ErrorKind::ValueValidation,
                                "first DNS name must not be a wildcard (use --allow-first-wildcard to override)",
                            )
                            .exit();
                    }
                }
            }

            let hook_debug = args.exec_debug || crate::env_helpers::env_is_one("EXEC_DEBUG");
            let resolved = crate::hooks::resolve_hook(&args, hook_debug)?;
            crate::commands::gen::run(args, resolved).await?;
            Ok(())
        }
        Some(crate::cli::Command::Validity(args)) => {
            crate::commands::validity::run(args)?;
            Ok(())
        }
        None => {
            let mut cmd = crate::cli::Cli::command();
            let _ = cmd.print_help();
            println!();
            Ok(())
        }
    }
}

fn fatal(e: &crate::error::CliError) -> ! {
    eprintln!("error: {e}");
    std::process::exit(1);
}
