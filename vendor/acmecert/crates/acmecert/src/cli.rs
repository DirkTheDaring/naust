use acmecert_core::api::{DnsName, EmailAddress, ProxyUrl};
use clap::{Args, Parser, Subcommand, ValueEnum};
use std::fs;
use std::path::PathBuf;

fn parse_existing_dir(input: &str) -> Result<PathBuf, String> {
    let path = PathBuf::from(input);
    let meta = fs::metadata(&path)
        .map_err(|e| format!("target directory does not exist or is not accessible: {e}"))?;
    if !meta.is_dir() {
        return Err("target directory is not a directory".to_string());
    }
    Ok(path)
}

#[derive(Parser, Debug)]
#[command(name = "acmecert", version = env!("CARGO_PKG_VERSION"))]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Subcommand, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum Command {
    /// Generate something (placeholder)
    Gen(GenArgs),

    /// Show how long the stored certificate is still valid
    Validity(ValidityArgs),
}

#[derive(Args, Debug)]
pub struct GenArgs {
    /// DNS hook implementation to use.
    ///
    /// - `auto` (default): ispone if `--acme-url`/`ACME_URL` is set, else exec if `--exec-path`/`EXEC_PATH` is set, else manual.
    /// - `manual`: print required TXT record and wait for Enter.
    /// - `exec`: call a local helper script/binary.
    /// - `gandi`: update TXT records via Gandi LiveDNS API.
    /// - `ispone`: call ispone-bridge HTTP API.
    #[arg(long, value_name = "HOOK", default_value = "auto")]
    pub hook: HookKind,

    /// Allow the first DNS name to be a wildcard (e.g. *.example.com)
    #[arg(long)]
    pub allow_first_wildcard: bool,

    /// ACME account contact email (used for Let's Encrypt)
    #[arg(long, value_name = "EMAIL")]
    pub email: EmailAddress,

    /// Target directory to write generated files into
    #[arg(long, value_name = "DIR")]
    pub target_dir: Option<PathBuf>,

    /// Explicit HTTP proxy URL (overrides proxy environment variables)
    #[arg(long, value_name = "URL")]
    pub proxy: Option<ProxyUrl>,

    /// Base URL for the `ispone` DNS hook (falls back to env ACME_URL)
    #[arg(long, value_name = "URL")]
    pub acme_url: Option<String>,

    /// Token for the `ispone` DNS hook (falls back to env ACME_TOKEN or BRIDGE_API_KEY)
    ///
    /// If the value contains a space it will be used as-is as the Authorization header value.
    /// Otherwise it will be sent as `Bearer <token>`.
    #[arg(long, value_name = "TOKEN")]
    pub acme_token: Option<String>,

    /// Path for the `exec` DNS hook (falls back to env EXEC_PATH)
    #[arg(long, value_name = "PATH")]
    pub exec_path: Option<PathBuf>,

    /// Print helper output / HTTP responses for the DNS hook (exec/ispone) (falls back to env EXEC_DEBUG=1)
    #[arg(long)]
    pub exec_debug: bool,

    /// Fail if DNS propagation cannot be verified via public resolvers.
    ///
    /// Currently this affects the default Cloudflare DoH propagation check.
    #[arg(long)]
    pub strict_propagation: bool,

    /// Disable public DNS propagation verification.
    ///
    /// This skips the Cloudflare DoH polling step entirely.
    #[arg(long, conflicts_with = "strict_propagation")]
    pub no_propagation_check: bool,

    /// Gandi LiveDNS API key (falls back to env GANDI_API_KEY)
    #[arg(long, value_name = "KEY")]
    pub gandi_api_key: Option<String>,

    /// Gandi zone/domain (e.g. example.com) (falls back to env GANDI_ZONE)
    #[arg(long, value_name = "ZONE")]
    pub gandi_zone: Option<String>,

    /// TTL to use for Gandi TXT records
    #[arg(long, value_name = "SECONDS", default_value_t = 300)]
    pub gandi_ttl: u32,

    /// Output format for the issued certificate.
    ///
    /// If omitted, writes `cert.pem`/`key.pem` into the target directory (legacy behavior).
    /// If set to `json` or `yaml`, prints to stdout by default or writes to `--output`.
    #[arg(long, value_name = "FORMAT")]
    pub output_format: Option<OutputFormat>,

    /// Output target for `--output-format json|yaml`.
    ///
    /// Use `-` for stdout (default).
    #[arg(long, value_name = "PATH")]
    pub output: Option<String>,

    /// One or more DNS names (e.g. example.com, *.example.com)
    #[arg(value_name = "DNS_NAME", required = true)]
    pub names: Vec<DnsName>,
}

#[derive(Args, Debug)]
pub struct ValidityArgs {
    /// Target directory where cert.pem is located
    #[arg(long, value_name = "DIR", value_parser = parse_existing_dir)]
    pub target_dir: Option<PathBuf>,

    /// DNS name (to use default directory layout) or path to a cert.pem file
    #[arg(value_name = "DNS_NAME_OR_CERT_PATH")]
    pub input: String,
}

#[derive(ValueEnum, Debug, Clone, Copy)]
pub enum OutputFormat {
    Json,
    Yaml,
}

#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookKind {
    Auto,
    Manual,
    Exec,
    Gandi,
    #[value(alias = "bridge")]
    Ispone,
}
