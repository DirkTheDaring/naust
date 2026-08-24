use crate::acme_dns01::AcmeDns01Challenge;
use crate::error::AnyError;
use crate::hook::DnsHook;
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;

/// External script/binary hook.
///
/// Contract:
/// - `<exec_path> present <record_fqdn> <txt_value>`
/// - `<exec_path> cleanup <record_fqdn> <txt_value>`
///
/// Exit code 0 indicates success.
#[derive(Debug, Clone)]
pub struct ExecHook {
    pub path: PathBuf,
    pub debug: bool,
}

impl DnsHook for ExecHook {
    fn present<'a>(
        &'a self,
        challenge: &'a AcmeDns01Challenge,
    ) -> Pin<Box<dyn Future<Output = Result<(), AnyError>> + Send + 'a>> {
        Box::pin(async move {
            exec_exec_path(
                &self.path,
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
            exec_exec_path(
                &self.path,
                "cleanup",
                &challenge.record_fqdn,
                &challenge.txt_value,
                self.debug,
            )
            .await
        })
    }
}

async fn exec_exec_path(
    exec_path: &Path,
    action: &str,
    record_fqdn: &str,
    txt_value: &str,
    exec_debug: bool,
) -> Result<(), AnyError> {
    let exec_path_buf = exec_path.to_owned();
    let action = action.to_string();
    let record_fqdn = record_fqdn.to_string();
    let txt_value = txt_value.to_string();

    let exec_path_for_err = exec_path_buf.clone();
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new(&exec_path_buf)
            .arg(action)
            .arg(record_fqdn)
            .arg(txt_value)
            .output()
    })
    .await
    .map_err(|e| Box::new(e) as AnyError)?
    .map_err(|e| {
        Box::new(io::Error::other(format!(
            "failed to execute {:?}: {e}",
            exec_path_for_err
        ))) as AnyError
    })?;

    if exec_debug {
        if !output.stdout.is_empty() {
            eprintln!(
                "ExecHook stdout:\n{}",
                String::from_utf8_lossy(&output.stdout)
            );
        }
        if !output.stderr.is_empty() {
            eprintln!(
                "ExecHook stderr:\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }

    if !output.status.success() {
        let code = output
            .status
            .code()
            .map(|c| c.to_string())
            .unwrap_or_else(|| "signal".to_string());

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        return Err(Box::new(io::Error::other(format!(
            "ExecHook {:?} returned non-zero exit ({code}). stdout: {stdout} stderr: {stderr}",
            exec_path_for_err
        ))) as AnyError);
    }

    Ok(())
}
