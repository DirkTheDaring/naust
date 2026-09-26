pub fn write_to_target(rendered: &str, target: &str) -> Result<(), crate::error::CliError> {
    if target == "-" {
        println!("{rendered}");
        return Ok(());
    }

    std::fs::write(target, rendered).map_err(|e| {
        crate::error::CliError::Message(format!("failed to write output file {target:?}: {e}"))
    })
}
