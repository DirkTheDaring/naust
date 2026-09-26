use acmecert_core::api::DnsName;
use std::path::PathBuf;

pub fn default_target_dir(names: &[DnsName]) -> PathBuf {
    let first = names.first().map(|n| n.base_domain()).unwrap_or("unknown");
    PathBuf::from("acmecert-data").join(sanitize_dir_component(first))
}

fn sanitize_dir_component(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        let ok = ch.is_ascii_alphanumeric() || ch == '.' || ch == '-' || ch == '_';
        out.push(if ok { ch } else { '_' });
    }

    if out.is_empty() {
        "unknown".to_string()
    } else {
        out
    }
}
