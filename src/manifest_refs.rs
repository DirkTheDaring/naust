use crate::registry::digest::Digest;

#[derive(Clone, Debug, Default)]
pub(crate) struct ManifestRefs {
    pub blobs: Vec<String>,
    pub manifests: Vec<String>,
}

pub(crate) fn parse_manifest_refs(bytes: &[u8]) -> Option<ManifestRefs> {
    let v: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    let mut refs = ManifestRefs::default();

    // Index/list: manifests[].digest
    if let Some(manifests) = v.get("manifests").and_then(|m| m.as_array()) {
        for m in manifests {
            if let Some(d) = m.get("digest").and_then(|d| d.as_str()) {
                if Digest::parse(d).is_ok() {
                    refs.manifests.push(d.to_string());
                }
            }
        }
    }

    // Manifest: config.digest + layers[].digest
    if let Some(cfg_digest) = v
        .get("config")
        .and_then(|c| c.get("digest"))
        .and_then(|d| d.as_str())
    {
        if Digest::parse(cfg_digest).is_ok() {
            refs.blobs.push(cfg_digest.to_string());
        }
    }
    if let Some(layers) = v.get("layers").and_then(|l| l.as_array()) {
        for layer in layers {
            if let Some(d) = layer.get("digest").and_then(|d| d.as_str()) {
                if Digest::parse(d).is_ok() {
                    refs.blobs.push(d.to_string());
                }
            }
        }
    }

    // OCI/ORAS artifact manifest: blobs[].digest
    if let Some(blobs) = v.get("blobs").and_then(|b| b.as_array()) {
        for blob in blobs {
            if let Some(d) = blob.get("digest").and_then(|d| d.as_str()) {
                if Digest::parse(d).is_ok() {
                    refs.blobs.push(d.to_string());
                }
            }
        }
    }

    // OCI artifacts may reference a subject by digest.
    if let Some(subject) = v
        .get("subject")
        .and_then(|s| s.get("digest"))
        .and_then(|d| d.as_str())
    {
        if Digest::parse(subject).is_ok() {
            refs.blobs.push(subject.to_string());
        }
    }

    Some(refs)
}
