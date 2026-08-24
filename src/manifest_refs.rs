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

    // OCI artifacts may reference a subject by digest (points to another manifest/index).
    if let Some(subject) = v
        .get("subject")
        .and_then(|s| s.get("digest"))
        .and_then(|d| d.as_str())
    {
        if Digest::parse(subject).is_ok() {
            refs.manifests.push(subject.to_string());
        }
    }

    Some(refs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_manifest_with_subject_as_manifest_ref() {
        let manifest = serde_json::json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "artifactType": "application/vnd.example.sbom.v1",
            "config": {
                "mediaType": "application/vnd.oci.empty.v1+json",
                "digest": "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
                "size": 2
            },
            "layers": [
                {
                    "mediaType": "text/spdx+json",
                    "digest": "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                    "size": 1234
                }
            ],
            "subject": {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": "sha256:5b3b31e177b836470409a803a726d4ca84345d0ed158f4a1801267822557c6b9",
                "size": 768
            }
        });

        let bytes = serde_json::to_vec(&manifest).unwrap();
        let refs = parse_manifest_refs(&bytes).expect("parsed refs");

        assert_eq!(
            refs.manifests,
            vec!["sha256:5b3b31e177b836470409a803a726d4ca84345d0ed158f4a1801267822557c6b9"]
        );
        assert_eq!(
            refs.blobs,
            vec![
                "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
                "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
            ]
        );
    }

    #[test]
    fn parses_index_with_subject() {
        let index = serde_json::json!({
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "artifactType": "application/vnd.example.attestation.v1",
            "manifests": [
                {
                    "mediaType": "application/vnd.oci.image.manifest.v1+json",
                    "digest": "sha256:1111111111111111111111111111111111111111111111111111111111111111",
                    "size": 500
                }
            ],
            "subject": {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": "sha256:2222222222222222222222222222222222222222222222222222222222222222",
                "size": 800
            }
        });

        let bytes = serde_json::to_vec(&index).unwrap();
        let refs = parse_manifest_refs(&bytes).expect("parsed refs");

        assert_eq!(
            refs.manifests,
            vec![
                "sha256:1111111111111111111111111111111111111111111111111111111111111111",
                "sha256:2222222222222222222222222222222222222222222222222222222222222222"
            ]
        );
        assert!(refs.blobs.is_empty());
    }
}
