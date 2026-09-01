//! Backwards-compatibility re-exports and aliases for manifest lifecycle types.
//!
//! This module provides compatibility aliases for external callers.
//! Internal codebase callers should use [`crate::manifest_lifecycle`] directly.

pub use crate::manifest_lifecycle::{
    MAX_MANIFEST_SIZE, ManifestLifecycleError as PublishManifestError, ManifestLifecycleService,
    ManifestLifecycleService as ManifestPublisher, ProxyEvictionResult, ProxyPublicationEvidence,
    PublishManifestRequest, PublishedManifest, is_supported_manifest_media_type,
};
