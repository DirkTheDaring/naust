//! Server-side storage composition (ADR-010 §2.3, plan Phase 1c).
//!
//! These factories map the parsed server `Config` into concrete storage wiring.
//! They were moved out of `storage/mod.rs`: core storage never consumes the
//! server configuration; composition roots (server, CLI) call in here.

use crate::config::{Config, StorageBackend};
use crate::storage::{StorageError, StorageWiring, fs, ports, s3};
use std::sync::Arc;

#[cfg(test)]
mod tests;

pub fn storage_wiring_try_from_config(config: &Config) -> Result<StorageWiring, StorageError> {
    match config.storage_backend {
        StorageBackend::Filesystem => {
            let limits = storage_fs::DirEnumerationLimits::new(
                config.fs_manifest_listing_max_entries,
                config.fs_manifest_listing_max_name_bytes,
            );
            let discovery_limits = fs::repo_discovery::DiscoveryLimits {
                max_depth: config.fs_gc_discovery_max_depth,
                max_dir_enumerations: config.fs_gc_discovery_max_dir_enumerations,
                max_total_entries: config.fs_gc_discovery_max_total_discovery_entries,
                max_manifest_dirs: config.fs_gc_discovery_max_manifest_dirs,
                max_retained_path_bytes: config.fs_gc_discovery_max_discovery_retained_path_bytes,
                per_dir_limits: storage_fs::DirEnumerationLimits::new(
                    config.fs_gc_discovery_intermediate_dir_max_entries,
                    config.fs_gc_discovery_intermediate_dir_max_name_bytes,
                ),
            };
            let ref_limits = fs::manifest_refs::ManifestReferenceLimits {
                max_terminal_dir_enumerations: config.fs_gc_discovery_max_terminal_dir_enumerations,
                per_dir_limits: storage_fs::DirEnumerationLimits::new(
                    config.fs_gc_discovery_terminal_dir_max_entries,
                    config.fs_gc_discovery_terminal_dir_max_name_bytes,
                ),
                max_total_manifest_entries: config.fs_gc_discovery_max_total_manifest_entries,
                max_manifests_read: config.fs_gc_discovery_max_manifests_read,
                max_total_references: config.fs_gc_discovery_max_total_references,
                max_retained_logical_bytes: config.fs_gc_discovery_max_retained_logical_bytes,
                max_manifest_payload_bytes: config.fs_gc_discovery_max_manifest_payload_bytes,
            };
            let tag_listing_limits = fs::tag_listing::TagListingLimits {
                repo_probe_limits: storage_fs::DirEnumerationLimits::new(
                    config.fs_tag_listing_repo_probe_max_entries,
                    config.fs_tag_listing_repo_probe_max_name_bytes,
                ),
                tags_dir_limits: storage_fs::DirEnumerationLimits::new(
                    config.fs_tag_listing_max_entries,
                    config.fs_tag_listing_max_name_bytes,
                ),
                payload_limits: fs::tag_listing::TagReadLimits {
                    max_payload_bytes: Some(config.fs_tag_listing_max_payload_bytes),
                },
            };
            let fs_storage = fs::FsStorage::try_new_with_all_limits(
                config.fs_root.clone(),
                config.max_upload_bytes,
                limits,
                discovery_limits,
                ref_limits,
                tag_listing_limits,
            )?;
            Ok(StorageWiring::from_backend(Arc::new(fs_storage)))
        }
        StorageBackend::S3 => {
            let session_cfg = s3::S3SessionConfig {
                lease_duration_secs: config.s3_lease_duration_secs,
                lease_renewal_interval_secs: config.s3_lease_renewal_interval_secs,
                max_retry_attempts: config.s3_max_retry_attempts,
                receipt_lifetime_secs: config.upload_receipt_lifetime_secs,
                upload_expiration_secs: config.upload_gc_max_age_secs,
                legacy_multipart_cleanup_policy: config.s3_legacy_multipart_cleanup_policy,
                part_size_bytes: config.s3_part_size_bytes,
            };
            let s3_storage = s3::S3Storage::new(
                config.s3_endpoint.clone(),
                config.s3_region.clone(),
                config.s3_bucket.clone(),
                config.s3_prefix.clone(),
                config.max_upload_bytes,
            )
            .with_session_config(session_cfg);
            Ok(StorageWiring::from_backend(Arc::new(s3_storage)))
        }
    }
}

pub fn storage_wiring_from_config(config: &Config) -> StorageWiring {
    storage_wiring_try_from_config(config).unwrap_or_else(|err| {
        eprintln!("storage: initialization failed: {err}");
        std::process::exit(1);
    })
}

pub fn try_from_config(config: &Config) -> Result<StorageWiring, StorageError> {
    storage_wiring_try_from_config(config)
}

pub fn from_config(config: &Config) -> StorageWiring {
    storage_wiring_from_config(config)
}

pub fn proxy_cache_storage_try_from_config(
    config: &Config,
    upstream: Option<&crate::config::ProxyUpstreamRoute>,
) -> Result<Arc<dyn ports::ProxyStoragePort>, StorageError> {
    match config.storage_backend {
        StorageBackend::Filesystem => {
            let root = match upstream {
                Some(up) => up.cache_fs_root.clone().ok_or_else(|| {
                    StorageError::configuration(
                        "proxy upstream enabled: filesystem cache requires cache_fs_root",
                    )
                })?,
                None => config
                    .proxy
                    .cache_fs_root
                    .clone()
                    .unwrap_or_else(|| config.fs_root.join("cache")),
            };
            let limits = storage_fs::DirEnumerationLimits::new(
                config.fs_manifest_listing_max_entries,
                config.fs_manifest_listing_max_name_bytes,
            );
            let tag_listing_limits = fs::tag_listing::TagListingLimits {
                repo_probe_limits: storage_fs::DirEnumerationLimits::new(
                    config.fs_tag_listing_repo_probe_max_entries,
                    config.fs_tag_listing_repo_probe_max_name_bytes,
                ),
                tags_dir_limits: storage_fs::DirEnumerationLimits::new(
                    config.fs_tag_listing_max_entries,
                    config.fs_tag_listing_max_name_bytes,
                ),
                payload_limits: fs::tag_listing::TagReadLimits {
                    max_payload_bytes: Some(config.fs_tag_listing_max_payload_bytes),
                },
            };
            let fs_storage = fs::FsStorage::try_new_with_all_limits(
                root,
                config.max_upload_bytes,
                limits,
                fs::repo_discovery::DiscoveryLimits::default(),
                fs::manifest_refs::ManifestReferenceLimits::default(),
                tag_listing_limits,
            )?;
            Ok(Arc::new(fs_storage))
        }
        StorageBackend::S3 => {
            let endpoint = config.s3_endpoint.clone().ok_or_else(|| {
                StorageError::configuration("proxy enabled: S3 cache requires STORAGE_S3_ENDPOINT")
            })?;
            let region = config.s3_region.clone().ok_or_else(|| {
                StorageError::configuration("proxy enabled: S3 cache requires STORAGE_S3_REGION")
            })?;
            let bucket = config.s3_bucket.clone().ok_or_else(|| {
                StorageError::configuration("proxy enabled: S3 cache requires STORAGE_S3_BUCKET")
            })?;
            let prefix =
                match upstream {
                    Some(up) => up.cache_s3_prefix.clone().ok_or_else(|| {
                        StorageError::configuration(
                            "proxy upstream enabled: S3 cache requires cache_s3_prefix",
                        )
                    })?,
                    None => config.proxy.cache_s3_prefix.clone().unwrap_or_else(|| {
                        format!("{}/cache", config.s3_prefix.trim_end_matches('/'))
                    }),
                };
            let session_cfg = s3::S3SessionConfig {
                lease_duration_secs: config.s3_lease_duration_secs,
                lease_renewal_interval_secs: config.s3_lease_renewal_interval_secs,
                max_retry_attempts: config.s3_max_retry_attempts,
                receipt_lifetime_secs: config.upload_receipt_lifetime_secs,
                upload_expiration_secs: config.upload_gc_max_age_secs,
                legacy_multipart_cleanup_policy: config.s3_legacy_multipart_cleanup_policy,
                part_size_bytes: config.s3_part_size_bytes,
            };
            let s3_storage = s3::S3Storage::new(
                Some(endpoint),
                Some(region),
                Some(bucket),
                prefix,
                config.max_upload_bytes,
            )
            .with_session_config(session_cfg);
            Ok(Arc::new(s3_storage))
        }
    }
}

pub(crate) async fn storage_wiring_try_from_config_async_with_factory<F>(
    config: &Config,
    storage_factory: F,
) -> Result<StorageWiring, StorageError>
where
    F: FnOnce(&Config) -> Result<StorageWiring, StorageError> + Send + 'static,
{
    match config.storage_backend {
        StorageBackend::Filesystem => {
            let config_clone = config.clone();
            tokio::task::spawn_blocking(move || storage_factory(&config_clone))
                .await
                .map_err(|join_err| {
                    StorageError::backend(format!(
                        "filesystem storage initialization task failed: {join_err}"
                    ))
                })?
        }
        StorageBackend::S3 => storage_factory(config),
    }
}

pub(crate) async fn proxy_cache_storage_try_from_config_async_with_factory<F>(
    config: &Config,
    upstream: Option<&crate::config::ProxyUpstreamRoute>,
    factory: F,
) -> Result<Arc<dyn ports::ProxyStoragePort>, StorageError>
where
    F: FnOnce(
            &Config,
            Option<&crate::config::ProxyUpstreamRoute>,
        ) -> Result<Arc<dyn ports::ProxyStoragePort>, StorageError>
        + Send
        + 'static,
{
    match config.storage_backend {
        StorageBackend::Filesystem => {
            let config_clone = config.clone();
            let upstream_clone = upstream.cloned();
            tokio::task::spawn_blocking(move || factory(&config_clone, upstream_clone.as_ref()))
                .await
                .map_err(|join_err| {
                    StorageError::backend(format!(
                        "filesystem proxy cache storage initialization task failed: {join_err}"
                    ))
                })?
        }
        StorageBackend::S3 => factory(config, upstream),
    }
}
