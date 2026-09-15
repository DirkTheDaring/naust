//! Shared helpers for translating backend-neutral [`StoreError`]s into the
//! registry taxonomy — pieces common to every migrated object family.

use storage_core::object_store::StoreError;

/// Structured detection of an exhausted-storage failure (`ENOSPC` /
/// `StorageFull`) anywhere in a backend error's source chain.
///
/// The filesystem backend historically classified `ENOSPC` as
/// [`crate::storage::StorageError::InsufficientStorage`] (HTTP 507). The
/// generic adapter reports such failures as `StoreError::Backend` with the
/// causal `std::io::Error` preserved in the source chain; this walks that
/// chain looking for the STRUCTURED signal (`ErrorKind::StorageFull` or a
/// raw `ENOSPC`), never message text. Remote backends carry no such cause
/// and keep their accepted `Backend` classification.
pub(crate) fn store_error_is_storage_full(err: &StoreError) -> bool {
    let mut source: Option<&(dyn std::error::Error + 'static)> = match err {
        StoreError::Backend { source, .. } | StoreError::PermissionDenied { source, .. } => source
            .as_deref()
            .map(|s| s as &(dyn std::error::Error + 'static)),
        _ => None,
    };
    while let Some(err) = source {
        if let Some(io_err) = err.downcast_ref::<std::io::Error>() {
            if io_err.kind() == std::io::ErrorKind::StorageFull
                || io_err.raw_os_error() == Some(libc::ENOSPC)
            {
                return true;
            }
        }
        source = err.source();
    }
    false
}

/// Process-global serialization for tests that arm the storage-fs
/// fault-injection table (the table and `fault::reset` are global; parallel
/// arming tests would consume or clear each other's rules).
#[cfg(test)]
pub(crate) static FAULT_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
