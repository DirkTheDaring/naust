//! Executable, TEST-ONLY prototype for the coherent (single-tree) filesystem
//! upload-session and receipt cleanup mechanism proposed in
//! `docs/architecture/filesystem-upload-lifecycle-contained-cleanup-design.md`.
//!
//! This file is an integration test. It is a separate crate from the library,
//! is compiled only under `cargo test`/`cargo check --all-targets`, and is NOT
//! referenced by any production module. Building it changes no production
//! routing or behaviour.
//!
//! CLAIM BOUNDARY — read before trusting any assertion in this file. Every
//! assertion falls into exactly one of three categories, labelled at each test.
//!
//! * REAL FILESYSTEM EVIDENCE. Scenarios that run real `*at` syscalls
//!   (`openat2`, `unlinkat`, `flock`, `fstat`, `ftruncate`, `fdopendir`) against
//!   a real temporary directory tree on the host. The single-tree coherence,
//!   lock-identity, and flock-exclusion results are real-kernel evidence on this
//!   Linux host. They are NOT claimed for non-Linux platforms and NOT claimed as
//!   hardware durability (no power-loss testing).
//!
//! * PROTOTYPE BEHAVIOUR. [`PinnedUploadsAuthority`] and [`prototype_reap`]
//!   re-implement the PROPOSED mechanism in test code. They are not the
//!   production `FsStorage` reaper and are deliberately structured differently
//!   (locked-inner destructive variants that receive an explicit lock token; a
//!   success/skip/error report; a public-result mapping). Assertions about them
//!   describe the proposal, not current production, which remains the
//!   internally-coherent ambient reaper.
//!
//! * REPRESENTATIVE MODELING. Some branches (notably Finalizing recovery, which
//!   in production inspects CAS content, links repository membership, and writes
//!   a receipt across three subtrees) are modelled with a simplified on-disk
//!   layout to exercise the AUTHORITY FLOW (which descriptor each read/write
//!   resolves through), not the production record formats or CAS sharding. These
//!   are explicitly NOT production integration evidence.
//!
//! * SIMULATED CAPABILITY CONTRACT. The [`RootRelativeMutator`] trait models the
//!   additive `naust_storage_fs` primitives that DO NOT YET EXIST in the dependency
//!   (`storage-layer-rust @ 0a628fd0` exposes a read-only `FsMetadataReader` with
//!   no fd-relative mutation and no raw-fd accessor). The libc implementation
//!   here demonstrates the KERNEL mechanism is sound on this host. It is NOT
//!   proof that the dependency's future in-crate implementation is correct; that
//!   requires the dependency's own tests, enumerated in the design doc.
//!
//! * REAL ASYNC CANCELLATION EVIDENCE vs OWNERSHIP MODELING. Section F holds two
//!   DISTINCT artifacts and does not conflate them. `tokio_cancellation_*` runs a
//!   real multi-threaded Tokio runtime and a real `spawn_blocking`: it aborts the
//!   awaiting task and shows the detached blocking closure keeps the session lock
//!   until its mutation finishes, then releases it. That is real async
//!   cancellation evidence on this host. `blocking_ownership_model_*` is a thread +
//!   channel illustration of the SAME ownership rule (guard owned by the worker,
//!   released only after the mutation); it is explicitly NOT async cancellation
//!   evidence and is labelled as ownership modeling only.
//!
//! No sleeps, no host devices, no blocking special-file opens. Ordering-
//! sensitive scenarios use an explicit in-process hook, a held lock, or channel
//! barriers between threads/tasks, never timing.

#![cfg(target_os = "linux")]

use std::ffi::{CStr, CString};
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

// ---------------------------------------------------------------------------
// Low-level `*at` helpers (REAL FILESYSTEM EVIDENCE).
//
// These mirror how `naust_storage_fs` already resolves reads: every per-operation
// resolution issues `openat2` from a pinned directory descriptor with
// `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`. The prototype
// reuses that exact resolve mask so the containment characteristics match the
// dependency's existing read path.
// ---------------------------------------------------------------------------

const RESOLVE_MASK: u64 =
    libc::RESOLVE_BENEATH | libc::RESOLVE_NO_SYMLINKS | libc::RESOLVE_NO_MAGICLINKS;

/// Open the storage root once, as an `O_PATH` directory descriptor. This mirrors
/// `FsMetadataReader::open`, which pins the root with `O_DIRECTORY | O_PATH`.
fn open_root_opath(path: &Path) -> io::Result<OwnedFd> {
    let cpath = CString::new(path.as_os_str().to_str().expect("utf-8 test path"))
        .expect("no interior NUL in test path");
    let raw = unsafe {
        libc::open(
            cpath.as_ptr(),
            libc::O_DIRECTORY | libc::O_PATH | libc::O_CLOEXEC,
        )
    };
    if raw < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(raw) })
}

/// `openat2` a path beneath `base`, contained by the shared resolve mask.
fn openat2_beneath(base: RawFd, rel: &CStr, flags: u64, mode: u64) -> io::Result<OwnedFd> {
    // `open_how` is `#[non_exhaustive]`; zero-init then assign, as the
    // dependency's own `openat2` call sites do.
    let mut how: libc::open_how = unsafe { std::mem::zeroed() };
    how.flags = flags | libc::O_CLOEXEC as u64;
    how.mode = mode;
    how.resolve = RESOLVE_MASK;
    let res = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            base,
            rel.as_ptr(),
            &how as *const libc::open_how,
            std::mem::size_of::<libc::open_how>(),
        )
    };
    if res < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(res as RawFd) })
}

/// `unlinkat(base, rel, 0)` — removes the directory ENTRY `rel` beneath `base`.
///
/// NOTE (design requirement 1): this removes a *name*, not a specific inode.
/// The pinned `base` guarantees the name is resolved in the intended directory
/// inode; tying that name to a previously-inspected file inode is the job of the
/// held lock plus revalidation, not of `unlinkat` itself.
fn unlinkat_entry(base: RawFd, rel: &CStr) -> io::Result<()> {
    let res = unsafe { libc::unlinkat(base, rel.as_ptr(), 0) };
    if res < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// `(st_dev, st_ino)` for an open descriptor — the inode identity token used for
/// revalidation under lock.
fn fd_identity(fd: RawFd) -> io::Result<(u64, u64)> {
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    let res = unsafe { libc::fstat(fd, &mut st as *mut libc::stat) };
    if res < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok((st.st_dev as u64, st.st_ino as u64))
}

/// `st_size` for an open descriptor — used by the representative CAS inspection.
fn fd_len(fd: RawFd) -> io::Result<u64> {
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    let res = unsafe { libc::fstat(fd, &mut st as *mut libc::stat) };
    if res < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(st.st_size as u64)
}

/// Non-blocking exclusive `flock`. `Ok(true)` = acquired, `Ok(false)` = busy
/// (`EWOULDBLOCK`/`EAGAIN`), `Err` = a genuine failure.
fn flock_ex_nb(fd: RawFd) -> io::Result<bool> {
    let res = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };
    if res == 0 {
        return Ok(true);
    }
    let err = io::Error::last_os_error();
    match err.raw_os_error() {
        Some(code) if code == libc::EWOULDBLOCK || code == libc::EAGAIN => Ok(false),
        _ => Err(err),
    }
}

/// Enumerate entries beneath a pinned directory descriptor. Opens a FRESH
/// enumeration descriptor via `openat2(".")` rather than `dup`, because `dup`
/// shares the pinned descriptor's directory read offset and a second pass would
/// start at EOF. This also mirrors the dependency, which opens a fresh fd per
/// `enumerate_dir` call.
fn list_dir(dir_fd: RawFd) -> io::Result<Vec<String>> {
    let fresh = openat2_beneath(dir_fd, c".", (libc::O_RDONLY | libc::O_DIRECTORY) as u64, 0)?;
    let raw = fresh.into_raw_fd(); // fdopendir/closedir take ownership of this fd.
    let dirp = unsafe { libc::fdopendir(raw) };
    if dirp.is_null() {
        let err = io::Error::last_os_error();
        unsafe { libc::close(raw) };
        return Err(err);
    }
    let mut names = Vec::new();
    loop {
        // Clear errno so we can distinguish end-of-stream from a real error.
        unsafe { *libc::__errno_location() = 0 };
        let entry = unsafe { libc::readdir(dirp) };
        if entry.is_null() {
            let errno = unsafe { *libc::__errno_location() };
            unsafe { libc::closedir(dirp) };
            if errno != 0 {
                return Err(io::Error::from_raw_os_error(errno));
            }
            break;
        }
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) }
            .to_string_lossy()
            .into_owned();
        if name != "." && name != ".." {
            names.push(name);
        }
    }
    names.sort();
    Ok(names)
}

fn read_fd_to_string(fd: OwnedFd) -> io::Result<String> {
    let mut file = std::fs::File::from(fd);
    let mut buf = String::new();
    file.read_to_string(&mut buf)?;
    Ok(buf)
}

/// Create-or-truncate and write a leaf beneath a pinned directory descriptor
/// (contained by the resolve mask). Models an additive contained-write primitive.
fn create_write_leaf(base: RawFd, rel: &CStr, bytes: &[u8]) -> io::Result<()> {
    let fd = openat2_beneath(
        base,
        rel,
        (libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC) as u64,
        0o600,
    )?;
    let mut file = std::fs::File::from(fd);
    file.write_all(bytes)?;
    Ok(())
}

/// `mkdirat(base, name, mode)` beneath a pinned directory descriptor. `EEXIST` is
/// idempotent success. Models an additive contained parent-directory creation
/// primitive (production `ensure_dir` before `link_repo_blob`/CAS rename target).
fn mkdirat_beneath(base: RawFd, name: &CStr) -> io::Result<()> {
    let res = unsafe { libc::mkdirat(base, name.as_ptr(), 0o700) };
    if res < 0 {
        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EEXIST) {
            return Ok(());
        }
        return Err(err);
    }
    Ok(())
}

/// Ensure a subdirectory exists beneath `base` and return a pinned descriptor for
/// it (contained parent-directory creation, then containment-checked open). The
/// open re-validates containment via the shared resolve mask, so a symlink planted
/// at `name` is rejected rather than followed.
fn ensure_subdir_beneath(base: RawFd, name: &CStr) -> io::Result<OwnedFd> {
    mkdirat_beneath(base, name)?;
    openat2_beneath(base, name, (libc::O_RDONLY | libc::O_DIRECTORY) as u64, 0)
}

/// `renameat(from_base, from, to_base, to)` — atomically replaces `to` with `from`
/// beneath pinned directory descriptors. On Linux `rename(2)` replaces the
/// destination atomically, which is how "atomic leaf replacement" is achieved.
fn renameat_beneath(from_base: RawFd, from: &CStr, to_base: RawFd, to: &CStr) -> io::Result<()> {
    let res = unsafe { libc::renameat(from_base, from.as_ptr(), to_base, to.as_ptr()) };
    if res < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Monotonic suffix source for unique temp names (never reused within a process).
static TEMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// ATOMIC LEAF REPLACEMENT (design requirement 3). Write `bytes` to a unique temp
/// leaf beneath `base`, then `renameat` it onto `name`, so a reader ever sees
/// either the old leaf or the complete new one — never a half-written file. This
/// models ATOMIC VISIBILITY, not crash durability: no `fsync` is issued, so the
/// rename's ordering against power loss is not guaranteed (design §6, D-5).
///
/// `fail_before_rename` simulates a mutation failure AFTER the temp is written but
/// BEFORE it becomes visible (e.g. an injected receipt-write fault): the temp is
/// unlinked (no leak, no partial visible leaf) and the error propagates. The
/// destination `name` is left untouched, so any prior value survives.
fn write_leaf_atomic(
    base: RawFd,
    name: &str,
    bytes: &[u8],
    fail_before_rename: bool,
) -> io::Result<()> {
    let seq = TEMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let tmp_name = format!(".tmp.{name}.{seq}");
    let tmp = CString::new(tmp_name).unwrap();
    create_write_leaf(base, &tmp, bytes)?;
    if fail_before_rename {
        // Clean the temp up so no orphan is left, then surface the failure.
        let _ = unlinkat_entry(base, &tmp);
        return Err(invalid("injected leaf-write failure before rename"));
    }
    let dst = CString::new(name.to_string()).unwrap();
    match renameat_beneath(base, &tmp, base, &dst) {
        Ok(()) => Ok(()),
        Err(err) => {
            // Rename failed: clean the temp so a retry starts fresh.
            let _ = unlinkat_entry(base, &tmp);
            Err(err)
        }
    }
}

fn invalid(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
}

// ---------------------------------------------------------------------------
// SIMULATED CAPABILITY CONTRACT.
//
// The trait below is the shape the design proposes `naust_storage_fs` expose
// additively (fd-relative, contained mutation anchored on the pinned root). It
// does not exist in the dependency today. The single libc-backed implementation
// proves the kernel mechanism; production would route through the dependency.
// ---------------------------------------------------------------------------

/// Additive, root-anchored mutation primitives the dependency must provide for a
/// coherent reaper. Modeled here; not yet implemented in `naust_storage_fs`.
trait RootRelativeMutator {
    /// Resolve and open a leaf beneath a pinned directory descriptor.
    fn open_leaf(&self, base: RawFd, rel: &CStr, flags: u64, mode: u64) -> io::Result<OwnedFd>;
    /// Remove a directory entry beneath a pinned directory descriptor.
    fn unlink_leaf(&self, base: RawFd, rel: &CStr) -> io::Result<()>;
}

/// The one real implementation: direct `*at` syscalls. Real-fs evidence only.
struct LibcRootRelative;

impl RootRelativeMutator for LibcRootRelative {
    fn open_leaf(&self, base: RawFd, rel: &CStr, flags: u64, mode: u64) -> io::Result<OwnedFd> {
        openat2_beneath(base, rel, flags, mode)
    }
    fn unlink_leaf(&self, base: RawFd, rel: &CStr) -> io::Result<()> {
        unlinkat_entry(base, rel)
    }
}

// ---------------------------------------------------------------------------
// Cooperative session lock (shared domain with writers) — STABLE IDENTITY.
//
// Production writers (`create_session`, `append_if_offset`, `begin_finalize`,
// `commit_finalize`, `session_status`, `recover_session`) and the reaper all
// `flock` the same `.lock.{uuid}` file. The prototype opens that lock file
// beneath the pinned `uploads` descriptor so the reaper shares the writers' lock
// domain and storage tree, not a reaper-only lock.
//
// STABLE-IDENTITY PROTOCOL (design requirement 1): the lock file is RETAINED —
// it is never unlinked as part of abort/cleanup. Production `abort_session`
// unlinks `.lock.{uuid}`, which lets an already-open waiter and a subsequent
// opener hold exclusive locks on two DIFFERENT inodes at the same name
// (`unlinking_lock_on_abort_splits_identity_and_breaks_exclusion`). Retaining
// the lock file preserves mutual exclusion
// (`stable_lock_identity_retained_file_preserves_mutual_exclusion_across_abort`).
// ---------------------------------------------------------------------------

/// Held flock on `.lock.{uuid}`, released on drop via explicit `LOCK_UN`. `flock`
/// is per-open-file-description, so the destructive helpers below must NOT
/// re-open/re-lock; they are "locked-inner" variants that RECEIVE this guard as
/// an explicit token (design requirement 3: locked-inner methods receive and
/// retain the matching lock, they do not merely omit a lock-acquisition line).
struct SessionLock {
    fd: OwnedFd,
    uuid: String,
}

impl SessionLock {
    /// The uuid this guard locks — used to assert a locked-inner call is invoked
    /// with the matching lock (prototype-level coherence check).
    fn uuid(&self) -> &str {
        &self.uuid
    }
}

impl Drop for SessionLock {
    fn drop(&mut self) {
        unsafe { libc::flock(self.fd.as_raw_fd(), libc::LOCK_UN) };
    }
}

/// Try to acquire the exclusive session lock without blocking, beneath the pinned
/// `uploads` descriptor. The lock file is created if absent and RETAINED.
/// `Ok(Some)` = acquired, `Ok(None)` = busy (a distinct, non-error outcome),
/// `Err` = a genuine failure.
fn try_lock_session(uploads_fd: RawFd, uuid: &str) -> io::Result<Option<SessionLock>> {
    let rel = CString::new(format!(".lock.{uuid}")).unwrap();
    let fd = openat2_beneath(
        uploads_fd,
        &rel,
        (libc::O_RDWR | libc::O_CREAT) as u64,
        0o600,
    )?;
    if flock_ex_nb(fd.as_raw_fd())? {
        Ok(Some(SessionLock {
            fd,
            uuid: uuid.to_string(),
        }))
    } else {
        Ok(None)
    }
}

// ---------------------------------------------------------------------------
// Minimal session/receipt records (prototype behaviour / representative).
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct MetaRecord {
    state: String,
    last_active: u64,
}

impl MetaRecord {
    fn to_json(&self) -> String {
        format!(
            "{{\"state\":\"{}\",\"last_active_at_unix_secs\":{}}}",
            self.state, self.last_active
        )
    }
    fn parse(text: &str) -> io::Result<Self> {
        // Deliberately tiny hand parser: absence is handled by the caller (an
        // open failure), corruption here surfaces as an error, never as absence.
        let state = extract_str(text, "\"state\":\"").ok_or_else(|| invalid("missing state"))?;
        let last_active = extract_u64(text, "\"last_active_at_unix_secs\":")
            .ok_or_else(|| invalid("missing last_active"))?;
        Ok(MetaRecord { state, last_active })
    }
}

struct ReceiptRecord {
    finalized_at: u64,
}

impl ReceiptRecord {
    fn to_json(&self) -> String {
        format!("{{\"finalized_at_unix_secs\":{}}}", self.finalized_at)
    }
    fn parse(text: &str) -> io::Result<Self> {
        let finalized_at = extract_u64(text, "\"finalized_at_unix_secs\":")
            .ok_or_else(|| invalid("missing finalized_at"))?;
        Ok(ReceiptRecord { finalized_at })
    }
}

fn extract_str(text: &str, key: &str) -> Option<String> {
    let start = text.find(key)? + key.len();
    let rest = &text[start..];
    let end = rest.find('"')?;
    Some(rest[..end].to_string())
}

fn extract_u64(text: &str, key: &str) -> Option<u64> {
    let start = text.find(key)? + key.len();
    let rest = &text[start..];
    let end = rest
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(rest.len());
    rest[..end].parse().ok()
}

// ---------------------------------------------------------------------------
// The pinned authority (prototype behaviour).
// ---------------------------------------------------------------------------

/// Outcome of the representative Finalizing-recovery roll-forward.
#[derive(Debug, PartialEq, Eq)]
enum FinalizingOutcome {
    /// CAS content matched; membership marker and receipt were written.
    RolledForward,
    /// A receipt already existed; recovery is idempotent and did nothing.
    AlreadyFinalized,
    /// The CAS blob was absent or size-mismatched; recovery published NOTHING
    /// (safe: it does not fabricate a receipt for content it cannot confirm).
    CasUnavailable,
}

/// The `uploads/`, `.finalized/`, `blobs/`, and repo-membership descriptors are
/// captured once. From then on, every inspection, lock, and mutation resolves
/// relative to these descriptors, so ancestor replacement (any of those
/// directories renamed aside and re-created) cannot redirect the pass onto a
/// fresh tree. `finalized`, `blobs`, and `membership` are optional: an absent
/// `.finalized/` must not prevent session processing (design requirement 6);
/// `blobs`/`membership` are pinned only for the representative recovery scenario.
struct PinnedUploadsAuthority {
    _root: OwnedFd,
    uploads: OwnedFd,
    finalized: Option<OwnedFd>,
    blobs: Option<OwnedFd>,
    membership: Option<OwnedFd>,
    mutator: LibcRootRelative,
    /// Test-only injection: when set, the Finalizing recovery's RECEIPT write
    /// fails AFTER membership has been written and BEFORE the receipt becomes
    /// visible. Models "membership success followed by receipt-write failure"
    /// (design requirement 3) so the retry-after-partial-success path is testable.
    receipt_write_fault: AtomicBool,
}

/// Pin a directory beneath `base`; `Ok(None)` on genuine absence (`ENOENT`),
/// `Err` on any other failure — absence is never conflated with an error.
fn pin_dir_opt(base: RawFd, name: &CStr) -> io::Result<Option<OwnedFd>> {
    match openat2_beneath(base, name, (libc::O_RDONLY | libc::O_DIRECTORY) as u64, 0) {
        Ok(fd) => Ok(Some(fd)),
        Err(err) if err.raw_os_error() == Some(libc::ENOENT) => Ok(None),
        Err(err) => Err(err),
    }
}

impl PinnedUploadsAuthority {
    /// Pin `uploads/` (required) and `.finalized/` (best-effort). `blobs`/
    /// `membership` are left unpinned; use [`open_full`] for recovery scenarios.
    fn open(root_path: &Path) -> io::Result<Self> {
        Self::open_inner(root_path, false)
    }

    /// Additionally pin `blobs/` and the repo-membership subtree, for the
    /// representative Finalizing-recovery scenario.
    fn open_full(root_path: &Path) -> io::Result<Self> {
        Self::open_inner(root_path, true)
    }

    fn open_inner(root_path: &Path, full: bool) -> io::Result<Self> {
        let root = open_root_opath(root_path)?;
        let uploads = openat2_beneath(
            root.as_raw_fd(),
            c"uploads",
            (libc::O_RDONLY | libc::O_DIRECTORY) as u64,
            0,
        )?;
        let finalized = pin_dir_opt(uploads.as_raw_fd(), c".finalized")?;
        let (blobs, membership) = if full {
            (
                pin_dir_opt(root.as_raw_fd(), c"blobs")?,
                pin_dir_opt(root.as_raw_fd(), c"membership")?,
            )
        } else {
            (None, None)
        };
        Ok(PinnedUploadsAuthority {
            _root: root,
            uploads,
            finalized,
            blobs,
            membership,
            mutator: LibcRootRelative,
            receipt_write_fault: AtomicBool::new(false),
        })
    }

    /// Test-only: arm/disarm the receipt-write fault used by the
    /// membership-then-receipt-failure and retry-after-partial-success scenarios.
    fn set_receipt_write_fault(&self, on: bool) {
        self.receipt_write_fault.store(on, Ordering::SeqCst);
    }

    fn uploads_fd(&self) -> RawFd {
        self.uploads.as_raw_fd()
    }
    fn finalized_fd(&self) -> Option<RawFd> {
        self.finalized.as_ref().map(|f| f.as_raw_fd())
    }
    fn blobs_fd(&self) -> io::Result<RawFd> {
        self.blobs
            .as_ref()
            .map(|f| f.as_raw_fd())
            .ok_or_else(|| invalid("blobs/ not pinned"))
    }
    fn membership_fd(&self) -> io::Result<RawFd> {
        self.membership
            .as_ref()
            .map(|f| f.as_raw_fd())
            .ok_or_else(|| invalid("membership/ not pinned"))
    }

    /// Inspect a session's metadata beneath the pinned `uploads` descriptor.
    /// Returns `(record, inode-identity)`. `Ok(None)` is genuine absence only
    /// (`ENOENT`); every other failure — including a symlink rejected by
    /// `RESOLVE_NO_SYMLINKS` — propagates as `Err`, never as absence.
    fn inspect_meta(&self, uuid: &str) -> io::Result<Option<(MetaRecord, (u64, u64))>> {
        let rel = CString::new(format!("{uuid}.meta.json")).unwrap();
        let fd = match self
            .mutator
            .open_leaf(self.uploads_fd(), &rel, libc::O_RDONLY as u64, 0)
        {
            Ok(fd) => fd,
            Err(err) if err.raw_os_error() == Some(libc::ENOENT) => return Ok(None),
            Err(err) => return Err(err),
        };
        let identity = fd_identity(fd.as_raw_fd())?;
        let text = read_fd_to_string(fd)?;
        let record = MetaRecord::parse(&text)?;
        Ok(Some((record, identity)))
    }

    /// Re-resolve the meta name under the held lock and confirm it still names the
    /// inode inspected earlier. Reduces — but, against a non-cooperating actor
    /// that ignores the lock, cannot eliminate — the inspect→act race
    /// (documented limitation in the design doc).
    fn revalidate_meta(&self, uuid: &str, expected: (u64, u64)) -> io::Result<bool> {
        self.revalidate_leaf(self.uploads_fd(), &format!("{uuid}.meta.json"), expected)
    }

    fn revalidate_leaf(&self, base: RawFd, name: &str, expected: (u64, u64)) -> io::Result<bool> {
        let rel = CString::new(name.to_string()).unwrap();
        let fd = match self.mutator.open_leaf(base, &rel, libc::O_PATH as u64, 0) {
            Ok(fd) => fd,
            Err(err) if err.raw_os_error() == Some(libc::ENOENT) => return Ok(false),
            Err(err) => return Err(err),
        };
        Ok(fd_identity(fd.as_raw_fd())? == expected)
    }

    /// LOCKED-INNER abort: RECEIVES the matching session lock as an explicit
    /// token and asserts it matches this uuid, then unlinks the session's
    /// data/hash/meta entries beneath the pinned `uploads` descriptor WITHOUT
    /// re-acquiring the lock. This is the boundary that removes the production
    /// reaper's drop-before-act window (production drops the lock so the
    /// re-locking `abort_session`/`recover_session` do not self-deadlock).
    ///
    /// DELETION ORDER (design requirement 6, crash discoverability): meta is
    /// removed LAST. If a crash interrupts the sequence, the `.meta.json` entry
    /// survives, so the next pass re-enumerates and re-aborts the session
    /// idempotently. Removing meta FIRST would orphan the `.data`/`.hash` entries
    /// (no longer enumerated by the `.meta.json`-keyed session loop). The lock
    /// file is RETAINED (stable identity), not unlinked.
    /// `ENOENT` is idempotent success, not an error.
    fn abort_locked(&self, lock: &SessionLock, uuid: &str) -> io::Result<()> {
        debug_assert_eq!(
            lock.uuid(),
            uuid,
            "abort_locked called with a mismatched lock"
        );
        for name in [
            format!("{uuid}.data"),
            format!("{uuid}.hash.0"),
            format!("{uuid}.meta.json"), // meta LAST — crash-discoverability.
        ] {
            let rel = CString::new(name).unwrap();
            match self.mutator.unlink_leaf(self.uploads_fd(), &rel) {
                Ok(()) => {}
                Err(err) if err.raw_os_error() == Some(libc::ENOENT) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(())
    }

    /// LOCKED-INNER recover for a torn tail (the production `Active`/else branch):
    /// truncate the data file back to a committed offset beneath the pinned
    /// `uploads` descriptor, no re-lock. This models ONLY the truncation branch;
    /// the Finalizing branch is [`recover_finalizing_locked`].
    fn recover_truncate_locked(
        &self,
        lock: &SessionLock,
        uuid: &str,
        committed_offset: u64,
    ) -> io::Result<()> {
        debug_assert_eq!(lock.uuid(), uuid, "recover called with a mismatched lock");
        let rel = CString::new(format!("{uuid}.data")).unwrap();
        let fd = self
            .mutator
            .open_leaf(self.uploads_fd(), &rel, libc::O_WRONLY as u64, 0)?;
        let res = unsafe { libc::ftruncate(fd.as_raw_fd(), committed_offset as libc::off_t) };
        if res < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// LOCKED-INNER Finalizing recovery — REPRESENTATIVE MODELING (design
    /// requirement 3). Production `recover_session`'s Finalizing branch is NOT a
    /// truncation: it (a) reads the session meta's finalizing info, (b) inspects
    /// the CAS blob for a size match, (c) links repository membership, (d) writes
    /// a finalized receipt, and it LEAVES the session meta in place (so a repeat
    /// pass is idempotent). This models that authority flow across three pinned
    /// subtrees — `uploads/` (meta read + receipt write via `.finalized/`),
    /// `blobs/` (CAS inspect), and the membership subtree (membership write) — so
    /// no step escapes the pinned authority. It uses a SIMPLIFIED flat CAS layout
    /// (`blobs/{digest}`) and a nested `membership/{repo}/{uuid}.membership` marker
    /// whose per-repo parent is created CONTAINED on demand; membership and receipt
    /// writes are ATOMIC LEAF REPLACEMENTS (temp + renameat). It is not evidence
    /// for the production CAS sharding, record formats, or fsync/durability.
    fn recover_finalizing_locked(
        &self,
        lock: &SessionLock,
        uuid: &str,
    ) -> io::Result<FinalizingOutcome> {
        debug_assert_eq!(lock.uuid(), uuid, "recover called with a mismatched lock");

        // Idempotency: a receipt already present means a prior recovery/commit
        // already published; do nothing.
        if self.inspect_receipt(uuid)?.is_some() {
            return Ok(FinalizingOutcome::AlreadyFinalized);
        }

        // (a) Read the finalizing meta (size + digest + repo) beneath pinned
        // uploads.
        let rel = CString::new(format!("{uuid}.meta.json")).unwrap();
        let fd = self
            .mutator
            .open_leaf(self.uploads_fd(), &rel, libc::O_RDONLY as u64, 0)?;
        let text = read_fd_to_string(fd)?;
        let size = extract_u64(&text, "\"size\":").ok_or_else(|| invalid("missing size"))?;
        let digest =
            extract_str(&text, "\"digest\":\"").ok_or_else(|| invalid("missing digest"))?;
        // The repo namespace routes the membership write; production resolves this
        // per-repo subtree (`<repo-blobs-dir>/…`). Modeled as a single segment.
        let repo = extract_str(&text, "\"repo\":\"").ok_or_else(|| invalid("missing repo"))?;

        // (b) CAS inspection beneath pinned blobs/. Absence or size mismatch =>
        // publish nothing.
        let blobs = self.blobs_fd()?;
        let crel = CString::new(digest.clone()).unwrap();
        let cas_fd = match self
            .mutator
            .open_leaf(blobs, &crel, libc::O_RDONLY as u64, 0)
        {
            Ok(fd) => fd,
            Err(err) if err.raw_os_error() == Some(libc::ENOENT) => {
                return Ok(FinalizingOutcome::CasUnavailable);
            }
            Err(err) => return Err(err),
        };
        if fd_len(cas_fd.as_raw_fd())? != size {
            return Ok(FinalizingOutcome::CasUnavailable);
        }

        // (c) Membership write beneath pinned membership/. The per-repo parent may
        // not exist yet: create it CONTAINED (mkdirat beneath the pinned membership
        // descriptor, then a containment-checked open — a planted symlink is
        // rejected), retaining authority. The write is an ATOMIC LEAF REPLACEMENT
        // (temp + renameat), so a concurrent/repeat pass never sees a half-written
        // membership record; a retry overwrites idempotently.
        let repo_dir = ensure_subdir_beneath(self.membership_fd()?, &CString::new(repo).unwrap())?;
        write_leaf_atomic(
            repo_dir.as_raw_fd(),
            &format!("{uuid}.membership"),
            format!("{{\"uuid\":\"{uuid}\",\"digest\":\"{digest}\",\"size\":{size}}}").as_bytes(),
            false,
        )?;

        // (d) Receipt write beneath pinned .finalized/, LAST, as an atomic leaf
        // replacement. Meta is LEFT in place (matches production; a repeat pass is
        // idempotent via the receipt check above). Ordering rationale: membership
        // is written BEFORE the receipt, so a crash between them leaves the receipt
        // absent — the next pass re-enters recovery (not AlreadyFinalized),
        // re-writes membership idempotently, and completes the receipt. The
        // `receipt_write_fault` injection exercises exactly that partial state.
        let finalized = self
            .finalized_fd()
            .ok_or_else(|| invalid("no .finalized/ to publish into"))?;
        write_leaf_atomic(
            finalized,
            &format!("{uuid}.json"),
            ReceiptRecord { finalized_at: NOW }.to_json().as_bytes(),
            self.receipt_write_fault.load(Ordering::SeqCst),
        )?;

        Ok(FinalizingOutcome::RolledForward)
    }

    fn inspect_receipt(&self, uuid: &str) -> io::Result<Option<(ReceiptRecord, (u64, u64))>> {
        let Some(finalized) = self.finalized_fd() else {
            return Ok(None);
        };
        let rel = CString::new(format!("{uuid}.json")).unwrap();
        let fd = match self
            .mutator
            .open_leaf(finalized, &rel, libc::O_RDONLY as u64, 0)
        {
            Ok(fd) => fd,
            Err(err) if err.raw_os_error() == Some(libc::ENOENT) => return Ok(None),
            Err(err) => return Err(err),
        };
        let identity = fd_identity(fd.as_raw_fd())?;
        let text = read_fd_to_string(fd)?;
        Ok(Some((ReceiptRecord::parse(&text)?, identity)))
    }

    fn revalidate_receipt(&self, uuid: &str, expected: (u64, u64)) -> io::Result<bool> {
        let Some(finalized) = self.finalized_fd() else {
            return Ok(false);
        };
        self.revalidate_leaf(finalized, &format!("{uuid}.json"), expected)
    }

    /// LOCKED-INNER receipt deletion: RECEIVES the matching session lock token
    /// (the same `.lock.{uuid}` domain the receipt writers hold). `ENOENT` is
    /// idempotent success.
    fn unlink_receipt_locked(&self, lock: &SessionLock, uuid: &str) -> io::Result<()> {
        debug_assert_eq!(
            lock.uuid(),
            uuid,
            "unlink_receipt called with a mismatched lock"
        );
        let Some(finalized) = self.finalized_fd() else {
            return Ok(());
        };
        let rel = CString::new(format!("{uuid}.json")).unwrap();
        match self.mutator.unlink_leaf(finalized, &rel) {
            Ok(()) => Ok(()),
            Err(err) if err.raw_os_error() == Some(libc::ENOENT) => Ok(()),
            Err(err) => Err(err),
        }
    }
}

// ---------------------------------------------------------------------------
// The prototype reaper pass.
// ---------------------------------------------------------------------------

/// Honest outcome of a pass. Unlike the production reaper's single attempt
/// counter, this separates confirmed successes, busy-skips, safe rejections, and
/// surfaced errors, and distinguishes a FATAL top-level failure (e.g. the
/// uploads enumeration itself failed) from per-candidate errors (design
/// requirement 6: outcome mapping).
#[derive(Default, Debug)]
struct ReapReport {
    successes: usize,
    skipped_busy: usize,
    skipped_not_expired: usize,
    rejected_changed: usize,
    /// Finalizing recovery found a receipt already present (idempotent no-op) — not
    /// a fresh success this pass.
    skipped_already_final: usize,
    /// Finalizing recovery could not confirm CAS content (absent/size mismatch);
    /// it published nothing and will retry on a later pass — not a success, not an
    /// error.
    skipped_incomplete: usize,
    errors: Vec<String>,
    fatal: Option<String>,
}

/// Maps the prototype report onto the production public contract
/// `Result<usize, StorageError>` (design requirement 6). A FATAL top-level
/// failure returns `Err` (the pass could not be performed; the caller retries on
/// its next schedule). Per-candidate errors do NOT erase earlier confirmed
/// successes: the pass returns `Ok(successes)` and surfaces per-item errors out
/// of band (logs/metrics in production). "Confirmed success" = the intended
/// terminal effect was achieved, INCLUDING an idempotent `ENOENT` on a
/// re-unlink; it does NOT count a busy-skip or a safe rejection.
fn report_to_public_result(report: &ReapReport) -> Result<usize, String> {
    match &report.fatal {
        Some(msg) => Err(msg.clone()),
        None => Ok(report.successes),
    }
}

/// Optional hook fired AFTER inspection+identity-capture and BEFORE the
/// destructive action, while the session lock is held. Used only by the
/// inspect→act scenarios to inject a deterministic mid-pass change; `None` in
/// every other scenario.
type MidPassHook<'a> = Option<&'a mut dyn FnMut(&PinnedUploadsAuthority, &str)>;

fn prototype_reap(
    auth: &PinnedUploadsAuthority,
    now: u64,
    max_age_secs: u64,
    receipt_ttl_secs: u64,
    mut hook: MidPassHook<'_>,
) -> ReapReport {
    let mut report = ReapReport::default();

    // 1. Sessions. An enumeration failure here is FATAL for the pass.
    let entries = match list_dir(auth.uploads_fd()) {
        Ok(e) => e,
        Err(err) => {
            report.fatal = Some(format!("enumerate uploads: {err}"));
            return report;
        }
    };
    for name in &entries {
        let Some(uuid) = name.strip_suffix(".meta.json") else {
            continue;
        };

        // Acquire the shared session lock (non-blocking). Busy is a skip, not an
        // error, and is distinct from a genuine lock failure.
        let lock = match try_lock_session(auth.uploads_fd(), uuid) {
            Ok(Some(lock)) => lock,
            Ok(None) => {
                report.skipped_busy += 1;
                continue;
            }
            Err(err) => {
                report.errors.push(format!("{uuid}: lock: {err}"));
                continue;
            }
        };

        // Inspect UNDER the lock.
        let (meta, identity) = match auth.inspect_meta(uuid) {
            Ok(Some(v)) => v,
            Ok(None) => continue, // vanished after enumeration; nothing to do.
            Err(err) => {
                // Inspection failure => NO destructive action for this candidate.
                report.errors.push(format!("{uuid}: inspect: {err}"));
                continue;
            }
        };

        if now.saturating_sub(meta.last_active) < max_age_secs {
            report.skipped_not_expired += 1;
            continue;
        }

        // Deterministic mid-pass mutation injection (inspect→act scenarios only).
        if let Some(hook) = hook.as_deref_mut() {
            hook(auth, uuid);
        }

        // Revalidate identity under the still-held lock before acting.
        match auth.revalidate_meta(uuid, identity) {
            Ok(true) => {}
            Ok(false) => {
                report.rejected_changed += 1;
                continue; // safe rejection: the entry changed; do not delete.
            }
            Err(err) => {
                report.errors.push(format!("{uuid}: revalidate: {err}"));
                continue;
            }
        }

        // LOCKED-INNER destructive dispatch — no drop, no re-lock. An expired
        // Finalizing session is routed through the representative ROLL-FORWARD
        // (recover_finalizing_locked), NOT truncate-to-zero: it inspects CAS,
        // links membership, and writes a receipt across pinned subtrees. Every
        // other expired state aborts. Outcomes map to the honest counters:
        // RolledForward => success; AlreadyFinalized/CasUnavailable => distinct
        // skips (nothing terminal published this pass); Err => surfaced error.
        if meta.state == "Finalizing" {
            match auth.recover_finalizing_locked(&lock, uuid) {
                Ok(FinalizingOutcome::RolledForward) => report.successes += 1,
                Ok(FinalizingOutcome::AlreadyFinalized) => report.skipped_already_final += 1,
                Ok(FinalizingOutcome::CasUnavailable) => report.skipped_incomplete += 1,
                Err(err) => report.errors.push(format!("{uuid}: recover: {err}")),
            }
        } else {
            match auth.abort_locked(&lock, uuid) {
                Ok(()) => report.successes += 1,
                Err(err) => report.errors.push(format!("{uuid}: act: {err}")),
            }
        }
        // lock dropped here.
    }

    // 2. Finalized receipts. An ABSENT `.finalized/` is not an error and did not
    // block the session loop above (design requirement 6). Each receipt is
    // deleted under the SAME `.lock.{uuid}` domain its writers hold, held
    // continuously across inspect → expiry → revalidate → unlink.
    let Some(finalized_fd) = auth.finalized_fd() else {
        return report; // no receipt directory: sessions already processed.
    };
    let receipts = match list_dir(finalized_fd) {
        Ok(e) => e,
        Err(err) => {
            report.errors.push(format!("enumerate finalized: {err}"));
            return report;
        }
    };
    for name in &receipts {
        let Some(uuid) = name.strip_suffix(".json") else {
            continue;
        };

        // Carry the session lock across the whole receipt boundary.
        let lock = match try_lock_session(auth.uploads_fd(), uuid) {
            Ok(Some(lock)) => lock,
            Ok(None) => {
                report.skipped_busy += 1;
                continue;
            }
            Err(err) => {
                report.errors.push(format!("{uuid}: receipt lock: {err}"));
                continue;
            }
        };

        let (receipt, identity) = match auth.inspect_receipt(uuid) {
            Ok(Some(v)) => v,
            Ok(None) => continue, // vanished after enumeration.
            Err(err) => {
                report
                    .errors
                    .push(format!("{uuid}: receipt inspect: {err}"));
                continue;
            }
        };

        if now.saturating_sub(receipt.finalized_at) < receipt_ttl_secs {
            report.skipped_not_expired += 1;
            continue;
        }

        match auth.revalidate_receipt(uuid, identity) {
            Ok(true) => {}
            Ok(false) => {
                report.rejected_changed += 1;
                continue; // the receipt changed under us; do not delete.
            }
            Err(err) => {
                report
                    .errors
                    .push(format!("{uuid}: receipt revalidate: {err}"));
                continue;
            }
        }

        match auth.unlink_receipt_locked(&lock, uuid) {
            Ok(()) => report.successes += 1,
            Err(err) => report.errors.push(format!("{uuid}: receipt unlink: {err}")),
        }
        // lock dropped here.
    }

    report
}

// ---------------------------------------------------------------------------
// Test fixtures.
// ---------------------------------------------------------------------------

const HOUR: u64 = 3600;
const NOW: u64 = 1_000_000_000;

/// Build `<parent>/<name>/uploads/.finalized` and return the tree root path.
fn make_tree(parent: &Path, name: &str) -> std::path::PathBuf {
    let root = parent.join(name);
    std::fs::create_dir_all(root.join("uploads").join(".finalized")).unwrap();
    root
}

fn write_session(root: &Path, uuid: &str, state: &str, last_active: u64) {
    let uploads = root.join("uploads");
    std::fs::write(uploads.join(format!("{uuid}.data")), b"payload-bytes").unwrap();
    std::fs::write(
        uploads.join(format!("{uuid}.meta.json")),
        MetaRecord {
            state: state.to_string(),
            last_active,
        }
        .to_json(),
    )
    .unwrap();
    std::fs::write(uploads.join(format!("{uuid}.hash.0")), b"hashstate").unwrap();
}

fn write_receipt(root: &Path, uuid: &str, finalized_at: u64) {
    std::fs::write(
        root.join("uploads")
            .join(".finalized")
            .join(format!("{uuid}.json")),
        ReceiptRecord { finalized_at }.to_json(),
    )
    .unwrap();
}

fn exists(root: &Path, rel: &str) -> bool {
    root.join(rel).exists()
}

/// Create `blobs/` + `membership/` and write a Finalizing session meta carrying
/// `size`/`digest`/`repo`, for the representative roll-forward scenarios.
fn write_finalizing_session(
    root: &Path,
    uuid: &str,
    repo: &str,
    digest: &str,
    size: u64,
    last_active: u64,
) {
    std::fs::create_dir_all(root.join("blobs")).unwrap();
    std::fs::create_dir_all(root.join("membership")).unwrap();
    std::fs::write(
        root.join("uploads").join(format!("{uuid}.meta.json")),
        format!(
            "{{\"state\":\"Finalizing\",\"last_active_at_unix_secs\":{last_active},\"size\":{size},\"digest\":\"{digest}\",\"repo\":\"{repo}\"}}"
        ),
    )
    .unwrap();
}

/// Write a flat CAS blob `blobs/{digest}` with the given bytes.
fn write_cas_blob(root: &Path, digest: &str, bytes: &[u8]) {
    std::fs::create_dir_all(root.join("blobs")).unwrap();
    std::fs::write(root.join("blobs").join(digest), bytes).unwrap();
}

// ===========================================================================
// SECTION A — single-tree coherence (REAL FILESYSTEM EVIDENCE). Unchanged from
// the prior prototype; ancestor replacement cannot redirect a pinned pass.
// ===========================================================================

/// Root replacement before cleanup: the pinned pass acts ONLY on the detached
/// original tree; the fresh same-named replacement tree is never touched.
#[test]
fn root_replacement_acts_only_on_detached_tree() {
    let parent = tempfile::tempdir().unwrap();
    let root = make_tree(parent.path(), "root");
    write_session(&root, "aaaa", "Active", NOW - 10 * HOUR); // expired
    write_receipt(&root, "aaaa", NOW - 10 * HOUR); // expired

    let auth = PinnedUploadsAuthority::open(&root).unwrap();

    let detached = parent.path().join("root_old");
    std::fs::rename(&root, &detached).unwrap();
    let fresh = make_tree(parent.path(), "root");
    write_session(&fresh, "aaaa", "Active", NOW); // fresh, not expired
    write_receipt(&fresh, "aaaa", NOW); // fresh, not expired

    let report = prototype_reap(&auth, NOW, HOUR, HOUR, None);

    assert!(!exists(&detached, "uploads/aaaa.meta.json"));
    assert!(!exists(&detached, "uploads/aaaa.data"));
    assert!(!exists(&detached, "uploads/.finalized/aaaa.json"));
    assert!(exists(&fresh, "uploads/aaaa.meta.json"));
    assert!(exists(&fresh, "uploads/aaaa.data"));
    assert!(exists(&fresh, "uploads/.finalized/aaaa.json"));
    assert_eq!(
        report.successes, 2,
        "one session + one receipt on detached tree"
    );
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(report.fatal.is_none());
}

/// `uploads/` replaced beneath a stable root: the pinned `uploads` descriptor
/// still names the detached inode; the fresh `uploads/` tree is untouched.
#[test]
fn uploads_dir_ancestor_replacement_stays_on_detached_inode() {
    let parent = tempfile::tempdir().unwrap();
    let root = make_tree(parent.path(), "root");
    write_session(&root, "bbbb", "Active", NOW - 10 * HOUR);

    let auth = PinnedUploadsAuthority::open(&root).unwrap();

    std::fs::rename(root.join("uploads"), root.join("uploads_old")).unwrap();
    std::fs::create_dir_all(root.join("uploads").join(".finalized")).unwrap();
    write_session(&root, "bbbb", "Active", NOW);

    let report = prototype_reap(&auth, NOW, HOUR, HOUR, None);

    assert!(!exists(&root, "uploads_old/bbbb.meta.json"));
    assert!(
        exists(&root, "uploads/bbbb.meta.json"),
        "fresh uploads/ survives"
    );
    assert_eq!(report.successes, 1);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}

/// `.finalized/` replaced beneath a stable `uploads/`: the pinned `.finalized`
/// descriptor still names the detached inode.
#[test]
fn finalized_dir_ancestor_replacement_stays_on_detached_inode() {
    let parent = tempfile::tempdir().unwrap();
    let root = make_tree(parent.path(), "root");
    write_receipt(&root, "cccc", NOW - 10 * HOUR);

    let auth = PinnedUploadsAuthority::open(&root).unwrap();

    let finalized = root.join("uploads").join(".finalized");
    std::fs::rename(&finalized, root.join("uploads").join(".finalized_old")).unwrap();
    std::fs::create_dir_all(&finalized).unwrap();
    write_receipt(&root, "cccc", NOW);

    let report = prototype_reap(&auth, NOW, HOUR, HOUR, None);

    assert!(!exists(&root, "uploads/.finalized_old/cccc.json"));
    assert!(
        exists(&root, "uploads/.finalized/cccc.json"),
        "fresh receipt survives"
    );
    assert_eq!(report.successes, 1);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}

/// Expired old receipt vs a fresh same-UUID receipt after root replacement: only
/// the detached-tree receipt is removed.
#[test]
fn expired_old_receipt_vs_fresh_same_uuid() {
    let parent = tempfile::tempdir().unwrap();
    let root = make_tree(parent.path(), "root");
    write_receipt(&root, "dddd", NOW - 10 * HOUR);

    let auth = PinnedUploadsAuthority::open(&root).unwrap();

    let detached = parent.path().join("root_old");
    std::fs::rename(&root, &detached).unwrap();
    let fresh = make_tree(parent.path(), "root");
    write_receipt(&fresh, "dddd", NOW);

    let report = prototype_reap(&auth, NOW, HOUR, HOUR, None);

    assert!(!exists(&detached, "uploads/.finalized/dddd.json"));
    assert!(exists(&fresh, "uploads/.finalized/dddd.json"));
    assert_eq!(report.successes, 1);
}

/// Expired old session vs an active replacement, plus in-tree age selectivity.
#[test]
fn expired_old_session_vs_active_replacement_and_age_selectivity() {
    let parent = tempfile::tempdir().unwrap();
    let root = make_tree(parent.path(), "root");
    write_session(&root, "eeee", "Active", NOW - 10 * HOUR); // expired
    write_session(&root, "ffff", "Active", NOW); // active, not expired

    let auth = PinnedUploadsAuthority::open(&root).unwrap();

    let report = prototype_reap(&auth, NOW, HOUR, HOUR, None);

    assert!(!exists(&root, "uploads/eeee.meta.json"), "expired aborted");
    assert!(!exists(&root, "uploads/eeee.data"));
    assert!(
        exists(&root, "uploads/ffff.meta.json"),
        "active session preserved"
    );
    assert!(exists(&root, "uploads/ffff.data"));
    assert_eq!(report.successes, 1);
    assert_eq!(report.skipped_not_expired, 1);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}

// ===========================================================================
// SECTION B — stable lock identity (REAL FILESYSTEM EVIDENCE). Design req. 1.
// ===========================================================================

/// STABLE IDENTITY: with the lock file RETAINED (never unlinked on abort), an
/// already-open waiter and a subsequent opener across the abort boundary resolve
/// the SAME inode, so `flock` still mutually excludes them. This is the proposed
/// protocol.
#[test]
fn stable_lock_identity_retained_file_preserves_mutual_exclusion_across_abort() {
    let parent = tempfile::tempdir().unwrap();
    let root = make_tree(parent.path(), "root");
    let auth = PinnedUploadsAuthority::open(&root).unwrap();
    let up = auth.uploads_fd();
    let rel = CString::new(".lock.aaaa").unwrap();

    // Already-open waiter (opened before the abort boundary, not yet locked).
    let waiter = openat2_beneath(up, &rel, (libc::O_RDWR | libc::O_CREAT) as u64, 0o600).unwrap();
    let id_waiter = fd_identity(waiter.as_raw_fd()).unwrap();

    // Abort happens but RETAINS the lock file: nothing unlinks `.lock.aaaa`.

    // Subsequent opener after the abort boundary resolves the same inode.
    let opener = openat2_beneath(up, &rel, (libc::O_RDWR | libc::O_CREAT) as u64, 0o600).unwrap();
    let id_opener = fd_identity(opener.as_raw_fd()).unwrap();
    assert_eq!(
        id_waiter, id_opener,
        "retained lock file keeps a single inode identity"
    );

    // Mutual exclusion preserved: waiter locks; the opener is denied.
    assert!(flock_ex_nb(waiter.as_raw_fd()).unwrap(), "waiter acquires");
    assert!(
        !flock_ex_nb(opener.as_raw_fd()).unwrap(),
        "second opener is correctly excluded on the same inode"
    );
    unsafe { libc::flock(waiter.as_raw_fd(), libc::LOCK_UN) };
}

/// THE DEFECT (real-kernel): production `abort_session` UNLINKS `.lock.{uuid}`.
/// An already-open waiter (old inode) and a subsequent opener (fresh inode via
/// `O_CREAT`) then BOTH acquire the exclusive lock — a cooperative-concurrency
/// defect, not merely the documented limitation against hostile actors. This
/// test proves the split the stable-identity protocol prevents.
#[test]
fn unlinking_lock_on_abort_splits_identity_and_breaks_exclusion() {
    let parent = tempfile::tempdir().unwrap();
    let root = make_tree(parent.path(), "root");
    let auth = PinnedUploadsAuthority::open(&root).unwrap();
    let up = auth.uploads_fd();
    let rel = CString::new(".lock.aaaa").unwrap();

    // Already-open waiter on old inode X.
    let waiter = openat2_beneath(up, &rel, (libc::O_RDWR | libc::O_CREAT) as u64, 0o600).unwrap();
    let id_x = fd_identity(waiter.as_raw_fd()).unwrap();

    // Abort UNLINKS the lock file (models current production abort_session).
    unlinkat_entry(up, &rel).unwrap();

    // Subsequent opener creates a fresh inode Y at the same name.
    let opener = openat2_beneath(up, &rel, (libc::O_RDWR | libc::O_CREAT) as u64, 0o600).unwrap();
    let id_y = fd_identity(opener.as_raw_fd()).unwrap();
    assert_ne!(
        id_x, id_y,
        "unlink+recreate produced a different lock inode"
    );

    // BOTH can hold the exclusive lock simultaneously => exclusion broken.
    assert!(
        flock_ex_nb(waiter.as_raw_fd()).unwrap(),
        "waiter holds inode X"
    );
    assert!(
        flock_ex_nb(opener.as_raw_fd()).unwrap(),
        "second holder also acquired inode Y => split lock domain (the defect)"
    );
    unsafe { libc::flock(waiter.as_raw_fd(), libc::LOCK_UN) };
    unsafe { libc::flock(opener.as_raw_fd(), libc::LOCK_UN) };
}

/// AMBIENT LOCK PATH splits the domain across ancestor replacement (design
/// req. 4): a writer that resolves `.lock.{uuid}` by pathname before `uploads/`
/// is replaced locks the OLD inode; a second actor resolving the same pathname
/// after replacement locks a DIFFERENT inode under the NEW `uploads/`. Identical
/// lock FILENAMES, different lock DOMAINS. A pinned descriptor keeps one domain.
#[test]
fn ambient_lock_path_splits_domain_across_uploads_replacement() {
    let parent = tempfile::tempdir().unwrap();
    let root = make_tree(parent.path(), "root");
    let lock_path = root.join("uploads").join(".lock.aaaa");

    // Writer A: ambient open + flock on the OLD uploads inode.
    let a = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .unwrap();
    assert!(
        flock_ex_nb(a.as_raw_fd()).unwrap(),
        "A holds the old-inode lock"
    );

    // uploads/ replaced beneath the stable root.
    std::fs::rename(root.join("uploads"), root.join("uploads_old")).unwrap();
    std::fs::create_dir_all(root.join("uploads").join(".finalized")).unwrap();

    // Actor B: the SAME ambient pathname now resolves the NEW uploads inode.
    let b = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .unwrap();
    assert!(
        flock_ex_nb(b.as_raw_fd()).unwrap(),
        "ambient path split across replacement => two 'session lock' holders (identical filename, different domain)"
    );

    // Contrast: beneath a PINNED uploads descriptor both resolve one inode.
    let auth = PinnedUploadsAuthority::open(&root).unwrap(); // pins the NEW uploads
    let l1 = try_lock_session(auth.uploads_fd(), "gggg")
        .unwrap()
        .unwrap();
    let l2 = try_lock_session(auth.uploads_fd(), "gggg").unwrap();
    assert!(
        l2.is_none(),
        "pinned-descriptor lock domain preserves exclusion"
    );
    drop(l1);
    unsafe { libc::flock(a.as_raw_fd(), libc::LOCK_UN) };
    unsafe { libc::flock(b.as_raw_fd(), libc::LOCK_UN) };
}

/// DETERMINISTIC REGRESSION (design requirement 1): the previously-proposed
/// ONLINE lock-reclamation sweep — "acquire the lock, confirm no session
/// artifacts, then unlink the lock file" — is UNSAFE. Acquiring the lock and
/// finding no artifacts does NOT prove there are no other open descriptors on the
/// lock inode: a process can `open()` the lock file (an unacquired WAITER) before
/// the sweep runs. This proves the sweep can split lock identity DESPITE a
/// successful acquisition AND artifact absence — so reclamation is safe only under
/// global quiescence (no holders, no waiters, no new openers), never online.
#[test]
fn online_reclamation_sweep_splits_lock_identity_despite_acquisition_and_artifact_absence() {
    let parent = tempfile::tempdir().unwrap();
    let root = make_tree(parent.path(), "root");
    let auth = PinnedUploadsAuthority::open(&root).unwrap();
    let up = auth.uploads_fd();
    let rel = CString::new(".lock.aaaa").unwrap();
    // No session artifacts exist for "aaaa": the sweep's absence check will pass.

    // A pre-existing UNACQUIRED WAITER opens the lock inode X (has an fd, has not
    // yet flocked). This is the descriptor the sweep can neither see nor exclude.
    let waiter = openat2_beneath(up, &rel, (libc::O_RDWR | libc::O_CREAT) as u64, 0o600).unwrap();
    let id_x = fd_identity(waiter.as_raw_fd()).unwrap();

    // The (unsafe) ONLINE sweep: acquire, confirm absence, unlink, release.
    let swept = {
        let lock = try_lock_session(up, "aaaa")
            .unwrap()
            .expect("sweep acquires");
        assert!(
            auth.inspect_meta("aaaa").unwrap().is_none(),
            "sweep confirms no session artifacts"
        );
        unlinkat_entry(up, &rel).unwrap(); // <-- the unsafe reclamation
        drop(lock);
        true
    };
    assert!(
        swept,
        "the sweep acquired the lock AND saw artifact absence"
    );

    // A new opener now creates a FRESH inode Y at the same name.
    let opener = openat2_beneath(up, &rel, (libc::O_RDWR | libc::O_CREAT) as u64, 0o600).unwrap();
    let id_y = fd_identity(opener.as_raw_fd()).unwrap();
    assert_ne!(id_x, id_y, "sweep's unlink+recreate split the inode");

    // The pre-existing waiter and the new opener BOTH acquire "the session lock":
    // split identity despite the sweep's success and artifact absence.
    assert!(
        flock_ex_nb(waiter.as_raw_fd()).unwrap(),
        "old waiter locks inode X"
    );
    assert!(
        flock_ex_nb(opener.as_raw_fd()).unwrap(),
        "new opener locks inode Y => split domain (the online sweep is unsafe)"
    );
    unsafe { libc::flock(waiter.as_raw_fd(), libc::LOCK_UN) };
    unsafe { libc::flock(opener.as_raw_fd(), libc::LOCK_UN) };
}

// ===========================================================================
// SECTION C — lock/act coherence & busy handling (PROTOTYPE BEHAVIOUR).
// ===========================================================================

/// Busy lock: a held exclusive lock makes the candidate a non-error skip; the
/// record survives the pass, and a retry after release cleans it.
#[test]
fn busy_lock_is_skipped_not_deleted() {
    let parent = tempfile::tempdir().unwrap();
    let root = make_tree(parent.path(), "root");
    write_session(&root, "aaaa", "Active", NOW - 10 * HOUR);

    let auth = PinnedUploadsAuthority::open(&root).unwrap();
    let holder = try_lock_session(auth.uploads_fd(), "aaaa")
        .unwrap()
        .unwrap();

    let report = prototype_reap(&auth, NOW, HOUR, HOUR, None);
    assert_eq!(report.skipped_busy, 1);
    assert_eq!(report.successes, 0);
    assert!(
        exists(&root, "uploads/aaaa.meta.json"),
        "held session not reaped"
    );

    drop(holder);
    let report2 = prototype_reap(&auth, NOW, HOUR, HOUR, None);
    assert_eq!(report2.successes, 1);
    assert!(!exists(&root, "uploads/aaaa.meta.json"));
}

/// Locked-inner ops receive the lock token and do not re-acquire the lock: the
/// reaper holds the lock and acts while holding it, with no self-deadlock.
#[test]
fn locked_inner_ops_do_not_reacquire_lock() {
    let parent = tempfile::tempdir().unwrap();
    let root = make_tree(parent.path(), "root");
    write_session(&root, "aaaa", "Active", NOW - 10 * HOUR);

    let auth = PinnedUploadsAuthority::open(&root).unwrap();
    let lock = try_lock_session(auth.uploads_fd(), "aaaa")
        .unwrap()
        .unwrap();
    auth.abort_locked(&lock, "aaaa").unwrap();
    drop(lock);

    assert!(!exists(&root, "uploads/aaaa.meta.json"));
    assert!(!exists(&root, "uploads/aaaa.data"));
    assert!(!exists(&root, "uploads/aaaa.hash.0"));
}

/// Candidate changes between inspection and action: under the held lock,
/// revalidation detects the identity change and SAFELY REJECTS the deletion.
#[test]
fn candidate_change_between_inspect_and_action_is_rejected() {
    let parent = tempfile::tempdir().unwrap();
    let root = make_tree(parent.path(), "root");
    write_session(&root, "aaaa", "Active", NOW - 10 * HOUR);

    let auth = PinnedUploadsAuthority::open(&root).unwrap();

    let mut swapped = false;
    let mut hook = |_a: &PinnedUploadsAuthority, uuid: &str| {
        let meta = root.join("uploads").join(format!("{uuid}.meta.json"));
        // Detach (not delete): the old inode stays occupied so the swapped-in
        // candidate is guaranteed a new identity even under ext4 inode reuse.
        let detached = meta.with_extension("json.detached-swap");
        std::fs::rename(&meta, &detached).unwrap();
        std::fs::write(
            &meta,
            MetaRecord {
                state: "Active".into(),
                last_active: NOW,
            }
            .to_json(),
        )
        .unwrap();
        swapped = true;
    };

    let report = prototype_reap(&auth, NOW, HOUR, HOUR, Some(&mut hook));

    assert!(swapped);
    assert_eq!(
        report.rejected_changed, 1,
        "identity change must be rejected"
    );
    assert_eq!(report.successes, 0);
    assert!(exists(&root, "uploads/aaaa.meta.json"));
}

/// Failure before the destructive action: an inspection that fails closed (a
/// symlinked meta rejected by `RESOLVE_NO_SYMLINKS`) yields an error and NO
/// unlink — the payload survives.
#[test]
fn inspection_failure_performs_no_destructive_action() {
    let parent = tempfile::tempdir().unwrap();
    let root = make_tree(parent.path(), "root");
    let uploads = root.join("uploads");
    std::fs::write(uploads.join("aaaa.data"), b"payload-bytes").unwrap();
    std::fs::write(uploads.join("aaaa.hash.0"), b"hashstate").unwrap();
    std::fs::write(
        uploads.join("target.json"),
        MetaRecord {
            state: "Active".into(),
            last_active: NOW - 10 * HOUR,
        }
        .to_json(),
    )
    .unwrap();
    std::os::unix::fs::symlink("target.json", uploads.join("aaaa.meta.json")).unwrap();

    let auth = PinnedUploadsAuthority::open(&root).unwrap();
    let report = prototype_reap(&auth, NOW, HOUR, HOUR, None);

    assert_eq!(report.successes, 0);
    assert_eq!(
        report.errors.len(),
        1,
        "inspection error surfaced: {:?}",
        report.errors
    );
    assert!(exists(&root, "uploads/aaaa.data"));
    assert!(exists(&root, "uploads/aaaa.hash.0"));
}

// ===========================================================================
// SECTION D — receipt inspect→delete boundary under the shared lock.
// Design requirement 2. (PROTOTYPE BEHAVIOUR.)
// ===========================================================================

/// A busy session lock (held by a concurrent finalize/recover writer) makes an
/// expired receipt a non-error skip — the receipt is NOT deleted out from under
/// the writer that shares its `.lock.{uuid}` domain.
#[test]
fn receipt_busy_lock_is_skipped_not_deleted() {
    let parent = tempfile::tempdir().unwrap();
    let root = make_tree(parent.path(), "root");
    write_receipt(&root, "aaaa", NOW - 10 * HOUR); // expired

    let auth = PinnedUploadsAuthority::open(&root).unwrap();
    // A writer holds the shared session lock for this uuid.
    let holder = try_lock_session(auth.uploads_fd(), "aaaa")
        .unwrap()
        .unwrap();

    let report = prototype_reap(&auth, NOW, HOUR, HOUR, None);
    assert_eq!(report.skipped_busy, 1);
    assert_eq!(report.successes, 0);
    assert!(
        exists(&root, "uploads/.finalized/aaaa.json"),
        "held receipt not reaped"
    );

    drop(holder);
    let report2 = prototype_reap(&auth, NOW, HOUR, HOUR, None);
    assert_eq!(report2.successes, 1);
    assert!(!exists(&root, "uploads/.finalized/aaaa.json"));
}

/// A receipt replaced (rebind to a fresh inode) between inspection and action is
/// safely rejected under the held lock, and a fresh same-UUID receipt is
/// preserved. Note (design requirement 2): `unlinkat` removes a NAME; the held
/// lock plus identity revalidation is what prevents deleting the replacement.
#[test]
fn receipt_replacement_between_inspect_and_action_is_rejected() {
    let parent = tempfile::tempdir().unwrap();
    let root = make_tree(parent.path(), "root");
    write_receipt(&root, "aaaa", NOW - 10 * HOUR); // expired -> eligible

    let auth = PinnedUploadsAuthority::open(&root).unwrap();

    // There are no sessions, so the receipt loop runs; inject the swap via a
    // manual replay of the receipt boundary to hit the revalidation precisely.
    let lock = try_lock_session(auth.uploads_fd(), "aaaa")
        .unwrap()
        .unwrap();
    let (_r, identity) = auth.inspect_receipt("aaaa").unwrap().unwrap();

    // A cooperating writer rebinds the receipt to a fresh (non-expired) inode.
    // ext4 reuses freed inode numbers immediately; hold the freed inode with a
    // keeper so the rewritten receipt is a genuinely new identity.
    let receipt = root.join("uploads").join(".finalized").join("aaaa.json");
    // Detach (not delete): the old inode must stay occupied so the rewritten
    // receipt is guaranteed a new identity even under ext4 inode reuse.
    let detached = receipt.with_file_name("aaaa.json.detached-swap");
    std::fs::rename(&receipt, &detached).unwrap();
    std::fs::write(&receipt, ReceiptRecord { finalized_at: NOW }.to_json()).unwrap();

    assert!(
        !auth.revalidate_receipt("aaaa", identity).unwrap(),
        "identity change detected"
    );
    // Because revalidation failed, we do NOT unlink. The fresh receipt survives.
    drop(lock);
    assert!(exists(&root, "uploads/.finalized/aaaa.json"));
}

/// A receipt that DISAPPEARS between enumeration and inspection is a clean no-op
/// (idempotent), not an error or a false success.
#[test]
fn receipt_disappearance_is_idempotent_noop() {
    let parent = tempfile::tempdir().unwrap();
    let root = make_tree(parent.path(), "root");
    write_receipt(&root, "aaaa", NOW - 10 * HOUR);

    let auth = PinnedUploadsAuthority::open(&root).unwrap();

    // Model the disappearance: remove the receipt, then run a pass. The pass
    // enumerated nothing for it, and a direct locked inspect yields None.
    let lock = try_lock_session(auth.uploads_fd(), "aaaa")
        .unwrap()
        .unwrap();
    std::fs::remove_file(root.join("uploads").join(".finalized").join("aaaa.json")).unwrap();
    assert!(
        auth.inspect_receipt("aaaa").unwrap().is_none(),
        "absence, not error"
    );
    // A locked unlink of the now-absent receipt is idempotent success.
    auth.unlink_receipt_locked(&lock, "aaaa").unwrap();
    drop(lock);

    let report = prototype_reap(&auth, NOW, HOUR, HOUR, None);
    assert_eq!(report.successes, 0);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}

// ===========================================================================
// SECTION E — representative Finalizing recovery (REPRESENTATIVE MODELING).
// Design requirement 3.
// ===========================================================================

/// Finalizing recovery is a roll-forward across three pinned subtrees, NOT a
/// truncation. With a matching CAS blob present, recovery links membership and
/// writes a receipt through pinned descriptors; the session meta is LEFT in
/// place; a repeat pass is idempotent (AlreadyFinalized).
#[test]
fn representative_finalizing_recovery_rolls_forward_under_pinned_authority() {
    let parent = tempfile::tempdir().unwrap();
    let root = make_tree(parent.path(), "root");

    // A Finalizing session meta carrying size + digest + repo; CAS blob present.
    let digest = "deadbeef";
    let payload = b"cas-object-bytes"; // 16 bytes
    write_cas_blob(&root, digest, payload);
    write_finalizing_session(
        &root,
        "hhhh",
        "repoA",
        digest,
        payload.len() as u64,
        NOW - 10 * HOUR,
    );

    let auth = PinnedUploadsAuthority::open_full(&root).unwrap();
    let lock = try_lock_session(auth.uploads_fd(), "hhhh")
        .unwrap()
        .unwrap();

    let outcome = auth.recover_finalizing_locked(&lock, "hhhh").unwrap();
    assert_eq!(outcome, FinalizingOutcome::RolledForward);
    assert!(
        exists(&root, "uploads/.finalized/hhhh.json"),
        "receipt published"
    );
    assert!(
        exists(&root, "membership/repoA/hhhh.membership"),
        "membership linked under contained per-repo parent"
    );
    assert!(
        exists(&root, "uploads/hhhh.meta.json"),
        "meta left in place"
    );
    assert!(exists(&root, "blobs/deadbeef"), "CAS untouched");

    // Idempotent re-recovery.
    let again = auth.recover_finalizing_locked(&lock, "hhhh").unwrap();
    assert_eq!(again, FinalizingOutcome::AlreadyFinalized);
    drop(lock);
}

/// If the CAS blob is absent or size-mismatched, Finalizing recovery publishes
/// NOTHING (no receipt, no membership) — it never fabricates a receipt for
/// content it cannot confirm.
#[test]
fn representative_finalizing_recovery_without_cas_match_does_not_publish() {
    let parent = tempfile::tempdir().unwrap();
    let root = make_tree(parent.path(), "root");

    // meta says size 16, but the CAS blob is a different size (mismatch).
    write_cas_blob(&root, "deadbeef", b"short");
    write_finalizing_session(&root, "iiii", "repoA", "deadbeef", 16, NOW - 10 * HOUR);

    let auth = PinnedUploadsAuthority::open_full(&root).unwrap();
    let lock = try_lock_session(auth.uploads_fd(), "iiii")
        .unwrap()
        .unwrap();

    let outcome = auth.recover_finalizing_locked(&lock, "iiii").unwrap();
    assert_eq!(outcome, FinalizingOutcome::CasUnavailable);
    assert!(
        !exists(&root, "uploads/.finalized/iiii.json"),
        "no receipt fabricated"
    );
    assert!(
        !exists(&root, "membership/repoA/iiii.membership"),
        "no membership fabricated"
    );
    drop(lock);
}

// ===========================================================================
// SECTION E2 — Finalizing recovery driven THROUGH THE REAPER (design req. 3).
// The reaper's Finalizing branch routes to the roll-forward, not truncate-to-
// zero. These exercise recovery via `prototype_reap` and assert outcome/count
// mapping, contained parent creation, atomic writes, and retry-after-partial.
// ===========================================================================

/// The reaper routes an expired Finalizing session through the roll-forward:
/// membership + receipt are published, meta is left, count is a confirmed success.
#[test]
fn reaper_routes_expired_finalizing_through_rollforward() {
    let parent = tempfile::tempdir().unwrap();
    let root = make_tree(parent.path(), "root");
    let payload = b"cas-object-bytes"; // 16
    write_cas_blob(&root, "d0", payload);
    write_finalizing_session(
        &root,
        "jjjj",
        "repoA",
        "d0",
        payload.len() as u64,
        NOW - 10 * HOUR,
    );

    let auth = PinnedUploadsAuthority::open_full(&root).unwrap();
    let report = prototype_reap(&auth, NOW, HOUR, HOUR, None);

    assert_eq!(report.successes, 1, "roll-forward is a confirmed success");
    assert_eq!(report.skipped_incomplete, 0);
    assert_eq!(report.skipped_already_final, 0);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(
        exists(&root, "uploads/.finalized/jjjj.json"),
        "receipt published"
    );
    assert!(
        exists(&root, "membership/repoA/jjjj.membership"),
        "membership linked"
    );
    assert!(
        exists(&root, "uploads/jjjj.meta.json"),
        "meta left in place"
    );
}

/// The reaper maps a Finalizing session whose CAS cannot be confirmed to
/// `skipped_incomplete` — publishes nothing, records no error, no success.
#[test]
fn reaper_finalizing_without_cas_is_skipped_incomplete() {
    let parent = tempfile::tempdir().unwrap();
    let root = make_tree(parent.path(), "root");
    // No CAS blob written for digest "d0".
    write_finalizing_session(&root, "kkkk", "repoA", "d0", 16, NOW - 10 * HOUR);

    let auth = PinnedUploadsAuthority::open_full(&root).unwrap();
    let report = prototype_reap(&auth, NOW, HOUR, HOUR, None);

    assert_eq!(report.successes, 0);
    assert_eq!(
        report.skipped_incomplete, 1,
        "CAS unconfirmed => incomplete skip"
    );
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(!exists(&root, "uploads/.finalized/kkkk.json"), "no receipt");
    assert!(
        !exists(&root, "membership/repoA/kkkk.membership"),
        "no membership"
    );
    assert!(
        exists(&root, "uploads/kkkk.meta.json"),
        "meta retained for retry"
    );
}

/// The reaper maps a Finalizing session that already has a receipt to
/// `skipped_already_final` (idempotent), not a fresh success.
#[test]
fn reaper_finalizing_existing_receipt_is_already_final() {
    let parent = tempfile::tempdir().unwrap();
    let root = make_tree(parent.path(), "root");
    write_finalizing_session(&root, "llll", "repoA", "d0", 16, NOW - 10 * HOUR);
    write_receipt(&root, "llll", NOW); // fresh receipt already present

    let auth = PinnedUploadsAuthority::open_full(&root).unwrap();
    let report = prototype_reap(&auth, NOW, HOUR, HOUR, None);

    assert_eq!(
        report.successes, 0,
        "already finalized is not a fresh success"
    );
    assert_eq!(report.skipped_already_final, 1);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(
        exists(&root, "uploads/llll.meta.json"),
        "meta left in place"
    );
}

/// Membership success followed by a receipt-write failure, then retry after the
/// partial success (design requirement 3). First pass: membership is written but
/// the receipt write fails atomically — no partial receipt, no leaked temp, the
/// pass records ONE error and NO success, and the session stays discoverable
/// (receipt absent). Second pass with the fault cleared: recovery re-enters (not
/// AlreadyFinalized), re-writes membership idempotently, completes the receipt,
/// and reports a confirmed success.
#[test]
fn reaper_finalizing_membership_then_receipt_failure_then_retry() {
    let parent = tempfile::tempdir().unwrap();
    let root = make_tree(parent.path(), "root");
    let payload = b"cas-object-bytes"; // 16
    write_cas_blob(&root, "d0", payload);
    write_finalizing_session(
        &root,
        "mmmm",
        "repoA",
        "d0",
        payload.len() as u64,
        NOW - 10 * HOUR,
    );

    let auth = PinnedUploadsAuthority::open_full(&root).unwrap();
    auth.set_receipt_write_fault(true);

    let r1 = prototype_reap(&auth, NOW, HOUR, HOUR, None);
    assert_eq!(r1.successes, 0, "receipt failed => no success this pass");
    assert_eq!(
        r1.errors.len(),
        1,
        "the receipt-write failure surfaced: {:?}",
        r1.errors
    );
    assert!(
        exists(&root, "membership/repoA/mmmm.membership"),
        "membership committed before the receipt failure"
    );
    assert!(
        !exists(&root, "uploads/.finalized/mmmm.json"),
        "no partial receipt visible (atomic replacement)"
    );
    // Atomic replacement leaves no orphaned temp in .finalized/.
    let leftover: Vec<_> = std::fs::read_dir(root.join("uploads").join(".finalized"))
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with(".tmp."))
        .collect();
    assert!(leftover.is_empty(), "no leaked temp files: {leftover:?}");

    // Retry with the fault cleared: completes the receipt idempotently.
    auth.set_receipt_write_fault(false);
    let r2 = prototype_reap(&auth, NOW, HOUR, HOUR, None);
    assert_eq!(r2.successes, 1, "retry completes the roll-forward");
    assert!(r2.errors.is_empty(), "{:?}", r2.errors);
    assert!(
        exists(&root, "uploads/.finalized/mmmm.json"),
        "receipt now present"
    );
    assert!(
        exists(&root, "membership/repoA/mmmm.membership"),
        "membership still present"
    );
}

/// A required membership parent directory is absent; recovery creates it CONTAINED
/// (mkdirat beneath the pinned membership descriptor) before writing the leaf.
#[test]
fn reaper_finalizing_creates_missing_membership_parent() {
    let parent = tempfile::tempdir().unwrap();
    let root = make_tree(parent.path(), "root");
    let payload = b"cas-object-bytes"; // 16
    write_cas_blob(&root, "d0", payload);
    write_finalizing_session(
        &root,
        "nnnn",
        "repoZ",
        "d0",
        payload.len() as u64,
        NOW - 10 * HOUR,
    );
    // The per-repo parent is absent at pass start.
    assert!(!exists(&root, "membership/repoZ"), "per-repo parent absent");

    let auth = PinnedUploadsAuthority::open_full(&root).unwrap();
    let report = prototype_reap(&auth, NOW, HOUR, HOUR, None);

    assert_eq!(report.successes, 1);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(
        exists(&root, "membership/repoZ"),
        "parent created contained"
    );
    assert!(
        exists(&root, "membership/repoZ/nnnn.membership"),
        "membership written"
    );
}

/// Accurate outcome/count mapping across a mixed pass: one roll-forward, one
/// CAS-unavailable, one already-final, one plain abort.
#[test]
fn reaper_mixed_finalizing_outcomes_map_to_accurate_counts() {
    let parent = tempfile::tempdir().unwrap();
    let root = make_tree(parent.path(), "root");
    let payload = b"cas-object-bytes"; // 16

    // (1) roll-forward: CAS present.
    write_cas_blob(&root, "d10", payload);
    write_finalizing_session(
        &root,
        "m1ff",
        "repoA",
        "d10",
        payload.len() as u64,
        NOW - 10 * HOUR,
    );
    // (2) cas-unavailable: no blob for d20.
    write_finalizing_session(&root, "m2ff", "repoA", "d20", 16, NOW - 10 * HOUR);
    // (3) already-final: receipt present.
    write_finalizing_session(&root, "m3ff", "repoA", "d30", 16, NOW - 10 * HOUR);
    write_receipt(&root, "m3ff", NOW);
    // (4) plain abort: expired Active.
    write_session(&root, "m4ac", "Active", NOW - 10 * HOUR);

    let auth = PinnedUploadsAuthority::open_full(&root).unwrap();
    let report = prototype_reap(&auth, NOW, HOUR, HOUR, None);

    assert_eq!(
        report.successes, 2,
        "roll-forward + abort are the successes"
    );
    assert_eq!(report.skipped_incomplete, 1, "the CAS-unavailable session");
    assert_eq!(report.skipped_already_final, 1, "the already-final session");
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    // Abort happened; roll-forward left its meta; already-final left its meta.
    assert!(!exists(&root, "uploads/m4ac.meta.json"), "aborted");
    assert!(
        exists(&root, "uploads/m1ff.meta.json"),
        "roll-forward leaves meta"
    );
    assert!(exists(&root, "membership/repoA/m1ff.membership"));
}

/// `recover_truncate_locked` models the Active TORN-TAIL crash-recovery branch
/// (production `recover_session` Active arm) — DISTINCT from the Finalizing
/// roll-forward and NOT used by the expiry reaper. Kept and tested so the two
/// recovery branches are not conflated.
#[test]
fn truncation_recovery_is_the_torn_tail_branch_distinct_from_rollforward() {
    let parent = tempfile::tempdir().unwrap();
    let root = make_tree(parent.path(), "root");
    write_session(&root, "tttt", "Active", NOW);
    std::fs::write(root.join("uploads").join("tttt.data"), b"0123456789").unwrap(); // 10 bytes

    let auth = PinnedUploadsAuthority::open(&root).unwrap();
    let lock = try_lock_session(auth.uploads_fd(), "tttt")
        .unwrap()
        .unwrap();
    auth.recover_truncate_locked(&lock, "tttt", 4).unwrap();
    drop(lock);

    let len = std::fs::metadata(root.join("uploads").join("tttt.data"))
        .unwrap()
        .len();
    assert_eq!(len, 4, "torn tail truncated to the committed offset");
}

// ===========================================================================
// SECTION F — cancellation / operation completion. This section separates REAL
// ASYNC CANCELLATION EVIDENCE (a Tokio runtime + `spawn_blocking` + `abort`) from
// OWNERSHIP MODELING (threads + channels illustrating the same rule). Design
// requirement 5.
// ===========================================================================

/// REAL ASYNC CANCELLATION EVIDENCE. An async operation OWNS the session lock and
/// the pinned authority and performs its mutation inside `spawn_blocking`. When
/// the awaiting caller is cancelled (`JoinHandle::abort`), the detached blocking
/// closure keeps running and keeps the lock: while it is in flight a fresh acquire
/// is busy and the mutation is not yet visible; only after it completes (guard
/// dropped INSIDE the closure, explicit `LOCK_UN` after the mutation) is the lock
/// acquirable and the effect visible. Deterministic via channel barriers, no
/// sleeps. This is the operation boundary the design's owned API must express: the
/// lock ownership reaches the blocking work and survives caller cancellation
/// without an early `LOCK_UN`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_cancellation_retains_lock_until_blocking_completion() {
    let parent = tempfile::tempdir().unwrap();
    let root = make_tree(parent.path(), "root");
    write_session(&root, "aaaa", "Active", NOW - 10 * HOUR);
    let auth = Arc::new(PinnedUploadsAuthority::open(&root).unwrap());

    use std::sync::mpsc::channel;
    let (acquired_tx, acquired_rx) = channel::<()>();
    let (proceed_tx, proceed_rx) = channel::<()>();
    let (done_tx, done_rx) = channel::<()>();

    // The owned operation: authority + lock are moved into the blocking closure;
    // the guard is dropped inside it, AFTER the mutation.
    let op_auth = auth.clone();
    let handle = tokio::spawn(async move {
        tokio::task::spawn_blocking(move || {
            let lock = try_lock_session(op_auth.uploads_fd(), "aaaa")
                .unwrap()
                .unwrap();
            acquired_tx.send(()).unwrap();
            proceed_rx.recv().unwrap(); // work in progress; caller may be cancelled now
            op_auth.abort_locked(&lock, "aaaa").unwrap();
            drop(lock); // explicit LOCK_UN INSIDE the closure, AFTER the mutation
            done_tx.send(()).unwrap();
        })
        .await
        .unwrap();
    });

    // The operation has acquired the lock and is mid-flight.
    acquired_rx.recv().unwrap();

    // Cancel the awaiting caller. The blocking closure is detached and keeps going.
    handle.abort();

    // Exclusion still holds while the work is active: a fresh acquire is busy, and
    // the mutation has not yet happened. Do the blocking acquire off the runtime.
    let probe_auth = auth.clone();
    let busy = tokio::task::spawn_blocking(move || {
        try_lock_session(probe_auth.uploads_fd(), "aaaa")
            .unwrap()
            .is_none()
    })
    .await
    .unwrap();
    assert!(
        busy,
        "lock retained by the running blocking op despite caller cancellation"
    );
    assert!(
        exists(&root, "uploads/aaaa.meta.json"),
        "mutation not yet performed"
    );

    // Allow completion; only now is the lock released and the effect visible.
    proceed_tx.send(()).unwrap();
    done_rx.recv().unwrap();

    let release_auth = auth.clone();
    let acquirable = tokio::task::spawn_blocking(move || {
        try_lock_session(release_auth.uploads_fd(), "aaaa")
            .unwrap()
            .is_some()
    })
    .await
    .unwrap();
    assert!(acquirable, "lock released only after the op completed");
    assert!(
        !exists(&root, "uploads/aaaa.meta.json"),
        "mutation completed"
    );
}

/// OWNERSHIP MODELING (NOT async cancellation evidence — see the Tokio test
/// above). A worker thread OWNS the lock guard and releases it (explicit
/// `LOCK_UN`) only AFTER the mutation completes. This illustrates the ownership
/// rule with threads + channel barriers; it is deterministic but does not involve
/// a runtime or future cancellation.
#[test]
fn blocking_ownership_model_retains_lock_until_completion() {
    let parent = tempfile::tempdir().unwrap();
    let root = make_tree(parent.path(), "root");
    write_session(&root, "aaaa", "Active", NOW - 10 * HOUR);
    let auth = PinnedUploadsAuthority::open(&root).unwrap();

    use std::sync::mpsc::channel;
    let (acquired_tx, acquired_rx) = channel::<()>();
    let (proceed_tx, proceed_rx) = channel::<()>();
    let (done_tx, done_rx) = channel::<()>();

    let auth = &auth;
    std::thread::scope(|s| {
        // Models the spawn_blocking closure: it OWNS the lock guard and the work.
        // `auth` is borrowed (shared, read-only descriptors); the lock guard is
        // what the closure owns and releases only on completion.
        s.spawn(move || {
            let lock = try_lock_session(auth.uploads_fd(), "aaaa")
                .unwrap()
                .unwrap();
            acquired_tx.send(()).unwrap();
            // The awaiting caller may be cancelled at this point; the closure
            // keeps running and keeps holding the lock.
            proceed_rx.recv().unwrap();
            auth.abort_locked(&lock, "aaaa").unwrap();
            drop(lock); // explicit LOCK_UN INSIDE the closure, AFTER the mutation.
            done_tx.send(()).unwrap();
        });

        // The "awaiting future" is cancelled: we stop waiting on the worker and
        // observe the lock is STILL held (a fresh acquire is busy) and the
        // mutation has NOT yet happened.
        acquired_rx.recv().unwrap();
        assert!(
            try_lock_session(auth.uploads_fd(), "aaaa")
                .unwrap()
                .is_none(),
            "lock retained by the running blocking op despite caller cancellation"
        );
        assert!(
            exists(&root, "uploads/aaaa.meta.json"),
            "mutation not yet performed"
        );

        // Let the operation finish; only now is the lock released.
        proceed_tx.send(()).unwrap();
        done_rx.recv().unwrap();
        assert!(
            try_lock_session(auth.uploads_fd(), "aaaa")
                .unwrap()
                .is_some(),
            "lock released only after the op completed"
        );
        assert!(
            !exists(&root, "uploads/aaaa.meta.json"),
            "mutation completed"
        );
    });
}

// ===========================================================================
// SECTION G — outcomes, missing directories, partial-crash discoverability.
// Design requirement 6.
// ===========================================================================

/// Earlier success followed by a later per-candidate error: the pass records the
/// success AND the error, and maps to `Ok(successes)` — a per-item error does not
/// erase confirmed successes.
#[test]
fn earlier_success_then_later_error_is_reported_honestly() {
    let parent = tempfile::tempdir().unwrap();
    let root = make_tree(parent.path(), "root");
    let uploads = root.join("uploads");
    write_session(&root, "aaaa", "Active", NOW - 10 * HOUR); // success
    std::fs::write(uploads.join("zzzz.data"), b"payload-bytes").unwrap();
    std::fs::write(
        uploads.join("ztarget.json"),
        MetaRecord {
            state: "Active".into(),
            last_active: NOW - 10 * HOUR,
        }
        .to_json(),
    )
    .unwrap();
    std::os::unix::fs::symlink("ztarget.json", uploads.join("zzzz.meta.json")).unwrap();

    let auth = PinnedUploadsAuthority::open(&root).unwrap();
    let report = prototype_reap(&auth, NOW, HOUR, HOUR, None);

    assert_eq!(report.successes, 1, "the clean candidate was reaped");
    assert_eq!(
        report.errors.len(),
        1,
        "the failing candidate surfaced an error"
    );
    assert!(report.fatal.is_none(), "a per-item error is not fatal");
    assert_eq!(
        report_to_public_result(&report),
        Ok(1),
        "maps to Ok(successes)"
    );
    assert!(!exists(&root, "uploads/aaaa.meta.json"));
    assert!(
        exists(&root, "uploads/zzzz.data"),
        "failed candidate untouched"
    );
}

/// A FATAL top-level failure (uploads enumeration itself failed) maps to `Err`,
/// distinct from per-item errors. The prototype sets `fatal` when the uploads
/// enumeration fails; here the mapping is exercised on a constructed report so
/// the `Err` branch is covered deterministically (the enumeration-failure path
/// is not reachable through a pinned dir fd on a healthy host).
#[test]
fn fatal_enumeration_failure_maps_to_err() {
    let report = ReapReport {
        successes: 3,
        fatal: Some("enumerate uploads: simulated".to_string()),
        ..Default::default()
    };
    assert!(
        report_to_public_result(&report).is_err(),
        "a fatal pass failure returns Err even with earlier successes"
    );
}

/// An ABSENT `.finalized/` directory does not prevent eligible session
/// processing (design requirement 6): the session is reaped, receipts are simply
/// skipped, and the pass is not an error.
#[test]
fn absent_finalized_dir_does_not_block_session_processing() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("root");
    std::fs::create_dir_all(root.join("uploads")).unwrap(); // NO .finalized/
    write_session(&root, "aaaa", "Active", NOW - 10 * HOUR);

    let auth = PinnedUploadsAuthority::open(&root).unwrap();
    assert!(auth.finalized_fd().is_none(), "no .finalized/ pinned");

    let report = prototype_reap(&auth, NOW, HOUR, HOUR, None);
    assert_eq!(
        report.successes, 1,
        "session reaped despite absent receipt dir"
    );
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(report.fatal.is_none());
    assert!(!exists(&root, "uploads/aaaa.meta.json"));
}

/// Partial-crash discoverability (design requirement 6): abort removes meta
/// LAST, so a crash that interrupts cleanup leaves the `.meta.json` present and
/// the session is re-enumerated and completed on the next pass. Here we simulate
/// a crash after the `.data` unlink but before the `.meta.json` unlink, then run
/// a full pass and confirm rediscovery and completion.
#[test]
fn partial_abort_is_rediscovered_and_completed() {
    let parent = tempfile::tempdir().unwrap();
    let root = make_tree(parent.path(), "root");
    write_session(&root, "aaaa", "Active", NOW - 10 * HOUR);

    // Simulated crash mid-abort: data already removed, meta (and hence
    // discoverability) still present. This is the ORDER abort_locked uses.
    std::fs::remove_file(root.join("uploads").join("aaaa.data")).unwrap();
    assert!(
        exists(&root, "uploads/aaaa.meta.json"),
        "meta still discoverable"
    );

    let auth = PinnedUploadsAuthority::open(&root).unwrap();
    let report = prototype_reap(&auth, NOW, HOUR, HOUR, None);

    assert_eq!(
        report.successes, 1,
        "rediscovered and completed (data ENOENT is idempotent)"
    );
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(!exists(&root, "uploads/aaaa.meta.json"));
    assert!(!exists(&root, "uploads/aaaa.hash.0"));
}

/// Valid cleanup then idempotent retry: a first pass reaps the expired session
/// and receipt; a second pass over the now-drained tree is a clean no-op.
#[test]
fn valid_cleanup_then_idempotent_retry() {
    let parent = tempfile::tempdir().unwrap();
    let root = make_tree(parent.path(), "root");
    write_session(&root, "aaaa", "Active", NOW - 10 * HOUR);
    write_receipt(&root, "aaaa", NOW - 10 * HOUR);

    let auth = PinnedUploadsAuthority::open(&root).unwrap();

    let first = prototype_reap(&auth, NOW, HOUR, HOUR, None);
    assert_eq!(first.successes, 2);
    assert!(first.errors.is_empty(), "{:?}", first.errors);
    assert!(!exists(&root, "uploads/aaaa.meta.json"));
    assert!(!exists(&root, "uploads/.finalized/aaaa.json"));

    let second = prototype_reap(&auth, NOW, HOUR, HOUR, None);
    assert_eq!(second.successes, 0, "nothing left to reap");
    assert!(
        second.errors.is_empty(),
        "retry is a clean no-op: {:?}",
        second.errors
    );
    assert_eq!(report_to_public_result(&second), Ok(0));
}
