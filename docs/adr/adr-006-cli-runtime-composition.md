# ADR-006: CLI Runtime Composition, Command Safety Policies, and Typed Errors

* **Status:** Accepted
* **Date:** 2026-09-01
* **Authors:** Senior Software Architect (OCI Distribution & Storage Systems)
* **Scope:** CLI Runtime Composition Root, Command Safety Policy Taxonomy, Non-Mutating Read-Only Invariants, Single-Ownership Authority Unwinding, Typed Command Errors
* **Refines:** `docs/architecture/adr-001-first-refactoring-boundary.md`, `docs/architecture/adr-002-application-service-boundary.md`, `docs/architecture/adr-003-storage-capability-ports.md`, `docs/architecture/adr-004-application-read-services.md`, `docs/architecture/adr-005-server-runtime-composition-root.md`, `docs/architecture/current-code-assessment.md`

---

## 1. Context & Problem Statement

Following the establishment of the server composition root (ADR-005), CLI and maintenance commands remained fragmented and exhibited critical safety and architectural gaps:

1. **Undifferentiated Lifecycles:** All commands were forced through ad-hoc initialization sequences without formal classification of their safety invariants, locking requirements, or readiness expectations.
2. **Mutating "Read-Only" Operations:**
   * `ref-index check` inadvertently created sled database directories, WAL files, and schema metadata when run against a non-existent path.
   * `blob-gc plan` called `ensure_healthy_or_rebuild`, mutatively repairing corrupt index state and creating lock metadata during supposedly dry-run operations.
3. **Ambiguous Authority Ownership & Resource Leaks:**
   * Distributed mutation authority was acquired and released without a strict RAII unwinding discipline.
   * Error paths during `blob-gc quarantine` returned early before releasing mutation authority, leaving deployment writer locks stranded.
4. **Untyped String Errors & Mixed Exit Codes:** Error handling in CLI dispatch relied on untyped strings and immediate `process::exit` calls scattered across command dispatchers.

---

## 2. Decision: Command Safety Policy Taxonomy (`src/cli/policy.rs`)

We introduce an explicit, crate-private `CommandPolicy` taxonomy that classifies every CLI and maintenance operation by its required authority and mutation semantics:

```rust
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum CommandPolicy {
    /// Pure local computation or configuration inspection; constructs no storage,
    /// acquires no locks, and has no side effects.
    Pure,

    /// Non-mutating inspection of existing persistent state.
    /// Absolutely no creation, rebuild, repair, readiness-marker write, quarantine, or deletion.
    ReadOnly,

    /// Non-mutating offline inspection that requires exclusive mutation authority
    /// to guarantee a coherent, point-in-time snapshot.
    ExclusiveInspection { lock_suffix: &'static str },

    /// Offline exclusive mutation requiring filesystem root lock (where applicable)
    /// followed by distributed mutation authority lease.
    ExclusiveMutation { lock_suffix: &'static str },

    /// Repository membership migration requiring exclusive authority and migration-specific
    /// readiness rules (can execute before readiness is achieved; writes marker upon completion).
    Migration { lock_suffix: &'static str },

    /// Administrative lock inspection and destructive recovery; must NOT attempt to acquire
    /// the deployment writer lock being inspected or cleared.
    BreakGlass,
}
```

### Complete Command Policy Mapping

| Command | Subcommand | Policy | FsRootLock | Mutation Authority | Readiness Preflight |
|---|---|---|---|---|---|
| `server` | — | `ExclusiveMutation` | Yes (FS) | Yes (`server`) | Fail-closed (auto-init if empty) |
| `check-config` | — | `Pure` | No | No | None (no storage) |
| `hash-secret` | — | `Pure` | No | No | None (no storage) |
| `audit-permissions` | — | `Pure` | No | No | None (no storage) |
| `ref-index` | `check` | `ReadOnly` | No | No | None (`check_path_health`) |
| `ref-index` | `rebuild` | `ExclusiveMutation` | Yes (FS) | Yes (`ref-index-rebuild`) | Fail-closed |
| `ref-index` | `ensure` | `ExclusiveMutation` | Yes (FS) | Yes (`ref-index-ensure`) | Fail-closed |
| `blob-gc` | `plan` | `ReadOnly` | No | No | Fail-closed (no repair!) |
| `blob-gc` | `quarantine` | `ExclusiveMutation` | Yes (FS) | Yes (`blob-gc-quarantine`) | Fail-closed |
| `blob-gc` | `delete` | `ExclusiveMutation` | Yes (FS) | Yes (`blob-gc-delete`) | Fail-closed |
| `migrate-membership` | `plan` | `ExclusiveInspection` | Yes (FS) | Yes (`migrate-membership-plan`) | Migration-exempt |
| `migrate-membership` | `apply` | `Migration` | Yes (FS) | Yes (`migrate-membership-apply`) | Migration-exempt (writes Ready) |
| `migrate-membership` | `verify` | `ExclusiveInspection` | Yes (FS) | Yes (`migrate-membership-verify`) | Migration-exempt |
| `inspect-lock` | — | `ReadOnly` | No | No | None |
| `admin-clear-lock` | — | `BreakGlass` | No | No (Clears target lock) | None |

---

## 3. Decision: Non-Mutating Read-Only Invariants

### 3.1. `BlobRefIndex::check_path_health` & `open_existing`

`BlobRefIndex` provides narrow read-only inspection methods that inspect disk state without invoking `sled::open` on absent directories:

```rust
impl BlobRefIndex {
    pub fn open_existing(path: &Path) -> Result<Self, RefIndexError> {
        if !path.exists() {
            return Err(RefIndexError::NotFound(path.to_path_buf()));
        }
        Self::open(path.to_path_buf())
    }

    pub fn check_path_health(path: &Path) -> Result<(), RefIndexError> {
        if !path.exists() {
            return Err(RefIndexError::NotFound(path.to_path_buf()));
        }
        let idx = Self::open(path.to_path_buf())?;
        idx.check_health()
    }
}
```

* `ref-index check` on a missing path returns `CliError::IndexMissing { path }` and creates **0 files, directories, WAL records, or lock metadata**.
* `blob-gc plan` on a corrupt or missing index returns `CliError::IndexUnhealthy` or `CliError::IndexMissing` without executing auto-repair or creating quarantine/CAS mutations.
* `ReadOnly` commands acquire neither `FsRootLock` nor `RuntimeMutationAuthority`.

---

## 4. Decision: Bounded Maintenance Runtime (`src/cli/runtime.rs`)

`MaintenanceRuntime` serves as the bounded composition root for CLI commands with strict single ownership:

```rust
pub struct MaintenanceRuntime {
    config: Arc<Config>,
    storage_wiring: StorageWiring,
    fs_root_lock: Option<FsRootLock>,
    authority: Option<RuntimeMutationAuthority>,
}
```

### 4.1. Single-Ownership Teardown & Unwinding Guarantees

* **Single Authority Owner:** `MaintenanceRuntime` is the sole owner of the `RuntimeMutationAuthority` lease. `GcService` receives only a non-owning borrow (`&RuntimeMutationAuthority`) to mint `GcMutationPermit`. Zero `Arc<Mutex<Option<RuntimeMutationAuthority>>>` exists in CLI composition.
* **Acquisition Lock Ordering:** Mirrors `ServerRuntime` (`FsRootLock` on filesystem, followed by distributed `RuntimeMutationAuthority`).
* **Deterministic Teardown:** Every command execution path (success, operational failure, or compound failure) releases `authority` and clears `fs_root_lock` in reverse order before returning control.
* **Compound Error Preservation:** If command execution fails and authority release subsequently fails, `CliError::ExecutionAndTeardownFailed` preserves both errors with the command's primary exit code.

### 4.2. Break-Glass Recovery Isolation

`admin-clear-lock` executes via standalone static capability methods without creating a `MaintenanceRuntime` instance and without acquiring the target lease it intends to clear.

---

## 5. Decision: Typed Command Errors & Exit Code Preservation (`src/cli/errors.rs`)

All command errors are encapsulated in the typed `CliError` enum:

```rust
#[derive(Debug, Error)]
pub enum CliError {
    #[error("failed to load config: {0}")]
    Config(#[from] ConfigError),

    #[error("ref-index is disabled (storage.ref_index.enabled=false)")]
    RefIndexDisabled,

    #[error("refusing to run while registry is active ({0}); stop the server first")]
    ServerActive(String),

    #[error("failed to acquire exclusive deployment writer authority: {0}")]
    LockContention(StorageError),

    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("ref-index not found at {path}; run `ref-index rebuild` or `ensure` to initialize it")]
    IndexMissing { path: PathBuf },

    #[error("ref-index at {path} is unhealthy: {reason}; run `ref-index rebuild` or `ensure` to repair it")]
    IndexUnhealthy { path: PathBuf, reason: String },

    #[error("repository blob memberships require migration; run `migrate-membership apply` before proceeding")]
    MembershipBackfillRequired,

    #[error("{source}; authority release also failed: {release_error}")]
    ExecutionAndTeardownFailed {
        source: Box<CliError>,
        release_error: StorageError,
    },
    ...
}
```

### Baseline-Preserved Exit Code Mappings

* **`0` (Success):** All operations completed successfully.
* **`1` (Operational Failure):** Lock contention (`LockContention`), corrupted index (`IndexUnhealthy`), missing index (`IndexMissing`), unmigrated storage backfill required (`MembershipBackfillRequired`), verification failure (`MembershipVerificationFailed`), empty secret / stdin read error, or authority release failure.
* **`2` (Usage / Configuration Error):** Invalid configuration file (`Config`), disabled ref-index (`RefIndexDisabled`), active server lock conflict (`ServerActive`), or missing S3 confirmation flag (`S3GcConfirmationRequired`).

---

## 6. Consequences & System Invariants

1. **Zero Unintentional Mutations:** Read-only CLI invocations are mathematically guaranteed to perform zero storage writes, marker updates, database creations, or lock acquisitions.
2. **Deterministic Contention Handling:** Server vs. CLI and CLI vs. CLI lock contention produces immediate, structured diagnostic errors without lingering state.
3. **Verified Lifecycle Resilience:** Every maintenance command releases its authority and filesystem locks under normal execution, operational failure, and panic/unwind conditions.
4. **Strict Single Ownership:** `RuntimeMutationAuthority` has exactly one owner at every point in time with RAII and explicit release safety.
5. **Architectural Symmetry:** Both server runtime (`ServerRuntime` in `src/runtime.rs`) and maintenance operations (`MaintenanceRuntime` in `src/cli/runtime.rs`) assemble isolated, bounded dependency graphs via `StorageWiring` without leaking global mutable state.

---

## Reconciliation addendum (2026-09-19, `master` `2718bc16`)

The decision stands as implemented. One divergence recorded: an unused parallel classification `CommandIntent` (`src/cli/mod.rs:225-291`) disagrees with the enforced `CommandPolicy` on `migrate-membership verify` (`ReadOnly` vs `ExclusiveInspection`). Tracked as [KI-06](../technical-debt.md); resolution is an unapproved code decision.
