> **ARCHIVED (2026-09-19 consolidation).** This was the living current-state inventory; it is superseded by [`../../architecture/README.md`](../../architecture/README.md) and no longer maintained. Its self-description below ("Living inventory … Supersedes …") was true until this date.

# Current architecture state (code-aligned)

- **Document:** `docs/architecture/current-state.md`
- **Role:** Living inventory of what `registry-rust` implements. Supersedes remaining-gap lists and “NOT COMMITTED” stamps that disagree with this HEAD.
- **Registry-rust:** `master` at `9405991` (`refactor(storage): eliminate FsListingBudgets with true streaming CAS listing`)
- **Date:** 2026-09-18
- **Scope:** Documentation alignment with production sources. No claim that quality-gate *acceptance* is closed. Canonical gate tracking: [acceptance-gates.md](../../technical-debt.md); requirements: [../requirements.md](../../requirements.md); issues: [../known-issues.md](../../technical-debt.md).
- **Verification note (2026-09-19):** every claim in this document that the documentation-reconciliation audit checked at `2718bc1` was confirmed against source; precision fixes from that audit are folded in below and marked *(2026-09-19)*.

Sibling `storage-layer-rust` / `acmecert` were not re-audited for this note; only this crate’s path dependencies and call sites were used.

---

## 1. What the process is

Single-crate OCI Distribution v2 registry (`registry-rust` 0.9.0). One binary, clap subcommands. Inbound HTTP is hand-routed Axum (`/v2`, `/token`, `/_meta`, optional `/_admin/gc`). No OpenAPI or protobuf in tree. *(2026-09-19: the `/v2` parser also handles two non-standard extensions — tag delete via `/v2/<name>/tags/reference/<tag>` and `_oci/ext/discover`; `src/http_api/routing.rs`.)*

Persistence: `FsStorage` or `S3Storage` for content; sled `BlobRefIndex`; optional proxy-cache store + sled. Identity/RBAC is config-file only.

Composition roots:

- Server: `src/runtime.rs` (`build_server_runtime`, `assemble_application_services`) then `src/supervisor.rs` (TLS/ACME, router, workers, shutdown flush).
- CLI: `src/cli/runtime.rs` (`MaintenanceRuntime`) + `src/cli/policy.rs` (`CommandPolicy`).

`ServerRuntime` at this HEAD holds `app_state: AppState`, optional `ref_index`, and `Arc<tokio::sync::Mutex<Option<RuntimeMutationAuthority>>>` (`src/runtime.rs`). ADR-005’s sketched extra fields on that struct (`storage_wiring`, `consistency_coordinator`) are not stored after assembly; they are still constructed in `build_server_runtime` and injected into services.

---

## 2. Layering (Wave 1 — landed)

```
CLI / HTTP
  → AppState application services (seven)
    → domain engines (upload coordinator, manifest lifecycle, membership ledger, GC, proxy)
      → StorageWiring capability ports
        → FsStorage | S3Storage
          → storage-fs / storage-s3 / storage-core (sibling crates)
```

Evidence:

- HTTP-free `src/application/` (no Axum/`StatusCode`/`HeaderMap`). *(2026-09-19: framework independence holds exactly; one module-path edge points the wrong way — `src/application/errors.rs` imports `crate::http_api::upload_state::StateTokenError`, itself HTTP-type-free. Tracked as [KI-07](../../technical-debt.md).)*
- `StorageWiring::from_backend` is generic over the **port** traits, not `dyn Storage`. Production HTTP/CLI consumers take those port views or application services.
- Adapters still `impl Storage for FsStorage` / `S3Storage`. That omnibus impl is used by tests and as the same concrete type that also implements the ports; it is not the production consumer contract. *(2026-09-19 precision: it is, however, the production implementation vehicle — the port impl macros expand to `Storage::<method>` calls for both backends, `src/storage/ports/mod.rs:386-714`. No production consumer holds `Storage`/`dyn Storage`.)*
- ADRs 001–009 are Accepted (consistency tokens, mutation/read services, ports, composition roots, CLI policy, publication shim, test sidecars, `StorageErrorKind`).
- *(2026-09-19)* `ConsistencyCoordinator` is per-composition-root, not process-global: one server instance (`src/runtime.rs:577`); one per CLI maintenance operation (`src/cli/runtime.rs:265,314,363`); another in `src/task_supervisor.rs:680`.

Residual Wave 1 coupling (still in code):

- `AppState` still also holds config, proxy, `GcService`, `BlobRefIndex`, semaphores, IP limiter (`src/app_state.rs`). Admin GC and token mint read those fields; OCI blob/manifest/catalog/tag/referrer routes go through the seven services.
- `src/config.rs` is still passed as the whole `Config` into assembly.
- `src/http_api/handlers.rs` is still a large transport dispatcher (auth, streaming, policy reads from `state.config`) even though mutations/reads go through services.

---

## 3. Filesystem / ObjectStore cutover (Wave 2 — mostly landed)

Committed after the older remaining-gap notes (those notes often cite `0cd6a73` or “NOT COMMITTED”). Later commits overlay earlier ones: `f555e5f` contained FS mutation authorities; phases 3–8 then moved several families onto shared `ObjectStore` domains (current call path).

| Commit | What landed |
|--------|-------------|
| `00676c7` | Quarantine / upload receipt point-read containment |
| `f555e5f` | Contained FS mutation authorities (uploads, then-current tag/manifest/referrer/membership/GC quarantine paths) |
| `8c0ac64` | Durability barriers on contained mutation (CAS publish order, quarantine-restore dirs, durable journal). Also made `meta/` checkpoint writes use the propagated pathname `atomic_write_file` helper — durable, still not descriptor-relative. |
| `32c42c6`–`84dbe13` | Shared domains on `ObjectStore`: tags (3), manifests (4), referrers (5), membership **point** ops (6), journal (7), repo timestamps (8) — FS and S3 |
| `1772f0a`–`9405991` | CAS listing streams (`stream_dir`); `FsListingBudgets` removed |

`FsStorage` owns a pinned `FsMetadataReader`, `FsBlobCasReadAdapter`, `UploadAuthorities`, and the phase 3–8 domain fields (`src/storage/fs.rs`).

Upload reaper (`reap_expired_sessions`) enumerates and mutates through `upload_authorities` and `run_locked`, not ambient `read_dir`. GC quarantine timestamp helpers in `src/blob_gc/mod.rs` take `&ContainedDir`.

There is no production `list_tag_files` symbol.

---

## 4. Residual pathname / unmigrated surfaces (HEAD)

These are remaining *pathname or unmigrated* surfaces, not the historical R-13–R-15 reaper deferral. `8c0ac64` already made the two `meta/` writes crash-durable; they are still not pinned-descriptor writes.

| Area | Symbol / location | What the code does |
|------|-------------------|--------------------|
| Membership ready marker **write** | `FsStorage::mark_membership_ready` | Pathname `tokio::fs::create_dir_all` + `atomic_write_file` on `meta/membership_ready.json` |
| Migration checkpoint **write** | `FsStorage::save_migration_checkpoint` | `ensure_dir` (pathname `std::fs::create_dir_all` helper, `src/storage/mod.rs`) + the same `atomic_write_file` on `meta/migration_checkpoint.json` *(2026-09-19)* |
| Membership ready / checkpoint **read** | `membership_read.rs` | Contained reads through the pinned `FsMetadataReader` |
| Membership **enumeration** | `membership_read.rs` | Still the per-backend listing seam (not `ObjectStore::list_page`); uses the pinned reader |
| Repo lease | `FsStorage::acquire_repo_lease` | Pathname `open` + `fs2` exclusive `flock` on `repos/<repo>/.repo_lock`; `renew_repo_lease` is a no-op `Ok(true)` — no TTL enforced *(2026-09-19; [KI-12](../../technical-debt.md))* |
| Repository-existence probe | `src/storage/fs/tag_listing.rs` | Contained probe; explicitly **not** on `ObjectStore` (“later phase”) |
| Omnibus `Storage` impl | `impl Storage for FsStorage` / `S3Storage` | Still present; production wiring uses port bounds. *(2026-09-19: the port impls themselves delegate to `Storage::<method>` via macros — see §2 evidence note)* |
| Test-only ambient version | `compute_fs_blob_version` | `#[cfg(test)]` pathname hash; production versions use contained authorities |

`atomic_write_file` / `fsync_dir` are the pathname helpers used by the two `meta/` **writes** above.

---

## 5. Quality gates (honest: still OPEN as acceptance)

> *(2026-09-19)* Canonical gate STATUS now lives in [acceptance-gates.md](../../technical-debt.md) (GATE-O03…GATE-FSD06, authority unresolved). The relation-to-code summary below is retained; where it disagrees with the register, the register wins.

Historical FS notes still mark **O-03, O-04, O-05, O-06, O-13, O-15, O-16**, and filesystem-doc **D-06** as **OPEN**. This inventory does **not** close them.

How that relates to code:

- **O-05** (read containment) — most standalone and mutation-path **reads** are pinned-descriptor. Ready-marker and checkpoint **reads** are contained; leftover **writes** in §4 are an O-04 concern, not unread CAS/tag/manifest paths.
- **O-04** (write durability/containment) — mutation cutover + durability barriers landed; deletion crash-persistence is explicitly best-effort (`8c0ac64`); `meta/` marker/checkpoint **writes** and repo leases still use pathnames.
- **O-03** — CAS listing no longer uses `FsListingBudgets`; it streams with a bounded heap. Older “fixed budget” notes for that path are stale. Other listing/token contracts may still be open.
- **O-06** — live S3 tests exist (`2e6bb3b`); AccessDenied remains ignore-gated.
- **O-13** — crate is `0.9.0`. *(2026-09-19 correction: an annotated tag `v0.9.0` exists locally, created 2026-09-06 at `1680f68` — ADR-009's "not tagged" line predates it. Publication/distribution remain open: no git remotes on this repo or the sibling; sibling crates are `publish = false`. See [GATE-O13](../../technical-debt.md).)*
- **O-15** — Linux `openat2` required at `FsStorage` init; non-Linux unverified.
- **O-16** — inventory completeness of the *historical* slice series, not a missing reaper.

---

## 6. Documents this note supersedes as *current remaining work*

Keep the files as historical snapshots; do not use their remaining-work tables as HEAD inventory:

- `filesystem-read-containment-remaining-gaps.md`
- `filesystem-read-containment-post-*-assessment.md` (including post-upload-quarantine)
- `filesystem-quarantine-upload-inspection-containment.md` (reaper-deferred section)
- `filesystem-upload-lifecycle-contained-cleanup-design.md` (“ambient reaper still shipped”)
- `current-code-assessment.md` 2026-08-26 §2.1/§2.2 diagrams. D-01/D-02 are **no longer** “Planned” in §9; that table and the 2026-09-18 addendum are the updated register.

`filesystem-upload-lifecycle-contained-cleanup.md` describes contained upload lifecycle behavior that **did land**; only its “NOT COMMITTED — NOT PUSHED” header was stale (see that file’s updated status).
