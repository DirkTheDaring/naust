# Architecture Assessment: Filesystem Tag Mutation Write Characterization

- **Document:** `docs/architecture/filesystem-tag-mutation-write-characterization.md`
- **Status:** Characterization Record — Production Behavior Unchanged — Not Committed
- **Canonical Quality Gates:** `O-03`, `O-04`, `O-05`, `O-06`, `O-13`, `O-15`, `O-16`, and `D-06` remain explicitly **OPEN**
- **Registry-Rust Baseline HEAD:** `00676c721fde2687196eececbc2cdb097bb1fd9c` (working tree carries the accepted, uncommitted upload-lifecycle slice on top)
- **Storage-Layer-Rust Baseline HEAD:** `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e` (strictly read-only; not touched by this slice)

---

## 1. Scope & Position in the O-04 Cadence

This is the **characterization** step of the repo-scoped write-containment slice under gate **O-04** (filesystem write durability + containment). It freezes the *current, ambient* (not-yet-contained) behavior of the tag-mutation write paths at the production `FsStorage` boundary so the later contained-integration-design → seam → readiness → cutover steps can be proven behavior-preserving.

**No production code, dependency, configuration, or external crate is changed by this step.** The only change is the addition of behavior-freezing tests. Tag *reads* (`resolve_tag`, `get_tag_with_version`) were already contained and characterized in the tag-read slice; this slice targets the remaining ambient **write** side.

Production methods characterized, all in [`src/storage/fs.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs.rs):

1. `set_tag` (delegates to `mutate_tag(.., Replace)`).
2. `mutate_tag` (`CreateOnly` / `Replace` policies).
3. `delete_tag` (unconditional).
4. `delete_tag_conditional` (version-precondition and unconditional-`None` branches).

The version-**precondition role** of `delete_tag_conditional` (raw-byte SHA-256 token, `PreconditionFailed`, newline sensitivity) and the `mutate_tag` **concurrency** outcomes were already frozen by pre-existing tests (`test_tag_conditional_delete_version_precondition_role`, `test_fs_direct_concurrent_create_only`, `test_fs_direct_concurrent_replacements_chain`, `test_fs_direct_repeated_replacements`) and are **not** re-litigated here.

## 2. Current Production Behavior (as read from source)

All tag-mutation writes today are **ambient pathname operations** rooted at `self.root.join("repos").join(name).join("tags")`, with **no** canonical-name validation (contrast `write_lifecycle_journal`, which parses `CanonicalRepoName` first) and **no** contained (openat2 / `RESOLVE_BENEATH`) authority. The frontier map is recorded in memory `o04-write-containment-frontier`.

Frozen invariants (each mapped to a test in §3):

- **Contained path & body layout.** A tag write lands at exactly `repos/<repo>/tags/<tag>`; the on-disk body is `format!("{}\n", digest.as_str())`, i.e. `sha256:<hex>\n`. This byte layout is what the version token hashes and what the cutover must reproduce bit-for-bit.
- **Temp-then-rename durability, no residue.** `mutate_tag` writes `.tmp.<tag>.<uuid>` (`create_new` / `O_EXCL`) → `write_all` → `flush` → `sync_all` → `rename` → best-effort parent-dir fsync. After any success the tags dir contains only the tag leaf and the retained `.lock.<tag>` file — never a `.tmp.*` residue.
- **Retained advisory lock.** `.lock.<tag>` is opened `create(true)`, flock-exclusive for the critical section, unlocked, and **never unlinked**. It persists as a normal directory entry.
- **Same-digest short-circuit precedes policy.** The `existing == new` equality check runs *before* the `CreateOnly`/`Replace` branch, so `CreateOnly` against an identical existing digest returns `TagMutation::Unchanged`, **not** `TagAlreadyExists`, and performs no write.
- **Divergent `CreateOnly` conflicts non-destructively.** `CreateOnly` over a different existing digest returns `StorageError::TagAlreadyExists` and leaves the existing tag byte-identical.
- **Corrupt existing content is treated as absent.** Existing bytes that fail `Digest::parse` (via `from_utf8_lossy(..).trim()`) collapse to `None`: `CreateOnly` then *succeeds* as `Created` and `Replace` reports `Created` (no `previous`), silently overwriting the corrupt content with the canonical body.
- **`delete_tag_conditional` branches.** `expected_version = None` deletes unconditionally → `Deleted`; an absent tag → `NotFound` (having created its `.lock.<tag>` first). The `Some(..)` precondition branch is covered by the pre-existing precondition test.
- **`delete_tag` (unconditional).** Removes an existing tag; maps an absent tag to `StorageError::NotFound`.

These are recorded as *observed behavior*, **not** as containment guarantees — the ambient code provides no fail-closed path validation, and no such property is asserted here.

## 3. Codified Tests

Added as module `tag_mutation_write_characterization` in [`src/storage/fs/tests.rs`](file:///home/dietmar/devel/rust/registry-rust/src/storage/fs/tests.rs), all exercising the real `FsStorage` methods:

| Test | Freezes |
|---|---|
| `test_set_tag_writes_canonical_bytes_at_contained_path` | Contained `repos/<repo>/tags/<tag>` location; `sha256:<hex>\n` body; `{tag, .lock.<tag>}`-only dir (no `.tmp.*`); nothing outside `repos/`; round-trip through `resolve_tag` and `get_tag_with_version`. |
| `test_mutate_tag_replace_temp_rename_leaves_no_residue` | `Replace` create → same-digest `Unchanged` (no rewrite) → divergent `Replaced { previous }`; no `.tmp.*` residue at any step; lock retained. |
| `test_create_only_same_digest_returns_unchanged_not_conflict` | Same-digest short-circuit ahead of the `CreateOnly` existence check. |
| `test_create_only_conflict_on_divergent_digest` | Divergent `CreateOnly` → `TagAlreadyExists`; existing tag untouched. |
| `test_mutate_tag_treats_corrupt_existing_as_absent` | Unparseable existing bytes → absent: `CreateOnly` → `Created` (overwrites); `Replace` → `Created` (no `previous`). |
| `test_delete_tag_conditional_unconditional_and_absent` | `None` precondition → `Deleted`; absent → `NotFound`. |
| `test_delete_tag_unconditional_success_and_absent` | `delete_tag` success; absent → `StorageError::NotFound`. |

## 4. Verification (executed this step)

- `cargo fmt --check`: clean.
- `cargo clippy --lib --tests`: clean (no new warnings).
- `cargo test --lib tag_mutation_write_characterization`: **7 passed; 0 failed**.
- `cargo test --lib` (full registry suite): **1041 passed; 0 failed; 18 ignored** (was 1034 before this slice; +7 new).
- Dependency `storage-layer-rust` was not built or modified by this step (no dependency code changed).

## 5. Next Cadence Step & Deferred Decisions

The next step is **contained-integration-design** for repo-scoped tag-mutation writes (`repos/<repo>/tags/<leaf>`). Two policy sub-decisions bind at that step / cutover, **not** during characterization, and remain **open for the user**:

1. **Per-repo pinned-authority pattern.** Current `UploadAuthorities` pin fixed top-level dirs only (uploads, finalized, blobs, memberships). Repo-scoped writes need a per-`<repo>` contained authority for `repos/<repo>/{tags,manifests,referrers,...}`; the pattern (lazily-pinned per-repo `ContainedDir`, cached vs. per-call, and its interaction with repo deletion) is undecided.
2. **Path-validation contract.** Whether the tag/repo leaf is validated structurally (reject `..`, empty components, separators, NUL) or against the canonical OCI grammar, and where that gate sits relative to the pinned open. The contained tag-**read** seam already rejects traversal/symlinks; the write cutover must choose a contract consistent with it.

Invariant the cutover must preserve: the `sha256:<hex>\n` body and its raw-byte SHA-256 version token, so `get_tag_with_version` / `delete_tag_conditional` optimistic-concurrency tokens stay bit-compatible.
