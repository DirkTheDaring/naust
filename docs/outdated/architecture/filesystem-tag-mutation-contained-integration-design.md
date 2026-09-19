> **Historical snapshot — dated banner added 2026-09-19 (documentation reconciliation at `master` `2718bc16`).**
> Status stamps ("NOT COMMITTED", "PRODUCTION UNCHANGED", "DESIGN ONLY", pending-decision rows) and remaining-work lists below reflect authoring time, not the current tree. This chain has no dedicated cutover document; contained tag mutation landed via `f555e5f` and the `tag_domain` migration (`32c42c6`).
> Current state: [current-state.md](current-state.md) · Gates: [acceptance-gates.md](../../technical-debt.md) · Issues: [../known-issues.md](../../technical-debt.md) · Requirements: [../requirements.md](../../requirements.md)

# Architecture Design: Filesystem Tag-Mutation Contained-Integration

- **Document:** `docs/architecture/filesystem-tag-mutation-contained-integration-design.md`
- **Status:** Contained-Integration Design — DESIGN ONLY — No Production/Dependency Code Changed — Not Committed
- **Cadence position:** O-04 repo-scoped write-containment slice, step **contained-integration-design** (follows the completed `filesystem-tag-mutation-write-characterization`).
- **Canonical Quality Gates:** `O-03`, `O-04`, `O-05`, `O-06`, `O-13`, `O-15`, `O-16`, `D-06` remain **OPEN**.
- **Registry-Rust Baseline HEAD:** `00676c721fde2687196eececbc2cdb097bb1fd9c` (working tree carries the accepted, uncommitted upload-lifecycle slice on top).
- **Storage-Layer-Rust Baseline HEAD:** `0a628fd08232c3a5ce37c7a2d1d5f3ba2b2fe08e` (read-only; not touched by this step).
- **Ground truth:** current `src/storage/fs.rs`, `src/storage/fs/tag_read.rs`, `src/registry/canonical_name.rs`, dependency `crates/storage-fs/src/mutate.rs`, and the seven `tag_mutation_write_characterization` tests.

This document supersedes the two deferred design questions recorded in §5 of the characterization record; it does **not** re-open the characterization or the accepted PHASE5 upload slice.

---

## 1. Current production behavior and ambient-operation map

The four target methods (`src/storage/fs.rs`) operate today as **ambient pathname** operations rooted at `self.root` with **no** name validation and **no** contained (`openat2` / `RESOLVE_BENEATH`) authority.

### 1.1 `set_tag` → `mutate_tag(Replace)`
`set_tag` (fs.rs:1168–1172) is a thin delegate to `mutate_tag(name, tag, digest, TagMutationPolicy::Replace)`.

### 1.2 `mutate_tag` (fs.rs:1174–1285)
Ambient operation sequence:
1. `dir = self.root.join("repos").join(name).join("tags")` — **ambient join, unvalidated `name`/`tag`.**
2. `ensure_dir(&dir)` (`create_dir_all`) — creates `repos/<name>/tags` if absent (also silently creates `repos`, `repos/<name>`).
3. Lock: open `dir.join(format!(".lock.{tag}"))` with `create(true)`, `fs2` **exclusive flock** inside `spawn_blocking`; retained (never unlinked).
4. Read existing: `std::fs::read(dir.join(tag))` → `String::from_utf8_lossy(..).trim()` → `Digest::parse(..).ok()`. Unreadable/absent/unparseable ⇒ `None`.
5. **Same-digest short-circuit** (runs *before* policy): if `existing == Some(new)` ⇒ `TagMutation::Unchanged`, no write.
6. Policy branch:
   - `CreateOnly` with `existing == Some(other)` ⇒ `StorageError::TagAlreadyExists` (non-destructive).
   - Otherwise write: temp sibling `dir.join(format!(".tmp.{tag}.{uuid}"))` opened `create_new` (`O_EXCL`) → `write_all(body)` → `flush` → **`sync_all`** (fsync temp file) → `rename(temp, dir/tag)` → best-effort parent-dir fsync (`File::open(&dir).sync_all()`, error ignored).
   - Result: `CreateOnly`/absent ⇒ `Created`; `Replace` ⇒ `Replaced { previous }` when a prior parseable digest existed, else `Created`.
7. Body written: `format!("{}\n", digest.as_str())` = `sha256:<hex>\n`.

### 1.3 `delete_tag` (unconditional, fs.rs:1287–1298)
1. `tokio::fs::remove_file(self.tag_path(name, tag))` where `tag_path` = `root/repos/<name>/tags/<tag>` (fs.rs:886–889).
2. `ErrorKind::NotFound` ⇒ `StorageError::NotFound`; other errors mapped to IO.
3. Best-effort parent-dir fsync. **No lock is taken.**

### 1.4 `delete_tag_conditional` (fs.rs:1380–1444)
1. Compute `dir` and ensure it (same as mutate).
2. Lock `.lock.{tag}` (exclusive flock, `create(true)`) — created **even when the tag is absent**.
3. Read bytes (`std::fs::read`). Absent ⇒ `ConditionalDeleteResult::NotFound` (lock file left behind).
4. If `expected_version = Some(exp)`: compute `hex(sha256(raw_bytes))`; mismatch ⇒ `PreconditionFailed { current_version: Some(actual) }`.
5. If `None`, or version matches: `remove_file` + best-effort dir fsync ⇒ `Deleted`.

### 1.5 Read side (already contained — the consistency anchor)
`get_tag_with_version` / `resolve_tag` route through `self.reader` (`FsMetadataReader`, pinned root fd). `tag_read::tag_key(repo, tag)` (tag_read.rs:86–92) validates both identifiers with `validate_path_component` and builds `ObjectKey::parse("repos/{repo}/tags/{tag}")`; `open_payload` resolves the **whole multi-component relative key** in a single `openat2` beneath the pinned root with `RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`. Version token = `hex(sha256(raw file bytes))` (newline-sensitive).

### 1.6 Trait-boundary and caller reality
- `Storage::{set_tag,mutate_tag,delete_tag,delete_tag_conditional}` (src/storage/mod.rs) take `name: &str, tag: &str` — **no type-level canonicalization guarantee at the storage boundary.**
- Application callers (`src/application/tags.rs`, `src/application/manifest.rs`, `src/http_api/handlers.rs`) generally parse `CanonicalRepoName` upstream, but `src/manifest_lifecycle.rs` and in-crate/test callers pass raw `&str`. The fs implementation therefore **cannot assume** canonical input and validates nothing today.
- No `delete_repository` / `remove_dir_all` of a repo tree exists in `fs.rs` (only `delete_manifest`, `delete_tag`). Repo-tree removal, if it happens at all, is out-of-band / cooperative-external.

## 2. Frozen characterization invariants (must be preserved bit-for-bit)

From `tag_mutation_write_characterization` (7 tests) and the pre-existing concurrency/precondition tests:

1. Final location exactly `repos/<repo>/tags/<tag>`.
2. On-disk body exactly `sha256:<hex>\n` (`format!("{}\n", digest.as_str())`).
3. Version token = raw-byte SHA-256 (newline-sensitive) — `get_tag_with_version` / `delete_tag_conditional` token compatibility.
4. Atomic temp-write + rename visibility; **no `.tmp.*` residue after success.**
5. `.lock.<tag>` created and **retained** (never unlinked); a directory listing after a successful write is exactly `{<tag>, .lock.<tag>}`.
6. Same-digest short-circuit precedes policy: `CreateOnly` over identical digest ⇒ `Unchanged` (no write), not `TagAlreadyExists`.
7. Divergent `CreateOnly` ⇒ `TagAlreadyExists`, existing tag byte-identical (non-destructive).
8. Corrupt/unparseable existing bytes treated as **absent**: `CreateOnly` ⇒ `Created` (overwrite); `Replace` ⇒ `Created` (no `previous`).
9. `delete_tag_conditional(None)` ⇒ `Deleted`; absent ⇒ `NotFound`; version mismatch ⇒ `PreconditionFailed { current_version: Some(..) }`.
10. Unconditional `delete_tag`: success on present; absent ⇒ `StorageError::NotFound`.
11. Existing concurrency semantics (per-`{repo,tag}` `.lock.<tag>` serialization; `delete_tag` unlocked).

**None** of these may be silently converted into a stronger durability guarantee.

## 3. Threat / race model

- **Namespace-traversal / symlink escape (containment target).** Ambient `join` + `std::fs` follows symlinks and does not fail-closed on `..` in the `&str` inputs. The contained read path already fails closed; the write path does not. This is the defect O-04 closes for writes.
- **Ancestor pathname replacement mid-flight.** `repos`, `repos/<repo>`, or `tags` could be renamed/replaced (cooperative or external) between resolution and use. A pinned fd follows the **inode**, not the pathname; a fresh resolution follows the **current pathname**. The two differ precisely under replacement — this is the core of Decision A.
- **Repo deletion + recreation.** A repo dir removed and recreated at the same pathname yields a **new inode**. A stale pinned per-repo authority would keep writing into the detached old inode (invisible to readers resolving the new pathname). This is the hazard §11 addresses.
- **Same-instance concurrency.** Multiple tasks mutating the same `{repo,tag}` must serialize on the same lock inode.
- **Cross-process / multi-instance.** Two `FsStorage` processes over the same root serialize only via on-disk `flock` on the shared `.lock.<tag>` inode; there is **no** cross-process snapshot isolation and none is claimed.
- **Inspect-one-tree / mutate-another split-brain.** An operation must read the existing tag, evaluate policy, and write/delete **within one pinned directory handle**, or it could observe one inode and mutate another.
- **External (non-cooperative) FS mutation** is out of scope for guarantees; containment only constrains *this* implementation's own syscalls to stay beneath the pinned root.

## 4. Decision A — per-repository authority

Tags live at `repos/<repository>/tags/<tag>`, where `<repository>` may itself be **multi-segment** (`team/image`, `a/b/c/d/e`; see §5). The question is how the write side obtains a stable contained authority for the `repos/<repo>/tags` directory.

### Option A — lazily cached per-repository pinned authorities
A map `HashMap<String, ContainedDir>` (or per-repo `OnceCell`) pinning `repos/<repo>/tags` on first use, reused thereafter.
- **Pros:** amortizes the `openat2` chain; mirrors the `UploadAuthorities` `OnceCell` shape.
- **Cons (decisive):**
  - **Stale-inode hazard.** Repo dir deleted/recreated ⇒ cached authority mutates the **detached old inode**; a `set_tag` "succeeds" but is invisible to readers (which re-resolve fresh). This diverges from the read path and from current ambient behavior, i.e. it **changes externally observable deletion/recreation semantics**.
  - **Unbounded memory growth.** One pinned fd per distinct repo, never reclaimed (no online reclamation exists in the dependency; lock/authority lifetimes are process-lifetime). A registry with many repos leaks fds/authorities.
  - **Read/write incoherence.** Reads never cache per-repo; a cached writer and a fresh reader can diverge under replacement.
  - The `UploadAuthorities` caching rationale does **not** transfer: those pin **fixed top-level** dirs (`uploads`, `blobs`, `repo-memberships`) that are never deleted/recreated in normal operation. Per-repo dirs are created and (potentially) removed as repos come and go.

### Option B — fresh contained resolution per operation from a stable pinned `repos` authority (RECOMMENDED)
Pin only the **fixed** `repos` directory once (a top-level, never-deleted namespace root, exactly analogous to `blobs`/`uploads`), and resolve `<repo>/tags` **freshly per operation** beneath it (chained `ensure_subdir` per path segment, one contained `openat2`+`mkdirat` per level), yielding a per-operation `ContainedDir` for the tags directory.
- **Pros:**
  - **Read/write consistency.** Both reads (`open_payload`, fresh per call) and writes re-resolve the current repo inode beneath the same stable root ⇒ identical view of deletion/recreation.
  - **No stale-inode hazard.** A deleted/recreated repo is observed on the next operation; no writing into detached trees.
  - **Bounded memory.** Exactly one cached authority (`repos`); no per-repo growth.
  - **No split-brain.** The whole operation (lock → read existing → policy → write/unlink) runs on the single per-operation tags `ContainedDir` (§7), so it cannot inspect one tree and mutate another within a call.
  - Preserves current externally observable behavior (ambient code also re-resolves the pathname every call).
- **Cons:** one extra `openat2` chain per mutation vs. a cached fd. Tag mutations are **low-frequency** and already pay `spawn_blocking` + `flock` + `fsync`; the extra resolution (a handful of `openat2` on a shallow path) is negligible and not on any hot read path.

### Option C — considered alternatives
- **C1: cache the fixed `repos` authority, resolve `<repo>/tags` fresh (the refinement adopted).** This is Option B made concrete: the *only* thing cached is the never-deleted `repos` root; everything repo-specific is fresh. Recommended.
- **C2: per-repo cache with inode-revalidation** (`fstat` the cached authority and compare against a fresh path resolve before each use). This adds a stat + fresh resolve per op — i.e. it pays Option B's cost **plus** cache bookkeeping, with no benefit. Rejected.
- **C3: pin per-`{repo}` and re-derive on error.** Adds retry/invalidation complexity and still risks a window of writing to a stale inode before the error is observed. Rejected.

### Recommendation
**Option B / C1:** cache the single fixed `repos` authority (lazily, via a `OnceCell<ContainedDir>` beside the existing `UploadAuthorities` root), and resolve `repos/<repo>/tags` **fresh per operation** by chaining `ensure_subdir` beneath it. No per-repository caching. This is the only option that preserves read/write coherence and current deletion/recreation semantics without unbounded fd growth, and it introduces no snapshot-isolation or cross-process claim that the implementation does not actually provide.

## 5. Decision B — path-validation contract

### Existing contracts (from source)
- **Read boundary:** `tag_read::validate_path_component` (tag_read.rs:94–137), applied to **both** repo and tag by `tag_key`. It is **structural**: rejects empty, leading/trailing `/`, `\`, NUL/control chars, and empty/`.`/`..` **segments**; it **allows** interior `/` (multi-segment `org/team/app`, `sub/nested_tag`) and does **not** enforce the OCI charset. Violations ⇒ `StorageError::InvalidRepoName`.
- **Application boundary (upstream, not at storage):** `CanonicalRepoName::parse` (canonical_name.rs) — strict multi-segment OCI grammar. Applied by some callers, **not guaranteed** at the `Storage` trait boundary.
- **Dependency mechanism:** `FileName::new` rejects empty, `.`, `..`, `/`, NUL — i.e. a **single** path component. `ensure_subdir`/`write_leaf_atomic`/`unlink`/`read_leaf`/`lock` each take a `FileName` (single component); `open_contained_dir(relative)` accepts a slash-separated multi-component relative path (each segment validated as a `FileName`, resolved together).

### Options
- **A. Structural containment only (match the read seam):** validate repo and tag with the existing `validate_path_component` contract; realize the repo as a chain of per-segment `FileName`s and the tag as a leaf `FileName`.
- **B. Canonical OCI grammar at the FS boundary:** require `CanonicalRepoName` (and an OCI tag grammar) inside `FsStorage`. This **silently strengthens** the storage-boundary input contract (rejecting names the ambient code and the contained *read* path currently accept), which the task forbids without separate authorization. Rejected.
- **C. Reuse an existing upstream validated type:** `CanonicalRepoName` exists but (a) is not enforced at the trait boundary and (b) is *stricter* than the read seam — adopting it here is equivalent to Option B's strengthening. Rejected as the primary contract; noted as a possible *future* unification if the trait signatures are tightened to accept validated types (out of scope).

### The read/write asymmetry to handle explicitly
The read seam accepts a **multi-segment tag** (`sub/nested_tag`). The dependency's leaf primitives require a **single-component** leaf. Under the current ambient write, a multi-segment tag already **fails** (the temp sibling `.tmp.<tag>.<uuid>` and the rename target sit under a non-existent `tags/<segment>` subdir that `ensure_dir` never creates) — i.e. **no multi-segment tag is writable today.** Making them writable (by creating intermediate tag subdirs) would be *new capability*, not preservation, and is out of scope.

### Recommendation
**Option A, refined:** reuse the existing read-seam contract for filesystem safety, and add the minimal leaf constraint the dependency requires:
1. Validate `repo` with the existing `validate_path_component` semantics (may be multi-segment) → `StorageError::InvalidRepoName` on violation. Split into segments; each becomes a `FileName` for the `ensure_subdir` chain (guaranteed valid because `validate_path_component` already excludes empty/`.`/`..` segments and `/`-adjacent emptiness).
2. Validate `tag` with the same structural rules **and additionally require it to be a single path segment** (reject interior `/`), because it must be a leaf `FileName`. Violation ⇒ `StorageError::InvalidRepoName` (same error kind as the read seam).

This is the **narrowest** rule that establishes filesystem safety while preserving every input that is **writable today**. Its only externally observable delta is the *error taxonomy* for a multi-segment tag: today an opaque IO error, post-cutover a clean `InvalidRepoName`. Both are failures; no successful write changes. Multi-segment tags remain **readable if present** (out-of-band) — the pre-existing read/write asymmetry is preserved, not widened. See §17 for the one point offered to the user.

## 6. Proposed authority hierarchy and lifetime

```
FsMetadataReader (pinned root fd, RESOLVE_BENEATH|NO_SYMLINKS|NO_MAGICLINKS)   [existing; read anchor]
 └─ UploadAuthorities.root : ContainedDir (== reader root inode; captured in `new`)   [existing]
      ├─ uploads / finalized / blobs / memberships : OnceCell<ContainedDir>          [existing, fixed dirs]
      └─ repos : OnceCell<ContainedDir>   ← NEW: fixed top-level, cached once, never deleted
            └─ (per operation, NOT cached)
               ensure_subdir(<repo-seg-1>) … ensure_subdir(<repo-seg-N>)
                └─ ensure_subdir("tags") ⇒ tags_dir : ContainedDir   ← per-operation authority
```

- **Cached (process-lifetime):** the root and the fixed top-level dirs (`repos` added alongside the existing set). These are never deleted/recreated in normal operation, so caching is safe and bounded.
- **Per-operation (dropped at end of call):** everything from `repos/<repo>` down to `tags_dir`. This is the authority all four ops flow through. Dropped when the operation returns; no accumulation.
- **Consistency with reads:** `repos` is opened beneath the **same** pinned root the reader uses (captured together in `new`), so a write's fresh resolution and a read's `open_payload` see the same root inode and therefore the same current repo inode.

## 7. Proposed locking model

- **Identity/placement unchanged:** the advisory lock remains `.lock.<tag>` **inside the per-operation `tags_dir`**, acquired via `ContainedDir::lock(FileName::new(format!(".lock.{tag}")))`. Same name, same directory, same retained-file semantics (dependency `lock` opens `O_RDWR|O_CREAT` and never unlinks — matching current behavior).
- **Critical section on one handle:** `run_locked` runs the body on a blocking thread holding the flock; the body receives a `BlockingDir` for `tags_dir`. **All** of {read existing leaf, policy evaluation, `write_leaf_atomic`/`unlink`, dir `sync`} execute against that one handle ⇒ no inspect-one/mutate-another window (addresses §3).
- **`mutate_tag` / `delete_tag_conditional`:** take the lock (as today).
- **`delete_tag` (unconditional):** **no lock** (as today) — a bare contained `unlink` on `tags_dir`.
- **Serialization guarantees (unchanged, precisely stated):**
  - Same instance, same `{repo,tag}`: serialized on the shared `.lock.<tag>` inode.
  - Cross-process/multi-instance: serialized only by kernel `flock` on the shared on-disk lock inode; **no snapshot isolation, no cross-process transaction** — same as today, and not strengthened.
  - If a repo dir is deleted/recreated between two operations, they resolve different `tags_dir`/lock inodes and do **not** serialize — identical to the current ambient path-based lock and an inherent property of concurrent repo deletion; **no regression.**
- **Lock-file accumulation:** retained `.lock.<tag>` entries accumulate unbounded, exactly as today (no online reclamation in the dependency). No change.

## 8. Read/write authority consistency

- Reads (`open_payload`) and writes (fresh `ensure_subdir` chain) both resolve beneath the **same pinned root**, both **per call**, both with `RESOLVE_BENEATH|NO_SYMLINKS|NO_MAGICLINKS`. Consequently:
  - A tag written via the contained write path is immediately resolvable via the contained read path (same inode reached from the same root).
  - Deletion/recreation of a repo is observed identically by both sides.
  - The version token computed by a read equals `hex(sha256)` of the exact bytes the write placed (`sha256:<hex>\n`), preserving optimistic-concurrency compatibility across `get_tag_with_version` ↔ `delete_tag_conditional` ↔ `mutate_tag`.
- Neither side provides namespace snapshotting nor read/write serialization beyond the per-`{repo,tag}` flock; this design **does not** claim otherwise.

## 9. Exact mapping of each operation to contained primitives

Common prelude (per operation): validate `repo`/`tag` (§5) → `repos_authority = upload_authorities.repos()` (OnceCell init via `ensure_subdir("repos")` on root) → chain `ensure_subdir` for each repo segment then `"tags"` ⇒ `tags_dir: ContainedDir`.

### 9.1 `mutate_tag(name, tag, digest, policy)`
```
lock = tags_dir.lock(FileName(".lock.<tag>"))            // exclusive flock, retained
tags_dir.run_locked(lock, |bd: BlockingDir| {
    existing = match bd.read_leaf(FileName("<tag>"), UNBOUNDED) {   // see §12 note on limit
        Ok(bytes) => Digest::parse(from_utf8_lossy(bytes).trim()).ok(),   // corrupt ⇒ None
        Err(NotFound | NotARegularFile | ResolutionRejected) => None,     // absent-as-None
    };
    if existing == Some(new) { return Unchanged; }                 // short-circuit before policy
    match policy {
        CreateOnly if existing.is_some() => return Err(TagAlreadyExists),   // non-destructive
        _ => bd.write_leaf_atomic(FileName("<tag>"), b"sha256:<hex>\n", durable=true),
    }
    // Created / Replaced{previous} exactly as today
})
```
Primitives: `ensure_subdir`, `lock`, `run_locked`, `read_leaf`, `write_leaf_atomic(durable)`. `set_tag` = `mutate_tag(.., Replace)` unchanged.

### 9.2 `delete_tag(name, tag)` (unconditional, no lock)
```
match tags_dir.unlink(FileName("<tag>"), missing_ok=false) {
    Ok(()) => { tags_dir.sync(); Ok(()) }         // best-effort dir durability
    Err(NotFound) => Err(StorageError::NotFound),
}
```
Primitives: `ensure_subdir` (to resolve `tags_dir`), `unlink`, `sync`. No lock (preserves current semantics).

### 9.3 `delete_tag_conditional(name, tag, expected)`
```
lock = tags_dir.lock(FileName(".lock.<tag>"))         // created even when tag absent (preserved)
tags_dir.run_locked(lock, |bd| {
    bytes = match bd.read_leaf(FileName("<tag>"), UNBOUNDED) {
        Ok(b) => b,
        Err(NotFound) => return ConditionalDeleteResult::NotFound,
    };
    if let Some(exp) = expected {
        actual = hex(sha256(bytes));                  // raw-byte token, unchanged
        if actual != exp { return PreconditionFailed { current_version: Some(actual) }; }
    }
    bd.unlink(FileName("<tag>"), missing_ok=false); bd.sync();
    Deleted
})
```
Primitives: `ensure_subdir`, `lock`, `run_locked`, `read_leaf`, `unlink`, `sync`.

## 10. Error mapping and behavioral compatibility

| Situation | Today | Contained | Compatibility |
|---|---|---|---|
| Invalid repo/tag (structural) | opaque IO error / traversal | `StorageError::InvalidRepoName` (fail-closed) | **Intended containment gain**; matches read seam |
| Multi-segment tag (`a/b`) | opaque IO failure (write never succeeds) | `InvalidRepoName` | Both fail; error-kind delta only (§17) |
| Absent tag (read/mutate) | `None` | `read_leaf`→`NotFound`⇒`None` | Identical |
| Corrupt existing bytes | parsed lossily ⇒ `None` | same parse on `read_leaf` bytes ⇒ `None` | Identical |
| Same digest | `Unchanged` | `Unchanged` | Identical |
| `CreateOnly` divergent | `TagAlreadyExists` | `TagAlreadyExists` | Identical |
| `delete_tag` absent | `NotFound` | `unlink` `NotFound`⇒`NotFound` | Identical |
| `delete_tag_conditional` absent | `NotFound` (lock left) | `read_leaf` `NotFound`⇒`NotFound` (lock left) | Identical |
| Version mismatch | `PreconditionFailed{Some}` | same | Identical |
| Symlinked tag leaf | followed (ambient) | `ResolutionRejected` (fail-closed) | **Intended containment gain**, aligned with read seam |
| Parent-dir fsync failure | ignored (best-effort) | see §12 | One delta (§12/§17) |

`FsMutateError` variants map through the existing `map_fs_mutate_err` / `map_fs_mutate_startup_err` helpers (fs.rs:362–380): `NotFound`→`StorageError::NotFound`, `ResolutionRejected`/`InvalidName`→`InvalidRepoName`, `Busy`→existing busy mapping, `Io`→IO, etc. Any new mapping needed is additive within these helpers.

## 11. Repository deletion / recreation analysis

- **No first-party repo-tree deletion** exists in `fs.rs` today; recreation is via lazy `ensure` on the next tag write, and any full-tree removal is out-of-band/cooperative-external.
- With the **recommended Option B** (no per-repo caching): if a repo dir is pinned-then-deleted-then-recreated, the **next** operation resolves the **new** inode (fresh `ensure_subdir` chain beneath the stable `repos` root), exactly as the read path does. There is **no** stale authority mutating a detached tree, and **no** change to externally observable deletion/recreation semantics.
- Under the rejected Option A (per-repo cache), a cached authority would keep writing into the detached old inode after recreation — a silent split-brain that violates read/write consistency. This is the decisive reason Option A is rejected.
- **Cooperative vs. external:** this design constrains only *this* implementation's syscalls to remain beneath the pinned root and to re-resolve per operation. It makes **no** guarantee against arbitrary external processes racing `rename`/`rmdir` on the tree; it only guarantees that *cooperative* operations never write to an inode unreachable from the current pathname beyond the inherent per-op resolution window.

## 12. Durability guarantees and explicit non-guarantees

Separating the four layers the task requires:

1. **Containment / namespace safety.** Provided by `openat2`+`RESOLVE_BENEATH|NO_SYMLINKS|NO_MAGICLINKS` on every resolution and every leaf op. This is the primary deliverable of the slice.
2. **Atomic rename visibility.** `write_leaf_atomic` performs `O_EXCL` temp create → write → `renameat` — the tag appears atomically or not at all; no `.tmp.*` residue on success. Equivalent to today.
3. **Process-crash behavior.** `durable=true` fsyncs the temp file before rename (matching today's `sync_all`) and fsyncs the parent directory after rename, so a committed rename survives process crash once `write_leaf_atomic` returns. Equivalent-or-stronger than today.
4. **Hardware / power-loss durability.** Bounded by the same single parent-directory fsync the current code already best-effort issues; this slice does **not** introduce a broader barrier/journal scheme and **does not** claim end-to-end power-loss durability beyond that fsync. No repository-wide fsync redesign.

**Mapping decision:** use `write_leaf_atomic(durable=true)`. Current `mutate_tag` already `sync_all`s the temp file and best-effort fsyncs the parent dir, so `durable=true` is the faithful match. There is **no** primitive mode that reproduces "fsync temp **and** *swallow* the parent-dir fsync error": `durable=false` would drop the temp fsync (weaker than today, disallowed), and `durable=true` **propagates** a parent-dir fsync error that today is ignored. That propagation delta is therefore **inseparable** from preserving current temp durability via the primitive, so it is accepted in this slice (an honest failure surfaced, never a weaker guarantee). Consequence to note (§17): a parent-dir fsync *failure* (rare, e.g. `EIO`) surfaces as an error after the rename already made the tag visible, where today the call returned `Ok`.

**Read-limit note (delete_tag_conditional / mutate existing-read):** current code uses unbounded `std::fs::read`. `read_leaf` requires a `limit`; to avoid a semantic change (a pathologically large tag file that currently reads would otherwise become `LimitExceeded`) the cutover passes an effectively-unbounded limit (`u64::MAX`). Tag files are tiny in practice; this preserves exact behavior.

**Non-guarantees (explicit):** no namespace snapshot isolation; no cross-process serialization beyond per-`{repo,tag}` `flock`; no power-loss durability beyond the single parent-dir fsync; no protection against non-cooperative external FS mutation. Full parent-directory-entry power-loss durability, if ever required beyond the current best-effort fsync, is an **O-04 durability follow-up**, not part of this containment slice.

## 13. Dependency changes, if any

**None required.** Every operation maps onto existing `storage-fs` primitives: `ensure_subdir`, `lock`/`run_locked`, `read_leaf`, `write_leaf_atomic(durable)`, `unlink`, `sync`, `inspect`, and `FileName`. No new generic primitive, no registry-specific behavior pushed into `storage-layer-rust`.

Two **minor, inseparable** deltas from reusing the primitives (neither justifies a dependency change in this slice; both are strictly safer):
- **File mode:** `write_leaf_atomic` creates the temp with mode `0o600`, so the final tag file is `0o600` vs. today's `~0o644` (umask-dependent). Tighter, owner-rw. If byte-exact `0o644` is ever required, that is a *future* dependency enhancement (add a mode parameter) — **out of scope**, flagged in §17.
- **Parent-dir fsync error propagation** (see §12).

## 14. Concrete implementation plan (next step — NOT this task)

1. Add `repos: OnceCell<ContainedDir>` to `UploadAuthorities` with an accessor `repos()` that lazily `ensure_subdir("repos")` on the pinned root (mirror the existing `uploads()`/`blobs()` accessors). Confirm capture path in `new`/`try_new` (fs.rs:589/651/659).
2. Add a private helper `tags_authority(&self, repo: &str) -> Result<ContainedDir, StorageError>`: validate `repo` (reuse `tag_read::validate_path_component`), then chain `ensure_subdir` for each `/`-segment and `"tags"` beneath `repos()`. Map `FsMutateError` via the existing helpers.
3. Add a private tag-leaf validator: reuse `validate_path_component` for `tag` plus a single-segment (no interior `/`) check ⇒ `FileName`.
4. Rewrite `mutate_tag` to §9.1 (lock → `run_locked` → `read_leaf` → same-digest short-circuit → policy → `write_leaf_atomic(durable=true)`), preserving `TagMutation` result construction (`Unchanged`/`Created`/`Replaced{previous}`) exactly.
5. Rewrite `delete_tag` to §9.2 (contained `unlink` + `sync`, no lock), preserving `NotFound`.
6. Rewrite `delete_tag_conditional` to §9.3 (lock → `read_leaf` → `None`/hash branch → `unlink` + `sync`), preserving `NotFound`/`PreconditionFailed{current_version}`/`Deleted` and the "lock created even when absent" quirk.
7. Leave `set_tag` as the `mutate_tag(Replace)` delegate.
8. Keep `tag_path`/ambient helpers only if still referenced elsewhere; otherwise remove dead code.
9. Run the gate matrix (fmt, clippy `--lib --tests`, full lib suite) — the seven characterization tests must pass **unchanged**, plus the new production-boundary tests in §15.

## 15. Required production-boundary tests (added at cutover)

Beyond the seven existing characterization tests (which must remain green unchanged), add tests asserting the *containment* the cutover introduces:
1. **Traversal fail-closed:** `set_tag`/`delete_tag`/`delete_tag_conditional` with `..`/leading-slash in `repo` or `tag` ⇒ `InvalidRepoName`, no filesystem effect.
2. **Symlink leaf fail-closed:** a symlinked tag leaf is `ResolutionRejected` (not followed) on read/mutate/conditional-delete, matching the read seam.
3. **Multi-segment tag rejected:** tag containing `/` ⇒ `InvalidRepoName` (documents the §5 taxonomy delta).
4. **Nested-repo happy path:** `team/image` (multi-segment repo) round-trips: `set_tag` writes `repos/team/image/tags/<tag>` with `sha256:<hex>\n`, resolvable by the read seam, lock retained, no `.tmp.*`.
5. **Deletion/recreation coherence:** after removing `repos/<repo>` out-of-band and recreating via a new `set_tag`, a subsequent read/write sees the new inode (no stale-authority write).
6. **Version-token cross-check:** `mutate_tag` then `get_tag_with_version` then `delete_tag_conditional(Some(version))` succeeds; a stale version ⇒ `PreconditionFailed{current_version: Some}`.
7. **No-residue/lock-identity invariance:** post-cutover dir listing after success is exactly `{<tag>, .lock.<tag>}` (guards temp-name-format change).

## 16. Rollout / rollback considerations

- **Rollout:** a single in-crate change to `FsStorage` (no API/signature change on the `Storage` trait; no dependency change). The four method bodies are swapped to the contained path; `UploadAuthorities` gains one `OnceCell`.
- **Behavior preservation gate:** the seven characterization tests are the rollout gate — they assert the externally observable contract (path, body, no residue, lock identity, outcomes) and must pass **unchanged**.
- **Rollback:** revert the single commit; no schema/on-disk-format change occurs (bodies, lock names, and directory layout are identical), so rollback is safe with live on-disk data. The only on-disk-visible difference is tag-file mode (`0o600` vs `~0o644`), which does not impede the reverted ambient reader.
- **Feature-flagging:** optional; given the change is confined and fully covered by characterization tests, a direct swap behind the normal review/test gate is adequate. No staged/dual-write migration is needed (same paths/bytes).

## 17. Open decisions that genuinely require user approval

The design is **behavior-preserving for every input that is writable today** and needs no policy decision to proceed. The following are **awareness items** offered for veto, not blockers; each has a clear recommendation:

1. **Multi-segment tag error taxonomy (minor).** Post-cutover a tag containing `/` returns `InvalidRepoName` instead of today's opaque IO failure. No successful write changes (such tags are unwritable today); multi-segment tags remain *readable* if present. **Recommendation:** accept (clean, fail-closed, symmetric with the read seam).
2. **Tag-file permission mode (minor, on-disk-visible).** Contained tags are created `0o600` vs. today's `~0o644`. Matters only if a *different* UID reads tag files directly by path (not part of the registry's public contract). **Recommendation:** accept the tighter mode; if `0o644` is contractually required, defer to a future dependency enhancement adding a mode parameter (out of scope here).
3. **Parent-dir fsync error surfacing (rare).** A parent-directory fsync failure now surfaces as an error after the tag is already visible, where today it was ignored. Inseparable from preserving temp durability via `write_leaf_atomic(durable=true)` (see §12). **Recommendation:** accept (honest failure surfacing, never a weaker guarantee).

If the user vetoes none of the above, the implementation is fully specified and behavior-preserving; proceed to the cutover step using the consolidated prompt provided in the completion report.

---

### Verification of this design step
- No Rust source modified (design-only). Registry HEAD `00676c72…` and dependency HEAD `0a628fd0…` unchanged; nothing staged.
- Only artifact added: this document. No evidence archive created. No characterization/PHASE5/earlier evidence touched.
