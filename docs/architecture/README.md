# Architecture documents

This directory mixes **accepted decisions**, **historical slice notes**, and a **living current-state inventory**.

Use them in this order:

1. **[current-state.md](current-state.md)** — what the code does at the recorded `master` HEAD. Prefer this over any remaining-gap list, “NOT COMMITTED” stamp, or baseline diagram that disagrees with it.
2. **ADRs 001–009** — accepted layering, ports, composition, CLI policy, test topology, and `StorageErrorKind`. Historical decision records; implementation notes inside them may lag later storage cutovers.
3. **[current-code-assessment.md](current-code-assessment.md)** — original 2026-08-26 baseline plus later slice-status addenda. Section 1 and the pre-addendum maps describe the *baseline*, not HEAD, unless a later addendum says otherwise.
4. **Characterization / cutover notes** (`filesystem-*`, `o-05-*`, supervisor/membership tag-listing notes) — snapshots of a named commit or working tree. Many still say “NOT COMMITTED”, “PRODUCTION UNCHANGED”, or “reaper deferred” after that work landed on `master`. Treat those stamps as historical unless [current-state.md](current-state.md) repeats them.

## Quality-gate IDs

Two different **D-06** labels exist:

- Assessment **D-06** in `current-code-assessment.md` / ADR-009: stringly-typed `StorageError::Internal`. **Resolved.**
- Filesystem-doc **D-06**: extraction, cutover, compatibility, and distribution acceptance. **Still OPEN** as an acceptance gate, not as “no cutover has occurred.”

Canonical FS gates **O-03, O-04, O-05, O-06, O-13, O-15, O-16** remain **OPEN** in the historical notes as *acceptance* criteria. Code can (and does) implement containment without those documents closing the gates.

## Do not treat these as open work

At HEAD they are **gone or already replaced**. Older notes that still describe them are snapshots:

- Ambient upload reaper (`reap_expired_sessions` via `tokio::fs::read_dir`)
- `list_tag_files` as a live production symbol
- `AppState.storage: Arc<dyn Storage>` as a production field
- Handlers calling omnibus storage for mutations
- `FsListingBudgets` as the CAS listing contract
