# Qualification record — Naust public launch

- **Date:** 2026-09-26
- **Repositories published (public, MIT):**
  - https://github.com/DirkTheDaring/naust
  - https://github.com/DirkTheDaring/storage-layer-rust
  - https://github.com/DirkTheDaring/acmecert
- **First green hosted CI run (KI-19 criterion):** https://github.com/DirkTheDaring/naust/actions/runs/36238807454
  - `checks` job: fmt → ADR-010 boundary gate → `cargo test --workspace --locked --no-fail-fast` → conformance fs/basic/token — **success**
  - `live-s3` job: MinIO (bitnamilegacy archive, digest-pinned) → full `s3_live_integration` suite (`--ignored --test-threads=1`) → conformance s3 — **success**
- **Local verification at push:** workspace 1598 passed / 0 failed / 47 ignored (live suite is opt-in via `--ignored`).

## Learnings encoded during launch hardening

1. Four swap/identity test fixtures assumed freed inode numbers are never reused (true on btrfs, false on the runners' ext4) — fixed with an inode-keeper helper; fixtures are now filesystem-agnostic.
2. MinIO's official images (docker.io and quay.io) no longer allow anonymous pulls; CI uses the frozen `bitnamilegacy/minio` archive pinned by digest.
3. The live-S3 suite is ignore-gated (opt-in), restoring its historical contract; plain workspace runs report it as ignored.
