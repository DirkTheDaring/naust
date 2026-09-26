# Operations guide

- **Role:** canonical operational guidance. Describes behavior at `master` @ `2718bc16`; documenting behavior does not approve it — divergences carry KI/GATE IDs from [`technical-debt.md`](technical-debt.md).
- Running, configuration reference, env vars, packaging, and compose recipes live in the root [`README.md`](../README.md). Separate guides retained for distinct topics: [`traefik-configuration.md`](traefik-configuration.md) (reverse-proxy tuning) and [`container-testing-guide.md`](container-testing-guide.md) (client/API test recipes).

## 1. Garbage collection

One GC engine (`src/blob_gc/`), backend-selected strategy:

- **Filesystem — two-phase:** `quarantine` moves eligible blobs to `quarantine/<alg>/<p2>/<hex>` (reversible; timestamp at `quarantine/meta/…<hex>.ts`), then `delete` permanently removes them after `quarantine_delay_secs`. Reads fall back LIVE→QUARANTINED while quarantined.
- **S3 — direct conditional:** single-phase delete with `If-Match` ETag preconditions. Quarantine on S3 returns empty stats; the admin API surfaces HTTP 422 `StrategyUnsupported`. S3 GC **fails closed unless bucket versioning is fully disabled**.

**Three entry points:**

1. **CLI (offline):** `naust blob-gc {plan|quarantine|delete}` — guarded by `FsRootLock` (`fs_root/.locks/naust.lock`) and `RuntimeMutationAuthority`; S3 destructive ops additionally require `--confirm-all-writers-stopped`. Since 2026-09-26 (KI-05 resolved) the CLI **respects** `blob_gc.enabled=false` (and `enable_delete=false` for delete) and refuses with a clear error; pass `--force-gc` to override deliberately. `plan` remains read-only and ungated.
2. **Admin HTTP:** `/_admin/gc/{health,plan,quarantine,delete}`, registered only when `admin_api.enabled`. `plan` is read-only and not kill-switch gated; `quarantine` needs `blob_gc.enabled=true`; `delete` additionally `blob_gc.enable_delete=true`.
3. **Background scheduler:** gated by `blob_gc.schedule_enabled` (interval `schedule_interval_secs`, missed ticks skipped). Each run first executes the **repository-membership sweep** (Active → Candidate → Unlink aging); the sweep has no CLI or admin route.

**Protection model — five axes, all must pass before deletion** (`src/blob_gc/validation.rs:101-181`): not pinned (sled `pins` tree, TTL-heartbeat pins from in-flight uploads/proxy publications) → zero repository memberships → not policy-reachable (`manifest_rooted` default / `tag_rooted`; discovery errors fail closed, never an empty protected set) → no active lifecycle-journal WAL record → older than `min_age_secs`.

**Effective configuration:**

| Knob | Default | Effect |
|---|---|---|
| `blob_gc.enabled` / `.enable_delete` | false / false | Gate admin quarantine/delete + scheduler **and** CLI quarantine/delete (override: `--force-gc`; KI-05 resolved); not admin/CLI `plan` |
| `blob_gc.default_min_age_secs` / `default_quarantine_delay_secs` | 7 d / 24 h | candidate age floor / FS delete delay |
| `blob_gc.default_max_{blobs,bytes,seconds}` | 1000 / u64::MAX / 60 | per-run budgets |
| `blob_gc.schedule_enabled` / `schedule_interval_secs` | false / 7 d | background scheduler |
| `gc_pin_duration_secs` | 3600 | **actual** pin TTL protecting in-flight publications |
| `blob_gc_finalize_grace_secs` | 72 h | **inert — parsed but never read** (KI-03); do not rely on it |
| `ref_index.auto_rebuild_on_corruption` | true | enables index self-heal |
| `s3_single_instance_mode` | — | required (hard config conflict otherwise) for delete-enabled online GC on S3 |
| `storage.fs.gc.discovery.*` | — | resource ceilings for contained FS discovery (limits only; no legacy-path switch exists) |

**Locks:** cross-process safety = FsRootLock (CLI vs server) + deployment writer lease (`RuntimeMutationAuthority`; on S3 an ETag-guarded lease object at `meta/exclusive_writer.lock` — `src/storage/s3.rs:2012`; on the filesystem backend the storage-level lease is the trait's no-op default and single-writer exclusion comes from `FsRootLock`). Deletion needs both a `GcMutationPermit` and a `GcRevalidationGuard`. FS GC run lock file: `quarantine/gc.lock` (two code comments still say `quarantine/.lock` — KI-08).

**Ref index:** sled, 7 trees, states `ready/building/dirty`; rebuild is destructive and serialized; upload finalization heals the index *before* the fail-closed pin gate. Maintenance: `naust ref-index {check|rebuild|ensure}`.

**Membership backfill (pre-existing data):** the server refuses to boot on non-empty storage until `migrate-membership apply` + `verify` have marked storage Ready (empty storage auto-marks). The migration is resumable (checkpoint with 60 s owner lease).

## 2. Identity, RBAC, and tokens

Model: single auth middleware on `/v2/*`; anonymous pull default-on (`auth.anonymous_pull`), overridden per-repo by the configurable `auth.private_name_prefixes` list (default: private/secret/protected/restricted; KI-17 resolved); Basic auth (robots → users → legacy `REGISTRY_USERNAME`/`PASSWORD`; robots shadow users on name collision — `audit-permissions` warns); `/token` mints HMAC-SHA256 Bearer tokens bound to `token.service` with bounded TTL. RBAC invariants (deny-by-default; granted ⊆ requested ∩ policy; prefix-boundary-safe; no arbitrary wildcards) are REQ-005 and are test-covered. An explicit `*` grant confers catalog scope only while `auth.star_grants_catalog=true` (the default; KI-18 resolved).

**Config shape (TOML-only for policy, so it stays in source control):**

```toml
[auth.robots]
enabled = true
[[auth.robots.accounts]]
name = "ci"
secret_hash = "$argon2id$..."           # never plaintext; generate via `naust hash-secret` (reads stdin)
grants = [ { repo_prefix = "org1/", actions = ["pull","push"] } ]
max_ttl_secs = 600

[auth.users]
enabled = true
[[auth.users.accounts]]
name = "alice"
secret_hash = "$argon2id$..."
groups = ["devs"]
[[auth.groups]]                          # note: groups sit at [[auth.groups]], not under users
name = "devs"
grants = [ { repo_prefix = "org1/", actions = ["pull","push"] } ]
```

`repo_prefix` accepts a `prefix/`, an exact repository name, or a bare `*` (discouraged; see KI-18). Prefer one robot per workload.

**Playbooks:**

- *Rotate a robot/user secret:* generate a new hash (`naust hash-secret`, secret on stdin), replace `secret_hash`, restart. Old secrets stop working immediately.
- *Rotate token signing keys (overlap):* add the new key as the FIRST `[[token.signing_keys]]` entry, keep old keys listed for ≥ max token TTL, restart; then remove old keys and restart. The first entry mints; all entries verify. `TOKEN_SIGNING_KEY` env is ignored while the keyring is present.
- *Suspected leakage:* rotate the affected `secret_hash` or remove the compromised signing key, restart. the `token_issued`/`token_denied`/`token_error` tracing events (with denial reasons) are emitted since 2026-09-26 (KI-04 resolved) — filter on `event = "token_denied"` to follow the staged-rollout playbook.
- *Token endpoint rate limit:* global fixed window, env-only (`TOKEN_RATE_LIMIT_RPM`, default 1200/60 s; `0` disables). Outside `Config`, so `check-config` does not validate it (KI-09).

## 3. TLS / ACME

Provisioning runs at startup (`[server.tls.acme]` generates or reuses `cert.pem`/`key.pem` in `output_dir` before binding) **and at runtime** (KI-01 resolved 2026-09-26): a supervised `tls_manager` task re-attempts ACME renewal every `server.tls.acme.renew_check_interval_secs` (default 12 h; the ACME call renews only inside `renewal_window_secs`) and hot-swaps the served certificate via in-place reload — **no restart needed**. For externally renewed certificates (static `TLS_CERT_PATH`/`TLS_KEY_PATH` without ACME), the file is watched every `server.tls.reload_poll_secs` (default 300 s) and reloaded on change.

SAN validation: at startup, ACME-managed certificates are checked against `acme.names` and the server **fails closed on a mismatch** (break-glass override: `server.tls.acme.allow_san_mismatch=true`). At runtime, a renewed certificate that fails parsing or SAN coverage is **refused** — the server keeps serving the previous certificate and logs the refusal. Served SANs and expiry are logged at every load/reload. Covered by unit tests and an end-to-end reload test (`tests/tls_reload_tests.rs`).

Static TLS: set `TLS_CERT_PATH`/`TLS_KEY_PATH`. Behind a reverse proxy, see [`traefik-configuration.md`](traefik-configuration.md) and the `limits.trusted_proxies` setting (X-Forwarded-For is ignored from unlisted peers).

## 4. Proxy (pull-through cache)

Cached content lives in a separate root/prefix (`proxy.cache.fs_root` / `cache_s3_prefix`, default `./data/cache/...`) with its own sled index — cache cleanup is deleting that directory. `max_cache_bytes` is **required** when the proxy is enabled (per upstream route with `[[proxy.upstreams]]`) and is enforced on **both backends** (KI-02 resolved 2026-09-26): a supervised worker bounds the cache to `max_cache_bytes` total, evicting unprotected blobs in LRU order (never-accessed first) via version-conditional deletes; content protected by eviction policies (`keep_tags`, semver pinning) is never evicted — if protected content alone exceeds the budget, a warning reports the residual. The cache listing fails closed on malformed cache-directory entries (process-owned tree; delete the cache directory to recover). Scrub (digest re-verification of cached files) remains filesystem-only by design: S3 object integrity is enforced by the object store. Proxy-only hosts (`[proxy.routing].proxy_hosts` or `[[proxy.upstreams]].hosts`) never consult local storage and reject writes with 405. Safety rails: allowed upstream hosts/repo prefixes, private-network blocking, redirect policy.

## 5. Packaging and deployment cautions

- The packaged conffiles under `etc/naust/` currently ship environment-specific values, an unknown config key that hard-fails strict mode, and credential material — review before deploying a package (KI-11).
- RPM and DEB systemd units diverge (DEB lacks `LimitNOFILE`/`ReadWritePaths`; KI-21).
- The container image build does not stage the `storage-layer-rust` path dependencies; build viability is unverified (KI-10).
- Non-Linux hosts: `FsStorage` requires Linux `openat2` and fails closed at startup elsewhere; non-Linux operation is unverified (GATE-O15).

## 6. Development / observability notes (ADR-010 crate split)

- **Log-filter targets:** the FS storage `tracing` targets renamed with the crate split — `naust::storage::fs` → `naust_core::storage::fs`. Update any `RUST_LOG`/collector filters that reference the old target (KI-27b, resolved here).
- **Test invocation:** the workspace has two crates; bare `cargo test` at the repo root runs only the server package. Use `cargo test --workspace --locked` to include `naust-core`'s ~900 unit tests (KI-27c, resolved here).
