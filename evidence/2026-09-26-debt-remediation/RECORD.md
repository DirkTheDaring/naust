# Qualification record — technical-debt remediation (R0–R5)

- **Date:** 2026-09-26
- **Revision qualified:** `master` @ `40be191` (chain: R0 `499eb3c` → R1 `fd0e057` → R2 `4ee6522`,`5114848` → R3 `fda6465` → R5 `40be191`)
- **Plan:** `plans/technical-debt-remediation-plan.md`

## Results

| Check | Result |
|---|---|
| `cargo fmt --all --check` | clean |
| `make core-boundary` | clean |
| `cargo test --workspace --locked` | 1511 passed / 0 failed (excl. env-gated live suite) / 14 ignored |
| Conformance `fs` / `basic` / `token` | exit 0 each (junit archived here) |
| Conformance `s3` (live MinIO) | exit 0 (junit archived here) |
| `s3_live_integration` (live MinIO, `--test-threads=1`) | 33 passed / 0 failed / 1 ignored × 2 consecutive runs (R2) |
| TLS reload end-to-end (`tests/tls_reload_tests.rs`) | pass (5-swap soak + SAN-mismatch refusal) |
| Container image build (`podman build`, KI-10) | exit 0; binary executes in image |

MinIO: pre-existing local container started for the s3/live legs and stopped
again afterwards. Live-suite runs for R3+ (post-R2 revisions) cover the same
suite; the R2 live qualification commit point was `5114848`.
