# OCI Distribution Spec compliance tests

This directory runs the **official OCI distribution-spec conformance suite**
(<https://github.com/opencontainers/distribution-spec>, `conformance/` subdirectory)
against the current registry implementation as a black box: the runner builds the
registry with `cargo build`, starts a throwaway instance per matrix, and drives it
with the upstream Go test harness.

## Upstream pin

The suite is pinned to **v1.1.1**, commit `a139cc423184af6078077b9b7ee336eddbd03f8f`
(see `common.sh`). On first run it is cloned and built into `.cache/` (gitignored);
repeat runs reuse the cached `conformance.test` binary and work offline.
Override with `DISTRIBUTION_SPEC_REF` / `DISTRIBUTION_SPEC_COMMIT`, or force a
re-fetch with `CONFORMANCE_FORCE_REFRESH=1`.

## Prerequisites

- `git`, `go` (1.20+), `curl`
- `python3` with `pyyaml` (for the derived `result.yaml` summary; `run.sh` only)
- For the `s3` matrix: MinIO at `TEST_S3_ENDPOINT` (default `http://127.0.0.1:9000`),
  e.g. via `scripts/start-minio.sh`

## Usage

```sh
# All 4 matrices; s3 is skipped with a warning if MinIO is unreachable
tests/compliance/run.sh
# or
make conformance

# Selected matrices only (explicitly requesting s3 hard-fails without MinIO)
tests/compliance/run.sh fs token
```

### Matrices (`run.sh`)

| Matrix  | Backend | Auth strategy | Port |
|---------|---------|---------------|------|
| `fs`    | fs      | both          | 5081 |
| `s3`    | s3      | both          | 5082 |
| `basic` | fs      | basic         | 5083 |
| `token` | fs      | token (bearer)| 5084 |

Each matrix enables all four conformance workflows: Pull, Push, Content Discovery,
Content Management.

### Historically-skipped specs (`run-skipped.sh`)

`run-skipped.sh` runs focused invocations that exercise the specs the main run
skips (env-only setup branches, blob delete, and cross-mount with
`REGISTRY_AUTOMATIC_CROSSMOUNT` toggled both ways). It expects a free port at
`ADDR` (default `127.0.0.1:5000`).

## Environment knobs

| Variable | Default | Meaning |
|----------|---------|---------|
| `RESULTS_BASE` | `tests/compliance/results` (`.../results/skipped` for `run-skipped.sh`) | Where reports land |
| `DISTRIBUTION_SPEC_REF` / `DISTRIBUTION_SPEC_COMMIT` | `v1.1.1` / `a139cc4…` | Upstream pin |
| `CONFORMANCE_FORCE_REFRESH` | `0` | `1` = discard `.cache/` and re-fetch/rebuild the harness |
| `TEST_S3_ENDPOINT` / `TEST_S3_BUCKET` / `TEST_S3_REGION` | `http://127.0.0.1:9000` / `registry-live-test` / `us-east-1` | S3 matrix target |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | `minioadmin` / `minioadmin` | S3 credentials |

## Results

Per matrix, under `results/<matrix>/` (gitignored):

- `junit.xml`, `report.html` — official harness output
- `result.yaml` — derived summary (labeled as derived from the official junit.xml)
- `exit_code.txt` — harness exit code
- `registry.log` — registry log for the run

(The pre-existing gitignored `/conformance-results` directory at the repo root is
the legacy location from the retired `scripts/oci-conformance.sh`.)

## Related in-repo tests

The Rust-side conformance regression tests (in-process/black-box, run via
`cargo test`) live in `tests/oci_conformance_regression_tests.rs` and
`tests/oci_1_1_tests.rs`. This directory complements them with the official
upstream suite.
