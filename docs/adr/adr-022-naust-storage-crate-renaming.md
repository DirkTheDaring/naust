# ADR-022: Storage Crate Renaming — `naust-storage-*`

- **Status:** Accepted (2026-09-28)
- **Date:** 2026-09-28
- **Relates to:** ADR-010 (`naust-core` crate boundary), ADR-012 (product naming — Naust).

## 1. Context

In ADR-012, product and repository naming was consolidated under **Naust** (`naust`, `naust-core`, `naust-auth`, `naust-types`). At the time, the underlying storage layer crates in the sibling repository `storage-layer-rust` retained generic names (`storage-core`, `storage-fs`, `storage-s3`).

Retaining generic `storage-*` package names created several inconsistencies:
1. **Naming & Branding Inconsistency:** The storage layer stood out as the only non-prefixed domain component in the repository graph.
2. **Crates.io Publishing Collisions:** Names like `storage-core` and `storage-fs` are already claimed or excessively generic for the public crates.io namespace.
3. **Workspace Path Dependencies:** Downstream crates referenced heterogeneous crate names across `vendor/` and sibling paths.

## 2. Decision

Rename the crates in `storage-layer-rust` to match the unified `naust-*` ecosystem namespace:

| Previous Crate Name | New Crate Name | Directory Path |
|---|---|---|
| `storage-core` | `naust-storage-core` | `crates/naust-storage-core` |
| `storage-fs` | `naust-storage-fs` | `crates/naust-storage-fs` |
| `storage-s3` | `naust-storage-s3` | `crates/naust-storage-s3` |

### Invariants Preserved:
1. **Domain Neutrality:** `naust-storage-core`, `naust-storage-fs`, and `naust-storage-s3` remain completely domain-neutral and registry-agnostic (zero OCI or Docker dependencies).
2. **Descriptor-Relative Containment:** `naust-storage-fs` continues to enforce Linux `openat2` resolution beneath pinned directory descriptors (`RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS | RESOLVE_NO_MAGICLINKS`).
3. **Deterministic Mocking:** `naust-storage-s3` continues to export its in-memory `S3Client` mock double under the `mock-client` test feature.

## 3. Consequences

- **Positive:** Uniform naming across the entire product suite (`naust`, `naust-core`, `naust-auth`, `naust-types`, `naust-storage-core`, `naust-storage-fs`, `naust-storage-s3`).
- **Positive:** Resolves all crates.io namespace collision hazards.
- **Positive:** Clean path-dependency staging in `Makefile` and `vendor/storage-layer-rust/`.
