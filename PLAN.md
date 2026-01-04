# Registry-Rust Plan (Docker/OCI Image Registry)

Date: 2026-01-02

## Goal
Implement a Docker/OCI-compatible image registry in Rust.

### Policy
- Anonymous **pull** (no auth required for reads)
- Authenticated **push** (writes require auth)

### Storage
- Pluggable storage backend:
  - Filesystem **default**
  - S3-compatible optional backend

## Compatibility Targets
- Docker CLI (`docker pull`, `docker push`, `docker login`)
- Containerd (OCI distribution compatibility)

## MVP: Supported API Surface
This follows the OCI Distribution / Docker Registry HTTP API v2 shape.

### Required (for docker pull/push)
- `GET /v2/` 
  - Registry ping
  - Must return `Docker-Distribution-API-Version: registry/2.0`

**Pull path (anonymous):**
- `HEAD /v2/<name>/blobs/<digest>`
- `GET  /v2/<name>/blobs/<digest>`
- `HEAD /v2/<name>/manifests/<ref>`
- `GET  /v2/<name>/manifests/<ref>`

**Push path (requires auth):**
- `POST  /v2/<name>/blobs/uploads/`
- `PATCH /v2/<name>/blobs/uploads/<uuid>`
- `PUT   /v2/<name>/blobs/uploads/<uuid>?digest=sha256:...`
- `PUT   /v2/<name>/manifests/<ref>`

### Optional (later)
- `GET /v2/<name>/tags/list`
- Better client performance: `Range` support for blob downloads

## Media Types
MVP should accept/serve:
- OCI image manifest
- Docker v2 schema2 manifest

If unsupported media types are requested or pushed, return a clean registry error.

## Data Model
- **Blob store**: content-addressable by digest (`sha256:<hex>`)
- **Manifest store**:
  - store manifest bytes by computed digest
  - resolve `<ref>`:
    - if `<ref>` is a digest: load manifest by digest
    - if `<ref>` is a tag: resolve `tag -> manifest digest`
- **Tags**: pointers from tag name to manifest digest

## Filesystem Layout (default backend)
Example layout under a root directory (configurable):
- `blobs/sha256/<prefix2>/<digest>`
- `repos/<name>/manifests/<digest>`
- `repos/<name>/tags/<tag>` (content: manifest digest)
- `uploads/<uuid>.data` (resumable upload staging)
- `uploads/<uuid>.state` (offset + metadata)

## Storage Abstraction
Define a `Storage` trait used by HTTP handlers, with implementations:
- `FsStorage` (default)
- `S3Storage` (optional)

The trait covers:
- blob: `has_blob`, `read_blob_stream`, `write_blob_from_reader`
- uploads: `create_upload`, `append_upload`, `finalize_upload`
- manifests: `put_manifest`, `get_manifest`, `head_manifest`
- tags: `get_tag`, `set_tag`, (later `list_tags`)

## Authentication (MVP)
- Use HTTP Basic auth for **push endpoints only**.
- Pull endpoints do not require auth.

Notes:
- Docker supports basic auth via `docker login`.
- In production, prefer TLS termination in front (reverse proxy) or run the registry behind HTTPS.

## RBAC / token issuance (planned)

This is a critical security flow: authorization decisions directly control which Bearer token scopes are minted.

Security-first implementation plan:

- Start with **robot accounts + repo-prefix ACL** (most value, minimal new attack surface).
- Keep authorization logic in a small pure module (easy to review + test).
- Enforce invariants: deny-by-default, never grant more than requested, never grant more than policy allows.

See `docs/rbac.md` for the detailed invariants, TOML schema proposal, and phased rollout plan.

## Error Handling
Implement Docker Registry error response format (JSON with `errors` array) and error codes like:
- `NAME_INVALID`
- `BLOB_UNKNOWN`
- `MANIFEST_UNKNOWN`
- `UNAUTHORIZED`
- `DENIED`

Map errors to correct HTTP status codes.

## Hardening / Operational Defaults
- Body size limits for uploads
- Timeouts
- Path normalization (avoid traversal)
- Structured logging (`tracing`)

## Acceptance Checks (MVP)
1) Ping:
- `curl -i http://localhost:<port>/v2/` returns `200` and header `Docker-Distribution-API-Version: registry/2.0`

2) Push requires auth:
- `docker push localhost:<port>/myrepo:latest` fails before `docker login`

3) Push works with auth, pull is anonymous:
- `docker login localhost:<port>` (basic auth)
- `docker push localhost:<port>/myrepo:latest` succeeds
- `docker logout localhost:<port>`
- `docker pull localhost:<port>/myrepo:latest` succeeds

## Implementation Milestones
1. Scaffold Rust project (`axum`, `tokio`, `tower`, `tracing`).
2. Implement `GET /v2/`.
3. Define `Storage` trait; implement FS backend skeleton.
4. Implement registry error type + response mapper.
5. Implement anonymous pull: blobs + manifests.
6. Implement authenticated push: uploads + manifests.
7. Add config + logging + limits.
8. Add S3 backend.
9. Add integration smoke tests + CI.
10. Add packaging (container image + compose examples).
