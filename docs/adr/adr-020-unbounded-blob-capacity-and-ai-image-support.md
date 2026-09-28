# ADR-020: Unbounded Default Blob Capacity & AI Container Scale

- **Status:** Accepted (2026-09-28)
- **Date:** 2026-09-28
- **Relates to:** ADR-003 (storage capability ports), ADR-010 (`naust-core` crate boundary), ADR-018 (request-path capacity).

## 1. Context

Large AI and ML container images (e.g., PyTorch, CUDA base distributions, LLM model weights formatted as Safetensors or GGUF, and training checkpoints) commonly range from 10 GiB to 500+ GiB per layer blob.

The codebase historically contained several implicit and explicit size and timeout barriers:

1. **Upload Size Ceilings:** `max_upload_bytes` defaulted to `5 * 1024 * 1024 * 1024` (5 GiB) in configuration and Docker packaging. Standard AI containers were rejected out-of-the-box with HTTP 413 Payload Too Large.
2. **S3 Multipart 50 GB Ceiling:** S3 multipart upload chunking was hardcoded to `S3_PART_SIZE = 5 * 1024 * 1024` (5 MiB). Because AWS S3 and MinIO enforce a hard limit of 10,000 parts per multipart upload (`1 <= PartNumber <= 10000`), uploads of blobs larger than $\approx 48.82\text{ GiB}$ failed on part #10001 with S3 `InvalidArgument`.
3. **Timeout Abort on Large Streams:** `upload_request_timeout_secs` defaulted to 3600s (1 hour), cutting off slow multi-hundred GB uploads. Non-upload routes (including proxy blob downloads on cache misses) were bound to `request_timeout_secs` (300s / 60s), abruptly terminating upstream layer fetches.

## 2. Decision

1. **Unbounded Default Blob Capacity (`max_upload_bytes = 0`):**
   - The default value of `max_upload_bytes` is changed to `0` (unlimited).
   - Blob uploads are unrestricted by default across filesystem and S3 backends.
   - An explicit upper limit is enforced only when `max_upload_bytes > 0` is configured via `limits.max_upload_bytes` or `MAX_UPLOAD_BYTES`.
   - `0` is consistently established across all storage backends (`fs.rs`, `s3.rs`, `upload_coordinator.rs`) as meaning "no size limit".

2. **Configurable & Scalable S3 Multipart Part Sizing:**
   - Add `part_size_bytes` to S3 session configuration (`[uploads.s3] part_size_bytes` / `S3_PART_SIZE_BYTES`).
   - Server default is set to `64 MiB` ($67,108,864\text{ bytes}$), expanding default single-blob capacity on S3 to **640 GiB**.
   - Validated between S3 protocol limits: 5 MiB minimum, 5 GiB maximum (enabling single-blob capacity up to **5 TiB**).

3. **Decoupled Streaming Timeouts & Resilient Activity Guards:**
   - `upload_request_timeout_secs` defaults to `0` (unlimited wall-clock duration), shifting streaming protection to activity-based monitors.
   - Blob streaming endpoints (`/v2/.../blobs/...`) are classified as streaming paths, exempt from the short metadata API `request_timeout_secs`.
   - Default `upload_chunk_idle_timeout_secs` is increased to `60` seconds to accommodate client-side layer compression pauses.

4. **Resource Profile Policy & Memory Budget Auto-Tuning:**
   - Introduces `ResourceProfile` (`AiScale`, `Balanced`, `LowMemory`) to automatically balance throughput vs. memory footprint.
   - Auto-tunes S3 part size (8 MiB to 64 MiB) and concurrency slots based on explicit `memory_budget_bytes` (< 512 MiB $\to$ `LowMemory`, 512 MiB–2 GiB $\to$ `Balanced`, $\ge$ 2 GiB $\to$ `AiScale`).
   - Maintains strict precedence where individual explicit configuration options override profile presets.

## 3. Architecture & Invariants

```
                        ┌─────────────────────────────────────────────────────────┐
                        │                   HTTP Request Layer                    │
                        │   - No hardcoded Content-Length cutoff by default       │
                        │   - Streaming MonitoredUploadStream (activity-based)    │
                        └───────────────────────────┬─────────────────────────────┘
                                                    │
                                                    ▼
                        ┌─────────────────────────────────────────────────────────┐
                        │              Upload Coordinator & Core API              │
                        │   - max_upload_bytes = 0 -> Unlimited                   │
                        │   - Verified streaming digest computation (SHA-256/512) │
                        └───────────────────────────┬─────────────────────────────┘
                                                    │
                                   ┌────────────────┴────────────────┐
                                   ▼                                 ▼
                     ┌───────────────────────────┐     ┌───────────────────────────┐
                     │   Filesystem Backend      │     │       S3 / MinIO Backend  │
                     │  - Zero copy chunk write  │     │  - Configurable Part Size │
                     │  - 0 = Unlimited bytes    │     │  - 64MB-512MB chunking    │
                     │  - Atomic hash recovery   │     │  - Up to 5 TB per blob    │
                     └───────────────────────────┘     └───────────────────────────┘
```

- **Zero-Copy Streaming:** Blobs continue to stream directly between network sockets and storage (filesystem disk or S3 multipart chunk buffers) without buffering full multi-gigabyte layers in memory.
- **Transport-Free Core:** `naust-core` receives policy parameters via `S3SessionConfig` and `UploadCoordinatorConfig` without depending on HTTP frameworks or server configuration types.

## 4. Consequences

- **Positive:** Out-of-the-box compatibility with large multi-hundred GB AI/ML container images.
- **Positive:** S3 storage backend supports large model layers up to 640 GB by default, and up to 5 TB with 512 MiB parts.
- **Positive:** No unexpected upload aborts for long-running pushes over slow or bursty network connections.
- **Operational:** Operators requiring hard quotas can set `limits.max_upload_bytes` to enforce an explicit byte ceiling.
