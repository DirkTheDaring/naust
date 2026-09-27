# ADR-018: Request-path capacity

- **Status:** Accepted (2026-09-27)
- **Date:** 2026-09-27
- **Relates to:** the high-performance architecture review. Does not change ADR-001 (`ConsistencyCoordinator` stays one process-wide lock).

## Context

Three request-path costs are worth fixing. The single-writer lock and the metric set are not part of this decision.

1. Every rejected upload session in `src/http_api/uploads.rs` drains the body with `to_bytes` and no cap (`usize::MAX`). `/v2` disables Axum's body limit so accepted chunks can stream, so a request that is about to return 400 or 401 can still materialize an arbitrary payload. There are twelve such drains. Accepted `PATCH` and `PUT` bodies already stream and are bounded by `max_upload_bytes` (default 5 GiB) inside the upload session.

2. A closed `Range` on blob GET copies `end - start + 1` bytes into one `Vec` after reading the stream from offset 0 (`src/http_api/blobs.rs`). The filesystem reader is a contained file that has already been type-erased to `AsyncRead`, so the handler cannot seek it. The S3 reader collects the whole object in `S3Driver::get_object` before the handler runs. The HTTP contract is already fixed by tests, including live S3: a closed satisfiable range is 206 with that exact slice; open-ended, suffix, and unsatisfiable ranges are 416 with `Content-Range: bytes */<size>`. A GET with no `Range` header streams.

3. When the upload semaphore (default 32) or the other-request semaphore (default 256) is full, `concurrency_limit_v2_non_upload` waits until the request timeout (default 300 seconds). The per-IP limiter and the token limiter already answer 429 with `Retry-After` and do not wait.

## Decision

1. A rejected upload body is discarded up to `max_request_body_bytes` (default 32 MiB) and then the connection is closed. Bytes are not retained. An accepted upload stream is unchanged.

2. A satisfiable closed blob range is read from the requested offset and streamed back. The filesystem seeks before the file is type-erased. S3 issues a ranged `GetObject` and does not collect the object. The existing status codes and `Content-Range` grammar stay.

3. A `/v2` request that cannot take a slot returns 429 immediately, with `Retry-After: 5` and `Connection: close`. It does not wait on the semaphore.

## Design

### 1. Discard budget

One helper in the upload handler owns all twelve drains. It reads the body as a stream and drops each chunk.

- If `Content-Length` is greater than `max_request_body_bytes`, read nothing.
- If the stream crosses that limit, stop reading.
- In both of those cases set `Connection: close` on the response that was already going to be returned (400, 401, or 404).
- If the body fits, discard it fully and leave the connection reusable. That preserves keep-alive for the small bodies these rejects normally carry (missing `_state`, bad digest, failed auth).

The helper does not wrap accepted `PATCH` or `PUT`. Those keep `MonitoredUploadStream` and `max_upload_bytes`.

`GET` and `HEAD` on an upload session also drain today. They use the same helper. Their bodies are normally empty, so they stay keep-alive.

### 2. Ranged blob read

`BlobCasReader` gains:

```text
open_blob_range(digest, start, end_inclusive)
    -> (BlobMeta, AsyncRead)
```

`BlobMeta.size` remains the full object size, because `Content-Range` needs it. The reader yields exactly `end - start + 1` bytes. A default method may open the full object and skip, so existing test doubles keep compiling. Production filesystem and S3 implementations must override it. A test on each production path fails if the full-object open is used for a range.

The HTTP handler parses `Range` before opening the payload:

| Request | Result |
|---|---|
| No `Range` header | Today's `get_blob`: 200 and `Body::from_stream` of the whole object |
| `bytes=<start>-<end>`, `start <= end < size` | `open_blob_range`, 206, `Content-Length` = span length, `Content-Range: bytes start-end/size`, body is the stream |
| Open-ended (`bytes=0-`), suffix (`bytes=-N`), or `end >= size` | 416, `Content-Range: bytes */size`, no payload read |

Membership and auth run before the open, as they do for a full GET. The handler does not allocate the span.

**Filesystem.** `storage-fs` seeks the contained `std::fs::File` to `start` and limits the read to the span length inside `open_payload` acquisition, before the file is boxed as `AsyncRead`. `naust-core`'s blob adapter calls that ranged open for primary and quarantine keys. Seeking after the box is not possible and is not the design. A read of the prefix is not the design.

**S3.** `AwsS3Driver` gains a ranged get that sets `Range: bytes=<start>-<end>` and returns the SDK body as `AsyncRead` without `collect`. `S3Storage::open_blob` stays the full-object collect. Range GET must not call it. Full-object S3 GET staying buffered is deliberate and out of scope.

**Proxy cache.** A cache hit serves the range through `open_blob_range` on the cache reader. A cache miss still fetches the whole blob into the cache, then serves the range locally. This decision does not pass `Range` through to the upstream registry.

### 3. Slot shed

`try_acquire_owned` failure returns:

- `429 Too Many Requests`
- `Retry-After: 5`
- `Connection: close`
- a short plain-text body, the same shape as the per-IP limiter

The handler does not run, so auth work and the body are not consumed. `Connection: close` covers the unread body on a keep-alive connection. The active-request gauges do not count the refused request. The existing saturation warning, at most once every 10 seconds, stays.

The semaphore layer is the outer layer of the `/v2` router only. `/healthz`, `/metrics`, `/token`, `/_meta`, and `/_admin` do not take these slots. Upload paths and other `/v2` paths keep separate semaphores and the current defaults (32 and 256). No new configuration key.

The request-timeout layer still bounds a request that did get a slot.

## Consequences

- Operators see overload as 429 within one round trip. Clients that only retry on transport errors will need to honor 429; that is the same contract the per-IP limiter already has.
- A hostile or oversized rejected upload can no longer allocate past `max_request_body_bytes`, and past that limit it costs a connection instead of a buffer.
- Pulls that send a closed `Range` read and transmit the span. Pulls that send no `Range` are unchanged, including the buffered full-object S3 GET.
- Implementation touches `naust` (handlers, semaphore), `naust-core` (`BlobCasReader`, S3 driver), and `storage-fs` (seek before type erasure). CI compiles `vendor/`, so `make vendor-sync` is part of landing this.
- `ConsistencyCoordinator` is unchanged. Commit throughput stays one reachability mutation at a time.

## Verification

- A `PATCH` with no `_state` and a body larger than `max_request_body_bytes` returns 400 with `Connection: close` and does not retain the body. A smaller rejected body returns 400 without `Connection: close`.
- An accepted chunked `PATCH` of a blob under `max_upload_bytes` still returns 202.
- The existing closed-range, open-ended, suffix, and 416 cases keep their status and bytes, on filesystem and on S3.
- A filesystem range that starts past the first byte does not read the prefix. An S3 range calls the ranged get and does not call the collecting `get_object`.
- With `max_concurrent_requests = 1`, a second overlapping non-upload `/v2` request returns 429 and `Retry-After: 5` without waiting for the first to finish. `/healthz` still returns 200 while that slot is held.
