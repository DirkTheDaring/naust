# ADR-021: Adaptive Resource Policy & Runtime Threadpool Auto-Tuning

- **Status:** Accepted (2026-09-28)
- **Date:** 2026-09-28
- **Relates to:** ADR-005 (server runtime composition root), ADR-010 (`naust-core` crate boundary), ADR-018 (request-path capacity), ADR-020 (unbounded blob capacity and AI image support).

## 1. Context

Naust operates across diverse infrastructure targets, from minimal edge containers with constrained memory (128 MiB–512 MiB RAM) to high-spec multi-socket bare metal pushing 500+ GiB AI/ML container images over high-speed networks.

Historically, runtime thread management and concurrency limits exhibited potential friction under polarized workloads:
1. **OS Thread Stack Consumption:** In Tokio's default multi-threaded runtime, up to 512 blocking threads can be spawned for synchronous operations (e.g. filesystem storage I/O, directory traversal). On small memory budgets ($< 512\text{ MB}$ RAM), spawning hundreds of OS threads consumes gigabytes in thread stack allocations alone, causing abrupt out-of-memory (OOM) process termination.
2. **CPU Parallelism Underutilization:** On high-core bare metal, running with default worker configurations without coordinating application semaphores with S3 multipart buffers created suboptimal throughput during massive parallel TLS termination and cryptographic digest hashing.
3. **Configuration Complexity:** Operators had to manually calculate and coordinate multiple low-level knobs (`MAX_CONCURRENT_UPLOAD_REQUESTS`, `S3_PART_SIZE_BYTES`, `MAX_CONCURRENT_BUFFERED_REQUESTS`, `MAX_CONCURRENT_REQUESTS`, worker threads, blocking thread pools) to achieve optimal performance and safety.

## 2. Decision

We introduce a unified **Adaptive Resource Policy Engine** that provides declarative profiling and auto-tuning across runtime threads, application concurrency semaphores, and storage multipart part sizes.

1. **Resource Profile Presets (`ResourceProfile`):**
   - **`AiScale` (Default):** Tailored for high-throughput image transfers and AI/ML model artifacts.
     - Worker threads: Detected CPU cores ($N_{\text{cpu}}$)
     - Blocking threads: $\min(512, \max(64, N_{\text{cpu}} \times 8))$
     - S3 Part Size: 64 MiB (supporting single blobs up to 640 GiB)
     - Upload slots: 32 concurrent upload streams
     - Buffered request slots: 8 slots
     - General request slots: 256 slots
   - **`Balanced`:** Designed for general-purpose microservice registries.
     - Worker threads: $\min(4, N_{\text{cpu}})$
     - Blocking threads: 32
     - S3 Part Size: 16 MiB (supporting single blobs up to 160 GiB)
     - Upload slots: 16 concurrent upload streams
     - Buffered request slots: 4 slots
     - General request slots: 128 slots
   - **`LowMemory`:** Designed for constrained edge appliances and small containers ($< 512\text{ MB}$ RAM).
     - Worker threads: $\min(2, N_{\text{cpu}})$
     - Blocking threads: 16 (prevents thread stack explosion)
     - S3 Part Size: 8 MiB (supporting single blobs up to 80 GiB)
     - Upload slots: 8 concurrent upload streams
     - Buffered request slots: 2 slots
     - General request slots: 64 slots

2. **Auto-Tuning by Memory Budget (`memory_budget_bytes`):**
   - When a memory budget is specified (or configured via `MEMORY_BUDGET_BYTES` or container cgroup limits) without an explicit profile name:
     - $\text{Budget} < 512\text{ MiB} \implies \textbf{LowMemory}$
     - $512\text{ MiB} \le \text{Budget} < 2\text{ GiB} \implies \textbf{Balanced}$
     - $\text{Budget} \ge 2\text{ GiB} \implies \textbf{AiScale}$

3. **Strict Override Precedence Hierarchy:**
   - **Tier 1 (Highest):** Explicit individual knobs (`WORKER_THREADS`, `MAX_BLOCKING_THREADS`, `S3_PART_SIZE_BYTES`, `MAX_CONCURRENT_UPLOAD_REQUESTS`, etc.).
   - **Tier 2:** Explicit profile selection (`RESOURCE_PROFILE`).
   - **Tier 3:** Derived tier from `MEMORY_BUDGET_BYTES`.
   - **Tier 4 (Lowest):** Global default (`AiScale`).

4. **Zero Runtime Coupling in Core ([ADR-010](adr-010-naust-core-crate-boundary.md)):**
   - `naust-core` remains purely domain- and storage-focused with zero knowledge of Tokio runtime builders, threadpools, or server configuration files.

## 3. Architecture & Invariants

```
 ┌─────────────────────────────────────────────────────────────────────────────────────────┐
 │                                   CONFIGURATION LAYER                                   │
 │   Environment Variables        Configuration File              System Hardware/Cgroups  │
 │  (WORKER_THREADS, S3_*, ...)     (config.toml)               (available_parallelism)   │
 └───────────────────────────────────────────┬─────────────────────────────────────────────┘
                                             │
                                             ▼
 ┌─────────────────────────────────────────────────────────────────────────────────────────┐
 │                            RESOURCE POLICY RESOLUTION ENGINE                            │
 │   Explicit Overrides   ───────►   Profile Presets   ───────►   Auto-Tuning Engine       │
 │   (Highest Priority)             (AiScale/Balanced/LowMem)     (Memory Budget / CPU)    │
 └───────────────────────────────────────────┬─────────────────────────────────────────────┘
                                             │
             ┌───────────────────────────────┼───────────────────────────────┐
             │                               │                               │
             ▼                               ▼                               ▼
 ┌───────────────────────┐       ┌───────────────────────┐       ┌───────────────────────┐
 │   RUNTIME & THREADS   │       │      CONCURRENCY      │       │    STORAGE & I/O      │
 │       (main.rs)       │       │    (supervisor.rs)    │       │     (naust-core)      │
 ├───────────────────────┤       ├───────────────────────┤       ├───────────────────────┤
 │ • worker_threads      │       │ • max_concurrent_     │       │ • s3_part_size_bytes  │
 │ • max_blocking_       │       │   upload_requests     │       │ • stream hash buf     │
 │   threads             │       │ • max_concurrent_     │       │ • upload_chunk_idle_  │
 │                       │       │   buffered_requests   │       │   timeout_secs        │
 │                       │       │ • max_concurrent_reqs │       │ • zero-copy stream    │
 └───────────────────────┘       └───────────────────────┘       └───────────────────────┘
```

## 4. Consequences

- **Positive:** Out-of-the-box resilience against thread stack and buffer exhaustion OOMs in small container deployments.
- **Positive:** Optimized multi-core CPU and high-throughput network performance for massive AI/ML container workloads.
- **Positive:** Clean operational ergonomics: operators can either set a single high-level profile/budget or customize specific knobs with guaranteed precedence.
- **Negative / Operational:** Operators running in custom environments must be aware that setting `RESOURCE_PROFILE=low_memory` clamps concurrency to 8 upload slots unless explicitly overridden via `MAX_CONCURRENT_UPLOAD_REQUESTS`.
