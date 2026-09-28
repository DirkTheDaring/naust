# Adaptive Resource Policy & Runtime Thread Auto-Tuning

This document describes the design, execution tiers, and resolution mechanics of Naust's Adaptive Resource Policy Engine.

---

## 1. Motivation & Problem Statement

Container registries experience dynamic workloads with starkly contrasting resource footprints:
- **Small Microservice Images:** Frequent, high-concurrency requests with tiny layer payloads (tens of megabytes).
- **AI/ML Container Images:** Long-running, high-bandwidth uploads with massive layer payloads (10 GiB–500+ GiB).
- **Constrained Edge Deployments:** Running within minimal memory envelopes ($< 512\text{ MB}$ RAM) on IoT appliances or Kubernetes pods with tight cgroups limits.
- **High-Core Bare Metal Servers:** Running across 64–128+ physical CPU cores with 100GbE NICs and distributed S3 storage.

To run reliably across this spectrum without manual micro-configuration or crash risks, Naust provides a multi-tier adaptive resource policy.

---

## 2. The Three-Dimensional Resource Model

Naust manages capacity across three coordinated architectural layers:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        1. OS & RUNTIME THREADPOOLS                      │
│   - worker_threads: Async Tokio event loops (CPU crypto, TLS, I/O poller)│
│   - max_blocking_threads: Sync blocking pool (Filesystem I/O, disk sync)│
└────────────────────────────────────┬────────────────────────────────────┘
                                     │
┌────────────────────────────────────▼────────────────────────────────────┐
│                      2. APPLICATION CONCURRENCY                         │
│   - max_concurrent_upload_requests: Active concurrent upload streams    │
│   - max_concurrent_buffered_requests: Memory-buffered operations        │
│   - max_concurrent_requests: Total in-flight non-upload requests        │
└────────────────────────────────────┬────────────────────────────────────┘
                                     │
┌────────────────────────────────────▼────────────────────────────────────┐
│                      3. STORAGE BUFFERS & CHUNKING                      │
│   - s3_part_size_bytes: S3 multipart chunk size (8 MiB - 64 MiB)        │
│   - stream hash buffers: Constant O(1) 64 KiB slice verification        │
│   - upload_chunk_idle_timeout_secs: Stream silence watchdog             │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Profile Tiers

| Parameter | `LowMemory` Tier | `Balanced` Tier | `AiScale` Tier *(Default)* |
| :--- | :--- | :--- | :--- |
| **Target Environment** | $< 512\text{ MB}$ RAM / Edge | $512\text{ MB} - 2\text{ GB}$ RAM | $\ge 2\text{ GB}$ RAM / AI Workloads |
| **Worker Threads** | $\min(2, N_{\text{cpu}})$ | $\min(4, N_{\text{cpu}})$ | $N_{\text{cpu}}$ (Full CPU parallelism) |
| **Blocking Threads** | **16** | **32** | $\min(512, \max(64, N_{\text{cpu}} \times 8))$ |
| **S3 Part Size** | **8 MiB** (max 80 GB blob) | **16 MiB** (max 160 GB blob) | **64 MiB** (max 640 GB blob) |
| **Upload Slots** | **8** streams | **16** streams | **32** streams |
| **Buffered Slots** | **2** slots | **4** slots | **8** slots |
| **General Requests** | **64** requests | **128** requests | **256** requests |
| **Peak Upload RAM** | $\approx 64\text{ MB}$ | $\approx 256\text{ MB}$ | $\approx 2.0\text{ GB}$ |

---

## 4. Configuration & Precedence Semantics

Configuration parameters are resolved strictly in order of precedence:

1. **Explicit Individual Knobs (Highest Priority):**
   - E.g. `WORKER_THREADS`, `MAX_BLOCKING_THREADS`, `S3_PART_SIZE_BYTES`, `MAX_CONCURRENT_UPLOAD_REQUESTS`.
2. **Explicit Profile Selection:**
   - E.g. `RESOURCE_PROFILE="balanced"` or `[resources] profile = "low_memory"`.
3. **Auto-Tuning by Memory Budget:**
   - E.g. `MEMORY_BUDGET_BYTES=268435456` (auto-selects `LowMemory`).
4. **Default Baseline (Lowest Priority):**
   - E.g. `AiScale` defaults with $N_{\text{cpu}}$ worker threads and 64 MiB S3 part sizes.

---

## 5. Usage Examples

### Minimal TOML Configuration (`config.toml`)
```toml
[resources]
profile = "balanced"

# Optional granular override
[uploads.s3]
part_size_bytes = 33554432  # 32 MiB override
```

### Environment Variable Equivalents
```bash
# Set resource profile directly
export RESOURCE_PROFILE=low_memory

# Or specify a container memory budget
export MEMORY_BUDGET_BYTES=268435456

# Explicit granular overrides take priority
export S3_PART_SIZE_BYTES=67108864
export MAX_CONCURRENT_UPLOAD_REQUESTS=24
```
