# FalconFS KV Cache Memory Pool Design (Standalone)

## 1. Purpose

This document defines a complete, implementation-ready KV cache memory pool design for FalconFS integration with vLLM offloading.  
It is fully self-contained and does not depend on any external design document.

---

## 2. System Goals

1. Low-latency KV block lookup/load/store for inference.
2. Batch-first APIs for throughput and reduced RTT.
3. Correctness under partial failures, retries, and failover.
4. Deterministic recovery from persistent metadata.
5. BRPC as the unified RPC transport.

---

## 3. Architecture

### 3.1 Components

- **Client (Python OffloadingManager)**
  - Computes target metadata DN from block hash.
  - Calls DN metadata APIs via BRPC.
  - Calls Store data APIs via BRPC using DN-returned location.
  - Maintains short-lived local cache for recent block locations and lease data.

- **Metadata DN (PostgreSQL extension + C/C++)**
  - Source of truth for KV block metadata.
  - In-memory bitmap allocator (per store).
  - In-memory lease manager.
  - In-memory LRU manager.
  - Eviction coordinator.

- **Store (C++)**
  - DRAM block pool for hot data.
  - SSD spill files for evicted blocks.
  - Batch read/write data service.

### 3.2 Hot path latency model

For cache hit load:
1. Client -> DN (`BatchLookupWithLease`)
2. Client -> Store (`BatchReadBlock` or `BatchReadFromSSD`)

Target: 2 RTT on common path.

---

## 4. Data Model

## 4.1 Persistent metadata table

Table: `falcon_kvblock_table`

- `block_hash BYTEA PRIMARY KEY`
- `kv_group_idx INT NOT NULL`
- `layer_mask INT NOT NULL`
- `status SMALLINT NOT NULL`
- `store_node_id INT NOT NULL`
- `pool_offset BIGINT NOT NULL`
- `evicted_path TEXT NULL`
- `version BIGINT NOT NULL`
- `updated_at_ms BIGINT NOT NULL`

Status enum:
- `0 = ALLOCATED`
- `1 = STORED`
- `2 = EVICTING`
- `3 = EVICTED`
- `4 = FAILED`

### 4.2 In-memory DN state

- `bitmap[store_id]`: tracks occupied/free block slots.
- `lease_map[block_hash]`: token, owner_client_id, expire_ms, dn_epoch.
- `lru`: tracks access recency for `STORED` blocks.

---

## 5. BRPC API Contracts

All batch APIs return per-item:
- `success`
- `error_code`
- `retryable`
- item payload fields

DN metadata APIs:
1. `BatchLookupWithLease`
2. `BatchAllocateWithLease`
3. `BatchRenewLease`
4. `BatchUpdateBlockStatus` (CAS with expected version)
5. `BatchFreeAllocated`

Store data APIs:
1. `BatchWriteBlock`
2. `BatchReadBlock`
3. `BatchReadFromSSD`

Idempotency:
- Mutating APIs require `request_id`.
- DN/Store retain request result digest for replay-safe retries.

---

## 6. State Machine and Consistency Rules

### 6.1 State transitions

- `ALLOCATED -> STORED` only after Store confirms per-item write success.
- `STORED -> EVICTING -> EVICTED` via two-phase eviction.
- `EVICTING -> STORED` on eviction write failure.
- `ALLOCATED/STORED/EVICTED -> FAILED` for unrecoverable error.

### 6.2 CAS protection

All status updates use `(block_hash, expected_version)`:
- update only if current version matches expected.
- success increments version by 1.
- conflict returns `CAS_CONFLICT` (retryable).

### 6.3 Partial success policy

Batch operations are never treated as all-or-nothing by default.
- Every item is independently acknowledged.
- Client only commits metadata for successful Store writes.

---

## 7. Lease Design

### 7.1 Lease fields

- `lease_token`
- `owner_client_id`
- `expire_ms`
- `dn_epoch`

### 7.2 Renew rules

Renew succeeds when:
- owner matches caller and lease token valid, or
- owner is `UNKNOWN` (recovered lease) and caller claims ownership.

Renew fails when:
- stale epoch/token,
- active lease held by another owner.

### 7.3 Eviction check

Block can be evicted only if:
- status is `STORED`,
- no valid active lease.

---

## 8. Core Flows

### 8.1 Lookup + load

1. Client checks local cache.
2. Misses grouped by DN -> `BatchLookupWithLease`.
3. Hits grouped by Store -> batch read from DRAM/SSD.
4. Client calls `BatchRenewLease` for loaded keys.

### 8.2 Store (write path)

1. Client `BatchAllocateWithLease`.
2. Client sends `BatchWriteBlock` to Store.
3. Client collects per-item successes.
4. Client calls `BatchUpdateBlockStatus` only for successful writes:
   - expected: `ALLOCATED`
   - target: `STORED`
5. Optional cleanup for failed writes via `BatchFreeAllocated`.

### 8.3 Eviction

1. Select cold candidates from LRU.
2. CAS `STORED -> EVICTING`.
3. Store writes SSD file and returns result.
4. On success: CAS `EVICTING -> EVICTED` + save `evicted_path`.
5. On failure: CAS `EVICTING -> STORED`.

---

## 9. Recovery and Failover

### 9.1 Startup/failover procedure

1. DN increments `dn_epoch`.
2. Rebuild bitmap by scanning rows with statuses occupying DRAM (`ALLOCATED`, `STORED`, `EVICTING`).
3. Rebuild LRU from `STORED`.
4. Rebuild lease_map with owner `UNKNOWN` and current epoch.

### 9.2 Stale fencing

Any request with old epoch/token is rejected (`STALE_EPOCH`, retryable with refresh).

---

## 10. Concurrency Model

1. Shard-group metadata requests before processing.
2. Use shard-level locks (avoid global lock bottleneck).
3. Open relation/index once per shard-group.
4. Use bounded sub-batch size (for example, 64).
5. Use bounded worker pools.

---

## 11. Error Model

Recommended error codes:
- `OK`
- `NOT_FOUND`
- `INVALID_ARGUMENT`
- `LEASE_EXPIRED`
- `LEASE_OWNER_MISMATCH`
- `STALE_EPOCH`
- `CAS_CONFLICT`
- `STORE_WRITE_FAILED`
- `THROTTLED`
- `INTERNAL_ERROR`

Retryable:
- `CAS_CONFLICT`, `THROTTLED`, transient network/IO failures, stale epoch (after refresh).

Non-retryable:
- malformed request, persistent ownership mismatch, checksum corruption.

---

## 12. Performance Targets

For batch size 10 (initial target):
- `BatchLookupWithLease`: < 100 us (metadata side)
- `BatchAllocateWithLease`: < 80 us
- `BatchWriteBlock`: < 150 us (excluding large payload network variance)
- `BatchReadBlock`: < 120 us
- `BatchRenewLease`: < 60 us

Global goals:
- Stable p99 under sustained load.
- No correctness regression under retries/failover.

---

## 13. Observability

Per API metrics:
- qps, p50, p95, p99
- success rate, retryable error rate
- CAS conflict rate
- lease renew failure rate
- eviction throughput and rollback count
- recovery time

Structured logs:
- include `request_id`, `client_id`, `block_hash`, `dn_epoch`, `error_code`.

---

## 14. Security and Validation

1. Validate payload size against block size.
2. Validate checksum on write/read path.
3. Sanitize SSD paths (prevent traversal).
4. Reject oversized batch request beyond configured max.

---

## 15. Implementation Blueprint

### 15.1 Files and modules

- `vllm_kv_cache/proto/kv_metadata_service.proto`
- `vllm_kv_cache/proto/kv_data_service.proto`
- DN:
  - `metadata/kv_metadata_service_impl.*`
  - `metadata/kv_meta_table_accessor.*`
  - `metadata/bitmap_allocator.*`
  - `metadata/lease_manager.*`
  - `metadata/lru_manager.*`
  - `metadata/eviction_coordinator.*`
- Store:
  - `store/kv_data_service_impl.*`
  - `store/dram_block_pool.*`
  - `store/ssd_spill_manager.*`
- Python:
  - `python/falconfs_kv/offloading_manager.py`
  - `python/falconfs_kv/dn_client.py`
  - `python/falconfs_kv/store_client.py`
  - `python/falconfs_kv/router.py`

### 15.2 Delivery phases

1. Define proto and service skeleton.
2. Implement DN metadata core with CAS + lease.
3. Implement Store read/write/ssd.
4. Integrate Python OffloadingManager.
5. Add eviction + recovery.
6. Performance tuning and fault-injection tests.

---

## 16. Test Matrix

1. Batch lookup miss/hit.
2. Batch store then load cycle.
3. Partial write failure: only successful keys move to `STORED`.
4. CAS conflict behavior under concurrent status updates.
5. Lease expiry and renew correctness.
6. DN restart recovery and renewed lease ownership claim.
7. Eviction failure rollback.
8. Stale epoch rejection after failover.
9. Large-batch chunking and p99 stability.

---

## 17. Design Decisions and Reasons (Explicit)

1. **BRPC everywhere**
   - one transport stack reduces operational and code complexity.
2. **DN-local bitmap**
   - keeps allocation local, low-latency, and deterministic.
3. **Client direct-to-DN**
   - removes CN from hot path and reduces RTT.
4. **Per-item batch results**
   - required for safe partial success handling.
5. **CAS versioning**
   - prevents stale-write corruption in concurrent updates.
6. **Two-phase eviction**
   - avoids exposing incomplete DRAM->SSD transitions.
7. **Lease epoch fencing**
   - ensures safety after restart/failover.
8. **Idempotent mutating APIs**
   - makes retry behavior correct under timeout/network uncertainty.

