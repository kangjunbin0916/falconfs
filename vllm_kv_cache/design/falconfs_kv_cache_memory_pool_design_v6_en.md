# FalconFS KV Cache Memory Pool Design (v6 Canonical)

## Document Info

| Item | Value |
|---|---|
| Version | v6.0 |
| Date | 2026-04-30 |
| Status | Canonical implementation standard |
| Transport | BRPC only |
| Proto set | `kv_common.proto`, `kv_metadata_service.proto`, `kv_data_service.proto` |

---

## 1. Scope and Goals

This document defines the production design for FalconFS KV cache memory pool used by vLLM offloading.

Primary goals:
1. Keep hot-path latency low (2 RTT hit path: DN metadata + Store data).
2. Ensure correctness under retries, partial failures, and failover.
3. Support batch-first operations with per-item outcomes.
4. Provide deterministic recovery from persisted metadata.
5. Keep contracts and implementation aligned with BRPC proto definitions.

Non-goals:
- Global cross-DN transactions in KV fast path.
- Multi-tenant security model beyond request validation and path safety.

---

## 2. Architecture

### 2.1 Components

- **Python OffloadingManager**
  - Computes DN route from `block_hash`.
  - Calls DN metadata service over BRPC.
  - Calls Store data service over BRPC.
  - Maintains local short-TTL location/lease cache.

- **Metadata DN (PostgreSQL extension + C/C++)**
  - Persistent metadata table authority.
  - In-memory allocator state: bitmap.
  - In-memory lifecycle state: lease map + LRU.
  - Runs background eviction and recovery workflows.

- **Store (C++)**
  - DRAM block storage.
  - SSD spill/readback for evicted blocks.
  - Per-item write/read response reporting.

### 2.2 Hot path

Lookup/load hit:
1. Client -> DN: `BatchLookupWithLease`
2. Client -> Store: `BatchReadBlock` or `BatchReadFromSSD`

---

## 3. Data Model and Invariants

### 3.1 Persistent metadata (`falcon_kvblock_table`)

Required columns:
- `block_hash BYTEA PRIMARY KEY`
- `kv_group_idx INT NOT NULL`
- `layer_mask INT NOT NULL`
- `status SMALLINT NOT NULL`
- `store_node_id INT NOT NULL`
- `pool_offset BIGINT NOT NULL`
- `evicted_path TEXT NULL`
- `version BIGINT NOT NULL`
- `updated_at_ms BIGINT NOT NULL`

Recommended indexes:
- primary index on `block_hash`
- secondary index on `(status, updated_at_ms)` for recovery/eviction scans
- optional index on `(store_node_id, pool_offset)` for consistency audits

### 3.2 In-memory DN state

- `bitmap_by_store[store_id]`: allocation occupancy.
- `lease_map[block_hash]`: `{lease_token, owner_client_id, expire_ms, dn_epoch}`.
- `lru`: only `STORED` blocks.
- `idempotency_store[(api_name, request_id)]`: prior result replay.

### 3.3 State machine

Use proto enum `BlockStatus`:
- `BLOCK_STATUS_ALLOCATED`
- `BLOCK_STATUS_STORED`
- `BLOCK_STATUS_EVICTING`
- `BLOCK_STATUS_EVICTED`
- `BLOCK_STATUS_FAILED`

Valid transitions:
- `ALLOCATED -> STORED` (only after confirmed data write success)
- `STORED -> EVICTING -> EVICTED`
- `EVICTING -> STORED` (rollback on spill failure)
- `ALLOCATED/STORED/EVICTED -> FAILED` (irrecoverable path)

### 3.4 Core invariants (must hold)

1. For `STORED`, DRAM data must exist at `(store_node_id, pool_offset)`.
2. For `EVICTED`, `evicted_path` must be non-empty and readable.
3. Every status update must pass CAS by `expected_version`.
4. Bitmap occupancy must match metadata occupancy for non-evicted blocks.
5. Lease check must gate eviction/destructive operations.

---

## 4. BRPC Contract Semantics

Proto files:
- `vllm_kv_cache/proto/kv_common.proto`
- `vllm_kv_cache/proto/kv_metadata_service.proto`
- `vllm_kv_cache/proto/kv_data_service.proto`

Contract rules:
1. **Per-item responses are required** for all batch APIs.
2. Mutating APIs must include `CommonRequestMeta.request_id`.
3. Server must replay idempotent result for duplicate `(api, request_id)`.
4. `ItemResultMeta` drives retry logic (`retryable` + `ErrorCode`).
5. `LeaseInfo.dn_epoch` must be checked by client and server.

---

## 5. Detailed Flow Design

### 5.1 Batch lookup

1. Client groups keys by DN.
2. Client sends `BatchLookupWithLease` with `renew_lease_on_hit=true`.
3. DN per key:
   - read metadata row,
   - validate state,
   - optionally renew lease,
   - return `status`, `location`, `lease`, `version`, `cacheable`.
4. Client caches positive location entries until lease expiry.

### 5.2 Batch allocate

1. Client sends `BatchAllocateWithLease`.
2. DN deduplicates request keys if requested.
3. Per key:
   - if existing valid row and idempotent replay path, return existing allocation,
   - else allocate bitmap slot (prefer requested store when valid),
   - insert metadata row in `ALLOCATED`,
   - grant lease and return location + version.

### 5.3 Batch write + complete store

1. Client sends `BatchWriteBlock` to Store grouped by store node.
2. Store validates item sizes/checksum and writes each payload.
3. Store returns per-item `WriteResult`.
4. Client builds success subset and calls `BatchUpdateBlockStatus`:
   - `expected_from_status=ALLOCATED`
   - `to_status=STORED`
   - `expected_version` from allocation response.
5. Failed items:
   - optional immediate `BatchFreeAllocated`, or
   - background timeout cleanup path.

### 5.4 Batch load

1. Client selects `STORED` vs `EVICTED` items from DN response.
2. For `STORED`: call `BatchReadBlock`.
3. For `EVICTED`: call `BatchReadFromSSD`.
4. Client validates checksums and returns `LoadStoreSpec`.
5. Client calls `BatchRenewLease` after successful consumption.

### 5.5 Eviction

1. DN eviction worker scans cold `STORED` keys from LRU.
2. DN skips keys with valid active lease.
3. DN CAS `STORED -> EVICTING`.
4. Store spill to SSD.
5. On success: DN CAS `EVICTING -> EVICTED` + set `evicted_path`.
6. On failure: DN CAS rollback `EVICTING -> STORED`.

---

## 6. Lease and Epoch Model

### 6.1 Lease renewal rules

Renew succeeds when:
- owner matches requester and token/epoch valid, or
- owner is recoverable-unknown and caller claims ownership (`allow_owner_claim`).

Renew fails when:
- epoch stale (`STALE_EPOCH`),
- lease expired and claim disallowed,
- owner mismatch while lease active.

### 6.2 Epoch fencing

- DN increments `dn_epoch` at restart/failover.
- Newly issued leases include current epoch.
- Old epoch requests are rejected as retryable stale responses.

---

## 7. Recovery Design

### 7.1 Startup recovery sequence

1. Load/confirm shard routing metadata.
2. Increment `dn_epoch`.
3. Rebuild bitmap from rows in occupying states (`ALLOCATED`, `STORED`, `EVICTING`).
4. Rebuild LRU from `STORED`.
5. Rebuild lease map with unknown owner and current epoch.
6. Start serving metadata APIs.
7. Continue optional deep consistency scan in background.

### 7.2 Recovery correctness rules

1. No key may be allocated twice to the same `(store, offset)`.
2. Recovered leases must be claimable; never lock blocks permanently.
3. `EVICTING` leftovers are reconciled:
   - if spill artifact exists and valid -> finalize to `EVICTED`
   - else rollback to `STORED`.

---

## 8. Concurrency and Locking

1. Group by shard before metadata operations.
2. Use shard-level locks (allocator and metadata hot paths).
3. Open relation/index once per shard-group operation.
4. Keep lock scope minimal around CAS + bitmap mutation.
5. Use bounded worker pools and bounded batch chunks.

Recommended defaults:
- batch chunk: 64 items
- max in-flight batch RPC per client per DN: configurable, start with 32

---

## 9. Error Handling and Retry Policy

Use `ErrorCode` from `kv_common.proto` and enforce:

Retryable:
- `CAS_CONFLICT`
- `THROTTLED`
- transient `STORE_WRITE_FAILED`
- `STALE_EPOCH` (after refresh)

Non-retryable:
- `INVALID_ARGUMENT`
- persistent `LEASE_OWNER_MISMATCH`
- `CHECKSUM_MISMATCH` (investigate data path)

Client retry strategy:
- exponential backoff + jitter
- preserve `request_id` for idempotent replay on mutating retries

---

## 10. Performance Design

Targets (batch size 10, baseline):
- metadata lookup path: < 100 us
- allocate path: < 80 us
- renew path: < 60 us
- read/write path: optimize for stable p99 under load

Optimization knobs:
1. DN grouping by shard.
2. Store grouping by node.
3. request chunking (avoid oversized RPC payloads).
4. channel reuse and bounded queue depths.
5. optional compression for large payloads only.

---

## 11. Observability

Per API metrics:
- qps, latency p50/p95/p99
- success/error/retry counts by `ErrorCode`
- CAS conflict rate
- lease renewal failure rate
- eviction commit/rollback count
- recovery duration and reconciled key count

Structured logging fields:
- `request_id`, `trace_id`, `client_id`, `block_hash`,
- `status`, `version`, `dn_epoch`, `error_code`

---

## 12. Security and Validation

1. Validate payload and block size boundaries.
2. Enforce checksum verification when enabled.
3. Sanitize and constrain SSD path prefixes.
4. Reject oversized batch requests.
5. Validate ownership and epoch before destructive operations.

---

## 13. Implementation Plan

### Phase 1: Contract and scaffolding
1. Finalize proto + codegen wiring.
2. BRPC service skeletons for DN/Store.
3. Shared enums/error mapping library.

### Phase 2: DN core
1. Table accessor with internal PostgreSQL API.
2. Bitmap allocator.
3. Lease manager + epoch logic.
4. CAS status updater.
5. Idempotency store.

### Phase 3: Store core
1. DRAM write/read handlers.
2. SSD spill/read handlers.
3. per-item checksum/compression logic.

### Phase 4: Client integration
1. DN/Store client pools.
2. Router and local cache logic.
3. OffloadingManager batch APIs.

### Phase 5: Recovery + eviction hardening
1. restart/failover reconciliation.
2. `EVICTING` recovery rule implementation.
3. fault injection tests.

### Phase 6: Performance and stability
1. latency tuning and queue/backpressure tuning.
2. p99 regression gates.

---

## 14. Test Matrix (Required)

1. Batch lookup miss/hit correctness.
2. Store->lookup->load cycle correctness.
3. Partial store failure with selective status update.
4. CAS conflict under concurrency.
5. Lease expiry/renew/owner-claim behavior.
6. DN restart with lease/map rebuild and successful renew.
7. Eviction rollback on spill failure.
8. stale epoch request rejection and retry recovery.
9. large-batch chunking and overload behavior.

---

## 15. Why this design is improved

1. Fully self-contained and proto-aligned (no hidden assumptions).
2. Explicit invariants and transition legality reduce implementation ambiguity.
3. Per-item + idempotent semantics make retries safe in distributed failures.
4. Lease epoch fencing and owner-claim path fix restart/failover hazards.
5. Two-phase eviction with rollback prevents data visibility corruption.

