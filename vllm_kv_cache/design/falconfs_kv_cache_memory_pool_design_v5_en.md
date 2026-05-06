# FalconFS KV Cache Memory Pool Design (v5, Self-Contained Standard)

## Document Info

| Item | Value |
|---|---|
| Version | v5.1 |
| Date | 2026-04-30 |
| Scope | Standalone canonical design for FalconFS KV cache memory pool |
| Goal | Define a single self-contained implementation standard with BRPC-first interfaces and production-grade correctness/performance constraints |

---

## 1. Design Objectives

1. Keep hot-path latency low with client direct routing to DN and DN-local allocation.
2. Preserve correctness under retries, partial failures, and DN restarts/failovers.
3. Keep all APIs batch-first with explicit per-key outcomes.
4. Reuse patterns already proven in FalconFS metadata code (shard grouping, bounded grouping, cache invalidation).

---

## 2. Target Architecture

### 2.1 Roles

- **Client / OffloadingManager (Python)**
  - Computes target DN from `block_hash`.
  - Sends metadata requests to DN directly.
  - Sends data requests to Store based on DN-returned location.
  - Maintains short-lived local cache for lookup results and lease metadata.

- **Metadata DN (PostgreSQL extension + C/C++)**
  - Source of truth for block metadata table.
  - In-memory managers: bitmap allocator, lease map, LRU manager.
  - Coordinates eviction state transitions.
  - Provides batch metadata APIs.

- **Store (C++)**
  - DRAM block pool and SSD spill files.
  - Batch read/write APIs.
  - Returns per-item write outcomes.

### 2.2 Hot Path

- **Normal lookup/load hit path** remains 2 RTT:
  1. Client -> DN (`BatchLookupWithLease`)
  2. Client -> Store (`BatchReadBlock` or `BatchReadFromSSD`)

---

## 3. Data Model and States

## 3.1 KV block metadata table

Recommended columns:

- `block_hash BYTEA PRIMARY KEY`
- `kv_group_idx INT`
- `layer_mask INT`
- `status SMALLINT`:
  - `0=ALLOCATED`
  - `1=STORED`
  - `2=EVICTING`
  - `3=EVICTED`
  - `4=FAILED`
- `store_node_id INT`
- `pool_offset BIGINT`
- `evicted_path TEXT NULL`
- `version BIGINT` (CAS fence)
- `updated_at_ms BIGINT`

### 3.2 In-memory state (DN)

- `bitmap[store_id]`: occupied/free block index map.
- `lease_map[block_hash]`: `{token, owner_client_id, expire_ms, epoch}`.
- `lru`: only blocks in `STORED`.

### 3.3 State transition rules

- `ALLOCATED -> STORED`: only after successful data write confirmation.
- `STORED -> EVICTING -> EVICTED`: two-phase eviction.
- `ALLOCATED/STORED -> FAILED`: when an unrecoverable write/error happens.
- Any transition must be guarded by `version` compare-and-swap.

---

## 4. API Contracts (Batch-first)

All batch APIs return per-item fields:

- `key` / `block_hash`
- `success`
- `error_code`
- `retryable`
- operation-specific payload

**Transport choice (final):** BRPC for both DN metadata service and Store data service.

### 4.1 DN APIs

1. `BatchLookupWithLease(keys, client_id, client_hostname, request_id)`
2. `BatchAllocateWithLease(keys, context, request_id)`
3. `BatchRenewLease(keys, client_id, request_id)`
4. `BatchUpdateBlockStatus(updates, request_id)` with CAS `version`
5. Optional: `BatchFreeAllocated(keys, request_id)` for rollback cleanup

### 4.2 Store APIs

1. `BatchWriteBlock(items[{pool_offset, bytes, checksum}], request_id)`
2. `BatchReadBlock(offsets, block_size)`
3. `BatchReadFromSSD(paths)`

---

## 5. End-to-End Flows

### 5.1 Lookup + load

1. Client checks local cache.
2. Misses are grouped by DN and sent with `BatchLookupWithLease`.
3. Hits are grouped by Store and read in batch from DRAM/SSD.
4. Client sends `BatchRenewLease` for consumed keys.

### 5.2 Prepare store + complete store

1. Client calls `BatchAllocateWithLease`.
2. Client writes data via `BatchWriteBlock`.
3. Client calls `BatchUpdateBlockStatus` only for keys confirmed as written.
4. Failed keys:
   - remain `ALLOCATED` and get reclaimed by timeout cleanup, or
   - become `FAILED` and freed eagerly.

### 5.3 Eviction

1. Candidate selection: old LRU + no valid lease.
2. CAS update `STORED -> EVICTING`.
3. Copy DRAM block to SSD and fsync.
4. CAS update `EVICTING -> EVICTED` and set `evicted_path`.
5. On copy failure, rollback `EVICTING -> STORED`.

---

## 6. Recovery and Failover

### 6.1 Restart/failover recovery order

1. Recover bitmap from metadata (`ALLOCATED/STORED/EVICTING` considered occupied).
2. Recover LRU from `STORED`.
3. Recover lease entries with `epoch = current_dn_epoch`.

### 6.2 Lease recovery fix

Use ownership state machine:

- `owner = UNKNOWN` for recovered entries.
- First successful renew with valid epoch claims ownership.
- Subsequent renew must match owner or be rejected.

This avoids the recovered-lease deadlock where blocks cannot be renewed after restart/failover.

### 6.3 Epoch fencing

- DN increments `dn_epoch` at startup/failover.
- Lease token and renewal checks include epoch.
- Requests carrying stale epoch are rejected as retryable stale errors.

---

## 7. Concurrency, Idempotency, and Consistency

1. Use shard-level locks for bitmap and metadata hot paths.
2. Use `request_id` on mutating APIs for idempotent retries.
3. Require per-key responses; never assume all-or-nothing batch success.
4. Use CAS `version` for status transitions to prevent stale writer overwrite.
5. Keep lease validation in eviction and destructive update paths.

---

## 8. Performance Plan

1. Group requests by DN first, then by Store.
2. Cap batch size (for example 64 or 128 keys) to control tail latency.
3. Use bounded thread pools (`max_workers`) and persistent connections.
4. Expose p50/p95/p99 and retry/error counters by API.
5. Keep one table open/index open per shard-group operation where possible.

---

## 9. Design Points and Reasons (Explicit)

1. **Client direct routing to DN**
   - **Reason:** removes CN from hot path and keeps metadata RTT minimal.

2. **DN-local bitmap allocation**
   - **Reason:** allocation decision is local, no extra network hop.

3. **Batch-first APIs with per-key result**
   - **Reason:** real systems have partial success; per-key status enables safe retry.

4. **Two-phase eviction state (`EVICTING`)**
   - **Reason:** prevents exposing half-evicted blocks and supports rollback.

5. **Lease epoch fencing + recovery claim logic**
   - **Reason:** fixes restart/failover correctness and blocks stale clients.

6. **CAS version on status updates**
   - **Reason:** avoids stale metadata overwrite under concurrent writers.

7. **Idempotency request IDs**
   - **Reason:** network timeout retries become safe and deterministic.

8. **Typed hash consistency (`BYTEA` + matching operators)**
   - **Reason:** avoids index lookup mismatch and correctness regressions.

9. **Bounded batch size and bounded workers**
   - **Reason:** protects p99 latency and avoids overload from large bursts.

10. **Shard-grouped metadata operations**
   - **Reason:** reduces table/index open-close overhead and improves locality.

---

## 10. Improvements Inspired by FalconFS Code

The following choices are directly aligned with existing FalconFS implementation patterns:

1. **Group by shard before processing**
   - Observed in metadata handlers that build shard-indexed hash tables and process per shard.
   - Applied to KV metadata batch lookup/allocate/update.

2. **Open relation/index once per shard-group**
   - Existing code processes grouped entries with a single open/close cycle.
   - Applied to reduce overhead for KV batch metadata operations.

3. **Bounded sub-batch execution**
   - Existing metadata path uses bounded group size (`BATCH_OPERATION_GROUP_SIZE`).
   - Applied to KV APIs to control lock hold time and failure blast radius.

4. **Explicit cache invalidation/reload model**
   - Shard table logic uses invalidation + reload of shared cache.
   - Applied to client shard-table cache with explicit refresh trigger and TTL fallback.

5. **Fine-grained latency instrumentation**
   - Existing code tracks operation breakdown (`table_open`, `index_open`, `remote_call`, etc.).
   - Applied to KV APIs so optimization targets are evidence-driven.

---

## 11. Rollout Plan (Practical)

### Phase A: correctness first

1. Finalize state machine and CAS semantics.
2. Introduce per-key batch response schema.
3. Add `request_id` + idempotency storage.
4. Implement lease epoch fencing and recovery owner-claim logic.

### Phase B: performance

1. Shard-grouped metadata batch execution.
2. Batch size tuning and worker pool limits.
3. Connection reuse and backpressure.

### Phase C: reliability

1. Failure injection tests (partial write, DN restart, stale token, CAS conflict).
2. End-to-end retry tests for idempotent APIs.
3. Recovery duration and correctness validation at scale.

---

## 12. Test Matrix (Minimum)

1. **Partial write failure:** some Store writes fail, verify only successful keys become `STORED`.
2. **Lease recovery:** DN restart, recovered blocks can be renewed and are not spuriously evicted.
3. **Stale epoch rejection:** old client tokens fail after failover.
4. **CAS conflict:** concurrent update collision returns retryable conflict.
5. **Eviction rollback:** SSD write failure returns block to `STORED`.
6. **Batch overload:** large key list is chunked and processed without p99 explosion.

---

## 13. Open Decisions to Confirm

1. Persist lease table or reconstruct leases entirely from metadata on recovery.
2. `FAILED` state retention policy vs immediate free-on-failure.
3. Final batch size defaults by workload profile.

---

## 14. Detailed BRPC Interface Design

### 14.1 Proto package split

- `kv_metadata_service.proto`: DN-facing metadata APIs.
- `kv_data_service.proto`: Store-facing data APIs.
- Use a shared enum file or duplicated stable integer constants for status/error codes.

### 14.2 Metadata proto (DN)

```protobuf
syntax = "proto3";
package falconfs.kv;

service KVMetadataService {
  rpc BatchLookupWithLease(BatchLookupRequest) returns (BatchLookupResponse);
  rpc BatchAllocateWithLease(BatchAllocateRequest) returns (BatchAllocateResponse);
  rpc BatchRenewLease(BatchRenewLeaseRequest) returns (BatchRenewLeaseResponse);
  rpc BatchUpdateBlockStatus(BatchUpdateStatusRequest) returns (BatchUpdateStatusResponse);
  rpc BatchFreeAllocated(BatchFreeAllocatedRequest) returns (BatchFreeAllocatedResponse);
}

message RequestMeta {
  string request_id = 1;
  int64 client_id = 2;
  string client_hostname = 3;
  int64 client_epoch_hint = 4;
}

message LookupItem {
  bytes block_hash = 1;
}
message LookupResult {
  bytes block_hash = 1;
  bool success = 2;
  int32 error_code = 3;
  bool retryable = 4;
  int32 status = 5;
  int32 store_node_id = 6;
  int64 pool_offset = 7;
  string evicted_path = 8;
  int64 lease_token = 9;
  int64 lease_expire_ms = 10;
  int64 version = 11;
  int64 dn_epoch = 12;
}
message BatchLookupRequest {
  RequestMeta meta = 1;
  repeated LookupItem items = 2;
}
message BatchLookupResponse {
  repeated LookupResult results = 1;
}

message AllocateItem {
  bytes block_hash = 1;
  int32 kv_group_idx = 2;
  int32 layer_mask = 3;
  int32 block_size = 4;
}
message AllocateResult {
  bytes block_hash = 1;
  bool success = 2;
  int32 error_code = 3;
  bool retryable = 4;
  int32 store_node_id = 5;
  int64 pool_offset = 6;
  int64 lease_token = 7;
  int64 lease_expire_ms = 8;
  int64 version = 9;
  int64 dn_epoch = 10;
}
message BatchAllocateRequest {
  RequestMeta meta = 1;
  repeated AllocateItem items = 2;
}
message BatchAllocateResponse {
  repeated AllocateResult results = 1;
}

message RenewLeaseItem {
  bytes block_hash = 1;
  int64 lease_token = 2;
}
message RenewLeaseResult {
  bytes block_hash = 1;
  bool success = 2;
  int32 error_code = 3;
  bool retryable = 4;
  int64 lease_token = 5;
  int64 lease_expire_ms = 6;
  int64 dn_epoch = 7;
}
message BatchRenewLeaseRequest {
  RequestMeta meta = 1;
  repeated RenewLeaseItem items = 2;
}
message BatchRenewLeaseResponse {
  repeated RenewLeaseResult results = 1;
}

message StatusUpdateItem {
  bytes block_hash = 1;
  int32 expected_from_status = 2;
  int32 to_status = 3;
  int64 expected_version = 4;
  string evicted_path = 5;
}
message StatusUpdateResult {
  bytes block_hash = 1;
  bool success = 2;
  int32 error_code = 3;
  bool retryable = 4;
  int64 new_version = 5;
}
message BatchUpdateStatusRequest {
  RequestMeta meta = 1;
  repeated StatusUpdateItem items = 2;
}
message BatchUpdateStatusResponse {
  repeated StatusUpdateResult results = 1;
}

message FreeAllocatedItem {
  bytes block_hash = 1;
  int64 expected_version = 2;
}
message FreeAllocatedResult {
  bytes block_hash = 1;
  bool success = 2;
  int32 error_code = 3;
  bool retryable = 4;
}
message BatchFreeAllocatedRequest {
  RequestMeta meta = 1;
  repeated FreeAllocatedItem items = 2;
}
message BatchFreeAllocatedResponse {
  repeated FreeAllocatedResult results = 1;
}
```

### 14.3 Data proto (Store)

```protobuf
syntax = "proto3";
package falconfs.kv;

service KVDataService {
  rpc BatchWriteBlock(BatchWriteBlockRequest) returns (BatchWriteBlockResponse);
  rpc BatchReadBlock(BatchReadBlockRequest) returns (BatchReadBlockResponse);
  rpc BatchReadFromSSD(BatchReadFromSSDRequest) returns (BatchReadFromSSDResponse);
}

message DataRequestMeta {
  string request_id = 1;
  int64 client_id = 2;
}

message WriteItem {
  bytes block_hash = 1;
  int64 pool_offset = 2;
  bytes payload = 3;
  uint32 crc32 = 4;
}
message WriteResult {
  bytes block_hash = 1;
  bool success = 2;
  int32 error_code = 3;
  bool retryable = 4;
}
message BatchWriteBlockRequest {
  DataRequestMeta meta = 1;
  repeated WriteItem items = 2;
}
message BatchWriteBlockResponse {
  repeated WriteResult results = 1;
}

message ReadItem {
  bytes block_hash = 1;
  int64 pool_offset = 2;
  int32 block_size = 3;
}
message ReadResult {
  bytes block_hash = 1;
  bool success = 2;
  int32 error_code = 3;
  bool retryable = 4;
  bytes payload = 5;
}
message BatchReadBlockRequest {
  DataRequestMeta meta = 1;
  repeated ReadItem items = 2;
}
message BatchReadBlockResponse {
  repeated ReadResult results = 1;
}

message SSDReadItem {
  bytes block_hash = 1;
  string evicted_path = 2;
}
message SSDReadResult {
  bytes block_hash = 1;
  bool success = 2;
  int32 error_code = 3;
  bool retryable = 4;
  bytes payload = 5;
}
message BatchReadFromSSDRequest {
  DataRequestMeta meta = 1;
  repeated SSDReadItem items = 2;
}
message BatchReadFromSSDResponse {
  repeated SSDReadResult results = 1;
}
```

---

## 15. Component-Level Implementation Design

### 15.1 DN components

1. `KVMetadataBrpcService`  
   - Parses BRPC request and validates fields.
   - Performs idempotency check for mutating calls.
   - Dispatches to `KVMetadataEngine`.

2. `KVMetadataEngine`  
   - Owns high-level workflows:
     - lookup + lease issue
     - allocate + metadata insert
     - renew lease
     - status CAS update
     - free allocated block

3. `KVMetaTableAccessor` (PostgreSQL internal API wrapper)  
   - Open/close relation and index once per shard-group.
   - Provides typed methods:
     - `BatchLookupByHash`
     - `BatchInsertAllocated`
     - `BatchCASStatusUpdate`
     - `BatchDeleteOrMarkFailed`

4. `BitmapAllocator`  
   - per-store bitmap, free list hints, shard-lock partitioning.
   - methods:
     - `Allocate(client_affinity, block_size)`
     - `Free(store_id, pool_offset)`
     - `RecoverOccupied(store_id, pool_offset)`

5. `LeaseManager`  
   - maintains `lease_map`.
   - supports `Grant`, `Renew`, `ClaimRecovered`, `ValidateEvictable`.
   - background expiration scan.

6. `LRUManager`  
   - tracks `STORED` keys only.
   - supports touch/promote and cold candidate iteration.

7. `EvictionCoordinator`  
   - consumes LRU candidates.
   - performs `STORED -> EVICTING -> EVICTED` with Store callback and rollback.

8. `IdempotencyStore`  
   - maps `(request_id, api)` to prior result digest/entries.
   - TTL-based cleanup.

### 15.2 Store components

1. `KVDataBrpcService`
2. `DRAMBlockPool` (`offset -> address`)
3. `SSDSpillManager` (`write/read/fsync`, deterministic path format)
4. `WriteVerifier` (crc32 validation and payload size checks)

### 15.3 Python client components

1. `DNClientPool` (BRPC channel reuse)
2. `StoreClientPool` (BRPC channel reuse)
3. `ShardRouter` (local cache of shard table + invalidation refresh)
4. `KVOffloadingManager`:
   - `batch_lookup`
   - `prepare_store` (batch allocate)
   - `complete_store` (batch write + status update only success keys)
   - `prepare_load` (batch read DRAM/SSD)
   - `complete_load` and `touch` (batch renew)

---

## 16. Detailed Execution Logic

### 16.1 `BatchAllocateWithLease` (DN)

1. Validate items and deduplicate duplicate hashes in request.
2. Check idempotency by `request_id`; replay result if seen.
3. Group keys by local shard.
4. For each shard-group:
   - open relation/index once,
   - for each key:
     - if already exists and status usable, return existing location (idempotent allocate),
     - else allocate bitmap slot,
     - insert row with `ALLOCATED`, `version=1`,
     - grant lease token with `dn_epoch`,
     - add per-key success result.
5. Persist idempotency result.

### 16.2 `BatchWriteBlock` + `BatchUpdateBlockStatus` (Client + DN + Store)

1. Client sends grouped writes to Store.
2. Store returns per-key write result.
3. Client builds `success_keys` only.
4. Client sends CAS updates (`expected_from=ALLOCATED`, `to=STORED`, `expected_version`).
5. For failed writes:
   - optional immediate `BatchFreeAllocated`, or
   - leave as `ALLOCATED` for timeout cleanup.

### 16.3 `BatchRenewLease` with recovery owner-claim

Rules:
- If lease owner matches client -> renew.
- If owner is `UNKNOWN` and epoch valid -> claim + renew.
- If token/epoch stale -> return retryable stale error.
- If owner differs and active -> reject.

### 16.4 Eviction transaction

For each candidate:
1. CAS `STORED -> EVICTING`.
2. Ask Store to flush block to SSD path.
3. On success: CAS `EVICTING -> EVICTED`, set `evicted_path`.
4. On fail: CAS rollback `EVICTING -> STORED`.

---

## 17. Error Codes and Retry Policy

Recommended error classes:

- `OK`
- `NOT_FOUND`
- `LEASE_EXPIRED`
- `LEASE_OWNER_MISMATCH`
- `STALE_EPOCH`
- `CAS_CONFLICT` (retryable)
- `STORE_WRITE_FAILED` (retryable depending on subtype)
- `INVALID_ARGUMENT`
- `INTERNAL_ERROR`
- `THROTTLED` (retryable with backoff)

Retry guidance:

- retryable with exponential backoff + jitter:
  - `CAS_CONFLICT`, `THROTTLED`, transient store/network errors.
- non-retryable:
  - invalid argument, persistent owner mismatch, checksum corruption.

---

## 18. BRPC Runtime Configuration

DN and Store BRPC service defaults:

1. Connection type: pooled/short according to throughput target, prefer pooled.
2. Request timeout:
   - metadata calls: low timeout (for example 10-30ms)
   - data calls: slightly higher (for example 50-200ms based on block size)
3. Max in-flight per client channel to avoid overload.
4. Compression off for small metadata messages; evaluate for large data payloads.
5. Circuit breaker and health checks for Store endpoints.

---

## 19. Implementation Plan (Detailed)

### Phase 1: Contracts and scaffolding

1. Define BRPC proto files and generate stubs.
2. Implement status/error enums shared by DN/Store/client.
3. Build no-op service skeleton and integration tests.

### Phase 2: DN core

1. Implement metadata table access wrappers with internal C API.
2. Implement bitmap allocator + unit tests.
3. Implement lease manager + epoch fencing tests.
4. Implement CAS status update path.
5. Implement idempotency store.

### Phase 3: Store core

1. Implement DRAM block read/write batch handlers.
2. Implement SSD spill manager and checksum verification.
3. Add per-key result responses and failure injections.

### Phase 4: Client integration (Python)

1. Implement shard router and DN/Store client pools.
2. Implement OffloadingManager batch APIs.
3. Ensure `complete_store` updates metadata only for successful writes.

### Phase 5: Recovery and eviction

1. Implement restart recovery pipeline.
2. Implement `EVICTING` two-phase eviction.
3. Add restart/failover correctness tests.

### Phase 6: Performance hardening

1. Add batching heuristics and chunking.
2. Tune worker counts/timeouts.
3. Add p50/p95/p99 dashboards and regression gates.

---

## 20. Acceptance Criteria

1. No metadata/data inconsistency in partial write failure tests.
2. Recovered leases can be renewed and do not force immediate eviction.
3. Stale epoch clients are safely fenced.
4. p99 latency remains stable under target batch sizes.
5. All BRPC APIs are idempotent for retried mutating requests.

