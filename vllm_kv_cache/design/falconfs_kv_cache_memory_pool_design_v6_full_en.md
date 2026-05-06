# FalconFS KV Cache Memory Pool Design (v6 Full)

## Document Info

| Item | Value |
|---|---|
| Version | v6.2 full design |
| Date | 2026-04-30 |
| Status | Full implementation design |
| Transport | BRPC only |
| Proto files | `kv_common.proto`, `kv_metadata_service.proto`, `kv_data_service.proto` |
| Scope | FalconFS KV cache metadata + memory pool + Store data path + vLLM integration |

This document is self-contained. It does not depend on any previous design document.

### Changelog (v6.1)

1. Service hosting model clarified: DN BRPC runs **inside** the PostgreSQL extension and dispatches jobs to the existing PG connection-pool workers, mirroring `BrpcMetaServiceImpl` → `PGConnectionPool::DispatchMetaServiceJob`.
2. Transaction lifecycle made explicit: catalog access uses `BeginInternalSubTransaction` / `ReleaseCurrentSubTransaction` exactly like `FalconCreateHandle`, and reuses `BATCH_OPERATION_GROUP_SIZE = 8` for the inner sub-batch.
3. Bitmap, lease map, and idempotency store moved to **PostgreSQL shared memory** (LWLock-protected, like `ShardTableShmemInit`) so they are visible to all backends serving BRPC requests.
4. Recovery rules differentiated: DN restart preserves Store DRAM (rollback `EVICTING -> STORED` is valid), while Store restart loses DRAM (DRAM-only blocks must transition to `FAILED`).
5. vLLM integration aligned with the upstream `OffloadingManager` API surface: external methods stay singular (`lookup`, `prepare_load`, `complete_load`, `prepare_store`, `complete_store`, `touch`); batching happens inside the C++ client.
6. Reader race against eviction explicitly closed: hot-path lookups must use `renew_lease_on_hit=true`.
7. Added GUC plan, init sequencing, and a code-mapping appendix tying every component to existing FalconFS files (BRPC server, perf macros, shard table cache, connection pool, etc.).

### Changelog (v6.2)

1. Lease semantics corrected: leases protect DRAM blocks from eviction during access and are **not client-owned**. Removed `owner_client_id` / owner-claim semantics from the design.
2. Concurrent tuple update handling made explicit: `CatalogTupleUpdateWithInfo` may raise a PostgreSQL concurrent-update error; handlers convert it into retryable `CAS_CONFLICT` for the client.
3. Store registration redesigned: each Store partitions its DRAM pool into per-DN continuous regions and registers one region with each DN. DN bitmaps manage only the region assigned to that DN.
4. Epoch semantics clarified: epochs fence stale metadata/location/lease observations after DN or Store restart; they are not ownership markers.
5. Store failure handling expanded: heartbeat-based drain/offline/quarantine states temporarily remove slow or failed Stores from allocation while recovery/restart proceeds.

---

## 1. Goals, Non-Goals, and Core Decisions

### 1.1 Goals

1. Provide a FalconFS-backed external KV cache for vLLM prefix/offloading workflows.
2. Keep common cache-hit load path to two network RTTs:
   - Client -> Metadata DN for metadata and lease.
   - Client -> Store for block bytes.
3. Keep allocation fast by managing bitmap state inside the Metadata DN.
4. Support batch-first APIs for lookup, allocation, lease renewal, status update, read, write, and SSD readback.
5. Preserve correctness under partial write failure, retry, timeout, DN restart, Store restart, and failover.
6. Align implementation with existing FalconFS metadata patterns:
   - shard grouping,
   - relation/index opened once per group,
   - bounded batch group size,
   - shared-cache invalidation/reload,
   - fine-grained latency instrumentation.

### 1.2 Non-Goals

1. The KV cache path does not require cross-DN distributed transactions.
2. CN is not in the hot path for normal KV cache lookup/load/store.
3. Strong global ordering across unrelated KV blocks is not required.
4. Persistent lease storage is optional; the default design reconstructs leases on recovery.

### 1.3 Core Decisions

1. **BRPC only** for both metadata and data service RPCs.
2. **Client direct-to-DN routing** based on `block_hash`.
3. **Metadata DN owns allocation decisions** using local bitmap state.
4. **Store owns bytes only** and does not decide allocation.
5. **Every batch response is per-item**, never implicitly all-or-nothing.
6. **Mutating APIs are idempotent** by `(api_name, request_id)`.
7. **Status updates use CAS** by `expected_version`.
8. **Eviction is two-phase**: `STORED -> EVICTING -> EVICTED`.
9. **Lease tokens include epoch semantics** to fence stale metadata/location observations after DN or Store restart/failover.

---

## 2. System Architecture

### 2.1 Logical Components

```
vLLM Scheduler / Block Manager
        |
        v
FalconFS KV OffloadingManager (Python)
        |
        | BRPC metadata calls
        v
Metadata DN (PostgreSQL extension process)
  - KV block metadata table
  - Bitmap allocator
  - Lease manager
  - LRU manager
  - Idempotency store
  - Eviction coordinator
        |
        | BRPC data calls
        v
Falcon Store
  - DRAM block pool
  - SSD spill files
  - Batch read/write service
```

### 2.2 Client Responsibilities

The Python OffloadingManager:

1. receives vLLM block hashes,
2. groups requests by target DN,
3. calls DN BRPC metadata APIs,
4. groups returned locations by Store,
5. calls Store BRPC data APIs,
6. updates metadata status only for successful writes,
7. maintains a local lease-aware cache.

### 2.3 Metadata DN Responsibilities

The DN is the metadata authority for its shard range:

1. stores durable KV block metadata in PostgreSQL,
2. allocates DRAM offsets through local bitmap,
3. grants and renews leases,
4. tracks LRU order for eviction,
5. performs CAS status transitions,
6. reconstructs in-memory state after restart/failover.

### 2.4 Store Responsibilities

The Store:

1. owns the DRAM memory region,
2. reads/writes block payloads at DN-assigned offsets,
3. spills DRAM blocks to SSD on eviction,
4. validates size/checksum,
5. returns per-item success/failure.

### 2.5 Normal Hot Path

Cache hit load:

```
Client -> DN:    BatchLookupWithLease(keys)
DN -> Client:    status/location/lease/version
Client -> Store: BatchReadBlock(offsets) or BatchReadFromSSD(paths)
Store -> Client: block payloads
```

Store path:

```
Client -> DN:    BatchAllocateWithLease(keys)
DN -> Client:    store_id/pool_offset/lease/version
Client -> Store: BatchWriteBlock(payloads)
Store -> Client: per-key write result
Client -> DN:    BatchUpdateBlockStatus(success_keys, ALLOCATED -> STORED)
```

---

## 3. Persistent Metadata Model

### 3.0 Distribution Model

`falcon_kvblock_table` is created on **every DN** (not on CN). Routing is computed client-side from `block_hash`, identical to how `falcon/metadb/shard_table.c` (`SearchShardInfoByShardValue` / `SearchShardInfoByHashValue`) routes inode operations:

1. CN owns DDL and shard-table maintenance. CN runs the equivalent of `falcon_build_shard_table` and propagates DDL to every worker.
2. Each DN owns its slice of `falcon_kvblock_table` rows (rows whose `block_hash` hashes into one of that DN's shards).
3. Hot-path KV cache requests never go through CN. They go from the Python client directly to the target DN, the same way `falcon_client/src/router.cpp` routes file-system requests through `Router::GetWorkerConnByPath`.

### 3.1 KV Block Table

Table name: `falcon_kvblock_table`

Recommended schema:

```sql
CREATE TABLE falcon_kvblock_table (
    block_hash      BYTEA PRIMARY KEY,
    kv_group_idx    INT NOT NULL,
    layer_mask      INT NOT NULL,
    status          SMALLINT NOT NULL,
    store_node_id   INT NOT NULL,
    pool_offset     BIGINT NOT NULL,
    evicted_path    TEXT,
    version         BIGINT NOT NULL,
    updated_at_ms   BIGINT NOT NULL
);

CREATE INDEX falcon_kvblock_status_time_idx
    ON falcon_kvblock_table(status, updated_at_ms);

CREATE INDEX falcon_kvblock_store_offset_idx
    ON falcon_kvblock_table(store_node_id, pool_offset);
```

### 3.2 Status Values

The database status values should match the proto enum values excluding `UNSPECIFIED`:

| DB value | Proto enum | Meaning |
|---:|---|---|
| 1 | `BLOCK_STATUS_ALLOCATED` | metadata and DRAM slot allocated, bytes not committed |
| 2 | `BLOCK_STATUS_STORED` | bytes exist in Store DRAM |
| 3 | `BLOCK_STATUS_EVICTING` | eviction in progress |
| 4 | `BLOCK_STATUS_EVICTED` | bytes moved to SSD path |
| 5 | `BLOCK_STATUS_FAILED` | unrecoverable or quarantined block |

`BLOCK_STATUS_UNSPECIFIED = 0` must never be persisted.

### 3.3 Required Invariants

1. `STORED` requires readable DRAM bytes at `(store_node_id, pool_offset)`.
2. `EVICTED` requires non-empty, sanitized `evicted_path`.
3. `ALLOCATED`, `STORED`, and `EVICTING` occupy bitmap slots.
4. `EVICTED` does not occupy DRAM bitmap slots after eviction finalization.
5. `version` increments on every successful metadata state mutation.
6. All updates that change status use `(block_hash, expected_version)` CAS.

---

## 4. PostgreSQL Internal API Design

### 4.1 Why Internal API

The Metadata DN is already a PostgreSQL process/extension. Using PostgreSQL internal APIs avoids SQL parse/plan overhead on the hot path.

Preferred pattern (matches `falcon/metadb/meta_handle.c`):

1. group items by local shard,
2. open relation/index once,
3. scan/insert/update multiple items inside a **sub-transaction** (`BeginInternalSubTransaction`),
4. commit (`ReleaseCurrentSubTransaction`) or roll back (`RollbackAndReleaseCurrentSubTransaction`) per sub-batch,
5. close relation/index once.

### 4.1.1 Service Hosting Model (BRPC inside the PG extension)

The DN BRPC service is hosted **inside the PostgreSQL extension process**, the same way the existing meta service is hosted:

1. `falcon/brpc_comm_adapter/falcon_brpc_server.cpp` (`StartFalconCommunicationServer`, `FalconBrpcServer::Run`) runs the BRPC server in a background thread.
2. `falcon/brpc_comm_adapter/brpc_meta_service_imp.cpp` (`BrpcMetaServiceImpl::MetaCall`) creates a job and calls `m_jobDispatchFunc(job)`.
3. The dispatcher hands the job to a PG connection-pool worker (`falcon/connection_pool/pg_connection_pool.cpp`, `PGConnectionPool::DispatchMetaServiceJob`).
4. Inside the worker, the job opens a transaction context (`StartTransactionCommand` / `BeginInternalSubTransaction`) and calls into catalog C APIs.

The KV cache service follows the same flow:

```
brpc::Server (in PG extension thread)
   |
   v
KVMetadataServiceImpl::BatchLookupWithLease  (etc.)
   |
   v
KVCacheJob (analogous to BrpcMetaServiceJob)
   |
   v
KVConnectionPool::DispatchKVServiceJob  (PG worker pool)
   |
   v
KVMetadataEngine in worker context:
   - StartTransactionCommand (or use ambient transaction)
   - BeginInternalSubTransaction per sub-batch
   - table_open / index_open
   - per-item processing
   - ReleaseCurrentSubTransaction or rollback
   - close index/table
   - StopTransactionCommand
```

This avoids running PostgreSQL catalog APIs from a non-PG thread, which is unsupported.

### 4.1.2 Transaction Lifecycle for Mutating Calls

Mutating handlers must follow the FalconFS sub-transaction pattern (see `FalconCreateHandle` in `falcon/metadb/meta_handle.c`, sub-batch loop around `BeginInternalSubTransaction` ... `ReleaseCurrentSubTransaction` / `RollbackAndReleaseCurrentSubTransaction`):

```cpp
for each shard_group:
    open relation/index once
    for each chunk of BATCH_OPERATION_GROUP_SIZE items in shard_group:
        BeginInternalSubTransaction(NULL);
        PG_TRY();
        {
            for each item in chunk:
                process item;  // may set per-item errorCode, no longjmp escape
            ReleaseCurrentSubTransaction();
        }
        PG_CATCH();
        {
            FlushErrorState();
            RollbackAndReleaseCurrentSubTransaction();
            // mark items in this chunk as retryable CAS_CONFLICT/INTERNAL_ERROR
        }
        PG_END_TRY();
    close index/table
```

Notes:

1. `BATCH_OPERATION_GROUP_SIZE = 8` is the existing FalconFS constant. We keep that for the **inner** sub-batch to limit rollback blast radius. The **outer** request batch (e.g. 64 keys) is decomposed into 8-sized sub-batches.
2. Read-only handlers (`BatchLookupWithLease`) can use a single sub-transaction over the whole shard group, since rollback is rare.
3. Per-item errors must not throw: convert PostgreSQL errors into per-item `ItemResultMeta` so the rest of the chunk continues, exactly like `MetaProcessInfo::errorCode` + `CHECK_ERROR_CODE_WITH_CONTINUE` in `meta_handle.c`.

### 4.2 Table Accessor Module

Module: `KVMetaTableAccessor`

Responsibilities:

1. `BatchLookupByHash`
2. `BatchInsertAllocated`
3. `BatchCASStatusUpdate`
4. `BatchDeleteOrMarkFailed`
5. `ScanOccupiedForRecovery`
6. `ScanStoredForLRURecovery`
7. `ScanEvictingForReconcile`

### 4.3 Lookup Pseudocode

This pseudocode runs inside a PG worker that already has an active transaction (or starts one). It mirrors `SearchAndUpdateInodeTableInfo`-style scans in `falcon/metadb/meta_handle_helper.c`.

```cpp
bool LookupKVBlockMeta(const bytea* block_hash, KVBlockMeta* out) {
    // Caller must already be inside a transaction:
    //   StartTransactionCommand() in worker entry, and ideally a sub-tx for the chunk.

    Relation rel = table_open(KVBlockRelationId(), AccessShareLock);
    Relation idx = index_open(KVBlockHashIndexId(), AccessShareLock);

    ScanKeyData key[1];
    ScanKeyInit(&key[0],
                Anum_falcon_kvblock_table_block_hash,
                BTEqualStrategyNumber,
                F_BYTEAEQ,
                PointerGetDatum(block_hash));

    // Use the active snapshot for read-stability.
    SysScanDesc scan = systable_beginscan(
        rel, KVBlockHashIndexId(), true /*indexOK*/, GetActiveSnapshot(), 1, key);

    HeapTuple tuple = systable_getnext(scan);
    bool found = HeapTupleIsValid(tuple);
    if (found) {
        // heap_deform_tuple into Datum[] and extract:
        //   kv_group_idx, layer_mask, status, store_node_id,
        //   pool_offset, evicted_path, version, updated_at_ms.
    }

    systable_endscan(scan);
    index_close(idx, AccessShareLock);
    table_close(rel, AccessShareLock);
    return found;
}
```

Important correctness notes:

- `block_hash` is `BYTEA`. Scan operator must match (`F_BYTEAEQ`). Never pass a text datum for a bytea column. This is the analog of `cstring_to_text_with_len` mistakes seen in earlier drafts.
- Use `GetActiveSnapshot()` inside an open transaction. `GetTransactionSnapshot()` is acceptable when the worker explicitly manages snapshots, but `GetActiveSnapshot()` is the safer default once `StartTransactionCommand` has run.
- For destructive update paths, fetch the tuple inside the same sub-transaction as the update, and update it via `heap_modify_tuple` + `CatalogTupleUpdateWithInfo` (same pattern as `falcon_update_shard_table` in `shard_table.c`).

### 4.4 CAS Status Update

The CAS update is implemented as fetch + verify + `heap_modify_tuple` + `CatalogTupleUpdateWithInfo`, all inside the same sub-transaction.

Pseudocode:

```cpp
// Inside BeginInternalSubTransaction() ... ReleaseCurrentSubTransaction()

Relation rel = table_open(KVBlockRelationId(), RowExclusiveLock);
CatalogIndexState istate = CatalogOpenIndexes(rel);
TupleDesc tupdesc = RelationGetDescr(rel);

ScanKeyData k[1];
ScanKeyInit(&k[0],
            Anum_falcon_kvblock_table_block_hash,
            BTEqualStrategyNumber,
            F_BYTEAEQ,
            PointerGetDatum(block_hash));

SysScanDesc scan = systable_beginscan(rel, KVBlockHashIndexId(),
                                      true, GetActiveSnapshot(), 1, k);
HeapTuple cur = systable_getnext(scan);
bool ok = false;
if (HeapTupleIsValid(cur)) {
    int16 cur_status = DatumGetInt16(...);
    int64 cur_version = DatumGetInt64(...);

    if (cur_status == expected_from_status && cur_version == expected_version) {
        Datum   values[Natts];
        bool    nulls[Natts]   = {false};
        bool    repl[Natts]    = {false};

        values[Anum_status - 1]        = Int16GetDatum(to_status);
        values[Anum_version - 1]       = Int64GetDatum(cur_version + 1);
        values[Anum_updated_at_ms - 1] = Int64GetDatum(now_ms);
        if (evicted_path != NULL) {
            values[Anum_evicted_path - 1] = CStringGetTextDatum(evicted_path);
        }
        repl[Anum_status - 1]        = true;
        repl[Anum_version - 1]       = true;
        repl[Anum_updated_at_ms - 1] = true;
        if (evicted_path != NULL) repl[Anum_evicted_path - 1] = true;

        HeapTuple new_tuple = heap_modify_tuple(cur, tupdesc, values, nulls, repl);
        PG_TRY();
        {
            CatalogTupleUpdateWithInfo(rel, &new_tuple->t_self, new_tuple, istate);
            ok = true;
        }
        PG_CATCH();
        {
            // PostgreSQL may raise "tuple concurrently updated" or a related
            // serialization/update conflict. Convert it to per-item CAS_CONFLICT.
            FlushErrorState();
            ok = false;
            item_result.error_code = CAS_CONFLICT;
            item_result.retryable = true;
        }
        PG_END_TRY();
    }
    // else: CAS conflict, return current status/version to caller for diagnostics.
}
systable_endscan(scan);
CatalogCloseIndexes(istate);
table_close(rel, RowExclusiveLock);
```

Concurrency rules:

1. Two backends racing on the same row may either observe a version mismatch before update or hit PostgreSQL's `"tuple concurrently updated"` error during `CatalogTupleUpdateWithInfo`.
2. Both cases are surfaced to clients as `CAS_CONFLICT` with `retryable=true`. The client must re-run `BatchLookupWithLease` or retry the state transition with the newer version.
3. We do **not** rely on `SnapshotDirty` here, because the sub-transaction provides a consistent read; if a racing update lands first, `expected_version` mismatch or the concurrent-update error detects it.
4. `RowExclusiveLock` on the table is the same lock level used by `CatalogTupleUpdateWithInfo` callers in FalconFS today.

### 4.5 Batch Grouping

Batch metadata handlers follow the same structure as `FalconCreateHandle` in `meta_handle.c`:

```cpp
Group items by shard_id (HTAB keyed by shardId, like `batchMetaProcessInfoListPerShard`).
For each shard group:
    open relation/index once.
    For each chunk of size BATCH_OPERATION_GROUP_SIZE in the group:
        BeginInternalSubTransaction();
        PG_TRY:
            process items in the chunk.
            ReleaseCurrentSubTransaction();
        PG_CATCH:
            FlushErrorState();
            RollbackAndReleaseCurrentSubTransaction();
            mark items in this chunk as retryable failure.
    close relation/index.
```

Sizes:

| Knob | Value | Reason |
|---|---|---|
| External batch size (per RPC) | up to 64–128 | Bounds RPC payload and worker latency. |
| `BATCH_OPERATION_GROUP_SIZE` | `8` | Reuses existing FalconFS constant (`falcon/metadb/meta_handle.c:34`). Keeps sub-transaction rollback small. |
| Outer shard group size | unbounded inside one RPC | Naturally bounded by the external batch size. |

Reason:

- reduces table/index open-close overhead,
- bounds sub-transaction rollback blast radius to 8 items,
- matches the existing FalconFS code so reviewers see one pattern.

### 4.6 Schema Distribution and DDL

DDL strategy:

1. CN registers the KV cache extension metadata once (system catalog rows describing `KVBlockRelationId`).
2. CN runs `CREATE TABLE falcon_kvblock_table ...` on each worker (DN), the same way inode shard tables are created.
3. CN inserts the new shard column into the existing `falcon_shard_table` (or a parallel `falcon_kvblock_shard_table` if a different shard layout is needed) so that `SearchShardInfoByShardValue` resolves to the right DN.
4. The OID of the new relation is cached per backend via `CachedRelationOid[]` (see `falcon/utils/utils.c:GetRelationOid`), exactly like `ShardRelationId`.

---

## 5. Store Registration and Affinity Allocation

### 5.1 Store Registry

The DN needs a local view of available Store regions. A Store does **not** register its entire DRAM pool to every DN. Instead, each Store partitions its DRAM pool into continuous regions and registers one region with each DN.

```cpp
struct StoreInfo {
    int32_t store_node_id;
    std::string hostname;
    std::string brpc_address;
    uint64_t block_size;
    int64_t store_epoch;
    bool healthy;
    bool accepting_allocations;
    int64_t last_heartbeat_ms;
};

struct StoreRegionInfo {
    int32_t store_node_id;
    int32_t owner_dn_id;
    uint64_t base_offset;       // continuous region start in Store DRAM pool
    uint64_t region_bytes;      // continuous region size
    uint64_t block_size;
    uint64_t total_blocks;
    int64_t store_epoch;
    bool healthy;
    int64_t last_heartbeat_ms;
};
```

### 5.2 Registration Flow

Store startup:

1. Store allocates one large DRAM pool: `[0, dram_pool_bytes)`.
2. Store fetches the current DN/shard membership from the FalconFS shard table (or from a KV Store registry table maintained by CN).
3. Store partitions its DRAM pool into continuous per-DN regions. Example:

   ```
   Store DRAM pool:
     [0, 256 GiB)

   DN0 region: [0,        64 GiB)
   DN1 region: [64 GiB,  128 GiB)
   DN2 region: [128 GiB, 192 GiB)
   DN3 region: [192 GiB, 256 GiB)
   ```

4. Store sends `RegisterStoreRegion(store_node_id, store_epoch, base_offset, region_bytes, block_size, brpc_address)` to each owner DN.
5. Each DN creates/updates one `StoreRegionInfo` and initializes a bitmap only for the region assigned to that DN.
6. DN allocations return absolute `pool_offset = base_offset + block_idx * block_size`.
7. Store read/write validates that incoming offsets belong to a registered region for the requesting DN.

Region ownership invariant:

- A `(store_node_id, pool_offset)` belongs to exactly one DN at a time.
- DN bitmaps never overlap for the same Store.
- If DN membership changes, CN first marks affected regions `DRAINING`, waits for migration/expiry, then installs the new region map.

### 5.2.1 Store State Machine

Store region state per DN:

| State | Meaning | Allocation | Reads |
|---|---|---|---|
| `HEALTHY` | heartbeat fresh, region usable | allowed | allowed |
| `DRAINING` | being removed/rebalanced | disallowed | allowed |
| `SUSPECT` | missed heartbeat threshold | disallowed | best-effort |
| `OFFLINE` | store failed or unreachable | disallowed | fail fast |
| `QUARANTINED` | restarted, reconciliation in progress | disallowed | disallowed until scan finishes |

Heartbeat policy:

1. Store sends heartbeat to every DN that owns one of its regions.
2. DN marks region `SUSPECT` after `falcon_kv.store_suspect_ms` without heartbeat.
3. DN marks region `OFFLINE` after `falcon_kv.store_offline_ms`.
4. `SUSPECT`, `OFFLINE`, and `QUARANTINED` regions are removed from allocation immediately.
5. Existing blocks on `OFFLINE` regions are not marked failed immediately. A separate reconciliation job decides whether to wait, retry, or fail them based on operator policy.

### 5.3 Affinity Allocation

Allocation priority:

1. preferred Store region if `HEALTHY`, owned by this DN, and has space,
2. same-host Store region as client hostname,
3. least-used healthy Store region owned by this DN,
4. fallback allowed only if `allow_fallback_store=true`.

If no Store can allocate:

- return `THROTTLED` if transient pressure or all suitable regions are temporarily `SUSPECT`/`DRAINING`,
- return non-retryable storage exhaustion code if cluster capacity exhausted.

---

## 6. Bitmap Allocator

### 6.1 Storage Location

The bitmap **must live in PostgreSQL shared memory**, not in per-backend heap.

Rationale:

1. Multiple PG backends serve BRPC jobs concurrently (the existing connection-pool model).
2. Each backend may execute `BatchAllocateWithLease` for any block. They must agree on which offsets are free.
3. The shmem layout follows the same pattern used by `falcon/metadb/shard_table.c:ShardTableShmemInit` and `ShardTableShmemControl`.

Layout per Store region:

```cpp
struct KVBitmapHeader {
    int32_t  store_node_id;
    int32_t  owner_dn_id;
    uint64_t base_offset;              // start of this DN's continuous Store region
    uint64_t region_bytes;
    uint64_t block_size;
    uint64_t total_blocks;
    pg_atomic_uint64 free_blocks;     // atomic counter
    pg_atomic_uint64 next_hint;       // soft hint, atomic
    LWLock   lock;                    // shared tranche, like Shard Table
    uint64_t bitmap_words[/*flex*/];  // sized at shmem init
};
```

A top-level shmem segment maps `(store_node_id, owner_dn_id) -> KVBitmapHeader*`. The mapping table itself is an LWLock-protected array sized at `_PG_init` based on GUCs (e.g. `falcon_kv.max_stores`, `falcon_kv.max_dns`).

Initialization hook:

1. Implement `KVBitmapShmemSize()` and `KVBitmapShmemInit()`.
2. Call them from `FalconShmemInit` (see `falcon/falcon_init.c`) right after `ShardTableShmemInit()`.
3. Register a tranche name `"Falcon KV Bitmap"` via `LWLockNewTrancheId` / `LWLockRegisterTranche`, identical to `ShardTableShmemControl->lockTrancheName`.

### 6.2 Allocate

Algorithm:

1. choose Store region by affinity policy (see §5.3),
2. `LWLockAcquire(&hdr->lock, LW_EXCLUSIVE)` on selected Store-region bitmap,
3. scan from `next_hint` (with wraparound) for first zero bit,
4. set the bit, update `next_hint` to next index,
5. `pg_atomic_fetch_sub_u64(&hdr->free_blocks, 1)`,
6. release lock,
7. return absolute `pool_offset = hdr->base_offset + block_idx * block_size`.

Optional optimization (deferred): two-level bitmap (group-of-words summary bits) for very large pools.

### 6.3 Free

1. validate `base_offset <= offset < base_offset + region_bytes`,
2. validate offset alignment and compute `relative_offset = offset - base_offset`,
3. compute `block_idx = relative_offset / block_size`,
4. compute `word_idx = block_idx / 64` and `bit_idx = block_idx % 64`,
5. `LWLockAcquire(&hdr->lock, LW_EXCLUSIVE)`,
6. clear bit if set; if it was already zero, log a corruption warning (do not double-free),
7. `pg_atomic_fetch_add_u64(&hdr->free_blocks, 1)`,
8. release lock.

### 6.4 Recovery

On DN startup or failover:

1. Store region registrations rebuild `(store_node_id, owner_dn_id) -> KVBitmapHeader` entries.
2. `KVBitmapShmemInit` zero-fills every region bitmap and sets `free_blocks = total_blocks`.
3. Scan metadata via `ScanOccupiedForRecovery` (see §15) and for every row in `ALLOCATED`, `STORED`, `EVICTING`:
   - mark bit; on duplicate occupied offset, mark the later row as `FAILED` (corruption quarantine, like inode "wrong shard" handling).
4. Decrement `free_blocks` accordingly.
5. Recovery must complete **before** the BRPC handlers start accepting requests. The existing FalconFS init order makes this easy: register the shmem in `FalconShmemInit`, run recovery before `RegisterBackgroundWorker(... brpc server worker)`.

---

## 7. Lease Manager

Lease purpose:

The lease is a short-lived **anti-eviction guard**. It means: "this DRAM block may be accessed soon or is currently being accessed, so the eviction worker must not move/free it until the lease expires."

A lease is **not an ownership lock** and does not contain `owner_client_id`. Multiple clients may hold or renew protection for the same block. This is intentional because KV cache blocks are immutable after `ALLOCATED -> STORED`; there is no writer ownership to protect. The only safety property required from a lease is eviction exclusion.

### 7.0 Storage Location

The lease map also lives in shared memory, for the same reason as the bitmap: multiple PG backends serve different requests for the same block. A simple implementation uses a `dshash` (PostgreSQL dynamic shared hash) keyed by `block_hash`.

Alternative if `dshash` is undesirable in this code base: a fixed-size shmem hash table sized at init time (`hash_create` with `HASH_SHARED_MEM`, like FalconFS already does for foreign-server cache and connection-pool shmem in `falcon/falcon_init.c:FalconShmemInit`).

A single LWLock tranche (`"Falcon KV Lease"`) protects the table. Per-bucket LWLocks may be added later if contention shows up in profiling.

### 7.1 Lease Entry

```cpp
struct LeaseEntry {
    bytea_hash20 block_hash;          // fixed-size key (e.g., 20 or 32 bytes)
    int64_t      lease_token;
    int64_t      expire_ms;
    int64_t      dn_epoch;
    int64_t      store_epoch;         // epoch of the Store region containing the DRAM block
};
```

### 7.2 Grant

Grant occurs on:

- allocate,
- lookup/prepare-load hit when `renew_lease_on_hit=true`,
- explicit renew.

Grant semantics:

1. If no lease exists, create a new lease token and expiry.
2. If a valid lease exists, extend `expire_ms` and rotate `lease_token` only if configured. The default is to keep the token stable until expiry to reduce client churn.
3. If an expired lease exists, replace it with a new token and expiry.

### 7.3 Renew

Renew succeeds if:

1. the block still exists in `STORED` state,
2. `dn_epoch` matches the current DN epoch,
3. `store_epoch` matches the current Store-region epoch,
4. `lease_token` matches the current lease token, or the request is a fresh lookup/prepare-load that asks the DN to renew on hit.

Renew fails if:

- stale DN epoch,
- stale Store epoch,
- lease token mismatch,
- the block is no longer in DRAM (`EVICTING`, `EVICTED`, `FAILED`, or missing).

No owner mismatch is possible because leases do not have owners.

### 7.4 Eviction Protection

Eviction can proceed only if:

```
lease missing OR lease.expire_ms <= now
```

Active lease blocks are skipped.

### 7.5 Epoch Semantics

Epochs fence stale observations. They are needed because clients cache metadata locations and lease tokens.

There are two independent epochs:

1. `dn_epoch`: increments when the Metadata DN restarts/fails over and rebuilds in-memory state (bitmap, lease map, LRU). It prevents an old lease token from a previous DN incarnation from protecting a block after recovery.
2. `store_epoch`: increments when a Store restarts and loses/recreates its DRAM pool or region map. It prevents a client from reading an old `(store_node_id, pool_offset)` after that offset may now contain different data.

Epoch check rules:

| Request | Required epoch check | Failure |
|---|---|---|
| `BatchLookupWithLease` | client epoch hint optional; server returns current epochs | never fails solely because hint is absent |
| `BatchRenewLease` | `dn_epoch` and `store_epoch` must match lease entry/current region | `STALE_EPOCH`, retryable after lookup |
| `BatchReadBlock` | request carries expected Store epoch/version if available | `STALE_EPOCH` or `CAS_CONFLICT`, retryable after lookup |
| `BatchUpdateBlockStatus` | CAS `version`; DN epoch checked for request freshness | `CAS_CONFLICT` or `STALE_EPOCH` |

Epoch is not used for ownership. It is a stale-cache fencing mechanism.

---

## 8. LRU Manager

### 8.1 Scope

LRU tracks only `STORED` blocks.  
`ALLOCATED`, `EVICTING`, `EVICTED`, and `FAILED` are not normal LRU members.

### 8.2 Operations

1. `AddStored(block_hash)`
2. `Touch(block_hash)`
3. `Remove(block_hash)`
4. `ColdCandidates(limit)`

### 8.3 Recovery

If exact recency is not persisted, recovery inserts all `STORED` keys into cold side.  
Future accesses rebuild useful recency over time.

Optional future improvement:

- persist coarse `last_access_ms` and sort during LRU recovery.

---

## 9. Idempotency Store

### 9.1 Requirement

Mutating RPCs must be safe under retry after timeout.

Key:

```
(api_name, request_id, client_id)
```

Value:

```
serialized response (or response digest) + expire_ms
```

### 9.2 Storage and TTL

1. The store lives in PostgreSQL shared memory as a fixed-size hash table (`hash_create(... HASH_SHARED_MEM)`), protected by an LWLock tranche `"Falcon KV Idempotency"`.
2. Default TTL is 60–120 seconds. A bgworker scans expired entries periodically.
3. After DN restart, the idempotency table is empty. This is acceptable because all mutating handlers are CAS-guarded by `expected_version`; a duplicate replay-as-new will be detected and either succeed exactly once or return `CAS_CONFLICT`.
4. `request_id` is also forwarded into `brpc::Controller::log_id()` so that BRPC server-side logs and our application logs share a correlation id.

### 9.3 APIs Covered

1. `BatchAllocateWithLease`
2. `BatchUpdateBlockStatus`
3. `BatchFreeAllocated`
4. `BatchWriteBlock` (Store side)

Reads and renews may use request IDs for tracing but do not require replay storage.

---

## 10. BRPC Proto Contract

### 10.1 Proto Files

- `kv_common.proto`: common enums and metadata.
- `kv_metadata_service.proto`: DN service.
- `kv_data_service.proto`: Store service.

### 10.2 Common Request Metadata

`CommonRequestMeta` carries:

- `request_id`
- `client_id`
- `client_hostname`
- `trace_id`
- `request_start_ms`
- `client_epoch_hint`
- `timeout_ms`

### 10.3 Error Model

`ItemResultMeta` carries:

- `success`
- `error_code`
- `retryable`
- `error_message`

Every result item must include `ItemResultMeta`.

### 10.4 Error Codes

| Error | Retry | Meaning |
|---|---|---|
| `OK` | no | success |
| `NOT_FOUND` | no/depends | metadata or data missing |
| `INVALID_ARGUMENT` | no | malformed request |
| `CAS_CONFLICT` | yes | version/status mismatch |
| `LEASE_EXPIRED` | yes after lookup | lease no longer active |
| `LEASE_TOKEN_MISMATCH` | yes after lookup | client carried an old lease token; refresh metadata and retry |
| `STALE_EPOCH` | yes after route/epoch refresh | client token from old epoch |
| `STORE_WRITE_FAILED` | depends | transient or permanent Store write failure |
| `THROTTLED` | yes | overload/backpressure |
| `CHECKSUM_MISMATCH` | no | data corruption or wrong payload |
| `INTERNAL_ERROR` | depends | server-side unexpected failure |

---

## 11. Metadata DN BRPC Service

### 11.1 `BatchLookupWithLease`

Request:

- `CommonRequestMeta`
- repeated `LookupItem`
- `allow_partial_result`

Per item:

1. validate block hash,
2. lookup metadata,
3. if not found -> `NOT_FOUND`,
4. if status is `STORED` or `EVICTED`, return location,
5. if `renew_lease_on_hit`, grant/renew lease,
6. return `cacheable=true` only for successful usable entries.

### 11.2 `BatchAllocateWithLease`

Per item:

1. validate block hash and size,
2. if duplicate inside request and dedup enabled, reuse first result,
3. if row already exists:
   - if compatible with idempotent replay, return existing result,
   - otherwise return conflict/error,
4. allocate Store bitmap slot,
5. insert metadata row as `ALLOCATED`,
6. grant lease,
7. return location, lease, version.

Failure after bitmap allocation but before metadata insert:

- free bitmap before returning error.

### 11.3 `BatchRenewLease`

Per item:

1. find lease,
2. reject stale DN epoch or stale Store epoch,
3. verify lease token if the request is a token-based renew,
4. renew TTL,
5. return new lease info.

No owner validation is performed. Lease renewal extends eviction protection, not client ownership.

### 11.4 `BatchUpdateBlockStatus`

Per item:

1. lookup current row,
2. validate `expected_version`,
3. validate `expected_from_status`,
4. apply transition if legal,
5. update bitmap/LRU side effects:
   - `ALLOCATED -> STORED`: add LRU,
   - `STORED -> EVICTING`: remove or mark pending,
   - `EVICTING -> EVICTED`: free bitmap, remove LRU,
   - `EVICTING -> STORED`: restore LRU.

### 11.5 `BatchFreeAllocated`

Used to clean failed store allocations.

Allowed by default only for:

- `ALLOCATED`,
- `FAILED`,
- forced cleanup mode with strict operator/admin control.

Side effects:

- free bitmap,
- remove lease,
- delete row or mark failed depending retention policy.

---

## 12. Store BRPC Service

### 12.0 DRAM Memory Region

1. The KV block DRAM region is a separate aligned memory region, **not** the BRPC iobuf pool used by `RemoteIOServer::GetMemoryPool` (`std::pmr::synchronized_pool_resource` in `falcon_store/src/include/connection/brpc_server.h`).
2. Allocation: `mmap(addr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS [| MAP_HUGETLB], -1, 0)` with hugepages when available; falls back to 4 KiB pages.
3. Alignment: at least 4 KiB. The existing Store path already uses `constexpr size_t ALIGNMENT = 512` (`falcon_store/src/brpc/brpc_main.cpp`) for direct I/O; KV block alignment is independent and should be 4 KiB or larger to match GPU host buffer alignment.
4. Block size is configurable per Store (e.g. 16 KiB, 64 KiB, 128 KiB). The DN bitmap header records this so allocation math matches.
5. Compression: client compresses payloads if configured (proto field `WriteItem.compression`). Store stores opaque payload + `crc32` and does not decompress on read. This keeps Store CPU off the hot path.

### 12.1 `BatchWriteBlock`

Per item:

1. validate payload size <= block size,
2. validate offset alignment and range,
3. validate checksum if enabled,
4. copy payload into DRAM memory region,
5. return bytes written and checksum.

Important:

- Store does not update metadata.
- Store does not decide status.
- Store returns per-item result only.

### 12.2 `BatchReadBlock`

Per item:

1. validate offset and size,
2. copy bytes from DRAM,
3. compute checksum,
4. return payload.

### 12.3 `BatchReadFromSSD`

Per item:

1. validate path prefix,
2. read file,
3. verify size/checksum if metadata available,
4. return payload.

### 12.4 SSD Spill Support

Eviction coordinator may use an internal Store API or BRPC API:

```
SpillBlockToSSD(pool_offset, block_hash, version) -> evicted_path
```

Path format:

```
<ssd_root>/<store_node_id>/<hash_prefix>/<block_hash>.<version>.kv
```

`<ssd_root>` is restricted by configuration; the Store rejects any path outside `<ssd_root>` to prevent traversal.

### 12.5 Backpressure and Admission Control

1. The Store maintains a bounded inflight-request queue per BRPC service. When the queue is full, new requests are rejected with `THROTTLED` (retryable).
2. Per-Store concurrency knob: `falcon_kv.store_max_inflight` (default e.g. 256).
3. Per-DN connections to Store reuse `brpc::ChannelOptions { connection_type = "pooled", connect_timeout_ms = 5000, timeout_ms = 10000 }`, identical to `falcon_store/src/connection/node.cpp:CreateIOConnection`.

---

## 13. End-to-End Flows

### 13.1 Lookup + Load Flow

```
vLLM -> OffloadingManager.batch_lookup(keys)
OffloadingManager groups keys by DN
Client -> DN.BatchLookupWithLease
DN returns locations
OffloadingManager groups hits by Store
Client -> Store.BatchReadBlock / BatchReadFromSSD
OffloadingManager returns LoadStoreSpec to vLLM
OffloadingManager.BatchRenewLease after successful use
```

### 13.2 Store Flow

```
vLLM provides new KV block bytes
OffloadingManager.batch_prepare_store(keys)
Client -> DN.BatchAllocateWithLease
DN allocates bitmap and returns locations
Client -> Store.BatchWriteBlock
Store returns per-item results
Client -> DN.BatchUpdateBlockStatus(success_keys)
Failed keys -> BatchFreeAllocated or TTL cleanup
```

### 13.3 Touch Flow

```
vLLM marks cached blocks as recently used
Client -> DN.BatchRenewLease
DN renews lease and LRU touch
```

---

## 14. Eviction Design

### 14.1 Trigger

Eviction starts when:

- free blocks below threshold,
- background periodic scan,
- operator-triggered pressure mode.

### 14.2 Candidate Selection

1. read cold end of LRU,
2. skip blocks with active lease,
3. skip non-`STORED` states,
4. limit per cycle to bounded count.

### 14.3 Two-Phase Eviction

Per candidate:

1. CAS `STORED -> EVICTING`,
2. Store copies DRAM block to SSD and fsyncs,
3. if success:
   - CAS `EVICTING -> EVICTED`,
   - set `evicted_path`,
   - free bitmap,
4. if failure:
   - CAS `EVICTING -> STORED`,
   - restore LRU.

### 14.4 Reader Behavior During Eviction

Two protections together close the read-vs-evict race:

1. Hot-path lookups must set `LookupItem.renew_lease_on_hit = true`. The DN renews/grants the lease atomically with the metadata read. The eviction worker skips any block whose lease is still active (§7.4).
2. If a lookup sees status `EVICTING`, the DN returns the item with `error_code = CAS_CONFLICT`, `retryable = true`. The client retries after short backoff.

A naked probe-only lookup (`renew_lease_on_hit = false`) is allowed for vLLM `lookup()` calls that must be side-effect-free. In that case the location is **not** safe to use for a subsequent read; the client must call `prepare_load` (which sets `renew_lease_on_hit = true`) before reading bytes.

Do not return a half-transition location.

---

## 15. Recovery and Failover

The recovery rules are different for DN restart and Store restart, because DRAM ownership is split.

### 15.1 DN Restart Recovery (Store DRAM intact)

Steps:

1. start PostgreSQL and extension,
2. `KVBitmapShmemInit` zero-fills bitmaps; lease and idempotency shmem are initialized empty,
3. assign new `dn_epoch` (a monotonic counter persisted in a small catalog row),
4. load Store registry (heartbeats),
5. scan `falcon_kvblock_table`:
   - `ALLOCATED`, `STORED`, `EVICTING`: mark bitmap occupied,
   - `STORED`: add to LRU cold side,
   - all `STORED` rows: create a short recovery lease with current `dn_epoch` and the current Store-region `store_epoch`,
6. reconcile `EVICTING` rows (see §15.3),
7. open BRPC server (start accepting requests).

Ordering is enforced by `_PG_init` and `RegisterBackgroundWorker` calls: the BRPC server worker is started **after** shmem init and recovery scan complete.

### 15.2 Store Restart Recovery (DRAM lost)

When a Store restarts, every block on that Store whose status is `ALLOCATED`, `STORED`, or `EVICTING` is no longer recoverable from DRAM.

Steps:

1. Store registers with DN with a new Store epoch.
2. DN scans rows where `store_node_id == restarted_store_id` and:
   - if status is `EVICTED` and `evicted_path` is valid -> keep,
   - if status is `EVICTED` and the SSD file is missing/corrupt -> `FAILED`,
   - if status is `STORED` and a valid SSD copy exists (e.g. from a prior eviction) -> reset to `EVICTED` with that path,
   - otherwise -> `FAILED`.
3. DN clears the bitmap for that Store and rebuilds it from the post-reconciliation rows.
4. Clients observing `FAILED` get a non-retryable miss (or a clear `NOT_FOUND` if the row is later GC'd).

### 15.2.1 Store Temporary Removal

Store restart can be slow. The cluster must remove the Store from allocation before full reconciliation finishes.

Policy:

1. If heartbeat is missed for `store_suspect_ms`, each DN marks the Store region `SUSPECT` and immediately stops allocating new blocks there.
2. If heartbeat is missed for `store_offline_ms`, each DN marks the Store region `OFFLINE`. New reads fail fast with retryable `STORE_WRITE_FAILED`/`THROTTLED` depending operation. Existing metadata rows are not rewritten immediately.
3. If the Store later returns with the same `store_epoch` and passes a health check, DN can move region back to `HEALTHY`.
4. If the Store returns with a new `store_epoch`, DN marks the region `QUARANTINED`, runs Store-restart recovery (§15.2), then moves it to `HEALTHY`.
5. Operator can force `DRAINING` for maintenance. Draining means no new allocations, but existing reads continue while data is evicted or naturally expires.

This prevents a long Store restart from blocking unrelated allocations and gives the cluster a clean path to continue using other Stores.

### 15.3 EVICTING Reconciliation

For each `EVICTING` row encountered during recovery:

| Scenario | DRAM status | SSD status | Action |
|---|---|---|---|
| DN restart only, Store DRAM intact | usable | valid | finalize to `EVICTED`, free bitmap |
| DN restart only, Store DRAM intact | usable | missing/invalid | rollback to `STORED`, keep bitmap occupied |
| Store restart, DRAM lost | gone | valid | finalize to `EVICTED`, free bitmap |
| Store restart, DRAM lost | gone | missing/invalid | mark `FAILED`, free bitmap |

In other words, rolling back to `STORED` is allowed only when DRAM is still authoritative.

### 15.4 Lease Recovery

Recovered leases use:

```
lease_token = newly generated token
dn_epoch    = current DN epoch
store_epoch = current Store-region epoch
expire_ms   = now + recovery_lease_grace_ms
```

There is no owner claim step. Any client that obtains the current token through `BatchLookupWithLease` or `prepare_load` can renew the anti-eviction lease. All requests carrying old `dn_epoch` or old `store_epoch` are rejected with `STALE_EPOCH` (retryable after the client refreshes metadata through lookup).

---

## 16. vLLM OffloadingManager Integration

### 16.1 Required Methods

The Python manager implements the upstream vLLM `OffloadingManager` interface (`vllm/v1/core/offloading_manager.py`). External method names stay singular; internal batching is hidden behind the C++ client.

Externally exposed methods (must match upstream):

1. `lookup(key: str, req_context) -> bool | None`
2. `prepare_load(keys: list[str], req_context) -> LoadStoreSpec`
3. `complete_load(keys: list[str])`
4. `prepare_store(keys: list[str], req_context) -> Optional[PrepareStoreOutput]`
5. `complete_store(keys: list[str], success: bool = True)`
6. `touch(keys: list[str])`

Internal helpers (implementation detail, not part of the upstream contract):

- `_batch_lookup_impl(keys, req_context)` – calls `BatchLookupWithLease`, groups by DN.
- `_batch_prepare_store_impl(keys, req_context)` – calls `BatchAllocateWithLease`, groups by DN.
- `_batch_complete_store_impl(keys, success_keys, fail_keys)` – calls `BatchUpdateBlockStatus` for `success_keys` and optionally `BatchFreeAllocated` for `fail_keys`.
- `_batch_load_impl(keys)` – groups by Store, calls `BatchReadBlock` / `BatchReadFromSSD`.
- `_batch_renew_impl(keys)` – calls `BatchRenewLease`.

The upstream `lookup(key)` is implemented as: maintain a small request-coalescing buffer; flush when full or when `prepare_load` is called, so the per-key `lookup` cost amortizes across one batch RPC.

### 16.2 Local Cache

Cache entry:

```python
KVBlockLocation(
    block_hash,
    status,
    store_node_id,
    pool_offset,
    evicted_path,
    lease_token,
    lease_expire_ms,
    version,
    dn_epoch,
    store_epoch,
)
```

Cache is valid only if:

```
now < lease_expire_ms - safety_margin_ms
```

### 16.3 Longest Prefix Lookup

vLLM can call batch lookup for all candidate hashes, then compute longest contiguous hit prefix locally.

This avoids one lookup RTT per block.

---

## 17. Implementation File Layout

Recommended layout:

```text
vllm_kv_cache/
  proto/
    kv_common.proto
    kv_metadata_service.proto
    kv_data_service.proto
  python/falconfs_kv/
    __init__.py
    offloading_manager.py
    dn_client.py
    store_client.py
    router.py
  src/
    metadata/
      kv_metadata_service_impl.h/.cpp
      kv_metadata_engine.h/.cpp
      kv_meta_table_accessor.h/.cpp
      bitmap_allocator.h/.cpp
      lease_manager.h/.cpp
      lru_manager.h/.cpp
      idempotency_store.h/.cpp
      eviction_coordinator.h/.cpp
    store/
      kv_data_service_impl.h/.cpp
      dram_block_pool.h/.cpp
      ssd_spill_manager.h/.cpp
      checksum.h/.cpp
    common/
      kv_types.h
      error_codes.h
      time_utils.h
      hash_utils.h
  test/
    test_bitmap_allocator.cpp
    test_lease_manager.cpp
    test_metadata_dn.cpp
    test_store_kv_memory_pool.cpp
    test_dn_brpc_service.cpp
    test_offloading_manager.py
```

---

## 18. Development Phases

### Phase 1: Proto and build integration

1. finalize proto,
2. generate BRPC/protobuf code,
3. compile empty DN/Store services,
4. add basic service startup tests.

### Phase 2: Store memory pool

1. implement DRAM block pool,
2. implement batch write/read,
3. implement checksum validation,
4. implement SSD read/write helpers.

### Phase 3: DN local managers

1. bitmap allocator,
2. lease manager,
3. LRU manager,
4. idempotency store.

### Phase 4: PostgreSQL metadata accessor

1. table OID resolution,
2. lookup,
3. insert allocated,
4. CAS status update,
5. recovery scans.

### Phase 5: DN BRPC service

1. connect service handlers to metadata engine,
2. add per-item results,
3. add idempotency replay,
4. add latency metrics.

### Phase 6: Python client/offloading manager

1. DN client pool,
2. Store client pool,
3. router,
4. vLLM-facing batch APIs.

### Phase 7: eviction/recovery

1. eviction worker,
2. restart recovery,
3. EVICTING reconciliation,
4. Store restart handling.

### Phase 8: performance hardening

1. chunk tuning,
2. queue/backpressure tuning,
3. p99 regression tests,
4. fault injection.

---

## 19. Test Plan

### 19.1 Unit Tests

1. bitmap allocate/free/recover,
2. lease renew/expire/stale-epoch handling,
3. LRU candidate ordering,
4. idempotency replay,
5. CAS update conflict.

### 19.2 Store Tests

1. batch write/read,
2. checksum mismatch,
3. out-of-range offset,
4. SSD spill/readback,
5. concurrent write/read.

### 19.3 DN Tests

1. batch allocate,
2. batch lookup hit/miss,
3. batch renew,
4. status transition legality,
5. partial failure result handling.

### 19.4 End-to-End Tests

1. store -> lookup -> load,
2. partial Store write failure updates only successful keys,
3. DN restart and recovered lease renewal,
4. stale epoch rejection and retry,
5. eviction rollback,
6. Store restart handling,
7. large batch chunking.

### 19.5 Performance Tests

Measure:

- p50/p95/p99 latency by API,
- throughput by batch size,
- lock contention,
- CAS conflict rate,
- eviction throughput,
- recovery time for N metadata rows.

---

## 20. Observability

### 20.1 Metrics

Per API:

- QPS,
- p50/p95/p99,
- success count,
- retryable error count,
- non-retryable error count,
- per-error-code counts.

DN-specific:

- bitmap free ratio per Store,
- active leases,
- expired leases,
- LRU size,
- eviction success/rollback count,
- recovery duration.

Store-specific:

- DRAM used/free,
- read/write throughput,
- checksum failures,
- SSD spill/read latency.

### 20.2 Logs

Log fields:

- `request_id`,
- `trace_id`,
- `client_id`,
- `block_hash`,
- `status`,
- `version`,
- `dn_epoch`,
- `store_node_id`,
- `pool_offset`,
- `error_code`.

---

## 21. Performance Model

### 21.1 RTT Budget

Lookup hit:

- one DN metadata RTT,
- one Store data RTT.

Store:

- one DN allocation RTT,
- one Store write RTT,
- one DN status update RTT.

### 21.2 Batch Scaling

Batching reduces RTT count from `O(N)` to `O(number_of_DNs + number_of_Stores)`.

Expected improvements:

- lookup: 5-10x for 10 blocks,
- allocation: 5-10x,
- Store read/write: 5-10x depending payload size.

### 21.3 Tail-Latency Risks

1. oversized batch,
2. long bitmap lock hold,
3. Store queue saturation,
4. SSD spill contention,
5. PostgreSQL relation/index contention.

Mitigations:

- chunking,
- bounded worker pools,
- backpressure via `THROTTLED`,
- metrics-driven tuning.

---

## 22. Design Decisions and Reasons

1. **BRPC only**
   - avoids dual RPC implementation and schema drift.

2. **DN-local bitmap**
   - keeps allocation one metadata RTT and avoids Store-side allocation races.

3. **Store does not update metadata**
   - keeps metadata authority centralized in DN.

4. **Per-item batch result**
   - enables safe partial retry and precise failure handling.

5. **CAS versioning**
   - prevents stale clients from overwriting newer state.

6. **Epoch-fenced leases**
   - protects failover/restart boundaries.

7. **Ownerless anti-eviction leases**
   - matches the real purpose of leases: protect DRAM blocks during access without inventing client ownership.

8. **Two-phase eviction**
   - prevents half-evicted visibility and supports rollback.

9. **Idempotency by request ID**
   - makes timeout retries deterministic.

10. **Shard-grouped metadata operations**
   - follows FalconFS metadata patterns and reduces table/index overhead.

---

## 23. Acceptance Criteria

The implementation is acceptable when:

1. partial Store write failures never mark failed keys as `STORED`,
2. DN restart rebuilds bitmap/LRU/lease state correctly,
3. Store restart correctly invalidates DRAM-only blocks and preserves SSD-backed blocks,
4. Store failure/offline state removes affected regions from allocation before long restart/recovery completes,
5. recovered leases can be renewed after clients refresh metadata,
6. stale epoch requests are fenced,
7. PostgreSQL concurrent tuple updates are returned to clients as retryable `CAS_CONFLICT`,
8. `EVICTING` rows are reconciled per the rules in §15.3,
9. all mutating APIs are idempotent under retry,
10. p99 latency remains stable under target batch sizes,
11. metrics and logs are sufficient to debug correctness and performance issues.

---

## 24. Implementation Alignment with FalconFS

This appendix maps every architectural concept to existing FalconFS code so a reviewer can verify pattern reuse without guesswork. Paths are relative to the repo root.

### 24.1 BRPC service hosting

| Concept | FalconFS reference | Notes |
|---|---|---|
| BRPC server inside PG extension | `falcon/brpc_comm_adapter/falcon_brpc_server.cpp` (`StartFalconCommunicationServer`, `FalconBrpcServer::Run`) | Reuse the same bootstrap; add a new service type for KV cache. |
| Service implementation | `falcon/brpc_comm_adapter/brpc_meta_service_imp.cpp` (`BrpcMetaServiceImpl::MetaCall`) | The KV cache service mirrors this pattern: build a `KVCacheJob`, call `m_jobDispatchFunc(job)`. |
| Job dispatcher | `falcon/connection_pool/pg_connection_pool.cpp` (`PGConnectionPool::DispatchMetaServiceJob`) | KV cache adds a parallel dispatcher or extends this one with a new opcode group. |
| Per-job latency reporting | `falcon/include/brpc_comm_adapter/brpc_meta_service_job.h` (`Done`, `e2eTimer`, `GetOpcodeE2ELatencyData`) | Add new opcodes for KV cache and corresponding `LatencyData` slots. |

### 24.2 Routing and shard cache

| Concept | FalconFS reference | Notes |
|---|---|---|
| Shared-memory shard cache | `falcon/metadb/shard_table.c` (`ShardTableShmemInit`, `ShardTableShmemCache`, `ShardTableShmemCacheInvalid`) | Reuse the cache for KV cache routing if the shard column is the same; otherwise add a parallel shmem cache with the same invalidation pattern (`pg_atomic_uint32` flag + LWLock). |
| Lookup helpers | `SearchShardInfoByShardValue`, `SearchShardInfoByHashValue` | Reused by KV metadata handlers to map `block_hash` → DN. |
| Client-side routing | `falcon_client/src/router.cpp` (`Router::FetchShardTable`, `Router::GetWorkerConnByPath`) | The KV client uses an analogous router built on top of the BRPC channel cache. |

### 24.3 Catalog access patterns

| Concept | FalconFS reference | Notes |
|---|---|---|
| Cached relation OID | `falcon/utils/utils.c` (`CachedRelationOid[]`, `GetRelationOid`) | Add `CACHED_RELATION_KVBLOCK` and a wrapper `KVBlockRelationId()` like `ShardRelationId()`. |
| Sub-transaction batched updates | `falcon/metadb/meta_handle.c` (`FalconCreateHandle`, lines around `BeginInternalSubTransaction` / `ReleaseCurrentSubTransaction` / `RollbackAndReleaseCurrentSubTransaction`) | Mirror this exact structure for `BatchAllocateWithLease`, `BatchUpdateBlockStatus`, `BatchFreeAllocated`. |
| Per-item error propagation | `falcon/include/metadb/meta_process_info.h` (`MetaProcessInfoData::errorCode`) and macros `CHECK_ERROR_CODE_WITH_CONTINUE` (`meta_handle.c:57-67`) | Use the same pattern internally; translate to `ItemResultMeta` at the BRPC boundary. |
| `heap_modify_tuple` + `CatalogTupleUpdateWithInfo` | `falcon/metadb/shard_table.c:falcon_update_shard_table` (lines 126–129) | Apply for CAS status updates. |
| `systable_beginscan` patterns | `falcon/metadb/shard_table.c:falcon_update_shard_table` (lines 111–118), `falcon/metadb/meta_handle.c:FalconFetchSliceIdHandle` (lines 2649–2656) | Use `BTEqualStrategyNumber` + `F_BYTEAEQ` for `block_hash`. |
| Background worker registration | `falcon/falcon_init.c:FalconStart2PCCleanupWorker`, `FalconStartConnectionPoolWorker` | KV eviction worker, KV idempotency GC worker, and KV recovery worker register the same way. |

### 24.4 Connection pool, perf macros, store

| Concept | FalconFS reference | Notes |
|---|---|---|
| PG connection-pool GUCs | `falcon/falcon_init.c` (`falcon_connection_pool.pool_size`, `.batch_size`, `.port`), defaults in `falcon/include/connection_pool/connection_pool_config.h` (`FALCON_CONNECTION_POOL_SIZE_DEFAULT 32`, `BATCH_SIZE_DEFAULT 512`, `MAX_CONCURRENT_SOCKET 4096`) | Add parallel knobs `falcon_kv_pool.size`, `.batch_size`, `.port`, `.shmem_size`. |
| Perf macros | `falcon/include/perf_counter/perf_macros.h` (`PERF_LATENCY_BEGIN`, `PERF_LATENCY_END`, `PERF_SCOPED_TIMER`) | Use these directly inside KV cache handlers. |
| Store BRPC channel options | `falcon_store/src/connection/node.cpp:CreateIOConnection` (`connection_type = "pooled"`, `connect_timeout_ms = 5000`, `timeout_ms = 10000`) | Use the same defaults for the new Store BRPC channel. |
| Existing Store memory pool (for BRPC iobufs) | `falcon_store/src/include/connection/brpc_server.h` (`std::pmr::synchronized_pool_resource`) | Do **not** reuse for KV DRAM. Allocate KV DRAM via `mmap` (with optional `MAP_HUGETLB`). |

---

## 25. Configuration GUCs

Proposed GUCs (mirroring `falcon_connection_pool.*`):

| GUC | Default | Meaning |
|---|---|---|
| `falcon_kv_pool.size` | 32 | KV BRPC dispatch worker pool size. |
| `falcon_kv_pool.batch_size` | 512 | Max items per batch handled by one worker. |
| `falcon_kv_pool.port` | (e.g. 56660) | KV BRPC listening port. |
| `falcon_kv_pool.shmem_size` | (computed) | Shmem reserved for KV cache (bitmaps, lease, idempotency). |
| `falcon_kv.max_stores` | 64 | Max number of distinct Store nodes per DN. |
| `falcon_kv.lease_default_ttl_ms` | 5000 | Default lease TTL granted by allocate/lookup. |
| `falcon_kv.lease_recovery_grace_ms` | 5000 | Grace TTL for recovered anti-eviction leases. |
| `falcon_kv.idempotency_ttl_ms` | 90000 | TTL for idempotent response cache. |
| `falcon_kv.eviction_low_watermark` | 0.10 | Free-block ratio that triggers eviction. |
| `falcon_kv.eviction_high_watermark` | 0.20 | Free-block ratio at which eviction stops. |
| `falcon_kv.eviction_chunk` | 64 | Number of candidates per eviction cycle. |
| `falcon_kv.store_max_inflight` | 256 | Max in-flight RPCs accepted by a Store. |
| `falcon_kv.store_suspect_ms` | 3000 | Heartbeat gap before Store region leaves allocation. |
| `falcon_kv.store_offline_ms` | 10000 | Heartbeat gap before Store region is treated as offline. |
| `falcon_kv.client_max_inflight_per_dn` | 32 | Client-side cap on concurrent batches per DN. |
| `falcon_kv.compress` | `none` | Optional client-side compression (none/lz4/zstd). |

GUC registration follows `RegisterFalconConfigVariables` in `falcon/falcon_init.c`.

---

## 26. Init Sequencing

Hook into `_PG_init` (see `falcon/falcon_init.c`) in this order:

1. `RegisterFalconConfigVariables()` – add KV cache GUCs alongside existing ones.
2. `InitializeFalconShmemStruct()` – extend with:
   - `KVBitmapShmemSize` / `KVBitmapShmemInit`
   - `KVLeaseShmemSize` / `KVLeaseShmemInit`
   - `KVIdempotencyShmemSize` / `KVIdempotencyShmemInit`
3. `RegisterFalconTransactionCallback()` – KV cache transaction callbacks if needed (e.g. to release sub-transaction resources).
4. `ForeignServerCacheInit()` and `ShardTableShmemInit()` – existing.
5. `KVMetadataRecoveryAtStartup()` – new step: scan `falcon_kvblock_table` and rebuild bitmap/LRU/lease.
6. `FalconStart2PCCleanupWorker()` – existing.
7. `FalconStartConnectionPoolWorker()` – existing.
8. `FalconStartKVDispatchWorker()` – new: KV BRPC dispatcher pool.
9. `FalconStartKVEvictionWorker()` – new: cold-block evictor.
10. `FalconStartKVIdempotencyGCWorker()` – new: idempotency TTL GC.
11. `FalconStartKVBrpcServerWorker()` – new: starts only after recovery completes.

If any step in 1–5 fails, the BRPC server (step 11) must not start.

---

## 27. Concurrency and Locking Model

Locks introduced:

| Subject | Lock type | Tranche name |
|---|---|---|
| Per-store bitmap | LWLock (exclusive on alloc/free) | `Falcon KV Bitmap` |
| Lease hash | LWLock (shared on read, exclusive on grant/renew/expire) | `Falcon KV Lease` |
| Idempotency hash | LWLock (shared on read, exclusive on insert/expire) | `Falcon KV Idempotency` |
| Store registry | shared_mutex (C++ side) and LWLock (PG side) | `Falcon KV Store Registry` |

Rules:

1. Acquire shmem locks in a fixed global order to avoid deadlocks: bitmap → lease → idempotency → registry.
2. Never hold a shmem LWLock across a remote BRPC call.
3. Never hold a PostgreSQL row lock while waiting on a BRPC reply.
4. The eviction worker takes locks in the same order.

---

## 28. Schema and Sharding Plan

1. Schema is identical on all DNs; CN owns DDL.
2. `block_hash` is the shard key. The shard column passed to `SearchShardInfoByShardValue` is computed from `block_hash` (e.g. first 8 bytes interpreted as `uint64`).
3. Indexes:
   - primary on `block_hash`,
   - `(status, updated_at_ms)` for eviction/recovery scans,
   - `(store_node_id, pool_offset)` for consistency audits.
4. CN provides a maintenance function (similar to `falcon_renew_shard_table`) to:
   - rebuild KV cache shard mapping if cluster topology changes,
   - move ranges between DNs when stores are added/removed.
5. KV cache and inode metadata can share or split shard tables. Default: share `falcon_shard_table` to minimize operational surface; split only if hashing distributions diverge in production.

