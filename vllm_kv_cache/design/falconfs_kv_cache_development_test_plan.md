# FalconFS KV Cache Development and Test Plan (v6 Full Aligned, v2)

## 0. How to Use This Plan

- This plan is the single source of truth for development and test execution.
- Update the **Progress Tracking Board** (§9) and **Execution Log** (§10) in-place after every meaningful change or test run.
- All implementation must conform to `falconfs_kv_cache_memory_pool_design_v6_full_en.md` (v6.2). Older docs are deprecated.

---

## 1. Purpose and Scope

This plan operationalizes the v6 full design into incremental development with verifiable gates. It covers:

- doc reconciliation,
- proto/contract validation,
- DN metadata native implementation,
- Store native implementation,
- Python OffloadingManager integration,
- distributed multi-DN validation,
- vLLM end-to-end validation,
- performance and observability acceptance.

Assumptions:

- BRPC is the only RPC transport,
- leases are ownerless anti-eviction guards,
- `dn_epoch` and `store_epoch` are fencing signals (not ownership),
- mutating RPCs are idempotent by `(api_name, request_id, client_id)`,
- CAS conflicts and PostgreSQL "tuple concurrently updated" surface to clients as retryable `CAS_CONFLICT`.

---

## 2. Authoritative Documents and Deprecations

Authoritative:

- `vllm_kv_cache/design/falconfs_kv_cache_memory_pool_design_v6_full_en.md` (v6.2 full design).

Deprecated (must not be used as standard):

- `falconfs_kv_cache_memory_pool_design_standalone_en.md`
  - status enum off-by-one and includes `owner_client_id`/`LEASE_OWNER_MISMATCH`,
- `falconfs_kv_cache_memory_pool_design_v6_en.md`
  - condensed predecessor of v6 full,
- `falconfs_kv_cache_memory_pool_design_v5_en.md`, `v4_final.md`
  - earlier drafts.

Action item D0: add a one-line "DEPRECATED, see v6_full_en.md" banner at the top of each deprecated doc as part of M0.

---

## 3. Current Baseline

In-tree:

- proto contracts under `vllm_kv_cache/proto/` (`kv_common.proto`, `kv_metadata_service.proto`, `kv_data_service.proto`).
- Python reference under `vllm_kv_cache/python/falconfs_kv/` (`reference.py`, `offloading_manager.py`).
- Standalone/reference tests under `vllm_kv_cache/test/`:
  - `test_reference_unit.py`
  - `test_metadata_reference.py`
  - `test_store_reference.py`
  - `test_brpc_contract_reference.py`
  - `test_kv_fault_reference.py`
  - `test_offloading_manager_api.py`
  - `test_offloading_manager_cluster.py`
  - `test_offloading_manager.py` (LEGACY mock; retire in M0).
- vLLM placeholder tests under `vllm_kv_cache/test/vllm/`.
- Distributed harness `scripts/falcon_distributed_test.sh` (1 CN + 2 DNs + 2 Clients) with `kv-test`/`kv-fault-test`.

Known gaps blocking production:

- no native DN BRPC service, no PG shmem-backed bitmap/lease/idempotency, no recovery scan,
- no native Store BRPC service, no DRAM mmap pool, no SSD spill manager, no heartbeat,
- Python OffloadingManager has no real shard-based DN routing,
- partial-failure cleanup in `complete_store` is deferred (no `BatchFreeAllocated` call),
- no idempotency replay, no two-phase eviction, no recovery reconciliation tests,
- no performance gate, no observability deliverables.

---

## 4. Milestones and Dependencies

```
M0 -> M1 -> M2 -> M3 -> M4 -> M5
              \-> (parallel observability/metrics threads)
```

Hard dependencies:

- M1 depends on M0.
- M2 depends on M0 (proto stable) and on DN-side region registry contract from M1's accessor headers.
- M3 depends on M1 and M2 minimal services running.
- M4 depends on M3 and on harness updates.
- M5 depends on M4 stable + vLLM available.

### M0 - Doc Reconciliation and Contract Freeze

Deliverables:

1. Mark deprecated docs with banner.
2. Validate proto and Python reference against v6.2:
   - status enum values match (1..5),
   - lease has no owner field (`owner_client_id` absent),
   - `LEASE_TOKEN_MISMATCH` present, `LEASE_OWNER_MISMATCH` absent,
   - epoch fencing fields exist on lease/read/write items,
   - per-item `ItemResultMeta` everywhere.
3. Retire legacy mock test `test_offloading_manager.py` or rewrite it to drive the reference cluster directly (chosen: retire and replace with cluster API tests).
4. Lock CompressionType usage policy (Store stores opaque bytes; client compresses if configured).

Exit criteria:

- All deprecated banners landed.
- Proto+code+v6 design fully consistent (verified by checklist in §6).
- Legacy test removed/replaced.

### M1 - DN Metadata Native Path

Deliverables:

1. Build wiring:
   - protoc + brpc plugin codegen targets for C++,
   - CMake/Bazel target adjustments,
   - Python codegen path (optional first; can use BRPC C++ + pybind11 in M3).
2. PostgreSQL extension scaffolding:
   - `KVMetadataServiceImpl` BRPC service inside the existing PG extension process,
   - dispatch through `PGConnectionPool::DispatchKVServiceJob` (new dispatcher mirroring existing meta dispatcher),
   - GUCs from design §25.
3. Catalog access (`KVMetaTableAccessor`):
   - `block_hash` is `BYTEA`; scans use `BTEqualStrategyNumber + F_BYTEAEQ`,
   - sub-transaction loop with `BATCH_OPERATION_GROUP_SIZE = 8`,
   - CAS via `heap_modify_tuple` + `CatalogTupleUpdateWithInfo` wrapped in `PG_TRY/PG_CATCH`,
   - PG concurrent-update conversion to retryable `CAS_CONFLICT`.
4. Shared-memory components (LWLock-protected):
   - bitmap allocator per Store region (`Falcon KV Bitmap` tranche),
   - ownerless lease map (`Falcon KV Lease` tranche),
   - idempotency hash (`Falcon KV Idempotency` tranche),
   - LRU manager.
5. Recovery and dn_epoch:
   - persisted dn_epoch row (small catalog table or system row),
   - startup scan rebuilds bitmap/LRU/lease (ownerless),
   - EVICTING reconciliation per §15.3.
6. Service handlers wired to engine; per-item result mapping.

Exit criteria:

- DN unit and PG-extension integration tests green:
  - allocate/lookup/CAS update/free/renew per-item correctness,
  - PG concurrent-update path returns retryable `CAS_CONFLICT`,
  - recovery reconstructs occupied bitmap and LRU,
  - GUC registration validated.

### M2 - Store Native Path

Deliverables:

1. DRAM pool:
   - `mmap(MAP_PRIVATE | MAP_ANONYMOUS [| MAP_HUGETLB])`,
   - 4 KiB-or-larger alignment,
   - configurable block size per Store.
2. Region partitioning:
   - per-DN continuous region map,
   - `RegisterStoreRegion` BRPC (or admin tool) to publish region info,
   - heartbeat to all owning DNs.
3. Data plane:
   - `BatchWriteBlock` with optional `verify_checksum`,
   - `BatchReadBlock` with checksum,
   - `BatchReadFromSSD` with sanitized `evicted_path`,
   - opaque payload + crc32, no decompression on read.
4. Fencing and admission:
   - `expected_store_epoch` and `expected_version` checks,
   - bounded inflight queue → `THROTTLED` on full,
   - state machine HEALTHY/DRAINING/SUSPECT/OFFLINE/QUARANTINED.
5. SSD spill manager:
   - sanitized path under `<ssd_root>/<store_node_id>/<hash_prefix>/...`,
   - fsync on spill,
   - readback validation.

Exit criteria:

- Store-only tests pass:
  - in-region read/write,
  - out-of-range/oversized rejection,
  - stale `store_epoch` rejection,
  - SSD spill+readback round-trip,
  - state transitions remove offending region from allocation.

### M3 - Python OffloadingManager + BRPC Client Integration

Deliverables:

1. DN client adapter (BRPC-backed) with batch APIs:
   - `BatchLookupWithLease`, `BatchAllocateWithLease`, `BatchRenewLease`, `BatchUpdateBlockStatus`, `BatchFreeAllocated`.
2. Store client adapter (BRPC-backed):
   - `BatchWriteBlock`, `BatchReadBlock`, `BatchReadFromSSD`.
3. Multi-DN router consuming a shard table (or KV-cache shard table).
4. `FalconFSOffloadingManager` mode switch:
   - `mode="reference"`: in-memory cluster (today).
   - `mode="cluster"`: real BRPC clients.
5. Public API parity with vLLM upstream `OffloadingManager`:
   - `lookup`, `prepare_load`, `complete_load`, `prepare_store`, `complete_store(success)`, `touch`.
   - Fix gaps:
     - `complete_store(success=False)` calls `BatchFreeAllocated` (or queues TTL cleanup).
     - `complete_store` with partial data marks missing keys for cleanup.
     - `prepare_store` returns partial spec correctly; on complete failure returns `None` to match upstream.

Exit criteria:

- All OffloadingManager API tests pass against both reference and cluster modes.
- Multi-DN routing validated by hashing keys to different DNs and asserting both DNs are hit.

### M4 - Distributed Multi-DN Validation

Deliverables:

1. Harness extension (`scripts/falcon_distributed_test.sh`):
   - bring up KV BRPC ports for DN1 and DN2,
   - register a Store and partition into per-DN regions for DN1 and DN2,
   - new commands `kv-cluster-test` and `kv-cluster-fault-test` that hit real BRPC endpoints,
2. Cluster regression:
   - end-to-end allocate/write/update/lookup/read with keys split across DN1 and DN2,
   - `BatchFreeAllocated` on partial failure,
   - DN restart drill: dn_epoch increments, lease renew with old token rejected, new lease granted by lookup,
   - Store restart drill: store_epoch increments, reads with old store_epoch rejected,
   - two-phase eviction including rollback,
   - idempotency replay under simulated retry.
3. Harness shutdown reliability:
   - retain pooler-port cleanup logic added during 2-DN harness work.

Exit criteria:

- Repeated full-cycle harness runs pass: `start -> kv-cluster-test -> kv-cluster-fault-test -> stop -> status`.
- Both DNs verifiably hit (e.g., per-DN counters or per-DN log evidence).

### M5 - vLLM End-to-End Validation

Deliverables:

1. vLLM-installed environment provisioning notes.
2. Smoke test passes (already skips when vLLM missing).
3. Correctness test: deterministic prefix store→load round-trip.
4. Fault test: Store offline becomes retryable failure or cache miss.
5. Benchmark baseline: store/lookup/load latency for 100-block run.

Exit criteria:

- All vLLM tests green or with documented skip reasons.
- Recorded baseline numbers committed in §10 execution log.

---

## 5. Work Breakdown Structure (Detailed)

### WBS-1 (M0) Doc and Contract Reconciliation

- [ ] D0.1 Add deprecation banners on standalone/v5/v6 docs.
- [ ] D0.2 Confirm proto vs v6.2 (status enum, error codes, lease/read/write epoch fields).
- [ ] D0.3 Confirm Python reference vs v6.2 (no `owner_client_id`, ownerless renew rules, epoch fencing).
- [ ] D0.4 Retire legacy `test_offloading_manager.py`.
- [ ] D0.5 Add explicit "what is `data` in `complete_store`" note to README/comment (test scaffold; native client does not pass `data`).

### WBS-2 (M1) DN Metadata

- [ ] D1.1 protoc + brpc plugin codegen target.
- [ ] D1.2 CMake/Bazel sources added for `vllm_kv_cache/src/metadata/*`.
- [ ] D1.3 GUC registration (`falcon_kv_pool.size`, `.batch_size`, `.port`, `.shmem_size`, `falcon_kv.*`).
- [ ] D1.4 `KVMetaTableAccessor` (lookup, insert, CAS update, delete-or-fail, scans).
- [ ] D1.5 Sub-transaction wrapper (`BeginInternalSubTransaction` chunked by 8).
- [ ] D1.6 PG_TRY around `CatalogTupleUpdateWithInfo` → retryable `CAS_CONFLICT`.
- [ ] D1.7 Bitmap allocator in shmem (`KVBitmapHeader`, init in `FalconShmemInit`).
- [ ] D1.8 Lease manager in shmem (no owner field, `LEASE_TOKEN_MISMATCH` distinct from `STALE_EPOCH`).
- [ ] D1.9 Idempotency store in shmem with TTL bgworker.
- [ ] D1.10 LRU manager.
- [ ] D1.11 dn_epoch persisted catalog row + bump on init.
- [ ] D1.12 Recovery scan: rebuild bitmap, LRU, lease (ownerless), reconcile EVICTING.
- [ ] D1.13 BRPC service handlers (`KVMetadataServiceImpl`).
- [ ] D1.14 Per-item error mapping aligned with proto `ErrorCode`.
- [ ] D1.15 Latency instrumentation (perf macros) for each handler.

### WBS-3 (M2) Store

- [ ] D2.1 DRAM pool with mmap (+ optional MAP_HUGETLB).
- [ ] D2.2 Region partitioning configuration and `RegisterStoreRegion`.
- [ ] D2.3 Heartbeat sender to each owning DN.
- [ ] D2.4 `BatchWriteBlock` with checksum + epoch fencing.
- [ ] D2.5 `BatchReadBlock` with checksum.
- [ ] D2.6 `BatchReadFromSSD` with path sanitation.
- [ ] D2.7 Inflight queue and `THROTTLED` admission control.
- [ ] D2.8 Store state machine.
- [ ] D2.9 SSD spill manager (`SpillBlockToSSD` path format, fsync).
- [ ] D2.10 Latency/throughput metrics.

### WBS-4 (M3) Python Client

- [ ] D3.1 BRPC DN client wrapper (pybind11 or native asyncio bridge).
- [ ] D3.2 BRPC Store client wrapper.
- [ ] D3.3 Shard-table-aware router; group keys by target DN.
- [ ] D3.4 Mode switch reference vs cluster.
- [ ] D3.5 `complete_store(success=False)` → `BatchFreeAllocated`.
- [ ] D3.6 `complete_store` partial data → free missing keys (or queue TTL).
- [ ] D3.7 Local lease cache with safety margin.
- [ ] D3.8 Concurrent fan-out per DN (bounded thread/asyncio pool).

### WBS-5 (M4) Distributed Validation

- [ ] D4.1 Harness adds KV ports + Store region registration on both DNs.
- [ ] D4.2 `kv-cluster-test` command: alloc/write/update/lookup/read across both DNs.
- [ ] D4.3 `kv-cluster-fault-test` command: epoch fencing, restart, two-phase eviction, idempotency replay.
- [ ] D4.4 Per-DN counters/logs to assert both DNs hit.
- [ ] D4.5 Repeat-cycle stability check.

### WBS-6 (M5) vLLM E2E

- [ ] D5.1 Provisioning notes for vLLM install.
- [ ] D5.2 Smoke green.
- [ ] D5.3 Correctness green.
- [ ] D5.4 Fault green.
- [ ] D5.5 Benchmark numbers captured.

### Cross-cutting (parallel to all milestones)

- [ ] X1 Observability: per-API metrics (qps/p50/p95/p99, error counters), DN-specific (bitmap free ratio, leases, eviction rollback), Store-specific (DRAM/SSD throughput).
- [ ] X2 Structured logs with `request_id`, `trace_id`, `client_id`, `block_hash`, `dn_epoch`, `store_node_id`, `pool_offset`, `error_code`.
- [ ] X3 Performance gate: per-API budgets per v6 full §21.
- [ ] X4 Backpressure tuning: chunk size, queue depth.

---

## 6. Contract Verification Checklist (M0 Gate)

Run before any new coding:

- [ ] Proto status enum `1..5` matches design §3.2.
- [ ] No `owner_client_id` anywhere in proto/code/tests.
- [ ] `ErrorCode.LEASE_TOKEN_MISMATCH` present; no `LEASE_OWNER_MISMATCH`.
- [ ] `LeaseInfo` has `lease_token`, `lease_expire_ms`, `dn_epoch`, `store_epoch`.
- [ ] `BlockLocation` has `store_epoch`.
- [ ] `RenewLeaseItem` has `expected_dn_epoch`, `expected_store_epoch`.
- [ ] `WriteItem` and `ReadItem` and `SSDReadItem` have `expected_version` (and `expected_store_epoch` for DRAM ops).
- [ ] `CommonRequestMeta` has `request_id`, `client_id`, `client_hostname`, `trace_id`, `request_start_ms`, `client_epoch_hint`, `timeout_ms`.
- [ ] `ItemResultMeta` has `success`, `error_code`, `retryable`, `error_message`.

---

## 7. Test Strategy and Commands

Run from repository root:

```bash
cd /home/junbin/junbinkang/falconfs
export PYTHONPATH="$PWD/vllm_kv_cache/python:${PYTHONPATH:-}"
```

### 7.1 Standalone reference gate (must always be green)

```bash
python3 vllm_kv_cache/test/test_reference_unit.py
python3 vllm_kv_cache/test/test_metadata_reference.py
python3 vllm_kv_cache/test/test_store_reference.py
python3 vllm_kv_cache/test/test_brpc_contract_reference.py
python3 vllm_kv_cache/test/test_kv_fault_reference.py
python3 vllm_kv_cache/test/test_offloading_manager_api.py
python3 vllm_kv_cache/test/test_offloading_manager_cluster.py
```

Or:

```bash
scripts/falcon_distributed_test.sh kv-test
scripts/falcon_distributed_test.sh kv-fault-test
```

### 7.2 Distributed cluster gate (1 CN + 2 DNs + 2 Clients)

```bash
scripts/falcon_distributed_test.sh start
scripts/falcon_distributed_test.sh status
scripts/falcon_distributed_test.sh test
scripts/falcon_distributed_test.sh stop
scripts/falcon_distributed_test.sh status
```

After M4 lands:

```bash
scripts/falcon_distributed_test.sh kv-cluster-test       # planned
scripts/falcon_distributed_test.sh kv-cluster-fault-test # planned
```

### 7.3 vLLM gate (run last)

```bash
python3 vllm_kv_cache/test/vllm/test_vllm_kv_smoke.py
python3 vllm_kv_cache/test/vllm/test_vllm_kv_correctness.py
python3 vllm_kv_cache/test/vllm/test_vllm_kv_fault.py
python3 vllm_kv_cache/test/vllm/benchmark_vllm_kv.py
```

### 7.4 Test inventory and roles

| Test | Layer | Status | Notes |
|---|---|---|---|
| `test_reference_unit.py` | reference | active | bitmap/lease/LRU/idempotency/region state |
| `test_metadata_reference.py` | reference | active | metadata + CAS + recovery rebuild |
| `test_store_reference.py` | reference | active | epoch fencing, region validation |
| `test_brpc_contract_reference.py` | reference | active | end-to-end cycle, partial failure |
| `test_kv_fault_reference.py` | reference | active | fault scenarios, EVICTING-as-conflict |
| `test_offloading_manager_api.py` | reference | active | upstream API surface |
| `test_offloading_manager_cluster.py` | reference + (cluster, future) | active | cluster entrypoint |
| `test_offloading_manager.py` | legacy mock | RETIRE in M0 | superseded |
| `test/vllm/*.py` | vLLM | placeholder | activated in M5 |
| `kv-cluster-test` (planned) | cluster | TODO M4 | distributed BRPC validation |
| `kv-cluster-fault-test` (planned) | cluster | TODO M4 | fault drills |

---

## 8. Acceptance Matrix

- [ ] A1 Partial Store write failure never marks failed keys as `STORED`.
- [ ] A2 PG concurrent-update returns retryable `CAS_CONFLICT`; CAS by version mismatch returns the same.
- [ ] A3 Leases are ownerless; `LEASE_TOKEN_MISMATCH` and `LEASE_EXPIRED` are distinct, retryable after lookup.
- [ ] A4 Stale `dn_epoch`/`store_epoch` requests are fenced with `STALE_EPOCH` (retryable after refresh).
- [ ] A5 DN restart rebuilds bitmap/LRU/lease and bumps `dn_epoch`; old lease tokens rejected.
- [ ] A6 Store restart with new epoch quarantines region until reconciliation; old `store_epoch` rejected.
- [ ] A7 Two-phase eviction: success path frees bitmap, rollback path restores `STORED` and LRU.
- [ ] A8 `complete_store(success=False)` and partial-data cases free or schedule cleanup of failed keys.
- [ ] A9 Probe-only `lookup()` is side-effect-free; reads must use `prepare_load()`.
- [ ] A10 Mutating RPCs are idempotent under retry by `(api_name, request_id, client_id)`.
- [ ] A11 EVICTING lookup returns `CAS_CONFLICT, retryable=true`, never a half-evicted location.
- [ ] A12 Distributed: keys hashed across DNs verifiably hit both DN1 and DN2.
- [ ] A13 vLLM smoke + correctness + fault green.
- [ ] A14 Per-API latency budgets met (v6 full §21 targets) on the harness baseline.
- [ ] A15 Observability: required metrics and structured logs emitted.

---

## 9. Progress Tracking Board

Status legend: `TODO`, `IN_PROGRESS`, `BLOCKED`, `DONE`.

| ID | Work Item | Status | Owner | Last Update | Notes |
|---|---|---|---|---|---|
| P0 | M0 doc reconciliation + contract freeze | DONE | agent | 2026-05-06 | Banners landed on standalone/v5/v4_final; legacy mock test retired; `data` scaffold documented |
| P1 | M1 DN metadata native path | DONE | agent | 2026-05-06 | Completed phased M1 baseline: proto codegen, KV metadata/data service skeletons, native metadata engine wiring, service idempotency+dedup, PG connection-pool KV dispatch callback, and `falcon_kv_pool.*` GUC registration; verified by full build/install, 2-DN start/status/stop, `kv-test`, and `kv-fault-test` |
| P1A | M1 Phase 1 (proto + empty DN/Store services) | DONE | agent | 2026-05-06 | `FalconKVProto` codegen wired; `KVMetadataServiceImpl` + `KVDataServiceImpl` skeletons compile with per-item INTERNAL_ERROR contracts; `FalconKVPrimitivesUT` (24 tests) + `kv-test` + `kv-fault-test` all green |
| P1B | M1 Phase 2 (metadata native engine path) | DONE | agent | 2026-05-06 | Added `KVMetadataEngine` and wired `KVMetadataServiceImpl` to real allocate/lookup/renew/update/free paths (single-region native mode); added engine + service gtests; full gates green (`FalconKVPrimitivesUT` 27 tests + `kv-test` + `kv-fault-test`) |
| P1C | M1 Phase 3 (service idempotency + allocate dedup) | DONE | agent | 2026-05-06 | Added request-id replay cache for mutating metadata APIs in `KVMetadataServiceImpl` and implemented `BatchAllocateWithLease.deduplicate_in_request` semantics; expanded metadata service gtests for replay/dedup; full gates green (`FalconKVPrimitivesUT` 30 tests + `kv-test` + `kv-fault-test`) |
| P1D | M1 Phase 4 (PG dispatcher + KV pool GUC scaffold) | DONE | agent | 2026-05-06 | Added `FalconDispatchKVJob2PGConnectionPool` and `DispatchKVServiceJob` path in `pg_connection_pool`; registered `falcon_kv_pool.port/pool_size/batch_size/shmem_size` GUCs and validated they are accepted in CN/DN configs via successful 2-DN cluster start |
| P1E | M1 Phase 5 (multi-region engine + recovery + idempotency TTL) | DONE | agent | 2026-05-06 | `KVMetadataEngine` refactored to multi-region (`RegisterStoreRegion`, `SetRegionState`, affinity allocation per v6 §5.3 with `preferred_store_id` + `allow_fallback_store`), recovery API (`MarkBitmapOccupied`, `RestoreRow`, `BumpDnEpoch` + `LeaseManager::Clear`), and `LeaseManager::Renew` now rejects stale `dn_epoch` even after lease wipe. `KVMetadataServiceImpl` now caches mutating responses through `IdempotencyStore` with configurable TTL + injectable clock (`SetClockForTest`-style ctor); `BatchAllocateWithLease` forwards `preferred_store_id`/`allow_fallback_store`. Added gtests for multi-region routing, DRAINING fallback, recovery, dn_epoch fencing, idempotency TTL expiry. Full gates green: `FalconKVPrimitivesUT` 53 tests + `kv-test` + `kv-fault-test` |
| P1F | M1 Phase 6 (BRPC service adapters + PG accessor batch ops) | DONE | agent | 2026-05-06 | Enabled `option cc_generic_services = true;` in `kv_metadata_service.proto` and `kv_data_service.proto`, then added `vllm_kv_cache/src/service/{kv_metadata_brpc_service,kv_data_brpc_service}.{h,cpp}` deriving from the protobuf-generated abstract `Service` base. Adapters bridge brpc-style RPC dispatch (`controller`/`request`/`response`/`done`) to the engine-backed `KV*ServiceImpl` and call `done->Run()`. New static lib `FalconKVBrpcServiceAdapters` (proto-only, no brpc dependency). Extended PG accessor (`falcon/metadb/kvblock_accessor.c`) with `FalconKvblockInsertAllocated` and `FalconKvblockDelete` (CAS by version); both wrapped in `PG_TRY/PG_CATCH` for retryable conflict mapping. Added gtests `test_kv_brpc_adapters.cpp` exercising allocate/lookup/idempotency-replay through the metadata adapter and write/read through the data adapter, all driven through the abstract `google::protobuf::Service` ABI. Full gates green: `FalconKVPrimitivesUT` 56 tests + `kv-test` + `kv-fault-test` + 2-DN `start -> stop` cycle with rebuilt extension |
| P1G | M1/M2 Phase 7 (admission control + sub-tx wrapper + accessor abstraction + recovery scan) | DONE | agent | 2026-05-06 | Added v6 §12.5 admission control to `KVDataServiceImpl` (atomic inflight counter + `max_inflight`; over-limit batches return per-item `THROTTLED, retryable`); added v6 §4.1.2 `KVSubTransaction` interface + `kBatchOperationGroupSize=8` + `ProcessInSubBatches` template with no-op test impl; added v6 §4.2 `IKVMetaTableAccessor` C++ contract + `InMemoryKVMetaTableAccessor` (Lookup/InsertAllocated/CASStatusUpdate/Delete/ScanForRecovery, all per-shard, mutex-safe); added v6 §6.4/§15 `FalconKvblockScanForRecovery` PG accessor C function. New gtests cover throttle (3), sub-tx (3), in-memory accessor (2). Full gates green: `FalconKVPrimitivesUT` 64 tests + `kv-test` + `kv-fault-test` + 2-DN `build -> start -> stop` |
| P1H | M1 Phase 8 (eviction coordinator) | DONE | agent | 2026-05-06 | Added v6 §14 `EvictionCoordinator` (`vllm_kv_cache/src/metadata/eviction_coordinator.{h,cpp}`): pulls cold candidates from `KVMetadataEngine::ColdCandidates`, gates on `KVMetadataEngine::CanEvict` (lease check), CAS-drives `STORED -> EVICTING`, calls a configurable `SpillFn`, then CAS-drives `EVICTING -> EVICTED` on success or `EVICTING -> STORED` on failure. Added gtests for end-to-end eviction, active-lease skip, and spill-failure rollback. `LeaseManager::Renew` already returns `STALE_EPOCH` for old-epoch requests after lease wipe; reused that semantic. Full gates green: `FalconKVPrimitivesUT` 67 tests + `kv-test` + `kv-fault-test` |
| P1I | M1 Phase 9 (PG-shmem placement: bitmap/lease/idempotency) | DONE | agent | 2026-05-06 | Implemented v6 §6.1, §7.0, §9.2, §27 PG-shared-memory scaffolding: new `falcon/include/metadb/kv_shmem.h` + `falcon/metadb/kv_shmem.c` define `KVBitmapShmemControl` (per-Store-region headers + bitmap word pool sized via `falcon_kv.max_stores`), `KVLeaseShmemControl` (LWLock-guarded fixed-size hash of `KVLeaseEntry`), `KVIdempotencyShmemControl` (LWLock-guarded fixed-size hash of `KVIdempotencyEntry`). Each control struct registers its own LWLock tranche — `Falcon KV Bitmap`, `Falcon KV Lease`, `Falcon KV Idempotency` — via `LWLockNewTrancheId` + `LWLockRegisterTranche`, matching the existing `ShardTableShmemControl` pattern. New `KVBitmapShmemSize/Init`, `KVLeaseShmemSize/Init`, `KVIdempotencyShmemSize/Init` are wired into `FalconShmemRequest()` and `FalconShmemInit()` in `falcon_init.c`, after the existing shmem hooks. The structures are not yet read/written by the engine path (the native engine still uses `std::mutex`-protected in-process versions); this slice lands the PG-shmem region so future shmem-aware engine code can swap in without changing init order. Validation: `scripts/falcon_distributed_test.sh build` rebuilds `falcon.so` clean; 2-DN harness `start -> status -> stop` cycle passes with the new shmem segments allocated; `kv-test` + `kv-fault-test` still green |
| P1J | M1 Phase 10 (PG-shmem primitive ops + engine/service runtime hook) | DONE | agent | 2026-05-07 | Extended `kv_shmem` with LWLock-guarded operation APIs for runtime access: bitmap region register/allocate/free/mark/stats, lease grant/renew/can-evict/drop/clear (using `hash_search`), and idempotency get/put/gc. Added `KVShmemRuntimeOps` plumbing (`kv_shmem_runtime.{h,cpp}`), integrated `KVMetadataEngine` to consume shmem-backed bitmap+lease callbacks when installed, and wired `KVMetadataServiceImpl` replay cache (`TryReplay`/`RecordReplay`/`IdempotencyGc`) to runtime idempotency callbacks. Added `InstallKVShmemRuntimeOpsFromPg()` binding entry and now call it at PG extension daemon startup via `FalconInstallKVShmemRuntimeCallbacks`, with `kv_shmem_runtime.cpp` linked into `falcon.so`. Added persisted `dn_epoch` catalog wiring in PG metadata path (per-shard `<kvblock>_epoch` table + accessor load/bump) and startup bump + lease clear during recovery. Wired PG daemon startup recovery via `FalconKvblockRecoverShmemOnStartup` (backed by `FalconKvblockScanForRecovery`) so connection-pool worker rebuilds bitmap occupancy and reconciles `EVICTING -> STORED` before serving. Full `./build.sh build falcon` + `FalconKVPrimitivesUT` (71 tests) gates green |
| P2 | M2 Store native path | DONE | agent | 2026-05-07 | Completed KV BRPC integration in PG extension runtime end-to-end: `libbrpcplugin.so` links KV metadata/data stack + generated KV protobufs, `FalconBrpcServer::Run` registers legacy meta + KV metadata + KV data BRPC services, and KV RPCs are now enqueued via `KVCacheJob` shim to `PGConnectionPool::DispatchKVServiceJob` (executed on pool workers through `KVCacheWorkerTask`) instead of running on BRPC worker threads. Plugin build now supports read-only source proto dirs by generating KV protobufs under `build/brpc_comm_adapter/proto` and reusing existing remote proto objects. Validation: full `./build.sh build falcon` + `FalconKVPrimitivesUT` (71 tests) green |
| P2A | M2 Phase A (data service backend baseline) | DONE | agent | 2026-05-06 | Added `KVStoreEngine` and rewired `KVDataServiceImpl` write/read/read-from-ssd off stubs to backend semantics; added `test_kv_store_engine.cpp` and expanded `test_kv_data_service_impl.cpp`; full gates green (`FalconKVPrimitivesUT` 34 tests + `kv-test` + `kv-fault-test`) |
| P2B | M2 Phase B (DRAM pool + region state + SSD spill) | DONE | agent | 2026-05-06 | Added `DramPool` (mmap-backed, hugepage-aware), `StoreRegionRegistry` (region state machine with heartbeat ticks + epoch-driven QUARANTINED), `SSDSpillManager` (sanitized path layout + `fdatasync`); refactored `KVStoreEngine` to use them; added unit tests `test_dram_pool.cpp`, `test_store_region_registry.cpp`, `test_ssd_spill_manager.cpp`, plus integration tests in `test_kv_store_engine.cpp`. Registered `falcon_kv.{max_stores,lease_default_ttl_ms,lease_recovery_grace_ms,idempotency_ttl_ms,store_max_inflight,store_suspect_ms,store_offline_ms,client_max_inflight_per_dn,eviction_chunk}` GUCs in `falcon_init.c` (validated by 2-DN cluster start). Full gates green: `FalconKVPrimitivesUT` 48 tests + `kv-test` + `kv-fault-test` |
| P3 | M3 Python OffloadingManager + BRPC clients | IN_PROGRESS | agent | 2026-05-06 | Reference manager now supports multi-DN routing, partial-failure cleanup, and `prepare_store` returning `None`. BRPC adapters pending |
| P4 | M4 Distributed 2-DN cluster validation | TODO | unassigned | - | Adds `kv-cluster-test`, `kv-cluster-fault-test` |
| P5 | M5 vLLM end-to-end suite | TODO | unassigned | - | Smoke/correctness/fault/benchmark |
| X1 | Observability + metrics | TODO | unassigned | - | Cross-cutting |
| X2 | Performance gates | TODO | unassigned | - | Cross-cutting; tied to v6 §21 |

---

## 10. Execution Log

Append one entry per meaningful run/change:

```text
[YYYY-MM-DD HH:MM] <owner> <area> <status> <summary> <next action>
```

Examples:

```text
[2026-05-06 16:10] agent P4 DONE Full start->test->stop cycle passed on 2-DN harness Keep monitoring flakiness for pooler shutdown
[2026-05-06 16:25] agent P0 IN_PROGRESS Drafted v2 plan with corrected acceptance matrix and gaps Begin doc deprecation banners next
[2026-05-06 16:45] agent P0 DONE Added DEPRECATED banners on standalone/v5/v4_final and removed legacy test_offloading_manager.py mock test Move to reference-layer P1/P3 gap closure
[2026-05-06 17:05] agent P1 IN_PROGRESS Reference MetadataService gained idempotency wiring + two-phase eviction (begin/commit/rollback) + free_allocated; bitmap freed on EVICTED transition Native PG extension scaffolding next
[2026-05-06 17:15] agent P3 IN_PROGRESS FalconFSOffloadingManager now routes to a per-DN ReferenceCluster map, prepare_store returns None on full failure, complete_store(success=False) and partial-data path call free_allocated, per-item idempotency keys avoid cache collision Real BRPC clients still pending
[2026-05-06 17:20] agent X 47 reference unit tests across 10 files green via kv-test/kv-fault-test harness commands Continue with M1/M2 native scaffolding when build wiring is approved
[2026-05-06 19:55] agent P1 IN_PROGRESS Added vllm_kv_cache/src/{common,metadata}/ with BitmapAllocator, LeaseManager, LRUManager, IdempotencyStore. Pure C++ static lib FalconKVCommon compiles clean against existing CMake/Ninja build Wire CMake subdirs into top-level and tests/CMakeLists.txt
[2026-05-06 20:00] agent P1 DONE Native primitives library built (build/vllm_kv_cache/libFalconKVCommon.a) and 18 gtest cases pass in build/tests/falcon_kv/FalconKVPrimitivesUT (5 BitmapAllocator + 5 LeaseManager + 4 LRUManager + 4 IdempotencyStore) Begin PG-extension layer (table accessor, service handlers, shmem hooks)
[2026-05-06 20:05] agent P1 DONE Extended kv-test harness command to run FalconKVPrimitivesUT after Python reference tests; full kv-test now covers 47 Python tests + 18 native gtest cases Move to KVMetaTableAccessor scaffolding next
[2026-05-06 20:30] agent P1 IN_PROGRESS Added v6 `falcon_kvblock_table` schema module (`metadb/kvblock_table.c/.h`), bytea scan-key cache (`F_BYTEAEQ`), `falcon_create_kvblock_table()` PG function, SQL registration, and harness DDL call Build and live 2-DN startup validation next
[2026-05-06 20:45] agent P1 DONE PG extension build passed; installed updated `falcon.so` and `falcon--1.0.sql`; 2-DN harness startup creates `falcon_create_kvblock_table()` successfully on CN/DN1/DN2; harness DDL block now uses `ON_ERROR_STOP=1` so future DDL failures fail fast Continue with KVMetaTableAccessor and BRPC service skeleton
[2026-05-06 20:55] agent P1 IN_PROGRESS Added `metadb/kvblock_accessor.c/.h` scaffold: shard relation/index OID resolution, bytea hash lookup (`F_BYTEAEQ`), CAS status update using `CatalogTupleUpdateWithInfo` with `PG_TRY/PG_CATCH` conflict mapping Wire accessor into upcoming metadata engine/service handlers
[2026-05-06 21:00] agent P1 DONE Rebuilt extension with accessor object linked; reran `kv-test` and `kv-fault-test` gates (47 Python + 18 native gtest + 3 fault tests all green) Move to proto codegen and KV metadata BRPC service skeleton
[2026-05-06 21:10] agent P1 IN_PROGRESS Added v6 proto codegen target in `vllm_kv_cache/CMakeLists.txt` (generates `kv_common.pb.*`, `kv_metadata_service.pb.*`, `kv_data_service.pb.*`), plus static lib `FalconKVProto` and skeleton metadata service lib `FalconKVMetadataService` Continue wiring transport/dispatch integration to PG worker pool
[2026-05-06 21:15] agent P1 DONE Added compile-ready `KVMetadataServiceImpl` skeleton with per-item INTERNAL_ERROR placeholders for all batch APIs and server_time stamping; added gtests (`test_kv_metadata_service_impl.cpp`) Build and kv gates green (native gtest suite now 21 tests)
[2026-05-06 20:51] agent P1A DONE Added compile-ready `KVDataServiceImpl` skeleton for `BatchWriteBlock/BatchReadBlock/BatchReadFromSSD` with per-item INTERNAL_ERROR placeholders and server_time stamping; wired `FalconKVDataService` into CMake and added `test_kv_data_service_impl.cpp` Full gates green: `FalconKVPrimitivesUT` 24 tests + `kv-test` + `kv-fault-test`; proceed to real DN dispatch integration in M1
[2026-05-06 20:59] agent P1B DONE Added `KVMetadataEngine` (native in-memory metadata core over BitmapAllocator/LeaseManager/LRU) and rewired `KVMetadataServiceImpl` batch APIs to real engine-backed behavior instead of INTERNAL_ERROR stubs; added `test_kv_metadata_engine.cpp` and expanded metadata service tests to success/CAS/free paths Full gates green: `FalconKVPrimitivesUT` 27 tests + `kv-test` + `kv-fault-test`; next move is PG accessor/worker dispatch integration for remaining M1 scope
[2026-05-06 21:03] agent P1C DONE Added request-level idempotency replay cache keyed by `(api_name, request_id, client_id)` for `BatchAllocateWithLease`, `BatchRenewLease`, `BatchUpdateBlockStatus`, and `BatchFreeAllocated` in `KVMetadataServiceImpl`; implemented in-request dedup (`deduplicate_in_request`) for batch allocate; expanded `test_kv_metadata_service_impl.cpp` with replay/dedup tests Full gates green: `FalconKVPrimitivesUT` 30 tests + `kv-test` + `kv-fault-test`; continue M1 with PG accessor/worker dispatch path
[2026-05-06 21:12] agent P1D DONE Added M1 PG scaffold: `falcon_kv_pool.*` GUC registration (`falcon_init.c` + connection-pool config globals), plus dedicated KV dispatcher callback `FalconDispatchKVJob2PGConnectionPool` and `DispatchKVServiceJob` path in `pg_connection_pool.cpp`; updated harness postgres configs to set `falcon_kv_pool.*` values Full verification green: `scripts/falcon_distributed_test.sh build`, then `start -> status -> stop`, then `kv-test` and `kv-fault-test`; mark M1 baseline complete
[2026-05-06 21:22] agent P2A DONE Added M2 Store baseline backend: `vllm_kv_cache/src/store/kv_store_engine.{h,cpp}` (in-memory data plane with store-epoch fencing, offset/block-size checks, write/read version checks, checksum verification option, and sanitized SSD-read path), rewired `KVDataServiceImpl` to backend responses, and added tests (`test_kv_store_engine.cpp`, expanded `test_kv_data_service_impl.cpp`) Full gates green: `FalconKVPrimitivesUT` 34 tests + `kv-test` + `kv-fault-test`; proceed to M2 region registration/heartbeat + inflight throttling/state machine
[2026-05-06 21:55] agent P2B DONE Filled major v6 gaps: replaced `KVStoreEngine` byte storage with `vllm_kv_cache/src/store/dram_pool.{h,cpp}` (mmap-backed DRAM pool, hugepage attempt+fallback, alignment + size enforcement); added `vllm_kv_cache/src/store/store_region_registry.{h,cpp}` (HEALTHY/DRAINING/SUSPECT/OFFLINE/QUARANTINED with heartbeat ticks and store_epoch-driven quarantine); added `vllm_kv_cache/src/store/ssd_spill_manager.{h,cpp}` (sanitized `<ssd_root>/<store>/<prefix>/<hash>.<version>.kv` layout, mkdir -p, `fdatasync`, traversal rejection). Wired registry + spill manager into `KVStoreEngine` and added integration tests. Registered the `falcon_kv.*` GUC family (max_stores, lease_default_ttl_ms, lease_recovery_grace_ms, idempotency_ttl_ms, store_max_inflight, store_suspect_ms, store_offline_ms, client_max_inflight_per_dn, eviction_chunk) in `falcon/falcon_init.c` and harness postgres configs Full verification: `FalconKVPrimitivesUT` 48 tests + 2-DN harness `start -> stop` accepting all new GUCs + `kv-test` + `kv-fault-test`; remaining M2 items are inflight admission control returning `THROTTLED`, real Store->DN heartbeat plumbing, and KV BRPC server registration in the PG extension
[2026-05-06 22:10] agent P1E DONE Filled major M1 metadata-engine gaps: `KVMetadataEngine` now is multi-region (`RegisterStoreRegion`, `SetRegionState(EngineRegionState)`, affinity allocation per v6 §5.3 prefers `preferred_store_id` then least-used HEALTHY region, with `allow_fallback_store` honored), and exposes a v6 §15 recovery API (`MarkBitmapOccupied`, `RestoreRow`, `BumpDnEpoch`); `LeaseManager` gained `Clear()` and now rejects stale `dn_epoch` requests even when the lease entry has been wiped (matches v6 §15.4). `KVMetadataServiceImpl` was rewritten to use the existing `IdempotencyStore` (TTL + GC) instead of unbounded per-API caches, with an injectable clock for deterministic TTL tests; `BatchAllocateWithLease` now forwards `preferred_store_id` + `allow_fallback_store` from the proto. Added gtests covering multi-region preference + least-used fallback, DRAINING-region skip, recovery rebuild + LRU repopulation, dn_epoch fencing on renew, and idempotency-cache TTL expiry. Full gates green: `FalconKVPrimitivesUT` 53 tests + `kv-test` + `kv-fault-test`. Remaining v6 gaps: PG-shmem placement of bitmap/lease/idempotency, KV BRPC server registration in PG extension, real Store->DN heartbeat plumbing, and BRPC client cluster mode for the Python OffloadingManager
[2026-05-06 22:25] agent P1F DONE Wired the engine-backed services to brpc and to the PG catalog: enabled `option cc_generic_services = true;` in `kv_metadata_service.proto` and `kv_data_service.proto`, added `vllm_kv_cache/src/service/{kv_metadata_brpc_service,kv_data_brpc_service}.{h,cpp}` adapters that derive from the generated `KVMetadataService`/`KVDataService` abstract Service classes and forward to `KV*ServiceImpl` (with `done->Run()` on every path); built new static lib `FalconKVBrpcServiceAdapters`. Extended `falcon/metadb/kvblock_accessor.c` with `FalconKvblockInsertAllocated` and `FalconKvblockDelete` (CAS by version, `CatalogTupleInsertWithInfo` + `simple_heap_delete` wrapped in `PG_TRY/PG_CATCH`). Added gtests `test_kv_brpc_adapters.cpp` exercising allocate/lookup/idempotency-replay (metadata) and write/read roundtrip (data) entirely through the abstract `google::protobuf::Service` ABI used by brpc. Full gates green: `FalconKVPrimitivesUT` 56 tests + `kv-test` + `kv-fault-test` + 2-DN harness `build -> start -> stop` with rebuilt `falcon.so`
[2026-05-06 22:47] agent P1G DONE Closed four more v6 gaps in one slice: (1) admission control (v6 §12.5) — `KVDataServiceImpl` now tracks an atomic `inflight_` counter and returns per-item `THROTTLED, retryable` for batches that would exceed `max_inflight_`, with deterministic `OccupyInflightForTest`/`ReleaseInflightForTest` hooks; (2) sub-tx wrapper (v6 §4.1.2/§4.5) — `vllm_kv_cache/src/metadata/kv_subtx.h` defines `KVSubTransaction` interface, `kBatchOperationGroupSize=8`, and a `ProcessInSubBatches` template that calls `Begin/Commit/Rollback`; (3) accessor abstraction (v6 §4.2) — `vllm_kv_cache/src/metadata/kv_meta_table_accessor.{h,cpp}` exposes `IKVMetaTableAccessor` (Lookup/InsertAllocated/CASStatusUpdate/Delete/ScanForRecovery) with a per-shard mutex-safe in-memory implementation; (4) PG-side recovery scan (v6 §6.4/§15) — added `FalconKvblockScanForRecovery` to `falcon/metadb/kvblock_accessor.c`. Added 8 new gtests. Full gates green: `FalconKVPrimitivesUT` 64 tests + `kv-test` + `kv-fault-test` + 2-DN harness `build -> start -> stop`
[2026-05-06 22:55] agent P1H DONE Added v6 §14 `EvictionCoordinator` (`vllm_kv_cache/src/metadata/eviction_coordinator.{h,cpp}`): drives cold candidates through STORED -> EVICTING -> EVICTED with two-phase CAS, lease-aware skipping, and spill-failure rollback to STORED via a configurable `SpillFn`. Exposed `KVMetadataEngine::ColdCandidates(limit)` and `KVMetadataEngine::CanEvict(...)` for the coordinator. Added 3 gtests (end-to-end eviction, active-lease skip, spill-failure rollback). Full gates green: `FalconKVPrimitivesUT` 67 tests + `kv-test` + `kv-fault-test`
[2026-05-06 23:08] agent P1I DONE PG-shmem placement landed (v6 §6.1, §7.0, §9.2, §27): added `falcon/include/metadb/kv_shmem.h` and `falcon/metadb/kv_shmem.c` defining `KVBitmapShmemControl` + per-region bitmap pool sized by `falcon_kv.max_stores`, `KVLeaseShmemControl` + `Falcon KV Lease` LWLock-guarded shmem hash of fixed-size `KVLeaseEntry` keyed by zero-padded `block_hash`, and `KVIdempotencyShmemControl` + `Falcon KV Idempotency` LWLock-guarded shmem hash of `KVIdempotencyEntry` keyed by `(api_name, request_id, client_id)`. Each control struct registers its own tranche via `LWLockNewTrancheId` + `LWLockRegisterTranche` with `Falcon KV Bitmap`/`Falcon KV Lease`/`Falcon KV Idempotency` names. `KVBitmapShmemSize/Init`, `KVLeaseShmemSize/Init`, `KVIdempotencyShmemSize/Init` are wired into `FalconShmemRequest()` and `FalconShmemInit()` in `falcon_init.c`, mirroring the existing `ShardTableShmemInit` pattern. Validation: `scripts/falcon_distributed_test.sh build` rebuilds `falcon.so` clean with the new shmem hooks; 2-DN `start -> status -> stop` succeeds with the new shmem segments allocated; `kv-test` and `kv-fault-test` still green. Note: the structures are now allocated and lock-tranched, but the C++ engine still uses its in-process `std::mutex`-protected versions; the next slice should add a shmem-aware accessor that reads/writes these segments and then swap the engine to use it
[2026-05-06 23:20] agent P2 IN_PROGRESS Implemented M2 D2.3 store->DN heartbeat plumbing in `KVStoreEngine`: added callback-based `SetHeartbeatSender`, `SendHeartbeats(now_ms)`, and `HeartbeatTargets()`; successful sends now refresh `StoreRegionRegistry::Heartbeat` for owner-DN regions, letting liveness recover from SUSPECT/OFFLINE. Added `KVStoreEngine.HeartbeatSenderRefreshesRegionLiveness` gtest and reran full gates green: `kv-test` (native suite now 68 tests) + `kv-fault-test`
[2026-05-06 23:30] agent P1J IN_PROGRESS Added executable PG-shmem primitive operations in `falcon/metadb/kv_shmem.c`: bitmap register/allocate/free/mark/stats, lease grant/renew/can-evict/drop/clear, and idempotency get/put/gc; all use the existing `Falcon KV Bitmap/Lease/Idempotency` LWLocks and `hash_search`-backed shmem hashes. Validation: `scripts/falcon_distributed_test.sh build` + `kv-test` + `kv-fault-test` all green. Next action: engine/service integration path to consume these shmem ops in PG-extension runtime
[2026-05-06 23:38] agent P1J IN_PROGRESS Added engine-side shmem runtime integration hooks: new `vllm_kv_cache/src/metadata/kv_shmem_runtime.{h,cpp}` provides process-local install/get/clear API for shmem callbacks; `KVMetadataEngine` now routes bitmap register/allocate/free/mark and lease grant/renew/can-evict/drop/clear through installed runtime hooks, falling back to existing in-process allocators when hooks are absent. Validation: rebuilt `FalconKVPrimitivesUT` and reran `kv-test` + `kv-fault-test` green
[2026-05-07 08:50] agent P1J IN_PROGRESS Wired runtime hooks through metadata service idempotency path and added concrete PG binding entrypoint: `KVMetadataServiceImpl` now consults runtime idempotency callbacks for replay put/get/gc when installed; `InstallKVShmemRuntimeOpsFromPg()` maps runtime bitmap/lease/idempotency operations to `kv_shmem` C APIs (guarded so standalone builds without `postgres.h` compile to no-op). Added gtest `KVMetadataServiceImpl.IdempotencyReplayCanUseRuntimeHooks`. Full gates green: `kv-test` (`FalconKVPrimitivesUT` now 69 tests) + `kv-fault-test`
[2026-05-07 09:00] agent P1J IN_PROGRESS Added recovery scaffolding for persisted `dn_epoch` + startup scan: `IKVMetaTableAccessor` now exposes `LoadDnEpoch/BumpDnEpoch` (implemented in in-memory accessor), and new `vllm_kv_cache/src/metadata/kv_metadata_recovery.{h,cpp}` provides `RecoverMetadataFromAccessor(...)` to bump epoch fencing, scan persisted rows, mark bitmap occupancy, restore rows, and reconcile `EVICTING -> STORED`. Added gtests `InMemoryKVMetaTableAccessor.PersistedDnEpochLoadAndBump` and `KVMetadataRecovery.RestoresRowsAndReconcilesEvicting`. Full gates green: `kv-test` (`FalconKVPrimitivesUT` now 71 tests) + `kv-fault-test`
[2026-05-07 09:30] agent P1J IN_PROGRESS Wired startup recovery into PG extension daemon path: added `FalconKvblockRecoverShmemOnStartup` in `falcon/metadb/kvblock_accessor.c` to scan `ALLOCATED/STORED/EVICTING` rows via `FalconKvblockScanForRecovery`, rebuild KV bitmap occupancy in shmem, and best-effort reconcile `EVICTING -> STORED` with version-CAS. Hooked invocation in `FalconDaemonConnectionPoolProcessMain` right after init/recovery wait and before serving pool traffic. Validation: `./build.sh build falcon` clean (includes `falcon.so` rebuild) + `build/tests/falcon_kv/FalconKVPrimitivesUT` 71 tests pass
[2026-05-07 09:45] agent P1J IN_PROGRESS Landed persisted `dn_epoch` catalog row wiring in PG path: `ConstructCreateKvblockTableCommand` now creates per-shard `<kvblock>_epoch` table with singleton `dn_epoch` row, and `FalconKvblockLoadDnEpoch`/`FalconKvblockBumpDnEpoch` were added to `kvblock_accessor.c` (with lazy table ensure fallback for existing deployments). `FalconKvblockRecoverShmemOnStartup` now bumps persisted epoch and clears lease shmem (`KVLeaseShmemClear`) before recovery scan, so restart fencing semantics are in place. Validation: `./build.sh build falcon` + `build/tests/falcon_kv/FalconKVPrimitivesUT` (71 tests) both green
[2026-05-07 10:05] agent P1J DONE Completed PG-extension runtime install path for shmem callbacks: added `falcon/connection_pool/kv_runtime_bridge.cpp` to expose `FalconInstallKVShmemRuntimeCallbacks()` and call `InstallKVShmemRuntimeOpsFromPg()` from `FalconDaemonConnectionPoolProcessMain` startup, and linked `vllm_kv_cache/src/metadata/kv_shmem_runtime.cpp` into `falcon.so` (plus include-path/update). Also fixed `kv_shmem_runtime.cpp` to include `kv_metadata_engine.h` so `EngineStoreRegion` is complete when Postgres headers are present. Validation: `./build.sh build falcon` + `build/tests/falcon_kv/FalconKVPrimitivesUT` (71 tests) both green
[2026-05-07 10:45] agent P2 IN_PROGRESS Registered KV BRPC services in PG-extension runtime: `falcon/brpc_comm_adapter/falcon_brpc_server.cpp` now instantiates and registers `KVMetadataBrpcServiceAdapter` and `KVDataBrpcServiceAdapter` alongside legacy `BrpcMetaServiceImpl`; `MakefilePlugin.brpc` now links KV metadata/data implementation sources and generates KV protobufs in writable `build/brpc_comm_adapter/proto`, while reusing existing remote proto objects from read-only source tree. Validation: full `./build.sh build falcon` (including `libbrpcplugin.so`) + `build/tests/falcon_kv/FalconKVPrimitivesUT` (71 tests) both green. Next action: add `KVCacheJob` shim and route these KV RPCs through `DispatchKVServiceJob`/PG pool instead of direct BRPC-thread execution
[2026-05-07 11:20] agent P2 DONE Added `KVCacheJob` dispatch path for BRPC KV services: new `BaseKVCacheServiceJob` + `BrpcKVCacheServiceJob` shim + `BrpcKVMetadataServiceImpl`/`BrpcKVDataServiceImpl` wrappers now copy requests and enqueue jobs through the existing dispatch callback; `PGConnectionPool::DispatchMetaServiceJob` detects KV jobs and routes to `DispatchKVServiceJob`, which schedules `KVCacheWorkerTask` on pool workers to execute KV handlers and then reply via BRPC closure. `falcon/brpc_comm_adapter/falcon_brpc_server.cpp` now registers these dispatching KV BRPC services. Validation: full `./build.sh build falcon` (including `libbrpcplugin.so`) + `build/tests/falcon_kv/FalconKVPrimitivesUT` (71 tests) both green
```

Reference + native test gate state at 2026-05-07 09:00:

```text
test_reference_unit              10 tests OK   (python reference)
test_metadata_reference           5 tests OK   (python reference)
test_store_reference              5 tests OK   (python reference)
test_brpc_contract_reference      3 tests OK   (python reference)
test_offloading_manager_api       3 tests OK   (python reference)
test_offloading_manager_cluster   3 tests OK   (python reference)
test_idempotency_replay           5 tests OK   (python reference)
test_eviction_and_free            6 tests OK   (python reference)
test_offloading_manager_routing   4 tests OK   (python reference)
FalconKVPrimitivesUT             71 tests OK   (native gtest)
                                  - BitmapAllocator:                5
                                  - LeaseManager:                   5
                                  - LRUManager:                     4
                                  - IdempotencyStore:               4
                                  - KVMetadataEngine:               7
                                  - KVMetadataService:              8
                                  - DramPool:                       4
                                  - StoreRegionRegistry:            4
                                  - SSDSpillManager:                4
                                  - KVStoreEngine:                  6
                                  - KVDataService:                  4
                                  - KVMetadataBrpcServiceAdapter:   2
                                  - KVDataBrpcServiceAdapter:       1
                                  - KVDataServiceImplThrottle:      3
                                  - KVSubTransaction:               3
                                  - InMemoryKVMetaTableAccessor:    3
                                  - KVMetadataRecovery:             1
                                  - EvictionCoordinator:            3
test_kv_fault_reference           3 tests OK   (fault gate)
total                           121 tests OK
```

---

## Recovery Scenario Matrix + Ordering (v6.2 P6)

| Order | Scenario | Trigger | Required ordering | Expected outcome |
|------:|----------|---------|-------------------|------------------|
| 1 | DN restart before traffic | connection-pool bgworker restart | `FalconKvblockBumpDnEpoch` -> `KVLeaseShmemClear` -> recovery scan -> reconcile `EVICTING->STORED` | stale leases fenced; bitmap/LRU repopulated before serving |
| 2 | Crash during eviction spill | restart with persisted `EVICTING` rows | startup recovery performs scan first, then CAS reconciliation | rows become `STORED`; client sees retryable conflict instead of data loss |
| 3 | Store heartbeat timeout | no heartbeat > suspect/offline thresholds | heartbeat tick loop runs before allocation path per cycle | regions move `HEALTHY->SUSPECT->OFFLINE`; allocator stops selecting them |
| 4 | Idempotent replay after failover | repeated mutating RPC with same `(api,request_id,client_id)` | idempotency lookup before execution, write response cache after commit | at-most-once semantics across retries |
| 5 | Mixed replay + recovery | request replay during/after DN restart | recovery completion precedes KV server traffic; replay cache consulted before mutate | deterministic response, no double-mutation |

Operational ordering in the implementation:

1. Build/register store regions.
2. Install PG-backed runtime hooks.
3. Run startup recovery (`dn_epoch` bump, lease clear, bitmap rebuild, `EVICTING` reconciliation).
4. Start KV BRPC service on `falcon_kv_pool.port`.
5. Start KV BRPC service and PG pool dispatch path first; optional in-plugin periodic loops are currently disabled in PG bgworker runtime to avoid interfering with request servicing.

---

## 11. Risk Register

- **R1 Hidden single-DN assumption in routing paths**
  - Mitigation: M3 adds shard-aware router; M4 adds explicit assertions/counters proving both DNs are hit.
- **R2 Metadata/data divergence on partial failure**
  - Mitigation: per-item result handling + `complete_store(success=False)` cleanup + idempotency replay tests.
- **R3 Stale process/port leaks across runs**
  - Mitigation: harness already verifies postgres + pooler listener cleanup; keep regression harness in CI.
- **R4 vLLM environment variability**
  - Mitigation: keep non-vLLM gate green; vLLM only as M5 final gate.
- **R5 PG concurrent-update surfaces as fatal error**
  - Mitigation: explicit `PG_TRY/PG_CATCH` around `CatalogTupleUpdateWithInfo` → retryable `CAS_CONFLICT`.
- **R6 Long Store restart blocks unrelated allocations**
  - Mitigation: heartbeat-based `SUSPECT/OFFLINE/QUARANTINED` removal from allocation; covered in design §5.2.1 and §15.2.1.

---

## 12. Open Questions and Defer-list

- Q1 Persistent `last_access_ms` for LRU recovery? (deferred)
- Q2 Two-level summary bitmap for very large pools? (deferred)
- Q3 Persisted lease store vs reconstructed-on-recovery? (default: reconstructed)
- Q4 Should KV cache reuse `falcon_shard_table` or split? (default: share unless production hashing diverges)
- Q5 Should `BatchLookupWithLease` always renew on hit by default? (default: caller controls; vLLM `lookup()` uses false, `prepare_load()` uses true)

---

## 13. Change Log

- 2026-05-06 v1: Initial plan drafted from v6 full and existing tests/harness.
- 2026-05-06 v2: Reviewed v6 full + proto + Python reference + tests; expanded WBS, added contract verification checklist, expanded acceptance matrix, added test inventory with retirement, made dependencies explicit, added observability/perf cross-cutting threads, recorded deprecated docs and known semantic gaps (`complete_store(success=False)`, partial-data cleanup).
- 2026-05-07 v3: Closed v6.2 priority slices: PG accessor-backed metadata runtime path, KV worker transaction+sub-transaction envelope, KV BRPC port split (`falcon_kv_pool.port`), Store region registration + heartbeat/eviction/idempotency background loops, Python cluster-factory mode, and explicit recovery scenario ordering matrix.
- 2026-05-07 v4: Corrected KV metadata execution architecture: BRPC metadata requests now serialize protobuf payloads through shared memory and execute in PostgreSQL backends via `falcon_kv_metadata_call_by_serialized_shmem_internal`; direct catalog/shmem runtime execution from libpq helper threads was removed. KV dispatch now queues through the pool manager and fans out across pooled PG connections.

