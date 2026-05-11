/* Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 *
 * POD wire format for the v6.4 KV catalog sub-batch protocol. The same header
 * is included from both falcon.so (PG SPI accessor side) and libbrpcplugin.so
 * (BRPC plugin side) so the two ends agree on layout.
 *
 * Wire-trip shape (matches FalconFS BatchWorkerTask::DoWork pattern, v6.4 \u00a74.1):
 *   Plugin packs ALL items of a sub-batch into one contiguous buffer with the
 *   layout described below, sends it as a single `bytea` parameter to
 *   `falcon_kv_metadata_catalog_call(method int, payload bytea)`, gets back
 *   one `bytea` response containing the matching result array. The PG backend
 *   never parses protobuf; it operates on these POD structs directly via
 *   `table_open` / `systable_beginscan` / `heap_modify_tuple` /
 *   `CatalogTupleUpdateWithInfo` / `simple_heap_delete`.
 *
 * Request layout:    [uint32_t count] [Item       * count]
 * Response layout:   [uint32_t count] [Result     * count]
 *
 * Each Item / Result struct below has a fixed size and is packed (no implicit
 * padding) so the wire format is deterministic across compilers.
 */
#ifndef KV_CATALOG_WIRE_H
#define KV_CATALOG_WIRE_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define KV_CATALOG_BLOCK_HASH_MAX_LEN   64
#define KV_CATALOG_EVICTED_PATH_MAX_LEN 256

/* Method enum (matches v6.4 \u00a74.1.1 method mapping; numeric values are stable). */
typedef enum KVCatalogMethod {
    KV_CATALOG_METHOD_LOOKUP            = 1,
    KV_CATALOG_METHOD_INSERT_ALLOCATED  = 2,
    KV_CATALOG_METHOD_CAS_STATUS_UPDATE = 3,
    KV_CATALOG_METHOD_DELETE            = 4,
} KVCatalogMethod;

/* BlockStatus mirror (v6.4 \u00a73.2). 0 must never be persisted. */
#define KV_CATALOG_STATUS_UNSPECIFIED 0
#define KV_CATALOG_STATUS_ALLOCATED   1
#define KV_CATALOG_STATUS_STORED      2
#define KV_CATALOG_STATUS_EVICTING    3
#define KV_CATALOG_STATUS_EVICTED     4
#define KV_CATALOG_STATUS_FAILED      5

/* ---- Lookup ------------------------------------------------------------ */

typedef struct __attribute__((packed)) KVCatalogLookupItem {
    uint16_t block_hash_len;
    uint8_t  block_hash[KV_CATALOG_BLOCK_HASH_MAX_LEN];
} KVCatalogLookupItem;

typedef struct __attribute__((packed)) KVCatalogLookupResult {
    uint8_t  found;             /* 0 or 1 */
    uint8_t  status;            /* KV_CATALOG_STATUS_* */
    int32_t  store_node_id;
    int64_t  pool_offset;
    int64_t  version;
    int64_t  updated_at_ms;
    uint16_t evicted_path_len;
    char     evicted_path[KV_CATALOG_EVICTED_PATH_MAX_LEN];
} KVCatalogLookupResult;

/* ---- InsertAllocated --------------------------------------------------- */

typedef struct __attribute__((packed)) KVCatalogInsertItem {
    uint16_t block_hash_len;
    uint8_t  block_hash[KV_CATALOG_BLOCK_HASH_MAX_LEN];
    int32_t  kv_group_idx;
    int32_t  layer_mask;
    int32_t  store_node_id;
    int64_t  pool_offset;
    int64_t  now_ms;
} KVCatalogInsertItem;

typedef struct __attribute__((packed)) KVCatalogInsertResult {
    uint8_t  inserted;          /* 0 = duplicate-key conflict, 1 = inserted */
} KVCatalogInsertResult;

/* ---- CAS Status Update ------------------------------------------------- */

typedef struct __attribute__((packed)) KVCatalogCASItem {
    uint16_t block_hash_len;
    uint8_t  block_hash[KV_CATALOG_BLOCK_HASH_MAX_LEN];
    int32_t  expected_from_status;
    int32_t  to_status;
    int64_t  expected_version;
    int64_t  now_ms;
    uint8_t  has_evicted_path;  /* 0 or 1 */
    uint16_t evicted_path_len;
    char     evicted_path[KV_CATALOG_EVICTED_PATH_MAX_LEN];
} KVCatalogCASItem;

typedef struct __attribute__((packed)) KVCatalogCASResult {
    uint8_t  success;
    uint8_t  not_found;
    uint8_t  conflict;
    int32_t  current_status;
    int64_t  current_version;
} KVCatalogCASResult;

/* ---- Delete ------------------------------------------------------------ */

typedef struct __attribute__((packed)) KVCatalogDeleteItem {
    uint16_t block_hash_len;
    uint8_t  block_hash[KV_CATALOG_BLOCK_HASH_MAX_LEN];
    int64_t  expected_version;  /* 0 = no CAS check */
} KVCatalogDeleteItem;

typedef struct __attribute__((packed)) KVCatalogDeleteResult {
    uint8_t  success;
    uint8_t  conflict;
} KVCatalogDeleteResult;

/* Helper: compute response buffer size for a given method and item count. */
static inline uint64_t KVCatalogResponseSize(int method, uint32_t count)
{
    uint64_t header = sizeof(uint32_t);
    switch (method) {
        case KV_CATALOG_METHOD_LOOKUP:
            return header + (uint64_t) count * sizeof(KVCatalogLookupResult);
        case KV_CATALOG_METHOD_INSERT_ALLOCATED:
            return header + (uint64_t) count * sizeof(KVCatalogInsertResult);
        case KV_CATALOG_METHOD_CAS_STATUS_UPDATE:
            return header + (uint64_t) count * sizeof(KVCatalogCASResult);
        case KV_CATALOG_METHOD_DELETE:
            return header + (uint64_t) count * sizeof(KVCatalogDeleteResult);
        default:
            return 0;
    }
}

#ifdef __cplusplus
}
#endif

#endif  /* KV_CATALOG_WIRE_H */
