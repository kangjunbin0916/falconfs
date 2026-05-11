/* Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 *
 * v6 §15.1 DN-restart recovery driver. At plugin startup, before the BRPC
 * server begins serving traffic, this runner:
 *
 *   1. Opens a libpq connection back to the local PG.
 *   2. Calls `pg_catalog.falcon_kv_metadata_recovery_call(BUMP_DN_EPOCH, ...)`
 *      to bump the persisted dn_epoch — every lease token issued before the
 *      restart is now stale (v6 §15.4).
 *   3. Calls `pg_catalog.falcon_kv_metadata_recovery_call(SCAN_FOR_RECOVERY, ...)`
 *      and feeds every persisted row into `RecoverMetadataFromAccessor` so the
 *      DRAM bitmap, meta slots, and shard hash index are repopulated with
 *      grace leases (v6 §15.4).
 *   4. Reconciles `EVICTING -> STORED` per row (v6 §15.3) — the engine's
 *      RestoreRow already maps EVICTING to STORED at the in-process layer;
 *      the libpq recovery accessor also rewrites the persisted row via
 *      catalog CAS so the catalog matches the in-memory state.
 *
 * The runner is synchronous: BRPC traffic does not start until Run() returns.
 * Recovery on a freshly initialised cluster is a no-op (zero rows) and adds
 * only the dn_epoch round-trip.
 */
#ifndef KV_RECOVERY_RUNNER_H
#define KV_RECOVERY_RUNNER_H

#include <cstdint>
#include <memory>

namespace falconfs::kv {
class KVMetadataEngine;
}  // namespace falconfs::kv

namespace falcon::kv_proto {

struct KVRecoveryStats {
    bool ok = false;
    int64_t bumped_dn_epoch = 0;
    int64_t recovered_rows = 0;
    int64_t reconciled_evicting = 0;
};

class KVRecoveryRunner {
public:
    KVRecoveryRunner(std::shared_ptr<::falconfs::kv::KVMetadataEngine> engine,
                     int pg_port,
                     int32_t shard_id);

    // Synchronous: returns once the engine state is fully repopulated.
    KVRecoveryStats Run();

private:
    std::shared_ptr<::falconfs::kv::KVMetadataEngine> engine_;
    int pg_port_;
    int32_t shard_id_;
};

}  // namespace falcon::kv_proto

#endif  // KV_RECOVERY_RUNNER_H
