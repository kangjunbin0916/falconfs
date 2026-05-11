/* Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 *
 * Plugin-side KV runtime registration. At plugin startup the BRPC server calls
 * `KVRuntimeRegister::Install(engine_impl)` once. Install() forwards a single
 * extern "C" callback to falcon.so via `FalconKVSetProcessJob`. The callback
 * runs on a connection-pool worker thread and uses the worker's libpq
 * connection for the per-sub-batch catalog round-trip.
 */
#ifndef KV_RUNTIME_REGISTER_H
#define KV_RUNTIME_REGISTER_H

#include <memory>

#include "vllm_kv_cache/src/metadata/kv_metadata_service_impl.h"

namespace falcon::kv_proto {

class KVRuntimeRegister {
public:
    static void Install(std::shared_ptr<::falconfs::kv::KVMetadataServiceImpl> impl);
    static void Uninstall();
    static std::shared_ptr<::falconfs::kv::KVMetadataServiceImpl> GetImpl();

    /* Ensures `falcon_kvblock_table` exists by calling
     * `pg_catalog.falcon_create_kvblock_table()` on the supplied connection. */
    static bool EnsureKvblockTableOnConn(void *pg_conn_opaque);
};

}  // namespace falcon::kv_proto

#endif  // KV_RUNTIME_REGISTER_H
