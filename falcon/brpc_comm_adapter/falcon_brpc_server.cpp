/* Copyright (c) 2025 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "brpc_comm_adapter/falcon_brpc_server.h"

#include <brpc/server.h>
#include <libpq-fe.h>
#include <cstdlib>
#include <functional>
#include <memory>
#include <pwd.h>
#include <sstream>
#include <unistd.h>

#include "base_comm_adapter/base_meta_service_job.h"
#include "brpc_comm_adapter/brpc_kv_service_imp.h"
#include "brpc_comm_adapter/brpc_meta_service_imp.h"
#include "brpc_comm_adapter/kv_eviction_worker.h"
#include "brpc_comm_adapter/kv_recovery_runner.h"
#include "brpc_comm_adapter/kv_runtime_register.h"
#include "vllm_kv_cache/src/metadata/kv_metadata_engine.h"
#include "vllm_kv_cache/src/metadata/kv_metadata_service_impl.h"
#include "vllm_kv_cache/src/store/kv_data_service_impl.h"
#include "vllm_kv_cache/src/store/kv_store_engine.h"

extern "C" {
extern int FalconPGPort;
}

namespace {

constexpr int32_t kKvShardId = 1;

std::string GetCurrentUserName()
{
    const char *u = std::getenv("USER");
    if (u != nullptr && u[0] != '\0') return u;
    u = std::getenv("PGUSER");
    if (u != nullptr && u[0] != '\0') return u;
    struct passwd *pw = getpwuid(getuid());
    if (pw != nullptr && pw->pw_name != nullptr) return pw->pw_name;
    return "postgres";
}

/* One-time bootstrap: open a temporary libpq connection from the bgworker and
 * call `pg_catalog.falcon_create_kvblock_table()` so the schema exists before
 * any pool-worker accepts a KV catalog round-trip. The connection is closed
 * immediately afterwards; per-worker libpq connections in PGConnectionPool
 * handle the actual hot path. */
void EnsureKvblockTableOnce(int pg_port)
{
    if (pg_port <= 0) return;
    std::ostringstream conninfo;
    conninfo << "hostaddr=127.0.0.1 port=" << pg_port << " user=" << GetCurrentUserName()
             << " dbname=postgres";
    PGconn *conn = PQconnectdb(conninfo.str().c_str());
    if (PQstatus(conn) != CONNECTION_OK) {
        PQfinish(conn);
        return;
    }
    PGresult *res = PQexec(conn, "SELECT pg_catalog.falcon_create_kvblock_table()");
    PQclear(res);
    PQfinish(conn);
}

}  // namespace

class FalconBrpcServer {
  public:
    FalconBrpcServer(falcon_meta_job_dispatch_func dispatchFun, const char *serverIp, int port)
        : m_jobDispatchFunc(dispatchFun),
          m_serverIp(serverIp ? serverIp : ""),
          m_port(port)
    {
    }

    void Run()
    {
        falcon::meta_proto::BrpcMetaServiceImpl BrpcMetaServiceImpl(m_jobDispatchFunc);
        if (m_server.AddService(&BrpcMetaServiceImpl, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            throw std::runtime_error("FalconBrpcServer: brpc server AddService failed");
        }

        /* v6.4 \u00a74.1.1: build the shared engine + service impl. The engine is in
         * REMOTE_LIBPQ tier so any inline catalog read/write asserts; all
         * catalog work goes through the SplitForPoolWorker callback that the
         * plugin-registered process-job impl wires to the pool worker's libpq
         * connection. */
        m_kvMetadataEngine = std::make_shared<falconfs::kv::KVMetadataEngine>(
            /*store_node_id=*/1,
            /*dn_id=*/1,
            /*region_bytes=*/64LL * 65536LL,
            /*block_size=*/65536,
            /*dn_epoch=*/1,
            /*store_epoch=*/1,
            /*kvblock_shard_id=*/kKvShardId);
        m_kvMetadataEngine->SetCatalogTier(falconfs::kv::EngineCatalogTier::REMOTE_LIBPQ);
        m_kvMetadataImpl = std::make_shared<falconfs::kv::KVMetadataServiceImpl>(m_kvMetadataEngine);

        /* Make sure the kvblock catalog table exists before serving requests. */
        EnsureKvblockTableOnce(FalconPGPort);

        /* v6 \u00a715.1 DN-restart recovery: bump dn_epoch (rejects stale leases)
         * and rehydrate the in-memory bitmap + meta arrays from persisted
         * `pg_catalog.falcon_kvblock_table` rows BEFORE we register the runtime
         * bridge or start BRPC. By the time clients can reach this DN, every
         * persisted row already has a DRAM mirror with a fresh grace lease. */
        falcon::kv_proto::KVRecoveryRunner recoveryRunner(m_kvMetadataEngine, FalconPGPort,
                                                          kKvShardId);
        (void) recoveryRunner.Run();

        /* Register the engine with the falcon.so runtime bridge so connection
         * pool workers can route KV jobs through it. */
        falcon::kv_proto::KVRuntimeRegister::Install(m_kvMetadataImpl);

        m_kvStoreEngine = std::make_shared<falconfs::kv::KVStoreEngine>();
        m_kvDataImpl = std::make_shared<falconfs::kv::KVDataServiceImpl>(m_kvStoreEngine);

        /* v6 \u00a714 background eviction. Owns its own libpq connection so it does
         * not contend with pool-worker traffic for catalog round-trips. */
        m_kvEvictionWorker = std::make_unique<falcon::kv_proto::KVEvictionWorker>(
            m_kvMetadataEngine, m_kvMetadataImpl, m_kvStoreEngine, FalconPGPort);
        m_kvEvictionWorker->Start();

        falcon::kv_proto::BrpcKVMetadataServiceImpl kvMetadataBrpcService(m_jobDispatchFunc);
        falcon::kv_proto::BrpcKVDataServiceImpl kvDataBrpcService(m_kvDataImpl);
        if (m_server.AddService(&kvMetadataBrpcService, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            throw std::runtime_error("FalconBrpcServer: brpc server AddService(KV metadata) failed");
        }
        if (m_server.AddService(&kvDataBrpcService, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            throw std::runtime_error("FalconBrpcServer: brpc server AddService(KV data) failed");
        }

        butil::ip_t brpcServerIp;
        int ret = butil::str2ip(m_serverIp.c_str(), &brpcServerIp);
        if (ret != 0) {
            printf("FalconBrpcServer: failed to convert %s to brpc ip_t type, using 127.0.0.1 as server ip.",
                   m_serverIp.c_str());
            brpcServerIp = butil::IP_ANY;
        } else {
            printf("FalconBrpcServer: convert %s to brpc ip_t type success, using it as server ip.",
                   m_serverIp.c_str());
        }

        butil::EndPoint point;
        point = butil::EndPoint(brpcServerIp, m_port);
        brpc::ServerOptions options;
        if (m_server.Start(point, &options) != 0) {
            throw std::runtime_error("FalconBrpcServer: failed to start server.");
        }

        m_server.RunUntilAskedToQuit();
    }

    void Shutdown()
    {
        if (m_kvEvictionWorker) {
            m_kvEvictionWorker->Stop();
            m_kvEvictionWorker.reset();
        }
        m_server.Stop(0);
        m_server.Join();
        falcon::kv_proto::KVRuntimeRegister::Uninstall();
    }

  private:
    falcon_meta_job_dispatch_func m_jobDispatchFunc;
    std::string m_serverIp;
    int m_port;
    brpc::Server m_server;

    std::shared_ptr<falconfs::kv::KVMetadataEngine> m_kvMetadataEngine;
    std::shared_ptr<falconfs::kv::KVMetadataServiceImpl> m_kvMetadataImpl;
    std::shared_ptr<falconfs::kv::KVStoreEngine> m_kvStoreEngine;
    std::shared_ptr<falconfs::kv::KVDataServiceImpl> m_kvDataImpl;
    std::unique_ptr<falcon::kv_proto::KVEvictionWorker> m_kvEvictionWorker;
};

static std::unique_ptr<FalconBrpcServer> g_falconBrpcServerInstance = NULL;
int StartFalconCommunicationServer(falcon_meta_job_dispatch_func dispatchFunc, const char *serverIp, int serverListenPort)
{
    try {
        if (g_falconBrpcServerInstance == NULL) {
            g_falconBrpcServerInstance = std::make_unique<FalconBrpcServer>(dispatchFunc, serverIp, serverListenPort);
            g_falconBrpcServerInstance->Run();
            return true;
        }
    } catch (const std::runtime_error &e) {
        printf("%s", e.what());
        fflush(stdout);
        return 1;
    }
    return 0;
}

int StopFalconCommunicationServer()
{
    try {
        if (g_falconBrpcServerInstance != NULL) {
            g_falconBrpcServerInstance->Shutdown();
            g_falconBrpcServerInstance = NULL;
            return 0;
        }
    } catch (const std::exception &e) {
        printf("%s", e.what());
        fflush(stdout);
        return 1;
    }
    return 1;
}
