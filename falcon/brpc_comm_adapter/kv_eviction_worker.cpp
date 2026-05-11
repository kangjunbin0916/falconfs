/* Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "brpc_comm_adapter/kv_eviction_worker.h"

#include <libpq-fe.h>

#include <chrono>
#include <cstdlib>
#include <pwd.h>
#include <sstream>
#include <string>
#include <unistd.h>

#include "brpc_comm_adapter/kv_runtime_register.h"
#include "kv_metadata_service.pb.h"
#include "vllm_kv_cache/src/metadata/eviction_coordinator.h"
#include "vllm_kv_cache/src/metadata/kv_metadata_engine.h"
#include "vllm_kv_cache/src/metadata/kv_metadata_service_impl.h"
#include "vllm_kv_cache/src/store/kv_store_engine.h"

extern "C" {
/* falcon.so registers and assigns these GUCs (`falcon_init.c`); the plugin
 * reads them from inside libbrpcplugin.so. They are exported with default
 * visibility so dlopen()-ed plugins can resolve the symbols. */
extern int FalconKvEvictionPeriodMs;
extern int FalconKvEvictionLowWatermarkPct;
extern int FalconKvEvictionChunk;
}

namespace falcon::kv_proto {

namespace {

constexpr int kDefaultEvictionPeriodMs = 200;
constexpr int kDefaultEvictionLowWatermarkPct = 10;
constexpr int kDefaultEvictionChunk = 64;

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

PGconn *OpenWorkerConnection(int pg_port)
{
    if (pg_port <= 0) return nullptr;
    std::ostringstream conninfo;
    conninfo << "hostaddr=127.0.0.1 port=" << pg_port << " user=" << GetCurrentUserName()
             << " dbname=postgres application_name=falcon_kv_eviction";
    PGconn *conn = PQconnectdb(conninfo.str().c_str());
    if (PQstatus(conn) != CONNECTION_OK) {
        PQfinish(conn);
        return nullptr;
    }
    return conn;
}

int64_t NowMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch()).count();
}

int Clamp(int v, int lo, int hi)
{
    if (v < lo) return lo;
    if (v > hi) return hi;
    return v;
}

}  // namespace

KVEvictionWorker::KVEvictionWorker(std::shared_ptr<::falconfs::kv::KVMetadataEngine> engine,
                                   std::shared_ptr<::falconfs::kv::KVMetadataServiceImpl> impl,
                                   std::shared_ptr<::falconfs::kv::KVStoreEngine> store_engine,
                                   int pg_port)
    : engine_(std::move(engine)),
      impl_(std::move(impl)),
      store_engine_(std::move(store_engine)),
      pg_port_(pg_port)
{
}

KVEvictionWorker::~KVEvictionWorker()
{
    Stop();
}

void KVEvictionWorker::Start()
{
    if (thread_.joinable()) return;
    stop_.store(false, std::memory_order_release);
    thread_ = std::thread(&KVEvictionWorker::Loop, this);
}

void KVEvictionWorker::Stop()
{
    if (!thread_.joinable()) return;
    {
        std::lock_guard<std::mutex> lk(mu_);
        stop_.store(true, std::memory_order_release);
    }
    cv_.notify_all();
    thread_.join();
    if (pg_conn_ != nullptr) {
        PQfinish(static_cast<PGconn *>(pg_conn_));
        pg_conn_ = nullptr;
    }
}

void KVEvictionWorker::Loop()
{
    using ::falconfs::kv::EngineUpdateStatusResult;
    using ::falconfs::kv::EvictionCoordinator;
    using ::falconfs::kv::EvictionConfig;
    using ::falconfs::kv::EvictionCycleResult;
    using ::falconfs::kv::ItemResultMeta;
    using ::falconfs::kv::ErrorCode;

    pg_conn_ = OpenWorkerConnection(pg_port_);

    /* StatusUpdateFn: drives a single-item ALLOCATED/STORED/EVICTING/EVICTED
     * CAS through the same SplitForPoolWorker path the BRPC handler uses, so
     * DRAM Pass-1 + catalog Pass-2 semantics match exactly. */
    auto status_update_fn =
        [this](const std::string &block_hash, int32_t expected_from_status, int32_t to_status,
               int64_t expected_version, const std::string &evicted_path,
               bool allow_noop_if_already_target, int64_t now_ms) -> EngineUpdateStatusResult {
        EngineUpdateStatusResult err_out;
        err_out.result.success = false;
        err_out.result.error_code = static_cast<int32_t>(ErrorCode::INTERNAL_ERROR);
        err_out.result.retryable = true;
        err_out.result.error_message = "kv_eviction_worker: catalog update failed";
        err_out.new_version = expected_version;
        err_out.current_status = expected_from_status;

        if (impl_ == nullptr || pg_conn_ == nullptr) return err_out;

        ::falconfs::kv::BatchUpdateStatusRequest req;
        req.mutable_meta()->set_request_id("kv_eviction_worker_cas");
        req.mutable_meta()->set_client_id(0);
        auto *it = req.add_items();
        it->set_block_hash(block_hash);
        it->set_expected_from_status(static_cast<::falconfs::kv::BlockStatus>(expected_from_status));
        it->set_to_status(static_cast<::falconfs::kv::BlockStatus>(to_status));
        it->set_expected_version(expected_version);
        it->set_evicted_path(evicted_path);
        it->set_allow_noop_if_already_target(allow_noop_if_already_target);

        ::falconfs::kv::BatchUpdateStatusResponse resp;
        void *conn = pg_conn_;
        impl_->BatchUpdateBlockStatusSplitForPoolWorker(
            req, &resp,
            [conn](const std::string &payload) {
                return KVRuntimeRegister::CatalogCASStatusUpdateOnConn(conn, payload);
            });

        if (resp.results_size() != 1) return err_out;
        const auto &r = resp.results(0);
        EngineUpdateStatusResult out;
        out.result.success = r.result().success();
        out.result.error_code = static_cast<int32_t>(r.result().error_code());
        out.result.retryable = r.result().retryable();
        out.result.error_message = r.result().error_message();
        out.new_version = r.new_version();
        out.current_status = static_cast<int32_t>(r.current_status());
        (void)now_ms;
        return out;
    };

    /* SpillFn: in-process Store engine writes the DRAM block to SSD. */
    auto spill_fn =
        [this](const std::string &block_hash, int64_t version) -> EvictionCoordinator::SpillOutcome {
        EvictionCoordinator::SpillOutcome out;
        if (store_engine_ == nullptr) {
            return out;
        }
        std::string evicted_path;
        ::falconfs::kv::StoreWriteResult sp =
            store_engine_->SpillBlockToSSD(block_hash, version, &evicted_path);
        out.ok = sp.result.success;
        out.evicted_path = std::move(evicted_path);
        return out;
    };

    EvictionCoordinator coord(engine_, std::move(spill_fn), std::move(status_update_fn));

    // v6 §14: when no SSD spill manager is configured (typical for the
    // single-host harness today), every spill returns INTERNAL_ERROR and the
    // coordinator just rolls the row back to STORED, churning catalog
    // versions for no gain and racing with concurrent free()s. Detect once
    // at startup and short-circuit the loop into pure idle waits in that
    // case; if a spill manager is later attached the bgworker will need to
    // be restarted to pick it up (acceptable for this milestone).
    const bool spill_capable = store_engine_ && store_engine_->HasSpillManager();

    while (!stop_.load(std::memory_order_acquire)) {
        const int chunk = Clamp(FalconKvEvictionChunk > 0
                                    ? FalconKvEvictionChunk
                                    : kDefaultEvictionChunk,
                                1, 1024);
        const int low_pct = Clamp(FalconKvEvictionLowWatermarkPct > 0
                                      ? FalconKvEvictionLowWatermarkPct
                                      : kDefaultEvictionLowWatermarkPct,
                                  0, 100);
        const int period_ms = Clamp(FalconKvEvictionPeriodMs > 0
                                        ? FalconKvEvictionPeriodMs
                                        : kDefaultEvictionPeriodMs,
                                    50, 60000);

        const double low_watermark = static_cast<double>(low_pct) / 100.0;
        const double free_ratio = engine_ ? engine_->MinRegionFreeRatio() : 1.0;

        if (spill_capable) {
            EvictionConfig cfg;
            cfg.now_ms = NowMs();
            /* Aggressive mode: drop below watermark => use the configured
             * chunk, scan immediately. Steady mode: still scan periodically
             * with the same chunk to keep CLOCK ref bits ticking. */
            cfg.max_per_cycle = chunk;
            coord.RunOneCycle(cfg);

            /* If we're way below the watermark, skip the period wait and
             * run another cycle immediately. */
            if (free_ratio < low_watermark / 2.0) {
                continue;
            }
        }

        std::unique_lock<std::mutex> lk(mu_);
        cv_.wait_for(lk, std::chrono::milliseconds(period_ms),
                     [this]() { return stop_.load(std::memory_order_acquire); });
    }
}

}  // namespace falcon::kv_proto
