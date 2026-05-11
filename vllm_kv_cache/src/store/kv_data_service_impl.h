// Compile-ready Store data service for KV cache v6 contracts.
//
// Implements the contract from kv_data_service.proto on top of `KVStoreEngine`
// and an inflight admission counter (v6 §12.5). When the inflight counter
// would exceed `max_inflight_`, all items in the incoming batch are returned
// as `THROTTLED, retryable=true`, matching the v6 §10.4 error model.
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>

#include "kv_data_service.pb.h"
#include "vllm_kv_cache/src/store/kv_store_engine.h"

namespace falconfs::kv {

class KVDataServiceImpl {
public:
    KVDataServiceImpl();
    explicit KVDataServiceImpl(std::shared_ptr<KVStoreEngine> engine);

    void BatchWriteBlock(const BatchWriteBlockRequest& request, BatchWriteBlockResponse* response);
    void BatchReadBlock(const BatchReadBlockRequest& request, BatchReadBlockResponse* response);
    void BatchReadFromSSD(const BatchReadFromSSDRequest& request,
                          BatchReadFromSSDResponse* response);

    // Admission control knobs. `max_inflight=0` means unlimited (default).
    void SetMaxInflightForTest(int max_inflight);
    int CurrentInflight() const;

    // Test hook to simulate concurrent in-flight requests.
    void OccupyInflightForTest(int n);
    void ReleaseInflightForTest(int n);

private:
    static int64_t NowMs();
    static void FillResultMeta(const StoreResultMeta& src, ItemResultMeta* dst);

    template <typename Request, typename Response>
    bool TryAdmit(const Request& request, Response* response);

    void ReleaseAdmission(int items);

    template <typename Response>
    void FillThrottled(int n_items, const std::string* hashes, Response* response);

    std::shared_ptr<KVStoreEngine> engine_;
    std::atomic<int> inflight_{0};
    int max_inflight_ = 0;
};

}  // namespace falconfs::kv
