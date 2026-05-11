#include "vllm_kv_cache/src/store/kv_data_service_impl.h"

#include <chrono>
#include <utility>

namespace falconfs::kv {

KVDataServiceImpl::KVDataServiceImpl()
    : engine_(std::make_shared<KVStoreEngine>()) {}

KVDataServiceImpl::KVDataServiceImpl(std::shared_ptr<KVStoreEngine> engine)
    : engine_(std::move(engine)) {}

int64_t KVDataServiceImpl::NowMs() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

void KVDataServiceImpl::FillResultMeta(const StoreResultMeta& src, ItemResultMeta* dst) {
    dst->set_success(src.success);
    dst->set_error_code(static_cast<ErrorCode>(src.error_code));
    dst->set_retryable(src.retryable);
    dst->set_error_message(src.error_message);
}

void KVDataServiceImpl::SetMaxInflightForTest(int max_inflight) { max_inflight_ = max_inflight; }
int KVDataServiceImpl::CurrentInflight() const { return inflight_.load(); }
void KVDataServiceImpl::OccupyInflightForTest(int n) { inflight_.fetch_add(n); }
void KVDataServiceImpl::ReleaseInflightForTest(int n) { inflight_.fetch_sub(n); }

namespace {
template <typename ResultMsg>
void SetThrottledOnItem(ResultMsg* result) {
    auto* m = result->mutable_result();
    m->set_success(false);
    m->set_error_code(ErrorCode::THROTTLED);
    m->set_retryable(true);
    m->set_error_message("store inflight queue full");
}
}  // namespace

template <typename Request, typename Response>
bool KVDataServiceImpl::TryAdmit(const Request& request, Response* response) {
    const int n_items = request.items_size();
    if (n_items <= 0) return true;
    if (max_inflight_ <= 0) {
        inflight_.fetch_add(n_items);
        return true;
    }
    const int prev = inflight_.fetch_add(n_items);
    if (prev + n_items > max_inflight_) {
        inflight_.fetch_sub(n_items);
        response->clear_results();
        for (const auto& item : request.items()) {
            auto* r = response->add_results();
            r->set_block_hash(item.block_hash());
            SetThrottledOnItem(r);
        }
        response->set_server_time_ms(NowMs());
        return false;
    }
    return true;
}

void KVDataServiceImpl::ReleaseAdmission(int items) {
    if (items > 0) inflight_.fetch_sub(items);
}

void KVDataServiceImpl::BatchWriteBlock(const BatchWriteBlockRequest& request,
                                        BatchWriteBlockResponse* response) {
    if (!TryAdmit(request, response)) return;
    response->clear_results();
    const int64_t now_ms = NowMs();
    for (const auto& item : request.items()) {
        auto* result = response->add_results();
        result->set_block_hash(item.block_hash());
        StoreWriteResult out = engine_->Write(item.block_hash(),
                                              item.pool_offset(),
                                              item.payload(),
                                              static_cast<int32_t>(item.compression()),
                                              item.original_size(),
                                              item.block_size(),
                                              item.expected_version(),
                                              item.expected_store_epoch(),
                                              request.verify_checksum(),
                                              item.crc32());
        FillResultMeta(out.result, result->mutable_result());
        result->set_bytes_written(out.bytes_written);
        result->set_crc32(out.crc32);
    }
    response->set_server_time_ms(now_ms);
    ReleaseAdmission(request.items_size());
}

void KVDataServiceImpl::BatchReadBlock(const BatchReadBlockRequest& request,
                                       BatchReadBlockResponse* response) {
    if (!TryAdmit(request, response)) return;
    response->clear_results();
    const int64_t now_ms = NowMs();
    for (const auto& item : request.items()) {
        auto* result = response->add_results();
        result->set_block_hash(item.block_hash());
        StoreReadResult out = engine_->Read(item.block_hash(),
                                            item.pool_offset(),
                                            item.block_size(),
                                            item.expected_version(),
                                            item.expected_store_epoch());
        FillResultMeta(out.result, result->mutable_result());
        result->set_payload(out.payload);
        result->set_crc32(out.crc32);
        result->set_compression(static_cast<CompressionType>(out.compression));
        result->set_original_size(out.original_size);
    }
    response->set_server_time_ms(now_ms);
    ReleaseAdmission(request.items_size());
}

void KVDataServiceImpl::BatchReadFromSSD(const BatchReadFromSSDRequest& request,
                                         BatchReadFromSSDResponse* response) {
    if (!TryAdmit(request, response)) return;
    response->clear_results();
    const int64_t now_ms = NowMs();
    for (const auto& item : request.items()) {
        auto* result = response->add_results();
        result->set_block_hash(item.block_hash());
        StoreReadResult out = engine_->ReadFromSSD(item.block_hash(),
                                                   item.evicted_path(),
                                                   item.expected_version());
        FillResultMeta(out.result, result->mutable_result());
        result->set_payload(out.payload);
        result->set_crc32(out.crc32);
        result->set_compression(static_cast<CompressionType>(out.compression));
        result->set_original_size(out.original_size);
    }
    response->set_server_time_ms(now_ms);
    ReleaseAdmission(request.items_size());
}

}  // namespace falconfs::kv
