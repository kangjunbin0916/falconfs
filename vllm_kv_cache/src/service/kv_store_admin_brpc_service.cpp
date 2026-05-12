#include "vllm_kv_cache/src/service/kv_store_admin_brpc_service.h"

#include <functional>
#include <string>

#include "kv_common.pb.h"
#include "kv_store_admin_service.pb.h"
#include "vllm_kv_cache/src/metadata/kv_metadata_engine.h"
#include "vllm_kv_cache/src/store/kv_store_engine.h"

namespace falconfs::kv {

namespace {

void FillItemOk(ItemResultMeta* m)
{
    m->set_success(true);
    m->set_error_code(ErrorCode::OK);
    m->set_retryable(false);
    m->clear_error_message();
}

void FillItemErr(ItemResultMeta* m, ErrorCode code, bool retryable, const char* msg)
{
    m->set_success(false);
    m->set_error_code(code);
    m->set_retryable(retryable);
    if (msg != nullptr) {
        m->set_error_message(msg);
    } else {
        m->clear_error_message();
    }
}

void FillFromEngineMeta(const EngineResultMeta& src, ItemResultMeta* dst)
{
    dst->set_success(src.success);
    dst->set_error_code(static_cast<ErrorCode>(src.error_code));
    dst->set_retryable(src.retryable);
    dst->set_error_message(src.error_message);
}

class KVStoreAdminBrpcServiceImplDN final : public KVStoreAdminBrpcServiceImplBase {
public:
    explicit KVStoreAdminBrpcServiceImplDN(std::shared_ptr<KVMetadataEngine> engine,
                                           std::function<void()> after_region_registered)
        : engine_(std::move(engine)), after_region_registered_(std::move(after_region_registered)) {}

    void RegisterStoreRegion(const RegisterStoreRegionRequest& req,
                             RegisterStoreRegionResponse* resp) override {
        if (engine_ == nullptr) {
            FillItemErr(resp->mutable_result(), ErrorCode::INTERNAL_ERROR, false, "metadata engine null");
            return;
        }
        const StoreRegionInfo& pr = req.region();
        EngineStoreRegion r;
        r.store_node_id = pr.store_node_id();
        r.base_offset   = pr.base_offset();
        r.region_bytes  = pr.region_bytes();
        r.block_size    = pr.block_size();
        r.store_epoch   = pr.store_epoch();

        EngineResultMeta meta = engine_->RegisterStoreRegion(r);
        FillFromEngineMeta(meta, resp->mutable_result());
        resp->set_dn_epoch(engine_->DnEpoch());
        if (meta.success && after_region_registered_) {
            after_region_registered_();
        }
    }

    void Heartbeat(const HeartbeatRequest& req, HeartbeatResponse* resp) override {
        (void) req;
        if (engine_ == nullptr) {
            FillItemErr(resp->mutable_result(), ErrorCode::INTERNAL_ERROR, false, "metadata engine null");
            return;
        }
        FillItemOk(resp->mutable_result());
        resp->set_dn_epoch(engine_->DnEpoch());
        resp->set_marked_unhealthy(false);
    }

    void SpillBlockToSSD(const SpillBlockToSSDRequest& /*req*/,
                         SpillBlockToSSDResponse* resp) override {
        FillItemErr(resp->mutable_result(), ErrorCode::INTERNAL_ERROR, false,
                    "SpillBlockToSSD not hosted on DN (v6.5 P2 falcon_kv_store)");
    }

private:
    std::shared_ptr<KVMetadataEngine> engine_;
    std::function<void()> after_region_registered_;
};

class KVStoreAdminBrpcServiceImplStore final : public KVStoreAdminBrpcServiceImplBase {
public:
    explicit KVStoreAdminBrpcServiceImplStore(std::shared_ptr<KVStoreEngine> engine)
        : engine_(std::move(engine)) {}

    void RegisterStoreRegion(const RegisterStoreRegionRequest& /*req*/,
                             RegisterStoreRegionResponse* resp) override {
        FillItemErr(resp->mutable_result(), ErrorCode::INTERNAL_ERROR, false,
                    "RegisterStoreRegion not hosted on falcon_kv_store");
    }

    void Heartbeat(const HeartbeatRequest& /*req*/, HeartbeatResponse* resp) override {
        FillItemErr(resp->mutable_result(), ErrorCode::INTERNAL_ERROR, false,
                    "Heartbeat not hosted on falcon_kv_store (use DN KVStoreAdminService)");
    }

    void SpillBlockToSSD(const SpillBlockToSSDRequest& req, SpillBlockToSSDResponse* resp) override {
        if (engine_ == nullptr) {
            FillItemErr(resp->mutable_result(), ErrorCode::INTERNAL_ERROR, false, "store engine null");
            return;
        }
        const std::string hash(req.block_hash().data(), static_cast<std::size_t>(req.block_hash().size()));
        std::string evicted_path;
        StoreWriteResult wr =
            engine_->SpillBlockToSSD(hash, req.expected_version(), &evicted_path);
        ItemResultMeta* m = resp->mutable_result();
        m->set_success(wr.result.success);
        m->set_error_code(static_cast<ErrorCode>(wr.result.error_code));
        m->set_retryable(wr.result.retryable);
        m->set_error_message(wr.result.error_message);
        if (wr.result.success) {
            resp->set_evicted_path(std::move(evicted_path));
            resp->set_crc32(wr.crc32);
        }
    }

private:
    std::shared_ptr<KVStoreEngine> engine_;
};

struct ClosureGuard {
    ::google::protobuf::Closure* done;
    explicit ClosureGuard(::google::protobuf::Closure* d) : done(d) {}
    ~ClosureGuard() {
        if (done != nullptr) {
            done->Run();
        }
    }
};

}  // namespace

KVStoreAdminBrpcServiceAdapter::KVStoreAdminBrpcServiceAdapter(
    std::shared_ptr<KVStoreAdminBrpcServiceImplBase> impl)
    : impl_(std::move(impl)) {}

void KVStoreAdminBrpcServiceAdapter::RegisterStoreRegion(::google::protobuf::RpcController* /*controller*/,
                                                         const RegisterStoreRegionRequest* request,
                                                         RegisterStoreRegionResponse* response,
                                                         ::google::protobuf::Closure* done) {
    ClosureGuard guard(done);
    impl_->RegisterStoreRegion(*request, response);
}

void KVStoreAdminBrpcServiceAdapter::Heartbeat(::google::protobuf::RpcController* /*controller*/,
                                               const HeartbeatRequest* request,
                                               HeartbeatResponse* response,
                                               ::google::protobuf::Closure* done) {
    ClosureGuard guard(done);
    impl_->Heartbeat(*request, response);
}

void KVStoreAdminBrpcServiceAdapter::SpillBlockToSSD(::google::protobuf::RpcController* /*controller*/,
                                                     const SpillBlockToSSDRequest* request,
                                                     SpillBlockToSSDResponse* response,
                                                     ::google::protobuf::Closure* done) {
    ClosureGuard guard(done);
    impl_->SpillBlockToSSD(*request, response);
}

std::shared_ptr<KVStoreAdminBrpcServiceImplBase> CreateKVStoreAdminBrpcServiceImplForDn(
    std::shared_ptr<KVMetadataEngine> engine,
    std::function<void()> after_region_registered) {
    return std::make_shared<KVStoreAdminBrpcServiceImplDN>(std::move(engine),
                                                           std::move(after_region_registered));
}

std::shared_ptr<KVStoreAdminBrpcServiceImplBase> CreateKVStoreAdminBrpcServiceImplForStore(
    std::shared_ptr<KVStoreEngine> engine) {
    return std::make_shared<KVStoreAdminBrpcServiceImplStore>(std::move(engine));
}

}  // namespace falconfs::kv
