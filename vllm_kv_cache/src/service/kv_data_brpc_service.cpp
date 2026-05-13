#include "vllm_kv_cache/src/service/kv_data_brpc_service.h"

namespace falconfs::kv {

KVDataBrpcServiceAdapter::KVDataBrpcServiceAdapter(std::shared_ptr<KVDataServiceImpl> impl)
    : impl_(std::move(impl)) {}

namespace {

struct ClosureGuard {
    ::google::protobuf::Closure* done;
    explicit ClosureGuard(::google::protobuf::Closure* d) : done(d) {}
    ~ClosureGuard() {
        if (done != nullptr) done->Run();
    }
};

}  // namespace

void KVDataBrpcServiceAdapter::WriteBlock(::google::protobuf::RpcController* /*controller*/,
                                          const WriteBlockRequest* request,
                                          WriteBlockResponse* response,
                                          ::google::protobuf::Closure* done) {
    ClosureGuard guard(done);
    impl_->WriteBlock(*request, response);
}

void KVDataBrpcServiceAdapter::ReadBlock(::google::protobuf::RpcController* /*controller*/,
                                         const ReadBlockRequest* request,
                                         ReadBlockResponse* response,
                                         ::google::protobuf::Closure* done) {
    ClosureGuard guard(done);
    impl_->ReadBlock(*request, response);
}

void KVDataBrpcServiceAdapter::ReadFromSSD(::google::protobuf::RpcController* /*controller*/,
                                           const ReadFromSSDRequest* request,
                                           ReadFromSSDResponse* response,
                                           ::google::protobuf::Closure* done) {
    ClosureGuard guard(done);
    impl_->ReadFromSSD(*request, response);
}

void KVDataBrpcServiceAdapter::BatchWriteBlock(::google::protobuf::RpcController* /*controller*/,
                                               const BatchWriteBlockRequest* request,
                                               BatchWriteBlockResponse* response,
                                               ::google::protobuf::Closure* done) {
    ClosureGuard guard(done);
    impl_->BatchWriteBlock(*request, response);
}

void KVDataBrpcServiceAdapter::BatchReadBlock(::google::protobuf::RpcController* /*controller*/,
                                              const BatchReadBlockRequest* request,
                                              BatchReadBlockResponse* response,
                                              ::google::protobuf::Closure* done) {
    ClosureGuard guard(done);
    impl_->BatchReadBlock(*request, response);
}

void KVDataBrpcServiceAdapter::BatchReadFromSSD(::google::protobuf::RpcController* /*controller*/,
                                                const BatchReadFromSSDRequest* request,
                                                BatchReadFromSSDResponse* response,
                                                ::google::protobuf::Closure* done) {
    ClosureGuard guard(done);
    impl_->BatchReadFromSSD(*request, response);
}

}  // namespace falconfs::kv
