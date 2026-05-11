/* Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 */

#include "brpc_comm_adapter/brpc_kv_service_imp.h"

#include <brpc/server.h>

#include "brpc_comm_adapter/brpc_kv_cache_service_job.h"

namespace falcon::kv_proto {

namespace {

template <typename Request>
void DispatchKVJob(falcon_meta_job_dispatch_func dispatchFunc,
                   FalconKVServiceMethod method,
                   const Request *request,
                   ::google::protobuf::Message *response,
                   ::google::protobuf::Closure *done)
{
    /* The job owns its serialized request and the BRPC closure. After the
     * pool worker calls Done(), the job deletes itself (the worker `delete`s
     * its job when DoWork returns). v6.4 \u00a74.1.1 step 1: this thread does NOT
     * touch the cache. */
    auto *job = new BrpcKVCacheServiceJob(method, request->SerializeAsString(), response, done);
    dispatchFunc(static_cast<void *>(job));
}

}  // namespace

void BrpcKVMetadataServiceImpl::BatchLookupWithLease(::google::protobuf::RpcController *controller,
                                                     const ::falconfs::kv::BatchLookupRequest *request,
                                                     ::falconfs::kv::BatchLookupResponse *response,
                                                     ::google::protobuf::Closure *done)
{
    (void) controller;
    DispatchKVJob(dispatchFunc_, FalconKVServiceMethod::BATCH_LOOKUP_WITH_LEASE, request, response, done);
}

void BrpcKVMetadataServiceImpl::BatchAllocateWithLease(::google::protobuf::RpcController *controller,
                                                       const ::falconfs::kv::BatchAllocateRequest *request,
                                                       ::falconfs::kv::BatchAllocateResponse *response,
                                                       ::google::protobuf::Closure *done)
{
    (void) controller;
    DispatchKVJob(dispatchFunc_, FalconKVServiceMethod::BATCH_ALLOCATE_WITH_LEASE, request, response, done);
}

void BrpcKVMetadataServiceImpl::BatchRenewLease(::google::protobuf::RpcController *controller,
                                                const ::falconfs::kv::BatchRenewLeaseRequest *request,
                                                ::falconfs::kv::BatchRenewLeaseResponse *response,
                                                ::google::protobuf::Closure *done)
{
    (void) controller;
    DispatchKVJob(dispatchFunc_, FalconKVServiceMethod::BATCH_RENEW_LEASE, request, response, done);
}

void BrpcKVMetadataServiceImpl::BatchUpdateBlockStatus(::google::protobuf::RpcController *controller,
                                                       const ::falconfs::kv::BatchUpdateStatusRequest *request,
                                                       ::falconfs::kv::BatchUpdateStatusResponse *response,
                                                       ::google::protobuf::Closure *done)
{
    (void) controller;
    DispatchKVJob(dispatchFunc_, FalconKVServiceMethod::BATCH_UPDATE_BLOCK_STATUS, request, response, done);
}

void BrpcKVMetadataServiceImpl::BatchFreeAllocated(::google::protobuf::RpcController *controller,
                                                   const ::falconfs::kv::BatchFreeAllocatedRequest *request,
                                                   ::falconfs::kv::BatchFreeAllocatedResponse *response,
                                                   ::google::protobuf::Closure *done)
{
    (void) controller;
    DispatchKVJob(dispatchFunc_, FalconKVServiceMethod::BATCH_FREE_ALLOCATED, request, response, done);
}

void BrpcKVDataServiceImpl::BatchWriteBlock(::google::protobuf::RpcController *controller,
                                            const ::falconfs::kv::BatchWriteBlockRequest *request,
                                            ::falconfs::kv::BatchWriteBlockResponse *response,
                                            ::google::protobuf::Closure *done)
{
    (void) controller;
    brpc::ClosureGuard guard(done);
    impl_->BatchWriteBlock(*request, response);
}

void BrpcKVDataServiceImpl::BatchReadBlock(::google::protobuf::RpcController *controller,
                                           const ::falconfs::kv::BatchReadBlockRequest *request,
                                           ::falconfs::kv::BatchReadBlockResponse *response,
                                           ::google::protobuf::Closure *done)
{
    (void) controller;
    brpc::ClosureGuard guard(done);
    impl_->BatchReadBlock(*request, response);
}

void BrpcKVDataServiceImpl::BatchReadFromSSD(::google::protobuf::RpcController *controller,
                                             const ::falconfs::kv::BatchReadFromSSDRequest *request,
                                             ::falconfs::kv::BatchReadFromSSDResponse *response,
                                             ::google::protobuf::Closure *done)
{
    (void) controller;
    brpc::ClosureGuard guard(done);
    impl_->BatchReadFromSSD(*request, response);
}

}  // namespace falcon::kv_proto
