/* Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MulanPSL-2.0
 *
 * v6 §16 Python BRPC bridge for the KV cache services.
 *
 * Single-purpose CPython extension that wraps `brpc::Channel` and the
 * generated KVMetadataService / KVDataService stubs. Each Python function
 * takes the SERIALIZED protobuf request as `bytes` and returns the
 * SERIALIZED protobuf response as `bytes`. The Python side (built on
 * `kv_*_pb2.py` from `protoc --python_out`) is responsible for encoding the
 * request and decoding the response — this keeps the C++ surface tiny and
 * lets the Python OffloadingManager/router/dn_client live entirely in Python.
 *
 * The extension intentionally does NOT export classes or maintain channel
 * pools: every call constructs a fresh `brpc::Channel`. brpc itself caches
 * connections internally; for the unit-test workloads in
 * `vllm_kv_cache/test/` that is more than enough. A future revision can add
 * `KVMetadataChannel(endpoint)` if pooling becomes a bottleneck.
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/protocol.h>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <type_traits>
#include <vector>
#include <unistd.h>

#include "kv_common.pb.h"
#include "kv_data_service.pb.h"
#include "kv_metadata_service.pb.h"
#include "vllm_kv_cache/src/store/kv_store_facade.h"

namespace {

std::mutex g_registry_mu;
std::unique_ptr<falconfs::kv::KVStoreFacadeRegistry> g_registry;

// Facade BatchRead/BatchWrite counters (local SHM vs remote BRPC) for regression tests.
std::atomic<uint64_t> g_facade_ipc_local_reads{0};
std::atomic<uint64_t> g_facade_ipc_local_writes{0};
std::atomic<uint64_t> g_facade_ipc_remote_reads{0};
std::atomic<uint64_t> g_facade_ipc_remote_writes{0};

static void BumpFacadeIpcCounters(bool is_write, bool is_local)
{
    if (is_write) {
        if (is_local) {
            g_facade_ipc_local_writes.fetch_add(1, std::memory_order_relaxed);
        } else {
            g_facade_ipc_remote_writes.fetch_add(1, std::memory_order_relaxed);
        }
    } else {
        if (is_local) {
            g_facade_ipc_local_reads.fetch_add(1, std::memory_order_relaxed);
        } else {
            g_facade_ipc_remote_reads.fetch_add(1, std::memory_order_relaxed);
        }
    }
}

std::string ResolveLocalHostNodeName()
{
    const char* env = std::getenv("NODE_NAME");
    if (env != nullptr && env[0] != '\0') {
        return std::string(env);
    }
    char host[256];
    if (::gethostname(host, sizeof(host)) == 0) {
        host[sizeof(host) - 1] = '\0';
        return std::string(host);
    }
    return "localhost";
}

bool InitChannel(const char* endpoint, int timeout_ms, brpc::Channel* channel)
{
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.timeout_ms = timeout_ms > 0 ? timeout_ms : 30000;
    options.connect_timeout_ms = 5000;
    options.max_retry = 0;
    options.connection_type = "pooled";
    return channel->Init(endpoint, &options) == 0;
}

PyObject* WrapRpcError(const std::string& where, const std::string& detail)
{
    PyErr_Format(PyExc_RuntimeError, "falconfs_kv_brpc: %s: %s", where.c_str(), detail.c_str());
    return nullptr;
}

// Parse (endpoint, request_bytes, timeout_ms=30000) into out parameters.
// Returns true on success; on failure sets a Python exception and returns false.
bool ParseRpcArgs(PyObject* args, std::string* endpoint, std::string* request_bytes,
                  int* timeout_ms)
{
    const char* endpoint_c = nullptr;
    const char* request_buf = nullptr;
    Py_ssize_t request_len = 0;
    int timeout = 30000;
    if (!PyArg_ParseTuple(args, "sy#|i", &endpoint_c, &request_buf, &request_len, &timeout)) {
        return false;
    }
    endpoint->assign(endpoint_c);
    request_bytes->assign(request_buf, static_cast<size_t>(request_len));
    *timeout_ms = timeout;
    return true;
}

template <typename Request, typename Response, typename CallFn>
PyObject* DoMetadataCall(PyObject* args, const char* method_name, CallFn&& call_fn)
{
    std::string endpoint, request_bytes;
    int timeout_ms = 0;
    if (!ParseRpcArgs(args, &endpoint, &request_bytes, &timeout_ms)) return nullptr;

    Request req;
    if (!req.ParseFromString(request_bytes)) {
        return WrapRpcError(method_name, "ParseFromString failed");
    }

    brpc::Channel channel;
    {
        Py_BEGIN_ALLOW_THREADS;
        if (!InitChannel(endpoint.c_str(), timeout_ms, &channel)) {
            Py_BLOCK_THREADS;
            return WrapRpcError(method_name, std::string("channel init failed: ") + endpoint);
        }
        Py_END_ALLOW_THREADS;
    }

    falconfs::kv::KVMetadataService_Stub stub(&channel);
    Response resp;
    brpc::Controller cntl;
    {
        Py_BEGIN_ALLOW_THREADS;
        call_fn(stub, cntl, req, resp);
        Py_END_ALLOW_THREADS;
    }
    if (cntl.Failed()) {
        return WrapRpcError(method_name, cntl.ErrorText());
    }
    std::string out;
    if (!resp.SerializeToString(&out)) {
        return WrapRpcError(method_name, "SerializeToString failed");
    }
    return PyBytes_FromStringAndSize(out.data(), static_cast<Py_ssize_t>(out.size()));
}

template <typename Request, typename Response, typename CallFn>
PyObject* DoDataCall(PyObject* args, const char* method_name, CallFn&& call_fn)
{
    std::string endpoint, request_bytes;
    int timeout_ms = 0;
    if (!ParseRpcArgs(args, &endpoint, &request_bytes, &timeout_ms)) return nullptr;

    Request req;
    if (!req.ParseFromString(request_bytes)) {
        return WrapRpcError(method_name, "ParseFromString failed");
    }

    brpc::Channel channel;
    {
        Py_BEGIN_ALLOW_THREADS;
        if (!InitChannel(endpoint.c_str(), timeout_ms, &channel)) {
            Py_BLOCK_THREADS;
            return WrapRpcError(method_name, std::string("channel init failed: ") + endpoint);
        }
        Py_END_ALLOW_THREADS;
    }

    falconfs::kv::KVDataService_Stub stub(&channel);
    Response resp;
    brpc::Controller cntl;
    {
        Py_BEGIN_ALLOW_THREADS;
        call_fn(stub, cntl, req, resp);
        Py_END_ALLOW_THREADS;
    }
    if (cntl.Failed()) {
        return WrapRpcError(method_name, cntl.ErrorText());
    }
    std::string out;
    if (!resp.SerializeToString(&out)) {
        return WrapRpcError(method_name, "SerializeToString failed");
    }
    return PyBytes_FromStringAndSize(out.data(), static_cast<Py_ssize_t>(out.size()));
}

// Metadata methods.

PyObject* PyBatchLookupWithLease(PyObject* /*self*/, PyObject* args)
{
    using namespace falconfs::kv;
    return DoMetadataCall<BatchLookupRequest, BatchLookupResponse>(
        args, "BatchLookupWithLease",
        [](KVMetadataService_Stub& s, brpc::Controller& c,
           const BatchLookupRequest& req, BatchLookupResponse& rsp) {
            s.BatchLookupWithLease(&c, &req, &rsp, nullptr);
        });
}

PyObject* PyBatchAllocateWithLease(PyObject* /*self*/, PyObject* args)
{
    using namespace falconfs::kv;
    return DoMetadataCall<BatchAllocateRequest, BatchAllocateResponse>(
        args, "BatchAllocateWithLease",
        [](KVMetadataService_Stub& s, brpc::Controller& c,
           const BatchAllocateRequest& req, BatchAllocateResponse& rsp) {
            s.BatchAllocateWithLease(&c, &req, &rsp, nullptr);
        });
}

PyObject* PyBatchRenewLease(PyObject* /*self*/, PyObject* args)
{
    using namespace falconfs::kv;
    return DoMetadataCall<BatchRenewLeaseRequest, BatchRenewLeaseResponse>(
        args, "BatchRenewLease",
        [](KVMetadataService_Stub& s, brpc::Controller& c,
           const BatchRenewLeaseRequest& req, BatchRenewLeaseResponse& rsp) {
            s.BatchRenewLease(&c, &req, &rsp, nullptr);
        });
}

PyObject* PyBatchUpdateBlockStatus(PyObject* /*self*/, PyObject* args)
{
    using namespace falconfs::kv;
    return DoMetadataCall<BatchUpdateStatusRequest, BatchUpdateStatusResponse>(
        args, "BatchUpdateBlockStatus",
        [](KVMetadataService_Stub& s, brpc::Controller& c,
           const BatchUpdateStatusRequest& req, BatchUpdateStatusResponse& rsp) {
            s.BatchUpdateBlockStatus(&c, &req, &rsp, nullptr);
        });
}

PyObject* PyBatchFreeAllocated(PyObject* /*self*/, PyObject* args)
{
    using namespace falconfs::kv;
    return DoMetadataCall<BatchFreeAllocatedRequest, BatchFreeAllocatedResponse>(
        args, "BatchFreeAllocated",
        [](KVMetadataService_Stub& s, brpc::Controller& c,
           const BatchFreeAllocatedRequest& req, BatchFreeAllocatedResponse& rsp) {
            s.BatchFreeAllocated(&c, &req, &rsp, nullptr);
        });
}

// Data methods.

PyObject* PyBatchWriteBlock(PyObject* /*self*/, PyObject* args)
{
    using namespace falconfs::kv;
    return DoDataCall<BatchWriteBlockRequest, BatchWriteBlockResponse>(
        args, "BatchWriteBlock",
        [](KVDataService_Stub& s, brpc::Controller& c,
           const BatchWriteBlockRequest& req, BatchWriteBlockResponse& rsp) {
            s.BatchWriteBlock(&c, &req, &rsp, nullptr);
        });
}

PyObject* PyBatchReadBlock(PyObject* /*self*/, PyObject* args)
{
    using namespace falconfs::kv;
    return DoDataCall<BatchReadBlockRequest, BatchReadBlockResponse>(
        args, "BatchReadBlock",
        [](KVDataService_Stub& s, brpc::Controller& c,
           const BatchReadBlockRequest& req, BatchReadBlockResponse& rsp) {
            s.BatchReadBlock(&c, &req, &rsp, nullptr);
        });
}

PyObject* PyBatchReadFromSSD(PyObject* /*self*/, PyObject* args)
{
    using namespace falconfs::kv;
    return DoDataCall<BatchReadFromSSDRequest, BatchReadFromSSDResponse>(
        args, "BatchReadFromSSD",
        [](KVDataService_Stub& s, brpc::Controller& c,
           const BatchReadFromSSDRequest& req, BatchReadFromSSDResponse& rsp) {
            s.BatchReadFromSSD(&c, &req, &rsp, nullptr);
        });
}

PyObject* PyWriteBlock(PyObject* /*self*/, PyObject* args)
{
    using namespace falconfs::kv;
    return DoDataCall<WriteBlockRequest, WriteBlockResponse>(
        args, "WriteBlock",
        [](KVDataService_Stub& s, brpc::Controller& c,
           const WriteBlockRequest& req, WriteBlockResponse& rsp) {
            s.WriteBlock(&c, &req, &rsp, nullptr);
        });
}

PyObject* PyReadBlock(PyObject* /*self*/, PyObject* args)
{
    using namespace falconfs::kv;
    return DoDataCall<ReadBlockRequest, ReadBlockResponse>(
        args, "ReadBlock",
        [](KVDataService_Stub& s, brpc::Controller& c,
           const ReadBlockRequest& req, ReadBlockResponse& rsp) {
            s.ReadBlock(&c, &req, &rsp, nullptr);
        });
}

PyObject* PyReadFromSSD(PyObject* /*self*/, PyObject* args)
{
    using namespace falconfs::kv;
    return DoDataCall<ReadFromSSDRequest, ReadFromSSDResponse>(
        args, "ReadFromSSD",
        [](KVDataService_Stub& s, brpc::Controller& c,
           const ReadFromSSDRequest& req, ReadFromSSDResponse& rsp) {
            s.ReadFromSSD(&c, &req, &rsp, nullptr);
        });
}

bool EnsureRegistryLocked(const std::string& conninfo)
{
    if (!g_registry) {
        g_registry.reset(new falconfs::kv::KVStoreFacadeRegistry());
    }
    g_registry->Start(ResolveLocalHostNodeName(), conninfo);
    return true;
}

PyObject* PyMembershipStart(PyObject* /*self*/, PyObject* args)
{
    const char* conninfo_c = nullptr;
    if (!PyArg_ParseTuple(args, "s", &conninfo_c)) return nullptr;
    std::lock_guard<std::mutex> lk(g_registry_mu);
    if (!EnsureRegistryLocked(conninfo_c)) {
        return WrapRpcError("membership_start", "registry start failed");
    }
    Py_RETURN_NONE;
}

PyObject* PyMembershipStop(PyObject* /*self*/, PyObject* /*args*/)
{
    std::lock_guard<std::mutex> lk(g_registry_mu);
    if (g_registry) {
        g_registry->Stop();
    }
    Py_RETURN_NONE;
}

PyObject* PyMembershipRefresh(PyObject* /*self*/, PyObject* args)
{
    int timeout_ms = 2000;
    if (!PyArg_ParseTuple(args, "|i", &timeout_ms)) return nullptr;
    std::lock_guard<std::mutex> lk(g_registry_mu);
    if (!g_registry) {
        return WrapRpcError("membership_refresh", "registry not started");
    }
    falconfs::kv::RefreshResult out = g_registry->RefreshNow(timeout_ms);
    return Py_BuildValue("(iiK)", out.num_dns, out.num_stores,
                         static_cast<unsigned long long>(out.generation));
}

PyObject* PyDiscoverDnEndpoints(PyObject* /*self*/, PyObject* /*args*/)
{
    std::lock_guard<std::mutex> lk(g_registry_mu);
    if (!g_registry) {
        return WrapRpcError("discover_dn_endpoints", "registry not started");
    }
    PyObject* dict = PyDict_New();
    const auto snapshot = g_registry->SnapshotDnEndpoints();
    for (const auto& kv : snapshot) {
        PyObject* key = PyLong_FromLong(kv.first);
        PyObject* val = PyUnicode_FromString(kv.second.c_str());
        if (PyDict_SetItem(dict, key, val) != 0) {
            Py_DECREF(key);
            Py_DECREF(val);
            Py_DECREF(dict);
            return WrapRpcError("discover_dn_endpoints", "PyDict_SetItem failed");
        }
        Py_DECREF(key);
        Py_DECREF(val);
    }
    return dict;
}

PyObject* PyStoreLocality(PyObject* /*self*/, PyObject* /*args*/)
{
    std::lock_guard<std::mutex> lk(g_registry_mu);
    if (!g_registry) {
        return WrapRpcError("store_locality", "registry not started");
    }
    PyObject* dict = PyDict_New();
    const auto snapshot = g_registry->SnapshotStoreLocality();
    for (const auto& kv : snapshot) {
        PyObject* key = PyLong_FromLong(kv.first);
        PyObject* val = kv.second ? Py_True : Py_False;
        Py_INCREF(val);
        if (PyDict_SetItem(dict, key, val) != 0) {
            Py_DECREF(key);
            Py_DECREF(val);
            Py_DECREF(dict);
            return WrapRpcError("store_locality", "PyDict_SetItem failed");
        }
        Py_DECREF(key);
        Py_DECREF(val);
    }
    return dict;
}

template <typename Request, typename Response, typename CallFn>
PyObject* DoFacadeCall(PyObject* args, const char* method_name, bool is_write, CallFn&& call_fn)
{
    int store_node_id = 0;
    const char* request_buf = nullptr;
    Py_ssize_t request_len = 0;
    if (!PyArg_ParseTuple(args, "iy#", &store_node_id, &request_buf, &request_len)) {
        return nullptr;
    }
    Request req;
    if (!req.ParseFromArray(request_buf, static_cast<int>(request_len))) {
        return WrapRpcError(method_name, "ParseFromString failed");
    }
    std::shared_ptr<falconfs::kv::IKVStoreFacade> facade;
    {
        std::lock_guard<std::mutex> lk(g_registry_mu);
        if (!g_registry) {
            return WrapRpcError(method_name, "registry not started");
        }
        facade = g_registry->Resolve(store_node_id);
    }
    if (!facade) {
        return WrapRpcError(method_name, "unknown store_node_id");
    }
    Response rsp;
    {
        Py_BEGIN_ALLOW_THREADS;
        call_fn(facade, req, &rsp);
        Py_END_ALLOW_THREADS;
    }
    int bump_times = 1;
    if constexpr (std::is_same_v<Request, falconfs::kv::BatchWriteBlockRequest>) {
        bump_times = std::max(1, req.items_size());
    } else if constexpr (std::is_same_v<Request, falconfs::kv::BatchReadBlockRequest>) {
        bump_times = std::max(1, req.items_size());
    } else if constexpr (std::is_same_v<Request, falconfs::kv::WriteBlockRequest>) {
        bump_times = 1;
    } else if constexpr (std::is_same_v<Request, falconfs::kv::ReadBlockRequest>) {
        bump_times = 1;
    } else if constexpr (std::is_same_v<Request, falconfs::kv::ReadFromSSDRequest>) {
        bump_times = 1;
    }
    for (int i = 0; i < bump_times; ++i) {
        BumpFacadeIpcCounters(is_write, facade->IsLocal());
    }
    std::string out;
    if (!rsp.SerializeToString(&out)) {
        return WrapRpcError(method_name, "SerializeToString failed");
    }
    return PyBytes_FromStringAndSize(out.data(), static_cast<Py_ssize_t>(out.size()));
}

PyObject* PyFacadeIpcStatsReset(PyObject* /*self*/, PyObject* /*args*/)
{
    g_facade_ipc_local_reads.store(0, std::memory_order_relaxed);
    g_facade_ipc_local_writes.store(0, std::memory_order_relaxed);
    g_facade_ipc_remote_reads.store(0, std::memory_order_relaxed);
    g_facade_ipc_remote_writes.store(0, std::memory_order_relaxed);
    Py_RETURN_NONE;
}

PyObject* PyFacadeIpcStats(PyObject* /*self*/, PyObject* /*args*/)
{
    return Py_BuildValue(
        "(KKKK)",
        static_cast<unsigned long long>(g_facade_ipc_local_reads.load(std::memory_order_relaxed)),
        static_cast<unsigned long long>(g_facade_ipc_local_writes.load(std::memory_order_relaxed)),
        static_cast<unsigned long long>(g_facade_ipc_remote_reads.load(std::memory_order_relaxed)),
        static_cast<unsigned long long>(g_facade_ipc_remote_writes.load(std::memory_order_relaxed)));
}

PyObject* PyFacadeBatchWriteBlock(PyObject* /*self*/, PyObject* args)
{
    using namespace falconfs::kv;
    return DoFacadeCall<BatchWriteBlockRequest, BatchWriteBlockResponse>(
        args, "facade_batch_write_block", /*is_write=*/true,
        [](const std::shared_ptr<IKVStoreFacade>& facade,
           const BatchWriteBlockRequest& req,
           BatchWriteBlockResponse* rsp) { facade->BatchWriteBlock(req, rsp); });
}

PyObject* PyFacadeBatchReadBlock(PyObject* /*self*/, PyObject* args)
{
    using namespace falconfs::kv;
    return DoFacadeCall<BatchReadBlockRequest, BatchReadBlockResponse>(
        args, "facade_batch_read_block", /*is_write=*/false,
        [](const std::shared_ptr<IKVStoreFacade>& facade,
           const BatchReadBlockRequest& req,
           BatchReadBlockResponse* rsp) { facade->BatchReadBlock(req, rsp); });
}

PyObject* PyFacadeBatchReadFromSSD(PyObject* /*self*/, PyObject* args)
{
    using namespace falconfs::kv;
    return DoFacadeCall<BatchReadFromSSDRequest, BatchReadFromSSDResponse>(
        args, "facade_batch_read_from_ssd", /*is_write=*/false,
        [](const std::shared_ptr<IKVStoreFacade>& facade,
           const BatchReadFromSSDRequest& req,
           BatchReadFromSSDResponse* rsp) { facade->BatchReadFromSSD(req, rsp); });
}

PyObject* PyFacadeWriteBlock(PyObject* /*self*/, PyObject* args)
{
    using namespace falconfs::kv;
    return DoFacadeCall<WriteBlockRequest, WriteBlockResponse>(
        args, "facade_write_block", /*is_write=*/true,
        [](const std::shared_ptr<IKVStoreFacade>& facade,
           const WriteBlockRequest& req,
           WriteBlockResponse* rsp) { facade->WriteBlock(req, rsp); });
}

PyObject* PyFacadeReadBlock(PyObject* /*self*/, PyObject* args)
{
    using namespace falconfs::kv;
    return DoFacadeCall<ReadBlockRequest, ReadBlockResponse>(
        args, "facade_read_block", /*is_write=*/false,
        [](const std::shared_ptr<IKVStoreFacade>& facade,
           const ReadBlockRequest& req,
           ReadBlockResponse* rsp) { facade->ReadBlock(req, rsp); });
}

PyObject* PyFacadeReadFromSSD(PyObject* /*self*/, PyObject* args)
{
    using namespace falconfs::kv;
    return DoFacadeCall<ReadFromSSDRequest, ReadFromSSDResponse>(
        args, "facade_read_from_ssd", /*is_write=*/false,
        [](const std::shared_ptr<IKVStoreFacade>& facade,
           const ReadFromSSDRequest& req,
           ReadFromSSDResponse* rsp) { facade->ReadFromSSD(req, rsp); });
}

PyMethodDef kModuleMethods[] = {
    {"batch_lookup_with_lease", PyBatchLookupWithLease, METH_VARARGS,
     "Calls KVMetadataService.BatchLookupWithLease over BRPC.\n"
     "Args: endpoint:str, request_bytes:bytes, timeout_ms:int=30000\n"
     "Returns: response_bytes:bytes"},
    {"batch_allocate_with_lease", PyBatchAllocateWithLease, METH_VARARGS,
     "Calls KVMetadataService.BatchAllocateWithLease over BRPC."},
    {"batch_renew_lease", PyBatchRenewLease, METH_VARARGS,
     "Calls KVMetadataService.BatchRenewLease over BRPC."},
    {"batch_update_block_status", PyBatchUpdateBlockStatus, METH_VARARGS,
     "Calls KVMetadataService.BatchUpdateBlockStatus over BRPC."},
    {"batch_free_allocated", PyBatchFreeAllocated, METH_VARARGS,
     "Calls KVMetadataService.BatchFreeAllocated over BRPC."},
    {"batch_write_block", PyBatchWriteBlock, METH_VARARGS,
     "Calls KVDataService.BatchWriteBlock over BRPC."},
    {"batch_read_block", PyBatchReadBlock, METH_VARARGS,
     "Calls KVDataService.BatchReadBlock over BRPC."},
    {"batch_read_from_ssd", PyBatchReadFromSSD, METH_VARARGS,
     "Calls KVDataService.BatchReadFromSSD over BRPC."},
    {"write_block", PyWriteBlock, METH_VARARGS,
     "Calls KVDataService.WriteBlock over BRPC."},
    {"read_block", PyReadBlock, METH_VARARGS,
     "Calls KVDataService.ReadBlock over BRPC."},
    {"read_from_ssd", PyReadFromSSD, METH_VARARGS,
     "Calls KVDataService.ReadFromSSD over BRPC."},
    {"membership_start", PyMembershipStart, METH_VARARGS,
     "Starts KVStoreFacadeRegistry with CN conninfo."},
    {"membership_stop", PyMembershipStop, METH_VARARGS,
     "Stops KVStoreFacadeRegistry."},
    {"membership_refresh", PyMembershipRefresh, METH_VARARGS,
     "Calls KVStoreFacadeRegistry.RefreshNow(timeout_ms)."},
    {"discover_dn_endpoints", PyDiscoverDnEndpoints, METH_VARARGS,
     "Returns {dn_id: endpoint} from current membership snapshot."},
    {"store_locality", PyStoreLocality, METH_VARARGS,
     "Returns {store_id: is_local} from facade registry."},
    {"facade_ipc_stats_reset", PyFacadeIpcStatsReset, METH_VARARGS,
     "Resets per-process facade local/remote read/write counters (test hook)."},
    {"facade_ipc_stats", PyFacadeIpcStats, METH_VARARGS,
     "Returns (local_reads, local_writes, remote_reads, remote_writes) for facade data path."},
    {"facade_batch_write_block", PyFacadeBatchWriteBlock, METH_VARARGS,
     "Calls facade BatchWriteBlock for store_node_id."},
    {"facade_batch_read_block", PyFacadeBatchReadBlock, METH_VARARGS,
     "Calls facade BatchReadBlock for store_node_id."},
    {"facade_batch_read_from_ssd", PyFacadeBatchReadFromSSD, METH_VARARGS,
     "Calls facade BatchReadFromSSD for store_node_id."},
    {"facade_write_block", PyFacadeWriteBlock, METH_VARARGS,
     "Calls facade WriteBlock for store_node_id."},
    {"facade_read_block", PyFacadeReadBlock, METH_VARARGS,
     "Calls facade ReadBlock for store_node_id."},
    {"facade_read_from_ssd", PyFacadeReadFromSSD, METH_VARARGS,
     "Calls facade ReadFromSSD for store_node_id."},
    {nullptr, nullptr, 0, nullptr},
};

PyModuleDef kModuleDef = {
    PyModuleDef_HEAD_INIT,
    "falconfs_kv_brpc",
    "FalconFS KV cache BRPC bridge for Python (v6 §16). "
    "Each function calls KVMetadataService / KVDataService over brpc::Channel "
    "with serialized protobuf request/response bytes.",
    -1,
    kModuleMethods,
    nullptr,
    nullptr,
    nullptr,
    nullptr,
};

}  // namespace

PyMODINIT_FUNC PyInit_falconfs_kv_brpc(void)
{
    constexpr uint64_t kMinBrpcMaxBody = 512ULL * 1024 * 1024;
    if (brpc::FLAGS_max_body_size < kMinBrpcMaxBody) {
        brpc::FLAGS_max_body_size = kMinBrpcMaxBody;
    }
    return PyModule_Create(&kModuleDef);
}
