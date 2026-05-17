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
 * Metadata RPCs reuse a small endpoint-keyed brpc::Channel cache because v6
 * metadata fan-out measures sub-millisecond calls where channel construction is
 * visible. Data RPCs still use the facade registry / direct path below.
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/protocol.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <vector>
#include <unistd.h>

#include "kv_common.pb.h"
#include "kv_data_service.pb.h"
#include "kv_metadata_service.pb.h"
#include "vllm_kv_cache/src/store/kv_store_facade.h"

namespace {

std::mutex g_registry_mu;
std::unique_ptr<falconfs::kv::KVStoreFacadeRegistry> g_registry;

struct CachedMetadataChannel {
    std::shared_ptr<brpc::Channel> channel;
    int timeout_ms{0};
};

std::mutex g_metadata_channel_mu;
std::unordered_map<std::string, CachedMetadataChannel> g_metadata_channels;

// Facade BatchRead/BatchWrite counters (local SHM vs remote BRPC) for regression tests.
std::atomic<uint64_t> g_facade_ipc_local_reads{0};
std::atomic<uint64_t> g_facade_ipc_local_writes{0};
std::atomic<uint64_t> g_facade_ipc_remote_reads{0};
std::atomic<uint64_t> g_facade_ipc_remote_writes{0};

std::atomic<uint64_t> g_facade_perf_local_read_ops{0};
std::atomic<uint64_t> g_facade_perf_local_read_bytes{0};
std::atomic<uint64_t> g_facade_perf_local_read_ns{0};
std::atomic<uint64_t> g_facade_perf_local_write_ops{0};
std::atomic<uint64_t> g_facade_perf_local_write_bytes{0};
std::atomic<uint64_t> g_facade_perf_local_write_ns{0};
std::atomic<uint64_t> g_facade_perf_remote_read_ops{0};
std::atomic<uint64_t> g_facade_perf_remote_read_bytes{0};
std::atomic<uint64_t> g_facade_perf_remote_read_ns{0};
std::atomic<uint64_t> g_facade_perf_remote_write_ops{0};
std::atomic<uint64_t> g_facade_perf_remote_write_bytes{0};
std::atomic<uint64_t> g_facade_perf_remote_write_ns{0};
std::atomic<uint64_t> g_native_read_req_seq{0};


static bool ReadU64LEBytes(const std::string& in, size_t* pos, uint64_t* v)
{
    if (*pos + 8 > in.size()) return false;
    uint64_t x = 0;
    for (int i = 0; i < 8; ++i) {
        x |= (static_cast<uint64_t>(static_cast<unsigned char>(in[*pos + i])) << (8 * i));
    }
    *pos += 8;
    *v = x;
    return true;
}

static bool DecodePayloadFramesBytes(const std::string& in, int n, std::vector<std::string>* payloads)
{
    if (payloads == nullptr) return false;
    payloads->clear();
    payloads->reserve(std::max(0, n));
    size_t pos = 0;
    for (int i = 0; i < n; ++i) {
        uint64_t len = 0;
        if (!ReadU64LEBytes(in, &pos, &len)) return false;
        if (len > static_cast<uint64_t>(in.size() - pos)) return false;
        payloads->emplace_back(in.data() + pos, static_cast<size_t>(len));
        pos += static_cast<size_t>(len);
    }
    return pos == in.size();
}

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

static void BumpFacadePerfCounters(bool is_write, bool is_local, uint64_t elapsed_ns,
                                   uint64_t bytes)
{
    if (is_write) {
        if (is_local) {
            g_facade_perf_local_write_ops.fetch_add(1, std::memory_order_relaxed);
            g_facade_perf_local_write_bytes.fetch_add(bytes, std::memory_order_relaxed);
            g_facade_perf_local_write_ns.fetch_add(elapsed_ns, std::memory_order_relaxed);
        } else {
            g_facade_perf_remote_write_ops.fetch_add(1, std::memory_order_relaxed);
            g_facade_perf_remote_write_bytes.fetch_add(bytes, std::memory_order_relaxed);
            g_facade_perf_remote_write_ns.fetch_add(elapsed_ns, std::memory_order_relaxed);
        }
    } else {
        if (is_local) {
            g_facade_perf_local_read_ops.fetch_add(1, std::memory_order_relaxed);
            g_facade_perf_local_read_bytes.fetch_add(bytes, std::memory_order_relaxed);
            g_facade_perf_local_read_ns.fetch_add(elapsed_ns, std::memory_order_relaxed);
        } else {
            g_facade_perf_remote_read_ops.fetch_add(1, std::memory_order_relaxed);
            g_facade_perf_remote_read_bytes.fetch_add(bytes, std::memory_order_relaxed);
            g_facade_perf_remote_read_ns.fetch_add(elapsed_ns, std::memory_order_relaxed);
        }
    }
}

template <typename Request>
uint64_t FacadeRequestBytes(const Request& req)
{
    if constexpr (std::is_same_v<Request, falconfs::kv::WriteBlockRequest>) {
        return static_cast<uint64_t>(req.item().payload().size());
    } else if constexpr (std::is_same_v<Request, falconfs::kv::BatchWriteBlockRequest>) {
        uint64_t out = 0;
        for (const auto& item : req.items()) out += static_cast<uint64_t>(item.payload().size());
        return out;
    } else if constexpr (std::is_same_v<Request, falconfs::kv::ReadBlockRequest>) {
        return static_cast<uint64_t>(std::max(0, req.item().block_size()));
    } else if constexpr (std::is_same_v<Request, falconfs::kv::BatchReadBlockRequest>) {
        uint64_t out = 0;
        for (const auto& item : req.items()) out += static_cast<uint64_t>(std::max(0, item.block_size()));
        return out;
    }
    return 0;
}

template <typename Response>
uint64_t FacadeResponseBytes(const Response& rsp)
{
    if constexpr (std::is_same_v<Response, falconfs::kv::ReadBlockResponse>) {
        return static_cast<uint64_t>(rsp.result().payload().size());
    } else if constexpr (std::is_same_v<Response, falconfs::kv::BatchReadBlockResponse>) {
        uint64_t out = 0;
        for (const auto& item : rsp.results()) out += static_cast<uint64_t>(item.payload().size());
        return out;
    } else if constexpr (std::is_same_v<Response, falconfs::kv::ReadFromSSDResponse>) {
        return static_cast<uint64_t>(rsp.result().payload().size());
    } else if constexpr (std::is_same_v<Response, falconfs::kv::BatchReadFromSSDResponse>) {
        uint64_t out = 0;
        for (const auto& item : rsp.results()) out += static_cast<uint64_t>(item.payload().size());
        return out;
    }
    return 0;
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

std::shared_ptr<brpc::Channel> ResolveMetadataChannel(const std::string& endpoint,
                                                      int timeout_ms,
                                                      std::string* error)
{
    const int normalized_timeout = timeout_ms > 0 ? timeout_ms : 30000;
    {
        std::lock_guard<std::mutex> lk(g_metadata_channel_mu);
        auto it = g_metadata_channels.find(endpoint);
        if (it != g_metadata_channels.end() &&
            it->second.timeout_ms == normalized_timeout &&
            it->second.channel) {
            return it->second.channel;
        }
    }

    auto channel = std::make_shared<brpc::Channel>();
    if (!InitChannel(endpoint.c_str(), normalized_timeout, channel.get())) {
        if (error != nullptr) {
            *error = std::string("channel init failed: ") + endpoint;
        }
        return nullptr;
    }

    {
        std::lock_guard<std::mutex> lk(g_metadata_channel_mu);
        auto& slot = g_metadata_channels[endpoint];
        if (slot.channel && slot.timeout_ms == normalized_timeout) {
            return slot.channel;
        }
        slot.channel = channel;
        slot.timeout_ms = normalized_timeout;
    }
    return channel;
}

void EvictMetadataChannel(const std::string& endpoint,
                          const std::shared_ptr<brpc::Channel>& channel)
{
    std::lock_guard<std::mutex> lk(g_metadata_channel_mu);
    auto it = g_metadata_channels.find(endpoint);
    if (it != g_metadata_channels.end() && it->second.channel == channel) {
        g_metadata_channels.erase(it);
    }
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

    std::shared_ptr<brpc::Channel> channel;
    std::string channel_error;
    {
        Py_BEGIN_ALLOW_THREADS;
        channel = ResolveMetadataChannel(endpoint, timeout_ms, &channel_error);
        Py_END_ALLOW_THREADS;
    }
    if (!channel) {
        return WrapRpcError(method_name, channel_error.empty() ? "channel init failed" : channel_error);
    }

    falconfs::kv::KVMetadataService_Stub stub(channel.get());
    Response resp;
    brpc::Controller cntl;
    {
        Py_BEGIN_ALLOW_THREADS;
        call_fn(stub, cntl, req, resp);
        Py_END_ALLOW_THREADS;
    }
    if (cntl.Failed()) {
        EvictMetadataChannel(endpoint, channel);
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
    if constexpr (std::is_same_v<Request, falconfs::kv::WriteBlockRequest>) {
        if (!req.item().payload().empty()) {
            cntl.request_attachment().append(req.item().payload());
            req.mutable_item()->clear_payload();
        }
    }
    {
        Py_BEGIN_ALLOW_THREADS;
        call_fn(stub, cntl, req, resp);
        Py_END_ALLOW_THREADS;
    }
    if (cntl.Failed()) {
        return WrapRpcError(method_name, cntl.ErrorText());
    }
    if constexpr (std::is_same_v<Response, falconfs::kv::ReadBlockResponse>) {
        if (cntl.response_attachment().size() > 0 && resp.has_result() &&
            resp.result().payload().empty()) {
            std::string payload;
            cntl.response_attachment().copy_to(&payload);
            resp.mutable_result()->set_payload(std::move(payload));
        }
    } else if constexpr (std::is_same_v<Response, falconfs::kv::BatchReadBlockResponse>) {
        if (cntl.response_attachment().size() > 0 && resp.results_size() > 0) {
            std::string framed;
            cntl.response_attachment().copy_to(&framed);
            std::vector<std::string> payloads;
            if (DecodePayloadFramesBytes(framed, resp.results_size(), &payloads)) {
                for (int i = 0; i < resp.results_size(); ++i) {
                    if (resp.results(i).payload().empty()) {
                        resp.mutable_results(i)->set_payload(std::move(payloads[static_cast<size_t>(i)]));
                    }
                }
            }
        }
    } else if constexpr (std::is_same_v<Response, falconfs::kv::ReadFromSSDResponse>) {
        if (cntl.response_attachment().size() > 0 && resp.has_result() &&
            resp.result().payload().empty()) {
            std::string payload;
            cntl.response_attachment().copy_to(&payload);
            resp.mutable_result()->set_payload(std::move(payload));
        }
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
    const bool is_local = facade->IsLocal();
    const uint64_t request_bytes = FacadeRequestBytes(req);
    auto t0 = std::chrono::steady_clock::now();
    {
        Py_BEGIN_ALLOW_THREADS;
        call_fn(facade, req, &rsp);
        Py_END_ALLOW_THREADS;
    }
    auto t1 = std::chrono::steady_clock::now();
    const uint64_t elapsed_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
    const uint64_t response_bytes = FacadeResponseBytes(rsp);
    BumpFacadePerfCounters(is_write, is_local, elapsed_ns,
                           response_bytes > 0 ? response_bytes : request_bytes);
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
        BumpFacadeIpcCounters(is_write, is_local);
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

void ResetFacadePerfCounterTriplet(std::atomic<uint64_t>& ops, std::atomic<uint64_t>& bytes,
                                   std::atomic<uint64_t>& ns)
{
    ops.store(0, std::memory_order_relaxed);
    bytes.store(0, std::memory_order_relaxed);
    ns.store(0, std::memory_order_relaxed);
}

PyObject* PyFacadePerfStatsReset(PyObject* /*self*/, PyObject* /*args*/)
{
    ResetFacadePerfCounterTriplet(g_facade_perf_local_read_ops, g_facade_perf_local_read_bytes,
                                  g_facade_perf_local_read_ns);
    ResetFacadePerfCounterTriplet(g_facade_perf_local_write_ops, g_facade_perf_local_write_bytes,
                                  g_facade_perf_local_write_ns);
    ResetFacadePerfCounterTriplet(g_facade_perf_remote_read_ops, g_facade_perf_remote_read_bytes,
                                  g_facade_perf_remote_read_ns);
    ResetFacadePerfCounterTriplet(g_facade_perf_remote_write_ops, g_facade_perf_remote_write_bytes,
                                  g_facade_perf_remote_write_ns);
    Py_RETURN_NONE;
}

PyObject* MakeFacadePerfEntry(const std::atomic<uint64_t>& ops, const std::atomic<uint64_t>& bytes,
                              const std::atomic<uint64_t>& ns)
{
    const uint64_t ops_v = ops.load(std::memory_order_relaxed);
    const uint64_t bytes_v = bytes.load(std::memory_order_relaxed);
    const uint64_t ns_v = ns.load(std::memory_order_relaxed);
    PyObject* d = PyDict_New();
    PyObject* v = PyLong_FromUnsignedLongLong(ops_v);
    PyDict_SetItemString(d, "ops", v);
    Py_DECREF(v);
    v = PyLong_FromUnsignedLongLong(bytes_v);
    PyDict_SetItemString(d, "bytes", v);
    Py_DECREF(v);
    v = PyFloat_FromDouble(static_cast<double>(ns_v) / 1.0e9);
    PyDict_SetItemString(d, "total_s", v);
    Py_DECREF(v);
    v = PyFloat_FromDouble(ops_v > 0 ? (static_cast<double>(ns_v) / 1.0e6 / ops_v) : 0.0);
    PyDict_SetItemString(d, "avg_ms_per_op", v);
    Py_DECREF(v);
    v = PyFloat_FromDouble(ns_v > 0 ? (static_cast<double>(bytes_v) / 1.0e6) /
                                          (static_cast<double>(ns_v) / 1.0e9)
                                    : 0.0);
    PyDict_SetItemString(d, "instrumented_mb_s", v);
    Py_DECREF(v);
    return d;
}

PyObject* PyFacadePerfStats(PyObject* /*self*/, PyObject* /*args*/)
{
    PyObject* d = PyDict_New();
    PyObject* v = MakeFacadePerfEntry(g_facade_perf_local_read_ops, g_facade_perf_local_read_bytes,
                                      g_facade_perf_local_read_ns);
    PyDict_SetItemString(d, "local_reads", v);
    Py_DECREF(v);
    v = MakeFacadePerfEntry(g_facade_perf_local_write_ops, g_facade_perf_local_write_bytes,
                            g_facade_perf_local_write_ns);
    PyDict_SetItemString(d, "local_writes", v);
    Py_DECREF(v);
    v = MakeFacadePerfEntry(g_facade_perf_remote_read_ops, g_facade_perf_remote_read_bytes,
                            g_facade_perf_remote_read_ns);
    PyDict_SetItemString(d, "remote_reads", v);
    Py_DECREF(v);
    v = MakeFacadePerfEntry(g_facade_perf_remote_write_ops, g_facade_perf_remote_write_bytes,
                            g_facade_perf_remote_write_ns);
    PyDict_SetItemString(d, "remote_writes", v);
    Py_DECREF(v);
    return d;
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


PyObject* PyFacadeBatchReadBlockSplit(PyObject* /*self*/, PyObject* args)
{
    using namespace falconfs::kv;
    int store_node_id = 0;
    const char* request_buf = nullptr;
    Py_ssize_t request_len = 0;
    if (!PyArg_ParseTuple(args, "iy#", &store_node_id, &request_buf, &request_len)) {
        return nullptr;
    }
    BatchReadBlockRequest req;
    if (!req.ParseFromArray(request_buf, static_cast<int>(request_len))) {
        return WrapRpcError("facade_batch_read_block_split", "ParseFromString failed");
    }
    std::shared_ptr<IKVStoreFacade> facade;
    {
        std::lock_guard<std::mutex> lk(g_registry_mu);
        if (!g_registry) return WrapRpcError("facade_batch_read_block_split", "registry not started");
        facade = g_registry->Resolve(store_node_id);
    }
    if (!facade) return WrapRpcError("facade_batch_read_block_split", "unknown store_node_id");
    BatchReadBlockResponse rsp;
    std::vector<std::string> payloads;
    const bool is_local = facade->IsLocal();
    auto t0 = std::chrono::steady_clock::now();
    {
        Py_BEGIN_ALLOW_THREADS;
        facade->BatchReadBlockPayloads(req, &rsp, &payloads);
        Py_END_ALLOW_THREADS;
    }
    auto t1 = std::chrono::steady_clock::now();
    uint64_t bytes = 0;
    for (const auto& p : payloads) bytes += static_cast<uint64_t>(p.size());
    const uint64_t elapsed_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
    BumpFacadePerfCounters(/*is_write=*/false, is_local, elapsed_ns,
                           bytes > 0 ? bytes : FacadeRequestBytes(req));
    for (int i = 0; i < std::max(1, req.items_size()); ++i) BumpFacadeIpcCounters(false, is_local);
    std::string out;
    if (!rsp.SerializeToString(&out)) return WrapRpcError("facade_batch_read_block_split", "SerializeToString failed");
    PyObject* resp_obj = PyBytes_FromStringAndSize(out.data(), static_cast<Py_ssize_t>(out.size()));
    PyObject* list = PyList_New(static_cast<Py_ssize_t>(payloads.size()));
    if (resp_obj == nullptr || list == nullptr) { Py_XDECREF(resp_obj); Py_XDECREF(list); return nullptr; }
    for (Py_ssize_t i = 0; i < static_cast<Py_ssize_t>(payloads.size()); ++i) {
        PyObject* b = PyBytes_FromStringAndSize(payloads[static_cast<size_t>(i)].data(),
                                                static_cast<Py_ssize_t>(payloads[static_cast<size_t>(i)].size()));
        if (b == nullptr) { Py_DECREF(resp_obj); Py_DECREF(list); return nullptr; }
        PyList_SET_ITEM(list, i, b);
    }
    PyObject* tuple = PyTuple_New(2);
    PyTuple_SET_ITEM(tuple, 0, resp_obj);
    PyTuple_SET_ITEM(tuple, 1, list);
    return tuple;
}


PyObject* PyFacadeBatchReadBlockFast(PyObject* /*self*/, PyObject* args)
{
    using namespace falconfs::kv;
    int store_node_id = 0;
    const char* request_buf = nullptr;
    Py_ssize_t request_len = 0;
    if (!PyArg_ParseTuple(args, "iy#", &store_node_id, &request_buf, &request_len)) {
        return nullptr;
    }
    BatchReadBlockRequest req;
    if (!req.ParseFromArray(request_buf, static_cast<int>(request_len))) {
        return WrapRpcError("facade_batch_read_block_fast", "ParseFromString failed");
    }
    std::shared_ptr<IKVStoreFacade> facade;
    {
        std::lock_guard<std::mutex> lk(g_registry_mu);
        if (!g_registry) return WrapRpcError("facade_batch_read_block_fast", "registry not started");
        facade = g_registry->Resolve(store_node_id);
    }
    if (!facade) return WrapRpcError("facade_batch_read_block_fast", "unknown store_node_id");

    BatchReadBlockResponse rsp;
    std::vector<std::string> payloads;
    const bool is_local = facade->IsLocal();
    auto t0 = std::chrono::steady_clock::now();
    {
        Py_BEGIN_ALLOW_THREADS;
        facade->BatchReadBlockPayloads(req, &rsp, &payloads);
        Py_END_ALLOW_THREADS;
    }
    auto t1 = std::chrono::steady_clock::now();

    uint64_t bytes = 0;
    for (const auto& p : payloads) bytes += static_cast<uint64_t>(p.size());
    const uint64_t elapsed_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
    BumpFacadePerfCounters(/*is_write=*/false, is_local, elapsed_ns,
                           bytes > 0 ? bytes : FacadeRequestBytes(req));
    for (int i = 0; i < std::max(1, req.items_size()); ++i) BumpFacadeIpcCounters(false, is_local);

    PyObject* ok_list = PyList_New(static_cast<Py_ssize_t>(rsp.results_size()));
    PyObject* payload_list = PyList_New(static_cast<Py_ssize_t>(payloads.size()));
    if (ok_list == nullptr || payload_list == nullptr) {
        Py_XDECREF(ok_list);
        Py_XDECREF(payload_list);
        return nullptr;
    }
    for (Py_ssize_t i = 0; i < static_cast<Py_ssize_t>(rsp.results_size()); ++i) {
        bool ok = rsp.results(static_cast<int>(i)).has_result() &&
                  rsp.results(static_cast<int>(i)).result().success();
        PyObject* b = PyBool_FromLong(ok ? 1 : 0);
        if (b == nullptr) { Py_DECREF(ok_list); Py_DECREF(payload_list); return nullptr; }
        PyList_SET_ITEM(ok_list, i, b);
    }
    for (Py_ssize_t i = 0; i < static_cast<Py_ssize_t>(payloads.size()); ++i) {
        const std::string& payload = payloads[static_cast<size_t>(i)];
        PyObject* b = PyBytes_FromStringAndSize(payload.data(), static_cast<Py_ssize_t>(payload.size()));
        if (b == nullptr) { Py_DECREF(ok_list); Py_DECREF(payload_list); return nullptr; }
        PyList_SET_ITEM(payload_list, i, b);
    }
    PyObject* tuple = PyTuple_New(2);
    if (tuple == nullptr) { Py_DECREF(ok_list); Py_DECREF(payload_list); return nullptr; }
    PyTuple_SET_ITEM(tuple, 0, ok_list);
    PyTuple_SET_ITEM(tuple, 1, payload_list);
    return tuple;
}


PyObject* PyFacadeBatchReadPayloadsFast(PyObject* /*self*/, PyObject* args)
{
    using namespace falconfs::kv;
    int store_node_id = 0;
    PyObject* offsets_obj = nullptr;
    PyObject* epochs_obj = nullptr;
    PyObject* hashes_obj = nullptr;
    long long block_size_ll = 0;
    if (!PyArg_ParseTuple(args, "iOOOL", &store_node_id, &offsets_obj, &epochs_obj,
                          &hashes_obj, &block_size_ll)) {
        return nullptr;
    }
    if (block_size_ll <= 0) {
        return WrapRpcError("facade_batch_read_payloads_fast", "block_size must be positive");
    }

    PyObject* offsets = PySequence_Fast(offsets_obj, "offsets must be a sequence");
    PyObject* epochs = PySequence_Fast(epochs_obj, "epochs must be a sequence");
    PyObject* hashes = PySequence_Fast(hashes_obj, "block_hashes must be a sequence");
    if (offsets == nullptr || epochs == nullptr || hashes == nullptr) {
        Py_XDECREF(offsets);
        Py_XDECREF(epochs);
        Py_XDECREF(hashes);
        return nullptr;
    }
    const Py_ssize_t n = PySequence_Fast_GET_SIZE(offsets);
    if (PySequence_Fast_GET_SIZE(epochs) != n || PySequence_Fast_GET_SIZE(hashes) != n) {
        Py_DECREF(offsets);
        Py_DECREF(epochs);
        Py_DECREF(hashes);
        return WrapRpcError("facade_batch_read_payloads_fast", "input sequence length mismatch");
    }

    BatchReadBlockRequest req;
    req.mutable_meta()->set_request_id(
        "py_native_batch_read_" + std::to_string(g_native_read_req_seq.fetch_add(1)));
    for (Py_ssize_t i = 0; i < n; ++i) {
        PyObject* offset_obj = PySequence_Fast_GET_ITEM(offsets, i);
        PyObject* epoch_obj = PySequence_Fast_GET_ITEM(epochs, i);
        PyObject* hash_obj = PySequence_Fast_GET_ITEM(hashes, i);
        const long long pool_offset = PyLong_AsLongLong(offset_obj);
        if (PyErr_Occurred()) {
            Py_DECREF(offsets);
            Py_DECREF(epochs);
            Py_DECREF(hashes);
            return nullptr;
        }
        const long long store_epoch = PyLong_AsLongLong(epoch_obj);
        if (PyErr_Occurred()) {
            Py_DECREF(offsets);
            Py_DECREF(epochs);
            Py_DECREF(hashes);
            return nullptr;
        }
        const char* hash_buf = nullptr;
        Py_ssize_t hash_len = 0;
        if (PyBytes_AsStringAndSize(hash_obj, const_cast<char**>(&hash_buf), &hash_len) != 0) {
            Py_DECREF(offsets);
            Py_DECREF(epochs);
            Py_DECREF(hashes);
            return nullptr;
        }
        ReadItem* it = req.add_items();
        it->set_block_hash(hash_buf, static_cast<size_t>(hash_len));
        it->set_pool_offset(pool_offset);
        it->set_block_size(static_cast<int32_t>(block_size_ll));
        it->set_expected_store_epoch(store_epoch);
        it->set_expected_version(0);
    }
    Py_DECREF(offsets);
    Py_DECREF(epochs);
    Py_DECREF(hashes);

    std::shared_ptr<IKVStoreFacade> facade;
    {
        std::lock_guard<std::mutex> lk(g_registry_mu);
        if (!g_registry) return WrapRpcError("facade_batch_read_payloads_fast", "registry not started");
        facade = g_registry->Resolve(store_node_id);
    }
    if (!facade) return WrapRpcError("facade_batch_read_payloads_fast", "unknown store_node_id");

    BatchReadBlockResponse rsp;
    std::vector<std::string> payloads;
    const bool is_local = facade->IsLocal();
    auto t0 = std::chrono::steady_clock::now();
    {
        Py_BEGIN_ALLOW_THREADS;
        facade->BatchReadBlockPayloads(req, &rsp, &payloads);
        Py_END_ALLOW_THREADS;
    }
    auto t1 = std::chrono::steady_clock::now();

    uint64_t bytes = 0;
    for (const auto& p : payloads) bytes += static_cast<uint64_t>(p.size());
    const uint64_t elapsed_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
    BumpFacadePerfCounters(/*is_write=*/false, is_local, elapsed_ns,
                           bytes > 0 ? bytes : FacadeRequestBytes(req));
    for (int i = 0; i < std::max(1, req.items_size()); ++i) BumpFacadeIpcCounters(false, is_local);

    PyObject* ok_list = PyList_New(static_cast<Py_ssize_t>(rsp.results_size()));
    PyObject* payload_list = PyList_New(static_cast<Py_ssize_t>(payloads.size()));
    if (ok_list == nullptr || payload_list == nullptr) {
        Py_XDECREF(ok_list);
        Py_XDECREF(payload_list);
        return nullptr;
    }
    for (Py_ssize_t i = 0; i < static_cast<Py_ssize_t>(rsp.results_size()); ++i) {
        const bool ok = rsp.results(static_cast<int>(i)).has_result() &&
                        rsp.results(static_cast<int>(i)).result().success();
        PyObject* b = PyBool_FromLong(ok ? 1 : 0);
        if (b == nullptr) { Py_DECREF(ok_list); Py_DECREF(payload_list); return nullptr; }
        PyList_SET_ITEM(ok_list, i, b);
    }
    for (Py_ssize_t i = 0; i < static_cast<Py_ssize_t>(payloads.size()); ++i) {
        const std::string& payload = payloads[static_cast<size_t>(i)];
        PyObject* b = PyBytes_FromStringAndSize(payload.data(), static_cast<Py_ssize_t>(payload.size()));
        if (b == nullptr) { Py_DECREF(ok_list); Py_DECREF(payload_list); return nullptr; }
        PyList_SET_ITEM(payload_list, i, b);
    }
    PyObject* tuple = PyTuple_New(2);
    if (tuple == nullptr) { Py_DECREF(ok_list); Py_DECREF(payload_list); return nullptr; }
    PyTuple_SET_ITEM(tuple, 0, ok_list);
    PyTuple_SET_ITEM(tuple, 1, payload_list);
    return tuple;
}

PyObject* PyFacadeBatchWriteBlockPayloads(PyObject* /*self*/, PyObject* args)
{
    using namespace falconfs::kv;
    int store_node_id = 0;
    const char* request_buf = nullptr;
    Py_ssize_t request_len = 0;
    PyObject* payload_list = nullptr;
    if (!PyArg_ParseTuple(args, "iy#O", &store_node_id, &request_buf, &request_len, &payload_list)) {
        return nullptr;
    }
    if (!PySequence_Check(payload_list)) {
        return WrapRpcError("facade_batch_write_block_payloads", "payloads must be a sequence");
    }
    BatchWriteBlockRequest req;
    if (!req.ParseFromArray(request_buf, static_cast<int>(request_len))) {
        return WrapRpcError("facade_batch_write_block_payloads", "ParseFromString failed");
    }
    std::vector<std::string> payloads;
    const Py_ssize_t n = PySequence_Size(payload_list);
    if (n != req.items_size()) {
        return WrapRpcError("facade_batch_write_block_payloads", "payload count mismatch");
    }
    payloads.reserve(static_cast<size_t>(n));
    for (Py_ssize_t i = 0; i < n; ++i) {
        PyObject* item = PySequence_GetItem(payload_list, i);
        if (item == nullptr) return nullptr;
        char* buf = nullptr;
        Py_ssize_t len = 0;
        if (PyBytes_AsStringAndSize(item, &buf, &len) != 0) { Py_DECREF(item); return nullptr; }
        payloads.emplace_back(buf, static_cast<size_t>(len));
        Py_DECREF(item);
    }
    std::shared_ptr<IKVStoreFacade> facade;
    {
        std::lock_guard<std::mutex> lk(g_registry_mu);
        if (!g_registry) return WrapRpcError("facade_batch_write_block_payloads", "registry not started");
        facade = g_registry->Resolve(store_node_id);
    }
    if (!facade) return WrapRpcError("facade_batch_write_block_payloads", "unknown store_node_id");
    BatchWriteBlockResponse rsp;
    const bool is_local = facade->IsLocal();
    uint64_t bytes = 0;
    for (const auto& p : payloads) bytes += static_cast<uint64_t>(p.size());
    auto t0 = std::chrono::steady_clock::now();
    {
        Py_BEGIN_ALLOW_THREADS;
        facade->BatchWriteBlockPayloads(req, payloads, &rsp);
        Py_END_ALLOW_THREADS;
    }
    auto t1 = std::chrono::steady_clock::now();
    const uint64_t elapsed_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
    BumpFacadePerfCounters(/*is_write=*/true, is_local, elapsed_ns, bytes);
    for (int i = 0; i < std::max(1, req.items_size()); ++i) BumpFacadeIpcCounters(true, is_local);
    std::string out;
    if (!rsp.SerializeToString(&out)) return WrapRpcError("facade_batch_write_block_payloads", "SerializeToString failed");
    return PyBytes_FromStringAndSize(out.data(), static_cast<Py_ssize_t>(out.size()));
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

PyObject* PyFacadeWriteBlockPayload(PyObject* /*self*/, PyObject* args)
{
    using namespace falconfs::kv;
    int store_node_id = 0;
    const char* request_buf = nullptr;
    Py_ssize_t request_len = 0;
    const char* payload_buf = nullptr;
    Py_ssize_t payload_len = 0;
    if (!PyArg_ParseTuple(args, "iy#y#", &store_node_id, &request_buf, &request_len,
                          &payload_buf, &payload_len)) {
        return nullptr;
    }
    WriteBlockRequest req;
    if (!req.ParseFromArray(request_buf, static_cast<int>(request_len))) {
        return WrapRpcError("facade_write_block_payload", "ParseFromString failed");
    }
    std::shared_ptr<IKVStoreFacade> facade;
    {
        std::lock_guard<std::mutex> lk(g_registry_mu);
        if (!g_registry) {
            return WrapRpcError("facade_write_block_payload", "registry not started");
        }
        facade = g_registry->Resolve(store_node_id);
    }
    if (!facade) {
        return WrapRpcError("facade_write_block_payload", "unknown store_node_id");
    }

    WriteBlockResponse rsp;
    const bool is_local = facade->IsLocal();
    auto t0 = std::chrono::steady_clock::now();
    {
        Py_BEGIN_ALLOW_THREADS;
        facade->WriteBlockPayload(req, payload_buf, static_cast<size_t>(payload_len), &rsp);
        Py_END_ALLOW_THREADS;
    }
    auto t1 = std::chrono::steady_clock::now();
    const uint64_t elapsed_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
    BumpFacadePerfCounters(/*is_write=*/true, is_local, elapsed_ns,
                           static_cast<uint64_t>(payload_len));
    BumpFacadeIpcCounters(/*is_write=*/true, is_local);

    std::string out;
    if (!rsp.SerializeToString(&out)) {
        return WrapRpcError("facade_write_block_payload", "SerializeToString failed");
    }
    return PyBytes_FromStringAndSize(out.data(), static_cast<Py_ssize_t>(out.size()));
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

template <typename Request, typename Response, typename CallFn>
PyObject* DoFacadeReadSplit(PyObject* args, const char* method_name, CallFn&& call_fn)
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
    const bool is_local = facade->IsLocal();
    const uint64_t request_bytes = FacadeRequestBytes(req);
    std::string payload;
    auto t0 = std::chrono::steady_clock::now();
    {
        Py_BEGIN_ALLOW_THREADS;
        if constexpr (std::is_same_v<Request, falconfs::kv::ReadBlockRequest> &&
                      std::is_same_v<Response, falconfs::kv::ReadBlockResponse>) {
            facade->ReadBlockPayload(req, &rsp, &payload);
        } else {
            call_fn(facade, req, &rsp);
        }
        Py_END_ALLOW_THREADS;
    }
    auto t1 = std::chrono::steady_clock::now();

    if constexpr (std::is_same_v<Response, falconfs::kv::ReadBlockResponse>) {
        if (payload.empty() && rsp.has_result() && !rsp.result().payload().empty()) {
            payload = std::move(*rsp.mutable_result()->mutable_payload());
            rsp.mutable_result()->clear_payload();
        }
    } else if constexpr (std::is_same_v<Response, falconfs::kv::ReadFromSSDResponse>) {
        if (rsp.has_result() && !rsp.result().payload().empty()) {
            payload = std::move(*rsp.mutable_result()->mutable_payload());
            rsp.mutable_result()->clear_payload();
        }
    }

    const uint64_t elapsed_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
    const uint64_t byte_count = !payload.empty() ? static_cast<uint64_t>(payload.size())
                                                : request_bytes;
    BumpFacadePerfCounters(/*is_write=*/false, is_local, elapsed_ns, byte_count);
    BumpFacadeIpcCounters(/*is_write=*/false, is_local);

    std::string out;
    if (!rsp.SerializeToString(&out)) {
        return WrapRpcError(method_name, "SerializeToString failed");
    }
    PyObject* resp_obj = PyBytes_FromStringAndSize(out.data(), static_cast<Py_ssize_t>(out.size()));
    PyObject* payload_obj = PyBytes_FromStringAndSize(payload.data(),
                                                     static_cast<Py_ssize_t>(payload.size()));
    if (resp_obj == nullptr || payload_obj == nullptr) {
        Py_XDECREF(resp_obj);
        Py_XDECREF(payload_obj);
        return nullptr;
    }
    PyObject* tuple = PyTuple_New(2);
    PyTuple_SET_ITEM(tuple, 0, resp_obj);
    PyTuple_SET_ITEM(tuple, 1, payload_obj);
    return tuple;
}

PyObject* PyFacadeReadBlockSplit(PyObject* /*self*/, PyObject* args)
{
    using namespace falconfs::kv;
    return DoFacadeReadSplit<ReadBlockRequest, ReadBlockResponse>(
        args, "facade_read_block_split",
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

PyObject* PyFacadeReadFromSSDSplit(PyObject* /*self*/, PyObject* args)
{
    using namespace falconfs::kv;
    return DoFacadeReadSplit<ReadFromSSDRequest, ReadFromSSDResponse>(
        args, "facade_read_from_ssd_split",
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
    {"facade_perf_stats_reset", PyFacadePerfStatsReset, METH_VARARGS,
     "Resets C++ facade local/remote timing counters (test hook)."},
    {"facade_perf_stats", PyFacadePerfStats, METH_VARARGS,
     "Returns C++ facade timing and byte counters split by local/remote read/write path."},
    {"facade_batch_write_block", PyFacadeBatchWriteBlock, METH_VARARGS,
     "Calls facade BatchWriteBlock for store_node_id."},
    {"facade_batch_read_block", PyFacadeBatchReadBlock, METH_VARARGS,
     "Calls facade BatchReadBlock for store_node_id."},
    {"facade_batch_read_block_split", PyFacadeBatchReadBlockSplit, METH_VARARGS,
     "Calls facade BatchReadBlock and returns (response, payloads)."},
    {"facade_batch_read_block_fast", PyFacadeBatchReadBlockFast, METH_VARARGS,
     "Calls facade BatchReadBlock and returns (success_flags, payloads) without protobuf response serialization."},
    {"facade_batch_read_payloads_fast", PyFacadeBatchReadPayloadsFast, METH_VARARGS,
     "Builds a BatchReadBlock request in C++ and returns (success_flags, payloads)."},
    {"facade_batch_write_block_payloads", PyFacadeBatchWriteBlockPayloads, METH_VARARGS,
     "Calls facade BatchWriteBlock with payload attachment frames."},
    {"facade_batch_read_from_ssd", PyFacadeBatchReadFromSSD, METH_VARARGS,
     "Calls facade BatchReadFromSSD for store_node_id."},
    {"facade_write_block", PyFacadeWriteBlock, METH_VARARGS,
     "Calls facade WriteBlock for store_node_id."},
    {"facade_write_block_payload", PyFacadeWriteBlockPayload, METH_VARARGS,
     "Calls facade WriteBlock with request protobuf metadata and payload bytes split."},
    {"facade_read_block", PyFacadeReadBlock, METH_VARARGS,
     "Calls facade ReadBlock for store_node_id."},
    {"facade_read_block_split", PyFacadeReadBlockSplit, METH_VARARGS,
     "Calls facade ReadBlock and returns (response_without_payload, payload)."},
    {"facade_read_from_ssd", PyFacadeReadFromSSD, METH_VARARGS,
     "Calls facade ReadFromSSD for store_node_id."},
    {"facade_read_from_ssd_split", PyFacadeReadFromSSDSplit, METH_VARARGS,
     "Calls facade ReadFromSSD and returns (response_without_payload, payload)."},
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
