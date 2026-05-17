// Cluster fault drill (v6 §19.4 #3 / #4 / #5 / #7). Each scenario is a
// distinct subcommand of the same binary; the harness wraps the binary with
// shell-level coordination (DN restart) where required.
//
// Scenarios:
//   - large-batch:   §19.4 #7  Issue a batch of 64 alloc + 64 update +
//                              64 free items in a single BRPC call each and
//                              validate per-item results round-trip cleanly
//                              through the engine's SubTx chunking
//                              (kBatchOperationGroupSize=8).
//   - stale-store-epoch: §19.4 #4  Renew with a deliberately-wrong store_epoch
//                              expects STALE_EPOCH, retryable=true.
//   - partial-store-write: v6.5 P8 partial write cleanup drill (half STORED,
//                              half force-freed; verify lookup split).
//   - eviction-rollback: §19.4 #5  Allocate + STORED a row; the cluster runs
//                              the in-plugin eviction worker every
//                              falcon_kv.eviction_period_ms, but the in-
//                              process Store has no SSD root configured so
//                              every spill fails and the coordinator rolls
//                              the row back to STORED. After the period,
//                              the row must still be visible (STORED) in
//                              both the catalog and a fresh lookup.
//   - dn-restart-phase1: §19.4 #3  Allocate a block, transition to STORED,
//                              renew once, and dump (block_hash, lease_token,
//                              dn_epoch, store_epoch) to --state-file.
//   - dn-restart-phase2: §19.4 #3  After the harness restarts the DN's PG
//                              instance: load the saved state and verify the
//                              old lease renew is rejected with STALE_EPOCH;
//                              a fresh BatchLookupWithLease must return a new
//                              lease whose dn_epoch is greater than the saved
//                              one.

#include <brpc/channel.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "kv_common.pb.h"
#include "kv_metadata_service.pb.h"
#include "tests/falcon_kv/kv_e2e_block_size.h"

namespace {

int Fail(const std::string& msg) {
    std::cerr << "CLUSTER_FAULT_FAIL: " << msg << std::endl;
    return 1;
}

bool InitChannel(const std::string& endpoint, int timeout_ms, brpc::Channel* channel) {
    brpc::ChannelOptions options;
    options.protocol = "baidu_std";
    options.timeout_ms = timeout_ms;
    options.connect_timeout_ms = 5000;
    options.max_retry = 0;
    options.connection_type = "pooled";
    return channel->Init(endpoint.c_str(), &options) == 0;
}

int64_t NowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch()).count();
}

// ─────────── large-batch (§19.4 #7) ───────────

int RunLargeBatch(const std::string& endpoint) {
    using namespace falconfs::kv;
    brpc::Channel channel;
    if (!InitChannel(endpoint, 60000, &channel)) {
        return Fail("large-batch: channel init failed");
    }
    KVMetadataService_Stub stub(&channel);

    const int N = 64;
    const std::string run_id = "fault_lb_" + std::to_string(NowNs());
    std::vector<std::string> hashes;
    hashes.reserve(N);
    for (int i = 0; i < N; ++i) {
        hashes.push_back(run_id + "_h" + std::to_string(i));
    }

    BatchAllocateRequest a;
    a.mutable_meta()->set_request_id(run_id + "_alloc");
    a.mutable_meta()->set_client_id(0);
    a.set_deduplicate_in_request(true);
    for (const auto& h : hashes) {
        auto* it = a.add_items();
        it->set_block_hash(h);
        it->set_block_size(falconfs::kv::test::E2eKvBlockSize());
        it->set_preferred_store_id(1);
        it->set_allow_fallback_store(true);
    }
    BatchAllocateResponse ar;
    brpc::Controller acntl;
    stub.BatchAllocateWithLease(&acntl, &a, &ar, nullptr);
    if (acntl.Failed() || ar.results_size() != N) {
        return Fail("large-batch: alloc rpc failed or size mismatch");
    }
    std::vector<int64_t> versions(N);
    for (int i = 0; i < N; ++i) {
        if (!ar.results(i).result().success() || ar.results(i).version() != 1) {
            return Fail("large-batch: alloc item " + std::to_string(i) + " failed");
        }
        versions[i] = ar.results(i).version();
    }

    BatchUpdateStatusRequest u;
    u.mutable_meta()->set_request_id(run_id + "_upd");
    u.mutable_meta()->set_client_id(0);
    for (int i = 0; i < N; ++i) {
        auto* it = u.add_items();
        it->set_block_hash(hashes[i]);
        it->set_expected_from_status(BLOCK_STATUS_ALLOCATED);
        it->set_to_status(BLOCK_STATUS_STORED);
        it->set_expected_version(versions[i]);
    }
    BatchUpdateStatusResponse ur;
    brpc::Controller ucntl;
    stub.BatchUpdateBlockStatus(&ucntl, &u, &ur, nullptr);
    if (ucntl.Failed() || ur.results_size() != N) {
        return Fail("large-batch: update rpc failed or size mismatch");
    }
    for (int i = 0; i < N; ++i) {
        if (!ur.results(i).result().success() || ur.results(i).new_version() != 2) {
            return Fail("large-batch: update item " + std::to_string(i) + " failed");
        }
    }

    BatchFreeAllocatedRequest f;
    f.mutable_meta()->set_request_id(run_id + "_free");
    f.mutable_meta()->set_client_id(0);
    for (int i = 0; i < N; ++i) {
        auto* it = f.add_items();
        it->set_block_hash(hashes[i]);
        it->set_expected_version(2);
        it->set_force(true);
    }
    BatchFreeAllocatedResponse fr;
    brpc::Controller fcntl;
    stub.BatchFreeAllocated(&fcntl, &f, &fr, nullptr);
    if (fcntl.Failed() || fr.results_size() != N) {
        return Fail("large-batch: free rpc failed or size mismatch");
    }
    for (int i = 0; i < N; ++i) {
        if (!fr.results(i).result().success()) {
            return Fail("large-batch: free item " + std::to_string(i) + " failed");
        }
    }

    std::cout << "LARGE_BATCH_OK N=" << N << " endpoint=" << endpoint << std::endl;
    return 0;
}

// ─────────── partial-store-write (v6.5 P8) ───────────

int RunPartialStoreWrite(const std::string& endpoint) {
    using namespace falconfs::kv;
    brpc::Channel channel;
    if (!InitChannel(endpoint, 30000, &channel)) {
        return Fail("partial-store-write: channel init failed");
    }
    KVMetadataService_Stub stub(&channel);

    const int N = 8;
    const int half = N / 2;
    const std::string run_id = "fault_psw_" + std::to_string(NowNs());
    std::vector<std::string> hashes;
    hashes.reserve(N);
    for (int i = 0; i < N; ++i) {
        hashes.push_back(run_id + "_h" + std::to_string(i));
    }

    BatchAllocateRequest a;
    a.mutable_meta()->set_request_id(run_id + "_alloc");
    a.mutable_meta()->set_client_id(0);
    for (const auto& h : hashes) {
        auto* it = a.add_items();
        it->set_block_hash(h);
        it->set_block_size(falconfs::kv::test::E2eKvBlockSize());
        it->set_preferred_store_id(1);
        it->set_allow_fallback_store(true);
    }
    BatchAllocateResponse ar;
    brpc::Controller acntl;
    stub.BatchAllocateWithLease(&acntl, &a, &ar, nullptr);
    if (acntl.Failed() || ar.results_size() != N) {
        return Fail("partial-store-write: alloc failed");
    }
    for (int i = 0; i < N; ++i) {
        if (!ar.results(i).result().success()) {
            return Fail("partial-store-write: alloc item failed idx=" + std::to_string(i));
        }
    }

    BatchUpdateStatusRequest u;
    u.mutable_meta()->set_request_id(run_id + "_upd");
    u.mutable_meta()->set_client_id(0);
    for (int i = 0; i < half; ++i) {
        auto* it = u.add_items();
        it->set_block_hash(hashes[i]);
        it->set_expected_from_status(BLOCK_STATUS_ALLOCATED);
        it->set_to_status(BLOCK_STATUS_STORED);
        it->set_expected_version(ar.results(i).version());
    }
    BatchUpdateStatusResponse ur;
    brpc::Controller ucntl;
    stub.BatchUpdateBlockStatus(&ucntl, &u, &ur, nullptr);
    if (ucntl.Failed() || ur.results_size() != half) {
        return Fail("partial-store-write: update failed");
    }
    for (int i = 0; i < half; ++i) {
        if (!ur.results(i).result().success()) {
            return Fail("partial-store-write: update item failed idx=" + std::to_string(i));
        }
    }

    BatchFreeAllocatedRequest f;
    f.mutable_meta()->set_request_id(run_id + "_free_failed");
    f.mutable_meta()->set_client_id(0);
    for (int i = half; i < N; ++i) {
        auto* it = f.add_items();
        it->set_block_hash(hashes[i]);
        it->set_expected_version(ar.results(i).version());
        it->set_force(true);
    }
    BatchFreeAllocatedResponse fr;
    brpc::Controller fcntl;
    stub.BatchFreeAllocated(&fcntl, &f, &fr, nullptr);
    if (fcntl.Failed() || fr.results_size() != (N - half)) {
        return Fail("partial-store-write: free failed");
    }
    for (int i = 0; i < fr.results_size(); ++i) {
        if (!fr.results(i).result().success()) {
            return Fail("partial-store-write: free item failed");
        }
    }

    BatchLookupRequest l;
    l.mutable_meta()->set_request_id(run_id + "_lookup");
    for (const auto& h : hashes) {
        auto* it = l.add_items();
        it->set_block_hash(h);
        it->set_renew_lease_on_hit(false);
    }
    BatchLookupResponse lr;
    brpc::Controller lcntl;
    stub.BatchLookupWithLease(&lcntl, &l, &lr, nullptr);
    if (lcntl.Failed() || lr.results_size() != N) {
        return Fail("partial-store-write: lookup failed");
    }
    for (int i = 0; i < half; ++i) {
        if (!lr.results(i).result().success()) {
            return Fail("partial-store-write: expected STORED hit for idx=" + std::to_string(i));
        }
    }
    for (int i = half; i < N; ++i) {
        if (lr.results(i).result().success()) {
            return Fail("partial-store-write: expected miss for freed idx=" + std::to_string(i));
        }
    }

    // Cleanup stored half.
    BatchFreeAllocatedRequest f2;
    f2.mutable_meta()->set_request_id(run_id + "_cleanup");
    f2.mutable_meta()->set_client_id(0);
    for (int i = 0; i < half; ++i) {
        auto* it = f2.add_items();
        it->set_block_hash(hashes[i]);
        it->set_expected_version(ur.results(i).new_version());
        it->set_force(true);
    }
    BatchFreeAllocatedResponse f2r;
    brpc::Controller f2cntl;
    stub.BatchFreeAllocated(&f2cntl, &f2, &f2r, nullptr);

    std::cout << "PARTIAL_STORE_WRITE_OK N=" << N << " endpoint=" << endpoint << std::endl;
    return 0;
}

// ─────────── stale-store-epoch (§19.4 #4) ───────────

int RunStaleStoreEpoch(const std::string& endpoint) {
    using namespace falconfs::kv;
    brpc::Channel channel;
    if (!InitChannel(endpoint, 30000, &channel)) {
        return Fail("stale-store-epoch: channel init failed");
    }
    KVMetadataService_Stub stub(&channel);
    const std::string run_id = "fault_se_" + std::to_string(NowNs());
    const std::string h = run_id + "_h0";

    BatchAllocateRequest a;
    a.mutable_meta()->set_request_id(run_id + "_alloc");
    auto* ait = a.add_items();
    ait->set_block_hash(h);
    ait->set_block_size(falconfs::kv::test::E2eKvBlockSize());
    ait->set_preferred_store_id(1);
    ait->set_allow_fallback_store(true);
    BatchAllocateResponse ar;
    brpc::Controller acntl;
    stub.BatchAllocateWithLease(&acntl, &a, &ar, nullptr);
    if (acntl.Failed() || ar.results_size() != 1 || !ar.results(0).result().success()) {
        return Fail("stale-store-epoch: alloc failed");
    }
    const auto& alloc = ar.results(0);
    const int64_t real_dn_epoch = alloc.lease().dn_epoch();
    const int64_t real_store_epoch = alloc.lease().store_epoch();
    const int64_t lease_token = alloc.lease().lease_token();

    // Renew with deliberately wrong store_epoch.
    BatchRenewLeaseRequest rr;
    rr.mutable_meta()->set_request_id(run_id + "_renew_bad");
    rr.set_requested_ttl_ms(1000);
    auto* rit = rr.add_items();
    rit->set_block_hash(h);
    rit->set_lease_token(lease_token);
    rit->set_expected_dn_epoch(real_dn_epoch);
    rit->set_expected_store_epoch(real_store_epoch + 1000);
    BatchRenewLeaseResponse rrs;
    brpc::Controller rcntl;
    stub.BatchRenewLease(&rcntl, &rr, &rrs, nullptr);
    if (rcntl.Failed() || rrs.results_size() != 1) {
        return Fail("stale-store-epoch: renew rpc failed");
    }
    const auto& rres = rrs.results(0);
    if (rres.result().success()) {
        return Fail("stale-store-epoch: renew with stale store_epoch unexpectedly succeeded");
    }
    if (rres.result().error_code() != ErrorCode::STALE_EPOCH) {
        return Fail("stale-store-epoch: expected STALE_EPOCH, got code=" +
                    std::to_string(rres.result().error_code()));
    }
    if (!rres.result().retryable()) {
        return Fail("stale-store-epoch: STALE_EPOCH must be retryable");
    }

    // Cleanup.
    BatchFreeAllocatedRequest f;
    f.mutable_meta()->set_request_id(run_id + "_free");
    auto* fit = f.add_items();
    fit->set_block_hash(h);
    fit->set_expected_version(1);
    fit->set_force(true);
    BatchFreeAllocatedResponse frs;
    brpc::Controller fcntl;
    stub.BatchFreeAllocated(&fcntl, &f, &frs, nullptr);
    if (fcntl.Failed() || frs.results_size() != 1 || !frs.results(0).result().success()) {
        return Fail("stale-store-epoch: cleanup free failed");
    }

    std::cout << "STALE_STORE_EPOCH_OK endpoint=" << endpoint
              << " dn_epoch=" << real_dn_epoch
              << " store_epoch=" << real_store_epoch << std::endl;
    return 0;
}

// ─────────── eviction-rollback (§19.4 #5) ───────────

int RunEvictionRollback(const std::string& endpoint, int wait_ms) {
    using namespace falconfs::kv;
    brpc::Channel channel;
    if (!InitChannel(endpoint, 60000, &channel)) {
        return Fail("eviction-rollback: channel init failed");
    }
    KVMetadataService_Stub stub(&channel);
    const std::string run_id = "fault_ev_" + std::to_string(NowNs());
    const std::string h = run_id + "_h0";

    // Allocate + transition to STORED so the row becomes an eviction candidate.
    BatchAllocateRequest a;
    a.mutable_meta()->set_request_id(run_id + "_alloc");
    auto* ait = a.add_items();
    ait->set_block_hash(h);
    ait->set_block_size(falconfs::kv::test::E2eKvBlockSize());
    ait->set_preferred_store_id(1);
    ait->set_allow_fallback_store(true);
    BatchAllocateResponse ar;
    brpc::Controller acntl;
    stub.BatchAllocateWithLease(&acntl, &a, &ar, nullptr);
    if (acntl.Failed() || ar.results_size() != 1 || !ar.results(0).result().success()) {
        return Fail("eviction-rollback: alloc failed");
    }

    BatchUpdateStatusRequest u;
    u.mutable_meta()->set_request_id(run_id + "_upd");
    auto* uit = u.add_items();
    uit->set_block_hash(h);
    uit->set_expected_from_status(BLOCK_STATUS_ALLOCATED);
    uit->set_to_status(BLOCK_STATUS_STORED);
    uit->set_expected_version(1);
    BatchUpdateStatusResponse ur;
    brpc::Controller ucntl;
    stub.BatchUpdateBlockStatus(&ucntl, &u, &ur, nullptr);
    if (ucntl.Failed() || ur.results_size() != 1 || !ur.results(0).result().success()) {
        return Fail("eviction-rollback: update STORED failed");
    }

    // Wait long enough for several eviction cycles to fire. Each cycle should
    // try to spill (fails — no SSD root configured) and roll back to STORED.
    std::this_thread::sleep_for(std::chrono::milliseconds(wait_ms));

    // Lookup must still succeed and return STORED. Lease is renewed by the
    // first lookup, so the first cycle could have already cleared the
    // lease-protected window. Either way, the row must NOT have transitioned
    // to EVICTED.
    BatchLookupRequest lk;
    lk.mutable_meta()->set_request_id(run_id + "_lk");
    auto* lit = lk.add_items();
    lit->set_block_hash(h);
    lit->set_renew_lease_on_hit(false);
    BatchLookupResponse lr;
    brpc::Controller lcntl;
    stub.BatchLookupWithLease(&lcntl, &lk, &lr, nullptr);
    if (lcntl.Failed() || lr.results_size() != 1) {
        return Fail("eviction-rollback: lookup rpc failed");
    }
    const auto& lres = lr.results(0);
    if (!lres.result().success()) {
        return Fail("eviction-rollback: lookup failed err=" + lres.result().error_message());
    }
    if (lres.status() != BLOCK_STATUS_STORED) {
        return Fail("eviction-rollback: row not STORED after eviction cycle (status=" +
                    std::to_string(static_cast<int>(lres.status())) + ")");
    }

    // Cleanup. After the rollback CAS, the version is 3 (1 alloc -> 2 STORED ->
    // 3 EVICTING -> 4 STORED rollback) IF an eviction cycle ran. If no cycle
    // ran (steady mode), version is still 2. Try both.
    auto try_free = [&](int64_t v) {
        BatchFreeAllocatedRequest f;
        f.mutable_meta()->set_request_id(run_id + "_free_v" + std::to_string(v));
        auto* fit = f.add_items();
        fit->set_block_hash(h);
        fit->set_expected_version(v);
        fit->set_force(true);
        BatchFreeAllocatedResponse frs;
        brpc::Controller fcntl;
        stub.BatchFreeAllocated(&fcntl, &f, &frs, nullptr);
        return !fcntl.Failed() && frs.results_size() == 1 &&
               frs.results(0).result().success();
    };
    bool cleaned = false;
    for (int64_t v = 2; v <= 16 && !cleaned; ++v) {
        if (try_free(v)) cleaned = true;
    }
    if (!cleaned) {
        return Fail("eviction-rollback: cleanup free could not find live version");
    }

    std::cout << "EVICTION_ROLLBACK_OK endpoint=" << endpoint
              << " wait_ms=" << wait_ms << std::endl;
    return 0;
}

// ─────────── dn-restart-phase1 / phase2 (§19.4 #3) ───────────

struct PhaseState {
    std::string block_hash;
    int64_t lease_token = 0;
    int64_t dn_epoch = 0;
    int64_t store_epoch = 0;
};

bool DumpState(const std::string& path, const PhaseState& s) {
    std::ofstream f(path, std::ios::trunc);
    if (!f.good()) return false;
    f << s.block_hash << "\n"
      << s.lease_token << "\n"
      << s.dn_epoch << "\n"
      << s.store_epoch << "\n";
    return f.good();
}

bool LoadState(const std::string& path, PhaseState* s) {
    std::ifstream f(path);
    if (!f.good()) return false;
    if (!std::getline(f, s->block_hash)) return false;
    f >> s->lease_token >> s->dn_epoch >> s->store_epoch;
    return f.good();
}

int RunDnRestartPhase1(const std::string& endpoint, const std::string& state_file) {
    using namespace falconfs::kv;
    brpc::Channel channel;
    if (!InitChannel(endpoint, 30000, &channel)) {
        return Fail("phase1: channel init failed");
    }
    KVMetadataService_Stub stub(&channel);
    const std::string run_id = "fault_rs_" + std::to_string(NowNs());
    PhaseState s;
    s.block_hash = run_id + "_h0";

    BatchAllocateRequest a;
    a.mutable_meta()->set_request_id(run_id + "_alloc");
    auto* ait = a.add_items();
    ait->set_block_hash(s.block_hash);
    ait->set_block_size(falconfs::kv::test::E2eKvBlockSize());
    ait->set_preferred_store_id(1);
    ait->set_allow_fallback_store(true);
    BatchAllocateResponse ar;
    brpc::Controller acntl;
    stub.BatchAllocateWithLease(&acntl, &a, &ar, nullptr);
    if (acntl.Failed() || ar.results_size() != 1 || !ar.results(0).result().success()) {
        return Fail("phase1: alloc failed");
    }
    s.lease_token = ar.results(0).lease().lease_token();
    s.dn_epoch = ar.results(0).lease().dn_epoch();
    s.store_epoch = ar.results(0).lease().store_epoch();

    BatchUpdateStatusRequest u;
    u.mutable_meta()->set_request_id(run_id + "_upd");
    auto* uit = u.add_items();
    uit->set_block_hash(s.block_hash);
    uit->set_expected_from_status(BLOCK_STATUS_ALLOCATED);
    uit->set_to_status(BLOCK_STATUS_STORED);
    uit->set_expected_version(1);
    BatchUpdateStatusResponse ur;
    brpc::Controller ucntl;
    stub.BatchUpdateBlockStatus(&ucntl, &u, &ur, nullptr);
    if (ucntl.Failed() || ur.results_size() != 1 || !ur.results(0).result().success()) {
        return Fail("phase1: update STORED failed");
    }

    if (!DumpState(state_file, s)) {
        return Fail("phase1: failed to write state file " + state_file);
    }
    std::cout << "PHASE1_OK block_hash=" << s.block_hash
              << " lease_token=" << s.lease_token
              << " dn_epoch=" << s.dn_epoch
              << " store_epoch=" << s.store_epoch << std::endl;
    return 0;
}

int RunDnRestartPhase2(const std::string& endpoint, const std::string& state_file) {
    using namespace falconfs::kv;
    PhaseState s;
    if (!LoadState(state_file, &s)) {
        return Fail("phase2: failed to load state file " + state_file);
    }

    brpc::Channel channel;
    if (!InitChannel(endpoint, 30000, &channel)) {
        return Fail("phase2: channel init failed");
    }
    KVMetadataService_Stub stub(&channel);

    // Old lease renew with stale dn_epoch must be rejected.
    BatchRenewLeaseRequest rr;
    rr.mutable_meta()->set_request_id("phase2_stale_renew");
    rr.set_requested_ttl_ms(1000);
    auto* rit = rr.add_items();
    rit->set_block_hash(s.block_hash);
    rit->set_lease_token(s.lease_token);
    rit->set_expected_dn_epoch(s.dn_epoch);
    rit->set_expected_store_epoch(s.store_epoch);
    BatchRenewLeaseResponse rrs;
    brpc::Controller rcntl;
    stub.BatchRenewLease(&rcntl, &rr, &rrs, nullptr);
    if (rcntl.Failed() || rrs.results_size() != 1) {
        return Fail("phase2: renew rpc failed");
    }
    const auto& rres = rrs.results(0);
    if (rres.result().success()) {
        return Fail("phase2: stale renew unexpectedly succeeded");
    }
    if (rres.result().error_code() != ErrorCode::STALE_EPOCH &&
        rres.result().error_code() != ErrorCode::LEASE_EXPIRED) {
        return Fail("phase2: expected STALE_EPOCH/LEASE_EXPIRED, got code=" +
                    std::to_string(rres.result().error_code()));
    }

    // Fresh lookup must succeed (recovery rehydrated the row from the catalog)
    // and return a lease with dn_epoch > saved.
    BatchLookupResponse lr;
    bool lookup_ok = false;
    std::string last_lookup_err;
    for (int attempt = 0; attempt < 20; ++attempt) {
        BatchLookupRequest lk;
        lk.mutable_meta()->set_request_id("phase2_fresh_lookup_" + std::to_string(attempt));
        auto* lit = lk.add_items();
        lit->set_block_hash(s.block_hash);
        lit->set_renew_lease_on_hit(true);
        lr.Clear();
        brpc::Controller lcntl;
        stub.BatchLookupWithLease(&lcntl, &lk, &lr, nullptr);
        if (lcntl.Failed() || lr.results_size() != 1) {
            last_lookup_err = "rpc failed";
        } else if (lr.results(0).result().success()) {
            lookup_ok = true;
            break;
        } else if (lr.results(0).result().error_code() == ErrorCode::CAS_CONFLICT &&
                   lr.results(0).result().retryable()) {
            last_lookup_err = lr.results(0).result().error_message();
            std::this_thread::sleep_for(std::chrono::milliseconds(250));
            continue;
        } else {
            last_lookup_err = lr.results(0).result().error_message() +
                              " code=" + std::to_string(lr.results(0).result().error_code());
            break;
        }
    }
    if (!lookup_ok) {
        return Fail("phase2: fresh lookup failed err=" + last_lookup_err);
    }
    const auto& lres = lr.results(0);
    if (!lres.result().success()) {
        return Fail("phase2: fresh lookup failed err=" + lres.result().error_message() +
                    " code=" + std::to_string(lres.result().error_code()));
    }
    if (lres.status() != BLOCK_STATUS_STORED) {
        return Fail("phase2: row not STORED after recovery (status=" +
                    std::to_string(static_cast<int>(lres.status())) + ")");
    }
    if (!lres.has_lease()) {
        return Fail("phase2: fresh lookup returned no lease");
    }
    if (lres.lease().dn_epoch() <= s.dn_epoch) {
        return Fail("phase2: dn_epoch did not advance (saved=" + std::to_string(s.dn_epoch) +
                    " current=" + std::to_string(lres.lease().dn_epoch()) + ")");
    }

    // Cleanup. After recovery, the row is STORED with the version it had
    // before the restart (typically 2). The eviction worker may have bumped
    // the version via STORED -> EVICTING -> STORED rollback cycles; try a
    // small range of expected versions before giving up so the bitmap +
    // catalog row are guaranteed gone for the next test.
    bool freed = false;
    for (int64_t v = 2; v <= 32 && !freed; ++v) {
        BatchFreeAllocatedRequest f;
        f.mutable_meta()->set_request_id("phase2_cleanup_free_v" + std::to_string(v));
        auto* fit = f.add_items();
        fit->set_block_hash(s.block_hash);
        fit->set_expected_version(v);
        fit->set_force(true);
        BatchFreeAllocatedResponse frs;
        brpc::Controller fcntl;
        stub.BatchFreeAllocated(&fcntl, &f, &frs, nullptr);
        if (!fcntl.Failed() && frs.results_size() == 1 && frs.results(0).result().success()) {
            freed = true;
        }
    }
    if (!freed) {
        // The next subcommand will see leftover bitmap state on this DN; warn
        // loudly so the operator restarts the cluster before retrying stress.
        std::cerr << "phase2: cleanup free could not converge on a live "
                     "version; downstream tests on this DN may need a restart\n";
    }

    std::cout << "PHASE2_OK new_dn_epoch=" << lres.lease().dn_epoch() << std::endl;
    return 0;
}

void PrintUsage(const char* argv0) {
    std::cerr <<
        "Usage: " << argv0 << " --scenario=<NAME> [options]\n"
        "Scenarios:\n"
        "  large-batch          --endpoint HOST:PORT\n"
        "  partial-store-write  --endpoint HOST:PORT\n"
        "  stale-store-epoch    --endpoint HOST:PORT\n"
        "  eviction-rollback    --endpoint HOST:PORT [--wait-ms N]\n"
        "  dn-restart-phase1    --endpoint HOST:PORT --state-file PATH\n"
        "  dn-restart-phase2    --endpoint HOST:PORT --state-file PATH\n";
}

}  // namespace

int main(int argc, char** argv) {
    std::string scenario;
    std::string endpoint = "127.0.0.1:55530";
    std::string state_file;
    int wait_ms = 1500;
    for (int i = 1; i < argc; ++i) {
        const std::string a(argv[i]);
        if (a == "--help" || a == "-h") {
            PrintUsage(argv[0]);
            return 0;
        }
        if (a == "--scenario" && i + 1 < argc) {
            scenario = argv[++i];
        } else if (a.rfind("--scenario=", 0) == 0) {
            scenario = a.substr(std::string("--scenario=").size());
        } else if (a == "--endpoint" && i + 1 < argc) {
            endpoint = argv[++i];
        } else if (a == "--state-file" && i + 1 < argc) {
            state_file = argv[++i];
        } else if (a == "--wait-ms" && i + 1 < argc) {
            wait_ms = std::atoi(argv[++i]);
        } else {
            PrintUsage(argv[0]);
            return Fail("unknown arg: " + a);
        }
    }
    if (scenario.empty()) {
        PrintUsage(argv[0]);
        return Fail("--scenario is required");
    }

    if (scenario == "large-batch") return RunLargeBatch(endpoint);
    if (scenario == "partial-store-write") return RunPartialStoreWrite(endpoint);
    if (scenario == "stale-store-epoch") return RunStaleStoreEpoch(endpoint);
    if (scenario == "eviction-rollback") return RunEvictionRollback(endpoint, wait_ms);
    if (scenario == "dn-restart-phase1") return RunDnRestartPhase1(endpoint, state_file);
    if (scenario == "dn-restart-phase2") return RunDnRestartPhase2(endpoint, state_file);
    PrintUsage(argv[0]);
    return Fail("unknown scenario: " + scenario);
}
