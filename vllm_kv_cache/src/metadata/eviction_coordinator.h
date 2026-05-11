// Eviction coordinator (v6 §14).
//
// Picks cold candidates from `KVMetadataEngine`'s LRU, drives them through
// the v6 two-phase eviction state machine (`STORED -> EVICTING -> EVICTED`),
// and rolls back to `STORED` if the spill callback fails. The spill callback
// is decoupled from the coordinator so unit tests can drive it without any
// real DRAM/SSD plumbing, while production code wires it to
// `KVStoreEngine::SpillBlockToSSD`.
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "vllm_kv_cache/src/metadata/kv_metadata_engine.h"

namespace falconfs::kv {

struct EvictionConfig {
    int max_per_cycle = 64;
    int64_t now_ms = 0;
};

struct EvictionCycleResult {
    int candidates_considered = 0;
    int evicted = 0;
    int rolled_back = 0;
    int skipped_active_lease = 0;
    int skipped_other = 0;
};

class EvictionCoordinator {
public:
    // Spill callback contract: given the block_hash and the current EVICTING
    // version, perform the SSD spill and return `{ok, evicted_path}`. On
    // failure, the coordinator CAS-rolls the row back to STORED.
    struct SpillOutcome {
        bool ok = false;
        std::string evicted_path;
    };
    using SpillFn = std::function<SpillOutcome(const std::string& block_hash, int64_t version)>;

    EvictionCoordinator(std::shared_ptr<KVMetadataEngine> engine, SpillFn spill_fn);

    EvictionCycleResult RunOneCycle(const EvictionConfig& cfg);

private:
    std::shared_ptr<KVMetadataEngine> engine_;
    SpillFn spill_fn_;
};

}  // namespace falconfs::kv
