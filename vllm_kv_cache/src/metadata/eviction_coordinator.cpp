#include "vllm_kv_cache/src/metadata/eviction_coordinator.h"

#include <utility>

#include "kv_common.pb.h"

namespace falconfs::kv {

namespace {
constexpr int32_t kStored = static_cast<int32_t>(BlockStatus::BLOCK_STATUS_STORED);
constexpr int32_t kEvicting = static_cast<int32_t>(BlockStatus::BLOCK_STATUS_EVICTING);
constexpr int32_t kEvicted = static_cast<int32_t>(BlockStatus::BLOCK_STATUS_EVICTED);
}  // namespace

EvictionCoordinator::EvictionCoordinator(std::shared_ptr<KVMetadataEngine> engine, SpillFn spill_fn)
    : engine_(std::move(engine)), spill_fn_(std::move(spill_fn)) {}

EvictionCycleResult EvictionCoordinator::RunOneCycle(const EvictionConfig& cfg) {
    EvictionCycleResult result;
    if (engine_ == nullptr || !spill_fn_) return result;

    const int limit = cfg.max_per_cycle > 0 ? cfg.max_per_cycle : 64;
    auto candidates = engine_->ColdCandidates(static_cast<std::size_t>(limit), cfg.now_ms);
    result.candidates_considered = static_cast<int>(candidates.size());

    for (const auto& block_hash : candidates) {
        EngineLookupResult lookup = engine_->Lookup(block_hash, /*renew=*/false, cfg.now_ms);
        if (!lookup.result.success || !lookup.row.has_value() ||
            lookup.row->status != kStored) {
            ++result.skipped_other;
            continue;
        }
        if (!engine_->CanEvict(block_hash, cfg.now_ms)) {
            ++result.skipped_active_lease;
            continue;
        }

        EngineUpdateStatusResult to_evicting = engine_->UpdateStatus(
            block_hash, kStored, kEvicting, lookup.row->version,
            /*evicted_path=*/"", /*allow_noop_if_already_target=*/false, cfg.now_ms);
        if (!to_evicting.result.success) {
            // Lost a race (someone else mutated). Skip and move on.
            ++result.skipped_other;
            continue;
        }

        SpillOutcome spill = spill_fn_(block_hash, to_evicting.new_version);
        if (spill.ok) {
            EngineUpdateStatusResult to_evicted = engine_->UpdateStatus(
                block_hash, kEvicting, kEvicted, to_evicting.new_version,
                spill.evicted_path, /*allow_noop_if_already_target=*/false, cfg.now_ms);
            if (to_evicted.result.success) {
                ++result.evicted;
            } else {
                // Could not finalize; roll back to STORED.
                EngineUpdateStatusResult rollback = engine_->UpdateStatus(
                    block_hash, kEvicting, kStored, to_evicting.new_version,
                    /*evicted_path=*/"", /*allow_noop_if_already_target=*/false, cfg.now_ms);
                if (rollback.result.success) {
                    ++result.rolled_back;
                } else {
                    ++result.skipped_other;
                }
            }
        } else {
            EngineUpdateStatusResult rollback = engine_->UpdateStatus(
                block_hash, kEvicting, kStored, to_evicting.new_version,
                /*evicted_path=*/"", /*allow_noop_if_already_target=*/false, cfg.now_ms);
            if (rollback.result.success) {
                ++result.rolled_back;
            } else {
                ++result.skipped_other;
            }
        }
    }

    return result;
}

}  // namespace falconfs::kv
