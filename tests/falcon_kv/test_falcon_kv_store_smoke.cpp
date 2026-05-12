#include <gtest/gtest.h>

#include <memory>

#include "vllm_kv_cache/src/service/kv_store_admin_brpc_service.h"
#include "vllm_kv_cache/src/store/kv_store_engine.h"

namespace falconfs::kv {

TEST(FalconKvStoreSmoke, StoreAdminSpillFailsWithoutSpillManager) {
    auto engine = std::make_shared<KVStoreEngine>(/*store_node_id=*/1,
                                                   /*base_offset=*/0,
                                                   /*region_bytes=*/64LL * 65536LL,
                                                   /*block_size=*/65536,
                                                   /*store_epoch=*/1);
    auto admin = CreateKVStoreAdminBrpcServiceImplForStore(engine);

    SpillBlockToSSDRequest req;
    SpillBlockToSSDResponse resp;
    req.mutable_meta()->set_request_id("ut_spill");
    req.set_store_node_id(1);
    req.set_pool_offset(0);
    req.set_block_hash("nope");
    req.set_expected_version(1);
    req.set_expected_store_epoch(1);
    admin->SpillBlockToSSD(req, &resp);
    EXPECT_FALSE(resp.result().success());
}

}  // namespace falconfs::kv
