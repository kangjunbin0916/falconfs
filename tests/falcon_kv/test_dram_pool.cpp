#include <gtest/gtest.h>

#include <stdexcept>
#include <string>

#include "vllm_kv_cache/src/store/dram_pool.h"

namespace falconfs::kv {

TEST(DramPool, AlignedWriteAndReadRoundtrip) {
    DramPool pool(/*region_bytes=*/4 * 4096, /*block_size=*/4096);
    EXPECT_EQ(pool.RegionBytes(), 4 * 4096u);
    EXPECT_EQ(pool.BlockSize(), 4096u);

    DramPoolWriteResult w = pool.Write(/*pool_offset=*/0, "hello");
    EXPECT_TRUE(w.ok);
    EXPECT_EQ(w.bytes_written, 5);

    std::string out;
    EXPECT_TRUE(pool.Read(/*pool_offset=*/0, /*size=*/5, &out));
    EXPECT_EQ(out, "hello");
}

TEST(DramPool, RejectsUnalignedAndOutOfRangeOffsets) {
    DramPool pool(8192, 4096);
    EXPECT_FALSE(pool.ValidOffset(-1));
    EXPECT_FALSE(pool.ValidOffset(1));        // unaligned
    EXPECT_FALSE(pool.ValidOffset(8192));     // out of range
    EXPECT_TRUE(pool.ValidOffset(0));
    EXPECT_TRUE(pool.ValidOffset(4096));

    DramPoolWriteResult w = pool.Write(1, "x");
    EXPECT_FALSE(w.ok);

    std::string out;
    EXPECT_FALSE(pool.Read(8192, 1, &out));
}

TEST(DramPool, OversizedPayloadRejected) {
    DramPool pool(4096, 1024);
    std::string big(2048, 'a');
    DramPoolWriteResult w = pool.Write(0, big);
    EXPECT_FALSE(w.ok);
}

TEST(DramPool, RejectsNonMultipleSize) {
    EXPECT_THROW(DramPool(5000, 4096), std::invalid_argument);
}

}  // namespace falconfs::kv
