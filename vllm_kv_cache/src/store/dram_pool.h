// mmap-backed DRAM pool for FalconFS KV Cache (v6 §12.0).
//
// Allocates a contiguous mmap region (PROT_READ|PROT_WRITE,
// MAP_PRIVATE|MAP_ANONYMOUS, optionally MAP_HUGETLB). Block-aligned writes
// and reads index into the region by `pool_offset` relative to the pool base
// address; this matches the Store data plane in the v6 design.
//
// The pool deliberately stores opaque bytes only and does not interpret
// payload semantics. Per-key metadata (version, crc, compression, original
// size) lives in `KVStoreEngine`.
#pragma once

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>

namespace falconfs::kv {

struct DramPoolWriteResult {
    bool ok = false;
    int32_t bytes_written = 0;
};

class DramPool {
public:
    // Constructs an mmap-backed pool sized to `region_bytes`. If
    // `try_huge_pages=true`, MAP_HUGETLB is attempted first and the constructor
    // falls back to standard pages if hugepages are not available.
    DramPool(std::size_t region_bytes, std::size_t block_size, bool try_huge_pages = false);
    ~DramPool();

    DramPool(const DramPool&) = delete;
    DramPool& operator=(const DramPool&) = delete;

    std::size_t RegionBytes() const { return region_bytes_; }
    std::size_t BlockSize() const { return block_size_; }
    bool UsedHugePages() const { return used_huge_pages_; }

    // True if `pool_offset` is in-range and aligned to `block_size_`.
    bool ValidOffset(int64_t pool_offset) const;

    // Writes `payload` into the slot starting at `pool_offset`. Returns
    // `ok=false` if the offset is invalid or the payload exceeds `block_size_`.
    DramPoolWriteResult Write(int64_t pool_offset, const std::string& payload);

    // Reads `size` bytes (must be <= `block_size_`) starting at `pool_offset`.
    bool Read(int64_t pool_offset, int32_t size, std::string* out) const;

private:
    void Unmap();

    void* base_ = nullptr;
    std::size_t region_bytes_ = 0;
    std::size_t block_size_ = 0;
    bool used_huge_pages_ = false;
    mutable std::mutex mu_;
};

}  // namespace falconfs::kv
