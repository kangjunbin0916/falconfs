#include "vllm_kv_cache/src/store/dram_pool.h"

#include <sys/mman.h>

#include <cstring>
#include <stdexcept>

namespace falconfs::kv {

namespace {

void* MapAnonymous(std::size_t bytes, bool try_huge_pages, bool* used_huge_pages) {
    int flags = MAP_PRIVATE | MAP_ANONYMOUS;
    *used_huge_pages = false;

#ifdef MAP_HUGETLB
    if (try_huge_pages) {
        void* addr = ::mmap(nullptr, bytes, PROT_READ | PROT_WRITE, flags | MAP_HUGETLB, -1, 0);
        if (addr != MAP_FAILED) {
            *used_huge_pages = true;
            return addr;
        }
    }
#endif

    void* addr = ::mmap(nullptr, bytes, PROT_READ | PROT_WRITE, flags, -1, 0);
    return addr;
}

}  // namespace

DramPool::DramPool(std::size_t region_bytes, std::size_t block_size, bool try_huge_pages)
    : region_bytes_(region_bytes), block_size_(block_size) {
    if (region_bytes == 0 || block_size == 0 || (region_bytes % block_size) != 0) {
        throw std::invalid_argument("DramPool: region_bytes must be a positive multiple of block_size");
    }
    base_ = MapAnonymous(region_bytes_, try_huge_pages, &used_huge_pages_);
    if (base_ == MAP_FAILED) {
        base_ = nullptr;
        throw std::runtime_error("DramPool: mmap failed");
    }
}

DramPool::~DramPool() { Unmap(); }

void DramPool::Unmap() {
    if (base_ != nullptr) {
        ::munmap(base_, region_bytes_);
        base_ = nullptr;
    }
}

bool DramPool::ValidOffset(int64_t pool_offset) const {
    if (pool_offset < 0) return false;
    auto offset = static_cast<std::size_t>(pool_offset);
    if (offset >= region_bytes_) return false;
    return (offset % block_size_) == 0;
}

DramPoolWriteResult DramPool::Write(int64_t pool_offset, const std::string& payload) {
    DramPoolWriteResult result;
    if (!ValidOffset(pool_offset)) {
        return result;
    }
    if (payload.size() > block_size_) {
        return result;
    }
    auto offset = static_cast<std::size_t>(pool_offset);
    if (offset + block_size_ > region_bytes_) {
        return result;
    }
    std::lock_guard<std::mutex> lock(mu_);
    char* dst = static_cast<char*>(base_) + offset;
    if (!payload.empty()) {
        std::memcpy(dst, payload.data(), payload.size());
    }
    if (payload.size() < block_size_) {
        std::memset(dst + payload.size(), 0, block_size_ - payload.size());
    }
    result.ok = true;
    result.bytes_written = static_cast<int32_t>(payload.size());
    return result;
}

bool DramPool::Read(int64_t pool_offset, int32_t size, std::string* out) const {
    if (out == nullptr) return false;
    if (!ValidOffset(pool_offset)) return false;
    if (size < 0 || static_cast<std::size_t>(size) > block_size_) return false;
    auto offset = static_cast<std::size_t>(pool_offset);
    if (offset + static_cast<std::size_t>(size) > region_bytes_) return false;
    std::lock_guard<std::mutex> lock(mu_);
    const char* src = static_cast<const char*>(base_) + offset;
    out->assign(src, static_cast<std::size_t>(size));
    return true;
}

}  // namespace falconfs::kv
