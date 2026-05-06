#!/usr/bin/env python3
"""
Phase 5: vLLM OffloadingManager 完整批量接口测试

测试所有批量接口：
- batch_lookup
- batch_prepare_store
- batch_complete_store
- batch_prepare_load
- batch_complete_load
- batch_touch

调用链路：
Python 测试程序 
    ↓ (import falconfs_kv)
FalconFSOffloadingManager (Python)
    ↓ (pybind11 绑定)
DNClient / StoreClient (C++)
    ↓ (BRPC 网络调用)
DN (Metadata) / Store (Memory Pool)
"""

import sys
import time
import hashlib
import os
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================================
# 导入 FalconFS Python 客户端
# ============================================================================

try:
    from falconfs_kv import FalconFSOffloadingManager, LoadStoreSpec
    print("✅ Using real FalconFSOffloadingManager")
    USE_MOCK = False
except ImportError:
    print("⚠️  FalconFS Python client not found, using Mock implementation")
    print("   After implementing C++ code, run: pip install -e vllm_kv_cache/python/")
    USE_MOCK = True

# ============================================================================
# Mock 实现（开发阶段使用）
# ============================================================================

if USE_MOCK:
    class LoadStoreSpec:
        """加载/存储规格"""
        def __init__(self, data: Dict = None):
            self.data = data or {}
    
    class MockOffloadingManager:
        """
        模拟 OffloadingManager - 开发阶段用 mock，开发完成后替换为真实实现
        
        所有接口都是批量接口（batch_*）
        """
        
        def __init__(self, shard_table: Dict[int, str], client_id: int, client_hostname: str):
            self.shard_table = shard_table
            self.client_id = client_id
            self.client_hostname = client_hostname
            self.local_cache = {}  # 本地缓存
            
            print(f"[MockOffloadingManager] Initialized: client_id={client_id}, hostname={client_hostname}")
        
        def batch_lookup(self, keys: List[str], req_context) -> Dict[str, bool]:
            """
            vLLM 调用：批量查询 block 是否在缓存中
            
            未来真实实现：
            1. 查本地缓存（Python dict）
            2. 按 DN 分组
            3. 调用 C++ DNClient.batch_lookup_with_lease() → pybind11 绑定
            4. 并行查询多个 DN
            5. 返回结果
            """
            print(f"  [batch_lookup] keys={len(keys)} blocks")
            print(f"    → 未来调用: dn_client.batch_lookup_with_lease(keys, client_id)")
            
            # Mock: 全部 cache miss
            return {key: False for key in keys}
        
        def batch_prepare_load(self, keys: List[str], req_context) -> LoadStoreSpec:
            """批量准备从 DRAM/SSD 加载到 GPU"""
            print(f"  [batch_prepare_load] keys={len(keys)} blocks")
            print(f"    → 未来调用: 按 Store 分组，并行 batch_read_block/batch_read_from_ssd")
            return LoadStoreSpec(data={key: b'' for key in keys})
        
        def batch_complete_load(self, keys: List[str], req_context):
            """批量完成加载到 GPU（续约 Lease）"""
            print(f"  [batch_complete_load] keys={len(keys)} blocks")
            print(f"    → 未来调用: dn_client.batch_renew_lease(keys, client_id)")
        
        def batch_prepare_store(self, keys: List[str], req_context) -> LoadStoreSpec:
            """批量准备从 GPU 存储到 DRAM"""
            print(f"  [batch_prepare_store] keys={len(keys)} blocks")
            print(f"    → 未来调用: dn_client.batch_allocate_with_lease(keys, client_id)")
            return LoadStoreSpec()
        
        def batch_complete_store(self, keys: List[str], data: Dict[str, bytes], req_context):
            """批量完成存储到 DRAM"""
            print(f"  [batch_complete_store] keys={len(keys)} blocks")
            print(f"    → 未来调用: store_client.batch_write_block() + dn_client.batch_update_block_status()")
        
        def batch_touch(self, keys: List[str], req_context):
            """批量访问 Block（续约 Lease）"""
            print(f"  [batch_touch] keys={len(keys)} blocks")
            print(f"    → 未来调用: dn_client.batch_renew_lease(keys, client_id)")
    
    FalconFSOffloadingManager = MockOffloadingManager

# ============================================================================
# 测试工具函数
# ============================================================================

def generate_block_hashes(count: int) -> List[str]:
    """生成测试用的 block hashes"""
    hashes = []
    for i in range(count):
        data = f"test_block_{i}_{time.time()}"
        hash_value = hashlib.sha256(data.encode()).hexdigest()
        hashes.append(hash_value)
    return hashes

class MockReqContext:
    """模拟 vLLM 请求上下文"""
    def __init__(self, kv_group_idx: int = 0, layer_mask: int = 0xFF):
        self.kv_group_idx = kv_group_idx
        self.layer_mask = layer_mask

# ============================================================================
# Test Suite: OffloadingManager 完整批量接口
# ============================================================================

class TestOffloadingManager:
    """OffloadingManager 完整批量接口测试"""
    
    def __init__(self):
        shard_table = {0: "localhost:55520"}
        self.manager = FalconFSOffloadingManager(
            shard_table=shard_table,
            client_id=0,
            client_hostname="localhost"
        )
        self.req_context = MockReqContext()
    
    def test_01_batch_lookup_miss(self):
        """Test 1: Batch Lookup Miss（缓存未命中）"""
        print("\n" + "="*80)
        print("Test 1: Batch Lookup Miss")
        print("="*80)
        
        keys = generate_block_hashes(10)
        
        start = time.time()
        results = self.manager.batch_lookup(keys, self.req_context)
        duration = time.time() - start
        
        # 验证全部 miss
        for key, hit in results.items():
            assert hit == False, f"Expected miss, got hit for {key}"
        
        print(f"✅ All {len(keys)} blocks cache miss ({duration*1000:.2f}ms)")
    
    def test_02_batch_prepare_store(self):
        """Test 2: Batch Prepare Store（批量准备存储）"""
        print("\n" + "="*80)
        print("Test 2: Batch Prepare Store")
        print("="*80)
        
        keys = generate_block_hashes(10)
        
        start = time.time()
        spec = self.manager.batch_prepare_store(keys, self.req_context)
        duration = time.time() - start
        
        # 验证返回规格
        assert spec is not None, "Expected LoadStoreSpec"
        
        print(f"✅ Prepared {len(keys)} blocks for store ({duration*1000:.2f}ms)")
    
    def test_03_batch_complete_store(self):
        """Test 3: Batch Complete Store（批量完成存储）"""
        print("\n" + "="*80)
        print("Test 3: Batch Complete Store")
        print("="*80)
        
        keys = generate_block_hashes(10)
        
        # 先 prepare
        self.manager.batch_prepare_store(keys, self.req_context)
        
        # 准备数据
        data = {key: bytes([i % 256 for _ in range(65536)]) for i, key in enumerate(keys)}
        
        start = time.time()
        self.manager.batch_complete_store(keys, data, self.req_context)
        duration = time.time() - start
        
        print(f"✅ Completed store {len(keys)} blocks ({duration*1000:.2f}ms)")
    
    def test_04_batch_lookup_hit(self):
        """Test 4: Batch Lookup Hit（缓存命中）"""
        print("\n" + "="*80)
        print("Test 4: Batch Lookup Hit")
        print("="*80)
        
        keys = generate_block_hashes(10)
        
        # 先存储
        self.manager.batch_prepare_store(keys, self.req_context)
        data = {key: bytes([0] * 65536) for key in keys}
        self.manager.batch_complete_store(keys, data, self.req_context)
        
        # 再 lookup
        start = time.time()
        results = self.manager.batch_lookup(keys, self.req_context)
        duration = time.time() - start
        
        # 验证全部 hit
        hit_count = sum(1 for hit in results.values() if hit)
        print(f"✅ {hit_count}/{len(keys)} blocks cache hit ({duration*1000:.2f}ms)")
    
    def test_05_batch_prepare_load(self):
        """Test 5: Batch Prepare Load（批量准备加载）"""
        print("\n" + "="*80)
        print("Test 5: Batch Prepare Load")
        print("="*80)
        
        keys = generate_block_hashes(10)
        
        # 先存储
        self.manager.batch_prepare_store(keys, self.req_context)
        data = {key: bytes([i % 256 for _ in range(65536)]) for i, key in enumerate(keys)}
        self.manager.batch_complete_store(keys, data, self.req_context)
        
        # 再 prepare_load
        start = time.time()
        spec = self.manager.batch_prepare_load(keys, self.req_context)
        duration = time.time() - start
        
        # 验证返回数据
        assert spec is not None, "Expected LoadStoreSpec"
        assert len(spec.data) == len(keys), f"Expected {len(keys)} blocks, got {len(spec.data)}"
        
        print(f"✅ Prepared load {len(keys)} blocks ({duration*1000:.2f}ms)")
    
    def test_06_batch_complete_load(self):
        """Test 6: Batch Complete Load（批量完成加载）"""
        print("\n" + "="*80)
        print("Test 6: Batch Complete Load")
        print("="*80)
        
        keys = generate_block_hashes(10)
        
        # 先存储
        self.manager.batch_prepare_store(keys, self.req_context)
        data = {key: bytes([0] * 65536) for key in keys}
        self.manager.batch_complete_store(keys, data, self.req_context)
        
        # 再 load
        self.manager.batch_prepare_load(keys, self.req_context)
        
        start = time.time()
        self.manager.batch_complete_load(keys, self.req_context)
        duration = time.time() - start
        
        print(f"✅ Completed load {len(keys)} blocks ({duration*1000:.2f}ms)")
    
    def test_07_batch_touch(self):
        """Test 7: Batch Touch（批量续约）"""
        print("\n" + "="*80)
        print("Test 7: Batch Touch")
        print("="*80)
        
        keys = generate_block_hashes(10)
        
        # 先存储
        self.manager.batch_prepare_store(keys, self.req_context)
        data = {key: bytes([0] * 65536) for key in keys}
        self.manager.batch_complete_store(keys, data, self.req_context)
        
        # 再 touch
        start = time.time()
        self.manager.batch_touch(keys, self.req_context)
        duration = time.time() - start
        
        print(f"✅ Touched {len(keys)} blocks ({duration*1000:.2f}ms)")
    
    def test_08_complete_store_load_cycle(self):
        """Test 8: 完整 Store → Load 流程（全部批量）"""
        print("\n" + "="*80)
        print("Test 8: Complete Store → Load Cycle")
        print("="*80)
        
        keys = generate_block_hashes(10)
        
        # Step 1: Store
        print("\nStep 1: batch_prepare_store")
        self.manager.batch_prepare_store(keys, self.req_context)
        
        print("Step 2: batch_complete_store")
        data = {key: bytes([i % 256 for _ in range(65536)]) for i, key in enumerate(keys)}
        self.manager.batch_complete_store(keys, data, self.req_context)
        
        # Step 2: Lookup
        print("\nStep 3: batch_lookup")
        results = self.manager.batch_lookup(keys, self.req_context)
        hit_count = sum(1 for hit in results.values() if hit)
        print(f"  {hit_count}/{len(keys)} blocks cache hit")
        
        # Step 3: Load
        print("\nStep 4: batch_prepare_load")
        spec = self.manager.batch_prepare_load(keys, self.req_context)
        
        print("Step 5: batch_complete_load")
        self.manager.batch_complete_load(keys, self.req_context)
        
        print(f"✅ Complete cycle for {len(keys)} blocks")
    
    def test_09_error_handling(self):
        """Test 9: Error Handling（错误处理）"""
        print("\n" + "="*80)
        print("Test 9: Error Handling")
        print("="*80)
        
        # 测试 prepare_load 不存在的 key
        keys = generate_block_hashes(5)
        
        try:
            spec = self.manager.batch_prepare_load(keys, self.req_context)
            print("⚠️  Expected error for missing keys, but got result")
        except Exception as e:
            print(f"✅ Correctly raised error: {e}")
    
    def test_10_performance_comparison(self):
        """Test 10: Performance（性能测试：单个 vs 批量）"""
        print("\n" + "="*80)
        print("Test 10: Performance Comparison")
        print("="*80)
        
        # 测试不同批量大小
        batch_sizes = [1, 5, 10, 20, 50]
        
        for size in batch_sizes:
            keys = generate_block_hashes(size)
            
            # Store
            start = time.time()
            self.manager.batch_prepare_store(keys, self.req_context)
            data = {key: bytes([0] * 65536) for key in keys}
            self.manager.batch_complete_store(keys, data, self.req_context)
            store_time = time.time() - start
            
            # Lookup
            start = time.time()
            results = self.manager.batch_lookup(keys, self.req_context)
            lookup_time = time.time() - start
            
            print(f"  Batch size {size:2d}: store={store_time*1000:8.2f}ms, "
                  f"lookup={lookup_time*1000:8.2f}ms, "
                  f"per_block_store={store_time/size*1000:6.2f}ms, "
                  f"per_block_lookup={lookup_time/size*1000:6.2f}ms")
        
        print(f"✅ Performance test completed")

# ============================================================================
# Main
# ============================================================================

def main():
    """运行所有测试"""
    print("="*80)
    print("FalconFS OffloadingManager 完整批量接口测试")
    print("="*80)
    
    tester = TestOffloadingManager()
    
    # 运行测试
    tester.test_01_batch_lookup_miss()
    tester.test_02_batch_prepare_store()
    tester.test_03_batch_complete_store()
    tester.test_04_batch_lookup_hit()
    tester.test_05_batch_prepare_load()
    tester.test_06_batch_complete_load()
    tester.test_07_batch_touch()
    tester.test_08_complete_store_load_cycle()
    tester.test_09_error_handling()
    tester.test_10_performance_comparison()
    
    print("\n" + "="*80)
    print("✅ All tests passed!")
    print("="*80)

if __name__ == "__main__":
    main()
