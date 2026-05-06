#!/usr/bin/env python3
import sys
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))

from falconfs_kv.reference import (  # noqa: E402
    BlockStatus,
    ErrorCode,
    IdempotencyStore,
    LRUManager,
    LeaseManager,
    StoreRegion,
    StoreRegionRegistry,
    StoreRegionState,
)


class BitmapRegionTest(unittest.TestCase):
    def test_allocate_and_free_inside_continuous_region(self):
        region = StoreRegion(
            store_node_id=1,
            owner_dn_id=2,
            base_offset=1024,
            region_bytes=4 * 64,
            block_size=64,
        )
        result, loc = region.allocate()
        self.assertTrue(result.success)
        self.assertIsNotNone(loc)
        self.assertEqual(loc.pool_offset, 1024)
        self.assertEqual(region.free_blocks, 3)

        free = region.free(loc.pool_offset)
        self.assertTrue(free.success)
        self.assertEqual(region.free_blocks, 4)

    def test_reject_double_free_and_cross_region_offset(self):
        region = StoreRegion(
            store_node_id=1,
            owner_dn_id=2,
            base_offset=1024,
            region_bytes=2 * 64,
            block_size=64,
        )
        result, loc = region.allocate()
        self.assertTrue(result.success)
        self.assertTrue(region.free(loc.pool_offset).success)
        self.assertFalse(region.free(loc.pool_offset).success)
        self.assertFalse(region.free(0).success)

    def test_exhaustion_is_retryable_throttle(self):
        region = StoreRegion(
            store_node_id=1,
            owner_dn_id=2,
            base_offset=0,
            region_bytes=64,
            block_size=64,
        )
        self.assertTrue(region.allocate()[0].success)
        result, _ = region.allocate()
        self.assertFalse(result.success)
        self.assertEqual(result.error_code, ErrorCode.THROTTLED)
        self.assertTrue(result.retryable)


class LeaseManagerTest(unittest.TestCase):
    def test_ownerless_renew_success(self):
        manager = LeaseManager(default_ttl_ms=1000, dn_epoch=7)
        lease = manager.grant("k", store_epoch=3, now_ms=100)
        result, renewed = manager.renew("k", lease.lease_token, 7, 3, 200)
        self.assertTrue(result.success)
        self.assertIsNotNone(renewed)
        self.assertGreater(renewed.lease_expire_ms, lease.lease_expire_ms - 1)

    def test_stale_epochs_are_retryable(self):
        manager = LeaseManager(default_ttl_ms=1000, dn_epoch=7)
        lease = manager.grant("k", store_epoch=3, now_ms=100)
        result, _ = manager.renew("k", lease.lease_token, 6, 3, 200)
        self.assertFalse(result.success)
        self.assertEqual(result.error_code, ErrorCode.STALE_EPOCH)
        self.assertTrue(result.retryable)

        result, _ = manager.renew("k", lease.lease_token, 7, 2, 200)
        self.assertFalse(result.success)
        self.assertEqual(result.error_code, ErrorCode.STALE_EPOCH)
        self.assertTrue(result.retryable)

    def test_token_mismatch_and_expiry(self):
        manager = LeaseManager(default_ttl_ms=10, dn_epoch=1)
        lease = manager.grant("k", store_epoch=1, now_ms=100)
        result, _ = manager.renew("k", lease.lease_token + 1, 1, 1, 101)
        self.assertEqual(result.error_code, ErrorCode.LEASE_TOKEN_MISMATCH)

        expired, _ = manager.renew("k", lease.lease_token, 1, 1, 111)
        self.assertEqual(expired.error_code, ErrorCode.LEASE_EXPIRED)
        self.assertTrue(manager.can_evict("k", 111))


class LRUAndIdempotencyTest(unittest.TestCase):
    def test_lru_ordering(self):
        lru = LRUManager()
        lru.add_stored("a")
        lru.add_stored("b")
        lru.touch("a")
        self.assertEqual(lru.cold_candidates(2), ["b", "a"])
        lru.remove("b")
        self.assertEqual(lru.cold_candidates(2), ["a"])

    def test_idempotency_replay_and_gc(self):
        store = IdempotencyStore(ttl_ms=10)
        store.put("api", "req", 1, {"ok": True}, now_ms=100)
        self.assertEqual(store.get("api", "req", 1, now_ms=105), {"ok": True})
        self.assertIsNone(store.get("api", "req", 1, now_ms=111))


class StoreRegionStateTest(unittest.TestCase):
    def test_health_transitions_remove_allocation(self):
        registry = StoreRegionRegistry(suspect_ms=10, offline_ms=20)
        region = StoreRegion(
            store_node_id=1,
            owner_dn_id=1,
            base_offset=0,
            region_bytes=64,
            block_size=64,
            last_heartbeat_ms=100,
        )
        registry.register(region)
        registry.refresh_health(111)
        self.assertEqual(region.state, StoreRegionState.SUSPECT)
        result, _ = region.allocate()
        self.assertFalse(result.success)
        self.assertEqual(result.error_code, ErrorCode.THROTTLED)

        registry.refresh_health(121)
        self.assertEqual(region.state, StoreRegionState.OFFLINE)
        registry.heartbeat(1, 122)
        self.assertEqual(region.state, StoreRegionState.HEALTHY)

    def test_new_store_epoch_quarantines_region(self):
        registry = StoreRegionRegistry()
        region = StoreRegion(
            store_node_id=1,
            owner_dn_id=1,
            base_offset=0,
            region_bytes=64,
            block_size=64,
            store_epoch=1,
        )
        registry.register(region)
        registry.heartbeat(1, int(time.time() * 1000), store_epoch=2)
        self.assertEqual(region.store_epoch, 2)
        self.assertEqual(region.state, StoreRegionState.QUARANTINED)


if __name__ == "__main__":
    unittest.main()
