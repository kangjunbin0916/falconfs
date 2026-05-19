from __future__ import annotations

import os
import unittest

from falconfs_kv.offloading_manager import FalconFSOffloadingManager


class AdaptiveBatchingPolicyTest(unittest.TestCase):
    def setUp(self):
        self._old = dict(os.environ)

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self._old)

    def _manager(self):
        return FalconFSOffloadingManager(
            client_id=1,
            client_hostname="ut",
            shard_table={1: "unused"},
            mode="reference",
            block_size=1024 * 1024,
        )

    def test_static_defaults_do_not_adapt_when_flag_off(self):
        os.environ["FALCON_KV_CLIENT_ADAPTIVE_BATCHING"] = "0"
        os.environ["FALCON_KV_CLIENT_BATCH_READ_MAX_BLOCKS"] = "8"
        os.environ["FALCON_KV_CLIENT_BATCH_READ_TARGET_BYTES"] = str(2 * 1024 * 1024)
        mgr = self._manager()
        try:
            self.assertEqual(mgr._effective_batch_max_blocks(is_write=False), 8)
        finally:
            mgr._data_stage_pool.shutdown(wait=False, cancel_futures=True)
            mgr._meta_stage_pool.shutdown(wait=False, cancel_futures=True)

    def test_adaptive_flag_caps_by_target_bytes_and_reports_enabled(self):
        os.environ["FALCON_KV_CLIENT_ADAPTIVE_BATCHING"] = "1"
        os.environ["FALCON_KV_CLIENT_BATCH_READ_MAX_BLOCKS"] = "8"
        os.environ["FALCON_KV_CLIENT_BATCH_READ_TARGET_BYTES"] = str(2 * 1024 * 1024)
        mgr = self._manager()
        try:
            self.assertEqual(mgr._effective_batch_max_blocks(is_write=False), 2)
            self.assertTrue(mgr._adaptive_batching_enabled)
        finally:
            mgr._data_stage_pool.shutdown(wait=False, cancel_futures=True)
            mgr._meta_stage_pool.shutdown(wait=False, cancel_futures=True)

    def test_adaptive_controller_accepts_then_rolls_back_regression(self):
        os.environ["FALCON_KV_CLIENT_ADAPTIVE_BATCHING"] = "1"
        os.environ["FALCON_KV_CLIENT_BATCH_READ_MAX_BLOCKS"] = "8"
        os.environ["FALCON_KV_CLIENT_BATCH_READ_TARGET_BYTES"] = str(8 * 1024 * 1024)
        mgr = self._manager()
        try:
            st = mgr._adaptive_batching_state["local_read"]
            st["selected"] = 1
            st["previous"] = 1
            st["last_good"] = 1
            mgr._adaptive_observe_path("local_read", mb_s=1000.0, p50_ms=1.0, p95_ms=2.0, error_rate=0.0)
            self.assertEqual(mgr._adaptive_batching_state["local_read"]["selected"], 2)
            mgr._adaptive_observe_path("local_read", mb_s=1200.0, p50_ms=1.0, p95_ms=2.0, error_rate=0.0)
            self.assertEqual(mgr._adaptive_batching_state["local_read"]["selected"], 3)
            mgr._adaptive_observe_path("local_read", mb_s=800.0, p50_ms=2.0, p95_ms=4.5, error_rate=0.0)
            self.assertEqual(mgr._adaptive_batching_state["local_read"]["selected"], 2)
            snap = mgr._adaptive_batching_snapshot()
            self.assertGreaterEqual(snap["controller"]["local_read"]["rollback_count"], 1)
        finally:
            mgr._data_stage_pool.shutdown(wait=False, cancel_futures=True)
            mgr._meta_stage_pool.shutdown(wait=False, cancel_futures=True)


if __name__ == "__main__":
    unittest.main()
