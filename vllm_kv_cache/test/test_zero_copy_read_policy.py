from __future__ import annotations

import os
import unittest
from types import SimpleNamespace

from falconfs_kv.offloading_manager import FalconFSOffloadingManager, KVBlockLocation


class _FakeStore:
    def batch_read_payload_views(self, store_id, pool_offsets, store_epochs, block_hashes, block_size):
        del store_id, store_epochs, block_size
        payloads = [memoryview((b"x" * 16) + bytes([i])) for i, _ in enumerate(pool_offsets)]
        return [True] * len(pool_offsets), payloads, ["remote_attachment_buffer"] * len(pool_offsets)


class ZeroCopyReadPolicyTest(unittest.TestCase):
    def setUp(self):
        self._old = dict(os.environ)
        os.environ["FALCON_KV_CLIENT_ZERO_COPY_READS"] = "1"

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self._old)

    def test_dram_group_uses_readonly_buffer_objects_when_enabled(self):
        mgr = FalconFSOffloadingManager(
            client_id=1,
            client_hostname="ut",
            shard_table={1: "unused"},
            clusters={1: SimpleNamespace(store=_FakeStore())},
            mode="cluster",
            block_size=16,
        )
        try:
            keys = ["zk0", "zk1"]
            for i, key in enumerate(keys):
                mgr.local_cache[key] = KVBlockLocation(
                    block_hash=key,
                    status=2,
                    store_id=1,
                    pool_offset=i * 16,
                    lease_expire_ms=999999,
                    store_epoch=1,
                    dn_id=1,
                )
            out = mgr._load_dram_key_group(1, 1, keys)
            self.assertEqual(set(out), set(keys))
            self.assertTrue(all(isinstance(v, memoryview) for v in out.values()))
            self.assertTrue(all(v.readonly for v in out.values()))
            snap = mgr.perf_breakdown()["zero_copy_read"]
            self.assertEqual(snap["zero_copy_blocks"], 2)
            self.assertEqual(snap["remote_attachment_views"], 2)
        finally:
            mgr._data_stage_pool.shutdown(wait=False, cancel_futures=True)
            mgr._meta_stage_pool.shutdown(wait=False, cancel_futures=True)


if __name__ == "__main__":
    unittest.main()
