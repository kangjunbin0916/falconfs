#!/usr/bin/env python3
import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))

from falconfs_kv.offloading_manager import FalconFSOffloadingManager  # noqa: E402
from falconfs_kv.reference import (  # noqa: E402
    BlockLocation,
    BlockMeta,
    BlockStatus,
    ItemResult,
)


class _FakeMetadata:
    def __init__(self, row):
        self._row = row
        self.lease_manager = mock.Mock()

    def lookup(self, block_hashes, renew_lease_on_hit=True):
        del renew_lease_on_hit
        out = {}
        for h in block_hashes:
            out[h] = (ItemResult(True), self._row, None)
        return out


class _FakeStore:
    def read(self, store_node_id, pool_offset, expected_store_epoch):
        del store_node_id, pool_offset, expected_store_epoch
        return ItemResult(False), b""

    def read_from_ssd(
        self, store_node_id, evicted_path, expected_store_epoch, expected_version=0, block_size=65536
    ):
        del store_node_id, evicted_path, expected_store_epoch, expected_version, block_size
        return ItemResult(True), b"from-ssd"

    def write(self, store_node_id, pool_offset, payload, expected_store_epoch, block_size, expected_version=0):
        del store_node_id, pool_offset, payload, expected_store_epoch, block_size, expected_version
        return ItemResult(True)


class _FakeCluster:
    def __init__(self, row):
        self.metadata = _FakeMetadata(row)
        self.store = _FakeStore()


class PromoteOnReadTest(unittest.TestCase):
    def test_evicted_load_queues_promote_worker(self):
        row = BlockMeta(
            block_hash="k",
            kv_group_idx=0,
            layer_mask=0,
            status=BlockStatus.EVICTED,
            location=BlockLocation(store_node_id=1, pool_offset=0, evicted_path="/tmp/e", store_epoch=1),
            version=5,
        )
        cluster = _FakeCluster(row)
        manager = FalconFSOffloadingManager(
            client_id=10,
            client_hostname="h",
            shard_table={1: "127.0.0.1:55530"},
            clusters={1: cluster},
            mode="cluster",
        )
        manager._promote_worker = mock.Mock()
        loaded = manager.prepare_load(["k"], req_context=None)
        self.assertEqual(loaded.data["k"], b"from-ssd")
        manager._promote_worker.enqueue.assert_called_once()


if __name__ == "__main__":
    unittest.main()
