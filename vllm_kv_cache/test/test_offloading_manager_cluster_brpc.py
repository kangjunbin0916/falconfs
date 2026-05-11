#!/usr/bin/env python3
"""v6 §16 / M3: end-to-end cluster-mode OffloadingManager tests.

Requires a running 1 CN + 2 DNs harness (`falcon_distributed_test.sh start`).
Tests are skipped (not failed) when the cluster cannot be reached on the
expected DN BRPC ports, mirroring the upstream vLLM test pattern.
"""

import socket
import sys
import time
import unittest
from pathlib import Path
from typing import Dict, List

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))


DN1_ENDPOINT = "127.0.0.1:55530"
DN2_ENDPOINT = "127.0.0.1:55550"


def _can_reach(endpoint: str, timeout_s: float = 1.0) -> bool:
    host, _, port = endpoint.partition(":")
    try:
        with socket.create_connection((host, int(port)), timeout=timeout_s):
            return True
    except OSError:
        return False


def _all_endpoints_reachable(endpoints) -> bool:
    return all(_can_reach(ep) for ep in endpoints)


@unittest.skipUnless(
    _all_endpoints_reachable([DN1_ENDPOINT, DN2_ENDPOINT]),
    "Cluster not reachable; run scripts/falcon_distributed_test.sh start first",
)
class OffloadingManagerClusterBrpcTest(unittest.TestCase):
    def setUp(self):
        from falconfs_kv.offloading_manager import FalconFSOffloadingManager  # noqa: WPS433
        self.shard_table: Dict[int, str] = {1: DN1_ENDPOINT, 2: DN2_ENDPOINT}
        # Unique per-test prefix so re-runs do not collide with leftover rows.
        self._prefix = f"py_omcb_{int(time.time() * 1000)}_{id(self)}"
        self.mgr = FalconFSOffloadingManager(
            shard_table=self.shard_table,
            client_id=4242,
            client_hostname="py-cluster-test",
            mode="cluster",
            block_size=65536,
            timeout_ms=10000,
        )

    def _key(self, name: str) -> str:
        return f"{self._prefix}_{name}"

    def _cleanup(self, keys: List[str]) -> None:
        # Force-free anything we might have left around even on test failure.
        for k in keys:
            loc = self.mgr.local_cache.get(k)
            if loc is None:
                continue
            cluster = self.mgr._clusters.get(loc.dn_id)
            if cluster is None:
                continue
            try:
                cluster.metadata.free_allocated(
                    k, expected_version=loc.version, force=True,
                    request_id=f"{self._prefix}_cleanup_{k}",
                    client_id=self.mgr.client_id,
                )
            except Exception:
                pass

    def test_lookup_miss_then_alloc_then_lookup_hit_then_free(self):
        keys = [self._key(f"h{i}") for i in range(4)]
        try:
            # Lookup miss: every block is unknown to the cluster.
            miss = self.mgr.batch_lookup(keys, req_context=None)
            self.assertEqual(len(miss), len(keys))
            for k in keys:
                self.assertFalse(miss[k])

            # Allocate via prepare_store.
            spec = self.mgr.prepare_store(keys, req_context=None)
            self.assertIsNotNone(spec)
            allocated_keys = {s["block_hash"] for s in spec.specs}
            self.assertEqual(allocated_keys, set(keys))

            # complete_store(success=True) drives BatchWrite + STORED CAS.
            self.mgr.complete_store(
                keys,
                data={k: b"\x00" * 256 for k in keys},
                req_context=None,
                success=True,
            )

            # After STORED, lookup hits and refreshes leases.
            hits = self.mgr.batch_lookup(keys, req_context=None)
            for k in keys:
                self.assertTrue(hits.get(k), f"expected hit for {k}, got {hits.get(k)}")

            # touch (renew) succeeds end-to-end without raising.
            self.mgr.touch(keys, req_context=None)
        finally:
            self._cleanup(keys)

    def test_keys_route_across_both_dns(self):
        """Generate enough keys that hash modulo lands at least one on each DN."""
        keys = [self._key(f"router{i}") for i in range(32)]
        grouping = self.mgr._group_by_dn(keys)
        self.assertGreaterEqual(
            len(grouping), 2,
            f"expected keys to land on both DN1+DN2; got {grouping.keys()}"
        )
        # Sanity: every grouping bucket maps to one of the configured DNs.
        for dn_id in grouping.keys():
            self.assertIn(dn_id, self.shard_table)

    def test_complete_store_failure_path_frees_allocations(self):
        keys = [self._key(f"fail{i}") for i in range(3)]
        try:
            spec = self.mgr.prepare_store(keys, req_context=None)
            self.assertIsNotNone(spec)
            self.assertGreater(len(spec.specs), 0)
            # Simulate a host-side failure (e.g., dropped payload). Manager
            # must call BatchFreeAllocated on every key.
            self.mgr.complete_store(keys, data=None, req_context=None, success=False)
            # local_cache entries are dropped.
            for k in keys:
                self.assertNotIn(k, self.mgr.local_cache)
        finally:
            self._cleanup(keys)


if __name__ == "__main__":
    unittest.main()
