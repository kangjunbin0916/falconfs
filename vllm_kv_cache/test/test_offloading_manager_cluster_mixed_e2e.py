#!/usr/bin/env python3
"""End-to-end OffloadingManager tests for 3-DN + multi-store mixed topology (v6.6.x).

Requires ``scripts/falcon_distributed_test.sh start`` with ``KV_THREE_DNS=1`` and
``STORE_COUNT=4`` (full regression profile). Tests are skipped when DN3 or the
fourth store BRPC port is unreachable so smoke / two-DN clusters stay green.

**v6.6.x alignment (design §29.5):** ``FalconFSOffloadingManager`` uses
``_data_stage_pool`` with default ``FALCON_KV_CLIENT_DATA_PARALLELISM_MAX=min(64,hw*2)``
and ``FALCON_KV_CLIENT_META_PARALLELISM_MAX=num_dns`` — **no** legacy
``FALCON_KV_OM_PARALLEL_STORE_IO`` switch. Data plane work is **one unary RPC per
logical block** (``Store.write`` / ``Store.read``) fanned out across the pool so
the KV store engine sees concurrent ``WriteBlock`` / ``ReadBlock`` traffic.

Throughput and facade path checks are recorded in ``throughput_events`` (no
``print``). Optionally write JSON::

    FALCON_MIX_THROUGHPUT_JSON=/path/out.json

Workload sizing (vLLM-aligned)::

    FALCON_MIX_KV_BLOCK_BYTES       # default vLLM formula (must match catalog block_size)
    FALCON_MIX_THROUGHPUT_KEYS      # default scales with CPU count (stress many concurrent blocks)
    FALCON_MIX_BATCH_TOUCH_KEYS     # default scales with CPU count
    FALCON_MIX_REQUIRE_STORE_MB_S   # optional: fail if ``complete_store`` MB/s below this (decimal MB/s)

**Throughput:** §29.8 / §19.2 describe **scaling vs** ``client_data_parallelism_max`` and
hardware (memcpy / NIC). Python→BRPC on a single host rarely hits 10+ GB/s; for a
**strict** high-bandwidth gate (e.g. colocated SHM + many keys), set
``FALCON_MIX_REQUIRE_STORE_MB_S`` (e.g. ``10000`` for ~10 GB/s in decimal MB/s) in
addition to raising ``FALCON_MIX_THROUGHPUT_KEYS`` and ``FALCON_KV_CLIENT_DATA_PARALLELISM_MAX``.

The module sets ``FALCON_KV_STORE_BRPC_ENDPOINT`` to ``127.0.0.1:${KV_STORE_BRPC_PORT}``
when unset so store I/O targets ``falcon_kv_store`` BRPC, not the DN pooler.
For the single-node harness, ``psql`` can prime ``falcon_dn_node.healthy`` before
each test (opt out with ``FALCON_MIX_SKIP_DN_PRIME=1``).
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import time
import unittest
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))

DN1_POOLER = os.environ.get("FALCON_KV_DN1_POOLER", "127.0.0.1:55530")
DN2_POOLER = os.environ.get("FALCON_KV_DN2_POOLER", "127.0.0.1:55550")
DN3_POOLER = os.environ.get("FALCON_KV_DN3_POOLER", "127.0.0.1:55570")
CN_SQL = os.environ.get("FALCON_KV_CN_SQL", "127.0.0.1:55500")
STORE_BASE = int(os.environ.get("KV_STORE_BRPC_PORT", "18765"))
# BrpcCluster defaults store I/O to DN endpoint if unset; mixed harness writes
# ``/tmp/falcon_kv_store_env.sh`` for daemons only — tests must point at BRPC.
os.environ.setdefault("FALCON_KV_STORE_BRPC_ENDPOINT", f"127.0.0.1:{STORE_BASE}")

# vLLM: bytes per logical GPU KV block ≈ 2 * num_layers * num_kv_heads * head_dim
# * gpu_block_tokens * sizeof(dtype).  Representative fp16 Llama-class slice:
_VLLM_FORMULA_KV_BYTES = 2 * 32 * 8 * 128 * 16 * 2


def _mixed_workload_block_bytes() -> int:
    # Default matches ``falcon_kv_store`` / harness vLLM-style block_size (see
    # ``FALCON_KV_STORE_BLOCK_SIZE``). Override with ``FALCON_MIX_KV_BLOCK_BYTES``
    # for smaller catalog rows when running legacy 64 KiB stores.
    raw = int(os.environ.get("FALCON_MIX_KV_BLOCK_BYTES", str(_VLLM_FORMULA_KV_BYTES)))
    return max(4096, min(raw, _VLLM_FORMULA_KV_BYTES))


def _mixed_throughput_num_keys() -> int:
    # §29.8: enough concurrent unary data tasks to stress the engine; cap total payload for CI RAM.
    cpus = os.cpu_count() or 8
    default_nk = max(96, min(384, cpus * 24))
    return max(8, int(os.environ.get("FALCON_MIX_THROUGHPUT_KEYS", str(default_nk))))


def _mixed_batch_touch_key_count() -> int:
    cpus = os.cpu_count() or 8
    default_b = max(48, min(256, cpus * 16))
    return max(4, int(os.environ.get("FALCON_MIX_BATCH_TOUCH_KEYS", str(default_b))))


def _require_store_mb_s() -> float:
    """Optional floor on ``complete_store`` payload MB/s (decimal MB = 1e6 bytes)."""
    raw = os.environ.get("FALCON_MIX_REQUIRE_STORE_MB_S", "").strip()
    if not raw:
        return 0.0
    try:
        return float(raw)
    except ValueError:
        return 0.0


def _mixed_payload(byte_count: int, *, variant: int = 0) -> bytes:
    pat = bytes(((i + variant) & 0xFF) for i in range(256))
    rep = (byte_count + 255) // 256
    return (pat * rep)[:byte_count]


def _can_reach(endpoint: str, timeout_s: float = 1.0) -> bool:
    host, _, port = endpoint.partition(":")
    try:
        with socket.create_connection((host, int(port)), timeout=timeout_s):
            return True
    except OSError:
        return False


def _three_dns_reachable() -> bool:
    return all(
        _can_reach(ep)
        for ep in (CN_SQL, DN1_POOLER, DN2_POOLER, DN3_POOLER)
    )


def _four_kv_stores_listening() -> bool:
    for i in range(4):
        if not _can_reach(f"127.0.0.1:{STORE_BASE + i}"):
            return False
    return True


def mixed_topology_available() -> bool:
    """True when CN + 3 DN BRPC poolers and four ``falcon_kv_store`` ports answer.

    Evaluated at call time (not import time) so harness ``start`` completes before checks.
    """
    return _three_dns_reachable() and _four_kv_stores_listening()


def _prime_dn_catalog_for_mixed_e2e(conninfo: str) -> None:
    """Single-node harness: CN watchdog can mark DNs unhealthy without live heartbeats.

    Without healthy rows, ``discover_dn_endpoints`` is empty and the facade registry
    skips DN metadata routing. Priming keeps Python E2E aligned with CI clusters
    that run full DN stacks. Opt out with ``FALCON_MIX_SKIP_DN_PRIME=1``.
    """
    if os.environ.get("FALCON_MIX_SKIP_DN_PRIME", "").strip().lower() in ("1", "true", "yes", "on"):
        return
    if not shutil.which("psql"):
        return
    sql = (
        "UPDATE pg_catalog.falcon_dn_node SET healthy = true, "
        "last_heartbeat_ms = (EXTRACT(EPOCH FROM now()) * 1000)::bigint;"
    )
    subprocess.run(
        ["psql", conninfo, "-v", "ON_ERROR_STOP=1", "-q", "-c", sql],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )


def _parse_child_report(stdout: str) -> Optional[Dict[str, Any]]:
    for line in stdout.splitlines():
        if line.startswith("FALCON_MIX_REPORT\t"):
            return json.loads(line.split("\t", 1)[1])
    return None


class OffloadingManagerClusterMixedE2E(unittest.TestCase):
    throughput_events: ClassVar[List[Dict[str, Any]]] = []

    @classmethod
    def setUpClass(cls) -> None:
        if not mixed_topology_available():
            raise unittest.SkipTest(
                "Need CN + 3 DN poolers + 4 store BRPC ports (KV_THREE_DNS=1, STORE_COUNT=4 cluster)"
            )
        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        out = os.environ.get("FALCON_MIX_THROUGHPUT_JSON", "").strip()
        if out and cls.throughput_events:
            with open(out, "w", encoding="utf-8") as f:
                json.dump(cls.throughput_events, f, indent=2, ensure_ascii=False)
        super().tearDownClass()

    @classmethod
    def _record(cls, event: Dict[str, Any]) -> None:
        event["ts_wall_ms"] = int(time.time() * 1000)
        cls.throughput_events.append(event)

    def setUp(self) -> None:
        from falconfs_kv.offloading_manager import FalconFSOffloadingManager  # noqa: WPS433

        self.cn_conninfo = os.environ.get(
            "FALCON_KV_CN_CONNINFO",
            f"host=127.0.0.1 port=55500 dbname=postgres user={os.environ.get('USER', 'postgres')}",
        )
        _prime_dn_catalog_for_mixed_e2e(self.cn_conninfo)
        self._prefix = f"py_omix_{int(time.time() * 1000)}_{id(self)}"
        self._kv_block_bytes = _mixed_workload_block_bytes()
        self.mgr = FalconFSOffloadingManager(
            client_id=5150,
            client_hostname="py-mixed-e2e-host",
            mode="cluster",
            block_size=self._kv_block_bytes,
            timeout_ms=45000,
            cn_conninfo=self.cn_conninfo,
        )
        self.shard_table = dict(self.mgr.shard_table)
        self.assertGreaterEqual(
            len(self.shard_table),
            3,
            f"expected ≥3 DNs in shard_table, got {self.shard_table!r}",
        )

    def _key(self, name: str) -> str:
        return f"{self._prefix}_{name}"

    def _cleanup(self, keys: List[str]) -> None:
        for k in keys:
            loc = self.mgr.local_cache.get(k)
            if loc is None:
                continue
            cluster = self.mgr._clusters.get(loc.dn_id)
            if cluster is None:
                continue
            try:
                cluster.metadata.free_allocated(
                    k,
                    expected_version=loc.version,
                    force=True,
                    request_id=f"{self._prefix}_cleanup_{k}",
                    client_id=self.mgr.client_id,
                )
            except Exception:
                pass

    def test_routing_touches_at_least_three_dns(self) -> None:
        keys = [self._key(f"r{i}") for i in range(72)]
        grouping = self.mgr._group_by_dn(keys)
        self.assertGreaterEqual(
            len(grouping),
            3,
            f"expected keys to span ≥3 DNs; got dn_ids={sorted(grouping.keys())}",
        )
        type(self)._record({"test": "routing_three_dns", "distinct_dns": len(grouping)})

    def test_batch_store_load_touch_full_block_three_dns(self) -> None:
        n = _mixed_batch_touch_key_count()
        keys = [self._key(f"b{i}") for i in range(n)]
        bs = self.mgr.block_size
        payload = {k: _mixed_payload(bs, variant=i) for i, k in enumerate(keys)}
        nbytes = sum(len(v) for v in payload.values())
        try:
            bs = self.mgr.block_size
            miss = self.mgr.batch_lookup(keys, None)
            for k in keys:
                self.assertFalse(miss[k])

            t0 = time.perf_counter()
            spec = self.mgr.prepare_store(keys, None)
            t_prepare = time.perf_counter() - t0
            self.assertIsNotNone(spec)
            self.assertEqual({s["block_hash"] for s in spec.specs}, set(keys))

            t1 = time.perf_counter()
            self.mgr.complete_store(keys, payload, None, success=True)
            t_store = time.perf_counter() - t1

            hits = self.mgr.batch_lookup(keys, None)
            for k in keys:
                self.assertTrue(hits.get(k), k)

            t2 = time.perf_counter()
            ld = self.mgr.prepare_load(keys, None)
            t_load = time.perf_counter() - t2
            for k in keys:
                self.assertEqual(ld.data.get(k), payload[k], k)

            self.mgr.complete_load(keys, None)
            self.mgr.touch(keys, None)

            mb = nbytes / 1.0e6
            type(self)._record(
                {
                    "test": "batch_store_load_touch_multi",
                    "keys": len(keys),
                    "kv_block_bytes": bs,
                    "vllm_formula_kv_bytes": _VLLM_FORMULA_KV_BYTES,
                    "bytes_total": nbytes,
                    "seconds_prepare_store": round(t_prepare, 6),
                    "seconds_complete_store": round(t_store, 6),
                    "seconds_prepare_load": round(t_load, 6),
                    "mb_s_complete_store": round(mb / t_store, 3) if t_store > 0 else None,
                    "mb_s_prepare_load": round(mb / t_load, 3) if t_load > 0 else None,
                }
            )
            floor = _require_store_mb_s()
            if floor > 0 and t_store > 0:
                self.assertGreaterEqual(
                    mb / t_store,
                    floor,
                    f"complete_store MB/s below FALCON_MIX_REQUIRE_STORE_MB_S={floor}",
                )
        finally:
            self._cleanup(keys)

    def test_facade_ipc_stats_track_batch_store_and_load(self) -> None:
        """Each successful facade read/write bumps C++ counters (local or remote)."""
        from falconfs_kv import falconfs_kv_brpc as b

        b.facade_ipc_stats_reset()
        keys = [self._key(f"ipc{i}") for i in range(3)]
        bs = self.mgr.block_size
        payload = {k: _mixed_payload(bs, variant=i) for i, k in enumerate(keys)}
        nbytes = sum(len(v) for v in payload.values())
        try:
            self.assertFalse(any(self.mgr.batch_lookup(keys, None).values()))
            t0 = time.perf_counter()
            self.assertIsNotNone(self.mgr.prepare_store(keys, None))
            t_ps = time.perf_counter() - t0
            t1 = time.perf_counter()
            self.mgr.complete_store(keys, payload, None, success=True)
            t_cs = time.perf_counter() - t1
            lr0, lw0, rr0, rw0 = b.facade_ipc_stats()
            self.assertGreaterEqual(lw0 + rw0, 3, "three facade writes expected")
            t2 = time.perf_counter()
            ld = self.mgr.prepare_load(keys, None)
            t_pl = time.perf_counter() - t2
            for k in keys:
                self.assertEqual(ld.data.get(k), payload[k])
            lr1, lw1, rr1, rw1 = b.facade_ipc_stats()
            self.assertGreaterEqual((lr1 - lr0) + (rr1 - rr0), 3, "three facade reads expected")
            self.mgr.complete_load(keys, None)
            mb = nbytes / 1.0e6
            type(self)._record(
                {
                    "test": "facade_ipc_batch_3keys",
                    "kv_block_bytes": bs,
                    "vllm_formula_kv_bytes": _VLLM_FORMULA_KV_BYTES,
                    "facade_after_write": {"lr": int(lr0), "lw": int(lw0), "rr": int(rr0), "rw": int(rw0)},
                    "facade_after_read": {"lr": int(lr1), "lw": int(lw1), "rr": int(rr1), "rw": int(rw1)},
                    "local_writes": int(lw0),
                    "remote_writes": int(rw0),
                    "local_read_delta": int(lr1 - lr0),
                    "remote_read_delta": int(rr1 - rr0),
                    "seconds_prepare_store": round(t_ps, 6),
                    "seconds_complete_store": round(t_cs, 6),
                    "seconds_prepare_load": round(t_pl, 6),
                    "mb_s_complete_store": round(mb / t_cs, 3) if t_cs > 0 else None,
                    "mb_s_prepare_load": round(mb / t_pl, 3) if t_pl > 0 else None,
                }
            )
        finally:
            self._cleanup(keys)

    def test_batch_throughput_local_vs_remote_facade_split(self) -> None:
        """Larger batch: classify blocks by local vs remote store from ``store_locality()``."""
        from falconfs_kv import falconfs_kv_brpc as b

        loc = dict(b.store_locality())
        nk = _mixed_throughput_num_keys()
        keys = [self._key(f"tp{i}") for i in range(nk)]
        bs = self.mgr.block_size
        payload = {k: _mixed_payload(bs, variant=i) for i, k in enumerate(keys)}
        nbytes = sum(len(v) for v in payload.values())
        b.facade_ipc_stats_reset()
        try:
            self.assertFalse(any(self.mgr.batch_lookup(keys, None).values()))
            t0 = time.perf_counter()
            spec = self.mgr.prepare_store(keys, None)
            t_ps = time.perf_counter() - t0
            self.assertIsNotNone(spec)
            assert spec is not None
            by_sid: Dict[str, int] = {}
            for s in spec.specs:
                h = s["block_hash"]
                by_sid[h] = int(s["store_id"])
            t1 = time.perf_counter()
            self.mgr.complete_store(keys, payload, None, success=True)
            t_cs = time.perf_counter() - t1
            lr0, lw0, rr0, rw0 = b.facade_ipc_stats()
            t2 = time.perf_counter()
            ld = self.mgr.prepare_load(keys, None)
            t_pl = time.perf_counter() - t2
            for k in keys:
                self.assertEqual(ld.data[k], payload[k])
            lr1, lw1, rr1, rw1 = b.facade_ipc_stats()
            self.mgr.complete_load(keys, None)

            local_blocks = 0
            remote_blocks = 0
            for k in keys:
                sid = by_sid.get(k, 0)
                is_local = bool(loc.get(sid))
                if is_local:
                    local_blocks += 1
                else:
                    remote_blocks += 1
            self.assertGreater(local_blocks + remote_blocks, 0)
            self.assertGreaterEqual(lw0 + rw0, len(keys))
            self.assertGreaterEqual((lr1 - lr0) + (rr1 - rr0), len(keys))

            mb = nbytes / 1.0e6
            type(self)._record(
                {
                    "test": "throughput_local_remote_split",
                    "keys": len(keys),
                    "kv_block_bytes": bs,
                    "vllm_formula_kv_bytes": _VLLM_FORMULA_KV_BYTES,
                    "client_data_parallelism_max": int(self.mgr._data_parallelism_max),
                    "client_meta_parallelism_max": int(self.mgr._meta_parallelism_max),
                    "blocks_local_store": local_blocks,
                    "blocks_remote_store": remote_blocks,
                    "store_locality": {int(k): bool(v) for k, v in loc.items()},
                    "facade_after_write": {"lr": int(lr0), "lw": int(lw0), "rr": int(rr0), "rw": int(rw0)},
                    "facade_after_read": {"lr": int(lr1), "lw": int(lw1), "rr": int(rr1), "rw": int(rw1)},
                    "seconds_prepare_store": round(t_ps, 6),
                    "seconds_complete_store": round(t_cs, 6),
                    "seconds_prepare_load": round(t_pl, 6),
                    "mb_s_complete_store": round(mb / t_cs, 3) if t_cs > 0 else None,
                    "mb_s_prepare_load": round(mb / t_pl, 3) if t_pl > 0 else None,
                    "bytes_total": nbytes,
                }
            )
            floor = _require_store_mb_s()
            if floor > 0 and t_cs > 0:
                self.assertGreaterEqual(
                    mb / t_cs,
                    floor,
                    f"complete_store MB/s below FALCON_MIX_REQUIRE_STORE_MB_S={floor}",
                )
        finally:
            self._cleanup(keys)

    def test_refresh_membership_sees_multiple_stores(self) -> None:
        num_dns, num_stores, gen = 0, 0, 0
        for _attempt in range(3):
            stats = self.mgr.refresh_membership(timeout_ms=12000)
            self.assertEqual(len(stats), 3)
            num_dns, num_stores, gen = int(stats[0]), int(stats[1]), int(stats[2])
            if num_stores >= 4 and num_dns >= 3:
                break
            time.sleep(0.4)
        self.assertGreaterEqual(num_dns, 3)
        self.assertGreaterEqual(num_stores, 4)
        type(self)._record(
            {"test": "refresh_membership", "num_dns": num_dns, "num_stores": num_stores}
        )

    def test_four_node_name_clients_subprocess_roundtrip(self) -> None:
        """Four processes × ``NODE_NAME=v65mix{0..3}`` — store/read + throughput JSON."""
        child = Path(__file__).resolve().parent / "mixed_topology_offloader_child.py"
        self.assertTrue(child.is_file(), f"missing helper {child}")
        env_base = os.environ.copy()
        env_base["PYTHONPATH"] = f"{ROOT / 'python'}:{ROOT / 'test'}"
        env_base.setdefault("FALCON_KV_STORE_BRPC_ENDPOINT", f"127.0.0.1:{STORE_BASE}")
        env_base["FALCON_MIX_KV_BLOCK_BYTES"] = str(self._kv_block_bytes)
        env_base["FALCON_KV_CN_CONNINFO"] = self.cn_conninfo
        for i in range(4):
            _prime_dn_catalog_for_mixed_e2e(self.cn_conninfo)
            key = self._key(f"subc{i}")
            env = env_base.copy()
            env["NODE_NAME"] = f"v65mix{i}"
            env["FALCON_MIX_KEY"] = key
            env["FALCON_MIX_CLIENT_ID"] = str(9200 + i)
            rc = subprocess.run(
                [sys.executable, str(child)],
                env=env,
                capture_output=True,
                text=True,
                timeout=120,
            )
            self.assertEqual(
                rc.returncode,
                0,
                f"child i={i} failed rc={rc.returncode} stderr={rc.stderr!r} stdout={rc.stdout!r}",
            )
            rep = _parse_child_report(rc.stdout)
            self.assertIsNotNone(rep, f"no FALCON_MIX_REPORT line in stdout={rc.stdout!r}")
            assert rep is not None
            self.assertTrue(rep.get("ok"), rep)
            # Child already validates SHM vs BRPC counters for colocated vs fallback alloc.
            self.assertGreater(rep.get("mb_s_complete_store") or 0, 0, rep)
            self.assertGreater(rep.get("mb_s_prepare_load") or 0, 0, rep)
            rep["test"] = f"subprocess_client_v65mix{i}"
            type(self)._record(rep)


if __name__ == "__main__":
    unittest.main()
