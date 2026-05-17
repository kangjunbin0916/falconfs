#!/usr/bin/env python3
"""Direct KV Store data-path micro-benchmark.

This intentionally bypasses OffloadingManager metadata and leases. It writes and
reads fixed Store offsets to estimate the upper-bound of the Store facade data
path for a chosen block size, store set, and parallelism.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))

from falconfs_kv import falconfs_kv_brpc  # noqa: E402
from falconfs_kv.dn_client import BrpcMetadataService  # noqa: E402
from falconfs_kv.store_client import BrpcKVStore  # noqa: E402


def _payload(n: int, variant: int) -> bytes:
    pat = bytes(((i + variant) & 0xFF) for i in range(256))
    return (pat * ((n + 255) // 256))[:n]




def _copy_bound(block_bytes: int, iters: int = 256) -> Dict[str, float]:
    src = bytearray((i & 0xFF) for i in range(block_bytes))
    dst = bytearray(block_bytes)
    for _ in range(min(16, iters)):
        dst[:] = src
    t0 = time.perf_counter()
    for _ in range(iters):
        dst[:] = src
    copy_s = time.perf_counter() - t0
    one = bytes(src)
    t1 = time.perf_counter()
    for _ in range(iters):
        _ = bytes(bytearray(one))
    alloc_s = time.perf_counter() - t1
    bytes_total = float(block_bytes * iters)
    return {
        "copy_ms_per_block": round(1000.0 * copy_s / max(iters, 1), 4),
        "copy_mb_s": round((bytes_total / 1.0e6) / max(copy_s, 1e-12), 3),
        "alloc_copy_ms_per_block": round(1000.0 * alloc_s / max(iters, 1), 4),
        "alloc_copy_mb_s": round((bytes_total / 1.0e6) / max(alloc_s, 1e-12), 3),
    }


def _target_analysis(result: Dict[str, Any]) -> Dict[str, Any]:
    copy_bound = result.get("copy_bound") or {}
    read = result.get("read") or {}
    write = result.get("write") or {}
    alloc_ms = float(copy_bound.get("alloc_copy_ms_per_block", 0.0) or 0.0)
    read_avg = float(read.get("avg_ms", 0.0) or 0.0)
    write_avg = float(write.get("avg_ms", 0.0) or 0.0)
    return {
        "remote_e2e_target_read_mb_s": 1000.0,
        "remote_e2e_target_write_mb_s": 1500.0,
        "microbench_target_read_mb_s": 3000.0,
        "microbench_target_write_mb_s": 3000.0,
        "read_avg_vs_alloc_copy_x": round(read_avg / alloc_ms, 2) if alloc_ms > 0 else None,
        "write_avg_vs_alloc_copy_x": round(write_avg / alloc_ms, 2) if alloc_ms > 0 else None,
        "read_gap_hint": (
            "RPC/queueing/Python allocation dominates local copy"
            if alloc_ms > 0 and read_avg > 5.0 * alloc_ms
            else "near local copy bound"
        ),
    }

def _percentile(vals: List[float], pct: float) -> float:
    if not vals:
        return 0.0
    vals = sorted(vals)
    idx = min(len(vals) - 1, max(0, int(round((pct / 100.0) * (len(vals) - 1)))))
    return vals[idx]


def _summarize_phase(name: str, seconds: List[float], byte_count: int, wall_s: float) -> Dict[str, Any]:
    return {
        "phase": name,
        "ops": len(seconds),
        "bytes": byte_count,
        "wall_s": round(wall_s, 6),
        "wall_mb_s": round((byte_count / 1.0e6) / max(wall_s, 1e-12), 3),
        "avg_ms": round(1000.0 * statistics.mean(seconds), 4) if seconds else 0.0,
        "p50_ms": round(1000.0 * _percentile(seconds, 50), 4),
        "p95_ms": round(1000.0 * _percentile(seconds, 95), 4),
        "p99_ms": round(1000.0 * _percentile(seconds, 99), 4),
        "instrumented_mb_s": round((byte_count / 1.0e6) / max(sum(seconds), 1e-12), 3),
    }


def _print_table(result: Dict[str, Any]) -> None:
    print("--- KV_STORE_MICROBENCH ---")
    print(
        f"mode={result['mode']} allocation={result.get('allocation', 'fixed')} stores={result['stores']} "
        f"blocks={result['blocks']} block_bytes={result['block_bytes']} parallelism={result['parallelism']}"
    )
    for phase in ("write", "read"):
        p = result[phase]
        print(
            f"{phase:>5}: wall={p['wall_s']:.6f}s wall_MB/s={p['wall_mb_s']:.3f} "
            f"avg={p['avg_ms']:.4f}ms p50={p['p50_ms']:.4f}ms "
            f"p95={p['p95_ms']:.4f}ms p99={p['p99_ms']:.4f}ms "
            f"instr_MB/s={p['instrumented_mb_s']:.3f}"
        )
    cb = result.get("copy_bound") or {}
    if cb:
        print("copy_bound=" + json.dumps(cb, sort_keys=True))
    ta = result.get("target_analysis") or {}
    if ta:
        print("target_analysis=" + json.dumps(ta, sort_keys=True))
    cxx = result.get("facade_perf_stats") or {}
    if cxx:
        print("facade_perf_stats=" + json.dumps(cxx, sort_keys=True))
    print("--- end KV_STORE_MICROBENCH ---")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cn", default=os.environ.get("FALCON_KV_CN_CONNINFO", "host=127.0.0.1 port=55500 dbname=postgres"))
    ap.add_argument("--endpoint", default=os.environ.get("FALCON_KV_STORE_BRPC_ENDPOINT", "127.0.0.1:18765"))
    ap.add_argument("--metadata-endpoint", default=os.environ.get("FALCON_KV_MICRO_METADATA_ENDPOINT", "127.0.0.1:55530"))
    ap.add_argument("--stores", default=os.environ.get("FALCON_KV_MICRO_STORES", "1,2,3,4"))
    ap.add_argument("--blocks", type=int, default=int(os.environ.get("FALCON_KV_MICRO_BLOCKS", "256")))
    ap.add_argument("--block-bytes", type=int, default=int(os.environ.get("FALCON_KV_MICRO_BLOCK_BYTES", str(2 * 1024 * 1024))))
    ap.add_argument("--parallelism", type=int, default=int(os.environ.get("FALCON_KV_MICRO_PARALLELISM", str((os.cpu_count() or 8) * 2))))
    ap.add_argument("--epoch", type=int, default=int(os.environ.get("FALCON_KV_MICRO_STORE_EPOCH", "1")))
    ap.add_argument("--mode", choices=("facade", "direct"), default=os.environ.get("FALCON_KV_MICRO_MODE", "facade"))
    ap.add_argument("--allocation", choices=("metadata", "fixed"), default=os.environ.get("FALCON_KV_MICRO_ALLOCATION", "metadata"),
                    help="metadata allocates valid Store offsets first; fixed writes synthetic offsets for isolated Store smoke tests")
    ap.add_argument("--key-prefix", default=os.environ.get("FALCON_KV_MICRO_KEY_PREFIX", ""))
    ap.add_argument("--keep-allocations", action="store_true")
    ap.add_argument("--json", default=os.environ.get("FALCON_KV_MICRO_JSON", ""))
    ap.add_argument("--copy-bound-iters", type=int, default=int(os.environ.get("FALCON_KV_MICRO_COPY_BOUND_ITERS", "256")))
    ap.add_argument("--no-verify", action="store_true")
    args = ap.parse_args()

    stores = [int(x) for x in args.stores.split(",") if x.strip()]
    if not stores:
        raise SystemExit("no stores selected")

    if args.mode == "facade":
        falconfs_kv_brpc.membership_start(args.cn)
        falconfs_kv_brpc.membership_refresh(20000)
        if hasattr(falconfs_kv_brpc, "facade_perf_stats_reset"):
            falconfs_kv_brpc.facade_perf_stats_reset()
    store = BrpcKVStore(args.endpoint, timeout_ms=30000, use_facade_registry=(args.mode == "facade"))

    total_bytes = args.blocks * args.block_bytes
    key_prefix = args.key_prefix or f"micro-{int(time.time() * 1000)}-{os.getpid()}"
    block_hashes = [f"{key_prefix}-{i}" for i in range(args.blocks)]
    versions = [0 for _ in range(args.blocks)]
    leases = [None for _ in range(args.blocks)]

    if args.allocation == "metadata":
        old_pref = os.environ.get("FALCON_KV_PREFERRED_STORE_IDS")
        if old_pref is None:
            os.environ["FALCON_KV_PREFERRED_STORE_IDS"] = ",".join(str(s) for s in stores)
        meta = BrpcMetadataService(args.metadata_endpoint, timeout_ms=30000, block_size=args.block_bytes)
        alloc = meta.allocate(block_hashes, request_id=f"{key_prefix}-alloc")
        assignments = []
        allocated_for_cleanup = []
        try:
            for i, h in enumerate(block_hashes):
                ir, row, lease = alloc.get(h, (None, None, None))
                if ir is None or not ir.success or row is None:
                    msg = ir.error_message if ir is not None else "missing allocation result"
                    raise RuntimeError(f"metadata allocation failed idx={i} hash={h}: {msg}")
                assignments.append((int(row.location.store_node_id), int(row.location.pool_offset), i))
                versions[i] = int(row.version)
                leases[i] = lease
                allocated_for_cleanup.append((h, versions[i], True))
        except Exception:
            if allocated_for_cleanup:
                try:
                    meta.batch_free_allocated(allocated_for_cleanup, request_id=f"{key_prefix}-alloc-fail-free")
                except Exception:
                    pass
            raise
        finally:
            if old_pref is None:
                os.environ.pop("FALCON_KV_PREFERRED_STORE_IDS", None)
    else:
        meta = None
        per_store_slots: Dict[int, int] = {sid: 0 for sid in stores}
        assignments = []
        for i in range(args.blocks):
            sid = stores[i % len(stores)]
            slot = per_store_slots[sid]
            per_store_slots[sid] = slot + 1
            assignments.append((sid, slot * args.block_bytes, i))

    payloads = [_payload(args.block_bytes, i) for i in range(args.blocks)]

    def do_write(item: Tuple[int, int, int]) -> float:
        sid, off, idx = item
        t0 = time.perf_counter()
        r = store.write(sid, off, payloads[idx], args.epoch, args.block_bytes, expected_version=versions[idx], block_hash=block_hashes[idx].encode())
        dt = time.perf_counter() - t0
        if not r.success:
            raise RuntimeError(f"write failed sid={sid} off={off}: {r.error_message}")
        return dt

    def do_read(item: Tuple[int, int, int]) -> float:
        sid, off, idx = item
        t0 = time.perf_counter()
        r, data = store.read(sid, off, args.epoch, expected_version=versions[idx], block_size=args.block_bytes, block_hash=block_hashes[idx].encode())
        dt = time.perf_counter() - t0
        if not r.success:
            raise RuntimeError(f"read failed sid={sid} off={off}: {r.error_message}")
        if not args.no_verify and data != payloads[idx]:
            raise RuntimeError(f"read mismatch sid={sid} off={off} idx={idx}")
        return dt

    def run_phase(fn) -> Tuple[List[float], float]:
        t0 = time.perf_counter()
        out: List[float] = []
        with ThreadPoolExecutor(max_workers=args.parallelism) as ex:
            futs = [ex.submit(fn, item) for item in assignments]
            for fut in as_completed(futs):
                out.append(float(fut.result()))
        return out, time.perf_counter() - t0

    # Warm pooled BRPC channels and facade mappings outside the measured window.
    seen = set()
    for item in assignments:
        sid, _off, _idx = item
        if sid in seen:
            continue
        seen.add(sid)
        do_write(item)
        do_read(item)

    write_s, write_wall = run_phase(do_write)
    read_s, read_wall = run_phase(do_read)

    result: Dict[str, Any] = {
        "mode": args.mode,
        "stores": stores,
        "blocks": args.blocks,
        "block_bytes": args.block_bytes,
        "parallelism": args.parallelism,
        "allocation": args.allocation,
        "metadata_endpoint": args.metadata_endpoint if args.allocation == "metadata" else "",
        "bytes_total": total_bytes,
        "write": _summarize_phase("write", write_s, total_bytes, write_wall),
        "read": _summarize_phase("read", read_s, total_bytes, read_wall),
        "copy_bound": _copy_bound(args.block_bytes, max(1, args.copy_bound_iters)),
        "ts_wall_ms": int(time.time() * 1000),
    }
    result["target_analysis"] = _target_analysis(result)
    if args.mode == "facade" and hasattr(falconfs_kv_brpc, "facade_perf_stats"):
        result["facade_perf_stats"] = dict(falconfs_kv_brpc.facade_perf_stats())
        try:
            result["store_locality"] = {int(k): bool(v) for k, v in dict(falconfs_kv_brpc.store_locality()).items()}
        except Exception:
            pass

    store_counts: Dict[int, int] = {}
    for sid, _off, _idx in assignments:
        store_counts[sid] = store_counts.get(sid, 0) + 1
    result["store_distribution"] = store_counts

    try:
        if args.allocation == "metadata" and not args.keep_allocations and meta is not None:
            frees = [(h, versions[i], True) for i, h in enumerate(block_hashes)]
            free_results = meta.batch_free_allocated(frees, request_id=f"{key_prefix}-free")
            result["free_allocated_success"] = sum(1 for ir, _ in free_results if ir.success)
    except Exception as e:
        result["free_allocated_error"] = str(e)

    _print_table(result)
    out = args.json or str(ROOT.parent / "logs" / "falcon_kv_store_microbench_latest.json")
    Path(out).parent.mkdir(parents=True, exist_ok=True)
    Path(out).write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
