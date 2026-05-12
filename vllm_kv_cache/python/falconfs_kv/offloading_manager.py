from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional

from .reference import BlockStatus, ErrorCode, ReferenceCluster
from .router import Router
from .promote_worker import PromoteWorker

try:
    from . import falconfs_kv_brpc
except ImportError:  # pragma: no cover - only in no-extension unit envs
    falconfs_kv_brpc = None


LEASE_DURATION_MS = 5000
BLOCK_SIZE = 65536

STATUS_ALLOCATED = int(BlockStatus.ALLOCATED)
STATUS_STORED = int(BlockStatus.STORED)
STATUS_EVICTING = int(BlockStatus.EVICTING)
STATUS_EVICTED = int(BlockStatus.EVICTED)
STATUS_FAILED = int(BlockStatus.FAILED)


@dataclass
class LoadStoreSpec:
    data: Dict[str, bytes] = field(default_factory=dict)
    specs: List[dict] = field(default_factory=list)


@dataclass
class KVBlockLocation:
    block_hash: str
    status: int
    store_id: int
    pool_offset: int
    lease_expire_ms: int
    evicted_path: Optional[str] = None
    lease_token: int = 0
    version: int = 0
    dn_epoch: int = 1
    store_epoch: int = 1
    dn_id: int = 0


def _route_dn_id(shard_table: Dict[int, str], block_hash: str) -> int:
    """Pick a DN id for the given block hash.

    The reference router uses a stable hash modulo of the shard-table keys
    so the same block always lands on the same DN. The native client will
    eventually replace this with the real FalconFS shard table lookup.
    """
    if not shard_table:
        raise RuntimeError("empty shard_table; at least one DN must be registered")
    keys = sorted(shard_table.keys())
    return keys[hash(block_hash) % len(keys)]


class FalconFSOffloadingManager:
    """Reference OffloadingManager with upstream public API names.

    The implementation uses one in-memory reference cluster per DN until C++
    BRPC clients are wired in. The `data` parameter on `complete_store` is a
    test scaffold that lets reference tests push payload bytes through the
    Python in-memory store; the native (cluster-mode) client receives bytes
    via the Store BRPC service and does not pass them through this method.
    """

    def __init__(
        self,
        client_id: int,
        client_hostname: str,
        shard_table: Optional[Dict[int, str]] = None,
        cluster=None,
        clusters: Optional[Dict[int, ReferenceCluster]] = None,
        cluster_factory: Optional[Callable[[int, str], ReferenceCluster]] = None,
        block_size: int = BLOCK_SIZE,
        mode: str = "reference",
        timeout_ms: int = 30000,
        cn_conninfo: Optional[str] = None,
    ):
        if mode == "cluster" and (not shard_table):
            if not cn_conninfo:
                raise RuntimeError("mode=cluster requires shard_table or cn_conninfo")
            if falconfs_kv_brpc is None:
                raise RuntimeError("falconfs_kv_brpc extension is required for cluster mode")
            falconfs_kv_brpc.membership_start(cn_conninfo)
            # Prime registry + DN endpoint view.
            falconfs_kv_brpc.membership_refresh(timeout_ms)
            shard_table = falconfs_kv_brpc.discover_dn_endpoints()
        self.shard_table = dict(shard_table or {})
        if mode == "cluster" and not self.shard_table:
            raise RuntimeError("cluster mode discovered no DN endpoints")
        self.client_id = client_id
        self.client_hostname = client_hostname
        self.local_cache: Dict[str, KVBlockLocation] = {}
        self.block_size = block_size
        self.mode = mode
        self._cn_conninfo = cn_conninfo
        self.timeout_ms = timeout_ms
        self._cluster_factory = cluster_factory
        self._cluster_singleton = cluster
        if clusters:
            self._clusters: Dict[int, ReferenceCluster] = clusters
        elif cluster_factory is not None:
            self._clusters = {
                dn_id: cluster_factory(dn_id, endpoint)
                for dn_id, endpoint in sorted(self.shard_table.items())
            }
        elif cluster is not None:
            keys = sorted(self.shard_table.keys()) or [0]
            self._clusters = {dn_id: cluster for dn_id in keys}
        elif mode == "cluster":
            # v6 §16: BRPC client per DN. Each DN endpoint is addressed by a
            # single `BrpcCluster` that exposes `metadata` + `store` shims so
            # the rest of this manager stays mode-agnostic.
            from .store_client import BrpcCluster
            self._clusters = {
                dn_id: BrpcCluster(endpoint, dn_id=dn_id, client_id=client_id,
                                   timeout_ms=timeout_ms, block_size=block_size,
                                   use_facade_registry=bool(cn_conninfo))
                for dn_id, endpoint in sorted(self.shard_table.items())
            }
        else:
            keys = sorted(self.shard_table.keys()) or [0]
            self._clusters = {dn_id: ReferenceCluster(block_size=block_size) for dn_id in keys}
        # Stable per-DN routing.
        self._router = Router(self.shard_table) if self.shard_table else None
        self._routing: Dict[str, int] = {}
        self._promote_worker = PromoteWorker(self) if mode == "cluster" else None

    def lookup(self, key: str, req_context=None) -> bool | None:
        result = self._batch_lookup_impl([key], req_context, renew_lease_on_hit=False)
        return result.get(key)

    def prepare_load(self, keys: List[str], req_context=None) -> LoadStoreSpec:
        lookup = self._batch_lookup_impl(keys, req_context, renew_lease_on_hit=True)
        missing = [key for key, hit in lookup.items() if not hit]
        if missing:
            raise RuntimeError(f"Blocks {missing} not found in cache")
        return self._batch_load_impl(keys)

    def complete_load(self, keys: List[str], req_context=None):
        self._batch_renew_impl(keys)

    def prepare_store(self, keys: List[str], req_context=None) -> Optional[LoadStoreSpec]:
        spec = self._batch_prepare_store_impl(keys, req_context)
        if not spec.specs:
            # match upstream contract: None when nothing is allocatable
            return None
        return spec

    def complete_store(
        self,
        keys: List[str],
        data: Optional[Dict[str, bytes]] = None,
        req_context=None,
        success: bool = True,
    ):
        if not success:
            self._free_allocated_for_keys(keys)
            return
        successful = self._batch_complete_store_impl(keys, data or {}, req_context)
        failed = [key for key in keys if key not in successful]
        if failed:
            self._free_allocated_for_keys(failed)

    def touch(self, keys: List[str], req_context=None):
        self._batch_renew_impl(keys)

    def refresh_membership(self, timeout_ms: int = 2000):
        if self.mode != "cluster":
            return (0, 0, 0)
        if falconfs_kv_brpc is None:
            raise RuntimeError("falconfs_kv_brpc extension is required for cluster mode")
        stats = falconfs_kv_brpc.membership_refresh(timeout_ms)
        discovered = dict(falconfs_kv_brpc.discover_dn_endpoints())
        if discovered and discovered != self.shard_table:
            self.shard_table = discovered
            if self._cluster_factory is not None:
                self._clusters = {
                    dn_id: self._cluster_factory(dn_id, endpoint)
                    for dn_id, endpoint in sorted(self.shard_table.items())
                }
            elif self._cluster_singleton is not None:
                self._clusters = {dn_id: self._cluster_singleton for dn_id in self.shard_table.keys()}
            else:
                from .store_client import BrpcCluster
                self._clusters = {
                    dn_id: BrpcCluster(endpoint, dn_id=dn_id, client_id=self.client_id,
                                       timeout_ms=self.timeout_ms, block_size=self.block_size,
                                       use_facade_registry=bool(self._cn_conninfo))
                    for dn_id, endpoint in sorted(self.shard_table.items())
                }
            self._router = Router(self.shard_table)
            self._routing.clear()
        return stats

    def batch_lookup(self, keys: List[str], req_context) -> Dict[str, bool]:
        return self._batch_lookup_impl(keys, req_context, renew_lease_on_hit=True)

    def batch_prepare_store(self, keys: List[str], req_context) -> LoadStoreSpec:
        spec = self.prepare_store(keys, req_context)
        return spec or LoadStoreSpec()

    def batch_complete_store(self, keys: List[str], data: Dict[str, bytes], req_context):
        self.complete_store(keys, data, req_context)

    def batch_prepare_load(self, keys: List[str], req_context) -> LoadStoreSpec:
        return self.prepare_load(keys, req_context)

    def batch_complete_load(self, keys: List[str], req_context):
        self.complete_load(keys, req_context)

    def batch_touch(self, keys: List[str], req_context):
        self.touch(keys, req_context)

    def _cluster_for(self, block_hash: str) -> ReferenceCluster:
        dn_id = self._routing.get(block_hash)
        if dn_id is None:
            dn_id = _route_dn_id(self.shard_table, block_hash)
            self._routing[block_hash] = dn_id
        if dn_id not in self._clusters:
            # fallback: use any available cluster
            dn_id = next(iter(self._clusters.keys()))
            self._routing[block_hash] = dn_id
        return self._clusters[dn_id]

    def _group_by_dn(self, keys: List[str]) -> Dict[int, List[str]]:
        grouped: Dict[int, List[str]] = {}
        for key in keys:
            dn_id = self._routing.get(key) or _route_dn_id(self.shard_table, key)
            self._routing[key] = dn_id
            grouped.setdefault(dn_id, []).append(key)
        return grouped

    def _batch_lookup_impl(self, keys: List[str], req_context, renew_lease_on_hit: bool) -> Dict[str, bool]:
        results: Dict[str, bool] = {}
        for dn_id, dn_keys in self._group_by_dn(keys).items():
            cluster = self._clusters[dn_id]
            for key, (result, row, lease) in cluster.metadata.lookup(
                dn_keys, renew_lease_on_hit=renew_lease_on_hit
            ).items():
                if result.success and row:
                    if lease:
                        self._cache_location(key, row, lease, dn_id)
                    elif int(row.status) == STATUS_EVICTED:
                        self.local_cache[key] = KVBlockLocation(
                            block_hash=key,
                            status=int(row.status),
                            store_id=row.location.store_node_id,
                            pool_offset=row.location.pool_offset,
                            lease_expire_ms=self._now_ms(),
                            evicted_path=row.location.evicted_path,
                            lease_token=0,
                            version=row.version,
                            dn_epoch=1,
                            store_epoch=row.location.store_epoch,
                            dn_id=dn_id,
                        )
                    results[key] = True
                elif result.error_code in (ErrorCode.CAS_CONFLICT, ErrorCode.THROTTLED):
                    results[key] = None  # type: ignore[assignment]
                else:
                    results[key] = False
        return results

    def _batch_prepare_store_impl(self, keys: List[str], req_context) -> LoadStoreSpec:
        request_id = self._mk_request_id()
        specs: List[dict] = []
        for dn_id, dn_keys in self._group_by_dn(keys).items():
            cluster = self._clusters[dn_id]
            allocations = cluster.metadata.allocate(
                dn_keys,
                request_id=request_id,
                client_id=self.client_id,
            )
            for key, (result, row, lease) in allocations.items():
                if not result.success or not row or not lease:
                    continue
                self._cache_location(key, row, lease, dn_id)
                specs.append(
                    {
                        "block_hash": row.block_hash,
                        "store_id": row.location.store_node_id,
                        "pool_offset": row.location.pool_offset,
                        "dn_id": dn_id,
                    }
                )
        return LoadStoreSpec(specs=specs)

    def _batch_complete_store_impl(
        self, keys: List[str], data: Dict[str, bytes], req_context
    ) -> Dict[str, bool]:
        batch_id = self._mk_request_id()
        successful: Dict[str, bool] = {}
        for key in keys:
            payload = data.get(key)
            if payload is None:
                continue
            loc = self.local_cache.get(key)
            if not loc:
                continue
            cluster = self._clusters.get(loc.dn_id) or self._cluster_for(key)
            write = cluster.store.write(
                loc.store_id,
                loc.pool_offset,
                payload,
                loc.store_epoch,
                self.block_size,
            )
            if not write.success:
                continue
            update, row = cluster.metadata.update_status(
                key,
                BlockStatus.ALLOCATED,
                BlockStatus.STORED,
                loc.version,
                request_id=f"{batch_id}:{key}",
                client_id=self.client_id,
            )
            if update.success and row:
                loc.status = int(row.status)
                loc.version = row.version
                successful[key] = True
        return successful

    def _free_allocated_for_keys(self, keys: List[str]) -> None:
        batch_id = self._mk_request_id()
        for key in keys:
            loc = self.local_cache.get(key)
            if not loc:
                continue
            cluster = self._clusters.get(loc.dn_id)
            if cluster is None:
                continue
            cluster.metadata.free_allocated(
                key,
                expected_version=loc.version,
                request_id=f"{batch_id}:{key}",
                client_id=self.client_id,
            )
            self.local_cache.pop(key, None)
            self._routing.pop(key, None)

    def _batch_load_impl(self, keys: List[str]) -> LoadStoreSpec:
        loaded: Dict[str, bytes] = {}
        for key in keys:
            loc = self.local_cache.get(key)
            if not loc:
                raise RuntimeError(f"Block {key} not found in local cache")
            cluster = self._clusters.get(loc.dn_id) or self._cluster_for(key)
            if loc.status == STATUS_EVICTED and loc.evicted_path:
                result, payload = cluster.store.read_from_ssd(
                    loc.evicted_path, loc.store_epoch, expected_version=loc.version, block_size=self.block_size
                )
                if result.success and self._promote_worker is not None:
                    self._promote_worker.enqueue(
                        key,
                        payload,
                        loc.store_id,
                        loc.pool_offset,
                        loc.version,
                        loc.store_epoch,
                        loc.dn_id,
                    )
            else:
                try:
                    result, payload = cluster.store.read(
                        loc.store_id,
                        loc.pool_offset,
                        loc.store_epoch,
                        expected_version=loc.version,
                        block_size=self.block_size,
                    )
                except TypeError:
                    result, payload = cluster.store.read(
                        loc.store_id, loc.pool_offset, loc.store_epoch
                    )
            if not result.success:
                raise RuntimeError(f"Failed to read {key}: {result.error_code}")
            loaded[key] = payload
        return LoadStoreSpec(data=loaded)

    def _batch_renew_impl(self, keys: List[str]) -> None:
        now = self._now_ms()
        for key in keys:
            loc = self.local_cache.get(key)
            if not loc:
                continue
            if loc.lease_token <= 0:
                continue
            cluster = self._clusters.get(loc.dn_id) or self._cluster_for(key)
            result, lease = cluster.metadata.lease_manager.renew(
                key,
                loc.lease_token,
                loc.dn_epoch,
                loc.store_epoch,
                now,
            )
            if result.success and lease:
                loc.lease_expire_ms = lease.lease_expire_ms

    def _cache_location(self, key, row, lease, dn_id: int) -> None:
        self.local_cache[key] = KVBlockLocation(
            block_hash=key,
            status=int(row.status),
            store_id=row.location.store_node_id,
            pool_offset=row.location.pool_offset,
            lease_expire_ms=lease.lease_expire_ms,
            evicted_path=row.location.evicted_path,
            lease_token=lease.lease_token,
            version=row.version,
            dn_epoch=lease.dn_epoch,
            store_epoch=lease.store_epoch,
            dn_id=dn_id,
        )

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    @staticmethod
    def _mk_request_id() -> str:
        return uuid.uuid4().hex
