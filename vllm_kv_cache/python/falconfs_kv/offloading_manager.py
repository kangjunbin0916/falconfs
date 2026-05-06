from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from .reference import BlockStatus, ErrorCode, ReferenceCluster


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


class FalconFSOffloadingManager:
    """Reference OffloadingManager with upstream public API names.

    The implementation uses an in-memory reference cluster until C++ BRPC
    clients are wired in. Batch-prefixed methods are kept as compatibility
    aliases for older tests, but the public v6 surface is lookup/prepare/complete.
    """

    def __init__(self, shard_table: Dict[int, str], client_id: int, client_hostname: str, cluster=None):
        self.shard_table = shard_table
        self.client_id = client_id
        self.client_hostname = client_hostname
        self.local_cache: Dict[str, KVBlockLocation] = {}
        self._cluster = cluster or ReferenceCluster(block_size=BLOCK_SIZE)

    def lookup(self, key: str, req_context=None) -> bool | None:
        return self._batch_lookup_impl([key], req_context, renew_lease_on_hit=False).get(key)

    def prepare_load(self, keys: List[str], req_context=None) -> LoadStoreSpec:
        lookup = self._batch_lookup_impl(keys, req_context, renew_lease_on_hit=True)
        missing = [key for key, hit in lookup.items() if not hit]
        if missing:
            raise RuntimeError(f"Blocks {missing} not found in cache")
        return self._batch_load_impl(keys)

    def complete_load(self, keys: List[str], req_context=None):
        self._batch_renew_impl(keys)

    def prepare_store(self, keys: List[str], req_context=None) -> LoadStoreSpec:
        return self._batch_prepare_store_impl(keys, req_context)

    def complete_store(self, keys: List[str], data: Optional[Dict[str, bytes]] = None, req_context=None, success: bool = True):
        if not success:
            return
        self._batch_complete_store_impl(keys, data or {}, req_context)

    def touch(self, keys: List[str], req_context=None):
        self._batch_renew_impl(keys)

    def batch_lookup(self, keys: List[str], req_context) -> Dict[str, bool]:
        return self._batch_lookup_impl(keys, req_context, renew_lease_on_hit=True)

    def batch_prepare_store(self, keys: List[str], req_context) -> LoadStoreSpec:
        return self.prepare_store(keys, req_context)

    def batch_complete_store(self, keys: List[str], data: Dict[str, bytes], req_context):
        self.complete_store(keys, data, req_context)

    def batch_prepare_load(self, keys: List[str], req_context) -> LoadStoreSpec:
        return self.prepare_load(keys, req_context)

    def batch_complete_load(self, keys: List[str], req_context):
        self.complete_load(keys, req_context)

    def batch_touch(self, keys: List[str], req_context):
        self.touch(keys, req_context)

    def _batch_lookup_impl(self, keys: List[str], req_context, renew_lease_on_hit: bool) -> Dict[str, bool]:
        results: Dict[str, bool] = {}
        for key, (result, row, lease) in self._cluster.metadata.lookup(
            keys, renew_lease_on_hit=renew_lease_on_hit
        ).items():
            if result.success and row:
                if lease:
                    self._cache_location(key, row, lease)
                results[key] = True
            elif result.error_code in (ErrorCode.CAS_CONFLICT, ErrorCode.THROTTLED):
                results[key] = None  # type: ignore[assignment]
            else:
                results[key] = False
        return results

    def _batch_prepare_store_impl(self, keys: List[str], req_context) -> LoadStoreSpec:
        specs = []
        for key, (result, row, lease) in self._cluster.metadata.allocate(keys).items():
            if not result.success or not row or not lease:
                continue
            self._cache_location(key, row, lease)
            specs.append(
                {
                    "block_hash": row.block_hash,
                    "store_id": row.location.store_node_id,
                    "pool_offset": row.location.pool_offset,
                }
            )
        return LoadStoreSpec(specs=specs)

    def _batch_complete_store_impl(self, keys: List[str], data: Dict[str, bytes], req_context):
        for key in keys:
            payload = data.get(key)
            if payload is None:
                continue
            loc = self.local_cache.get(key)
            if not loc:
                continue
            write = self._cluster.store.write(
                loc.store_id,
                loc.pool_offset,
                payload,
                loc.store_epoch,
                BLOCK_SIZE,
            )
            if not write.success:
                continue
            update, row = self._cluster.metadata.update_status(
                key,
                BlockStatus.ALLOCATED,
                BlockStatus.STORED,
                loc.version,
            )
            if update.success and row:
                loc.status = int(row.status)
                loc.version = row.version

    def _batch_load_impl(self, keys: List[str]) -> LoadStoreSpec:
        loaded: Dict[str, bytes] = {}
        for key in keys:
            loc = self.local_cache.get(key)
            if not loc:
                raise RuntimeError(f"Block {key} not found in local cache")
            result, payload = self._cluster.store.read(loc.store_id, loc.pool_offset, loc.store_epoch)
            if not result.success:
                raise RuntimeError(f"Failed to read {key}: {result.error_code}")
            loaded[key] = payload
        return LoadStoreSpec(data=loaded)

    def _batch_renew_impl(self, keys: List[str]) -> None:
        now = self._now_ms()
        for key in keys:
            loc = self.local_cache.get(key)
            if loc:
                result, lease = self._cluster.metadata.lease_manager.renew(
                    key,
                    loc.lease_token,
                    loc.dn_epoch,
                    loc.store_epoch,
                    now,
                )
                if result.success and lease:
                    loc.lease_expire_ms = lease.lease_expire_ms

    def _cache_location(self, key, row, lease) -> None:
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
        )

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)
