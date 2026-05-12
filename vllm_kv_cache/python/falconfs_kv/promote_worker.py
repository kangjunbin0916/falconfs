from __future__ import annotations

import os

from . import kv_metadata_service_pb2 as _kvmeta
from .reference import BlockStatus


class PromoteWorker:
    """Best-effort promote-on-read helper for evicted blocks."""

    def __init__(self, manager):
        self._manager = manager
        self._enabled = os.environ.get("FALCON_KV_ENABLE_PROMOTE_ON_READ", "1") != "0"

    def enqueue(
        self,
        block_hash: str,
        payload: bytes,
        store_id: int,
        pool_offset: int,
        expected_version: int,
        expected_store_epoch: int,
        dn_id: int,
    ) -> None:
        if not self._enabled:
            return
        cluster = self._manager._clusters.get(dn_id)
        if cluster is None:
            return
        alloc_result = None
        try:
            alloc_map = cluster.metadata.allocate(
                [block_hash],
                request_id=f"promote_alloc:{block_hash}",
                client_id=self._manager.client_id,
                allocate_hint=_kvmeta.ALLOCATE_HINT_PROMOTE_FROM_EVICTED,
            )
            alloc_result = alloc_map.get(block_hash)
        except TypeError:
            alloc_map = cluster.metadata.allocate(
                [block_hash],
                request_id=f"promote_alloc:{block_hash}",
                client_id=self._manager.client_id,
            )
            alloc_result = alloc_map.get(block_hash)
        if not alloc_result:
            return
        alloc_status, alloc_row, _ = alloc_result
        if not alloc_status.success or alloc_row is None:
            return
        write = cluster.store.write(
            alloc_row.location.store_node_id,
            alloc_row.location.pool_offset,
            payload,
            alloc_row.location.store_epoch,
            self._manager.block_size,
            expected_version=alloc_row.version,
        )
        if not write.success:
            return
        result, row = cluster.metadata.update_status(
            block_hash,
            BlockStatus.ALLOCATED,
            BlockStatus.STORED,
            alloc_row.version,
            request_id=f"promote:{block_hash}",
            client_id=self._manager.client_id,
        )
        if result.success and row:
            loc = self._manager.local_cache.get(block_hash)
            if loc is not None:
                loc.status = int(BlockStatus.STORED)
                loc.version = row.version
                loc.store_id = alloc_row.location.store_node_id
                loc.pool_offset = alloc_row.location.pool_offset
                loc.store_epoch = alloc_row.location.store_epoch
