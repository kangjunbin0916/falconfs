"""BRPC-backed Store data client (v6 §12 / M3).

Duck-type drop-in for `reference.KVStore` exposing ``write`` and ``read``,
which is what the OffloadingManager invokes. Spilled blocks are read only
through ``KVDataService.BatchReadFromSSD`` via :meth:`read_from_ssd` (never
by opening ``evicted_path`` through the FalconFS file client in the vLLM
process — v6.6.2 design).
"""

from __future__ import annotations

import os
import threading
from dataclasses import dataclass
from typing import Dict, List, Sequence, Tuple

from . import falconfs_kv_brpc
from . import kv_data_service_pb2 as _kvdata
from .dn_client import _result_meta_to_item_result
from .reference import ErrorCode, ItemResult

_store_endpoint_locks: Dict[str, threading.Lock] = {}
_store_endpoint_locks_mu = threading.Lock()


def _endpoint_wire_lock(endpoint: str) -> threading.Lock:
    with _store_endpoint_locks_mu:
        return _store_endpoint_locks.setdefault(endpoint, threading.Lock())


@dataclass(frozen=True)
class StoreBlockWrite:
    """One logical KV block for a batched ``BatchWriteBlock`` RPC."""

    pool_offset: int
    payload: bytes
    block_hash: bytes
    block_size: int
    expected_store_epoch: int
    expected_version: int = 0


@dataclass(frozen=True)
class StoreBlockRead:
    """One logical KV block for a batched ``BatchReadBlock`` RPC."""

    pool_offset: int
    block_hash: bytes
    block_size: int
    expected_store_epoch: int
    expected_version: int = 0


class BrpcKVStore:
    def __init__(self, endpoint: str, *, timeout_ms: int = 30000, use_facade_registry: bool = False):
        self.endpoint = endpoint
        self.timeout_ms = timeout_ms
        self._req_seq = 0
        self._use_facade_registry = use_facade_registry
        self._ep_lock = _endpoint_wire_lock(endpoint)

    def _mk_request_id(self, tag: str) -> str:
        self._req_seq += 1
        return f"py_store_client_{tag}_{self._req_seq}"

    def write(
        self,
        store_node_id: int,
        pool_offset: int,
        payload: bytes,
        expected_store_epoch: int,
        block_size: int,
        expected_version: int = 0,
        *,
        block_hash: bytes = b"",
    ) -> ItemResult:
        with self._ep_lock:
            req = _kvdata.BatchWriteBlockRequest()
            req.meta.request_id = self._mk_request_id("write")
            it = req.items.add()
            it.block_hash = block_hash
            it.pool_offset = pool_offset
            it.payload = payload
            it.block_size = block_size
            it.expected_store_epoch = expected_store_epoch
            it.expected_version = expected_version
            rsp = _kvdata.BatchWriteBlockResponse()
            wire = b""
            if self._use_facade_registry:
                try:
                    wire = falconfs_kv_brpc.facade_batch_write_block(
                        int(store_node_id), req.SerializeToString()
                    )
                except Exception:
                    wire = b""
            if not wire:
                wire = falconfs_kv_brpc.batch_write_block(
                    self.endpoint, req.SerializeToString(), self.timeout_ms
                )
            rsp.ParseFromString(wire)
            if not rsp.results:
                return ItemResult(False, ErrorCode.INTERNAL_ERROR, True, "empty response")
            return _result_meta_to_item_result(rsp.results[0].result)

    def read(
        self,
        store_node_id: int,
        pool_offset: int,
        expected_store_epoch: int,
        expected_version: int = 0,
        block_size: int = 65536,
        *,
        block_hash: bytes = b"",
    ) -> Tuple[ItemResult, bytes]:
        with self._ep_lock:
            req = _kvdata.BatchReadBlockRequest()
            req.meta.request_id = self._mk_request_id("read")
            it = req.items.add()
            it.block_hash = block_hash
            it.pool_offset = pool_offset
            it.block_size = block_size
            it.expected_store_epoch = expected_store_epoch
            it.expected_version = expected_version
            rsp = _kvdata.BatchReadBlockResponse()
            wire = b""
            if self._use_facade_registry:
                try:
                    wire = falconfs_kv_brpc.facade_batch_read_block(
                        int(store_node_id), req.SerializeToString()
                    )
                except Exception:
                    wire = b""
            if not wire:
                wire = falconfs_kv_brpc.batch_read_block(
                    self.endpoint, req.SerializeToString(), self.timeout_ms
                )
            rsp.ParseFromString(wire)
            if not rsp.results:
                return ItemResult(False, ErrorCode.INTERNAL_ERROR, True, "empty response"), b""
            r = rsp.results[0]
            return _result_meta_to_item_result(r.result), r.payload

    def batch_write_blocks(
        self,
        store_node_id: int,
        blocks: Sequence[StoreBlockWrite],
    ) -> List[ItemResult]:
        if not blocks:
            return []
        with self._ep_lock:
            req = _kvdata.BatchWriteBlockRequest()
            req.meta.request_id = self._mk_request_id("batch_write")
            for b in blocks:
                it = req.items.add()
                it.block_hash = b.block_hash
                it.pool_offset = b.pool_offset
                it.payload = b.payload
                it.block_size = b.block_size
                it.expected_store_epoch = b.expected_store_epoch
                it.expected_version = b.expected_version
            rsp = _kvdata.BatchWriteBlockResponse()
            wire = b""
            if self._use_facade_registry:
                try:
                    wire = falconfs_kv_brpc.facade_batch_write_block(
                        int(store_node_id), req.SerializeToString()
                    )
                except Exception:
                    wire = b""
            if not wire:
                wire = falconfs_kv_brpc.batch_write_block(
                    self.endpoint, req.SerializeToString(), self.timeout_ms
                )
            rsp.ParseFromString(wire)
            out: List[ItemResult] = []
            for r in rsp.results:
                out.append(_result_meta_to_item_result(r.result))
            return out

    def batch_read_blocks(
        self,
        store_node_id: int,
        blocks: Sequence[StoreBlockRead],
    ) -> List[Tuple[ItemResult, bytes]]:
        if not blocks:
            return []
        with self._ep_lock:
            req = _kvdata.BatchReadBlockRequest()
            req.meta.request_id = self._mk_request_id("batch_read")
            for b in blocks:
                it = req.items.add()
                it.block_hash = b.block_hash
                it.pool_offset = b.pool_offset
                it.block_size = b.block_size
                it.expected_store_epoch = b.expected_store_epoch
                it.expected_version = b.expected_version
            rsp = _kvdata.BatchReadBlockResponse()
            wire = b""
            if self._use_facade_registry:
                try:
                    wire = falconfs_kv_brpc.facade_batch_read_block(
                        int(store_node_id), req.SerializeToString()
                    )
                except Exception:
                    wire = b""
            if not wire:
                wire = falconfs_kv_brpc.batch_read_block(
                    self.endpoint, req.SerializeToString(), self.timeout_ms
                )
            rsp.ParseFromString(wire)
            out: List[Tuple[ItemResult, bytes]] = []
            for r in rsp.results:
                out.append((_result_meta_to_item_result(r.result), r.payload))
            return out

    def read_from_ssd(
        self,
        store_node_id: int,
        evicted_path: str,
        expected_store_epoch: int,
        expected_version: int = 0,
        block_size: int = 65536,
    ) -> Tuple[ItemResult, bytes]:
        """Read spilled KV bytes via ``KVDataService.BatchReadFromSSD`` (always through the Store).

        ``store_node_id`` selects the facade when ``use_facade_registry`` is enabled
        (same pattern as :meth:`read` / :meth:`batch_read_blocks`). ``block_size`` is
        kept for API compatibility; the wire ``SSDReadItem`` only carries
        ``block_hash``, ``evicted_path``, and ``expected_version`` today.
        """
        with self._ep_lock:
            req = _kvdata.BatchReadFromSSDRequest()
            req.meta.request_id = self._mk_request_id("read_ssd")
            it = req.items.add()
            it.block_hash = b""
            it.evicted_path = evicted_path
            it.expected_version = expected_version
            rsp = _kvdata.BatchReadFromSSDResponse()
            wire = b""
            if self._use_facade_registry:
                try:
                    wire = falconfs_kv_brpc.facade_batch_read_from_ssd(
                        int(store_node_id), req.SerializeToString()
                    )
                except Exception:
                    wire = b""
            if not wire:
                wire = falconfs_kv_brpc.batch_read_from_ssd(
                    self.endpoint, req.SerializeToString(), self.timeout_ms
                )
            rsp.ParseFromString(wire)
            if not rsp.results:
                return ItemResult(False, ErrorCode.INTERNAL_ERROR, True, "empty response"), b""
            r = rsp.results[0]
            return _result_meta_to_item_result(r.result), r.payload


class BrpcCluster:
    """Pair of (BrpcMetadataService, BrpcKVStore) addressed by one DN endpoint.
    Drop-in for `reference.ReferenceCluster` from the OffloadingManager's view.
    """

    def __init__(self, endpoint: str, *, dn_id: int = 1, client_id: int = 0,
                 timeout_ms: int = 30000, block_size: int = 65536,
                 store_endpoint: str | None = None, use_facade_registry: bool = False):
        from .dn_client import BrpcMetadataService
        self.endpoint = endpoint
        store_ep = store_endpoint or os.environ.get("FALCON_KV_STORE_BRPC_ENDPOINT") or endpoint
        self.metadata = BrpcMetadataService(
            endpoint, dn_id=dn_id, client_id=client_id,
            timeout_ms=timeout_ms, block_size=block_size,
        )
        self.store = BrpcKVStore(
            store_ep,
            timeout_ms=timeout_ms,
            use_facade_registry=use_facade_registry,
        )
