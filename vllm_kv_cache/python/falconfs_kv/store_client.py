"""BRPC-backed Store data client (v6 §12 / M3).

Duck-type drop-in for `reference.KVStore` exposing only `write` and `read`,
which is what the OffloadingManager invokes. `BatchReadFromSSD` is also
exported as `read_from_ssd` for callers that need it (vLLM's `prepare_load`
fallback path).
"""

from __future__ import annotations

import os
from typing import Tuple

from . import falconfs_kv_brpc
from . import kv_data_service_pb2 as _kvdata
from .dn_client import _result_meta_to_item_result
from .reference import ErrorCode, ItemResult


class BrpcKVStore:
    def __init__(self, endpoint: str, *, timeout_ms: int = 30000, use_facade_registry: bool = False):
        self.endpoint = endpoint
        self.timeout_ms = timeout_ms
        self._req_seq = 0
        self._use_facade_registry = use_facade_registry

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
    ) -> ItemResult:
        req = _kvdata.BatchWriteBlockRequest()
        req.meta.request_id = self._mk_request_id("write")
        it = req.items.add()
        it.block_hash = b""  # the engine looks up by (store_node_id, pool_offset) on writes
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
                    int(store_node_id), req.SerializeToString(), self.timeout_ms
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
    ) -> Tuple[ItemResult, bytes]:
        req = _kvdata.BatchReadBlockRequest()
        req.meta.request_id = self._mk_request_id("read")
        it = req.items.add()
        it.block_hash = b""
        it.pool_offset = pool_offset
        it.block_size = block_size
        it.expected_store_epoch = expected_store_epoch
        it.expected_version = expected_version
        rsp = _kvdata.BatchReadBlockResponse()
        wire = b""
        if self._use_facade_registry:
            try:
                wire = falconfs_kv_brpc.facade_batch_read_block(
                    int(store_node_id), req.SerializeToString(), self.timeout_ms
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

    def read_from_ssd(
        self,
        evicted_path: str,
        expected_store_epoch: int,
        expected_version: int = 0,
        block_size: int = 65536,
    ) -> Tuple[ItemResult, bytes]:
        req = _kvdata.BatchReadFromSSDRequest()
        req.meta.request_id = self._mk_request_id("read_ssd")
        it = req.items.add()
        it.block_hash = b""
        it.evicted_path = evicted_path
        it.block_size = block_size
        it.expected_store_epoch = expected_store_epoch
        it.expected_version = expected_version
        rsp = _kvdata.BatchReadFromSSDResponse()
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
