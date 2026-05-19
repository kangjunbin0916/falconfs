import os
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))
sys.path.insert(0, str(ROOT / "test"))

import kv_store_microbench as micro
from falconfs_kv import kv_data_service_pb2 as kvdata
from falconfs_kv import store_client


class MicrobenchModeHelpersTest(unittest.TestCase):
    def test_path_mode_aliases(self):
        self.assertEqual(micro._normalize_path_mode("local_shm_zero_copy_read"), "local-shm-zero-copy-read")
        self.assertEqual(micro._normalize_path_mode("remote_brpc_attachment"), "remote-brpc-attachment")
        self.assertEqual(micro._normalize_path_mode("pure_native_memcpy"), "pure-native-memcpy")

    def test_local_mode_rejects_remote_store(self):
        locality = {"selected": {1: False}, "all_local": False, "all_remote": True, "known_count": 1}
        with self.assertRaises(SystemExit):
            micro._enforce_locality("local-shm-zero-copy-read", [1], locality, 0)

    def test_remote_mode_rejects_local_store(self):
        locality = {"selected": {1: True}, "all_local": True, "all_remote": False, "known_count": 1}
        with self.assertRaises(SystemExit):
            micro._enforce_locality("remote-brpc-attachment", [1], locality, 0)


class BufferProtocolWriteTest(unittest.TestCase):
    def test_unary_write_passes_memoryview_to_native_payload_api(self):
        captured = {}
        old = store_client.falconfs_kv_brpc.facade_write_block_payload

        def fake(_store_id, _req, payload):
            captured["type"] = type(payload).__name__
            captured["is_memoryview"] = isinstance(payload, memoryview)
            rsp = kvdata.WriteBlockResponse()
            rsp.result.result.success = True
            return rsp.SerializeToString()

        store_client.falconfs_kv_brpc.facade_write_block_payload = fake
        try:
            st = store_client.BrpcKVStore("127.0.0.1:1", use_facade_registry=True)
            result = st.write(1, 0, memoryview(bytearray(b"abcd")), 1, 4, block_hash=b"h")
            self.assertTrue(result.success)
            self.assertTrue(captured.get("is_memoryview"), captured)
        finally:
            store_client.falconfs_kv_brpc.facade_write_block_payload = old

    def test_batch_write_passes_memoryviews_to_native_payload_api(self):
        captured = {}
        old = store_client.falconfs_kv_brpc.facade_batch_write_block_payloads

        def fake(_store_id, _req, payloads):
            captured["all_memoryview"] = all(isinstance(p, memoryview) for p in payloads)
            rsp = kvdata.BatchWriteBlockResponse()
            for _ in payloads:
                item = rsp.results.add()
                item.result.success = True
            return rsp.SerializeToString()

        store_client.falconfs_kv_brpc.facade_batch_write_block_payloads = fake
        try:
            st = store_client.BrpcKVStore("127.0.0.1:1", use_facade_registry=True)
            blocks = [
                store_client.StoreBlockWrite(0, memoryview(bytearray(b"abcd")), b"h0", 4, 1),
                store_client.StoreBlockWrite(4, memoryview(bytearray(b"efgh")), b"h1", 4, 1),
            ]
            results = st.batch_write_blocks(1, blocks)
            self.assertEqual([r.success for r in results], [True, True])
            self.assertTrue(captured.get("all_memoryview"), captured)
        finally:
            store_client.falconfs_kv_brpc.facade_batch_write_block_payloads = old


if __name__ == "__main__":
    unittest.main()
