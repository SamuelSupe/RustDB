from __future__ import annotations

import hashlib
import importlib.util
import tempfile
import unittest
import sys
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "clickbench" / "run.py"
sys.path.insert(0, str(MODULE_PATH.parent))
SPEC = importlib.util.spec_from_file_location("clickbench_run", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
clickbench = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(clickbench)


class ClickBenchIdentityTests(unittest.TestCase):
    def test_records_and_verifies_observed_sha256(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "fixture.parquet"
            path.write_bytes(b"official fixture")
            expected = hashlib.sha256(path.read_bytes()).hexdigest()

            facts = clickbench.identity_facts(path, expected, "fixture")

        self.assertEqual(facts["sha256"], expected)
        self.assertEqual(facts["expected_sha256"], expected)
        self.assertTrue(facts["identity_verified"])

    def test_rejects_mismatch_and_marks_unpinned_input(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "fixture.parquet"
            path.write_bytes(b"different fixture")
            with self.assertRaisesRegex(RuntimeError, "SHA-256 is"):
                clickbench.identity_facts(path, "0" * 64, "fixture")

            facts = clickbench.identity_facts(path, None, "fixture")

        self.assertFalse(facts["identity_verified"])
        self.assertIsNone(facts["expected_sha256"])

    def test_pinned_oracle_validates_identity_and_result(self) -> None:
        oracle_path = MODULE_PATH.parent / "functional-oracle-v1.json"
        value, identity = clickbench.load_oracle(
            oracle_path,
            expected_sha256="3040ce083db2647e6efef0a185b0b7772f4a5722898e0f351b8a1f8e46ac43b7",
            profile="functional",
            mode="execute",
            query_sha256="a7d6673357348ee9680443216b6f26f30d1dce9f313b419d38502417b2c2a219",
            dataset_sha256="fa134fe101e68324e0de851146fda69624f5cbb707d387141d1c2a88a219a16d",
        )
        expected = value["results"][0]
        result = {
            "checksum_algorithm": value["checksum_algorithm"],
            "result_rows": expected["rows"],
            "result_checksum_sha256": expected["checksum"],
        }

        self.assertTrue(identity["identity_verified"])
        self.assertIsNone(
            clickbench.compare_result(result, expected, value["checksum_algorithm"])
        )
        result["result_rows"] += 1
        self.assertIn(
            "rows",
            clickbench.compare_result(result, expected, value["checksum_algorithm"]),
        )

    def test_oracle_rejects_wrong_identity(self) -> None:
        with self.assertRaisesRegex(ValueError, "oracle SHA-256"):
            clickbench.load_oracle(
                MODULE_PATH.parent / "functional-oracle-v1.json",
                expected_sha256="0" * 64,
                profile="functional",
                mode="execute",
                query_sha256="a" * 64,
                dataset_sha256="b" * 64,
            )


if __name__ == "__main__":
    unittest.main()
