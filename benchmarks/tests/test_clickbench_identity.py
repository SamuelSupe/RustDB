from __future__ import annotations

import hashlib
import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "clickbench" / "run.py"
sys.path.insert(0, str(MODULE_PATH.parent))
from reference_common import reference_sql  # noqa: E402
from oracle import PINNED_SHA256  # noqa: E402

SPEC = importlib.util.spec_from_file_location("clickbench_run", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
clickbench = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(clickbench)


class ClickBenchIdentityTests(unittest.TestCase):
    def test_functional_queries_have_stable_limit_boundaries(self) -> None:
        path = MODULE_PATH.parent / "queries-rustdb.sql"
        queries = clickbench.load_queries(path)

        self.assertEqual(len(queries), 43)
        self.assertEqual(
            clickbench.sha256(path),
            "5386a67950894eb01803dc4f216a0bda61bb219940f8b783a3c648f0c4f76749",
        )
        for query in queries:
            if " LIMIT " in query.upper():
                self.assertIn(" ORDER BY ", query.upper())
        expected = {
            12: "ORDER BY u DESC, MobilePhone ASC, MobilePhoneModel ASC",
            18: "ORDER BY UserID ASC, SearchPhrase ASC",
            19: "ORDER BY COUNT(*) DESC, UserID ASC, m ASC, SearchPhrase ASC",
            24: "ORDER BY EventTime ASC, WatchID ASC",
            25: "ORDER BY EventTime ASC, WatchID ASC, SearchPhrase ASC",
            27: "ORDER BY EventTime ASC, SearchPhrase ASC, WatchID ASC",
            31: "ORDER BY c DESC, SearchEngineID ASC, ClientIP ASC",
            32: "ORDER BY c DESC, WatchID ASC, ClientIP ASC",
            33: "ORDER BY c DESC, WatchID ASC, ClientIP ASC",
            39: "ORDER BY PageViews DESC, URL ASC",
        }
        for number, fragment in expected.items():
            self.assertIn(fragment, queries[number - 1])

    def test_reference_uses_exact_q04_and_unshadowed_q24_binary(self) -> None:
        queries = clickbench.load_queries(MODULE_PATH.parent / "queries-rustdb.sql")
        columns = [
            {"name": "WatchID", "type": "Int64"},
            {"name": "URL", "type": "String"},
            {"name": "EventTime", "type": "Int64"},
        ]

        q04 = reference_sql(queries[3], 4, "hits.parquet", columns)
        q24 = reference_sql(queries[23], 24, "hits.parquet", columns)

        self.assertIn("sum(toInt128(UserID)), count()", q04)
        self.assertNotIn("avg(UserID)", q04)
        self.assertIn("lower(hex(`URL`))", q24)
        self.assertNotIn("lower(hex(`URL`)) AS", q24)
        self.assertIn("ORDER BY toDateTime(EventTime, 'UTC') ASC", q24)

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
        oracle_path = MODULE_PATH.parent / "functional-oracle-v2.json"
        value, identity = clickbench.load_oracle(
            oracle_path,
            expected_sha256=PINNED_SHA256,
            profile="functional",
            mode="execute",
            query_sha256="5386a67950894eb01803dc4f216a0bda61bb219940f8b783a3c648f0c4f76749",
            canonical_query_sha256="a7d6673357348ee9680443216b6f26f30d1dce9f313b419d38502417b2c2a219",
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
                MODULE_PATH.parent / "functional-oracle-v2.json",
                expected_sha256="0" * 64,
                profile="functional",
                mode="execute",
                query_sha256="a" * 64,
                canonical_query_sha256="c" * 64,
                dataset_sha256="b" * 64,
            )


if __name__ == "__main__":
    unittest.main()
