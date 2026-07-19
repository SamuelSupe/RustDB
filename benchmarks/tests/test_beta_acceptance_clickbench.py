from __future__ import annotations

import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts" / "ci"))

import beta_acceptance_evidence as evidence  # noqa: E402
import beta_acceptance_fixtures as fixtures  # noqa: E402
import beta_acceptance_reports as reports  # noqa: E402
from benchmarks.tests.clickbench_acceptance_fixture import (  # noqa: E402
    ClickBenchAcceptanceFixture,
)


class BetaAcceptanceClickBenchTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def test_fixture_binds_canonical_effective_queries_and_oracle(self):
        clickbench = self.root / "clickbench-input"
        clickbench.mkdir()
        canonical = clickbench / "queries.sql"
        effective = clickbench / "queries-rustdb.sql"
        data = clickbench / "fixture.parquet"
        canonical.write_bytes(b"canonical queries")
        effective.write_bytes(b"deterministic queries")
        data.write_bytes(b"dataset")
        canonical_sha = hashlib.sha256(canonical.read_bytes()).hexdigest()
        effective_sha = hashlib.sha256(effective.read_bytes()).hexdigest()
        data_sha = hashlib.sha256(data.read_bytes()).hexdigest()
        oracle = self.root / "oracle.json"
        oracle.write_text(
            json.dumps(
                {
                    "schema": "rustdb-clickbench-functional-oracle-v2",
                    "profile": "functional",
                    "mode": "execute",
                    "query_count": 43,
                    "query_sha256": effective_sha,
                    "canonical_query_sha256": canonical_sha,
                    "dataset_sha256": data_sha,
                    "checksum_algorithm": "rustdb-typed-multiset-sha256-v1",
                    "reference": {
                        "engine": {
                            "image_digest": (
                                "sha256:f40cd6034fb8c54dce6a85338750fbad"
                                "79f387e2705e1991a85f2e7086b5b9ea"
                            )
                        },
                        "verified_queries": 43,
                        "q04_exact_ratio": {
                            "expected_f64_bits": "0x43bb0960eb622986"
                        },
                        "q24_event_time_watch_id_duplicate_groups": 0,
                        "semantic_results": [
                            {"query": number, "rows": 1}
                            for number in range(1, 44)
                        ],
                    },
                    "results": [
                        {"query": number, "rows": 1, "checksum": "a" * 64}
                        for number in range(1, 44)
                    ],
                }
            ),
            encoding="utf-8",
        )
        oracle_sha = hashlib.sha256(oracle.read_bytes()).hexdigest()
        with patch.multiple(
            fixtures,
            CLICKBENCH={"functional": ("fixture.parquet", 7, '"etag"', data_sha)},
            CLICKBENCH_DIR=clickbench,
            CLICKBENCH_CANONICAL_QUERY_SHA256=canonical_sha,
            CLICKBENCH_FUNCTIONAL_QUERY_SHA256=effective_sha,
            PINNED_SHA256=oracle_sha,
        ):
            value = fixtures.clickbench_fixture(clickbench, "functional", oracle)
        self.assertEqual(value["query"]["sha256"], effective_sha)
        self.assertEqual(value["canonical_query"]["sha256"], canonical_sha)
        self.assertTrue(value["oracle"]["identity_verified"])

    def test_accepts_and_seals_exact_reports(self):
        item = ClickBenchAcceptanceFixture(self.root)
        accepted = reports.clickbench_summary(item.path, item.inputs, item.commit)
        self.assertTrue(accepted["identity_verified"])
        self.assertEqual(accepted["build_id"], item.commit)
        self.assertEqual(accepted["binary_sha256"], item.digest)
        self.assertEqual(accepted["validated_raw_reports"], 43)

    def test_rejects_resource_contract_changes(self):
        item = ClickBenchAcceptanceFixture(self.root)
        mutations = (
            ("container_cpus", None),
            ("container_cpus", "max 100000"),
            ("container_cpus", "300000 100000"),
            ("container_memory_bytes", None),
            ("container_memory_bytes", "max"),
            ("container_memory_bytes", str(16 * 1024**3)),
            ("engine_threads", 8),
            ("engine_memory_limit_bytes", 2 * 1024**3),
            ("batch_size", 4096),
            ("io_concurrency", 8),
            ("metadata_cache_bytes", 0),
        )
        for field, replacement in mutations:
            with self.subTest(field=field, replacement=replacement):
                changed = item.clone(item.manifest)
                changed["resource_contract"][field] = replacement
                item.write_manifest(changed)
                with self.assertRaisesRegex(ValueError, "ClickBench"):
                    reports.clickbench_summary(item.path, item.inputs, item.commit)

    def test_requires_preflight_identities_and_typed_results(self):
        item = ClickBenchAcceptanceFixture(self.root)
        for identity in ("data", "query", "canonical_query"):
            with self.subTest(identity=identity):
                inputs = item.clone(item.inputs)
                inputs["clickbench"][identity]["identity_verified"] = False
                with self.assertRaisesRegex(ValueError, "different fixture"):
                    reports.clickbench_summary(item.path, inputs, item.commit)
        changed = item.clone(item.manifest)
        changed["results"][17]["result_checksum_sha256"] = "c" * 64
        item.write_manifest(changed)
        with self.assertRaisesRegex(ValueError, "query 18 differs"):
            reports.clickbench_summary(item.path, item.inputs, item.commit)

    def test_rejects_build_and_summary_changes(self):
        item = ClickBenchAcceptanceFixture(self.root)
        changed = item.clone(item.manifest)
        changed["build"]["binary_sha256"] = "not-a-sha"
        item.write_manifest(changed)
        with self.assertRaisesRegex(ValueError, "invalid binary SHA-256"):
            reports.clickbench_summary(item.path, item.inputs, item.commit)
        changed = item.clone(item.manifest)
        changed["acceptance_summary"]["max_peak_engine_reservation_bytes"] = 99
        item.write_manifest(changed)
        with self.assertRaisesRegex(ValueError, "summary differs"):
            reports.clickbench_summary(item.path, item.inputs, item.commit)

    def test_rejects_raw_report_changes_and_peak_overflow(self):
        item = ClickBenchAcceptanceFixture(self.root)
        mutations = (
            ("build_id", lambda raw: raw.update(build_id="c" * 40), "manifest"),
            ("binary", lambda raw: raw.update(binary_sha256="c" * 64), "manifest"),
            ("config", lambda raw: raw["config"].update(batch_size=4096), "config"),
            ("rows", lambda raw: raw["runs"][0].update(rows=2), "cleanup"),
            (
                "checksum",
                lambda raw: raw.update(result_checksum_sha256="c" * 64),
                "manifest",
            ),
            (
                "cleanup",
                lambda raw: raw["runs"][0].update(current_memory_bytes=1),
                "cleanup",
            ),
            (
                "peak",
                lambda raw: raw["runs"][0].update(
                    engine_peak_reservation_bytes=4 * 1024**3 + 1
                ),
                "resource peaks",
            ),
        )
        for name, mutate, message in mutations:
            with self.subTest(name=name):
                item.write_raw_reports(
                    lambda number, raw: mutate(raw) if number == 18 else None
                )
                with self.assertRaisesRegex(ValueError, message):
                    reports.clickbench_summary(item.path, item.inputs, item.commit)

    def test_execution_contract_matches_finalizer(self):
        contract = evidence.execution_contract()["clickbench"]
        self.assertEqual(contract["container_cpus"], 4)
        self.assertEqual(contract["container_memory_bytes"], 12 * 1024**3)
        self.assertEqual(contract["batch_size"], 8192)
        self.assertEqual(contract["io_concurrency"], 16)
        self.assertEqual(contract["metadata_cache_bytes"], 256 * 1024**2)
        self.assertTrue(contract["binary_sha256_required"])


if __name__ == "__main__":
    unittest.main()
