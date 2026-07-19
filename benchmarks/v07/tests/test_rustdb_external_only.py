from __future__ import annotations

import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import rustdb_external_only as external


CHECKSUM = "a" * 64
CONFIG = {
    "threads": 4,
    "memory_limit_bytes": 2_000,
    "concurrency": 1,
    "batch_size": 8192,
}


def hello() -> dict:
    return {
        "kind": "hello",
        "engine": "rustdb",
        "version": "0.7.0-alpha.1",
        "build_id": "b" * 64,
        **CONFIG,
        "cache_state": {
            "os_page_cache": "warm-uncontrolled",
            "metadata_cache": "disabled",
            "external_file_cache": "not-applicable",
        },
    }


def run(run_id: str, elapsed: float, peak: int) -> dict:
    return {
        "kind": "run",
        "run_id": run_id,
        "engine": "rustdb",
        "version": "0.7.0-alpha.1",
        "build_id": "b" * 64,
        **CONFIG,
        "cache_state": hello()["cache_state"],
        "storage_track": "parquet",
        "engine_order": 0,
        "group_elapsed_ms": elapsed,
        "rss_baseline_bytes": 1_000,
        "peak_rss_bytes": peak,
        "engine_root_current_reservation_bytes": 0,
        "engine_root_lifetime_peak_reservation_bytes": 800,
        "engine_root_memory_limit_bytes": CONFIG["memory_limit_bytes"],
        "throughput_queries_per_second": 1_000.0 / elapsed,
        "queries": [
            {
                "elapsed_ms": elapsed - 1,
                "ttfb_ms": elapsed / 2,
                "rows": 1,
                "batches": 1,
                "checksum": CHECKSUM,
                "checksum_mode": "multiset-sha256-v2",
                "checksum_backend": "rust-sha256-v2",
                "checksum_compute_ms": 0.1,
                "complete": True,
                "discovered_files": 1,
                "current_reservation_bytes": 0,
                "peak_reservation_bytes": 800,
            }
        ],
    }


class ExternalOnlyTests(unittest.TestCase):
    def test_summary_reports_latency_memory_and_checksum(self) -> None:
        value = external.summary(
            [
                run("measured-0-rustdb", 10.0, 1_400),
                run("measured-1-rustdb", 20.0, 1_600),
            ],
            CONFIG["memory_limit_bytes"],
        )
        self.assertEqual(value["p50_elapsed_ms"], 15.0)
        self.assertEqual(value["peak_rss_bytes"], 1_600)
        self.assertEqual(value["memory_headroom_bytes"], 400)
        self.assertEqual(value["checksum"], CHECKSUM)
        self.assertEqual(value["terminal_reservation_bytes"], 0)
        self.assertEqual(value["engine_root_current_reservation_bytes"], 0)
        self.assertEqual(value["engine_root_lifetime_peak_reservation_bytes"], 800)
        self.assertEqual(value["engine_root_memory_limit_bytes"], 2_000)
        self.assertEqual(value["discovered_files"], 1)

    def test_report_is_explicitly_diagnostic_and_self_validating(self) -> None:
        args = SimpleNamespace(
            threads=4,
            memory_limit_bytes=2_000,
            concurrency=1,
            batch_size=8192,
            storage_track="parquet",
            storage_medium="local-nvme",
            warmup=1,
            iterations=1,
        )
        value = external.make_report(
            args,
            hello(),
            [run("warmup-0-rustdb", 12.0, 1_500)],
            [run("measured-0-rustdb", 10.0, 1_400)],
            {"system": "test"},
            {
                "path": "data",
                "bytes": 1,
                "files": 1,
                "sha256": "c" * 64,
                "expected_discovered_files": 1,
            },
            {"path": "query.sql", "bytes": 1, "sha256": "d" * 64},
        )
        external.validate_report(value)
        self.assertTrue(value["diagnostic_only"])
        self.assertFalse(value["comparison_gate_eligible"])

    def test_report_rejects_retained_reservation(self) -> None:
        measured = run("measured-0-rustdb", 10.0, 1_400)
        measured["queries"][0]["current_reservation_bytes"] = 1
        value = {
            "schema": external.SCHEMA,
            "diagnostic_only": True,
            "comparison_gate_eligible": False,
            "storage_track": "parquet",
            "storage_medium": "local-nvme",
            "warmup": 0,
            "iterations": 1,
            "config": CONFIG,
            "cache_state": hello()["cache_state"],
            "dataset": {"expected_discovered_files": 1},
            "hello": hello(),
            "warmup_runs": [],
            "runs": [measured],
            "summary": copy.deepcopy(external.summary([measured], 2_000)),
        }
        with self.assertRaisesRegex(RuntimeError, "retained an engine reservation"):
            external.validate_report(value)

    def test_run_rejects_invalid_engine_root_memory(self) -> None:
        cases = (
            ("engine_root_current_reservation_bytes", 1, "must be zero"),
            ("engine_root_memory_limit_bytes", 1_999, "memory_limit_bytes"),
            (
                "engine_root_lifetime_peak_reservation_bytes",
                2_001,
                "exceeds limit",
            ),
        )
        for field, value, message in cases:
            with self.subTest(field=field):
                measured = run("measured-0-rustdb", 10.0, 1_400)
                measured[field] = value
                with self.assertRaisesRegex(RuntimeError, message):
                    external.validate_run(
                        measured,
                        hello(),
                        CONFIG,
                        "parquet",
                        "measured-0-rustdb",
                        1,
                    )

    def test_beta_manifest_count_must_match_every_query(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            manifest = Path(temporary) / "manifest.json"
            manifest.write_text(
                json.dumps(
                    {
                        "schema": external.BETA_OBJECT_FIXTURE_SCHEMA,
                        "root_uri": "s3://bucket/data",
                        "objects": [{"uri": "a"}, {"uri": "b"}],
                    }
                ),
                encoding="utf-8",
            )
            self.assertEqual(
                external.dataset_facts(manifest)["expected_discovered_files"], 2
            )

        measured = run("measured-0-rustdb", 10.0, 1_400)
        with self.assertRaisesRegex(RuntimeError, "discovered 1 files; expected 2"):
            external.validate_run(
                measured,
                hello(),
                CONFIG,
                "parquet",
                "measured-0-rustdb",
                2,
            )


if __name__ == "__main__":
    unittest.main()
