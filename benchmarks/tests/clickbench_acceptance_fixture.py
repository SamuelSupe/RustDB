from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable


RawMutator = Callable[[int, dict[str, Any]], None]


class ClickBenchAcceptanceFixture:
    def __init__(self, root: Path):
        self.digest = "a" * 64
        self.commit = "b" * 40
        self.algorithm = "rustdb-typed-multiset-sha256-v1"
        self.output = root / "clickbench"
        self.raw_directory = self.output / "reports"
        self.raw_directory.mkdir(parents=True)
        self.path = self.output / "manifest.json"
        self.oracle_results = [
            {"query": number, "rows": 1, "checksum": self.digest}
            for number in range(1, 44)
        ]
        self.results = [self._result(number) for number in range(1, 44)]
        self.manifest = self._manifest()
        self.inputs = self._inputs()
        self.write_raw_reports()
        self.write_manifest()

    def _result(self, number: int) -> dict[str, Any]:
        return {
            "query": number,
            "status": "passed",
            "terminal_query_reservation_bytes": 0,
            "terminal_engine_reservation_bytes": 0,
            "spill_cleaned": True,
            "checksum_algorithm": self.algorithm,
            "result_rows": 1,
            "result_checksum_sha256": self.digest,
            "oracle_expected_rows": 1,
            "oracle_expected_checksum_sha256": self.digest,
            "oracle_match": True,
            "oracle_error": None,
            "exit_code": 0,
            "timed_out": False,
            "report_parse_error": None,
            "cleanup_error": None,
            "peak_engine_reservation_bytes": 100,
            "process_peak_rss_bytes": 200,
            "peak_active_lanes": 2,
            "rendered_sql": f"/results/run/rendered/q{number:02d}.sql",
            "report": f"/results/run/reports/q{number:02d}.json",
        }

    def _manifest(self) -> dict[str, Any]:
        identity = {
            "sha256": self.digest,
            "expected_sha256": self.digest,
            "identity_verified": True,
        }
        return {
            "schema": "rustdb-clickbench-v1",
            "complete": True,
            "mode": "execute",
            "binary_as_string": True,
            "query_count": 43,
            "passed": 43,
            "failed": 0,
            "results": self.results,
            "dataset": identity
            | {
                "profile": "functional",
                "bytes": 123,
                "expected_source_etag": '"etag"',
            },
            "queries": dict(identity),
            "canonical_queries": dict(identity),
            "oracle": identity
            | {
                "schema": "rustdb-clickbench-functional-oracle-v2",
                "profile": "functional",
                "mode": "execute",
                "query_count": 43,
                "checksum_algorithm": self.algorithm,
                "query_sha256": self.digest,
                "canonical_query_sha256": self.digest,
                "dataset_sha256": self.digest,
            },
            "resource_contract": {
                "container_cpus": "400000 100000",
                "container_memory_bytes": str(12 * 1024**3),
                "engine_threads": 4,
                "engine_memory_limit_bytes": 4 * 1024**3,
                "batch_size": 8192,
                "io_concurrency": 16,
                "metadata_cache_bytes": 256 * 1024**2,
            },
            "build": {"id": self.commit, "binary_sha256": self.digest},
            "acceptance_summary": {
                "max_peak_engine_reservation_bytes": 100,
                "max_process_peak_rss_bytes": 200,
                "max_peak_active_lanes": 2,
                "all_terminal_query_reservations_zero": True,
                "all_terminal_engine_reservations_zero": True,
                "all_spill_cleaned": True,
            },
        }

    def _inputs(self) -> dict[str, Any]:
        identity = {
            "sha256": self.digest,
            "expected_sha256": self.digest,
            "identity_verified": True,
        }
        return {
            "clickbench": {
                "profile": "functional",
                "data": identity
                | {"bytes": 123, "expected_source_etag": '"etag"'},
                "query": dict(identity),
                "canonical_query": dict(identity),
                "oracle": identity
                | {
                    "schema": "rustdb-clickbench-functional-oracle-v2",
                    "profile": "functional",
                    "mode": "execute",
                    "query_count": 43,
                    "checksum_algorithm": self.algorithm,
                    "results": self.oracle_results,
                },
            }
        }

    def raw_report(self, number: int) -> dict[str, Any]:
        result = self.results[number - 1]
        return {
            "build_id": self.commit,
            "binary_sha256": self.digest,
            "query_file": result["rendered_sql"],
            "warmup": 0,
            "iterations": 1,
            "checksum_algorithm": self.algorithm,
            "result_checksum_sha256": self.digest,
            "config": {
                "compute_threads": 4,
                "memory_limit_bytes": 4 * 1024**3,
                "batch_size": 8192,
                "io_concurrency": 16,
                "metadata_cache_bytes": 256 * 1024**2,
            },
            "runs": [
                {
                    "rows": 1,
                    "result_checksum_sha256": self.digest,
                    "engine_peak_reservation_bytes": 100,
                    "process_peak_rss_bytes": 200,
                    "peak_active_lanes": 2,
                    "current_memory_bytes": 0,
                    "engine_current_reservation_bytes": 0,
                    "spill_cleaned": True,
                }
            ],
        }

    def write_manifest(self, value: dict[str, Any] | None = None) -> None:
        self.path.write_text(json.dumps(value or self.manifest), encoding="utf-8")

    def write_raw_reports(self, mutator: RawMutator | None = None) -> None:
        for number in range(1, 44):
            raw = self.raw_report(number)
            if mutator is not None:
                mutator(number, raw)
            (self.raw_directory / f"q{number:02d}.json").write_text(
                json.dumps(raw), encoding="utf-8"
            )

    @staticmethod
    def clone(value: dict[str, Any]) -> dict[str, Any]:
        return json.loads(json.dumps(value))
