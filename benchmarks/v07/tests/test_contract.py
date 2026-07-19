from __future__ import annotations

import copy
import hashlib
import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import contract
from contract import (
    CHECKSUM_BACKENDS,
    CONTRACT_VERSION,
    HISTORICAL_CONTRACT_VERSION,
    HISTORICAL_CONTRACT_VERSION_V2,
    ContractError,
    validate_report,
)
from coordinator import (
    load_native_manifest,
    native_setup,
    summary as coordinator_summary,
    verify_native_sources,
)


CHECKSUM = "a" * 64
NATIVE_SOURCE_FILES = [
    {
        "relative_path": "lineitem/part-0.parquet",
        "location": "/bench-data/lineitem/part-0.parquet",
        "bytes": 223,
        "sha256": "d" * 64,
    }
]
NATIVE_STATEMENTS = [
    "CREATE TABLE \"lineitem\" AS SELECT * FROM "
    "read_parquet('/bench-data/lineitem/part-0.parquet')"
]


def source_digest(files: list[dict]) -> str:
    digest = hashlib.sha256()
    for source in files:
        relative = source["relative_path"].encode()
        digest.update(len(relative).to_bytes(8, "little"))
        digest.update(relative)
        digest.update(source["bytes"].to_bytes(8, "little"))
        digest.update(bytes.fromhex(source["sha256"]))
    return digest.hexdigest()


def query(
    engine: str,
    run_id: str,
    query_slot: int = 0,
    start_offset_ms: float = 0.1,
) -> dict:
    value = {
        "query_slot": query_slot,
        "harness_query_id": f"{run_id}:query-{query_slot}",
        "start_offset_ms": start_offset_ms,
        "finish_offset_ms": start_offset_ms + 10.0,
        "elapsed_ms": 10.0,
        "ttfb_ms": 5.0,
        "rows": 3,
        "batches": 1,
        "checksum": CHECKSUM,
        "checksum_mode": "multiset-sha256-v2",
        "checksum_backend": CHECKSUM_BACKENDS[engine],
        "checksum_compute_ms": 0.5,
        "complete": True,
    }
    if engine == "rustdb":
        value.update(
            {
                "discovered_files": 1,
                "scanned_rows": 3,
                "scanned_bytes": 128,
                "parquet_reader_builds": 0,
                "parquet_local_file_opens": 0,
                "parquet_narrow_decimal_columns": 0,
                "parquet_range_bytes_read": 128,
                "parquet_range_read_time_ms": 2.0,
                "parquet_decode_compute_time_ms": 3.0,
                "parquet_decode_compute_permit_wait_ms": 0.5,
                "parquet_decode_polls": 4,
                "parquet_decode_pending_polls": 2,
                "parquet_row_filter_compute_time_ms": 1.0,
                "parquet_row_filter_evaluations": 1,
                "parquet_row_filter_input_rows": 3,
                "parquet_alignment_time_ms": 0.1,
                "native_predicate_sidecar_bytes_read": 0,
                "native_predicate_sidecar_rows_evaluated": 0,
                "native_predicate_sidecar_rows_selected": 0,
                "native_predicate_sidecar_exact_bypasses": 0,
                "native_predicate_sidecar_full_projection_bypasses": 0,
                "native_predicate_sidecar_full_projection_rows": 0,
                "native_predicate_sidecar_full_projection_fallback_row_groups": 0,
                "native_predicate_sidecar_fallbacks": 0,
                "csv_source_bytes": 128,
                "csv_decompressed_bytes": 128,
                "csv_morsels": 1,
                "peak_csv_parser_lanes": 1,
                "current_reservation_bytes": 0,
                "peak_reservation_bytes": 4096,
                "peak_active_lanes": 1,
                "scheduler_wait_ms": 0.25,
                "compute_permit_wait_ms": 0.05,
                "queue_backpressure_wait_ms": 0.10,
                "barrier_wait_ms": 0.02,
                "csv_source_io_time_ms": 2.0,
                "csv_framing_time_ms": 1.0,
                "csv_decode_compute_time_ms": 4.0,
                "spill_read_bytes": 0,
                "spill_write_bytes": 0,
                "join_candidate_pairs": 0,
                "operators": [
                    {
                        "id": 0,
                        "parent_id": None,
                        "name": "Scan",
                        "input_rows": 0,
                        "input_batches": 0,
                        "output_rows": 3,
                        "output_batches": 1,
                        "output_bytes": 128,
                        "elapsed_ms": 9.0,
                        "wait_ms": 0.1,
                    }
                ],
            }
        )
    return value


def run(engine: str, version: str, iteration: int, order: int) -> dict:
    run_id = f"measured-{iteration}-{engine}"
    return {
        "kind": "run",
        "run_id": run_id,
        "engine": engine,
        "version": version,
        "build_id": ("1" if engine == "rustdb" else "2") * 64,
        "threads": 4,
        "memory_limit_bytes": 2_147_483_648,
        "concurrency": 1,
        "batch_size": 8192,
        "cache_state": {
            "os_page_cache": "warm-uncontrolled",
            "metadata_cache": "disabled",
            "external_file_cache": "disabled" if engine == "duckdb" else "not-applicable",
        },
        "storage_track": "csv",
        "storage_medium": "local-nvme",
        "engine_order": order,
        "group_elapsed_ms": 11.0,
        "rss_baseline_bytes": 10_000_000,
        "peak_rss_bytes": 12_000_000,
        "start_skew_ms": 0.0,
        "throughput_queries_per_second": 1000.0 / 11.0,
        "queries": [query(engine, run_id)],
    }


def add_engine_root_memory(run_value: dict, peak: int = 8192) -> None:
    run_value.update(
        {
            "engine_root_current_reservation_bytes": 0,
            "engine_root_lifetime_peak_reservation_bytes": peak,
            "engine_root_memory_limit_bytes": run_value["memory_limit_bytes"],
        }
    )


def worker_resources() -> dict:
    return {
        "cpu_affinity_list": "0-7",
        "cpu_affinity_count": 8,
        "cgroup_cpuset_cpus_effective": "0-7",
        "cgroup_cpu_quota_us": None,
        "cgroup_cpu_period_us": 100_000,
        "cgroup_memory_max_bytes": 4 << 30,
        "visible_memory_bytes": 64 << 30,
    }


def add_worker_resource_protocol(value: dict) -> None:
    resources = worker_resources()
    for engine_value in value["engines"].values():
        engine_value["hello"]["worker_resources"] = copy.deepcopy(resources)
        for run_value in engine_value["runs"]:
            run_value["worker_resources"] = copy.deepcopy(resources)


def hello(engine: str, version: str) -> dict:
    return {
        "kind": "hello",
        "engine": engine,
        "version": version,
        "build_id": ("1" if engine == "rustdb" else "2") * 64,
        "threads": 4,
        "memory_limit_bytes": 2_147_483_648,
        "concurrency": 1,
        "batch_size": 8192,
        "cache_state": {
            "os_page_cache": "warm-uncontrolled",
            "metadata_cache": "disabled",
            "external_file_cache": "disabled" if engine == "duckdb" else "not-applicable",
        },
    }


def report() -> dict:
    orders = [["rustdb", "duckdb"], ["duckdb", "rustdb"]]
    rustdb_runs = [
        run("rustdb", "0.7.0-alpha.1", 0, 0),
        run("rustdb", "0.7.0-alpha.1", 1, 1),
    ]
    duckdb_runs = [run("duckdb", "1.5.4", 0, 1), run("duckdb", "1.5.4", 1, 0)]
    value = {
        "contract_version": CONTRACT_VERSION,
        "host": {
            "system": "Darwin",
            "release": "25.0.0",
            "machine": "arm64",
            "cpu_model": "Apple M5 Max",
            "logical_cpus": 16,
            "total_memory_bytes": 64 << 30,
        },
        "storage_track": "csv",
        "storage_medium": "local-nvme",
        "warmup": 1,
        "iterations": 2,
        "config": {
            "threads": 4,
            "memory_limit_bytes": 2_147_483_648,
            "concurrency": 1,
            "batch_size": 8192,
        },
        "dataset": {"path": "employees.csv", "bytes": 223, "sha256": "b" * 64},
        "query": {"path": "query.sql", "bytes": 100, "sha256": "c" * 64},
        "engine_order": orders,
        "engines": {
            "rustdb": {
                "hello": hello("rustdb", "0.7.0-alpha.1"),
                "summary": rss_summary(rustdb_runs),
                "runs": rustdb_runs,
            },
            "duckdb": {
                "hello": hello("duckdb", "1.5.4"),
                "summary": rss_summary(duckdb_runs),
                "runs": duckdb_runs,
            },
        },
    }
    for run_value in value["engines"]["rustdb"]["runs"]:
        add_engine_root_memory(run_value)
    add_worker_resource_protocol(value)
    return value


def rss_summary(runs: list[dict]) -> dict:
    return coordinator_summary(runs)


def historical_report() -> dict:
    value = without_start_protocol(report())
    value["contract_version"] = HISTORICAL_CONTRACT_VERSION
    value.pop("host")
    value.pop("storage_medium")
    for engine in value["engines"].values():
        engine["hello"].pop("build_id")
        engine["summary"].pop("peak_rss_delta_bytes")
        for run_value in engine["runs"]:
            run_value.pop("build_id")
            for measured in run_value["queries"]:
                measured["checksum_mode"] = "multiset-sha256-v1"
                measured.pop("checksum_backend")
                measured.pop("checksum_compute_ms")
    return value


def setup_identity() -> dict:
    encoded = json.dumps(
        NATIVE_STATEMENTS, separators=(",", ":"), ensure_ascii=False
    ).encode()
    statements_sha256 = hashlib.sha256(encoded).hexdigest()
    source_sha256 = source_digest(NATIVE_SOURCE_FILES)
    identity = {
        "source_sha256": source_sha256,
        "statements_sha256": statements_sha256,
    }
    setup_id = hashlib.sha256(
        json.dumps(identity, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    return {
        "setup_id": setup_id,
        "storage_track": "native",
        "setup_engine_order": ["rustdb", "duckdb"],
        "source_sha256": source_sha256,
        "source_bytes": 223,
        "source_files": copy.deepcopy(NATIVE_SOURCE_FILES),
        "statements_sha256": statements_sha256,
        "statements": NATIVE_STATEMENTS,
        "max_storage_bytes": 1_000_000,
    }


def setup_response(engine: str, setup_id: str) -> dict:
    return {
        "kind": "setup",
        "engine": engine,
        "setup_id": setup_id,
        "complete": True,
        "load_elapsed_ms": 50.0 if engine == "rustdb" else 60.0,
        "rss_baseline_bytes": 10_000_000,
        "peak_rss_bytes": 12_000_000,
        "storage_baseline_bytes": 1024,
        "storage_peak_bytes": 4096,
        "storage_final_bytes": 3072,
        "table_count": len(NATIVE_STATEMENTS),
    }


def native_report() -> dict:
    value = report()
    identity = setup_identity()
    value["storage_track"] = "native"
    value["warmup"] = 0
    value["iterations"] = 10
    value["native_setup"] = identity
    value["dataset"].update(
        {
            "bytes": identity["source_bytes"],
            "files": len(identity["source_files"]),
            "sha256": identity["source_sha256"],
        }
    )
    value["engine_order"] = [
        ["rustdb", "duckdb"] if index % 2 == 0 else ["duckdb", "rustdb"]
        for index in range(10)
    ]
    for engine, version in (("rustdb", "0.7.0-alpha.1"), ("duckdb", "1.5.4")):
        runs = []
        for index, order in enumerate(value["engine_order"]):
            measured = run(engine, version, index, order.index(engine))
            measured["storage_track"] = "native"
            measured["setup_id"] = identity["setup_id"]
            runs.append(measured)
        setup = setup_response(engine, identity["setup_id"])
        value["engines"][engine] = {
            "hello": hello(engine, version),
            "setup": setup,
            "summary": coordinator_summary(runs, setup),
            "runs": runs,
        }
    for run_value in value["engines"]["rustdb"]["runs"]:
        add_engine_root_memory(run_value)
    add_worker_resource_protocol(value)
    return value


def concurrent_report(concurrency: int = 4) -> dict:
    value = report()
    value["config"]["concurrency"] = concurrency
    for engine, engine_value in value["engines"].items():
        engine_value["hello"]["concurrency"] = concurrency
        for run_value in engine_value["runs"]:
            starts = [0.1 + slot * 0.05 for slot in range(concurrency)]
            run_value["concurrency"] = concurrency
            run_value["queries"] = [
                query(engine, run_value["run_id"], slot, starts[slot])
                for slot in range(concurrency)
            ]
            run_value["start_skew_ms"] = max(starts) - min(starts)
            run_value["throughput_queries_per_second"] = (
                concurrency * 1000.0 / run_value["group_elapsed_ms"]
            )
            if engine == "rustdb":
                add_engine_root_memory(run_value)
        engine_value["summary"] = coordinator_summary(engine_value["runs"])
    add_worker_resource_protocol(value)
    return value


def without_start_protocol(value: dict) -> dict:
    value = copy.deepcopy(value)
    for engine_value in value["engines"].values():
        engine_value["hello"].pop("worker_resources", None)
        for run_value in engine_value["runs"]:
            run_value.pop("start_skew_ms", None)
            run_value.pop("worker_resources", None)
            for field in contract.ENGINE_ROOT_MEMORY_FIELDS:
                run_value.pop(field, None)
            for measured in run_value["queries"]:
                for field in (
                    "query_slot",
                    "harness_query_id",
                    "start_offset_ms",
                    "finish_offset_ms",
                ):
                    measured.pop(field, None)
    return value


class ContractTests(unittest.TestCase):
    def test_accepts_complete_current_report(self) -> None:
        validate_report(report())

    def test_accepts_four_concurrent_identified_queries(self) -> None:
        validate_report(concurrent_report())

    def test_accepts_complete_single_query_worker_resources(self) -> None:
        value = report()
        add_worker_resource_protocol(value)
        validate_report(value)

    def test_rejects_partial_single_query_worker_resources(self) -> None:
        value = report()
        value["engines"]["rustdb"]["runs"][0].pop("worker_resources")
        with self.assertRaisesRegex(ContractError, "hello and every run or none"):
            validate_report(value)

        value = report()
        add_worker_resource_protocol(value)
        value["engines"]["rustdb"]["hello"]["worker_resources"].pop(
            "cpu_affinity_count"
        )
        with self.assertRaisesRegex(ContractError, "must contain exactly"):
            validate_report(value)

    def test_concurrent_report_requires_worker_resources(self) -> None:
        value = concurrent_report()
        for engine_value in value["engines"].values():
            engine_value["hello"].pop("worker_resources")
            for run_value in engine_value["runs"]:
                run_value.pop("worker_resources")
        with self.assertRaisesRegex(ContractError, "current protocol"):
            validate_report(value)

    def test_rejects_asymmetric_or_changed_worker_resources(self) -> None:
        value = report()
        add_worker_resource_protocol(value)
        value["engines"]["duckdb"]["hello"].pop("worker_resources")
        for run_value in value["engines"]["duckdb"]["runs"]:
            run_value.pop("worker_resources")
        with self.assertRaisesRegex(ContractError, "current protocol"):
            validate_report(value)

        value = report()
        add_worker_resource_protocol(value)
        value["engines"]["rustdb"]["runs"][0]["worker_resources"][
            "visible_memory_bytes"
        ] += 1
        with self.assertRaisesRegex(ContractError, "must equal hello"):
            validate_report(value)

        value = report()
        add_worker_resource_protocol(value)
        for carrier in [value["engines"]["duckdb"]["hello"]] + value["engines"][
            "duckdb"
        ]["runs"]:
            carrier["worker_resources"]["visible_memory_bytes"] += 1
        with self.assertRaisesRegex(ContractError, "must be identical"):
            validate_report(value)

    def test_rejects_worker_resources_below_configured_limits(self) -> None:
        cases = (
            ("cpu_affinity_count", 7, "cpu_affinity_count"),
            ("cpu_affinity_list", "0-2", "below configured threads"),
            (
                "cgroup_cpuset_cpus_effective",
                "0-2",
                "below configured threads",
            ),
            ("cgroup_cpu_quota_us", 399_999, "below configured threads"),
            ("cgroup_memory_max_bytes", (2 << 30) - 1, "below configured memory"),
            ("visible_memory_bytes", (2 << 30) - 1, "below configured memory"),
        )
        for field, invalid, message in cases:
            with self.subTest(field=field):
                value = report()
                add_worker_resource_protocol(value)
                value["engines"]["rustdb"]["hello"]["worker_resources"][
                    field
                ] = invalid
                if field == "cpu_affinity_list":
                    value["engines"]["rustdb"]["hello"]["worker_resources"][
                        "cpu_affinity_count"
                    ] = 3
                if field == "visible_memory_bytes":
                    value["engines"]["rustdb"]["hello"]["worker_resources"][
                        "cgroup_memory_max_bytes"
                    ] = None
                with self.assertRaisesRegex(ContractError, message):
                    validate_report(value)

    def test_accepts_complete_single_query_engine_root_memory(self) -> None:
        value = report()
        for run_value in value["engines"]["rustdb"]["runs"]:
            add_engine_root_memory(run_value)
        validate_report(value)

    def test_rejects_partial_single_query_engine_root_memory(self) -> None:
        value = report()
        value["engines"]["rustdb"]["runs"][0].pop(
            "engine_root_current_reservation_bytes"
        )
        with self.assertRaisesRegex(ContractError, "all Engine root memory fields"):
            validate_report(value)

    def test_single_query_protocol_activation_applies_to_every_run(self) -> None:
        complete = report()
        value = without_start_protocol(complete)
        source = complete["engines"]["rustdb"]["runs"][0]
        target = value["engines"]["rustdb"]["runs"][0]
        target["start_skew_ms"] = source["start_skew_ms"]
        for field in contract.ENGINE_ROOT_MEMORY_FIELDS:
            target[field] = source[field]
        for field in (
            "query_slot",
            "harness_query_id",
            "start_offset_ms",
            "finish_offset_ms",
        ):
            target["queries"][0][field] = source["queries"][0][field]
        with self.assertRaisesRegex(ContractError, r"runs\[1\].start_skew_ms"):
            validate_report(value)

        value = without_start_protocol(complete)
        value["engines"]["rustdb"]["runs"][0]["queries"][0][
            "worker_resources"
        ] = {}
        with self.assertRaisesRegex(ContractError, r"runs\[0\].start_skew_ms"):
            validate_report(value)

    def test_concurrent_report_requires_engine_root_memory(self) -> None:
        value = concurrent_report()
        for field in contract.ENGINE_ROOT_MEMORY_FIELDS:
            value["engines"]["rustdb"]["runs"][0].pop(field)
        with self.assertRaisesRegex(ContractError, "must record Engine root memory"):
            validate_report(value)

    def test_rejects_invalid_engine_root_memory_accounting(self) -> None:
        cases = (
            ("engine_root_current_reservation_bytes", 1, "must be zero"),
            (
                "engine_root_lifetime_peak_reservation_bytes",
                2_147_483_649,
                "exceeds limit",
            ),
            ("engine_root_memory_limit_bytes", 1 << 30, "memory_limit_bytes"),
            (
                "engine_root_lifetime_peak_reservation_bytes",
                4095,
                "below a query peak",
            ),
        )
        for field, invalid, message in cases:
            with self.subTest(field=field, invalid=invalid):
                value = concurrent_report()
                value["engines"]["rustdb"]["runs"][0][field] = invalid
                with self.assertRaisesRegex(ContractError, message):
                    validate_report(value)

    def test_accepts_retained_single_query_v3_without_start_protocol(self) -> None:
        value = without_start_protocol(report())
        value["engines"]["rustdb"]["runs"][0]["queries"][0][
            "current_reservation_bytes"
        ] = 1
        validate_report(value)

    def test_rejects_partial_single_query_start_protocol(self) -> None:
        value = without_start_protocol(report())
        value["engines"]["rustdb"]["runs"][0]["queries"][0]["query_slot"] = 0
        with self.assertRaisesRegex(ContractError, "start_skew_ms"):
            validate_report(value)

    def test_concurrent_current_report_requires_start_protocol(self) -> None:
        value = without_start_protocol(concurrent_report())
        with self.assertRaisesRegex(ContractError, "start_skew_ms"):
            validate_report(value)

    def test_rejects_duplicate_concurrent_slot_or_id(self) -> None:
        value = concurrent_report()
        measured = value["engines"]["rustdb"]["runs"][0]["queries"]
        measured[1]["query_slot"] = 0
        with self.assertRaisesRegex(ContractError, "query_slot"):
            validate_report(value)

        value = concurrent_report()
        measured = value["engines"]["duckdb"]["runs"][0]["queries"]
        measured[1]["harness_query_id"] = measured[0]["harness_query_id"]
        with self.assertRaisesRegex(ContractError, "harness_query_id"):
            validate_report(value)

    def test_rejects_inconsistent_start_skew_or_offsets(self) -> None:
        value = concurrent_report()
        value["engines"]["rustdb"]["runs"][0]["start_skew_ms"] += 1.0
        with self.assertRaisesRegex(ContractError, "start_skew_ms"):
            validate_report(value)

        value = concurrent_report()
        measured = value["engines"]["duckdb"]["runs"][0]["queries"][2]
        measured["finish_offset_ms"] = measured["start_offset_ms"] - 0.01
        with self.assertRaisesRegex(ContractError, "finish_offset_ms"):
            validate_report(value)

    def test_rejects_nonzero_terminal_current_reservation(self) -> None:
        value = report()
        value["engines"]["rustdb"]["runs"][0]["queries"][0][
            "current_reservation_bytes"
        ] = 1
        with self.assertRaisesRegex(ContractError, "must be zero"):
            validate_report(value)

    def test_accepts_native_setup_and_ten_round_amortization(self) -> None:
        validate_report(native_report())

    def test_native_run_must_echo_setup_id(self) -> None:
        value = native_report()
        value["engines"]["rustdb"]["runs"][0].pop("setup_id")
        with self.assertRaisesRegex(ContractError, "setup_id"):
            validate_report(value)

    def test_rejects_stale_native_amortization(self) -> None:
        value = native_report()
        value["engines"]["duckdb"]["summary"]["amortized_elapsed_ms"] += 1
        with self.assertRaisesRegex(ContractError, "amortized_elapsed_ms"):
            validate_report(value)

    def test_rejects_native_setup_over_memory_or_storage_limit(self) -> None:
        value = native_report()
        value["engines"]["rustdb"]["setup"]["peak_rss_bytes"] = 3 << 30
        with self.assertRaisesRegex(ContractError, "configured memory"):
            validate_report(value)
        value = native_report()
        value["engines"]["duckdb"]["setup"]["storage_peak_bytes"] = 1_000_001
        with self.assertRaisesRegex(ContractError, "max_storage_bytes"):
            validate_report(value)

    def test_rejects_native_configured_storage_above_contract_limit(self) -> None:
        value = native_report()
        value["native_setup"]["max_storage_bytes"] = (
            contract.native_storage_maximum(223, len(NATIVE_STATEMENTS)) + 1
        )
        with self.assertRaisesRegex(ContractError, "2x plus bounded metadata"):
            validate_report(value)

    def test_rejects_native_storage_order_and_table_count_mismatch(self) -> None:
        value = native_report()
        value["engines"]["rustdb"]["setup"]["storage_final_bytes"] = 5000
        with self.assertRaisesRegex(ContractError, "baseline <= final <= peak"):
            validate_report(value)
        value = native_report()
        value["engines"]["duckdb"]["setup"]["table_count"] = 2
        with self.assertRaisesRegex(ContractError, "table_count"):
            validate_report(value)

    def test_rejects_native_statement_identity_mismatch(self) -> None:
        value = native_report()
        value["native_setup"]["statements_sha256"] = "f" * 64
        with self.assertRaisesRegex(ContractError, "statements_sha256"):
            validate_report(value)

    def test_rejects_native_source_file_identity_mismatch(self) -> None:
        value = native_report()
        value["native_setup"]["source_files"][0]["sha256"] = "e" * 64
        with self.assertRaisesRegex(ContractError, "source_files sha256"):
            validate_report(value)

    def test_rejects_duplicate_native_source_file(self) -> None:
        value = native_report()
        value["native_setup"]["source_files"].append(
            copy.deepcopy(value["native_setup"]["source_files"][0])
        )
        with self.assertRaisesRegex(ContractError, "duplicated"):
            validate_report(value)

    def test_rejects_unsorted_native_source_files(self) -> None:
        files = [
            {
                "relative_path": "z.parquet",
                "location": "/bench-data/z.parquet",
                "bytes": 1,
                "sha256": "d" * 64,
            },
            {
                "relative_path": "a.parquet",
                "location": "/bench-data/a.parquet",
                "bytes": 1,
                "sha256": "e" * 64,
            },
        ]
        dataset = {"bytes": 2, "files": 2, "sha256": "f" * 64}
        with self.assertRaisesRegex(ContractError, "sorted by relative_path"):
            contract.validate_native_source_files(files, dataset)

    def test_rejects_native_statement_wildcard_or_missing_location(self) -> None:
        value = native_report()
        value["native_setup"]["statements"] = [
            "CREATE TABLE lineitem AS SELECT * FROM "
            "read_parquet('/bench-data/lineitem/*.parquet')"
        ]
        with self.assertRaisesRegex(ContractError, "explicit source location"):
            validate_report(value)

    def test_native_setup_order_is_attributed(self) -> None:
        value = native_report()
        value["native_setup"]["setup_engine_order"].reverse()
        with self.assertRaisesRegex(ContractError, "setup_engine_order"):
            validate_report(value)

    def test_native_requires_zero_warmup_and_ten_rounds(self) -> None:
        value = native_report()
        value["warmup"] = 1
        with self.assertRaisesRegex(ContractError, "warmup=0"):
            validate_report(value)

    def test_manifest_statements_are_sorted_and_sql_escaped(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "a").mkdir()
            (root / "z").mkdir()
            (root / "a" / "part-2.parquet").write_bytes(b"aa")
            (root / "a" / "part-1.parquet").write_bytes(b"a")
            (root / "z" / "part.parquet").write_bytes(b"z")
            manifest = root / "native.json"
            manifest.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "source_root": "/bench'data",
                        "tables": [
                            {"name": "z", "path": "z/*.parquet"},
                            {"name": 'a"b', "path": "a/*.parquet"},
                        ],
                    }
                ),
                encoding="utf-8",
            )
            loaded = load_native_manifest(manifest, root)
        self.assertEqual(
            loaded["statements"],
            [
                'CREATE TABLE "a""b" AS SELECT * FROM '
                "read_parquet('/bench''data/a/part-1.parquet') UNION ALL "
                "SELECT * FROM read_parquet('/bench''data/a/part-2.parquet')",
                'CREATE TABLE "z" AS SELECT * FROM '
                "read_parquet('/bench''data/z/part.parquet')",
            ],
        )
        self.assertEqual(
            [source["relative_path"] for source in loaded["source_files"]],
            ["a/part-1.parquet", "a/part-2.parquet", "z/part.parquet"],
        )
        self.assertEqual(
            [source["location"] for source in loaded["source_files"]],
            [
                "/bench'data/a/part-1.parquet",
                "/bench'data/a/part-2.parquet",
                "/bench'data/z/part.parquet",
            ],
        )
        self.assertEqual(loaded["dataset"]["files"], 3)
        self.assertEqual(loaded["dataset"]["bytes"], 4)

    def test_manifest_rejects_missing_duplicate_and_escaping_sources(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "part.parquet").write_bytes(b"data")
            manifest = root / "native.json"

            def write(tables: list[dict]) -> None:
                manifest.write_text(
                    json.dumps(
                        {"version": 1, "source_root": "/bench-data", "tables": tables}
                    ),
                    encoding="utf-8",
                )

            write([{"name": "missing", "path": "missing/*.parquet"}])
            with self.assertRaisesRegex(ValueError, "matched no files"):
                load_native_manifest(manifest, root)
            write(
                [
                    {"name": "first", "path": "*.parquet"},
                    {"name": "second", "path": "part.parquet"},
                ]
            )
            with self.assertRaisesRegex(ValueError, "selected more than once"):
                load_native_manifest(manifest, root)
            write([{"name": "escape", "path": "../*.parquet"}])
            with self.assertRaisesRegex(ValueError, "escapes"):
                load_native_manifest(manifest, root)

    def test_manifest_requires_local_absolute_source_root(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "part.parquet").write_bytes(b"data")
            manifest = root / "native.json"
            for source_root in (
                "relative",
                "s3://bucket",
                "/bench/*",
                "/bench-data/.",
                "/bench-data/../source",
                "/bench-data//source",
                "//bench-data",
                "/bench-data/",
            ):
                manifest.write_text(
                    json.dumps(
                        {
                            "version": 1,
                            "source_root": source_root,
                            "tables": [{"name": "t", "path": "part.parquet"}],
                        }
                    ),
                    encoding="utf-8",
                )
                with self.assertRaisesRegex(ValueError, "absolute POSIX"):
                    load_native_manifest(manifest, root)

    def test_contract_rejects_non_normal_native_source_locations(self) -> None:
        dataset = {
            "bytes": 223,
            "files": 1,
            "sha256": source_digest(NATIVE_SOURCE_FILES),
        }
        for location in (
            "/bench-data/./lineitem/part-0.parquet",
            "/bench-data/stale/../lineitem/part-0.parquet",
            "/bench-data//lineitem/part-0.parquet",
            "//bench-data/lineitem/part-0.parquet",
        ):
            files = copy.deepcopy(NATIVE_SOURCE_FILES)
            files[0]["location"] = location
            with self.subTest(location=location):
                with self.assertRaisesRegex(ContractError, "lexically-normal"):
                    contract.validate_native_source_files(files, dataset)

    def test_manifest_rejects_wildcard_in_resolved_file_name(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "part*.parquet").write_bytes(b"data")
            manifest = root / "native.json"
            manifest.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "source_root": "/bench-data",
                        "tables": [{"name": "t", "path": "part*.parquet"}],
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "wildcard syntax"):
                load_native_manifest(manifest, root)

    def test_native_setup_rechecks_source_identity(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "part.parquet"
            source.write_bytes(b"before")
            manifest = root / "native.json"
            manifest.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "source_root": "/bench-data",
                        "tables": [{"name": "t", "path": "part.parquet"}],
                    }
                ),
                encoding="utf-8",
            )
            args = SimpleNamespace(
                native_manifest=manifest,
                dataset=root,
                max_native_workspace_bytes=1_000_000,
            )
            expected = native_setup(args)
            source.write_bytes(b"after")
            with self.assertRaisesRegex(RuntimeError, "changed during benchmark setup"):
                verify_native_sources(args, expected)

    def test_native_setup_rejects_cli_storage_limit_above_contract(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "part.parquet").write_bytes(b"data")
            manifest = root / "native.json"
            manifest.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "source_root": "/bench-data",
                        "tables": [{"name": "t", "path": "part.parquet"}],
                    }
                ),
                encoding="utf-8",
            )
            args = SimpleNamespace(
                native_manifest=manifest,
                dataset=root,
                max_native_workspace_bytes=2_000_000,
            )
            with self.assertRaisesRegex(ValueError, "2x plus bounded metadata"):
                native_setup(args)

    def test_current_contract_requires_host_facts(self) -> None:
        value = report()
        value.pop("host")
        with self.assertRaisesRegex(ContractError, "host"):
            validate_report(value)

    def test_current_contract_requires_storage_medium(self) -> None:
        value = report()
        value.pop("storage_medium")
        with self.assertRaisesRegex(ContractError, "storage_medium"):
            validate_report(value)

    def test_rejects_historical_report_by_default(self) -> None:
        with self.assertRaisesRegex(ContractError, "--allow-historical"):
            validate_report(historical_report())

    def test_accepts_all_v1_report_in_historical_mode(self) -> None:
        self.assertEqual(
            validate_report(historical_report(), allow_historical=True),
            "historical",
        )

    def test_v2_is_historical_only(self) -> None:
        value = report()
        value["contract_version"] = HISTORICAL_CONTRACT_VERSION_V2
        with self.assertRaisesRegex(ContractError, "--allow-historical"):
            validate_report(value)
        self.assertEqual(validate_report(value, allow_historical=True), "historical")

    def test_rejects_mixed_checksum_modes(self) -> None:
        value = report()
        value["engines"]["duckdb"]["runs"][1]["queries"][0][
            "checksum_mode"
        ] = "multiset-sha256-v1"
        with self.assertRaisesRegex(ContractError, "cannot be mixed"):
            validate_report(value)

    def test_historical_contract_rejects_v2_checksum(self) -> None:
        value = historical_report()
        value["engines"]["rustdb"]["runs"][0]["queries"][0][
            "checksum_mode"
        ] = "multiset-sha256-v2"
        with self.assertRaisesRegex(ContractError, "cannot be mixed"):
            validate_report(value, allow_historical=True)

    def test_rejects_cross_engine_checksum_mismatch(self) -> None:
        value = report()
        value["engines"]["duckdb"]["runs"][1]["queries"][0]["checksum"] = "d" * 64
        with self.assertRaisesRegex(ContractError, "checksums differ|checksum mismatch"):
            validate_report(value)

    def test_rejects_after_query_rss_disguised_as_peak(self) -> None:
        value = report()
        value["engines"]["rustdb"]["runs"][0]["peak_rss_bytes"] = 1
        with self.assertRaisesRegex(ContractError, "peak RSS is below baseline"):
            validate_report(value)

    def test_rejects_non_alternating_order(self) -> None:
        value = copy.deepcopy(report())
        value["engine_order"][1] = ["rustdb", "duckdb"]
        with self.assertRaisesRegex(ContractError, "alternate exactly"):
            validate_report(value)

    def test_rejects_negative_rustdb_csv_metric(self) -> None:
        value = report()
        value["engines"]["rustdb"]["runs"][0]["queries"][0]["csv_morsels"] = -1
        with self.assertRaisesRegex(ContractError, "csv_morsels must be a non-negative integer"):
            validate_report(value)

    def test_rejects_negative_additive_timing_metric(self) -> None:
        value = report()
        query_value = value["engines"]["rustdb"]["runs"][0]["queries"][0]
        query_value["csv_decode_compute_time_ms"] = -1.0
        with self.assertRaisesRegex(
            ContractError, "csv_decode_compute_time_ms must be non-negative"
        ):
            validate_report(value)

    def test_accepts_optional_queue_edge_wait_metrics_and_missing_fields(self) -> None:
        value = report()
        measured = value["engines"]["rustdb"]["runs"][0]["queries"][0]
        measured.update(
            {
                "csv_morsel_queue_wait_ms": 0.1,
                "scan_pipeline_output_queue_wait_ms": 0.2,
                "aggregate_lane_dispatch_queue_wait_ms": 0.3,
                "aggregate_partial_output_queue_wait_ms": 0.4,
            }
        )
        validate_report(value)

        for field in (
            "csv_morsel_queue_wait_ms",
            "scan_pipeline_output_queue_wait_ms",
            "aggregate_lane_dispatch_queue_wait_ms",
            "aggregate_partial_output_queue_wait_ms",
        ):
            measured.pop(field, None)
        validate_report(value)

    def test_rejects_negative_optional_queue_edge_wait_metric(self) -> None:
        value = report()
        measured = value["engines"]["rustdb"]["runs"][0]["queries"][0]
        measured["aggregate_lane_dispatch_queue_wait_ms"] = -0.1
        with self.assertRaisesRegex(
            ContractError,
            "aggregate_lane_dispatch_queue_wait_ms must be non-negative",
        ):
            validate_report(value)

    def test_accepts_operator_parent_before_parent_entry(self) -> None:
        value = report()
        measured = value["engines"]["rustdb"]["runs"][0]["queries"][0]
        root = copy.deepcopy(measured["operators"][0])
        root["id"] = 7
        child = copy.deepcopy(root)
        child.update({"id": 8, "parent_id": 7, "name": "Filter"})
        measured["operators"] = [child, root]
        validate_report(value)

    def test_rejects_duplicate_operator_id(self) -> None:
        value = report()
        measured = value["engines"]["rustdb"]["runs"][0]["queries"][0]
        measured["operators"].append(copy.deepcopy(measured["operators"][0]))
        with self.assertRaisesRegex(ContractError, "duplicate operator id 0"):
            validate_report(value)

    def test_rejects_missing_operator_parent(self) -> None:
        value = report()
        measured = value["engines"]["rustdb"]["runs"][0]["queries"][0]
        measured["operators"][0]["parent_id"] = 99
        with self.assertRaisesRegex(ContractError, "parent_id 99 does not reference"):
            validate_report(value)

    def test_rejects_self_parented_operator(self) -> None:
        value = report()
        measured = value["engines"]["rustdb"]["runs"][0]["queries"][0]
        measured["operators"][0]["parent_id"] = 0
        with self.assertRaisesRegex(ContractError, "cannot be its own parent"):
            validate_report(value)

    def test_accepts_retained_v3_without_native_predicate_sidecar_metrics(self) -> None:
        value = report()
        fields = (
            "native_predicate_sidecar_bytes_read",
            "native_predicate_sidecar_rows_evaluated",
            "native_predicate_sidecar_rows_selected",
            "native_predicate_sidecar_exact_bypasses",
            "native_predicate_sidecar_full_projection_bypasses",
            "native_predicate_sidecar_full_projection_rows",
            "native_predicate_sidecar_full_projection_fallback_row_groups",
            "native_predicate_sidecar_fallbacks",
        )
        for run_value in value["engines"]["rustdb"]["runs"]:
            for field in fields:
                run_value["queries"][0].pop(field)
        validate_report(value)

    def test_rejects_invalid_native_predicate_sidecar_metrics(self) -> None:
        value = report()
        measured = value["engines"]["rustdb"]["runs"][0]["queries"][0]
        measured["native_predicate_sidecar_bytes_read"] = -1
        with self.assertRaisesRegex(
            ContractError,
            "native_predicate_sidecar_bytes_read must be a non-negative integer",
        ):
            validate_report(value)

        value = report()
        measured = value["engines"]["rustdb"]["runs"][0]["queries"][0]
        measured["native_predicate_sidecar_rows_evaluated"] = 2
        measured["native_predicate_sidecar_rows_selected"] = 3
        with self.assertRaisesRegex(
            ContractError,
            "native_predicate_sidecar_rows_selected exceeds evaluated rows",
        ):
            validate_report(value)

        value = report()
        measured = value["engines"]["rustdb"]["runs"][0]["queries"][0]
        measured["native_predicate_sidecar_full_projection_fallback_row_groups"] = -1
        with self.assertRaisesRegex(
            ContractError,
            "native_predicate_sidecar_full_projection_fallback_row_groups must be a non-negative integer",
        ):
            validate_report(value)

    def test_current_contract_requires_attributable_timing_metrics(self) -> None:
        value = report()
        value["engines"]["rustdb"]["runs"][0]["queries"][0].pop(
            "queue_backpressure_wait_ms"
        )
        with self.assertRaisesRegex(ContractError, "queue_backpressure_wait_ms"):
            validate_report(value)

    def test_accepts_optional_query_preparation_diagnostics(self) -> None:
        value = report()
        query_value = value["engines"]["rustdb"]["runs"][0]["queries"][0]
        query_value.update(
            {
                "execute_return_ms": query_value["ttfb_ms"],
                "query_admission_wait_ms": 0.05,
                "sql_parse_time_ms": 0.1,
                "table_function_prepare_time_ms": 0.15,
                "bind_time_ms": 0.2,
                "provider_prepare_time_ms": 0.3,
                "optimize_time_ms": 0.4,
                "native_verification_time_ms": 0.05,
                "native_full_verification_segments": 0,
            }
        )
        validate_report(value)

    def test_rejects_execute_return_after_ttfb(self) -> None:
        value = report()
        query_value = value["engines"]["rustdb"]["runs"][0]["queries"][0]
        query_value["execute_return_ms"] = query_value["ttfb_ms"] + 1.0
        with self.assertRaisesRegex(ContractError, "execute_return_ms exceeds ttfb_ms"):
            validate_report(value)

    def test_rejects_negative_optional_query_preparation_diagnostic(self) -> None:
        value = report()
        query_value = value["engines"]["rustdb"]["runs"][0]["queries"][0]
        query_value["native_verification_time_ms"] = -1.0
        with self.assertRaisesRegex(ContractError, "native_verification_time_ms"):
            validate_report(value)

    def test_requires_engine_specific_checksum_backend(self) -> None:
        value = report()
        measured = value["engines"]["duckdb"]["runs"][0]["queries"][0]
        measured["checksum_backend"] = CHECKSUM_BACKENDS["rustdb"]
        with self.assertRaisesRegex(ContractError, "checksum_backend"):
            validate_report(value)

    def test_requires_stable_worker_build_id(self) -> None:
        value = report()
        value["engines"]["rustdb"]["runs"][0]["build_id"] = "3" * 64
        with self.assertRaisesRegex(ContractError, "build_id"):
            validate_report(value)

    def test_requires_checksum_compute_time(self) -> None:
        value = report()
        value["engines"]["rustdb"]["runs"][0]["queries"][0].pop(
            "checksum_compute_ms"
        )
        with self.assertRaisesRegex(ContractError, "checksum_compute_ms"):
            validate_report(value)

    def test_rejects_large_result_until_native_consumer_exists(self) -> None:
        value = report()
        value["engines"]["rustdb"]["runs"][0]["queries"][0]["rows"] = 65_537
        with self.assertRaisesRegex(ContractError, "common native checksum consumer"):
            validate_report(value)

    def test_rejects_stale_absolute_peak_summary(self) -> None:
        value = report()
        value["engines"]["rustdb"]["summary"]["peak_rss_bytes"] += 1
        with self.assertRaisesRegex(ContractError, "summary.peak_rss_bytes"):
            validate_report(value)

    def test_rejects_stale_peak_delta_summary(self) -> None:
        value = report()
        value["engines"]["duckdb"]["summary"]["peak_rss_delta_bytes"] += 1
        with self.assertRaisesRegex(ContractError, "summary.peak_rss_delta_bytes"):
            validate_report(value)

    def test_rejects_stale_latency_summary(self) -> None:
        value = report()
        value["engines"]["duckdb"]["summary"]["p50_elapsed_ms"] += 1
        with self.assertRaisesRegex(ContractError, "summary.p50_elapsed_ms"):
            validate_report(value)

    def test_rejects_inconsistent_run_throughput(self) -> None:
        value = report()
        value["engines"]["rustdb"]["runs"][0][
            "throughput_queries_per_second"
        ] += 1
        with self.assertRaisesRegex(ContractError, "throughput_queries_per_second"):
            validate_report(value)

    def test_coordinator_summary_records_absolute_and_delta_peaks(self) -> None:
        runs = [
            run("duckdb", "1.5.4", 0, 1),
            run("duckdb", "1.5.4", 1, 0),
        ]
        runs[1]["rss_baseline_bytes"] = 12_000_000
        runs[1]["peak_rss_bytes"] = 13_000_000
        value = coordinator_summary(runs)
        self.assertEqual(value["peak_rss_bytes"], 13_000_000)
        self.assertEqual(value["peak_rss_delta_bytes"], 2_000_000)

    def test_accepts_historical_report_without_additive_metrics(self) -> None:
        value = historical_report()
        for run_value in value["engines"]["rustdb"]["runs"]:
            measured = run_value["queries"][0]
            for field in (
                "discovered_files",
                "parquet_range_bytes_read",
                "parquet_range_read_time_ms",
                "parquet_decode_compute_time_ms",
                "parquet_decode_compute_permit_wait_ms",
                "parquet_decode_polls",
                "parquet_decode_pending_polls",
                "parquet_row_filter_compute_time_ms",
                "parquet_row_filter_evaluations",
                "parquet_row_filter_input_rows",
                "parquet_alignment_time_ms",
                "parquet_reader_builds",
                "parquet_local_file_opens",
                "parquet_narrow_decimal_columns",
                "native_predicate_sidecar_bytes_read",
                "native_predicate_sidecar_rows_evaluated",
                "native_predicate_sidecar_rows_selected",
                "native_predicate_sidecar_exact_bypasses",
                "native_predicate_sidecar_full_projection_bypasses",
                "native_predicate_sidecar_full_projection_rows",
                "native_predicate_sidecar_full_projection_fallback_row_groups",
                "native_predicate_sidecar_fallbacks",
                "csv_source_bytes",
                "csv_decompressed_bytes",
                "csv_morsels",
                "peak_csv_parser_lanes",
                "compute_permit_wait_ms",
                "queue_backpressure_wait_ms",
                "barrier_wait_ms",
                "csv_source_io_time_ms",
                "csv_framing_time_ms",
                "csv_decode_compute_time_ms",
            ):
                measured.pop(field)
        validate_report(value, allow_historical=True)

    def test_cli_marks_historical_report_as_non_gate_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "historical.json"
            path.write_text(json.dumps(historical_report()), encoding="utf-8")
            output = io.StringIO()
            with mock.patch.object(
                sys,
                "argv",
                ["contract.py", "--allow-historical", str(path)],
            ), redirect_stdout(output):
                self.assertEqual(contract.main(), 0)
        self.assertIn("historical", output.getvalue())
        self.assertIn("not valid v0.7 gate evidence", output.getvalue())


if __name__ == "__main__":
    unittest.main()
