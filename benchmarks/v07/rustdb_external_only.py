#!/usr/bin/env python3
"""Run a bounded RustDB-only diagnostic against external CSV or Parquet."""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from contract import CHECKSUM_BACKENDS, CURRENT_CHECKSUM_MODE
from coordinator import Worker, command, file_facts, path_facts, run_command
from host_facts import collect as collect_host_facts


SCHEMA = "rustdb-external-only-diagnostic-v1"
SHA256 = re.compile(r"^[0-9a-f]{64}$")


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run RustDB-only external CSV or Parquet diagnostics"
    )
    parser.add_argument("--query", type=Path, required=True)
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--storage-track", choices=("csv", "parquet"), required=True)
    parser.add_argument(
        "--storage-medium", choices=("local-nvme", "minio"), required=True
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--threads", type=positive, default=4)
    parser.add_argument(
        "--memory",
        "--memory-limit",
        dest="memory_limit_bytes",
        type=positive,
        default=2_147_483_648,
    )
    parser.add_argument("--concurrency", type=positive, default=1)
    parser.add_argument(
        "--batch", "--batch-size", dest="batch_size", type=positive, default=8192
    )
    parser.add_argument("--warmup", type=nonnegative, default=1)
    parser.add_argument("--iterations", type=positive, default=2)
    parser.add_argument("--rustdb-command-json", required=True)
    return parser.parse_args()


def positive(value: str) -> int:
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def nonnegative(value: str) -> int:
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return number


def config(args: argparse.Namespace) -> dict[str, int]:
    return {
        "threads": args.threads,
        "memory_limit_bytes": args.memory_limit_bytes,
        "concurrency": args.concurrency,
        "batch_size": args.batch_size,
    }


def validate_hello(value: dict[str, Any], expected: dict[str, int]) -> None:
    fields: dict[str, Any] = {"kind": "hello", "engine": "rustdb"} | expected
    for key, wanted in fields.items():
        if value.get(key) != wanted:
            raise RuntimeError(f"RustDB hello has unexpected {key}: {value.get(key)!r}")
    if not isinstance(value.get("version"), str) or not value["version"]:
        raise RuntimeError("RustDB hello omitted its version")
    if not valid_sha(value.get("build_id")):
        raise RuntimeError("RustDB hello has an invalid build id")
    cache = value.get("cache_state")
    if not isinstance(cache, dict) or cache.get("metadata_cache") != "disabled":
        raise RuntimeError("external-only diagnostics require a disabled metadata cache")
    if cache.get("os_page_cache") != "warm-uncontrolled":
        raise RuntimeError("RustDB hello has an unexpected OS page-cache state")
    if cache.get("external_file_cache") != "not-applicable":
        raise RuntimeError("RustDB hello has an unexpected external-file-cache state")


def validate_run(
    value: dict[str, Any],
    hello: dict[str, Any],
    expected: dict[str, int],
    storage_track: str,
    run_id: str,
) -> str:
    fields: dict[str, Any] = {
        "kind": "run",
        "run_id": run_id,
        "engine": "rustdb",
        "version": hello["version"],
        "build_id": hello["build_id"],
        "storage_track": storage_track,
        "engine_order": 0,
    } | expected
    for key, wanted in fields.items():
        if value.get(key) != wanted:
            raise RuntimeError(f"RustDB run {run_id!r} has unexpected {key}")
    if value.get("cache_state") != hello.get("cache_state"):
        raise RuntimeError(f"RustDB run {run_id!r} changed its cache state")
    group_elapsed = positive_number(value.get("group_elapsed_ms"), "group_elapsed_ms")
    throughput = positive_number(
        value.get("throughput_queries_per_second"), "throughput_queries_per_second"
    )
    wanted_throughput = expected["concurrency"] * 1000.0 / group_elapsed
    if not math.isclose(throughput, wanted_throughput, rel_tol=1e-6):
        raise RuntimeError(f"RustDB run {run_id!r} has inconsistent throughput")
    baseline = positive_integer(value.get("rss_baseline_bytes"), "rss_baseline_bytes")
    peak = positive_integer(value.get("peak_rss_bytes"), "peak_rss_bytes")
    if peak < baseline or peak > expected["memory_limit_bytes"]:
        raise RuntimeError(f"RustDB run {run_id!r} exceeded its RSS limit")

    queries = value.get("queries")
    if not isinstance(queries, list) or len(queries) != expected["concurrency"]:
        raise RuntimeError(f"RustDB run {run_id!r} has an invalid query count")
    checksums: set[str] = set()
    for index, query in enumerate(queries):
        label = f"RustDB run {run_id!r} query {index}"
        if not isinstance(query, dict) or query.get("complete") is not True:
            raise RuntimeError(f"{label} was not fully consumed")
        if query.get("current_reservation_bytes") != 0:
            raise RuntimeError(f"{label} retained an engine reservation")
        peak_reservation = nonnegative_integer(
            query.get("peak_reservation_bytes"), "peak_reservation_bytes"
        )
        if peak_reservation > expected["memory_limit_bytes"]:
            raise RuntimeError(f"{label} exceeded its reservation limit")
        elapsed = positive_number(query.get("elapsed_ms"), "elapsed_ms")
        ttfb = positive_number(query.get("ttfb_ms"), "ttfb_ms")
        if ttfb > elapsed * 1.01 or elapsed > group_elapsed * 1.01:
            raise RuntimeError(f"{label} has inconsistent elapsed time")
        nonnegative_integer(query.get("rows"), "rows")
        nonnegative_integer(query.get("batches"), "batches")
        if query.get("checksum_mode") != CURRENT_CHECKSUM_MODE:
            raise RuntimeError(f"{label} used an unexpected checksum mode")
        if query.get("checksum_backend") != CHECKSUM_BACKENDS["rustdb"]:
            raise RuntimeError(f"{label} used an unexpected checksum backend")
        nonnegative_number(query.get("checksum_compute_ms"), "checksum_compute_ms")
        checksum = query.get("checksum")
        if not valid_sha(checksum):
            raise RuntimeError(f"{label} has an invalid checksum")
        checksums.add(checksum)
    if len(checksums) != 1:
        raise RuntimeError(f"RustDB run {run_id!r} returned inconsistent checksums")
    return checksums.pop()


def summary(runs: list[dict[str, Any]], memory_limit: int) -> dict[str, Any]:
    queries = [query for run in runs for query in run["queries"]]
    checksums = {query["checksum"] for query in queries}
    if len(checksums) != 1:
        raise RuntimeError("measured RustDB runs have inconsistent checksums")
    peak_rss = max(run["peak_rss_bytes"] for run in runs)
    return {
        "iterations": len(runs),
        "measured_queries": len(queries),
        "checksum": checksums.pop(),
        "p50_elapsed_ms": statistics.median(
            run["group_elapsed_ms"] for run in runs
        ),
        "p50_query_elapsed_ms": statistics.median(
            query["elapsed_ms"] for query in queries
        ),
        "p50_ttfb_ms": statistics.median(query["ttfb_ms"] for query in queries),
        "mean_throughput_queries_per_second": statistics.mean(
            run["throughput_queries_per_second"] for run in runs
        ),
        "peak_rss_bytes": peak_rss,
        "peak_rss_delta_bytes": max(
            run["peak_rss_bytes"] - run["rss_baseline_bytes"] for run in runs
        ),
        "memory_headroom_bytes": memory_limit - peak_rss,
        "terminal_reservation_bytes": max(
            query["current_reservation_bytes"] for query in queries
        ),
    }


def make_report(
    args: argparse.Namespace,
    hello: dict[str, Any],
    warmup_runs: list[dict[str, Any]],
    runs: list[dict[str, Any]],
    host: dict[str, Any],
    dataset: dict[str, Any],
    query: dict[str, Any],
) -> dict[str, Any]:
    expected = config(args)
    return {
        "schema": SCHEMA,
        "diagnostic_only": True,
        "comparison_gate_eligible": False,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "host": host,
        "storage_track": args.storage_track,
        "storage_medium": args.storage_medium,
        "warmup": args.warmup,
        "iterations": args.iterations,
        "config": expected,
        "cache_state": hello["cache_state"],
        "dataset": dataset,
        "query": query,
        "hello": hello,
        "warmup_runs": warmup_runs,
        "runs": runs,
        "summary": summary(runs, expected["memory_limit_bytes"]),
    }


def validate_report(value: dict[str, Any]) -> None:
    if (
        value.get("schema") != SCHEMA
        or value.get("diagnostic_only") is not True
        or value.get("comparison_gate_eligible") is not False
    ):
        raise RuntimeError("external-only report is not clearly diagnostic-only")
    expected = value.get("config")
    if not isinstance(expected, dict):
        raise RuntimeError("external-only report omitted its configuration")
    for key in ("threads", "memory_limit_bytes", "concurrency", "batch_size"):
        positive_integer(expected.get(key), f"config.{key}")
    validate_hello(value.get("hello", {}), expected)
    if value.get("cache_state") != value["hello"]["cache_state"]:
        raise RuntimeError("external-only report has inconsistent cache state")
    storage_track = value.get("storage_track")
    if storage_track not in ("csv", "parquet"):
        raise RuntimeError("external-only report has an invalid storage track")
    if value.get("storage_medium") not in ("local-nvme", "minio"):
        raise RuntimeError("external-only report has an invalid storage medium")
    warmup = nonnegative_integer(value.get("warmup"), "warmup")
    iterations = positive_integer(value.get("iterations"), "iterations")
    warmup_runs = value.get("warmup_runs")
    runs = value.get("runs")
    if not isinstance(warmup_runs, list) or len(warmup_runs) != warmup:
        raise RuntimeError("external-only report has an invalid warmup count")
    if not isinstance(runs, list) or len(runs) != iterations:
        raise RuntimeError("external-only report has an invalid iteration count")
    checksums = set()
    for index, run in enumerate(warmup_runs):
        checksums.add(
            validate_run(
                run, value["hello"], expected, storage_track, f"warmup-{index}-rustdb"
            )
        )
    for index, run in enumerate(runs):
        checksums.add(
            validate_run(
                run,
                value["hello"],
                expected,
                storage_track,
                f"measured-{index}-rustdb",
            )
        )
    if len(checksums) != 1:
        raise RuntimeError("RustDB checksum changed across diagnostic rounds")
    if value.get("summary") != summary(runs, expected["memory_limit_bytes"]):
        raise RuntimeError("external-only report summary does not match its runs")


def valid_sha(value: Any) -> bool:
    return isinstance(value, str) and SHA256.fullmatch(value) is not None


def positive_integer(value: Any, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise RuntimeError(f"{label} must be a positive integer")
    return value


def nonnegative_integer(value: Any, label: str) -> int:
    if type(value) is not int or value < 0:
        raise RuntimeError(f"{label} must be a non-negative integer")
    return value


def positive_number(value: Any, label: str) -> float:
    if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
        raise RuntimeError(f"{label} must be positive and finite")
    return float(value)


def nonnegative_number(value: Any, label: str) -> float:
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        raise RuntimeError(f"{label} must be non-negative and finite")
    return float(value)


def main() -> int:
    args = arguments()
    expected = config(args)
    sql = args.query.read_text(encoding="utf-8")
    dataset = path_facts(args.dataset)
    query = file_facts(args.query)
    rustdb_command = command(args.rustdb_command_json, "rustdb")
    worker = Worker("rustdb", rustdb_command)
    warmup_runs: list[dict[str, Any]] = []
    runs: list[dict[str, Any]] = []
    try:
        validate_hello(worker.hello, expected)
        for index in range(args.warmup):
            run_id = f"warmup-{index}-rustdb"
            response = worker.run(run_command(run_id, sql, args.storage_track, 0))
            validate_run(response, worker.hello, expected, args.storage_track, run_id)
            warmup_runs.append(response)
        for index in range(args.iterations):
            run_id = f"measured-{index}-rustdb"
            response = worker.run(run_command(run_id, sql, args.storage_track, 0))
            validate_run(response, worker.hello, expected, args.storage_track, run_id)
            runs.append(response)
    finally:
        worker.close()

    report = make_report(
        args,
        worker.hello,
        warmup_runs,
        runs,
        collect_host_facts(),
        dataset,
        query,
    )
    validate_report(report)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.output.with_suffix(args.output.suffix + ".tmp")
    temporary.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, args.output)
    print(args.output)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
