#!/usr/bin/env python3
"""Run a bounded Native diagnostic without producing comparison-gate evidence."""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from coordinator import (
    NATIVE_ROUNDS,
    Worker,
    command,
    file_facts,
    load_native_manifest,
    native_setup,
    native_storage_maximum,
    run_command,
    validate_setup_response,
    verify_native_sources,
)
from host_facts import collect as collect_host_facts


SCHEMA = "rustdb-only-diagnostic-v1"


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run ten post-import RustDB Native diagnostic rounds"
    )
    parser.add_argument("--query", type=Path, required=True)
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--native-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--threads", type=positive, default=4)
    parser.add_argument("--memory-limit", type=positive, default=2_147_483_648)
    parser.add_argument("--concurrency", type=positive, default=1)
    parser.add_argument("--batch-size", type=positive, default=8192)
    parser.add_argument(
        "--rounds",
        type=positive,
        default=NATIVE_ROUNDS,
        help=f"number of measured rounds (default: {NATIVE_ROUNDS})",
    )
    parser.add_argument("--rustdb-command-json", required=True)
    return parser.parse_args()


def positive(value: str) -> int:
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def summary(
    runs: list[dict[str, Any]], setup: dict[str, Any], storage_limit: int
) -> dict[str, Any]:
    elapsed = [run["group_elapsed_ms"] for run in runs]
    ttfb = [query["ttfb_ms"] for run in runs for query in run["queries"]]
    steady = elapsed[1:] if len(elapsed) > 1 else elapsed
    query_total = sum(elapsed)
    return {
        "rounds": len(runs),
        "p50_elapsed_ms": statistics.median(elapsed),
        "first_post_reopen_elapsed_ms": elapsed[0],
        "steady_state_p50_elapsed_ms": statistics.median(steady),
        "p50_ttfb_ms": statistics.median(ttfb),
        "query_round_total_ms": query_total,
        "load_elapsed_ms": setup["load_elapsed_ms"],
        "amortized_elapsed_ms": (setup["load_elapsed_ms"] + query_total) / len(runs),
        "mean_throughput_queries_per_second": statistics.mean(
            run["throughput_queries_per_second"] for run in runs
        ),
        "peak_rss_bytes": max(run["peak_rss_bytes"] for run in runs),
        "peak_rss_delta_bytes": max(
            run["peak_rss_bytes"] - run["rss_baseline_bytes"] for run in runs
        ),
        "storage_limit_bytes": storage_limit,
        "storage_baseline_bytes": setup["storage_baseline_bytes"],
        "storage_peak_bytes": setup["storage_peak_bytes"],
        "storage_final_bytes": setup["storage_final_bytes"],
    }


def validate_hello(value: dict[str, Any], args: argparse.Namespace) -> None:
    expected = {
        "kind": "hello",
        "engine": "rustdb",
        "threads": args.threads,
        "memory_limit_bytes": args.memory_limit,
        "concurrency": args.concurrency,
        "batch_size": args.batch_size,
    }
    for key, wanted in expected.items():
        if value.get(key) != wanted:
            raise RuntimeError(f"RustDB hello has unexpected {key}: {value.get(key)!r}")
    if value.get("cache_state", {}).get("metadata_cache") != "disabled":
        raise RuntimeError("RustDB-only diagnostic requires a disabled metadata cache")


def validate_setup_limits(
    value: dict[str, Any], args: argparse.Namespace, setup_id: str, table_count: int, limit: int
) -> None:
    validate_setup_response(value, "rustdb", setup_id)
    if value.get("table_count") != table_count:
        raise RuntimeError("RustDB setup imported an unexpected table count")
    baseline = value.get("storage_baseline_bytes")
    final = value.get("storage_final_bytes")
    peak = value.get("storage_peak_bytes")
    if not all(isinstance(item, int) for item in (baseline, final, peak)):
        raise RuntimeError("RustDB setup omitted storage accounting")
    if not baseline <= final <= peak <= limit:
        raise RuntimeError("RustDB setup exceeded the Native workspace limit")
    rss_baseline = value.get("rss_baseline_bytes")
    rss_peak = value.get("peak_rss_bytes")
    if (
        not isinstance(rss_baseline, int)
        or not isinstance(rss_peak, int)
        or not 0 < rss_baseline <= rss_peak <= args.memory_limit
    ):
        raise RuntimeError("RustDB setup exceeded the configured memory limit")


def validate_run(
    value: dict[str, Any],
    args: argparse.Namespace,
    hello: dict[str, Any],
    index: int,
    setup_id: str,
) -> str:
    expected = {
        "kind": "run",
        "run_id": f"measured-{index}-rustdb",
        "engine": "rustdb",
        "version": hello.get("version"),
        "build_id": hello.get("build_id"),
        "threads": args.threads,
        "memory_limit_bytes": args.memory_limit,
        "concurrency": args.concurrency,
        "batch_size": args.batch_size,
        "storage_track": "native",
        "setup_id": setup_id,
        "engine_order": 0,
    }
    for key, wanted in expected.items():
        if value.get(key) != wanted:
            raise RuntimeError(f"RustDB round {index} has unexpected {key}")
    baseline = value.get("rss_baseline_bytes")
    peak = value.get("peak_rss_bytes")
    if not isinstance(baseline, int) or not isinstance(peak, int) or not 0 < baseline <= peak:
        raise RuntimeError(f"RustDB round {index} has invalid RSS accounting")
    if peak > args.memory_limit:
        raise RuntimeError(f"RustDB round {index} exceeded the configured memory limit")
    queries = value.get("queries")
    if not isinstance(queries, list) or len(queries) != args.concurrency:
        raise RuntimeError(f"RustDB round {index} has an invalid query count")
    checksums = set()
    for query in queries:
        if query.get("complete") is not True:
            raise RuntimeError(f"RustDB round {index} contains an incomplete query")
        if query.get("current_reservation_bytes") != 0:
            raise RuntimeError(f"RustDB round {index} retained an engine reservation")
        checksum = query.get("checksum")
        if not isinstance(checksum, str) or len(checksum) != 64:
            raise RuntimeError(f"RustDB round {index} has an invalid checksum")
        checksums.add(checksum)
    if len(checksums) != 1:
        raise RuntimeError(f"RustDB round {index} returned inconsistent checksums")
    return checksums.pop()


def validate_report(value: dict[str, Any], rounds: int) -> None:
    if value.get("schema") != SCHEMA or value.get("diagnostic_only") is not True:
        raise RuntimeError("RustDB-only report is not labeled as diagnostic evidence")
    runs = value.get("runs")
    if not isinstance(runs, list) or len(runs) != rounds:
        raise RuntimeError(f"RustDB-only report requires exactly {rounds} rounds")
    expected_summary = summary(
        runs, value["setup"], value["native_setup"]["max_storage_bytes"]
    )
    if value.get("summary") != expected_summary:
        raise RuntimeError("RustDB-only report summary does not match its runs")


def main() -> int:
    args = arguments()
    rustdb_command = command(args.rustdb_command_json, "rustdb")
    manifest = load_native_manifest(args.native_manifest, args.dataset)
    storage_limit = native_storage_maximum(
        manifest["dataset"]["bytes"], len(manifest["statements"])
    )
    setup_args = SimpleNamespace(
        native_manifest=args.native_manifest,
        dataset=args.dataset,
        max_native_workspace_bytes=storage_limit,
    )
    print("rustdb-only: preparing native workspace", file=sys.stderr, flush=True)
    native = native_setup(setup_args)
    sql = args.query.read_text(encoding="utf-8")

    setup_worker = Worker("rustdb", rustdb_command)
    try:
        validate_hello(setup_worker.hello, args)
        print("rustdb-only: setup started", file=sys.stderr, flush=True)
        verify_native_sources(setup_args, native)
        setup = setup_worker.run(native["command"], timeout=3_600)
        print("rustdb-only: setup finished", file=sys.stderr, flush=True)
        verify_native_sources(setup_args, native)
        validate_setup_limits(
            setup, args, native["setup_id"], len(manifest["statements"]), storage_limit
        )
    finally:
        setup_worker.close()

    worker = Worker("rustdb", rustdb_command)
    runs: list[dict[str, Any]] = []
    checksums = set()
    try:
        validate_hello(worker.hello, args)
        for index in range(args.rounds):
            print(
                f"rustdb-only: round {index + 1}/{args.rounds} started",
                file=sys.stderr,
                flush=True,
            )
            response = worker.run(
                run_command(
                    f"measured-{index}-rustdb", sql, "native", 0, native["setup_id"]
                )
            )
            checksums.add(validate_run(response, args, worker.hello, index, native["setup_id"]))
            runs.append(response)
            print(
                f"rustdb-only: round {index + 1}/{args.rounds} finished",
                file=sys.stderr,
                flush=True,
            )
        verify_native_sources(setup_args, native)
    finally:
        worker.close()
    if len(checksums) != 1:
        raise RuntimeError("RustDB checksums changed between diagnostic rounds")

    native_report = {
        key: value for key, value in native["command"].items() if key != "command"
    }
    native_report["setup_engine"] = "rustdb"
    report = {
        "schema": SCHEMA,
        "diagnostic_only": True,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "host": collect_host_facts(),
        "storage_track": "native",
        "storage_medium": "local-nvme",
        "config": {
            "threads": args.threads,
            "memory_limit_bytes": args.memory_limit,
            "concurrency": args.concurrency,
            "batch_size": args.batch_size,
            "warmup": 0,
            "rounds": args.rounds,
        },
        "dataset": native["dataset"],
        "query": file_facts(args.query),
        "native_setup": native_report,
        "hello": worker.hello,
        "setup": setup,
        "runs": runs,
        "summary": summary(runs, setup, storage_limit),
    }
    validate_report(report, args.rounds)
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
