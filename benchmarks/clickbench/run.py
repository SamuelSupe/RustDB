#!/usr/bin/env python3
"""Run the official ClickBench query set once and retain per-query evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCHEMA = "rustdb-clickbench-v1"
EXPECTED_QUERIES = 43
CLICKBENCH_TEXT_COLUMNS = (
    "MobilePhoneModel",
    "SearchPhrase",
    "Referer",
    "Title",
    "URL",
)


def positive(value: str) -> int:
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run one functional ClickBench pass with RustDB"
    )
    parser.add_argument("--queries", type=Path, required=True)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument(
        "--dataset-profile", choices=("functional", "full"), default="functional"
    )
    parser.add_argument("--data-etag")
    parser.add_argument(
        "--binary-as-string",
        action="store_true",
        help="adapt historical partitioned ClickBench Binary text columns",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--threads", type=positive, default=4)
    parser.add_argument("--memory-limit", type=positive, default=12 * 1024**3)
    parser.add_argument("--batch-size", type=positive, default=8192)
    parser.add_argument("--io-concurrency", type=positive, default=16)
    parser.add_argument("--metadata-cache-bytes", type=int, default=256 * 1024**2)
    parser.add_argument("--timeout-seconds", type=positive, default=3600)
    parser.add_argument(
        "--mode",
        choices=("execute", "explain"),
        default="execute",
        help="EXPLAIN is a quick binder/planner preflight; execute is the acceptance run",
    )
    parser.add_argument("--build-id", default="unrecorded")
    parser.add_argument("--rustc-version", default="unrecorded")
    parser.add_argument("--cpu-model")
    return parser.parse_args()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def load_queries(path: Path) -> list[str]:
    queries = [line.strip() for line in path.read_text(encoding="utf-8").splitlines()]
    queries = [query for query in queries if query and not query.startswith("--")]
    if len(queries) != EXPECTED_QUERIES:
        raise RuntimeError(
            f"expected {EXPECTED_QUERIES} ClickBench queries, found {len(queries)}"
        )
    if any(not query.endswith(";") for query in queries):
        raise RuntimeError("every ClickBench query must be one semicolon-terminated line")
    return queries


def render(query: str, data: Path, mode: str, binary_as_string: bool) -> str:
    source = f"read_parquet('{data.as_posix()}')"
    rendered, replacements = re.subn(
        r"\bFROM\s+hits\b", f"FROM {source}", query, flags=re.IGNORECASE
    )
    if replacements == 0:
        raise RuntimeError(f"query does not contain FROM hits: {query}")
    # The official fixture intentionally omits logical annotations. Match the
    # official ClickBench DataFusion adapter: EventDate stores epoch days and
    # EventTime stores epoch seconds.
    rendered = re.sub(
        r"\bEventDate\b", "CAST(EventDate AS DATE)", rendered, flags=re.IGNORECASE
    )
    rendered = re.sub(
        r"\bEventTime\b",
        "to_timestamp_seconds(EventTime)",
        rendered,
        flags=re.IGNORECASE,
    )
    if binary_as_string:
        for column in CLICKBENCH_TEXT_COLUMNS:
            rendered = re.sub(
                rf"\b{column}\b",
                f"CAST({column} AS VARCHAR)",
                rendered,
                flags=re.IGNORECASE,
            )
    if mode == "explain":
        rendered = f"EXPLAIN {rendered}"
    return rendered + "\n"


def cgroup_value(path: str) -> str | None:
    try:
        return Path(path).read_text(encoding="utf-8").strip()
    except OSError:
        return None


def write_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary, path)


def terminate(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGINT)
        process.wait(timeout=30)
    except (ProcessLookupError, subprocess.TimeoutExpired):
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()


def run_query(args: argparse.Namespace, number: int, sql_path: Path) -> dict[str, Any]:
    reports = args.output / "reports"
    logs = args.output / "logs"
    report_path = reports / f"q{number:02d}.json"
    stderr_path = logs / f"q{number:02d}.stderr.log"
    spill_path = args.output / "spill"
    command = [
        str(args.binary),
        "--query",
        str(sql_path),
        "--warmup",
        "0",
        "--iterations",
        "1",
        "--memory-limit",
        str(args.memory_limit),
        "--threads",
        str(args.threads),
        "--batch-size",
        str(args.batch_size),
        "--io-concurrency",
        str(args.io_concurrency),
        "--metadata-cache-bytes",
        str(args.metadata_cache_bytes),
        "--spill-directory",
        str(spill_path),
        "--build-id",
        args.build_id,
        "--build-profile",
        "release",
        "--build-rustflags=-C target-cpu=native",
        "--rustc-version",
        args.rustc_version,
    ]
    if args.cpu_model:
        command.extend(("--cpu-model", args.cpu_model))

    started = time.monotonic()
    timed_out = False
    with report_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
        process = subprocess.Popen(
            command,
            stdout=stdout,
            stderr=stderr,
            start_new_session=True,
        )
        try:
            return_code = process.wait(timeout=args.timeout_seconds)
        except subprocess.TimeoutExpired:
            timed_out = True
            terminate(process)
            return_code = process.returncode

    elapsed = time.monotonic() - started
    report: dict[str, Any] | None = None
    parse_error: str | None = None
    if return_code == 0:
        try:
            report = json.loads(report_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            parse_error = str(error)

    run = report["runs"][0] if report and report.get("runs") else None
    cleanup_error: str | None = None
    if run is not None:
        if run.get("current_memory_bytes") != 0:
            cleanup_error = "query memory reservation did not return to zero"
        elif run.get("engine_current_reservation_bytes") != 0:
            cleanup_error = "engine reservation did not return to zero"
        elif run.get("spill_cleaned") is not True:
            cleanup_error = "query Spill state was not cleaned"
    status = (
        "passed"
        if return_code == 0 and report is not None and run is not None and cleanup_error is None
        else "failed"
    )
    return {
        "query": number,
        "status": status,
        "exit_code": return_code,
        "timed_out": timed_out,
        "wall_elapsed_seconds": elapsed,
        "rendered_sql": str(sql_path),
        "report": str(report_path),
        "stderr": str(stderr_path),
        "report_parse_error": parse_error,
        "cleanup_error": cleanup_error,
        "result_rows": run["rows"] if run else None,
        "engine_elapsed_ms": run["elapsed_ms"] if run else None,
        "result_checksum_sha256": report.get("result_checksum_sha256") if report else None,
        "peak_engine_reservation_bytes": (
            run.get("engine_peak_reservation_bytes") if run else None
        ),
        "process_peak_rss_bytes": run.get("process_peak_rss_bytes") if run else None,
        "peak_active_lanes": run.get("peak_active_lanes") if run else None,
        "terminal_query_reservation_bytes": (
            run.get("current_memory_bytes") if run else None
        ),
        "terminal_engine_reservation_bytes": (
            run.get("engine_current_reservation_bytes") if run else None
        ),
        "spill_write_bytes": run.get("spill_write_bytes") if run else None,
        "spill_cleaned": run.get("spill_cleaned") if run else None,
    }


def main() -> int:
    args = arguments()
    if args.metadata_cache_bytes < 0:
        raise RuntimeError("metadata cache bytes cannot be negative")
    if not args.binary.is_file() or not os.access(args.binary, os.X_OK):
        raise RuntimeError(f"benchmark binary is not executable: {args.binary}")
    if not args.data.is_file():
        raise RuntimeError(f"ClickBench data file does not exist: {args.data}")
    args.output.mkdir(parents=True, exist_ok=False)
    for directory in ("rendered", "reports", "logs", "spill"):
        (args.output / directory).mkdir()

    queries = load_queries(args.queries)
    manifest = {
        "schema": SCHEMA,
        "complete": False,
        "mode": args.mode,
        "binary_as_string": args.binary_as_string,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "query_count": len(queries),
        "queries": {
            "path": str(args.queries),
            "sha256": sha256(args.queries),
        },
        "dataset": {
            "profile": args.dataset_profile,
            "path": str(args.data),
            "bytes": args.data.stat().st_size,
            "etag": args.data_etag,
        },
        "resource_contract": {
            "container_cpus": cgroup_value("/sys/fs/cgroup/cpu.max"),
            "container_memory_bytes": cgroup_value("/sys/fs/cgroup/memory.max"),
            "engine_threads": args.threads,
            "engine_memory_limit_bytes": args.memory_limit,
            "batch_size": args.batch_size,
            "io_concurrency": args.io_concurrency,
            "metadata_cache_bytes": args.metadata_cache_bytes,
        },
        "build": {
            "id": args.build_id,
            "binary": str(args.binary),
            "binary_sha256": sha256(args.binary),
            "rustc_version": args.rustc_version,
            "rustflags": "-C target-cpu=native",
        },
        "results": [],
    }
    write_json(args.output / "manifest.json", manifest)

    for index, query in enumerate(queries, start=1):
        sql_path = args.output / "rendered" / f"q{index:02d}.sql"
        sql_path.write_text(
            render(query, args.data, args.mode, args.binary_as_string), encoding="utf-8"
        )
        print(
            f"clickbench: q{index:02d}/{len(queries)} {args.mode} started",
            file=sys.stderr,
            flush=True,
        )
        result = run_query(args, index, sql_path)
        manifest["results"].append(result)
        write_json(args.output / "manifest.json", manifest)
        print(
            f"clickbench: q{index:02d} {result['status']} "
            f"({result['wall_elapsed_seconds']:.3f}s)",
            file=sys.stderr,
            flush=True,
        )

    passed = sum(result["status"] == "passed" for result in manifest["results"])
    manifest["finished_at"] = datetime.now(timezone.utc).isoformat()
    manifest["passed"] = passed
    manifest["failed"] = len(queries) - passed
    manifest["complete"] = passed == len(queries)
    manifest["acceptance_summary"] = {
        "max_peak_engine_reservation_bytes": max(
            result["peak_engine_reservation_bytes"] or 0
            for result in manifest["results"]
        ),
        "max_process_peak_rss_bytes": max(
            result["process_peak_rss_bytes"] or 0 for result in manifest["results"]
        ),
        "max_peak_active_lanes": max(
            result["peak_active_lanes"] or 0 for result in manifest["results"]
        ),
        "total_engine_elapsed_ms": sum(
            result["engine_elapsed_ms"] or 0 for result in manifest["results"]
        ),
        "total_spill_write_bytes": sum(
            result["spill_write_bytes"] or 0 for result in manifest["results"]
        ),
        "all_terminal_query_reservations_zero": all(
            result["terminal_query_reservation_bytes"] == 0
            for result in manifest["results"]
        ),
        "all_terminal_engine_reservations_zero": all(
            result["terminal_engine_reservation_bytes"] == 0
            for result in manifest["results"]
        ),
        "all_spill_cleaned": all(
            result["spill_cleaned"] is True for result in manifest["results"]
        ),
    }
    write_json(args.output / "manifest.json", manifest)
    print(
        f"clickbench: {passed}/{len(queries)} queries passed; "
        f"manifest={args.output / 'manifest.json'}",
        file=sys.stderr,
    )
    return 0 if manifest["complete"] else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # Keep a durable error in container logs.
        print(f"clickbench: error: {error}", file=sys.stderr)
        raise SystemExit(2)
