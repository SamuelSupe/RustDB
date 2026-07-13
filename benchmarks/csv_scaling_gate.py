#!/usr/bin/env python3
"""Validate RustDB single-file CSV scaling evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import statistics
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


RELEASE_ENGINE_VERSION = "0.5.0-alpha.1"
RELEASE_SOURCE_BYTES = 10 * 1024**3
RELEASE_WARMUP = 2
RELEASE_ITERATIONS = 5
RELEASE_MEMORY_BYTES = 1024**3
RELEASE_BATCH_SIZE = 8192
RELEASE_IO_CONCURRENCY = 32
RELEASE_METADATA_CACHE_BYTES = 0
RELEASE_MORSEL_BYTES = 8 * 1024**2
RELEASE_MINIMUM_SPEEDUP = 1.8
RELEASE_RUSTFLAGS = "-C target-cpu=native"
COMMIT = re.compile(r"^[0-9a-f]{40}$")
SHA256 = re.compile(r"^[0-9a-f]{64}$")


class GateError(ValueError):
    pass


@dataclass(frozen=True)
class ReleaseContext:
    build_id: str
    binary_sha256: str
    cpu_model: str


def fail(message: str) -> None:
    raise GateError(message)


def positive_int(value: Any, label: str) -> int:
    if type(value) is not int or value <= 0:
        fail(f"{label} must be a positive integer, got {value!r}")
    return value


def source_facts(path: Path, release: bool) -> tuple[int, int]:
    try:
        size = path.stat().st_size
        with path.open("rb") as source:
            header = source.read(11)
            source.seek(-64, 2)
            tail = source.read(64)
    except (OSError, ValueError) as error:
        fail(f"cannot inspect CSV source {path}: {error}")
    if header != b"id,payload\n" or tail != b"1," + b"x" * 61 + b"\n":
        fail("CSV source does not use the deterministic 64-byte record fixture")
    data_bytes = size - len(header)
    if data_bytes <= 0 or data_bytes % 64:
        fail("CSV source does not contain complete deterministic records")
    if release and not RELEASE_SOURCE_BYTES <= size < RELEASE_SOURCE_BYTES + 64:
        fail(f"release CSV source must be the fixed 10 GiB fixture, got {size} bytes")
    return size, data_bytes // 64


def load_report(path: Path, threads: int, iterations: int) -> dict[str, Any]:
    label = f"threads-{threads}"
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        fail(f"cannot read {label} report {path}: {error}")
    if not isinstance(report, dict):
        fail(f"{label} report must be a JSON object")
    config = report.get("config")
    if not isinstance(config, dict):
        fail(f"{label}.config must be an object")
    if type(config.get("compute_threads")) is not int or config.get("compute_threads") != threads:
        fail(f"{label}.config.compute_threads must be {threads}")
    if config.get("csv_parallel_single_file") is not True:
        fail(f"{label}.config.csv_parallel_single_file must be true")
    if report.get("iterations") != iterations:
        fail(f"{label}.iterations must be {iterations}")
    runs = report.get("runs")
    if not isinstance(runs, list) or len(runs) != iterations:
        fail(f"{label}.runs must contain exactly {iterations} measured runs")
    p50 = report.get("p50_ms")
    elapsed = []
    for index, run in enumerate(runs):
        value = run.get("elapsed_ms") if isinstance(run, dict) else None
        if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
            fail(f"{label}.runs[{index}].elapsed_ms must be positive and finite")
        elapsed.append(float(value))
    if type(p50) not in (int, float) or not math.isfinite(p50) or p50 <= 0:
        fail(f"{label}.p50_ms must be positive and finite")
    if not math.isclose(float(p50), statistics.median(elapsed), abs_tol=1e-6):
        fail(f"{label}.p50_ms does not equal the median elapsed_ms")
    return report


def validate_release_report(
    report: dict[str, Any], label: str, context: ReleaseContext
) -> None:
    expected_config = {
        "memory_limit_bytes": RELEASE_MEMORY_BYTES,
        "batch_size": RELEASE_BATCH_SIZE,
        "io_concurrency": RELEASE_IO_CONCURRENCY,
        "metadata_cache_bytes": RELEASE_METADATA_CACHE_BYTES,
        "csv_target_morsel_bytes": RELEASE_MORSEL_BYTES,
    }
    if report.get("engine_version") != RELEASE_ENGINE_VERSION:
        fail(f"{label}.engine_version must be {RELEASE_ENGINE_VERSION}")
    if report.get("build_id") != context.build_id:
        fail(f"{label}.build_id does not match the clean candidate commit")
    if report.get("binary_sha256") != context.binary_sha256:
        fail(f"{label}.binary_sha256 does not match the candidate executable")
    build = report.get("build")
    if not isinstance(build, dict):
        fail(f"{label}.build must be an object")
    if build.get("cargo_profile") != "release":
        fail(f"{label}.build.cargo_profile must be release")
    if build.get("rustflags") != RELEASE_RUSTFLAGS:
        fail(f"{label}.build.rustflags must be {RELEASE_RUSTFLAGS!r}")
    rustc = build.get("rustc_version")
    if not isinstance(rustc, str) or not rustc.startswith("rustc "):
        fail(f"{label}.build.rustc_version is not a recorded rustc version")
    environment = report.get("environment")
    if not isinstance(environment, dict) or environment.get("cpu_model") != context.cpu_model:
        fail(f"{label}.environment.cpu_model does not match the fixed host")
    config = report["config"]
    for field, expected in expected_config.items():
        if type(config.get(field)) is not int or config.get(field) != expected:
            fail(f"{label}.config.{field} must be {expected}")


def run_signature(
    run: Any, *, label: str, expected_rows: int, source_bytes: int
) -> tuple[int, int, int, int]:
    if not isinstance(run, dict):
        fail(f"{label} must be an object")
    values = tuple(
        positive_int(run.get(field), f"{label}.{field}")
        for field in ("rows", "batches", "csv_source_bytes", "csv_decompressed_bytes")
    )
    rows, _, read_source_bytes, decompressed_bytes = values
    if rows != expected_rows:
        fail(f"{label}.rows: expected {expected_rows}, got {rows}")
    scanned_rows = positive_int(run.get("scanned_rows"), f"{label}.scanned_rows")
    if scanned_rows != expected_rows:
        fail(f"{label}.scanned_rows: expected {expected_rows}, got {scanned_rows}")
    if read_source_bytes < source_bytes:
        fail(f"{label}.csv_source_bytes did not cover the complete {source_bytes}-byte source")
    if decompressed_bytes < source_bytes:
        fail(f"{label}.csv_decompressed_bytes did not cover the complete source")
    return values


def report_signature(
    report: dict[str, Any], *, label: str, expected_rows: int, source_bytes: int
) -> tuple[int, int, int, int]:
    signatures = {
        run_signature(
            run,
            label=f"{label}.runs[{index}]",
            expected_rows=expected_rows,
            source_bytes=source_bytes,
        )
        for index, run in enumerate(report["runs"])
    }
    if len(signatures) != 1:
        fail(f"{label} row/batch/byte metrics differ between measured runs")
    return signatures.pop()


def evaluate(
    one_path: Path,
    four_path: Path,
    *,
    mode: str,
    minimum_speedup: float,
    source_bytes: int,
    expected_rows: int,
    release_context: ReleaseContext | None = None,
) -> dict[str, Any]:
    release = mode == "release"
    if mode not in ("release", "smoke"):
        fail(f"unknown gate mode {mode!r}")
    if release:
        minimum_speedup = RELEASE_MINIMUM_SPEEDUP
        iterations = RELEASE_ITERATIONS
        if release_context is None:
            fail("release gate requires independently verified candidate context")
        if not COMMIT.fullmatch(release_context.build_id):
            fail("release candidate build ID must be an exact 40-character commit")
        if not SHA256.fullmatch(release_context.binary_sha256):
            fail("release candidate executable SHA-256 is invalid")
        if "M5 Max" not in release_context.cpu_model:
            fail("release gate requires an Apple M5 Max")
    else:
        iterations = positive_int(json.loads(one_path.read_text())["iterations"], "iterations")
    if not math.isfinite(minimum_speedup) or minimum_speedup <= 0:
        fail("minimum speedup must be positive and finite")
    positive_int(source_bytes, "source_bytes")
    positive_int(expected_rows, "expected_rows")
    one = load_report(one_path, 1, iterations)
    four = load_report(four_path, 4, iterations)
    if release:
        assert release_context is not None
        for label, report in (("threads-1", one), ("threads-4", four)):
            validate_release_report(report, label, release_context)
            if report.get("warmup") != RELEASE_WARMUP:
                fail(f"{label}.warmup must be {RELEASE_WARMUP}")
        if one["build"] != four["build"]:
            fail("one-thread and four-thread build provenance differs")
    one_signature = report_signature(
        one, label="threads-1", expected_rows=expected_rows, source_bytes=source_bytes
    )
    four_signature = report_signature(
        four, label="threads-4", expected_rows=expected_rows, source_bytes=source_bytes
    )
    if one_signature != four_signature:
        fail("one-thread and four-thread row/batch/byte metrics differ")
    parser_lanes = max(
        positive_int(run.get("peak_csv_parser_lanes"), f"threads-4.runs[{index}].peak_csv_parser_lanes")
        for index, run in enumerate(four["runs"])
    )
    if release and parser_lanes < 2:
        fail("four-thread CSV run never observed two active parser lanes")
    speedup = float(one["p50_ms"]) / float(four["p50_ms"])
    if speedup < minimum_speedup:
        fail(f"CSV four-thread speedup {speedup:.3f} is below required {minimum_speedup:.3f}")
    rows, batches, read_source_bytes, decompressed_bytes = one_signature
    return {
        "gate": "csv-single-file-scaling",
        "mode": mode,
        "release_qualified": release,
        "source_bytes": source_bytes,
        "expected_rows": expected_rows,
        "rows": rows,
        "batches": batches,
        "csv_source_bytes": read_source_bytes,
        "csv_decompressed_bytes": decompressed_bytes,
        "threads_1_p50_ms": one["p50_ms"],
        "threads_4_p50_ms": four["p50_ms"],
        "peak_four_thread_parser_lanes": parser_lanes,
        "speedup": speedup,
        "minimum_speedup": minimum_speedup,
    }


def release_context(root: Path) -> ReleaseContext:
    try:
        build_id = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=root, check=True, capture_output=True, text=True
        ).stdout.strip()
        dirty = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=normal"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError) as error:
        fail(f"cannot inspect candidate Git state: {error}")
    if not COMMIT.fullmatch(build_id) or dirty:
        fail("release gate requires a clean candidate at an exact 40-character commit")
    cpu_model = subprocess.run(
        ["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True
    ).stdout.strip()
    if "M5 Max" not in cpu_model:
        fail(f"release gate requires an Apple M5 Max, detected {cpu_model!r}")
    binary = root / "target/release/rustdb-bench"
    try:
        digest = hashlib.sha256(binary.read_bytes()).hexdigest()
    except OSError as error:
        fail(f"cannot hash candidate benchmark executable: {error}")
    if not SHA256.fullmatch(digest):
        fail("candidate benchmark executable SHA-256 is invalid")
    return ReleaseContext(build_id, digest, cpu_model)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--one", type=Path, required=True)
    parser.add_argument("--four", type=Path, required=True)
    parser.add_argument("--source-file", type=Path, required=True)
    parser.add_argument("--mode", choices=("release", "smoke"), required=True)
    parser.add_argument("--minimum-speedup", type=float)
    args = parser.parse_args()
    try:
        release = args.mode == "release"
        if release and args.minimum_speedup is not None:
            fail("release minimum speedup is fixed and cannot be overridden")
        minimum = (
            args.minimum_speedup
            if args.minimum_speedup is not None
            else RELEASE_MINIMUM_SPEEDUP
        )
        source_bytes, expected_rows = source_facts(args.source_file, release)
        context = release_context(Path(__file__).resolve().parent.parent) if release else None
        result = evaluate(
            args.one,
            args.four,
            mode=args.mode,
            minimum_speedup=minimum,
            source_bytes=source_bytes,
            expected_rows=expected_rows,
            release_context=context,
        )
    except (GateError, OSError, json.JSONDecodeError, KeyError) as error:
        print(f"CSV scaling gate: FAIL: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
