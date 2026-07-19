"""Strict validation for the retained ClickBench acceptance reports."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from beta_acceptance_common import read_json


CONTAINER_CPUS = 4
CONTAINER_MEMORY_BYTES = 12 * 1024**3
ENGINE_THREADS = 4
ENGINE_MEMORY_BYTES = 4 * 1024**3
BATCH_SIZE = 8192
IO_CONCURRENCY = 16
METADATA_CACHE_BYTES = 256 * 1024**2
REPORT_COUNT = 43

ENGINE_CONFIG = {
    "compute_threads": ENGINE_THREADS,
    "memory_limit_bytes": ENGINE_MEMORY_BYTES,
    "batch_size": BATCH_SIZE,
    "io_concurrency": IO_CONCURRENCY,
    "metadata_cache_bytes": METADATA_CACHE_BYTES,
}


def validate_resource_contract(resource: Any) -> None:
    if not isinstance(resource, dict):
        raise ValueError("ClickBench resource contract must be an object")
    cpu_max = resource.get("container_cpus")
    match = re.fullmatch(r"([1-9][0-9]*) ([1-9][0-9]*)", cpu_max or "")
    if match is None:
        raise ValueError("ClickBench cgroup CPU limit is missing or malformed")
    quota, period = (int(value) for value in match.groups())
    if quota != CONTAINER_CPUS * period:
        raise ValueError("ClickBench cgroup CPU limit is not exactly four CPUs")
    if resource.get("container_memory_bytes") != str(CONTAINER_MEMORY_BYTES):
        raise ValueError("ClickBench cgroup memory limit is not exactly 12 GiB")
    expected = {
        "engine_threads": ENGINE_THREADS,
        "engine_memory_limit_bytes": ENGINE_MEMORY_BYTES,
        "batch_size": BATCH_SIZE,
        "io_concurrency": IO_CONCURRENCY,
        "metadata_cache_bytes": METADATA_CACHE_BYTES,
    }
    if any(resource.get(field) != value for field, value in expected.items()):
        raise ValueError("ClickBench used a different Engine execution profile")


def validate_raw_reports(
    manifest_path: Path,
    results: list[dict[str, Any]],
    commit: str,
    binary_sha256: str,
    checksum_algorithm: str,
) -> dict[str, int]:
    reports = manifest_path.parent / "reports"
    expected_names = [f"q{number:02d}.json" for number in range(1, REPORT_COUNT + 1)]
    actual_names = sorted(path.name for path in reports.glob("q*.json"))
    if actual_names != expected_names:
        raise ValueError("ClickBench raw report inventory is incomplete or contains extras")
    engine_peaks: list[int] = []
    rss_peaks: list[int] = []
    lane_peaks: list[int] = []
    for number, (name, result) in enumerate(zip(expected_names, results), start=1):
        raw = read_json(reports / name)
        config = raw.get("config")
        if not isinstance(config, dict) or any(
            config.get(field) != value for field, value in ENGINE_CONFIG.items()
        ):
            raise ValueError(f"ClickBench raw report {number} has a different Engine config")
        if (
            raw.get("build_id") != commit
            or raw.get("binary_sha256") != binary_sha256
            or raw.get("warmup") != 0
            or raw.get("iterations") != 1
            or raw.get("checksum_algorithm") != checksum_algorithm
            or raw.get("result_checksum_sha256")
            != result.get("result_checksum_sha256")
            or raw.get("query_file") != result.get("rendered_sql")
            or Path(str(result.get("report", ""))).name != name
        ):
            raise ValueError(f"ClickBench raw report {number} differs from its manifest")
        runs = raw.get("runs")
        if not isinstance(runs, list) or len(runs) != 1 or not isinstance(runs[0], dict):
            raise ValueError(f"ClickBench raw report {number} has an invalid run count")
        run = runs[0]
        engine_peak = run.get("engine_peak_reservation_bytes")
        rss_peak = run.get("process_peak_rss_bytes")
        lane_peak = run.get("peak_active_lanes")
        if (
            type(engine_peak) is not int
            or not 0 <= engine_peak <= ENGINE_MEMORY_BYTES
            or type(rss_peak) is not int
            or not 0 <= rss_peak <= CONTAINER_MEMORY_BYTES
            or type(lane_peak) is not int
            or not 0 <= lane_peak <= ENGINE_THREADS
        ):
            raise ValueError(f"ClickBench raw report {number} exceeded its resource peaks")
        if (
            result.get("exit_code") != 0
            or result.get("timed_out") is not False
            or result.get("report_parse_error") is not None
            or result.get("cleanup_error") is not None
            or run.get("rows") != result.get("result_rows")
            or run.get("result_checksum_sha256")
            != result.get("result_checksum_sha256")
            or engine_peak != result.get("peak_engine_reservation_bytes")
            or rss_peak != result.get("process_peak_rss_bytes")
            or lane_peak != result.get("peak_active_lanes")
            or run.get("current_memory_bytes") != 0
            or run.get("engine_current_reservation_bytes") != 0
            or run.get("spill_cleaned") is not True
        ):
            raise ValueError(f"ClickBench raw report {number} has inconsistent results or cleanup")
        engine_peaks.append(engine_peak)
        rss_peaks.append(rss_peak)
        lane_peaks.append(lane_peak)
    return {
        "validated_raw_reports": REPORT_COUNT,
        "max_peak_engine_reservation_bytes": max(engine_peaks),
        "max_process_peak_rss_bytes": max(rss_peaks),
        "max_peak_active_lanes": max(lane_peaks),
    }
