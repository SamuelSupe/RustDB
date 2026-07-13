"""Validate the complete low-memory evidence consumed by the v0.5 gate."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
MEMORY_LIMITS = (64 * 1024**2, 128 * 1024**2)
LOW_MEMORY_CASES = {
    "sort": ROOT / "benchmarks/suites/low-memory/sort.sql",
    "aggregate": ROOT / "benchmarks/suites/low-memory/aggregate.sql",
    "inner-join": ROOT / "benchmarks/suites/low-memory/inner_join.sql",
    "left-join": ROOT / "benchmarks/suites/low-memory/left_join.sql",
    "right-join": ROOT / "benchmarks/suites/low-memory/right_join.sql",
    "full-join": ROOT / "benchmarks/suites/low-memory/full_join.sql",
    "semi-join": ROOT / "benchmarks/suites/low-memory/semi_join.sql",
    "anti-join": ROOT / "benchmarks/suites/low-memory/anti_join.sql",
    "distinct-set": ROOT / "benchmarks/suites/low-memory/distinct_set.sql",
    "repeat-intersect-all": (
        ROOT / "benchmarks/suites/low-memory/repeat_intersect_all.sql"
    ),
    "repeat-except-all": ROOT / "benchmarks/suites/low-memory/repeat_except_all.sql",
    "window": ROOT / "benchmarks/suites/low-memory/window.sql",
}
JOIN_CASES = (
    "inner-join",
    "left-join",
    "right-join",
    "full-join",
    "semi-join",
    "anti-join",
)
SHA256 = re.compile(r"^[0-9a-f]{64}$")


class LowMemoryManifestError(ValueError):
    """The low-memory manifest is incomplete or internally inconsistent."""


def load_low_memory_manifest(
    manifest_path: Path, join_paths: list[Path]
) -> dict[str, Any]:
    """Load all 24 cases and bind the supplied Join inputs to their entries."""
    document = _load_object(manifest_path, "low-memory manifest")
    _expect(document, "suite", "rustdb-low-memory-v1", "low-memory manifest")
    _expect_true_fields(
        document.get("correctness"),
        "low-memory manifest.correctness",
        ("verified", "memory_limited", "spill_required"),
    )
    _expect_true_fields(
        document.get("assertions"),
        "low-memory manifest.assertions",
        (
            "full_result_consumed",
            "peak_memory_within_limit",
            "spill_required",
            "spill_directories_cleaned",
        ),
    )
    config = document.get("config")
    if not isinstance(config, dict):
        raise LowMemoryManifestError("low-memory manifest.config must be an object")
    for field, expected in (
        ("threads", 4),
        ("batch_size", 8192),
        ("io_concurrency", 32),
        ("warmup", 0),
        ("iterations", 1),
    ):
        _expect(config, field, expected, "low-memory manifest.config")

    runs = document.get("runs")
    expected = {(case, limit) for case in LOW_MEMORY_CASES for limit in MEMORY_LIMITS}
    if not isinstance(runs, list) or len(runs) != len(expected):
        raise LowMemoryManifestError(
            "low-memory manifest.runs must contain exactly 24 entries"
        )

    entries: dict[tuple[str, int], dict[str, Any]] = {}
    report_paths: set[Path] = set()
    checksum_paths: set[Path] = set()
    for index, entry in enumerate(runs, start=1):
        label = f"low-memory manifest.runs[{index}]"
        if not isinstance(entry, dict):
            raise LowMemoryManifestError(f"{label} must be an object")
        case = entry.get("case")
        limit = entry.get("memory_limit_bytes")
        if not isinstance(case, str) or type(limit) is not int:
            raise LowMemoryManifestError(
                f"{label} must contain a string case and integer memory limit"
            )
        key = (case, limit)
        if key not in expected:
            raise LowMemoryManifestError(
                f"{label} has unexpected case/memory pair {key!r}"
            )
        if key in entries:
            raise LowMemoryManifestError(f"duplicate low-memory case {key!r}")
        _expect(entry, "require_spill", True, label)
        report = _artifact(manifest_path, entry.get("report"), f"{label}.report")
        checksum = _artifact(
            manifest_path, entry.get("checksum_report"), f"{label}.checksum_report"
        )
        if report in report_paths:
            raise LowMemoryManifestError(f"duplicate report artifact {report}")
        if checksum in checksum_paths:
            raise LowMemoryManifestError(f"duplicate checksum artifact {checksum}")
        report_paths.add(report)
        checksum_paths.add(checksum)
        entries[key] = {
            "case": case,
            "memory_limit_bytes": limit,
            "report": report,
            "checksum_report": checksum,
            "checksum": read_checksum(checksum, f"{label}.checksum_report"),
        }

    if set(entries) != expected:
        missing = sorted(expected - set(entries))
        raise LowMemoryManifestError(f"missing low-memory cases: {missing!r}")

    supplied_joins = [_resolved(path) for path in join_paths]
    if len(supplied_joins) != len(set(supplied_joins)):
        raise LowMemoryManifestError("duplicate --join report path")
    expected_joins = {
        entries[(case, MEMORY_LIMITS[1])]["report"] for case in JOIN_CASES
    }
    if set(supplied_joins) != expected_joins:
        raise LowMemoryManifestError(
            "--join inputs must be exactly the six 128 MiB reports named by "
            "the low-memory manifest"
        )
    return {"document": document, "entries": entries}


def read_checksum(path: Path, label: str) -> str:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise LowMemoryManifestError(f"cannot read {label} {path}: {error}") from error
    if len(lines) != 1 or SHA256.fullmatch(lines[0]) is None:
        raise LowMemoryManifestError(
            f"{label} must contain exactly one lowercase SHA-256"
        )
    return lines[0]


def _load_object(path: Path, label: str) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise LowMemoryManifestError(f"cannot read {label} {path}: {error}") from error
    if not isinstance(document, dict):
        raise LowMemoryManifestError(f"{label} must contain a JSON object")
    return document


def _expect(source: dict[str, Any], field: str, expected: Any, label: str) -> None:
    actual = source.get(field)
    if type(actual) is not type(expected) or actual != expected:
        raise LowMemoryManifestError(
            f"{label}.{field}: expected {expected!r}, got {actual!r}"
        )


def _expect_true_fields(source: Any, label: str, fields: tuple[str, ...]) -> None:
    if not isinstance(source, dict):
        raise LowMemoryManifestError(f"{label} must be an object")
    for field in fields:
        _expect(source, field, True, label)


def _artifact(manifest_path: Path, value: Any, label: str) -> Path:
    if not isinstance(value, str) or not value:
        raise LowMemoryManifestError(f"{label} must be a non-empty path")
    path = Path(value)
    candidates = [path] if path.is_absolute() else [ROOT / path, manifest_path.parent / path]
    for candidate in candidates:
        if candidate.is_file():
            return _resolved(candidate)
    raise LowMemoryManifestError(f"cannot resolve {label} artifact {value!r}")


def _resolved(path: Path) -> Path:
    try:
        return path.resolve(strict=True)
    except OSError as error:
        raise LowMemoryManifestError(f"cannot resolve artifact {path}: {error}") from error
