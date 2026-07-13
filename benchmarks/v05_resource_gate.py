"""Strict validation for the RustDB v0.5 SF10 resource gate."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

from v05_low_memory_gate import (
    JOIN_CASES,
    LOW_MEMORY_CASES,
    LowMemoryManifestError,
    load_low_memory_manifest,
    read_checksum,
)
from v05_report_evidence import (
    GateError,
    SHA256,
    comparable_q21 as _comparable_q21,
    expect as _expect,
    load_report as _load,
    same_candidate_evidence as _same_candidate_evidence,
    same_execution_config as _same_execution_config,
    validate_dataset as _validate_dataset,
    validate_evidence as _validate_evidence,
)


MEMORY_LIMIT = 128 * 1024**2
BASELINE_ENGINE_VERSION = "0.4.0-alpha.1"
CANDIDATE_ENGINE_VERSION = "0.5.0-alpha.1"

Q17_MAX_WRITE_BYTES = 8 * 1024**3
Q17_MAX_ACTIVE_BYTES = 2 * 1024**3
MAX_REPARTITION_DEPTH = 1
MAX_SPILL_FILES = 512
Q21_MAX_BASELINE_RATIO = 0.50
Q21_MAX_TAIL_RATIO = 1.30
MAX_JOIN_WRITE_TO_SCAN_RATIO = 3.0

ROOT = Path(__file__).resolve().parent.parent
JOIN_TEMPLATES = {case: LOW_MEMORY_CASES[case] for case in JOIN_CASES}


def evaluate(
    q17_path: Path,
    q21_candidate_path: Path,
    q21_baseline_path: Path,
    join_paths: list[Path],
    q17_checksum_path: Path,
    q21_checksum_path: Path,
    low_memory_manifest_path: Path,
    *,
    baseline_build_id: str | None = None,
    candidate_build_id: str | None = None,
    candidate_binary_sha256: str | None = None,
    actual_cpu_model: str | None = None,
    expected_dataset_manifest_sha256: str | None = None,
    allow_legacy_v04_missing_dataset: bool = False,
) -> dict[str, Any]:
    """Validate provenance, comparability, and resource thresholds."""
    if len(join_paths) != len(JOIN_TEMPLATES):
        raise GateError(
            "exactly one 128 MiB report is required for each of "
            + ", ".join(JOIN_TEMPLATES)
        )
    try:
        low_memory = load_low_memory_manifest(low_memory_manifest_path, join_paths)
        q17_checksum = read_checksum(q17_checksum_path, "Q17 checksum")
        q21_checksum = read_checksum(q21_checksum_path, "Q21 checksum")
    except LowMemoryManifestError as error:
        raise GateError(str(error)) from error

    q17_report = _load(q17_path, "Q17")
    candidate_report = _load(q21_candidate_path, "Q21 candidate")
    baseline_report = _load(q21_baseline_path, "Q21 baseline")

    q17_evidence = _validate_evidence(
        q17_report,
        q17_path,
        "Q17",
        query_name="q17",
        template=ROOT / "benchmarks/tpch/q17.sql",
        engine_version=CANDIDATE_ENGINE_VERSION,
        expected_build_id=candidate_build_id,
        expected_binary_sha256=candidate_binary_sha256,
        actual_cpu_model=actual_cpu_model,
        expected_dataset_manifest_sha256=expected_dataset_manifest_sha256,
        expected_memory_limit=MEMORY_LIMIT,
        expected_metadata_cache_bytes=0,
        expected_warmup=0,
        expected_iterations=1,
    )
    candidate_evidence = _validate_evidence(
        candidate_report,
        q21_candidate_path,
        "Q21 candidate",
        query_name="q21",
        template=ROOT / "benchmarks/tpch/q21.sql",
        engine_version=CANDIDATE_ENGINE_VERSION,
        expected_build_id=candidate_build_id,
        expected_binary_sha256=candidate_binary_sha256,
        actual_cpu_model=actual_cpu_model,
        expected_dataset_manifest_sha256=expected_dataset_manifest_sha256,
        expected_memory_limit=MEMORY_LIMIT,
        expected_metadata_cache_bytes=0,
        expected_warmup=2,
        expected_iterations=5,
    )
    baseline_evidence = _validate_evidence(
        baseline_report,
        q21_baseline_path,
        "Q21 baseline",
        query_name="q21",
        template=ROOT / "benchmarks/tpch/q21.sql",
        engine_version=BASELINE_ENGINE_VERSION,
        expected_build_id=baseline_build_id,
        actual_cpu_model=actual_cpu_model,
        expected_dataset_manifest_sha256=expected_dataset_manifest_sha256,
        allow_missing_dataset=allow_legacy_v04_missing_dataset,
        expected_memory_limit=MEMORY_LIMIT,
        expected_metadata_cache_bytes=0,
        expected_warmup=2,
        expected_iterations=5,
    )
    _check_terminal_resources(q17_report, "Q17")
    _check_terminal_resources(candidate_report, "Q21 candidate")

    _same_candidate_evidence(q17_evidence, candidate_evidence, "Q17 and Q21 candidate")
    _same_execution_config(q17_evidence, candidate_evidence, "Q17 and Q21 candidate")
    if candidate_evidence["binary_sha256"] == baseline_evidence["binary_sha256"]:
        raise GateError("Q21 baseline and candidate binary digests must differ")
    legacy_dataset = baseline_evidence["dataset"] is None
    if not legacy_dataset and candidate_evidence["dataset"] != baseline_evidence["dataset"]:
        raise GateError("Q21 baseline and candidate SF10 dataset fingerprints differ")
    _comparable_q21(candidate_evidence, baseline_evidence)

    low_document = low_memory["document"]
    low_dataset = _validate_dataset(
        low_document,
        low_memory_manifest_path,
        "low-memory manifest",
        expected_dataset_manifest_sha256,
        False,
    )
    if low_dataset != candidate_evidence["dataset"]:
        raise GateError(
            "low-memory and candidate reports have different dataset generation or "
            "manifest digest"
        )
    _expect(
        low_document,
        "rustdb_build_id",
        candidate_evidence["build_id"],
        "low-memory manifest",
    )
    _expect(
        low_document,
        "benchmark_binary_sha256",
        candidate_evidence["binary_sha256"],
        "low-memory manifest",
    )
    if low_document.get("build") != candidate_evidence["build"]:
        raise GateError("low-memory manifest.build does not match the candidate build")

    for (case, limit), entry in sorted(low_memory["entries"].items()):
        label = f"low-memory {case} at {limit} bytes"
        report = _load(entry["report"], label)
        evidence = _validate_evidence(
            report,
            entry["report"],
            label,
            query_name=case,
            template=LOW_MEMORY_CASES[case],
            engine_version=CANDIDATE_ENGINE_VERSION,
            expected_build_id=candidate_evidence["build_id"],
            expected_binary_sha256=candidate_evidence["binary_sha256"],
            actual_cpu_model=actual_cpu_model,
            expected_dataset_manifest_sha256=expected_dataset_manifest_sha256,
            expected_memory_limit=limit,
            expected_metadata_cache_bytes=0,
            expected_warmup=0,
            expected_iterations=1,
        )
        _same_candidate_evidence(candidate_evidence, evidence, f"Q21 and {label}")
        _same_execution_config(
            candidate_evidence, evidence, f"Q21 and {label}", ignore_memory_limit=True
        )
        _check_terminal_resources(report, label)
        _check_low_memory_run(report, label, limit)

    joins = []
    seen: set[str] = set()
    for path in join_paths:
        report = _load(path, f"Join {path}")
        query_file = report.get("query_file")
        if not isinstance(query_file, str) or not query_file:
            raise GateError(f"Join {path}.query_file must identify a Join query")
        query_name = Path(query_file).stem
        template = JOIN_TEMPLATES.get(query_name)
        if template is None:
            raise GateError(f"Join {path} has unknown query identity {query_name!r}")
        if query_name in seen:
            raise GateError(f"duplicate Join query identity {query_name!r}")
        seen.add(query_name)
        evidence = _validate_evidence(
            report,
            path,
            f"Join {query_name}",
            query_name=query_name,
            template=template,
            engine_version=CANDIDATE_ENGINE_VERSION,
            expected_build_id=candidate_evidence["build_id"],
            expected_binary_sha256=candidate_evidence["binary_sha256"],
            actual_cpu_model=actual_cpu_model,
            expected_dataset_manifest_sha256=expected_dataset_manifest_sha256,
            expected_memory_limit=MEMORY_LIMIT,
            expected_metadata_cache_bytes=0,
            expected_warmup=0,
            expected_iterations=1,
        )
        _same_candidate_evidence(candidate_evidence, evidence, f"Q21 and {query_name}")
        _same_execution_config(candidate_evidence, evidence, f"Q21 and {query_name}")
        _check_terminal_resources(report, f"Join {query_name}")
        joins.append(_check_join(report, query_name))
    missing = set(JOIN_TEMPLATES) - seen
    if missing:
        raise GateError(f"missing Join query identities: {', '.join(sorted(missing))}")

    return {
        "status": "pass",
        "evidence": {
            "candidate_build_id": candidate_evidence["build_id"],
            "candidate_binary_sha256": candidate_evidence["binary_sha256"],
            "baseline_build_id": baseline_evidence["build_id"],
            "baseline_binary_sha256": baseline_evidence["binary_sha256"],
            "dataset_manifest_sha256": candidate_evidence["dataset"][1],
            "q17_checksum": q17_checksum,
            "q21_checksum": q21_checksum,
            "low_memory_cases": len(low_memory["entries"]),
            "legacy_v04_missing_dataset": legacy_dataset,
        },
        "q17": _check_q17(q17_report),
        "q21": _check_q21(candidate_report, baseline_report),
        "joins": joins,
    }


def _runs(report: dict[str, Any], label: str) -> list[dict[str, Any]]:
    runs = report.get("runs")
    if not isinstance(runs, list) or not runs:
        raise GateError(f"{label}.runs must be a non-empty array")
    if not all(isinstance(run, dict) for run in runs):
        raise GateError(f"{label}.runs must contain JSON objects")
    iterations = report.get("iterations")
    if type(iterations) is not int or iterations != len(runs):
        raise GateError(f"{label}.iterations must equal the number of runs")
    return runs


def _check_terminal_resources(report: dict[str, Any], label: str) -> None:
    for index, run in enumerate(_runs(report, label), start=1):
        run_label = f"{label} run {index}"
        for field in (
            "current_memory_bytes",
            "active_spill_bytes",
            "active_spill_files",
        ):
            value = _number(run, field, run_label)
            if value != 0:
                raise GateError(f"{run_label}.{field} must be zero after completion")
        _expect(run, "spill_cleaned", True, run_label)


def _check_low_memory_run(
    report: dict[str, Any], label: str, memory_limit: int
) -> None:
    for index, run in enumerate(_runs(report, label), start=1):
        run_label = f"{label} run {index}"
        peak = _number(run, "peak_memory_bytes", run_label)
        if peak > memory_limit:
            raise GateError(
                f"{run_label}.peak_memory_bytes is {peak:g}; limit is {memory_limit}"
            )
        _number(run, "spill_write_bytes", run_label, positive=True)
        _number(run, "spill_read_bytes", run_label, positive=True)


def _number(source: dict[str, Any], field: str, label: str, *, positive: bool = False) -> float:
    value = source.get(field)
    if type(value) not in (int, float) or not math.isfinite(value):
        raise GateError(f"{label}.{field} must be a finite number")
    if value < 0 or (positive and value <= 0):
        qualifier = "positive" if positive else "non-negative"
        raise GateError(f"{label}.{field} must be {qualifier}")
    return float(value)


def _check_q17(report: dict[str, Any]) -> dict[str, Any]:
    maxima = {
        "spill_write_bytes": 0.0,
        "peak_active_spill_bytes": 0.0,
        "max_repartition_depth": 0.0,
        "peak_active_spill_files": 0.0,
    }
    limits = {
        "spill_write_bytes": float(Q17_MAX_WRITE_BYTES),
        "peak_active_spill_bytes": float(Q17_MAX_ACTIVE_BYTES),
        "max_repartition_depth": float(MAX_REPARTITION_DEPTH),
        "peak_active_spill_files": float(MAX_SPILL_FILES),
    }
    for index, run in enumerate(_runs(report, "Q17"), start=1):
        for field, limit in limits.items():
            value = _number(run, field, f"Q17 run {index}")
            maxima[field] = max(maxima[field], value)
            if value > limit:
                raise GateError(
                    f"Q17 run {index} {field} is {value:g}; maximum is {limit:g}"
                )
    return {"maxima": maxima, "limits": limits}


def _nearest_rank(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    index = math.ceil((len(ordered) - 1) * percentile)
    return ordered[index]


def _timings(report: dict[str, Any], label: str) -> tuple[float, float]:
    runs = _runs(report, label)
    if len(runs) != 5:
        raise GateError(f"{label} must contain exactly five measured runs")
    elapsed = [
        _number(run, "elapsed_ms", f"{label} run {index}", positive=True)
        for index, run in enumerate(runs, start=1)
    ]
    p50 = _number(report, "p50_ms", label, positive=True)
    p95 = _number(report, "p95_ms", label, positive=True)
    if not math.isclose(p50, _nearest_rank(elapsed, 0.50), abs_tol=1e-6):
        raise GateError(f"{label}.p50_ms does not match measured runs")
    if not math.isclose(p95, _nearest_rank(elapsed, 0.95), abs_tol=1e-6):
        raise GateError(f"{label}.p95_ms does not match measured runs")
    return p50, p95


def _check_q21(candidate: dict[str, Any], baseline: dict[str, Any]) -> dict[str, float]:
    candidate_p50, candidate_p95 = _timings(candidate, "Q21 candidate")
    baseline_p50, _ = _timings(baseline, "Q21 baseline")
    baseline_ratio = candidate_p50 / baseline_p50
    tail_ratio = candidate_p95 / candidate_p50
    if baseline_ratio > Q21_MAX_BASELINE_RATIO:
        raise GateError(
            f"Q21 candidate/baseline p50 ratio is {baseline_ratio:.3f}; "
            f"maximum is {Q21_MAX_BASELINE_RATIO:.3f}"
        )
    if tail_ratio > Q21_MAX_TAIL_RATIO:
        raise GateError(
            f"Q21 p95/p50 ratio is {tail_ratio:.3f}; "
            f"maximum is {Q21_MAX_TAIL_RATIO:.3f}"
        )
    return {
        "candidate_p50_ms": candidate_p50,
        "baseline_p50_ms": baseline_p50,
        "candidate_p95_ms": candidate_p95,
        "candidate_to_baseline_p50": baseline_ratio,
        "p95_to_p50": tail_ratio,
    }


def _check_join(report: dict[str, Any], query_name: str) -> dict[str, Any]:
    run_results = []
    for index, run in enumerate(_runs(report, f"Join {query_name}"), start=1):
        label = f"Join {query_name} run {index}"
        scanned = _number(run, "scanned_bytes", label, positive=True)
        written = _number(run, "spill_write_bytes", label, positive=True)
        read = _number(run, "spill_read_bytes", label, positive=True)
        files = _number(run, "peak_active_spill_files", label)
        write_ratio = written / scanned
        read_ratio = read / scanned
        if write_ratio > MAX_JOIN_WRITE_TO_SCAN_RATIO:
            raise GateError(
                f"{label} spill write/scanned ratio is {write_ratio:.3f}; maximum is 3.000"
            )
        if files > MAX_SPILL_FILES:
            raise GateError(
                f"{label} peak_active_spill_files is {files:g}; maximum is {MAX_SPILL_FILES}"
            )
        run_results.append(
            {
                "write_to_scan": write_ratio,
                "read_to_scan": read_ratio,
                "peak_active_spill_files": files,
            }
        )
    return {"query": query_name, "runs": run_results}
