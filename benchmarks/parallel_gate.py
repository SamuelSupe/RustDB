#!/usr/bin/env python3
"""Validation logic for the RustDB v0.2 fixed-hardware performance gate."""

from __future__ import annotations

import json
import hashlib
import math
import re
import statistics
from pathlib import Path
from typing import Any


CASES = ("scan-filter", "aggregate")
THREADS = (1, 4)
MEMORY_LIMIT = 1_073_741_824
SHA256 = re.compile(r"^[0-9a-f]{64}$")
GIT_COMMIT = re.compile(r"^[0-9a-f]{40}$")
NATIVE = re.compile(r"(?:^|\s)-C(?:\s+)?target-cpu=native(?:\s|$)")


class GateError(ValueError):
    pass


def fail(message: str) -> None:
    raise GateError(message)


def load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        fail(f"cannot read {label} {path}: {error}")
    if not isinstance(value, dict):
        fail(f"{label} must be a JSON object: {path}")
    return value


def sha256_file(path: Path, label: str) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
    except OSError as error:
        fail(f"cannot read {label} {path}: {error}")
    return digest.hexdigest()


def repository_root(manifest_path: Path, label: str) -> Path:
    for parent in manifest_path.parents:
        if (parent / "Cargo.toml").is_file():
            return parent
    fail(f"cannot locate the repository root for {label} manifest {manifest_path}")


def validate_harness(
    manifest: dict[str, Any], manifest_path: Path, label: str
) -> Path:
    harness = manifest.get("harness")
    if not isinstance(harness, dict):
        fail(f"{label}.harness must be an object")
    root = repository_root(manifest_path, label)
    artifacts = {
        "runner_sha256": root / "benchmarks/run_baseline.sh",
        "library_sha256": root / "benchmarks/suites/lib.sh",
        "checksum_runner_sha256": root / "tools/tpch/compare_query.sh",
    }
    for field, path in artifacts.items():
        expected = harness.get(field)
        if not isinstance(expected, str) or SHA256.fullmatch(expected) is None:
            fail(f"{label}.harness.{field} must be a lowercase SHA-256")
        actual = sha256_file(path, f"{label} harness artifact")
        if actual != expected:
            fail(
                f"{label}.harness.{field} does not match {path}: "
                f"expected {expected}, got {actual}"
            )
    return root


def expect(document: dict[str, Any], key: str, expected: Any, label: str) -> None:
    actual = document.get(key)
    if type(actual) is not type(expected) or actual != expected:
        fail(f"{label}.{key}: expected {expected!r}, got {actual!r}")


def resolve_artifact(manifest_path: Path, value: Any, label: str) -> Path:
    if not isinstance(value, str) or not value:
        fail(f"{label} must be a non-empty path")
    path = Path(value)
    if path.is_absolute():
        return path
    for parent in manifest_path.parents:
        candidate = parent / path
        if (parent / "Cargo.toml").is_file() and candidate.exists():
            return candidate
    repo_path = Path(__file__).resolve().parent.parent / path
    return repo_path if repo_path.exists() else manifest_path.parent / path


def validate_build(build: Any, label: str) -> dict[str, Any]:
    if not isinstance(build, dict):
        fail(f"{label}.build must be an object")
    expect(build, "cargo_profile", "release", f"{label}.build")
    flags = build.get("rustflags")
    if not isinstance(flags, str) or NATIVE.search(flags) is None:
        fail(f"{label}.build.rustflags must enable '-C target-cpu=native'")
    if not isinstance(build.get("rustc_version"), str) or not build["rustc_version"]:
        fail(f"{label}.build.rustc_version must be non-empty")
    return build


def read_checksum(path: Path, label: str) -> str:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        fail(f"cannot read {label} {path}: {error}")
    if len(lines) != 1 or SHA256.fullmatch(lines[0]) is None:
        fail(f"{label} must contain exactly one lowercase SHA-256: {path}")
    return lines[0]


def validate_report(
    path: Path,
    *,
    label: str,
    build_id: str,
    build: dict[str, Any],
    threads: int,
    engine_version: str,
    binary_sha256: str | None,
    actual_cpu_model: str | None,
) -> float:
    report = load_json(path, f"{label} report")
    expect(report, "engine_version", engine_version, label)
    expect(report, "build_id", build_id, label)
    if binary_sha256 is not None:
        expect(report, "binary_sha256", binary_sha256, label)
    if report.get("build") != build:
        fail(f"{label}.build does not match its manifest")
    expect(report, "warmup", 2, label)
    expect(report, "iterations", 5, label)

    config = report.get("config")
    if not isinstance(config, dict):
        fail(f"{label}.config must be an object")
    for key, expected in (
        ("memory_limit_bytes", MEMORY_LIMIT),
        ("compute_threads", threads),
        ("batch_size", 8192),
    ):
        expect(config, key, expected, f"{label}.config")
    cache_bytes = config.get("metadata_cache_bytes")
    if type(cache_bytes) is not int or cache_bytes <= 0:
        fail(f"{label}.config.metadata_cache_bytes must be positive")

    environment = report.get("environment")
    if not isinstance(environment, dict):
        fail(f"{label}.environment must be an object")
    cpu = environment.get("cpu_model")
    if not isinstance(cpu, str) or "M5 Max" not in cpu:
        fail(f"{label}.environment.cpu_model must identify an M5 Max, got {cpu!r}")
    if actual_cpu_model is not None and cpu != actual_cpu_model:
        fail(
            f"{label}.environment.cpu_model does not match the detected host CPU: "
            f"expected {actual_cpu_model!r}, got {cpu!r}"
        )

    p50 = report.get("p50_ms")
    runs = report.get("runs")
    if type(p50) not in (int, float) or not math.isfinite(p50) or p50 <= 0:
        fail(f"{label}.p50_ms must be a positive finite number")
    if not isinstance(runs, list) or len(runs) != 5:
        fail(f"{label}.runs must contain exactly five measured runs")
    elapsed = []
    for index, run in enumerate(runs):
        value = run.get("elapsed_ms") if isinstance(run, dict) else None
        if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
            fail(f"{label}.runs[{index}].elapsed_ms must be positive and finite")
        elapsed.append(float(value))
    if not math.isclose(float(p50), statistics.median(elapsed), rel_tol=1e-9, abs_tol=1e-6):
        fail(f"{label}.p50_ms does not equal the median measured elapsed_ms")
    return float(p50)


def inspect_manifest(
    path: Path,
    *,
    label: str,
    engine_version: str,
    actual_cpu_model: str | None = None,
) -> dict[str, Any]:
    manifest = load_json(path, f"{label} manifest")
    expect(manifest, "suite", "rustdb-baseline-v1", label)
    expect(manifest, "memory_limit_bytes", MEMORY_LIMIT, label)
    build_id = manifest.get("rustdb_build_id")
    if not isinstance(build_id, str) or not build_id or build_id == "unknown":
        fail(f"{label}.rustdb_build_id must be non-empty and known")
    if label == "candidate" and GIT_COMMIT.fullmatch(build_id) is None:
        fail(
            "candidate build id must be an exact clean 40-character Git commit, "
            f"got {build_id!r}"
        )
    build = validate_build(manifest.get("build"), label)
    validate_harness(manifest, path, label)
    binary_sha256 = manifest.get("benchmark_binary_sha256")
    if label == "candidate":
        if not isinstance(binary_sha256, str) or SHA256.fullmatch(binary_sha256) is None:
            fail(f"{label}.benchmark_binary_sha256 must be a lowercase SHA-256")
    elif binary_sha256 is not None and (
        not isinstance(binary_sha256, str) or SHA256.fullmatch(binary_sha256) is None
    ):
        fail(f"{label}.benchmark_binary_sha256 must be a lowercase SHA-256 when present")

    dataset = manifest.get("dataset")
    if not isinstance(dataset, dict) or not isinstance(dataset.get("generation"), dict):
        fail(f"{label}.dataset.generation must be an object")
    if str(dataset["generation"].get("scale_factor")) != "10":
        fail(f"{label} must use the SF10 dataset")
    digest = dataset.get("manifest_sha256")
    if not isinstance(digest, str) or SHA256.fullmatch(digest) is None:
        fail(f"{label}.dataset.manifest_sha256 must be a lowercase SHA-256")
    dataset_manifest = resolve_artifact(
        path, dataset.get("manifest"), f"{label} dataset manifest"
    )
    actual_dataset_digest = sha256_file(dataset_manifest, f"{label} dataset manifest")
    if actual_dataset_digest != digest:
        fail(
            f"{label}.dataset.manifest_sha256 does not match {dataset_manifest}: "
            f"expected {digest}, got {actual_dataset_digest}"
        )

    correctness = manifest.get("correctness")
    if not isinstance(correctness, dict) or correctness.get("verified") is not True:
        fail(f"{label}.correctness.verified must be true")
    targets = correctness.get("targets")
    if not isinstance(targets, list) or "local" not in targets:
        fail(f"{label}.correctness.targets must include local")

    runs = manifest.get("runs")
    if not isinstance(runs, list):
        fail(f"{label}.runs must be an array")
    selected: dict[tuple[str, int], dict[str, Any]] = {}
    for entry in runs:
        if not isinstance(entry, dict):
            fail(f"{label}.runs entries must be objects")
        if (
            entry.get("target") == "local"
            and entry.get("cache_mode") == "metadata-warm"
            and entry.get("case") in CASES
            and type(entry.get("threads")) is int
            and entry.get("threads") in THREADS
            and entry.get("batch_size") == 8192
        ):
            key = (entry["case"], entry["threads"])
            if key in selected:
                fail(f"{label} has duplicate gate run {key}")
            selected[key] = entry

    measurements: dict[tuple[str, int], float] = {}
    checksums: dict[tuple[str, int], str] = {}
    checksum_paths: dict[Path, tuple[str, int]] = {}
    for case in CASES:
        for threads in THREADS:
            key = (case, threads)
            entry = selected.get(key)
            if entry is None:
                fail(f"{label} is missing local/metadata-warm/{case}/t{threads}/b8192")
            expect(entry, "warmup", 2, f"{label}.{case}.t{threads}")
            expect(entry, "iterations", 5, f"{label}.{case}.t{threads}")
            report_path = resolve_artifact(path, entry.get("report"), f"{label} report")
            measurements[key] = validate_report(
                report_path,
                label=f"{label}.{case}.t{threads}",
                build_id=build_id,
                build=build,
                threads=threads,
                engine_version=engine_version,
                binary_sha256=binary_sha256 if label == "candidate" else None,
                actual_cpu_model=actual_cpu_model,
            )
            checksum_path = resolve_artifact(
                path, entry.get("checksum_report"), f"{label} checksum"
            ).resolve()
            previous = checksum_paths.get(checksum_path)
            if previous is not None:
                fail(
                    f"{label} reuses checksum evidence {checksum_path} for "
                    f"{previous} and {key}; every thread configuration must be executed "
                    "and recorded independently"
                )
            checksum_paths[checksum_path] = key
            checksums[key] = read_checksum(checksum_path, f"{label} checksum")

    return {
        "build_id": build_id,
        "binary_sha256": binary_sha256,
        "dataset": {
            "generation": dataset["generation"],
            "manifest_sha256": digest,
        },
        "measurements": measurements,
        "checksums": checksums,
    }


def evaluate(
    candidate_path: Path,
    baseline_path: Path,
    alpha2_build_id: str,
    candidate_build_id: str | None = None,
    candidate_binary_sha256: str | None = None,
    actual_cpu_model: str | None = None,
) -> dict[str, Any]:
    baseline = inspect_manifest(
        baseline_path,
        label="baseline",
        engine_version="0.1.0",
        actual_cpu_model=actual_cpu_model,
    )
    candidate = inspect_manifest(
        candidate_path,
        label="candidate",
        engine_version="0.2.0-alpha.1",
        actual_cpu_model=actual_cpu_model,
    )
    if baseline["build_id"] != alpha2_build_id:
        fail(
            "baseline build id does not match v0.1.0-alpha.2: "
            f"expected {alpha2_build_id}, got {baseline['build_id']}"
        )
    actual_candidate_id = candidate["build_id"]
    if candidate_build_id is not None and actual_candidate_id != candidate_build_id:
        fail(
            "candidate build id does not match the current clean HEAD: "
            f"expected {candidate_build_id}, got {actual_candidate_id}"
        )
    if (
        candidate_binary_sha256 is not None
        and candidate["binary_sha256"] != candidate_binary_sha256
    ):
        fail(
            "candidate benchmark executable does not match the executable rebuilt from "
            f"the current clean HEAD: expected {candidate_binary_sha256}, "
            f"got {candidate['binary_sha256']}"
        )
    if candidate["dataset"] != baseline["dataset"]:
        fail("candidate and alpha.2 baseline dataset fingerprints differ")

    results: dict[str, Any] = {}
    for case in CASES:
        baseline_checksum = baseline["checksums"][(case, 1)]
        all_checksums = {
            baseline["checksums"][(case, threads)] for threads in THREADS
        } | {candidate["checksums"][(case, threads)] for threads in THREADS}
        if all_checksums != {baseline_checksum}:
            fail(f"{case} checksums differ across candidate/baseline thread configurations")
        t1 = candidate["measurements"][(case, 1)]
        t4 = candidate["measurements"][(case, 4)]
        baseline_t1 = baseline["measurements"][(case, 1)]
        speedup = t1 / t4
        regression = t1 / baseline_t1 - 1.0
        if speedup < 2.0:
            fail(f"{case} 4-thread throughput is only {speedup:.3f}x; require >= 2.000x")
        if regression > 0.10:
            fail(f"{case} 1-thread p50 regressed {regression:.2%}; maximum is 10.00%")
        results[case] = {
            "baseline_t1_p50_ms": baseline_t1,
            "candidate_t1_p50_ms": t1,
            "candidate_t4_p50_ms": t4,
            "throughput_multiplier": speedup,
            "single_thread_regression": regression,
            "checksum": baseline_checksum,
        }
    return {
        "status": "pass",
        "baseline_build_id": baseline["build_id"],
        "candidate_build_id": candidate["build_id"],
        "dataset_manifest_sha256": candidate["dataset"]["manifest_sha256"],
        "cases": results,
    }
