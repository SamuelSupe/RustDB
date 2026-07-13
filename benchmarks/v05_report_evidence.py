"""Shared report provenance checks for the v0.5 release gates."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any


COMPUTE_THREADS = 4
BATCH_SIZE = 8192
IO_CONCURRENCY = 32
SHA256 = re.compile(r"^[0-9a-f]{64}$")
GIT_COMMIT = re.compile(r"^[0-9a-f]{40}$")
NATIVE = re.compile(r"(?:^|\s)-C(?:\s+)?target-cpu=native(?:\s|$)")
ROOT = Path(__file__).resolve().parent.parent


class GateError(ValueError):
    """A report is missing evidence or exceeds a release threshold."""


def load_report(path: Path, label: str) -> dict[str, Any]:
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise GateError(f"cannot read {label} report {path}: {error}") from error
    if not isinstance(document, dict):
        raise GateError(f"{label} report must contain a JSON object")
    return document


def expect(source: dict[str, Any], field: str, expected: Any, label: str) -> None:
    actual = source.get(field)
    if type(actual) is not type(expected) or actual != expected:
        raise GateError(f"{label}.{field}: expected {expected!r}, got {actual!r}")


def validate_evidence(
    report: dict[str, Any],
    report_path: Path,
    label: str,
    *,
    query_name: str,
    template: Path,
    engine_version: str,
    expected_build_id: str | None = None,
    expected_binary_sha256: str | None = None,
    actual_cpu_model: str | None = None,
    expected_dataset_manifest_sha256: str | None = None,
    allow_missing_dataset: bool = False,
    expected_memory_limit: int,
    expected_metadata_cache_bytes: int = 0,
    expected_warmup: int | None = None,
    expected_iterations: int | None = None,
) -> dict[str, Any]:
    expect(report, "engine_version", engine_version, label)
    build_id = report.get("build_id")
    if not isinstance(build_id, str) or GIT_COMMIT.fullmatch(build_id) is None:
        raise GateError(
            f"{label}.build_id must be an exact clean 40-character Git commit"
        )
    if expected_build_id is not None and build_id != expected_build_id:
        raise GateError(
            f"{label}.build_id does not match the required build: "
            f"expected {expected_build_id}, got {build_id}"
        )

    binary = report.get("binary_sha256")
    if not isinstance(binary, str) or SHA256.fullmatch(binary) is None:
        raise GateError(f"{label}.binary_sha256 must be a lowercase SHA-256")
    if expected_binary_sha256 is not None and binary != expected_binary_sha256:
        raise GateError(
            f"{label}.binary_sha256 does not match the rebuilt candidate: "
            f"expected {expected_binary_sha256}, got {binary}"
        )

    build = report.get("build")
    if not isinstance(build, dict):
        raise GateError(f"{label}.build must be an object")
    expect(build, "cargo_profile", "release", f"{label}.build")
    flags = build.get("rustflags")
    if not isinstance(flags, str) or NATIVE.search(flags) is None:
        raise GateError(f"{label}.build.rustflags must enable '-C target-cpu=native'")
    rustc = build.get("rustc_version")
    if not isinstance(rustc, str) or not rustc:
        raise GateError(f"{label}.build.rustc_version must be non-empty")

    config = report.get("config")
    if not isinstance(config, dict):
        raise GateError(f"{label}.config must be an object")
    for field, expected in (
        ("memory_limit_bytes", expected_memory_limit),
        ("compute_threads", COMPUTE_THREADS),
        ("batch_size", BATCH_SIZE),
        ("io_concurrency", IO_CONCURRENCY),
        ("metadata_cache_bytes", expected_metadata_cache_bytes),
    ):
        expect(config, field, expected, f"{label}.config")

    environment = report.get("environment")
    if not isinstance(environment, dict):
        raise GateError(f"{label}.environment must be an object")
    cpu = environment.get("cpu_model")
    if not isinstance(cpu, str) or "M5 Max" not in cpu:
        raise GateError(f"{label}.environment.cpu_model must identify an Apple M5 Max")
    if actual_cpu_model is not None and cpu != actual_cpu_model:
        raise GateError(
            f"{label}.environment.cpu_model does not match the host: "
            f"expected {actual_cpu_model!r}, got {cpu!r}"
        )
    for field in ("os", "arch"):
        value = environment.get(field)
        if not isinstance(value, str) or not value:
            raise GateError(f"{label}.environment.{field} must be non-empty")

    if expected_warmup is not None:
        expect(report, "warmup", expected_warmup, label)
    if expected_iterations is not None:
        expect(report, "iterations", expected_iterations, label)
    query_root = _validate_query(report, report_path, label, query_name, template)
    dataset = validate_dataset(
        report,
        report_path,
        label,
        expected_dataset_manifest_sha256,
        allow_missing_dataset,
    )
    return {
        "build_id": build_id,
        "binary_sha256": binary,
        "build": {
            "cargo_profile": build["cargo_profile"],
            "rustflags": flags,
            "rustc_version": rustc,
        },
        "config": {
            field: config[field]
            for field in (
                "memory_limit_bytes",
                "compute_threads",
                "batch_size",
                "io_concurrency",
                "metadata_cache_bytes",
            )
        },
        "full_config": dict(config),
        "environment": {
            field: environment[field] for field in ("os", "arch", "cpu_model")
        },
        "dataset": dataset,
        "query_root": query_root,
    }


def validate_dataset(
    report: dict[str, Any],
    report_path: Path,
    label: str,
    expected_digest: str | None,
    allow_missing: bool,
) -> tuple[str, str] | None:
    dataset = _dataset_source(report, report_path)
    if dataset is None and allow_missing:
        return None
    if not isinstance(dataset, dict):
        raise GateError(
            f"{label}.dataset must provide SF10 provenance; only the explicit "
            "legacy v0.4 compatibility switch may omit it"
        )
    generation = dataset.get("generation")
    if not isinstance(generation, dict) or str(generation.get("scale_factor")) != "10":
        raise GateError(f"{label}.dataset.generation.scale_factor must be SF10")
    digest = dataset.get("manifest_sha256")
    if not isinstance(digest, str) or SHA256.fullmatch(digest) is None:
        raise GateError(f"{label}.dataset.manifest_sha256 must be a lowercase SHA-256")
    if expected_digest is not None and digest != expected_digest:
        raise GateError(
            f"{label}.dataset.manifest_sha256 does not match the required SF10 manifest"
        )
    manifest = dataset.get("manifest")
    if manifest is not None:
        if not isinstance(manifest, str) or not manifest:
            raise GateError(f"{label}.dataset.manifest must be a non-empty path")
        path = _resolve_artifact(report_path, manifest, f"{label} dataset manifest")
        if _sha256_file(path) != digest:
            raise GateError(f"{label}.dataset.manifest_sha256 does not match {path}")
    generation_fingerprint = json.dumps(
        generation, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return (generation_fingerprint, digest)


def same_candidate_evidence(
    left: dict[str, Any], right: dict[str, Any], label: str
) -> None:
    for field in ("build_id", "binary_sha256", "build", "dataset", "query_root"):
        if left[field] != right[field]:
            raise GateError(
                f"{label} do not share the same {field.replace('_', ' ')}"
            )


def same_execution_config(
    left: dict[str, Any],
    right: dict[str, Any],
    label: str,
    *,
    ignore_memory_limit: bool = False,
) -> None:
    left_config = dict(left["full_config"])
    right_config = dict(right["full_config"])
    if ignore_memory_limit:
        left_config.pop("memory_limit_bytes", None)
        right_config.pop("memory_limit_bytes", None)
    if left_config != right_config:
        raise GateError(f"{label} do not share the same complete execution config")


def comparable_q21(candidate: dict[str, Any], baseline: dict[str, Any]) -> None:
    for field in ("config", "build", "environment"):
        if candidate[field] != baseline[field]:
            raise GateError(f"Q21 baseline and candidate have incomparable {field}")


def _validate_query(
    report: dict[str, Any],
    report_path: Path,
    label: str,
    query_name: str,
    template_path: Path,
) -> str:
    query_file = report.get("query_file")
    if not isinstance(query_file, str) or not query_file:
        raise GateError(f"{label}.query_file must be non-empty")
    if Path(query_file).stem != query_name:
        raise GateError(
            f"{label}.query_file must identify {query_name}.sql, got {query_file!r}"
        )
    query_path = _resolve_artifact(report_path, query_file, f"{label} query")
    try:
        query = query_path.read_text(encoding="utf-8").replace("\r\n", "\n").strip()
        template = template_path.read_text(encoding="utf-8").replace("\r\n", "\n").strip()
    except OSError as error:
        raise GateError(f"cannot read {label} query identity: {error}") from error
    chunks = template.split("__TPCH_ROOT__")
    pattern = ""
    for index, chunk in enumerate(chunks):
        if index:
            pattern += "([^'\r\n]+)"
        pattern += re.escape(chunk)
    match = re.fullmatch(pattern, query)
    if match is None:
        raise GateError(f"{label}.query_file does not match the canonical {query_name} query")
    roots = set(match.groups())
    if len(roots) != 1:
        raise GateError(f"{label}.query_file mixes multiple dataset roots")
    return roots.pop()


def _resolve_artifact(report_path: Path, value: str, label: str) -> Path:
    path = Path(value)
    candidates = []
    if path.is_absolute():
        candidates.append(path)
        if path.parts[:2] == ("/", "workspace"):
            candidates.append(ROOT.joinpath(*path.parts[2:]))
            candidates.append(report_path.parent / path.name)
    else:
        candidates.extend((report_path.parent / path, ROOT / path))
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise GateError(f"cannot resolve {label} artifact {value!r}")


def _dataset_source(report: dict[str, Any], report_path: Path) -> Any:
    if "dataset" in report:
        return report["dataset"]
    for parent in report_path.parents:
        manifest = parent / "manifest.json"
        if not manifest.is_file():
            continue
        document = load_report(manifest, "enclosing benchmark manifest")
        if "dataset" in document:
            return document["dataset"]
    return None


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
    except OSError as error:
        raise GateError(f"cannot hash {path}: {error}") from error
    return digest.hexdigest()
