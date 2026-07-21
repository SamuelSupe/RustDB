#!/usr/bin/env python3
"""Capture and validate the two single-pass TPC-H SF1 correctness reports."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import sys
from pathlib import Path
from typing import Any

from beta_acceptance_common import read_json, sha256


QUERIES = tuple(f"q{number:02d}" for number in range(1, 23))
TABLES = (
    "customer",
    "lineitem",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier",
)
MEMORY_LIMIT = 4 * 1024**3
THREADS = 4
BATCH_SIZE = 8192
IO_CONCURRENCY = 16


def capture(workspace: Path, output: Path, medium: str) -> dict[str, Any]:
    workspace = workspace.resolve()
    output = output.resolve()
    source = workspace / "data" / "tpch-sf1" / "results" / "latest"
    destination = output / "tpch" / medium
    if destination.exists():
        raise ValueError(f"TPC-H {medium} evidence already exists")
    destination.mkdir(mode=0o700, parents=True)
    for name in ("status.tsv", "checksums.sha256", "provenance.json"):
        path = source / name
        if not path.is_file():
            raise ValueError(f"TPC-H {medium} report is missing {name}")
        shutil.copyfile(path, destination / name)
        (destination / name).chmod(0o600)
    dataset = output / "tpch" / "dataset"
    local_dataset = workspace / "data" / "tpch-sf1"
    if medium == "local":
        if dataset.exists():
            raise ValueError("TPC-H dataset evidence already exists")
        dataset.mkdir(mode=0o700, parents=True)
        for name in ("manifest.json", "manifest.sha256"):
            shutil.copyfile(local_dataset / name, dataset / name)
            (dataset / name).chmod(0o600)
    elif medium != "minio":
        raise ValueError("TPC-H medium must be local or minio")
    accepted = read_json(output / "inputs.json").get("git", {}).get("commit")
    return summary(output, medium, accepted, workspace)


def summary(
    output: Path,
    medium: str,
    accepted_commit: str,
    workspace: Path,
) -> dict[str, Any]:
    report = output / "tpch" / medium
    checksums = parse_status(report / "status.tsv")
    if parse_checksums(report / "checksums.sha256") != checksums:
        raise ValueError(f"TPC-H {medium} status and checksum files differ")
    dataset = validate_dataset(output / "tpch" / "dataset")
    provenance = read_json(report / "provenance.json")
    binary = validate_provenance(
        provenance,
        medium,
        accepted_commit,
        workspace,
        dataset["manifest_sha256"],
    )
    return {
        "status": str(report / "status.tsv"),
        "status_sha256": sha256(report / "status.tsv"),
        "checksums_path": str(report / "checksums.sha256"),
        "checksums_sha256": sha256(report / "checksums.sha256"),
        "provenance": str(report / "provenance.json"),
        "provenance_sha256": sha256(report / "provenance.json"),
        "queries": len(checksums),
        "checksums": checksums,
        "binary_sha256": binary,
        "dataset": dataset,
    }


def parse_status(path: Path) -> dict[str, str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines or lines[0] != "query\tstatus\tchecksum\tdiagnostic":
        raise ValueError("TPC-H status has the wrong header")
    result: dict[str, str] = {}
    for expected, line in zip(QUERIES, lines[1:], strict=False):
        fields = line.split("\t")
        if (
            len(fields) != 4
            or fields[0] != expected
            or fields[1] != "pass"
            or re.fullmatch(r"[0-9a-f]{64}", fields[2]) is None
            or fields[3] != "-"
        ):
            raise ValueError(f"TPC-H status is invalid for {expected}")
        result[expected] = fields[2]
    if len(lines) != len(QUERIES) + 1 or tuple(result) != QUERIES:
        raise ValueError("TPC-H status must contain Q1-Q22 exactly once")
    return result


def parse_checksums(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    lines = path.read_text(encoding="utf-8").splitlines()
    for expected, line in zip(QUERIES, lines, strict=False):
        match = re.fullmatch(r"([0-9a-f]{64})  (q[0-9]{2})", line)
        if match is None or match.group(2) != expected:
            raise ValueError(f"TPC-H checksum is invalid for {expected}")
        result[expected] = match.group(1)
    if len(lines) != len(QUERIES) or tuple(result) != QUERIES:
        raise ValueError("TPC-H checksums must contain Q1-Q22 exactly once")
    return result


def validate_dataset(path: Path) -> dict[str, Any]:
    manifest = path / "manifest.sha256"
    metadata_path = path / "manifest.json"
    expected_paths = tuple(f"{table}/part-00000.parquet" for table in TABLES)
    records = []
    for line in manifest.read_text(encoding="utf-8").splitlines():
        match = re.fullmatch(r"([0-9a-f]{64})  ([a-z]+/part-00000\.parquet)", line)
        if match is None:
            raise ValueError("TPC-H dataset manifest is malformed")
        records.append(match.group(2))
    if tuple(sorted(records)) != tuple(sorted(expected_paths)):
        raise ValueError("TPC-H dataset manifest does not contain the eight SF1 tables")
    metadata = read_json(metadata_path)
    if metadata != {
        "duckdb": "1.4.3",
        "scale_factor": "1",
        "compression": "zstd:3",
        "row_group_size": 122880,
    }:
        raise ValueError("TPC-H dataset metadata is not the pinned SF1 layout")
    return {
        "manifest": str(manifest),
        "manifest_sha256": sha256(manifest),
        "metadata": str(metadata_path),
        "metadata_sha256": sha256(metadata_path),
    }


def validate_provenance(
    value: dict[str, Any],
    medium: str,
    accepted_commit: str,
    workspace: Path,
    manifest_sha256: str,
) -> str:
    expected_root = "data/tpch-sf1" if medium == "local" else "s3://rustdb-tests/tpch-sf1"
    source = value.get("source", {})
    binary = value.get("binary", {})
    dataset = value.get("dataset", {})
    configuration = value.get("configuration", {})
    if (
        value.get("format_version") != 1
        or not re.fullmatch(r"[0-9a-f]{40}", accepted_commit or "")
        or source != {"git_commit": accepted_commit, "worktree_dirty": False}
        or binary.get("path") != "target/release/rustdb"
        or binary.get("build_skipped") != (medium == "minio")
        or re.fullmatch(r"[0-9a-f]{64}", binary.get("sha256", "")) is None
        or dataset.get("reference_manifest_sha256") != manifest_sha256
        or dataset.get("rustdb_root") != expected_root
        or dataset.get("rustdb_manifest_sha256")
        != (manifest_sha256 if medium == "local" else None)
    ):
        raise ValueError(f"TPC-H {medium} provenance is not commit/dataset bound")
    expected_configuration = {
        "memory_limit_bytes": MEMORY_LIMIT,
        "compute_threads": THREADS,
        "batch_size": BATCH_SIZE,
        "io_concurrency": IO_CONCURRENCY,
        "require_spill": False,
        "spill_directory": None,
        "s3_endpoint": None if medium == "local" else "http://minio:9000",
        "s3_region": None if medium == "local" else "us-east-1",
        "s3_path_style": None if medium == "local" else "1",
    }
    if configuration != expected_configuration:
        raise ValueError(f"TPC-H {medium} execution configuration changed")
    validate_queries(value.get("queries", {}), workspace, medium)
    return binary["sha256"]


def validate_queries(value: dict[str, Any], workspace: Path, medium: str) -> None:
    if value != query_contract(workspace, medium):
        raise ValueError(f"TPC-H {medium} query provenance changed")


def query_contract(workspace: Path, medium: str) -> dict[str, Any]:
    query_list = workspace / "benchmarks" / "tpch" / "cases" / f"sf1-{medium}.txt"
    files = []
    combined = hashlib.sha256()
    for identifier in QUERIES:
        relative = f"benchmarks/tpch/{identifier}.sql"
        digest = sha256(workspace / relative)
        files.append({"id": identifier, "path": relative, "sha256": digest})
        combined.update(f"{identifier}  {digest}\n".encode())
    return {
        "list": query_list.relative_to(workspace).as_posix(),
        "list_sha256": sha256(query_list),
        "files_sha256": combined.hexdigest(),
        "files": files,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("capture", choices=("capture",))
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--medium", choices=("local", "minio"), required=True)
    args = parser.parse_args()
    print(json.dumps(capture(args.workspace, args.output, args.medium), separators=(",", ":")))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"beta acceptance TPC-H: {error}", file=sys.stderr)
        raise SystemExit(2)
