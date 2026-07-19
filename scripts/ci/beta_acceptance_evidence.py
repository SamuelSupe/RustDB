#!/usr/bin/env python3
"""Record Beta gate steps and seal one auditable acceptance result."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from beta_acceptance_clickbench import (
    BATCH_SIZE as CLICKBENCH_BATCH_SIZE,
    CONTAINER_CPUS as CLICKBENCH_CONTAINER_CPUS,
    CONTAINER_MEMORY_BYTES as CLICKBENCH_CONTAINER_MEMORY_BYTES,
    ENGINE_MEMORY_BYTES as CLICKBENCH_ENGINE_MEMORY_BYTES,
    ENGINE_THREADS as CLICKBENCH_ENGINE_THREADS,
    IO_CONCURRENCY as CLICKBENCH_IO_CONCURRENCY,
    METADATA_CACHE_BYTES as CLICKBENCH_METADATA_CACHE_BYTES,
    REPORT_COUNT as CLICKBENCH_REPORT_COUNT,
)
from beta_acceptance_common import atomic_json, read_json
from beta_acceptance_local import SCHEMA as LOCAL_VERIFICATION_SCHEMA
from beta_acceptance_minio import SCHEMA as MINIO_VERIFICATION_SCHEMA
from beta_acceptance_reports import (
    REPORTS,
    STEPS,
    clickbench_summary,
    existing_summaries,
    read_steps,
    report_summary,
    validate_git,
    validate_steps,
)


SCHEMA = "rustdb-beta-acceptance-v1"


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    step = subparsers.add_parser("record-step")
    step.add_argument("--file", type=Path, required=True)
    step.add_argument("--name", choices=STEPS, required=True)
    step.add_argument("--phase", choices=("started", "finished"), required=True)
    step.add_argument("--at", required=True)
    step.add_argument("--log")
    step.add_argument("--exit-code", type=int)

    finalize = subparsers.add_parser("finalize")
    finalize.add_argument("--workspace", type=Path, required=True)
    finalize.add_argument("--output", type=Path, required=True)
    finalize.add_argument("--status", choices=("passed", "failed"), required=True)
    finalize.add_argument("--exit-code", type=int, required=True)
    finalize.add_argument("--failed-step", default="")
    finalize.add_argument("--started-at", required=True)
    return parser.parse_args()


def record_step(args: argparse.Namespace) -> int:
    if args.phase == "finished" and args.exit_code is None:
        raise ValueError("finished step requires --exit-code")
    record: dict[str, Any] = {
        "name": args.name,
        "phase": args.phase,
        "at_utc": args.at,
    }
    if args.log:
        record["log"] = args.log
    if args.exit_code is not None:
        record["exit_code"] = args.exit_code
    args.file.parent.mkdir(parents=True, exist_ok=True)
    with args.file.open("a", encoding="utf-8") as target:
        target.write(json.dumps(record, separators=(",", ":")) + "\n")
        target.flush()
        os.fsync(target.fileno())
    os.chmod(args.file, 0o600)
    return 0


def accepted_reports(output: Path, inputs: dict[str, Any]) -> dict[str, Any]:
    return {
        name: report_summary(
            output / "reports" / f"{name}.json",
            medium,
            memory,
            inputs["local" if medium == "local-nvme" else "minio"],
        )
        for name, (medium, memory) in REPORTS.items()
    }


def validate_consistency(reports: dict[str, dict[str, Any]]) -> None:
    if len({report["checksum"] for report in reports.values()}) != 1:
        raise ValueError("local and MinIO results differ across the 2/4-GiB runs")
    if len({report["runner_build_id"] for report in reports.values()}) != 1:
        raise ValueError("external runs used different runner builds")


def minio_verification(output: Path, inputs: dict[str, Any]) -> dict[str, Any]:
    expected = inputs["minio"]
    values = {
        phase: read_json(output / f"minio-{phase}-verification.json")
        for phase in ("before", "after")
    }
    for phase, value in values.items():
        if (
            value.get("schema") != MINIO_VERIFICATION_SCHEMA
            or value.get("root_uri") != expected.get("root_uri")
            or value.get("objects") != expected.get("objects")
            or value.get("bytes") != expected.get("bytes")
            or value.get("manifest_sha256") != expected.get("manifest_sha256")
        ):
            raise ValueError(f"MinIO {phase} verification differs from preflight")
    if values["before"].get("inventory_sha256") != values["after"].get(
        "inventory_sha256"
    ):
        raise ValueError("MinIO inventory changed during Beta acceptance")
    return values


def local_verification(output: Path, inputs: dict[str, Any]) -> dict[str, Any]:
    expected = inputs["local"]
    value = read_json(output / "local-after-verification.json")
    if (
        value.get("schema") != LOCAL_VERIFICATION_SCHEMA
        or value.get("source_root") != expected.get("root")
        or value.get("files") != expected.get("files")
        or value.get("bytes") != expected.get("bytes")
        or value.get("inventory_sha256") != expected.get("inventory_sha256")
        or value.get("manifest_sha256") != expected.get("manifest_sha256")
    ):
        raise ValueError("local fixture verification differs from preflight")
    return value


def execution_contract() -> dict[str, Any]:
    return {
        "default_gate": ["scripts/ci/orbstack.sh", "all"],
        "external_runner": "benchmarks/v07/rustdb_external_only.py",
        "external_runs": {
            name: {
                "storage_medium": medium,
                "memory_limit_bytes": memory,
                "threads": 4,
                "concurrency": 8,
                "batch_size": 8192,
                "warmup": 0,
                "iterations": 1,
                "query_timeout_seconds": 3600,
            }
            for name, (medium, memory) in REPORTS.items()
        },
        "clickbench": {
            "runner": "benchmarks/clickbench/run.sh",
            "offline": True,
            "container_cpus": CLICKBENCH_CONTAINER_CPUS,
            "container_memory_bytes": CLICKBENCH_CONTAINER_MEMORY_BYTES,
            "threads": CLICKBENCH_ENGINE_THREADS,
            "engine_memory_limit_bytes": CLICKBENCH_ENGINE_MEMORY_BYTES,
            "batch_size": CLICKBENCH_BATCH_SIZE,
            "io_concurrency": CLICKBENCH_IO_CONCURRENCY,
            "metadata_cache_bytes": CLICKBENCH_METADATA_CACHE_BYTES,
            "raw_reports": CLICKBENCH_REPORT_COUNT,
            "build_id": "accepted_commit",
            "binary_sha256_required": True,
            "passes": 1,
        },
    }


def finalize(args: argparse.Namespace) -> int:
    output = args.output.resolve()
    inputs_path = output / "inputs.json"
    input_error = None
    step_error = None
    try:
        inputs = read_json(inputs_path) if inputs_path.is_file() else {}
    except Exception as error:
        inputs = {}
        input_error = str(error)
    try:
        records = read_steps(output / "steps.jsonl")
    except Exception as error:
        records = []
        step_error = str(error)
    evidence: dict[str, Any] = {
        "schema": SCHEMA,
        "status": args.status,
        "exit_code": args.exit_code,
        "failed_step": args.failed_step or None,
        "started_at_utc": args.started_at,
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        "comparison_claim": False,
        "execution_contract": execution_contract(),
        "inputs": inputs,
        "steps": records,
        "reports": existing_summaries(output, inputs),
    }
    if input_error:
        evidence["input_journal_error"] = input_error
    if step_error:
        evidence["step_journal_error"] = step_error
    final_error: str | None = None
    if args.status == "passed":
        try:
            if input_error or step_error:
                raise ValueError("acceptance input or step journal is unreadable")
            if inputs.get("preflight_complete") is not True:
                raise ValueError("fixture preflight was not completed")
            validate_steps(records)
            commit = validate_git(args.workspace.resolve(), inputs)
            reports = accepted_reports(output, inputs)
            validate_consistency(reports)
            local = local_verification(output, inputs)
            minio = minio_verification(output, inputs)
            clickbench = clickbench_summary(
                output / "clickbench" / "beta-acceptance" / "manifest.json",
                inputs,
                commit,
            )
            evidence.update(
                {
                    "accepted_commit": commit,
                    "reports": reports,
                    "local_verification": local,
                    "minio_verification": minio,
                    "clickbench": clickbench,
                }
            )
        except Exception as error:
            final_error = str(error)
            evidence.update(
                {
                    "status": "failed",
                    "exit_code": 1,
                    "failed_step": "finalization",
                    "finalization_error": final_error,
                }
            )
    else:
        clickbench_path = output / "clickbench" / "beta-acceptance" / "manifest.json"
        if clickbench_path.is_file():
            evidence["clickbench_manifest"] = str(clickbench_path)
    atomic_json(output / "evidence.json", evidence)
    print(output / "evidence.json")
    if final_error:
        print(f"beta acceptance finalization: {final_error}", file=sys.stderr)
        return 1
    return 0


def main() -> int:
    args = arguments()
    return record_step(args) if args.command == "record-step" else finalize(args)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"beta acceptance evidence: {error}", file=sys.stderr)
        raise SystemExit(1)
