#!/usr/bin/env python3
"""Validate explicit Beta inputs and build the runner command."""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from beta_acceptance_common import atomic_json, command, sha256
from beta_acceptance_fixtures import (
    absolute_path,
    clickbench_fixture,
    local_fixture,
    object_fixture,
)


SCHEMA = "rustdb-beta-acceptance-inputs-v2"
RELEASE_VERSION = "1.0.0-beta.3"
NATIVE_EPOCH = 4
CONFIG_SCHEMA = 2
MIN_MEMORY = 16 * 1024**3


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    create = subparsers.add_parser("create-output")
    create.add_argument("--workspace", type=Path, required=True)
    create.add_argument("--output", type=Path, required=True)

    preflight = subparsers.add_parser("preflight")
    preflight.add_argument("--workspace", type=Path, required=True)
    preflight.add_argument("--output", type=Path, required=True)
    for name in (
        "local-fixture",
        "local-format",
        "minio-manifest",
        "minio-format",
        "clickbench-data-dir",
        "clickbench-profile",
    ):
        preflight.add_argument(f"--{name}", required=True)

    runner = subparsers.add_parser("runner-command")
    runner.add_argument("--workspace", type=Path, required=True)
    runner.add_argument("--temporary", type=Path, required=True)
    runner.add_argument("--build-id", required=True)
    runner.add_argument("--memory-limit", required=True)
    runner.add_argument("--fixture", type=Path)
    runner.add_argument("--minio", action="store_true")
    return parser.parse_args()


def within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def create_output(args: argparse.Namespace) -> int:
    workspace = args.workspace.resolve()
    if not args.output.is_absolute():
        raise ValueError("RUSTDB_BETA_ACCEPTANCE_OUTPUT must be an absolute path")
    output = args.output.resolve()
    if within(output, workspace):
        raise ValueError("acceptance evidence must be stored outside the workspace")
    if output.exists():
        raise ValueError(f"acceptance output already exists: {output}")
    output.mkdir(mode=0o700, parents=True)
    for child in ("logs", "reports", "clickbench", "tpch"):
        (output / child).mkdir(mode=0o700)
    print(output)
    return 0


def host_facts() -> dict[str, Any]:
    logical_cpus = os.cpu_count() or 1
    if platform.system() == "Darwin":
        memory = command("sysctl", "-n", "hw.memsize")
        cpu_model = command("sysctl", "-n", "machdep.cpu.brand_string")
    else:
        memory = ""
        cpu_model = platform.processor()
    if not memory.isdigit():
        memory = str(os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE"))
    return {
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "cpu_model": cpu_model or "unknown",
        "logical_cpus": logical_cpus,
        "total_memory_bytes": int(memory),
    }


def git_facts(workspace: Path) -> dict[str, Any]:
    commit = command("git", "-C", str(workspace), "rev-parse", "HEAD")
    if not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError("workspace does not have a full Git commit")
    dirty = command(
        "git", "-C", str(workspace), "status", "--porcelain", "--untracked-files=all"
    )
    return {
        "commit": commit,
        "branch": command(
            "git", "-C", str(workspace), "rev-parse", "--abbrev-ref", "HEAD"
        ),
        "clean": not bool(dirty),
    }


def release_contract(workspace: Path) -> dict[str, Any]:
    cargo = workspace / "Cargo.toml"
    native = workspace / "src" / "storage" / "native" / "format.rs"
    config = workspace / "src" / "bin" / "rustdb" / "server_config.rs"
    server = workspace / "src" / "http_shell" / "server.rs"
    package = re.search(
        r'^version = "([^"]+)"$', cargo.read_text(encoding="utf-8"), re.MULTILINE
    )
    epoch = re.search(
        r"CURRENT_DATABASE_VERSION: u32 = (\d+)",
        native.read_text(encoding="utf-8"),
    )
    schema = re.search(
        r"CONFIG_SCHEMA_VERSION: u32 = (\d+)",
        config.read_text(encoding="utf-8"),
    )
    routes = server.read_text(encoding="utf-8")
    actual = (
        package.group(1) if package else None,
        int(epoch.group(1)) if epoch else None,
        int(schema.group(1)) if schema else None,
    )
    expected = (RELEASE_VERSION, NATIVE_EPOCH, CONFIG_SCHEMA)
    if actual != expected:
        raise ValueError(
            "Beta 2 release boundary differs from "
            f"version={expected[0]}, Native epoch={expected[1]}, config schema={expected[2]}"
        )
    if '"/v2/info"' not in routes or '"/v2/queries"' not in routes or '"/v1/' in routes:
        raise ValueError("Beta 2 HTTP routes must expose /v2 and must not expose /v1")
    return {
        "version": RELEASE_VERSION,
        "native_epoch": NATIVE_EPOCH,
        "config_schema": CONFIG_SCHEMA,
        "http_api": "v2",
        "source_sha256": {
            "cargo": sha256(cargo),
            "native_format": sha256(native),
            "service_config": sha256(config),
            "http_server": sha256(server),
        },
    }


def preflight(args: argparse.Namespace) -> int:
    output = args.output.resolve()
    result: dict[str, Any] = {
        "schema": SCHEMA,
        "preflight_complete": False,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "workspace": str(args.workspace.resolve()),
    }
    try:
        values = {
            "local fixture": args.local_fixture,
            "local format": args.local_format,
            "MinIO manifest": args.minio_manifest,
            "MinIO format": args.minio_format,
            "ClickBench data directory": args.clickbench_data_dir,
            "ClickBench profile": args.clickbench_profile,
        }
        missing = [name for name, value in values.items() if not value]
        if missing:
            raise ValueError("missing explicit inputs: " + ", ".join(missing))
        host = host_facts()
        result["host"] = host
        result["profile"] = acceptance_profile()
        result["release"] = release_contract(args.workspace.resolve())
        if host["logical_cpus"] < 4 or host["total_memory_bytes"] < MIN_MEMORY:
            raise ValueError("Beta acceptance requires at least 4 logical CPUs and 16 GiB RAM")
        result["git"] = git_facts(args.workspace.resolve())
        if result["git"]["clean"] is not True:
            raise ValueError("Beta acceptance requires a clean, fully committed worktree")
        local_root = absolute_path(args.local_fixture, "local fixture", "directory")
        minio_manifest = absolute_path(args.minio_manifest, "MinIO manifest", "file")
        clickbench_root = absolute_path(
            args.clickbench_data_dir,
            "ClickBench data directory",
            "directory",
        )
        result["local"] = local_fixture(
            local_root,
            output,
            args.local_format,
        )
        result["minio"] = object_fixture(
            minio_manifest,
            output,
            args.minio_format,
        )
        result["clickbench"] = clickbench_fixture(
            clickbench_root,
            args.clickbench_profile,
            args.workspace.resolve()
            / "benchmarks"
            / "clickbench"
            / "functional-oracle-v2.json",
        )
        result["preflight_complete"] = True
        atomic_json(output / "inputs.json", result)
        return 0
    except Exception as error:
        result["error"] = str(error)
        atomic_json(output / "inputs.json", result)
        print(f"beta acceptance preflight: {error}", file=sys.stderr)
        return 2


def acceptance_profile() -> dict[str, Any]:
    return {
        "class": "4c16g-or-higher",
        "threads": 4,
        "concurrency": 8,
        "batch_size": 8192,
        "memory_limits_bytes": [2 * 1024**3, 4 * 1024**3],
        "warmup": 0,
        "iterations": 1,
    }


def runner_command(args: argparse.Namespace) -> int:
    value = [
        "docker",
        "compose",
        "--project-directory",
        str(args.workspace.resolve()),
        "run",
        "--rm",
        "--no-deps",
        "--no-TTY",
    ]
    if args.fixture is not None:
        value.extend(("--volume", f"{args.fixture.resolve()}:/beta-data:ro"))
    value.extend(
        (
            "--volume",
            f"{args.temporary.resolve()}:/bench-tmp",
            "dev",
            "/workspace/target/release/rustdb-v07-runner",
            "--threads",
            "4",
            "--memory-limit",
            args.memory_limit,
            "--concurrency",
            "8",
            "--batch-size",
            "8192",
            "--metadata-cache-bytes",
            "0",
            "--spill-directory",
            "/bench-tmp/spill",
            "--build-id",
            args.build_id,
        )
    )
    if args.minio:
        value.extend(
            (
                "--s3-endpoint",
                "http://minio:9000",
                "--s3-region",
                "us-east-1",
                "--s3-path-style",
                "--s3-allow-http",
            )
        )
    print(json.dumps(value, separators=(",", ":")))
    return 0


def main() -> int:
    args = arguments()
    if args.command == "create-output":
        return create_output(args)
    if args.command == "preflight":
        return preflight(args)
    return runner_command(args)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"beta acceptance inputs: {error}", file=sys.stderr)
        raise SystemExit(2)
