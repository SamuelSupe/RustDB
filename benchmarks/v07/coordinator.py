#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import selectors
import statistics
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any

from contract import CONTRACT_VERSION, validate_report
from host_facts import collect as collect_host_facts

NATIVE_ROUNDS = 10
NATIVE_STORAGE_MULTIPLIER = 2
NATIVE_TABLE_METADATA_BYTES = 65_536
NATIVE_HARNESS_BYTES = 1_048_576


class Worker:
    def __init__(self, name: str, command: list[str]) -> None:
        self.name = name
        self.stderr = tempfile.TemporaryFile(mode="w+t", encoding="utf-8")
        self.process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self.stderr,
            text=True,
            bufsize=1,
        )
        try:
            self.hello = self._read(120)
        except BaseException:
            if self.process.poll() is None:
                self.process.kill()
                self.process.wait()
            self.stderr.close()
            raise
        if self.hello.get("kind") != "hello" or self.hello.get("engine") != name:
            self.close()
            raise RuntimeError(f"{name} worker did not return a valid hello: {self.hello!r}")

    def run(self, command: dict[str, Any], timeout: int = 300) -> dict[str, Any]:
        if self.process.stdin is None:
            raise RuntimeError(f"{self.name} worker stdin is closed")
        self.process.stdin.write(json.dumps(command, separators=(",", ":")) + "\n")
        self.process.stdin.flush()
        response = self._read(timeout)
        if response.get("kind") == "error":
            raise RuntimeError(f"{self.name} worker error: {response.get('message')}")
        return response

    def close(self) -> None:
        if self.process.poll() is None and self.process.stdin is not None:
            try:
                self.process.stdin.write('{"command":"shutdown"}\n')
                self.process.stdin.flush()
                self.process.wait(timeout=10)
            except (BrokenPipeError, subprocess.TimeoutExpired):
                self.process.kill()
                self.process.wait()
        self.stderr.close()

    def _read(self, timeout: int) -> dict[str, Any]:
        if self.process.stdout is None:
            raise RuntimeError(f"{self.name} worker stdout is closed")
        selector = selectors.DefaultSelector()
        selector.register(self.process.stdout, selectors.EVENT_READ)
        ready = selector.select(timeout)
        selector.close()
        if not ready:
            self._raise_failure(f"timed out after {timeout}s")
        line = self.process.stdout.readline()
        if not line:
            self._raise_failure("exited without a response")
        try:
            return json.loads(line)
        except json.JSONDecodeError as error:
            self._raise_failure(f"returned invalid JSON: {error}: {line!r}")
        raise AssertionError("unreachable")

    def _raise_failure(self, reason: str) -> None:
        self.stderr.seek(0)
        details = self.stderr.read().strip()
        raise RuntimeError(f"{self.name} worker {reason}; stderr={details!r}")


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a small fair RustDB/DuckDB comparison")
    parser.add_argument("--query", type=Path, required=True)
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--storage-track", choices=("csv", "parquet", "native"), required=True)
    parser.add_argument("--storage-medium", choices=("local-nvme", "minio"), required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--threads", type=positive, default=4)
    parser.add_argument("--memory-limit", type=positive, default=2_147_483_648)
    parser.add_argument("--concurrency", type=positive, default=1)
    parser.add_argument("--batch-size", type=positive, default=8192)
    parser.add_argument("--warmup", type=nonnegative, default=1)
    parser.add_argument("--iterations", type=positive, default=2)
    parser.add_argument(
        "--diagnostic-only",
        action="store_true",
        help="allow short non-gate runs; report is not release evidence",
    )
    parser.add_argument("--native-manifest", type=Path)
    parser.add_argument("--max-native-workspace-bytes", type=positive)
    parser.add_argument("--rustdb-command-json", required=True)
    parser.add_argument("--duckdb-command-json", required=True)
    return parser.parse_args()


def positive(value: str) -> int:
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def nonnegative(value: str) -> int:
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return number


def main() -> int:
    args = arguments()
    validate_native_args(args)
    rustdb_command = command(args.rustdb_command_json, "rustdb")
    duckdb_command = command(args.duckdb_command_json, "duckdb")
    commands = {"rustdb": rustdb_command, "duckdb": duckdb_command}
    sql = args.query.read_text(encoding="utf-8")
    native = native_setup(args) if args.storage_track == "native" else None
    dataset = native["dataset"] if native is not None else path_facts(args.dataset)
    workers: dict[str, Worker] = {}
    setups: dict[str, dict[str, Any]] = {}
    try:
        workers = start_workers(commands)
        if native is not None:
            for engine in ("rustdb", "duckdb"):
                verify_native_sources(args, native)
                response = workers[engine].run(native["command"], timeout=3_600)
                verify_native_sources(args, native)
                validate_setup_response(response, engine, native["setup_id"])
                setups[engine] = response
            close_workers(workers)
            workers = start_workers(commands)
        for warmup in range(args.warmup):
            for engine in ("rustdb", "duckdb"):
                workers[engine].run(
                    run_command(f"warmup-{warmup}-{engine}", sql, args.storage_track, 0)
                )
        runs = {"rustdb": [], "duckdb": []}
        orders = []
        for iteration in range(args.iterations):
            order = ["rustdb", "duckdb"] if iteration % 2 == 0 else ["duckdb", "rustdb"]
            orders.append(order)
            for position, engine in enumerate(order):
                runs[engine].append(
                    workers[engine].run(
                        run_command(
                            f"measured-{iteration}-{engine}",
                            sql,
                            args.storage_track,
                            position,
                            None if native is None else native["setup_id"],
                        )
                    )
                )
        report = {
            "contract_version": CONTRACT_VERSION,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "host": collect_host_facts(),
            "storage_track": args.storage_track,
            "storage_medium": args.storage_medium,
            "diagnostic_only": args.diagnostic_only,
            "comparison_gate_eligible": not args.diagnostic_only,
            "warmup": args.warmup,
            "iterations": args.iterations,
            "config": {
                "threads": args.threads,
                "memory_limit_bytes": args.memory_limit,
                "concurrency": args.concurrency,
                "batch_size": args.batch_size,
            },
            "cache_state": {
                "os_page_cache": "warm-uncontrolled",
                "metadata_cache": "disabled",
                "duckdb_external_file_cache": "disabled",
            },
            "dataset": dataset,
            "query": file_facts(args.query),
            "engine_order": orders,
            "engines": {
                engine: ({
                    "hello": workers[engine].hello,
                    "summary": summary(engine_runs, setups.get(engine)),
                    "runs": engine_runs,
                } | ({"setup": setups[engine]} if native is not None else {}))
                for engine, engine_runs in runs.items()
            },
        }
        if native is not None:
            report["native_setup"] = native["report"]
        validate_report(report)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        temporary = args.output.with_suffix(args.output.suffix + ".tmp")
        temporary.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        os.replace(temporary, args.output)
        print(args.output)
        return 0
    finally:
        for worker in workers.values():
            worker.close()


def run_command(
    run_id: str,
    sql: str,
    track: str,
    order: int,
    setup_id: str | None = None,
) -> dict[str, Any]:
    value = {
        "command": "run",
        "run_id": run_id,
        "sql": sql,
        "storage_track": track,
        "engine_order": order,
    }
    if setup_id is not None:
        value["setup_id"] = setup_id
    return value


def start_workers(commands: dict[str, list[str]]) -> dict[str, Worker]:
    workers = {}
    try:
        for engine in ("rustdb", "duckdb"):
            workers[engine] = Worker(engine, commands[engine])
        return workers
    except BaseException:
        close_workers(workers)
        raise


def close_workers(workers: dict[str, Worker]) -> None:
    for worker in workers.values():
        worker.close()
    workers.clear()


def validate_setup_response(
    response: dict[str, Any], engine: str, setup_id: str
) -> None:
    if (
        response.get("kind") != "setup"
        or response.get("engine") != engine
        or response.get("setup_id") != setup_id
        or response.get("complete") is not True
    ):
        raise RuntimeError(f"{engine} returned an invalid setup response: {response!r}")


def validate_native_args(args: argparse.Namespace) -> None:
    if args.storage_track == "native":
        if args.native_manifest is None or args.max_native_workspace_bytes is None:
            raise ValueError(
                "native track requires --native-manifest and --max-native-workspace-bytes"
            )
        if args.warmup != 0 or (
            not args.diagnostic_only and args.iterations != NATIVE_ROUNDS
        ):
            raise ValueError(
                "native track requires --warmup 0 and --iterations 10, "
                "unless --diagnostic-only is set"
            )
    elif args.native_manifest is not None or args.max_native_workspace_bytes is not None:
        raise ValueError("native setup options require --storage-track native")


def native_setup(args: argparse.Namespace) -> dict[str, Any]:
    manifest = load_native_manifest(args.native_manifest, args.dataset)
    statements = manifest["statements"]
    dataset = manifest["dataset"]
    statements_sha256 = compact_sha(statements)
    identity = {
        "source_sha256": dataset["sha256"],
        "statements_sha256": statements_sha256,
    }
    setup_id = compact_sha(identity, sort_keys=True)
    command_value = {
        "command": "setup",
        "setup_id": setup_id,
        "storage_track": "native",
        "source_sha256": dataset["sha256"],
        "source_bytes": dataset["bytes"],
        "source_files": manifest["source_files"],
        "statements_sha256": statements_sha256,
        "statements": statements,
        "max_storage_bytes": args.max_native_workspace_bytes,
    }
    maximum = native_storage_maximum(dataset["bytes"], len(statements))
    if args.max_native_workspace_bytes > maximum:
        raise ValueError(
            "--max-native-workspace-bytes exceeds the Native 2x plus bounded "
            f"metadata limit of {maximum} bytes"
        )
    report_value = {key: value for key, value in command_value.items() if key != "command"}
    report_value["setup_engine_order"] = ["rustdb", "duckdb"]
    return {
        "setup_id": setup_id,
        "dataset": dataset,
        "command": command_value,
        "report": report_value,
        "source_files": manifest["source_files"],
    }


def verify_native_sources(args: argparse.Namespace, expected: dict[str, Any]) -> None:
    current = load_native_manifest(args.native_manifest, args.dataset)
    if (
        current["source_files"] != expected["source_files"]
        or current["dataset"] != expected["dataset"]
        or current["statements"] != expected["command"]["statements"]
    ):
        raise RuntimeError("native source Parquet files changed during benchmark setup")


def native_storage_maximum(source_bytes: int, table_count: int) -> int:
    return (
        source_bytes * NATIVE_STORAGE_MULTIPLIER
        + table_count * NATIVE_TABLE_METADATA_BYTES
        + NATIVE_HARNESS_BYTES
    )


def load_native_manifest(path: Path, dataset_root: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if (
        not isinstance(value, dict)
        or set(value) != {"version", "source_root", "tables"}
        or value.get("version") != 1
    ):
        raise ValueError("native manifest version must be 1")
    source_root = value["source_root"]
    if (
        not isinstance(source_root, str)
        or not source_root
        or "\x00" in source_root
        or "://" in source_root
        or "\\" in source_root
        or any(character in source_root for character in "*?[]")
        or not is_lexically_normal_absolute_posix(source_root)
    ):
        raise ValueError(
            "native manifest source_root must be a lexically-normal absolute POSIX path"
        )
    if not dataset_root.is_dir():
        raise ValueError("native dataset must be a directory")
    resolved_root = dataset_root.resolve()
    tables = value.get("tables")
    if not isinstance(tables, list) or not tables:
        raise ValueError("native manifest tables must be a non-empty array")
    normalized: list[tuple[str, list[dict[str, Any]]]] = []
    names = set()
    source_files: list[dict[str, Any]] = []
    selected_paths: set[Path] = set()
    for index, table in enumerate(tables):
        if not isinstance(table, dict) or set(table) != {"name", "path"}:
            raise ValueError(f"native manifest table {index} must contain name and path")
        name = table["name"]
        pattern = table["path"]
        if not isinstance(name, str) or not name or "\x00" in name:
            raise ValueError(f"native manifest table {index} has an invalid name")
        if not isinstance(pattern, str) or not pattern or "\x00" in pattern:
            raise ValueError(f"native manifest table {index} has an invalid path")
        parsed = PurePosixPath(pattern)
        if parsed.is_absolute() or ".." in parsed.parts or "\\" in pattern:
            raise ValueError(f"native manifest table {index} path escapes the dataset")
        folded = name.casefold()
        if folded in names:
            raise ValueError(f"native manifest contains duplicate table name {name!r}")
        names.add(folded)
        matches = sorted(child for child in dataset_root.glob(pattern) if child.is_file())
        if not matches:
            raise ValueError(f"native manifest table {name!r} path matched no files")
        for child in matches:
            resolved = child.resolve()
            try:
                resolved.relative_to(resolved_root)
            except ValueError as error:
                raise ValueError(
                    f"native manifest table {name!r} path escapes the dataset"
                ) from error
            if child.suffix.lower() != ".parquet":
                raise ValueError(f"native manifest table {name!r} matched a non-Parquet file")
            if resolved in selected_paths:
                raise ValueError(f"native manifest source file is selected more than once: {child}")
            selected_paths.add(resolved)
        table_files = []
        for child in matches:
            relative_path = child.relative_to(dataset_root).as_posix()
            if any(character in relative_path for character in "*?[]"):
                raise ValueError(
                    f"native source file path contains wildcard syntax: {relative_path}"
                )
            size, digest = hash_file(child)
            source = {
                "relative_path": relative_path,
                "location": join_location(source_root, relative_path),
                "bytes": size,
                "sha256": digest.hex(),
            }
            table_files.append(source)
            source_files.append(source)
        normalized.append((name, table_files))
    source_files.sort(key=lambda source: source["relative_path"])
    statements = [
        create_native_statement(name, [source["location"] for source in files])
        for name, files in sorted(normalized, key=lambda item: item[0])
    ]
    return {
        "statements": statements,
        "source_files": source_files,
        "dataset": source_file_dataset(dataset_root, source_files),
    }


def create_native_statement(name: str, locations: list[str]) -> str:
    identifier = name.replace('"', '""')
    reads = " UNION ALL ".join(
        f"SELECT * FROM read_parquet('{location.replace(chr(39), chr(39) * 2)}')"
        for location in locations
    )
    return f'CREATE TABLE "{identifier}" AS {reads}'


def join_location(root: str, path: str) -> str:
    return f"{root.rstrip('/')}/{path}"


def is_lexically_normal_absolute_posix(value: str) -> bool:
    path = PurePosixPath(value)
    return (
        path.is_absolute()
        and path.as_posix() == value
        and ".." not in path.parts
        and "//" not in value
    )


def source_file_dataset(
    dataset_root: Path, source_files: list[dict[str, Any]]
) -> dict[str, Any]:
    digest = hashlib.sha256()
    total = 0
    for source in source_files:
        relative = source["relative_path"].encode()
        size = source["bytes"]
        digest.update(len(relative).to_bytes(8, "little"))
        digest.update(relative)
        digest.update(size.to_bytes(8, "little"))
        digest.update(bytes.fromhex(source["sha256"]))
        total += size
    return {
        "path": str(dataset_root),
        "bytes": total,
        "files": len(source_files),
        "sha256": digest.hexdigest(),
    }


def compact_sha(value: Any, *, sort_keys: bool = False) -> str:
    encoded = json.dumps(
        value,
        separators=(",", ":"),
        ensure_ascii=False,
        sort_keys=sort_keys,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def command(value: str, label: str) -> list[str]:
    parsed = json.loads(value)
    if (
        not isinstance(parsed, list)
        or not parsed
        or not all(isinstance(item, str) for item in parsed)
    ):
        raise ValueError(f"{label} command must be a non-empty JSON string array")
    return parsed


def file_facts(path: Path) -> dict[str, Any]:
    size, digest = hash_file(path)
    return {
        "path": str(path),
        "bytes": size,
        "sha256": digest.hex(),
    }


def path_facts(path: Path) -> dict[str, Any]:
    if path.is_file():
        return file_facts(path)
    if not path.is_dir():
        raise ValueError(f"dataset does not exist: {path}")
    children = sorted(item for item in path.rglob("*") if item.is_file())
    if not children:
        raise ValueError(f"dataset directory is empty: {path}")
    return selected_path_facts(path, children)


def selected_path_facts(path: Path, children: list[Path]) -> dict[str, Any]:
    digest = hashlib.sha256()
    total = 0
    for child in sorted(children):
        relative = child.relative_to(path).as_posix().encode()
        size, child_digest = hash_file(child)
        digest.update(len(relative).to_bytes(8, "little"))
        digest.update(relative)
        digest.update(size.to_bytes(8, "little"))
        digest.update(child_digest)
        total += size
    return {
        "path": str(path),
        "bytes": total,
        "files": len(children),
        "sha256": digest.hexdigest(),
    }


def hash_file(path: Path) -> tuple[int, bytes]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as source:
        while chunk := source.read(8 << 20):
            size += len(chunk)
            digest.update(chunk)
    return size, digest.digest()


def summary(
    runs: list[dict[str, Any]], setup: dict[str, Any] | None = None
) -> dict[str, Any]:
    elapsed = [run["group_elapsed_ms"] for run in runs]
    ttfb = [query["ttfb_ms"] for run in runs for query in run["queries"]]
    steady = elapsed[1:] if len(elapsed) > 1 else elapsed
    value = {
        "p50_elapsed_ms": statistics.median(elapsed),
        "p50_ttfb_ms": statistics.median(ttfb),
        "peak_rss_bytes": max(run["peak_rss_bytes"] for run in runs),
        "peak_rss_delta_bytes": max(
            run["peak_rss_bytes"] - run["rss_baseline_bytes"] for run in runs
        ),
        "mean_throughput_queries_per_second": statistics.mean(
            run["throughput_queries_per_second"] for run in runs
        ),
    }
    if setup is not None:
        query_total = sum(elapsed)
        value.update(
            {
                "load_elapsed_ms": setup["load_elapsed_ms"],
                "query_round_total_ms": query_total,
                "first_post_reopen_elapsed_ms": elapsed[0],
                "steady_state_p50_elapsed_ms": statistics.median(steady),
                "amortized_elapsed_ms": (
                    setup["load_elapsed_ms"] + query_total
                )
                / len(runs),
            }
        )
    return value


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
