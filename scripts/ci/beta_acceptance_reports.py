"""Validate reports produced by the Beta acceptance entrypoint."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from beta_acceptance_common import command, read_json, sha256


REPORT_SCHEMA = "rustdb-external-only-diagnostic-v1"
CLICKBENCH_SCHEMA = "rustdb-clickbench-v1"
STEPS = (
    "preflight",
    "orbstack-all",
    "minio-fixture-verify",
    "runner-build",
    "local-2g",
    "local-4g",
    "local-fixture-reverify",
    "minio-2g",
    "minio-4g",
    "minio-fixture-reverify",
    "clickbench",
)
REPORTS = {
    "local-2g": ("local-nvme", 2 * 1024**3),
    "local-4g": ("local-nvme", 4 * 1024**3),
    "minio-2g": ("minio", 2 * 1024**3),
    "minio-4g": ("minio", 4 * 1024**3),
}


def read_steps(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    records = []
    for index, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(),
        start=1,
    ):
        try:
            value = json.loads(line)
        except json.JSONDecodeError as error:
            raise ValueError(f"invalid step record {index}: {error}") from error
        if not isinstance(value, dict):
            raise ValueError(f"invalid step record {index}")
        records.append(value)
    return records


def validate_steps(records: list[dict[str, Any]]) -> None:
    completed: dict[str, int] = {}
    started: set[str] = set()
    for record in records:
        name, phase = record.get("name"), record.get("phase")
        if name not in STEPS or phase not in ("started", "finished"):
            raise ValueError("step journal contains an unknown record")
        if phase == "started":
            if name in started:
                raise ValueError(f"step {name} was started more than once")
            started.add(name)
        else:
            if name not in started or name in completed:
                raise ValueError(f"step {name} has an invalid completion record")
            completed[name] = record.get("exit_code")
    if tuple(name for name in STEPS if name in completed) != STEPS:
        raise ValueError("not every required Beta acceptance step completed")
    failed = [name for name, code in completed.items() if code != 0]
    if failed:
        raise ValueError("Beta acceptance steps failed: " + ", ".join(failed))


def validate_git(workspace: Path, inputs: dict[str, Any]) -> str:
    accepted = inputs.get("git", {}).get("commit")
    current = command("git", "-C", str(workspace), "rev-parse", "HEAD")
    if not re.fullmatch(r"[0-9a-f]{40}", accepted or "") or current != accepted:
        raise ValueError("workspace commit changed during Beta acceptance")
    dirty = command(
        "git", "-C", str(workspace), "status", "--porcelain", "--untracked-files=all"
    )
    if dirty:
        raise ValueError("workspace became dirty during Beta acceptance")
    return current


def report_summary(
    path: Path,
    medium: str,
    memory: int,
    fixture: dict[str, Any],
) -> dict[str, Any]:
    value = read_json(path)
    if value.get("schema") != REPORT_SCHEMA or value.get("diagnostic_only") is not True:
        raise ValueError(f"invalid external report contract: {path}")
    track = fixture.get("format")
    if value.get("storage_medium") != medium or value.get("storage_track") != track:
        raise ValueError(f"external report has the wrong storage profile: {path}")
    if value.get("query", {}).get("sha256") != fixture.get("query", {}).get("sha256"):
        raise ValueError(f"external report used a different SQL file: {path}")
    dataset = value.get("dataset", {})
    expected_discovered_files = fixture.get(
        "files" if medium == "local-nvme" else "objects"
    )
    if (
        dataset.get("path") != fixture.get("manifest")
        or dataset.get("sha256") != fixture.get("manifest_sha256")
        or type(expected_discovered_files) is not int
        or expected_discovered_files < 1
        or dataset.get("expected_discovered_files") != expected_discovered_files
    ):
        raise ValueError(f"external report used a different fixture manifest: {path}")
    expected = {
        "threads": 4,
        "memory_limit_bytes": memory,
        "concurrency": 8,
        "batch_size": 8192,
    }
    if (
        value.get("config") != expected
        or value.get("warmup") != 0
        or value.get("iterations") != 1
    ):
        raise ValueError(f"external report has the wrong execution profile: {path}")
    summary = value.get("summary", {})
    if (
        summary.get("measured_queries") != 8
        or summary.get("terminal_reservation_bytes") != 0
        or summary.get("engine_root_current_reservation_bytes") != 0
        or summary.get("engine_root_memory_limit_bytes") != memory
        or summary.get("discovered_files") != expected_discovered_files
    ):
        raise ValueError(f"external report did not quiesce all eight queries: {path}")
    engine_root_peak = summary.get("engine_root_lifetime_peak_reservation_bytes")
    if type(engine_root_peak) is not int or not 0 <= engine_root_peak <= memory:
        raise ValueError(f"external report exceeded its Engine root memory contract: {path}")
    runs = value.get("runs")
    if not isinstance(runs, list) or len(runs) != 1:
        raise ValueError(f"external report has the wrong measured run count: {path}")
    run = runs[0]
    queries = run.get("queries") if isinstance(run, dict) else None
    if not isinstance(queries, list) or len(queries) != 8:
        raise ValueError(f"external report has the wrong query count: {path}")
    if (
        run.get("engine_root_current_reservation_bytes") != 0
        or run.get("engine_root_lifetime_peak_reservation_bytes") != engine_root_peak
        or run.get("engine_root_memory_limit_bytes") != memory
    ):
        raise ValueError(f"external report has invalid Engine root memory evidence: {path}")
    if any(
        query.get("complete") is not True
        or query.get("current_reservation_bytes") != 0
        or type(query.get("peak_reservation_bytes")) is not int
        or query["peak_reservation_bytes"] < 0
        or query["peak_reservation_bytes"] > engine_root_peak
        or query["peak_reservation_bytes"] > memory
        or query.get("discovered_files") != expected_discovered_files
        for query in queries
        if isinstance(query, dict)
    ) or not all(isinstance(query, dict) for query in queries):
        raise ValueError(f"external report retained resources: {path}")
    checksum = summary.get("checksum")
    build_id = value.get("hello", {}).get("build_id")
    if not re.fullmatch(r"[0-9a-f]{64}", checksum or ""):
        raise ValueError(f"external report has an invalid checksum: {path}")
    if not re.fullmatch(r"[0-9a-f]{64}", build_id or ""):
        raise ValueError(f"external report has an invalid runner build ID: {path}")
    if {query.get("checksum") for query in queries} != {checksum}:
        raise ValueError(f"external report query checksums differ: {path}")
    peak_rss = summary.get("peak_rss_bytes")
    if type(peak_rss) is not int or peak_rss > memory:
        raise ValueError(f"external report exceeded its RSS contract: {path}")
    return {
        "path": str(path),
        "report_sha256": sha256(path),
        "storage_medium": medium,
        "storage_track": track,
        "memory_limit_bytes": memory,
        "measured_queries": 8,
        "checksum": checksum,
        "runner_build_id": build_id,
        "peak_rss_bytes": peak_rss,
        "memory_headroom_bytes": summary.get("memory_headroom_bytes"),
        "terminal_reservation_bytes": 0,
        "engine_root_current_reservation_bytes": 0,
        "engine_root_lifetime_peak_reservation_bytes": engine_root_peak,
        "engine_root_memory_limit_bytes": memory,
        "discovered_files": expected_discovered_files,
    }


def clickbench_summary(
    path: Path,
    inputs: dict[str, Any],
    commit: str,
) -> dict[str, Any]:
    value = read_json(path)
    if value.get("schema") != CLICKBENCH_SCHEMA:
        raise ValueError("ClickBench manifest has the wrong schema")
    if (
        value.get("complete") is not True
        or value.get("mode") != "execute"
        or value.get("binary_as_string") is not True
        or value.get("query_count") != 43
        or value.get("passed") != 43
        or value.get("failed") != 0
    ):
        raise ValueError("ClickBench did not pass all 43 queries")
    results = value.get("results")
    if not isinstance(results, list) or len(results) != 43:
        raise ValueError("ClickBench manifest has the wrong result count")
    expected = inputs["clickbench"]
    expected_oracle = expected["oracle"]
    expected_results = expected_oracle.get("results")
    if not isinstance(expected_results, list) or len(expected_results) != 43:
        raise ValueError("ClickBench preflight oracle has the wrong result count")
    for number, (result, oracle_result) in enumerate(
        zip(results, expected_results), start=1
    ):
        if (
            not isinstance(result, dict)
            or not isinstance(oracle_result, dict)
            or result.get("query") != number
            or oracle_result.get("query") != number
            or result.get("status") != "passed"
            or result.get("terminal_query_reservation_bytes") != 0
            or result.get("terminal_engine_reservation_bytes") != 0
            or result.get("spill_cleaned") is not True
            or result.get("checksum_algorithm")
            != expected_oracle.get("checksum_algorithm")
            or result.get("result_rows") != oracle_result.get("rows")
            or result.get("result_checksum_sha256") != oracle_result.get("checksum")
            or result.get("oracle_expected_rows") != oracle_result.get("rows")
            or result.get("oracle_expected_checksum_sha256")
            != oracle_result.get("checksum")
            or result.get("oracle_match") is not True
            or result.get("oracle_error") is not None
        ):
            raise ValueError(f"ClickBench query {number} differs from its typed oracle")
    dataset = value.get("dataset", {})
    queries = value.get("queries", {})
    oracle = value.get("oracle", {})
    if (
        dataset.get("profile") != expected["profile"]
        or dataset.get("bytes") != expected["data"]["bytes"]
        or dataset.get("sha256") != expected["data"]["sha256"]
        or dataset.get("expected_sha256") != expected["data"]["expected_sha256"]
        or dataset.get("identity_verified") is not True
        or dataset.get("expected_source_etag")
        != expected["data"]["expected_source_etag"]
        or queries.get("sha256") != expected["query"]["sha256"]
        or queries.get("expected_sha256") != expected["query"]["expected_sha256"]
        or queries.get("identity_verified") is not True
        or oracle.get("schema") != expected_oracle["schema"]
        or oracle.get("profile") != expected_oracle["profile"]
        or oracle.get("mode") != expected_oracle["mode"]
        or oracle.get("query_count") != expected_oracle["query_count"]
        or oracle.get("sha256") != expected_oracle["sha256"]
        or oracle.get("expected_sha256") != expected_oracle["expected_sha256"]
        or oracle.get("identity_verified") is not True
        or expected_oracle.get("identity_verified") is not True
        or oracle.get("checksum_algorithm")
        != expected_oracle["checksum_algorithm"]
        or oracle.get("query_sha256") != expected["query"]["sha256"]
        or oracle.get("dataset_sha256") != expected["data"]["sha256"]
    ):
        raise ValueError("ClickBench used a different fixture or oracle")
    resource = value.get("resource_contract", {})
    if (
        resource.get("engine_threads") != 4
        or resource.get("engine_memory_limit_bytes") != 4 * 1024**3
    ):
        raise ValueError("ClickBench used a different CPU or engine-memory profile")
    if value.get("build", {}).get("id") != commit:
        raise ValueError("ClickBench build is not bound to the accepted commit")
    acceptance = value.get("acceptance_summary", {})
    required_true = (
        "all_terminal_query_reservations_zero",
        "all_terminal_engine_reservations_zero",
        "all_spill_cleaned",
    )
    if not all(acceptance.get(field) is True for field in required_true):
        raise ValueError("ClickBench left reservations or Spill state behind")
    return {
        "path": str(path),
        "manifest_sha256": sha256(path),
        "profile": dataset["profile"],
        "dataset_bytes": dataset["bytes"],
        "dataset_sha256": dataset["sha256"],
        "query_sha256": queries["sha256"],
        "oracle_sha256": oracle["sha256"],
        "checksum_algorithm": oracle["checksum_algorithm"],
        "oracle_matches": 43,
        "identity_verified": True,
        "passed": 43,
        "failed": 0,
        "resource_contract": resource,
        "acceptance_summary": acceptance,
    }


def existing_summaries(
    output: Path,
    inputs: dict[str, Any],
) -> dict[str, Any]:
    summaries: dict[str, Any] = {}
    for name, (medium, memory) in REPORTS.items():
        path = output / "reports" / f"{name}.json"
        if not path.is_file():
            continue
        fixture = inputs.get(
            "local" if medium == "local-nvme" else "minio",
            {},
        )
        try:
            summaries[name] = report_summary(path, medium, memory, fixture)
        except Exception as error:
            summaries[name] = {"path": str(path), "validation_error": str(error)}
    return summaries
