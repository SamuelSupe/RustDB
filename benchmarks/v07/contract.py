#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import statistics
from pathlib import Path, PurePosixPath
from typing import Any


CONTRACT_VERSION = "rustdb-v07-benchmark-v3"
HISTORICAL_CONTRACT_VERSION_V2 = "rustdb-v07-benchmark-v2"
HISTORICAL_CONTRACT_VERSION = "rustdb-v07-benchmark-v1"
CHECKSUM = re.compile(r"^[0-9a-f]{64}$")
CURRENT_CHECKSUM_MODE = "multiset-sha256-v2"
HISTORICAL_CHECKSUM_MODE = "multiset-sha256-v1"
CHECKSUM_BACKENDS = {
    "rustdb": "rust-sha256-v2",
    "duckdb": "python-arrow-buffer-sha256-v2",
}
MAX_GATE_RESULT_ROWS = 65_536
TRACKS = {"csv", "parquet", "native"}
STORAGE_MEDIA = {"local-nvme", "minio"}
NATIVE_ROUNDS = 10
NATIVE_STORAGE_MULTIPLIER = 2
NATIVE_TABLE_METADATA_BYTES = 65_536
NATIVE_HARNESS_BYTES = 1_048_576
ENGINE_ROOT_MEMORY_FIELDS = (
    "engine_root_current_reservation_bytes",
    "engine_root_lifetime_peak_reservation_bytes",
    "engine_root_memory_limit_bytes",
)
WORKER_RESOURCE_FIELDS = (
    "cpu_affinity_list",
    "cpu_affinity_count",
    "cgroup_cpuset_cpus_effective",
    "cgroup_cpu_quota_us",
    "cgroup_cpu_period_us",
    "cgroup_memory_max_bytes",
    "visible_memory_bytes",
)
QUERY_PROTOCOL_FIELDS = {
    "query_slot",
    "harness_query_id",
    "start_offset_ms",
    "finish_offset_ms",
}
CURRENT_PROTOCOL_FIELDS = QUERY_PROTOCOL_FIELDS | {
    "start_skew_ms",
    "worker_resources",
    *ENGINE_ROOT_MEMORY_FIELDS,
}


class ContractError(ValueError):
    pass


def validate_report(report: Any, *, allow_historical: bool = False) -> str:
    root = mapping(report, "report")
    contract_version = root.get("contract_version")
    if contract_version == CONTRACT_VERSION:
        historical = False
        expected_checksum_mode = CURRENT_CHECKSUM_MODE
    elif contract_version in (
        HISTORICAL_CONTRACT_VERSION_V2,
        HISTORICAL_CONTRACT_VERSION,
    ):
        if not allow_historical:
            fail(
                f"contract_version {contract_version!r} is historical; "
                "use --allow-historical for read-only validation"
            )
        historical = True
        expected_checksum_mode = (
            CURRENT_CHECKSUM_MODE
            if contract_version == HISTORICAL_CONTRACT_VERSION_V2
            else HISTORICAL_CHECKSUM_MODE
        )
    else:
        fail(
            "contract_version must be "
            f"{CONTRACT_VERSION!r}, got {contract_version!r}"
        )
    track = root.get("storage_track")
    if track not in TRACKS:
        fail(f"storage_track must be one of {sorted(TRACKS)}, got {track!r}")
    storage_medium = root.get("storage_medium")
    if not historical and storage_medium not in STORAGE_MEDIA:
        fail(
            f"storage_medium must be one of {sorted(STORAGE_MEDIA)}, "
            f"got {storage_medium!r}"
        )
    if not historical or "host" in root:
        host = mapping(root.get("host"), "host")
        for field in ("system", "release", "machine", "cpu_model"):
            if not isinstance(host.get(field), str) or not host[field]:
                fail(f"host.{field} must be recorded")
        positive_int(host.get("logical_cpus"), "host.logical_cpus")
        positive_int(host.get("total_memory_bytes"), "host.total_memory_bytes")
    config = mapping(root.get("config"), "config")
    threads = positive_int(config.get("threads"), "config.threads")
    memory = positive_int(config.get("memory_limit_bytes"), "config.memory_limit_bytes")
    concurrency = positive_int(config.get("concurrency"), "config.concurrency")
    positive_int(config.get("batch_size"), "config.batch_size")
    batch_size = config["batch_size"]
    iterations = positive_int(root.get("iterations"), "iterations")
    warmup = nonnegative_int(root.get("warmup"), "warmup")
    dataset = mapping(root.get("dataset"), "dataset")
    require_sha(dataset.get("sha256"), "dataset.sha256")
    positive_int(dataset.get("bytes"), "dataset.bytes")
    query = mapping(root.get("query"), "query")
    require_sha(query.get("sha256"), "query.sha256")
    native_setup = None
    diagnostic_only = root.get("diagnostic_only") is True
    if not historical and track == "native":
        if (not diagnostic_only and (warmup != 0 or iterations != NATIVE_ROUNDS)) or (
            diagnostic_only and warmup != 0
        ):
            fail(
                "native track requires warmup=0 and iterations=10 unless "
                "diagnostic_only=true"
            )
        native_setup = validate_native_setup(
            mapping(root.get("native_setup"), "native_setup"), dataset
        )
    elif not historical and "native_setup" in root:
        fail("native_setup is valid only for the native storage track")

    order = root.get("engine_order")
    if not isinstance(order, list) or len(order) != iterations:
        fail("engine_order must contain one entry per measured iteration")
    expected_orders = []
    for index in range(iterations):
        expected_orders.append(
            ["rustdb", "duckdb"] if index % 2 == 0 else ["duckdb", "rustdb"]
        )
    if order != expected_orders:
        fail(f"engine_order must alternate exactly, expected {expected_orders!r}")

    engines = mapping(root.get("engines"), "engines")
    if set(engines) != {"rustdb", "duckdb"}:
        fail("engines must contain exactly rustdb and duckdb")
    protocol_required = not historical and (
        concurrency > 1 or uses_current_protocol(engines)
    )
    engine_checksums: dict[str, str] = {}
    for engine in ("rustdb", "duckdb"):
        checksum = validate_engine(
            mapping(engines[engine], f"engines.{engine}"),
            engine,
            track,
            threads,
            memory,
            concurrency,
            batch_size,
            iterations,
            order,
            historical,
            expected_checksum_mode,
            native_setup,
            protocol_required,
        )
        engine_checksums[engine] = checksum
    if len(set(engine_checksums.values())) != 1:
        fail(f"cross-engine checksum mismatch: {engine_checksums}")
    validate_worker_resource_protocol(
        engines,
        threads,
        memory,
        protocol_required,
    )
    return "historical" if historical else "current"


def uses_current_protocol(engines: dict[str, Any]) -> bool:
    for engine_value in engines.values():
        if not isinstance(engine_value, dict):
            continue
        hello = engine_value.get("hello")
        if isinstance(hello, dict) and not CURRENT_PROTOCOL_FIELDS.isdisjoint(hello):
            return True
        runs = engine_value.get("runs")
        if not isinstance(runs, list):
            continue
        for run in runs:
            if not isinstance(run, dict):
                continue
            if not CURRENT_PROTOCOL_FIELDS.isdisjoint(run):
                return True
            queries = run.get("queries")
            if isinstance(queries, list) and any(
                isinstance(query, dict) and not CURRENT_PROTOCOL_FIELDS.isdisjoint(query)
                for query in queries
            ):
                return True
    return False


def validate_engine(
    value: dict[str, Any],
    engine: str,
    track: str,
    threads: int,
    memory: int,
    concurrency: int,
    batch_size: int,
    iterations: int,
    order: list[list[str]],
    historical: bool,
    expected_checksum_mode: str,
    native_setup: dict[str, Any] | None,
    protocol_required: bool,
) -> str:
    hello = mapping(value.get("hello"), f"engines.{engine}.hello")
    equal(hello.get("kind"), "hello", f"engines.{engine}.hello.kind")
    equal(hello.get("engine"), engine, f"engines.{engine}.hello.engine")
    version = hello.get("version")
    if not isinstance(version, str) or not version:
        fail(f"engines.{engine}.hello.version must be recorded")
    if engine == "duckdb" and version != "1.5.4":
        fail(f"DuckDB benchmark reference must be 1.5.4, got {version!r}")
    build_id = hello.get("build_id")
    if not historical:
        build_id = require_sha(build_id, f"engines.{engine}.hello.build_id")
    equal(hello.get("threads"), threads, f"engines.{engine}.hello.threads")
    equal(
        hello.get("memory_limit_bytes"),
        memory,
        f"engines.{engine}.hello.memory_limit_bytes",
    )
    equal(
        hello.get("concurrency"), concurrency, f"engines.{engine}.hello.concurrency"
    )
    equal(hello.get("batch_size"), batch_size, f"engines.{engine}.hello.batch_size")
    validate_cache(mapping(hello.get("cache_state"), f"engines.{engine}.hello.cache_state"))

    setup = None
    if native_setup is not None:
        setup = validate_engine_setup(
            mapping(value.get("setup"), f"engines.{engine}.setup"),
            engine,
            memory,
            native_setup,
        )
    elif not historical and "setup" in value:
        fail(f"engines.{engine}.setup is valid only for the native storage track")

    runs = value.get("runs")
    if not isinstance(runs, list) or len(runs) != iterations:
        fail(f"engines.{engine}.runs must contain {iterations} measured runs")
    checksums: set[str] = set()
    for index, run_value in enumerate(runs):
        run = mapping(run_value, f"engines.{engine}.runs[{index}]")
        equal(run.get("kind"), "run", f"engines.{engine}.runs[{index}].kind")
        equal(
            run.get("run_id"),
            f"measured-{index}-{engine}",
            f"engines.{engine}.runs[{index}].run_id",
        )
        equal(run.get("engine"), engine, f"engines.{engine}.runs[{index}].engine")
        equal(run.get("version"), version, f"engines.{engine}.runs[{index}].version")
        if not historical:
            equal(
                run.get("build_id"),
                build_id,
                f"engines.{engine}.runs[{index}].build_id",
            )
        equal(run.get("storage_track"), track, f"engines.{engine}.runs[{index}].storage_track")
        if native_setup is not None:
            equal(
                run.get("setup_id"),
                native_setup["setup_id"],
                f"engines.{engine}.runs[{index}].setup_id",
            )
        equal(run.get("threads"), threads, f"engines.{engine}.runs[{index}].threads")
        equal(
            run.get("memory_limit_bytes"),
            memory,
            f"engines.{engine}.runs[{index}].memory_limit_bytes",
        )
        equal(
            run.get("concurrency"),
            concurrency,
            f"engines.{engine}.runs[{index}].concurrency",
        )
        equal(
            run.get("batch_size"),
            batch_size,
            f"engines.{engine}.runs[{index}].batch_size",
        )
        equal(
            run.get("engine_order"),
            order[index].index(engine),
            f"engines.{engine}.runs[{index}].engine_order",
        )
        validate_cache(
            mapping(run.get("cache_state"), f"engines.{engine}.runs[{index}].cache_state")
        )
        group_elapsed = positive_number(
            run.get("group_elapsed_ms"), f"engines.{engine}.runs[{index}].group_elapsed_ms"
        )
        throughput = positive_number(
            run.get("throughput_queries_per_second"),
            f"engines.{engine}.runs[{index}].throughput_queries_per_second",
        )
        close(
            throughput,
            concurrency * 1000.0 / group_elapsed,
            f"engines.{engine}.runs[{index}].throughput_queries_per_second",
        )
        baseline = positive_int(
            run.get("rss_baseline_bytes"),
            f"engines.{engine}.runs[{index}].rss_baseline_bytes",
        )
        peak = positive_int(
            run.get("peak_rss_bytes"), f"engines.{engine}.runs[{index}].peak_rss_bytes"
        )
        if peak < baseline:
            fail(f"engines.{engine}.runs[{index}] peak RSS is below baseline")
        if peak > memory:
            fail(f"engines.{engine}.runs[{index}] peak RSS exceeds configured memory")
        queries = run.get("queries")
        if not isinstance(queries, list) or len(queries) != concurrency:
            fail(f"engines.{engine}.runs[{index}].queries must contain {concurrency} entries")
        start_skew = None
        if protocol_required:
            start_skew = nonnegative_number(
                run.get("start_skew_ms"),
                f"engines.{engine}.runs[{index}].start_skew_ms",
            )
        run_checksums = set()
        query_slots: list[int] = []
        harness_query_ids: set[str] = set()
        start_offsets: list[float] = []
        for query_index, query_value in enumerate(queries):
            label = f"engines.{engine}.runs[{index}].queries[{query_index}]"
            measured = mapping(query_value, label)
            if protocol_required:
                query_slot = nonnegative_int(
                    measured.get("query_slot"), f"{label}.query_slot"
                )
                if query_slot != query_index:
                    fail(f"{label}.query_slot must equal its query array position")
                query_slots.append(query_slot)
                harness_query_id = measured.get("harness_query_id")
                expected_query_id = f"{run['run_id']}:query-{query_slot}"
                if harness_query_id != expected_query_id:
                    fail(
                        f"{label}.harness_query_id must be {expected_query_id!r}, "
                        f"got {harness_query_id!r}"
                    )
                if harness_query_id in harness_query_ids:
                    fail(f"{label}.harness_query_id is duplicated")
                harness_query_ids.add(harness_query_id)
                start_offset = nonnegative_number(
                    measured.get("start_offset_ms"), f"{label}.start_offset_ms"
                )
                finish_offset = nonnegative_number(
                    measured.get("finish_offset_ms"), f"{label}.finish_offset_ms"
                )
                if finish_offset < start_offset:
                    fail(f"{label}.finish_offset_ms is below start_offset_ms")
                if finish_offset > group_elapsed * 1.01:
                    fail(f"{label}.finish_offset_ms exceeds group_elapsed_ms")
                start_offsets.append(start_offset)
            if measured.get("complete") is not True:
                fail(f"{label}.complete must be true")
            checksum_mode = measured.get("checksum_mode")
            if checksum_mode != expected_checksum_mode:
                fail(
                    f"{label}.checksum_mode must be {expected_checksum_mode!r} "
                    "for this contract_version, "
                    f"got {checksum_mode!r}; checksum versions cannot be mixed"
                )
            checksum = require_sha(measured.get("checksum"), f"{label}.checksum")
            elapsed = positive_number(measured.get("elapsed_ms"), f"{label}.elapsed_ms")
            ttfb = positive_number(measured.get("ttfb_ms"), f"{label}.ttfb_ms")
            if ttfb > elapsed * 1.01:
                fail(f"{label}.ttfb_ms exceeds elapsed_ms")
            rows = nonnegative_int(measured.get("rows"), f"{label}.rows")
            if not historical:
                equal(
                    measured.get("checksum_backend"),
                    CHECKSUM_BACKENDS[engine],
                    f"{label}.checksum_backend",
                )
                nonnegative_number(
                    measured.get("checksum_compute_ms"),
                    f"{label}.checksum_compute_ms",
                )
                if rows > MAX_GATE_RESULT_ROWS:
                    fail(
                        f"{label}.rows exceeds the current gate limit of "
                        f"{MAX_GATE_RESULT_ROWS}; large result sets require a common "
                        "native checksum consumer before they are valid gate evidence"
                    )
            nonnegative_int(measured.get("batches"), f"{label}.batches")
            if engine == "rustdb":
                validate_rustdb_metrics(
                    measured, label, memory, historical, protocol_required
                )
            if elapsed > group_elapsed * 1.01:
                fail(f"{label}.elapsed_ms exceeds group_elapsed_ms")
            run_checksums.add(checksum)
        if engine == "rustdb":
            validate_engine_root_memory(
                run,
                queries,
                f"engines.{engine}.runs[{index}]",
                memory,
                required=protocol_required,
            )
        if protocol_required:
            if query_slots != list(range(concurrency)):
                fail(
                    f"engines.{engine}.runs[{index}] query slots must be "
                    f"0..{concurrency - 1}"
                )
            expected_start_skew = max(start_offsets) - min(start_offsets)
            close(
                start_skew,
                expected_start_skew,
                f"engines.{engine}.runs[{index}].start_skew_ms",
            )
        if len(run_checksums) != 1:
            fail(f"engines.{engine}.runs[{index}] concurrent checksums differ")
        checksums.update(run_checksums)
    if len(checksums) != 1:
        fail(f"engines.{engine} measured checksums differ between iterations")
    if not historical:
        validate_current_summary(
            mapping(value.get("summary"), f"engines.{engine}.summary"),
            runs,
            f"engines.{engine}.summary",
            setup,
            False,
        )
    return checksums.pop()


def validate_worker_resource_protocol(
    engines: dict[str, Any],
    threads: int,
    memory: int,
    required: bool,
) -> None:
    resources: dict[str, dict[str, Any] | None] = {}
    for engine in ("rustdb", "duckdb"):
        engine_value = mapping(engines[engine], f"engines.{engine}")
        hello = mapping(engine_value.get("hello"), f"engines.{engine}.hello")
        runs = engine_value.get("runs")
        if not isinstance(runs, list):
            fail(f"engines.{engine}.runs must be an array")
        carriers = [hello] + [
            mapping(run, f"engines.{engine}.runs[{index}]")
            for index, run in enumerate(runs)
        ]
        present = ["worker_resources" in carrier for carrier in carriers]
        if any(present) and not all(present):
            fail(
                f"engines.{engine} must record worker_resources in hello and every run or none"
            )
        if required and not all(present):
            fail(f"engines.{engine} must record worker_resources for the current protocol")
        if not all(present):
            resources[engine] = None
            continue

        hello_resources = validate_worker_resources(
            hello.get("worker_resources"),
            f"engines.{engine}.hello.worker_resources",
            threads,
            memory,
        )
        for index, run in enumerate(carriers[1:]):
            run_resources = validate_worker_resources(
                run.get("worker_resources"),
                f"engines.{engine}.runs[{index}].worker_resources",
                threads,
                memory,
            )
            if run_resources != hello_resources:
                fail(
                    f"engines.{engine}.runs[{index}].worker_resources must equal hello"
                )
        resources[engine] = hello_resources

    if (resources["rustdb"] is None) != (resources["duckdb"] is None):
        fail("RustDB and DuckDB must both record worker_resources or both omit it")
    if resources["rustdb"] != resources["duckdb"]:
        fail("RustDB and DuckDB worker_resources must be identical")


def validate_worker_resources(
    value: Any,
    label: str,
    threads: int,
    memory: int,
) -> dict[str, Any]:
    resources = mapping(value, label)
    if set(resources) != set(WORKER_RESOURCE_FIELDS):
        fail(f"{label} must contain exactly {WORKER_RESOURCE_FIELDS!r}")

    affinity_count = cpu_list_count(
        resources.get("cpu_affinity_list"), f"{label}.cpu_affinity_list"
    )
    equal(
        positive_int(resources.get("cpu_affinity_count"), f"{label}.cpu_affinity_count"),
        affinity_count,
        f"{label}.cpu_affinity_count",
    )
    if affinity_count < threads:
        fail(f"{label}.cpu_affinity_count is below configured threads")

    cpuset_count = cpu_list_count(
        resources.get("cgroup_cpuset_cpus_effective"),
        f"{label}.cgroup_cpuset_cpus_effective",
    )
    if cpuset_count < threads:
        fail(f"{label}.cgroup_cpuset_cpus_effective is below configured threads")

    period = positive_int(
        resources.get("cgroup_cpu_period_us"), f"{label}.cgroup_cpu_period_us"
    )
    quota = resources.get("cgroup_cpu_quota_us")
    if quota is not None:
        quota = positive_int(quota, f"{label}.cgroup_cpu_quota_us")
        if quota < threads * period:
            fail(f"{label}.cgroup_cpu_quota_us is below configured threads")

    memory_max = resources.get("cgroup_memory_max_bytes")
    if memory_max is not None:
        memory_max = positive_int(memory_max, f"{label}.cgroup_memory_max_bytes")
        if memory_max < memory:
            fail(f"{label}.cgroup_memory_max_bytes is below configured memory")
    visible_memory = positive_int(
        resources.get("visible_memory_bytes"), f"{label}.visible_memory_bytes"
    )
    if visible_memory < memory:
        fail(f"{label}.visible_memory_bytes is below configured memory")
    return resources


def cpu_list_count(value: Any, label: str) -> int:
    if not isinstance(value, str) or not value or value.strip() != value:
        fail(f"{label} must be a non-empty canonical CPU list")
    count = 0
    previous_end = None
    for segment in value.split(","):
        parts = segment.split("-")
        if len(parts) == 1:
            start = end = cpu_number(parts[0], label)
        elif len(parts) == 2:
            start = cpu_number(parts[0], label)
            end = cpu_number(parts[1], label)
        else:
            fail(f"{label} contains an invalid CPU range")
        if start > end or (previous_end is not None and start <= previous_end):
            fail(f"{label} CPU ranges must be ordered and disjoint")
        count += end - start + 1
        previous_end = end
    return count


def cpu_number(value: str, label: str) -> int:
    if not value.isascii() or not value.isdigit():
        fail(f"{label} contains a non-numeric CPU")
    return int(value)


def validate_native_setup(
    value: dict[str, Any], dataset: dict[str, Any]
) -> dict[str, Any]:
    equal(value.get("storage_track"), "native", "native_setup.storage_track")
    equal(
        value.get("setup_engine_order"),
        ["rustdb", "duckdb"],
        "native_setup.setup_engine_order",
    )
    source_files = validate_native_source_files(value.get("source_files"), dataset)
    source_sha256 = require_sha(value.get("source_sha256"), "native_setup.source_sha256")
    equal(source_sha256, dataset["sha256"], "native_setup.source_sha256")
    source_bytes = positive_int(value.get("source_bytes"), "native_setup.source_bytes")
    equal(source_bytes, dataset["bytes"], "native_setup.source_bytes")
    statements = value.get("statements")
    if (
        not isinstance(statements, list)
        or not statements
        or not all(isinstance(statement, str) and statement for statement in statements)
    ):
        fail("native_setup.statements must be a non-empty string array")
    validate_native_statement_locations(statements, source_files)
    encoded = json.dumps(
        statements, separators=(",", ":"), ensure_ascii=False
    ).encode()
    statements_sha256 = require_sha(
        value.get("statements_sha256"), "native_setup.statements_sha256"
    )
    equal(
        statements_sha256,
        hashlib.sha256(encoded).hexdigest(),
        "native_setup.statements_sha256",
    )
    identity = {
        "source_sha256": source_sha256,
        "statements_sha256": statements_sha256,
    }
    expected_setup_id = hashlib.sha256(
        json.dumps(identity, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    setup_id = require_sha(value.get("setup_id"), "native_setup.setup_id")
    equal(setup_id, expected_setup_id, "native_setup.setup_id")
    max_storage_bytes = positive_int(
        value.get("max_storage_bytes"), "native_setup.max_storage_bytes"
    )
    maximum = native_storage_maximum(source_bytes, len(statements))
    if max_storage_bytes > maximum:
        fail(
            "native_setup.max_storage_bytes exceeds the Native 2x plus bounded "
            f"metadata limit of {maximum} bytes"
        )
    return {
        "setup_id": setup_id,
        "table_count": len(statements),
        "max_storage_bytes": max_storage_bytes,
    }


def validate_native_source_files(
    value: Any, dataset: dict[str, Any]
) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value:
        fail("native_setup.source_files must be a non-empty array")
    sources: list[dict[str, Any]] = []
    relative_paths: set[str] = set()
    locations: set[str] = set()
    roots: set[str] = set()
    digest = hashlib.sha256()
    total = 0
    for index, source_value in enumerate(value):
        label = f"native_setup.source_files[{index}]"
        source = mapping(source_value, label)
        if set(source) != {"relative_path", "location", "bytes", "sha256"}:
            fail(f"{label} must contain relative_path, location, bytes, and sha256")
        relative_path = validate_native_relative_path(source.get("relative_path"), label)
        location = validate_native_location(source.get("location"), relative_path, label)
        if relative_path in relative_paths:
            fail(f"{label}.relative_path is duplicated")
        if location in locations:
            fail(f"{label}.location is duplicated")
        relative_paths.add(relative_path)
        locations.add(location)
        roots.add(native_source_root(location, relative_path))
        size = nonnegative_int(source.get("bytes"), f"{label}.bytes")
        sha256 = require_sha(source.get("sha256"), f"{label}.sha256")
        relative = relative_path.encode()
        digest.update(len(relative).to_bytes(8, "little"))
        digest.update(relative)
        digest.update(size.to_bytes(8, "little"))
        digest.update(bytes.fromhex(sha256))
        total += size
        sources.append(source)
    expected_order = sorted(relative_paths)
    if [source["relative_path"] for source in sources] != expected_order:
        fail("native_setup.source_files must be sorted by relative_path")
    if len(roots) != 1:
        fail("native_setup.source_files locations must share one source root")
    equal(total, dataset["bytes"], "native_setup.source_files bytes")
    equal(
        len(sources),
        positive_int(dataset.get("files"), "dataset.files"),
        "native_setup.source_files count",
    )
    equal(digest.hexdigest(), dataset["sha256"], "native_setup.source_files sha256")
    return sources


def validate_native_relative_path(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value or "\x00" in value or "\\" in value:
        fail(f"{label}.relative_path must be a relative POSIX path")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or any(c in value for c in "*?[]"):
        fail(f"{label}.relative_path must be a wildcard-free relative POSIX path")
    return value


def validate_native_location(value: Any, relative_path: str, label: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or "\x00" in value
        or "\\" in value
        or "://" in value
        or any(character in value for character in "*?[]")
        or not is_lexically_normal_absolute_posix(value)
        or not value.endswith(f"/{relative_path}")
    ):
        fail(
            f"{label}.location must be a wildcard-free, lexically-normal "
            "absolute POSIX source path"
        )
    root = native_source_root(value, relative_path)
    if not is_lexically_normal_absolute_posix(root):
        fail(f"{label}.location must have a lexically-normal absolute POSIX source root")
    return value


def native_source_root(location: str, relative_path: str) -> str:
    root = location[: -(len(relative_path) + 1)]
    return root or "/"


def is_lexically_normal_absolute_posix(value: str) -> bool:
    path = PurePosixPath(value)
    return (
        path.is_absolute()
        and path.as_posix() == value
        and ".." not in path.parts
        and "//" not in value
    )


def validate_native_statement_locations(
    statements: list[str], source_files: list[dict[str, Any]]
) -> None:
    locations: list[str] = []
    pattern = re.compile(r"read_parquet\('((?:''|[^'])*)'\)", re.IGNORECASE)
    for index, statement in enumerate(statements):
        matches = [match.replace("''", "'") for match in pattern.findall(statement)]
        if statement.lower().count("read_parquet") != len(matches):
            fail(f"native_setup.statements[{index}] has a non-literal read_parquet source")
        locations.extend(matches)
    expected = [source["location"] for source in source_files]
    if len(locations) != len(expected) or set(locations) != set(expected):
        fail("native_setup.statements must reference every explicit source location once")


def native_storage_maximum(source_bytes: int, table_count: int) -> int:
    return (
        source_bytes * NATIVE_STORAGE_MULTIPLIER
        + table_count * NATIVE_TABLE_METADATA_BYTES
        + NATIVE_HARNESS_BYTES
    )


def validate_engine_setup(
    value: dict[str, Any],
    engine: str,
    memory: int,
    expected: dict[str, Any],
) -> dict[str, Any]:
    label = f"engines.{engine}.setup"
    equal(value.get("kind"), "setup", f"{label}.kind")
    equal(value.get("engine"), engine, f"{label}.engine")
    equal(value.get("setup_id"), expected["setup_id"], f"{label}.setup_id")
    if value.get("complete") is not True:
        fail(f"{label}.complete must be true")
    load_elapsed_ms = positive_number(
        value.get("load_elapsed_ms"), f"{label}.load_elapsed_ms"
    )
    rss_baseline = positive_int(
        value.get("rss_baseline_bytes"), f"{label}.rss_baseline_bytes"
    )
    peak_rss = positive_int(value.get("peak_rss_bytes"), f"{label}.peak_rss_bytes")
    if peak_rss < rss_baseline:
        fail(f"{label}.peak_rss_bytes is below baseline")
    if peak_rss > memory:
        fail(f"{label}.peak_rss_bytes exceeds configured memory")
    storage_baseline = nonnegative_int(
        value.get("storage_baseline_bytes"), f"{label}.storage_baseline_bytes"
    )
    storage_final = nonnegative_int(
        value.get("storage_final_bytes"), f"{label}.storage_final_bytes"
    )
    storage_peak = nonnegative_int(
        value.get("storage_peak_bytes"), f"{label}.storage_peak_bytes"
    )
    if not storage_baseline <= storage_final <= storage_peak:
        fail(f"{label} storage bytes must satisfy baseline <= final <= peak")
    if storage_peak > expected["max_storage_bytes"]:
        fail(f"{label}.storage_peak_bytes exceeds max_storage_bytes")
    equal(
        positive_int(value.get("table_count"), f"{label}.table_count"),
        expected["table_count"],
        f"{label}.table_count",
    )
    return {"load_elapsed_ms": load_elapsed_ms}


def validate_current_summary(
    value: dict[str, Any],
    runs: list[Any],
    label: str,
    setup: dict[str, Any] | None = None,
    diagnostic_only: bool = False,
) -> None:
    expected_elapsed = statistics.median(run["group_elapsed_ms"] for run in runs)
    expected_ttfb = statistics.median(
        query["ttfb_ms"] for run in runs for query in run["queries"]
    )
    expected_peak = max(run["peak_rss_bytes"] for run in runs)
    expected_delta = max(
        run["peak_rss_bytes"] - run["rss_baseline_bytes"] for run in runs
    )
    expected_throughput = statistics.mean(
        run["throughput_queries_per_second"] for run in runs
    )
    elapsed = positive_number(value.get("p50_elapsed_ms"), f"{label}.p50_elapsed_ms")
    ttfb = positive_number(value.get("p50_ttfb_ms"), f"{label}.p50_ttfb_ms")
    peak = positive_int(value.get("peak_rss_bytes"), f"{label}.peak_rss_bytes")
    delta = nonnegative_int(
        value.get("peak_rss_delta_bytes"), f"{label}.peak_rss_delta_bytes"
    )
    throughput = positive_number(
        value.get("mean_throughput_queries_per_second"),
        f"{label}.mean_throughput_queries_per_second",
    )
    close(elapsed, expected_elapsed, f"{label}.p50_elapsed_ms")
    close(ttfb, expected_ttfb, f"{label}.p50_ttfb_ms")
    equal(peak, expected_peak, f"{label}.peak_rss_bytes")
    equal(delta, expected_delta, f"{label}.peak_rss_delta_bytes")
    close(
        throughput,
        expected_throughput,
        f"{label}.mean_throughput_queries_per_second",
    )
    if setup is not None:
        query_total = sum(run["group_elapsed_ms"] for run in runs)
        load_elapsed = setup["load_elapsed_ms"]
        close(
            positive_number(value.get("load_elapsed_ms"), f"{label}.load_elapsed_ms"),
            load_elapsed,
            f"{label}.load_elapsed_ms",
        )
        close(
            positive_number(
                value.get("query_round_total_ms"), f"{label}.query_round_total_ms"
            ),
            query_total,
            f"{label}.query_round_total_ms",
        )
        close(
            positive_number(
                value.get("first_post_reopen_elapsed_ms"),
                f"{label}.first_post_reopen_elapsed_ms",
            ),
            runs[0]["group_elapsed_ms"],
            f"{label}.first_post_reopen_elapsed_ms",
        )
        steady_runs = runs[1:] if len(runs) > 1 else runs
        close(
            positive_number(
                value.get("steady_state_p50_elapsed_ms"),
                f"{label}.steady_state_p50_elapsed_ms",
            ),
            statistics.median(run["group_elapsed_ms"] for run in steady_runs),
            f"{label}.steady_state_p50_elapsed_ms",
        )
        close(
            positive_number(
                value.get("amortized_elapsed_ms"), f"{label}.amortized_elapsed_ms"
            ),
            (load_elapsed + query_total)
            / (len(runs) if diagnostic_only or len(runs) != NATIVE_ROUNDS else NATIVE_ROUNDS),
            f"{label}.amortized_elapsed_ms",
        )


def validate_rustdb_metrics(
    value: dict[str, Any],
    label: str,
    memory: int,
    historical: bool,
    require_terminal_zero: bool,
) -> None:
    if "execute_return_ms" in value:
        execute_return = nonnegative_number(
            value.get("execute_return_ms"), f"{label}.execute_return_ms"
        )
        ttfb = nonnegative_number(value.get("ttfb_ms"), f"{label}.ttfb_ms")
        if execute_return > ttfb:
            fail(f"{label}.execute_return_ms exceeds ttfb_ms")
    nonnegative_int(value.get("scanned_rows"), f"{label}.scanned_rows")
    nonnegative_int(value.get("scanned_bytes"), f"{label}.scanned_bytes")
    for field in (
        "discovered_files",
        "parquet_reader_builds",
        "parquet_local_file_opens",
        "csv_source_bytes",
        "csv_decompressed_bytes",
        "csv_morsels",
        "peak_csv_parser_lanes",
    ):
        if not historical or field in value:
            nonnegative_int(value.get(field), f"{label}.{field}")
    for field in (
        "parquet_range_bytes_read",
        "parquet_decode_polls",
        "parquet_decode_pending_polls",
        "parquet_row_filter_evaluations",
        "parquet_row_filter_input_rows",
        "parquet_narrow_decimal_columns",
        "native_predicate_sidecar_bytes_read",
        "native_predicate_sidecar_rows_evaluated",
        "native_predicate_sidecar_rows_selected",
        "native_predicate_sidecar_exact_bypasses",
        "native_predicate_sidecar_full_projection_bypasses",
        "native_predicate_sidecar_full_projection_rows",
        "native_predicate_sidecar_full_projection_fallback_row_groups",
        "native_predicate_sidecar_fallbacks",
    ):
        if field in value:
            nonnegative_int(value.get(field), f"{label}.{field}")
    for field in (
        "parquet_range_read_time_ms",
        "parquet_decode_compute_time_ms",
        "parquet_decode_compute_permit_wait_ms",
        "parquet_row_filter_compute_time_ms",
        "parquet_alignment_time_ms",
    ):
        if field in value:
            nonnegative_number(value.get(field), f"{label}.{field}")
    if value.get("parquet_decode_pending_polls", 0) > value.get("parquet_decode_polls", 0):
        fail(f"{label}.parquet_decode_pending_polls exceeds total polls")
    if value.get("native_predicate_sidecar_rows_selected", 0) > value.get(
        "native_predicate_sidecar_rows_evaluated", 0
    ):
        fail(
            f"{label}.native_predicate_sidecar_rows_selected exceeds evaluated rows"
        )
    current = nonnegative_int(
        value.get("current_reservation_bytes"), f"{label}.current_reservation_bytes"
    )
    peak = nonnegative_int(
        value.get("peak_reservation_bytes"), f"{label}.peak_reservation_bytes"
    )
    if current > peak:
        fail(f"{label}.current_reservation_bytes exceeds peak")
    if require_terminal_zero and current != 0:
        fail(f"{label}.current_reservation_bytes must be zero after query completion")
    if peak > memory:
        fail(f"{label}.peak_reservation_bytes exceeds configured memory")
    nonnegative_int(value.get("peak_active_lanes"), f"{label}.peak_active_lanes")
    nonnegative_number(value.get("scheduler_wait_ms"), f"{label}.scheduler_wait_ms")
    for field in (
        "compute_permit_wait_ms",
        "queue_backpressure_wait_ms",
        "barrier_wait_ms",
        "csv_source_io_time_ms",
        "csv_framing_time_ms",
        "csv_decode_compute_time_ms",
    ):
        if not historical or field in value:
            nonnegative_number(value.get(field), f"{label}.{field}")
    for field in (
        "csv_morsel_queue_wait_ms",
        "scan_pipeline_output_queue_wait_ms",
        "aggregate_lane_dispatch_queue_wait_ms",
        "aggregate_partial_output_queue_wait_ms",
    ):
        if field in value:
            nonnegative_number(value.get(field), f"{label}.{field}")
    for field in (
        "query_admission_wait_ms",
        "sql_parse_time_ms",
        "table_function_prepare_time_ms",
        "bind_time_ms",
        "provider_prepare_time_ms",
        "optimize_time_ms",
        "native_verification_time_ms",
    ):
        if field in value:
            nonnegative_number(value.get(field), f"{label}.{field}")
    if "native_full_verification_segments" in value:
        nonnegative_int(
            value.get("native_full_verification_segments"),
            f"{label}.native_full_verification_segments",
        )
    nonnegative_int(value.get("spill_read_bytes"), f"{label}.spill_read_bytes")
    nonnegative_int(value.get("spill_write_bytes"), f"{label}.spill_write_bytes")
    nonnegative_int(value.get("join_candidate_pairs"), f"{label}.join_candidate_pairs")
    validate_operator_tree(value.get("operators"), label)


def validate_engine_root_memory(
    run: dict[str, Any],
    queries: list[Any],
    label: str,
    configured_limit: int,
    *,
    required: bool,
) -> None:
    present = [field in run for field in ENGINE_ROOT_MEMORY_FIELDS]
    if any(present) and not all(present):
        fail(f"{label} must record all Engine root memory fields or none")
    if required and not all(present):
        fail(f"{label} must record Engine root memory for the current run protocol")
    if not all(present):
        return

    current = nonnegative_int(
        run.get("engine_root_current_reservation_bytes"),
        f"{label}.engine_root_current_reservation_bytes",
    )
    peak = nonnegative_int(
        run.get("engine_root_lifetime_peak_reservation_bytes"),
        f"{label}.engine_root_lifetime_peak_reservation_bytes",
    )
    limit = positive_int(
        run.get("engine_root_memory_limit_bytes"),
        f"{label}.engine_root_memory_limit_bytes",
    )
    equal(limit, configured_limit, f"{label}.engine_root_memory_limit_bytes")
    if current != 0:
        fail(f"{label}.engine_root_current_reservation_bytes must be zero")
    if peak > limit:
        fail(f"{label}.engine_root_lifetime_peak_reservation_bytes exceeds limit")

    query_peak = max(
        nonnegative_int(
            mapping(query, f"{label}.queries[{index}]").get("peak_reservation_bytes"),
            f"{label}.queries[{index}].peak_reservation_bytes",
        )
        for index, query in enumerate(queries)
    )
    if peak < query_peak:
        fail(
            f"{label}.engine_root_lifetime_peak_reservation_bytes is below "
            "a query peak reservation"
        )


def validate_operator_tree(value: Any, query_label: str) -> None:
    label = f"{query_label}.operators"
    operators = value
    if not isinstance(operators, list) or not operators:
        fail(f"{label} must contain at least one operator")
    entries: list[tuple[dict[str, Any], str, int]] = []
    ids: set[int] = set()
    for index, operator_value in enumerate(operators):
        operator_label = f"{label}[{index}]"
        operator = mapping(operator_value, operator_label)
        operator_id = nonnegative_int(operator.get("id"), f"{operator_label}.id")
        if operator_id in ids:
            fail(f"{label} contains duplicate operator id {operator_id}")
        ids.add(operator_id)
        entries.append((operator, operator_label, operator_id))

    for operator, operator_label, operator_id in entries:
        if operator.get("parent_id") is not None:
            parent_id = nonnegative_int(
                operator.get("parent_id"), f"{operator_label}.parent_id"
            )
            if parent_id == operator_id:
                fail(f"{operator_label} cannot be its own parent")
            if parent_id not in ids:
                fail(
                    f"{operator_label}.parent_id {parent_id} does not reference "
                    f"an operator in {label}"
                )
        if not isinstance(operator.get("name"), str) or not operator["name"]:
            fail(f"{operator_label}.name must be recorded")
        for field in (
            "input_rows",
            "input_batches",
            "output_rows",
            "output_batches",
            "output_bytes",
        ):
            nonnegative_int(operator.get(field), f"{operator_label}.{field}")
        nonnegative_number(operator.get("elapsed_ms"), f"{operator_label}.elapsed_ms")
        nonnegative_number(operator.get("wait_ms"), f"{operator_label}.wait_ms")


def validate_cache(value: dict[str, Any]) -> None:
    if value.get("os_page_cache") != "warm-uncontrolled":
        fail("cache_state.os_page_cache must explicitly be warm-uncontrolled")
    if value.get("metadata_cache") not in ("disabled", "enabled"):
        fail("cache_state.metadata_cache must be disabled or enabled")
    if value.get("external_file_cache") not in ("disabled", "not-applicable"):
        fail("cache_state.external_file_cache must be disabled or not-applicable")


def mapping(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        fail(f"{label} must be an object")
    return value


def positive_int(value: Any, label: str) -> int:
    if type(value) is not int or value <= 0:
        fail(f"{label} must be a positive integer")
    return value


def nonnegative_int(value: Any, label: str) -> int:
    if type(value) is not int or value < 0:
        fail(f"{label} must be a non-negative integer")
    return value


def positive_number(value: Any, label: str) -> float:
    if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
        fail(f"{label} must be positive and finite")
    return float(value)


def nonnegative_number(value: Any, label: str) -> float:
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
        fail(f"{label} must be non-negative and finite")
    return float(value)


def require_sha(value: Any, label: str) -> str:
    if not isinstance(value, str) or not CHECKSUM.fullmatch(value):
        fail(f"{label} must be a lowercase SHA-256")
    return value


def equal(actual: Any, expected: Any, label: str) -> None:
    if actual != expected:
        fail(f"{label}: expected {expected!r}, got {actual!r}")


def close(actual: float, expected: float, label: str) -> None:
    if not math.isclose(actual, expected, rel_tol=1e-9, abs_tol=1e-9):
        fail(f"{label}: expected {expected!r}, got {actual!r}")


def fail(message: str) -> None:
    raise ContractError(message)


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate a RustDB v0.7 benchmark report")
    parser.add_argument(
        "--allow-historical",
        action="store_true",
        help="read a v1/v2 historical report; it is never valid gate evidence",
    )
    parser.add_argument("report", type=Path)
    args = parser.parse_args()
    try:
        report_kind = validate_report(
            json.loads(args.report.read_text(encoding="utf-8")),
            allow_historical=args.allow_historical,
        )
    except (OSError, json.JSONDecodeError, ContractError) as error:
        print(f"error: {error}", file=__import__("sys").stderr)
        return 1
    if report_kind == "historical":
        print(
            "valid historical v0.7 benchmark report "
            f"(not valid v0.7 gate evidence): {args.report}"
        )
    else:
        print(f"valid current v0.7 benchmark report: {args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
