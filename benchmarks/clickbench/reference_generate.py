#!/usr/bin/env python3
"""Generate an independent ClickHouse rowset for the functional oracle."""

from __future__ import annotations

import argparse
import json
import struct
import subprocess
from fractions import Fraction
from pathlib import Path
from typing import Any

from reference_common import (
    DATA_SHA256,
    EFFECTIVE_QUERY_SHA256,
    REFERENCE_DIGEST,
    REFERENCE_IMAGE,
    SEMANTIC_ALGORITHM,
    SOURCE_QUERY_SHA256,
    load_queries,
    reference_sql,
    require_sha,
    semantic_checksum,
)


SCHEMA = "rustdb-clickbench-reference-v1"


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-queries", type=Path, required=True)
    parser.add_argument("--effective-queries", type=Path, required=True)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def docker(data: Path, *command: str, stdin: str | None = None) -> str:
    result = subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "--volume",
            f"{data.parent}:/data:ro",
            REFERENCE_IMAGE,
            *command,
        ],
        input=stdin,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "clickhouse-local failed")
    return result.stdout


def verify_image() -> None:
    result = subprocess.run(
        ["docker", "image", "inspect", REFERENCE_IMAGE, "--format", "{{json .RepoDigests}}"],
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"pinned ClickHouse image is unavailable: {REFERENCE_IMAGE}")
    digests = json.loads(result.stdout)
    if not any(value.endswith(f"@{REFERENCE_DIGEST}") for value in digests):
        raise RuntimeError("local ClickHouse image does not match the pinned digest")


def clickhouse_json(data: Path, sql: str) -> list[list[Any]]:
    output = docker(
        data,
        "clickhouse-local",
        "--output_format_json_quote_64bit_integers=0",
        "--query",
        sql,
    )
    return [json.loads(line) for line in output.splitlines() if line.strip()]


def source_schema(data: Path) -> list[dict[str, str]]:
    output = docker(
        data,
        "clickhouse-local",
        "--query",
        (
            f"DESCRIBE file('/data/{data.name}', Parquet) "
            "FORMAT JSONEachRow"
        ),
    )
    return [json.loads(line) for line in output.splitlines() if line.strip()]


def q24_duplicate_keys(data: Path) -> int:
    rows = clickhouse_json(
        data,
        (
            "SELECT count() FROM ("
            "SELECT EventTime, WatchID FROM "
            f"file('/data/{data.name}', Parquet) "
            "WHERE URL LIKE '%google%' GROUP BY EventTime, WatchID "
            "HAVING count() > 1) FORMAT JSONCompactEachRow"
        ),
    )
    return int(rows[0][0])


def f64_bits(value: float) -> str:
    return f"0x{struct.unpack('>Q', struct.pack('>d', value))[0]:016x}"


def main() -> int:
    args = arguments()
    args.source_queries = args.source_queries.resolve()
    args.effective_queries = args.effective_queries.resolve()
    args.data = args.data.resolve()
    args.output = args.output.resolve()
    for path in (args.source_queries, args.effective_queries, args.data):
        if not path.is_file():
            raise ValueError(f"input is not a file: {path}")
    require_sha(args.source_queries, SOURCE_QUERY_SHA256, "source query")
    require_sha(args.effective_queries, EFFECTIVE_QUERY_SHA256, "effective query")
    require_sha(args.data, DATA_SHA256, "dataset")
    if args.output.exists():
        raise ValueError(f"output already exists: {args.output}")
    verify_image()
    columns = source_schema(args.data)
    queries = load_queries(args.effective_queries)
    version = docker(args.data, "clickhouse-local", "--version").strip()
    results = []
    q4_ratio: dict[str, Any] | None = None
    for number, query in enumerate(queries, start=1):
        rows = clickhouse_json(
            args.data,
            reference_sql(query, number, args.data.name, columns),
        )
        if number == 4:
            numerator, denominator = (int(rows[0][0]), int(rows[0][1]))
            value = float(Fraction(numerator, denominator))
            rows = [[value]]
            q4_ratio = {
                "numerator": str(numerator),
                "denominator": denominator,
                "expected_f64_bits": f64_bits(value),
            }
        results.append(
            {
                "query": number,
                "rows": len(rows),
                "semantic_checksum": semantic_checksum(rows),
                "values": rows,
            }
        )
    duplicates = q24_duplicate_keys(args.data)
    if duplicates != 0:
        raise RuntimeError("Q24 EventTime/WatchID is not unique in the pinned fixture")
    manifest = {
        "schema": SCHEMA,
        "semantic_checksum_algorithm": SEMANTIC_ALGORITHM,
        "reference_engine": {
            "name": "ClickHouse clickhouse-local",
            "version": version,
            "image": REFERENCE_IMAGE,
            "image_digest": REFERENCE_DIGEST,
        },
        "source_query_sha256": SOURCE_QUERY_SHA256,
        "effective_query_sha256": EFFECTIVE_QUERY_SHA256,
        "dataset_sha256": DATA_SHA256,
        "q04_exact_ratio": q4_ratio,
        "q24_event_time_watch_id_duplicate_groups": duplicates,
        "source_schema": columns,
        "results": results,
    }
    args.output.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
