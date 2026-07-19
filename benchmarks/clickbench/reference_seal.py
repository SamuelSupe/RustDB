#!/usr/bin/env python3
"""Compare RustDB with the pinned reference and seal the typed oracle."""

from __future__ import annotations

import argparse
import json
import math
import re
import struct
import subprocess
from pathlib import Path
from typing import Any

from oracle import CHECKSUM_ALGORITHM, SCHEMA
from reference_common import (
    DATA_SHA256,
    EFFECTIVE_QUERY_SHA256,
    QUERY_COUNT,
    REFERENCE_DIGEST,
    SEMANTIC_ALGORITHM,
    SOURCE_QUERY_SHA256,
    load_queries,
    require_sha,
    semantic_checksum,
    sha256,
)
from run import render


REFERENCE_SCHEMA = "rustdb-clickbench-reference-v1"


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--candidate-manifest", type=Path, required=True)
    parser.add_argument("--source-queries", type=Path, required=True)
    parser.add_argument("--effective-queries", type=Path, required=True)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--rustdb", type=Path, required=True)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def json_file(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"JSON root must be an object: {path}")
    return value


def validate_reference(value: dict[str, Any]) -> None:
    engine = value.get("reference_engine", {})
    if (
        value.get("schema") != REFERENCE_SCHEMA
        or value.get("semantic_checksum_algorithm") != SEMANTIC_ALGORITHM
        or value.get("source_query_sha256") != SOURCE_QUERY_SHA256
        or value.get("effective_query_sha256") != EFFECTIVE_QUERY_SHA256
        or value.get("dataset_sha256") != DATA_SHA256
        or engine.get("image_digest") != REFERENCE_DIGEST
        or value.get("q24_event_time_watch_id_duplicate_groups") != 0
    ):
        raise ValueError("reference manifest has the wrong immutable identity")
    results = value.get("results")
    if not isinstance(results, list) or len(results) != QUERY_COUNT:
        raise ValueError("reference manifest must contain 43 query results")


def validate_candidate(value: dict[str, Any]) -> None:
    queries = value.get("queries", {})
    canonical = value.get("canonical_queries", {})
    dataset = value.get("dataset", {})
    results = value.get("results")
    if (
        queries.get("sha256") != EFFECTIVE_QUERY_SHA256
        or canonical.get("sha256") != SOURCE_QUERY_SHA256
        or dataset.get("sha256") != DATA_SHA256
        or not isinstance(results, list)
        or len(results) != QUERY_COUNT
    ):
        raise ValueError("RustDB candidate manifest has the wrong immutable identity")
    for number, result in enumerate(results, start=1):
        checksum = result.get("result_checksum_sha256")
        if (
            result.get("query") != number
            or result.get("exit_code") != 0
            or result.get("timed_out") is not False
            or result.get("report_parse_error") is not None
            or result.get("cleanup_error") is not None
            or result.get("terminal_query_reservation_bytes") != 0
            or result.get("terminal_engine_reservation_bytes") != 0
            or result.get("spill_cleaned") is not True
            or type(result.get("result_rows")) is not int
            or result["result_rows"] < 0
            or not isinstance(checksum, str)
            or re.fullmatch(r"[0-9a-f]{64}", checksum) is None
        ):
            raise ValueError(f"RustDB candidate query {number} did not execute cleanly")


def rustdb_rows(binary: Path, sql: str, threads: int) -> list[list[Any]]:
    result = subprocess.run(
        [
            str(binary),
            "--threads",
            str(threads),
            "--format",
            "jsonl",
            "-c",
            sql,
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or "RustDB reference query failed")
    return [
        list(json.loads(line).values())
        for line in result.stdout.splitlines()
        if line.strip()
    ]


def equal_value(left: Any, right: Any) -> bool:
    if isinstance(left, bool) or isinstance(right, bool):
        return left is right
    if isinstance(left, (int, float)) and isinstance(right, (int, float)):
        if isinstance(left, float) or isinstance(right, float):
            return math.isclose(float(left), float(right), rel_tol=1e-15, abs_tol=1e-12)
        return left == right
    if isinstance(left, str) and isinstance(right, str):
        timestamp = r"^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2}(?:\.\d+)?)$"
        left_match = re.fullmatch(timestamp, left)
        right_match = re.fullmatch(timestamp, right)
        if left_match and right_match:
            return left_match.groups() == right_match.groups()
    return left == right


def equal_row(left: list[Any], right: list[Any]) -> bool:
    return len(left) == len(right) and all(
        equal_value(a, b) for a, b in zip(left, right)
    )


def equal_multiset(left: list[list[Any]], right: list[list[Any]]) -> bool:
    unmatched = list(right)
    for row in left:
        for index, candidate in enumerate(unmatched):
            if equal_row(row, candidate):
                unmatched.pop(index)
                break
        else:
            return False
    return not unmatched


def f64_bits(value: float) -> str:
    return f"0x{struct.unpack('>Q', struct.pack('>d', value))[0]:016x}"


def main() -> int:
    args = arguments()
    for name in (
        "reference",
        "candidate_manifest",
        "source_queries",
        "effective_queries",
        "data",
        "rustdb",
        "output",
    ):
        setattr(args, name, getattr(args, name).resolve())
    if args.threads <= 0:
        raise ValueError("threads must be positive")
    for path in (
        args.reference,
        args.candidate_manifest,
        args.source_queries,
        args.effective_queries,
        args.data,
        args.rustdb,
    ):
        if not path.is_file():
            raise ValueError(f"input is not a file: {path}")
    if args.output.exists():
        raise ValueError(f"output already exists: {args.output}")
    require_sha(args.source_queries, SOURCE_QUERY_SHA256, "source query")
    require_sha(args.effective_queries, EFFECTIVE_QUERY_SHA256, "effective query")
    require_sha(args.data, DATA_SHA256, "dataset")
    reference = json_file(args.reference)
    candidate = json_file(args.candidate_manifest)
    validate_reference(reference)
    validate_candidate(candidate)
    queries = load_queries(args.effective_queries)
    verified = []
    for number, (query, expected) in enumerate(
        zip(queries, reference["results"]), start=1
    ):
        sql = render(query, args.data, "execute", True)
        rows = rustdb_rows(args.rustdb, sql, args.threads)
        reference_rows = expected["values"]
        if number == 4:
            bits = f64_bits(float(rows[0][0]))
            required = reference["q04_exact_ratio"]["expected_f64_bits"]
            if len(rows) != 1 or len(rows[0]) != 1 or bits != required:
                raise RuntimeError(f"Q04 exact average differs: {bits} != {required}")
        elif not equal_multiset(rows, reference_rows):
            raise RuntimeError(f"Q{number:02d} differs from the ClickHouse rowset")
        if len(rows) != expected["rows"]:
            raise RuntimeError(f"Q{number:02d} row count differs from the reference")
        verified.append(
            {
                "query": number,
                "rows": len(rows),
                "reference_semantic_checksum": expected["semantic_checksum"],
                "rustdb_semantic_checksum": semantic_checksum(rows),
            }
        )
    results = [
        {
            "query": number,
            "rows": result["result_rows"],
            "checksum": result["result_checksum_sha256"],
        }
        for number, result in enumerate(candidate["results"], start=1)
    ]
    oracle = {
        "schema": SCHEMA,
        "profile": "functional",
        "mode": "execute",
        "query_count": QUERY_COUNT,
        "query_sha256": EFFECTIVE_QUERY_SHA256,
        "canonical_query_sha256": SOURCE_QUERY_SHA256,
        "dataset_sha256": DATA_SHA256,
        "checksum_algorithm": CHECKSUM_ALGORITHM,
        "reference": {
            "manifest_sha256": sha256(args.reference),
            "engine": reference["reference_engine"],
            "semantic_checksum_algorithm": SEMANTIC_ALGORITHM,
            "verified_queries": QUERY_COUNT,
            "q04_exact_ratio": reference["q04_exact_ratio"],
            "q24_event_time_watch_id_duplicate_groups": 0,
            "rustdb_binary_sha256": sha256(args.rustdb),
            "rustdb_candidate_manifest_sha256": sha256(args.candidate_manifest),
            "semantic_results": verified,
        },
        "results": results,
    }
    args.output.write_text(
        json.dumps(oracle, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
