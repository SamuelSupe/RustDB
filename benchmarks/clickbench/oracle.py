"""Pinned functional ClickBench result oracle validation."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from pathlib import Path
from typing import Any


SCHEMA = "rustdb-clickbench-functional-oracle-v2"
CHECKSUM_ALGORITHM = "rustdb-typed-multiset-sha256-v1"
PINNED_SHA256 = "1e431a93f6942b50682178e7f21e8b81ab87e02c3a4e7247842e6ef296c354e5"
EXPECTED_QUERIES = 43
REFERENCE_IMAGE_DIGEST = (
    "sha256:f40cd6034fb8c54dce6a85338750fbad79f387e2705e1991a85f2e7086b5b9ea"
)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def load_oracle(
    path: Path,
    *,
    expected_sha256: str,
    profile: str,
    mode: str,
    query_sha256: str,
    canonical_query_sha256: str,
    dataset_sha256: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    observed_sha256 = file_sha256(path)
    if not hmac.compare_digest(observed_sha256, expected_sha256):
        raise ValueError(
            f"ClickBench oracle SHA-256 is {observed_sha256}, expected {expected_sha256}"
        )
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict) or value.get("schema") != SCHEMA:
        raise ValueError(f"ClickBench oracle schema must be {SCHEMA}")
    expected_header = {
        "profile": profile,
        "mode": mode,
        "query_count": EXPECTED_QUERIES,
        "query_sha256": query_sha256,
        "canonical_query_sha256": canonical_query_sha256,
        "dataset_sha256": dataset_sha256,
        "checksum_algorithm": CHECKSUM_ALGORITHM,
    }
    for field, expected in expected_header.items():
        if value.get(field) != expected:
            raise ValueError(
                f"ClickBench oracle {field} is {value.get(field)!r}, expected {expected!r}"
            )
    results = value.get("results")
    if not isinstance(results, list) or len(results) != EXPECTED_QUERIES:
        raise ValueError("ClickBench oracle must contain exactly 43 query results")
    for number, result in enumerate(results, start=1):
        if not isinstance(result, dict) or result.get("query") != number:
            raise ValueError("ClickBench oracle query numbers must be contiguous from 1")
        rows, checksum = result.get("rows"), result.get("checksum")
        if type(rows) is not int or rows < 0:
            raise ValueError(f"ClickBench oracle query {number} has invalid rows")
        if not isinstance(checksum, str) or re.fullmatch(r"[0-9a-f]{64}", checksum) is None:
            raise ValueError(f"ClickBench oracle query {number} has invalid checksum")
    reference = value.get("reference")
    semantic_results = reference.get("semantic_results") if isinstance(reference, dict) else None
    engine = reference.get("engine", {}) if isinstance(reference, dict) else {}
    if (
        not isinstance(reference, dict)
        or engine.get("image_digest") != REFERENCE_IMAGE_DIGEST
        or reference.get("verified_queries") != EXPECTED_QUERIES
        or reference.get("q24_event_time_watch_id_duplicate_groups") != 0
        or reference.get("q04_exact_ratio", {}).get("expected_f64_bits")
        != "0x43bb0960eb622986"
        or not isinstance(semantic_results, list)
        or len(semantic_results) != EXPECTED_QUERIES
    ):
        raise ValueError("ClickBench oracle lacks the pinned independent reference")
    for number, result in enumerate(semantic_results, start=1):
        if (
            not isinstance(result, dict)
            or result.get("query") != number
            or type(result.get("rows")) is not int
            or result["rows"] < 0
        ):
            raise ValueError("ClickBench oracle reference results are not contiguous")
    identity = {
        "path": str(path),
        "sha256": observed_sha256,
        "expected_sha256": expected_sha256,
        "identity_verified": True,
    }
    return value, identity


def compare_result(
    result: dict[str, Any],
    expected: dict[str, Any],
    checksum_algorithm: str,
) -> str | None:
    mismatches = []
    if result.get("checksum_algorithm") != checksum_algorithm:
        mismatches.append(
            f"checksum algorithm {result.get('checksum_algorithm')!r} != {checksum_algorithm!r}"
        )
    if result.get("result_rows") != expected["rows"]:
        mismatches.append(f"rows {result.get('result_rows')!r} != {expected['rows']}")
    if result.get("result_checksum_sha256") != expected["checksum"]:
        mismatches.append("typed checksum differs")
    return "; ".join(mismatches) or None
