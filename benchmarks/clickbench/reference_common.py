"""Shared identities and normalization for the ClickBench functional oracle."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any


QUERY_COUNT = 43
SOURCE_QUERY_SHA256 = (
    "a7d6673357348ee9680443216b6f26f30d1dce9f313b419d38502417b2c2a219"
)
EFFECTIVE_QUERY_SHA256 = (
    "5386a67950894eb01803dc4f216a0bda61bb219940f8b783a3c648f0c4f76749"
)
DATA_SHA256 = "fa134fe101e68324e0de851146fda69624f5cbb707d387141d1c2a88a219a16d"
REFERENCE_IMAGE = (
    "clickhouse/clickhouse-server@"
    "sha256:f40cd6034fb8c54dce6a85338750fbad79f387e2705e1991a85f2e7086b5b9ea"
)
REFERENCE_DIGEST = (
    "sha256:f40cd6034fb8c54dce6a85338750fbad79f387e2705e1991a85f2e7086b5b9ea"
)
SEMANTIC_ALGORITHM = "rustdb-clickbench-semantic-json-sha256-v1"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def require_sha(path: Path, expected: str, label: str) -> None:
    observed = sha256(path)
    if observed != expected:
        raise ValueError(f"{label} SHA-256 is {observed}, expected {expected}")


def load_queries(path: Path) -> list[str]:
    queries = [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("--")
    ]
    if len(queries) != QUERY_COUNT or any(not query.endswith(";") for query in queries):
        raise ValueError("the effective ClickBench query file must contain 43 statements")
    return queries


def reference_sql(
    query: str,
    number: int,
    data_name: str,
    source_columns: list[dict[str, str]],
) -> str:
    if number == 4:
        return (
            "SELECT sum(toInt128(UserID)), count() "
            f"FROM file('/data/{data_name}', Parquet) FORMAT JSONCompactEachRow"
        )
    rendered, replacements = re.subn(
        r"\bFROM\s+hits\b",
        f"FROM file('/data/{data_name}', Parquet)",
        query,
        flags=re.IGNORECASE,
    )
    if replacements != 1:
        raise ValueError(f"query {number} must contain exactly one FROM hits")
    rendered = re.sub(
        r"\bEventDate\b", "toDate(EventDate)", rendered, flags=re.IGNORECASE
    )
    rendered = re.sub(
        r"\bEventTime\b",
        "toDateTime(EventTime, 'UTC')",
        rendered,
        flags=re.IGNORECASE,
    )
    # RustDB's functional adapter casts these Binary columns to UTF-8 first;
    # SQL length therefore counts characters rather than encoded bytes.
    rendered = re.sub(r"\blength\(", "lengthUTF8(", rendered, flags=re.IGNORECASE)
    if number == 24:
        projection = ", ".join(
            binary_projection(column["name"])
            if is_string(column["type"])
            else quote_identifier(column["name"])
            for column in source_columns
        )
        rendered = rendered.replace("SELECT *", f"SELECT {projection}", 1)
    return rendered.removesuffix(";") + " FORMAT JSONCompactEachRow"


def quote_identifier(value: str) -> str:
    return f"`{value.replace('`', '``')}`"


def is_string(value: str) -> bool:
    return "String" in value


def binary_projection(value: str) -> str:
    identifier = quote_identifier(value)
    return f"lower(hex({identifier}))"


def semantic_checksum(rows: list[list[Any]]) -> str:
    encoded = [
        json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        for row in rows
    ]
    encoded.sort()
    digest = hashlib.sha256()
    for row in encoded:
        digest.update(len(row).to_bytes(8, "big"))
        digest.update(row)
    return digest.hexdigest()
