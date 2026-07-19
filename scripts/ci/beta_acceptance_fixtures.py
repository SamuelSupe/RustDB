"""Fixture contracts for the one-time Beta acceptance run."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from beta_acceptance_common import atomic_json, sha256

CLICKBENCH_DIR = Path(__file__).resolve().parents[2] / "benchmarks" / "clickbench"
sys.path.insert(0, str(CLICKBENCH_DIR))
from oracle import PINNED_SHA256, load_oracle  # noqa: E402


LOCAL_SCHEMA = "rustdb-beta-local-fixture-v1"
OBJECT_SCHEMA = "rustdb-beta-object-manifest-v1"
MIN_BYTES = 100 * 1024**3
MIN_FILES = 10_000
ACCEPTED_SUFFIXES = {
    "csv": (".csv", ".csv.gz", ".csv.zst", ".csv.zstd"),
    "parquet": (".parquet",),
}
CLICKBENCH = {
    "functional": (
        "hits-1m.parquet",
        122_446_530,
        '"843c108848a3929260d44588b39ec1b6-6"',
        "fa134fe101e68324e0de851146fda69624f5cbb707d387141d1c2a88a219a16d",
    ),
}
CLICKBENCH_CANONICAL_QUERY_SHA256 = (
    "a7d6673357348ee9680443216b6f26f30d1dce9f313b419d38502417b2c2a219"
)
CLICKBENCH_FUNCTIONAL_QUERY_SHA256 = (
    "5386a67950894eb01803dc4f216a0bda61bb219940f8b783a3c648f0c4f76749"
)


def absolute_path(value: str, label: str, kind: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise ValueError(f"{label} must be an absolute path")
    path = path.resolve()
    if kind == "file" and not path.is_file():
        raise ValueError(f"{label} is not a file: {path}")
    if kind == "directory" and not path.is_dir():
        raise ValueError(f"{label} is not a directory: {path}")
    return path


def write_acceptance_query(output: Path, name: str, location: str, track: str) -> dict[str, Any]:
    if track not in ACCEPTED_SUFFIXES:
        raise ValueError("fixture format must be csv or parquet")
    if "'" in location:
        raise ValueError("fixture location cannot contain a single quote")
    reader = "read_csv" if track == "csv" else "read_parquet"
    options = ", compression = 'auto'" if track == "csv" else ""
    path = output / f"{name}-query.sql"
    path.write_text(
        f"SELECT count(*) AS rustdb_beta_rows FROM {reader}('{location}/*'{options});\n",
        encoding="utf-8",
    )
    return {
        "path": str(path),
        "bytes": path.stat().st_size,
        "sha256": sha256(path),
        "owned_by_acceptance": True,
    }


def local_fixture(
    root: Path,
    output: Path,
    track: str,
) -> dict[str, Any]:
    suffixes = accepted_suffixes(track)
    files: list[dict[str, Any]] = []
    digest = hashlib.sha256()
    total = 0
    for path in sorted(root.iterdir(), key=lambda value: value.name):
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"local fixture must contain only flat regular files: {path}")
        if not path.name.lower().endswith(suffixes):
            raise ValueError(f"local fixture contains a non-{track} file: {path}")
        stat = path.stat()
        relative = path.name
        files.append(
            {"path": relative, "bytes": stat.st_size, "mtime_ns": stat.st_mtime_ns}
        )
        encoded = relative.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "little"))
        digest.update(encoded)
        digest.update(stat.st_size.to_bytes(8, "little"))
        digest.update(stat.st_mtime_ns.to_bytes(8, "little", signed=True))
        total += stat.st_size
    require_large("local fixture", len(files), total, "files")
    manifest_path = output / "local-fixture-manifest.json"
    atomic_json(
        manifest_path,
        {
            "schema": LOCAL_SCHEMA,
            "container_root": "/beta-data",
            "source_root": str(root),
            "format": track,
            "files": files,
            "total_bytes": total,
            "inventory_sha256": digest.hexdigest(),
        },
    )
    return {
        "root": str(root),
        "format": track,
        "files": len(files),
        "bytes": total,
        "inventory_sha256": digest.hexdigest(),
        "manifest": str(manifest_path),
        "manifest_sha256": sha256(manifest_path),
        "query": write_acceptance_query(output, "local", "/beta-data", track),
    }


def object_fixture(
    path: Path,
    output: Path,
    track: str,
) -> dict[str, Any]:
    accepted_suffixes(track)
    value = json.loads(path.read_text(encoding="utf-8"))
    if value.get("schema") != OBJECT_SCHEMA:
        raise ValueError(f"MinIO manifest schema must be {OBJECT_SCHEMA}")
    root_uri = value.get("root_uri")
    parsed = urlsplit(root_uri) if isinstance(root_uri, str) else None
    if (
        parsed is None
        or parsed.scheme != "s3"
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("MinIO root_uri must be a credential-free s3:// URI")
    root_uri = root_uri.rstrip("/")
    objects = value.get("objects")
    if not isinstance(objects, list):
        raise ValueError("MinIO manifest objects must be an array")
    normalized = normalize_objects(objects, root_uri, track)
    total = sum(item["size"] for item in normalized)
    require_large("MinIO fixture", len(normalized), total, "objects")
    normalized_path = output / "minio-fixture-manifest.json"
    atomic_json(
        normalized_path,
        {"schema": OBJECT_SCHEMA, "root_uri": root_uri, "objects": normalized},
    )
    return {
        "source_manifest": str(path),
        "source_manifest_sha256": sha256(path),
        "manifest": str(normalized_path),
        "manifest_sha256": sha256(normalized_path),
        "root_uri": root_uri,
        "format": track,
        "objects": len(normalized),
        "bytes": total,
        "query": write_acceptance_query(output, "minio", root_uri, track),
    }


def normalize_objects(objects: list[Any], root_uri: str, track: str) -> list[dict[str, Any]]:
    suffixes = accepted_suffixes(track)
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, item in enumerate(objects):
        if not isinstance(item, dict):
            raise ValueError(f"MinIO object {index} must be an object")
        uri, size = item.get("uri"), item.get("size")
        parsed = urlsplit(uri) if isinstance(uri, str) else None
        if (
            not isinstance(uri, str)
            or not uri.startswith(root_uri + "/")
            or parsed is None
            or parsed.scheme != "s3"
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or uri in seen
        ):
            raise ValueError(f"MinIO object {index} has a duplicate or out-of-root URI")
        if type(size) is not int or size < 0:
            raise ValueError(f"MinIO object {index} has an invalid size")
        relative = uri[len(root_uri) + 1 :]
        if "/" in relative or not relative.lower().endswith(suffixes):
            raise ValueError(f"MinIO fixture must contain only flat {track} objects")
        etag = item.get("etag")
        if not isinstance(etag, str) or not etag.strip('"'):
            raise ValueError(f"MinIO object {index} needs a non-empty etag")
        seen.add(uri)
        normalized.append({"uri": uri, "size": size, "etag": etag.strip('"')})
    return sorted(normalized, key=lambda item: item["uri"])


def accepted_suffixes(track: str) -> tuple[str, ...]:
    try:
        return ACCEPTED_SUFFIXES[track]
    except KeyError as error:
        raise ValueError("fixture format must be csv or parquet") from error


def require_large(label: str, count: int, total: int, unit: str) -> None:
    if count < MIN_FILES or total < MIN_BYTES:
        raise ValueError(
            f"{label} has {count} {unit}/{total} bytes; "
            f"requires at least {MIN_FILES} {unit}/{MIN_BYTES} bytes"
        )


def clickbench_fixture(root: Path, profile: str, oracle_path: Path) -> dict[str, Any]:
    if profile not in CLICKBENCH:
        raise ValueError("Beta acceptance requires the SHA-256-pinned functional profile")
    name, expected_bytes, expected_source_etag, expected_sha256 = CLICKBENCH[profile]
    canonical_query = root / "queries.sql"
    query = CLICKBENCH_DIR / "queries-rustdb.sql"
    data = root / name
    if not canonical_query.is_file() or not query.is_file() or not data.is_file():
        raise ValueError(f"ClickBench input must contain queries.sql and {name}")
    if data.stat().st_size != expected_bytes:
        raise ValueError(
            f"ClickBench {name} has {data.stat().st_size} bytes; expected {expected_bytes}"
        )
    query_sha256 = sha256(query)
    canonical_query_sha256 = sha256(canonical_query)
    data_sha256 = sha256(data)
    if canonical_query_sha256 != CLICKBENCH_CANONICAL_QUERY_SHA256:
        raise ValueError(
            "canonical ClickBench queries.sql SHA-256 is "
            f"{canonical_query_sha256}; expected {CLICKBENCH_CANONICAL_QUERY_SHA256}"
        )
    if query_sha256 != CLICKBENCH_FUNCTIONAL_QUERY_SHA256:
        raise ValueError(
            "ClickBench functional query SHA-256 is "
            f"{query_sha256}; expected {CLICKBENCH_FUNCTIONAL_QUERY_SHA256}"
        )
    if data_sha256 != expected_sha256:
        raise ValueError(
            f"ClickBench {name} SHA-256 is {data_sha256}; expected {expected_sha256}"
        )
    oracle, oracle_identity = load_oracle(
        oracle_path,
        expected_sha256=PINNED_SHA256,
        profile=profile,
        mode="execute",
        query_sha256=query_sha256,
        canonical_query_sha256=canonical_query_sha256,
        dataset_sha256=data_sha256,
    )
    return {
        "root": str(root),
        "profile": profile,
        "query": {
            "path": str(query),
            "bytes": query.stat().st_size,
            "sha256": query_sha256,
            "expected_sha256": CLICKBENCH_FUNCTIONAL_QUERY_SHA256,
            "identity_verified": True,
        },
        "canonical_query": {
            "path": str(canonical_query),
            "bytes": canonical_query.stat().st_size,
            "sha256": canonical_query_sha256,
            "expected_sha256": CLICKBENCH_CANONICAL_QUERY_SHA256,
            "identity_verified": True,
        },
        "data": {
            "path": str(data),
            "bytes": expected_bytes,
            "sha256": data_sha256,
            "expected_sha256": expected_sha256,
            "identity_verified": True,
            "expected_source_etag": expected_source_etag,
        },
        "oracle": oracle_identity
        | {
            "schema": oracle["schema"],
            "profile": oracle["profile"],
            "mode": oracle["mode"],
            "query_count": oracle["query_count"],
            "checksum_algorithm": oracle["checksum_algorithm"],
            "query_sha256": oracle["query_sha256"],
            "canonical_query_sha256": oracle["canonical_query_sha256"],
            "dataset_sha256": oracle["dataset_sha256"],
            "results": oracle["results"],
        },
    }
