#!/usr/bin/env python3
"""Bind the declared Beta MinIO fixture to a live `mc ls` inventory."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from beta_acceptance_common import atomic_json, read_json, sha256


SCHEMA = "rustdb-beta-minio-verification-v1"


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    alias = subparsers.add_parser("alias-path")
    alias.add_argument("--manifest", type=Path, required=True)
    verify = subparsers.add_parser("verify")
    verify.add_argument("--manifest", type=Path, required=True)
    verify.add_argument("--listing", type=Path, required=True)
    verify.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def root_parts(manifest: dict[str, Any]) -> tuple[str, str]:
    root = manifest.get("root_uri")
    parsed = urlsplit(root) if isinstance(root, str) else None
    if parsed is None or parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError("normalized MinIO manifest has an invalid root_uri")
    prefix = parsed.path.strip("/")
    if not prefix:
        raise ValueError("Beta MinIO root_uri must include a dedicated prefix")
    return parsed.netloc, prefix


def alias_path(path: Path) -> str:
    bucket, prefix = root_parts(read_json(path))
    return f"{bucket}/{prefix}"


def live_objects(path: Path, bucket: str, prefix: str) -> dict[str, dict[str, Any]]:
    objects: dict[str, dict[str, Any]] = {}
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        try:
            value = json.loads(line)
        except json.JSONDecodeError as error:
            raise ValueError(f"invalid mc listing line {number}: {error}") from error
        if (
            not isinstance(value, dict)
            or value.get("status") != "success"
            or value.get("type") != "file"
        ):
            raise ValueError(f"mc listing line {number} is not a successful file")
        key, size, etag = value.get("key"), value.get("size"), value.get("etag")
        if (
            not isinstance(key, str)
            or not key
            or "/" in key
            or key in (".", "..")
            or type(size) is not int
            or size < 0
            or not isinstance(etag, str)
            or not etag.strip('"')
        ):
            raise ValueError(f"mc listing line {number} has invalid identity fields")
        uri = f"s3://{bucket}/{prefix}/{key}"
        if uri in objects:
            raise ValueError(f"mc listing contains duplicate object {uri}")
        objects[uri] = {"size": size, "etag": etag.strip('"')}
    return objects


def verify(manifest_path: Path, listing_path: Path, output: Path) -> dict[str, Any]:
    manifest = read_json(manifest_path)
    bucket, prefix = root_parts(manifest)
    declared_items = manifest.get("objects")
    if not isinstance(declared_items, list):
        raise ValueError("normalized MinIO manifest has no objects array")
    declared = {
        item["uri"]: {"size": item["size"], "etag": item["etag"].strip('"')}
        for item in declared_items
        if isinstance(item, dict)
        and isinstance(item.get("uri"), str)
        and type(item.get("size")) is int
        and isinstance(item.get("etag"), str)
    }
    if len(declared) != len(declared_items):
        raise ValueError("normalized MinIO manifest contains invalid or duplicate objects")
    live = live_objects(listing_path, bucket, prefix)
    if declared.keys() != live.keys():
        missing = sorted(declared.keys() - live.keys())[:3]
        extra = sorted(live.keys() - declared.keys())[:3]
        raise ValueError(f"live MinIO inventory differs; missing={missing}, extra={extra}")
    mismatched = [uri for uri in declared if declared[uri] != live[uri]]
    if mismatched:
        raise ValueError(f"live MinIO identity differs for {mismatched[:3]}")
    digest = hashlib.sha256()
    for uri in sorted(live):
        item = live[uri]
        digest.update(uri.encode("utf-8"))
        digest.update(item["size"].to_bytes(8, "little"))
        digest.update(item["etag"].encode("ascii"))
    result = {
        "schema": SCHEMA,
        "root_uri": manifest["root_uri"],
        "objects": len(live),
        "bytes": sum(item["size"] for item in live.values()),
        "inventory_sha256": digest.hexdigest(),
        "manifest_sha256": sha256(manifest_path),
        "listing_sha256": sha256(listing_path),
    }
    atomic_json(output, result)
    return result


def main() -> int:
    args = arguments()
    if args.command == "alias-path":
        print(alias_path(args.manifest))
        return 0
    result = verify(args.manifest, args.listing, args.output)
    print(args.output)
    print(json.dumps(result, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"beta acceptance MinIO verification: {error}", file=sys.stderr)
        raise SystemExit(2)
