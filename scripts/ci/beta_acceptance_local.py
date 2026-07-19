#!/usr/bin/env python3
"""Revalidate the local Beta fixture after its acceptance queries."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

from beta_acceptance_common import atomic_json, read_json, sha256
from beta_acceptance_fixtures import LOCAL_SCHEMA


SCHEMA = "rustdb-beta-local-verification-v1"


def inventory(root: Path) -> tuple[list[dict[str, Any]], int, str]:
    files: list[dict[str, Any]] = []
    digest = hashlib.sha256()
    total = 0
    for path in sorted(root.iterdir(), key=lambda value: value.name):
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"local fixture must contain only flat regular files: {path}")
        stat = path.stat()
        name = path.name
        files.append({"path": name, "bytes": stat.st_size, "mtime_ns": stat.st_mtime_ns})
        encoded = name.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "little"))
        digest.update(encoded)
        digest.update(stat.st_size.to_bytes(8, "little"))
        digest.update(stat.st_mtime_ns.to_bytes(8, "little", signed=True))
        total += stat.st_size
    return files, total, digest.hexdigest()


def verify(manifest_path: Path, root: Path, output: Path) -> dict[str, Any]:
    manifest = read_json(manifest_path)
    if manifest.get("schema") != LOCAL_SCHEMA:
        raise ValueError("normalized local fixture manifest has the wrong schema")
    root = root.resolve()
    if str(root) != manifest.get("source_root"):
        raise ValueError("local fixture root differs from preflight")
    files, total, digest = inventory(root)
    if (
        files != manifest.get("files")
        or total != manifest.get("total_bytes")
        or digest != manifest.get("inventory_sha256")
    ):
        raise ValueError("local fixture inventory changed during Beta acceptance")
    result = {
        "schema": SCHEMA,
        "source_root": str(root),
        "files": len(files),
        "bytes": total,
        "inventory_sha256": digest,
        "manifest_sha256": sha256(manifest_path),
    }
    atomic_json(output, result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = verify(args.manifest, args.root, args.output)
    print(args.output)
    print(json.dumps(result, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"beta acceptance local verification: {error}", file=sys.stderr)
        raise SystemExit(2)
