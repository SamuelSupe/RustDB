#!/usr/bin/env python3

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
from typing import Optional, Tuple


QUERY_ID = re.compile(r"q[0-9]{2}")


def digest(path: Path) -> str:
    checksum = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            checksum.update(chunk)
    return checksum.hexdigest()


def optional_digest(path: Optional[Path]) -> Optional[str]:
    return digest(path) if path is not None else None


def optional_positive_integer(value: str) -> Optional[int]:
    if not value:
        return None
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a positive integer") from error
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def query_metadata(workspace: Path, query_list: Path) -> dict:
    identifiers = [
        line.strip()
        for line in query_list.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    if not identifiers:
        raise SystemExit(f"query list is empty: {query_list}")
    if len(set(identifiers)) != len(identifiers):
        raise SystemExit(f"query list contains duplicate identifiers: {query_list}")

    files = []
    file_manifest = []
    for identifier in identifiers:
        if QUERY_ID.fullmatch(identifier) is None:
            raise SystemExit(f"invalid query identifier: {identifier}")
        path = workspace / "benchmarks" / "tpch" / f"{identifier}.sql"
        if not path.is_file():
            raise SystemExit(f"missing query template: {path}")
        checksum = digest(path)
        relative = path.relative_to(workspace).as_posix()
        files.append({"id": identifier, "path": relative, "sha256": checksum})
        file_manifest.append(f"{identifier}  {checksum}\n")

    combined = hashlib.sha256("".join(file_manifest).encode()).hexdigest()
    return {
        "list": query_list.relative_to(workspace).as_posix(),
        "list_sha256": digest(query_list),
        "files_sha256": combined,
        "files": files,
    }


def git_state(workspace: Path) -> Tuple[str, bool]:
    commit = subprocess.check_output(
        ["git", "-C", str(workspace), "rev-parse", "--verify", "HEAD"],
        text=True,
    ).strip()
    dirty = bool(
        subprocess.check_output(
            ["git", "-C", str(workspace), "status", "--porcelain"],
            text=True,
        ).strip()
    )
    return commit, dirty


def path_or_none(value: str) -> Optional[Path]:
    return Path(value) if value else None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--query-list", type=Path, required=True)
    parser.add_argument("--reference-manifest", type=Path, required=True)
    parser.add_argument("--rustdb-manifest", default="")
    parser.add_argument("--binary-path", required=True)
    parser.add_argument("--binary-sha256", required=True)
    parser.add_argument("--rustdb-root", required=True)
    parser.add_argument("--memory-limit", type=optional_positive_integer)
    parser.add_argument("--threads", type=optional_positive_integer)
    parser.add_argument("--batch-size", type=optional_positive_integer)
    parser.add_argument("--io-concurrency", type=optional_positive_integer)
    parser.add_argument("--require-spill", type=int, choices=(0, 1), required=True)
    parser.add_argument("--skip-build", type=int, choices=(0, 1), required=True)
    parser.add_argument("--s3-endpoint", default="")
    parser.add_argument("--s3-region", default="")
    parser.add_argument("--s3-path-style", default="")
    args = parser.parse_args()

    workspace = args.workspace.resolve()
    query_list = args.query_list.resolve()
    if re.fullmatch(r"[0-9a-f]{64}", args.binary_sha256) is None:
        raise SystemExit("RustDB binary SHA-256 must contain 64 lowercase hex digits")
    if not args.reference_manifest.is_file():
        raise SystemExit(f"reference dataset manifest is missing: {args.reference_manifest}")
    rustdb_manifest = path_or_none(args.rustdb_manifest)
    if rustdb_manifest is not None and not rustdb_manifest.is_file():
        raise SystemExit(f"RustDB dataset manifest is missing: {rustdb_manifest}")

    commit, dirty = git_state(workspace)
    provenance = {
        "format_version": 1,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": {"git_commit": commit, "worktree_dirty": dirty},
        "binary": {
            "path": args.binary_path,
            "sha256": args.binary_sha256,
            "build_skipped": bool(args.skip_build),
        },
        "queries": query_metadata(workspace, query_list),
        "dataset": {
            "reference_manifest_sha256": digest(args.reference_manifest),
            "rustdb_manifest_sha256": optional_digest(rustdb_manifest),
            "rustdb_root": args.rustdb_root,
        },
        "configuration": {
            "memory_limit_bytes": args.memory_limit,
            "compute_threads": args.threads,
            "batch_size": args.batch_size,
            "io_concurrency": args.io_concurrency,
            "require_spill": bool(args.require_spill),
            "spill_directory": "per-query-temporary" if args.require_spill else None,
            "s3_endpoint": args.s3_endpoint or None,
            "s3_region": args.s3_region or None,
            "s3_path_style": args.s3_path_style or None,
        },
    }
    args.output.write_text(
        json.dumps(provenance, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()
