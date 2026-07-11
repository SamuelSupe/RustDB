#!/usr/bin/env python3

import argparse
import hashlib
from pathlib import Path


TABLES = (
    "customer",
    "lineitem",
    "nation",
    "orders",
    "part",
    "partsupp",
    "region",
    "supplier",
)


def parquet_files(root: Path) -> list[Path]:
    return sorted(root.glob("*/*.parquet"), key=lambda path: path.as_posix())


def digest(path: Path) -> str:
    checksum = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            checksum.update(chunk)
    return checksum.hexdigest()


def expected_files(root: Path) -> list[Path]:
    return [root / table / "part-00000.parquet" for table in TABLES]


def create(root: Path) -> None:
    files = parquet_files(root)
    expected = expected_files(root)
    if files != expected:
        raise SystemExit("generated dataset does not contain exactly one Parquet file per TPC-H table")
    content = "".join(f"{digest(path)}  {path.relative_to(root).as_posix()}\n" for path in files)
    (root / "manifest.sha256").write_text(content, encoding="utf-8")


def verify(root: Path) -> None:
    manifest = root / "manifest.sha256"
    if not manifest.is_file():
        raise SystemExit(f"missing dataset manifest: {manifest}")
    expected = expected_files(root)
    if parquet_files(root) != expected:
        raise SystemExit("dataset file set differs from its expected eight TPC-H tables")
    records = {}
    for line in manifest.read_text(encoding="utf-8").splitlines():
        checksum, separator, relative = line.partition("  ")
        if not separator or relative in records:
            raise SystemExit(f"invalid manifest line: {line!r}")
        records[relative] = checksum
    expected_names = [path.relative_to(root).as_posix() for path in expected]
    if sorted(records) != expected_names:
        raise SystemExit("dataset manifest file set is invalid")
    for relative in expected_names:
        if digest(root / relative) != records[relative]:
            raise SystemExit(f"dataset checksum mismatch: {relative}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("create", "verify"))
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    if args.action == "create":
        create(args.root)
    else:
        verify(args.root)


if __name__ == "__main__":
    main()
