#!/usr/bin/env python3
"""Create a deterministic tar.gz archive from one distribution directory."""

from __future__ import annotations

import argparse
import gzip
import os
import tarfile
import tempfile
from pathlib import Path


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--epoch", type=int, default=0)
    return parser.parse_args()


def entries(source: Path) -> list[Path]:
    return [source, *sorted(source.rglob("*"), key=lambda path: path.as_posix())]


def archive_name(source: Path, path: Path) -> str:
    if path == source:
        return source.name
    return f"{source.name}/{path.relative_to(source).as_posix()}"


def mode(path: Path) -> int:
    if path.is_dir() or os.access(path, os.X_OK):
        return 0o755
    return 0o644


def add_entry(archive: tarfile.TarFile, source: Path, path: Path, epoch: int) -> None:
    if path.is_symlink() or not (path.is_dir() or path.is_file()):
        raise RuntimeError(f"unsupported distribution entry: {path}")
    info = tarfile.TarInfo(archive_name(source, path))
    info.uid = 0
    info.gid = 0
    info.uname = "root"
    info.gname = "root"
    info.mtime = epoch
    info.mode = mode(path)
    if path.is_dir():
        info.type = tarfile.DIRTYPE
        archive.addfile(info)
        return
    info.size = path.stat().st_size
    with path.open("rb") as payload:
        archive.addfile(info, payload)


def main() -> None:
    args = arguments()
    source = args.source.resolve()
    output = args.output.resolve()
    if not source.is_dir():
        raise RuntimeError(f"distribution source is not a directory: {source}")
    if args.epoch < 0:
        raise RuntimeError("archive epoch cannot be negative")
    output.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{output.name}.", suffix=".tmp", dir=output.parent
    )
    os.close(descriptor)
    try:
        with open(temporary, "wb") as raw:
            with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=args.epoch) as zipped:
                with tarfile.open(
                    fileobj=zipped, mode="w", format=tarfile.USTAR_FORMAT
                ) as archive:
                    for path in entries(source):
                        add_entry(archive, source, path, args.epoch)
        os.replace(temporary, output)
        os.chmod(output, 0o644)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


if __name__ == "__main__":
    main()
