#!/usr/bin/env python3
"""Resume a large immutable HTTP object through bounded parallel ranges."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


PROCESS_LOCK = threading.Lock()
PROCESSES: Set[subprocess.Popen] = set()


def positive(value: str) -> int:
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="bounded parallel HTTP range download")
    parser.add_argument("--url", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--size", type=positive, required=True)
    parser.add_argument("--etag")
    parser.add_argument("--connections", type=positive, default=32)
    parser.add_argument("--chunk-bytes", type=positive, default=128 * 1024**2)
    return parser.parse_args()


def ranges(start: int, end: int, chunk_bytes: int) -> List[Tuple[int, int]]:
    chunks = []
    while start < end:
        stop = min(end - 1, start + chunk_bytes - 1)
        chunks.append((start, stop))
        start = stop + 1
    return chunks


def fetch(url: str, etag: Optional[str], part: Path, start: int, end: int) -> Path:
    expected = end - start + 1
    if part.is_file() and part.stat().st_size == expected:
        return part
    if part.exists():
        part.unlink()
    command = [
        "curl",
        "--fail",
        "--location",
        "--http1.1",
        "--silent",
        "--show-error",
        "--retry",
        "5",
        "--retry-all-errors",
        "--range",
        f"{start}-{end}",
        "--output",
        str(part),
    ]
    if etag:
        command.extend(("--header", f"If-Match: {etag}"))
    command.append(url)
    process = subprocess.Popen(command)
    with PROCESS_LOCK:
        PROCESSES.add(process)
    try:
        return_code = process.wait()
    finally:
        with PROCESS_LOCK:
            PROCESSES.discard(process)
    if return_code != 0:
        raise subprocess.CalledProcessError(return_code, command)
    actual = part.stat().st_size
    if actual != expected:
        raise RuntimeError(
            f"range {start}-{end} returned {actual} bytes instead of {expected}"
        )
    return part


def stop_downloads() -> None:
    with PROCESS_LOCK:
        processes = list(PROCESSES)
    for process in processes:
        process.terminate()


def main() -> int:
    args = arguments()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    current = args.output.stat().st_size if args.output.exists() else 0
    if current > args.size:
        raise RuntimeError(f"existing output is {current} bytes, above {args.size}")
    if current == args.size:
        print(f"download: complete ({current} bytes)", file=sys.stderr)
        return 0

    part_dir = args.output.with_name(args.output.name + ".parts")
    part_dir.mkdir(exist_ok=True)
    chunks = ranges(current, args.size, args.chunk_bytes)
    expected_parts = {
        f"{start:020d}-{end:020d}.part" for start, end in chunks
    }
    for stale in part_dir.glob("*.part"):
        if stale.name not in expected_parts:
            stale.unlink()
    in_flight: Dict[int, Future[Path]] = {}
    next_chunk = 0
    executor = ThreadPoolExecutor(max_workers=args.connections)
    try:
        while next_chunk < min(args.connections, len(chunks)):
            start, end = chunks[next_chunk]
            part = part_dir / f"{start:020d}-{end:020d}.part"
            in_flight[next_chunk] = executor.submit(
                fetch, args.url, args.etag, part, start, end
            )
            next_chunk += 1

        with args.output.open("ab") as output:
            for index, (start, end) in enumerate(chunks):
                part = in_flight.pop(index).result()
                with part.open("rb") as source:
                    while block := source.read(8 * 1024**2):
                        output.write(block)
                part.unlink()
                current = end + 1
                print(
                    f"download: {current}/{args.size} bytes "
                    f"({current * 100.0 / args.size:.1f}%)",
                    file=sys.stderr,
                    flush=True,
                )
                if next_chunk < len(chunks):
                    following_start, following_end = chunks[next_chunk]
                    following = part_dir / (
                        f"{following_start:020d}-{following_end:020d}.part"
                    )
                    in_flight[next_chunk] = executor.submit(
                        fetch,
                        args.url,
                        args.etag,
                        following,
                        following_start,
                        following_end,
                    )
                    next_chunk += 1
            output.flush()
            os.fsync(output.fileno())
    except BaseException:
        stop_downloads()
        executor.shutdown(wait=False, cancel_futures=True)
        raise
    else:
        executor.shutdown(wait=True)

    if args.output.stat().st_size != args.size:
        raise RuntimeError("assembled object has an unexpected size")
    try:
        part_dir.rmdir()
    except OSError:
        pass
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(f"download: error: {error}", file=sys.stderr)
        raise SystemExit(1)
