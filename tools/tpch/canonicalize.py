#!/usr/bin/env python3

import argparse
import csv
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN
import hashlib
import json
from pathlib import Path
import re


QUANTUM = Decimal("0.000001")
ISO_TIMESTAMP = re.compile(
    r"^(?P<date>[0-9]{4,}-[0-9]{2}-[0-9]{2})[T ](?P<time>[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?)$"
)


def canonical_value(value: str) -> str:
    if value == "":
        return ""
    timestamp = ISO_TIMESTAMP.fullmatch(value)
    if timestamp is not None:
        # Arrow's CSV writer uses the ISO `T` separator while DuckDB's CSV
        # writer uses a space for the same timezone-free timestamp value.
        return f"{timestamp.group('date')} {timestamp.group('time')}"
    try:
        number = Decimal(value)
    except InvalidOperation:
        return value
    if not number.is_finite():
        return value
    if number == number.to_integral_value():
        return str(number.quantize(Decimal(1)))
    rounded = number.quantize(QUANTUM, rounding=ROUND_HALF_EVEN)
    return format(rounded, "f")


def canonical_bytes(path: Path, preserve_order: bool = False) -> bytes:
    with path.open(newline="", encoding="utf-8") as source:
        rows = list(csv.reader(source))
    if not rows or not rows[0]:
        raise SystemExit(f"CSV result is missing a header: {path}")
    width = len(rows[0])
    if any(len(row) != width for row in rows):
        raise SystemExit(f"inconsistent CSV row width: {path}")
    body = [[canonical_value(value) for value in row] for row in rows[1:]]
    if not preserve_order:
        body.sort()
    records = [rows[0], *body]
    return b"".join(
        (json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
        for row in records
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument(
        "--preserve-order",
        action="store_true",
        help="retain result row order while normalizing values",
    )
    args = parser.parse_args()
    content = canonical_bytes(args.input, preserve_order=args.preserve_order)
    args.output.write_bytes(content)
    print(hashlib.sha256(content).hexdigest())


if __name__ == "__main__":
    main()
