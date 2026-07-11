#!/usr/bin/env python3

import argparse
import csv
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN
import hashlib
import json
from pathlib import Path


QUANTUM = Decimal("0.000001")
EMPTY_RESULT = b"[]\n"


def canonical_value(value: str) -> str:
    if value == "":
        return ""
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


def canonical_bytes(path: Path) -> bytes:
    with path.open(newline="", encoding="utf-8") as source:
        rows = list(csv.reader(source))
    if len(rows) <= 1:
        return EMPTY_RESULT
    width = len(rows[0])
    if any(len(row) != width for row in rows):
        raise SystemExit(f"inconsistent CSV row width: {path}")
    body = sorted([canonical_value(value) for value in row] for row in rows[1:])
    records = [rows[0], *body]
    return b"".join(
        (json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
        for row in records
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    content = canonical_bytes(args.input)
    args.output.write_bytes(content)
    print(hashlib.sha256(content).hexdigest())


if __name__ == "__main__":
    main()
