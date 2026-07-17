from __future__ import annotations

import hashlib
import math
import struct
from dataclasses import dataclass

import pyarrow as pa

from checksum import MultisetChecksum


_CANONICAL_NAN_BITS = 0x7FF8_0000_0000_0000
_I64_MIN = -(1 << 63)
_I64_MAX = (1 << 63) - 1


@dataclass(frozen=True, slots=True)
class Column:
    kind: str
    tag: bytes
    offset: int
    validity: memoryview | None = None
    values: memoryview | None = None
    extra: memoryview | int | str | None = None


def update_batch(checksum: MultisetChecksum, batch: pa.RecordBatch) -> None:
    columns = [prepare_column(column) for column in batch.columns]
    prefix = struct.pack("<I", batch.num_columns)
    for row in range(batch.num_rows):
        digest = hashlib.sha256()
        digest.update(prefix)
        for column in columns:
            encode_value(digest, column, row)
        checksum.update_digest(digest.digest())


def prepare_column(array: pa.Array) -> Column:
    data_type = array.type
    buffers = array.buffers()
    validity = view(buffers[0])
    values = view(buffers[1]) if len(buffers) > 1 else None
    offset = array.offset

    if pa.types.is_null(data_type):
        # DuckDB exports an untyped SQL NULL as Int32. Canonicalize Arrow Null
        # to the same logical family while retaining explicit typed NULLs.
        return Column("null", b"i", offset)
    if pa.types.is_boolean(data_type):
        return Column("bool", b"b", offset, validity, values)
    if pa.types.is_integer(data_type):
        width = data_type.bit_width // 8
        signed = pa.types.is_signed_integer(data_type)
        return Column("int", b"i", offset, validity, values, width if signed else -width)
    if pa.types.is_float32(data_type):
        return Column("float32", b"f", offset, validity, values)
    if pa.types.is_float64(data_type):
        return Column("float64", b"f", offset, validity, values)
    if pa.types.is_decimal128(data_type):
        return Column("decimal", b"d", offset, validity, values, data_type.scale)
    if pa.types.is_string(data_type) or pa.types.is_binary(data_type):
        return Column(
            "bytes32",
            b"s" if pa.types.is_string(data_type) else b"x",
            offset,
            validity,
            view(buffers[2]),
            values,
        )
    if pa.types.is_large_string(data_type) or pa.types.is_large_binary(data_type):
        return Column(
            "bytes64",
            b"s" if pa.types.is_large_string(data_type) else b"x",
            offset,
            validity,
            view(buffers[2]),
            values,
        )
    if pa.types.is_date32(data_type):
        return Column("date32", b"a", offset, validity, values)
    if pa.types.is_timestamp(data_type):
        return Column("timestamp", b"t", offset, validity, values, data_type.unit)
    raise TypeError(f"checksum does not yet support Arrow result type {data_type}")


def encode_value(digest: object, column: Column, row: int) -> None:
    index = column.offset + row
    if column.kind == "null" or not is_valid(column.validity, index):
        digest.update(column.tag + b"\x00")
        return

    if column.kind == "bool":
        value = bytes([(column.values[index >> 3] >> (index & 7)) & 1])
    elif column.kind == "int":
        width = abs(column.extra)
        start = index * width
        value = str(
            int.from_bytes(
                column.values[start : start + width],
                "little",
                signed=column.extra > 0,
            )
        ).encode()
    elif column.kind == "float32":
        number = struct.unpack_from("<f", column.values, index * 4)[0]
        value = canonical_float(number)
    elif column.kind == "float64":
        number = struct.unpack_from("<d", column.values, index * 8)[0]
        value = canonical_float(number)
    elif column.kind == "decimal":
        start = index * 16
        raw = int.from_bytes(column.values[start : start + 16], "little", signed=True)
        value = decimal_text(raw, column.extra).encode()
    elif column.kind in ("bytes32", "bytes64"):
        width = 4 if column.kind == "bytes32" else 8
        code = "<i" if width == 4 else "<q"
        start = struct.unpack_from(code, column.extra, index * width)[0]
        end = struct.unpack_from(code, column.extra, (index + 1) * width)[0]
        value = column.values[start:end]
    elif column.kind == "date32":
        value = column.values[index * 4 : index * 4 + 4]
    elif column.kind == "timestamp":
        raw = struct.unpack_from("<q", column.values, index * 8)[0]
        value = struct.pack("<q", timestamp_micros(raw, column.extra))
    else:
        raise AssertionError(f"unknown checksum column kind {column.kind}")
    write_present(digest, column.tag, value)


def write_present(digest: object, tag: bytes, value: bytes | memoryview) -> None:
    digest.update(tag + b"\x01")
    digest.update(struct.pack("<Q", len(value)))
    digest.update(value)


def is_valid(bitmap: memoryview | None, index: int) -> bool:
    return bitmap is None or ((bitmap[index >> 3] >> (index & 7)) & 1) == 1


def view(buffer: pa.Buffer | None) -> memoryview | None:
    return None if buffer is None else memoryview(buffer)


def canonical_float(value: float) -> bytes:
    if math.isnan(value):
        bits = _CANONICAL_NAN_BITS
    elif value == 0:
        bits = 0
    else:
        bits = struct.unpack("<Q", struct.pack("<d", value))[0]
    return struct.pack("<Q", bits)


def decimal_text(value: int, scale: int) -> str:
    negative = value < 0
    digits = str(abs(value))
    if scale > 0:
        digits = digits.rjust(scale + 1, "0")
        digits = f"{digits[:-scale]}.{digits[-scale:]}".rstrip("0").rstrip(".")
    elif scale < 0:
        digits += "0" * -scale
    if negative and digits != "0":
        digits = f"-{digits}"
    return digits


def timestamp_micros(value: int, unit: str) -> int:
    if unit == "s":
        value *= 1_000_000
    elif unit == "ms":
        value *= 1_000
    elif unit == "ns":
        if value % 1_000 != 0:
            raise ValueError("nanosecond Timestamp cannot be represented exactly in checksum v2")
        value //= 1_000
    elif unit != "us":
        raise ValueError(f"unsupported Timestamp unit {unit}")
    if not _I64_MIN <= value <= _I64_MAX:
        raise ValueError("Timestamp overflows checksum microsecond representation")
    return value
