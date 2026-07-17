from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any


MARKER_SUFFIX = ".rustdb-v07-setup.json"
MARKER_FORMAT_VERSION = 1
MARKER_FIELDS = {
    "format_version",
    "setup_id",
    "source_sha256",
    "source_bytes",
    "statements_sha256",
    "table_count",
    "table_names",
}


def marker_path(database: str) -> Path:
    return Path(f"{database}{MARKER_SUFFIX}")


def statements_digest(statements: list[str]) -> str:
    encoded = json.dumps(
        statements,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def setup_digest(source_sha256: str, statements_sha256: str) -> str:
    encoded = json.dumps(
        {
            "source_sha256": source_sha256,
            "statements_sha256": statements_sha256,
        },
        separators=(",", ":"),
        ensure_ascii=False,
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def read_marker(path: Path) -> dict[str, Any] | None:
    try:
        encoded = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    try:
        value = json.loads(encoded)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"invalid native setup marker {path}: {error}") from error
    validate_marker(value, path)
    return value


def validate_marker(value: Any, path: Path) -> None:
    if not isinstance(value, dict):
        raise RuntimeError(f"invalid native setup marker {path}: expected an object")
    if set(value) != MARKER_FIELDS:
        raise RuntimeError(f"invalid native setup marker {path}: unexpected fields")
    format_version = value.get("format_version")
    if (
        not isinstance(format_version, int)
        or isinstance(format_version, bool)
        or format_version != MARKER_FORMAT_VERSION
    ):
        raise RuntimeError(f"invalid native setup marker {path}: bad format_version")
    required_strings = ("setup_id", "source_sha256", "statements_sha256")
    for name in required_strings:
        if not isinstance(value.get(name), str) or not value[name]:
            raise RuntimeError(f"invalid native setup marker {path}: bad {name}")
    for name in required_strings:
        if not is_sha256(value[name]):
            raise RuntimeError(f"invalid native setup marker {path}: bad {name}")
    if value["setup_id"] != setup_digest(
        value["source_sha256"], value["statements_sha256"]
    ):
        raise RuntimeError(f"invalid native setup marker {path}: setup_id mismatch")
    for name in ("source_bytes", "table_count"):
        if (
            not isinstance(value.get(name), int)
            or isinstance(value[name], bool)
            or value[name] < 0
        ):
            raise RuntimeError(f"invalid native setup marker {path}: bad {name}")
    table_names = value.get("table_names")
    if (
        not isinstance(table_names, list)
        or any(not isinstance(name, str) or not name for name in table_names)
        or table_names != sorted(set(table_names))
        or len(table_names) != value["table_count"]
    ):
        raise RuntimeError(f"invalid native setup marker {path}: bad table_names")


def write_marker_atomic(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (
        json.dumps(value, separators=(",", ":"), ensure_ascii=False, sort_keys=True) + "\n"
    ).encode("utf-8")
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(encoded)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        _sync_directory(path.parent)
    except Exception:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
        raise


def remove_marker(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return
    _sync_directory(path.parent)


def is_sha256(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _sync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
