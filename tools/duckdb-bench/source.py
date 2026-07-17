from __future__ import annotations

import hashlib
from pathlib import Path, PurePosixPath
from typing import Any

from marker import is_sha256


GLOB_CHARACTERS = "*?[]"


def validate_source_manifest(
    value: Any,
    expected_sha256: str,
    expected_bytes: int,
) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value:
        raise ValueError("source_files must be a non-empty array")
    files = [_validate_entry(entry, index) for index, entry in enumerate(value)]
    files.sort(key=lambda entry: entry["relative_path"])
    relative_paths = [entry["relative_path"] for entry in files]
    locations = [entry["location"] for entry in files]
    if len(set(relative_paths)) != len(relative_paths):
        raise ValueError("source_files contains a duplicate relative_path")
    if len(set(locations)) != len(locations):
        raise ValueError("source_files contains a duplicate location")
    roots = {entry["root"] for entry in files}
    if len(roots) != 1:
        raise ValueError("source_files locations do not share one source root")
    actual_bytes = sum(entry["bytes"] for entry in files)
    if actual_bytes != expected_bytes:
        raise ValueError(
            f"source_bytes mismatch: expected {expected_bytes}, got {actual_bytes}"
        )
    actual_sha256 = source_digest(files)
    if actual_sha256 != expected_sha256:
        raise ValueError(
            f"source_sha256 mismatch: expected {expected_sha256}, got {actual_sha256}"
        )
    return files


def verify_source_files(files: list[dict[str, Any]]) -> None:
    resolved_paths = set()
    root = files[0]["root_path"].resolve(strict=True)
    for entry in files:
        location = entry["location_path"]
        try:
            resolved = location.resolve(strict=True)
            resolved.relative_to(root)
        except (FileNotFoundError, ValueError) as error:
            raise RuntimeError(
                f"source file escapes or is missing: {entry['location']}"
            ) from error
        if resolved in resolved_paths:
            raise RuntimeError(f"source_files resolves to a duplicate file: {location}")
        resolved_paths.add(resolved)
        if not resolved.is_file():
            raise RuntimeError(f"source file is not a regular file: {location}")
        size, digest = hash_file(location)
        if size != entry["bytes"] or digest != entry["sha256"]:
            raise RuntimeError(
                f"source file identity changed: {entry['relative_path']} "
                f"at {entry['location']}"
            )


def validate_statement_sources(
    statements: list[str], files: list[dict[str, Any]]
) -> None:
    actual: list[str] = []
    for index, statement in enumerate(statements):
        locations = parquet_locations(statement, index)
        if not locations:
            raise ValueError(f"statements[{index}] must read at least one Parquet source")
        if any(
            any(character in location for character in GLOB_CHARACTERS)
            for location in locations
        ):
            raise ValueError(f"statements[{index}] read_parquet source contains a glob")
        actual.extend(locations)
    expected = [entry["location"] for entry in files]
    if sorted(actual) != sorted(expected):
        raise ValueError("setup CTAS sources do not match source_files exactly")


def parquet_locations(statement: str, statement_index: int) -> list[str]:
    locations: list[str] = []
    index = 0
    while index < len(statement):
        if statement.startswith("--", index):
            newline = statement.find("\n", index + 2)
            index = len(statement) if newline < 0 else newline + 1
            continue
        if statement.startswith("/*", index):
            end = statement.find("*/", index + 2)
            index = len(statement) if end < 0 else end + 2
            continue
        if statement[index] in "'\"":
            index = skip_quoted(statement, index, statement[index])
            continue
        if statement[index].isalnum() or statement[index] in "_$":
            end = index + 1
            while end < len(statement) and (
                statement[end].isalnum() or statement[end] in "_$"
            ):
                end += 1
            if statement[index:end].lower() == "read_parquet":
                location, index = literal_argument(statement, end, statement_index)
                locations.append(location)
            else:
                index = end
            continue
        index += 1
    return locations


def literal_argument(statement: str, index: int, statement_index: int) -> tuple[str, int]:
    index = skip_spaces(statement, index)
    if index >= len(statement) or statement[index] != "(":
        raise ValueError(f"statements[{statement_index}] read_parquet must be a function call")
    index = skip_spaces(statement, index + 1)
    if index >= len(statement) or statement[index] != "'":
        raise ValueError(
            f"statements[{statement_index}] read_parquet source must be one literal"
        )
    index += 1
    value: list[str] = []
    while index < len(statement):
        if statement[index] != "'":
            value.append(statement[index])
            index += 1
            continue
        if index + 1 < len(statement) and statement[index + 1] == "'":
            value.append("'")
            index += 2
            continue
        index = skip_spaces(statement, index + 1)
        if index >= len(statement) or statement[index] != ")":
            raise ValueError(
                f"statements[{statement_index}] read_parquet source must be one literal"
            )
        return "".join(value), index + 1
    raise ValueError(f"statements[{statement_index}] read_parquet source is unterminated")


def skip_spaces(statement: str, index: int) -> int:
    while index < len(statement) and statement[index].isspace():
        index += 1
    return index


def skip_quoted(statement: str, index: int, quote: str) -> int:
    index += 1
    while index < len(statement):
        if statement[index] != quote:
            index += 1
            continue
        if index + 1 < len(statement) and statement[index + 1] == quote:
            index += 2
            continue
        return index + 1
    return len(statement)


def source_digest(files: list[dict[str, Any]]) -> str:
    digest = hashlib.sha256()
    for entry in sorted(files, key=lambda item: item["relative_path"]):
        relative = entry["relative_path"].encode("utf-8")
        digest.update(len(relative).to_bytes(8, "little"))
        digest.update(relative)
        digest.update(entry["bytes"].to_bytes(8, "little"))
        digest.update(bytes.fromhex(entry["sha256"]))
    return digest.hexdigest()


def hash_file(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as source:
        while chunk := source.read(8 << 20):
            size += len(chunk)
            digest.update(chunk)
    return size, digest.hexdigest()


def _validate_entry(value: Any, index: int) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != {
        "relative_path",
        "location",
        "bytes",
        "sha256",
    }:
        raise ValueError(f"source_files[{index}] must contain identity fields")
    relative_path = value["relative_path"]
    location = value["location"]
    if not isinstance(relative_path, str) or not relative_path or "\x00" in relative_path:
        raise ValueError(f"source_files[{index}].relative_path is invalid")
    if not isinstance(location, str) or not location or "\x00" in location:
        raise ValueError(f"source_files[{index}].location is invalid")
    if any(character in relative_path for character in GLOB_CHARACTERS):
        raise ValueError(f"source_files[{index}].relative_path contains a glob")
    if any(character in location for character in GLOB_CHARACTERS):
        raise ValueError(f"source_files[{index}].location contains a glob")
    relative = PurePosixPath(relative_path)
    absolute = PurePosixPath(location)
    if (
        relative.is_absolute()
        or relative_path != relative.as_posix()
        or relative_path == "."
        or ".." in relative.parts
        or "\\" in relative_path
    ):
        raise ValueError(f"source_files[{index}].relative_path escapes the source root")
    if (
        not absolute.is_absolute()
        or location != absolute.as_posix()
        or ".." in absolute.parts
        or "\\" in location
    ):
        raise ValueError(f"source_files[{index}].location must be an absolute local path")
    location_path = Path(location)
    root_path = location_path
    for _ in relative.parts:
        root_path = root_path.parent
    if root_path.joinpath(*relative.parts) != location_path:
        raise ValueError(f"source_files[{index}].location escapes the source root")
    size = value["bytes"]
    if not isinstance(size, int) or isinstance(size, bool) or size < 0:
        raise ValueError(f"source_files[{index}].bytes must be a non-negative integer")
    sha256 = value["sha256"]
    if not isinstance(sha256, str) or not is_sha256(sha256):
        raise ValueError(f"source_files[{index}].sha256 must be a lowercase SHA-256")
    return {
        "relative_path": relative_path,
        "location": location,
        "location_path": location_path,
        "root": str(root_path),
        "root_path": root_path,
        "bytes": size,
        "sha256": sha256,
    }
