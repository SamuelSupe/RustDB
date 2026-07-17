from __future__ import annotations

import re
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import duckdb

from disk import StorageSampler, common_storage_root
from marker import (
    MARKER_FORMAT_VERSION,
    is_sha256,
    marker_path,
    read_marker,
    remove_marker,
    setup_digest,
    statements_digest,
    write_marker_atomic,
)
from rss import RssSampler
from source import (
    validate_source_manifest,
    validate_statement_sources,
    verify_source_files,
)


class NativeSetup:
    def __init__(
        self,
        connection: duckdb.DuckDBPyConnection,
        database: str,
        temp_directory: str,
    ) -> None:
        self.connection = connection
        self.database = database
        self.temp_directory = temp_directory
        self.path = marker_path(database)
        self.marker = read_marker(self.path)
        self.attempted = False
        tables = user_table_names(connection)
        if self.marker is not None and self.marker["table_names"] != tables:
            raise RuntimeError(
                f"native setup marker table_names={self.marker['table_names']!r} "
                f"does not match database table_names={tables!r}"
            )

    @property
    def setup_id(self) -> str | None:
        if self.marker is None:
            return None
        return self.marker["setup_id"]

    def require_ready(self, requested_setup_id: Any) -> str:
        setup_id = self.setup_id
        if setup_id is None:
            raise RuntimeError("native benchmark run requires a completed setup marker")
        if not isinstance(requested_setup_id, str) or not is_sha256(requested_setup_id):
            raise ValueError("native run setup_id must be a lowercase SHA-256")
        if requested_setup_id != setup_id:
            raise RuntimeError(
                f"native run setup_id {requested_setup_id} does not match marker {setup_id}"
            )
        return setup_id

    def execute(self, command: dict[str, Any]) -> dict[str, Any]:
        if self.attempted:
            raise RuntimeError("native setup may be attempted only once per worker process")
        self.attempted = True
        setup = validate_setup_command(command)
        if self.marker is not None or self.path.exists():
            raise RuntimeError(f"native setup marker already exists: {self.path}")
        existing_tables = user_table_count(self.connection)
        if existing_tables != 0:
            raise RuntimeError(
                f"native setup requires an empty database, found {existing_tables} user table(s)"
            )

        root = common_storage_root(self.database, self.temp_directory)
        storage = StorageSampler(
            root,
            setup["max_storage_bytes"],
            on_exceeded=self.connection.interrupt,
        )
        rss = RssSampler()
        storage.start()
        rss.start()
        marker_value: dict[str, Any] | None = None
        load_elapsed_ms: float | None = None
        failure: Exception | None = None
        storage_values: tuple[int, int, int] | None = None
        rss_values: tuple[int, int] | None = None
        try:
            storage.check_limit()
            verify_source_files(setup["source_files"])
            started = time.perf_counter()
            for statement in setup["statements"]:
                storage.check_limit()
                self.connection.execute(statement)
                storage.check_limit()
            self.connection.execute("FORCE CHECKPOINT")
            load_elapsed_ms = (time.perf_counter() - started) * 1000
            storage.check_limit()
            table_names = user_table_names(self.connection)
            table_count = len(table_names)
            if table_count != len(setup["statements"]):
                raise RuntimeError(
                    f"native setup left {table_count} user table(s); "
                    f"expected {len(setup['statements'])}"
                )
            verify_source_files(setup["source_files"])
            marker_value = {
                "format_version": MARKER_FORMAT_VERSION,
                "setup_id": setup["setup_id"],
                "source_sha256": setup["source_sha256"],
                "source_bytes": setup["source_bytes"],
                "statements_sha256": setup["statements_sha256"],
                "table_count": table_count,
                "table_names": table_names,
            }
            write_marker_atomic(self.path, marker_value)
            storage.check_limit()
        except Exception as error:
            try:
                storage.check_limit()
            except Exception as quota_error:
                failure = quota_error
            else:
                failure = error
        finally:
            try:
                storage_values = storage.stop()
            except Exception as error:
                if failure is None:
                    failure = error
            try:
                rss_values = rss.stop()
            except Exception as error:
                if failure is None:
                    failure = error

        if failure is not None:
            remove_marker(self.path)
            raise failure
        if (
            storage_values is None
            or rss_values is None
            or marker_value is None
            or load_elapsed_ms is None
        ):
            remove_marker(self.path)
            raise RuntimeError("native setup sampling did not complete")

        storage_baseline, storage_peak, storage_final = storage_values
        if not (
            storage_baseline
            <= storage_final
            <= storage_peak
            <= setup["max_storage_bytes"]
        ):
            remove_marker(self.path)
            raise RuntimeError(
                "native setup storage invariant failed: "
                f"baseline={storage_baseline}, final={storage_final}, peak={storage_peak}, "
                f"limit={setup['max_storage_bytes']}"
            )
        rss_baseline, peak_rss = rss_values
        self.marker = marker_value
        return {
            "kind": "setup",
            "engine": "duckdb",
            "setup_id": setup["setup_id"],
            "complete": True,
            "load_elapsed_ms": load_elapsed_ms,
            "rss_baseline_bytes": rss_baseline,
            "peak_rss_bytes": peak_rss,
            "storage_baseline_bytes": storage_baseline,
            "storage_peak_bytes": storage_peak,
            "storage_final_bytes": storage_final,
            "table_count": marker_value["table_count"],
        }


def validate_setup_command(command: dict[str, Any]) -> dict[str, Any]:
    if command.get("storage_track") != "native":
        raise ValueError("setup storage_track must be 'native'")
    setup_id = command.get("setup_id")
    if not isinstance(setup_id, str) or not is_sha256(setup_id):
        raise ValueError("setup_id must be a lowercase SHA-256")
    source_sha256 = command.get("source_sha256")
    statements_sha256 = command.get("statements_sha256")
    if not isinstance(source_sha256, str) or not is_sha256(source_sha256):
        raise ValueError("source_sha256 must be a lowercase SHA-256")
    if not isinstance(statements_sha256, str) or not is_sha256(statements_sha256):
        raise ValueError("statements_sha256 must be a lowercase SHA-256")
    expected_setup_id = setup_digest(source_sha256, statements_sha256)
    if setup_id != expected_setup_id:
        raise ValueError(
            f"setup_id mismatch: expected {expected_setup_id}, got {setup_id}"
        )
    source_bytes = non_negative_integer(command.get("source_bytes"), "source_bytes")
    max_storage_bytes = positive_integer(
        command.get("max_storage_bytes"), "max_storage_bytes"
    )
    statements = command.get("statements")
    if not isinstance(statements, list) or not statements:
        raise ValueError("statements must be a non-empty array")
    if any(not isinstance(statement, str) or not statement.strip() for statement in statements):
        raise ValueError("every setup statement must be a non-empty string")
    for statement in statements:
        validate_ctas(statement)
    actual_digest = statements_digest(statements)
    if actual_digest != statements_sha256:
        raise ValueError(
            f"statements_sha256 mismatch: expected {statements_sha256}, got {actual_digest}"
        )
    source_files = validate_source_manifest(
        command.get("source_files"), source_sha256, source_bytes
    )
    validate_statement_sources(statements, source_files)
    return {
        "setup_id": setup_id,
        "source_sha256": source_sha256,
        "source_bytes": source_bytes,
        "statements_sha256": statements_sha256,
        "statements": statements,
        "source_files": source_files,
        "max_storage_bytes": max_storage_bytes,
    }


def validate_ctas(statement: str) -> None:
    sql = statement.strip()
    if sql.endswith(";"):
        sql = sql[:-1].rstrip()
    if ";" in sql:
        raise ValueError("each setup entry must contain exactly one CTAS statement")
    if re.match(r"(?is)^CREATE\s+TABLE\s+", sql) is None or re.search(
        r"(?is)\s+AS\s+", sql
    ) is None:
        raise ValueError("every setup statement must be CREATE TABLE ... AS ...")


def user_table_count(connection: duckdb.DuckDBPyConnection) -> int:
    return len(user_table_names(connection))


def user_table_names(connection: duckdb.DuckDBPyConnection) -> list[str]:
    rows = connection.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_catalog = current_database() "
        "AND table_schema NOT IN ('information_schema', 'pg_catalog') "
        "AND table_type = 'BASE TABLE'"
    ).fetchall()
    return sorted(row[0] for row in rows)


def non_negative_integer(value: Any, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def positive_integer(value: Any, name: str) -> int:
    value = non_negative_integer(value, name)
    if value == 0:
        raise ValueError(f"{name} must be positive")
    return value
