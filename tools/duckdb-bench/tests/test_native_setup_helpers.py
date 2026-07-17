from __future__ import annotations

import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


MODULE_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MODULE_ROOT))

from disk import StorageSampler, common_storage_root, storage_bytes
from marker import (
    MARKER_FORMAT_VERSION,
    marker_path,
    read_marker,
    setup_digest,
    statements_digest,
    write_marker_atomic,
)
from native_setup import NativeSetup, validate_setup_command
from source import (
    hash_file,
    source_digest,
    validate_source_manifest,
    validate_statement_sources,
    verify_source_files,
)


HASH = "a" * 64


class MarkerTests(unittest.TestCase):
    def test_statement_hash_uses_compact_utf8_json(self) -> None:
        statements = ["CREATE TABLE 数据 AS SELECT 1", "CREATE TABLE b AS SELECT 2"]
        expected = hashlib.sha256(
            json.dumps(
                statements,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode("utf-8")
        ).hexdigest()
        self.assertEqual(statements_digest(statements), expected)

    def test_marker_is_atomic_and_round_trips(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "benchmark.duckdb"
            path = marker_path(str(database))
            value = {
                "format_version": MARKER_FORMAT_VERSION,
                "setup_id": setup_digest(HASH, "b" * 64),
                "source_sha256": HASH,
                "source_bytes": 10,
                "statements_sha256": "b" * 64,
                "table_count": 2,
                "table_names": ["a", "数据"],
            }
            write_marker_atomic(path, value)

            self.assertEqual(path, Path(f"{database}.rustdb-v07-setup.json"))
            self.assertEqual(read_marker(path), value)
            self.assertEqual(list(path.parent.glob(f".{path.name}.*.tmp")), [])

            write_marker_atomic(path, value | {"unexpected": True})
            with self.assertRaisesRegex(RuntimeError, "unexpected fields"):
                read_marker(path)
            write_marker_atomic(path, value | {"format_version": 2})
            with self.assertRaisesRegex(RuntimeError, "format_version"):
                read_marker(path)


class DiskTests(unittest.TestCase):
    def test_sampler_defaults_to_two_millisecond_interval(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            sampler = StorageSampler(Path(directory), 32)
            self.assertEqual(sampler.interval_seconds, 0.002)

    def test_sampler_counts_the_common_root_and_tracks_peak(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            database = root / "database" / "benchmark.duckdb"
            temporary = root / "temporary"
            database.parent.mkdir()
            temporary.mkdir()
            database.write_bytes(b"db")
            self.assertEqual(common_storage_root(str(database), str(temporary)), root.resolve())
            self.assertEqual(storage_bytes(root), 2)

            sampler = StorageSampler(root, 32, interval_seconds=1)
            sampler.start()
            (temporary / "spill").write_bytes(b"12345")
            sampler.sample_now()
            baseline, peak, final = sampler.stop()
            self.assertEqual((baseline, final), (2, 7))
            self.assertGreaterEqual(peak, final)

    def test_sampler_interrupts_once_on_first_quota_excess(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            callbacks = []
            sampler = StorageSampler(
                root,
                4,
                interval_seconds=1,
                on_exceeded=lambda: callbacks.append("interrupt"),
            )
            sampler.start()
            (root / "large").write_bytes(b"12345")
            sampler.sample_now()
            sampler.sample_now()
            with self.assertRaisesRegex(RuntimeError, "storage exceeded"):
                sampler.check_limit()
            sampler.stop()
            self.assertEqual(callbacks, ["interrupt"])


class ValidationTests(unittest.TestCase):
    def test_setup_validation_accepts_only_matching_ctas_array(self) -> None:
        statements = [
            "CREATE TABLE native_data AS "
            "SELECT * FROM read_parquet('/bench-data/native/part.parquet')"
        ]
        statements_sha256 = statements_digest(statements)
        source_files = [
            {
                "relative_path": "native/part.parquet",
                "location": "/bench-data/native/part.parquet",
                "bytes": 123,
                "sha256": HASH,
            }
        ]
        source_sha256 = source_digest(source_files)
        command = {
            "storage_track": "native",
            "setup_id": setup_digest(source_sha256, statements_sha256),
            "source_sha256": source_sha256,
            "source_bytes": 123,
            "source_files": source_files,
            "statements_sha256": statements_sha256,
            "statements": statements,
            "max_storage_bytes": 1_000_000,
        }
        self.assertEqual(validate_setup_command(command)["statements"], statements)

        valid_setup_id = command["setup_id"]
        command["setup_id"] = "setup-1"
        with self.assertRaisesRegex(ValueError, "setup_id must be a lowercase SHA-256"):
            validate_setup_command(command)
        command["setup_id"] = "c" * 64
        with self.assertRaisesRegex(ValueError, "setup_id mismatch"):
            validate_setup_command(command)
        command["setup_id"] = valid_setup_id

        command["statements_sha256"] = "b" * 64
        command["setup_id"] = setup_digest(source_sha256, command["statements_sha256"])
        with self.assertRaisesRegex(ValueError, "statements_sha256 mismatch"):
            validate_setup_command(command)

        command["statements_sha256"] = statements_digest(["SELECT 1"])
        command["setup_id"] = setup_digest(source_sha256, command["statements_sha256"])
        command["statements"] = ["SELECT 1"]
        with self.assertRaisesRegex(ValueError, "CREATE TABLE"):
            validate_setup_command(command)

    def test_setup_rejects_invalid_source_references(self) -> None:
        files = [
            {
                "relative_path": "native/part.parquet",
                "location": "/bench-data/native/part.parquet",
                "bytes": 123,
                "sha256": HASH,
            }
        ]
        valid = [
            "CREATE TABLE native_data AS "
            "SELECT * FROM read_parquet('/bench-data/native/part.parquet')"
        ]
        validate_statement_sources(valid, files)
        invalid = {
            "literal": ["CREATE TABLE t AS SELECT * FROM read_parquet(path)"],
            "glob": [
                "CREATE TABLE t AS "
                "SELECT * FROM read_parquet('/bench-data/native/*.parquet')"
            ],
            "missing": ["CREATE TABLE t AS SELECT 1"],
            "extra": [
                "CREATE TABLE t AS SELECT * FROM read_parquet('/bench-data/native/part.parquet') "
                "UNION ALL SELECT * FROM read_parquet('/bench-data/native/extra.parquet')"
            ],
            "duplicate": [
                "CREATE TABLE t AS SELECT * FROM read_parquet('/bench-data/native/part.parquet') "
                "UNION ALL SELECT * FROM read_parquet('/bench-data/native/part.parquet')"
            ],
        }
        for label, statements in invalid.items():
            with self.subTest(label=label), self.assertRaises(ValueError):
                validate_statement_sources(statements, files)

    def test_restart_rejects_same_count_with_different_table_names(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "benchmark.duckdb"
            path = marker_path(str(database))
            value = {
                "format_version": MARKER_FORMAT_VERSION,
                "setup_id": setup_digest(HASH, "b" * 64),
                "source_sha256": HASH,
                "source_bytes": 10,
                "statements_sha256": "b" * 64,
                "table_count": 1,
                "table_names": ["expected"],
            }
            write_marker_atomic(path, value)
            with patch("native_setup.user_table_names", return_value=["different"]):
                with self.assertRaisesRegex(RuntimeError, "table_names"):
                    NativeSetup(object(), str(database), directory)


class SourceTests(unittest.TestCase):
    def test_source_manifest_verifies_before_and_after_import(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "table" / "part.parquet"
            source.parent.mkdir()
            source.write_bytes(b"parquet fixture")
            size, digest = hash_file(source)
            value = [
                {
                    "relative_path": "table/part.parquet",
                    "location": source.as_posix(),
                    "bytes": size,
                    "sha256": digest,
                }
            ]
            expected = source_digest(value)
            files = validate_source_manifest(value, expected, size)
            verify_source_files(files)
            source.write_bytes(b"changed")
            with self.assertRaisesRegex(RuntimeError, "identity changed"):
                verify_source_files(files)

    def test_source_manifest_rejects_globs_escapes_and_duplicates(self) -> None:
        entry = {
            "relative_path": "table/part.parquet",
            "location": "/bench-data/table/part.parquet",
            "bytes": 10,
            "sha256": HASH,
        }
        with self.assertRaisesRegex(ValueError, "contains a glob"):
            validate_source_manifest(
                [entry | {"location": "/bench-data/table/*.parquet"}], HASH, 10
            )
        with self.assertRaisesRegex(ValueError, "escapes"):
            validate_source_manifest(
                [entry | {"relative_path": "../part.parquet"}], HASH, 10
            )
        with self.assertRaisesRegex(ValueError, "duplicate relative_path"):
            validate_source_manifest([entry, entry], HASH, 20)


if __name__ == "__main__":
    unittest.main()
