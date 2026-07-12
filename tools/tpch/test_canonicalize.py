#!/usr/bin/env python3

import tempfile
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from canonicalize import canonical_bytes


class CanonicalizeTests(unittest.TestCase):
    def test_header_only_result_preserves_its_schema(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            header = Path(directory, "header.csv")
            header.write_text("partkey,value\n", encoding="utf-8")

            self.assertEqual(canonical_bytes(header), b'["partkey","value"]\n')

    def test_different_empty_result_schemas_do_not_match(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            left = Path(directory, "left.csv")
            renamed = Path(directory, "renamed.csv")
            narrower = Path(directory, "narrower.csv")
            left.write_text("partkey,value\n", encoding="utf-8")
            renamed.write_text("partkey,total\n", encoding="utf-8")
            narrower.write_text("partkey\n", encoding="utf-8")

            self.assertNotEqual(canonical_bytes(left), canonical_bytes(renamed))
            self.assertNotEqual(canonical_bytes(left), canonical_bytes(narrower))

    def test_missing_header_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            empty = Path(directory, "empty.csv")
            empty.write_text("", encoding="utf-8")

            with self.assertRaisesRegex(SystemExit, "missing a header"):
                canonical_bytes(empty)

    def test_non_empty_result_keeps_header_and_canonical_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory, "result.csv")
            result.write_text("key,value\n2,1.2500004\n1,3\n", encoding="utf-8")

            self.assertEqual(
                canonical_bytes(result),
                b'["key","value"]\n["1","3"]\n["2","1.250000"]\n',
            )
            self.assertEqual(
                canonical_bytes(result, preserve_order=True),
                b'["key","value"]\n["2","1.250000"]\n["1","3"]\n',
            )

    def test_arrow_and_duckdb_timestamp_separators_are_equivalent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            arrow = Path(directory, "arrow.csv")
            duckdb = Path(directory, "duckdb.csv")
            arrow.write_text(
                "ts\n2024-03-29T12:34:56.123456\n", encoding="utf-8"
            )
            duckdb.write_text(
                "ts\n2024-03-29 12:34:56.123456\n", encoding="utf-8"
            )

            self.assertEqual(canonical_bytes(arrow), canonical_bytes(duckdb))


if __name__ == "__main__":
    unittest.main()
