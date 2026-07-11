#!/usr/bin/env python3

import tempfile
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from canonicalize import EMPTY_RESULT, canonical_bytes


class CanonicalizeTests(unittest.TestCase):
    def test_empty_and_header_only_results_are_the_same_zero_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            empty = Path(directory, "empty.csv")
            header = Path(directory, "header.csv")
            empty.write_text("", encoding="utf-8")
            header.write_text("partkey,value\n", encoding="utf-8")

            self.assertEqual(canonical_bytes(empty), EMPTY_RESULT)
            self.assertEqual(canonical_bytes(header), EMPTY_RESULT)

    def test_non_empty_result_keeps_header_and_canonical_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result = Path(directory, "result.csv")
            result.write_text("key,value\n2,1.2500004\n1,3\n", encoding="utf-8")

            self.assertEqual(
                canonical_bytes(result),
                b'["key","value"]\n["1","3"]\n["2","1.250000"]\n',
            )


if __name__ == "__main__":
    unittest.main()
