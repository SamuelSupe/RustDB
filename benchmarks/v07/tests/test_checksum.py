from __future__ import annotations

import sys
import unittest
from pathlib import Path


TOOLS = Path(__file__).resolve().parents[3] / "tools" / "duckdb-bench"
sys.path.insert(0, str(TOOLS))

from checksum import MultisetChecksum


class ChecksumTests(unittest.TestCase):
    def checksum(self, rows: list[tuple[object, ...]]) -> str:
        checksum = MultisetChecksum()
        for row in rows:
            checksum.update_encoded(repr(row).encode())
        return checksum.finish()

    def test_is_order_independent_and_duplicate_sensitive(self) -> None:
        rows = [("a", 1), ("b", 2)]
        self.assertEqual(self.checksum(rows), self.checksum(list(reversed(rows))))
        self.assertNotEqual(self.checksum(rows), self.checksum(rows + [("a", 1)]))


if __name__ == "__main__":
    unittest.main()
