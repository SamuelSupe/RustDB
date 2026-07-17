from __future__ import annotations

import sys
import unittest
from decimal import Decimal
from pathlib import Path


TOOLS = Path(__file__).resolve().parents[3] / "tools" / "duckdb-bench"
sys.path.insert(0, str(TOOLS))

try:
    import pyarrow as pa

    from checksum import MultisetChecksum
    from checksum_arrow import update_batch
except ModuleNotFoundError:
    pa = None


@unittest.skipIf(pa is None, "PyArrow is installed only in the pinned benchmark image")
class ArrowChecksumTests(unittest.TestCase):
    def checksum(self, batch: object) -> str:
        checksum = MultisetChecksum()
        update_batch(checksum, batch)
        return checksum.finish()

    def test_typed_nulls_do_not_hide_type_mismatches(self) -> None:
        integer = pa.record_batch([pa.array([None], type=pa.int64())], names=["value"])
        floating = pa.record_batch([pa.array([None], type=pa.float64())], names=["value"])
        self.assertNotEqual(self.checksum(integer), self.checksum(floating))

    def test_untyped_null_matches_duckdb_default_integer_null(self) -> None:
        untyped = pa.record_batch([pa.nulls(1)], names=["value"])
        integer = pa.record_batch([pa.array([None], type=pa.int32())], names=["value"])
        self.assertEqual(self.checksum(untyped), self.checksum(integer))

    def test_finite_float_and_timestamp_have_stable_v2_encoding(self) -> None:
        batch = pa.record_batch(
            [
                pa.array([1e20, -0.0, float("nan"), float("inf")], type=pa.float64()),
                pa.array([123_456, -1, None, 0], type=pa.timestamp("us")),
            ],
            names=["value", "time"],
        )
        self.assertEqual(
            self.checksum(batch),
            "ca02c4c79bed388dfc877ef715693cd03b629f3b0eee195845c1b0486d1f4a8c",
        )

    def test_v2_golden_covers_supported_scalar_families(self) -> None:
        batch = pa.record_batch(
            [
                pa.array([True, None], type=pa.bool_()),
                pa.array([2**64 - 1, 0], type=pa.uint64()),
                pa.array([Decimal("123.4500"), None], type=pa.decimal128(38, 4)),
                pa.array([1e20, float("nan")], type=pa.float64()),
                pa.array(["héllo", None], type=pa.string()),
                pa.array([b"\x00\xff", b""], type=pa.binary()),
                pa.array([1, -1], type=pa.date32()),
                pa.array([123_456, -1], type=pa.timestamp("us")),
            ],
            names=["flag", "unsigned", "decimal", "float", "text", "bytes", "date", "time"],
        )
        self.assertEqual(
            self.checksum(batch),
            "0b00e14b84691ab79ceeec727f7f8dcbfde96c03896117872f6787035f8115ad",
        )


if __name__ == "__main__":
    unittest.main()
