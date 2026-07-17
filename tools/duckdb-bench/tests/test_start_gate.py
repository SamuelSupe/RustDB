from __future__ import annotations

import concurrent.futures
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from start_gate import QueryStartGate


class QueryStartGateTests(unittest.TestCase):
    def test_releases_four_ready_workers_from_one_origin(self) -> None:
        gate = QueryStartGate(4, 5)
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(gate.wait_for_start) for _ in range(4)]
            gate.wait_until_ready()
            origin = gate.release()
            starts = [future.result() for future in futures]

        self.assertTrue(all(observed == origin for observed, _ in starts))
        self.assertTrue(all(started >= origin for _, started in starts))


if __name__ == "__main__":
    unittest.main()
