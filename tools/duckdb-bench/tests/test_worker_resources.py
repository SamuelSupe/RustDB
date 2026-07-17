from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from worker_resources import parse_resources


STATUS = "Name:\tpython3\nCpus_allowed_list:\t0-3,8-11\n"
MEMINFO = "MemTotal:       8388608 kB\nMemFree:         100 kB\n"


class WorkerResourceTests(unittest.TestCase):
    def test_parses_finite_and_unlimited_resource_fixtures(self) -> None:
        finite = parse_resources(
            STATUS,
            "0-7\n",
            "800000 100000\n",
            "4294967296\n",
            MEMINFO,
        )
        self.assertEqual(finite["cpu_affinity_list"], "0-3,8-11")
        self.assertEqual(finite["cpu_affinity_count"], 8)
        self.assertEqual(finite["cgroup_cpuset_cpus_effective"], "0-7")
        self.assertEqual(finite["cgroup_cpu_quota_us"], 800_000)
        self.assertEqual(finite["cgroup_cpu_period_us"], 100_000)
        self.assertEqual(finite["cgroup_memory_max_bytes"], 4_294_967_296)
        self.assertEqual(finite["visible_memory_bytes"], 8 << 30)

        unlimited = parse_resources(STATUS, "0-7", "max 100000", "max", MEMINFO)
        self.assertIsNone(unlimited["cgroup_cpu_quota_us"])
        self.assertIsNone(unlimited["cgroup_memory_max_bytes"])

    def test_rejects_missing_or_malformed_resource_fixtures(self) -> None:
        cases = (
            ("Name:\tpython3\n", "0-7", "max 100000", "max", MEMINFO),
            (STATUS, "0-3,3-7", "max 100000", "max", MEMINFO),
            (STATUS, "0-7", "max 0", "max", MEMINFO),
            (STATUS, "0-7", "max 100000", "0", MEMINFO),
            (STATUS, "0-7", "max 100000", "max", "MemTotal: 1024 bytes\n"),
        )
        for values in cases:
            with self.subTest(values=values):
                with self.assertRaises(ValueError):
                    parse_resources(*values)


if __name__ == "__main__":
    unittest.main()
