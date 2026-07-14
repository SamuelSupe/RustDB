import json
import tempfile
import unittest
from pathlib import Path

from v05_resource_gate_fixture import (
    CANDIDATE_BINARY,
    CANDIDATE_BUILD,
    ROOT,
    ResourceGateFixture,
    timed_runs,
)
from v05_resource_gate import GateError, JOIN_TEMPLATES


class V05ResourceGateTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.fixture = ResourceGateFixture(Path(self.temporary.name))

    def tearDown(self):
        self.temporary.cleanup()

    def test_boundary_values_and_strict_evidence_pass(self):
        result = self.fixture.evaluate()
        self.assertEqual(result["status"], "pass")
        self.assertEqual(result["evidence"]["candidate_build_id"], CANDIDATE_BUILD)
        self.assertEqual(result["evidence"]["low_memory_cases"], 24)
        self.assertRegex(result["evidence"]["q17_checksum"], r"^[0-9a-f]{64}$")
        self.assertRegex(result["evidence"]["q21_checksum"], r"^[0-9a-f]{64}$")
        self.assertFalse(result["evidence"]["legacy_v04_missing_dataset"])
        self.assertAlmostEqual(result["q21"]["candidate_to_baseline_p50"], 0.42)
        self.assertEqual({join["query"] for join in result["joins"]}, set(JOIN_TEMPLATES))

    def test_each_q17_limit_is_enforced(self):
        cases = {
            "spill_write_bytes": 8 * 1024**3 + 1,
            "peak_active_spill_bytes": 2 * 1024**3 + 1,
            "max_repartition_depth": 2,
            "peak_active_spill_files": 513,
        }
        original = self.fixture.load("q17")
        for field, value in cases.items():
            with self.subTest(field=field):
                changed = json.loads(json.dumps(original))
                changed["runs"][0][field] = value
                self.fixture.replace("q17", changed)
                with self.assertRaisesRegex(GateError, field):
                    self.fixture.evaluate()
        self.fixture.replace("q17", original)

    def test_candidate_terminal_resources_must_be_released(self):
        original = self.fixture.load("q17")
        for field, value in (
            ("current_memory_bytes", 1),
            ("active_spill_bytes", 1),
            ("active_spill_files", 1),
            ("spill_cleaned", False),
        ):
            with self.subTest(field=field):
                changed = json.loads(json.dumps(original))
                changed["runs"][0][field] = value
                self.fixture.replace("q17", changed)
                with self.assertRaisesRegex(GateError, field):
                    self.fixture.evaluate()
        self.fixture.replace("q17", original)

    def test_terminal_resource_check_covers_q21_and_join_reports(self):
        for name in ("q21_candidate", "inner-join"):
            with self.subTest(report=name):
                original = self.fixture.load(name)
                changed = json.loads(json.dumps(original))
                changed["runs"][0]["spill_cleaned"] = False
                self.fixture.replace(name, changed)
                with self.assertRaisesRegex(GateError, "spill_cleaned"):
                    self.fixture.evaluate()
                self.fixture.replace(name, original)

    def test_q21_timing_thresholds_and_summary_are_enforced(self):
        original = self.fixture.load("q21_candidate")
        changed = json.loads(json.dumps(original))
        changed.update(timed_runs([51]))
        changed["runs"][0].update(
            current_memory_bytes=0,
            active_spill_bytes=0,
            active_spill_files=0,
            spill_cleaned=True,
        )
        self.fixture.replace("q21_candidate", changed)
        with self.assertRaisesRegex(GateError, "candidate/baseline p50 ratio"):
            self.fixture.evaluate()

        changed = json.loads(json.dumps(original))
        changed["p95_ms"] = changed["p50_ms"] + 1
        self.fixture.replace("q21_candidate", changed)
        with self.assertRaisesRegex(GateError, "p95_ms does not match"):
            self.fixture.evaluate()

        changed = json.loads(json.dumps(original))
        changed["p50_ms"] = 1
        self.fixture.replace("q21_candidate", changed)
        with self.assertRaisesRegex(GateError, "p50_ms does not match"):
            self.fixture.evaluate()

    def test_query_identity_checks_name_content_and_single_root(self):
        q17 = self.fixture.load("q17")
        q17["query_file"] = q17["query_file"].replace("q17.sql", "q16.sql")
        self.fixture.replace("q17", q17)
        with self.assertRaisesRegex(GateError, "identify q17.sql"):
            self.fixture.evaluate()

        q17 = self.fixture.load("q17")
        q17["query_file"] = str(self.fixture.render(
            ROOT / "benchmarks/tpch/q17.sql",
            "changed/q17.sql",
            "/workspace/data/tpch-sf10",
        ))
        Path(q17["query_file"]).write_text("SELECT 1;\n", encoding="utf-8")
        self.fixture.replace("q17", q17)
        with self.assertRaisesRegex(GateError, "canonical q17"):
            self.fixture.evaluate()

    def test_removed_container_query_path_can_use_report_sidecar(self):
        baseline = self.fixture.load("q21_baseline")
        baseline["query_file"] = "/workspace/removed-worktree/q21.sql"
        self.fixture.render(
            ROOT / "benchmarks/tpch/q21.sql", "q21.sql", "/dataset"
        )
        self.fixture.replace("q21_baseline", baseline)
        self.assertEqual(self.fixture.evaluate()["status"], "pass")

    def test_config_and_cpu_are_fixed(self):
        original = self.fixture.load("q17")
        for field, value in (
            ("memory_limit_bytes", 64 * 1024**2),
            ("compute_threads", 1),
            ("batch_size", 4096),
            ("io_concurrency", 16),
        ):
            with self.subTest(field=field):
                changed = json.loads(json.dumps(original))
                changed["config"][field] = value
                self.fixture.replace("q17", changed)
                with self.assertRaisesRegex(GateError, field):
                    self.fixture.evaluate()
        changed = json.loads(json.dumps(original))
        changed["environment"]["cpu_model"] = "Apple M4 Max"
        self.fixture.replace("q17", changed)
        with self.assertRaisesRegex(GateError, "M5 Max"):
            self.fixture.evaluate()

    def test_clean_build_and_binary_identity_are_required(self):
        q17 = self.fixture.load("q17")
        q17["build_id"] = CANDIDATE_BUILD + "-dirty"
        self.fixture.replace("q17", q17)
        with self.assertRaisesRegex(GateError, "exact clean 40-character"):
            self.fixture.evaluate()

        q17 = self.fixture.load("q17")
        q17["build_id"] = "e" * 40
        q17["binary_sha256"] = "f" * 64
        self.fixture.replace("q17", q17)
        with self.assertRaisesRegex(GateError, "required build|rebuilt candidate"):
            self.fixture.evaluate()

    def test_candidate_reports_must_share_build_binary_dataset_and_root(self):
        original = self.fixture.load("q21_candidate")
        changes = (
            ("build_id", "e" * 40, "build"),
            ("binary_sha256", "f" * 64, "binary"),
        )
        for field, value, message in changes:
            with self.subTest(field=field):
                changed = json.loads(json.dumps(original))
                changed[field] = value
                self.fixture.replace("q21_candidate", changed)
                with self.assertRaisesRegex(GateError, message):
                    self.fixture.evaluate(candidate_build_id=None, candidate_binary_sha256=None)
        changed = json.loads(json.dumps(original))
        changed["dataset"]["manifest_sha256"] = "e" * 64
        changed["dataset"].pop("manifest")
        self.fixture.replace("q21_candidate", changed)
        with self.assertRaisesRegex(GateError, "dataset"):
            self.fixture.evaluate(expected_dataset_manifest_sha256=None)

        changed = json.loads(json.dumps(original))
        changed["query_file"] = str(self.fixture.render(
            ROOT / "benchmarks/tpch/q21.sql",
            "different-root/q21.sql",
            "/workspace/data/other-tpch-sf10",
        ))
        self.fixture.replace("q21_candidate", changed)
        with self.assertRaisesRegex(GateError, "query root"):
            self.fixture.evaluate()

    def test_baseline_and_candidate_binary_digests_must_differ(self):
        baseline = self.fixture.load("q21_baseline")
        baseline["binary_sha256"] = CANDIDATE_BINARY
        self.fixture.replace("q21_baseline", baseline)
        with self.assertRaisesRegex(GateError, "binary digests must differ"):
            self.fixture.evaluate()

    def test_q21_baseline_and_candidate_configs_must_be_comparable(self):
        original = self.fixture.load("q21_baseline")
        cases = (
            ("config", "metadata_cache_bytes", 1, "metadata_cache_bytes"),
            ("build", "rustc_version", "rustc other", "incomparable"),
            ("environment", "arch", "x86_64", "incomparable"),
        )
        for section, field, value, message in cases:
            with self.subTest(field=field):
                changed = json.loads(json.dumps(original))
                changed[section][field] = value
                self.fixture.replace("q21_baseline", changed)
                with self.assertRaisesRegex(GateError, message):
                    self.fixture.evaluate()

    def test_dataset_must_be_sf10_and_match_manifest(self):
        original = self.fixture.load("q17")
        q17 = json.loads(json.dumps(original))
        q17["dataset"]["generation"]["scale_factor"] = "1"
        self.fixture.replace("q17", q17)
        with self.assertRaisesRegex(GateError, "SF10"):
            self.fixture.evaluate()

        q17 = json.loads(json.dumps(original))
        q17["dataset"]["manifest_sha256"] = "e" * 64
        self.fixture.replace("q17", q17)
        with self.assertRaisesRegex(GateError, "required SF10 manifest"):
            self.fixture.evaluate()

    def test_q17_and_q21_each_require_one_valid_checksum(self):
        for path, contents in (
            (self.fixture.q17_checksum, "not-a-checksum\n"),
            (self.fixture.q21_checksum, "a" * 64 + "\n" + "b" * 64 + "\n"),
        ):
            with self.subTest(path=path.name):
                original = path.read_text(encoding="utf-8")
                path.write_text(contents, encoding="utf-8")
                with self.assertRaisesRegex(GateError, "exactly one lowercase SHA-256"):
                    self.fixture.evaluate()
                path.write_text(original, encoding="utf-8")

if __name__ == "__main__":
    unittest.main()
