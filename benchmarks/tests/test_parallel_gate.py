import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from parallel_gate import GateError, evaluate  # noqa: E402
from check_parallel_gate import clean_candidate_build_id  # noqa: E402


SHA = "a" * 64
CANDIDATE = "c" * 40


class GateFixture:
    def __init__(self, root: Path):
        self.root = root
        self.manifests = {}
        self.reports = {}
        self.entries = {}
        self._write_variant(
            "baseline",
            "alpha2-id",
            "0.1.0",
            {("scan-filter", 1): 100, ("scan-filter", 4): 90,
             ("aggregate", 1): 200, ("aggregate", 4): 180},
        )
        self._write_variant(
            "candidate",
            CANDIDATE,
            "0.2.0-alpha.1",
            {("scan-filter", 1): 105, ("scan-filter", 4): 50,
             ("aggregate", 1): 210, ("aggregate", 4): 100},
        )

    def _write_variant(self, variant, build_id, engine_version, timings):
        directory = self.root / variant
        reports = directory / "reports"
        reports.mkdir(parents=True)
        build = {
            "cargo_profile": "release",
            "rustflags": "-C target-cpu=native",
            "rustc_version": "rustc fixture",
        }
        entries = []
        for (case, threads), p50 in timings.items():
            report_path = reports / f"{case}-t{threads}.json"
            checksum_path = reports / f"{case}-t{threads}.checksum.txt"
            report = {
                "engine_version": engine_version,
                "build_id": build_id,
                "build": build,
                "warmup": 2,
                "iterations": 5,
                "config": {
                    "memory_limit_bytes": 1_073_741_824,
                    "compute_threads": threads,
                    "batch_size": 8192,
                    "io_concurrency": 32,
                    "metadata_cache_bytes": 67_108_864,
                },
                "environment": {"cpu_model": "Apple M5 Max"},
                "p50_ms": p50,
                "runs": [{"elapsed_ms": p50} for _ in range(5)],
            }
            report_path.write_text(json.dumps(report), encoding="utf-8")
            checksum_path.write_text(f"{SHA}\n", encoding="utf-8")
            entry = {
                "target": "local",
                "cache_mode": "metadata-warm",
                "case": case,
                "threads": threads,
                "batch_size": 8192,
                "warmup": 2,
                "iterations": 5,
                "report": f"reports/{report_path.name}",
                "checksum_report": f"reports/{checksum_path.name}",
            }
            entries.append(entry)
            self.reports[(variant, case, threads)] = report_path
            self.entries[(variant, case, threads)] = entry
        manifest = {
            "suite": "rustdb-baseline-v1",
            "memory_limit_bytes": 1_073_741_824,
            "rustdb_build_id": build_id,
            "build": build,
            "harness": {
                "runner_sha256": "d" * 64,
                "library_sha256": "e" * 64,
                "checksum_runner_sha256": "f" * 64,
            },
            "dataset": {
                "generation": {"duckdb": "1.4.3", "scale_factor": "10"},
                "manifest": "data/tpch-sf10/manifest.sha256",
                "manifest_sha256": "b" * 64,
            },
            "correctness": {"verified": True, "targets": ["local"]},
            "runs": entries,
        }
        manifest_path = directory / "manifest.json"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        self.manifests[variant] = manifest_path

    def rewrite_manifest(self, variant):
        path = self.manifests[variant]
        document = json.loads(path.read_text(encoding="utf-8"))
        document["runs"] = [
            self.entries[(variant, case, threads)]
            for case in ("scan-filter", "aggregate")
            for threads in (1, 4)
            if (variant, case, threads) in self.entries
        ]
        path.write_text(json.dumps(document), encoding="utf-8")


class ParallelGateTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.fixture = GateFixture(Path(self.temporary.name))

    def tearDown(self):
        self.temporary.cleanup()

    def evaluate(self):
        return evaluate(
            self.fixture.manifests["candidate"],
            self.fixture.manifests["baseline"],
            "alpha2-id",
            CANDIDATE,
        )

    def test_valid_fixture_passes(self):
        result = self.evaluate()
        self.assertEqual(result["status"], "pass")
        self.assertAlmostEqual(result["cases"]["scan-filter"]["throughput_multiplier"], 2.1)

    def test_missing_required_entry_fails(self):
        del self.fixture.entries[("candidate", "aggregate", 4)]
        self.fixture.rewrite_manifest("candidate")
        with self.assertRaisesRegex(GateError, "missing local/metadata-warm/aggregate/t4"):
            self.evaluate()

    def test_missing_report_field_fails(self):
        path = self.fixture.reports[("candidate", "scan-filter", 1)]
        report = json.loads(path.read_text(encoding="utf-8"))
        del report["build_id"]
        path.write_text(json.dumps(report), encoding="utf-8")
        with self.assertRaisesRegex(GateError, "build_id"):
            self.evaluate()

    def test_checksum_mismatch_fails(self):
        entry = self.fixture.entries[("candidate", "scan-filter", 4)]
        checksum = self.fixture.manifests["candidate"].parent / entry["checksum_report"]
        checksum.write_text(f"{'c' * 64}\n", encoding="utf-8")
        with self.assertRaisesRegex(GateError, "checksums differ"):
            self.evaluate()

    def test_shared_checksum_evidence_fails(self):
        first = self.fixture.entries[("candidate", "scan-filter", 1)]
        second = self.fixture.entries[("candidate", "scan-filter", 4)]
        second["checksum_report"] = first["checksum_report"]
        self.fixture.rewrite_manifest("candidate")
        with self.assertRaisesRegex(GateError, "reuses checksum evidence"):
            self.evaluate()

    def test_dirty_candidate_build_id_fails(self):
        path = self.fixture.manifests["candidate"]
        manifest = json.loads(path.read_text(encoding="utf-8"))
        manifest["rustdb_build_id"] = f"{CANDIDATE}-dirty"
        path.write_text(json.dumps(manifest), encoding="utf-8")
        with self.assertRaisesRegex(GateError, "exact clean 40-character Git commit"):
            evaluate(path, self.fixture.manifests["baseline"], "alpha2-id")

    def test_candidate_must_match_expected_commit(self):
        with self.assertRaisesRegex(GateError, "current clean HEAD"):
            evaluate(
                self.fixture.manifests["candidate"],
                self.fixture.manifests["baseline"],
                "alpha2-id",
                "d" * 40,
            )

    def test_candidate_git_state_must_be_clean(self):
        root = Path(self.temporary.name) / "candidate-git"
        root.mkdir()
        subprocess.run(["git", "init", "--quiet"], cwd=root, check=True)
        subprocess.run(
            ["git", "config", "user.email", "gate@example.invalid"], cwd=root, check=True
        )
        subprocess.run(["git", "config", "user.name", "Gate Test"], cwd=root, check=True)
        tracked = root / "tracked.txt"
        tracked.write_text("clean\n", encoding="utf-8")
        subprocess.run(["git", "add", "tracked.txt"], cwd=root, check=True)
        subprocess.run(["git", "commit", "--quiet", "-m", "fixture"], cwd=root, check=True)

        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        self.assertEqual(clean_candidate_build_id(root), commit)

        tracked.write_text("dirty\n", encoding="utf-8")
        with self.assertRaisesRegex(GateError, "worktree must be clean"):
            clean_candidate_build_id(root)

    def test_dataset_mismatch_fails(self):
        path = self.fixture.manifests["candidate"]
        manifest = json.loads(path.read_text(encoding="utf-8"))
        manifest["dataset"]["manifest_sha256"] = "d" * 64
        path.write_text(json.dumps(manifest), encoding="utf-8")
        with self.assertRaisesRegex(GateError, "dataset fingerprints differ"):
            self.evaluate()

    def test_threshold_failure_is_not_a_pass(self):
        path = self.fixture.reports[("candidate", "scan-filter", 4)]
        report = json.loads(path.read_text(encoding="utf-8"))
        report["p50_ms"] = 60
        report["runs"] = [{"elapsed_ms": 60} for _ in range(5)]
        path.write_text(json.dumps(report), encoding="utf-8")
        with self.assertRaisesRegex(GateError, "only 1.750x"):
            self.evaluate()


if __name__ == "__main__":
    unittest.main()
