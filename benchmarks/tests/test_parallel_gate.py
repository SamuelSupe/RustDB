import hashlib
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
CANDIDATE_BINARY = "9" * 64


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class GateFixture:
    def __init__(self, root: Path):
        self.root = root
        self.manifests = {}
        self.reports = {}
        self.entries = {}
        baseline_placeholder = "0" * 40
        self._write_variant(
            "baseline",
            baseline_placeholder,
            "0.2.0-alpha.1",
            {("scan-filter", 1): 100, ("scan-filter", 4): 90,
             ("aggregate", 1): 200, ("aggregate", 4): 180},
        )
        self.baseline_build_id = self._commit_baseline_harness()
        self._replace_build_id("baseline", baseline_placeholder, self.baseline_build_id)
        self._write_variant(
            "candidate",
            CANDIDATE,
            "0.4.0-alpha.1",
            {("scan-filter", 1): 105, ("scan-filter", 4): 50,
             ("aggregate", 1): 210, ("aggregate", 4): 100},
        )

    def _write_variant(self, variant, build_id, engine_version, timings):
        directory = self.root / variant
        directory.mkdir(parents=True)
        (directory / "Cargo.toml").write_text("[package]\nname='gate-fixture'\n", encoding="utf-8")
        harness_paths = {
            "runner_sha256": directory / "benchmarks/run_baseline.sh",
            "library_sha256": directory / "benchmarks/suites/lib.sh",
            "checksum_runner_sha256": directory / "tools/tpch/compare_query.sh",
        }
        for field, path in harness_paths.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(f"{variant}:{field}\n", encoding="utf-8")
        dataset_manifest = directory / "data/tpch-sf10/manifest.sha256"
        dataset_manifest.parent.mkdir(parents=True)
        dataset_manifest.write_text("identical sf10 fixture\n", encoding="utf-8")
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
            if variant == "candidate":
                report["binary_sha256"] = CANDIDATE_BINARY
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
                field: file_sha256(path) for field, path in harness_paths.items()
            },
            "dataset": {
                "generation": {"duckdb": "1.4.3", "scale_factor": "10"},
                "manifest": "data/tpch-sf10/manifest.sha256",
                "manifest_sha256": file_sha256(dataset_manifest),
            },
            "correctness": {"verified": True, "targets": ["local"]},
            "runs": entries,
        }
        if variant == "candidate":
            manifest["benchmark_binary_sha256"] = CANDIDATE_BINARY
        manifest_path = directory / "manifest.json"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        self.manifests[variant] = manifest_path

    def _commit_baseline_harness(self):
        directory = self.root / "baseline"
        subprocess.run(["git", "init", "--quiet"], cwd=directory, check=True)
        subprocess.run(
            ["git", "config", "user.email", "gate@example.invalid"],
            cwd=directory,
            check=True,
        )
        subprocess.run(
            ["git", "config", "user.name", "Gate Test"], cwd=directory, check=True
        )
        subprocess.run(
            [
                "git",
                "add",
                "Cargo.toml",
                "benchmarks/run_baseline.sh",
                "benchmarks/suites/lib.sh",
                "tools/tpch/compare_query.sh",
            ],
            cwd=directory,
            check=True,
        )
        subprocess.run(
            ["git", "commit", "--quiet", "-m", "baseline harness fixture"],
            cwd=directory,
            check=True,
        )
        return subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=directory,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    def _replace_build_id(self, variant, previous, replacement):
        for (report_variant, _, _), path in self.reports.items():
            if report_variant != variant:
                continue
            report = json.loads(path.read_text(encoding="utf-8"))
            self._replace_value(report, previous, replacement)
            path.write_text(json.dumps(report), encoding="utf-8")
        manifest_path = self.manifests[variant]
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        self._replace_value(manifest, previous, replacement)
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    @staticmethod
    def _replace_value(value, previous, replacement):
        if isinstance(value, dict):
            for key, item in value.items():
                if item == previous:
                    value[key] = replacement
                else:
                    GateFixture._replace_value(item, previous, replacement)
        elif isinstance(value, list):
            for item in value:
                GateFixture._replace_value(item, previous, replacement)

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
            self.fixture.baseline_build_id,
            CANDIDATE,
        )

    def test_valid_fixture_passes(self):
        result = self.evaluate()
        self.assertEqual(result["status"], "pass")
        self.assertAlmostEqual(result["cases"]["scan-filter"]["throughput_multiplier"], 2.1)

    def test_v03_candidate_report_is_rejected(self):
        path = self.fixture.reports[("candidate", "scan-filter", 1)]
        report = json.loads(path.read_text(encoding="utf-8"))
        report["engine_version"] = "0.3.0-alpha.1"
        path.write_text(json.dumps(report), encoding="utf-8")
        with self.assertRaisesRegex(
            GateError,
            "engine_version: expected '0.4.0-alpha.1'",
        ):
            self.evaluate()

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

    def test_candidate_binary_digest_must_match_every_report(self):
        path = self.fixture.reports[("candidate", "scan-filter", 1)]
        report = json.loads(path.read_text(encoding="utf-8"))
        report["binary_sha256"] = "8" * 64
        path.write_text(json.dumps(report), encoding="utf-8")
        with self.assertRaisesRegex(GateError, "binary_sha256"):
            self.evaluate()

    def test_candidate_binary_must_match_clean_rebuild(self):
        with self.assertRaisesRegex(GateError, "rebuilt from the current clean HEAD"):
            evaluate(
                self.fixture.manifests["candidate"],
                self.fixture.manifests["baseline"],
                self.fixture.baseline_build_id,
                CANDIDATE,
                "8" * 64,
            )

    def test_host_cpu_must_match_reports(self):
        with self.assertRaisesRegex(GateError, "detected host CPU"):
            evaluate(
                self.fixture.manifests["candidate"],
                self.fixture.manifests["baseline"],
                self.fixture.baseline_build_id,
                CANDIDATE,
                CANDIDATE_BINARY,
                "Apple M5 Max (different fixture)",
            )

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
            evaluate(
                path,
                self.fixture.manifests["baseline"],
                self.fixture.baseline_build_id,
            )

    def test_candidate_must_match_expected_commit(self):
        with self.assertRaisesRegex(GateError, "current clean HEAD"):
            evaluate(
                self.fixture.manifests["candidate"],
                self.fixture.manifests["baseline"],
                self.fixture.baseline_build_id,
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
        with self.assertRaisesRegex(GateError, "dataset.manifest_sha256 does not match"):
            self.evaluate()

    def test_harness_tampering_fails(self):
        runner = self.fixture.manifests["candidate"].parent / "benchmarks/run_baseline.sh"
        runner.write_text("tampered\n", encoding="utf-8")
        with self.assertRaisesRegex(GateError, "runner_sha256 does not match"):
            self.evaluate()

    def test_baseline_harness_is_read_from_its_git_tree(self):
        root = self.fixture.manifests["baseline"].parent
        for relative in (
            "benchmarks/run_baseline.sh",
            "benchmarks/suites/lib.sh",
            "tools/tpch/compare_query.sh",
        ):
            (root / relative).write_text("tampered worktree copy\n", encoding="utf-8")

        result = self.evaluate()
        self.assertEqual(result["status"], "pass")

    def test_every_baseline_harness_hash_is_checked_against_git(self):
        path = self.fixture.manifests["baseline"]
        original = json.loads(path.read_text(encoding="utf-8"))
        for field in (
            "runner_sha256",
            "library_sha256",
            "checksum_runner_sha256",
        ):
            with self.subTest(field=field):
                manifest = json.loads(json.dumps(original))
                manifest["harness"][field] = "b" * 64
                path.write_text(json.dumps(manifest), encoding="utf-8")
                with self.assertRaisesRegex(
                    GateError, rf"baseline\.harness\.{field} does not match Git tree"
                ):
                    self.evaluate()
        path.write_text(json.dumps(original), encoding="utf-8")

    def test_baseline_build_id_rejects_non_commit_input(self):
        path = self.fixture.manifests["baseline"]
        manifest = json.loads(path.read_text(encoding="utf-8"))
        manifest["rustdb_build_id"] = "--help"
        path.write_text(json.dumps(manifest), encoding="utf-8")
        with self.assertRaisesRegex(
            GateError, "baseline build id must be an exact clean 40-character Git commit"
        ):
            self.evaluate()

    def test_missing_baseline_git_tree_has_a_clear_error(self):
        path = self.fixture.manifests["baseline"]
        manifest = json.loads(path.read_text(encoding="utf-8"))
        manifest["rustdb_build_id"] = "f" * 40
        path.write_text(json.dumps(manifest), encoding="utf-8")
        with self.assertRaisesRegex(
            GateError, "cannot read baseline harness artifact .* from Git tree"
        ):
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
