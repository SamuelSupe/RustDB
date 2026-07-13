import contextlib
import io
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from v05_resource_gate_fixture import (
    BASELINE_BUILD,
    CANDIDATE_BINARY,
    CANDIDATE_BUILD,
    CPU_MODEL,
    ROOT,
    ResourceGateFixture,
)
import check_v05_resource_gate as cli
from v05_resource_gate import GateError, evaluate


class V05ResourceGateReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.fixture = ResourceGateFixture(Path(self.temporary.name))

    def tearDown(self):
        self.temporary.cleanup()

    def test_low_memory_manifest_identity_correctness_and_assertions_are_strict(self):
        original = self.fixture.load_low_manifest()
        mutations = (
            (lambda value: value.update(suite="other"), "suite"),
            (
                lambda value: value["correctness"].update(verified=False),
                "correctness.verified",
            ),
            (
                lambda value: value["assertions"].update(spill_required=False),
                "assertions.spill_required",
            ),
            (lambda value: value["config"].update(threads=1), "config.threads"),
            (
                lambda value: value["runs"][0].update(require_spill=False),
                "require_spill",
            ),
            (lambda value: value["runs"].pop(), "exactly 24"),
            (
                lambda value: value["runs"].__setitem__(
                    -1, json.loads(json.dumps(value["runs"][0]))
                ),
                "duplicate low-memory case",
            ),
        )
        for mutate, message in mutations:
            with self.subTest(message=message):
                changed = json.loads(json.dumps(original))
                mutate(changed)
                self.fixture.replace_low_manifest(changed)
                with self.assertRaisesRegex(GateError, message):
                    self.fixture.evaluate()
        self.fixture.replace_low_manifest(original)

    def test_low_memory_entries_require_unique_existing_reports_and_checksums(self):
        original = self.fixture.load_low_manifest()
        cases = (
            ("report", "/missing/report.json", "cannot resolve.*report"),
            ("checksum_report", "/missing/checksum.txt", "cannot resolve.*checksum"),
            (
                "checksum_report",
                original["runs"][1]["checksum_report"],
                "duplicate checksum artifact",
            ),
            ("report", original["runs"][1]["report"], "duplicate report artifact"),
        )
        for field, value, message in cases:
            with self.subTest(field=field, message=message):
                changed = json.loads(json.dumps(original))
                changed["runs"][0][field] = value
                self.fixture.replace_low_manifest(changed)
                with self.assertRaisesRegex(GateError, message):
                    self.fixture.evaluate()
        self.fixture.replace_low_manifest(original)

        checksum = Path(original["runs"][0]["checksum_report"])
        contents = checksum.read_text(encoding="utf-8")
        checksum.write_text("invalid\n", encoding="utf-8")
        with self.assertRaisesRegex(GateError, "exactly one lowercase SHA-256"):
            self.fixture.evaluate()
        checksum.write_text(contents, encoding="utf-8")

    def test_low_memory_candidate_build_config_cleanup_and_spill_are_bound(self):
        manifest = self.fixture.load_low_manifest()
        original_manifest = json.loads(json.dumps(manifest))
        for field, value, message in (
            ("rustdb_build_id", "e" * 40, "rustdb_build_id"),
            ("benchmark_binary_sha256", "f" * 64, "benchmark_binary_sha256"),
        ):
            with self.subTest(field=field):
                changed = json.loads(json.dumps(original_manifest))
                changed[field] = value
                self.fixture.replace_low_manifest(changed)
                with self.assertRaisesRegex(GateError, message):
                    self.fixture.evaluate()
        self.fixture.replace_low_manifest(original_manifest)

        entry = original_manifest["runs"][0]
        report_path = Path(entry["report"])
        original_report = json.loads(report_path.read_text(encoding="utf-8"))
        mutations = (
            (lambda value: value.update(build_id="e" * 40), "required build"),
            (
                lambda value: value.update(binary_sha256="f" * 64),
                "rebuilt candidate",
            ),
            (
                lambda value: value["config"].update(compute_threads=1),
                "compute_threads",
            ),
            (
                lambda value: value["config"].update(runtime_filter_bytes=1),
                "complete execution config",
            ),
            (
                lambda value: value["runs"][0].update(spill_cleaned=False),
                "spill_cleaned",
            ),
            (
                lambda value: value["runs"][0].update(spill_write_bytes=0),
                "spill_write_bytes.*positive",
            ),
            (
                lambda value: value["runs"][0].update(spill_read_bytes=0),
                "spill_read_bytes.*positive",
            ),
            (
                lambda value: value["runs"][0].update(
                    peak_memory_bytes=entry["memory_limit_bytes"] + 1
                ),
                "peak_memory_bytes",
            ),
        )
        for mutate, message in mutations:
            with self.subTest(message=message):
                changed = json.loads(json.dumps(original_report))
                mutate(changed)
                report_path.write_text(json.dumps(changed), encoding="utf-8")
                with self.assertRaisesRegex(GateError, message):
                    self.fixture.evaluate()
        report_path.write_text(json.dumps(original_report), encoding="utf-8")

    def test_low_memory_dataset_generation_must_match_candidate_exactly(self):
        manifest = self.fixture.load_low_manifest()
        manifest["dataset"]["generation"]["row_group_size"] = 1
        self.fixture.replace_low_manifest(manifest)
        with self.assertRaisesRegex(GateError, "different dataset generation"):
            self.fixture.evaluate()

    def test_join_arguments_must_be_the_exact_manifest_reports(self):
        copied = self.fixture.root / "copied-inner-join.json"
        copied.write_bytes(self.fixture.join_paths[0].read_bytes())
        with self.assertRaisesRegex(GateError, "exactly the six 128 MiB reports"):
            evaluate(
                self.fixture.paths["q17"],
                self.fixture.paths["q21_candidate"],
                self.fixture.paths["q21_baseline"],
                [copied, *self.fixture.join_paths[1:]],
                self.fixture.q17_checksum,
                self.fixture.q21_checksum,
                self.fixture.low_memory_manifest,
            )

    def test_only_v04_dataset_can_use_explicit_legacy_escape(self):
        baseline = self.fixture.load("q21_baseline")
        baseline.pop("dataset")
        self.fixture.replace("q21_baseline", baseline)
        with self.assertRaisesRegex(GateError, "legacy v0.4"):
            self.fixture.evaluate()
        result = self.fixture.evaluate(allow_legacy_v04_missing_dataset=True)
        self.assertTrue(result["evidence"]["legacy_v04_missing_dataset"])

        candidate = self.fixture.load("q21_candidate")
        candidate.pop("dataset")
        self.fixture.replace("q21_candidate", candidate)
        with self.assertRaisesRegex(GateError, "dataset"):
            self.fixture.evaluate(allow_legacy_v04_missing_dataset=True)

    def test_all_six_join_identities_are_required_without_duplicates(self):
        with self.assertRaisesRegex(GateError, "exactly one"):
            evaluate(
                self.fixture.paths["q17"],
                self.fixture.paths["q21_candidate"],
                self.fixture.paths["q21_baseline"],
                self.fixture.join_paths[:-1],
                self.fixture.q17_checksum,
                self.fixture.q21_checksum,
                self.fixture.low_memory_manifest,
            )
        duplicated = self.fixture.join_paths[:-1] + [self.fixture.join_paths[0]]
        with self.assertRaisesRegex(GateError, "duplicate --join"):
            evaluate(
                self.fixture.paths["q17"],
                self.fixture.paths["q21_candidate"],
                self.fixture.paths["q21_baseline"],
                duplicated,
                self.fixture.q17_checksum,
                self.fixture.q21_checksum,
                self.fixture.low_memory_manifest,
            )

    def test_join_thresholds_and_missing_metrics_are_enforced(self):
        original = self.fixture.load("inner-join")
        for field, value, message in (
            ("spill_write_bytes", 301, "spill write"),
            ("spill_write_bytes", 0, "spill_write_bytes.*positive"),
            ("spill_read_bytes", 0, "spill_read_bytes.*positive"),
            ("peak_active_spill_files", 513, "peak_active_spill_files"),
            ("scanned_bytes", 0, "positive"),
        ):
            with self.subTest(field=field):
                changed = json.loads(json.dumps(original))
                changed["runs"][0][field] = value
                self.fixture.replace("inner-join", changed)
                with self.assertRaisesRegex(GateError, message):
                    self.fixture.evaluate()

        changed = json.loads(json.dumps(original))
        changed["runs"][0]["spill_read_bytes"] = 1_000
        self.fixture.replace("inner-join", changed)
        result = self.fixture.evaluate()
        inner = next(item for item in result["joins"] if item["query"] == "inner-join")
        self.assertEqual(inner["runs"][0]["read_to_scan"], 10.0)

    def test_cli_uses_live_strict_context_and_emits_json(self):
        stdout = io.StringIO()
        stderr = io.StringIO()
        with (
            mock.patch.object(cli, "clean_candidate_build_id", return_value=CANDIDATE_BUILD),
            mock.patch.object(cli, "tagged_build_id", return_value=BASELINE_BUILD),
            mock.patch.object(cli, "actual_cpu_model", return_value=CPU_MODEL),
            mock.patch.object(
                cli, "rebuild_candidate_binary_sha256", return_value=CANDIDATE_BINARY
            ),
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(stderr),
        ):
            result = cli.main(self.fixture.cli_args() + ["--json"])
        self.assertEqual(result, 0, stderr.getvalue())
        self.assertEqual(json.loads(stdout.getvalue())["status"], "pass")

    def test_cli_default_rejects_legacy_v04_missing_dataset(self):
        baseline = self.fixture.load("q21_baseline")
        baseline.pop("dataset")
        self.fixture.replace("q21_baseline", baseline)

        def run(extra):
            stdout = io.StringIO()
            stderr = io.StringIO()
            with (
                mock.patch.object(
                    cli, "clean_candidate_build_id", return_value=CANDIDATE_BUILD
                ),
                mock.patch.object(cli, "tagged_build_id", return_value=BASELINE_BUILD),
                mock.patch.object(cli, "actual_cpu_model", return_value=CPU_MODEL),
                mock.patch.object(
                    cli,
                    "rebuild_candidate_binary_sha256",
                    return_value=CANDIDATE_BINARY,
                ),
                contextlib.redirect_stdout(stdout),
                contextlib.redirect_stderr(stderr),
            ):
                code = cli.main(self.fixture.cli_args() + extra)
            return code, stdout.getvalue(), stderr.getvalue()

        code, _, error = run([])
        self.assertEqual(code, 1)
        self.assertIn("v0.5 resource gate: FAIL", error)
        code, output, error = run(["--allow-legacy-v04-missing-dataset", "--json"])
        self.assertEqual(code, 0, error)
        self.assertTrue(json.loads(output)["evidence"]["legacy_v04_missing_dataset"])
        self.assertIn("warning:", error)

    def test_release_runner_pins_inputs_and_cleans_its_detached_worktree(self):
        runner = ROOT / "benchmarks/run_v05_resource_gate.sh"
        source = runner.read_text(encoding="utf-8")
        help_result = subprocess.run(
            ["sh", str(runner), "--help"],
            capture_output=True,
            check=False,
            text=True,
        )
        self.assertEqual(help_result.returncode, 2)
        self.assertIn("--dataset-root ROOT", help_result.stderr)
        self.assertIn("--low-memory-run DIR", help_result.stderr)
        self.assertIn("metadata_cache_bytes=0", source)
        self.assertIn("memory_limit=134217728", source)
        self.assertIn("threads=4", source)
        self.assertIn("batch_size=8192", source)
        self.assertIn("io_concurrency=32", source)
        self.assertIn(
            "inner-join left-join right-join full-join semi-join anti-join",
            source,
        )
        self.assertIn("git worktree add --quiet --detach", source)
        self.assertIn("git -C \"$WORKSPACE\" worktree remove --force", source)
        self.assertIn("check_v05_resource_gate.py", source)
        self.assertIn("tools/tpch/compare_query.sh", source)
        self.assertIn("--q17-checksum", source)
        self.assertIn("--q21-checksum", source)
        self.assertIn("--low-memory-manifest", source)
        self.assertIn("--allow-legacy-v04-missing-dataset", source)
        self.assertIn("requires an Apple M5 Max", source)
        self.assertIn("requires SF10", source)
        self.assertLess(
            source.index("candidate worktree must be clean"),
            source.index("mkdir -p \"$output_host/rendered\""),
        )
        self.assertNotIn("git push", source)
        self.assertNotIn("git tag", source)

if __name__ == "__main__":
    unittest.main()
