import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from csv_scaling_gate import (  # noqa: E402
    GateError,
    RELEASE_SOURCE_BYTES,
    ReleaseContext,
    evaluate,
    rebuild_candidate_binary_sha256,
    source_facts,
)


COMMIT = "a" * 40
DIGEST = "b" * 64
CPU = "Apple M5 Max"


class CsvScalingGateTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.context = ReleaseContext(COMMIT, DIGEST, CPU)
        self.one = self.write_report("one.json", 1, 100.0, 1)
        self.four = self.write_report("four.json", 4, 40.0, 2)

    def tearDown(self):
        self.temporary.cleanup()

    def write_report(self, name, threads, elapsed, parser_lanes, *, release=True):
        path = self.root / name
        iterations = 1 if release else 2
        run = {
            "elapsed_ms": elapsed,
            "rows": 10,
            "batches": 2,
            "scanned_rows": 10,
            "csv_source_bytes": 700,
            "csv_decompressed_bytes": 680,
            "peak_csv_parser_lanes": parser_lanes,
        }
        report = {
            "iterations": iterations,
            "p50_ms": elapsed,
            "config": {
                "compute_threads": threads,
                "csv_parallel_single_file": True,
                "memory_limit_bytes": 1024**3,
                "batch_size": 8192,
                "io_concurrency": 32,
                "metadata_cache_bytes": 0,
                "csv_target_morsel_bytes": 8 * 1024**2,
            },
            "runs": [dict(run) for _ in range(iterations)],
        }
        if release:
            report.update(
                {
                    "engine_version": "0.5.0-alpha.1",
                    "build_id": COMMIT,
                    "binary_sha256": DIGEST,
                    "build": {
                        "cargo_profile": "release",
                        "rustflags": "-C target-cpu=native",
                        "rustc_version": "rustc 1.90.0 (fake)",
                    },
                    "environment": {"cpu_model": CPU},
                    "warmup": 0,
                }
            )
        path.write_text(json.dumps(report), encoding="utf-8")
        return path

    def evaluate_release(self):
        return evaluate(
            self.one,
            self.four,
            mode="release",
            minimum_speedup=0.01,
            source_bytes=651,
            expected_rows=10,
            release_context=self.context,
        )

    def mutate_report(self, path, field, value):
        report = json.loads(path.read_text(encoding="utf-8"))
        target = report
        parts = field.split(".")
        for part in parts[:-1]:
            target = target[part]
        target[parts[-1]] = value
        path.write_text(json.dumps(report), encoding="utf-8")

    def mutate_run(self, path, field, value, run=0):
        report = json.loads(path.read_text(encoding="utf-8"))
        report["runs"][run][field] = value
        path.write_text(json.dumps(report), encoding="utf-8")

    def test_release_reports_pass_and_cannot_lower_speedup_threshold(self):
        result = self.evaluate_release()
        self.assertTrue(result["release_qualified"])
        self.assertEqual(result["mode"], "release")
        self.assertEqual(result["minimum_speedup"], 1.8)
        self.assertEqual(result["speedup"], 2.5)

    def test_explicit_smoke_is_never_release_qualified(self):
        one = self.write_report("smoke-one.json", 1, 100.0, 1, release=False)
        four = self.write_report("smoke-four.json", 4, 80.0, 1, release=False)
        result = evaluate(
            one,
            four,
            mode="smoke",
            minimum_speedup=1.1,
            source_bytes=651,
            expected_rows=10,
        )
        self.assertFalse(result["release_qualified"])
        self.assertEqual(result["mode"], "smoke")

    def test_release_rejects_non_fixed_execution_provenance(self):
        cases = (
            ("engine_version", "0.4.0-alpha.1", "engine_version"),
            ("build_id", "c" * 40, "clean candidate commit"),
            ("binary_sha256", "d" * 64, "candidate executable"),
            ("build.cargo_profile", "debug", "cargo_profile"),
            ("build.rustflags", "", "rustflags"),
            ("build.rustc_version", "unrecorded", "rustc_version"),
            ("environment.cpu_model", "Apple M4 Max", "fixed host"),
            ("warmup", 1, "warmup"),
            ("iterations", 4, "iterations"),
            ("config.memory_limit_bytes", 512 * 1024**2, "memory_limit_bytes"),
            ("config.batch_size", 4096, "batch_size"),
            ("config.io_concurrency", 16, "io_concurrency"),
            ("config.metadata_cache_bytes", 1, "metadata_cache_bytes"),
            ("config.csv_target_morsel_bytes", 1024, "csv_target_morsel_bytes"),
        )
        for field, value, message in cases:
            with self.subTest(field=field):
                self.one = self.write_report("one.json", 1, 100.0, 1)
                self.mutate_report(self.one, field, value)
                with self.assertRaisesRegex(GateError, message):
                    self.evaluate_release()

    def test_release_rejects_invalid_independent_context(self):
        for context, message in (
            (ReleaseContext("dirty", DIGEST, CPU), "40-character"),
            (ReleaseContext(COMMIT, "bad", CPU), "SHA-256"),
            (ReleaseContext(COMMIT, DIGEST, "Apple M4 Max"), "M5 Max"),
        ):
            with self.subTest(message=message):
                self.context = context
                with self.assertRaisesRegex(GateError, message):
                    self.evaluate_release()

    @patch("csv_scaling_gate.subprocess.run")
    def test_candidate_digest_is_rebuilt_inside_the_compose_target_volume(self, run):
        run.return_value = subprocess.CompletedProcess(
            args=[], returncode=0, stdout=f"{DIGEST}  target/release/rustdb-bench\n"
        )
        self.assertEqual(rebuild_candidate_binary_sha256(self.root), DIGEST)
        command = run.call_args.args[0]
        self.assertIn("RUSTFLAGS=-C target-cpu=native", command)
        self.assertIn("sha256sum target/release/rustdb-bench", command[-1])

        run.return_value = subprocess.CompletedProcess(args=[], returncode=0, stdout="bad\n")
        with self.assertRaisesRegex(GateError, "exactly one"):
            rebuild_candidate_binary_sha256(self.root)

    def test_rejects_incomplete_or_inconsistent_results(self):
        cases = (
            (self.four, "csv_source_bytes", 650, "complete 651-byte source"),
            (self.one, "csv_decompressed_bytes", 650, "complete source"),
            (self.four, "rows", 9, "rows"),
            (self.four, "scanned_rows", 9, "scanned_rows"),
        )
        for path, field, value, message in cases:
            with self.subTest(field=field):
                self.one = self.write_report("one.json", 1, 100.0, 1)
                self.four = self.write_report("four.json", 4, 40.0, 2)
                self.mutate_run(path if path.name == "four.json" else self.one, field, value)
                with self.assertRaisesRegex(GateError, message):
                    self.evaluate_release()

    def test_records_cross_thread_batch_boundaries_but_rejects_byte_drift(self):
        self.mutate_run(self.four, "batches", 3)
        result = self.evaluate_release()
        self.assertEqual(result["threads_1_batches"], 2)
        self.assertEqual(result["threads_4_batches"], 3)

        self.four = self.write_report("four.json", 4, 40.0, 2)
        self.mutate_run(self.four, "csv_source_bytes", 701)
        with self.assertRaisesRegex(GateError, "one-thread and four-thread row/byte"):
            self.evaluate_release()

    def test_rejects_metric_drift_between_iterations(self):
        one = self.write_report("smoke-one.json", 1, 100.0, 1, release=False)
        four = self.write_report("smoke-four.json", 4, 80.0, 1, release=False)
        self.mutate_run(one, "batches", 3, 1)
        with self.assertRaisesRegex(GateError, "differ between measured runs"):
            evaluate(
                one,
                four,
                mode="smoke",
                minimum_speedup=1.1,
                source_bytes=651,
                expected_rows=10,
            )

    def test_rejects_serial_or_slow_four_thread_run(self):
        self.mutate_run(self.four, "peak_csv_parser_lanes", 1)
        with self.assertRaisesRegex(GateError, "never observed two"):
            self.evaluate_release()
        self.four = self.write_report("four.json", 4, 60.0, 2)
        with self.assertRaisesRegex(GateError, "below required"):
            self.evaluate_release()

    def test_source_facts_distinguishes_fixed_release_from_smoke(self):
        release_source = self.root / "release.csv"
        size = RELEASE_SOURCE_BYTES + 11
        with release_source.open("wb") as source:
            source.write(b"id,payload\n")
            source.seek(size - 64)
            source.write(b"1," + b"x" * 61 + b"\n")
        measured, rows = source_facts(release_source, True)
        self.assertEqual(measured, size)
        self.assertEqual(rows, (size - 11) // 64)

        oversized = self.root / "oversized.csv"
        with oversized.open("wb") as source:
            source.write(b"id,payload\n")
            source.seek(size + 64 - 64)
            source.write(b"1," + b"x" * 61 + b"\n")
        with self.assertRaisesRegex(GateError, "fixed 10 GiB"):
            source_facts(oversized, True)
        self.assertEqual(source_facts(oversized, False)[0], size + 64)


if __name__ == "__main__":
    unittest.main()
