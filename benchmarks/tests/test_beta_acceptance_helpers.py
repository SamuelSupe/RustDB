from __future__ import annotations

import argparse
import io
import json
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts" / "ci"))

import beta_acceptance_evidence as evidence  # noqa: E402
import beta_acceptance_fixtures as fixtures  # noqa: E402
import beta_acceptance_inputs as inputs  # noqa: E402
import beta_acceptance_minio as minio  # noqa: E402
import beta_acceptance_reports as reports  # noqa: E402


class BetaAcceptanceHelperTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def test_acceptance_trap_preserves_the_failing_exit_code(self):
        script = (ROOT / "scripts" / "ci" / "beta_acceptance.sh").read_text(
            encoding="utf-8"
        )
        start = script.index("finish() {")
        end = script.index("\n}\ntrap finish EXIT", start) + 2
        finish = script[start:end]
        harness = f"""
set -u
ROOT=/workspace
EVIDENCE_HELPER=/workspace/scripts/ci/beta_acceptance_evidence.py
OUTPUT=/tmp/evidence
STARTED_AT=2026-07-19T00:00:00Z
CURRENT_STEP=clickbench
TEMP_ROOT=
python3() {{ printf 'ARG=%s\\n' "$@"; return 0; }}
{finish}
failure() {{ return 37; }}
failure
finish
"""

        result = subprocess.run(
            ["bash", "-c", harness],
            check=False,
            capture_output=True,
            text=True,
        )

        self.assertEqual(result.returncode, 37)
        self.assertIn("ARG=failed\n", result.stdout)
        self.assertIn("ARG=37\n", result.stdout)
        self.assertIn("ARG=clickbench\n", result.stdout)

    def test_beta2_release_boundary_and_lifecycle_stage_are_pinned(self):
        contract = inputs.release_contract(ROOT)
        self.assertEqual(contract["version"], "1.0.0-beta.3")
        self.assertEqual(contract["native_epoch"], 4)
        self.assertEqual(contract["config_schema"], 2)
        self.assertEqual(contract["http_api"], "v2")
        self.assertIn("beta2-lifecycle", reports.STEPS)
        lifecycle = evidence.execution_contract()["lifecycle_gate"]
        self.assertEqual(lifecycle["timeout_seconds"], 3600)
        self.assertEqual(lifecycle["passes"], 1)
        self.assertIn("restart_interrupted_arrow_prefix", lifecycle["covers"])

    def test_release_steps_require_one_ordered_local_and_minio_tpch_pass(self):
        records = [
            {"name": name, "phase": phase, "exit_code": 0}
            for name in reports.STEPS
            for phase in ("started", "finished")
        ]
        reports.validate_steps(records)
        self.assertEqual(reports.STEPS.count("tpch-sf1-local"), 1)
        self.assertEqual(reports.STEPS.count("tpch-sf1-minio"), 1)
        records[4:8] = records[6:8] + records[4:6]
        with self.assertRaisesRegex(ValueError, "required order"):
            reports.validate_steps(records)

    def runner_command(self, *, minio: bool, fixture: Path | None) -> list[str]:
        workspace = self.root / "workspace"
        temporary = self.root / "runner-tmp"
        workspace.mkdir(exist_ok=True)
        temporary.mkdir(exist_ok=True)
        output = io.StringIO()
        with redirect_stdout(output):
            result = inputs.runner_command(
                argparse.Namespace(
                    workspace=workspace,
                    temporary=temporary,
                    build_id="a" * 64,
                    memory_limit="2147483648",
                    fixture=fixture,
                    minio=minio,
                )
            )
        self.assertEqual(result, 0)
        return json.loads(output.getvalue())

    def test_runner_commands_pin_local_and_minio_arguments(self):
        fixture = self.root / "fixture"
        fixture.mkdir()
        workspace = str((self.root / "workspace").resolve())
        temporary = str((self.root / "runner-tmp").resolve())
        common = [
            "docker",
            "compose",
            "--project-directory",
            workspace,
            "run",
            "--rm",
            "--no-deps",
            "--no-TTY",
        ]
        runner = [
            "--volume",
            f"{temporary}:/bench-tmp",
            "dev",
            "/workspace/target/release/rustdb-v07-runner",
            "--threads",
            "4",
            "--memory-limit",
            "2147483648",
            "--concurrency",
            "8",
            "--batch-size",
            "8192",
            "--metadata-cache-bytes",
            "0",
            "--spill-directory",
            "/bench-tmp/spill",
            "--build-id",
            "a" * 64,
        ]

        self.assertEqual(
            self.runner_command(minio=False, fixture=fixture),
            common
            + ["--volume", f"{fixture.resolve()}:/beta-data:ro"]
            + runner,
        )
        self.assertEqual(
            self.runner_command(minio=True, fixture=None),
            common
            + runner
            + [
                "--s3-endpoint",
                "http://minio:9000",
                "--s3-region",
                "us-east-1",
                "--s3-path-style",
                "--s3-allow-http",
            ],
        )

    def test_small_local_and_minio_fixture_manifests_are_normalized(self):
        output = self.root / "output"
        local = self.root / "local"
        output.mkdir()
        local.mkdir()
        (local / "b.parquet").write_bytes(b"bbb")
        (local / "a.parquet").write_bytes(b"aa")
        source_manifest = self.root / "objects.json"
        source_manifest.write_text(
            json.dumps(
                {
                    "schema": fixtures.OBJECT_SCHEMA,
                    "root_uri": "s3://bucket/data/",
                    "objects": [
                        {"uri": "s3://bucket/data/b.csv", "size": 3, "etag": "b"},
                        {"uri": "s3://bucket/data/a.csv", "size": 2, "etag": "a"},
                    ],
                }
            ),
            encoding="utf-8",
        )
        with patch.multiple(fixtures, MIN_FILES=2, MIN_BYTES=5):
            local_result = fixtures.local_fixture(local, output, "parquet")
            object_result = fixtures.object_fixture(source_manifest, output, "csv")

        self.assertEqual(local_result["files"], 2)
        self.assertEqual(local_result["bytes"], 5)
        local_manifest = json.loads(
            Path(local_result["manifest"]).read_text(encoding="utf-8")
        )
        self.assertEqual(
            [entry["path"] for entry in local_manifest["files"]],
            ["a.parquet", "b.parquet"],
        )
        self.assertEqual(object_result["root_uri"], "s3://bucket/data")
        object_manifest = json.loads(
            Path(object_result["manifest"]).read_text(encoding="utf-8")
        )
        self.assertEqual(
            [entry["uri"] for entry in object_manifest["objects"]],
            ["s3://bucket/data/a.csv", "s3://bucket/data/b.csv"],
        )

    def test_minio_manifest_rejects_credentials(self):
        manifest = self.root / "objects.json"
        manifest.write_text(
            json.dumps(
                {
                    "schema": fixtures.OBJECT_SCHEMA,
                    "root_uri": "s3://user:secret@bucket/data",
                    "objects": [],
                }
            ),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValueError, "credential-free"):
            fixtures.object_fixture(manifest, self.root, "parquet")

    def test_live_minio_inventory_must_match_uri_size_and_etag(self):
        manifest = self.root / "manifest.json"
        listing = self.root / "listing.jsonl"
        result = self.root / "verification.json"
        manifest.write_text(
            json.dumps(
                {
                    "schema": fixtures.OBJECT_SCHEMA,
                    "root_uri": "s3://bucket/prefix",
                    "objects": [
                        {
                            "uri": "s3://bucket/prefix/a.parquet",
                            "size": 3,
                            "etag": "abc",
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        listing.write_text(
            json.dumps(
                {
                    "status": "success",
                    "type": "file",
                    "key": "a.parquet",
                    "size": 3,
                    "etag": "abc",
                }
            )
            + "\n",
            encoding="utf-8",
        )
        self.assertEqual(minio.verify(manifest, listing, result)["objects"], 1)

        listing.write_text(
            listing.read_text(encoding="utf-8").replace('"abc"', '"changed"'),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValueError, "identity differs"):
            minio.verify(manifest, listing, result)

    def test_report_rejects_retained_query_resources(self):
        memory = 2 * 1024**3
        checksum = "b" * 64
        fixture = {
            "format": "parquet",
            "files": 2,
            "query": {"sha256": "c" * 64},
            "manifest": "/tmp/local-fixture-manifest.json",
            "manifest_sha256": "d" * 64,
        }
        report = {
            "schema": reports.REPORT_SCHEMA,
            "diagnostic_only": True,
            "storage_medium": "local-nvme",
            "storage_track": "parquet",
            "query": fixture["query"],
            "dataset": {
                "path": fixture["manifest"],
                "sha256": fixture["manifest_sha256"],
                "expected_discovered_files": 2,
            },
            "config": {
                "threads": 4,
                "memory_limit_bytes": memory,
                "concurrency": 8,
                "batch_size": 8192,
            },
            "warmup": 0,
            "iterations": 1,
            "summary": {
                "measured_queries": 8,
                "terminal_reservation_bytes": 0,
                "engine_root_current_reservation_bytes": 0,
                "engine_root_lifetime_peak_reservation_bytes": memory,
                "engine_root_memory_limit_bytes": memory,
                "discovered_files": 2,
                "checksum": checksum,
                "peak_rss_bytes": memory - 1,
            },
            "hello": {"build_id": "e" * 64},
            "runs": [
                {
                    "engine_root_current_reservation_bytes": 0,
                    "engine_root_lifetime_peak_reservation_bytes": memory,
                    "engine_root_memory_limit_bytes": memory,
                    "queries": [
                        {
                            "complete": True,
                            "discovered_files": 2,
                            "current_reservation_bytes": 0,
                            "peak_reservation_bytes": memory,
                            "checksum": checksum,
                        }
                        for _ in range(8)
                    ]
                }
            ],
        }
        path = self.root / "report.json"
        path.write_text(json.dumps(report), encoding="utf-8")
        accepted = reports.report_summary(path, "local-nvme", memory, fixture)
        self.assertEqual(accepted["measured_queries"], 8)
        self.assertEqual(accepted["engine_root_current_reservation_bytes"], 0)
        self.assertEqual(accepted["engine_root_memory_limit_bytes"], memory)
        self.assertEqual(accepted["discovered_files"], 2)

        report["runs"][0]["queries"][3]["current_reservation_bytes"] = 1
        path.write_text(json.dumps(report), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "retained resources"):
            reports.report_summary(path, "local-nvme", memory, fixture)

        report["runs"][0]["queries"][3]["current_reservation_bytes"] = 0
        report["runs"][0]["queries"][3]["discovered_files"] = 1
        path.write_text(json.dumps(report), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "retained resources"):
            reports.report_summary(path, "local-nvme", memory, fixture)

        report["runs"][0]["queries"][3]["discovered_files"] = 2
        report["runs"][0]["engine_root_current_reservation_bytes"] = 1
        path.write_text(json.dumps(report), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Engine root memory"):
            reports.report_summary(path, "local-nvme", memory, fixture)

    def test_evidence_cannot_mark_an_incomplete_run_as_passed(self):
        output = self.root / "evidence"
        output.mkdir()
        (output / "inputs.json").write_text(
            json.dumps(
                {
                    "schema": inputs.SCHEMA,
                    "preflight_complete": True,
                    "release": {
                        "version": "1.0.0-beta.3",
                        "native_epoch": 4,
                        "config_schema": 2,
                        "http_api": "v2",
                        "source_sha256": {
                            "cargo": "a" * 64,
                            "native_format": "b" * 64,
                            "service_config": "c" * 64,
                            "http_server": "d" * 64,
                        },
                    },
                }
            ),
            encoding="utf-8",
        )
        args = argparse.Namespace(
            workspace=self.root,
            output=output,
            status="passed",
            exit_code=0,
            failed_step="",
            started_at="2026-07-19T00:00:00Z",
        )
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            self.assertEqual(evidence.finalize(args), 1)
        result = json.loads((output / "evidence.json").read_text(encoding="utf-8"))
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["failed_step"], "finalization")
        self.assertIn("not every required", result["finalization_error"])


if __name__ == "__main__":
    unittest.main()
