import argparse
import hashlib
import io
import json
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

    def test_clickbench_fixture_binds_the_pinned_oracle(self):
        clickbench = self.root / "clickbench"
        clickbench.mkdir()
        query = clickbench / "queries.sql"
        data = clickbench / "fixture.parquet"
        query.write_bytes(b"queries")
        data.write_bytes(b"dataset")
        query_sha = hashlib.sha256(query.read_bytes()).hexdigest()
        data_sha = hashlib.sha256(data.read_bytes()).hexdigest()
        oracle = self.root / "oracle.json"
        oracle.write_text(
            json.dumps(
                {
                    "schema": "rustdb-clickbench-functional-oracle-v1",
                    "profile": "functional",
                    "mode": "execute",
                    "query_count": 43,
                    "query_sha256": query_sha,
                    "dataset_sha256": data_sha,
                    "checksum_algorithm": "rustdb-typed-multiset-sha256-v1",
                    "results": [
                        {"query": number, "rows": 1, "checksum": "a" * 64}
                        for number in range(1, 44)
                    ],
                }
            ),
            encoding="utf-8",
        )
        oracle_sha = hashlib.sha256(oracle.read_bytes()).hexdigest()
        with patch.multiple(
            fixtures,
            CLICKBENCH={"functional": ("fixture.parquet", 7, '"etag"', data_sha)},
            CLICKBENCH_QUERY_SHA256=query_sha,
            PINNED_SHA256=oracle_sha,
        ):
            value = fixtures.clickbench_fixture(clickbench, "functional", oracle)

        self.assertEqual(value["oracle"]["sha256"], oracle_sha)
        self.assertTrue(value["oracle"]["identity_verified"])
        self.assertEqual(len(value["oracle"]["results"]), 43)

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
            json.dumps({"preflight_complete": True}), encoding="utf-8"
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

    def test_clickbench_report_is_bound_to_observed_hashes(self):
        digest = "a" * 64
        commit = "b" * 40
        algorithm = "rustdb-typed-multiset-sha256-v1"
        oracle_results = [
            {"query": number, "rows": 1, "checksum": digest}
            for number in range(1, 44)
        ]
        results = [
            {
                "query": number,
                "status": "passed",
                "terminal_query_reservation_bytes": 0,
                "terminal_engine_reservation_bytes": 0,
                "spill_cleaned": True,
                "checksum_algorithm": algorithm,
                "result_rows": 1,
                "result_checksum_sha256": digest,
                "oracle_expected_rows": 1,
                "oracle_expected_checksum_sha256": digest,
                "oracle_match": True,
                "oracle_error": None,
            }
            for number in range(1, 44)
        ]
        value = {
            "schema": reports.CLICKBENCH_SCHEMA,
            "complete": True,
            "mode": "execute",
            "binary_as_string": True,
            "query_count": 43,
            "passed": 43,
            "failed": 0,
            "results": results,
            "dataset": {
                "profile": "functional",
                "bytes": 123,
                "sha256": digest,
                "expected_sha256": digest,
                "identity_verified": True,
                "expected_source_etag": '"etag"',
            },
            "queries": {
                "sha256": digest,
                "expected_sha256": digest,
                "identity_verified": True,
            },
            "oracle": {
                "schema": "rustdb-clickbench-functional-oracle-v1",
                "profile": "functional",
                "mode": "execute",
                "query_count": 43,
                "sha256": digest,
                "expected_sha256": digest,
                "identity_verified": True,
                "checksum_algorithm": algorithm,
                "query_sha256": digest,
                "dataset_sha256": digest,
            },
            "resource_contract": {
                "engine_threads": 4,
                "engine_memory_limit_bytes": 4 * 1024**3,
            },
            "build": {"id": commit},
            "acceptance_summary": {
                "all_terminal_query_reservations_zero": True,
                "all_terminal_engine_reservations_zero": True,
                "all_spill_cleaned": True,
            },
        }
        expected = {
            "clickbench": {
                "profile": "functional",
                "data": {
                    "bytes": 123,
                    "sha256": digest,
                    "expected_sha256": digest,
                    "expected_source_etag": '"etag"',
                },
                "query": {"sha256": digest, "expected_sha256": digest},
                "oracle": {
                    "schema": "rustdb-clickbench-functional-oracle-v1",
                    "profile": "functional",
                    "mode": "execute",
                    "query_count": 43,
                    "sha256": digest,
                    "expected_sha256": digest,
                    "identity_verified": True,
                    "checksum_algorithm": algorithm,
                    "results": oracle_results,
                },
            }
        }
        path = self.root / "clickbench.json"
        path.write_text(json.dumps(value), encoding="utf-8")
        self.assertTrue(
            reports.clickbench_summary(path, expected, commit)["identity_verified"]
        )

        value["dataset"]["identity_verified"] = False
        path.write_text(json.dumps(value), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "different fixture"):
            reports.clickbench_summary(path, expected, commit)

        value["dataset"]["identity_verified"] = True
        value["results"][17]["result_checksum_sha256"] = "c" * 64
        path.write_text(json.dumps(value), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "query 18 differs"):
            reports.clickbench_summary(path, expected, commit)


if __name__ == "__main__":
    unittest.main()
