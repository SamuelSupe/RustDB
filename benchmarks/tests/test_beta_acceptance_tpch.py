from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts" / "ci"))

import beta_acceptance_evidence as evidence  # noqa: E402
import beta_acceptance_tpch as tpch  # noqa: E402


class BetaAcceptanceTpchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.output = Path(self.temporary.name)
        self.commit = "a" * 40
        self.binary = "b" * 64
        self.checksums = {query: f"{number:064x}" for number, query in enumerate(tpch.QUERIES, 1)}
        self.write_dataset()
        self.write_report("local")
        self.write_report("minio")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def write_dataset(self) -> None:
        destination = self.output / "tpch" / "dataset"
        destination.mkdir(parents=True, exist_ok=True)
        destination.joinpath("manifest.sha256").write_text(
            "".join(
                f"{'c' * 64}  {table}/part-00000.parquet\n" for table in tpch.TABLES
            ),
            encoding="utf-8",
        )
        destination.joinpath("manifest.json").write_text(
            json.dumps(
                {
                    "duckdb": "1.4.3",
                    "scale_factor": "1",
                    "compression": "zstd:3",
                    "row_group_size": 122880,
                }
            )
            + "\n",
            encoding="utf-8",
        )

    def write_report(self, medium: str) -> None:
        destination = self.output / "tpch" / medium
        destination.mkdir(parents=True, exist_ok=True)
        destination.joinpath("status.tsv").write_text(
            "query\tstatus\tchecksum\tdiagnostic\n"
            + "".join(
                f"{query}\tpass\t{checksum}\t-\n"
                for query, checksum in self.checksums.items()
            ),
            encoding="utf-8",
        )
        destination.joinpath("checksums.sha256").write_text(
            "".join(
                f"{checksum}  {query}\n" for query, checksum in self.checksums.items()
            ),
            encoding="utf-8",
        )
        manifest = tpch.sha256(self.output / "tpch" / "dataset" / "manifest.sha256")
        remote = medium == "minio"
        destination.joinpath("provenance.json").write_text(
            json.dumps(
                {
                    "format_version": 1,
                    "source": {"git_commit": self.commit, "worktree_dirty": False},
                    "binary": {
                        "path": "target/release/rustdb",
                        "sha256": self.binary,
                        "build_skipped": remote,
                    },
                    "queries": tpch.query_contract(ROOT, medium),
                    "dataset": {
                        "reference_manifest_sha256": manifest,
                        "rustdb_manifest_sha256": None if remote else manifest,
                        "rustdb_root": (
                            "s3://rustdb-tests/tpch-sf1" if remote else "data/tpch-sf1"
                        ),
                    },
                    "configuration": {
                        "memory_limit_bytes": tpch.MEMORY_LIMIT,
                        "compute_threads": tpch.THREADS,
                        "batch_size": tpch.BATCH_SIZE,
                        "io_concurrency": tpch.IO_CONCURRENCY,
                        "require_spill": False,
                        "spill_directory": None,
                        "s3_endpoint": "http://minio:9000" if remote else None,
                        "s3_region": "us-east-1" if remote else None,
                        "s3_path_style": "1" if remote else None,
                    },
                }
            ),
            encoding="utf-8",
        )

    def test_accepts_exact_local_and_minio_sf1_reports(self) -> None:
        accepted = evidence.accepted_tpch(self.output, ROOT, self.commit)
        self.assertEqual(accepted["local"]["queries"], 22)
        self.assertEqual(accepted["local"]["checksums"], self.checksums)
        self.assertEqual(accepted["minio"]["binary_sha256"], self.binary)
        contract = evidence.execution_contract()["tpch_sf1"]
        self.assertEqual(contract["storage_passes"], {"local": 1, "minio": 1})
        self.assertEqual(contract["comparison_claim"], "correctness_only")

    def test_rejects_missing_query_and_cross_medium_checksum_drift(self) -> None:
        status = self.output / "tpch" / "local" / "status.tsv"
        status.write_text("\n".join(status.read_text().splitlines()[:-1]) + "\n")
        with self.assertRaisesRegex(ValueError, "Q1-Q22"):
            tpch.summary(self.output, "local", self.commit, ROOT)

        self.write_report("local")
        checksums = self.output / "tpch" / "minio" / "checksums.sha256"
        checksums.write_text(
            checksums.read_text().replace(self.checksums["q22"], "f" * 64),
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValueError, "status and checksum"):
            evidence.accepted_tpch(self.output, ROOT, self.commit)

    def test_rejects_commit_dataset_and_execution_drift(self) -> None:
        provenance = self.output / "tpch" / "minio" / "provenance.json"
        value = json.loads(provenance.read_text())
        value["source"]["git_commit"] = "d" * 40
        provenance.write_text(json.dumps(value), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "commit/dataset bound"):
            tpch.summary(self.output, "minio", self.commit, ROOT)

        self.write_report("minio")
        value = json.loads(provenance.read_text())
        value["configuration"]["compute_threads"] = 8
        provenance.write_text(json.dumps(value), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "configuration changed"):
            tpch.summary(self.output, "minio", self.commit, ROOT)


if __name__ == "__main__":
    unittest.main()
