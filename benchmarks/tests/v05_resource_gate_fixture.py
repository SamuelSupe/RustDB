import hashlib
import json
import math
import sys
from pathlib import Path


BENCHMARKS = Path(__file__).resolve().parents[1]
ROOT = BENCHMARKS.parent
sys.path.insert(0, str(BENCHMARKS))

from v05_resource_gate import JOIN_TEMPLATES, evaluate  # noqa: E402
from v05_low_memory_gate import LOW_MEMORY_CASES, MEMORY_LIMITS  # noqa: E402

CANDIDATE_BUILD = "a" * 40
BASELINE_BUILD = "b" * 40
CANDIDATE_BINARY = "c" * 64
BASELINE_BINARY = "d" * 64
CPU_MODEL = "Apple M5 Max"


def timed_runs(elapsed):
    ordered = sorted(elapsed)
    return {
        "iterations": len(elapsed),
        "runs": [{"elapsed_ms": value} for value in elapsed],
        "p50_ms": ordered[math.ceil((len(ordered) - 1) * 0.50)],
        "p95_ms": ordered[math.ceil((len(ordered) - 1) * 0.95)],
    }


class ResourceGateFixture:
    def __init__(self, root: Path):
        self.root = root
        self.manifest = root / "manifest.sha256"
        self.manifest.write_text("fixture SF10 manifest\n", encoding="utf-8")
        self.dataset_digest = hashlib.sha256(self.manifest.read_bytes()).hexdigest()
        self.paths = {}

        q17_query = self.render(
            ROOT / "benchmarks/tpch/q17.sql", "q17.sql", "/workspace/data/tpch-sf10"
        )
        candidate_query = self.render(
            ROOT / "benchmarks/tpch/q21.sql",
            "candidate/q21.sql",
            "/workspace/data/tpch-sf10",
        )
        baseline_query = self.render(
            ROOT / "benchmarks/tpch/q21.sql", "baseline/q21.sql", "/dataset"
        )

        self.paths["q17"] = self.write(
            "q17.json",
            self.report(
                q17_query,
                runs=[
                    {
                        "spill_write_bytes": 8 * 1024**3,
                        "peak_active_spill_bytes": 2 * 1024**3,
                        "max_repartition_depth": 1,
                        "peak_active_spill_files": 512,
                    }
                ],
                warmup=0,
            ),
        )
        self.paths["q21_candidate"] = self.write(
            "q21-candidate.json",
            self.report(
                candidate_query,
                **timed_runs([40, 41, 42, 43, 50]),
            ),
        )
        self.paths["q21_baseline"] = self.write(
            "q21-baseline.json",
            self.report(
                baseline_query,
                engine_version="0.4.0-alpha.1",
                build_id=BASELINE_BUILD,
                binary_sha256=BASELINE_BINARY,
                **timed_runs([90, 95, 100, 105, 110]),
            ),
        )
        self.q17_checksum = self.write_checksum("q17.checksum.txt", "q17")
        self.q21_checksum = self.write_checksum("q21.checksum.txt", "q21")

        low_root = self.root / "low-memory"
        low_manifest = low_root / "manifest.json"
        low_runs = []
        self.join_paths = []
        for name, template in LOW_MEMORY_CASES.items():
            query = self.render(
                template,
                f"low-memory/rendered/{name}.sql",
                "/workspace/data/tpch-sf10",
            )
            for limit in MEMORY_LIMITS:
                report = self.write(
                    f"low-memory/reports/{name}-{limit}.json",
                    self.report(
                        query,
                        runs=[
                            {
                                "scanned_bytes": 100,
                                "peak_memory_bytes": limit - 1,
                                "spill_write_bytes": 100,
                                "spill_read_bytes": 100,
                                "peak_active_spill_files": 1,
                            }
                        ],
                        warmup=0,
                        memory_limit=limit,
                        dataset=False,
                    ),
                )
                checksum = self.write_checksum(
                    f"low-memory/reports/{name}-{limit}.checksum.txt",
                    f"{name}-{limit}",
                )
                low_runs.append(
                    {
                        "case": name,
                        "memory_limit_bytes": limit,
                        "require_spill": True,
                        "report": str(report),
                        "checksum_report": str(checksum),
                    }
                )
                if name in JOIN_TEMPLATES and limit == MEMORY_LIMITS[1]:
                    self.paths[name] = report
                    self.join_paths.append(report)
        low_manifest.write_text(
            json.dumps(
                {
                    "suite": "rustdb-low-memory-v1",
                    "rustdb_build_id": CANDIDATE_BUILD,
                    "benchmark_binary_sha256": CANDIDATE_BINARY,
                    "build": {
                        "cargo_profile": "release",
                        "rustflags": "-C target-cpu=native",
                        "rustc_version": "rustc 1.97.0",
                    },
                    "dataset": self.dataset(),
                    "correctness": {
                        "verified": True,
                        "memory_limited": True,
                        "spill_required": True,
                    },
                    "config": {
                        "threads": 4,
                        "batch_size": 8192,
                        "io_concurrency": 32,
                        "warmup": 0,
                        "iterations": 1,
                    },
                    "assertions": {
                        "full_result_consumed": True,
                        "peak_memory_within_limit": True,
                        "spill_required": True,
                        "spill_directories_cleaned": True,
                    },
                    "runs": low_runs,
                }
            ),
            encoding="utf-8",
        )
        self.low_memory_manifest = low_manifest

    def dataset(self):
        return {
            "generation": {"scale_factor": "10", "duckdb": "1.4.3"},
            "manifest": str(self.manifest),
            "manifest_sha256": self.dataset_digest,
        }

    def report(
        self,
        query_file,
        *,
        runs,
        warmup=2,
        iterations=None,
        p50_ms=None,
        p95_ms=None,
        engine_version="0.5.0-alpha.1",
        build_id=CANDIDATE_BUILD,
        binary_sha256=CANDIDATE_BINARY,
        dataset=True,
        memory_limit=134217728,
    ):
        if engine_version == "0.5.0-alpha.1":
            for run in runs:
                run.setdefault("current_memory_bytes", 0)
                run.setdefault("active_spill_bytes", 0)
                run.setdefault("active_spill_files", 0)
                run.setdefault("spill_cleaned", True)
        document = {
            "engine_version": engine_version,
            "build_id": build_id,
            "binary_sha256": binary_sha256,
            "build": {
                "cargo_profile": "release",
                "rustflags": "-C target-cpu=native",
                "rustc_version": "rustc 1.97.0",
            },
            "query_file": str(query_file),
            "warmup": warmup,
            "iterations": len(runs) if iterations is None else iterations,
            "config": {
                "memory_limit_bytes": memory_limit,
                "compute_threads": 4,
                "batch_size": 8192,
                "io_concurrency": 32,
                "metadata_cache_bytes": 0,
            },
            "environment": {
                "os": "linux",
                "arch": "aarch64",
                "cpu_model": CPU_MODEL,
            },
            "runs": runs,
        }
        if dataset:
            document["dataset"] = self.dataset()
        if p50_ms is not None:
            document["p50_ms"] = p50_ms
        if p95_ms is not None:
            document["p95_ms"] = p95_ms
        return document

    def render(self, template, relative, root):
        destination = self.root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(
            template.read_text(encoding="utf-8").replace("__TPCH_ROOT__", root),
            encoding="utf-8",
        )
        return destination

    def write(self, name, document):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(document), encoding="utf-8")
        return path

    def write_checksum(self, name, seed):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(hashlib.sha256(seed.encode()).hexdigest() + "\n", encoding="utf-8")
        return path

    def load(self, name):
        return json.loads(self.paths[name].read_text(encoding="utf-8"))

    def replace(self, name, document):
        self.paths[name].write_text(json.dumps(document), encoding="utf-8")

    def load_low_manifest(self):
        return json.loads(self.low_memory_manifest.read_text(encoding="utf-8"))

    def replace_low_manifest(self, document):
        self.low_memory_manifest.write_text(json.dumps(document), encoding="utf-8")

    def evaluate(self, **overrides):
        options = {
            "baseline_build_id": BASELINE_BUILD,
            "candidate_build_id": CANDIDATE_BUILD,
            "candidate_binary_sha256": CANDIDATE_BINARY,
            "actual_cpu_model": CPU_MODEL,
            "expected_dataset_manifest_sha256": self.dataset_digest,
        }
        options.update(overrides)
        return evaluate(
            self.paths["q17"],
            self.paths["q21_candidate"],
            self.paths["q21_baseline"],
            self.join_paths,
            self.q17_checksum,
            self.q21_checksum,
            self.low_memory_manifest,
            **options,
        )

    def cli_args(self):
        args = [
            "--q17",
            str(self.paths["q17"]),
            "--q17-checksum",
            str(self.q17_checksum),
            "--q21-candidate",
            str(self.paths["q21_candidate"]),
            "--q21-checksum",
            str(self.q21_checksum),
            "--q21-baseline",
            str(self.paths["q21_baseline"]),
            "--low-memory-manifest",
            str(self.low_memory_manifest),
            "--dataset-manifest",
            str(self.manifest),
        ]
        for path in self.join_paths:
            args.extend(("--join", str(path)))
        return args
