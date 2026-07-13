#!/usr/bin/env python3
"""Strictly check RustDB v0.5 Q17, Q21, and Join release evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path

sys.dont_write_bytecode = True

from v05_resource_gate import (
    BASELINE_ENGINE_VERSION,
    CANDIDATE_ENGINE_VERSION,
    SHA256,
    GateError,
    evaluate,
)


DEFAULT_BASELINE_TAG = "v0.4.0-alpha.1"


def tagged_build_id(root: Path, tag: str) -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", f"{tag}^{{commit}}"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as error:
        raise GateError(f"cannot resolve local {tag} tag: {error}") from error


def clean_candidate_build_id(root: Path) -> str:
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        dirty = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=normal"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError) as error:
        raise GateError(f"cannot inspect candidate Git state: {error}") from error
    if dirty:
        raise GateError("candidate worktree must be clean before evaluating resource evidence")
    return commit


def actual_cpu_model() -> str:
    commands = (
        ["sysctl", "-n", "machdep.cpu.brand_string"],
        [
            "sh",
            "-c",
            "lscpu | awk -F: '/^Model name:/ "
            "{sub(/^[ \\t]+/, \"\", $2); print $2; exit}'",
        ],
    )
    for command in commands:
        try:
            model = subprocess.run(
                command,
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
        except (OSError, subprocess.CalledProcessError):
            continue
        if not model or model == "-":
            continue
        if "M5 Max" not in model:
            raise GateError(
                f"fixed-hardware resource gate requires an Apple M5 Max, detected {model!r}"
            )
        return model
    raise GateError("cannot detect the host CPU model for the fixed-hardware resource gate")


def rebuild_candidate_binary_sha256(root: Path) -> str:
    command = [
        "docker",
        "compose",
        "run",
        "--rm",
        "--no-deps",
        "--no-TTY",
        "--env",
        "RUSTFLAGS=-C target-cpu=native",
        "dev",
        "sh",
        "-c",
        "cargo build --locked --quiet --release --bin rustdb-bench && "
        "sha256sum target/release/rustdb-bench",
    ]
    try:
        lines = subprocess.run(
            command,
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.splitlines()
    except (OSError, subprocess.CalledProcessError) as error:
        detail = getattr(error, "stderr", "") or str(error)
        raise GateError(
            f"cannot rebuild the candidate benchmark executable: {detail}"
        ) from error
    digests = [line.split()[0] for line in lines if line.split()]
    digests = [digest for digest in digests if SHA256.fullmatch(digest)]
    if len(digests) != 1:
        raise GateError(
            "candidate rebuild did not emit exactly one benchmark executable SHA-256"
        )
    return digests[0]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
    except OSError as error:
        raise GateError(f"cannot read SF10 dataset manifest {path}: {error}") from error
    return digest.hexdigest()


def main(argv: list[str] | None = None) -> int:
    root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--q17", type=Path, required=True)
    parser.add_argument("--q17-checksum", type=Path, required=True)
    parser.add_argument("--q21-candidate", type=Path, required=True)
    parser.add_argument("--q21-checksum", type=Path, required=True)
    parser.add_argument("--q21-baseline", type=Path, required=True)
    parser.add_argument("--low-memory-manifest", type=Path, required=True)
    parser.add_argument(
        "--join",
        type=Path,
        action="append",
        required=True,
        help="128 MiB Join report; pass exactly one for each of six Join kinds",
    )
    parser.add_argument("--baseline-tag", default=DEFAULT_BASELINE_TAG)
    parser.add_argument(
        "--dataset-manifest",
        type=Path,
        default=root / "data/tpch-sf10/manifest.sha256",
        help="authoritative SF10 manifest.sha256 file",
    )
    parser.add_argument(
        "--allow-legacy-v04-missing-dataset",
        action="store_true",
        help=(
            "allow only the v0.4 Q21 JSON to omit dataset provenance; "
            "this weakens baseline comparability and is never the default"
        ),
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    try:
        candidate_build_id = clean_candidate_build_id(root)
        baseline_build_id = tagged_build_id(root, args.baseline_tag)
        cpu_model = actual_cpu_model()
        candidate_binary = rebuild_candidate_binary_sha256(root)
        dataset_digest = sha256_file(args.dataset_manifest.resolve())
        result = evaluate(
            args.q17.resolve(),
            args.q21_candidate.resolve(),
            args.q21_baseline.resolve(),
            [path.resolve() for path in args.join],
            args.q17_checksum.resolve(),
            args.q21_checksum.resolve(),
            args.low_memory_manifest.resolve(),
            baseline_build_id=baseline_build_id,
            candidate_build_id=candidate_build_id,
            candidate_binary_sha256=candidate_binary,
            actual_cpu_model=cpu_model,
            expected_dataset_manifest_sha256=dataset_digest,
            allow_legacy_v04_missing_dataset=args.allow_legacy_v04_missing_dataset,
        )
    except GateError as error:
        print(f"v0.5 resource gate: FAIL: {error}", file=sys.stderr)
        return 1

    if result["evidence"]["legacy_v04_missing_dataset"]:
        print(
            "warning: accepted v0.4 Q21 evidence without dataset provenance; "
            "the candidate remains strict",
            file=sys.stderr,
        )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        q21 = result["q21"]
        print(
            f"Q21: {q21['candidate_to_baseline_p50']:.3f}x baseline p50, "
            f"{q21['p95_to_p50']:.3f} p95/p50"
        )
        print(f"Join reports: {len(result['joins'])}")
        print(f"Candidate build: {result['evidence']['candidate_build_id']}")
        print("v0.5 resource gate: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
