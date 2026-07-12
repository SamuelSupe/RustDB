#!/usr/bin/env python3
"""Strict fixed-hardware parallel performance gate for RustDB v0.4."""

from __future__ import annotations

import argparse
import re
import json
import subprocess
import sys
from pathlib import Path

sys.dont_write_bytecode = True

from parallel_gate import GateError, evaluate


SHA256 = re.compile(r"^[0-9a-f]{64}$")


def baseline_build_id() -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "v0.2.0-alpha.1^{commit}"],
            cwd=Path(__file__).resolve().parent.parent,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as error:
        raise GateError(f"cannot resolve local v0.2.0-alpha.1 tag: {error}") from error


def clean_candidate_build_id(root: Path | None = None) -> str:
    root = root or Path(__file__).resolve().parent.parent
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
        raise GateError("candidate worktree must be clean before evaluating performance evidence")
    return commit


def actual_cpu_model() -> str:
    commands = (
        ["sysctl", "-n", "machdep.cpu.brand_string"],
        ["sh", "-c", "lscpu | awk -F: '/^Model name:/ {sub(/^[ \\t]+/, \"\", $2); print $2; exit}'"],
    )
    for command in commands:
        try:
            model = subprocess.run(
                command, check=True, capture_output=True, text=True
            ).stdout.strip()
        except (OSError, subprocess.CalledProcessError):
            continue
        if model and model != "-":
            if "M5 Max" not in model:
                raise GateError(
                    f"fixed-hardware gate requires an Apple M5 Max, detected {model!r}"
                )
            return model
    raise GateError("cannot detect the host CPU model for the fixed-hardware gate")


def rebuild_candidate_binary_sha256(root: Path | None = None) -> str:
    root = root or Path(__file__).resolve().parent.parent
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
        "cargo build --quiet --release --bin rustdb-bench && "
        "sha256sum target/release/rustdb-bench",
    ]
    try:
        output = subprocess.run(
            command,
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.splitlines()
    except (OSError, subprocess.CalledProcessError) as error:
        detail = getattr(error, "stderr", "") or str(error)
        raise GateError(f"cannot rebuild the candidate benchmark executable: {detail}") from error
    digests = [line.split()[0] for line in output if line.split() and SHA256.fullmatch(line.split()[0])]
    if len(digests) != 1:
        raise GateError(
            "candidate rebuild did not emit exactly one benchmark executable SHA-256"
        )
    return digests[0]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True, help="candidate manifest.json")
    parser.add_argument(
        "--baseline", type=Path, required=True, help="v0.2.0-alpha.1 manifest.json"
    )
    parser.add_argument("--json", action="store_true", help="emit machine-readable results")
    args = parser.parse_args()
    try:
        root = Path(__file__).resolve().parent.parent
        candidate_build_id = clean_candidate_build_id(root)
        cpu_model = actual_cpu_model()
        binary_sha256 = rebuild_candidate_binary_sha256(root)
        result = evaluate(
            args.candidate.resolve(),
            args.baseline.resolve(),
            baseline_build_id(),
            candidate_build_id,
            binary_sha256,
            cpu_model,
        )
    except GateError as error:
        print(f"parallel performance gate: FAIL: {error}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        for case, values in result["cases"].items():
            print(
                f"{case}: {values['throughput_multiplier']:.3f}x parallel, "
                f"t1 regression {values['single_thread_regression']:.2%}"
            )
        print("parallel performance gate: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
