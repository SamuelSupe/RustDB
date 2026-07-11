#!/usr/bin/env python3
"""Strict fixed-hardware parallel performance gate for RustDB v0.2."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

sys.dont_write_bytecode = True

from parallel_gate import GateError, evaluate


def alpha2_build_id() -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "v0.1.0-alpha.2^{commit}"],
            cwd=Path(__file__).resolve().parent.parent,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as error:
        raise GateError(f"cannot resolve local v0.1.0-alpha.2 tag: {error}") from error


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


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", type=Path, required=True, help="candidate manifest.json")
    parser.add_argument("--baseline", type=Path, required=True, help="alpha.2 manifest.json")
    parser.add_argument("--json", action="store_true", help="emit machine-readable results")
    args = parser.parse_args()
    try:
        result = evaluate(
            args.candidate.resolve(),
            args.baseline.resolve(),
            alpha2_build_id(),
            clean_candidate_build_id(),
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
